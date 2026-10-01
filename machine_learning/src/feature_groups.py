"""Named feature groups for the season-level (career) model, so variants can be mixed and matched.

Every group adds columns to the career matrix keyed by (player_id, season) using only information
known by the END of season T (the row's season). That keeps every variant comparable in the same
walk-forward backtest, whose market reference is KTC the following February. Signals that need
next-season information (the week-1 depth chart, a new team for next year) are in-season features
and are deliberately not here.

    GROUPS                         name -> FeatureGroup(name, columns, build, source)
    resolve(["base", "injury"])    -> ordered, de-duplicated groups (ValueError on an unknown name)
    feature_columns(groups)        -> the model's feature list for that combination
    assemble(matrix, groups, ctx)  -> matrix with every selected group's columns present (null if a
                                      source has no coverage for that season)

``ctx`` is a ``Context`` holding the lake tables the groups read (depth charts, injuries, player
weeks); production loads them lazily from GCS, tests pass small frames.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import polars as pl

import career
import model as one_year

DEPTH_PATH = "silver/fantasy/fact_depth_chart_week/data.parquet"
INJURY_PATH = "silver/fantasy/fact_player_injury_week/data.parquet"
WEEK_PATH = "silver/fantasy/fact_player_week/data.parquet"
KEY = ["player_id", "season"]


class Context:
    """Lake tables for feature building, loaded on first use (or injected)."""

    def __init__(self, depth: pl.DataFrame | None = None, injury: pl.DataFrame | None = None,
                 weeks: pl.DataFrame | None = None):
        self._depth, self._injury, self._weeks = depth, injury, weeks

    @staticmethod
    def _load(path: str) -> pl.DataFrame:
        import gcs_io
        return gcs_io.read_lake(path)

    @property
    def depth(self) -> pl.DataFrame:
        if self._depth is None:
            self._depth = self._load(DEPTH_PATH)
        return self._depth

    @property
    def injury(self) -> pl.DataFrame:
        if self._injury is None:
            self._injury = self._load(INJURY_PATH)
        return self._injury

    @property
    def weeks(self) -> pl.DataFrame:
        if self._weeks is None:
            self._weeks = self._load(WEEK_PATH)
        return self._weeks


@dataclass(frozen=True)
class FeatureGroup:
    name: str
    columns: list[str]
    build: Callable[[pl.DataFrame, Context], pl.DataFrame] | None   # None: columns already in the matrix
    source: str


def _reg(df: pl.DataFrame, col: str = "game_type") -> pl.DataFrame:
    """Regular-season rows (keeps rows whose game type is unknown)."""
    return df.filter(pl.col(col).is_null() | (pl.col(col) == "REG")) if col in df.columns else df


def _coverage_fill(matrix: pl.DataFrame, cols: list[str], first_season: int | None) -> pl.DataFrame:
    """Within the source's coverage a missing player means 0; before coverage it means unknown."""
    if first_season is None:
        return matrix
    return matrix.with_columns([
        pl.when(pl.col("season") >= first_season).then(pl.col(c).fill_null(0)).otherwise(None).alias(c) for c in cols
    ])


# ------------------------------------------------------------------------------ injury
INJURY_COLS = ["inj_weeks_out", "inj_weeks_listed", "inj_reserve_weeks", "inj_soft_tissue_weeks",
               "inj_structural_weeks", "inj_concussion_weeks", "lag1_inj_weeks_out", "lag1_inj_soft_tissue_weeks",
               "lag1_inj_structural_weeks"]


def build_injury(matrix: pl.DataFrame, ctx: Context) -> pl.DataFrame:
    inj = _reg(ctx.injury)
    if inj.height == 0:
        return matrix
    agg = (inj.group_by("gsis_id", "season").agg(
                pl.col("is_out").cast(pl.Int32).sum().alias("inj_weeks_out"),
                pl.col("on_injury_report").cast(pl.Int32).sum().alias("inj_weeks_listed"),
                pl.col("on_injured_reserve").cast(pl.Int32).sum().alias("inj_reserve_weeks"),
                (pl.col("injury_class") == "soft_tissue").cast(pl.Int32).sum().alias("inj_soft_tissue_weeks"),
                (pl.col("injury_class") == "knee_achilles").cast(pl.Int32).sum().alias("inj_structural_weeks"),
                (pl.col("injury_class") == "concussion").cast(pl.Int32).sum().alias("inj_concussion_weeks"))
              .rename({"gsis_id": "player_id"}).with_columns(pl.col("season").cast(pl.Int64)))
    lag = agg.select("player_id", (pl.col("season") + 1).alias("season"),
                     pl.col("inj_weeks_out").alias("lag1_inj_weeks_out"),
                     pl.col("inj_soft_tissue_weeks").alias("lag1_inj_soft_tissue_weeks"),
                     pl.col("inj_structural_weeks").alias("lag1_inj_structural_weeks"))
    first = int(agg["season"].min())
    out = matrix.join(agg, on=KEY, how="left").join(lag, on=KEY, how="left")
    out = _coverage_fill(out, [c for c in INJURY_COLS if not c.startswith("lag1_")], first)
    return _coverage_fill(out, [c for c in INJURY_COLS if c.startswith("lag1_")], first + 1)


# -------------------------------------------------------------------------------- role
ROLE_COLS = ["depth_end", "depth_start", "depth_best", "depth_weeks_listed", "depth_starter_share",
             "depth_moves", "depth_change_season", "position_rank_end"]


def build_role(matrix: pl.DataFrame, ctx: Context) -> pl.DataFrame:
    d = _reg(ctx.depth)
    if d.height == 0:
        return matrix
    agg = (d.sort("week").group_by("gsis_id", "season", "position").agg(
                pl.col("depth_rank").last().alias("depth_end"), pl.col("depth_rank").first().alias("depth_start"),
                pl.col("depth_rank").min().alias("depth_best"), pl.len().alias("depth_weeks_listed"),
                (pl.col("depth_rank") == 1).cast(pl.Float64).mean().alias("depth_starter_share"),
                (pl.col("depth_rank").diff().fill_null(0) != 0).cast(pl.Int32).sum().alias("depth_moves"),
                pl.col("position_rank").last().alias("position_rank_end"))
             .with_columns((pl.col("depth_end") - pl.col("depth_start")).alias("depth_change_season"))
             .rename({"gsis_id": "player_id"}).with_columns(pl.col("season").cast(pl.Int64)))
    out = matrix.join(agg, on=KEY + ["position"], how="left")
    return _coverage_fill(out, ["depth_weeks_listed", "depth_moves"], int(agg["season"].min()))


# ------------------------------------------------------------------------------- trend
TREND_COLS = ["ppg_first_half", "ppg_second_half", "trend_ppg", "trend_targets_pg", "trend_touches_pg",
              "last4_ppg", "last4_targets_pg"]


def build_trend(matrix: pl.DataFrame, ctx: Context) -> pl.DataFrame:
    w = ctx.weeks
    w = w.filter(pl.col("season_type") == "REG") if "season_type" in w.columns else w
    if w.height == 0:
        return matrix
    for c in ("targets", "rush_att", "rec"):
        if c not in w.columns:
            w = w.with_columns(pl.lit(0).alias(c))
    w = (w.sort(["player_id", "season", "week"])
          .with_columns(pl.int_range(pl.len()).over(KEY).alias("_gi"), pl.len().over(KEY).alias("_n"))
          .with_columns((pl.col("_gi") < pl.col("_n") / 2).alias("_first"),
                        (pl.col("rush_att") + pl.col("rec")).alias("_touches")))
    agg = (w.group_by(KEY).agg(
                pl.col("fpts").filter(pl.col("_first")).mean().alias("ppg_first_half"),
                pl.col("fpts").filter(~pl.col("_first")).mean().alias("ppg_second_half"),
                (pl.col("targets").filter(~pl.col("_first")).mean() - pl.col("targets").filter(pl.col("_first")).mean()).alias("trend_targets_pg"),
                (pl.col("_touches").filter(~pl.col("_first")).mean() - pl.col("_touches").filter(pl.col("_first")).mean()).alias("trend_touches_pg"),
                pl.col("fpts").tail(4).mean().alias("last4_ppg"), pl.col("targets").tail(4).mean().alias("last4_targets_pg"))
             .with_columns((pl.col("ppg_second_half") - pl.col("ppg_first_half")).alias("trend_ppg"),
                           pl.col("season").cast(pl.Int64)))
    return matrix.join(agg, on=KEY, how="left")


# --------------------------------------------------------------------------- situation
SITUATION_COLS = ["moved_this_season", "lag1_moved"]


def build_situation(matrix: pl.DataFrame, ctx: Context) -> pl.DataFrame:
    if "team" not in matrix.columns:
        return matrix
    teams = matrix.select(KEY + ["team"]).unique(KEY)
    prev = teams.select("player_id", (pl.col("season") + 1).alias("season"), pl.col("team").alias("_prev_team"))
    prev2 = teams.select("player_id", (pl.col("season") + 2).alias("season"), pl.col("team").alias("_prev2_team"))
    out = matrix.join(prev, on=KEY, how="left").join(prev2, on=KEY, how="left")
    return out.with_columns(
        pl.when(pl.col("_prev_team").is_null()).then(None).otherwise((pl.col("team") != pl.col("_prev_team")).cast(pl.Int8)).alias("moved_this_season"),
        pl.when(pl.col("_prev2_team").is_null() | pl.col("_prev_team").is_null()).then(None)
          .otherwise((pl.col("_prev_team") != pl.col("_prev2_team")).cast(pl.Int8)).alias("lag1_moved"),
    ).drop(["_prev_team", "_prev2_team"])


# ------------------------------------------------------------------------------ rookie
ROOKIE_COLS = ["pick_x_rookie", "pick_x_young", "pick_over_exp", "round_x_rookie"]


def build_rookie(matrix: pl.DataFrame, ctx: Context) -> pl.DataFrame:
    """Draft capital weighted by how new the player is: its pull on next-season points fades from
    -0.52 (rookie year) to -0.27 (year 11+), so give the trees the interaction explicitly.
    Undrafted players sit past the last pick (260) / round 8."""
    exp = pl.col("exp_at_season").cast(pl.Float64).fill_null(0.0)
    pick = pl.col("draft_pick").cast(pl.Float64).fill_null(260.0)
    rnd = pl.col("draft_round").cast(pl.Float64).fill_null(8.0)
    return matrix.with_columns(
        (pick * (exp == 0).cast(pl.Float64)).alias("pick_x_rookie"),
        (pick * (exp <= 2).cast(pl.Float64)).alias("pick_x_young"),
        (pick / (1.0 + exp)).alias("pick_over_exp"),
        (rnd * (exp == 0).cast(pl.Float64)).alias("round_x_rookie"),
    )


# ---------------------------------------------------------------------------- registry
GROUPS: dict[str, FeatureGroup] = {
    "base": FeatureGroup("base", list(one_year.FEATURE_COLS), None, "fact_player_season (+ lags)"),
    "career": FeatureGroup("career", list(career.CAREER_FEATURES), None, "fact_player_season cumulative"),
    "injury": FeatureGroup("injury", INJURY_COLS, build_injury, "fact_player_injury_week"),
    "role": FeatureGroup("role", ROLE_COLS, build_role, "fact_depth_chart_week"),
    "trend": FeatureGroup("trend", TREND_COLS, build_trend, "fact_player_week (second half vs first half, last 4)"),
    "situation": FeatureGroup("situation", SITUATION_COLS, build_situation, "team changes (fact_player_season)"),
    "rookie": FeatureGroup("rookie", ROOKIE_COLS, build_rookie, "draft capital x experience (fact_player_season)"),
}
DEFAULT = ["base", "career"]            # the production model today


def resolve(names: list[str] | str) -> list[FeatureGroup]:
    if isinstance(names, str):
        names = [n.strip() for n in names.split(",") if n.strip()]
    out: list[FeatureGroup] = []
    for n in names:
        if n not in GROUPS:
            raise ValueError(f"unknown feature group {n!r}; known: {sorted(GROUPS)}")
        if GROUPS[n] not in out:
            out.append(GROUPS[n])
    return out


def feature_columns(groups: list[FeatureGroup]) -> list[str]:
    cols: list[str] = []
    for g in groups:
        cols += [c for c in g.columns if c not in cols]
    return cols


def assemble(matrix: pl.DataFrame, groups: list[FeatureGroup], ctx: Context) -> pl.DataFrame:
    """Add every selected group's columns to the career matrix (null where a source is silent)."""
    out = matrix
    for g in groups:
        if g.build is not None:
            out = g.build(out, ctx)
        missing = [c for c in g.columns if c not in out.columns and not c.startswith("pos_")]
        if missing:
            out = out.with_columns([pl.lit(None, pl.Float64).alias(c) for c in missing])
    return out
