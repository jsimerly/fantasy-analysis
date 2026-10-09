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

import numpy as np
import polars as pl

import career
import model as one_year

DEPTH_PATH = "silver/fantasy/fact_depth_chart_week/data.parquet"
INJURY_PATH = "silver/fantasy/fact_player_injury_week/data.parquet"
WEEK_PATH = "silver/fantasy/fact_player_week/data.parquet"
COLLEGE_PATH = "silver/fantasy/fact_college_player_season/data.parquet"
COLLEGE_XWALK_PATH = "silver/fantasy/dim_college_crosswalk/data.parquet"
WEEK_STATUS_PATH = "silver/fantasy/fact_player_week_status/data.parquet"
TEAM_PATH = "silver/fantasy/fact_team_season_strength/data.parquet"
CONTRACT_PATH = "silver/fantasy/fact_player_contract_season/data.parquet"
TEAM_CODE = {"OAK": "LV", "SD": "LAC", "STL": "LA", "JAC": "JAX", "LAR": "LA"}   # every era's code -> the current franchise
KEY = ["player_id", "season"]


class Context:
    """Lake tables for feature building, loaded on first use (or injected)."""

    def __init__(self, depth: pl.DataFrame | None = None, injury: pl.DataFrame | None = None,
                 weeks: pl.DataFrame | None = None, team: pl.DataFrame | None = None, contracts: pl.DataFrame | None = None):
        self._depth, self._injury, self._weeks = depth, injury, weeks
        self._team, self._contracts = team, contracts

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

    _college: pl.DataFrame | None = None
    _college_cfbd: pl.DataFrame | None = None

    def _load_college(self) -> None:
        try:
            fact, xw = self._load(COLLEGE_PATH), self._load(COLLEGE_XWALK_PATH)
        except Exception:  # noqa: BLE001 - not ingested yet
            self._college, self._college_cfbd = pl.DataFrame(), pl.DataFrame()
        else:
            self._college_cfbd = college_per_cfbd(fact, xw)
            self._college = college_per_player(fact, xw)

    @property
    def college(self) -> pl.DataFrame | None:
        """College production per player keyed by gsis id (None until the CFBD lake tables exist)."""
        if self._college is None:
            self._load_college()
        return self._college if self._college is not None and self._college.height else None

    @property
    def college_cfbd(self) -> pl.DataFrame | None:
        """The same per CFBD athlete id, for draft rows of players without NFL rows."""
        if self._college_cfbd is None:
            self._load_college()
        return self._college_cfbd if self._college_cfbd is not None and self._college_cfbd.height else None

    _week_status: pl.DataFrame | None = None

    @property
    def week_status(self) -> pl.DataFrame | None:
        """What every player did each regular-season week (None until the silver table exists)."""
        if self._week_status is None:
            try:
                self._week_status = self._load(WEEK_STATUS_PATH)
            except Exception:  # noqa: BLE001 - not built yet
                self._week_status = pl.DataFrame()
        return self._week_status if self._week_status.height else None

    _team: pl.DataFrame | None = None

    @property
    def team(self) -> pl.DataFrame | None:
        """Team strength per (season, team) (None until the silver table exists)."""
        if self._team is None:
            try:
                self._team = self._load(TEAM_PATH)
            except Exception:  # noqa: BLE001 - not built yet
                self._team = pl.DataFrame()
        return self._team if self._team.height else None

    _contracts: pl.DataFrame | None = None

    @property
    def contracts(self) -> pl.DataFrame | None:
        """The contract in force per (gsis_id, season) (None until the silver table exists)."""
        if self._contracts is None:
            try:
                self._contracts = self._load(CONTRACT_PATH)
            except Exception:  # noqa: BLE001 - not built yet
                self._contracts = pl.DataFrame()
        return self._contracts if self._contracts.height else None


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


# ------------------------------------------------------------------------------ college
COLLEGE_COLS = ["col_dominator_last", "col_dominator_best", "col_yptp_last", "col_usage_last", "col_breakout_age",
                "col_seasons", "col_team_sp_last", "col_early_declare", "col_touch_share_last"]


def college_per_cfbd(fact: pl.DataFrame, xw: pl.DataFrame) -> pl.DataFrame:
    """One row per CFBD athlete id: the final college season's shares and usage, the best dominator,
    the breakout age (age in the first season with dominator >= 0.20; needs the crosswalk's birth
    date), seasons played, the final team's SP+ rating, and early declaration (final class year <= 3)."""
    f = fact.sort(["cfbd_id", "season"])
    last = f.group_by("cfbd_id").agg(
        pl.col("dominator").last().alias("col_dominator_last"), pl.col("dominator").max().alias("col_dominator_best"),
        pl.col("yards_per_team_play").last().alias("col_yptp_last"),
        (pl.col("usage_overall").last() if "usage_overall" in f.columns else pl.lit(None, pl.Float64)).alias("col_usage_last"),
        pl.col("touch_share").last().alias("col_touch_share_last"),
        pl.len().alias("col_seasons"), pl.col("team_sp").last().alias("col_team_sp_last"),
        (pl.col("class_year").last() if "class_year" in f.columns else pl.lit(None, pl.Int64)).alias("_class_last"),
        pl.col("breakout_season_dom").first().alias("_breakout"))
    x = xw.select("cfbd_id", pl.col("birth_date") if "birth_date" in xw.columns else pl.lit(None, pl.Utf8).alias("birth_date")).unique("cfbd_id")
    out = last.join(x, on="cfbd_id", how="left")
    by = pl.col("birth_date").cast(pl.Utf8).str.slice(0, 4).cast(pl.Int64, strict=False)
    return out.with_columns(
        (pl.col("_breakout") - by).cast(pl.Float64).alias("col_breakout_age"),
        (pl.col("_class_last") <= 3).cast(pl.Float64).alias("col_early_declare"),
        pl.col("col_seasons").cast(pl.Float64)).select(["cfbd_id"] + COLLEGE_COLS).unique("cfbd_id")


def college_per_player(fact: pl.DataFrame, xw: pl.DataFrame) -> pl.DataFrame:
    """The per-athlete college row keyed by gsis id (players the crosswalk ties to the NFL side)."""
    per = college_per_cfbd(fact, xw)
    x = xw.filter(pl.col("gsis_id").is_not_null()).select("cfbd_id", "gsis_id").unique("cfbd_id")
    return per.join(x, on="cfbd_id", how="inner").select(["gsis_id"] + COLLEGE_COLS).unique("gsis_id")


def build_college(matrix: pl.DataFrame, ctx: Context) -> pl.DataFrame:
    """Join the per-player college row onto every season row (static over a career); nulls when the
    lake has no college data for the player or at all."""
    col = ctx.college
    if col is None:
        return matrix.with_columns([pl.lit(None, pl.Float64).alias(c) for c in COLLEGE_COLS if c not in matrix.columns])
    out = matrix.join(col.rename({"gsis_id": "player_id"}), on="player_id", how="left")
    cf = ctx.college_cfbd
    if cf is not None and "cfbd_id" in out.columns:            # draft rows: by the athlete id, filling what the gsis join left null
        cf = cf.with_columns(pl.col("cfbd_id").cast(pl.Int64)).rename({c: f"{c}_cf" for c in COLLEGE_COLS})
        out = (out.with_columns(pl.col("cfbd_id").cast(pl.Int64, strict=False)).join(cf, on="cfbd_id", how="left")
                  .with_columns([pl.coalesce([pl.col(c), pl.col(f"{c}_cf")]).alias(c) for c in COLLEGE_COLS])
                  .drop([f"{c}_cf" for c in COLLEGE_COLS]))
    return out


# ------------------------------------------------------------------------------ weekly
# the season as a sequence: one slot per regular-season week (18 since 2021, 17 before: slot 18 is null
# then), each with what the player did (STATUS_CODES), the body part if he was hurt (INJURY_CODES), his
# points, opportunities (targets + carries) and offensive snap share (2013+); plus how many weeks of
# the season, and of the season before, went to each reason. Weeks before 2002 have no reasons: null.
STATUS_CODES = {"played": 0, "bye": 1, "injured_reserve": 2, "injured_out": 3, "suspended": 4,
                "practice_squad": 5, "inactive": 6, "dnp": 7, "not_rostered": 8}
INJURY_CODES = {"soft_tissue": 1, "knee_achilles": 2, "ankle_foot": 3, "upper_body": 4, "concussion": 5, "not_injury": 6, "other": 7}
WEEK_SLOTS = 18
SLOT_KINDS = {"status": "status_code", "inj": "inj_code", "fpts": "fpts", "opp": "opportunities", "snap": "offense_pct"}
WEEKLY_SLOT_COLS = [f"wk{w}_{k}" for w in range(1, WEEK_SLOTS + 1) for k in SLOT_KINDS]
REASON_COLS = ["wks_played", "wks_bye", "wks_inj_reserve", "wks_inj_out", "wks_suspended", "wks_practice_squad", "wks_inactive",
               "wks_dnp", "wks_unrostered", "inj_wks_soft", "inj_wks_knee", "inj_wks_ankle", "inj_wks_upper", "inj_wks_concussion", "inj_wks_other"]
WEEKLY_COLS = WEEKLY_SLOT_COLS + REASON_COLS + [f"lag1_{c}" for c in REASON_COLS] + ["snap_pct_mean", "snap_pct_last4"]


def weekly_per_season(ws: pl.DataFrame) -> pl.DataFrame:
    """(player_id, season) -> the slot columns, the reason counts, and the snap summaries."""
    d = (ws.filter(pl.col("week").is_between(1, WEEK_SLOTS))
           .with_columns(pl.col("status").replace_strict(STATUS_CODES, default=None, return_dtype=pl.Int8).alias("status_code"),
                         pl.col("injury_class").replace_strict(INJURY_CODES, default=None, return_dtype=pl.Int8).alias("inj_code"),
                         pl.col("week").cast(pl.Int32), pl.col("season").cast(pl.Int64))
           .with_columns(pl.when(pl.col("status").is_in(["injured_reserve", "injured_out"])).then(pl.col("inj_code").fill_null(0)).otherwise(None).alias("inj_code")))
    slots = d.pivot(on="week", index=["gsis_id", "season"], values=list(SLOT_KINDS.values()), aggregate_function="first")
    ren = {}
    for k, v in SLOT_KINDS.items():
        for w in range(1, WEEK_SLOTS + 1):
            for cand in (f"{v}_{w}", f"{v}_week_{w}", f"{w}_{v}"):
                if cand in slots.columns:
                    ren[cand] = f"wk{w}_{k}"
    slots = slots.rename(ren)
    for c in WEEKLY_SLOT_COLS:
        if c not in slots.columns:
            slots = slots.with_columns(pl.lit(None, pl.Float64).alias(c))
    slots = slots.select(["gsis_id", "season"] + WEEKLY_SLOT_COLS)
    st = lambda s: (pl.col("status") == s).cast(pl.Int32).sum()           # noqa: E731
    ic = lambda s: (pl.col("injury_class") == s).cast(pl.Int32).sum()     # noqa: E731
    played_snaps = pl.col("offense_pct").filter(pl.col("status") == "played")
    counts = d.sort("week").group_by("gsis_id", "season").agg(
        st("played").alias("wks_played"), st("bye").alias("wks_bye"), st("injured_reserve").alias("wks_inj_reserve"), st("injured_out").alias("wks_inj_out"),
        st("suspended").alias("wks_suspended"), st("practice_squad").alias("wks_practice_squad"), st("inactive").alias("wks_inactive"),
        st("dnp").alias("wks_dnp"), st("not_rostered").alias("wks_unrostered"),
        ic("soft_tissue").alias("inj_wks_soft"), ic("knee_achilles").alias("inj_wks_knee"), ic("ankle_foot").alias("inj_wks_ankle"),
        ic("upper_body").alias("inj_wks_upper"), ic("concussion").alias("inj_wks_concussion"),
        pl.col("injury_class").is_in(["other", "not_injury"]).cast(pl.Int32).sum().alias("inj_wks_other"),
        played_snaps.mean().alias("snap_pct_mean"), played_snaps.tail(4).mean().alias("snap_pct_last4"))
    return slots.join(counts, on=["gsis_id", "season"], how="left").rename({"gsis_id": "player_id"})


def build_weekly(matrix: pl.DataFrame, ctx: Context) -> pl.DataFrame:
    ws = ctx.week_status
    if ws is None:
        return matrix
    per = weekly_per_season(ws)
    lag = per.select("player_id", (pl.col("season") + 1).alias("season"), *[pl.col(c).alias(f"lag1_{c}") for c in REASON_COLS])
    first = int(per["season"].min())
    out = matrix.join(per, on=KEY, how="left").join(lag, on=KEY, how="left")
    out = _coverage_fill(out, REASON_COLS, first)
    return _coverage_fill(out, [f"lag1_{c}" for c in REASON_COLS], first + 1)


# ---------------------------------------------------------------------------- registry

# ------------------------------------------------------------------------------ team
# how good the player's team was that season (fact_team_season_strength): the market's rating (mean
# favoritism margin over the closing spreads, which prices the QB and the injuries), the scoring
# environment (total line, implied points), the realized point differential and record, offensive
# EPA per play and pass rate, the QB situation (starters used, the main starter's share), the
# team's own lags, and the change a mover sees (this team's rating vs his previous team's last year).
TEAM_FACT_COLS = ["mkt_margin", "mkt_margin_last4", "mkt_total", "mkt_implied_pts", "mkt_win_prob", "point_diff_pg", "win_pct",
                  "off_epa_per_play", "pass_rate", "plays_pg", "n_starting_qbs", "qb_main_share", "lag1_mkt_margin", "lag1_off_epa_per_play"]
TEAM_COLS = [f"tm_{c}" for c in TEAM_FACT_COLS] + ["tm_mkt_margin_vs_prev_team"]


def normalize_team(col: str = "team") -> pl.Expr:
    return pl.col(col).cast(pl.Utf8).str.strip_chars().replace(TEAM_CODE)


def build_team(matrix: pl.DataFrame, ctx: Context) -> pl.DataFrame:
    tf = ctx.team
    if tf is None or "team" not in matrix.columns:
        return matrix.with_columns([pl.lit(None, pl.Float64).alias(c) for c in TEAM_COLS if c not in matrix.columns])
    fact = tf.with_columns(pl.col("season").cast(pl.Int64), normalize_team("team").alias("team"))
    fact = fact.select(["season", "team"] + [pl.col(c).cast(pl.Float64, strict=False).alias(f"tm_{c}") for c in TEAM_FACT_COLS if c in fact.columns])
    out = matrix.with_columns(normalize_team("team").alias("_tm")).join(fact.rename({"team": "_tm"}), on=["season", "_tm"], how="left")
    # the previous team's rating last season, for the change a mover sees
    prev_team = matrix.select("player_id", (pl.col("season") + 1).alias("season"), normalize_team("team").alias("_prev_tm")).unique(KEY)
    prev_rating = fact.select((pl.col("season") + 1).alias("season"), pl.col("team").alias("_prev_tm"), pl.col("tm_mkt_margin").alias("_prev_margin"))
    out = (out.join(prev_team, on=KEY, how="left").join(prev_rating, on=["season", "_prev_tm"], how="left")
              .with_columns((pl.col("tm_mkt_margin") - pl.col("_prev_margin")).alias("tm_mkt_margin_vs_prev_team"))
              .drop(["_tm", "_prev_tm", "_prev_margin"]))
    return out.with_columns([pl.lit(None, pl.Float64).alias(c) for c in TEAM_COLS if c not in out.columns])


# ------------------------------------------------------------------------------ contract
# the contract in force that season (fact_player_contract_season, Over The Cap via nflverse): APY as
# a share of the cap, guarantees as a share of the cap at signing, years left after the season, the
# contract-year and rookie-deal flags, the age of the deal, the cap number the team carried that year
# and the one scheduled next year, the rank among the position's deals, how many deals so far; plus
# a has-contract flag so "no deal on file" is a state, not a null (seasons before 1994 stay null).
CONTRACT_FACT_COLS = {"apy_cap_pct": "ct_apy_cap_pct", "guaranteed_cap_pct": "ct_guaranteed_cap_pct", "years_left": "ct_years_left",
                      "contract_year": "ct_contract_year", "contract_age": "ct_contract_age", "contract_years": "ct_contract_years",
                      "is_rookie_deal": "ct_is_rookie_deal", "cap_pct_season": "ct_cap_pct_season", "cap_pct_next": "ct_cap_pct_next",
                      "guaranteed_salary_season": "ct_guaranteed_salary_season", "apy_cap_pct_pos_pctl": "ct_pos_pctl", "n_contracts_signed": "ct_n_contracts"}
CONTRACT_COLS = list(CONTRACT_FACT_COLS.values()) + ["ct_has_contract"]
CONTRACT_FIRST_SEASON = 1994


def build_contract(matrix: pl.DataFrame, ctx: Context) -> pl.DataFrame:
    cf = ctx.contracts
    if cf is None:
        return matrix.with_columns([pl.lit(None, pl.Float64).alias(c) for c in CONTRACT_COLS if c not in matrix.columns])
    fact = cf.with_columns(pl.col("season").cast(pl.Int64), pl.col("gsis_id").cast(pl.Utf8).str.strip_chars().alias("player_id"))
    fact = fact.select(KEY + [pl.col(c).cast(pl.Float64, strict=False).alias(n) for c, n in CONTRACT_FACT_COLS.items() if c in fact.columns]
                       + [pl.lit(1.0).alias("ct_has_contract")]).unique(KEY, keep="first", maintain_order=True)
    out = matrix.join(fact, on=KEY, how="left")
    out = out.with_columns(pl.when(pl.col("season") >= CONTRACT_FIRST_SEASON).then(pl.col("ct_has_contract").fill_null(0.0)).otherwise(None).alias("ct_has_contract"))
    return out.with_columns([pl.lit(None, pl.Float64).alias(c) for c in CONTRACT_COLS if c not in out.columns])



# ------------------------------------------------------------------------------ noise
# a control, not a feature: 15 columns of seeded Gaussian noise keyed by (player_id, season). Four
# different additions to the 3.5 pooled candidate (team, contract, both, weekly) all cost the top 150
# the same ~0.04 wins of error while sorting the whole pool better; if noise does the same, that is the
# width of the table acting on the model, not information in the columns.
NOISE_COLS = [f"noise_{i:02d}" for i in range(15)]


def build_noise(matrix: pl.DataFrame, ctx: Context) -> pl.DataFrame:
    import hashlib
    keys = matrix.select(KEY).unique()
    rows = []
    for pid, season in keys.iter_rows():
        seed = int(hashlib.md5(f"{pid}|{season}".encode()).hexdigest()[:8], 16)
        rng = np.random.default_rng(seed)
        rows.append([pid, season] + [float(x) for x in rng.standard_normal(len(NOISE_COLS))])
    noise = pl.DataFrame(rows, schema=KEY + NOISE_COLS, orient="row").with_columns(pl.col("season").cast(matrix.schema["season"]))
    return matrix.join(noise, on=KEY, how="left")


GROUPS: dict[str, FeatureGroup] = {
    "base": FeatureGroup("base", list(one_year.FEATURE_COLS), None, "fact_player_season (+ lags)"),
    "career": FeatureGroup("career", list(career.CAREER_FEATURES), None, "fact_player_season cumulative"),
    "injury": FeatureGroup("injury", INJURY_COLS, build_injury, "fact_player_injury_week"),
    "role": FeatureGroup("role", ROLE_COLS, build_role, "fact_depth_chart_week"),
    "trend": FeatureGroup("trend", TREND_COLS, build_trend, "fact_player_week (second half vs first half, last 4)"),
    "situation": FeatureGroup("situation", SITUATION_COLS, build_situation, "team changes (fact_player_season)"),
    "rookie": FeatureGroup("rookie", ROOKIE_COLS, build_rookie, "draft capital x experience (fact_player_season)"),
    "college": FeatureGroup("college", COLLEGE_COLS, build_college, "fact_college_player_season + dim_college_crosswalk (CFBD)"),
    "weekly": FeatureGroup("weekly", WEEKLY_COLS, build_weekly, "fact_player_week_status (18 weekly slots: status, injury class, points, opportunities, snap share; miss-reason counts)"),
    "team": FeatureGroup("team", TEAM_COLS, build_team, "fact_team_season_strength (schedules closing lines + team_stats EPA)"),
    "contract": FeatureGroup("contract", CONTRACT_COLS, build_contract, "fact_player_contract_season (Over The Cap via nflverse)"),
    "noise": FeatureGroup("noise", NOISE_COLS, build_noise, "control: seeded Gaussian noise, no information"),
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
