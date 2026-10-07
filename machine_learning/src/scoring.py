"""Post-hoc scoring adjustment: one model, many leagues.

The projection model is trained once, on the primary league's scoring (Stuck in High School).
Another league's rules are applied after the fact: every player's real weeks from the last few
seasons are re-scored under both rule sets, and the ratio of the two totals scales his projected
points per game (and his historical ppg, so replacement level is in the same units). A WR in a
league with the same receiving rules gets a ratio of 1; a turnover-prone QB in a league that
charges two points per interception gets a ratio a little under 1.

Only the keys the weekly fact can rebuild are used (yards, touchdowns, receptions,
interceptions, position reception bonuses); fumbles, two-point conversions and yardage bonuses
are shared across the owner's leagues and cancel in the ratio.
"""
from __future__ import annotations

import polars as pl

CORE_KEYS = {"pass_yd": "pass_yds", "pass_td": "pass_tds", "pass_int": "pass_int",
             "rush_yd": "rush_yds", "rush_td": "rush_tds", "rec": "rec", "rec_yd": "rec_yds", "rec_td": "rec_tds"}
POS_REC_BONUS = {"bonus_rec_te": "TE", "bonus_rec_wr": "WR", "bonus_rec_rb": "RB"}
MIN_BASE_POINTS = 20.0
SCALE_RANGE = (0.5, 1.5)


def league_scoring(settings_df: pl.DataFrame, league_id: str) -> dict[str, float]:
    """The computable scoring keys of a league's current settings row (missing / null = 0)."""
    row = settings_df.filter(pl.col("is_current") & (pl.col("league_id") == league_id))
    if row.height == 0:
        raise ValueError(f"no current settings for league {league_id}")
    r = row.sort("valid_from", descending=True).head(1).to_dicts()[0]
    return {k: float(r.get(k) or 0.0) for k in list(CORE_KEYS) + list(POS_REC_BONUS)}


def scoring_diff(a: dict[str, float], b: dict[str, float]) -> dict[str, tuple[float, float]]:
    return {k: (a.get(k, 0.0), b.get(k, 0.0)) for k in set(a) | set(b) if abs(a.get(k, 0.0) - b.get(k, 0.0)) > 1e-9}


def core_points(scoring: dict[str, float]) -> pl.Expr:
    """Points of a weekly row under ``scoring`` from the stat columns of fact_player_week."""
    expr = pl.lit(0.0)
    for key, col in CORE_KEYS.items():
        w = scoring.get(key, 0.0)
        if w:
            expr = expr + w * pl.col(col).cast(pl.Float64).fill_null(0.0)
    for key, pos in POS_REC_BONUS.items():
        w = scoring.get(key, 0.0)
        if w:
            expr = expr + pl.when(pl.col("position") == pos).then(w * pl.col("rec").cast(pl.Float64).fill_null(0.0)).otherwise(0.0)
    return expr


def ppg_scale(weeks: pl.DataFrame, scoring_from: dict[str, float], scoring_to: dict[str, float],
              seasons: list[int] | None = None) -> pl.DataFrame:
    """(player_id, scale): projected ppg under ``scoring_to`` = projected ppg under ``scoring_from`` x scale."""
    w = weeks if seasons is None else weeks.filter(pl.col("season").is_in(seasons))
    agg = (w.with_columns(core_points(scoring_from).alias("_from"), core_points(scoring_to).alias("_to"))
            .group_by("player_id").agg(pl.col("_from").sum(), pl.col("_to").sum()))
    return agg.select("player_id",
                      pl.when(pl.col("_from") < MIN_BASE_POINTS).then(1.0)
                        .otherwise((pl.col("_to") / pl.col("_from")).clip(*SCALE_RANGE)).alias("scale"))


def apply_scale(df: pl.DataFrame, scale: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """Multiply the given ppg columns by each player's scale (players without a scale: 1)."""
    cols = [c for c in cols if c in df.columns]
    out = df.join(scale, on="player_id", how="left").with_columns(pl.col("scale").fill_null(1.0))
    return out.with_columns([(pl.col(c) * pl.col("scale")).alias(c) for c in cols]).drop("scale")
