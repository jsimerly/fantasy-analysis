"""Replacement level: what a freely available starter produces in THIS league's format.

Value is only meaningful relative to what you could start instead. For each position we
work out how many players the league starts (dedicated slots + a share of the flex slots +
the superflex, which QBs fill in a 2QB-ish league), and define replacement level as the
points-per-game of the next player past that line, averaged over recent seasons.

Everything here is league-format-driven (slots from ``dim_league_settings``) so the same code
prices value for a 12-team 1QB league differently from the owner's 10-team superflex league.
"""
from __future__ import annotations

import numpy as np
import polars as pl

POSITIONS = ["QB", "RB", "WR", "TE"]
PRIMARY_LINEAGE = "730630605066371072"

# How flex-type slots get filled, on average. The superflex is a QB slot in practice.
FLEX_SHARE = {"RB": 0.45, "WR": 0.45, "TE": 0.10}
SUPERFLEX_SHARE = {"QB": 1.0}

SLOT_COLS = ["qb_slots", "rb_slots", "wr_slots", "te_slots", "flex_slots", "superflex_slots"]


def league_lineup(settings_df: pl.DataFrame, lineage_id: str = PRIMARY_LINEAGE) -> tuple[dict[str, int], int]:
    """(starting slots, number of teams) from the lineage's latest current settings row."""
    cur = settings_df.filter(pl.col("is_current") & (pl.col("league_lineage_id") == lineage_id))
    if cur.is_empty():
        raise ValueError(f"no current settings for lineage {lineage_id}")
    # the newest LEAGUE of the lineage first (Sleeper ids grow season over season), then the newest
    # settings version. Not the other way round: the legacy SCD2 trigger stamps a current row keyed by
    # the lineage's ROOT league id (an older season's rules: 2 WR, a -1 fumble) with a later valid_from,
    # and "newest version first" picked that row whenever it ran last (2026-10-07).
    row = (
        cur.with_columns(pl.col("league_id").cast(pl.Utf8).cast(pl.Int64, strict=False).alias("_lid"))
        .sort(["_lid", "valid_from"], descending=[True, True]).head(1).to_dicts()[0]
    )
    slots = {c: int(row.get(c) or 0) for c in SLOT_COLS}
    return slots, int(row["num_teams"])


def starters_per_position(slots: dict[str, int], teams: int) -> dict[str, float]:
    """League-wide number of starters at each position (fractional via flex shares)."""
    per_team = {
        "QB": float(slots.get("qb_slots", 0)), "RB": float(slots.get("rb_slots", 0)),
        "WR": float(slots.get("wr_slots", 0)), "TE": float(slots.get("te_slots", 0)),
    }
    for pos, share in FLEX_SHARE.items():
        per_team[pos] += slots.get("flex_slots", 0) * share
    for pos, share in SUPERFLEX_SHARE.items():
        per_team[pos] += slots.get("superflex_slots", 0) * share
    return {pos: n * teams for pos, n in per_team.items()}


def replacement_levels(
    season_df: pl.DataFrame, starters: dict[str, float], seasons: list[int] | None = None,
    n_seasons: int = 5, min_games: int = 8,
) -> dict[str, float]:
    """Replacement ppg per position = ppg of the player just outside the league's starters
    (rank floor(starters)+1 by ppg among players with >= ``min_games``), averaged over the
    last ``n_seasons`` complete seasons (or the given ``seasons``)."""
    pool_all = season_df.filter(pl.col("season_complete")) if "season_complete" in season_df.columns else season_df
    if seasons is None:
        seasons = sorted(pool_all["season"].unique().to_list())[-n_seasons:]
    out: dict[str, float] = {}
    for pos, n in starters.items():
        rank = int(np.floor(n)) + 1
        vals = []
        for s in seasons:
            pool = (
                pool_all.filter((pl.col("season") == s) & (pl.col("position") == pos) & (pl.col("games") >= min_games))
                .sort("ppg", descending=True)
            )
            if pool.height >= rank:
                vals.append(float(pool["ppg"][rank - 1]))
        out[pos] = float(np.mean(vals)) if vals else 0.0
    return out
