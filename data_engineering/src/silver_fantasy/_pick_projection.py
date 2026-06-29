"""silver_fantasy/_pick_projection.py

Measure-layer helpers for the future ``algo(team, pick_data)`` that turns the
ownership-independent ``fact_pick_values`` time-series into a team's pick value.

This is NOT a deployed Cloud Run job and NOT part of any fact — it's a library of
projection logic that the analysis/measure layer imports. It is kept separate from
storage on purpose: the way a pick maps to a tier (Early/Mid/Late) is the evolving
part, so improving it must never require rebuilding a fact.

v1 projection (`compute_draft_slots`): rookie draft order is the reverse of the
prior season's final standings (worst team drafts 1.01 = Early), so a pick's tier
comes from its *original* roster's prior-season standing. We only hold standings
for the current league's season, so only the next draft
(``draft_season = league_season + 1``) is tierable; further-out picks have no basis
yet and must be handled by the caller (e.g. round-average) — see [[fact-pick-values]].

Future: forward-looking multi-season projection estimating teams 2-3 years out,
likely trained on many other leagues' data.
"""
import polars as pl


def compute_draft_slots(team_state_df: pl.DataFrame, leagues_df: pl.DataFrame) -> pl.DataFrame:
    """Project each roster's draft slot + tier for the *next* draft.

    Draft order is the reverse of final standings: rank by (wins asc, fpts asc) so
    the worst team is slot 1 (Early). Tier is the league tercile of the slot. The
    result applies to ``draft_season = league_season + 1`` (the draft that order
    sets). Returns ``(league_id, roster_id, draft_season, draft_slot, tier)``.
    """
    league_season = leagues_df.select(
        pl.col("league_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64).alias("league_season"),
        pl.col("total_rosters").cast(pl.Int64),
    ).unique(subset=["league_id"], keep="first")

    standings = team_state_df.select(
        pl.col("league_id").cast(pl.Utf8),
        pl.col("roster_id").cast(pl.Int64),
        pl.col("wins").cast(pl.Int64),
        pl.col("fpts").cast(pl.Int64),
    ).join(league_season, on="league_id", how="inner")

    slots = (
        standings
        .sort(["league_id", "wins", "fpts"])  # worst first within each league
        .with_columns(
            pl.col("roster_id").cum_count().over("league_id").alias("draft_slot")
        )
        .with_columns(
            (((pl.col("draft_slot") - 1) * 3) // pl.col("total_rosters")).alias("_tercile")
        )
        .with_columns(
            pl.when(pl.col("_tercile") <= 0).then(pl.lit("Early"))
            .when(pl.col("_tercile") == 1).then(pl.lit("Mid"))
            .otherwise(pl.lit("Late")).alias("tier"),
            (pl.col("league_season") + 1).alias("draft_season"),
        )
    )
    return slots.select("league_id", "roster_id", "draft_season", "draft_slot", "tier")
