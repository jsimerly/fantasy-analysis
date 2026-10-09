"""One model for the in-season and the career horizons (BACKLOG 32), step one: mid-season snapshots
as rows of the career matrix.

A career row is a player's state at the end of season T with the seasons T+1, T+2.. as targets. A
snapshot row is the same player's state after week W of season T, with the same targets: the
to-date rate, games and usage in the season's columns, last season in the lag columns, the career
to date including the partial season, and ``row_week`` = W (a complete season reads 18, a draft row
0) so the model learns how far a partial season should move the projection. ``is_snapshot_row``
marks them. Snapshot rows never serve as another row's future season, never form a test cohort,
and never feed replacement levels or survival (``draft_rows.drop_draft_rows`` drops them too).

Step two (the in-season use: next season from a snapshot through this model instead of the
in-season model) builds on the same rows.
"""
from __future__ import annotations

from typing import Iterable

import polars as pl

import inseason

SEASON_ROW_WEEK = 18          # row_week of a complete season
DEFAULT_SNAPSHOT_WEEKS = (9,)
DEFAULT_SNAPSHOT_FROM = 2010  # the in-context cap (50k rows on TabPFN 3.5) decides how many snapshots fit


def snapshot_rows(wk: pl.DataFrame, season_df: pl.DataFrame, weeks: Iterable[int] = DEFAULT_SNAPSHOT_WEEKS,
                  from_season: int = DEFAULT_SNAPSHOT_FROM) -> pl.DataFrame:
    """Career-schema rows from ``inseason.build_snapshots`` at the given checkpoint weeks (players
    with at least one game by then), seasons ``from_season`` on. ``season_df`` is the played-season
    frame (lags and career columns on it are used as last season's values)."""
    weeks = [int(w) for w in weeks]
    snaps = inseason.build_snapshots(wk.filter(pl.col("season") >= from_season), season_df, weeks=weeks)
    done = set(season_df.filter(pl.col("season_complete"))["season"].unique().to_list()) if "season_complete" in season_df.columns else None

    def col(c: str) -> pl.Expr:
        return pl.col(c).cast(pl.Float64, strict=False) if c in snaps.columns else pl.lit(None, pl.Float64)

    out = snaps.select(
        pl.col("player_id"), pl.col("season").cast(pl.Int64), pl.col("player_name"), pl.col("position"), pl.col("team"),
        col("age_at_season").alias("age_at_season"), col("exp_at_season").alias("exp_at_season"),
        col("draft_round").alias("draft_round"), col("draft_pick").alias("draft_pick"),
        pl.col("is_undrafted"), pl.col("is_rookie"),
        pl.col("week").cast(pl.Int64).alias("row_week"), pl.lit(True).alias("is_snapshot_row"),
        # this season to date, in the season's columns
        col("td_fpts").alias("fpts"), col("td_ppg").alias("ppg"), col("td_games").alias("games"),
        (col("td_targets_pg") * col("td_games")).alias("targets"), (col("td_touches_pg") * col("td_games")).alias("total_touches"),
        col("td_target_share").alias("target_share_avg"), col("td_wopr").alias("wopr_avg"),
        # last season and the one before, in the lag columns
        col("prev_fpts").alias("lag1_fpts"), col("prev_ppg").alias("lag1_ppg"), col("prev_games").alias("lag1_games"),
        col("prev_targets").alias("lag1_targets"), col("prev_total_touches").alias("lag1_total_touches"), col("prev_pass_yds").alias("lag1_pass_yds"),
        col("prev_lag1_fpts").alias("lag2_fpts"), col("prev_lag1_ppg").alias("lag2_ppg"), col("prev_lag1_games").alias("lag2_games"),
        # the career to date, the partial season included
        (col("prev_career_seasons").fill_null(0.0) + 1.0).alias("career_seasons"),
        (col("prev_career_fpts").fill_null(0.0) + col("td_fpts").fill_null(0.0)).alias("career_fpts"),
        (col("prev_career_games").fill_null(0.0) + col("td_games").fill_null(0.0)).alias("career_games"),
        pl.max_horizontal(col("prev_best_ppg"), col("td_ppg")).alias("best_ppg"),
    )
    if done is not None:
        out = out.with_columns(pl.col("season").is_in(sorted(done)).alias("season_complete"))
    else:
        out = out.with_columns(pl.lit(True).alias("season_complete"))
    return out.sort(["season", "player_id", "row_week"])


def mask_group_columns(df: pl.DataFrame, cols: Iterable[str]) -> pl.DataFrame:
    """Null the season-level feature-group columns on snapshot rows: those groups (injury weeks, the
    second-half trend, the team move) describe the whole season, which a mid-season row has not seen."""
    cols = [c for c in cols if c in df.columns]
    if not cols or "is_snapshot_row" not in df.columns:
        return df
    snap = pl.col("is_snapshot_row").cast(pl.Float64, strict=False).fill_null(0.0) > 0.5
    return df.with_columns([pl.when(snap).then(None).otherwise(pl.col(c)).alias(c) for c in cols])


def with_snapshot_rows(matrix: pl.DataFrame, rows: pl.DataFrame) -> pl.DataFrame:
    """Append snapshot rows to a career matrix before the horizon targets are attached; played-season
    rows get ``row_week`` = 18 and ``is_snapshot_row`` = False."""
    m = matrix
    if "row_week" not in m.columns:
        m = m.with_columns(pl.lit(SEASON_ROW_WEEK, pl.Int64).alias("row_week"))
    else:
        m = m.with_columns(pl.col("row_week").fill_null(SEASON_ROW_WEEK).cast(pl.Int64))
    if "is_snapshot_row" not in m.columns:
        m = m.with_columns(pl.lit(False).alias("is_snapshot_row"))
    r = rows.with_columns(pl.col("season").cast(m.schema["season"]))
    return pl.concat([m, r], how="diagonal_relaxed")
