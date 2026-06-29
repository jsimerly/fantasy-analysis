"""Model-exclusive feature engineering for the dynasty value model.

Reads the reusable ``fact_player_season`` dataset (built by data_engineering) and adds the
pieces that are specific to *this* model: prior-year lags, the next-season (T+1) supervised
target, and simple deltas. Rows without a T+1 record are dropped — a supervised training
matrix (note the survivorship caveat: players who leave the NFL get no target row).

The base production/bio columns and the league re-scoring all live upstream in
``data_engineering/silver_fantasy/fact_player_season.py``; this module owns only the
modeling-specific transforms.
"""
from __future__ import annotations

import polars as pl

import gcs_io

FACT_PATH = "silver/fantasy/fact_player_season/data.parquet"

# season-level columns carried backward as prior-year features
LAG1_COLS = ["fpts", "ppg", "games", "pass_yds", "rush_yds", "rec_yds", "targets", "rec", "total_touches"]
LAG2_COLS = ["fpts", "ppg", "games"]


def load_fact_player_season() -> pl.DataFrame:
    """Read the player-season production fact from the lake."""
    return gcs_io.read_lake(FACT_PATH)


def _shift(df: pl.DataFrame, season_delta: int, cols: list[str], prefix: str) -> pl.DataFrame:
    """Re-key season rows by ``season + season_delta`` so a join lands them as lag/target."""
    return df.select(
        ["player_id", (pl.col("season") + season_delta).alias("season")]
        + [pl.col(c).alias(prefix + c) for c in cols]
    )


def attach_lags_and_target(df: pl.DataFrame, drop_no_target: bool = True) -> pl.DataFrame:
    """Attach T-1/T-2 lag features and the T+1 target. Pure (no IO) for testability.

    Leakage-safe by construction: lag columns come only from seasons < T (joined via a
    +1/+2 season shift), and ``target_*`` columns come only from season T+1 (a −1 shift).
    """
    lag1 = _shift(df, 1, LAG1_COLS, "lag1_")
    lag2 = _shift(df, 2, LAG2_COLS, "lag2_")
    target = df.select(
        ["player_id", (pl.col("season") - 1).alias("season"),
         pl.col("fpts").alias("target_fpts_next"),
         pl.col("ppg").alias("target_ppg_next"),
         pl.col("games").alias("target_games_next")]
    )
    out = (
        df.join(lag1, on=["player_id", "season"], how="left")
        .join(lag2, on=["player_id", "season"], how="left")
        .join(target, on=["player_id", "season"], how="left")
        .with_columns(
            (pl.col("fpts") - pl.col("lag1_fpts")).alias("d_fpts_1"),
            (pl.col("ppg") - pl.col("lag1_ppg")).alias("d_ppg_1"),
        )
    )
    if drop_no_target:
        out = out.filter(pl.col("target_fpts_next").is_not_null())
    return out.sort(["season", "fpts"], descending=[False, True])


def build_training_matrix() -> pl.DataFrame:
    """Load the player-season fact and attach lags + the T+1 target. Built in-memory
    (not persisted): this is model-exclusive feature dev, regenerated each training run."""
    return attach_lags_and_target(load_fact_player_season())
