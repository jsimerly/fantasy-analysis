"""Market values (KTC) for the players in the production fact, as of a date.

The fact keys players by nflverse ``player_id`` (gsis id); KTC values in
``fact_asset_values_daily`` are keyed by the master ``player_key``. ``dim_players_master``
bridges the two via ``gsis_id`` (some of those carry a stray leading space -- trimmed here),
and a normalised name + position match is the fallback for players the id bridge misses.
We use the dynasty / superflex / standard-TE series, which is the one KTC has published
continuously since 2020 (needed for backtests).
"""
from __future__ import annotations

from datetime import date, timedelta

import polars as pl

import gcs_io

DIM_PLAYERS_PATH = "silver/fantasy/dim_players_master/data.parquet"
FACT_ASSET_VALUES_PATH = "silver/fantasy/fact_asset_values_daily"   # single parquet object


def norm_name(col: str) -> pl.Expr:
    """lower-case, letters/spaces only, no Jr/Sr/II/III/IV, single spaces."""
    return (
        pl.col(col).str.to_lowercase().str.replace_all(r"[^a-z ]", "")
        .str.replace_all(r"\b(jr|sr|ii|iii|iv)\b", "").str.strip_chars().str.replace_all(r"\s+", " ")
    )


def load_crosswalk() -> pl.DataFrame:
    pm = gcs_io.read_lake(DIM_PLAYERS_PATH)
    return (
        pm.with_columns(pl.col("gsis_id").str.strip_chars())
        .filter(pl.col("gsis_id").is_not_null() & (pl.col("gsis_id") != ""))
        .select("gsis_id", "player_key", "ktc_id", pl.col("display_name").alias("master_name"))
        .unique(subset=["gsis_id"], keep="first")
    )


def load_ktc_history() -> pl.DataFrame:
    fav = gcs_io.read_lake(FACT_ASSET_VALUES_PATH)
    return (
        fav.filter(
            (pl.col("market_type") == "DYNASTY") & (pl.col("qb_format") == "SF")
            & (pl.col("te_premium") == "Standard") & (pl.col("ktc_value") > 0)
        )
        .select(pl.col("player_id").alias("player_key"), pl.col("name").alias("ktc_name"),
                pl.col("position").alias("ktc_position"), "valuation_date", "ktc_value")
    )


def ktc_as_of(hist: pl.DataFrame, as_of: date, tolerance_days: int = 14) -> pl.DataFrame:
    """Latest KTC value per player on or before ``as_of`` (within ``tolerance_days``)."""
    lo = as_of - timedelta(days=tolerance_days)
    keep = [c for c in ("ktc_name", "ktc_position") if c in hist.columns]
    return (
        hist.filter((pl.col("valuation_date") <= as_of) & (pl.col("valuation_date") >= lo))
        .sort("valuation_date")
        .unique(subset=["player_key"], keep="last", maintain_order=True)
        .select(["player_key", *keep, pl.col("valuation_date").alias("ktc_date"), "ktc_value"])
    )


def attach_market(df: pl.DataFrame, as_of: date, hist: pl.DataFrame | None = None,
                  crosswalk: pl.DataFrame | None = None) -> pl.DataFrame:
    """Join ``ktc_value`` / ``ktc_date`` / ``player_key`` onto fact rows (keyed by nflverse
    ``player_id``): by id through the master first, then by normalised name + position.
    ``market_match`` records which ("id", "name", or null)."""
    hist = load_ktc_history() if hist is None else hist
    crosswalk = load_crosswalk() if crosswalk is None else crosswalk
    vals = ktc_as_of(hist, as_of)

    by_id = (
        df.join(crosswalk, left_on="player_id", right_on="gsis_id", how="left")
        .join(vals.select("player_key", "ktc_date", "ktc_value"), on="player_key", how="left")
        .with_columns(pl.when(pl.col("ktc_value").is_not_null()).then(pl.lit("id")).otherwise(None).alias("market_match"))
    )
    if "ktc_name" not in vals.columns or "player_name" not in df.columns:
        return by_id

    by_name = (
        vals.with_columns(norm_name("ktc_name").alias("_n"))
        .rename({"ktc_position": "position"})
        .unique(subset=["_n", "position"], keep="first")
        .select("_n", "position", pl.col("player_key").alias("_pk"), pl.col("ktc_date").alias("_d"),
                pl.col("ktc_value").alias("_v"))
    )
    return (
        by_id.with_columns(norm_name("player_name").alias("_n"))
        .join(by_name, on=["_n", "position"], how="left")
        .with_columns(
            pl.when(pl.col("ktc_value").is_null() & pl.col("_v").is_not_null()).then(pl.lit("name"))
            .otherwise(pl.col("market_match")).alias("market_match"),
            pl.coalesce(["player_key", "_pk"]).alias("player_key"),
            pl.coalesce(["ktc_date", "_d"]).alias("ktc_date"),
            pl.coalesce(["ktc_value", "_v"]).alias("ktc_value"),
        )
        .drop("_n", "_pk", "_d", "_v")
    )
