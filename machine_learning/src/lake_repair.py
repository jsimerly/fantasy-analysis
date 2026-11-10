"""Repairs applied to every lake table the model reads, before any feature is built.

The model never writes the lake, and a lake table can be wrong for a day: on 2026-11-10 nflverse's players
table moved the whole 2012 rookie class to ``rookie_season`` 2013 (467 players), silver derived
``exp_at_season = season - rookie_season = -1`` on their 2012 rows, the rookie group divided the draft pick
by ``1 + exp = 0`` and the week 9 refresh died on an infinite feature 50 seconds in. A player cannot have
played before his rookie season, so the frame itself says what the rookie season can be at most."""
from __future__ import annotations

import polars as pl


def repair_rookie_season(df: pl.DataFrame) -> pl.DataFrame:
    """``rookie_season`` never later than the player's first season in the frame; ``exp_at_season`` and
    ``is_rookie`` re-derived from it the way silver derives them. A null rookie season stays null; a frame
    without the columns comes back untouched."""
    if not {"player_id", "season", "rookie_season"} <= set(df.columns):
        return df
    first = pl.col("season").min().over("player_id")
    rk = pl.col("rookie_season")
    out = df.with_columns(pl.when(rk.is_not_null() & (rk > first)).then(first).otherwise(rk).cast(df.schema["rookie_season"]).alias("rookie_season"))
    derived = []
    if "exp_at_season" in df.columns:
        derived.append((pl.col("season") - pl.col("rookie_season")).cast(df.schema["exp_at_season"]).alias("exp_at_season"))
    if "is_rookie" in df.columns:
        derived.append((pl.col("season") == pl.col("rookie_season")).alias("is_rookie"))
    return out.with_columns(derived) if derived else out


def repair(df: pl.DataFrame) -> pl.DataFrame:
    """Every repair, in order; ``gcs_io.read_lake`` runs it on each table it returns."""
    return repair_rookie_season(df)
