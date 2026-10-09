"""Draft rows (BACKLOG 30): one pre-NFL row per drafted skill player, so the career model projects a
rookie's first seasons from his college production and draft capital the way it projects a
veteran's from his stats, and the hand-made rookie tail goes away for drafted players.

A draft row sits at ``season = draft_year - 1``, the as-of point before the rookie season (so h1 is
the rookie year): position, draft round and pick, age, ``is_rookie`` = 1, ``exp_at_season`` = 0 (the
rookie interactions of the ``rookie`` group apply), ``is_draft_row`` = 1 (so a model can tell it from
a played season), every production column null, and ``cfbd_id`` for the ``college`` group's join.
``player_id`` is the gsis id when the player has NFL rows (the horizon targets then attach like any
row) and ``cfbd:<id>`` otherwise: no NFL rows means his observable seasons are 0 games, the attrition
the model has to learn. The crosswalk carries no birth dates, so age is the rookie-season age from
the season fact minus one, else the draft class's median rookie age minus one.
"""
from __future__ import annotations

import polars as pl

GSIS_RE = r"^00-00\d{5}$"
SKILL = ["QB", "RB", "WR", "TE"]
FIRST_DRAFT_YEAR = 2010          # the college fact starts in 2010
FALLBACK_ROOKIE_AGE = 23.2       # median rookie-season age of recent classes


def build_draft_rows(xwalk: pl.DataFrame, season_fact: pl.DataFrame, first_draft_year: int = FIRST_DRAFT_YEAR) -> pl.DataFrame:
    """One row per drafted QB / RB / WR / TE in the crosswalk from ``first_draft_year`` on."""
    x = (xwalk.filter(pl.col("position").is_in(SKILL) & (pl.col("draft_year") >= first_draft_year) & pl.col("cfbd_id").is_not_null())
              .with_columns(pl.col("gsis_id").cast(pl.Utf8).str.strip_chars().alias("gsis_id"), pl.col("draft_year").cast(pl.Int64),
                            pl.col("round").cast(pl.Int64).alias("draft_round"), pl.col("overall").cast(pl.Int64).alias("draft_pick"))
              .unique("cfbd_id", keep="first", maintain_order=True))
    x = x.with_columns(pl.when(pl.col("gsis_id").str.contains(GSIS_RE)).then(pl.col("gsis_id")).otherwise(pl.concat_str([pl.lit("cfbd:"), pl.col("cfbd_id").cast(pl.Utf8)])).alias("player_id"))
    # age: the rookie-season age from the season fact (minus one year), else the class median
    rk = (season_fact.filter(pl.col("season") == pl.col("rookie_season"))
                     .select(pl.col("player_id").cast(pl.Utf8), pl.col("rookie_season").cast(pl.Int64).alias("draft_year"), pl.col("age_at_season").cast(pl.Float64).alias("_rookie_age")))
    by_year = rk.group_by("draft_year").agg(pl.col("_rookie_age").median().alias("_class_age"))
    x = (x.join(rk.select("player_id", "_rookie_age").unique("player_id"), on="player_id", how="left")
          .join(by_year, on="draft_year", how="left")
          .with_columns((pl.coalesce([pl.col("_rookie_age"), pl.col("_class_age"), pl.lit(FALLBACK_ROOKIE_AGE)]) - 1.0).alias("age_at_season")))
    return x.select(
        "player_id", pl.col("cfbd_id").cast(pl.Int64), (pl.col("draft_year") - 1).alias("season"), "draft_year",
        pl.col("name").cast(pl.Utf8).alias("player_name"), "position", "draft_round", "draft_pick", "age_at_season",
        pl.lit(0).alias("exp_at_season"), pl.lit(True).alias("is_rookie"), pl.lit(False).alias("is_undrafted"), pl.lit(True).alias("is_draft_row"),
        pl.lit(0, pl.Int64).alias("row_week"),
        pl.lit(True).alias("season_complete"), pl.lit(None, pl.Utf8).alias("team"),
    ).sort(["draft_year", "draft_pick"])


def with_draft_rows(matrix: pl.DataFrame, rows: pl.DataFrame) -> pl.DataFrame:
    """Append draft rows to a career matrix (before the horizon targets are attached); played-season
    rows get ``is_draft_row`` = False and the draft rows carry nulls for everything they lack."""
    m = matrix.with_columns(pl.lit(False).alias("is_draft_row")) if "is_draft_row" not in matrix.columns else matrix
    r = rows.with_columns(pl.col("season").cast(m.schema["season"]))
    return pl.concat([m, r], how="diagonal_relaxed")


def _flag(df: pl.DataFrame, col: str) -> pl.Expr:
    if col not in df.columns:
        return pl.lit(False)
    return pl.col(col).cast(pl.Float64, strict=False).fill_null(0.0) > 0.5


def is_draft(df: pl.DataFrame) -> pl.Expr:
    """Boolean expression for the draft rows; False when the column is absent or null (a feature frame
    may carry it as a float column of nulls)."""
    return _flag(df, "is_draft_row")


def is_aux(df: pl.DataFrame) -> pl.Expr:
    """The auxiliary rows: draft rows and mid-season snapshot rows (unified.py). They train the model
    and are projected, but never serve as another row's future season, form a test cohort, or feed
    replacement levels and survival."""
    return _flag(df, "is_draft_row") | _flag(df, "is_snapshot_row")


def drop_draft_rows(df: pl.DataFrame) -> pl.DataFrame:
    """The played-season rows only (replacement levels, survival, snapshots and tiers are fitted on these)."""
    if "is_draft_row" not in df.columns and "is_snapshot_row" not in df.columns:
        return df
    return df.filter(~is_aux(df))
