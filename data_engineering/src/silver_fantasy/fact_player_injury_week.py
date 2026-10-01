"""silver: fact_player_injury_week -- one row per (season, week, gsis_id).

The official weekly injury report (``bronze/nflverse/injuries``, 2009+) normalized, plus the
players on an injury reserve list that week (``bronze/nflverse/rosters_weekly`` status RES / PUP /
NON), who are *not* on the report. Without the reserve rows a season-long IR stint looks exactly
like a healthy scratch or a benching.

Normalization:
  * keys cast to Int32 (older nflverse files carry ``week``/``season`` as Float64);
  * several report rows for one player-week (status updated during the week) collapse to the
    latest ``date_modified``, then the more severe game status;
  * ``primary_injury`` = report body part, falling back to the practice-report body part;
  * ``injury_class`` buckets the body part (soft_tissue / knee_achilles / ankle_foot /
    concussion / upper_body / not_injury / other; ``unknown`` for reserve-list rows, whose
    cause is not published);
  * ``practice_participation`` = full / limited / dnp / out;
  * ``game_status_rank`` 0..4 = none / Probable / Questionable / Doubtful / Out;
  * ``is_out`` = ruled Out or on an injury reserve list.
Team codes are canonicalized like ``fact_player_week`` (e.g. OAK -> LV, JAX -> JAC).

Output: ``gs://<bucket>/silver/fantasy/fact_player_injury_week/data.parquet``
"""
from __future__ import annotations

import io
import os

import polars as pl
from dotenv import load_dotenv
from google.cloud import storage

from silver_fantasy.fact_player_week import _canon_team
from silver_fantasy.utils import read_bronze_prefix

load_dotenv()

INJURIES_PREFIX = "bronze/nflverse/injuries/"
ROSTERS_WEEKLY_PREFIX = "bronze/nflverse/rosters_weekly/"
OUTPUT_PATH = "silver/fantasy/fact_player_injury_week/data.parquet"
KEY = ["season", "week", "gsis_id"]

RESERVE_STATUSES = ["RES", "PUP", "NON"]          # injured reserve / physically unable / non-football injury
GAME_STATUS_RANK = {"Out": 4, "Doubtful": 3, "Questionable": 2, "Probable": 1}

# body-part bucket, first match wins (checked on the lower-cased text)
INJURY_CLASSES: list[tuple[str, list[str]]] = [
    ("concussion", ["concussion", "head"]),
    ("not_injury", ["not injury", "illness", "personal", "rest", "covid", "coach", "load management"]),
    ("soft_tissue", ["hamstring", "groin", "calf", "quad", "thigh", "hip", "oblique", "abdomen", "core", "adductor"]),
    ("knee_achilles", ["knee", "acl", "mcl", "pcl", "achilles", "patella"]),
    ("ankle_foot", ["ankle", "foot", "toe", "heel"]),
    ("upper_body", ["shoulder", "elbow", "hand", "wrist", "thumb", "finger", "rib", "chest", "pectoral",
                    "back", "neck", "collarbone", "clavicle", "bicep", "tricep", "forearm", "arm"]),
]


def classify_injury(col: str) -> pl.Expr:
    """``injury_class`` for a body-part column: 'none' when null/blank, 'other' when unmatched."""
    text = pl.col(col).str.to_lowercase().str.strip_chars()
    expr = pl.when(text.is_null() | (text == "")).then(pl.lit("none"))
    for label, needles in INJURY_CLASSES:
        cond = pl.any_horizontal([text.str.contains(n, literal=True) for n in needles])
        expr = expr.when(cond).then(pl.lit(label))
    return expr.otherwise(pl.lit("other"))


def practice_participation(col: str) -> pl.Expr:
    text = pl.col(col).str.to_lowercase()
    return (pl.when(text.str.contains("did not", literal=True)).then(pl.lit("dnp"))
              .when(text.str.contains("limited", literal=True)).then(pl.lit("limited"))
              .when(text.str.contains("full", literal=True)).then(pl.lit("full"))
              .when(text.str.contains("out", literal=True)).then(pl.lit("out"))
              .otherwise(None))


def _blank_to_null(col: str) -> pl.Expr:
    c = pl.col(col).cast(pl.Utf8)
    return pl.when(c.str.strip_chars() == "").then(None).otherwise(c)


def normalize_report(inj: pl.DataFrame) -> pl.DataFrame:
    """One normalized row per (season, week, gsis_id) from the raw report rows."""
    cols = inj.columns
    opt = lambda c: _blank_to_null(c) if c in cols else pl.lit(None, pl.Utf8)  # noqa: E731
    modified = pl.lit(None, pl.Datetime("us"))
    if "date_modified" in cols:
        modified = pl.col("date_modified")
        if getattr(inj.schema["date_modified"], "time_zone", None):           # tz-aware in the newer files
            modified = modified.dt.replace_time_zone(None)
        modified = modified.cast(pl.Datetime("us"))
    df = inj.filter(pl.col("gsis_id").is_not_null()).select(
        pl.col("season").cast(pl.Int32),
        pl.col("week").cast(pl.Int32),
        (pl.col("game_type") if "game_type" in cols else pl.lit(None, pl.Utf8)).alias("game_type"),
        _canon_team("team").alias("team"),
        pl.col("gsis_id"),
        (pl.col("full_name") if "full_name" in cols else pl.lit(None, pl.Utf8)).alias("player_name"),
        (pl.col("position") if "position" in cols else pl.lit(None, pl.Utf8)).alias("position"),
        opt("report_status").alias("report_status"),
        opt("practice_status").alias("practice_status"),
        pl.coalesce(opt("report_primary_injury"), opt("practice_primary_injury")).alias("primary_injury"),
        pl.coalesce(opt("report_secondary_injury"), opt("practice_secondary_injury")).alias("secondary_injury"),
        modified.alias("date_modified"),
    ).with_columns(
        pl.col("report_status").replace_strict(GAME_STATUS_RANK, default=0, return_dtype=pl.Int8).fill_null(0).alias("game_status_rank"),
        practice_participation("practice_status").alias("practice_participation"),
        classify_injury("primary_injury").alias("injury_class"),
    )
    # latest update wins, then the more severe status
    return (df.sort(["date_modified", "game_status_rank"], descending=[True, True], nulls_last=True)
              .unique(subset=KEY, keep="first", maintain_order=True))


def roster_status(rosters_weekly: pl.DataFrame) -> pl.DataFrame:
    """(season, week, gsis_id) -> roster status (+ team / position / name for reserve-only rows)."""
    cols = rosters_weekly.columns
    name = pl.col("full_name") if "full_name" in cols else pl.lit(None, pl.Utf8)
    return (rosters_weekly.filter(pl.col("gsis_id").is_not_null() & pl.col("week").is_not_null())
            .select(pl.col("season").cast(pl.Int32), pl.col("week").cast(pl.Int32), pl.col("gsis_id"),
                    pl.col("status").alias("roster_status"), _canon_team("team").alias("r_team"),
                    pl.col("position").alias("r_position"), name.alias("r_name"),
                    (pl.col("game_type") if "game_type" in cols else pl.lit(None, pl.Utf8)).alias("r_game_type"))
            .sort("roster_status")                                  # deterministic when a week has two rows
            .unique(subset=KEY, keep="first", maintain_order=True))


def build_fact_player_injury_week(inj: pl.DataFrame, rosters_weekly: pl.DataFrame) -> pl.DataFrame:
    report = normalize_report(inj)
    roster = roster_status(rosters_weekly)
    on_report = report.join(roster, on=KEY, how="left").with_columns(pl.lit(True).alias("on_injury_report"))
    reserve_only = (roster.filter(pl.col("roster_status").is_in(RESERVE_STATUSES))
                          .join(report.select(KEY), on=KEY, how="anti")
                          .with_columns(pl.lit(False).alias("on_injury_report"),
                                        pl.lit("unknown").alias("injury_class"),
                                        pl.lit(0, pl.Int8).alias("game_status_rank")))
    out = pl.concat([on_report, reserve_only], how="diagonal_relaxed")
    return (out.with_columns(
                pl.coalesce("team", "r_team").alias("team"),
                pl.coalesce("position", "r_position").alias("position"),
                pl.coalesce("player_name", "r_name").alias("player_name"),
                pl.coalesce("game_type", "r_game_type").alias("game_type"),
                pl.col("roster_status").is_in(RESERVE_STATUSES).fill_null(False).alias("on_injured_reserve"),
            )
            .with_columns(((pl.col("report_status") == "Out") | pl.col("on_injured_reserve")).fill_null(False).alias("is_out"))
            .select("season", "week", "game_type", "team", "gsis_id", "player_name", "position",
                    "on_injury_report", "report_status", "game_status_rank", "practice_status", "practice_participation",
                    "primary_injury", "secondary_injury", "injury_class", "roster_status", "on_injured_reserve",
                    "is_out", "date_modified")
            .sort(["season", "week", "team", "gsis_id"]))


def _bucket() -> str:
    b = os.environ.get("GCS_BUCKET_NAME")
    if not b:
        raise RuntimeError("GCS_BUCKET_NAME not set")
    return b


def save_df_to_gcs(df: pl.DataFrame, bucket_name: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf)
    storage.Client().bucket(bucket_name).blob(OUTPUT_PATH).upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    print(f"✅ Saved fact_player_injury_week ({df.shape[0]} rows) to gs://{bucket_name}/{OUTPUT_PATH}")


def main() -> None:
    bucket = _bucket()
    inj = read_bronze_prefix(bucket, INJURIES_PREFIX)
    rw = read_bronze_prefix(bucket, ROSTERS_WEEKLY_PREFIX, dedupe_on=["season", "week", "gsis_id", "team"])
    fact = build_fact_player_injury_week(inj, rw)
    print(f"seasons {fact['season'].min()}-{fact['season'].max()}; on report {fact['on_injury_report'].sum():,}; "
          f"reserve-only {(~fact['on_injury_report']).sum():,}; classes: {fact['injury_class'].value_counts().sort('count', descending=True).to_dicts()}")
    save_df_to_gcs(fact, bucket)


if __name__ == "__main__":
    main()
