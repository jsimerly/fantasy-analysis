"""silver_fantasy/fact_player_season.py

Season rollup of ``fact_player_week`` — one row per (player, season) with the league-scored
fantasy total, volume sums, efficiency, and bio. This is the lake's materialized "view" of
the atomic weekly fact (no semantic layer / BigQuery here; the rollup is just a parquet job).

The dynasty model reads this season-grain table; replacement-level / ppg work reads the
weekly ``fact_player_week`` directly.

Depends on ``fact_player_week`` (run it first). Output:
``gs://<bucket>/silver/fantasy/fact_player_season/data.parquet``
"""
from __future__ import annotations

import io
import os
from datetime import datetime

import polars as pl
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

WEEK_PATH = "silver/fantasy/fact_player_week/data.parquet"


def rollup_to_season(wk: pl.DataFrame) -> pl.DataFrame:
    """Aggregate the player-week fact to one row per (player_id, season).

    Output schema matches the historical ``fact_player_season`` (the machine_learning side
    reads it unchanged): season fantasy total, volume sums, ppg/efficiency, and bio.
    ``games`` = weeks with a recorded stat line (bye/inactive weeks have no row).
    """
    season = wk.group_by(["player_id", "season"]).agg(
        pl.col("player_name").first().alias("player_name"),
        pl.col("position").first().alias("position"),
        pl.col("team").sort_by("week").last().alias("team"),
        pl.len().alias("games"),
        pl.col("fpts").sum().alias("fpts"),
        pl.col("fpts_ppr_nflverse").sum().alias("fpts_ppr_nflverse"),
        pl.col("pass_att").sum().alias("pass_att"),
        pl.col("pass_cmp").sum().alias("pass_cmp"),
        pl.col("pass_yds").sum().alias("pass_yds"),
        pl.col("pass_tds").sum().alias("pass_tds"),
        pl.col("pass_int").sum().alias("pass_int"),
        pl.col("rush_att").sum().alias("rush_att"),
        pl.col("rush_yds").sum().alias("rush_yds"),
        pl.col("rush_tds").sum().alias("rush_tds"),
        pl.col("targets").sum().alias("targets"),
        pl.col("rec").sum().alias("rec"),
        pl.col("rec_yds").sum().alias("rec_yds"),
        pl.col("rec_tds").sum().alias("rec_tds"),
        pl.col("target_share").mean().alias("target_share_avg"),
        pl.col("wopr").mean().alias("wopr_avg"),
        # bio is constant within a season
        pl.col("birth_date").first().alias("birth_date"),
        pl.col("draft_round").first().alias("draft_round"),
        pl.col("draft_pick").first().alias("draft_pick"),
        pl.col("rookie_season").first().alias("rookie_season"),
        pl.col("college_name").first().alias("college_name"),
        pl.col("age_at_season").first().alias("age_at_season"),
        pl.col("exp_at_season").first().alias("exp_at_season"),
        pl.col("is_rookie").first().alias("is_rookie"),
        pl.col("is_undrafted").first().alias("is_undrafted"),
        pl.col("scored_under_league_id").first().alias("scored_under_league_id"),
    ).with_columns(
        (pl.col("fpts") / pl.col("games")).alias("ppg"),
        (pl.col("rush_att") + pl.col("rec")).alias("total_touches"),
        (pl.col("rush_yds") + pl.col("rec_yds")).alias("scrim_yds"),
        (pl.col("pass_tds") + pl.col("rush_tds") + pl.col("rec_tds")).alias("total_tds"),
    ).with_columns(
        pl.when(pl.col("total_touches") > 0)
        .then(pl.col("scrim_yds") / pl.col("total_touches"))
        .otherwise(None)
        .alias("yds_per_touch"),
        pl.lit(datetime.now()).alias("loaded_at"),
    )
    return season.sort(["season", "fpts"], descending=[False, True])


# ------------------------------------------------------------------------------- IO
def _bucket() -> str:
    return os.environ.get("GCS_BUCKET_NAME") or "nfl-data-bronze"


def _read_gcs(name: str) -> pl.DataFrame:
    return pl.read_parquet(io.BytesIO(storage.Client().bucket(_bucket()).blob(name).download_as_bytes()))


def save_df_to_gcs(df: pl.DataFrame, bucket_name: str) -> None:
    """Write the rollup to ``silver/fantasy/fact_player_season/data.parquet``."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    blob = storage.Client().bucket(bucket_name).blob("silver/fantasy/fact_player_season/data.parquet")
    blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    print(f"✅ Saved fact_player_season ({df.shape[0]} rows) to gs://{bucket_name}/silver/fantasy/fact_player_season/data.parquet")


def main() -> None:
    bucket = _bucket()
    season = rollup_to_season(_read_gcs(WEEK_PATH))
    save_df_to_gcs(season, bucket)


if __name__ == "__main__":
    main()
