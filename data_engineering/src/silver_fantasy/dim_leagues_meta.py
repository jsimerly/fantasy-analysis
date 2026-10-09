from pathlib import Path
from datetime import datetime
import os

import polars as pl
from dotenv import load_dotenv

from silver_fantasy.utils import chain_lineage, get_latest_bronze_path, merge_full_and_incremental, read_latest_incremental_by_key

load_dotenv()

def transform_dim_leagues_meta() -> pl.DataFrame:
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    # --- 1. Load League Data (Base Identity) ---
    full_leagues_path = get_latest_bronze_path(bucket_name, "league/leagues/full_load")
    full_leagues_df = pl.read_parquet(full_leagues_path)
    # Latest observation per league across ALL incremental partitions -- not just the newest
    # file, which only holds the leagues that were still active when it was written (a league
    # leaves the daily feed the day after it's first seen `complete`; see utils).
    daily_leagues_df = read_latest_incremental_by_key(
        bucket_name, "league/leagues/incremental", join_key='league_id'
    )
    
    leagues_df = merge_full_and_incremental(
        full_leagues_df,
        daily_leagues_df,
        join_key='league_id',
        preserve_columns=['league_lineage_id']
    )

    # Backfill lineage for newly-discovered seasons (chained through previous_league_id; shared
    # with the settings dim -- see utils.chain_lineage).
    leagues_df = chain_lineage(leagues_df)

    # --- 2. Load Settings Data (For Status/Leg only) ---
    status_cols = ['league_id', 'leg', 'last_scored_leg']

    full_settings_path = get_latest_bronze_path(bucket_name, "league/settings/full_load")
    full_settings_df = pl.read_parquet(full_settings_path)
    daily_settings_df = read_latest_incremental_by_key(
        bucket_name, "league/settings/incremental", join_key='league_id', columns=status_cols
    )
    
    settings_df = merge_full_and_incremental(
        full_settings_df.select([c for c in status_cols if c in full_settings_df.columns]),
        daily_settings_df.select([c for c in status_cols if c in daily_settings_df.columns]),
        join_key='league_id',
        preserve_columns=[]
    )

    # --- 3. Join and Transform ---
    dim_leagues_meta = leagues_df.join(settings_df, on='league_id', how='left')

    dim_leagues_meta = dim_leagues_meta.with_columns([
        pl.col('league_name'),
        (pl.col('league_id') == pl.col('league_lineage_id')).alias('is_original'),  
        (pl.col('status') != 'complete').alias('is_active'),
        pl.lit('sleeper').alias('source_system'),
        pl.lit(datetime.now()).alias('loaded_at')
    ])

    target_cols = [
        'league_id', 'league_name', 'season', 'status', 'season_type', 
        'total_rosters', 'draft_id', 'bracket_id', 'leg', 'last_scored_leg', 
        'previous_league_id', 'league_lineage_id', 'is_original', 
        'is_active', 'source_system', 'loaded_at'
    ]

    # Final Cleaning
    existing_cols = dim_leagues_meta.columns
    for col in target_cols:
        if col not in existing_cols:
            dim_leagues_meta = dim_leagues_meta.with_columns(pl.lit(None).alias(col))

    return dim_leagues_meta.select(target_cols)

def save_df_to_gcs(df: pl.DataFrame, bucket_name: str):
    file_path = f"gs://{bucket_name}/silver/fantasy/dim_leagues_meta/data.parquet"
    
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved leagues meta table to {file_path}") 
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

if __name__ == "__main__":
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    df = transform_dim_leagues_meta()
    save_df_to_gcs(df, bucket_name)