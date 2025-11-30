from pathlib import Path
from datetime import datetime
import os

import polars as pl
from dotenv import load_dotenv

from utils import get_latest_bronze_path, merge_full_and_incremental

load_dotenv()

def transform_dim_leagues_meta() -> pl.DataFrame:
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    # --- 1. Load League Data (Base Identity) ---
    full_leagues_path = get_latest_bronze_path(bucket_name, "league/leagues/full_load")
    daily_leagues_path = get_latest_bronze_path(bucket_name, "league/leagues/incremental")

    full_leagues_df = pl.read_parquet(full_leagues_path)
    daily_leagues_df = pl.read_parquet(daily_leagues_path)
    
    leagues_df = merge_full_and_incremental(
        full_leagues_df,
        daily_leagues_df,
        join_key='league_id',
        preserve_columns=['league_lineage_id']
    )

    # --- 2. Load Settings Data (For Status/Leg only) ---
    full_settings_path = get_latest_bronze_path(bucket_name, "league/settings/full_load")
    daily_settings_path = get_latest_bronze_path(bucket_name, "league/settings/incremental")

    full_settings_df = pl.read_parquet(full_settings_path)
    daily_settings_df = pl.read_parquet(daily_settings_path)

    status_cols = ['league_id', 'leg', 'last_scored_leg']
    
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