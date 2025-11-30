from pathlib import Path
from datetime import datetime
import os

import polars as pl
from dotenv import load_dotenv

from utils import get_latest_bronze_path, merge_full_and_incremental

load_dotenv()

def transform_dim_franchises_meta() -> pl.DataFrame:
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    # --- 1. Load Data ---    
    leagues_path = f"gs://{bucket_name}/silver/fantasy/dim_leagues_meta/data.parquet"
    users_path = f"gs://{bucket_name}/silver/fantasy/dim_users/data.parquet"
    rosters_path = get_latest_bronze_path(bucket_name, "rosters/team_state/daily")

    leagues_df = pl.read_parquet(leagues_path)
    users_df = pl.read_parquet(users_path)
    
    try:
        rosters_df = pl.read_parquet(rosters_path)
    except:
        rosters_df = pl.read_parquet(f"{rosters_path}/*.parquet")

    # --- 2. Construct the Franchise Entity ---    
    franchise_base = rosters_df.join(
        leagues_df.select(['league_id', 'league_lineage_id']),
        on='league_id',
        how='left'
    )

    franchise_base = franchise_base.with_columns([
        pl.concat_str([
            pl.col('league_lineage_id'),
            pl.lit('_'),
            pl.col('roster_id').cast(pl.Utf8)
        ]).alias('franchise_id')
    ])
    
    print(f"PRE dim_franchises count: {len(franchise_base)}")

    # --- 3. Enrich with Owner Details ---
    user_cols = ['user_id', 'primary_name', 'avatar']
    
    dim_franchises = franchise_base.join(
        users_df.select(user_cols),
        left_on='owner_id',
        right_on='user_id',
        how='left'
    )
    
    # --- 4. Transformations ---
    dim_franchises = dim_franchises.with_columns([
        pl.coalesce([
            pl.col('primary_name'),
            pl.format("Orphan Roster {}", pl.col('roster_id'))
        ]).alias('current_team_name'),
        
        pl.col('owner_id').is_null().alias('is_orphan'),
        pl.lit('sleeper').alias('source_system'),
        pl.lit(datetime.now()).alias('loaded_at')
    ])

    # --- 5. Final Select ---
    target_cols = [
        'franchise_id',
        'league_id',
        'league_lineage_id',
        'roster_id',
        'owner_id',
        'current_team_name',
        'avatar',
        'is_orphan',
        'source_system',
        'loaded_at'
    ]

    return dim_franchises.select(
        [c for c in target_cols if c in dim_franchises.columns]
    )

def save_df_to_gcs(df: pl.DataFrame, bucket_name: str):
    file_path = f"gs://{bucket_name}/silver/fantasy/dim_franchises_meta/data.parquet"
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved franchises meta to {file_path}") 
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

if __name__ == "__main__":
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    df = transform_dim_franchises_meta()
    save_df_to_gcs(df, bucket_name)