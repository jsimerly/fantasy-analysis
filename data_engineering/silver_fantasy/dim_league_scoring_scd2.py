from pathlib import Path
from datetime import datetime
import os

import polars as pl
from dotenv import load_dotenv

from utils import get_latest_bronze_path, merge_full_and_incremental

load_dotenv()

def transform_dim_league_scoring_scd2() -> pl.DataFrame:
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    
    # Get paths
    full_scoring_path = get_latest_bronze_path(bucket_name, "league/scoring/full_load")
    daily_scoring_path = get_latest_bronze_path(bucket_name, "league/scoring/incremental")
    existing_silver_path = f"gs://{bucket_name}/silver/leagues/dim_league_scoring.parquet"
    
    # Read bronze data
    full_scoring_df = pl.read_parquet(full_scoring_path)
    daily_scoring_df = pl.read_parquet(daily_scoring_path)
    
    # Merge full and incremental
    new_scoring_df = merge_full_and_incremental(
        full_scoring_df,
        daily_scoring_df,
        join_key='league_id',
        preserve_columns=['league_lineage_id']
    )
    
    # Try to read existing silver data
    try:
        existing_df = pl.read_parquet(existing_silver_path)
    except:
        # First run - no existing data
        existing_df = None
    
    current_timestamp = datetime.now()
    
    if existing_df is None:
        # First load - all records are current
        result_df = new_scoring_df.with_columns([
            pl.lit(current_timestamp).alias('valid_from'),
            pl.lit(None).alias('valid_to'),
            pl.lit(True).alias('is_current'),
            pl.lit('sleeper').alias('source_system'),
            pl.lit(current_timestamp).alias('loaded_at')
        ])
    else:
        # Get currently active records
        current_records = existing_df.filter(pl.col('is_current') == True)
        
        # Identify scoring columns (exclude metadata columns)
        metadata_cols = ['league_id', 'league_lineage_id', 'valid_from', 'valid_to', 
                        'is_current', 'source_system', 'loaded_at']
        scoring_cols = [c for c in new_scoring_df.columns if c not in metadata_cols]
        
        # Find changes by comparing new data to current records
        comparison = new_scoring_df.join(
            current_records.select(['league_id'] + scoring_cols),
            on='league_id',
            how='left',
            suffix='_old'
        )
        
        # Create a flag for whether any scoring column changed
        change_conditions = [
            (pl.col(col) != pl.col(f"{col}_old")) | 
            (pl.col(col).is_null() != pl.col(f"{col}_old").is_null())
            for col in scoring_cols 
            if f"{col}_old" in comparison.columns
        ]
        
        if change_conditions:
            comparison = comparison.with_columns([
                pl.any_horizontal(change_conditions).alias('has_changed')
            ])
        else:
            comparison = comparison.with_columns([
                pl.lit(False).alias('has_changed')
            ])
        
        # Separate changed, unchanged, and new leagues
        changed_leagues = comparison.filter(pl.col('has_changed') == True).select(new_scoring_df.columns)
        unchanged_leagues = comparison.filter(pl.col('has_changed') == False).select(new_scoring_df.columns)
        new_leagues = comparison.filter(pl.col('has_changed').is_null()).select(new_scoring_df.columns)
        
        changed_league_ids = changed_leagues['league_id'].to_list() if len(changed_leagues) > 0 else []
        unchanged_league_ids = unchanged_leagues['league_id'].to_list() if len(unchanged_leagues) > 0 else []
        
        # For changed leagues: expire old records and create new ones
        if changed_league_ids:
            expired_records = current_records.filter(
                pl.col('league_id').is_in(changed_league_ids)
            ).with_columns([
                pl.lit(current_timestamp).alias('valid_to'),
                pl.lit(False).alias('is_current')
            ])
            
            new_changed_records = changed_leagues.with_columns([
                pl.lit(current_timestamp).alias('valid_from'),
                pl.lit(None).alias('valid_to'),
                pl.lit(True).alias('is_current'),
                pl.lit('sleeper').alias('source_system'),
                pl.lit(current_timestamp).alias('loaded_at')
            ])
        else:
            expired_records = pl.DataFrame()
            new_changed_records = pl.DataFrame()
        
        # For unchanged leagues: keep existing record but update loaded_at
        if unchanged_league_ids:
            unchanged_current_records = current_records.filter(
                pl.col('league_id').is_in(unchanged_league_ids)
            ).with_columns([
                pl.lit(current_timestamp).alias('loaded_at')  # Update to show it was verified
            ])
        else:
            unchanged_current_records = pl.DataFrame()
        
        # Keep all historical (non-current) records as-is
        historical_records = existing_df.filter(pl.col('is_current') == False)
        
        # Handle new leagues (in new data but not in existing)
        if len(new_leagues) > 0:
            new_leagues_records = new_leagues.with_columns([
                pl.lit(current_timestamp).alias('valid_from'),
                pl.lit(None).alias('valid_to'),
                pl.lit(True).alias('is_current'),
                pl.lit('sleeper').alias('source_system'),
                pl.lit(current_timestamp).alias('loaded_at')
            ])
        else:
            new_leagues_records = pl.DataFrame()
        
        # Combine everything (filter out empty dataframes)
        dfs_to_concat = [
            df for df in [
                historical_records,
                expired_records,
                unchanged_current_records,
                new_changed_records,
                new_leagues_records
            ] if len(df) > 0
        ]
        
        result_df = pl.concat(dfs_to_concat, how='diagonal')
    
    return result_df

def save_df_to_gcs(df: pl.DataFrame, bucket_name: str):
    file_path = f"gs://{bucket_name}/silver/fantasy/dim_league_scoring/data.parquet"
    
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved league scoring scd2 table to {file_path}") 
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

if __name__ == "__main__":
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    df = transform_dim_league_scoring_scd2()
    save_df_to_gcs(df, bucket_name)