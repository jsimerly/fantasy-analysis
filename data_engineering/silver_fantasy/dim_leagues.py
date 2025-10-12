from pathlib import Path
from datetime import datetime
import os

import polars as pl
from dotenv import load_dotenv

from utils import get_latest_bronze_path, merge_full_and_incremental

load_dotenv()

def transform_dim_leagues() -> pl.DataFrame:
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    # Get paths
    full_leagues_path = get_latest_bronze_path(bucket_name, "league/leagues/full_load")
    daily_leagues_path = get_latest_bronze_path(bucket_name, "league/leagues/incremental")
    full_rosters_path = get_latest_bronze_path(bucket_name, "league/roster_slots/full_load")
    daily_rosters_path = get_latest_bronze_path(bucket_name, "league/roster_slots/incremental")
    full_settings_path = get_latest_bronze_path(bucket_name, "league/settings/full_load")
    daily_settings_path = get_latest_bronze_path(bucket_name, "league/settings/incremental")

    # Read all dataframes
    full_leagues_df = pl.read_parquet(full_leagues_path)
    daily_leagues_df = pl.read_parquet(daily_leagues_path)
    full_rosters_df = pl.read_parquet(full_rosters_path)
    daily_rosters_df = pl.read_parquet(daily_rosters_path)
    full_settings_df = pl.read_parquet(full_settings_path)
    daily_settings_df = pl.read_parquet(daily_settings_path)
    
    ### Merge full and incremental data ###
    
    # League - preserve league_lineage_id
    leagues_df = merge_full_and_incremental(
        full_leagues_df,
        daily_leagues_df,
        join_key='league_id',
        preserve_columns=['league_lineage_id']
    )
    
    # Roster - preserve league_lineage_id
    rosters_df = merge_full_and_incremental(
        full_rosters_df,
        daily_rosters_df,
        join_key='league_id',
        preserve_columns=['league_lineage_id']
    )
    
    # Settings - select key columns first, then merge
    settings_key_cols = [
        'league_id', 
        'league_lineage_id', 
        'leg',
        'last_scored_leg',
        'best_ball',
        'type',
        'num_teams',
        'playoff_teams',
        'playoff_type',
        'playoff_week_start',
        'draft_rounds',
        'waiver_type',
        'waiver_budget',
        'trade_deadline',
        'league_average_match',
        'taxi_years'
    ]
    
    full_settings_selected = full_settings_df.select(
        [c for c in settings_key_cols if c in full_settings_df.columns]
    )
    daily_settings_selected = daily_settings_df.select(
        [c for c in settings_key_cols if c in daily_settings_df.columns]
    )
    
    settings_df = merge_full_and_incremental(
        full_settings_selected,
        daily_settings_selected,
        join_key='league_id',
        preserve_columns=['league_lineage_id']
    )

    ## Rename roster columns to be more descriptive
    roster_rename_map = {
        'QB': 'qb_slots',
        'RB': 'rb_slots',
        'WR': 'wr_slots',
        'TE': 'te_slots',
        'FLEX': 'flex_slots',
        'SUPER_FLEX': 'superflex_slots',
        'BN': 'bench_slots',
        'TAXI': 'taxi_slots',
        'IR': 'ir_slots',
        'K': 'k_slots',
        'DEF': 'def_slots'
    }
    rosters_df = rosters_df.rename(roster_rename_map)

    roster_slot_cols = [
        'qb_slots', 'rb_slots', 'wr_slots', 'te_slots', 'flex_slots',
        'superflex_slots', 'bench_slots', 'taxi_slots', 'ir_slots', 
        'k_slots', 'def_slots'
    ]
    rosters_df = rosters_df.with_columns([
        pl.col(col).fill_null(0).cast(pl.Int64) for col in roster_slot_cols
    ])

    ### Join everything together ###
    dim_leagues = (
        leagues_df
        .join(rosters_df.drop('league_lineage_id'), on='league_id', how='left')
        .join(settings_df.drop('league_lineage_id'), on='league_id', how='left')
    )

    # Add derived columns
    dim_leagues = dim_leagues.with_columns([
        # Is Original - check if this is the founding league
        (pl.col('league_id') == pl.col('league_lineage_id')).alias('is_original'),  
        
        # Calculate total roster spots
        (pl.col('qb_slots') + 
         pl.col('rb_slots') + 
         pl.col('wr_slots') + 
         pl.col('te_slots') + 
         pl.col('flex_slots') + 
         pl.col('superflex_slots') + 
         pl.col('bench_slots') + 
         pl.col('taxi_slots') + 
         pl.col('ir_slots') + 
         pl.col('k_slots') + 
         pl.col('def_slots')).alias('total_roster_spots'),
        
        # Active flag
        (pl.col('status') != 'complete').alias('is_active'),
        
        # Check if superflex
        (pl.col('superflex_slots').fill_null(0) > 0).alias('is_superflex'),
        
        # Source system
        pl.lit('sleeper').alias('source_system'),
        
        # Load timestamp
        pl.lit(datetime.now()).alias('loaded_at')
    ])

    return dim_leagues

def save_df_to_gcs(df: pl.DataFrame, bucket_name: str):
    file_path = f"gs://{bucket_name}/silver/fantasy/dim_leagues/data.parquet"
    
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved leagues table to {file_path}") 
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

if __name__ == "__main__":
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    df = transform_dim_leagues()
    save_df_to_gcs(df, bucket_name)
