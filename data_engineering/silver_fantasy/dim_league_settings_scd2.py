from pathlib import Path
from datetime import datetime
import os

import polars as pl
from dotenv import load_dotenv

from utils import get_latest_bronze_path, merge_full_and_incremental

load_dotenv()

def transform_dim_league_settings_scd2() -> pl.DataFrame:
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    
    # --- 1. Setup Paths ---
    # Scoring
    full_scoring_path = get_latest_bronze_path(bucket_name, "league/scoring/full_load")
    daily_scoring_path = get_latest_bronze_path(bucket_name, "league/scoring/incremental")
    # Rosters
    full_rosters_path = get_latest_bronze_path(bucket_name, "league/roster_slots/full_load")
    daily_rosters_path = get_latest_bronze_path(bucket_name, "league/roster_slots/incremental")
    # Settings
    full_settings_path = get_latest_bronze_path(bucket_name, "league/settings/full_load")
    daily_settings_path = get_latest_bronze_path(bucket_name, "league/settings/incremental")
    
    existing_silver_path = f"gs://{bucket_name}/silver/fantasy/dim_league_settings/data.parquet"

    # --- 2. Process Scoring (The Base) ---
    scoring_df = merge_full_and_incremental(
        pl.read_parquet(full_scoring_path),
        pl.read_parquet(daily_scoring_path),
        join_key='league_id',
        preserve_columns=['league_lineage_id']
    )

    # --- 3. Process Rosters ---
    raw_rosters_df = merge_full_and_incremental(
        pl.read_parquet(full_rosters_path),
        pl.read_parquet(daily_rosters_path),
        join_key='league_id',
        preserve_columns=['league_lineage_id']
    )

    # Rename and clean up Roster Columns
    roster_rename_map = {
        'QB': 'qb_slots', 'RB': 'rb_slots', 'WR': 'wr_slots', 'TE': 'te_slots',
        'FLEX': 'flex_slots', 'SUPER_FLEX': 'superflex_slots', 'BN': 'bench_slots',
        'TAXI': 'taxi_slots', 'IR': 'ir_slots', 'K': 'k_slots', 'DEF': 'def_slots'
    }
    rosters_df = raw_rosters_df.rename(roster_rename_map)

    roster_slot_cols = list(roster_rename_map.values())
    
    rosters_df = rosters_df.with_columns([
        pl.col(col).fill_null(0).cast(pl.Int64) for col in roster_slot_cols if col in rosters_df.columns
    ])

    # --- 4. Process Settings ---
    full_settings_raw = pl.read_parquet(full_settings_path)
    daily_settings_raw = pl.read_parquet(daily_settings_path)

    # EXCLUDING 'leg' and 'last_scored_leg' because they change weekly and would bloat the history table.
    settings_key_cols = [
        'league_id', 'league_lineage_id', 'best_ball', 'type', 'num_teams',
        'playoff_teams', 'playoff_type', 'playoff_week_start', 'draft_rounds',
        'waiver_type', 'waiver_budget', 'trade_deadline', 'league_average_match',
        'taxi_years'
    ]

    settings_df = merge_full_and_incremental(
        full_settings_raw.select([c for c in settings_key_cols if c in full_settings_raw.columns]),
        daily_settings_raw.select([c for c in settings_key_cols if c in daily_settings_raw.columns]),
        join_key='league_id',
        preserve_columns=['league_lineage_id']
    )

    # --- 5. Join into One "Constitution" DataFrame ---
    new_rules_df = (
        scoring_df
        .join(rosters_df.drop('league_lineage_id'), on='league_id', how='left')
        .join(settings_df.drop('league_lineage_id'), on='league_id', how='left')
    )

    # --- 6. SCD Type 2 Logic ---
    
    try:
        existing_df = pl.read_parquet(existing_silver_path)
    except:
        existing_df = None
    
    current_timestamp = datetime.now()
    
    if existing_df is None:
        # First load - all records are current
        result_df = new_rules_df.with_columns([
            pl.lit(current_timestamp).alias('valid_from'),
            pl.lit(None).alias('valid_to'),
            pl.lit(True).alias('is_current'),
            pl.lit('sleeper').alias('source_system'),
            pl.lit(current_timestamp).alias('loaded_at')
        ])
    else:
        # Get currently active records
        current_records = existing_df.filter(pl.col('is_current') == True)
        
        # Identify columns to check for changes (Everything except metadata)
        metadata_cols = ['league_id', 'league_lineage_id', 'valid_from', 'valid_to', 
                         'is_current', 'source_system', 'loaded_at']
        rule_cols = [c for c in new_rules_df.columns if c not in metadata_cols]
        
        # Find changes by comparing new data to current records
        comparison = new_rules_df.join(
            current_records.select(['league_id'] + rule_cols),
            on='league_id',
            how='left',
            suffix='_old'
        )
        
        # Create a flag for whether ANY rule column changed
        change_conditions = [
            (pl.col(col) != pl.col(f"{col}_old")) | 
            (pl.col(col).is_null() != pl.col(f"{col}_old").is_null())
            for col in rule_cols 
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
        changed_leagues = comparison.filter(pl.col('has_changed') == True).select(new_rules_df.columns)
        unchanged_leagues = comparison.filter(pl.col('has_changed') == False).select(new_rules_df.columns)
        new_leagues = comparison.filter(pl.col('has_changed').is_null()).select(new_rules_df.columns)
        
        changed_league_ids = changed_leagues['league_id'].to_list() if len(changed_leagues) > 0 else []
        unchanged_league_ids = unchanged_leagues['league_id'].to_list() if len(unchanged_leagues) > 0 else []
        
        # 1. Expire old records for changed leagues
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
        
        # 2. Update loaded_at for unchanged leagues (Heartbeat)
        if unchanged_league_ids:
            unchanged_current_records = current_records.filter(
                pl.col('league_id').is_in(unchanged_league_ids)
            ).with_columns([
                pl.lit(current_timestamp).alias('loaded_at')
            ])
        else:
            unchanged_current_records = pl.DataFrame()
        
        # 3. Keep historical records
        historical_records = existing_df.filter(pl.col('is_current') == False)
        
        # 4. Create new records for new leagues
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
        
        # Combine everything
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
    file_path = f"gs://{bucket_name}/silver/fantasy/dim_league_settings/data.parquet"
    
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved league settings SCD2 table to {file_path}") 
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

if __name__ == "__main__":
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    df = transform_dim_league_settings_scd2()
    save_df_to_gcs(df, bucket_name)