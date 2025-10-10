import os
import sys
from pathlib import Path
from datetime import datetime, timezone
import time

import polars as pl

env_path = sys.path.insert(0, str(Path(__file__).parent.parent))
from _utils import get_bronze_leagues
from api.league import get_league

def flatten_league_to_parquets(league: dict):
    leagues_records = []
    settings_records = []
    rosters_records = []
    
    # LEAGUES TABLE - one row per league/season
    league_record = {
        'league_id': league['league_id'],
        'league_name': league['name'],
        'season': league['season'],
        'status': league['status'],
        'season_type': league['season_type'],
        'total_rosters': league['total_rosters'],
        'draft_id': league['draft_id'],
        'bracket_id': league['bracket_id'],
        'leg': league['settings']['leg'],
        'last_scored_leg': league['settings']['last_scored_leg'],
        'previous_league_id': league['previous_league_id']
    }
    leagues_records.append(league_record)
    
    # SETTINGS TABLE - one row per league with all settings
    settings_record = {
        'league_id': league['league_id'],
        **league['settings']  # Unpack all settings as columns
    }
    settings_records.append(settings_record)
    
    # ROSTERS TABLE - count unique positions
    roster_positions = league['roster_positions']
    position_counts = {}
    
    for position in roster_positions:
        position_counts[position] = position_counts.get(position, 0) + 1

    position_counts['TAXI'] = league['settings'].get('taxi_slots', 0) or 0
    position_counts['IR'] = league['settings'].get('reserve_slots', 0) or 0
    
    roster_record = {
        'league_id': league['league_id'],
        **position_counts  
    }
    rosters_records.append(roster_record)
    
    # Convert to Polars DataFrames
    leagues_df = pl.DataFrame(leagues_records)
    settings_df = pl.DataFrame(settings_records)
    rosters_df = pl.DataFrame(rosters_records)
    
    # Fill null values in rosters_df with 0 for position columns
    position_cols = [col for col in rosters_df.columns if col not in ['league_id', 'league_lineage_id']]
    rosters_df = rosters_df.with_columns([
        pl.col(col).fill_null(0).cast(pl.Int64) for col in position_cols
    ])
    
    return leagues_df, settings_df, rosters_df

def save_df_to_gcs(df: pl.DataFrame, bucket_name: str, base_date: str, file_name: str):
    file_path = f"gs://{bucket_name}/bronze/sleeper/league/leagues/daily/load_date={base_date}/{file_name}.parquet"
    
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved {file_name} to {file_path}") 
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

def main():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date =  datetime.now(timezone.utc).strftime('%Y-%m-%d')

    leagues = get_bronze_leagues()
    active_leagues = leagues.filter(pl.col("status") == "in_season")
    league_ids = active_leagues.select("league_id").to_series().to_list()
    
    for league_id in league_ids:
        league_data = get_league(league_id=league_id)

        leagues_df, settings_df, rosters_df = flatten_league_to_parquets(league_data)
    
        save_df_to_gcs(leagues_df, bucket_name, current_date, file_name="leagues")
        save_df_to_gcs(settings_df, bucket_name, current_date, file_name="settings")
        save_df_to_gcs(rosters_df, bucket_name, current_date, file_name="roster_slots")
        time.sleep(1)

if __name__ == "__main__":
    main()