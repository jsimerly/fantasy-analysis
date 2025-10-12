import os
import sys
from pathlib import Path
from datetime import datetime, timezone
import time

import polars as pl

env_path = sys.path.insert(0, str(Path(__file__).parent.parent))
from _utils import get_fantasy_leagues
from api.league import get_league

def flatten_league_to_parquets(league: dict):
    leagues_records = []
    settings_records = []
    rosters_records = []
    scoring_records = []
    
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

    # SCORING SETTINGS TABLE - only if scoring_settings exists
    if 'scoring_settings' in league and league['scoring_settings']:
        scoring_record = {
            'league_id': league['league_id'],
            **league['scoring_settings'] 
        }
        scoring_records.append(scoring_record)
    else:
        # Add a record with just the IDs if no scoring settings
        scoring_records.append({
            'league_id': league['league_id'],
        })
            
    
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
    scoring_df = pl.DataFrame(scoring_record)
    rosters_df = pl.DataFrame(rosters_records)
    
    # Fill null values in rosters_df with 0 for position columns
    position_cols = [col for col in rosters_df.columns if col not in ['league_id', 'league_lineage_id']]
    rosters_df = rosters_df.with_columns([
        pl.col(col).fill_null(0).cast(pl.Int64) for col in position_cols
    ])
    
    return leagues_df, settings_df, scoring_df, rosters_df

def save_df_to_gcs(df: pl.DataFrame, bucket_name: str, base_date: str, entity: str):
    file_path = f"gs://{bucket_name}/bronze/sleeper/league/{entity}/incremental/load_date={base_date}/data.parquet"
    
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved {entity} to {file_path}") 
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

def main():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date =  datetime.now(timezone.utc).strftime('%Y-%m-%d')

    leagues = get_fantasy_leagues()
    active_leagues = leagues.filter(pl.col("status") != "complete").filter(pl.col("source_system") == "sleeper")
    league_ids = active_leagues.select("league_id").to_series().to_list()
    
    all_leagues = []
    all_settings = []
    all_scoring = []
    all_roster_slots = []

    for league_id in league_ids:
        print(f"Processing league: {league_id}...")
        league_data = get_league(league_id=league_id)

        leagues_df, settings_df, scoring_df, rosters_df = flatten_league_to_parquets(league_data)

        all_leagues.append(leagues_df)
        all_settings.append(settings_df)
        all_scoring.append(scoring_df)
        all_roster_slots.append(rosters_df)
        print(f"✅ Processed {league_id}")
        time.sleep(1)

    combined_leagues = pl.concat(all_leagues)
    combined_settings = pl.concat(all_settings, how='align')
    combined_scoring = pl.concat(all_scoring, how='align')
    combined_roster_slots = pl.concat(all_roster_slots, how='align')
    
    save_df_to_gcs(combined_leagues, bucket_name, current_date, entity="leagues")
    save_df_to_gcs(combined_settings, bucket_name, current_date, entity="settings")
    save_df_to_gcs(combined_scoring, bucket_name, current_date, entity="scoring")
    save_df_to_gcs(combined_roster_slots, bucket_name, current_date, entity="roster_slots")
        
if __name__ == "__main__":
    main()