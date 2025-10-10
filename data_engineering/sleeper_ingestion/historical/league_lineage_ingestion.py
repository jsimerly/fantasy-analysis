### Goal of this code is to ingest all previous leagues primary data, we will reconstuct the league history in the silver layer

import os
from datetime import datetime, timezone
import sys
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

env_path = sys.path.insert(0, str(Path(__file__).parent.parent))
from api.league import get_user_leagues_for_year, get_league
from _utils import get_sport_state

load_dotenv(dotenv_path=env_path)
    
def get_user_leagues_lineage(user_id: str = '730630390791929856', sport: str = "nfl", season: str | int = None):
    if season is None:
        sports_state = get_sport_state(sport)
        season = sports_state['season']

    leagues = get_user_leagues_for_year(user_id=user_id, sport=sport, year=season)

    all_leagues = []
    for league in leagues:
        league_name = league['name']

        league_info = {
            'league_id': league['league_id'],
            'league_name': league['name'],
            'roster_positions': league['roster_positions'],
            'draft_id': league['draft_id'],
            'bracket_id': league['bracket_id'],
            'status': league['status'],
            'season': league['season'],
            'total_rosters': league['total_rosters'],
            'settings': league['settings'],
            'season_type': league['season_type'],
            'previous_league_id': league['previous_league_id']
        }

        lineage = [league_info]
        curr_league_id = league['previous_league_id']
        while curr_league_id:
            curr_league = get_league(league_id=curr_league_id)

            league_info = {
                'league_id': curr_league['league_id'],
                'league_name': curr_league['name'],
                'roster_positions': curr_league['roster_positions'],
                'draft_id': curr_league['draft_id'],
                'bracket_id': curr_league['bracket_id'],
                'status': curr_league['status'],
                'season': curr_league['season'],
                'total_rosters': curr_league['total_rosters'],
                'settings': curr_league['settings'],
                'season_type': curr_league['season_type'],
                'previous_league_id': curr_league['previous_league_id']
            }
            lineage.append(league_info)
            
            curr_league_id = curr_league['previous_league_id']

        total_seasons = len(lineage)
        league_pk= lineage[-1]['league_id']
        
        for i, league_entry in enumerate(lineage):
            league_entry['season_number'] = total_seasons - i
            league_entry['league_lineage_pk'] = league_pk

        league_data = {
            'league_name': league_name,
            'league_lineage_id': league_pk,
            'lineage': lineage,
        }

        all_leagues.append(league_data)
    
    return all_leagues

def flatten_lineage_to_parquets(all_leagues: list[dict]) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Flatten league lineage data into three normalized DataFrames.
    
    Returns:
        tuple: (leagues_df, settings_df, rosters_df)
    """
    
    leagues_records = []
    settings_records = []
    rosters_records = []
    
    for league_group in all_leagues:
        league_lineage_id = league_group['league_lineage_id']
        
        for league in league_group['lineage']:
            # LEAGUES TABLE - one row per league/season
            league_record = {
                'league_id': league['league_id'],
                'league_lineage_id': league_lineage_id,
                'league_name': league['league_name'],
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
                'league_lineage_id': league_lineage_id,
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
                'league_lineage_id': league_lineage_id,
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

def save_df_to_gcs(df: pl.DataFrame, bucket_name: str, base_date: str, entity: str):
    file_path = f"gs://{bucket_name}/bronze/sleeper/league/{entity}/load_date={base_date}/data.parquet" 

    try:
        df.write_parquet(file_path)
        print(f"✅ Saved {entity} to {file_path}") 
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

if __name__ == "__main__":
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date =  datetime.now(timezone.utc).strftime('%Y-%m-%d')

    leagues_data = get_user_leagues_lineage()
    leagues_df, settings_df, rosters_df = flatten_lineage_to_parquets(leagues_data)
    
    save_df_to_gcs(leagues_df, bucket_name, current_date, entity="leagues")
    save_df_to_gcs(settings_df, bucket_name, current_date, entity="settings")
    save_df_to_gcs(rosters_df, bucket_name, current_date, entity="roster_slots")