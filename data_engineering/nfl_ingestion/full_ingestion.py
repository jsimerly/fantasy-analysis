from datetime import datetime
import os
from typing import Any

import polars as pl
import nflreadpy as nfl
from dotenv import load_dotenv

load_dotenv()

def fetch_all_nfl_data() -> dict[str, pl.DataFrame]:
    datasets = {}
    current_year = datetime.now().year
    available_seasons = list(range(1999, current_year+1))

    try:
        print("Fetching comprehensive NFL data from nflverse...")
        
        # Core player data
        print("- Fetching players...")
        datasets['players'] = nfl.load_players()
        print(f"  ✓ Players: {len(datasets['players'])} records")
        
        # Fantasy player IDs (includes Sleeper, ESPN, etc.)
        print("- Fetching fantasy player IDs...")
        datasets['ff_playerids'] = nfl.load_ff_playerids()
        print(f"  ✓ Fantasy IDs: {len(datasets['ff_playerids'])} records")
        
        # ALL SEASONS Player stats
        print("- Fetching ALL seasons player stats...")
        datasets['player_stats_all_seasons'] = nfl.load_player_stats(available_seasons)
        print(f"  ✓ All Seasons Stats: {len(datasets['player_stats_all_seasons'])} records")
        
        # ALL SEASONS Rosters
        print("- Fetching ALL seasons rosters...")
        datasets['rosters_all_seasons'] = nfl.load_rosters(available_seasons)
        print(f"  ✓ All Seasons Rosters: {len(datasets['rosters_all_seasons'])} records")
        
        # ALL SEASONS Schedules
        print("- Fetching ALL seasons schedules...")
        datasets['schedules_all_seasons'] = nfl.load_schedules(available_seasons)
        print(f"  ✓ All Seasons Schedules: {len(datasets['schedules_all_seasons'])} records")
        
        # Team stats (all available)
        print("- Fetching team stats...")
        datasets['team_stats'] = nfl.load_team_stats(seasons=True) 
        print(f"  ✓ Team Stats: {len(datasets['team_stats'])} records")
        
        print(f"Successfully fetched {len(datasets)} datasets covering all available seasons.")
        return datasets
        
    except Exception as e:
        print(f"An error occurred while fetching data: {e}")
        # Return any datasets that were successfully fetched
        return datasets
    
def save_datasets_to_gcs(datasets: dict[str, pl.DataFrame], bucket_name: str, base_date: str) -> dict[str, bool]:
    """Save all datasets to GCS with organized folder structure using Polars"""
    results = {}
    
    for dataset_name, df in datasets.items():
        if df is None or df.height == 0:
            print(f"Skipping empty dataset: {dataset_name}")
            results[dataset_name] = False
            continue
        
        # Organize by data type and date
        if 'pbp' in dataset_name:
            folder = 'play_by_play'
        elif 'player_stats' in dataset_name:
            folder = 'player_stats'
        elif 'rosters' in dataset_name:
            folder = 'rosters'
        elif 'schedules' in dataset_name:
            folder = 'schedules'
        elif 'team_stats' in dataset_name:
            folder = 'team_stats'
        elif dataset_name == 'players':
            folder = 'players'
        elif dataset_name == 'ff_playerids':
            folder = 'fantasy_ids'
        else:
            folder = 'other'
        
        file_path = f'bronze/nflreadpy/{folder}/load_date={base_date}/{dataset_name}.parquet'
        
        try:
            gcs_path = f"gs://{bucket_name}/{file_path}"
            
            print(f"  → Saving {dataset_name}: {df.height} rows, {df.width} columns")
            
            df.write_parquet(gcs_path)
            print(f"Saved {dataset_name} to {file_path}")
            results[dataset_name] = True
            
        except Exception as e:
            print(f"Failed to save {dataset_name}: {e}")
            results[dataset_name] = False
    
    return results
    
def main(request: Any = None):
    """Main Cloud Function entry point"""
    try:
        bucket_name = os.environ.get('GCS_BUCKET_NAME')
        
        if not bucket_name:
            print("ERROR: GCS_BUCKET_NAME environment variable not set.")
            return 'Deployment Error: Missing GCS_BUCKET_NAME', 500

        datasets = fetch_all_nfl_data()
        
        if not datasets:
            print("No datasets fetched successfully.")
            return 'No data fetched', 200
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        results = save_datasets_to_gcs(datasets, bucket_name, current_date)
        
        successful_saves = sum(results.values())
        total_datasets = len(datasets)
        
        print(f"\n=== FINAL SUMMARY ===")
        print(f"Datasets fetched: {total_datasets}")
        print(f"Successfully saved: {successful_saves}")
        print(f"Failed saves: {total_datasets - successful_saves}")
            
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        return 'Internal Server Error', 500

if __name__ == "__main__":
    main()