from datetime import datetime
import os
import sys
from typing import Dict, List, Union

import polars as pl
import nflreadpy as nfl
from dotenv import load_dotenv

load_dotenv()

# Configuration constants
SCHEDULE_DAILY = 'daily'
SCHEDULE_TUESDAY = 'Tuesday' # Best day for waivers/rankings updates

# Datasets configuration
DATASETS_CONFIG = {
    # --- DAILY DATASETS (Game state, Rosters, Stats) ---
    'play_by_play': {
        'loader': nfl.load_pbp, 
        'folder': 'play_by_play', 
        'seasonal': True,
        'schedule': SCHEDULE_DAILY
    },
    'player_stats': {
        'loader': nfl.load_player_stats, 
        'folder': 'player_stats', 
        'seasonal': True,
        'schedule': SCHEDULE_DAILY
    },
    'team_stats': {
        'loader': lambda s: nfl.load_team_stats(seasons=s), 
        'folder': 'team_stats', 
        'seasonal': True,
        'schedule': SCHEDULE_DAILY
    },
    'schedules': {
        'loader': nfl.load_schedules, 
        'folder': 'schedules', 
        'seasonal': True,
        'schedule': SCHEDULE_DAILY
    },
    'rosters': {
        'loader': nfl.load_rosters, 
        'folder': 'rosters', 
        'seasonal': True,
        'schedule': SCHEDULE_DAILY
    },
    'rosters_weekly': {
        'loader': nfl.load_rosters_weekly, 
        'folder': 'rosters_weekly', 
        'seasonal': True,
        'schedule': SCHEDULE_DAILY
    },
    'depth_charts': {
        'loader': nfl.load_depth_charts, 
        'folder': 'depth_charts', 
        'seasonal': True,
        'schedule': SCHEDULE_DAILY
    },
    'snap_counts': {
        'loader': nfl.load_snap_counts, 
        'folder': 'snap_counts', 
        'seasonal': True,
        'schedule': SCHEDULE_DAILY
    },
    'nextgen_stats': {
        'loader': nfl.load_nextgen_stats, 
        'folder': 'nextgen_stats', 
        'seasonal': True,
        'schedule': SCHEDULE_DAILY
    },
    'ftn_charting': {
        'loader': nfl.load_ftn_charting, 
        'folder': 'ftn_charting', 
        'seasonal': True,
        'schedule': SCHEDULE_DAILY
    },
    'officials': {
        'loader': nfl.load_officials, 
        'folder': 'officials', 
        'seasonal': True,
        'schedule': SCHEDULE_DAILY
    },
    
    # --- WEEKLY DATASETS (Fantasy Meta, IDs, Rankings) ---
    # These typically settle by Tuesday morning after MNF corrections
    'ff_rankings': {
        'loader': nfl.load_ff_rankings, 
        'folder': 'fantasy_rankings', 
        'seasonal': False,
        'schedule': SCHEDULE_TUESDAY
    },
    'ff_opportunity': {
        'loader': nfl.load_ff_opportunity, 
        'folder': 'fantasy_opportunity', 
        'seasonal': False,
        'schedule': SCHEDULE_TUESDAY
    },
    'ff_player_ids': {
        'loader': nfl.load_ff_playerids, 
        'folder': 'fantasy_player_ids', 
        'seasonal': False,
        'schedule': SCHEDULE_TUESDAY
    },
    'nfl_players': {
        'loader': nfl.load_players,
        'folder': 'nfl_players', 
        'seasonal': False,
        'schedule': SCHEDULE_TUESDAY
    }
}

def should_run_dataset(name: str, config: Dict, current_day: str, force_run: bool) -> bool:
    """
    Determines if a dataset should run based on schedule or force flag.
    """

    if force_run:
        return True
        
    schedule = config.get('schedule', SCHEDULE_DAILY)
    
    if schedule == SCHEDULE_DAILY:
        return True
    
    if schedule == current_day:
        return True
        
    return False

def fetch_and_save_dataset(name: str, config: Dict, current_season: int, bucket_name: str) -> Dict:
    result = {'name': name, 'success': False, 'rows': 0, 'error': None, 'skipped': False}
    
    try:
        print(f"→ Fetching {name}...")
        if config['seasonal']:
            df = config['loader']([current_season])
        else:
            df = config['loader']()
        
        if df is None or df.height == 0:
            result['error'] = 'No data returned'
            print(f"  ✗ No data found")
            return result
        
        result['rows'] = df.height
        
        # Build GCS path (Keep existing logic: load_date is fine for weekly data too)
        if config['seasonal']:
            path = f"gs://{bucket_name}/bronze/nflverse/{config['folder']}/season={current_season}/data.parquet"
        else:
            load_date = datetime.now().strftime('%Y-%m-%d')
            path = f"gs://{bucket_name}/bronze/nflverse/{config['folder']}/load_date={load_date}/data.parquet"
        
        df.write_parquet(path)
        
        result['success'] = True
        print(f"  ✓ {df.height:,} rows → {path}")
        
    except Exception as e:
        result['error'] = str(e)
        print(f"  ✗ Error: {e}")
    
    return result

def main():
    try:
        bucket_name = os.environ.get('GCS_BUCKET_NAME')
        force_run = os.environ.get('FORCE_RUN', 'false').lower() == 'true'
        
        if not bucket_name:
            print("ERROR: GCS_BUCKET_NAME not set")
            sys.exit(2)
        
        # Time context
        now = datetime.now()
        current_season = nfl.get_current_season()
        current_week = nfl.get_current_week()
        current_day_name = now.strftime('%A') # e.g., 'Tuesday'
        
        print("=" * 70)
        print("NFL DATA LOAD JOB")
        print("=" * 70)
        print(f"Timestamp:   {now.isoformat()}")
        print(f"Day of Week: {current_day_name}")
        print(f"Season/Week: {current_season} / {current_week}")
        print(f"Force Run:   {force_run}")
        print("=" * 70)
        print()
        
        results = []
        
        for name, config in DATASETS_CONFIG.items():
            # Check schedule
            if not should_run_dataset(name, config, current_day_name, force_run):
                print(f"○ Skipping {name} (Scheduled for {config['schedule']})")
                results.append({'name': name, 'success': True, 'skipped': True, 'error': None})
                continue
                
            # Run load
            result = fetch_and_save_dataset(name, config, current_season, bucket_name)
            results.append(result)
        
        # Summary
        total_attempted = sum(1 for r in results if not r.get('skipped'))
        skipped = sum(1 for r in results if r.get('skipped'))
        failed = sum(1 for r in results if not r['success'] and not r.get('skipped'))
        
        print()
        print("=" * 70)
        print(f"SUMMARY: {total_attempted} Loaded | {skipped} Skipped | {failed} Failed")
        print("=" * 70)
        
        if failed > 0:
            print("\nFailures:")
            for r in results:
                if not r['success'] and not r.get('skipped'):
                    print(f"  • {r['name']}: {r['error']}")
            sys.exit(1)
            
        print("\n✓ Job completed successfully")
        sys.exit(0)
            
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)

if __name__ == "__main__":
    main()