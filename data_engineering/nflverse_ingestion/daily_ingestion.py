from datetime import datetime
import os
import sys
from typing import Dict

import polars as pl
import nflreadpy as nfl
from dotenv import load_dotenv

load_dotenv()


# Datasets that need daily updates during the season
DAILY_DATASETS = {
    # Game data - updates after games complete
    'play_by_play': {'loader': nfl.load_pbp, 'folder': 'play_by_play', 'seasonal': True},
    'player_stats': {'loader': nfl.load_player_stats, 'folder': 'player_stats', 'seasonal': True},
    'team_stats': {'loader': lambda s: nfl.load_team_stats(seasons=s), 'folder': 'team_stats', 'seasonal': True},
    'schedules': {'loader': nfl.load_schedules, 'folder': 'schedules', 'seasonal': True},
    
    # Roster data - updates daily at 7AM UTC
    'rosters': {'loader': nfl.load_rosters, 'folder': 'rosters', 'seasonal': True},
    'rosters_weekly': {'loader': nfl.load_rosters_weekly, 'folder': 'rosters_weekly', 'seasonal': True},
    # 'injuries': {'loader': nfl.load_injuries, 'folder': 'injuries', 'seasonal': True}, #NOTE: This is out of commission
    'depth_charts': {'loader': nfl.load_depth_charts, 'folder': 'depth_charts', 'seasonal': True},
    
    # Advanced stats - updates multiple times daily
    'snap_counts': {'loader': nfl.load_snap_counts, 'folder': 'snap_counts', 'seasonal': True},
    'nextgen_stats': {'loader': nfl.load_nextgen_stats, 'folder': 'nextgen_stats', 'seasonal': True},
    'ftn_charting': {'loader': nfl.load_ftn_charting, 'folder': 'ftn_charting', 'seasonal': True},
    'officials': {'loader': nfl.load_officials, 'folder': 'officials', 'seasonal': True},
    
    # Fantasy data - updates weekly
    'ff_rankings': {'loader': nfl.load_ff_rankings, 'folder': 'fantasy_rankings', 'seasonal': False},
    'ff_opportunity': {'loader': nfl.load_ff_opportunity, 'folder': 'fantasy_opportunity', 'seasonal': False},
}


def fetch_and_save_dataset(name: str, config: Dict, current_season: int, bucket_name: str) -> Dict:
    """
    Fetch a single dataset and immediately save to GCS
    Returns result dictionary for tracking
    """
    result = {'name': name, 'success': False, 'rows': 0, 'error': None}
    
    try:
        # Fetch data
        print(f"→ {name}...")
        if config['seasonal']:
            df = config['loader']([current_season])
        else:
            df = config['loader']()
        
        # Check if we got data
        if df is None or df.height == 0:
            result['error'] = 'No data returned'
            print(f"  ✗ No data")
            return result
        
        result['rows'] = df.height
        
        # Build GCS path
        if config['seasonal']:
            path = f"gs://{bucket_name}/bronze/nflverse/{config['folder']}/season={current_season}/{name}.parquet"
        else:
            load_date = datetime.now().strftime('%Y-%m-%d')
            path = f"gs://{bucket_name}/bronze/nflverse/{config['folder']}/load_date={load_date}/{name}.parquet"
        
        # Save to GCS
        df.write_parquet(path)
        
        result['success'] = True
        print(f"  ✓ {df.height:,} rows → {path}")
        
    except Exception as e:
        result['error'] = str(e)
        print(f"  ✗ Error: {e}")
    
    return result


def main():
    """
    Daily incremental load - fetches and saves current season data
    
    Process:
    1. Get current season
    2. For each dataset: fetch → save → continue (even on error)
    3. Exit with appropriate code for Cloud Run Job alerting
    
    Exit codes:
        - 0: All datasets successful
        - 1: Partial failure (some datasets failed) - triggers alert
        - 2: Fatal error (job crashed) - triggers alert
    """
    try:
        # Get environment variables
        bucket_name = os.environ.get('GCS_BUCKET_NAME')
        if not bucket_name:
            print("ERROR: GCS_BUCKET_NAME not set")
            exit(2)
        
        # Get current season info
        current_season = nfl.get_current_season()
        current_week = nfl.get_current_week()
        timestamp = datetime.now().isoformat()
        
        # Print header
        print("=" * 70)
        print("NFL DAILY INCREMENTAL LOAD")
        print("=" * 70)
        print(f"Timestamp: {timestamp}")
        print(f"Season:    {current_season}")
        print(f"Week:      {current_week}")
        print(f"Bucket:    {bucket_name}")
        print("=" * 70)
        print()
        
        # Process each dataset
        results = []
        for name, config in DAILY_DATASETS.items():
            result = fetch_and_save_dataset(name, config, current_season, bucket_name)
            results.append(result)
        
        # Calculate summary
        total = len(results)
        successful = sum(1 for r in results if r['success'])
        failed = total - successful
        failures = [r for r in results if not r['success']]
        
        # Print summary
        print()
        print("=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"Total:      {total}")
        print(f"Successful: {successful}")
        print(f"Failed:     {failed}")
        
        if failures:
            print()
            print("Failures:")
            for f in failures:
                print(f"  • {f['name']}: {f['error']}")
        
        print("=" * 70)
        
        # Exit with appropriate code for Cloud Run Job
        if failed == 0:
            # All successful - exit 0 (success)
            print("\n✓ All datasets loaded successfully")
            sys.exit(0)
        elif successful > 0:
            # Partial success - exit 1 (will trigger CR alert)
            print(f"\n⚠ Partial failure: {failed}/{total} datasets failed")
            sys.exit(1)
        else:
            # All failed - exit 2 (will trigger CR alert)
            print(f"\n✗ All datasets failed")
            sys.exit(2)
            
    except Exception as e:
        # Fatal error - exit 2
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()