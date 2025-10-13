from datetime import datetime
import os
import sys
from typing import Dict

import polars as pl
import nflreadpy as nfl
from dotenv import load_dotenv

load_dotenv()


def fetch_and_save_seasonal_dataset(name: str, loader_func: callable, 
                                    start_season: int, end_season: int,
                                    folder: str, bucket_name: str) -> Dict:
    """
    Fetch a seasonal dataset and save each season as a separate partition
    """
    result = {'name': name, 'success': False, 'seasons_loaded': 0, 'error': None}
    
    try:
        # Fetch all seasons at once
        available_seasons = list(range(start_season, end_season + 1))
        print(f"→ {name} ({start_season}-{end_season})...")
        
        df = loader_func(available_seasons)
        
        if df is None or df.height == 0:
            result['error'] = 'No data returned'
            print(f"  ✗ No data")
            return result
        
        print(f"  ✓ Fetched {df.height:,} records")
        
        # Check if 'season' column exists
        if 'season' not in df.columns:
            # No season column - save with load_date partition instead
            load_date = datetime.now().strftime('%Y-%m-%d')
            path = f"gs://{bucket_name}/bronze/nfl/{folder}/load_date={load_date}/{name}.parquet"
            df.write_parquet(path)
            print(f"  ⚠ No season column - saved to load_date partition: {path}")
            result['success'] = True
            result['seasons_loaded'] = 1
            return result
        
        # Get unique seasons in the data
        seasons_in_data = df.select('season').unique().sort('season')['season'].to_list()
        
        # Save each season as a separate partition
        print(f"  → Saving {len(seasons_in_data)} season partitions...")
        for season in seasons_in_data:
            season_df = df.filter(pl.col('season') == season)
            path = f"gs://{bucket_name}/bronze/nfl/{folder}/season={season}/{name}.parquet"
            season_df.write_parquet(path)
            result['seasons_loaded'] += 1
        
        print(f"  ✓ Saved {result['seasons_loaded']} seasons to bronze/nfl/{folder}/season=YYYY/")
        result['success'] = True
        
    except Exception as e:
        result['error'] = str(e)
        print(f"  ✗ Error: {e}")
    
    return result


def fetch_and_save_non_seasonal_dataset(name: str, loader_func: callable,
                                        folder: str, bucket_name: str) -> Dict:
    """
    Fetch a non-seasonal dataset and save with load_date partition
    """
    result = {'name': name, 'success': False, 'rows': 0, 'error': None}
    
    try:
        print(f"→ {name}...")
        
        df = loader_func()
        
        if df is None or df.height == 0:
            result['error'] = 'No data returned'
            print(f"  ✗ No data")
            return result
        
        result['rows'] = df.height
        
        # Save with load_date partition
        load_date = datetime.now().strftime('%Y-%m-%d')
        path = f"gs://{bucket_name}/bronze/nfl/{folder}/load_date={load_date}/{name}.parquet"
        df.write_parquet(path)
        
        print(f"  ✓ {df.height:,} rows → {path}")
        result['success'] = True
        
    except Exception as e:
        result['error'] = str(e)
        print(f"  ✗ Error: {e}")
    
    return result


def main():
    """
    Initial full load with season partitioning
    
    This loads ALL historical data and partitions seasonal data by season.
    After this runs, daily incremental loads will update only the current season.
    
    Structure created:
    bronze/nfl/
    ├── play_by_play/season=1999/, season=2000/, ..., season=2024/
    ├── player_stats/season=1999/, season=2000/, ..., season=2024/
    ├── players/load_date=2024-10-06/  (non-seasonal)
    └── ...
    """
    try:
        bucket_name = os.environ.get('GCS_BUCKET_NAME')
        if not bucket_name:
            print("ERROR: GCS_BUCKET_NAME not set")
            sys.exit(2)
        
        current_season = nfl.get_current_season()
        timestamp = datetime.now().isoformat()
        
        print("=" * 70)
        print("NFL DATA LAKE - INITIAL FULL LOAD (SEASON PARTITIONED)")
        print("=" * 70)
        print(f"Timestamp:      {timestamp}")
        print(f"Current Season: {current_season}")
        print(f"Bucket:         {bucket_name}")
        print("=" * 70)
        print()
        
        results = []
        
        # =====================================================================
        # SEASONAL DATA - Save with season partitions
        # =====================================================================
        print("=" * 70)
        print("SEASONAL DATA (Partitioned by season)")
        print("=" * 70)
        
        seasonal_datasets = {
            'play_by_play': (nfl.load_pbp, 1999, current_season, 'play_by_play'),
            'player_stats': (nfl.load_player_stats, 1999, current_season, 'player_stats'),
            'team_stats': (lambda s: nfl.load_team_stats(seasons=s), 1999, current_season, 'team_stats'),
            'rosters': (nfl.load_rosters, 1999, current_season, 'rosters'),
            'rosters_weekly': (nfl.load_rosters_weekly, 2002, current_season, 'rosters_weekly'),
            'schedules': (nfl.load_schedules, 1999, current_season, 'schedules'),
            'snap_counts': (nfl.load_snap_counts, 2012, current_season, 'snap_counts'),
            'nextgen_stats': (nfl.load_nextgen_stats, 2016, current_season, 'nextgen_stats'),
            'ftn_charting': (nfl.load_ftn_charting, 2022, current_season, 'ftn_charting'),
            'participation': (nfl.load_participation, 2016, 2024, 'participation'),
            'injuries': (nfl.load_injuries, 2009, current_season, 'injuries'),
            'officials': (nfl.load_officials, 2015, current_season, 'officials'),
            'depth_charts': (nfl.load_depth_charts, 2001, current_season, 'depth_charts'),
        }
        
        for name, (loader, start, end, folder) in seasonal_datasets.items():
            result = fetch_and_save_seasonal_dataset(name, loader, start, end, folder, bucket_name)
            results.append(result)
        
        # =====================================================================
        # NON-SEASONAL DATA - Save with load_date partition
        # =====================================================================
        print()
        print("=" * 70)
        print("NON-SEASONAL DATA (Partitioned by load_date)")
        print("=" * 70)
        
        non_seasonal_datasets = {
            'players': (nfl.load_players, 'players'),
            'ff_playerids': (nfl.load_ff_playerids, 'fantasy_ids'),
            'combine': (nfl.load_combine, 'combine'),
            'draft_picks': (nfl.load_draft_picks, 'draft_picks'),
            'contracts': (nfl.load_contracts, 'contracts'),
            'trades': (nfl.load_trades, 'trades'),
            'ff_rankings': (nfl.load_ff_rankings, 'fantasy_rankings'),
            'ff_opportunity': (nfl.load_ff_opportunity, 'fantasy_opportunity'),
        }
        
        for name, (loader, folder) in non_seasonal_datasets.items():
            result = fetch_and_save_non_seasonal_dataset(name, loader, folder, bucket_name)
            results.append(result)
        
        # =====================================================================
        # SUMMARY
        # =====================================================================
        total = len(results)
        successful = sum(1 for r in results if r['success'])
        failed = total - successful
        failures = [r for r in results if not r['success']]
        
        print()
        print("=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"Total datasets: {total}")
        print(f"Successful:     {successful}")
        print(f"Failed:         {failed}")
        
        if failures:
            print()
            print("Failures:")
            for f in failures:
                print(f"  • {f['name']}: {f['error']}")
        
        print("=" * 70)
        
        # Exit with appropriate code
        if failed == 0:
            print("\n✓ Initial load complete - all datasets loaded successfully")
            sys.exit(0)
        elif successful > 0:
            print(f"\n⚠ Initial load partial failure: {failed}/{total} datasets failed")
            sys.exit(1)
        else:
            print(f"\n✗ Initial load failed: all datasets failed")
            sys.exit(2)
            
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()