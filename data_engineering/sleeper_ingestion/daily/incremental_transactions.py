import sys
from pathlib import Path
import os
from datetime import datetime, timedelta
import time
import json
env_path = sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl

from api.league import get_transactions, get_league
from _utils import get_sport_state, get_leagues



def save_df_to_gcs(df: pl.DataFrame, bucket_name: str, file_name: str):
    weekly_partition = get_weekly_partition_date()
    file_path = f"gs://{bucket_name}/bronze/sleeper/league/transactions/daily/leg={weekly_partition}/{file_name}.parquet"
    
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved {df.count()} {file_name} to {file_path} (week of {weekly_partition})")
    except Exception as e:
        print(f"Failed to save {file_name} to GCS: {e}")
        raise

def flatten_transactions(all_transactions: list[dict], league_id: str) -> tuple[pl.DataFrame, pl.DataFrame | None, pl.DataFrame | None]:    
    transaction_records = []
    player_records = []
    draft_pick_records = []
    
    for txn in all_transactions:
        settings = txn.get('settings') or {}
        metadata = txn.get('metadata') or {}
        
        # 1. TRANSACTIONS TABLE - header info only
        transaction_record = {
            'transaction_id': txn['transaction_id'],
            'league_id': league_id,
            'type': txn['type'],
            'status': txn['status'],
            'created': txn['created'],
            'status_updated': txn['status_updated'],
            'creator': txn['creator'],
            'leg': txn['leg'],
            'waiver_bid': settings.get('waiver_bid'),
            'waiver_seq': settings.get('seq'),
            'waiver_budget': json.dumps(txn.get('waiver_budget')),
            'metadata_notes': metadata.get('notes'),
            'consenter_ids': json.dumps(txn.get('consenter_ids', [])),
            'roster_ids': json.dumps(txn.get('roster_ids', [])),
        }
        transaction_records.append(transaction_record)
        
        # 2. TRANSACTION_PLAYERS TABLE - one row per player added/dropped
        player_map = txn.get('player_map') or {}
        
        # Handle ADDS
        if txn.get('adds'):
            for player_id, roster_id in txn['adds'].items():
                player_info = player_map.get(player_id) or {}
                
                player_record = {
                    'transaction_id': txn['transaction_id'],
                    'league_id': league_id,
                    'player_id': player_id,
                    'roster_id': roster_id,
                    'action': 'add',
                    'player_first_name': player_info.get('first_name'),
                    'player_last_name': player_info.get('last_name'),
                    'player_position': player_info.get('position'),
                    'player_team': player_info.get('team'),
                    'player_number': player_info.get('number'),
                    'player_status': player_info.get('status'),
                    'player_injury_status': player_info.get('injury_status'),
                    'player_years_exp': player_info.get('years_exp')
                }
                player_records.append(player_record)
        
        # Handle DROPS
        if txn.get('drops'):
            for player_id, roster_id in txn['drops'].items():
                player_info = player_map.get(player_id) or {}
                
                player_record = {
                    'transaction_id': txn['transaction_id'],
                    'league_id': league_id,
                    'player_id': player_id,
                    'roster_id': roster_id,
                    'action': 'drop',
                    'player_first_name': player_info.get('first_name'),
                    'player_last_name': player_info.get('last_name'),
                    'player_position': player_info.get('position'),
                    'player_team': player_info.get('team'),
                    'player_number': player_info.get('number'),
                    'player_status': player_info.get('status'),
                    'player_injury_status': player_info.get('injury_status'),
                    'player_years_exp': player_info.get('years_exp')
                }
                player_records.append(player_record)
        
        # 3. TRANSACTION_DRAFT_PICKS TABLE - one row per draft pick
        if txn.get('draft_picks'):
            for draft_pick_str in txn['draft_picks']:
                parts = draft_pick_str.rsplit(',', 2)
                pick_sleeper_id = parts[0] 
                from_team_id = parts[1]     
                to_team_id = parts[2]     
             
                pick_parts = pick_sleeper_id.split(',')
                
                pick_record = {
                    'transaction_id': txn['transaction_id'],
                    'league_id': league_id,
                    'draft_pick_id': draft_pick_str,  # Full string as ID
                    'roster_id': int(pick_parts[0]) if len(pick_parts) > 0 else None,
                    'season': pick_parts[1] if len(pick_parts) > 1 else None,
                    'round': int(pick_parts[2]) if len(pick_parts) > 2 else None,
                    'from_team_id': int(from_team_id),
                    'to_team_id': int(to_team_id)
                }

                draft_pick_records.append(pick_record)
    
    # Convert to Polars DataFrames
    transactions_df = pl.DataFrame(transaction_records)
    players_df = pl.DataFrame(player_records)
    draft_picks_df = pl.DataFrame(draft_pick_records)
    
    return transactions_df, players_df, draft_picks_df

def main():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    leagues = get_leagues().filter(pl.col("status") == "complete")
    l_id = leagues['league_id'].last()
    test = get_league(league_id=l_id)

    print(test.keys())
    active_leagues = leagues.filter(pl.col("status") != "complete")
    print(active_leagues.columns)

    # for league_id in league_ids:
    #     transactions = get_transactions(league_id=league_id, week=week)
    #     transactions_df, players_df, draft_picks_df = flatten_transactions(transactions, league_id)
    #     save_df_to_gcs(transactions_df, bucket_name, file_name=f"transactions_{league_id}")
    #     save_df_to_gcs(players_df, bucket_name, file_name=f"players_{league_id}")
    #     save_df_to_gcs(draft_picks_df, bucket_name, file_name=f"draft_picks_{league_id}")

        # time.sleep(3)

if __name__ == "__main__":
    main()