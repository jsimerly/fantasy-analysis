import sys
from pathlib import Path
import os
from datetime import datetime, timezone
import time
import json
env_path = sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl

from api.league import get_transactions
from _utils import get_fantasy_leagues 

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
            'leg': txn.get('leg'),  # Can be null for offseason
            'api_week': txn.get('api_week'),  # Track which week we fetched from
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
            for draft_pick in txn['draft_picks']:
                pick_record = {
                    'transaction_id': txn['transaction_id'],
                    'league_id': league_id, 
                    'season': draft_pick.get('season'),
                    'round': draft_pick.get('round'),
                    'roster_id': draft_pick.get('roster_id'),  
                    'previous_owner_id': draft_pick.get('previous_owner_id'), 
                    'owner_id': draft_pick.get('owner_id'), 
                }
                draft_pick_records.append(pick_record)
    
    # Convert to Polars DataFrames
    transactions_df = pl.DataFrame(transaction_records) if transaction_records else None
    players_df = pl.DataFrame(player_records) if player_records else None
    draft_picks_df = pl.DataFrame(draft_pick_records) if draft_pick_records else None
    
    return transactions_df, players_df, draft_picks_df


def get_recent_transactions_incremental(league_id: str, league_status: str, current_leg: int) -> list[dict]:
    """
    Get recent transactions for daily incremental loads.
    Week 1 = offseason transactions
    """
    all_transactions = []
    
    if league_status == 'in_season':
        if current_leg == 0:
            weeks_to_fetch = [1, 2]
        else:
            weeks_to_fetch = [1, current_leg, current_leg + 1]
    
    elif league_status == 'complete':
        weeks_to_fetch = [1]
    
    else:
        weeks_to_fetch = [1]
    
    for week in weeks_to_fetch:
        week_transactions = get_transactions(league_id=league_id, week=week)

        if week_transactions:
            for txn in week_transactions:
                txn['api_week'] = week
            all_transactions.extend(week_transactions)
        
        time.sleep(0.1)
    
    return all_transactions


def save_transactions_to_bronze_deduplicated(
    league_id: str,
    transactions_df: pl.DataFrame,
    players_df: pl.DataFrame | None,
    draft_picks_df: pl.DataFrame | None,
):
    """
    Save transactions to bronze with deduplication.
    Merges with existing data and keeps most recent version.
    """
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    # Add load metadata
    if transactions_df is not None:
        transactions_df = transactions_df.with_columns([
            pl.lit(current_date).alias('load_date'),
        ])
    
    if players_df is not None:
        players_df = players_df.with_columns([
            pl.lit(current_date).alias('load_date')
        ])
    
    if draft_picks_df is not None:
        draft_picks_df = draft_picks_df.with_columns([
            pl.lit(current_date).alias('load_date')
        ])
    
    # Save each table type
    tables = {
        'transactions': transactions_df,
        'transaction_players': players_df,
        'draft_picks': draft_picks_df
    }
    
    for table_name, new_df in tables.items():
        if new_df is None or len(new_df) == 0:
            continue
            
        file_path = (
            f"gs://{bucket_name}/bronze/sleeper/transactions/{table_name}/daily/league_id={league_id}/data.parquet"
        )
        
        # Try to merge with existing data
        try:
            existing_df = pl.read_parquet(file_path)
            combined_df = pl.concat([existing_df, new_df], how="diagonal")
            
            # Deduplicate based on table type
            if table_name == 'transactions':
                combined_df = combined_df.unique(subset=['transaction_id'], keep='last')
            elif table_name == 'transaction_players':
                combined_df = combined_df.unique(
                    subset=['transaction_id', 'player_id', 'action'], 
                    keep='last'
                )
            elif table_name == 'transaction_draft_picks':
                combined_df = combined_df.unique(
                    subset=['transaction_id', 'draft_pick_id'], 
                    keep='last'
                )
            
            print(f"ℹ️  {table_name}: Merged {len(new_df)} new with {len(existing_df)} existing = {len(combined_df)} total")
        except:
            # First load - no existing data
            combined_df = new_df
            print(f"ℹ️  {table_name}: First load with {len(combined_df)} records")
        
        # Sort for easier reading
        if 'created' in combined_df.columns:
            combined_df = combined_df.sort('created')
        
        combined_df.write_parquet(file_path)
        print(f"✅ Saved {table_name} for league {league_id}")


def main():
    """
    Main function to load transactions for all active leagues.
    """
    leagues = get_fantasy_leagues()
    
    # Get active leagues (not complete) from Sleeper
    active_leagues = leagues.filter(
        (pl.col("status") != "complete") & 
        (pl.col("source_system") == "sleeper")
    )
    
    print(f"Found {len(active_leagues)} active leagues to process.\n")
    
    for row in active_leagues.iter_rows(named=True):
        league_id = row['league_id']
        league_status = row['status']
        current_leg = row['leg'] or 0
        league_name = row['league_name']
        
        print(f"📊 Processing: {league_name} (ID: {league_id})")
        print(f"   Status: {league_status}, Current Leg: {current_leg}")
        
        try:
            # Get transactions
            all_transactions = get_recent_transactions_incremental(
                league_id, 
                league_status, 
                current_leg
            )
            
            if not all_transactions:
                print(f"   ℹ️  No transactions found")
                print()
                continue
            
            print(f"   📥 Fetched {len(all_transactions)} transactions")
            
            # Flatten transactions
            transactions_df, players_df, draft_picks_df = flatten_transactions(
                all_transactions, 
                league_id
            )
            
            # Save to bronze with deduplication
            save_transactions_to_bronze_deduplicated(
                league_id=league_id,
                transactions_df=transactions_df,
                players_df=players_df,
                draft_picks_df=draft_picks_df,
            )
            
            print(f"   ✅ Successfully saved transactions for {league_name}")
            

        

if __name__ == "__main__":
    main()