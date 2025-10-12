import json
import os
from datetime import datetime, timezone
import sys
from pathlib import Path
import time

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import polars as pl
from dotenv import load_dotenv

env_path = sys.path.insert(0, str(Path(__file__).parent.parent))
from _utils import get_bronze_leagues

load_dotenv(dotenv_path=env_path)

def get_transactions(league_id: str, auth_header: str) -> list[dict]:
    transport = RequestsHTTPTransport(
        url='https://sleeper.com/graphql',
        use_json=True,
        headers={
            'Authorization': auth_header,  
            'Content-Type': 'application/json; charset=utf-8' 
        }
    )

    client = Client(transport=transport)
    query = gql("""
    query league_transactions_filtered($leagueId: String!) {
        league_transactions_filtered(
            league_id: $leagueId,
            roster_id_filters: [],
            type_filters: [],
            leg_filters: [],
            status_filters: ["complete"]) {
            adds
            consenter_ids
            created
            creator
            draft_picks
            drops
            league_id
            leg
            metadata
            roster_ids
            settings
            status
            status_updated
            transaction_id
            type
            player_map
            waiver_budget
        }
    }
    """)
    
    variables = {
        'leagueId': league_id
    }
    result = client.execute(query, variable_values=variables)
    
    return result['league_transactions_filtered']

def flatten_transactions(all_transactions: list[dict]) -> tuple[pl.DataFrame, pl.DataFrame | None, pl.DataFrame | None]:
    """
    Flatten transactions into normalized parquet files.
    
    Creates three tables:
    1. transactions - one row per transaction (header info)
    2. transaction_players - one row per player added/dropped
    3. transaction_draft_picks - one row per draft pick traded
    """
    
    transaction_records = []
    player_records = []
    draft_pick_records = []
    
    for txn in all_transactions:
        # Safely get nested dicts
        with open('debug_transactions.json', 'w') as f:
            json.dump(all_transactions, f, indent=2)

        settings = txn.get('settings') or {}
        metadata = txn.get('metadata') or {}
        
        # 1. TRANSACTIONS TABLE - header info only
        transaction_record = {
            'transaction_id': txn['transaction_id'],
            'league_id': txn['league_id'],
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
                    'league_id': txn['league_id'],
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
                    'league_id': txn['league_id'],
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
                    'league_id': txn['league_id'],
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

def save_df_to_gcs(df: pl.DataFrame, bucket_name: str, base_date: str, entity: str):
    file_path = f"gs://{bucket_name}/bronze/sleeper/transactions/{entity}/full_load/load_date={base_date}/data.parquet" 

    try:
        df.write_parquet(file_path)
        
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

def main(auth_header: str):
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date = datetime.now().strftime("%Y-%m-%d")

    leagues = get_bronze_leagues()
    league_ids = leagues.select("league_id").unique().to_series().to_list()

    # Collect all dataframes
    all_transactions = []
    all_players = []
    all_draft_picks = []

    for league_id in league_ids:
        print(f"Processing league: {league_id}...")
        league_transactions = get_transactions(league_id, auth_header)
        transactions_df, players_df, draft_picks_df = flatten_transactions(league_transactions)
        
        # Append to lists
        if transactions_df is not None and len(transactions_df) > 0:
            all_transactions.append(transactions_df)
        if players_df is not None and len(players_df) > 0:
            all_players.append(players_df)
        if draft_picks_df is not None and len(draft_picks_df) > 0:
            all_draft_picks.append(draft_picks_df)
        
        print(f"✅ Processed {league_id}")
        time.sleep(3)  # Rate limiting

    # Combine all leagues
    print("\nCombining all leagues...")
    combined_transactions = pl.concat(all_transactions) if all_transactions else None
    combined_players = pl.concat(all_players) if all_players else None
    combined_draft_picks = pl.concat(all_draft_picks) if all_draft_picks else None

    # Save once with all leagues
    print("\nSaving to GCS...")
    save_df_to_gcs(combined_transactions, bucket_name, current_date, entity="transactions")
    save_df_to_gcs(combined_players, bucket_name, current_date, entity="transaction_players")
    save_df_to_gcs(combined_draft_picks, bucket_name, current_date, entity="draft_picks")
    print(f"✅ Save successful")


if __name__ == "__main__":
    auth_header = os.environ.get("AUTH_TOKEN")

    main(auth_header)

