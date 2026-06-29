import os
import sys
import time
import requests
from datetime import datetime, timezone
import polars as pl
from dotenv import load_dotenv

load_dotenv()

def fetch_all_players(sport: str = "nfl") -> dict:
    """
    Fetches all players from Sleeper API.
    Returns a dictionary where keys are player_ids and values are player data dicts.
    """
    url = f"https://api.sleeper.app/v1/players/{sport}"
    print(f"☁️ Fetching all {sport} players from Sleeper API...")
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Successfully fetched {len(data):,} players")
        return data
    else:
        raise Exception(f"Failed to fetch players: {response.status_code} - {response.text}")

def save_df_to_gcs(df: pl.DataFrame, bucket_name: str, base_date: str, entity: str):
    """
    Saves DataFrame to GCS in the bronze/sleeper/league/{entity}/incremental structure.
    """

    try:
        # Ensure the bucket name doesn't have gs:// prefix if passed from env var
        clean_bucket = bucket_name.replace('gs://', '')
        full_path = f"gs://{clean_bucket}/bronze/sleeper/league/{entity}/incremental/load_date={base_date}/data.parquet"
        
        df.write_parquet(full_path)
        print(f"✅ Saved {entity} to {full_path}")
        
    except Exception as e:
        print(f"❌ Failed to save to GCS: {e}")
        raise

def main():
    try:
        bucket_name = os.environ.get('GCS_BUCKET_NAME')
        if not bucket_name:
            raise ValueError("GCS_BUCKET_NAME environment variable is not set")

        current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        # 1. Fetch Data
        players_dict = fetch_all_players(sport="nfl")
        print("⚙️ Processing and flattening player data...")
        
        players_list = []
        for p_id, p_data in players_dict.items():
            p_data['player_id'] = p_id
            players_list.append(p_data)

        # 3. Create DataFrame
        df = pl.from_dicts(players_list, infer_schema_length=10000)
        if 'player_id' in df.columns:
            df = df.with_columns(pl.col('player_id').cast(pl.Utf8))

        print(f"📊 Created DataFrame with shape: {df.shape}")

        # 4. Save
        save_df_to_gcs(df, bucket_name, current_date, "players")

    except Exception as e:
        print(f"❌ Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()