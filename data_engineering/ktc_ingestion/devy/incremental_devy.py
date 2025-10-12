import os
from datetime import datetime, timezone
import sys
from pathlib import Path
env_path = sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl
from dotenv import load_dotenv

from utils import (
    get_dynasty_playersArray,
    flatten_player_data,
    set_dtypes,
)

load_dotenv(env_path)

def save_player_to_gcs(df: pl.DataFrame, bucket_name: str,  base_date: str):
    file_path = f"gs://{bucket_name}/bronze/ktc/devy/daily_load/load_date={base_date}/player_data.parquet"  
    
    try:
        df.write_parquet(file_path)
        
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

def main():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date =  datetime.now(timezone.utc).strftime('%Y-%m-%d')

    std_url = "https://keeptradecut.com/devy-rankings"
    playerArray = get_dynasty_playersArray(std_url)
    flattened = [flatten_player_data(player) for player in playerArray]
    df = pl.from_dicts(flattened)
    df = set_dtypes(df)
    save_player_to_gcs(df, bucket_name, current_date)
    print(f"✓ Saved {len(playerArray)} players.")

    # We send an email on failure

if __name__ == "__main__":
    main()