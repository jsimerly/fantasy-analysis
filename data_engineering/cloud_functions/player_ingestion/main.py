from datetime import datetime
import os
from typing import Any

import pandas as pd
import nfl_data_py as nfl

def fetch_player_ids() -> pd.DataFrame:
    try:
        print("Fetching latest comprehensive player data from nflverse...")
        player_df = nfl.import_players()
        print(f"Successfully fetched {len(player_df)} players.")
        return player_df
    except Exception as e:
        print(f"An error occurred while fetching the data: {e}")
        return pd.DataFrame()
    
def save_df_to_gcs_bronze(df: pd.DataFrame, bucket_name: str, dest_blob_name: str) -> None:
    if df is None or df.empty:
        print("Dataframe is empty, save aborted")
        return None
    
    gcs_path = f"gs://{bucket_name}/{dest_blob_name}"
    try:
        df.to_parquet(gcs_path, index=False)
    except Exception as e:
        print(f"An error occurred while saving to GCS: {e}")

def main(event: Any, context: Any): #
    try:
        bucket_name = os.environ.get('GCS_BUCKET_NAME')
        
        if not bucket_name:
            print("ERROR: GCS_BUCKET_NAME environment variable not set.")
            return 'Deployment Error: Missing GCS_BUCKET_NAME', 500

        player_data = fetch_player_ids()

        if not player_data.empty:
            current_date = datetime.now().strftime('%Y-%m-%d')
            file_path = f'bronze/players/load_date={current_date}/players.parquet'
            
            save_df_to_gcs_bronze(player_data, bucket_name, file_path)
            
            print("Function executed successfully.")
            return 'Success', 200
        else:
            print("No data fetched, function finished.")
            return 'No data fetched', 200
            
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        return 'Internal Server Error', 500