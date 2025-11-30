from pathlib import Path
from datetime import datetime
import os

import polars as pl
from dotenv import load_dotenv

from utils import get_latest_bronze_path, merge_full_and_incremental

load_dotenv()

def transform_teams_data(bucket_name: str) -> pl.DataFrame:
    # league_path = get_latest_bronze_path(bucket_name, "league/") # Get Silver League Path

    team_state_path = get_latest_bronze_path(bucket_name, "rosters/team_state")
    users_path = get_latest_bronze_path(bucket_name, "rosters/users")

    team_state_df = pl.read_parquet(team_state_path)
    users_df = pl.read_parquet(users_path)


    ### This is a consistent team ###
    ## dim_team_persistent
    # Some Key using League Lineage and Team ID to keep teams consistent year to year?
    # League Lineage id
    # League id
    # Team id, may need to make this
    # Owner id
    # Roster id
    # Team Name
    # League Photo


def save_df_to_gcs(df: pl.DataFrame, bucket_name: str):
    file_path = f"gs://{bucket_name}/silver/fantasy/dim_teams/data.parquet"
    
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved leagues table to {file_path}") 
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise


def main() -> None:
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    df = transform_teams_data(bucket_name)
    save_df_to_gcs(df, bucket_name)

if __name__ == '__main__':
    main()
