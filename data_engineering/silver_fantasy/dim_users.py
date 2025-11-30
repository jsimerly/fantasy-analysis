from pathlib import Path
from datetime import datetime
import os

import polars as pl
import pandas as pd # Used for reliable URL fetching
from dotenv import load_dotenv

from utils import get_latest_bronze_path, merge_full_and_incremental

load_dotenv()

def get_google_sheet_data(sheet_id: str, gid: str = "0") -> pl.DataFrame:
    """
    Fetches a public Google Sheet as a Polars DataFrame.
    """
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    
    try:
        # We use Pandas here as an intermediate because it handles HTTPS/Redirection 
        # very robustly compared to raw Polars in some environments.
        pdf = pd.read_csv(url)
        df = pl.from_pandas(pdf)
        
        if "user_id" in df.columns:
            df = df.with_columns(pl.col("user_id").cast(pl.Utf8))
            
        print(f"✅ Loaded {len(df)} rows from Google Sheet")
        return df
    except Exception as e:
        print(f"⚠️ Failed to load Google Sheet: {e}")
        # Return empty DataFrame with expected schema to prevent crash
        return pl.DataFrame(
            {"user_id": [], "real_name": []}, 
            schema={"user_id": pl.Utf8, "real_name": pl.Utf8}
        )

def transform_dim_users() -> pl.DataFrame:
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    # --- 1. Load Sleeper User Data (The "Automated" Source) ---
    users_path = get_latest_bronze_path(bucket_name, "rosters/users/weekly")
    raw_users_df = pl.read_parquet(users_path)
    
    dim_users = raw_users_df.unique(subset=['user_id'], keep='last')

    # --- 2. Load Manual Enrichment (Google Sheet) ---
    SHEET_ID = "1o0ToVCGCPZRuZzs_pGjUvtVP_IXNdyrFIaXHJ39xslA"
    manual_map_df = get_google_sheet_data(SHEET_ID)

    # --- 3. Merge Automated + Manual ---
    dim_users = dim_users.join(
        manual_map_df,
        on='user_id',
        how='left'
    )

    # --- 4. Final Transformations ---
    dim_users = dim_users.with_columns([
        pl.coalesce([
            pl.col('real_name') if 'real_name' in dim_users.columns else pl.lit(None),
            pl.col('display_name'), 
            pl.lit('Unknown')
        ]).alias('primary_name'),
        
        pl.lit('sleeper').alias('source_system'),
        pl.lit(datetime.now()).alias('loaded_at')
    ])

    # Select standard columns
    target_cols = [
        'user_id',
        'display_name', 
        'real_name',  
        'primary_name',
        'avatar',
        'source_system',
        'loaded_at'
    ]

    return dim_users.select([c for c in target_cols if c in dim_users.columns])

def save_df_to_gcs(df: pl.DataFrame, bucket_name: str):
    file_path = f"gs://{bucket_name}/silver/fantasy/dim_users/data.parquet"
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved global dim_users to {file_path}") 
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

if __name__ == "__main__":
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    df = transform_dim_users()
    save_df_to_gcs(df, bucket_name)