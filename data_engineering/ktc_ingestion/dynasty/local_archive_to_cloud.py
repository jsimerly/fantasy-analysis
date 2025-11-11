import os
from datetime import datetime, timezone
import sys
from pathlib import Path
env_path = sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl
from dotenv import load_dotenv
import adbc_driver_postgresql.dbapi as adbc

load_dotenv(env_path)

def get_local_data(conn: adbc.Connection) -> pl.DataFrame:
    query = """
        SELECT
            p.gsis_id,
            p.ktc_id,
            p.display_name,
            p.sleeper_id,
            p.espn_id,
            k.date,
            k.value
        FROM ktc_values k
        LEFT JOIN players p
            ON p.id = k.player_id
        ORDER BY p.id
    """

    df = pl.read_database(query, conn)
    return df

def save_player_to_gcs(df: pl.DataFrame, bucket_name: str, base_date: str):
    file_path = f"gs://{bucket_name}/bronze/ktc/dynasty/local_load/load_date={base_date}/data.parquet"  

    try:
        df.write_parquet(file_path)
        print(f"Saved historic data to GCS: {file_path}")
    except Exception as e:
        print(f"Failed to save historic data to GCS: {e}")
        raise

def main():
    db_url = os.environ['LOCAL_DB_URI']
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date =  datetime.now(timezone.utc).strftime('%Y-%m-%d')

    conn = adbc.connect(db_url)
    df = get_local_data(conn)

    save_player_to_gcs(df, bucket_name, current_date)


if __name__ == "__main__":
    main()


