from google.cloud import storage
import polars as pl

def get_latest_bronze_path(bucket_name: str, entity_path: str, source: str = 'sleeper') -> str:
    """Get the most recent load_date partition for a bronze entity."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"bronze/{source}/{entity_path}"
    
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    # Extract load_dates from paths
    load_dates = []
    for blob in blobs:
        if "load_date=" in blob.name and blob.name.endswith(".parquet"):
            load_date = blob.name.split("load_date=")[1].split("/")[0]
            load_dates.append(load_date)
    
    if not load_dates:
        raise ValueError(f"No data found for {entity_path}")
    
    latest_date = max(load_dates)
    return f"gs://{bucket_name}/bronze/{source}/{entity_path}/load_date={latest_date}/data.parquet"



def merge_full_and_incremental(
    full_df: pl.DataFrame,
    incremental_df: pl.DataFrame,
    join_key: str = 'league_id',
    preserve_columns: list[str] = None
) -> pl.DataFrame:
    preserve_columns = preserve_columns or []
    preserved_data = full_df.select([join_key] + preserve_columns) if preserve_columns else None
    
    merged_df = pl.concat([full_df, incremental_df], how="diagonal").unique(
        subset=[join_key], 
        keep='last'
    )
    
    if preserved_data is not None:
        merged_df = merged_df.drop(preserve_columns)
        merged_df = merged_df.join(preserved_data, on=join_key, how='left')
    
    return merged_df