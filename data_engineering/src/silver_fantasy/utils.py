from concurrent.futures import ThreadPoolExecutor

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
    
    # maintain_order: a deterministic row order downstream (the daily league ingestion
    # iterates this frame; an arbitrary order made a dtype clash appear every other day)
    merged_df = pl.concat([full_df, incremental_df], how="diagonal").unique(
        subset=[join_key],
        keep='last',
        maintain_order=True,
    )
    
    if preserved_data is not None:
        merged_df = merged_df.drop(preserve_columns)
        merged_df = merged_df.join(preserved_data, on=join_key, how='left')
    
    return merged_df


def list_bronze_partitions(bucket_name: str, entity_path: str, source: str = 'sleeper') -> list[tuple[str, str]]:
    """Every ``load_date=`` partition of a bronze entity as ``(load_date, gs_path)``, oldest first.

    Assumes the one-file-per-partition ``data.parquet`` layout the daily incrementals write.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"bronze/{source}/{entity_path}"

    load_dates = set()
    for blob in bucket.list_blobs(prefix=prefix):
        if "load_date=" in blob.name and blob.name.endswith(".parquet"):
            load_dates.add(blob.name.split("load_date=")[1].split("/")[0])

    return [
        (d, f"gs://{bucket_name}/bronze/{source}/{entity_path}/load_date={d}/data.parquet")
        for d in sorted(load_dates)
    ]


def read_latest_incremental_by_key(
    bucket_name: str,
    entity_path: str,
    join_key: str = 'league_id',
    source: str = 'sleeper',
    columns: list[str] | None = None,
    max_workers: int = 16,
) -> pl.DataFrame:
    """The latest observed row per ``join_key`` across EVERY partition of an incremental entity.

    Why not just the newest partition (``get_latest_bronze_path``): the daily Sleeper
    incrementals only write the leagues that were *active* when they ran, so a league drops
    out of the feed the day after it is first observed ``complete``. Reading only the newest
    partition then loses that observation and the dim falls back to the stale full_load row
    (``in_season``) -- which re-activates the league, re-ingests it the next day, and flips it
    back: a two-day oscillation of ``status`` / ``is_active`` (seen 2026-09, and the trigger
    for the sleeper-incremental-league dtype crashes). Overlaying the last row seen for each
    key across all partitions gives the true latest state. Partitions are tiny (one row per
    active league), so reading them all -- in parallel -- is cheap.
    """
    partitions = list_bronze_partitions(bucket_name, entity_path, source=source)
    if not partitions:
        raise ValueError(f"No data found for {entity_path}")

    def _read(item: tuple[str, str]) -> pl.DataFrame:
        load_date, path = item
        df = pl.read_parquet(path)
        if columns is not None:
            df = df.select([c for c in columns if c in df.columns])
        return df.with_columns(pl.lit(load_date).alias('_load_date'))

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        frames = list(pool.map(_read, partitions))

    return (
        # diagonal_relaxed: partitions drift in nullability (a null-only bracket_id is a Null
        # column in one day's file and Int64 in the next); widen rather than fail
        pl.concat(frames, how='diagonal_relaxed')
        .sort('_load_date', maintain_order=True)
        .unique(subset=[join_key], keep='last', maintain_order=True)
        .drop('_load_date')
    )


def read_bronze_prefix(bucket_name: str, prefix: str, dedupe_on: list[str] | None = None,
                       partition_col: str = "partition_season") -> "pl.DataFrame":
    """Read every parquet under ``prefix`` (newest object first) into one frame.

    Partitions are concatenated with ``diagonal_relaxed`` because nflverse changes dtypes and
    columns between seasons. The ``season=YYYY`` folder each row came from is added as
    ``partition_col`` (null when the folder is not a season, e.g. ``season=None``), since the
    newer feeds (depth charts from 2025) carry no season column of their own. With ``dedupe_on``
    the newest file wins for any key more than one file carries.
    """
    import io
    import re

    import polars as pl
    from google.cloud import storage

    client = storage.Client()
    blobs = sorted(
        (b for b in client.list_blobs(bucket_name, prefix=prefix) if b.name.endswith(".parquet")),
        key=lambda b: (b.updated, b.name), reverse=True,
    )
    frames = []
    for b in blobs:
        df = pl.read_parquet(io.BytesIO(b.download_as_bytes()))
        m = re.search(r"/season=(\d{4})/", "/" + b.name)
        frames.append(df.with_columns(pl.lit(int(m.group(1)) if m else None, dtype=pl.Int32).alias(partition_col)))
    if not frames:
        raise FileNotFoundError(f"no parquet objects under gs://{bucket_name}/{prefix}")
    df = pl.concat(frames, how="diagonal_relaxed")
    keys = [c for c in (dedupe_on or []) if c in df.columns]
    if keys:
        before = df.height
        df = df.unique(subset=keys, keep="first", maintain_order=True)
        if df.height != before:
            print(f"  de-duplicated {before - df.height} rows under {prefix} on {keys} (newest file wins)")
    return df


def chain_lineage(leagues_df: pl.DataFrame) -> pl.DataFrame:
    """Fill a null ``league_lineage_id`` by chaining ``previous_league_id`` back to a league that has
    one; a league with no predecessor in the data is its own lineage root.

    ``league_lineage_id`` only comes from the one-time full_load, so every later season (which
    arrives through the incremental feed alone) would otherwise orphan itself from its dynasty.
    Shared by ``dim_leagues_meta`` and ``dim_league_settings_scd2`` (the settings dim lacked it:
    the 2026 leagues' rows carried a null lineage until the data-quality suite caught it, 2026-10-09).
    """
    out = leagues_df
    for _ in range(25):
        if out.filter(pl.col('league_lineage_id').is_null()).height == 0:
            break
        prev = out.select(pl.col('league_id').alias('previous_league_id'), pl.col('league_lineage_id').alias('_prev_lineage'))
        filled = (out.join(prev, on='previous_league_id', how='left')
                     .with_columns(pl.coalesce(['league_lineage_id', '_prev_lineage']).alias('league_lineage_id'))
                     .drop('_prev_lineage'))
        if filled.filter(pl.col('league_lineage_id').is_null()).height == out.filter(pl.col('league_lineage_id').is_null()).height:
            out = filled
            break
        out = filled
    return out.with_columns(pl.coalesce(['league_lineage_id', 'league_id']).alias('league_lineage_id'))

