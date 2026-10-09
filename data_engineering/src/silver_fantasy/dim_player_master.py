from pathlib import Path
from datetime import datetime
import os
import polars as pl
from dotenv import load_dotenv
from silver_fantasy.utils import get_latest_bronze_path

load_dotenv()

PLACEHOLDER_NAME = 'Duplicate Player'   # Sleeper's inactive stand-in rows, which carry a real player's gsis_id


def dedupe_gsis(dim_players: pl.DataFrame) -> pl.DataFrame:
    """One Sleeper player per gsis_id: a placeholder row ("Duplicate Player") never keeps a gsis_id,
    and when two real rows share one the active row keeps it and the other is nulled (the row stays,
    the player universe is the ledger's key). Found by the data-quality suite 2026-10-09: 7 shared
    gsis_ids, six of them placeholders."""
    name = pl.coalesce([pl.col('full_name'), pl.col('first_name') + ' ' + pl.col('last_name')]) if 'full_name' in dim_players.columns         else pl.col('first_name') + ' ' + pl.col('last_name')
    status = pl.col('status') if 'status' in dim_players.columns else pl.lit(None, dtype=pl.Utf8)
    ranked = dim_players.with_columns([
        (name == PLACEHOLDER_NAME).fill_null(False).alias('_placeholder'),
        (status == 'Active').fill_null(False).alias('_active'),
    ])
    # order within a gsis_id: real before placeholder, active before inactive, then the Sleeper id
    ranked = ranked.with_columns(
        pl.when(pl.col('gsis_id').is_null()).then(1)
          .otherwise(pl.col('_placeholder').cast(pl.Int8).mul(2).add(pl.col('_active').not_().cast(pl.Int8)).rank('ordinal').over('gsis_id'))
          .alias('_gsis_rank')
    )
    return ranked.with_columns(
        pl.when(pl.col('_placeholder') | (pl.col('_gsis_rank') > 1)).then(None).otherwise(pl.col('gsis_id')).alias('gsis_id')
    ).drop(['_placeholder', '_active', '_gsis_rank'])


def transform_dim_players_master() -> pl.DataFrame:
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    # A. Sleeper Players (The Base Universe)
    sleeper_path = get_latest_bronze_path(bucket_name, "league/players/incremental", source="sleeper")
    sleeper_df = pl.read_parquet(sleeper_path)

    # B. Fantasy Player IDs (The Bridge)
    ff_ids_path = get_latest_bronze_path(bucket_name, "fantasy_player_ids", source="nflverse")
    ff_ids_df = pl.read_parquet(ff_ids_path)

    # C. NFL Players (The Enrichment)
    try:
        nfl_players_path = get_latest_bronze_path(bucket_name, "players", source="nflverse")
    except ValueError:
        nfl_players_path = get_latest_bronze_path(bucket_name, "nfl_players", source="nflverse")
    
    nfl_players_df = pl.read_parquet(nfl_players_path)
    
    # Sleeper: Ensure ID is string
    sleeper_df = sleeper_df.with_columns(
        pl.col('player_id').cast(pl.Utf8)
    )

    # ID Map: Select relevant ID columns and dedupe
    # We rename columns to avoid collisions before the join
    # The bridge's gsis_id is renamed: Sleeper's own feed carries a sparse ``gsis_id`` too, and a
    # same-named join column lands as ``gsis_id_right`` and is silently ignored -- which left the
    # master with Sleeper's 3,900 ids instead of the bridge's 6,200 (a fifth of the KTC-priced pool
    # mapped; found by the data-quality suite 2026-10-09). The two are coalesced below, bridge first.
    ids_clean = ff_ids_df.select([
        pl.col('sleeper_id').cast(pl.Utf8),
        pl.col('gsis_id').alias('ff_gsis_id'),
        pl.col('ktc_id').cast(pl.Int64),
        pl.col('fantasy_data_id').cast(pl.Int64).alias('fantasydata_id'),
        pl.col('rotoworld_id').cast(pl.Int64),
        pl.col('espn_id').cast(pl.Int64),
        pl.col('yahoo_id').cast(pl.Int64),
        pl.col('pff_id')
    ]).unique(subset=['sleeper_id'], keep='first')

    # NFL Players: Select metadata to enrich with
    nfl_meta_clean = nfl_players_df.select([
        pl.col('gsis_id'),
        pl.col('headshot').alias('nflverse_headshot'),
        pl.col('college_name'),
        pl.col('draft_year').alias('nfl_draft_year'),
        pl.col('draft_round').alias('nfl_draft_round'),
        pl.col('draft_pick').alias('nfl_draft_pick')
    ]).unique(subset=['gsis_id'], keep='first')

    # Step 1: Attach Global IDs to Sleeper Players
    dim_players = sleeper_df.join(
        ids_clean,
        left_on='player_id',
        right_on='sleeper_id',
        how='left'
    )

    dim_players = dim_players.with_columns(
        pl.coalesce([
            pl.col('ff_gsis_id').cast(pl.Utf8).str.strip_chars(),
            pl.col('gsis_id').cast(pl.Utf8).str.strip_chars() if 'gsis_id' in dim_players.columns else pl.lit(None, dtype=pl.Utf8),
        ]).replace({'': None}).alias('gsis_id')
    ).drop('ff_gsis_id')
    dim_players = dedupe_gsis(dim_players)

    # Step 2: Attach NFL Metadata using the newly acquired GSIS ID
    dim_players = dim_players.join(
        nfl_meta_clean,
        on='gsis_id',
        how='left'
    )

    dim_players = dim_players.with_columns([
        pl.col('player_id').alias('player_key'),
        
        pl.coalesce([pl.col('full_name'), pl.col('first_name') + " " + pl.col('last_name')]).alias('display_name'),
        pl.coalesce([
            pl.col('nflverse_headshot'),
            pl.when(pl.col('swish_id').is_not_null())
              .then(
                  pl.lit('https://sleepercdn.com/content/nfl/players/')
                  + pl.col('swish_id').cast(pl.Utf8)
                  + pl.lit('.jpg')
              )
              .otherwise(None)
        ]).alias('avatar_url'),
        pl.coalesce([pl.col('nfl_draft_year'), pl.col('birth_date').str.slice(0, 4).cast(pl.Int64).add(22)]).alias('draft_year'),
        
        # Metadata
        pl.lit('sleeper+nflverse').alias('source_system'),
        pl.lit(datetime.now()).alias('loaded_at')
    ])

    target_cols = [
        # IDs
        'player_key',       # Sleeper ID (Primary)
        'gsis_id',          # NFLVerse/Official
        'ktc_id',           # Valuation
        'espn_id',
        'yahoo_id',
        'fantasydata_id',
        
        # Profile
        'display_name',
        'first_name',
        'last_name',
        'position',
        'team',            
        'age',
        'height',
        'weight',
        'college_name',
        'avatar_url',
        
        # Draft / Experience
        'draft_year',
        'nfl_draft_round',
        'nfl_draft_pick',
        'years_exp',
        
        # Status
        'status',         
        'injury_status',
        
        # Meta
        'source_system',
        'loaded_at'
    ]
    
    existing_cols = [c for c in target_cols if c in dim_players.columns]
    
    return dim_players.select(existing_cols)

def save_df_to_gcs(df: pl.DataFrame, bucket_name: str):
    file_path = f"gs://{bucket_name}/silver/fantasy/dim_players_master/data.parquet"
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved master player table to {file_path}") 
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise

if __name__ == "__main__":
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    df = transform_dim_players_master()
    save_df_to_gcs(df, bucket_name)