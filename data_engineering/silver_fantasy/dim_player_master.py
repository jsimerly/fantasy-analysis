from pathlib import Path
from datetime import datetime
import os
import polars as pl
from dotenv import load_dotenv
from utils import get_latest_bronze_path

load_dotenv()

def transform_dim_players_master() -> pl.DataFrame:
    bucket_name = os.environ.get('GCS_BUCKET_NAME')

    # --- 1. Load Data ---
    
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
    
    # --- 2. Clean & Prepare for Join ---
    
    # Sleeper: Ensure ID is string
    sleeper_df = sleeper_df.with_columns(
        pl.col('player_id').cast(pl.Utf8)
    )

    # ID Map: Select relevant ID columns and dedupe
    # We rename columns to avoid collisions before the join
    ids_clean = ff_ids_df.select([
        pl.col('sleeper_id').cast(pl.Utf8),
        pl.col('gsis_id'),
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

    # --- 3. Execute Joins ---
    
    # Step 1: Attach Global IDs to Sleeper Players
    dim_players = sleeper_df.join(
        ids_clean,
        left_on='player_id',
        right_on='sleeper_id',
        how='left'
    )

    # Step 2: Attach NFL Metadata using the newly acquired GSIS ID
    dim_players = dim_players.join(
        nfl_meta_clean,
        on='gsis_id',
        how='left'
    )

    # --- 4. Transformations & Coalescing ---
    
    dim_players = dim_players.with_columns([
        pl.col('player_id').alias('player_key'),
        
        pl.coalesce([pl.col('full_name'), pl.col('first_name') + " " + pl.col('last_name')]).alias('display_name'),
        pl.coalesce([pl.col('nflverse_headshot'), pl.col('swish_id')]).alias('avatar_url'),
        pl.coalesce([pl.col('nfl_draft_year'), pl.col('birth_date').str.slice(0, 4).cast(pl.Int64).add(22)]).alias('draft_year'),
        
        # Metadata
        pl.lit('sleeper+nflverse').alias('source_system'),
        pl.lit(datetime.now()).alias('loaded_at')
    ])

    # --- 5. Final Selection ---
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