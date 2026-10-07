import os
import polars as pl
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------
BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
BUCKET_ROOT = f"gs://{BUCKET_NAME}"

# INPUTS
STAGING_PATH = f"{BUCKET_ROOT}/silver/fantasy/_staging/asset_values_long"
DIM_PATH = f"{BUCKET_ROOT}/silver/fantasy/dim_players_master/data.parquet"

pl.Config.set_tbl_rows(10)      # Show more rows
pl.Config.set_tbl_cols(20)      # Show more columns
pl.Config.set_fmt_str_lengths(50) # Don't truncate strings too early

def inspect_dataframe(name, df):
    print(f"\n{'='*50}")
    print(f" TABLE: {name}")
    print(f"{'='*50}")
    
    # 1. SCHEMA & TYPES
    print("\n--- SCHEMA ---")
    print(df.schema)

    # 2. NULL COUNTS (Crucial for your Master table)
    print("\n--- NULL COUNTS ---")
    print(df.null_count())

    # 3. SAMPLE DATA
    print("\n--- FIRST 5 ROWS ---")
    print(df.head(5))

    # 4. RANDOM SAMPLE (To catch non-top-row weirdness)
    try:
        print("\n--- RANDOM SAMPLE (5 ROWS) ---")
        print(df.sample(5))
    except:
        pass

# -------------------------------------------------------------------------
# EXECUTION
# -------------------------------------------------------------------------

print(f"Reading Staging: {STAGING_PATH}...")
try:
    df_staging = pl.read_parquet(STAGING_PATH)
    # Filter to just KTC for a clearer look at the join key
    inspect_dataframe("STAGING (KTC Subset)", df_staging.filter(pl.col("source_system") == "KTC"))
except Exception as e:
    print(f"Error reading Staging: {e}")

print(f"\nReading Dimensions: {DIM_PATH}...")
try:
    df_dim = pl.read_parquet(DIM_PATH)
    inspect_dataframe("DIMENSIONS MASTER", df_dim)
    
    # SPECIAL CHECK: What do the Join Keys look like?
    print("\n>>> ID COMPARISON <<<")
    
    # Get a list of staging IDs (Strings? Ints?)
    staging_sample = df_staging.filter(pl.col("source_system") == "KTC").select("source_id").head(3).to_series().to_list()
    
    # Get a list of Master IDs
    # We check a few potential ID columns
    dim_cols = df_dim.columns
    id_cols = [c for c in dim_cols if "id" in c.lower() or "key" in c.lower()]
    
    print(f"Staging 'source_id' Example Values: {staging_sample}")
    print(f"Dimension ID Columns found: {id_cols}")
    
    for col in id_cols:
        val = df_dim.select(col).drop_nulls().head(3).to_series().to_list()
        print(f"  -> Dim '{col}' Examples: {val}")

except Exception as e:
    print(f"Error reading Dimensions: {e}")