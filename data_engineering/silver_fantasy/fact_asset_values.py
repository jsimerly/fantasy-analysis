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
DIM_PLAYERS_PATH = f"{BUCKET_ROOT}/silver/fantasy/dim_players_master/data.parquet"

# OUTPUTS
FACT_PATH = f"{BUCKET_ROOT}/silver/fantasy/fact_asset_values_daily"
QUARANTINE_PATH = f"{BUCKET_ROOT}/silver/fantasy/quarantine/unmapped_players.parquet"

COLS_TO_KEEP = ["valuation_date", "player_id", "name", "position",
                "market_type", "qb_format", "te_premium", "source_system", "value"]

PIVOT_INDEX = ["valuation_date", "player_id", "name", "position",
               "market_type", "qb_format", "te_premium"]


def resolve_and_split(df_staging: pl.DataFrame, df_dim: pl.DataFrame):
    """Link staging valuations to master player ids.

    Returns (df_valid, df_unmapped): rows that resolved to a player_id, and the
    de-duplicated set of rows that did not (for the quarantine report).
    """
    # --- Branch A: KTC ---
    # KTC source_id is a slug (e.g. "patrick-mahomes-1234"); extract the trailing
    # numeric id to match the Master 'ktc_id' (Int64).
    ktc_staging = df_staging.filter(pl.col("source_system") == "KTC").with_columns(
        pl.col("source_id")
        .str.split("-")
        .list.get(-1)
        .cast(pl.Int64, strict=False)
        .alias("join_id")
    )
    ktc_joined = (
        ktc_staging
        .join(df_dim, left_on="join_id", right_on="ktc_id", how="left")
        .drop("join_id")
        # Keep the source asset name when the id join misses, so unmapped rows
        # remain identifiable in the quarantine report.
        .with_columns(pl.coalesce(["name", "asset_name"]).alias("name"))
    )

    # --- Branch B: FantasyCalc ---
    # Fallback: join on name since Master has no fantasycalc_id column.
    fc_joined = (
        df_staging.filter(pl.col("source_system") == "FANTASYCALC")
        .join(df_dim, left_on="asset_name", right_on="name", how="left")
        .with_columns(pl.col("asset_name").alias("name"))
    )

    df_all_joined = pl.concat(
        [ktc_joined.select(COLS_TO_KEEP), fc_joined.select(COLS_TO_KEEP)],
        how="vertical",
    )

    df_unmapped = (
        df_all_joined
        .filter(pl.col("player_id").is_null())
        .select(["valuation_date", "source_system", "name", "market_type"])
        .unique(subset=["source_system", "name"])
    )
    df_valid = df_all_joined.filter(pl.col("player_id").is_not_null())
    return df_valid, df_unmapped


def build_fact(df_valid: pl.DataFrame) -> pl.DataFrame:
    """Pivot KTC / FantasyCalc values into wide columns and compute the diff.

    Intended: produce both ``ktc_value`` and ``fc_value`` columns even when one
    source contributed no rows for the current slice.
    """
    df_fact = df_valid.pivot(
        on="source_system",
        index=PIVOT_INDEX,
        values="value",
        aggregate_function="first",
    )

    # Only one source may be present in a given slice; rename what exists and
    # backfill the missing value column so the schema is stable.
    rename_map = {"KTC": "ktc_value", "FANTASYCALC": "fc_value"}
    df_fact = df_fact.rename({k: v for k, v in rename_map.items() if k in df_fact.columns})
    for col in ("ktc_value", "fc_value"):
        if col not in df_fact.columns:
            df_fact = df_fact.with_columns(pl.lit(0).alias(col))

    df_fact = df_fact.with_columns([
        pl.col("ktc_value").fill_null(0),
        pl.col("fc_value").fill_null(0),
    ]).with_columns(
        (pl.col("ktc_value") - pl.col("fc_value")).alias("value_diff")
    )
    return df_fact


def main():
    print(f"Reading Staging Data from {STAGING_PATH}...")
    df_staging = pl.read_parquet(STAGING_PATH).filter(pl.col("asset_type") == "PLAYER")

    print(f"Reading Dimensions from {DIM_PLAYERS_PATH}...")
    df_dim = pl.read_parquet(DIM_PLAYERS_PATH).select([
        pl.col("player_key").alias("player_id"),
        pl.col("display_name").alias("name"),
        "ktc_id",
        "espn_id",
        "position",
    ])

    print("Resolving Player IDs...")
    df_valid, df_unmapped = resolve_and_split(df_staging, df_dim)

    print("Checking for Unmapped Players...")
    if df_unmapped.height > 0:
        print(f"WARNING: Found {df_unmapped.height} unmapped players!")
        print(f"Writing quarantine data to: {QUARANTINE_PATH}")
        df_unmapped.write_parquet(QUARANTINE_PATH)
        print("Writing local report to: unmapped_players_report.csv")
        df_unmapped.write_csv("unmapped_players_report.csv")
    else:
        print("Success: All players mapped 100%.")

    print("Processing Valid Records and Pivoting...")
    df_fact = build_fact(df_valid)

    print(f"Writing Fact Table to {FACT_PATH}...")
    df_fact.write_parquet(FACT_PATH, partition_by=["market_type"], use_pyarrow=True)
    print("Done.")


if __name__ == "__main__":
    main()
