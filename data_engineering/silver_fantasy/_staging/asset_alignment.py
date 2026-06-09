import os
import polars as pl
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------
BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
BUCKET_ROOT = f"gs://{BUCKET_NAME}"

MAIN_STAGING_PATH = f"{BUCKET_ROOT}/silver/fantasy/_staging/asset_values_long"
DEVY_STAGING_PATH = f"{BUCKET_ROOT}/silver/fantasy/_staging/devy_values.parquet"

# -------------------------------------------------------------------------
# HELPER: The Melter (Enforcing Strict Types)
# -------------------------------------------------------------------------
def melt_ktc_values(lf: pl.LazyFrame , market_type):
    return (
        lf
        .select([
            # FORCE DATE TYPE
            pl.col("valuation_date").cast(pl.Date), 
            pl.col("slug").alias("source_id"), 
            pl.col("playerName").alias("asset_name"),
            # Cast inputs to Int64 before unpivoting to ensure uniform schema later
            pl.col("oneqb_value").cast(pl.Int64).alias("value_1QB_Standard"),
            pl.col("oneqb_tep_value").cast(pl.Int64).alias("value_1QB_TEP"),
            pl.col("sf_value").cast(pl.Int64).alias("value_SF_Standard"),
            pl.col("sf_tep_value").cast(pl.Int64).alias("value_SF_TEP"),
        ])
        .unpivot(
            index=["valuation_date", "source_id", "asset_name"],
            variable_name="scoring_code",
            value_name="value"
        )
        .with_columns([
            pl.col("scoring_code").str.split("_").list.get(1).alias("qb_format"),
            pl.col("scoring_code").str.split("_").list.get(2).alias("te_premium"),
            pl.lit("KTC").alias("source_system"),
            pl.lit(market_type).alias("market_type"),
            pl.lit("PLAYER").alias("asset_type")
        ])
        .drop("scoring_code")
        .filter(pl.col("value").is_not_null())
    )

# -------------------------------------------------------------------------
# HELPER: Simple Standardizer (Enforcing Strict Types)
# -------------------------------------------------------------------------
def standardize_simple(lf: pl.LazyFrame , source, market, qb_fmt, te_prem, val_col):
    return (
        lf
        .select([
            # FORCE DATE TYPE
            pl.col("valuation_date").cast(pl.Date),
            pl.col("source_id").cast(pl.Utf8), # Ensure IDs are strings
            pl.col("asset_name"),
            # FORCE INT64 TYPE
            pl.col(val_col).cast(pl.Int64).alias("value"),
            pl.lit(qb_fmt).alias("qb_format"),
            pl.lit(te_prem).alias("te_premium"),
            pl.lit(source).alias("source_system"),
            pl.lit(market).alias("market_type"),
            # UPDATED REGEX: Captures 1st, 2nd, 3rd, 4th, and years (e.g. 2026)
            pl.when(pl.col("asset_name").str.contains("(?i)pick|round|1st|2nd|3rd|4th|20[2-9][0-9]"))
              .then(pl.lit("PICK"))
              .otherwise(pl.lit("PLAYER"))
              .alias("asset_type")
        ])
    )

def main():
    # ---------------------------------------------------------------------
    # 1. KTC DAILY
    # ---------------------------------------------------------------------
    # Hive partitioning usually reads as Date, but we cast inside helper to be safe
    ktc_dyn_daily = pl.scan_parquet(f"{BUCKET_ROOT}/bronze/ktc/dynasty/daily_load/load_date=*/player_data.parquet", hive_partitioning=True).rename({"load_date": "valuation_date"})
    lf_ktc_dyn_long = melt_ktc_values(ktc_dyn_daily, "DYNASTY")

    ktc_red_daily = pl.scan_parquet(f"{BUCKET_ROOT}/bronze/ktc/redraft/daily_load/load_date=*/player_data.parquet", hive_partitioning=True).rename({"load_date": "valuation_date"})
    lf_ktc_red_long = melt_ktc_values(ktc_red_daily, "REDRAFT")

    # ---------------------------------------------------------------------
    # 2. KTC DEVY
    # ---------------------------------------------------------------------
    ktc_devy_daily = pl.scan_parquet(f"{BUCKET_ROOT}/bronze/ktc/devy/daily_load/load_date=*/player_data.parquet", hive_partitioning=True).rename({"load_date": "valuation_date"})
    lf_ktc_devy_long = melt_ktc_values(ktc_devy_daily, "DEVY")

    # ---------------------------------------------------------------------
    # 3. KTC HISTORIC (Schema: Datetime -> Date)
    # ---------------------------------------------------------------------
    ktc_historic = (
        pl.scan_parquet(f"{BUCKET_ROOT}/bronze/ktc/dynasty/local_load/load_date=*/data.parquet")
        .rename({"date": "valuation_date", "ktc_id": "source_id", "display_name": "asset_name"})
    )
    # The helper function will handle the Datetime -> Date cast
    lf_historic_long = standardize_simple(ktc_historic, "KTC", "DYNASTY", "SF", "Standard", "value")

    # ---------------------------------------------------------------------
    # 4. FANTASYCALC (Schema: String -> Date)
    # ---------------------------------------------------------------------
    fc_base = (
        pl.scan_parquet(f"{BUCKET_ROOT}/bronze/fantasycalc/values/daily/load_date=*/data.parquet")
        .rename({"load_date": "valuation_date", "id": "source_id", "name": "asset_name"})
    )
    # The helper function will handle the String -> Date cast
    lf_fc_dyn = standardize_simple(fc_base, "FANTASYCALC", "DYNASTY", "SF", "Standard", "value")
    lf_fc_red = standardize_simple(fc_base, "FANTASYCALC", "REDRAFT", "1QB", "Standard", "redraft_value")

    # ---------------------------------------------------------------------
    # EXECUTION
    # ---------------------------------------------------------------------
    print("Stacking Main Fantasy Data...")
    main_lf = pl.concat([
        lf_ktc_dyn_long,
        lf_ktc_red_long,
        lf_historic_long,
        lf_fc_dyn,
        lf_fc_red
    ], how="diagonal")

    main_lf = main_lf.unique(
        subset=["valuation_date", "source_id", "market_type", "qb_format", "te_premium", "source_system"],
        keep="last"
    )

    print(f"Collecting and Writing Main Data to {MAIN_STAGING_PATH}...")
    # Use collect() + write_parquet to support partition_by safely
    main_lf.collect().write_parquet(
        MAIN_STAGING_PATH,
        partition_by="market_type",
        use_pyarrow=True
    )

    print(f"Writing Devy Data to {DEVY_STAGING_PATH}...")
    lf_ktc_devy_long.collect().write_parquet(DEVY_STAGING_PATH)

    print("Done.")


if __name__ == "__main__":
    main()