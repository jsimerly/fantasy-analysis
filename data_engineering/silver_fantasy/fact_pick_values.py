"""silver_fantasy/fact_pick_values.py

Ownership-independent value time-series for draft picks.

KTC prices picks by *tier* (asset_name like ``2026 Early 1st``). This fact stores
those tiered values as-is, league-agnostic — one row per
``(valuation_date, season, round, tier, market_type, qb_format, te_premium) -> value``.
It deliberately carries NO ownership/franchise: combining a team's picks with these
values (and deciding which tier a pick maps to) is a separate measure-layer job,
``algo(team, pick_data)`` (see ``_pick_projection.py`` and [[fact-pick-values]]).

Two KTC feeds are unioned:
- ``daily_load`` — recent (~252d), names ``Season Tier Round``, 4 value columns
  (1QB/SF x Standard/TEP).
- ``local_load`` — historical archive (``date`` back to 2020), names REVERSED
  ``Tier Season Round``, a single value = dynasty SF Standard, with some dirty
  future-dated rows that are clipped.
On overlap the daily feed wins.
"""
import os
from datetime import datetime

import polars as pl
from dotenv import load_dotenv

load_dotenv()

_ROUND_WORDS = {"1st": 1, "2nd": 2, "3rd": 3, "4th": 4}
# KTC daily value columns -> (qb_format, te_premium)
_KTC_VALUE_COLS = {
    "oneqb_value": ("1QB", "Standard"),
    "oneqb_tep_value": ("1QB", "TEP"),
    "sf_value": ("SF", "Standard"),
    "sf_tep_value": ("SF", "TEP"),
}
_LONG_COLS = ["valuation_date", "season", "round", "tier",
              "market_type", "qb_format", "te_premium", "value"]
_KEY = ["valuation_date", "season", "round", "tier",
        "market_type", "qb_format", "te_premium"]


# -------------------------------------------------------------------------
# PARSERS
# -------------------------------------------------------------------------
def parse_ktc_daily_pick_values(ktc_df: pl.DataFrame) -> pl.DataFrame:
    """Daily KTC feed -> long pick values.

    Keeps only ``playerName`` matching ``<season> <tier> <round>`` and unpivots the
    four value columns into ``(market_type=DYNASTY, qb_format, te_premium, value)``.
    """
    pat = r"^(?P<season>20\d{2})\s+(?P<tier>Early|Mid|Late)\s+(?P<round>1st|2nd|3rd|4th)$"
    present = [c for c in _KTC_VALUE_COLS if c in ktc_df.columns]

    parsed = (
        ktc_df
        .with_columns(pl.col("playerName").str.extract_groups(pat).alias("_g"))
        .filter(pl.col("_g").struct.field("season").is_not_null())
        .with_columns(
            pl.col("_g").struct.field("season").cast(pl.Int64).alias("season"),
            pl.col("_g").struct.field("tier").alias("tier"),
            pl.col("_g").struct.field("round").replace_strict(_ROUND_WORDS, default=None)
              .cast(pl.Int64).alias("round"),
        )
        .select(["valuation_date", "season", "tier", "round"] + present)
    )

    return (
        parsed
        .unpivot(
            index=["valuation_date", "season", "tier", "round"],
            on=present, variable_name="_col", value_name="value",
        )
        .with_columns(
            pl.col("_col").replace_strict({k: v[0] for k, v in _KTC_VALUE_COLS.items()}, default=None).alias("qb_format"),
            pl.col("_col").replace_strict({k: v[1] for k, v in _KTC_VALUE_COLS.items()}, default=None).alias("te_premium"),
            pl.lit("DYNASTY").alias("market_type"),
        )
        .filter(pl.col("value").is_not_null())
        .with_columns(
            pl.col("valuation_date").cast(pl.Utf8),
            pl.col("value").cast(pl.Float64),
        )
        .select(_LONG_COLS)
    )


def parse_ktc_local_pick_values(local_df: pl.DataFrame, max_date: str | None = None) -> pl.DataFrame:
    """Historical KTC archive -> long pick values (dynasty SF Standard only).

    Names are reversed (``<tier> <season> <round>``); the single ``value`` is the
    dynasty SF-Standard value. Rows dated after ``max_date`` (default today) are
    dropped — the archive contains erroneous future-dated rows.
    """
    if max_date is None:
        max_date = datetime.now().strftime("%Y-%m-%d")
    pat = r"^(?P<tier>Early|Mid|Late)\s+(?P<season>20\d{2})\s+(?P<round>1st|2nd|3rd|4th)$"

    return (
        local_df
        .with_columns(pl.col("display_name").str.extract_groups(pat).alias("_g"))
        .filter(pl.col("_g").struct.field("season").is_not_null())
        .with_columns(
            pl.col("date").cast(pl.Date).cast(pl.Utf8).alias("valuation_date"),
            pl.col("_g").struct.field("season").cast(pl.Int64).alias("season"),
            pl.col("_g").struct.field("tier").alias("tier"),
            pl.col("_g").struct.field("round").replace_strict(_ROUND_WORDS, default=None)
              .cast(pl.Int64).alias("round"),
            pl.col("value").cast(pl.Float64).alias("value"),
        )
        .filter(pl.col("valuation_date") <= max_date)  # clip dirty future dates
        .with_columns(
            pl.lit("DYNASTY").alias("market_type"),
            pl.lit("SF").alias("qb_format"),
            pl.lit("Standard").alias("te_premium"),
        )
        .select(_LONG_COLS)
    )


def build_pick_values(daily_df: pl.DataFrame, local_df: pl.DataFrame) -> pl.DataFrame:
    """Union daily + historical pick values; daily wins on overlap.

    Order-independent: take all daily rows plus only the historical rows whose key
    is not already covered by daily, each de-duplicated within source.
    """
    daily = daily_df.unique(subset=_KEY)
    daily_keys = daily.select(_KEY).unique()
    local_only = local_df.unique(subset=_KEY).join(daily_keys, on=_KEY, how="anti")
    return pl.concat([daily, local_only], how="vertical_relaxed").select(_LONG_COLS)


# -------------------------------------------------------------------------
# IO / MAIN
# -------------------------------------------------------------------------
def _read_ktc_prefix(bucket_name: str, prefix: str, suffix: str,
                     add_valuation_date_from_partition: bool) -> pl.DataFrame:
    """Read every parquet under a KTC prefix and concat. For the daily feed the
    valuation_date comes from the ``load_date=`` partition; the archive carries its
    own ``date`` column."""
    from google.cloud import storage

    client = storage.Client()
    names = [
        b.name for b in client.bucket(bucket_name).list_blobs(prefix=prefix)
        if b.name.endswith(suffix)
    ]
    if not names:
        return pl.DataFrame()
    frames = []
    for name in names:
        df = pl.read_parquet(f"gs://{bucket_name}/{name}")
        if add_valuation_date_from_partition and "load_date=" in name:
            load_date = name.split("load_date=")[1].split("/")[0]
            df = df.with_columns(pl.lit(load_date).alias("valuation_date"))
        frames.append(df)
    return pl.concat(frames, how="diagonal_relaxed")


def main():
    bucket_name = os.environ.get("GCS_BUCKET_NAME")

    print("Reading KTC daily + historical archive...")
    daily_raw = _read_ktc_prefix(
        bucket_name, "bronze/ktc/dynasty/daily_load/", "player_data.parquet",
        add_valuation_date_from_partition=True,
    )
    local_raw = _read_ktc_prefix(
        bucket_name, "bronze/ktc/dynasty/local_load/", ".parquet",
        add_valuation_date_from_partition=False,
    )

    print("Parsing pick values...")
    daily = parse_ktc_daily_pick_values(daily_raw)
    local = parse_ktc_local_pick_values(local_raw) if local_raw.height else daily.clear()

    fact = build_pick_values(daily, local).with_columns(
        pl.lit("ktc").alias("source_system"),
        pl.lit(datetime.now()).alias("loaded_at"),
    )

    fact_path = f"gs://{bucket_name}/silver/fantasy/fact_pick_values"
    print(f"Writing {fact.height} rows to {fact_path}...")
    fact.write_parquet(fact_path, partition_by=["market_type"], use_pyarrow=True)
    print("Done.")


if __name__ == "__main__":
    main()
