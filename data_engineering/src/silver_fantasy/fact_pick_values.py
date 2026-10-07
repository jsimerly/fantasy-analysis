"""silver_fantasy/fact_pick_values.py

Ownership-independent value time-series for draft picks.

KTC prices picks by *tier* (asset_name like ``2026 Early 1st``). This fact stores
those tiered values as-is, league-agnostic — one row per
``(valuation_date, season, round, tier, market_type, qb_format, te_premium) -> value``.
It deliberately carries NO ownership/franchise: combining a team's picks with these
values (and deciding which tier a pick maps to) is a separate measure-layer job,
``algo(team, pick_data)`` (see ``_pick_projection.py`` and [[fact-pick-values]]).

KTC feeds are unioned in precedence order **daily > full_load > local_load**:
- ``daily_load`` — recent (~252d), names ``Season Tier Round``, 4 value columns
  (1QB/SF x Standard/TEP).
- ``full_load`` — per-player/pick historical scrape (``position == "RDP"`` rows are
  picks; ``player_name`` is ``Season Tier Round`` like the daily feed). Continuous
  daily ``sf_value``/``one_qb_value`` back to ~2020, but only for picks that still
  EXIST at scrape time (2026+ future picks).
- ``local_load`` — older, lower-quality archive (names REVERSED ``Tier Season Round``,
  SF Standard only, dirty future dates clipped). Kept as a FALLBACK: it's the only
  source for CONSUMED historical picks (2022-2025 tiers) that full_load no longer
  carries.
Higher-precedence feeds win on overlapping keys.

FantasyCalc pick values are also folded in (``source_system='fantasycalc'``), stored
at the round level (``tier='NA'``) since FC has no Early/Mid/Late tiers.
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


_FC_ROUND_WORDS = {"1st": 1, "2nd": 2, "3rd": 3, "4th": 4}


def parse_fc_pick_values(fc_df: pl.DataFrame) -> pl.DataFrame:
    """FantasyCalc pick rows -> long pick values at the ROUND level.

    FC marks picks with ``position == 'PICK'`` and a round-generic name like
    ``2026 1st`` (it has no Early/Mid/Late tiers, so ``tier='NA'``). The exact-slot
    rows (``2026 Pick 1.09``) are ignored here. FC's single ``value`` is its dynasty
    value -> recorded as DYNASTY / SF / Standard.
    """
    pat = r"^(?P<season>20\d{2})\s+(?P<round>1st|2nd|3rd|4th)$"
    return (
        fc_df
        .filter(pl.col("position") == "PICK")
        .with_columns(pl.col("name").str.extract_groups(pat).alias("_g"))
        .filter(pl.col("_g").struct.field("season").is_not_null())
        .with_columns(
            pl.col("valuation_date").cast(pl.Utf8),
            pl.col("_g").struct.field("season").cast(pl.Int64).alias("season"),
            pl.col("_g").struct.field("round").replace_strict(_FC_ROUND_WORDS, default=None)
              .cast(pl.Int64).alias("round"),
            pl.lit("NA").alias("tier"),
            pl.lit("DYNASTY").alias("market_type"),
            pl.lit("SF").alias("qb_format"),
            pl.lit("Standard").alias("te_premium"),
            pl.col("value").cast(pl.Float64),
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


def _read_fullload_picks(bucket_name: str) -> pl.DataFrame:
    """Per-player full_load scrape -> just the PICK rows, shaped like the daily feed
    so ``parse_ktc_daily_pick_values`` can parse them. ``position == 'RDP'`` selects
    picks; rename to the daily column names (TEP columns are absent -> SF+1QB Standard).

    Uses a single lazy glob scan (``extra_columns='ignore'`` — the per-player files have
    heterogeneous rank columns) rather than reading ~499 files one-by-one."""
    glob = f"gs://{bucket_name}/bronze/ktc/dynasty/full_load/load_date=*/*.parquet"
    try:
        lf = pl.scan_parquet(glob, extra_columns="ignore")
    except Exception:
        return pl.DataFrame()
    return (
        lf.filter(pl.col("position") == "RDP")
        .select("player_name", "ranking_date", "sf_value", "one_qb_value")
        .rename({"player_name": "playerName", "ranking_date": "valuation_date",
                 "one_qb_value": "oneqb_value"})
        .collect()
    )


def main():
    bucket_name = os.environ.get("GCS_BUCKET_NAME")

    print("Reading KTC daily + full_load + local archive...")
    daily_raw = _read_ktc_prefix(
        bucket_name, "bronze/ktc/dynasty/daily_load/", "player_data.parquet",
        add_valuation_date_from_partition=True,
    )
    full_raw = _read_fullload_picks(bucket_name)
    local_raw = _read_ktc_prefix(
        bucket_name, "bronze/ktc/dynasty/local_load/", ".parquet",
        add_valuation_date_from_partition=False,
    )

    print("Parsing pick values...")
    daily = parse_ktc_daily_pick_values(daily_raw)
    full_hist = parse_ktc_daily_pick_values(full_raw) if full_raw.height else daily.clear()
    local_hist = parse_ktc_local_pick_values(local_raw) if local_raw.height else daily.clear()

    # precedence: daily > full_load > local_load (local fills CONSUMED picks full_load lacks)
    ktc = build_pick_values(build_pick_values(daily, full_hist), local_hist).with_columns(
        pl.lit("ktc").alias("source_system"))

    print("Reading + parsing FantasyCalc picks...")
    fc_raw = _read_ktc_prefix(
        bucket_name, "bronze/fantasycalc/values/daily/", ".parquet",
        add_valuation_date_from_partition=True,
    )
    fc = parse_fc_pick_values(fc_raw).with_columns(pl.lit("fantasycalc").alias("source_system")) \
        if fc_raw.height else ktc.clear()

    fact = pl.concat([ktc, fc], how="vertical_relaxed").with_columns(
        pl.lit(datetime.now()).alias("loaded_at"))

    fact_path = f"gs://{bucket_name}/silver/fantasy/fact_pick_values"
    print(f"Writing {fact.height} rows to {fact_path} "
          f"(ktc={ktc.height}, fantasycalc={fc.height})...")
    fact.write_parquet(fact_path, partition_by=["market_type"], use_pyarrow=True)
    print("Done.")


if __name__ == "__main__":
    main()
