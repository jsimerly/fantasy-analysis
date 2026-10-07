"""silver_fantasy/fact_player_contract_season.py

What the NFL pays each skill player in each season, from nflverse's ``contracts`` release (Over The
Cap): the contract in force that season, its terms as a share of the cap (inflation-free), how much
of it is left, whether it is a rookie deal or a contract year, the cap number the team actually
carried that year, and where the deal ranks among the position's contracts that season. The NFL's
own forward valuation, for the dynasty model (machine_learning/BACKLOG.md item 31).

Input:   bronze/nflverse/contracts/load_date=YYYY-MM-DD/data.parquet (newest snapshot; one row per
         contract with the player's career cap history and renegotiation history nested).
Output:  silver/fantasy/fact_player_contract_season/data.parquet
         grain (gsis_id, season) for QB / RB / WR / TE with a known gsis id; a season appears only
         when a contract covers it. Leakage rule for the model: a contract counts from the year it
         was signed (``year_signed <= season``), never earlier; the table has no signing date, so an
         offseason extension first shows in the row of the season it was signed in.

Run: ``PYTHONPATH=src python -m silver_fantasy.fact_player_contract_season``
"""
from __future__ import annotations

import io
import os

import polars as pl
from dotenv import load_dotenv
from google.cloud import storage

CONTRACTS_PREFIX = "bronze/nflverse/contracts/"
OUTPUT_PATH = "silver/fantasy/fact_player_contract_season/data.parquet"
SKILL = ["QB", "RB", "WR", "TE"]
ROOKIE_TYPES = ["Drafted", "UDFA"]
FIRST_SEASON = 1994   # the cap era; earlier contracts are a handful of rows
_TYPES_SCHEMA = {"otc_id": pl.Int64, "year_signed": pl.Int64, "_apy_r": pl.Float64, "contract_type": pl.Utf8}
_CAP_SCHEMA = {"gsis_id": pl.Utf8, "year": pl.Int64, "cap_pct_season": pl.Float64, "guaranteed_salary_season": pl.Float64, "cash_paid_season": pl.Float64}


def _contract_types(contracts: pl.DataFrame) -> pl.DataFrame:
    """(otc_id, year_signed, apy) -> contract_type from the nested renegotiation history."""
    if "contract_history" not in contracts.columns:
        return pl.DataFrame(schema=_TYPES_SCHEMA)
    h = contracts.select("otc_id", "contract_history").explode("contract_history").unnest("contract_history")
    if "contract_type" not in h.columns:
        return pl.DataFrame(schema=_TYPES_SCHEMA)
    return (h.select(pl.col("otc_id").cast(pl.Int64), pl.col("year_signed").cast(pl.Int64), pl.col("apy").cast(pl.Float64).round(3).alias("_apy_r"),
                     pl.col("contract_type").cast(pl.Utf8))
             .drop_nulls(["otc_id", "year_signed"]).unique(["otc_id", "year_signed", "_apy_r"], keep="first", maintain_order=True))


def _career_cap(contracts: pl.DataFrame) -> pl.DataFrame:
    """(gsis_id, year) -> the cap percent, guaranteed salary and cash the player's deals carried that
    year (summed across teams when he moved mid-year); from the nested career history, which every
    contract row of a player repeats."""
    if "season_history" not in contracts.columns:
        return pl.DataFrame(schema=_CAP_SCHEMA)
    one = contracts.filter(pl.col("gsis_id").is_not_null()).unique("gsis_id", keep="first", maintain_order=True).select("gsis_id", "season_history")
    h = one.explode("season_history").unnest("season_history")
    if "year" not in h.columns:
        return pl.DataFrame(schema=_CAP_SCHEMA)

    def num(c: str) -> pl.Expr:
        return pl.col(c).cast(pl.Float64, strict=False) if c in h.columns else pl.lit(None, pl.Float64)

    return (h.with_columns(pl.col("year").cast(pl.Utf8).str.extract(r"(\d{4})").cast(pl.Int64, strict=False))
             .drop_nulls("year")
             .group_by("gsis_id", "year").agg(num("cap_percent").sum().alias("cap_pct_season"), num("guaranteed_salary").sum().alias("guaranteed_salary_season"),
                                              num("cash_paid").sum().alias("cash_paid_season")))


def build_fact_player_contract_season(contracts: pl.DataFrame) -> pl.DataFrame:
    c = contracts.filter(pl.col("position").is_in(SKILL) & pl.col("gsis_id").is_not_null())
    c = c.with_columns(pl.col("gsis_id").cast(pl.Utf8).str.strip_chars(), pl.col("year_signed").cast(pl.Int64), pl.col("years").cast(pl.Int64),
                       pl.col("otc_id").cast(pl.Int64), pl.col("apy").cast(pl.Float64), pl.col("value").cast(pl.Float64),
                       pl.col("guaranteed").cast(pl.Float64), pl.col("apy_cap_pct").cast(pl.Float64),
                       (pl.col("draft_year").cast(pl.Int64, strict=False) if "draft_year" in c.columns else pl.lit(None, pl.Int64)).alias("draft_year"),
                       (pl.col("is_active").cast(pl.Boolean, strict=False) if "is_active" in c.columns else pl.lit(None, pl.Boolean)).alias("is_active"))
    c = c.filter((pl.col("year_signed") >= FIRST_SEASON) & (pl.col("years") >= 1))
    c = c.with_columns(pl.col("apy").round(3).alias("_apy_r")).join(_contract_types(contracts), on=["otc_id", "year_signed", "_apy_r"], how="left")
    c = c.with_columns(
        (pl.col("contract_type").is_in(ROOKIE_TYPES) | (pl.col("year_signed") == pl.col("draft_year"))).fill_null(False).alias("is_rookie_deal"),
        pl.when(pl.col("apy") > 0).then(pl.col("guaranteed") * pl.col("apy_cap_pct") / pl.col("apy")).otherwise(None).alias("guaranteed_cap_pct"),
    )
    # how many deals the player had signed by each year (the deal itself counts)
    c = c.sort(["gsis_id", "year_signed", "apy"]).with_columns(pl.int_range(1, pl.len() + 1).over("gsis_id").alias("n_contracts_signed"))
    # one row per season the contract covers; when deals overlap the richer one is the one in force
    seasons = (c.with_columns(pl.int_ranges(pl.col("year_signed"), pl.col("year_signed") + pl.col("years")).alias("season")).explode("season")
                 .sort(["gsis_id", "season", "apy", "year_signed"], descending=[False, False, True, True])
                 .unique(["gsis_id", "season"], keep="first", maintain_order=True))
    seasons = seasons.with_columns(
        (pl.col("year_signed") + pl.col("years") - 1 - pl.col("season")).alias("years_left"),
        (pl.col("season") - pl.col("year_signed")).alias("contract_age"),
    ).with_columns((pl.col("years_left") == 0).alias("contract_year"))
    cap = _career_cap(contracts)
    seasons = (seasons.join(cap.rename({"year": "season"}), on=["gsis_id", "season"], how="left")
                      .join(cap.select("gsis_id", (pl.col("year") - 1).alias("season"), pl.col("cap_pct_season").alias("cap_pct_next")),
                            on=["gsis_id", "season"], how="left"))
    seasons = seasons.with_columns(
        pl.col("apy_cap_pct").rank(method="min", descending=True).over("season", "position").alias("apy_cap_pct_pos_rank"),
        (1.0 - (pl.col("apy_cap_pct").rank(method="average", descending=True).over("season", "position") - 1) / pl.len().over("season", "position"))
          .alias("apy_cap_pct_pos_pctl"),
    )
    cols = ["gsis_id", "season", "position", "year_signed", "contract_type", "is_rookie_deal", "years", "years_left", "contract_year", "contract_age",
            "apy", "value", "apy_cap_pct", "guaranteed", "guaranteed_cap_pct", "cap_pct_season", "guaranteed_salary_season", "cash_paid_season", "cap_pct_next",
            "apy_cap_pct_pos_rank", "apy_cap_pct_pos_pctl", "n_contracts_signed", "is_active"]
    return seasons.select(cols).rename({"years": "contract_years"}).sort(["gsis_id", "season"])


def _bucket() -> str:
    b = os.environ.get("GCS_BUCKET_NAME")
    if not b:
        raise RuntimeError("GCS_BUCKET_NAME not set")
    return b


def latest_contracts_path(bucket_name: str) -> str:
    """The newest snapshot under the contracts prefix (load_date partitions sort by name)."""
    names = sorted(b.name for b in storage.Client().list_blobs(bucket_name, prefix=CONTRACTS_PREFIX) if b.name.endswith(".parquet"))
    if not names:
        raise FileNotFoundError(f"no contracts snapshot under gs://{bucket_name}/{CONTRACTS_PREFIX} (run nflverse-daily on a Tuesday or FORCE_RUN=true)")
    return f"gs://{bucket_name}/{names[-1]}"


def save_df_to_gcs(df: pl.DataFrame, bucket_name: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf)
    storage.Client().bucket(bucket_name).blob(OUTPUT_PATH).upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    print(f"Saved fact_player_contract_season ({df.shape[0]} rows) to gs://{bucket_name}/{OUTPUT_PATH}")


def main() -> None:
    load_dotenv()
    bucket = _bucket()
    path = latest_contracts_path(bucket)
    contracts = pl.read_parquet(path)
    print(f"contracts snapshot {path.rsplit('/', 2)[-2]}: {contracts.height:,} contracts")
    fact = build_fact_player_contract_season(contracts)
    print(f"seasons {fact['season'].min()}-{fact['season'].max()}; rows {fact.height:,}; players {fact['gsis_id'].n_unique():,}; "
          f"rookie-deal rows {fact['is_rookie_deal'].mean():.0%}; cap number filled {fact['cap_pct_season'].is_not_null().mean():.0%}; "
          f"type known {fact['contract_type'].is_not_null().mean():.0%}")
    save_df_to_gcs(fact, bucket)


if __name__ == "__main__":
    main()
