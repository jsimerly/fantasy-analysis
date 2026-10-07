"""silver_fantasy/fact_team_season_strength.py

How good each NFL team was in each season, from two bronze nflverse tables the lake already holds:
``schedules`` (closing point spread, game total, moneylines, scores, starting QBs; 1999-) and
``team_stats`` (per-game offensive EPA, pass attempts, carries, sacks; 1999-). The market-implied
rating is the mean favoritism margin over the season's regular-season games (positive = the team was
favored on average), which prices the QB, the injuries and the schedule better than any box-score
number; the total line is the scoring environment. Realized strength (point differential, offensive
EPA per play, pass rate) and the QB situation (how many starters, the main starter's share) sit next
to it. The dynasty model joins it to a player's team for the season (machine_learning/BACKLOG.md
item 31).

Inputs:  bronze/nflverse/schedules/season=YYYY/*.parquet, bronze/nflverse/team_stats/season=YYYY/*.parquet
Output:  silver/fantasy/fact_team_season_strength/data.parquet
         grain (season, team), team in the CURRENT franchise code (OAK->LV, SD->LAC, STL->LA,
         JAC->JAX) so the season fact, which carries both eras' codes, joins on it after the same map.

Run: ``PYTHONPATH=src python -m silver_fantasy.fact_team_season_strength``
"""
from __future__ import annotations

import io
import os

import polars as pl
from dotenv import load_dotenv
from google.cloud import storage

from silver_fantasy.utils import read_bronze_prefix

SCHEDULES_PREFIX = "bronze/nflverse/schedules/"
TEAM_STATS_PREFIX = "bronze/nflverse/team_stats/"
OUTPUT_PATH = "silver/fantasy/fact_team_season_strength/data.parquet"
TEAM_CODE = {"OAK": "LV", "SD": "LAC", "STL": "LA", "JAC": "JAX", "LAR": "LA"}


def normalize_team(col: str = "team") -> pl.Expr:
    """The current franchise code for every era's abbreviation."""
    return pl.col(col).cast(pl.Utf8).str.strip_chars().replace(TEAM_CODE)


def _implied_prob(ml: pl.Expr) -> pl.Expr:
    """A moneyline's implied win probability (vig included; it is used relatively)."""
    return pl.when(ml.is_null()).then(None).when(ml < 0).then(-ml / (-ml + 100.0)).otherwise(100.0 / (ml + 100.0))


def team_game_sides(sched: pl.DataFrame) -> pl.DataFrame:
    """One row per (season, week, team) of the regular season, from both sides of every game: the
    team's points, the line from its point of view (positive = favored), the total, the implied
    win probability and the starting QB. Future games keep their line and carry null scores."""
    s = sched.filter(pl.col("game_type") == "REG") if "game_type" in sched.columns else sched
    for c in ("spread_line", "total_line", "home_moneyline", "away_moneyline"):
        s = s.with_columns(pl.col(c).cast(pl.Float64, strict=False) if c in s.columns else pl.lit(None, pl.Float64).alias(c))
    for c in ("home_qb_id", "away_qb_id"):
        s = s.with_columns(pl.col(c).cast(pl.Utf8) if c in s.columns else pl.lit(None, pl.Utf8).alias(c))
    s = s.with_columns(pl.col("season").cast(pl.Int64), pl.col("week").cast(pl.Int64),
                       pl.col("home_score").cast(pl.Float64, strict=False), pl.col("away_score").cast(pl.Float64, strict=False))
    home = s.select("season", "week", normalize_team("home_team").alias("team"), normalize_team("away_team").alias("opponent"),
                    pl.col("home_score").alias("pts_for"), pl.col("away_score").alias("pts_against"),
                    pl.col("spread_line").alias("line_margin"), "total_line", _implied_prob(pl.col("home_moneyline")).alias("win_prob"),
                    pl.col("home_qb_id").alias("qb_id"), pl.lit(True).alias("home"))
    away = s.select("season", "week", normalize_team("away_team").alias("team"), normalize_team("home_team").alias("opponent"),
                    pl.col("away_score").alias("pts_for"), pl.col("home_score").alias("pts_against"),
                    (-pl.col("spread_line")).alias("line_margin"), "total_line", _implied_prob(pl.col("away_moneyline")).alias("win_prob"),
                    pl.col("away_qb_id").alias("qb_id"), pl.lit(False).alias("home"))
    return pl.concat([home, away]).sort(["season", "team", "week"])


def season_market_and_record(sides: pl.DataFrame) -> pl.DataFrame:
    """Per (season, team): the market view over the lined games and the record over the played ones."""
    played = sides.filter(pl.col("pts_for").is_not_null() & pl.col("pts_against").is_not_null())
    lined = sides.filter(pl.col("line_margin").is_not_null())
    rec = played.group_by("season", "team").agg(
        pl.len().alias("games_played"),
        (pl.col("pts_for") > pl.col("pts_against")).sum().alias("wins"),
        (pl.col("pts_for") < pl.col("pts_against")).sum().alias("losses"),
        (pl.col("pts_for") == pl.col("pts_against")).sum().alias("ties"),
        pl.col("pts_for").mean().alias("pts_for_pg"), pl.col("pts_against").mean().alias("pts_against_pg"),
        (pl.col("pts_for") - pl.col("pts_against")).mean().alias("point_diff_pg"),
        pl.col("qb_id").drop_nulls().n_unique().alias("n_starting_qbs"),
    ).with_columns(((pl.col("wins") + 0.5 * pl.col("ties")) / pl.col("games_played")).alias("win_pct"))
    qb = (played.filter(pl.col("qb_id").is_not_null()).group_by("season", "team", "qb_id").len()
                .sort(["season", "team", "len"], descending=[False, False, True])
                .group_by("season", "team", maintain_order=True).agg(pl.col("qb_id").first().alias("qb_main_id"),
                                                                     (pl.col("len").first() / pl.col("len").sum()).alias("qb_main_share")))
    mkt = lined.sort("week").group_by("season", "team").agg(
        pl.len().alias("games_lined"),
        pl.col("line_margin").mean().alias("mkt_margin"),
        pl.col("line_margin").first().alias("mkt_margin_wk1"),
        pl.col("line_margin").tail(4).mean().alias("mkt_margin_last4"),
        pl.col("total_line").mean().alias("mkt_total"),
        (pl.col("total_line") / 2 + pl.col("line_margin") / 2).mean().alias("mkt_implied_pts"),
        pl.col("win_prob").mean().alias("mkt_win_prob"),
    )
    keys = sides.select("season", "team").unique()
    return (keys.join(rec, on=["season", "team"], how="left").join(qb, on=["season", "team"], how="left")
                .join(mkt, on=["season", "team"], how="left"))


def season_offense(team_stats: pl.DataFrame) -> pl.DataFrame:
    """Per (season, team): offensive EPA per play, pass rate and plays per game from the per-game team stats."""
    empty = pl.DataFrame(schema={"season": pl.Int64, "team": pl.Utf8, "off_epa_per_play": pl.Float64, "pass_rate": pl.Float64, "plays_pg": pl.Float64})
    t = team_stats.filter(pl.col("season_type") == "REG") if "season_type" in team_stats.columns else team_stats
    if t.height == 0:
        return empty

    def num(c: str) -> pl.Expr:
        return pl.col(c).cast(pl.Float64, strict=False).fill_null(0.0) if c in t.columns else pl.lit(0.0)

    t = t.with_columns(pl.col("season").cast(pl.Int64), normalize_team("team").alias("team"),
                       (num("attempts") + num("carries") + num("sacks_suffered")).alias("_plays"),
                       (num("passing_epa") + num("rushing_epa")).alias("_epa"),
                       (num("attempts") + num("sacks_suffered")).alias("_dropbacks"))
    if "week" in t.columns:
        t = t.unique(["season", "week", "team"], keep="first", maintain_order=True)
    return t.group_by("season", "team").agg(
        (pl.col("_epa").sum() / pl.col("_plays").sum()).alias("off_epa_per_play"),
        (pl.col("_dropbacks").sum() / pl.col("_plays").sum()).alias("pass_rate"),
        (pl.col("_plays").sum() / pl.len()).alias("plays_pg"),
    )


def build_fact_team_season_strength(sched: pl.DataFrame, team_stats: pl.DataFrame) -> pl.DataFrame:
    sides = team_game_sides(sched)
    out = season_market_and_record(sides).join(season_offense(team_stats), on=["season", "team"], how="left")
    prev = out.select((pl.col("season") + 1).alias("season"), "team", pl.col("mkt_margin").alias("lag1_mkt_margin"),
                      pl.col("point_diff_pg").alias("lag1_point_diff_pg"), pl.col("off_epa_per_play").alias("lag1_off_epa_per_play"))
    return out.join(prev, on=["season", "team"], how="left").sort(["season", "team"])


def _bucket() -> str:
    b = os.environ.get("GCS_BUCKET_NAME")
    if not b:
        raise RuntimeError("GCS_BUCKET_NAME not set")
    return b


def save_df_to_gcs(df: pl.DataFrame, bucket_name: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf)
    storage.Client().bucket(bucket_name).blob(OUTPUT_PATH).upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    print(f"Saved fact_team_season_strength ({df.shape[0]} rows) to gs://{bucket_name}/{OUTPUT_PATH}")


def main() -> None:
    load_dotenv()
    bucket = _bucket()
    sched = read_bronze_prefix(bucket, SCHEDULES_PREFIX, dedupe_on=["game_id"])
    ts = read_bronze_prefix(bucket, TEAM_STATS_PREFIX, dedupe_on=["season", "week", "team"])
    fact = build_fact_team_season_strength(sched, ts)
    lined = fact.filter(pl.col("mkt_margin").is_not_null())
    print(f"seasons {fact['season'].min()}-{fact['season'].max()}; rows {fact.height:,}; lined {lined.height:,}; "
          f"EPA filled {fact['off_epa_per_play'].is_not_null().mean():.0%}; moneyline filled {fact['mkt_win_prob'].is_not_null().mean():.0%}")
    top = lined.filter(pl.col("season") == lined["season"].max()).sort("mkt_margin", descending=True)
    print(f"{top['season'][0]} market top: {top.head(3).select('team', 'mkt_margin').to_dicts()} bottom: {top.tail(2).select('team', 'mkt_margin').to_dicts()}")
    save_df_to_gcs(fact, bucket)


if __name__ == "__main__":
    main()
