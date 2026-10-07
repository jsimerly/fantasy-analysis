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
Outputs: silver/fantasy/fact_team_season_strength/data.parquet
         grain (season, team), team in the CURRENT franchise code (OAK->LV, SD->LAC, STL->LA,
         JAC->JAX) so the season fact, which carries both eras' codes, joins on it after the same map.
         silver/fantasy/fact_team_week_strength/data.parquet
         grain (season, team, week): the same quantities to date after that week (for the in-season
         model's snapshots), this week's own line, last season's rating; every week of the schedule.

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
OUTPUT_WEEK_PATH = "silver/fantasy/fact_team_week_strength/data.parquet"   # the in-season (to-date) view, same job
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


def _weekly_offense(team_stats: pl.DataFrame) -> pl.DataFrame:
    """Per (season, week, team) of the regular season: offensive EPA, plays and dropbacks of that game."""
    t = team_stats.filter(pl.col("season_type") == "REG") if "season_type" in team_stats.columns else team_stats
    if t.height == 0 or "week" not in t.columns:
        return pl.DataFrame(schema={"season": pl.Int64, "week": pl.Int64, "team": pl.Utf8, "_epa": pl.Float64, "_plays": pl.Float64, "_dropbacks": pl.Float64})

    def num(c: str) -> pl.Expr:
        return pl.col(c).cast(pl.Float64, strict=False).fill_null(0.0) if c in t.columns else pl.lit(0.0)

    return (t.with_columns(pl.col("season").cast(pl.Int64), pl.col("week").cast(pl.Int64), normalize_team("team").alias("team"),
                           (num("passing_epa") + num("rushing_epa")).alias("_epa"),
                           (num("attempts") + num("carries") + num("sacks_suffered")).alias("_plays"),
                           (num("attempts") + num("sacks_suffered")).alias("_dropbacks"))
             .unique(["season", "week", "team"], keep="first", maintain_order=True)
             .select("season", "week", "team", "_epa", "_plays", "_dropbacks"))


def build_fact_team_week_strength(sched: pl.DataFrame, team_stats: pl.DataFrame) -> pl.DataFrame:
    """The in-season view of ``build_fact_team_season_strength``: per (season, team, week) what was
    known after that week's games: the to-date market rating (mean closing line over the games played
    so far), to-date total, record and point differential, offensive EPA per play and pass rate to
    date, this week's own line and total (null on a bye), and last season's full-season rating. One
    row for every week of the season's schedule, to-date values carried through byes; weeks with a
    line but no score yet (the current week) keep the line in the rating and the record as it was."""
    sides = team_game_sides(sched)
    if sides.height == 0:
        return pl.DataFrame()
    g = sides.sort(["season", "team", "week"]).with_columns(
        pl.col("pts_for").is_not_null().alias("_played"), pl.col("line_margin").is_not_null().alias("_lined"))
    over = ["season", "team"]
    g = g.with_columns(
        pl.col("_played").cast(pl.Int64).cum_sum().over(over).alias("games_td"),
        pl.col("_lined").cast(pl.Int64).cum_sum().over(over).alias("games_lined_td"),
        pl.col("line_margin").fill_null(0.0).cum_sum().over(over).alias("_cum_margin"),
        pl.col("total_line").fill_null(0.0).cum_sum().over(over).alias("_cum_total"),
        pl.col("win_prob").is_not_null().cast(pl.Int64).cum_sum().over(over).alias("_n_prob"),
        pl.col("win_prob").fill_null(0.0).cum_sum().over(over).alias("_cum_prob"),
        pl.when(pl.col("_played")).then(pl.col("pts_for") - pl.col("pts_against")).otherwise(0.0).cum_sum().over(over).alias("_cum_diff"),
        pl.when(pl.col("_played")).then((pl.col("pts_for") > pl.col("pts_against")).cast(pl.Float64) + 0.5 * (pl.col("pts_for") == pl.col("pts_against")).cast(pl.Float64))
          .otherwise(0.0).cum_sum().over(over).alias("_cum_wins"),
    ).with_columns(
        pl.when(pl.col("games_lined_td") > 0).then(pl.col("_cum_margin") / pl.col("games_lined_td")).otherwise(None).alias("mkt_margin_td"),
        pl.when(pl.col("games_lined_td") > 0).then(pl.col("_cum_total") / pl.col("games_lined_td")).otherwise(None).alias("mkt_total_td"),
        pl.when(pl.col("_n_prob") > 0).then(pl.col("_cum_prob") / pl.col("_n_prob")).otherwise(None).alias("mkt_win_prob_td"),
        pl.when(pl.col("games_td") > 0).then(pl.col("_cum_diff") / pl.col("games_td")).otherwise(None).alias("point_diff_td"),
        pl.when(pl.col("games_td") > 0).then(pl.col("_cum_wins") / pl.col("games_td")).otherwise(None).alias("win_pct_td"),
        pl.col("line_margin").alias("line_this_week"), pl.col("total_line").alias("total_this_week"), pl.col("_played").alias("played_this_week"),
    )
    off = _weekly_offense(team_stats).sort(["season", "team", "week"]).with_columns(
        pl.col("_epa").cum_sum().over(over).alias("_cum_epa"), pl.col("_plays").cum_sum().over(over).alias("_cum_plays"),
        pl.col("_dropbacks").cum_sum().over(over).alias("_cum_db")).with_columns(
        pl.when(pl.col("_cum_plays") > 0).then(pl.col("_cum_epa") / pl.col("_cum_plays")).otherwise(None).alias("off_epa_td"),
        pl.when(pl.col("_cum_plays") > 0).then(pl.col("_cum_db") / pl.col("_cum_plays")).otherwise(None).alias("pass_rate_td"),
    ).select("season", "week", "team", "off_epa_td", "pass_rate_td")
    # every week of the schedule for every team, to-date values carried through byes
    span = sides.group_by("season").agg(pl.col("week").min().alias("_w0"), pl.col("week").max().alias("_w1"))
    grid = (sides.select("season", "team").unique().join(span, on="season")
                 .with_columns(pl.int_ranges("_w0", pl.col("_w1") + 1).alias("week")).explode("week").drop(["_w0", "_w1"]))
    td_cols = ["games_td", "games_lined_td", "mkt_margin_td", "mkt_total_td", "mkt_win_prob_td", "point_diff_td", "win_pct_td"]
    out = (grid.join(g.select(["season", "team", "week"] + td_cols + ["line_this_week", "total_this_week", "played_this_week"]), on=["season", "team", "week"], how="left")
               .join(off, on=["season", "team", "week"], how="left").sort(["season", "team", "week"])
               .with_columns([pl.col(c).fill_null(strategy="forward").over(over) for c in td_cols + ["off_epa_td", "pass_rate_td"]])
               .with_columns(pl.col("games_td").fill_null(0), pl.col("games_lined_td").fill_null(0), pl.col("played_this_week").fill_null(False)))
    prev = (season_market_and_record(sides).select((pl.col("season") + 1).alias("season"), "team", pl.col("mkt_margin").alias("mkt_margin_prev"),
                                                   pl.col("point_diff_pg").alias("point_diff_prev")))
    return out.join(prev, on=["season", "team"], how="left").sort(["season", "team", "week"])


def _bucket() -> str:
    b = os.environ.get("GCS_BUCKET_NAME")
    if not b:
        raise RuntimeError("GCS_BUCKET_NAME not set")
    return b


def save_df_to_gcs(df: pl.DataFrame, bucket_name: str, path: str = OUTPUT_PATH) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf)
    storage.Client().bucket(bucket_name).blob(path).upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    print(f"Saved {path.split('/')[-2]} ({df.shape[0]} rows) to gs://{bucket_name}/{path}")


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
    week = build_fact_team_week_strength(sched, ts)
    print(f"week grid: {week.height:,} rows; to-date rating filled {week['mkt_margin_td'].is_not_null().mean():.0%}; "
          f"EPA to date filled {week['off_epa_td'].is_not_null().mean():.0%}; byes {(~week['played_this_week'] & week['line_this_week'].is_null()).sum():,}")
    save_df_to_gcs(week, bucket, OUTPUT_WEEK_PATH)


if __name__ == "__main__":
    main()
