"""silver_fantasy/fact_college_player_season.py

College production per (player, season) from the CFBD bronze datasets, plus the crosswalk from
CFBD athlete ids to the NFL side, so the dynasty model can see what a rookie did in college
(machine_learning/BACKLOG.md items 17 and 24).

Inputs (``cfbd_ingestion.backfill``): bronze/cfbd/{player_season_stats, player_usage, rosters,
team_sp, team_season_stats, draft_picks}/season=YYYY/data.parquet.

Outputs:
  silver/fantasy/fact_college_player_season/data.parquet
      grain (cfbd_id, season): team, conference, position, class_year, the box score (receiving /
      rushing / passing), usage shares (share of the team's plays), the team's SP+ rating and rank,
      team volume (plays, yards), and the shares a dynasty analyst actually uses:
      yards_share, td_share, dominator (their mean), yards_per_team_play; per player: seasons
      so far, breakout flags (first season with dominator >= 0.20 / usage >= 0.20).
  silver/fantasy/dim_college_crosswalk/data.parquet
      cfbd_id -> gsis_id (via CFBD draft picks: draft year + overall pick against nflverse
      fantasy_player_ids; name + position as the fallback), with draft year, round, overall pick,
      and the player's birth date where known, so breakout AGE can be computed downstream.

Run: ``PYTHONPATH=src uv run python -m silver_fantasy.fact_college_player_season``
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timezone

import polars as pl
from dotenv import load_dotenv

from silver_fantasy.utils import read_bronze_prefix

load_dotenv()

BUCKET = os.environ.get("GCS_BUCKET_NAME", "nfl-data-bronze")
OUT_FACT = "silver/fantasy/fact_college_player_season/data.parquet"
OUT_XWALK = "silver/fantasy/dim_college_crosswalk/data.parquet"
SKILL = ["QB", "RB", "WR", "TE"]
# CFBD stat (category, statType) -> fact column
STATS = {
    ("receiving", "REC"): "rec", ("receiving", "YDS"): "rec_yds", ("receiving", "TD"): "rec_td",
    ("rushing", "CAR"): "rush_att", ("rushing", "YDS"): "rush_yds", ("rushing", "TD"): "rush_td",
    ("passing", "ATT"): "pass_att", ("passing", "COMPLETIONS"): "pass_cmp", ("passing", "YDS"): "pass_yds", ("passing", "TD"): "pass_td", ("passing", "INT"): "pass_int",
}
TEAM_STATS = {"passAttempts": "team_pass_att", "rushingAttempts": "team_rush_att", "totalYards": "team_yards", "netPassingYards": "team_pass_yds", "rushingYards": "team_rush_yds",
              "passingTDs": "team_pass_td", "rushingTDs": "team_rush_td", "games": "team_games"}


def _norm(col: str) -> pl.Expr:
    return pl.col(col).cast(pl.Utf8).str.to_lowercase().str.replace_all(r"[^a-z ]", "").str.replace_all(r"\b(jr|sr|ii|iii|iv)\b", "").str.strip_chars().str.replace_all(r"\s+", " ")


def wide_stats(stats: pl.DataFrame) -> pl.DataFrame:
    """The long (player, category, statType, stat) feed to one row per (cfbd_id, season)."""
    s = stats.with_columns(pl.col("playerId").cast(pl.Utf8).alias("cfbd_id"), pl.col("season").cast(pl.Int64),
                           pl.col("stat").cast(pl.Utf8).str.replace_all(",", "").cast(pl.Float64, strict=False).alias("value"))
    keep = s.filter(pl.struct(["category", "statType"]).map_elements(lambda r: (r["category"], r["statType"]) in STATS, return_dtype=pl.Boolean))
    keep = keep.with_columns(pl.struct(["category", "statType"]).map_elements(lambda r: STATS[(r["category"], r["statType"])], return_dtype=pl.Utf8).alias("col"))
    base = keep.group_by(["cfbd_id", "season"]).agg(pl.col("player").first().alias("name"), pl.col("team").first(), pl.col("conference").first(), pl.col("position").first())
    wide = keep.pivot(on="col", index=["cfbd_id", "season"], values="value", aggregate_function="sum")
    for c in STATS.values():
        if c not in wide.columns:
            wide = wide.with_columns(pl.lit(None, pl.Float64).alias(c))
    return base.join(wide, on=["cfbd_id", "season"], how="left")


def team_volume(team_stats: pl.DataFrame) -> pl.DataFrame:
    """Team season totals (plays, yards, touchdowns, games) for the shares."""
    t = team_stats.filter(pl.col("statName").is_in(list(TEAM_STATS))).with_columns(pl.col("statValue").cast(pl.Float64, strict=False), pl.col("season").cast(pl.Int64))
    w = t.pivot(on="statName", index=["team", "season"], values="statValue", aggregate_function="first")
    w = w.rename({k: v for k, v in TEAM_STATS.items() if k in w.columns})
    for v in TEAM_STATS.values():
        if v not in w.columns:
            w = w.with_columns(pl.lit(None, pl.Float64).alias(v))
    return w.with_columns((pl.col("team_pass_att").fill_null(0) + pl.col("team_rush_att").fill_null(0)).alias("team_plays"),
                          (pl.col("team_pass_td").fill_null(0) + pl.col("team_rush_td").fill_null(0)).alias("team_td"))


def build_fact(stats: pl.DataFrame, usage: pl.DataFrame, rosters: pl.DataFrame, sp: pl.DataFrame, team_stats: pl.DataFrame) -> pl.DataFrame:
    f = wide_stats(stats)
    if usage.height:
        u = usage.with_columns(pl.col("id").cast(pl.Utf8).alias("cfbd_id"), pl.col("season").cast(pl.Int64))
        ucols = [c for c in u.columns if c.startswith("usage_")]
        f = f.join(u.select(["cfbd_id", "season"] + ucols).unique(["cfbd_id", "season"]), on=["cfbd_id", "season"], how="left")
    if rosters.height:
        r = rosters.with_columns(pl.col("id").cast(pl.Utf8).alias("cfbd_id"), pl.col("season").cast(pl.Int64))
        rcols = [c for c in ("year", "height", "weight", "homeState") if c in r.columns]
        f = f.join(r.select(["cfbd_id", "season"] + rcols).unique(["cfbd_id", "season"]).rename({"year": "class_year"} if "year" in rcols else {}), on=["cfbd_id", "season"], how="left")
    if sp.height:
        s = sp.with_columns(pl.col("season").cast(pl.Int64)).select("team", "season", pl.col("rating").cast(pl.Float64).alias("team_sp"), pl.col("ranking").cast(pl.Int64).alias("team_sp_rank"),
                                                                    pl.col("offense_rating").cast(pl.Float64).alias("team_sp_off") if "offense_rating" in sp.columns else pl.lit(None, pl.Float64).alias("team_sp_off"))
        f = f.join(s.unique(["team", "season"]), on=["team", "season"], how="left")
    if team_stats.height:
        f = f.join(team_volume(team_stats).unique(["team", "season"]), on=["team", "season"], how="left")
    else:
        for v in list(TEAM_STATS.values()) + ["team_plays", "team_td"]:
            f = f.with_columns(pl.lit(None, pl.Float64).alias(v))
    yds = pl.col("rec_yds").fill_null(0) + pl.col("rush_yds").fill_null(0)
    tds = pl.col("rec_td").fill_null(0) + pl.col("rush_td").fill_null(0)
    f = f.with_columns(
        (yds / pl.col("team_yards")).alias("yards_share"), (tds / pl.col("team_td")).alias("td_share"), (yds / pl.col("team_plays")).alias("yards_per_team_play"),
        ((pl.col("rec").fill_null(0) + pl.col("rush_att").fill_null(0)) / pl.col("team_plays")).alias("touch_share"))
    f = f.with_columns(((pl.col("yards_share") + pl.col("td_share")) / 2).alias("dominator"))
    f = f.sort(["cfbd_id", "season"]).with_columns(
        pl.col("season").cum_count().over("cfbd_id").alias("college_season_no"),
        pl.col("season").min().over("cfbd_id").alias("first_season"), pl.col("season").max().over("cfbd_id").alias("last_season"),
        pl.when(pl.col("dominator") >= 0.20).then(pl.col("season")).otherwise(None).min().over("cfbd_id").alias("breakout_season_dom"),
        pl.when(pl.col("usage_overall") >= 0.20).then(pl.col("season")).otherwise(None).min().over("cfbd_id").alias("breakout_season_usage") if "usage_overall" in f.columns else pl.lit(None, pl.Int64).alias("breakout_season_usage"))
    return f.with_columns(pl.lit(datetime.now(timezone.utc)).alias("loaded_at"))


def build_crosswalk(draft_picks: pl.DataFrame, ids: pl.DataFrame) -> pl.DataFrame:
    """CFBD athlete id -> gsis id. By (draft year, overall pick) against nflverse fantasy_player_ids
    first; by normalised name + position where the pick does not match (undrafted or mismatched)."""
    d = draft_picks.with_columns(pl.col("collegeAthleteId").cast(pl.Utf8).alias("cfbd_id"), pl.col("year").cast(pl.Int64).alias("draft_year"),
                                 pl.col("overall").cast(pl.Int64), pl.col("round").cast(pl.Int64), _norm("name").alias("_n"))
    d = d.filter(pl.col("cfbd_id").is_not_null()).unique("cfbd_id")
    n = ids.filter(pl.col("gsis_id").is_not_null()).with_columns(pl.col("draft_year").cast(pl.Int64), pl.col("draft_pick").cast(pl.Int64), _norm("name").alias("_n"))
    by_pick = d.join(n.select("gsis_id", "draft_year", pl.col("draft_pick").alias("overall"), pl.col("birthdate").alias("birth_date") if "birthdate" in n.columns else pl.lit(None, pl.Utf8).alias("birth_date")).unique(["draft_year", "overall"]),
                     on=["draft_year", "overall"], how="left")
    by_name = n.select("_n", "position", pl.col("gsis_id").alias("_g2"), pl.col("birthdate").alias("_b2") if "birthdate" in n.columns else pl.lit(None, pl.Utf8).alias("_b2")).unique(["_n", "position"])
    x = by_pick.join(by_name, on=["_n", "position"], how="left").with_columns(
        pl.coalesce([pl.col("gsis_id"), pl.col("_g2")]).alias("gsis_id"), pl.coalesce([pl.col("birth_date"), pl.col("_b2")]).alias("birth_date"),
        pl.when(pl.col("gsis_id").is_not_null()).then(pl.lit("pick")).when(pl.col("_g2").is_not_null()).then(pl.lit("name")).otherwise(None).alias("match"))
    return x.select("cfbd_id", "gsis_id", "match", "name", "position", "draft_year", "round", "overall", pl.col("collegeTeam").alias("college"), "birth_date").with_columns(pl.lit(datetime.now(timezone.utc)).alias("loaded_at"))


def main() -> None:
    frames = {}
    for ds in ("player_season_stats", "player_usage", "rosters", "team_sp", "team_season_stats", "draft_picks"):
        try:
            frames[ds] = read_bronze_prefix(BUCKET, f"bronze/cfbd/{ds}/")
        except Exception as e:  # noqa: BLE001
            print(f"{ds}: none ({str(e)[:60]})"); frames[ds] = pl.DataFrame()
    fact = build_fact(frames["player_season_stats"], frames["player_usage"], frames["rosters"], frames["team_sp"], frames["team_season_stats"])
    fact.write_parquet(f"gs://{BUCKET}/{OUT_FACT}")
    print(f"wrote {OUT_FACT}: {fact.shape}")
    ids = read_bronze_prefix(BUCKET, "bronze/nflverse/fantasy_player_ids/")
    ids = ids.sort("db_season" if "db_season" in ids.columns else ids.columns[0]).unique("gsis_id", keep="last") if "gsis_id" in ids.columns else ids
    xw = build_crosswalk(frames["draft_picks"], ids)
    xw.write_parquet(f"gs://{BUCKET}/{OUT_XWALK}")
    print(f"wrote {OUT_XWALK}: {xw.shape}; matched {xw['gsis_id'].is_not_null().sum()} of {xw.height}")


if __name__ == "__main__":
    main()
