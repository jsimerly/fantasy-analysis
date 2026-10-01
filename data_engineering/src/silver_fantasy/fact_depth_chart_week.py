"""silver: fact_depth_chart_week -- one row per (season, week, team, gsis_id, position) for the
offensive skill positions (QB / RB / WR / TE / FB): the team's depth-chart rank for the player
entering that week, plus movement versus his previous listed week.

nflverse publishes depth charts in two formats and both live in ``bronze/nflverse/depth_charts``:
  * weekly era (2001-2024 files): ``season`` / ``week`` / ``formation`` / ``depth_position`` /
    ``depth_team`` ('1', '2', '3') -- one listing per team-week; the three starting WRs are the
    depth-1 players of the LWR / RWR / SWR slots;
  * snapshot era (the 2025+ daily feed): a timestamp ``dt`` (about two snapshots a day, offseason
    included), ``pos_name`` / ``pos_slot`` / ``pos_rank`` and *no* season or week. ``pos_rank`` is
    the player's overall rank at the position (WR1..WRn across the slots: slot 1 holds ranks
    1, 4, 7..., slot 2 holds 2, 5, 8...). Each snapshot is assigned to the team's next scheduled
    game (``bronze/nflverse/schedules``) and the LAST snapshot before that game is the chart
    "entering the week"; snapshots after a team's final game are dropped. The season is the
    bronze partition the file sits in.

``depth_rank`` is the depth *within a slot* in both eras (1 = a starter at his position, 2 = the
backup for that slot), so WR3 and WR1 are both rank 1 and the eras agree. ``position_rank`` is
the overall rank at the position (WR4 = 4); it is only known in the snapshot era. A player listed
in several slots keeps his best rank.

Movement: ``prev_depth_rank`` / ``prev_week`` are the player's previous listed week in the same
season and position; ``depth_rank_change`` = depth_rank - prev_depth_rank (negative = moved up).

Output: ``gs://<bucket>/silver/fantasy/fact_depth_chart_week/data.parquet``
"""
from __future__ import annotations

import io
import os

import polars as pl
from dotenv import load_dotenv
from google.cloud import storage

from silver_fantasy.fact_player_week import TEAM_ALIASES, _canon_team
from silver_fantasy.utils import read_bronze_prefix

load_dotenv()

DEPTH_PREFIX = "bronze/nflverse/depth_charts/"
SCHEDULES_PREFIX = "bronze/nflverse/schedules/"
OUTPUT_PATH = "silver/fantasy/fact_depth_chart_week/data.parquet"
KEY = ["season", "week", "team", "gsis_id", "position"]

SLOT_TO_POSITION = {"QB": "QB", "RB": "RB", "HB": "RB", "FB": "FB", "WR": "WR", "LWR": "WR", "RWR": "WR",
                    "SWR": "WR", "TE": "TE"}
NAME_TO_POSITION = {"Quarterback": "QB", "Running Back": "RB", "Halfback": "RB", "Fullback": "FB",
                    "Wide Receiver": "WR", "Tight End": "TE"}

SCHEMA = {"season": pl.Int32, "week": pl.Int32, "game_type": pl.Utf8, "team": pl.Utf8, "gsis_id": pl.Utf8,
          "player_name": pl.Utf8, "position": pl.Utf8, "slot": pl.Utf8, "depth_rank": pl.Int32,
          "position_rank": pl.Int32, "source": pl.Utf8, "snapshot_at": pl.Datetime("us")}


def _finish(df: pl.DataFrame) -> pl.DataFrame:
    """Drop unusable rows, keep each player's best listing per key, fix the schema."""
    df = df.filter(pl.col("position").is_not_null() & pl.col("depth_rank").is_not_null())
    df = df.sort(["depth_rank", "position_rank", "slot"], nulls_last=True).unique(subset=KEY, keep="first", maintain_order=True)
    return df.select(list(SCHEMA)).cast(SCHEMA)


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)


def weekly_era(depth: pl.DataFrame) -> pl.DataFrame:
    """Rows that carry season/week (2001-2024 files) -> normalized weekly rows."""
    cols = depth.columns
    if "week" not in cols or "depth_team" not in cols:
        return _empty()
    df = depth.filter(pl.col("week").is_not_null() & pl.col("gsis_id").is_not_null())
    if "formation" in cols:
        df = df.filter(pl.col("formation").is_null() | (pl.col("formation") == "Offense"))
    team = pl.coalesce([pl.col(c).cast(pl.Utf8) for c in ("club_code", "team") if c in cols]).replace(TEAM_ALIASES)
    name_cols = [c for c in ("full_name", "player_name") if c in cols]
    name = pl.coalesce([pl.col(c).cast(pl.Utf8) for c in name_cols]) if name_cols else pl.lit(None, pl.Utf8)
    season = (pl.coalesce([pl.col("season").cast(pl.Int32), pl.col("partition_season").cast(pl.Int32)])
              if "partition_season" in cols else pl.col("season").cast(pl.Int32))
    df = df.select(
        season.alias("season"),
        pl.col("week").cast(pl.Int32).alias("week"),
        (pl.col("game_type").cast(pl.Utf8) if "game_type" in cols else pl.lit(None, pl.Utf8)).alias("game_type"),
        team.alias("team"),
        pl.col("gsis_id").cast(pl.Utf8),
        name.alias("player_name"),
        pl.col("depth_position").cast(pl.Utf8).replace_strict(SLOT_TO_POSITION, default=None, return_dtype=pl.Utf8).alias("position"),
        pl.col("depth_position").cast(pl.Utf8).alias("slot"),
        pl.col("depth_team").cast(pl.Utf8).cast(pl.Int32, strict=False).alias("depth_rank"),
        pl.lit(None, pl.Int32).alias("position_rank"),
        pl.lit("weekly").alias("source"),
        pl.lit(None, pl.Datetime("us")).alias("snapshot_at"),
    )
    return _finish(df)


def team_game_dates(schedules: pl.DataFrame) -> pl.DataFrame:
    """(season, team, week, game_type, game_date) for every scheduled game, both perspectives."""
    s = schedules.filter(pl.col("gameday").is_not_null()).select(
        pl.col("season").cast(pl.Int32), pl.col("week").cast(pl.Int32), pl.col("game_type").cast(pl.Utf8),
        pl.col("gameday").cast(pl.Utf8).str.slice(0, 10).str.to_date(strict=False).alias("game_date"),
        _canon_team("home_team").alias("home_team"), _canon_team("away_team").alias("away_team"),
    )
    home = s.select("season", "week", "game_type", "game_date", pl.col("home_team").alias("team"))
    away = s.select("season", "week", "game_type", "game_date", pl.col("away_team").alias("team"))
    return pl.concat([home, away]).unique(["season", "team", "week"]).sort(["season", "team", "game_date"])


def snapshot_era(depth: pl.DataFrame, schedules: pl.DataFrame) -> pl.DataFrame:
    """Rows that carry a snapshot timestamp (2025+ feed) -> one row per team-week from the last
    snapshot before that week's game."""
    cols = depth.columns
    if "dt" not in cols or "pos_rank" not in cols:
        return _empty()
    df = (depth.filter(pl.col("dt").is_not_null() & pl.col("gsis_id").is_not_null())
               .with_columns(pl.col("pos_name").cast(pl.Utf8).replace_strict(NAME_TO_POSITION, default=None, return_dtype=pl.Utf8).alias("position"))
               .filter(pl.col("position").is_not_null())
               .select(
                   pl.col("partition_season").cast(pl.Int32).alias("season"),
                   pl.col("dt").cast(pl.Utf8).str.slice(0, 19).str.to_datetime("%Y-%m-%dT%H:%M:%S").alias("snapshot_at"),
                   _canon_team("team").alias("team"), pl.col("gsis_id").cast(pl.Utf8),
                   pl.col("player_name").cast(pl.Utf8), "position",
                   pl.col("pos_abb").cast(pl.Utf8).alias("slot"),
                   pl.col("pos_slot").cast(pl.Int32).alias("_slot_no"),
                   pl.col("pos_rank").cast(pl.Int32).alias("position_rank"),
               )
               .filter(pl.col("season").is_not_null() & pl.col("position_rank").is_not_null())
               .with_columns(
                   # depth within the slot = ceil(overall rank / slots at the position): with 3 WR
                   # slots, overall ranks 1-3 are depth 1, 4-6 depth 2; a lone QB slot keeps QB2 = 2
                   ((pl.col("position_rank") - 1) // pl.col("_slot_no").n_unique().over(["snapshot_at", "team", "position"]) + 1)
                   .cast(pl.Int32).alias("depth_rank"),
                   pl.col("snapshot_at").dt.date().alias("snapshot_date"),
               ))
    games = team_game_dates(schedules)
    # next game on or after the snapshot date -> the week this chart is "entering"
    df = (df.sort("snapshot_date")
            .join_asof(games.sort("game_date"), left_on="snapshot_date", right_on="game_date",
                       by=["season", "team"], strategy="forward")
            .filter(pl.col("week").is_not_null()))
    last = df.group_by(["season", "team", "week"]).agg(pl.col("snapshot_at").max().alias("_last"))
    df = (df.join(last, on=["season", "team", "week"]).filter(pl.col("snapshot_at") == pl.col("_last"))
            .with_columns(pl.lit("snapshot").alias("source")))
    return _finish(df)


def attach_movement(df: pl.DataFrame) -> pl.DataFrame:
    """Previous listed week (same season, player, position) and the rank change since."""
    over = ["season", "gsis_id", "position"]
    df = df.sort(["season", "gsis_id", "position", "week"])
    return df.with_columns(
        pl.col("depth_rank").shift(1).over(over).alias("prev_depth_rank"),
        pl.col("week").shift(1).over(over).alias("prev_week"),
    ).with_columns(
        (pl.col("depth_rank") - pl.col("prev_depth_rank")).alias("depth_rank_change"),
        (pl.col("depth_rank") == 1).alias("is_starter"),
    )


def build_fact_depth_chart_week(depth: pl.DataFrame, schedules: pl.DataFrame) -> pl.DataFrame:
    both = pl.concat([weekly_era(depth), snapshot_era(depth, schedules)], how="diagonal_relaxed")
    # if a key somehow exists in both eras, the explicit weekly listing wins
    both = both.sort("source", descending=True).unique(subset=KEY, keep="first", maintain_order=True)
    return attach_movement(both).sort(["season", "week", "team", "position", "depth_rank", "gsis_id"])


def _bucket() -> str:
    b = os.environ.get("GCS_BUCKET_NAME")
    if not b:
        raise RuntimeError("GCS_BUCKET_NAME not set")
    return b


def save_df_to_gcs(df: pl.DataFrame, bucket_name: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf)
    storage.Client().bucket(bucket_name).blob(OUTPUT_PATH).upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    print(f"✅ Saved fact_depth_chart_week ({df.shape[0]} rows) to gs://{bucket_name}/{OUTPUT_PATH}")


def main() -> None:
    bucket = _bucket()
    depth = read_bronze_prefix(bucket, DEPTH_PREFIX)
    schedules = read_bronze_prefix(bucket, SCHEDULES_PREFIX, dedupe_on=["game_id"])
    fact = build_fact_depth_chart_week(depth, schedules)
    by_src = fact.group_by("source").agg(pl.len(), pl.col("season").min().alias("from"), pl.col("season").max().alias("to")).to_dicts()
    print(f"rows {fact.height:,}; by source {by_src}; positions {fact['position'].value_counts().sort('count', descending=True).to_dicts()}")
    save_df_to_gcs(fact, bucket)


if __name__ == "__main__":
    main()
