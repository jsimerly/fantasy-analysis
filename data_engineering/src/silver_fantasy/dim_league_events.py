"""silver_fantasy/dim_league_events.py

League-specific dated events (the half of the calendar that varies by league) — a long
table, one row per event:
    (league_lineage_id, league_id, season, event_type, event_date, source)

event_type:
  - startup_draft : the inaugural roster draft (many rounds, > 7) — one per lineage.
  - rookie_draft  : each season's rookie draft (few rounds, <= 7).
  - fantasy_end   : the league's championship date = end of its final playoff week,
                    from playoff_week_start + ceil(log2(playoff_teams)) rounds, mapped to
                    that NFL week's last game via the schedule.

Global NFL events (season start / playoff start / end) live in dim_dates.
"""
import os

import polars as pl
from dotenv import load_dotenv

load_dotenv()

STARTUP_MIN_ROUNDS = 7  # > this many rounds => a startup (whole-roster) draft, not a rookie draft


def draft_events(drafts: pl.DataFrame, leagues: pl.DataFrame) -> pl.DataFrame:
    """Classify each draft as startup (rounds > 7) or rookie; date = start_time (the draft
    day) else last_picked."""
    d = (
        drafts.unique("draft_id")
        .with_columns(
            pl.col("league_id").cast(pl.Utf8),
            pl.col("season").cast(pl.Int64),
            pl.col("rounds").cast(pl.Int64),
            pl.coalesce([pl.col("start_time"), pl.col("last_picked")]).cast(pl.Int64).alias("_ts"),
        )
        .with_columns(pl.from_epoch("_ts", time_unit="ms").dt.date().alias("event_date"))
        .join(leagues.select(pl.col("league_id").cast(pl.Utf8), "league_lineage_id"), on="league_id", how="inner")
        .with_columns(
            pl.when(pl.col("rounds") > STARTUP_MIN_ROUNDS).then(pl.lit("startup_draft"))
              .otherwise(pl.lit("rookie_draft")).alias("event_type")
        )
    )
    return d.select(
        "league_lineage_id", "league_id", "season", "event_type", "event_date",
        pl.lit("sleeper_drafts").alias("source"),
    )


def fantasy_end_events(settings: pl.DataFrame, leagues: pl.DataFrame, schedules: pl.DataFrame) -> pl.DataFrame:
    """Per league/season championship date = last game of the final playoff week.
    final_week = playoff_week_start + ceil(log2(playoff_teams)) - 1."""
    s = settings.filter(pl.col("is_current")) if "is_current" in settings.columns else settings
    s = (
        s.select(pl.col("league_id").cast(pl.Utf8), pl.col("playoff_week_start").cast(pl.Int64),
                 pl.col("playoff_teams").cast(pl.Int64))
        .unique("league_id")
        .join(leagues.select(pl.col("league_id").cast(pl.Utf8), "league_lineage_id",
                             pl.col("season").cast(pl.Int64)), on="league_id", how="inner")
        .with_columns(pl.col("playoff_teams").cast(pl.Float64).log(2.0).ceil().cast(pl.Int64).alias("_rounds"))
        .with_columns((pl.col("playoff_week_start") + pl.col("_rounds") - 1).alias("final_week"))
    )
    week_end = (
        schedules.filter(pl.col("game_type") == "REG")
        .with_columns(pl.col("season").cast(pl.Int64), pl.col("week").cast(pl.Int64),
                      pl.col("gameday").cast(pl.Date))
        .group_by("season", "week").agg(pl.col("gameday").max().alias("event_date"))
    )
    return (
        s.join(week_end, left_on=["season", "final_week"], right_on=["season", "week"], how="left")
        .select("league_lineage_id", "league_id", "season", pl.lit("fantasy_end").alias("event_type"),
                "event_date", pl.lit("sleeper_settings+nflverse").alias("source"))
    )


def build_dim_league_events(drafts, settings, leagues, schedules) -> pl.DataFrame:
    de = draft_events(drafts, leagues)
    fe = fantasy_end_events(settings, leagues, schedules)
    return (
        pl.concat([de, fe], how="vertical_relaxed")
        .filter(pl.col("event_date").is_not_null())
        .sort("league_lineage_id", "season", "event_type")
    )


def _read_drafts_schedules(bucket_name: str):
    from silver_fantasy.utils import get_latest_bronze_path
    from google.cloud import storage
    drafts = pl.read_parquet(get_latest_bronze_path(bucket_name, "drafts/drafts", source="sleeper"))
    client = storage.Client()
    sched_names = sorted(b.name for b in client.bucket(bucket_name).list_blobs(prefix="bronze/nflverse/schedules/")
                         if b.name.endswith(".parquet"))
    schedules = pl.concat([pl.read_parquet(f"gs://{bucket_name}/{n}").select("season", "game_type", "week", "gameday")
                           for n in sched_names], how="vertical_relaxed")
    return drafts, schedules


def main():
    bucket_name = os.environ.get("GCS_BUCKET_NAME")
    leagues = pl.read_parquet(f"gs://{bucket_name}/silver/fantasy/dim_leagues_meta/data.parquet")
    settings = pl.read_parquet(f"gs://{bucket_name}/silver/fantasy/dim_league_settings/data.parquet")
    drafts, schedules = _read_drafts_schedules(bucket_name)

    dim = build_dim_league_events(drafts, settings, leagues, schedules)
    path = f"gs://{bucket_name}/silver/fantasy/dim_league_events/data.parquet"
    print(f"Writing {dim.height} league events ({dim['event_type'].unique().to_list()}) to {path}...")
    dim.write_parquet(path)
    print("Done.")


if __name__ == "__main__":
    main()
