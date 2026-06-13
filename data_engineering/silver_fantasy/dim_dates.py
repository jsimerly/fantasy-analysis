"""silver_fantasy/dim_dates.py

A classic date-dimension: one row per calendar day across the project span, with
NFL-season context as attributes/flags. This is the global, league-agnostic spine —
league-specific events (drafts, fantasy season end) live in dim_league_events.

NFL events are derived from the nflverse schedule (game_type REG/WC/DIV/CON/SB,
gameday) per season:
  - season_start   = first REG gameday
  - reg_end        = last  REG gameday
  - playoff_start  = first POST (WC) gameday
  - playoff_end    = the Super Bowl (last POST) gameday   (null while a season is in progress)

Each calendar day is labelled with the NFL season it belongs to and its phase, where
the OFFSEASON (after the Super Bowl, before the next kickoff) is attributed to the
UPCOMING season — that's the season its rookie drafts / value moves are prepping for.
"""
import os
from datetime import date, timedelta

import polars as pl
from dotenv import load_dotenv

load_dotenv()

PHASES = ("regular", "playoffs", "offseason")


def season_boundaries(schedules: pl.DataFrame) -> pl.DataFrame:
    """Per NFL season -> (season_start, reg_end, playoff_start, playoff_end) as Dates."""
    sc = schedules.select(
        pl.col("season").cast(pl.Int64),
        pl.col("game_type").cast(pl.Utf8),
        pl.col("gameday").cast(pl.Date),
    ).drop_nulls("gameday")
    reg = (sc.filter(pl.col("game_type") == "REG").group_by("season")
           .agg(pl.col("gameday").min().alias("season_start"), pl.col("gameday").max().alias("reg_end")))
    post = (sc.filter(pl.col("game_type") != "REG").group_by("season")
            .agg(pl.col("gameday").min().alias("playoff_start"), pl.col("gameday").max().alias("playoff_end")))
    return reg.join(post, on="season", how="left").sort("season")


def build_dim_dates(schedules: pl.DataFrame, end_date: date | None = None) -> pl.DataFrame:
    """Daily spine from the first NFL season_start to ``end_date`` (default: latest
    known boundary + ~1y), each row labelled with nfl_season, nfl_phase, and the three
    NFL event flags."""
    b = season_boundaries(schedules)
    start = b["season_start"].min()
    known_max = max(d for d in [b["playoff_end"].max(), b["reg_end"].max(), b["season_start"].max()] if d is not None)
    end = end_date or (known_max + timedelta(days=400))

    spine = pl.DataFrame({"date": pl.date_range(start, end, interval="1d", eager=True)})
    # as-of: each date inherits the most recent season whose start has occurred
    labelled = (
        spine.sort("date")
        .join_asof(b.sort("season_start"), left_on="date", right_on="season_start", strategy="backward")
        .with_columns(
            # phase: regular -> playoffs -> offseason. A null playoff_end = season in progress,
            # so anything past reg_end is treated as playoffs (not yet offseason).
            pl.when(pl.col("date") <= pl.col("reg_end")).then(pl.lit("regular"))
              .when(pl.col("playoff_end").is_not_null() & (pl.col("date") <= pl.col("playoff_end"))).then(pl.lit("playoffs"))
              .when(pl.col("playoff_end").is_null()).then(pl.lit("playoffs"))
              .otherwise(pl.lit("offseason")).alias("nfl_phase"),
        )
        .with_columns(
            # offseason belongs to the UPCOMING season (prepping for next kickoff)
            pl.when(pl.col("nfl_phase") == "offseason").then(pl.col("season") + 1)
              .otherwise(pl.col("season")).alias("nfl_season"),
            (pl.col("date") == pl.col("season_start")).alias("is_nfl_season_start"),
            (pl.col("date") == pl.col("playoff_start")).fill_null(False).alias("is_nfl_playoff_start"),
            (pl.col("date") == pl.col("playoff_end")).fill_null(False).alias("is_nfl_playoff_end"),
        )
    )
    return labelled.select(
        "date",
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.day().alias("day"),
        pl.col("date").dt.weekday().alias("day_of_week"),
        pl.col("date").dt.week().alias("week_of_year"),
        "nfl_season", "nfl_phase",
        "is_nfl_season_start", "is_nfl_playoff_start", "is_nfl_playoff_end",
    ).sort("date")


def _read_schedules(bucket_name: str) -> pl.DataFrame:
    from google.cloud import storage
    client = storage.Client()
    names = sorted(b.name for b in client.bucket(bucket_name).list_blobs(prefix="bronze/nflverse/schedules/")
                   if b.name.endswith(".parquet"))
    return pl.concat([pl.read_parquet(f"gs://{bucket_name}/{n}").select("season", "game_type", "gameday")
                      for n in names], how="vertical_relaxed")


def main():
    bucket_name = os.environ.get("GCS_BUCKET_NAME")
    print("Reading nflverse schedules...")
    schedules = _read_schedules(bucket_name)
    dim = build_dim_dates(schedules)
    path = f"gs://{bucket_name}/silver/fantasy/dim_dates/data.parquet"
    print(f"Writing {dim.height} dates ({dim['date'].min()} -> {dim['date'].max()}) to {path}...")
    dim.write_parquet(path)
    print("Done.")


if __name__ == "__main__":
    main()
