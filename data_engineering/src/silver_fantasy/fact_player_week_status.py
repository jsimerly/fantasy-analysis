"""Build ``fact_player_week_status``: one row per (season, regular-season week, player) for every
skill-position player on an NFL roster or in the stat lines that season, saying what he did that
week and, when he did not play, WHY.

Why: the models see ``games`` as a count, so a lost season reads the same whether it was a torn ACL,
a suspension, a healthy scratch or a release, and a benching looks like an injury. The weekly status
is the input for sequence-shaped features (a season as 18 weekly slots) and for miss-reason counts.

Sources: bronze ``nflverse/rosters_weekly`` (2002+; team, roster status, PFR id), bronze
``nflverse/schedules`` (byes: a team-week with no regular-season game), bronze ``nflverse/snap_counts``
(2013+, joined through the roster's PFR id), silver ``fact_player_week`` (the stat lines: played, fpts,
targets, touches) and silver ``fact_player_injury_week`` (report status, body-part class, reserve lists).

``status`` (one of ``STATUSES``), in priority order:
  * ``played``           a stat line that week
  * ``bye``              the player's team had no game
  * ``injured_reserve``  on IR / PUP / NFI that week (reserve list)
  * ``injured_out``      on the injury report and did not play (Out / Doubtful / Questionable alike)
  * ``suspended``        roster status SUS or the commissioner's exempt list
  * ``practice_squad``   roster status DEV
  * ``inactive``         on the inactive list with no injury listed (a healthy scratch)
  * ``dnp``              on the active roster, no injury listed, no stat line (dressed or not, did not play)
  * ``not_rostered``     cut, retired, between teams, or no roster row at all that week
``injury_class`` is the report's body-part class, carried forward through a reserve stint whose own
rows have no report (an IR row says "reserve", not "knee"); null when the player was not injured.

Output: ``gs://<bucket>/silver/fantasy/fact_player_week_status/data.parquet``
"""
from __future__ import annotations

import io
import os

import polars as pl
from dotenv import load_dotenv
from google.cloud import storage

from silver_fantasy.fact_player_week import _canon_team
from silver_fantasy.utils import read_bronze_prefix

ROSTERS_WEEKLY_PREFIX = "bronze/nflverse/rosters_weekly/"
SCHEDULES_PREFIX = "bronze/nflverse/schedules/"
SNAP_COUNTS_PREFIX = "bronze/nflverse/snap_counts/"
WEEK_PATH = "silver/fantasy/fact_player_week/data.parquet"
INJURY_PATH = "silver/fantasy/fact_player_injury_week/data.parquet"
OUTPUT_PATH = "silver/fantasy/fact_player_week_status/data.parquet"

POSITIONS = ["QB", "RB", "WR", "TE"]
KEY = ["season", "week", "gsis_id"]
STATUSES = ["played", "bye", "injured_reserve", "injured_out", "suspended", "practice_squad", "inactive", "dnp", "not_rostered"]
RESERVE_STATUSES = ["RES", "PUP", "NON"]
SUSPENDED_STATUSES = ["SUS", "EXE", "E01", "E02", "E14"]
INJURED_STATUSES = ["injured_reserve", "injured_out"]
MIN_SEASON = 2002                      # weekly rosters start here; before that a miss has no reason


def _i32(col: str) -> pl.Expr:
    return pl.col(col).cast(pl.Float64, strict=False).cast(pl.Int32)


def games_by_team_week(schedules: pl.DataFrame) -> pl.DataFrame:
    """(season, week, team) for every regular-season game, plus each season's last regular week."""
    reg = schedules.filter(pl.col("game_type") == "REG").select(_i32("season").alias("season"), _i32("week").alias("week"), "home_team", "away_team")
    home = reg.select("season", "week", _canon_team("home_team").alias("team"))
    away = reg.select("season", "week", _canon_team("away_team").alias("team"))
    return pl.concat([home, away]).unique().with_columns(pl.lit(True).alias("has_game"))


def roster_weeks(rosters_weekly: pl.DataFrame) -> pl.DataFrame:
    """One roster row per (season, week, player) for the regular season: team, status, PFR id."""
    cols = rosters_weekly.columns
    desc = pl.col("status_description_abbr") if "status_description_abbr" in cols else pl.lit(None, pl.Utf8)
    pfr = pl.col("pfr_id") if "pfr_id" in cols else pl.lit(None, pl.Utf8)
    name = pl.col("full_name") if "full_name" in cols else pl.lit(None, pl.Utf8)
    gt = pl.col("game_type") if "game_type" in cols else pl.lit("REG")
    return (rosters_weekly.filter(pl.col("gsis_id").is_not_null() & pl.col("week").is_not_null() & (gt == "REG"))
            .select(_i32("season").alias("season"), _i32("week").alias("week"), "gsis_id",
                    _canon_team("team").alias("r_team"), pl.col("status").alias("roster_status"), desc.alias("status_description"),
                    pl.col("position").alias("r_position"), pfr.alias("pfr_id"), name.alias("r_name"))
            .sort(["season", "week", "gsis_id", "roster_status"])
            .unique(subset=KEY, keep="first", maintain_order=True))


def snap_shares(snap_counts: pl.DataFrame, roster: pl.DataFrame) -> pl.DataFrame:
    """(season, week, gsis_id) -> offensive snaps and share, through the roster's PFR id."""
    if snap_counts.height == 0 or "pfr_player_id" not in snap_counts.columns:
        return pl.DataFrame(schema={"season": pl.Int32, "week": pl.Int32, "gsis_id": pl.Utf8, "offense_snaps": pl.Float64, "offense_pct": pl.Float64})
    gt = pl.col("game_type") if "game_type" in snap_counts.columns else pl.lit("REG")
    sn = (snap_counts.filter(gt == "REG").select(_i32("season").alias("season"), _i32("week").alias("week"), pl.col("pfr_player_id").alias("pfr_id"),
                                                 pl.col("offense_snaps").cast(pl.Float64, strict=False), pl.col("offense_pct").cast(pl.Float64, strict=False))
          .filter(pl.col("pfr_id").is_not_null()))
    ids = roster.filter(pl.col("pfr_id").is_not_null()).select("season", "week", "gsis_id", "pfr_id").unique()
    return (sn.join(ids, on=["season", "week", "pfr_id"], how="inner")
              .group_by(KEY).agg(pl.col("offense_snaps").sum(), pl.col("offense_pct").max()))


def _stat_lines(stats: pl.DataFrame) -> pl.DataFrame:
    opp = (pl.col("targets").fill_null(0) + pl.col("rush_att").fill_null(0)).cast(pl.Float64)
    touches = (pl.col("rush_att").fill_null(0) + pl.col("rec").fill_null(0)).cast(pl.Float64)
    return (stats.select(_i32("season").alias("season"), _i32("week").alias("week"), pl.col("player_id").alias("gsis_id"),
                         pl.col("position").alias("s_position"), _canon_team("team").alias("s_team"), pl.col("player_name").alias("s_name"),
                         pl.col("fpts").cast(pl.Float64), pl.col("targets").cast(pl.Float64, strict=False), opp.alias("opportunities"), touches.alias("touches"))
                 .unique(subset=KEY, keep="first"))


def build_fact_player_week_status(stats: pl.DataFrame, rosters_weekly: pl.DataFrame, injury: pl.DataFrame,
                                  schedules: pl.DataFrame, snap_counts: pl.DataFrame | None = None) -> pl.DataFrame:
    roster = roster_weeks(rosters_weekly)
    lines = _stat_lines(stats)
    games = games_by_team_week(schedules)
    last_week = games.group_by("season").agg(pl.col("week").max().alias("last_week"))
    snaps = snap_shares(snap_counts, roster) if snap_counts is not None else None

    # the universe: every skill-position player-season on a roster or in the stat lines, from MIN_SEASON
    skill_roster = roster.filter(pl.col("r_position").is_in(POSITIONS)).select("season", "gsis_id")
    universe = (pl.concat([skill_roster, lines.filter(pl.col("s_position").is_in(POSITIONS)).select("season", "gsis_id")]).unique()
                  .filter(pl.col("season") >= MIN_SEASON).join(last_week, on="season", how="inner"))
    grid = (universe.with_columns(pl.int_ranges(1, pl.col("last_week") + 1).alias("week")).explode("week")
                    .with_columns(pl.col("week").cast(pl.Int32)).drop("last_week"))

    inj = (injury.select(_i32("season").alias("season"), _i32("week").alias("week"), "gsis_id",
                         pl.col("on_injury_report").fill_null(False), "report_status",
                         pl.when(pl.col("injury_class").is_in(["unknown", "none"])).then(None).otherwise(pl.col("injury_class")).alias("report_class"),
                         pl.col("is_out").fill_null(False), pl.col("on_injured_reserve").fill_null(False))
                 .unique(subset=KEY, keep="first"))

    d = grid.join(roster, on=KEY, how="left").join(lines, on=KEY, how="left").join(inj, on=KEY, how="left")
    if snaps is not None:
        d = d.join(snaps, on=KEY, how="left")
    else:
        d = d.with_columns(pl.lit(None, pl.Float64).alias("offense_snaps"), pl.lit(None, pl.Float64).alias("offense_pct"))

    # team: the roster's, else the stat line's, else the nearest roster week (forward then backward) within the season
    d = (d.sort(["season", "gsis_id", "week"])
          .with_columns(pl.coalesce("r_team", "s_team").alias("team_raw"))
          .with_columns(pl.col("team_raw").forward_fill().backward_fill().over(["season", "gsis_id"]).alias("team"))
          .join(games, on=["season", "week", "team"], how="left"))

    played = pl.col("fpts").is_not_null()
    on_report = pl.col("on_injury_report").fill_null(False)
    reserve = pl.col("on_injured_reserve").fill_null(False) | pl.col("roster_status").is_in(RESERVE_STATUSES)
    status = (pl.when(played).then(pl.lit("played"))
                .when(pl.col("team").is_not_null() & pl.col("has_game").is_null()).then(pl.lit("bye"))
                .when(reserve).then(pl.lit("injured_reserve"))
                .when(on_report | pl.col("is_out").fill_null(False)).then(pl.lit("injured_out"))
                .when(pl.col("roster_status").is_in(SUSPENDED_STATUSES)).then(pl.lit("suspended"))
                .when(pl.col("roster_status") == "DEV").then(pl.lit("practice_squad"))
                .when(pl.col("roster_status") == "INA").then(pl.lit("inactive"))
                .when(pl.col("roster_status") == "ACT").then(pl.lit("dnp"))
                .otherwise(pl.lit("not_rostered")))
    d = d.with_columns(status.alias("status"))
    # body part: the week's report, else the last reported class this season (an IR row has no report of its own)
    d = (d.with_columns(pl.col("report_class").forward_fill().over(["season", "gsis_id"]).alias("class_ff"))
          .with_columns(pl.when(pl.col("status").is_in(INJURED_STATUSES)).then(pl.coalesce("report_class", "class_ff")).otherwise(None).alias("injury_class")))
    # position and name: known on the weeks with a roster row or a stat line; the same player all season
    out = (d.with_columns(pl.coalesce("s_position", "r_position").alias("position"), pl.coalesce("s_name", "r_name").alias("player_name"),
                          played.alias("played"), pl.col("on_injury_report").fill_null(False))
             .with_columns(pl.col("position").forward_fill().backward_fill().over(["season", "gsis_id"]),
                           pl.col("player_name").forward_fill().backward_fill().over(["season", "gsis_id"]))
             .filter(pl.col("position").is_in(POSITIONS))
             .select("season", "week", "gsis_id", "player_name", "position", "team", "status", "roster_status", "status_description",
                     "on_injury_report", "report_status", "injury_class", "played", "fpts", "targets", "opportunities", "touches",
                     "offense_snaps", "offense_pct")
             .sort(["season", "gsis_id", "week"]))
    return out


def _bucket() -> str:
    b = os.environ.get("GCS_BUCKET_NAME")
    if not b:
        raise RuntimeError("GCS_BUCKET_NAME not set")
    return b


def _read_silver(bucket_name: str, path: str) -> pl.DataFrame:
    return pl.read_parquet(io.BytesIO(storage.Client().bucket(bucket_name).blob(path).download_as_bytes()))


def save_df_to_gcs(df: pl.DataFrame, bucket_name: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf)
    storage.Client().bucket(bucket_name).blob(OUTPUT_PATH).upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    print(f"✅ Saved fact_player_week_status ({df.shape[0]} rows) to gs://{bucket_name}/{OUTPUT_PATH}")


def main() -> None:
    load_dotenv()
    bucket = _bucket()
    stats = _read_silver(bucket, WEEK_PATH)
    injury = _read_silver(bucket, INJURY_PATH)
    rw = read_bronze_prefix(bucket, ROSTERS_WEEKLY_PREFIX, dedupe_on=["season", "week", "gsis_id", "team"])
    sched = read_bronze_prefix(bucket, SCHEDULES_PREFIX, dedupe_on=["game_id"])
    snaps = read_bronze_prefix(bucket, SNAP_COUNTS_PREFIX, dedupe_on=["game_id", "pfr_player_id"])
    fact = build_fact_player_week_status(stats, rw, injury, sched, snaps)
    print(f"seasons {fact['season'].min()}-{fact['season'].max()}; rows {fact.height:,}; "
          f"statuses: {fact['status'].value_counts().sort('count', descending=True).to_dicts()}")
    print(f"injury classes among misses: {fact.filter(pl.col('status').is_in(INJURED_STATUSES))['injury_class'].value_counts().sort('count', descending=True).to_dicts()}")
    print(f"snap share filled on played weeks from 2013: {fact.filter(pl.col('played') & (pl.col('season') >= 2013))['offense_pct'].is_not_null().mean():.0%}")
    save_df_to_gcs(fact, bucket)


if __name__ == "__main__":
    main()
