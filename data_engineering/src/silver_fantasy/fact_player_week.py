"""silver_fantasy/fact_player_week.py

Atomic player-production fact: one row per (player, NFL season, week) with fantasy points
re-scored under the league's scoring rules, the weekly stat line, the **game date** (so it
joins ``dim_dates`` -> nfl_phase / week windows), and player bio. Built from nflverse weekly
box scores (``bronze/nflverse/player_stats``) + schedules (``bronze/nflverse/schedules``, for
the date) + the nflverse player bio table + league scoring (``dim_league_settings``).

This is the lowest natural grain for *fantasy production* (points are a box-score, per-week
quantity). Play-by-play (``bronze/nflverse/play_by_play``) is a separate, finer fact for
opportunity/efficiency work — not needed to compute fantasy points. ``fact_player_season``
is a rollup of this table.

Output: ``gs://<bucket>/silver/fantasy/fact_player_week/data.parquet``
"""
from __future__ import annotations

import io
import os
from datetime import datetime

import polars as pl
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

PLAYER_STATS_PREFIX = "bronze/nflverse/player_stats/"
SCHEDULES_PREFIX = "bronze/nflverse/schedules/"
NFL_PLAYERS_PREFIX = "bronze/nflverse/nfl_players/"
SETTINGS_PATH = "silver/fantasy/dim_league_settings/data.parquet"
POSITIONS = ["QB", "RB", "WR", "TE"]

# nflverse player_stats uses the *current* franchise code for all history (LV/LAC/LA/JAC),
# while schedules use the *era* code (OAK/SD/STL/JAX). Canonicalize both sides of the
# game_date join so pre-relocation seasons still match. (Only these franchises differ —
# verified against the data.)
TEAM_ALIASES = {"OAK": "LV", "RAI": "LV", "SD": "LAC", "SDG": "LAC",
                "STL": "LA", "LAR": "LA", "RAM": "LA", "JAX": "JAC"}

# --- Sleeper scoring key -> nflverse player_stats column (direct per-stat weights) ---
DIRECT_MAP: dict[str, str] = {
    "pass_yd": "passing_yards", "pass_td": "passing_tds", "pass_int": "passing_interceptions",
    "pass_2pt": "passing_2pt_conversions", "pass_att": "attempts", "pass_cmp": "completions",
    "rush_yd": "rushing_yards", "rush_td": "rushing_tds", "rush_2pt": "rushing_2pt_conversions",
    "rush_att": "carries", "rush_fd": "rushing_first_downs",
    "rec": "receptions", "rec_yd": "receiving_yards", "rec_td": "receiving_tds",
    "rec_2pt": "receiving_2pt_conversions", "rec_fd": "receiving_first_downs",
}
POS_REC_BONUS = {"bonus_rec_te": "TE", "bonus_rec_wr": "WR", "bonus_rec_rb": "RB"}
FUMBLE_LOST_COLS = ["sack_fumbles_lost", "rushing_fumbles_lost", "receiving_fumbles_lost"]
FUMBLE_ALL_COLS = ["sack_fumbles", "rushing_fumbles", "receiving_fumbles"]
YARDAGE_BONUS = {
    "bonus_rec_yd_100": ("receiving_yards", 100),
    "bonus_rush_yd_100": ("rushing_yards", 100),
    "bonus_rush_yd_200": ("rushing_yards", 200),
}
COMPUTABLE_KEYS = set(DIRECT_MAP) | set(POS_REC_BONUS) | set(YARDAGE_BONUS) | {"fum_lost", "fum"}
KNOWN_OFFENSE_UNCOMPUTABLE = {
    "pass_td_40p", "pass_td_50p", "pass_cmp_40p", "rush_td_40p", "rush_td_50p", "rush_40p",
    "rec_td_40p", "rec_td_50p", "rec_40p", "rec_0_4", "rec_10_19", "rec_20_29", "rec_30_39",
}
OFFENSE_KEYS = COMPUTABLE_KEYS | KNOWN_OFFENSE_UNCOMPUTABLE


# --------------------------------------------------------------------------- scoring
def _c(name: str) -> pl.Expr:
    return pl.col(name).fill_null(0.0)


def score_expr(scoring: dict[str, float]) -> tuple[pl.Expr, list[str]]:
    """Weekly fantasy-points expression + the list of nonzero offense keys we can't compute."""
    terms: list[pl.Expr] = []
    unmodeled: list[str] = []
    for key, w in scoring.items():
        if w is None or abs(w) < 1e-9:
            continue
        if key in DIRECT_MAP:
            terms.append(_c(DIRECT_MAP[key]) * w)
        elif key == "fum_lost":
            terms.append(sum((_c(c) for c in FUMBLE_LOST_COLS), pl.lit(0.0)) * w)
        elif key == "fum":
            terms.append(sum((_c(c) for c in FUMBLE_ALL_COLS), pl.lit(0.0)) * w)
        elif key in POS_REC_BONUS:
            terms.append(
                pl.when(pl.col("position") == POS_REC_BONUS[key]).then(_c("receptions")).otherwise(0.0) * w
            )
        elif key in YARDAGE_BONUS:
            col, thresh = YARDAGE_BONUS[key]
            terms.append(pl.when(_c(col) >= thresh).then(1.0).otherwise(0.0) * w)
        else:
            unmodeled.append(key)
    expr = pl.sum_horizontal(terms) if terms else pl.lit(0.0)
    return expr.cast(pl.Float64).alias("fpts"), unmodeled


def score_player_weeks(df: pl.DataFrame, scoring: dict[str, float]) -> pl.DataFrame:
    """Add ``fpts`` = fantasy points for each player-week under ``scoring``."""
    expr, _ = score_expr(scoring)
    return df.with_columns(expr)


def resolve_league_scoring(settings_df: pl.DataFrame, league_id: str | None = None) -> dict:
    """Resolve offense scoring weights from a ``dim_league_settings`` frame.

    ``league_id=None`` picks the primary league = the lineage with the most current rows.
    """
    cur = settings_df.filter(pl.col("is_current"))
    if cur.is_empty():
        raise RuntimeError("dim_league_settings has no current rows")
    if league_id is not None:
        row = cur.filter(pl.col("league_id").cast(pl.Utf8) == str(league_id))
        if row.is_empty():
            raise ValueError(f"league_id {league_id!r} not found among current settings")
    else:
        primary = (
            cur.group_by("league_lineage_id").len().sort("len", descending=True)
            .get_column("league_lineage_id")[0]
        )
        row = cur.filter(pl.col("league_lineage_id") == primary)
    r = row.sort("valid_from", descending=True).head(1).to_dicts()[0]
    scoring = {
        k: round(float(r[k]), 4)
        for k in OFFENSE_KEYS
        if k in r and r[k] is not None and abs(float(r[k])) > 1e-9
    }
    _, unmodeled = score_expr(scoring)
    return {
        "league_id": str(r["league_id"]),
        "league_lineage_id": str(r["league_lineage_id"]),
        "scoring": scoring,
        "unmodeled_keys": unmodeled,
    }


# ----------------------------------------------------------------------- transforms
def filter_offense_weeks(weeks: pl.DataFrame) -> pl.DataFrame:
    """Keep regular-season offense rows (QB/RB/WR/TE) and normalize season/week dtypes."""
    return weeks.filter(
        (pl.col("season_type") == "REG") & pl.col("position").is_in(POSITIONS)
    ).with_columns(pl.col("season").cast(pl.Int64), pl.col("week").cast(pl.Int64))


def _canon_team(col: str) -> pl.Expr:
    """Map era/alias team codes to the canonical franchise code (unmapped pass through)."""
    return pl.col(col).replace(TEAM_ALIASES)


def build_team_dates(schedules: pl.DataFrame) -> pl.DataFrame:
    """(season, week, team, game_date, opponent) for regular-season games, one row per team.

    Unions the home and away perspectives of each game so a player's team maps to its game
    date — the key that joins ``dim_dates`` (on ``date``) for nfl_phase / week windows. Team
    codes are canonicalized so pre-relocation seasons match ``player_stats``.
    """
    s = schedules.filter(pl.col("game_type") == "REG").select(
        pl.col("season").cast(pl.Int64),
        pl.col("week").cast(pl.Int64),
        pl.col("gameday").cast(pl.Utf8).str.slice(0, 10).str.to_date(strict=False).alias("game_date"),
        _canon_team("home_team").alias("home_team"),
        _canon_team("away_team").alias("away_team"),
    )
    home = s.select("season", "week", "game_date",
                    pl.col("home_team").alias("team"), pl.col("away_team").alias("opponent"))
    away = s.select("season", "week", "game_date",
                    pl.col("away_team").alias("team"), pl.col("home_team").alias("opponent"))
    return pl.concat([home, away]).unique(["season", "week", "team"])


def attach_bio(df: pl.DataFrame, bio_df: pl.DataFrame) -> pl.DataFrame:
    """Join nflverse bio and derive age-as-of-season, experience, rookie/undrafted flags.

    Age is computed from ``birth_date`` against Sept 1 of the season (constant within a
    season, so it's valid at the week grain too).
    """
    b = bio_df.select(
        pl.col("gsis_id"),
        pl.col("birth_date").cast(pl.Utf8).str.slice(0, 10).str.to_date(strict=False).alias("birth_date"),
        pl.col("draft_round").cast(pl.Int64, strict=False).alias("draft_round"),
        pl.col("draft_pick").cast(pl.Int64, strict=False).alias("draft_pick"),
        pl.col("rookie_season").cast(pl.Int64, strict=False).alias("rookie_season"),
        pl.col("college_name"),
    ).unique(subset=["gsis_id"], keep="first")
    season_ref = pl.date(pl.col("season").cast(pl.Int32), 9, 1)
    return df.join(b, left_on="player_id", right_on="gsis_id", how="left").with_columns(
        ((season_ref - pl.col("birth_date")).dt.total_days() / 365.25).alias("age_at_season"),
        (pl.col("season") - pl.col("rookie_season")).alias("exp_at_season"),
        (pl.col("season") == pl.col("rookie_season")).alias("is_rookie"),
        pl.col("draft_pick").is_null().alias("is_undrafted"),
    )


# weekly stat columns carried on the fact (nflverse col -> output name)
WEEK_STATS = {
    "attempts": "pass_att", "completions": "pass_cmp", "passing_yards": "pass_yds",
    "passing_tds": "pass_tds", "passing_interceptions": "pass_int",
    "carries": "rush_att", "rushing_yards": "rush_yds", "rushing_tds": "rush_tds",
    "targets": "targets", "receptions": "rec", "receiving_yards": "rec_yds",
    "receiving_tds": "rec_tds", "target_share": "target_share", "wopr": "wopr",
}


def build_fact_player_week(
    weeks: pl.DataFrame, schedules: pl.DataFrame, bio: pl.DataFrame,
    scoring: dict[str, float], scored_under: str | None = None,
) -> pl.DataFrame:
    """Pure build: weekly box scores + schedules + bio + scoring -> player-week fact."""
    scored = score_player_weeks(filter_offense_weeks(weeks), scoring)
    wk = scored.select(
        "player_id", "season", "week",
        pl.col("player_display_name").alias("player_name"),
        "position", "team", "opponent_team",
        "fpts",
        pl.col("fantasy_points_ppr").alias("fpts_ppr_nflverse"),
        *[pl.col(src).alias(dst) for src, dst in WEEK_STATS.items()],
    )
    team_dates = build_team_dates(schedules).select(
        "season", "week", pl.col("team").alias("_team_key"), "game_date")
    wk = wk.with_columns(_canon_team("team").alias("_team_key")).join(
        team_dates, on=["season", "week", "_team_key"], how="left").drop("_team_key")
    wk = attach_bio(wk, bio)
    return wk.with_columns(
        pl.lit(scored_under).alias("scored_under_league_id"),
        pl.lit("nflverse+sleeper").alias("source_system"),
        pl.lit(datetime.now()).alias("loaded_at"),
    ).sort(["season", "week", "fpts"], descending=[False, False, True])


# ------------------------------------------------------------------------------- IO
def _bucket() -> str:
    return os.environ.get("GCS_BUCKET_NAME") or "nfl-data-bronze"


def _read_gcs(name: str) -> pl.DataFrame:
    return pl.read_parquet(io.BytesIO(storage.Client().bucket(_bucket()).blob(name).download_as_bytes()))


def _read_prefix(prefix: str) -> pl.DataFrame:
    cl = storage.Client()
    names = sorted(b.name for b in cl.list_blobs(_bucket(), prefix=prefix) if b.name.endswith(".parquet"))
    return pl.concat([_read_gcs(n) for n in names], how="diagonal_relaxed")


def load_bio() -> pl.DataFrame:
    cl = storage.Client()
    names = [b.name for b in cl.list_blobs(_bucket(), prefix=NFL_PLAYERS_PREFIX) if b.name.endswith(".parquet")]
    latest = max(names, key=lambda n: next((s for s in n.split("/") if s.startswith("load_date=")), ""))
    return _read_gcs(latest)


def save_df_to_gcs(df: pl.DataFrame, bucket_name: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf)
    storage.Client().bucket(bucket_name).blob("silver/fantasy/fact_player_week/data.parquet").upload_from_string(
        buf.getvalue(), content_type="application/octet-stream")
    print(f"✅ Saved fact_player_week ({df.shape[0]} rows) to gs://{bucket_name}/silver/fantasy/fact_player_week/data.parquet")


def main() -> None:
    bucket = _bucket()
    cfg = resolve_league_scoring(_read_gcs(SETTINGS_PATH))
    print(f"Scoring league {cfg['league_id']} (lineage {cfg['league_lineage_id']}): {cfg['scoring']}")
    if cfg["unmodeled_keys"]:
        print("!! unmodeled nonzero scoring keys:", cfg["unmodeled_keys"])
    fact = build_fact_player_week(
        _read_prefix(PLAYER_STATS_PREFIX), _read_prefix(SCHEDULES_PREFIX), load_bio(),
        cfg["scoring"], cfg["league_id"],
    )
    save_df_to_gcs(fact, bucket)


if __name__ == "__main__":
    main()
