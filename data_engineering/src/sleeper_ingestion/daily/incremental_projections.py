"""Sleeper's weekly player projections (the full slate, ~3,100 QB/RB/WR/TE a week, Rotowire-sourced,
2018 on): one row per (season, week, player) with the projected points on the three scoring scales
and the projected stat line, keyed by Sleeper ``player_id`` (= ``player_key`` in the lake).

The model's consensus feature group reads this (machine_learning/src/inseason.py, BACKLOG 38): the
coming week's projection is a provider's point-in-time view the model learns to trust or beat.

Writes ``bronze/sleeper/projections/season=<Y>/data.parquet``, the WHOLE season rebuilt on each run:
past weeks hold their final pre-game projection (what the endpoint serves once a week is over),
the current and coming weeks hold today's view, so a Monday run captures what the provider said
before the games. Seasons come from ``PROJ_SEASONS``: ``current`` (default; the daily job), a list
``2018,2019`` or a range ``2018-2025`` (the one-time backfill, run by hand).
"""
import os
import sys
import time
from datetime import datetime, timezone

import polars as pl
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "https://api.sleeper.app/projections/nfl"
POSITIONS = ("QB", "RB", "WR", "TE")
WEEKS = range(1, 19)
STATS = ["pts_ppr", "pts_half_ppr", "pts_std", "gp", "pass_att", "pass_cmp", "pass_yd", "pass_td", "pass_int", "rush_att", "rush_yd", "rush_td",
         "rec", "rec_tgt", "rec_yd", "rec_td", "fum_lost", "adp_dd_ppr", "pos_adp_dd_ppr"]
SCHEMA = {"season": pl.Int64, "week": pl.Int64, "player_id": pl.Utf8, "position": pl.Utf8, "team": pl.Utf8, "opponent": pl.Utf8,
          "game_date": pl.Utf8, "company": pl.Utf8, "updated_at": pl.Int64, **{s: pl.Float64 for s in STATS}, "loaded_at": pl.Datetime("us")}
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) fantasy-analysis/1.0"}
PAUSE_S = 1.5


def flatten_projections(payload: list[dict], season: int, week: int, loaded_at: datetime | None = None) -> pl.DataFrame:
    """One row per projection record: the ids, the game context and the projected stats, pinned to
    ``SCHEMA`` so a week with a stat missing still concatenates. Records without a player id are dropped."""
    loaded_at = loaded_at or datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for d in payload or []:
        pid = d.get("player_id")
        if pid is None:
            continue
        st = d.get("stats") or {}
        pm = d.get("player") or {}
        rows.append({"season": int(d.get("season") or season), "week": int(d.get("week") or week), "player_id": str(pid),
                     "position": pm.get("position"), "team": d.get("team") or pm.get("team"), "opponent": d.get("opponent"),
                     "game_date": d.get("date"), "company": d.get("company"), "updated_at": d.get("updated_at"),
                     **{s: (None if st.get(s) is None else float(st.get(s))) for s in STATS}, "loaded_at": loaded_at})
    if not rows:
        return pl.DataFrame(schema=SCHEMA)
    return pl.DataFrame(rows, schema_overrides=SCHEMA).select(list(SCHEMA))


def fetch_week(season: int, week: int, retries: int = 3) -> list[dict]:
    url = f"{BASE}/{season}/{week}?season_type=regular&" + "&".join(f"position[]={p}" for p in POSITIONS) + "&order_by=pts_ppr"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 200:
                return r.json()
            print(f"  {season} week {week}: status {r.status_code}, retrying", flush=True)
        except requests.RequestException as e:
            print(f"  {season} week {week}: {e}, retrying", flush=True)
        time.sleep(10 * (attempt + 1))
    raise RuntimeError(f"projections {season} week {week}: no response after {retries} attempts")


def resolve_seasons(spec: str | None, today: datetime | None = None) -> list[int]:
    """``current`` -> the NFL season in progress (March to February), a comma list, or a ``from-to`` range."""
    today = today or datetime.now(timezone.utc)
    spec = (spec or "current").strip().lower()
    if spec == "current":
        return [today.year if today.month >= 3 else today.year - 1]
    if "-" in spec:
        a, b = spec.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(s) for s in spec.split(",") if s.strip()]


def save_season(df: pl.DataFrame, bucket_name: str, season: int) -> str:
    path = f"gs://{bucket_name.replace('gs://', '')}/bronze/sleeper/projections/season={season}/data.parquet"
    df.write_parquet(path)
    print(f"Saved projections {season} ({df.height:,} rows) to {path}", flush=True)
    return path


def main() -> None:
    bucket_name = os.environ.get("GCS_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("GCS_BUCKET_NAME environment variable is not set")
    seasons = resolve_seasons(os.environ.get("PROJ_SEASONS"))
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    failures = []
    for season in seasons:
        frames = []
        for week in WEEKS:
            try:
                frames.append(flatten_projections(fetch_week(season, week), season, week, loaded_at))
            except Exception as e:  # noqa: BLE001 - a week that will not come keeps the season from writing short
                print(f"  {season} week {week}: FAILED {e}", flush=True)
                failures.append((season, week))
                break
            time.sleep(PAUSE_S)
        else:
            df = pl.concat(frames, how="vertical")
            print(f"{season}: {df.height:,} rows over {df['week'].n_unique()} weeks", flush=True)
            save_season(df, bucket_name, season)
    if failures:
        print(f"FAILED weeks: {failures}", flush=True)
        sys.exit(1)
    print("done", flush=True)


if __name__ == "__main__":
    main()
