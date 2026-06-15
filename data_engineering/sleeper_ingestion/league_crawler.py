"""Sleeper league crawler — broad discovery harvest.

Breadth-first walk of the Sleeper user/league graph, seeded from the users already
in our dataset (the franchise owners). For each user we pull their leagues for a
target season, and for each new league we pull its rosters (which give both the
player "bags" and the owner_ids that feed the next BFS layer). This harvests a large
connected component of real leagues that we can later layer KTC values onto.

Two API calls per step (both unauthenticated, documented public endpoints):
  GET /v1/user/<user_id>/leagues/nfl/<season>   -> full league objects (settings etc.)
  GET /v1/league/<league_id>/rosters            -> rosters (players + owner_id fan-out)

Sleeper asks callers to stay under 1000 requests/minute; we hold a steady rate well
below that and back off on 429/5xx. Stops at CRAWL_MAX_LEAGUES leagues.

Output: a distinct top-level prefix, deliberately NOT under bronze/sleeper/ so it can
never be swept into the curated 3-league ingestion or the silver builders:
  bronze/sleeper_crawl/leagues/load_date=<DATE>/data.parquet
  bronze/sleeper_crawl/rosters/load_date=<DATE>/data.parquet
  bronze/sleeper_crawl/users/load_date=<DATE>/data.parquet
A local mirror is also written to analysis/_cache/sleeper_crawl/ as a crash-safe checkpoint.

Env: GCS_BUCKET_NAME (default nfl-data-bronze), CRAWL_SEASON (2025),
     CRAWL_MAX_LEAGUES (10000), CRAWL_RATE_PER_MIN (650), CRAWL_NO_GCS (skip GCS write).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import time
from collections import deque
from pathlib import Path

import polars as pl
import requests
from google.cloud import storage

BASE = "https://api.sleeper.app/v1"
BUCKET = os.environ.get("GCS_BUCKET_NAME", "nfl-data-bronze")
SEASON = os.environ.get("CRAWL_SEASON", "2025")
MAX_LEAGUES = int(os.environ.get("CRAWL_MAX_LEAGUES", "10000"))
RATE_PER_MIN = int(os.environ.get("CRAWL_RATE_PER_MIN", "650"))  # << Sleeper's 1000/min ceiling
NO_GCS = os.environ.get("CRAWL_NO_GCS") == "1"
LOAD_DATE = dt.date.today().isoformat()
LOCAL_DIR = Path(__file__).resolve().parents[2] / "analysis" / "_cache" / "sleeper_crawl"


class _Rate:
    """Steady min-interval limiter -> ~RATE_PER_MIN requests/minute."""

    def __init__(self, per_min: int):
        self.interval = 60.0 / max(per_min, 1)
        self.last = 0.0

    def wait(self):
        gap = time.monotonic() - self.last
        if gap < self.interval:
            time.sleep(self.interval - gap)
        self.last = time.monotonic()


_rate = _Rate(RATE_PER_MIN)
_calls = 0


def _get(url: str, tries: int = 5):
    """GET with rate-limiting + exponential backoff on 429/5xx. None on hard miss."""
    global _calls
    for t in range(tries):
        _rate.wait()
        _calls += 1
        try:
            r = requests.get(url, timeout=20)
        except requests.RequestException:
            time.sleep(min(2 ** t, 30))
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                return None
        if r.status_code == 404:
            return None
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(2 ** t, 30))
            continue
        return None  # 4xx other than 404/429
    return None


def _seed_users() -> list[str]:
    """Franchise owners already in our silver dataset."""
    fr = pl.read_parquet(f"gs://{BUCKET}/silver/fantasy/dim_franchises_meta/data.parquet")
    return (fr.filter(pl.col("owner_id").is_not_null())["owner_id"]
            .cast(pl.Utf8).unique().drop_nulls().to_list())


def _league_row(lg: dict) -> dict:
    return {
        "league_id": str(lg.get("league_id")),
        "name": lg.get("name"),
        "season": lg.get("season"),
        "season_type": lg.get("season_type"),
        "sport": lg.get("sport"),
        "status": lg.get("status"),
        "total_rosters": lg.get("total_rosters"),
        "previous_league_id": lg.get("previous_league_id"),
        "draft_id": lg.get("draft_id"),
        "settings": json.dumps(lg.get("settings")),
        "scoring_settings": json.dumps(lg.get("scoring_settings")),
        "roster_positions": json.dumps(lg.get("roster_positions")),
    }


def _roster_row(lid: str, r: dict) -> dict:
    return {
        "league_id": lid,
        "roster_id": r.get("roster_id"),
        "owner_id": r.get("owner_id"),
        "co_owners": json.dumps(r.get("co_owners")),
        "players": json.dumps(r.get("players")),
        "starters": json.dumps(r.get("starters")),
        "reserve": json.dumps(r.get("reserve")),
        "taxi": json.dumps(r.get("taxi")),
        "keepers": json.dumps(r.get("keepers")),
        "settings": json.dumps(r.get("settings")),
    }


def _write(leagues: list[dict], rosters: list[dict], users: list[str], tag: str):
    """Persist current accumulation. Local always; GCS unless NO_GCS or final-only."""
    lg_df = pl.DataFrame(leagues) if leagues else pl.DataFrame()
    ro_df = pl.DataFrame(rosters) if rosters else pl.DataFrame()
    us_df = pl.DataFrame({"user_id": users}) if users else pl.DataFrame()
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    lg_df.write_parquet(LOCAL_DIR / "leagues.parquet")
    ro_df.write_parquet(LOCAL_DIR / "rosters.parquet")
    us_df.write_parquet(LOCAL_DIR / "users.parquet")
    if tag == "final" and not NO_GCS:
        root = f"gs://{BUCKET}/bronze/sleeper_crawl"
        lg_df.write_parquet(f"{root}/leagues/load_date={LOAD_DATE}/data.parquet")
        ro_df.write_parquet(f"{root}/rosters/load_date={LOAD_DATE}/data.parquet")
        us_df.write_parquet(f"{root}/users/load_date={LOAD_DATE}/data.parquet")
        print(f"  wrote GCS: {root}/{{leagues,rosters,users}}/load_date={LOAD_DATE}/data.parquet", flush=True)


def crawl():
    seed = _seed_users()
    print(f"seed users={len(seed)} | season={SEASON} | cap={MAX_LEAGUES} | rate~{RATE_PER_MIN}/min", flush=True)
    leagues: dict[str, dict] = {}
    rosters: list[dict] = []
    visited_users: set[str] = set()
    q: deque[str] = deque(seed)
    t0 = time.monotonic()

    while q and len(leagues) < MAX_LEAGUES:
        uid = q.popleft()
        if uid in visited_users:
            continue
        visited_users.add(uid)
        for lg in _get(f"{BASE}/user/{uid}/leagues/nfl/{SEASON}") or []:
            lid = str(lg.get("league_id"))
            if not lid or lid in leagues:
                continue
            if len(leagues) >= MAX_LEAGUES:
                break
            leagues[lid] = _league_row(lg)
            for r in _get(f"{BASE}/league/{lid}/rosters") or []:
                rosters.append(_roster_row(lid, r))
                oid = r.get("owner_id")
                if oid and oid not in visited_users:
                    q.append(str(oid))
                for co in (r.get("co_owners") or []):
                    if co and co not in visited_users:
                        q.append(str(co))
            n = len(leagues)
            if n % 200 == 0:
                el = time.monotonic() - t0
                print(f"  leagues={n:>5} rosters={len(rosters):>6} users_done={len(visited_users):>5} "
                      f"queue={len(q):>5} calls={_calls:>6} {el/60:.1f}m ({_calls/el*60:.0f}/min)", flush=True)
            if n % 2000 == 0:
                _write(list(leagues.values()), rosters, sorted(visited_users), "checkpoint")
                print(f"  [checkpoint] local mirror written at {n} leagues", flush=True)

    all_users = sorted(visited_users | {r["owner_id"] for r in rosters if r["owner_id"]})
    el = time.monotonic() - t0
    print(f"\nDONE: {len(leagues)} leagues, {len(rosters)} rosters, {len(all_users)} users, "
          f"{_calls} calls in {el/60:.1f}m", flush=True)
    _write(list(leagues.values()), rosters, all_users, "final")
    return leagues, rosters, all_users


if __name__ == "__main__":
    crawl()
