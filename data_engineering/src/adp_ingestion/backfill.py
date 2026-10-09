"""Average draft position by season: the preseason consensus with no survivorship (every player taken
in a draft appears, every year), the model's ``preseason`` group (BACKLOG 38). Two public sources:

* Fantasy Football Calculator's API (``/api/v1/adp/<format>?teams=12&year=``): standard scoring
  2009-2011, PPR 2012 on; ~200 players and hundreds to a thousand mock drafts a year; name + position.
* MyFantasyLeague's ADP export (``/<year>/export?TYPE=adp``): 2011 on, 320-460 players and
  thousands of drafts a year; MFL player ids, which nflverse's ``fantasy_player_ids`` maps to gsis.

Writes ``bronze/adp/<source>/season=<Y>/data.parquet``. A yearly job (the preseason view is final once
the season starts): ``ADP_SEASONS=current`` (default) or ``2009-2026`` for the one-time backfill;
``ADP_DRY_RUN=1`` parses and reports, writes nothing.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

import polars as pl
import requests
from dotenv import load_dotenv

load_dotenv()

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) fantasy-analysis/1.0"}
FFC_FIRST, MFL_FIRST = 2009, 2011
FFC_SCHEMA = {"season": pl.Int64, "scoring": pl.Utf8, "name": pl.Utf8, "position": pl.Utf8, "team": pl.Utf8, "adp": pl.Float64, "times_drafted": pl.Int64,
              "bye": pl.Int64, "ffc_id": pl.Utf8, "total_drafts": pl.Int64, "loaded_at": pl.Datetime("us")}
MFL_SCHEMA = {"season": pl.Int64, "mfl_id": pl.Utf8, "adp": pl.Float64, "rank": pl.Int64, "drafts_selected": pl.Int64, "min_pick": pl.Int64, "max_pick": pl.Int64,
              "total_drafts": pl.Int64, "loaded_at": pl.Datetime("us")}


def ffc_format(season: int) -> str:
    return "standard" if season < 2012 else "ppr"


def flatten_ffc(payload: dict, season: int, loaded_at: datetime | None = None) -> pl.DataFrame:
    loaded_at = loaded_at or datetime.now(timezone.utc).replace(tzinfo=None)
    total = (payload.get("meta") or {}).get("total_drafts")
    rows = [{"season": season, "scoring": ffc_format(season), "name": p.get("name"), "position": p.get("position"), "team": p.get("team"),
             "adp": p.get("adp"), "times_drafted": p.get("times_drafted"), "bye": p.get("bye"), "ffc_id": None if p.get("player_id") is None else str(p.get("player_id")),
             "total_drafts": total, "loaded_at": loaded_at} for p in payload.get("players") or [] if p.get("name")]
    return pl.DataFrame(rows, schema_overrides=FFC_SCHEMA).select(list(FFC_SCHEMA)) if rows else pl.DataFrame(schema=FFC_SCHEMA)


def flatten_mfl(payload: dict, season: int, loaded_at: datetime | None = None) -> pl.DataFrame:
    loaded_at = loaded_at or datetime.now(timezone.utc).replace(tzinfo=None)
    adp = payload.get("adp") or {}
    total = int(adp.get("totalDrafts") or 0)
    rows = []
    for p in adp.get("player") or []:
        try:
            rows.append({"season": season, "mfl_id": str(p.get("id")), "adp": float(p.get("averagePick")), "rank": int(p.get("rank") or 0) or None,
                         "drafts_selected": int(p.get("draftsSelectedIn") or 0), "min_pick": int(p.get("minPick") or 0) or None, "max_pick": int(p.get("maxPick") or 0) or None,
                         "total_drafts": total, "loaded_at": loaded_at})
        except (TypeError, ValueError):
            continue
    return pl.DataFrame(rows, schema_overrides=MFL_SCHEMA).select(list(MFL_SCHEMA)) if rows else pl.DataFrame(schema=MFL_SCHEMA)


def fetch_ffc(season: int, session: requests.Session) -> dict:
    r = session.get(f"https://fantasyfootballcalculator.com/api/v1/adp/{ffc_format(season)}?teams=12&year={season}", headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def fetch_mfl(season: int, session: requests.Session) -> dict:
    r = session.get(f"https://api.myfantasyleague.com/{season}/export", params={"TYPE": "adp", "PERIOD": "ALL", "FCOUNT": 12, "IS_PPR": 1, "IS_KEEPER": "N", "JSON": 1},
                    headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def resolve_seasons(spec: str | None, today: datetime | None = None) -> list[int]:
    today = today or datetime.now(timezone.utc)
    spec = (spec or "current").strip().lower()
    if spec == "current":
        return [today.year if today.month >= 3 else today.year - 1]
    if "-" in spec:
        a, b = spec.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(s) for s in spec.split(",") if s.strip()]


def save(df: pl.DataFrame, bucket_name: str, source: str, season: int) -> str:
    path = f"gs://{bucket_name.replace('gs://', '')}/bronze/adp/{source}/season={season}/data.parquet"
    df.write_parquet(path)
    print(f"Saved adp/{source} {season} ({df.height:,} rows) to {path}", flush=True)
    return path


def main() -> None:
    bucket = os.environ.get("GCS_BUCKET_NAME")
    dry = os.environ.get("ADP_DRY_RUN", "").lower() in ("1", "true", "yes")
    if not bucket and not dry:
        raise ValueError("GCS_BUCKET_NAME environment variable is not set (or ADP_DRY_RUN=1)")
    failures = []
    with requests.Session() as session:
        for season in resolve_seasons(os.environ.get("ADP_SEASONS")):
            for source, first, fetch, flatten in (("ffc", FFC_FIRST, fetch_ffc, flatten_ffc), ("mfl", MFL_FIRST, fetch_mfl, flatten_mfl)):
                if season < first:
                    continue
                try:
                    df = flatten(fetch(season, session), season)
                except Exception as e:  # noqa: BLE001
                    print(f"  {source} {season}: FAILED {e}", flush=True); failures.append((source, season)); continue
                if df.height == 0:
                    print(f"  {source} {season}: no rows served", flush=True); continue
                if dry:
                    print(f"  {source} {season}: {df.height} players (dry run)", flush=True)
                else:
                    save(df, bucket, source, season)
                time.sleep(1.5)
    if failures:
        print(f"FAILED: {failures}", flush=True); sys.exit(1)
    print("done", flush=True)


if __name__ == "__main__":
    main()
