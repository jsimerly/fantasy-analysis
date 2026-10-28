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
              "bye": pl.Int64, "ffc_id": pl.Utf8, "total_drafts": pl.Int64, "start_date": pl.Utf8, "end_date": pl.Utf8, "loaded_at": pl.Datetime("us")}
# FFC serves ONE window per year: for a past year the frozen final preseason week (2025: 8,470 drafts, Aug 25 - Sep 1),
# for the current year whatever its latest window is -- in season, a few dozen mock drafts (2026-10-13: 109, Sep 18-25).
# A partition is written only from a preseason window: one that ends by September 10 with this many drafts.
FFC_MIN_DRAFTS = 300
FFC_WINDOW_END = "-09-10"
# MFL's ADP export takes a period: ALL keeps growing all season (startup leagues drafting in October see the season so
# far), AUG15 is the fixed late-preseason window (2025: 642 drafts, 2026: 605) -- the same view every year, frozen at kickoff.
MFL_PERIOD = "AUG15"
MFL_SCHEMA = {"season": pl.Int64, "period": pl.Utf8, "mfl_id": pl.Utf8, "adp": pl.Float64, "rank": pl.Int64, "drafts_selected": pl.Int64, "min_pick": pl.Int64, "max_pick": pl.Int64,
              "total_drafts": pl.Int64, "loaded_at": pl.Datetime("us")}


def ffc_format(season: int) -> str:
    return "standard" if season < 2012 else "ppr"


def flatten_ffc(payload: dict, season: int, loaded_at: datetime | None = None) -> pl.DataFrame:
    loaded_at = loaded_at or datetime.now(timezone.utc).replace(tzinfo=None)
    meta = payload.get("meta") or {}
    total, start, end = meta.get("total_drafts"), meta.get("start_date"), meta.get("end_date")
    rows = [{"season": season, "scoring": ffc_format(season), "name": p.get("name"), "position": p.get("position"), "team": p.get("team"),
             "adp": p.get("adp"), "times_drafted": p.get("times_drafted"), "bye": p.get("bye"), "ffc_id": None if p.get("player_id") is None else str(p.get("player_id")),
             "total_drafts": total, "start_date": start, "end_date": end, "loaded_at": loaded_at} for p in payload.get("players") or [] if p.get("name")]
    return pl.DataFrame(rows, schema_overrides=FFC_SCHEMA).select(list(FFC_SCHEMA)) if rows else pl.DataFrame(schema=FFC_SCHEMA)


def flatten_mfl(payload: dict, season: int, loaded_at: datetime | None = None) -> pl.DataFrame:
    loaded_at = loaded_at or datetime.now(timezone.utc).replace(tzinfo=None)
    adp = payload.get("adp") or {}
    total = int(adp.get("totalDrafts") or 0)
    rows = []
    for p in adp.get("player") or []:
        try:
            rows.append({"season": season, "period": MFL_PERIOD, "mfl_id": str(p.get("id")), "adp": float(p.get("averagePick")), "rank": int(p.get("rank") or 0) or None,
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
    r = session.get(f"https://api.myfantasyleague.com/{season}/export", params={"TYPE": "adp", "PERIOD": MFL_PERIOD, "FCOUNT": 12, "IS_PPR": 1, "IS_KEEPER": "N", "JSON": 1},
                    headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def resolve_seasons(spec: str | None, today: datetime | None = None) -> list[int]:
    today = today or datetime.now(timezone.utc)
    spec = (spec or "current").strip().lower()
    if spec == "current":
        return [today.year if today.month >= 3 else today.year - 1]
    if spec == "live":                 # every day: last season (the canary; FFC's frozen final week catches up) and this one
        return [today.year - 1, today.year]
    if "-" in spec:
        a, b = spec.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(s) for s in spec.split(",") if s.strip()]


def save(df: pl.DataFrame, bucket_name: str, source: str, season: int) -> str:
    path = f"gs://{bucket_name.replace('gs://', '')}/bronze/adp/{source}/season={season}/data.parquet"
    df.write_parquet(path)
    print(f"Saved adp/{source} {season} ({df.height:,} rows) to {path}", flush=True)
    return path


def ffc_window_is_preseason(df: pl.DataFrame, season: int) -> tuple[bool, str]:
    """Whether an FFC payload is the preseason view (a window ending by September 10 of its season with FFC_MIN_DRAFTS+
    drafts) and a one-line description of the window."""
    if df.height == 0:
        return False, "empty"
    r = df.row(0, named=True)
    n, end = r.get("total_drafts") or 0, r.get("end_date")
    desc = f"{n} drafts, window {r.get('start_date')} to {end}"
    return (n >= FFC_MIN_DRAFTS) and (end is None or str(end) <= f"{season}{FFC_WINDOW_END}"), desc


def not_published_yet(e: Exception, season: int, today: datetime | None = None) -> bool:
    """A 400 / 404 for a season whose games have not started: its drafts have not begun (FFC answers 400, MFL 404)."""
    today = today or datetime.now(timezone.utc)
    status = getattr(getattr(e, "response", None), "status_code", None)
    return status in (400, 404) and (season > today.year or (season == today.year and today.month < 9))


def main() -> None:
    bucket = os.environ.get("GCS_BUCKET_NAME")
    dry = os.environ.get("ADP_DRY_RUN", "").lower() in ("1", "true", "yes")
    if not bucket and not dry:
        raise ValueError("GCS_BUCKET_NAME environment variable is not set (or ADP_DRY_RUN=1)")
    failures, served = [], set()          # served: sources that answered for an earlier season this run (the canary)
    with requests.Session() as session:
        for season in resolve_seasons(os.environ.get("ADP_SEASONS")):
            for source, first, fetch, flatten in (("ffc", FFC_FIRST, fetch_ffc, flatten_ffc), ("mfl", MFL_FIRST, fetch_mfl, flatten_mfl)):
                if season < first:
                    continue
                try:
                    df = flatten(fetch(season, session), season)
                except Exception as e:  # noqa: BLE001
                    if not_published_yet(e, season) and source in served:       # an earlier season answered: the provider is up
                        print(f"  {source} {season}: not published yet", flush=True); continue
                    print(f"  {source} {season}: FAILED {e}", flush=True); failures.append((source, season)); continue
                served.add(source)
                if df.height == 0:
                    print(f"  {source} {season}: no rows served", flush=True); continue
                if source == "ffc":
                    ok, desc = ffc_window_is_preseason(df, season)
                    if not ok:
                        print(f"  ffc {season}: not a preseason window ({desc}), not written", flush=True); continue
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
