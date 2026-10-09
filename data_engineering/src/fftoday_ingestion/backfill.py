"""Backfill FFToday's weekly projections, 2010 on, to ``bronze/fftoday/projections/season=<Y>/data.parquet``
(one file per season: 18 weeks x QB / RB / WR / TE). A LOCAL, run-by-hand job in the politeness style of
the other scrapers (a gaussian-jittered pause between pages, long breaks, backoff on errors): about 70
pages a season, ~5 minutes a season at the default pace.

    FFT_SEASONS=2010-2017 GCS_BUCKET_NAME=nfl-data-bronze python -m fftoday_ingestion.backfill
    FFT_SEASONS=current ...                         # the season in progress (re-run weekly in season)
    FFT_DRY_RUN=1 ...                                # parse and report, write nothing

The model's consensus group reads the result as the pre-2018 weekly consensus (BACKLOG 38).
"""
from __future__ import annotations

import os
import random
import sys
import time
from datetime import datetime, timezone

import polars as pl
import requests

from fftoday_ingestion._parse import POSITIONS, page_url, parse_week_page

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) fantasy-analysis/1.0",
           "Accept-Language": "en-US,en;q=0.9"}
WEEKS = range(1, 19)


def resolve_seasons(spec: str | None, today: datetime | None = None) -> list[int]:
    today = today or datetime.now(timezone.utc)
    spec = (spec or "current").strip().lower()
    if spec == "current":
        return [today.year if today.month >= 3 else today.year - 1]
    if "-" in spec:
        a, b = spec.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(s) for s in spec.split(",") if s.strip()]


def fetch(url: str, session: requests.Session, retries: int = 4) -> str:
    for attempt in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 200:
                return r.text
            print(f"  status {r.status_code} on {url}", flush=True)
        except requests.RequestException as e:
            print(f"  {e} on {url}", flush=True)
        time.sleep(30 * (attempt + 1))
    raise RuntimeError(f"no page after {retries} attempts: {url}")


def pause(pages_done: int) -> None:
    time.sleep(max(1.5, random.gauss(4.0, 1.2)))
    if pages_done % 25 == 0:
        time.sleep(random.uniform(20, 40))


def scrape_season(season: int, session: requests.Session) -> pl.DataFrame:
    frames, done = [], 0
    for week in WEEKS:
        for pos in POSITIONS:
            df = parse_week_page(fetch(page_url(season, week, pos), session), season, week, pos)
            frames.append(df)
            done += 1
            pause(done)
        print(f"  {season} week {week}: {sum(f.height for f in frames[-len(POSITIONS):])} rows", flush=True)
    return pl.concat(frames, how="vertical").with_columns(pl.lit(datetime.now(timezone.utc).replace(tzinfo=None)).alias("loaded_at"))


def save_season(df: pl.DataFrame, bucket_name: str, season: int) -> str:
    path = f"gs://{bucket_name.replace('gs://', '')}/bronze/fftoday/projections/season={season}/data.parquet"
    df.write_parquet(path)
    print(f"Saved fftoday projections {season} ({df.height:,} rows) to {path}", flush=True)
    return path


def main() -> None:
    bucket = os.environ.get("GCS_BUCKET_NAME")
    dry = os.environ.get("FFT_DRY_RUN", "").lower() in ("1", "true", "yes")
    if not bucket and not dry:
        raise ValueError("GCS_BUCKET_NAME environment variable is not set (or FFT_DRY_RUN=1)")
    seasons = resolve_seasons(os.environ.get("FFT_SEASONS"))
    with requests.Session() as session:
        for season in seasons:
            print(f"season {season}", flush=True)
            df = scrape_season(season, session)
            if df.height == 0:
                print(f"  {season}: no projections served (the site starts at 2010)", flush=True)
                continue
            if dry:
                print(f"  {season}: {df.height:,} rows, {df['week'].n_unique()} weeks, {df['fft_id'].n_unique():,} players (dry run)", flush=True)
            else:
                save_season(df, bucket, season)
    print("done", flush=True)


if __name__ == "__main__":
    sys.exit(main())
