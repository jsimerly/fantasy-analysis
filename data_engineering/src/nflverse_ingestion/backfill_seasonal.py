"""Backfill one seasonal nflverse dataset into the lake: one ``season=YYYY/data.parquet`` per season.

Why this exists: ``daily_ingestion`` only (re)writes the *current* season of each dataset in its
``DATASETS_CONFIG``, so history has to be loaded once. ``full_ingestion`` does that for a fixed
list, but it writes ``<name>.parquet`` next to the daily job's ``data.parquet`` (two files per
partition, which already bit ``player_stats``). This job writes exactly the path the daily job
writes, so every season ends up with one file and the daily job keeps the current season fresh.

It never overwrites a partition that already has a file unless ``OVERWRITE=true`` (the lake has
no backups), and it loads seasons one at a time so a season nflverse cannot serve is recorded
while the rest still load.

Usage (env vars, so the Cloud Run job can be driven with ``--update-env-vars``):

    GCS_BUCKET_NAME=nfl-data-bronze DATASET=injuries START_SEASON=2009 \\
        python -m nflverse_ingestion.backfill_seasonal
    # CLI flags work too and win over the env:
    python -m nflverse_ingestion.backfill_seasonal --dataset injuries --start-season 2009 [--end-season 2026] [--overwrite]
    # Cloud Run (execution-scoped overrides, nothing persists on the job):
    gcloud run jobs execute nflverse-backfill-seasonal --region us-central1 \\
        --update-env-vars DATASET=injuries,START_SEASON=2009

Frames are written raw, as the daily job does. nflverse changes dtypes between seasons (the
injuries ``week``/``season`` columns are Float64 in older files and Int32 in recent ones, and
``date_modified`` exists only in older seasons), so readers should concat partitions with
``how="diagonal_relaxed"`` and cast the keys.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import nflreadpy as nfl
from dotenv import load_dotenv

from nflverse_ingestion.daily_ingestion import DATASETS_CONFIG

load_dotenv()

BRONZE_ROOT = "bronze/nflverse"


def season_path(bucket: str, folder: str, season: int) -> str:
    """The partition file the daily job writes, so backfill and daily share one file per season."""
    return f"gs://{bucket}/{BRONZE_ROOT}/{folder}/season={season}/data.parquet"


def partition_exists(bucket: str, folder: str, season: int) -> bool:
    from google.cloud import storage

    key = season_path(bucket, folder, season).split(f"gs://{bucket}/", 1)[1]
    return storage.Client().bucket(bucket).blob(key).exists()


def seasonal_dataset(name: str) -> dict:
    cfg = DATASETS_CONFIG.get(name)
    if cfg is None or not cfg.get("seasonal"):
        seasonal = sorted(k for k, v in DATASETS_CONFIG.items() if v.get("seasonal"))
        raise ValueError(f"{name!r} is not a seasonal dataset in DATASETS_CONFIG; choose one of {seasonal}")
    return cfg


def backfill_season(name: str, cfg: dict, season: int, bucket: str, overwrite: bool,
                    exists=partition_exists) -> dict:
    """Load one season and write its partition. Returns a result row (never raises)."""
    result = {"name": name, "season": season, "success": False, "rows": 0, "skipped": False, "error": None}
    path = season_path(bucket, cfg["folder"], season)
    try:
        if not overwrite and exists(bucket, cfg["folder"], season):
            result.update(success=True, skipped=True)
            print(f"  ○ {season}: partition exists, skipped (OVERWRITE=true to replace) {path}")
            return result
        df = cfg["loader"]([season])
        if df is None or df.height == 0:
            result["error"] = "No data returned"
            print(f"  ✗ {season}: no data")
            return result
        df.write_parquet(path)
        result.update(success=True, rows=df.height)
        span = f", weeks {df['week'].min()}-{df['week'].max()}" if "week" in df.columns else ""
        print(f"  ✓ {season}: {df.height:,} rows{span} → {path}")
    except Exception as e:  # noqa: BLE001 - one bad season must not stop the others
        result["error"] = str(e)
        print(f"  ✗ {season}: {e}")
    return result


def backfill(name: str, start_season: int, end_season: int, bucket: str, overwrite: bool = False,
             exists=partition_exists) -> list[dict]:
    cfg = seasonal_dataset(name)
    if start_season > end_season:
        raise ValueError(f"start season {start_season} is after end season {end_season}")
    print("=" * 70)
    print(f"NFLVERSE SEASONAL BACKFILL: {name} {start_season}-{end_season}"
          f"{' (overwrite)' if overwrite else ''} → gs://{bucket}/{BRONZE_ROOT}/{cfg['folder']}/season=YYYY/data.parquet")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 70)
    return [backfill_season(name, cfg, s, bucket, overwrite, exists) for s in range(start_season, end_season + 1)]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env = os.environ
    ap = argparse.ArgumentParser(description="Backfill one seasonal nflverse dataset, one data.parquet per season.")
    ap.add_argument("--dataset", default=env.get("DATASET"), help="key in daily_ingestion.DATASETS_CONFIG (env DATASET)")
    ap.add_argument("--start-season", type=int, default=int(env["START_SEASON"]) if env.get("START_SEASON") else None,
                    help="first season to load (env START_SEASON)")
    ap.add_argument("--end-season", type=int, default=int(env["END_SEASON"]) if env.get("END_SEASON") else None,
                    help="last season to load; default = the current NFL season (env END_SEASON)")
    ap.add_argument("--overwrite", action="store_true", default=env.get("OVERWRITE", "false").lower() == "true",
                    help="replace partitions that already have a file (env OVERWRITE=true)")
    args = ap.parse_args(argv)
    if not args.dataset or args.start_season is None:
        ap.error("--dataset / DATASET and --start-season / START_SEASON are required")
    return args


def main(argv: list[str] | None = None) -> None:
    bucket = os.environ.get("GCS_BUCKET_NAME")
    if not bucket:
        print("ERROR: GCS_BUCKET_NAME not set")
        sys.exit(2)
    args = parse_args(argv)
    end_season = args.end_season if args.end_season is not None else nfl.get_current_season()
    try:
        results = backfill(args.dataset, args.start_season, end_season, bucket, args.overwrite)
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(2)

    loaded = [r for r in results if r["success"] and not r["skipped"]]
    skipped = [r for r in results if r["skipped"]]
    failed = [r for r in results if not r["success"]]
    print()
    print("=" * 70)
    print(f"SUMMARY: {len(loaded)} loaded ({sum(r['rows'] for r in loaded):,} rows) | {len(skipped)} skipped | {len(failed)} failed")
    print("=" * 70)
    if failed:
        print("\nFailures:")
        for r in failed:
            print(f"  • {r['name']} {r['season']}: {r['error']}")
        sys.exit(2 if not loaded and not skipped else 1)
    print("\n✓ Backfill complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
