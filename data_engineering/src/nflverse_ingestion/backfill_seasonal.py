"""Keep every seasonal nflverse dataset's history complete: one ``season=YYYY/data.parquet`` per season.

Two modes, one module:

* **Reconcile (scheduled, default)** -- for every seasonal dataset in ``DATASETS_CONFIG`` that
  declares a ``start_season``, list the season partitions already in the lake and load only the
  missing ones, from ``start_season`` up to the season *before* the current one (the daily job
  owns the current season). When nothing is missing it is a handful of list calls and exits 0,
  so it runs in the daily DAG next to ``nflverse-daily`` and a new dataset (or a lost partition)
  heals itself on the next run instead of waiting for someone to run a command.
* **Explicit (manual)** -- ``DATASET=injuries START_SEASON=2009 [END_SEASON=2026] [OVERWRITE=true]``
  loads one dataset's range; existing partitions are skipped unless OVERWRITE is set.

``--dry-run`` reports the gaps without loading anything.

Why not ``full_ingestion``: it writes ``<name>.parquet`` next to the daily job's ``data.parquet``
(two files per partition, which already bit ``player_stats``). This job writes the daily path,
and "partition exists" means *any* object under ``season=YYYY/``, so it never creates a twin.

Frames are written raw, as the daily job does. nflverse changes dtypes between seasons (the
injuries ``week``/``season`` columns are Float64 in older files and Int32 in recent ones), so
readers should concat partitions with ``how="diagonal_relaxed"`` and cast the keys.

Usage:
    python -m nflverse_ingestion.backfill_seasonal                      # reconcile all (the DAG step)
    python -m nflverse_ingestion.backfill_seasonal --dry-run            # just list the gaps
    DATASET=injuries START_SEASON=2009 python -m nflverse_ingestion.backfill_seasonal
    gcloud run jobs execute nflverse-backfill-seasonal --region us-central1 \\
        --update-env-vars DATASET=snap_counts,START_SEASON=2012,OVERWRITE=true
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime

import nflreadpy as nfl
from dotenv import load_dotenv

from nflverse_ingestion.daily_ingestion import DATASETS_CONFIG

load_dotenv()

BRONZE_ROOT = "bronze/nflverse"
_SEASON_RE = re.compile(r"/season=(\d{4})/")


def season_path(bucket: str, folder: str, season: int) -> str:
    """The partition file the daily job writes, so backfill and daily share one file per season."""
    return f"gs://{bucket}/{BRONZE_ROOT}/{folder}/season={season}/data.parquet"


def existing_seasons(bucket: str, folder: str) -> set[int]:
    """Seasons that already have ANY object under ``season=YYYY/`` (daily data.parquet or a
    full-load ``<name>.parquet``): one list call per dataset."""
    from google.cloud import storage

    prefix = f"{BRONZE_ROOT}/{folder}/season="
    found: set[int] = set()
    for blob in storage.Client().list_blobs(bucket, prefix=prefix):
        m = _SEASON_RE.search("/" + blob.name)
        if m:
            found.add(int(m.group(1)))
    return found


def seasonal_datasets() -> dict[str, dict]:
    return {k: v for k, v in DATASETS_CONFIG.items() if v.get("seasonal")}


def seasonal_dataset(name: str) -> dict:
    cfg = seasonal_datasets().get(name)
    if cfg is None:
        raise ValueError(f"{name!r} is not a seasonal dataset in DATASETS_CONFIG; choose one of {sorted(seasonal_datasets())}")
    return cfg


def load_season(name: str, cfg: dict, season: int, bucket: str, tolerate_empty: bool = False) -> dict:
    """Load one season and write its partition. Returns a result row (never raises).

    ``tolerate_empty``: an upstream season with no rows (nflverse publishes an empty file for
    some early seasons) is reported as ``empty`` instead of failed, so a scheduled reconcile does
    not fail the DAG every morning over a season that will never have data."""
    result = {"name": name, "season": season, "success": False, "rows": 0, "skipped": False, "error": None}
    path = season_path(bucket, cfg["folder"], season)
    try:
        df = cfg["loader"]([season])
        if df is None or df.height == 0:
            result["error"] = "No data returned"
            if tolerate_empty:
                result["empty"] = True
                print(f"  ○ {name} {season}: upstream has no rows (not an error; retried next run)")
            else:
                print(f"  ✗ {name} {season}: no data")
            return result
        df.write_parquet(path)
        result.update(success=True, rows=df.height)
        span = f", weeks {df['week'].min()}-{df['week'].max()}" if "week" in df.columns else ""
        print(f"  ✓ {name} {season}: {df.height:,} rows{span} → {path}")
    except Exception as e:  # noqa: BLE001 - one bad season must not stop the others
        result["error"] = str(e)
        print(f"  ✗ {name} {season}: {e}")
    return result


def _skipped(name: str, season: int) -> dict:
    return {"name": name, "season": season, "success": True, "rows": 0, "skipped": True, "error": None}


def backfill(name: str, start_season: int, end_season: int, bucket: str, overwrite: bool = False,
             dry_run: bool = False, existing=existing_seasons) -> list[dict]:
    """Explicit mode: one dataset, one season range."""
    cfg = seasonal_dataset(name)
    if start_season > end_season:
        raise ValueError(f"start season {start_season} is after end season {end_season}")
    have = set() if overwrite else existing(bucket, cfg["folder"])
    print(f"→ {name} {start_season}-{end_season}{' (overwrite)' if overwrite else ''}"
          f"{' [dry run]' if dry_run else ''} → gs://{bucket}/{BRONZE_ROOT}/{cfg['folder']}/season=YYYY/data.parquet")
    results = []
    for season in range(start_season, end_season + 1):
        if season in have:
            print(f"  ○ {name} {season}: partition exists, skipped (OVERWRITE=true to replace)")
            results.append(_skipped(name, season))
        elif dry_run:
            print(f"  · {name} {season}: missing (would load)")
            results.append({**_skipped(name, season), "skipped": False, "dry_run": True})
        else:
            results.append(load_season(name, cfg, season, bucket))
    return results


def reconcile(bucket: str, current_season: int, dry_run: bool = False, existing=existing_seasons) -> list[dict]:
    """Scheduled mode: fill every missing season < current_season of every seasonal dataset
    that declares ``start_season``. The current season belongs to the daily job."""
    results = []
    for name, cfg in sorted(seasonal_datasets().items()):
        start = cfg.get("start_season")
        if start is None:
            print(f"○ {name}: no start_season in DATASETS_CONFIG, not reconciled")
            continue
        wanted = range(start, current_season)
        have = existing(bucket, cfg["folder"])
        missing = [s for s in wanted if s not in have]
        if not missing:
            print(f"✓ {name}: {start}-{current_season - 1} complete ({len(have)} partitions)")
            continue
        print(f"→ {name}: missing {missing}{' [dry run]' if dry_run else ''}")
        for season in missing:
            if dry_run:
                results.append({"name": name, "season": season, "success": True, "rows": 0, "skipped": False, "error": None, "dry_run": True})
            else:
                results.append(load_season(name, cfg, season, bucket, tolerate_empty=True))
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env = os.environ
    ap = argparse.ArgumentParser(description="Reconcile (default) or explicitly backfill seasonal nflverse datasets.")
    ap.add_argument("--dataset", default=env.get("DATASET"), help="explicit mode: key in DATASETS_CONFIG (env DATASET)")
    ap.add_argument("--start-season", type=int, default=int(env["START_SEASON"]) if env.get("START_SEASON") else None,
                    help="explicit mode: first season (env START_SEASON)")
    ap.add_argument("--end-season", type=int, default=int(env["END_SEASON"]) if env.get("END_SEASON") else None,
                    help="explicit mode: last season, default = current NFL season (env END_SEASON)")
    ap.add_argument("--overwrite", action="store_true", default=env.get("OVERWRITE", "false").lower() == "true",
                    help="explicit mode: replace partitions that already exist (env OVERWRITE=true)")
    ap.add_argument("--dry-run", action="store_true", default=env.get("DRY_RUN", "false").lower() == "true",
                    help="report gaps, load nothing (env DRY_RUN=true)")
    args = ap.parse_args(argv)
    if args.dataset and args.start_season is None:
        ap.error("--start-season / START_SEASON is required with --dataset / DATASET")
    if args.dataset is None and (args.start_season is not None or args.overwrite):
        ap.error("--start-season / --overwrite only apply with --dataset (reconcile mode takes no range)")
    return args


def main(argv: list[str] | None = None) -> None:
    bucket = os.environ.get("GCS_BUCKET_NAME")
    if not bucket:
        print("ERROR: GCS_BUCKET_NAME not set")
        sys.exit(2)
    args = parse_args(argv)
    current_season = nfl.get_current_season()
    mode = f"explicit backfill of {args.dataset}" if args.dataset else "reconcile all seasonal datasets"
    print("=" * 70)
    print(f"NFLVERSE SEASONAL HISTORY: {mode}{' [dry run]' if args.dry_run else ''}")
    print(f"Timestamp: {datetime.now().isoformat()} | current season {current_season} | bucket {bucket}")
    print("=" * 70)
    try:
        if args.dataset:
            end = args.end_season if args.end_season is not None else current_season
            results = backfill(args.dataset, args.start_season, end, bucket, args.overwrite, args.dry_run)
        else:
            results = reconcile(bucket, current_season, args.dry_run)
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(2)

    loaded = [r for r in results if r["success"] and not r["skipped"] and not r.get("dry_run")]
    would = [r for r in results if r.get("dry_run")]
    skipped = [r for r in results if r["skipped"]]
    empty = [r for r in results if r.get("empty")]
    failed = [r for r in results if not r["success"] and not r.get("empty")]
    print()
    print("=" * 70)
    print(f"SUMMARY: {len(loaded)} loaded ({sum(r['rows'] for r in loaded):,} rows) | {len(would)} would load"
          f" | {len(skipped)} skipped | {len(empty)} empty upstream | {len(failed)} failed")
    print("=" * 70)
    if failed:
        print("\nFailures:")
        for r in failed:
            print(f"  • {r['name']} {r['season']}: {r['error']}")
        sys.exit(2 if not loaded and not skipped else 1)
    print("\n✓ Done")
    sys.exit(0)


if __name__ == "__main__":
    main()
