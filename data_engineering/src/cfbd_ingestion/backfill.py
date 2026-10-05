"""Backfill College Football Data into bronze, one parquet per (dataset, season).

    PYTHONPATH=src uv run python -m cfbd_ingestion.backfill --start 2010 --end 2025
    PYTHONPATH=src uv run python -m cfbd_ingestion.backfill --datasets draft_picks --start 2010 --end 2026 --force

Datasets (bronze/cfbd/<dataset>/season=YYYY/data.parquet):
  player_season_stats  /stats/player/season   one row per (player, category, stat): passing / rushing / receiving ...
  player_usage         /player/usage          one row per player: share of the team's plays (overall, pass, rush, by down)
  rosters              /roster                one row per player: team, position, class year, height, weight, hometown
  team_sp              /ratings/sp            one row per team: SP+ overall / offense / defense rating and rank
  team_season_stats    /stats/season          one row per (team, stat): plays, yards, attempts ... (team volume for shares)
  teams                /teams/fbs             one row per team: conference, classification
  draft_picks          /draft/picks           one row per NFL draft pick: college athlete id, college team, round, overall pick
                                              (the crosswalk from CFBD ids to the NFL side by draft year + overall pick)

Every row is tagged with ``season`` and ``loaded_at``. Existing (dataset, season) files are skipped
unless ``--force``. The API key comes from ``CFBD_API_KEY``; the bucket from ``GCS_BUCKET_NAME``.
"""
from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone

import polars as pl
import requests
from dotenv import load_dotenv

from cfbd_ingestion import client

load_dotenv()

BUCKET = os.environ.get("GCS_BUCKET_NAME", "nfl-data-bronze")
PREFIX = "bronze/cfbd"
DATASETS = ["player_season_stats", "player_usage", "rosters", "team_sp", "team_season_stats", "teams", "draft_picks"]


def _flat(d: dict, prefix: str = "") -> dict:
    """Flatten nested dicts one level deep with underscore keys (usage.overall -> usage_overall)."""
    out = {}
    for k, v in d.items():
        if isinstance(v, dict):
            for k2, v2 in v.items():
                if isinstance(v2, dict):
                    for k3, v3 in v2.items():
                        out[f"{prefix}{k}_{k2}_{k3}"] = v3
                else:
                    out[f"{prefix}{k}_{k2}"] = v2
        elif isinstance(v, list):
            out[f"{prefix}{k}"] = str(v) if v else None
        else:
            out[f"{prefix}{k}"] = v
    return out


def fetch(dataset: str, season: int, session: requests.Session | None = None) -> pl.DataFrame:
    """One dataset for one season, flattened to a frame (empty frame when the API has nothing)."""
    if dataset == "player_season_stats":
        rows = client.get("/stats/player/season", {"year": season, "seasonType": "regular"}, session=session)
    elif dataset == "player_usage":
        rows = client.get("/player/usage", {"year": season, "excludeGarbageTime": "false"}, session=session)
    elif dataset == "rosters":
        rows = client.get("/roster", {"year": season}, session=session)
    elif dataset == "team_sp":
        rows = client.get("/ratings/sp", {"year": season}, session=session)
    elif dataset == "team_season_stats":
        rows = client.get("/stats/season", {"year": season}, session=session)
    elif dataset == "teams":
        rows = client.get("/teams/fbs", {"year": season}, session=session)
    elif dataset == "draft_picks":
        rows = client.get("/draft/picks", {"year": season}, session=session)
    else:
        raise ValueError(dataset)
    flat = [_flat(r) for r in rows] if isinstance(rows, list) else []
    df = pl.DataFrame(flat, infer_schema_length=None) if flat else pl.DataFrame()
    return tag(df, season)


def tag(df: pl.DataFrame, season: int) -> pl.DataFrame:
    now = datetime.now(timezone.utc)
    if df.is_empty():
        return df
    # every column that is entirely null infers as Null dtype; cast those to Utf8 so partitions agree
    df = df.with_columns([pl.col(c).cast(pl.Utf8) for c, dt in zip(df.columns, df.dtypes) if dt == pl.Null])
    return df.with_columns(pl.lit(season).cast(pl.Int64).alias("season"), pl.lit(now).alias("loaded_at"))


def out_path(dataset: str, season: int) -> str:
    return f"gs://{BUCKET}/{PREFIX}/{dataset}/season={season}/data.parquet"


def exists(path: str) -> bool:
    try:
        pl.read_parquet(path, n_rows=1)
        return True
    except Exception:  # noqa: BLE001
        return False


def run(datasets: list[str], start: int, end: int, force: bool = False, sleep_s: float = 1.0) -> list[str]:
    written = []
    session = requests.Session()
    for dataset in datasets:
        for season in range(start, end + 1):
            path = out_path(dataset, season)
            if not force and exists(path):
                print(f"skip {dataset} {season} (exists)")
                continue
            try:
                df = fetch(dataset, season, session=session)
            except requests.HTTPError as e:  # noqa: PERF203
                print(f"  {dataset} {season}: HTTP {e.response.status_code if e.response is not None else '?'}; skipped")
                continue
            if df.is_empty():
                print(f"  {dataset} {season}: no rows")
                continue
            df.write_parquet(path)
            written.append(path)
            print(f"wrote {path} ({df.height} rows, {df.width} cols)")
            time.sleep(sleep_s)
    return written


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", default=",".join(DATASETS))
    ap.add_argument("--start", type=int, default=2010)
    ap.add_argument("--end", type=int, default=datetime.now().year)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    client.api_key()
    run([d for d in args.datasets.split(",") if d], args.start, args.end, force=args.force)


if __name__ == "__main__":
    main()
