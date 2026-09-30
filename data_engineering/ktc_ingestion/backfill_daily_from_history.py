"""Reconstruct missing KTC ``daily_load`` partitions from the per-player value-history pages.

The daily ``ktc-incremental-<market>`` jobs snapshot the rankings page once a day. When they
are down (2026-09-08 -> 2026-09-30: KTC changed its page layout) the ``daily_load`` partitions
for those days simply never exist, and every consumer that scans
``bronze/ktc/<market>/daily_load/load_date=*/player_data.parquet`` (silver staging,
fact_pick_values) has a hole. KTC's per-player pages carry the full *daily* value history
(``overallValue``), overall rank and positional rank for both 1QB and Superflex, so the missing
days can be rebuilt from them -- for every player (and pick) currently on the rankings page.

What a reconstructed partition contains: the exact column schema of the real snapshots (taken
from the newest existing partition of that market), with
  * player identity (playerName / playerID / slug / position / positionID) from the rankings page,
  * ``oneqb_value`` / ``oneqb_rank`` / ``oneqb_positionalRank`` and the ``sf_*`` equivalents from
    the history,
  * everything else NULL -- the TE-premium values (``*_tep*``), trends, liquidity, adp,
    kept/traded/cut counts and ``isTrending`` are not published as history, so they cannot be
    recovered.
Each reconstructed partition also gets a ``_RECONSTRUCTED.json`` sidecar (ignored by the
``player_data.parquet`` globs) recording when and from what it was built. Existing partitions are
never overwritten, so re-running only fills whatever is still missing (idempotent). Players that
were not on the rankings page at run time are absent from the rebuilt days.

Run manually (Cloud Run job ``ktc-backfill-daily``, or locally with ADC + GCS_BUCKET_NAME):
    python backfill_daily_from_history.py                  # all markets, gap -> yesterday
    python backfill_daily_from_history.py --market dynasty --end 2026-09-29
    python backfill_daily_from_history.py --dry-run --out ./tmp --limit 5   # no GCS writes
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

import polars as pl
from dotenv import load_dotenv
from google.cloud import storage

from utils import (
    fetch_soup,
    parse_historic_1QBplayer_data,
    parse_historic_SFplayer_data,
    parse_rankings_players,
)

load_dotenv()

# market -> (rankings page, per-player page prefix)
MARKETS: dict[str, tuple[str, str]] = {
    "dynasty": ("https://keeptradecut.com/dynasty-rankings",
                "https://keeptradecut.com/dynasty-rankings/players/"),
    "redraft": ("https://keeptradecut.com/fantasy-rankings",
                "https://keeptradecut.com/fantasy-rankings/players/"),
    "devy": ("https://keeptradecut.com/devy-rankings",
             "https://keeptradecut.com/devy-rankings/players/"),
}

# Abort (before writing anything) if more than this share of player pages failed to parse --
# a thin partition built from a half-broken scrape is worse than no partition.
MAX_FAILURE_RATE = 0.10

# rankings-page fields carried into every reconstructed row
_IDENTITY_FIELDS = ("playerName", "playerID", "slug", "position", "positionID")


# --------------------------------------------------------------------------- pure helpers
def parse_ktc_date(d: str) -> date:
    """KTC history dates are ``YYMMDD`` strings."""
    return datetime.strptime(d, "%y%m%d").date()


def _series_by_date(points: Iterable[dict] | None) -> dict[date, int]:
    """``[{"d": "260930", "v": 7806}, ...]`` -> ``{date: value}``; a later duplicate date wins."""
    out: dict[date, int] = {}
    for p in points or []:
        out[parse_ktc_date(p["d"])] = p["v"]
    return out


def missing_dates(existing: Iterable[date], start: date | None, end: date) -> list[date]:
    """Calendar days in ``[start, end]`` with no partition. ``start`` defaults to the day after
    the newest existing partition (or ``end`` itself when nothing exists)."""
    existing = set(existing)
    if start is None:
        start = (max(existing) + timedelta(days=1)) if existing else end
    if start > end:
        return []
    return [start + timedelta(days=i) for i in range((end - start).days + 1)
            if start + timedelta(days=i) not in existing]


def history_to_daily_rows(player: dict, one_qb: dict, superflex: dict,
                          wanted: Iterable[date]) -> list[dict]:
    """One row per wanted date on which the player has a 1QB or Superflex value.

    ``one_qb`` / ``superflex`` are the per-player page blobs (``parse_historic_*``): value history
    in ``overallValue``, ranks in ``overallRankHistory`` / ``positionalRankHistory`` (picks have no
    positional rank).
    """
    ident = {k: player.get(k) for k in _IDENTITY_FIELDS}
    one_val = _series_by_date(one_qb.get("overallValue"))
    one_rank = _series_by_date(one_qb.get("overallRankHistory"))
    one_pos = _series_by_date(one_qb.get("positionalRankHistory"))
    sf_val = _series_by_date(superflex.get("overallValue"))
    sf_rank = _series_by_date(superflex.get("overallRankHistory"))
    sf_pos = _series_by_date(superflex.get("positionalRankHistory"))

    rows = []
    for d in sorted(set(wanted)):
        if d not in one_val and d not in sf_val:
            continue  # not valued that day (e.g. entered the rankings later)
        rows.append({
            **ident,
            "_load_date": d,
            "oneqb_value": one_val.get(d),
            "oneqb_rank": one_rank.get(d),
            "oneqb_positionalRank": one_pos.get(d),
            "sf_value": sf_val.get(d),
            "sf_rank": sf_rank.get(d),
            "sf_positionalRank": sf_pos.get(d),
        })
    return rows


def conform_to_schema(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Cast to / order by the reference partition schema; absent columns become typed nulls, so
    the hive scans over ``load_date=*`` keep one uniform schema."""
    return df.select([
        pl.col(col).cast(dtype) if col in df.columns else pl.lit(None, dtype=dtype).alias(col)
        for col, dtype in schema.items()
    ])


def build_partitions(
    players: list[dict],
    wanted: list[date],
    fetch_history: Callable[[dict], tuple[dict, dict]],
    schema: dict[str, pl.DataType],
    log: Callable[[str], None] = print,
) -> tuple[dict[date, pl.DataFrame], list[dict]]:
    """Fetch every player's history (``fetch_history(player) -> (one_qb, superflex)``) and
    assemble one frame per wanted date. Returns ``(frames_by_date, errors)``; a player whose
    page fails is skipped and reported, never fatal here (the caller enforces MAX_FAILURE_RATE)."""
    rows: list[dict] = []
    errors: list[dict] = []
    for i, player in enumerate(players, 1):
        slug = player.get("slug")
        try:
            one_qb, superflex = fetch_history(player)
            player_rows = history_to_daily_rows(player, one_qb, superflex, wanted)
            rows.extend(player_rows)
            log(f"  ok  {slug} ({i}/{len(players)}): {len(player_rows)} day(s)")
        except Exception as e:  # noqa: BLE001 - keep going, report at the end
            errors.append({"slug": slug, "player_name": player.get("playerName"), "error": str(e),
                           "timestamp": datetime.now(timezone.utc).isoformat()})
            log(f"  ERR {slug} ({i}/{len(players)}): {e}")

    frames: dict[date, pl.DataFrame] = {}
    if rows:
        all_rows = pl.DataFrame(rows)
        for (d,), part in all_rows.group_by("_load_date", maintain_order=True):
            frames[d] = conform_to_schema(part.drop("_load_date"), schema)
    return frames, errors


# ------------------------------------------------------------------------------ GCS I/O
def _daily_prefix(market: str) -> str:
    return f"bronze/ktc/{market}/daily_load/"


def list_existing_partition_dates(bucket_name: str, market: str) -> list[date]:
    client = storage.Client()
    dates = set()
    for blob in client.list_blobs(bucket_name, prefix=_daily_prefix(market)):
        if blob.name.endswith("player_data.parquet") and "load_date=" in blob.name:
            dates.add(date.fromisoformat(blob.name.split("load_date=")[1].split("/")[0]))
    return sorted(dates)


def reference_schema(bucket_name: str, market: str, latest: date) -> dict[str, pl.DataType]:
    path = f"gs://{bucket_name}/{_daily_prefix(market)}load_date={latest}/player_data.parquet"
    return dict(pl.read_parquet_schema(path))


def write_partition(df: pl.DataFrame, bucket_name: str, market: str, d: date,
                    n_players: int, dry_run_dir: Path | None) -> str:
    rel = f"{_daily_prefix(market)}load_date={d}/"
    marker = {
        "reconstructed_at": datetime.now(timezone.utc).isoformat(),
        "source": "keeptradecut per-player value-history pages (backfill_daily_from_history)",
        "players_on_rankings_page": n_players,
        "rows": df.height,
        "note": "TE-premium values, trends, liquidity, adp, kept/traded/cut and isTrending are "
                "not published as history and are null in this partition.",
    }
    if dry_run_dir is not None:
        out = dry_run_dir / rel
        out.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out / "player_data.parquet")
        (out / "_RECONSTRUCTED.json").write_text(json.dumps(marker, indent=2))
        return str(out)

    bucket = storage.Client().bucket(bucket_name)
    if bucket.blob(rel + "player_data.parquet").exists():
        raise FileExistsError(f"gs://{bucket_name}/{rel}player_data.parquet already exists")
    df.write_parquet(f"gs://{bucket_name}/{rel}player_data.parquet")
    bucket.blob(rel + "_RECONSTRUCTED.json").upload_from_string(
        json.dumps(marker, indent=2), content_type="application/json")
    return f"gs://{bucket_name}/{rel}"


# ---------------------------------------------------------------------------------- main
def backfill_market(market: str, bucket_name: str, start: date | None, end: date,
                    limit: int | None, dry_run_dir: Path | None) -> int:
    rankings_url, player_url = MARKETS[market]
    existing = list_existing_partition_dates(bucket_name, market)
    if not existing:
        print(f"[{market}] no existing daily_load partitions -> nothing to take a schema from; skipping")
        return 0
    wanted = missing_dates(existing, start, end)
    if not wanted:
        print(f"[{market}] no missing partitions between {start or existing[-1] + timedelta(days=1)} "
              f"and {end}; nothing to do")
        return 0
    print(f"[{market}] {len(wanted)} missing day(s): {wanted[0]} .. {wanted[-1]}")

    schema = reference_schema(bucket_name, market, existing[-1])
    players = parse_rankings_players(fetch_soup(rankings_url))
    if limit:
        players = players[:limit]
    print(f"[{market}] fetching history for {len(players)} players/picks ...")

    def fetch_history(player: dict) -> tuple[dict, dict]:
        soup = fetch_soup(player_url + player["slug"])
        return parse_historic_1QBplayer_data(soup), parse_historic_SFplayer_data(soup)

    frames, errors = build_partitions(players, wanted, fetch_history, schema)

    failure_rate = len(errors) / max(len(players), 1)
    if failure_rate > MAX_FAILURE_RATE:
        print(f"[{market}] ABORT: {len(errors)}/{len(players)} player pages failed "
              f"({failure_rate:.0%} > {MAX_FAILURE_RATE:.0%}); nothing written. First errors:")
        for e in errors[:5]:
            print("   ", e)
        return 1

    for d in wanted:
        if d not in frames:
            print(f"[{market}] {d}: no player had a value that day; skipped")
            continue
        where = write_partition(frames[d], bucket_name, market, d, len(players), dry_run_dir)
        print(f"[{market}] {d}: wrote {frames[d].height} rows -> {where}")
    if errors:
        print(f"[{market}] done with {len(errors)} player page error(s):")
        for e in errors:
            print("   ", e["slug"], "-", e["error"])
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--market", choices=sorted(MARKETS), action="append",
                    help="market(s) to backfill (default: all)")
    ap.add_argument("--start", type=date.fromisoformat,
                    help="first day to consider (default: the day after the newest partition)")
    ap.add_argument("--end", type=date.fromisoformat,
                    help="last day to consider (default: yesterday UTC -- today's history is still moving)")
    ap.add_argument("--limit", type=int, help="only the first N players per market (for dry runs)")
    ap.add_argument("--dry-run", action="store_true", help="write partitions under --out instead of GCS")
    ap.add_argument("--out", type=Path, default=Path("./ktc_backfill_dry_run"))
    args = ap.parse_args(argv)

    bucket_name = os.environ.get("GCS_BUCKET_NAME")
    if not bucket_name:
        print("GCS_BUCKET_NAME is not set"); return 2
    end = args.end or (datetime.now(timezone.utc).date() - timedelta(days=1))
    dry_run_dir = args.out if args.dry_run else None

    rc = 0
    for market in (args.market or sorted(MARKETS)):
        rc |= backfill_market(market, bucket_name, args.start, end, args.limit, dry_run_dir)
    return rc


if __name__ == "__main__":
    sys.exit(main())
