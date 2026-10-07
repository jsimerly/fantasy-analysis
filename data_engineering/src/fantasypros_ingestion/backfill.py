"""Backfill FantasyPros projections 2012-2025 (Wayback + live union). Runs LOCALLY.

Per season: one CDX query per position lists the Wayback captures (snapshot-driven), then each
(season, slot) is unioned with the live site for survivor density in recent seasons
(>= FP_LIVE_FROM) where Wayback is thin. Wayback supplies the full as-of roster incl. departed
players (the whole point). Resumable: existing partitions are skipped.

Env: FP_YEAR_START (2012), FP_YEAR_END (2025), FP_WEEKS (18), FP_LIVE_FROM (2022),
FP_RATE_PER_MIN (4, live pacing).  Run: `python -m fantasypros_ingestion.backfill`
"""
from __future__ import annotations

import os
import time

from fantasypros_ingestion import _harvest as H
from fantasypros_ingestion import _sources as S


def main() -> None:
    y0 = int(os.environ.get("FP_YEAR_START", "2012"))
    y1 = int(os.environ.get("FP_YEAR_END", "2025"))
    weeks = int(os.environ.get("FP_WEEKS", "18"))
    live_from = int(os.environ.get("FP_LIVE_FROM", "2022"))
    rate = float(os.environ.get("FP_RATE_PER_MIN", "4"))

    gd = H.load_week_gamedays()
    done = skip = 0
    t0 = time.monotonic()
    print(f"FantasyPros backfill {y0}-{y1} | live-union >= {live_from} | rate~{rate}/min", flush=True)
    for year in range(y0, y1 + 1):
        do_live = year >= live_from
        wb_by_pos = {pos: H.harvest_wayback_season(pos, year, gd) for pos in S.POSITIONS}
        covered = sorted({s for d in wb_by_pos.values() for s in d}, key=lambda x: (x is None, x))
        print(f" {year}: wayback slots covered = {covered}", flush=True)
        for slot in [None] + list(range(1, weeks + 1)):
            if H.exists(year, slot):
                skip += 1
                continue
            df = H.assemble_slot(year, slot, wb_by_pos, do_live=do_live, live_rate=rate)
            tag = "season" if slot is None else f"wk{slot:02d}"
            if df is not None:
                H.write(df, year, slot)
                done += 1
                print(f"  {year} {tag}: {df.height} rows, src={df['source'].unique().to_list()} "
                      f"| done={done} skip={skip} | {(time.monotonic()-t0)/60:.1f}m", flush=True)
    print(f"DONE: wrote {done}, skipped {skip} in {(time.monotonic()-t0)/60:.1f}m", flush=True)


if __name__ == "__main__":
    main()
