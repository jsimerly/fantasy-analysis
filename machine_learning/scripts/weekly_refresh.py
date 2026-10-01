"""Weekly in-season refresh (BACKLOG item 14): one Cloud Run job that re-projects the season in
progress and rebuilds WAR for every league the owner is in, so the page data and the WAR history
in the ML bucket stay current without anyone running scripts by hand.

Steps (each is the existing script, run as a subprocess so its own CLI stays the source of truth):
  1. ``backtest_inseason.py --current``  -> inseason/season=S/week=W/run_date=D/projections.parquet
  2. ``build_war.py --source inseason --all-leagues --teams``  -> war/league=.../season=S/week=W/run_date=D/
  3. ``export_projections.py``  -> pages/season=S/week=W/run_date=D/projections.json in the ML bucket
     (the published page is republished from that file; a job cannot update a claude.ai artifact)

Season and week come from the weekly fact (the last week with stats), the run date is today (UTC).
``--dry-run`` prints the commands without running anything; ``--skip`` leaves a step out.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

import gcs_io  # noqa: E402

WEEK_PATH = "silver/fantasy/fact_player_week/data.parquet"
STEPS = ["inseason", "war", "export"]


def current_season_week() -> tuple[int, int]:
    wk = gcs_io.read_lake(WEEK_PATH)
    season = int(wk["season"].max())
    week = int(wk.filter(pl.col("season") == season)["week"].max())
    return season, week


def run(cmd: list[str], dry: bool) -> None:
    print("+", " ".join(cmd), flush=True)
    if not dry:
        subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip", nargs="*", default=[], choices=STEPS, help="steps to leave out")
    ap.add_argument("--run-date", default=datetime.now(timezone.utc).date().isoformat())
    ap.add_argument("--device", default="cpu")
    args = ap.parse_args()
    py = sys.executable
    season, week = current_season_week()
    print(f"season {season} through week {week}; run date {args.run_date}", flush=True)

    if "inseason" not in args.skip:
        run([py, "scripts/backtest_inseason.py", "--current", "--device", args.device], args.dry_run)
    if "war" not in args.skip:
        run([py, "scripts/build_war.py", "--source", "inseason", "--season", str(season), "--week", str(week),
             "--run-date", args.run_date, "--all-leagues", "--teams"], args.dry_run)
    if "export" not in args.skip:
        out = ROOT / "projections.json"
        run([py, "scripts/export_projections.py", "--source", "inseason", "--season", str(season), "--week", str(week),
             "--run-date", args.run_date, "--out", str(out)], args.dry_run)
        if not args.dry_run:
            p = gcs_io.write_ml_json(__import__("json").loads(out.read_text(encoding="utf-8")),
                                     "pages", f"season={season}", f"week={week}", f"run_date={args.run_date}", "projections.json")
            print("wrote", p, flush=True)
    print("weekly refresh done", flush=True)


if __name__ == "__main__":
    main()
