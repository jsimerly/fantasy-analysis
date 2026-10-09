"""Weekly in-season refresh (BACKLOG item 14): one Cloud Run job that re-projects the season in
progress and rebuilds WAR for every league the owner is in, so the page data and the WAR history
in the ML bucket stay current without anyone running scripts by hand.

Steps (each is the existing script, run as a subprocess so its own CLI stays the source of truth):
  1. ``backtest_inseason.py --current``  -> inseason/season=S/week=W/run_date=D/projections.parquet
  2. ``build_war.py --source inseason --all-leagues --teams``  -> war/league=.../season=S/week=W/run_date=D/
  3. the analysis reports the page embeds (``analysis/pick_slots.py`` + ``pick_slots_report.py --publish``,
     ``analysis/trade_report.py --rebuild --publish``), best-effort, in the repo-root venv
  4. ``export_projections.py``  -> pages/season=S/week=W/run_date=D/projections.json in the ML bucket
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
import power  # noqa: E402

WEEK_PATH = "silver/fantasy/fact_player_week/data.parquet"
STEPS = ["inseason", "war", "analysis", "export"]
# the adopted production configuration (BACKLOG 22, owner's call 2026-10-09): TabPFN 3.5 with the
# owner's feature set and pooled horizons for the career tail, TabPFN 3.5 for the in-season model,
# the 20/50/80 band on. A local GPU run: the Cloud Run job (no torch in its image) keeps the defaults.
PRESETS = {
    "production": dict(backend="tabpfn", groups="base,career,injury,trend,situation,rookie,college", stacked=True, range=True,
                       inseason_backend="tabpfn"),
}


def apply_preset(args):
    """Overlay a named preset on the parsed arguments (explicit flags do not override it: a preset is the configuration)."""
    name = getattr(args, "preset", None)
    if name:
        for k, v in PRESETS[name].items():
            setattr(args, k, v)
    return args
# the analysis reports the page embeds (draft-slot standings, the trade log) run in the repo-root
# venv (analysis/ has its own deps); when it is absent this interpreter is used
ANALYSIS_PY = ROOT.parent / ".venv" / "Scripts" / "python.exe"


def current_season_week() -> tuple[int, int]:
    wk = gcs_io.read_lake(WEEK_PATH)
    season = int(wk["season"].max())
    week = int(wk.filter(pl.col("season") == season)["week"].max())
    return season, week


def run(cmd: list[str], dry: bool, cwd: Path | None = None, best_effort: bool = False) -> bool:
    print("+", " ".join(cmd), flush=True)
    if dry:
        return True
    r = subprocess.run(cmd, check=not best_effort, cwd=cwd or ROOT)
    if r.returncode != 0:
        print(f"WARNING: step failed (exit {r.returncode}) and was skipped: {cmd[1] if len(cmd) > 1 else cmd}", flush=True)
    return r.returncode == 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip", nargs="*", default=[], choices=STEPS, help="steps to leave out")
    ap.add_argument("--run-date", default=datetime.now(timezone.utc).date().isoformat())
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--backend", default="xgb", choices=["xgb", "tabpfn", "blend"], help="career model estimator for the in-season refresh (tabpfn needs a GPU: run locally)")
    ap.add_argument("--tabpfn-params", nargs="*", default=[])
    ap.add_argument("--cap", default="30+t", help="age-survival cap on projected games (30+t = tier-aware from 30, production | 30+ | all | none)")
    ap.add_argument("--range", action="store_true", help="keep the career tail's 20/50/80 band (TabPFN backends): floor / ceiling wins on the page")
    ap.add_argument("--groups", default="base,career", help="feature groups for the career tail")
    ap.add_argument("--stacked", action="store_true", help="pooled horizons for the career tail")
    ap.add_argument("--target", choices=["level", "residual"], default="level")
    ap.add_argument("--inseason-groups", default="", help="in-season model groups: team, contract (inseason.EXTRA_GROUPS)")
    ap.add_argument("--inseason-backend", choices=["xgb", "tabpfn"], default="xgb", help="in-season model estimator (shares --tabpfn-params)")
    ap.add_argument("--preset", choices=sorted(PRESETS), help="a named configuration: 'production' = the adopted local GPU refresh (BACKLOG 22, 2026-10-09)")
    args = apply_preset(ap.parse_args())
    power.keep_awake()                      # hours of GPU work: do not let the machine sleep under it
    py = sys.executable
    season, week = current_season_week()
    print(f"season {season} through week {week}; run date {args.run_date}", flush=True)

    if "inseason" not in args.skip:
        run([py, "scripts/backtest_inseason.py", "--current", "--device", args.device, "--backend", args.backend, "--cap", args.cap, "--groups", args.groups, "--target", args.target]
            + (["--stacked"] if args.stacked else []) + (["--range"] if args.range else []) + (["--inseason-groups", args.inseason_groups] if args.inseason_groups else []) + ["--inseason-backend", args.inseason_backend] + (["--tabpfn-params", *args.tabpfn_params] if args.tabpfn_params else []), args.dry_run)
    if "war" not in args.skip:
        run([py, "scripts/build_war.py", "--source", "inseason", "--season", str(season), "--week", str(week),
             "--run-date", args.run_date, "--all-leagues", "--teams"], args.dry_run)
    if "analysis" not in args.skip:
        # the page's Draft slots and Trades tabs come from these summaries (latest run in the ML bucket);
        # rebuilt here so they carry the lake's newest standings and transactions. Best-effort: a failure
        # leaves the previous summaries in place and the refresh goes on.
        apy = str(ANALYSIS_PY) if ANALYSIS_PY.exists() else py
        root = ROOT.parent
        for cmd in (["analysis/pick_slots.py"],
                    ["analysis/pick_slots_report.py", "--out", "analysis/_cache/pick_slots", "--publish"],
                    ["analysis/trade_report.py", "--rebuild", "--out", "analysis/_cache/trades", "--min-seasons", "1", "--publish"]):
            run([apy, *cmd], args.dry_run, cwd=root, best_effort=True)
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
