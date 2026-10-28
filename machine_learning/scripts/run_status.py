"""Is the GPU run working? One command (owner, 2026-10-14: "how is there no way for visibility if a run is working?").

The 2026-10-13/14 refresh sat ten hours in the to-date career tail at 100 % GPU utilization and 91 W: utilization says
busy, power says crawling (TabPFN's memory-saving path after an allocator warning). This prints every live run (the
python processes of the refresh, the harness and the backtests, with their start and CPU time), each log's last line and
its age, the GPU's utilization / power / memory, the stage the log is in with its elapsed time against the stage's usual
duration, and a verdict: HEALTHY, SLOW (past the usual time) or STALLED (low power at full utilization, or a log that
has not moved for an hour).

  python scripts/run_status.py                      # the scratchpad's newest logs
  python scripts/run_status.py path/to/run.log ...  # these logs
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# stage -> (the log line that starts it, its usual wall time in minutes on the owner's GPU, cold)
STAGES = [
    ("career tail (base, cold)", re.compile(r"career tail: no cache for key"), 130),
    ("career tail (this season, to-date)", re.compile(r"career tail through week"), 150),
    ("in-season model fit", re.compile(r"in-season model: tabpfn"), 45),
    ("current week projected", re.compile(r"== \d{4} through week"), 10),
    ("WAR per league", re.compile(r"== .*: \d+ teams, slots"), 10),
    ("export", re.compile(r"performance summaries:"), 5),
]
LOW_POWER_W = 120.0           # at full utilization, below this the card is waiting on memory, not computing
STALE_MIN = 60.0              # a log that has not moved for this long while a process lives


def gpu() -> dict | None:
    try:
        out = subprocess.run(["nvidia-smi", "--query-gpu=utilization.gpu,power.draw,memory.used,memory.total,temperature.gpu",
                              "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=10).stdout.strip().split(", ")
        return {"util": float(out[0]), "power": float(out[1]), "mem_used": float(out[2]), "mem_total": float(out[3]), "temp": float(out[4])}
    except Exception:  # noqa: BLE001
        return None


def live_runs() -> list[dict]:
    ps = ("Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match "
          "'weekly_refresh|backtest_inseason|run_experiment|seam_eval|market_backtest|build_war|export_projections' } | ForEach-Object { "
          "$p = Get-Process -Id $_.ProcessId -ErrorAction SilentlyContinue; "
          "'{0}|{1:yyyy-MM-dd HH:mm:ss}|{2}|{3}|{4}' -f $_.ProcessId, $_.CreationDate, [int]$p.TotalProcessorTime.TotalSeconds, [int]($p.WorkingSet64/1MB), $_.CommandLine }")
    try:
        out = subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True, text=True, timeout=30).stdout
    except Exception:  # noqa: BLE001
        return []
    rows = []
    for line in out.splitlines():
        parts = line.split("|", 4)
        if len(parts) == 5 and parts[2].isdigit():
            cmd = parts[4]
            script = next((s for s in ("weekly_refresh", "backtest_inseason", "run_experiment", "seam_eval", "market_backtest", "build_war", "export_projections") if s in cmd), "?")
            rows.append({"pid": int(parts[0]), "started": parts[1], "cpu_s": int(parts[2]), "ram_mb": int(parts[3]), "script": script,
                         "args": " ".join(a for a in cmd.split(" ") if a.startswith("--"))[:160]})
    return rows


def log_report(path: Path, now: float) -> dict:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip() and not re.search(r"Warn|join_asof|stddev", ln)]
    age_min = (now - path.stat().st_mtime) / 60
    # the stage is the one whose marker is the LAST stage marker in the log (chronological), and the budget is cumulative:
    # by the end of stage i a healthy run has spent at most the sum of the usual times of stages 0..i
    stage, budget = None, None
    for ln in lines:
        for i, (name, pat, _norm) in enumerate(STAGES):
            if pat.search(ln):
                stage, budget = name, sum(n for _, _, n in STAGES[: i + 1])
    done = any("weekly refresh done" in ln or ("wrote gs://" in ln and "projections.json" in ln) for ln in lines)
    exited = bool(re.search(r"Traceback|SystemExit|exit [1-9]", text))
    return {"path": str(path), "last": (lines[-1][:120] if lines else ""), "age_min": age_min, "stage": stage, "norm_min": budget,
            "done": done, "error": exited, "lines": len(lines)}


def main() -> None:
    now = time.time()
    scratch = Path(os.environ.get("CLAUDE_SCRATCHPAD", "")) if os.environ.get("CLAUDE_SCRATCHPAD") else None
    logs = [Path(a) for a in sys.argv[1:]]
    if not logs and scratch and scratch.exists():
        logs = sorted((p for p in scratch.glob("*.log") if p.stat().st_mtime > now - 24 * 3600), key=lambda p: -p.stat().st_mtime)[:4]
    runs = live_runs()
    g = gpu()
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S}")
    print("GPU:", "unavailable" if g is None else f"{g['util']:.0f} % util, {g['power']:.0f} W, {g['mem_used']:.0f} / {g['mem_total']:.0f} MiB, {g['temp']:.0f} C")
    if not runs:
        print("live runs: none")
    for r in runs:
        if r["cpu_s"] == 0 and r["ram_mb"] < 50:          # the venv launcher wrappers around the real interpreter
            continue
        started = datetime.strptime(r["started"], "%Y-%m-%d %H:%M:%S")
        wall = (datetime.now() - started).total_seconds() / 60
        print(f"live: pid {r['pid']} {r['script']} started {started:%H:%M} ({wall:.0f} min ago), cpu {r['cpu_s'] / 60:.0f} min, ram {r['ram_mb']} MB  {r['args']}")
    verdict = []
    for p in logs:
        if not p.exists():
            print(f"log {p}: missing"); continue
        rep = log_report(p, now)
        print(f"log {p.name}: {rep['lines']} lines, last written {rep['age_min']:.0f} min ago | stage: {rep['stage'] or '?'}"
              + (f" (a healthy run is through it by {rep['norm_min']} min)" if rep["norm_min"] else "") + (" | DONE" if rep["done"] else "") + (" | ERROR in log" if rep["error"] else ""))
        print(f"   last: {rep['last']}")
        if runs and not rep["done"] and rep["stage"]:
            heavy = [r for r in runs if r["script"] in ("backtest_inseason", "run_experiment", "seam_eval", "market_backtest")]
            if heavy:
                started = datetime.strptime(heavy[-1]["started"], "%Y-%m-%d %H:%M:%S")
                wall = (datetime.now() - started).total_seconds() / 60
                if g and g["util"] >= 90 and g["power"] < LOW_POWER_W:
                    verdict.append(f"STALLED: {g['util']:.0f} % utilization at {g['power']:.0f} W -- the card is waiting on memory (the 2026-10-13 signature), not computing")
                elif rep["age_min"] > STALE_MIN and (g is None or g["util"] < 50):
                    verdict.append(f"STALLED: the log has not moved for {rep['age_min']:.0f} min and the GPU is idle")
                elif rep["norm_min"] and wall > 1.5 * rep["norm_min"]:
                    verdict.append(f"SLOW: {wall:.0f} min into the run, a healthy run is through stage '{rep['stage']}' by {rep['norm_min']} min")
                else:
                    verdict.append(f"HEALTHY: {wall:.0f} min into the run, stage '{rep['stage']}', GPU {g['power']:.0f} W" if g else "HEALTHY (no GPU reading)")
    for v in verdict[:1]:
        print("verdict:", v)
    if not verdict and runs:
        print("verdict: processes alive, no stage recognised in the logs given")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    main()
