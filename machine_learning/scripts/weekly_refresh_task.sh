#!/usr/bin/env bash
# The weekly production refresh on the owner's GPU, as Task Scheduler runs it (the task `ff-weekly-refresh`, Tuesdays,
# or a one-shot task for a run now): the production preset under a wall-time budget; if it fails or runs out of time,
# the orphans are killed and the same preset runs off the last complete season's career tail (`--no-career-todate`,
# the fallback after the 2026-10-13/14 refresh sat ten hours in the to-date tail). One log per run under
# `_cache/logs/`; this script's own lines go to `_cache/logs/weekly_refresh_task.log` (the .cmd appends them).
#
#   by hand:        scripts/weekly_refresh_task.cmd      (or: bash scripts/weekly_refresh_task.sh)
#   is it working?  scripts/run_status.py _cache/logs/production_<stamp>.log
#   afterwards:     the page -- republish projections.json with the HTML (an artifact publish needs a session)
set -u
ML="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ML" || exit 1
export PYTHONIOENCODING=utf-8 PYTHONUTF8=1 PYTHONUNBUFFERED=1 KEEP_DISPLAY=1
PY=.venv/Scripts/python
LOGS="$ML/_cache/logs"; mkdir -p "$LOGS"
stamp=$(date +%Y-%m-%d_%H%M)
log() { echo "$* $(date '+%a %F %T')"; }
orphans() { powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { \$_.Name -eq 'python.exe' -and \$_.CommandLine -match '$1' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force }"; }

busy=$(powershell -NoProfile -Command "@(Get-CimInstance Win32_Process | Where-Object { \$_.Name -eq 'python.exe' -and \$_.CommandLine -match 'weekly_refresh|backtest_inseason|run_experiment' }).Count" | tr -d '\r ')
if [ "${busy:-0}" -gt 0 ]; then log "weekly refresh: $busy python run(s) already on the GPU; not starting"; exit 0; fi

log "weekly refresh start (production preset, budget 6h) -> $LOGS/production_$stamp.log"
timeout -k 60s 6h $PY -u scripts/weekly_refresh.py --preset production --device cuda > "$LOGS/production_$stamp.log" 2>&1
rc=$?; log "weekly refresh exit $rc"
if [ $rc -ne 0 ]; then
  orphans 'backtest_inseason|build_war|export_projections|weekly_refresh'; sleep 10
  log "fallback start (--no-career-todate, budget 4h) -> $LOGS/production_${stamp}_fallback.log"
  timeout -k 60s 4h $PY -u scripts/weekly_refresh.py --preset production --no-career-todate --device cuda > "$LOGS/production_${stamp}_fallback.log" 2>&1
  rc=$?; log "fallback exit $rc"
  [ $rc -ne 0 ] && orphans 'backtest_inseason|build_war|export_projections|weekly_refresh'
fi
log "weekly refresh done (exit $rc); next: republish the page"
