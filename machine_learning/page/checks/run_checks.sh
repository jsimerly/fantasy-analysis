#!/bin/bash
# Headless checks of the page against a real export payload (jsdom). Run from this folder:
#   npm install            (once; jsdom)
#   bash run_checks.sh [path/to/projections.json]   (default: machine_learning/projections.json, the latest export;
#                                                    a relative path is taken from where you run it)
# Each script loads ../intrinsic_value.html with the payload, renders a tab and prints what it finds;
# "errors: 0" on every block is the pass line, and the run exits 1 when any block lacks it (2026-10-22: a wrong payload
# path made node fail on every check and the filtered output showed seven empty blocks with exit 0).
P="${1:-$(dirname "$0")/../../projections.json}"
case "$P" in /*|[A-Za-z]:*) ;; *) P="$(pwd)/$P" ;; esac
[ -f "$P" ] || { echo "no payload at $P"; exit 1; }
cd "$(dirname "$0")" || exit 1
rc=0
for c in final_check tv_check pick_probe market_check season_check title_check perf_check; do
  echo "=== $c"
  out=$(node "$c.js" ../intrinsic_value.html "$P" 2>&1)
  echo "$out" | grep -E "errors:|nan|rows:|tabs:|cards:|edges:|headers:|standings row 1:|curve svg|title cells|sorted by season|PICK|pick:|rosters ok|perf cards|heat rows" | head -12
  echo "$out" | grep -qE "^errors:? 0" || { echo "FAIL $c"; echo "$out" | grep -vE "^\s*$" | tail -4; rc=1; }
done
[ $rc -eq 0 ] && echo "all checks pass" || echo "CHECKS FAILED"
exit $rc
