#!/bin/bash
# Headless checks of the page against a real export payload (jsdom). Run from this folder:
#   npm install            (once; jsdom)
#   bash run_checks.sh [path/to/projections.json]   (default: ../projections.json, the latest export)
# Each script loads ../intrinsic_value.html with the payload, renders a tab and prints what it finds;
# "errors: 0" on every block is the pass line.
cd "$(dirname "$0")" || exit 1
P="${1:-../projections.json}"
for c in final_check tv_check pick_probe market_check season_check title_check; do
  echo "=== $c"
  node "$c.js" ../intrinsic_value.html "$P" 2>&1 | grep -E "errors:|nan|rows:|tabs:|cards:|edges:|headers:|standings row 1:|curve svg|title cells|sorted by season|PICK|pick:|rosters ok|perf cards|heat rows" | head -12
done
