# page — the browsable projections page

One self-contained HTML file, `intrinsic_value.html`, that reads `projections.json` (the export of
`scripts/export_projections.py`, ~2 MB, gitignored here) from the same folder and renders seven tabs:
**Players** (WAR / PAR per player at the chosen rate and years, bands, fair value, mispricing within
position and across positions, the season wind), **Rosters** (each league's projected lineups, marginal
wins and title odds this season and next per player, trade targets, the trade finder, picks),
**Model performance** (the story tiles, then the measurements), **Trades** (the leagues' trading,
scored), **Draft slots**, **Team value** (KTC power rankings over time) and **Market** (how the market
prices over time). No build step, no framework: vanilla JS, inline SVG for the figures, one `<style>`.

## Publishing
The page is published as a private claude.ai artifact (the owner's link) with `projections.json` as a
supporting file. Publishing the HTML alone keeps the old payload, so after a new export both go up
together. The weekly refresh writes the payload to the ML bucket
(`pages/season=S/week=W/run_date=D/projections.json`); the page itself has no backend.

## Checks
`checks/` holds headless jsdom scripts (`npm install` once) that load the page with a real payload and
assert each tab renders: `bash checks/run_checks.sh [payload]` prints one block per check and
`errors: 0` on every block is the pass line. Run them after any edit to the page and after any change
to the export's shape. The perf tiles, the title lens and the Season column have their own checks.

## Conventions
- Every number on the page comes from the payload; nothing is hard-coded except labels and the
  explanatory copy (the "how" paragraph on Players, the honesty paragraph on Model performance).
- Mispricing defaults to **within position**; the pooled column is labelled "All positions" and
  carries the model's quarterback lean (BACKLOG 42).
- State lives in `st` (rate, years, market, fair-value method, unit, league, team); the Players table
  sorts by the selected unit's rank by default.
- Keep the copy honest: walk-forward numbers only, the market as a yardstick, caveats stated.
