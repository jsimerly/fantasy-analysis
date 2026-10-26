# machine_learning — the dynasty value model

Read [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) first for how this fits the whole system. This
file is the working guide for this folder; [README.md](README.md) has the longer narrative and
[BACKLOG.md](BACKLOG.md) is the journal (numbered items, every experiment's verdict with numbers).

## Setup
- Own venv: `.venv/Scripts/python.exe` (Python 3.11, polars, xgboost, torch cu130 for the RTX 5070 Ti,
  TabPFN 9.x with the 3.5 weights cached; `TABPFN_TOKEN` for headless licence checks).
- Reads the lake (`gcs_io.read_lake`) and reads/writes the ML bucket (`gcs_io.read_ml_*` /
  `write_ml_*`). Never writes the lake; never writes local `data/`.
- Tests: `.venv/Scripts/python.exe -m pytest tests -q` (spec-style, ~5 s, no GCS: fixtures are synthetic).
- **Before any change that touches the weekly path**, run the in-season stage end to end on the trees (about a minute,
  reads the lake, writes nothing — `--no-write` covers the `--current` projections since 2026-10-13):
  `scripts/backtest_inseason.py --current --no-write --device cpu --backend xgb --inseason-backend xgb` plus the
  production preset's other flags (`weekly_refresh.py --preset production --dry-run` prints them). The 2026-10-13
  refresh died four hours in on a column the unit tests never exercised; this smoke would have caught it. **Never run
  `weekly_refresh.py` itself on the trees against the ML bucket:** its `build_war` / `export` steps write `war/` and
  `pages/` at today's run date, and a trees payload at the latest run date is what the exports pick up when the GPU run
  fails (the Cloud Run job `ml-weekly-refresh` does exactly this every Tuesday 15:00 UTC until it is paused — review
  2026-10-13, `docs/MODEL_REVIEW_2026-10-13.md`). `build_war` and `export_projections` refuse a run whose stamped
  models are not the production ones (`--require-backend`, default `tabpfn`; `weekly_refresh` passes `any` when its
  own backends are not production, so a trees refresh must say so).
- **Is the run working? `scripts/run_status.py <log>`** (owner, 2026-10-14): the live processes, the log's last line and
  its age, the stage against its cumulative budget, the GPU's power — and a verdict. The 2026-10-13/14 refresh sat ten
  hours in the to-date career tail at 100 % utilization and 91 W: utilization says busy, power says the card is waiting
  on memory. A GPU run is STALLED when its log has not moved for an hour, or the card draws under 120 W at full
  utilization; every unattended GPU run gets a wall-time budget (`timeout -k 60s 6h` in the chain) and the refresh has a
  fallback (`--no-career-todate`). The career model's pooled fit and its per-horizon predictions print their rows and
  seconds, so the tail is no longer one silent call.
- **A pricing change never waits for the GPU.** The GPU makes the projections (ppg, games, the band); everything after them
  is CPU arithmetic on the run in the bucket. `scripts/reprice_run.py` re-prices an in-season run with another age-level
  table (the originals kept beside it), and `weekly_refresh.py --preset production --skip inseason` rebuilds the WAR, the
  summaries and the exports from it (owner, 2026-10-14: "why do we have to wait until new projections come out of the
  GPU?"). The same holds for a replacement line, a discount or a sigma mode: re-price, rebuild, publish.
- **The unit of evidence is the cohort (test season), never the cohort-week.** The checkpoint weeks of one season share
  players and outcomes (intraclass correlation 0.6-0.9), so a t over cohort-weeks is ~1.7x too large (item 51's +6.9 is
  +4.0). Average within cohort first, then t over cohorts: `scripts/cohort_t.py`. Report the level (realized / priced)
  beside the ordering and the error: MAE on a zero-inflated target rewards shrinkage (review 2026-10-13, B3). **The
  level is part of the acceptance rule (owner, 2026-10-14):** every verdict prints the top-150 bias, and an error gain
  alone does not adopt when the candidate's level moves further from zero by more than 0.02 wins (it is a TRADE-OFF
  for the owner). Read the level by age band too (`scripts/age_gate.py`):
  under-25s realize ~1.2x their price and 29+ ~0.9x (C1), and a pricing change must not widen either end.

## What is where
| Path | What |
| --- | --- |
| `src/features.py`, `src/career.py` | the career matrix (lags, trends, targets at horizons 1-10) and `HorizonModels` (xgb / TabPFN / blend backends, pooled horizons, blend target, quantile bands, `estimate_sigma`, age-survival cap) |
| `src/feature_groups.py` | named feature groups for the career model (base, career, injury, trend, situation, rookie, college, team, contract, noise) + `Context` (lazy lake tables) |
| `src/inseason.py` | mid-season snapshots, the in-season groups (usage, role, schedule, opportunity, consensus, consensus_line, preseason, team_change), `InSeasonModels` (bags, focus_week), `inseason_value`, the FFToday bridge |
| `src/value.py`, `src/war.py`, `src/replacement.py`, `src/lineup.py`, `src/league.py` | replacement levels, PAR, WAR, the win curve, lineup optimisation, league specs |
| `src/market.py`, `src/picks.py`, `src/title.py`, `src/scoring.py` | KTC market + fair value, pick valuation (the pick curve, owned picks, rookie-year PAR), the title Monte Carlo, scoring scales |
| `src/experiments.py` | the harness: `run_experiment`, `score_cohort`, the ledger, `paired`, `verdict` (co-primary rule), `rescore` (offline value-side changes on saved cohort frames) |
| `src/draft_rows.py`, `src/unified.py` | pre-NFL rows for drafted rookies; snapshot rows in the career table (both tested, both NO) |
| `src/tail_cache.py` | the `--current` career tail cached per season (key = visible rows + config + code) |
| `src/power.py`, `src/gcs_io.py` | keep-awake for long runs; bucket IO |
| `scripts/run_experiment.py` | the harness CLI: variants, `--paired A B`, `--leaderboard`, `--save-cohorts`, `--rescore` |
| `scripts/backtest_inseason.py` | the in-season backtest by checkpoint week; `--current` = the production projection of the season in progress |
| `scripts/weekly_refresh.py` | the scheduled job: in-season projection → WAR per league → analysis summaries → the page export; `--preset production` |
| `scripts/build_war.py`, `scripts/export_projections.py` | per-league roster views (teams.parquet + meta) and `projections.json` for the page |
| `page/intrinsic_value.html`, `page/checks/` | the page itself (one HTML file reading `projections.json`) and its headless jsdom checks; see `page/CLAUDE.md` |
| `scripts/market_backtest.py`, `scripts/evaluate_*.py`, `scripts/build_*.py` | the model-vs-market backtest; older evaluation and matrix builders |

## The rules of the harness
- **Walk-forward cohorts** 2015-2022: train on what was known as of T, project T's rows, score realized
  wins three seasons out. `--replacement weekly` is production.
- **Co-primary verdict:** `spearman_war_top` (ordering of realized WAR among the top-150 projected) and
  `mae_war_top` (wins error on them). ADOPT when a candidate beats the **same-day** baseline past
  |t| = 2.4 on one with the other above t = -1. `verdict()` prints it; `--paired candidate baseline`.
- **Never two harness runs at once** (the ledger is read-modify-write). Chains run sequentially.
- **Regime:** a run is comparable only with runs on the same lineup / season-fact write / matrix height
  (`regime` column; `paired` warns).
- **Funnel for features:** trees screen (`backtest_inseason.py --device cpu --backend xgb
  --inseason-backend xgb --max-horizon 3`, ~1 min) → reduced 3.5 run (cohorts 2023-24, weeks 6/13)
  → full paired run. A feature that is flat on the trees stays in the registry, out of production.
- **Rescoring:** `--save-cohorts` on a run keeps its scored frames; `--rescore RUN --walk-scale |
  --fixed-scale QB=0.82 | --sigma band|none | --top-n` scores them again in minutes and pairs with the
  source. Use it for anything value-side.
- **Blends** (`scripts/blend_runs.py`, owner 2026-10-25): two runs' saved frames combined column by column and scored
  as a run (`--grid` the in-sample map, `--walk` the walk-forward weight that is the candidate, paired with run A);
  `A --market` maps the model + KTC rank blend on the priced cohorts. Verdicts: TabPFN + the trees NO at every weight
  (pure TabPFN best on both metrics; the walk-forward weight stayed at 1); model + market peaks at 0.6 on the model,
  0.71 vs 0.70 alone, three priced cohorts, in-sample -- not adopted (BACKLOG 57).
- **The wins unit is frozen within a year:** the harness fits the owner's league curve on the standings through the last
  complete season (`curve_for_lineage(..., through_load_date)`), and the curve is in the regime stamp. The season in
  progress had moved every realized WAR by up to 0.08 wins between runs eleven days apart; runs stamped before
  2026-10-25 carry the drifting curve. Two identical runs pair at t = 0 (not +-inf) and read NO.

## Production (what the owner runs)
- `weekly_refresh.py --preset production --device cuda`: TabPFN 3.5, the owner's feature set
  (base,career,injury,trend,situation,rookie,college), pooled horizons 1-10, blend target, bands on,
  in-season on 3.5, cap `30+t`. Writes `inseason/season=S/week=W/run_date=D/`, the `war/` views, the
  analysis summaries, `pages/.../projections.json`.
- The career tail and its sigma are cached per season (`_cache/career_tail/`, `tail_cache.py`); a run
  between completed seasons is the in-season stage plus exports. `--no-tail-cache` recomputes.
- The page's `projections.json` is a published supporting file of the artifact: republish it with the
  HTML (`files`) whenever the export changes.
- Long runs: write a `chain_*.sh`, wrap it in a `.cmd`, launch as a one-shot Task Scheduler task,
  watch its log. Never a background shell child (dies with the session). Time a preset end to end
  before scheduling it.
- **The Tuesday run is the local Task Scheduler task `ff-weekly-refresh`** (owner, 2026-10-21: "a cron job locally
  makes more sense right? Since the job runs local?"): Tuesdays 20:00 local, after the daily DAG (10:00 UTC) has the
  Monday game; it wakes the PC and runs `scripts/weekly_refresh_task.cmd` → `weekly_refresh_task.sh`: the production
  preset under a 6 h budget, then if that fails the orphans killed and the `--no-career-todate` fallback under 4 h; it
  refuses to start over another GPU run. Logs under `_cache/logs/` — `weekly_refresh_task.log` (the script's lines, every
  run appended) and one `production_<stamp>.log` per run, the file `run_status.py` takes. It runs only while the owner is
  logged on (interactive token: no stored password). The week is the lake's latest (`current_season_week`: the max week
  in the week table, no completeness check). The cloud job `ml-weekly-refresh` stays paused. A run now = the same
  `.cmd` as a one-shot task. The page is not in it: republishing `projections.json` with the HTML needs a session.

## Where the numbers are
- `BACKLOG.md` items 22 (bake-off), 36 (accuracy programme), 37-39 (in-season funnel), 40 (title
  odds), 42 (positional lean), 43-44 (draft rows NO, blend ADOPT), 32 (unified model).
- The ledger: `scripts/run_experiment.py --leaderboard`; a run's per-cohort rows under
  `experiments/runs/`.
