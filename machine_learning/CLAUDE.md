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
  2026-10-13, `docs/MODEL_REVIEW_2026-10-13.md`).
- **The unit of evidence is the cohort (test season), never the cohort-week.** The checkpoint weeks of one season share
  players and outcomes (intraclass correlation 0.6-0.9), so a t over cohort-weeks is ~1.7x too large (item 51's +6.9 is
  +4.0). Average within cohort first, then t over cohorts: `scripts/cohort_t.py`. Report the level (realized / priced)
  beside the ordering and the error: MAE on a zero-inflated target rewards shrinkage (review 2026-10-13, B3).

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

## Where the numbers are
- `BACKLOG.md` items 22 (bake-off), 36 (accuracy programme), 37-39 (in-season funnel), 40 (title
  odds), 42 (positional lean), 43-44 (draft rows NO, blend ADOPT), 32 (unified model).
- The ledger: `scripts/run_experiment.py --leaderboard`; a run's per-cohort rows under
  `experiments/runs/`.
