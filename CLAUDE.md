# Fantasy Analysis — repo guide

Dynasty fantasy-football analytics for the owner's Sleeper leagues: ingest raw data from several
providers into a GCS lake, model it into clean dimensions and facts, project every player's career
with a foundation model, turn the projections into wins for each league, and publish a page that
prices trades, picks and rosters.

**Start with [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md):** the whole system in three diagrams, the
model in one page, the lake layout, a glossary. This file is the short operating guide. Each major
folder has its own `CLAUDE.md` with the local details; read it when working there.

## Architecture (medallion: bronze → silver → model → page)
- **bronze** — raw provider data, lightly flattened, one folder per source under
  [data_engineering/src/](data_engineering/src/). Append-only, partitioned by `load_date` (or `season`).
- **silver** — modeled dims and facts built from bronze, in
  [data_engineering/src/silver_fantasy/](data_engineering/src/silver_fantasy/); checked by the
  data-quality suite as the last step of every daily run.
- **model** — [machine_learning/](machine_learning/): career and in-season TabPFN models, replacement
  and wins above replacement per league, the harness that accepts or rejects every change, the
  weekly refresh and the page export. Runs on the owner's GPU.
- **analysis** — [analysis/](analysis/): the scripts behind the page's Trades, Draft slots, Team value
  and Market tabs and the title odds, plus notebooks.

Data sources: **Sleeper** (the league platform: leagues, rosters, transactions, drafts, weekly
projections), **KTC** (KeepTradeCut dynasty values), **FantasyCalc** (values), **nflverse** (NFL stats,
schedules, injuries, depth charts, Next Gen, contracts, ids), **FantasyPros** (pre-game projections),
**FFToday** (weekly projections 2010-2017), **FFC / MFL** (ADP 2009 on), **CFBD** (college production).

## GCP / infra
- Project **`fantasy-football-473418`**, region **us-central1**. The lake is **`gs://nfl-data-bronze`**
  (`bronze/` and `silver/`); the model's own outputs go to **`gs://fantasy-football-ml/dynasty-value/`**.
- Each pipeline is a **Cloud Run job**; a single **Cloud Workflow** DAG runs them daily in dependency
  order — see [orchestration/](orchestration/) (don't re-document the schedule elsewhere).
- **Deploy = push to `main`** (GitHub Actions under [.github/workflows/](.github/workflows/)). Backfill
  `*-full` / `*-yearly` jobs are run by hand, never in the daily DAG, and only on the owner's go.

## Bucket layout (`gs://nfl-data-bronze`)
```
bronze/<source>/<entity>/<load_style>/load_date=<YYYY-MM-DD>/data.parquet   (or season=<YYYY>/)
  sleeper/…   ktc/…   fantasycalc/…   nflverse/…   fantasypros/…   fftoday/…   adp/…   cfbd/…
  sleeper_crawl/…  = the broad 10k-league discovery crawl (kept OUT of bronze/sleeper on purpose)
silver/fantasy/<dim_or_fact>/data.parquet
silver/_quality/run_date=<YYYY-MM-DD>/results.parquet
```
`load_style` is `full_load` (one-time backfill), `incremental`/`daily`, or `weekly`. The per-dataset
inventory with quirks lives in each source folder's `CLAUDE.md`; keep it current when a dataset changes.

## Working in this repo
- **Three Python projects, each with its own venv:** `data_engineering/` (uv project, `PYTHONPATH=src`,
  scripts run as `python -m <source>.<script>`), `machine_learning/` (uv project, GPU), `analysis/`
  (runs on the machine_learning venv). Polars everywhere; pandas only where a library needs it.
- **Tests:** `.venv/Scripts/python.exe -m pytest tests -q` inside `data_engineering/` and inside
  `machine_learning/` (CI runs both); `analysis/tests/` runs on the machine_learning venv. All green
  before pushing.
- **Data quality:** the code is tested by pytest, the *lake* by
  [data_engineering/src/data_quality/](data_engineering/src/data_quality/) (the daily DAG's last step).
  A data bug fixed without a check there is only half fixed.
- **Lake writes** (backfills, re-scrapes) only on the owner's explicit go. The model never writes the lake.
- **PRs:** the owner merges their own PRs — open the PR, don't merge it. Check `gh pr view <n>` before
  pushing to a branch that had a PR: a merged PR means a fresh branch off `main`.

## Repo map
| Path | What's there |
| --- | --- |
| [data_engineering/](data_engineering/) | bronze ingestion per source, silver modeling, the data-quality suite, the DE tests |
| [machine_learning/](machine_learning/) | the dynasty value model, the harness, the weekly refresh, the page export; `BACKLOG.md` is the journal |
| [analysis/](analysis/) | the page's analysis summaries (trades, draft slots, team value, market trends, title odds), `fantasy_lib.py`, notebooks |
| [orchestration/](orchestration/) | the daily Cloud Workflow DAG, the yearly college workflow, their deploy |
| [docs/](docs/) | the architecture overview and glossary |
| `.github/workflows/` | tests on every push; deploys of each project on push to `main` |
