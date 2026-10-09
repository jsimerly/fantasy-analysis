# Fantasy Analysis — repo guide

Dynasty fantasy-football analytics for the owner's leagues: ingest raw data from several
providers, model it into clean dimensions/facts, and explore it in notebooks (team value over
time, trade/pick valuation, draft-slot projection, a team-finish ML model).

## Architecture (medallion: bronze → silver → analysis)
- **bronze** — raw provider data, lightly flattened, one folder per source under
  [data_engineering/](data_engineering/). Append-only, partitioned by `load_date` (or `season`).
- **silver** — modeled dims & facts built from bronze, in
  [data_engineering/silver_fantasy/](data_engineering/silver_fantasy/).
- **analysis** — notebooks + a shared helper lib in [analysis/](analysis/) that read silver/bronze.

Data sources: **Sleeper** (the league platform — leagues, rosters, transactions, drafts),
**KTC** (KeepTradeCut dynasty values), **FantasyCalc** (values), **nflverse** (NFL stats/schedules),
**FantasyPros** (pre-game projections — on a feature branch).

## GCP / infra
- Project **`fantasy-football-473418`**, region **us-central1**, single bucket **`gs://nfl-data-bronze`**
  (holds both `bronze/` and `silver/`).
- Each pipeline is a **Cloud Run job**; a single **Cloud Workflow** DAG runs them daily in dependency
  order — see [orchestration/](orchestration/) (don't re-document the schedule elsewhere).
- **Deploy = push to `main`** (GitHub Actions under [.github/workflows/](.github/workflows/)). Backfill
  `*-full` jobs are run manually, not in the daily DAG.

## Bucket layout (`gs://nfl-data-bronze`)
```
bronze/<source>/<entity>/<load_style>/load_date=<YYYY-MM-DD>/data.parquet
  sleeper/…   ktc/…   fantasycalc/…   nflverse/… (by season)   fantasypros/…
  sleeper_crawl/…  = the broad 10k-league discovery crawl (kept OUT of bronze/sleeper on purpose)
silver/fantasy/<dim_or_fact>/data.parquet
```
`load_style` is `full_load` (one-time backfill), `incremental`/`daily`, or `weekly`. A fuller
per-dataset inventory with quirks lives in the data inventory note; mirror changes there.

## Working in this repo
- **Python via the repo-root venv:** `.venv/Scripts/python.exe` (polars-based; pandas where needed).
- **Tests:** `.venv/Scripts/python.exe -m pytest` from the root. Spec-style suite — see
  [tests/README.md](tests/README.md). Keep it green before pushing.
- **Data quality:** the code is tested by pytest, the *lake* by
  [data_engineering/src/data_quality/](data_engineering/src/data_quality/) (the daily DAG's last step).
  A data bug fixed without a check there is only half fixed.
- **PRs:** the owner merges their own PRs — open the PR, don't merge it.

## Repo map
| Path | What's there |
| --- | --- |
| [data_engineering/](data_engineering/) | all bronze ingestion + silver modeling (one folder per source) |
| [analysis/](analysis/) | notebooks + `fantasy_lib.py` (shared value/event helpers) |
| [orchestration/](orchestration/) | the daily Cloud Workflow DAG + deploy |
| [tests/](tests/) | pytest spec suite for the ETL pipelines |
| [alembic/](alembic/) | DB migrations (a local relational mirror; secondary to the GCS lake) |
| `data/`, `*.ipynb` (root), `*.xlsx/.csv` | older/loose analysis scratch — not part of the pipeline |

Each major folder has its own `CLAUDE.md` with the local details — read it when working there.
