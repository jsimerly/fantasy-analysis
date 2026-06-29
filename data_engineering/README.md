# data_engineering

Bronze ingestion + silver modeling for the fantasy data lake (`gs://nfl-data-bronze`).

**One uv-managed project.** Every package lives under `src/`, dependencies are consolidated
in `pyproject.toml` (Python 3.11), and each Cloud Run job runs **one module** via
`python -m <package.module>` off a single shared image.

## Layout

```
src/
  sleeper_ingestion/      api/, daily/, historical/, league_crawler*, _utils.py
  ktc_ingestion/          utils.py, dynasty/ devy/ redraft/
  fantasycalc_ingestion/  daily_ingestion.py
  nflverse_ingestion/     *_ingestion.py
  fantasypros_ingestion/  projections_scraper.py   (local-only; not deployed)
  silver_fantasy/         dim_*.py, fact_*.py, utils.py, _staging/
tests/                    per-package; pytest in importlib mode, gql/nflreadpy stubs + fake_gcs
Dockerfile                single image for all jobs (uv base, PYTHONPATH=/app/src)
pyproject.toml / uv.lock  consolidated deps
```

Imports are qualified (`from silver_fantasy.utils import ...`) — no `sys.path` hacks, no
`de_loader` shim. The previous per-package `requirements.txt` / `Dockerfile` / `.venv` are
gone; the heavy nflverse serverless cruft (Flask/FastAPI/`nfl_data_py`) was dropped.

## Develop / test

```bash
cd data_engineering
uv sync
uv run pytest                        # all tests
uv run pytest tests/silver_fantasy   # one package
```

## Run a job locally

```bash
PYTHONPATH=src uv run python -m silver_fantasy.fact_player_season
```

Local runs read `GCS_BUCKET_NAME` (+ `AUTH_TOKEN`, `LOCAL_DB_URI`) from `.env` (gitignored).
In Cloud Run these come from `--set-env-vars` instead.

## Deploy

Push to `main` → [.github/workflows/deploy-data-engineering.yaml](../.github/workflows/deploy-data-engineering.yaml)
builds the single image and deploys all 34 Cloud Run jobs (`--command python --args=-m,<module>`,
preserving job names + memory/cpu/timeout). The orchestration DAG (job names unchanged) is
deployed by `deploy-orchestration.yaml`. CI runs both test suites via
[.github/workflows/tests.yaml](../.github/workflows/tests.yaml).
