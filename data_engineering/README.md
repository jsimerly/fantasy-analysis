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
  nflverse_ingestion/     *_ingestion.py, backfill_seasonal.py (daily reconcile of seasonal history)
  fantasypros_ingestion/  projections_scraper.py   (local-only; not deployed)
  cfbd_ingestion/         client.py, backfill.py   (College Football Data; local backfill, CFBD_API_KEY)
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

## College data (CFBD)

`cfbd_ingestion.backfill` pulls College Football Data into `bronze/cfbd/<dataset>/season=YYYY/`
(player season stats, player usage shares, rosters, SP+ ratings, team season volume, FBS teams,
NFL draft picks with college athlete ids). It needs a free key from
https://collegefootballdata.com/key in `.env` as `CFBD_API_KEY`; the free tier is metered per
month, so the job writes one file per (dataset, season) and skips what exists. Then
`silver_fantasy.fact_college_player_season` builds the per-(player, season) college fact (box
score, usage, team SP+ and volume, yards / td / touch shares, dominator, breakout flags) and
`dim_college_crosswalk` (CFBD id → gsis id by draft year + overall pick, then name + position),
which the ML `college` feature group reads.

```bash
PYTHONPATH=src uv run python -m cfbd_ingestion.backfill --start 2010 --end 2025      # ~100 calls, a few minutes
PYTHONPATH=src uv run python -m silver_fantasy.fact_college_player_season
```

The crosswalk joins CFBD's draft picks to our ids by (draft year, overall pick) against
`fact_player_season` (every drafted skill player), then by name + position (CFBD spells positions
out; they are mapped): 1,719 of 4,043 drafted players across all positions, which covers the
drafted QB / RB / WR / TE of 2010-2025.

## Weekly player status

`silver_fantasy.fact_player_week_status` writes `silver/fantasy/fact_player_week_status/data.parquet`:
one row per (season, regular-season week, skill player) from 2002, with `status` = played / bye /
injured_reserve / injured_out / suspended / practice_squad / inactive / dnp / not_rostered, the
injury body-part class (the report's, carried through a reserve stint only from a week he missed;
null = hurt, body part unknown), roster status, the stat line (points, targets, opportunities,
touches) and the offensive snap share (2013+, through the roster's PFR id). Sources: nflverse weekly
rosters, schedules (byes), snap counts, `fact_player_week`, `fact_player_injury_week`. The ML
`weekly` feature group reads it. Run it after the weekly nflverse loads:

```
PYTHONPATH=src uv run python -m silver_fantasy.fact_player_week_status
```

Scheduled once a year: the `fantasy-college-yearly` Cloud Workflow (orchestration/college.yaml)
runs `cfbd-backfill` with `CFBD_SEASONS=last` and then the silver job on 5 February, two weeks
after the CFP title game; the key comes from Secret Manager (`cfbd-api-key`, see
orchestration/README.md). Locally, `--seasons last` does the same refresh
(`--datasets draft_picks --force` for the new class).

## Deploy

Push to `main` → [.github/workflows/deploy-data-engineering.yaml](../.github/workflows/deploy-data-engineering.yaml)
builds the single image and deploys all 34 Cloud Run jobs (`--command python --args=-m,<module>`,
preserving job names + memory/cpu/timeout). The orchestration DAG (job names unchanged) is
deployed by `deploy-orchestration.yaml`. CI runs both test suites via
[.github/workflows/tests.yaml](../.github/workflows/tests.yaml).
