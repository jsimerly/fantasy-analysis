# Dynasty Intrinsic Value Model

Estimates the **intrinsic dynasty value** of fantasy football players (project future
production → points above replacement → discount → present value) to find mispricings vs.
the market (KTC / FantasyCalc). Format: **superflex / 2QB**.

## Where things live

Datasets that are reusable beyond this model are **data engineering**; modeling-specific
feature work is **machine learning**:

- **`data_engineering/silver_fantasy/fact_player_season.py`** — builds the reusable
  player-season production fact (nflverse box scores re-scored under the league's rules +
  volume + bio) → `gs://nfl-data-bronze/silver/fantasy/fact_player_season/data.parquet`.
- **`machine_learning/`** (this dir) — reads that fact and adds the model-exclusive feature
  engineering (lags, T+1 target, splits, models).

Buckets, by ownership:
- `nfl-data-bronze` (lake) — shared DE datasets; the model only **reads** from here.
- ML bucket (`$ML_BUCKET`, default `fantasy-football-ml`) — model-exclusive artifacts,
  namespaced per project under `<ML_BUCKET>/dynasty-value/`. Not used in Phase 0 (nothing
  persisted yet); writes begin in Phase 1+.

## Phase 0 — player-season feature table (current)

One leakage-safe row per player-season: features as of year **T**, target = next-season
(**T+1**) fantasy points (re-scored upstream under the owner's superflex scoring).

```
src/
  gcs_io.py     # lake reader (read fact_player_season) + ML-bucket path helper
  features.py   # load fact + attach lags (T-1, T-2) + T+1 target (leakage-safe)
scripts/
  build_training_matrix.py   # read fact → in-memory training matrix + summary (no write)
tests/          # pytest: lag/target leakage guards (no network)
```

The re-scoring engine + player-season aggregation are tested in the DE suite
(`tests/silver_fantasy/test_fact_player_season.py`); this package tests only the
modeling-specific lag/target assembly.

## Setup

Fresh **uv** venv on **Python 3.11+** (isolated from the repo-root ETL venv). Requires
Application Default Credentials for GCS (already configured on this machine).

```bash
cd machine_learning
uv sync                                          # creates .venv (3.11) + installs deps
uv run pytest                                    # unit tests
uv run python scripts/build_training_matrix.py   # build the training matrix from the lake
```

Rebuild the upstream dataset (DE, run from repo root on the ETL venv) when nflverse data
or league scoring changes:

```bash
python data_engineering/silver_fantasy/fact_player_season.py
```
