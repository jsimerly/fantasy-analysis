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

## Phase 2 — intrinsic value (current)

What no site publishes: a value built from **fundamentals** (projected career production) rather
than from what the market thinks. Like a DCF for a company:

1. **Project the career** (`src/career.py`): for every player-season T, one model pair per
   horizon k = 1..H predicts season T+k — `ppg` (a rate, trained on players who played) and
   `games` (availability, with 0 for players who had left the league, so attrition is learnt,
   not assumed). Direct multi-horizon models: real outcomes per horizon, no compounding of a
   one-year model, and outcomes not yet observable are censored. `estimate_sigma` measures the
   out-of-sample spread of each horizon's `ppg` projection on held-out recent seasons.
2. **Replacement level** (`src/replacement.py`): from the league's lineup in
   `dim_league_settings` (10-team superflex: ~20 QB / 24.5 RB / 34.5 WR / 11 TE starters),
   replacement ppg = the player just outside the starters, averaged over the last 5 seasons.
3. **Value** (`src/value.py`): per horizon, *expected* points above replacement
   `E[max(ppg − rep, 0)] × games` under the projection's spread (so a player projected near
   replacement keeps his upside instead of being worth exactly 0), then
   `IV = Σ_k discount^k · VORP_k` with `discount = 0.8` (a manager time-preference parameter —
   tune it).
4. **Market comparison** (`src/market.py`, `value.compare_to_market`): KTC superflex values
   joined by id (`dim_players_master.gsis_id`, trimmed) with a name+position fallback; an
   isotonic fit maps IV onto KTC's scale so `mispricing = market − fair_value` is in market
   units, plus rank gaps.

```
scripts/
  evaluate_career.py        # walk-forward MAE per horizon: model vs carry-forward vs age/attrition decay
  backtest_value.py         # IV as of T vs KTC as of Feb T+1 vs REALIZED discounted value over T+1..T+H
  build_intrinsic_value.py  # today's IV for every player with a row in the latest complete season,
                            # market comparison, writes to gs://fantasy-football-ml/dynasty-value/intrinsic_value/
```

First results (2026-10-01, walk-forward 2010+): the career model beats carry-forward by 11 %
(h1) → 50 % (h5) MAE and the age/attrition decay baseline by 5–8 % at every horizon. Backtest
(cohorts 2020–2022, H=3): rank correlation with realized value **IV 0.67 vs KTC 0.66**, a
50/50 blend 0.68 — fundamentals alone are on par with the market and carry complementary
information. Known limits: production-only (no college / draft-year inputs → 2026 rookies and
players without a row in the latest complete season are not projected; young breakouts are
shrunk toward similar histories), one lineup (the primary lineage), point projections + a
normal spread rather than full distributions.

## Phase 0 — player-season feature table

One leakage-safe row per player-season: features as of year **T**, target = next-season
(**T+1**) fantasy points (re-scored upstream under the owner's superflex scoring). Only a
**complete** season can be a target (`fact_player_season.season_complete`): the lake also
carries the in-progress season, which must not become anyone's "next season" outcome.

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
