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

## Phase 3 — in-season update (current)

The primary in-season view (`src/inseason.py`). A **snapshot** is (player, season T, through
week W): the prior-season feature set (as of T−1) plus this season to date (games, per-game
rate, usage, last-3 form, missed weeks). Targets: rest-of-season ppg/games (0 games if he never
played again) and next-season ppg/games (attrition learnt). Trained on every (season, week)
snapshot back to 1999 (~199k rows), so the model learns how far a 3-week sample should move a
projection versus a 10-week one. `inseason_value` = ROS (undiscounted) + next season from this
model + seasons T+2.. from the career model off the latest complete season, discounted — the
dynasty-relevant number to compare with the market in-season.

```
scripts/backtest_inseason.py            # by checkpoint week x cohort vs KTC + naive baselines; market-lag test
scripts/backtest_inseason.py --current  # project the in-progress season; writes gs://.../inseason/season=YYYY/week=W/
```

Backtest (cohorts 2021–2024, checkpoints weeks 3/6/9/13, players KTC priced that week):
- **Rest of season**: rank correlation with realized ROS ppg, model **0.78 vs KTC 0.70** at week 3,
  widening to 0.69 vs 0.57 by week 13 (to-date ppg alone: 0.70; last season alone: 0.60).
- **Next season**: in-season IV **0.565 vs KTC 0.555** (W3) … 0.598 vs 0.581 (W13); a 50/50
  last-season/to-date blend is nearly as good (0.55–0.59), so the gain over a sensible heuristic is thin.
- **Market lag**: the gap between the *next-season* projection and KTC at week W predicts KTC's move
  to February (Spearman −0.19 at W3): the third the market priced cheap vs fundamentals gained ~+6 %,
  the rich third ~−7 %, and a naive "hot start" rule has no such power. The *multi-year* IV gap does
  not predict the move — the in-season market chases near-term production, so that is where the
  tradable lag is.
Known limits: rookies carry only draft slot + a few weeks (no college inputs); survivorship at the
oldest ages (see the age-survival prior); one lineup.

## Model performance panel

The three backtests persist a summary to the ML bucket (`backtests/{career_eval,value,inseason}/run_date=D/summary.json`,
skip with `--no-write`), and `export_projections.py` folds the latest of each, plus the experiment
leaderboard, into the table's data file. The page's "How the model performs" section renders them:
in-season rest-of-season and next-season rank agreement vs KTC / this-season / last-season / blend
baselines by checkpoint week, multi-year intrinsic value vs KTC with a paired bootstrap and the
mispricing terciles, career projection MAE vs carry-forward and age decay by horizon, the
feature-group leaderboard, and dated one-off checks.

## Intrinsic value v2 — wins above replacement (WAR), league-dependent

`src/league.py`, `src/lineup.py`, `src/war.py`, `scripts/build_war.py`. Value in **wins**, built
from the league's own configuration rather than a fixed line:

1. **Lineup** (`LeagueSpec`): teams + starting slots + slot eligibility, from `dim_league_settings`
   or any JSON (`leagues/12team_1qb.json`), so the same projections price differently per league.
2. **Replacement** (`lineup.league_fill`): an explicit fill of every team's slots from the pool
   (dedicated slots first, then the more restrictive flex), averaged over the last five real
   seasons. The owner's superflex league actually starts ~28 RB / ~31 WR / ~12 TE, not the
   24.5 / 34.5 / 11 the fixed flex shares assumed, which moves RB replacement from 10.9 to 9.9 ppg.
   Backtested like for like, fill-based replacement is at least as good as the production line
   (rank agreement with realized value 0.673 vs 0.671).
3. **Win curve** (`WinCurve`): P(win a week | points) on the league's own standings
   (`team_weeks_from_standings`); a logistic fit with 300+ team-weeks, otherwise a normal-margin
   curve from the league's weekly mean / spread. Wins are linear-to-concave in points, so there
   is no top-end convexity in production value; the title premium is a team-context layer.
4. **WAR** = Σ_k (1 − r)^(k−1) · games_k · [W(μ + excess_k) − W(μ)] with excess_k the sigma-aware
   points above replacement per game; the rest of this season is the first, undiscounted span.
   PAR is the linear special case and is reported alongside.
5. **Per roster** (`war.team_marginal_war`, `--teams`): marginal wins each player adds to HIS
   roster (optimal lineup with him minus without, exact assignment incl. FLEX / SUPER_FLEX),
   mapped through the curve at that roster's own weekly total, plus the best outside targets.

```
scripts/build_war.py --season 2026 --week 3 --run-date 2026-10-01 --teams      # owner's league, in-season
scripts/build_war.py --season 2026 --week 3 --run-date 2026-10-01 --league leagues/12team_1qb.json
```
Outputs: `war/league=<name>/season=S/week=W/run_date=D/{projections, teams}.parquet + meta.json`.

**One model, many leagues (`src/scoring.py`).** The projection model is trained once, on the
primary league's scoring (Stuck in High School). For another league, every player's real weeks
from the last three seasons are re-scored under both rule sets and the ratio scales his projected
ppg and his historical ppg (so replacement is in the same units); differences the weekly fact
cannot rebuild (fumbles, 2-pt, yardage bonuses) are shared across the owner's leagues and cancel.
`build_war.py --all-leagues` notes each league's adjustment in its meta.

Known calibration gap (first-3-span shares vs realized 3-year shares, 2017–2022 cohorts): the
model gives QBs ~33 % of league value where QBs delivered ~26 %, and WRs ~29 % where they
delivered ~38 %; QB projection spread (sigma 5.2 vs 2.5 for WR) inflates QB upside credit. A
position-level calibration is a value-definition experiment, not a feature one (BACKLOG 13).

## Experiments — mix and match feature groups

Every modelling angle is kept as a named **feature group** (`src/feature_groups.py`) and any
combination can be trained and scored through the same walk-forward backtest
(`src/experiments.py`, `scripts/run_experiment.py`). A group adds columns to the career matrix
using only information known by the end of season T, so every variant is comparable and
leak-free against KTC the following February:

| group | source | what it adds |
|---|---|---|
| `base` | fact_player_season (+ lags) | production, volume, bio — the production model's inputs |
| `career` | fact_player_season | cumulative seasons / points / games, best ppg |
| `injury` | fact_player_injury_week | weeks out / listed / on reserve, by class (soft tissue, structural, concussion), plus last year's |
| `role` | fact_depth_chart_week | depth entering / leaving the season, best depth, starter share, moves, overall position rank |
| `trend` | fact_player_week | second-half vs first-half ppg / targets / touches, last-4 form |
| `situation` | fact_player_season | changed team this season / last season |

```
scripts/run_experiment.py --list-groups
scripts/run_experiment.py --variants "current=base,career;injury=base,career,injury;all=base,career,injury,role,trend,situation" --horizon 3 --first-cohort 2015
scripts/run_experiment.py --leaderboard --horizon 3
```

Each run reports, per cohort and on average: rank agreement of IV with realized H-season PAR on
the players KTC priced (and KTC's own, the bar to clear), the same on every projected player,
top-decile precision, and points MAE per horizon. Summaries append to
`gs://fantasy-football-ml/dynasty-value/experiments/ledger.parquet` with the group list and git
commit, so the leaderboard compares like for like (same horizon, same cohorts). A change to the
production feature set is accepted only when it wins there.

## Phase 2 — intrinsic value

What no site publishes: a value built from **fundamentals** (projected career production) rather
than from what the market thinks. Like a DCF for a company:

1. **Project the career** (`src/career.py`): for every player-season T, one model pair per
   horizon k = 1..H predicts season T+k — `ppg` (a rate, trained on players who played) and
   `games` (availability, with 0 for players who had left the league, so attrition is learnt,
   not assumed). Direct multi-horizon models: real outcomes per horizon, no compounding of a
   one-year model, and outcomes not yet observable are censored. `estimate_sigma` measures the
   out-of-sample spread of each horizon's `ppg` projection on held-out recent seasons.
   `career.AgeSurvival` is a population prior on availability: the games models see only the
   survivors at the oldest ages (every 43-year-old QB season in the data is Tom Brady's), so
   projected games at horizon k are capped at `17 × P(still playing k years out | position, age)`
   from a logistic fit of year-over-year continuation. It only binds for old players.
2. **Replacement level** (`src/replacement.py`): from the league's lineup in
   `dim_league_settings` (10-team superflex: ~20 QB / 24.5 RB / 34.5 WR / 11 TE starters),
   replacement ppg = the player just outside the starters, averaged over the last 5 seasons.
3. **Value** (`src/value.py`): per horizon, *expected* points above replacement
   `E[max(ppg − rep, 0)] × games` under the projection's spread (so a player projected near
   replacement keeps his upside instead of being worth exactly 0), then
   `IV = Σ_k (1 − r)^(k−1) · VORP_k` with a per-year discount rate `r = 20 %` (season 1, the
   coming season, at full weight; `r = 100 %` means this season only; a manager time-preference
   parameter exposed as a slider in the projections table).
4. **Market comparison** (`src/market.py`, `value.compare_to_market`): KTC superflex values
   joined by id (`dim_players_master.gsis_id`, trimmed) with a name+position fallback; a
   power-law fit `log(market) = a + b·log(IV + 1)` (isotonic optional) maps IV onto the market's
   scale so `mispricing = market − fair_value` is in market units, plus rank gaps; the same fit
   within each position factors out a position-wide premium.

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
