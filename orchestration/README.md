# Pipeline orchestration

The daily pipeline is a **single Cloud Workflow** ([pipeline.yaml](pipeline.yaml)),
triggered once per day by one Cloud Scheduler job. It replaces the previous setup of
14 independent fixed-time scheduler triggers, where silver ran at a flat 11:00 and just
*hoped* bronze had finished by then.

Now the dependency order is explicit and lives in the repo, so it can't drift the way
per-job schedulers did (renamed jobs left orphan triggers firing the wrong thing).

## The DAG

```
                          ┌─────────────────────────── BRONZE (parallel) ───────────────────────────┐
  Cloud Scheduler         sleeper-incremental-league / -players / -rosters / -transactions / -users
  (1 trigger, 10:00 UTC)  sleeper-drafts-overview · fantasycalc-daily
        │                 ktc-incremental-{dynasty,redraft,devy} · nflverse-daily
        ▼                            │ (all complete)
   fantasy-pipeline ─────────────────┤
   (Cloud Workflow)                  ▼
                          ┌──────────── SILVER T1 (parallel, read bronze) ───────────┐
                          dim-league-meta · dim-league-settings · dim-player-master
                          dim-users · staging-asset-alignment
                                     │ (all complete)
                                     ▼
                          ┌──────────── SILVER T2 (read T1) ────────────┐
                          dim-franchise-meta · fact-asset-values · fact-player-week
                                     │ (all complete)
                                     ▼
                          ┌──────────── SILVER T3 (read T2) ────────────┐
                          fact-roster-membership · fact-player-season
```

### Dependency rationale
- **dim-franchise-meta** (T2) reads `dim_leagues_meta` + `dim_users` (T1).
- **fact-asset-values** (T2) reads `_staging/asset_values_long` (staging-asset-alignment, T1)
  + `dim_players_master` (T1).
- **fact-player-week** (T2) reads `dim_league_settings` (T1, for scoring) + bronze nflverse
  `player_stats`/`schedules`/`nfl_players`; it's the atomic player-week production fact.
- **fact-roster-membership** (T3) reads `dim_franchises_meta` (T2) + `dim_players_master`
  (T1) + `dim_leagues_meta` (T1) + bronze rosters/picks/drafts.
- **fact-player-season** (T3) is a season rollup of `fact-player-week` (T2).
- **sleeper-drafts-overview** is in the daily bronze tier (not just manual) because
  fact-roster-membership keys the pick lifecycle off draft `status`/`rounds` — it must
  stay current so a completed draft rolls the pick window.

### Execution semantics
Each job runs **best-effort**: a single failure is recorded but does not abort the run,
so independent jobs still complete and silver still builds on the latest *available*
bronze. All failures are collected and the workflow is **failed at the very end**, so a
bad run still shows up red for alerting. The `run.v2` connector blocks until each job's
execution finishes (a failed job raises and is caught per-job).

Excluded from the daily DAG (run manually): the `*-full` / backfill jobs
(`ktc-full-*`, `nflverse-{full,combine,draft,schedule,nfl-draft-picks}`,
`sleeper-{league-lineage,league-transactions,draft-picks}`) and
`sleeper-commissioner-adjustments` (a static hand-maintained list — run it only when you
edit the overrides).

## Deploy

[`.github/workflows/deploy-orchestration.yaml`](../.github/workflows/deploy-orchestration.yaml)
deploys the workflow and creates/updates the single scheduler (`fantasy-pipeline-daily`,
10:00 UTC) on push to `main` under `orchestration/**`, or via `workflow_dispatch`.

## Migration: retire the old triggers

After the new `fantasy-pipeline-daily` trigger is confirmed working, **delete the 14
legacy per-job schedulers** so jobs don't double-run:

```
silver-dim-leagues-scheduler-trigger          silver-dim-league-scoring-scheduler-trigger
silver-dim-franchise-meta-scheduler-trigger   silver-dim-player-master-scheduler-trigger
sleeper-incremental-league-scheduler-trigger  sleeper-incremental-players-scheduler-trigger
sleeper-incremental-users-scheduler-trigger   sleeper-incremental-rosters-scheduler-trigger
sleeper-incremental-transactions-scheduler-trigger
fantasycalc-daily-scheduler-trigger           ktc-dynasty-incremental-daily
ktc-incremental-redraft-scheduler-trigger     ktc-incremental-devy-scheduler-trigger
nflverse-daily-scheduler-trigger
```

(These are Cloud Scheduler jobs, not the Cloud Run jobs — deleting a trigger does not
touch the job it ran.)
