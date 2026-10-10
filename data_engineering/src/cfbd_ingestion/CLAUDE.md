# cfbd_ingestion — college production (College Football Data API)

The college inputs behind the model's `college` feature group and the rookie evaluation
(machine_learning/BACKLOG.md items 17 and 24): every drafted player's college box score, usage
share, team strength and the derived dominator / breakout flags.

| File | What |
| --- | --- |
| `client.py` | a thin GET client for https://api.collegefootballdata.com: Bearer token from `CFBD_API_KEY` (`.env` locally, the job's env in Cloud Run), 429s back off and retry, other errors raise |
| `backfill.py` | `python -m cfbd_ingestion.backfill --start 2010 --end 2025 [--datasets ...] [--force]`: one parquet per (dataset, season) under `bronze/cfbd/<dataset>/season=YYYY/`, skipping what exists (the free tier is metered per month) |

Datasets: `player_season_stats` (one row per player, category, stat), `player_usage` (share of the
team's plays, overall / pass / rush / by down), `rosters` (team, position, class year, size, hometown),
`team_sp` (SP+ overall / offense / defense and rank), `team_season_stats` (team volume: plays, yards,
attempts), `teams` (FBS teams, conference), `draft_picks` (NFL draft picks with the college athlete id,
the bridge to gsis).

Downstream: `silver_fantasy.fact_college_player_season` builds the per-(player, season) college fact and
`dim_college_crosswalk` (CFBD id → gsis by draft year + overall pick, then name + position). The yearly
refresh is the `fantasy-college-yearly` workflow (orchestration/college.yaml, February); the one-time
backfill was run by hand. The key is in Secret Manager (`cfbd-api-key`) for the deployed job.
