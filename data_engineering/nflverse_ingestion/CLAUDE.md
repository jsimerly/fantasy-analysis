# nflverse_ingestion — NFL stats & IDs

Wraps the **`nflreadpy`** package to pull NFL data (stats, schedules, rosters, IDs) into
`bronze/nflverse/…`. This is the league-agnostic NFL side that enriches the player dimension and (later)
feeds projections/actuals.

## Files
| File | Cloud Run job | Role |
| --- | --- | --- |
| `daily_ingestion.py` | `nflverse-daily` (in DAG) | config-driven loader of ~15 datasets (see below) |
| `full_ingestion.py` | `nflverse-full` (manual) | deep-history backfill of the daily datasets (1999→) |
| `combine_ingestion.py` | `nflverse-combine` (manual) | scouting combine |
| `draft_ingestion.py` / `nfl_draft_picks.py` | `nflverse-draft` / `nflverse-nfl-draft-picks` (manual) | NFL draft + picks |
| `schedule_ingestion.py` | `nflverse-schedule` (manual) | schedules backfill |

`daily_ingestion.py` is **config-driven**: `DATASETS_CONFIG` maps each dataset to its `nflreadpy`
loader, target folder, and schedule. It runs **best-effort** (one dataset failing doesn't abort the
rest; it prints a summary and exits non-zero if any failed).

## Datasets & partitioning
- **Seasonal** (partition `season=<YYYY>`): `play_by_play`, `player_stats`, `team_stats`, `schedules`,
  `rosters`, `rosters_weekly`, `depth_charts`, `snap_counts`, `nextgen_stats`, `ftn_charting`,
  `officials`.
- **Non-seasonal / weekly** (partition `load_date=`, gated to **Tuesday** — settles after MNF
  corrections): `fantasy_rankings` (FantasyPros ECR — carries FP/cbs/yahoo ids), `fantasy_opportunity`,
  `fantasy_player_ids` (**the ID bridge**), `nfl_players`.

## Why it matters downstream (crosswalks)
- **`fantasy_player_ids`** + **`nfl_players`** feed silver `dim_players_master` — the
  Sleeper↔nflverse↔KTC id bridge.
- `rosters_weekly.sleeper_id` bridges nflverse↔Sleeper; `fantasy_rankings.id` is the FantasyPros id
  that FantasyPros projections join onto.

## Gotchas
- **Canonical root is `bronze/nflverse/`** — both daily and the backfills write here, and silver reads
  here. (An earlier backfill wrote `bronze/nfl/…`; that's fixed — don't reintroduce the split root.)
- **`depth_charts` has a `season=None` partition** — the loader didn't carry a season column. Watch for
  it when reading depth charts.
