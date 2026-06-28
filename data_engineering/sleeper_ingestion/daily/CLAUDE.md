# daily — scheduled Sleeper incrementals

Daily Cloud Run jobs that snapshot the **curated active leagues** (`status != "complete"` and
`source_system == "sleeper"`, from `get_fantasy_leagues()`). Each script: `flatten_*()` → per-league
frames, then union and write. Run in the daily DAG ([orchestration/](../../../orchestration/)).

| Script | Cloud Run job | Writes (under `bronze/sleeper/…`) |
| --- | --- | --- |
| `incremental_league.py` | `sleeper-incremental-league` | `league/{leagues,settings,scoring,roster_slots}/incremental` |
| `incremental_rosters.py` | `sleeper-incremental-rosters` | `rosters/{roster_players,traded_picks}` (daily), `rosters/{team_state,nicknames}` (weekly) |
| `incremental_users.py` | `sleeper-incremental-users` | `rosters/users` (weekly) |
| `incremental_players.py` | `sleeper-incremental-players` | `league/players/incremental` (global Sleeper player list, not league-specific) |
| `incremental_transactions.py` | `sleeper-incremental-transactions` | `transactions/{transactions,transaction_players,draft_picks}/daily` |

Weekly entities overwrite within the week; the week starts **Tuesday** (`_get_week_start_from_str`).

## Gotchas (the "why" behind the defensive code)
- **Offseason / fresh-season settings drift.** Sleeper omits season-derived settings keys until the
  first game is scored — a new season is `in_season` at `leg=1` with **no `last_scored_leg`**. So the
  league flattener reads `leg`/`last_scored_leg` with `.get()` and **pins both to nullable `Int64`**:
  otherwise a `None` makes a polars `Null`-dtype column and the strict vertical `pl.concat` across
  leagues breaks the moment one league has scored (Int64) and another hasn't. (Fixed in PR #18; the
  spec tests in `tests/sleeper/test_incremental_league.py::TestOffseasonSettingsDrift` lock it in.)
- **Season rollover has no forward pointer.** A Sleeper league only links *backward*
  (`previous_league_id`), so re-fetching known leagues never finds the new season and a lineage
  freezes. `incremental_league.discover_new_season_leagues()` walks *forward* — reads each lineage's
  latest league's members, queries their leagues for the next season(s), and adopts any whose
  `previous_league_id` chains into our known set.
- **Transaction week-selection.** Sleeper records transactions per "leg" (week). `in_season` fetches
  only the recent legs (older legs were captured when current); **`complete`/offseason sweeps all legs
  1..22** so late-season and offseason moves aren't dropped.
- **Nullable transaction fields.** `adds` / `drops` / `draft_picks` come back `null` when empty
  (guarded with `if txn.get(...)`); `creator` / `status_updated` are `.get()` (null on system moves).
  `transaction_id` stays required — it's the dedup key.

Tests: [tests/sleeper/](../../../tests/sleeper/).
