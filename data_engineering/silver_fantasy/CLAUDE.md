# silver_fantasy — modeled dims & facts

The silver star schema, built from bronze with **polars-on-parquet (no BigQuery)**. Each table is a
pure `build_*`/`transform_*` function + a `main()` that does IO and writes
`silver/fantasy/<table>/data.parquet`. Reuse `utils.get_latest_bronze_path`; spec-tested in
[tests/silver_fantasy/](../../../tests/silver_fantasy/) via `load_de_module` + the `fake_gcs` fixture.
Build order / Cloud Run jobs are in [orchestration/README.md](../../../orchestration/README.md) (T1→T2→T3).

## Tables
| File | Job | Role |
| --- | --- | --- |
| `dim_leagues_meta.py` | `silver-dim-league-meta` | league identity + status/leg per season |
| `dim_league_settings_scd2.py` | `silver-dim-league-settings` | SCD2 league "constitution" (scoring + rosters + settings) |
| `dim_player_master.py` | `silver-dim-player-master` | player ID bridge + enrichment (Sleeper↔nflverse↔KTC) |
| `dim_users.py` | `silver-dim-users` | Sleeper users + manual Google-Sheet enrichment |
| `dim_franchise_meta.py` | `silver-dim-franchise-meta` | the franchise entity (lineage+roster) + owner |
| `dim_dates.py` | `silver-dim-dates` | global daily spine 1999→2027 (nfl_season, nfl_phase, season/playoff flags) |
| `dim_league_events.py` | `silver-dim-league-events` | per-league events (startup/rookie drafts, fantasy_end) |
| `_staging/asset_alignment.py` | `silver-staging-asset-alignment` | melt/standardize all value sources → `_staging/asset_values_long` (see [_staging/CLAUDE.md](_staging/CLAUDE.md)) |
| `fact_asset_values.py` | `silver-fact-asset-values` | player values (KTC + FantasyCalc), partitioned by `market_type` |
| `fact_pick_values.py` | `silver-fact-pick-values` | pick **tier** value over time (ownership-independent) |
| `fact_roster_membership.py` | `silver-fact-roster-membership` | SCD2 ownership ledger (players + picks), back to 2021 |
| `_pick_projection.py` | — (analysis helper) | reverse-standings tier projection (the measure layer, not a fact) |
| `utils.py` | — | shared `get_latest_bronze_path` etc. |

## The big gotchas
- **"Current league" scoping (non-obvious, will bite again).** To scope a *current*-state fact to the
  leagues we track, filter to leagues present in **`dim_franchises_meta`** — do **NOT** use
  `status`/`is_active` from `dim_leagues_meta`. In the offseason every league reads `status=complete`
  (last season finished, next not created yet), so an is_active filter yields **zero** leagues and
  silently empties the fact.
- **Lineage relabeling.** `build_lineage_map` maps every `league_id` → the current league of its
  dynasty lineage (`argmax(season)` per `league_lineage_id`). Pick sources are relabeled onto the
  current league before resolving, so commissioner overrides hand-coded under an *old* season's
  `league_id` keep resolving across rollovers with no manual edit. `roster_id` is **stable across a
  lineage** — the invariant the pick logic rests on.
- **Pick ownership resolver** (`resolve_pick_ownership`, shared by the facts): overlay precedence
  **commissioner override > txn draft-pick event > Sleeper traded state > original owner**, on a
  **deterministically enumerated** pick universe (`build_pick_universe`: current leagues × next 3
  undrafted seasons × `1..rookie_rounds` × `1..total_rosters`) — never inferred from trades (that
  silently drops untraded rounds). `reconcile()` enforces conservation and routes violations to
  `quarantine/` (quarantine + warn; the fact still builds).
- **Draft cutoff is keyed off draft `status`/`start_time`, not league status.** A rookie draft
  completes months before its league flips to `complete`; keying off league status would resurrect
  spent current-season picks (double-counting the pick *and* the player it became). `fact_pick_values`
  / the snapshot path use `_drafts_completed_by(drafts_df, day)` (date-aware, PR #14) so a now-complete
  *future* draft can't roll a historical day's picks forward.
- **`fact_pick_values` is ownership-INDEPENDENT** by design — one row per
  `(valuation_date, season, round, tier, market_type, qb_format, te_premium)`, league-agnostic.
  Combining value × ownership is a separate measure-layer job (`_pick_projection` + the notebooks), so
  point-in-time team value joins day-T ownership × day-T value, and improving the tier projection never
  forces a fact rebuild. KTC prices picks by tier (Early/Mid/Late); FantasyCalc has no tiers → stored
  at round level (`tier="NA"`). Rows carry `source_system` (ktc|fantasycalc).
- **`fact_roster_membership` is an SCD2 ledger** (one row per holding stint:
  `franchise_id, asset_type, asset_id, valid_from, valid_to, is_current`). Two eras: **snapshot**
  (2025-10-16→present, diff the daily `roster_players`/`traded_picks` snapshots — 100% authoritative)
  and **reconstructed** (2021→2025-10-15, backward replay from the first snapshot using the deduped
  event log). Picks have no creation txn, so the universe is synthesized at each draft completion.

## Watch-outs when editing
- Dedup on read: the bronze `transactions/full_load` and `drafts/drafts` carry duplicate dumps.
- The historical `transactions/draft_picks/full_load` had a **from/to swap** (now corrected on read);
  the event-log reader is dual-schema (`owner_id` if re-ingested, else legacy `from_team_id`).
- `dim_player_master.avatar_url` must build a Sleeper CDN URL, not coalesce the raw `swish_id`.

Memory cross-refs folded in here: `silver-modeling`, `fact-pick-values`, `roster-membership-ledger`,
`fantasy-etl-known-bugs`.
