# analysis — notebooks + shared helpers

Exploratory analysis on top of silver/bronze: team value over time, pick/player trends, ad-hoc
experiments. The reusable loaders and the team-value **measure** live in `fantasy_lib.py`; the
notebooks import from it.

## Files
| File | What |
| --- | --- |
| `fantasy_lib.py` | shared GCS/cache loaders + the team-value measure (the heart of the folder) |
| `02_team_value_over_time.ipynb` | per-league team value/power timelines with league-event overlays |
| `01_player_pick_trends.ipynb` | player/pick value trends |
| `ad_hoc_exp.ipynb` | scratch experiments |
| `_cache/*.parquet` | locally pre-built data (see caching note) |
| `requirements-analysis.txt` | extra deps for the notebooks |

## `fantasy_lib.py` API
- **Loaders (IO):** `load_ledger` (SCD2 ownership), `load_calendar`/`load_dims`/`load_league_events`/
  `load_nfl_season_starts` (graph overlays), `load_player_values` + `load_player_values_blend`
  (KTC SF/TEP-with-Standard-fallback lens), `load_pick_values_round` / `fc_pick_values_slot`
  (pick values), `load_rookie_draft_picks`.
- **Measure:** `team_value_timeseries` (held-assets × daily value), **`team_power_index`** (below),
  `league_diagnostics`, `team_bags` (current roster dump). Default lens is **SF / Standard / DYNASTY**
  (these leagues are superflex).

## Key concepts (folded from memory)
- **`team_power_index` reproduces KTC's `/power-rankings/teams` algo** (`prProcessV`, recovered verbatim
  from KTC's JS) to ±1 — a depth-adjusted, non-linear combine, **not** a plain value sum. Picks are
  folded in as extra assets when you pass `pick_values=` (KTC's "Include Picks" default); omit it for
  the players-only view. The simpler **league-overview "value"** number is just the player-value sum —
  `team_value_timeseries` is that lens. (This recovered algo is why we didn't need to scrape KTC's
  10k-league overview to fit a model.)
- **Pick values are round-level** for both sources (KTC tier=Mid; FantasyCalc round-generic) to keep the
  measure transparent; the standings-based tier/slot refinement is a documented v2 knob
  (`silver_fantasy/_pick_projection`), not baked in. `load_pick_values_round(..., carry_forward_seasons=)`
  maturity-shifts the furthest-priced class forward to value picks beyond KTC's pricing floor.
- **Value lenses:** KTC = deep history; FantasyCalc = better market signal but only ~2025-10→.
  **TEP** values only exist ~2025-10+ (Standard fallback before that).

## Gotchas
- **`_cache/` is a local prebuild** of the SCD2 ledger + KTC pick values, used because those silver
  tables weren't deployed when the notebooks were written. Rebuild it when the silver jobs redeploy, and
  re-point the loaders at production silver — otherwise the notebooks read stale local data.
- 2026 roster reconstruction depends on the REST-event backfill (offseason rollover), not GraphQL.

See [../data_engineering/silver_fantasy/CLAUDE.md](../data_engineering/silver_fantasy/CLAUDE.md) for the
facts these loaders read, and [../data_engineering/ktc_ingestion/CLAUDE.md](../data_engineering/ktc_ingestion/CLAUDE.md)
for the value source.
