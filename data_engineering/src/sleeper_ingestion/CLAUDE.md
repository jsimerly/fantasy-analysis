# sleeper_ingestion — Sleeper league data

Ingests data from [Sleeper](https://sleeper.com), the fantasy platform the owner's leagues run on.
Two distinct jobs live here:

1. **The curated leagues** — the handful of leagues we actually model (filtered from silver
   `dim_leagues_meta`). Written to `bronze/sleeper/…` and consumed by silver.
2. **The discovery crawl** — `league_crawler.py` walks the public Sleeper graph out from our franchise
   owners to ~10k real leagues, written to **`bronze/sleeper_crawl/…`** (deliberately a separate root
   so it's never swept into the curated ingestion or silver). Feeds the team-finish ML work.

## Layout
| Path | Role |
| --- | --- |
| [api/](api/) | thin wrappers over Sleeper's REST + GraphQL endpoints |
| [daily/](daily/) | scheduled incremental jobs (league, rosters, users, players, transactions) |
| [historical/](historical/) | one-time backfills (lineage, transactions, drafts, picks, commissioner overrides) |
| `league_crawler.py` | breadth-first 10k-league discovery crawl → `bronze/sleeper_crawl/` |
| `_utils.py` | shared helpers: league lists, latest-blob lookup, GraphQL login |

> The deep-history crawler (`league_history_crawler.py`, walks each lineage back through
> `previous_league_id` for the full per-season harvest) lands with its feature branch (PR #16).

## `_utils.py` helpers (used across the scripts)
- `get_fantasy_leagues()` → silver `dim_leagues_meta` (the curated set + `source_system`).
- `get_bronze_leagues()` → the bronze `league/leagues/full_load` dump (used by the GraphQL backfills,
  which predate the silver dim).
- `get_latest_blob_path(bucket, prefix)` → newest blob under a prefix (latest daily snapshot).
- `sleeper_login(email, password)` → `Bearer <token>` via GraphQL. Needed for the historical
  transactions backfill; **login is blocked by a captcha** (the token is supplied via `AUTH_TOKEN`
  env instead — see [historical/CLAUDE.md](historical/CLAUDE.md)).

## Source-level gotchas
- **Curated vs crawl never mix:** crawl output goes to `bronze/sleeper_crawl`, the curated jobs to
  `bronze/sleeper`. Keep that separation.
- **Sleeper drops settings keys in the offseason / fresh season** (e.g. `last_scored_leg`). Flatten
  defensively — details in [daily/CLAUDE.md](daily/CLAUDE.md).
- **Lineage:** a Sleeper league only points *backward* (`previous_league_id`); there is no forward
  pointer, so season rollover needs explicit discovery (`daily/incremental_league.py`) or a backward
  walk (`historical/league_lineage_ingestion.py`).
- **Rate:** Sleeper asks for < 1000 req/min. The REST wrappers in `api/` don't rate-limit; the
  crawlers do (steady limiter + 429/5xx backoff).

Tests: [tests/sleeper/](../../../tests/sleeper/).
