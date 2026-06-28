# api — Sleeper endpoint wrappers

Thin, uniform functions over Sleeper's **public REST** API (`api.sleeper.app/v1`). Each is a
one-liner: `get(build_route(BASE, VERSION, …))`. No business logic, no rate limiting, no caching —
callers (the daily jobs and crawlers) own pacing and retries.

| File | Contains |
| --- | --- |
| `league.py` | leagues, rosters, users, matchups, brackets, transactions, traded picks, sport state |
| `draft.py` | drafts, picks, traded draft picks (by league or draft id) |
| `user.py`, `player.py`, `avatar.py` | user lookup, the global player list, avatar bytes |
| `_constants.py` | base URL, `VERSION`, route segments | 
| `_utils.py` | `build_route`, `add_filters`, `get` (raises on non-2xx), `get_content` |
| `_types.py` | shared type aliases (e.g. `Sport`) |

## Conventions
- Functions take **keyword-only** args (`def get_rosters(*, league_id)`), so calls read self-documenting.
- `get()` does `raise_for_status()` then `.json()` — there's no built-in retry here; the resilient
  retry/backoff lives in the crawlers and the daily jobs.
- REST endpoints are **public/unauthenticated**. The one authenticated path is **GraphQL**
  (`sleeper.com/graphql`) for historical transactions, handled outside this folder
  (`_utils.sleeper_login` + `historical/league_transactions_ingestion.py`), which needs a Bearer token.
