# ktc_ingestion — KeepTradeCut values

Scrapes [KeepTradeCut](https://keeptradecut.com) player + pick values into
`bronze/ktc/<market>/…`, for **three markets**: `dynasty/` (the main one), `redraft/`, `devy/`.
Each market folder has parallel `full_*` / `incremental_*` scripts (same shape per market — documented
here, not in three near-identical child docs). Shared scrape/parse helpers live in `utils.py`.

## Scrape modes & files
| File (per market) | Cloud Run job | What it does → bronze store |
| --- | --- | --- |
| `incremental_<market>.py` | `ktc-incremental-<market>` (daily) | current snapshot: the page's `playersArray` flattened (all value formats) → `<market>/daily_load/load_date=*` |
| `full_<market>.py` | `ktc-full-<market>` (manual backfill) | per-player pages → the value **time series** (`ranking_date`,`sf_value`,`one_qb_value`,ranks) → `<market>/full_load/` (one parquet per player) |
| `dynasty/local_archive_to_cloud.py` | — (one-time) | imports an old local archive → `dynasty/local_load` |
| `utils.py` | — | `fetch_soup` (rate-limited), `playersArray`/per-player parsers, `flatten_player_data`, `transform_player_data`, `set_dtypes` |

## Value formats (the columns)
Every player carries **1QB** and **Superflex** values, each at four TE-premium levels:
**std / tep / tepp / teppp** (e.g. `sf_value`, `sf_tep_value`, `oneqb_value`, …) plus ranks/tiers and
market signals (adp, trade counts, liquidity). The silver staging
([../silver_fantasy/_staging/](../silver_fantasy/_staging/)) melts these into the long value schema.

## Data quirks / gotchas
- **`full_load` is the good historical source** — continuous per-player series 2020→2025-10-01.
  `local_load` is an older archive whose **player** values end ~2024-08-02 (a 14-month gap), kept only
  as a coverage fallback for players `full_load` lacks. `daily_load` carries 2025-10-01→present.
- **Pick tier→slot re-key (latent gotcha).** KTC prices a draft class by **tier**
  (`"<season> <Early|Mid|Late> <1st..4th>"`) until the draft order locks, then **re-keys to exact slots**
  (`"<season> Pick <round>.<slot>"`) and the tier names stop. Anything parsing KTC pick names must bridge
  slot↔tier or it loses the class. (Not biting yet — we have no numbered KTC pick rows — but watch it
  when adding live numbered-pick ingestion.)
- **Politeness:** `fetch_soup` sleeps `random.uniform(2,6)` between requests; the full per-player scrape
  is hundreds of pages, so it's a manual backfill, not in the daily DAG.

## Not here (cross-refs)
- The **power-ranking algo** (`prProcessV`) and the **trade-calculator combine** are *analysis* concerns,
  implemented in [analysis/fantasy_lib.py](../../analysis/) — see [analysis/CLAUDE.md](../../analysis/CLAUDE.md),
  not this ingestion folder.
- KTC↔Sleeper id mapping is in silver `dim_players_master` (`ktc_id`).
