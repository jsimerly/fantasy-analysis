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
| `backfill_daily_from_history.py` | `ktc-backfill-daily` (manual) | rebuilds **missing** `<market>/daily_load` partitions (all three markets) from the per-player history pages — see gotchas |
| `utils.py` | — | `fetch_soup` (rate-limited), `playersArray`/per-player parsers, `flatten_player_data`, `transform_player_data`, `set_dtypes` |

## Value formats (the columns)
Every player carries **1QB** and **Superflex** values, each at four TE-premium levels:
**std / tep / tepp / teppp** (e.g. `sf_value`, `sf_tep_value`, `oneqb_value`, …) plus ranks/tiers and
market signals (adp, trade counts, liquidity). The silver staging
([../silver_fantasy/_staging/](../silver_fantasy/_staging/)) melts these into the long value schema.

## Data quirks / gotchas
- **Gaps in `daily_load` are self-healable.** When the incrementals are down (2026-09-08 → 2026-09-30),
  run `ktc-backfill-daily` (or `python backfill_daily_from_history.py` locally): it lists the missing
  days per market up to yesterday, walks every player/pick on the rankings page, and writes each
  missing partition with the **exact snapshot schema** (from the newest real partition) so the hive
  scans stay uniform. Only 1QB/SF `value`, `rank`, `positionalRank` (+ identity) are recoverable —
  TE-premium values, trends, liquidity, adp, kept/traded/cut and `isTrending` are null in rebuilt
  days, and players not on the rankings page at run time are absent. Each rebuilt partition carries a
  `_RECONSTRUCTED.json` sidecar; existing partitions are never overwritten (idempotent re-runs).
- **Page layout changed 2026-09-08.** The rankings pages no longer inline `var playersArray = [...]`;
  the data ships as a JSON element (`<script type="application/json" id="ktc-players">`) that the
  page's JS `JSON.parse`s, and per-player pages likewise moved the value history into `id="pd-oneqb"`
  / `id="pd-superflex"`. `utils.parse_rankings_players` / `parse_historic_*` read those elements first
  and fall back to the legacy inline regex. The three `ktc-incremental-*` jobs failed every day from
  2026-09-08 until this landed, so `daily_load` has a gap from 2026-09-08: backfill the **dynasty**
  market by running `ktc-full-dynasty` (the per-player history covers the gap; staging unions
  `full_load`). The redraft/devy daily gaps are not recoverable from staging's inputs (it only
  reads `dynasty/full_load` as history).
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
