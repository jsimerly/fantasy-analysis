# fantasypros_ingestion — pre-game projections

Scrapes [FantasyPros](https://www.fantasypros.com/nfl/projections/) **pre-game consensus projections**
(what players were *projected* to score before each game/season — a gated, hard-to-find dataset) into
`bronze/fantasypros/projections/…`. FantasyPros publishes a multi-source consensus (CBS/ESPN/FFToday/…)
and is publicly reachable on historical pages.

## Files
| File | Role |
| --- | --- |
| `projections_scraper.py` | the scraper; `python projections_scraper.py audit` runs a read-only parser-audit matrix |
| `requirements.txt` / `Dockerfile` | deps + image (convention; this is a **local** backfill, not a daily job) |

## How it runs
**Locally**, rate-limited (~4 req/min), **resumable** (a `(year, week)` whose GCS output exists is
skipped). Env: `GCS_BUCKET_NAME`, `FP_NO_GCS` (1 = dry run), `FP_RATE_PER_MIN` (4),
`FP_YEAR_START` (2012), `FP_YEAR_END`, `FP_WEEKS` (18), `FP_SEASON_ONLY`/`FP_WEEKLY_ONLY`.

Output: `projections/weekly/season={Y}/week={WW}/data.parquet` and `projections/season/season={Y}/data.parquet`
— all 6 positions (qb/rb/wr/te/k/dst) unioned per file. Columns: `fp_id` (the FantasyPros player id from
the `fp-id-NNNN` link class — **joins to nflverse `fantasy_rankings.id`** + the id crosswalks), player,
team, raw projected stats, `fpts` (PPR), season, week (null = season-long), position, scoring, scraped_at.

## Parser (the main risk — format drift)
`parse_projection_page` is **header-driven**: it maps columns by **GROUP+STAT label, not by index**
(QB/RB/WR/TE pages have a 2-row header where ATT/YDS/TDS repeat under PASSING/RUSHING/RECEIVING; K/DST
have a 1-row header). It **fails loud** (`parse_fail`) on a header/column mismatch rather than mis-mapping
values, and treats colspan placeholders ("Not enough sources…") as empty, not failure. Verify broadly
with the `audit` subcommand before a full run.

## Big gotcha — survivorship bias (only lives here; not in any memory note)
**FantasyPros' historical projection pages only render players still in their current database**
(active / recently-active). Retired players are pruned retroactively, so **older seasons are sparse and
survivorship-biased** — and it's *not* a parser bug (the raw HTML genuinely has the rows missing). The
fingerprint tracks career length: RBs vanish fastest, QBs survive longest, and recent years are fuller.
- e.g. **2014 wk5 RB returns ~1 player** (Kyle Juszczyk — a still-active FB); **DeMarco Murray, the 2014
  NFL rushing leader, is absent** (retired 2017). RB-by-year ≈ 2014:1 → 2018:13 → 2022:49 → 2025:116.

**Implication:** treat pre-~2020 FantasyPros projections as partial/biased; **2022+** is mostly intact.
For a fuller/less-biased historical slate, **Sleeper** projections (2018+) return a much larger per-week
set and are the likely better backbone — a deliberate, separate follow-up dataset (kept single-source
here for consistency). Truly unbiased deep history needs point-in-time archives (FFA, paywalled).

## Politeness
Slow + human-like: gaussian-jittered gaps + periodic long breaks, rotating realistic browser headers
(UA + client hints), hard backoff on 429/403/5xx. This mirrors the
[fantasycalc](../fantasycalc_ingestion/CLAUDE.md) scraper template.
