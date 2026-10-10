# data_engineering — bronze ingestion + silver modeling

One folder per data source under `src/` (plus `silver_fantasy/` for the modeled layer). Each bronze
folder fetches from a provider, flattens to parquet, and writes to `gs://nfl-data-bronze/bronze/<source>/…`.
The directory is **one uv project** (`pyproject.toml`, `uv.lock`, one `Dockerfile`): every script runs
as `python -m <folder>.<script>` with `PYTHONPATH=src`, and each Cloud Run job runs one such module off
the single shared image.

| Folder | Source / role |
| --- | --- |
| [sleeper_ingestion/](src/sleeper_ingestion/) | the league platform — leagues, rosters, transactions, drafts, + the discovery crawler |
| [ktc_ingestion/](src/ktc_ingestion/) | KeepTradeCut dynasty/redraft/devy values |
| [fantasycalc_ingestion/](src/fantasycalc_ingestion/) | FantasyCalc player + pick values |
| [nflverse_ingestion/](src/nflverse_ingestion/) | NFL stats, schedules, rosters, play-by-play |
| [fantasypros_ingestion/](src/fantasypros_ingestion/) | FantasyPros pre-game projections (Wayback + live union): `backfill.py` (local, 2012 on) + `live_current.py` (the in-season daily capture) |
| [adp_ingestion/](src/adp_ingestion/) | average draft position by season (Fantasy Football Calculator 2009+, MyFantasyLeague 2011+): the preseason consensus with no survivorship, the model's `preseason` group (yearly job `adp-yearly`) |
| [fftoday_ingestion/](src/fftoday_ingestion/) | FFToday's weekly projections 2010 on, served on its own pages: the weekly consensus history before Sleeper's 2018 floor (local backfill, BACKLOG 38) |
| [cfbd_ingestion/](src/cfbd_ingestion/) | College Football Data API (`client.py` + `backfill.py`): college production, usage and team strength, the model's `college` feature source; one parquet per (dataset, season), refreshed by the yearly `fantasy-college-yearly` workflow |
| [silver_fantasy/](src/silver_fantasy/) | dims & facts modeled from bronze |
| [data_quality/](src/data_quality/) | checks on the lake itself (freshness, keys, history, invariants, drift), the last step of the daily DAG; **every data bug found gets a check** — see its CLAUDE.md |

## Conventions shared across the ingestion scripts
- **Imports are qualified package imports.** `src/` is on the path (`PYTHONPATH=src` at runtime,
  pytest's `pythonpath = ["src"]` in `pyproject.toml` — see [tests/conftest.py](tests/conftest.py)), so
  scripts and tests alike write `from sleeper_ingestion._utils import …` — no `sys.path` edits, no loader shim.
- **Script shape:** a pure `flatten_*()` that turns the API payload into DataFrame(s) (this is what the
  spec tests exercise), a `save_df_to_gcs()` writer, and a `main()` that loops the active leagues.
- **Which leagues are "active":** `_utils.get_fantasy_leagues()` reads silver `dim_leagues_meta`;
  daily jobs filter `status != "complete"` AND `source_system == "sleeper"`.
- **GCS writes** are plain `df.write_parquet("gs://…")` — fine for the small curated datasets. The
  **crawlers are the exception**: they write a local parquet then `blob.upload_from_filename(timeout=)`,
  because polars' direct `gs://` write has no timeout and hangs on large uploads.
- **polars resilience idioms** for messy API JSON: `infer_schema_length=None` (or a large N) so a
  column that's null in early rows then valued later types correctly, `strict=False` on casts to
  coerce odd scalars to null instead of raising, and `how="vertical_relaxed"`/`"diagonal_relaxed"`
  concat across slightly-divergent schemas.
- **Provider payloads omit keys** (especially Sleeper in the offseason) — flatten with `.get()` /
  `… or {}` and pin nullable numeric columns to a concrete dtype so per-league frames still concat.
  See [sleeper_ingestion/daily/CLAUDE.md](src/sleeper_ingestion/daily/CLAUDE.md).

## Storage shape
`bronze/<source>/<entity>/<load_style>/load_date=<d>/data.parquet`, where `load_style` ∈
`{full_load, incremental, daily, weekly}`. nflverse partitions by `season` instead of `load_date`.
Reading a dataset = glob all partitions and dedup on the natural key (several bronze tables carry
duplicate dumps — noted in each source's `CLAUDE.md`).
