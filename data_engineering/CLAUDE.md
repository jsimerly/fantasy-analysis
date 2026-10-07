# data_engineering — bronze ingestion + silver modeling

One folder per data source (plus `silver_fantasy/` for the modeled layer). Each bronze folder
fetches from a provider, flattens to parquet, and writes to `gs://nfl-data-bronze/bronze/<source>/…`.
Each folder is packaged as its own Cloud Run job (own `requirements.txt` / `Dockerfile`, often its
own nested `.venv`).

| Folder | Source / role |
| --- | --- |
| [sleeper_ingestion/](sleeper_ingestion/) | the league platform — leagues, rosters, transactions, drafts, + the discovery crawler |
| [ktc_ingestion/](ktc_ingestion/) | KeepTradeCut dynasty/redraft/devy values |
| [fantasycalc_ingestion/](fantasycalc_ingestion/) | FantasyCalc player + pick values |
| [nflverse_ingestion/](nflverse_ingestion/) | NFL stats, schedules, rosters, play-by-play |
| `fantasypros_ingestion/` | pre-game projections (lands with its feature branch) |
| [silver_fantasy/](silver_fantasy/) | dims & facts modeled from bronze |

## Conventions shared across the ingestion scripts
- **Scripts are NOT importable packages.** They run as flat scripts and resolve siblings via a
  runtime `sys.path.insert(0, <parent>)` followed by bare imports (`from _utils import …`,
  `from api.league import …`). This is why tests load them through
  [tests/de_loader.py](../tests/de_loader.py) instead of importing normally — keep that pattern when
  adding scripts.
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
  See [sleeper_ingestion/daily/CLAUDE.md](sleeper_ingestion/daily/CLAUDE.md).

## Storage shape
`bronze/<source>/<entity>/<load_style>/load_date=<d>/data.parquet`, where `load_style` ∈
`{full_load, incremental, daily, weekly}`. nflverse partitions by `season` instead of `load_date`.
Reading a dataset = glob all partitions and dedup on the natural key (several bronze tables carry
duplicate dumps — noted in each source's `CLAUDE.md`).
