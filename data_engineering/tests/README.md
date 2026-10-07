# ETL / Ingestion unit tests

Unit tests for the pipelines under `data_engineering/`. They are **spec tests**:
each one asserts the *intended* behavior. They were written first to document a
batch of real bugs (red), which have since been fixed — the suite is now fully
green and acts as a regression guard. See "Bugs this suite locked down" below.

## Running

```bash
# one-time: install test deps into your venv
pip install -r tests/requirements-dev.txt

# run everything from the repo root
pytest

# a single pipeline / file
pytest tests/sleeper
pytest tests/silver_fantasy/test_dim_league_settings_scd2.py -v
```

## How it works

The ingestion scripts are not importable packages — they rely on a runtime
`sys.path.insert(...)` and bare imports (`from utils import ...`,
`from api.draft import ...`), and several packages reuse module names
(two `utils.py`, two `daily_ingestion.py`). To import them cleanly:

- **`tests/de_loader.py`** — `load_de_module(relpath, pkg_dir)` loads one script
  with its package root temporarily on `sys.path`, evicting colliding modules
  first so the bare imports resolve to the right package.
- **`conftest.py`** (repo root) — installs lightweight stubs for `gql` and
  `nflreadpy` (imported at load time but never exercised in unit tests) and puts
  the repo root on `sys.path`.
- **`tests/conftest.py`** — the `fake_gcs` fixture patches
  `polars.read_parquet` / `DataFrame.write_parquet` with an in-memory dict, so
  any code that round-trips through `gs://` paths reads/writes locally.

No network or GCS access is required; everything is mocked.

## Bugs this suite locked down (all now fixed + green)

| Test | Bug (fixed) |
| --- | --- |
| `sleeper/test_incremental_rosters.py::TestWeekStart` (2) | `_get_week_start_from_str` weekday map: `wednesday=3` (should be 2), Thu/Fri/Sat missing |
| `sleeper/test_incremental_rosters.py::...empty_returns_typed_empty_frame` | `pl.Series([], pl.Utf8)` passed `[]` as the Series *name* — empty path raised |
| `sleeper/test_incremental_transactions.py::...draft_picks_table_dedupes_across_runs` | dedup branch keyed on `'transaction_draft_picks'` but table is `'draft_picks'` — never deduped |
| `sleeper/test_incremental_league.py::...missing_scoring_settings_does_not_crash` | built frame from stray singular `scoring_record` |
| `sleeper/test_league_transactions_hist.py::...does_not_write_debug_file` | dumped `debug_transactions.json` every loop iteration |
| `sleeper/test_league_lineage.py::...keeps_scoring_settings` | head-of-lineage league assembled without `scoring_settings` |
| `silver_fantasy/test_fact_asset_values.py::...single_source_still_yields_both_value_columns` | pivot `.rename` assumed both sources present |
| `silver_fantasy/test_fact_asset_values.py::...unmatched_rows_go_to_quarantine` | unmapped KTC rows got `name=None` (no fallback to `asset_name`) |
| `silver_fantasy/test_dim_player_master.py::...avatar_url_is_a_url_not_a_raw_id` | `avatar_url` coalesced the raw `swish_id` instead of a CDN URL |
| `nflverse/test_full_ingestion.py::...shared_nflverse_root` | backfill wrote `bronze/nfl/...`; daily + silver read `bronze/nflverse/...` |
