# data_quality — checks on the lake (tests for the data)

The spec suite under `tests/` proves the ETL *code* on synthetic payloads. This package proves the
*lake*: every morning, after the silver tiers, `python -m data_quality.run` reads every silver table
and the bronze partition listings and asserts what we know must hold. **Every bug we find in the
data gets a check here** so it cannot come back silently: the fix proves the code, the check proves
the lake stays fixed.

| File | What |
| --- | --- |
| `expectations.py` | pure expectations on frames (`unique_key`, `not_null`, `fresh`, `partition_fresh`, `no_date_gaps`, `monthly_presence`, `scd2`, `share_at_least`, `within_pct`, `not_below`, `unchanged`, …) returning a `Result`; no IO, spec-tested |
| `core.py` | `Check` (name, table, severity, `guards` = the bug it exists for, `known_open`), `Context` (cached lake access; tests seed frames + partition listings instead), `run_checks` → one results frame, `TABLES` (every silver table by short name, incl. the bare-blob facts) |
| `suite.py` | the catalogue: `bronze.*` (feeds landed and look like themselves), `dim.*` / `fact.*` (keys, freshness, history, invariants), `drift.*` (what must not change or shrink between runs) |
| `run.py` | the Cloud Run job `silver-data-quality` (last step of `orchestration/pipeline.yaml`); `--no-write` dry run, `--only <substring>` |

## Rules
- **Severity.** `error` fails the job (exit 1, the DAG records it, the run goes red); `warn` reports.
- **`known_open`** marks a defect we know about (a BACKLOG item): the check keeps its real severity
  but its failure is reported as a warning with the note, so the DAG does not go red daily for a
  known gap. When the fix lands, remove the note; the check then enforces. Never delete a check to
  make the run green.
- **Drift / regime checks** compare against the previous run (`silver/_quality/latest.parquet`): the
  current leagues' lineup settings must not change (the WAR-scale regime trap, BACKLOG 22), the
  league dim must not flap by more than a row, the big facts must never shrink.
- **Adding a check:** a `@check(name, table, severity, guards)` function in `suite.py` returning a
  `Result` (compose the expectations); a replay of the bug on the seeded lake in
  `tests/data_quality/test_suite.py` (`healthy()` must keep passing every enforced check).
- **Calibration lives in the check, with the reason** (e.g. KTC priced ~100 players in 2020, so the
  history check wants 80+ per month, not 150).
- **The leagues we track** come from `Context.current_leagues()` (dim_franchises_meta joined to
  dim_leagues_meta), never from `is_active` / `status` (the offseason scoping trap). The ledger's
  `franchise_id` prefix is the **lineage root**, not the current league id (`_lineage_of`).

## What the first run found (2026-10-09, all recorded as `known_open` → BACKLOG 34)
- `dim_league_settings`: the three current (2026) leagues' rows have a null `league_lineage_id`.
- `dim_players_master`: 7 gsis_ids shared by two players (Sleeper's inactive "Duplicate Player"
  placeholders, plus one real conflict); gsis_id present for a fifth of the KTC-priced pool.
- `fact_roster_membership`: 88 overlapping holding intervals (two franchises holding one asset at once).
