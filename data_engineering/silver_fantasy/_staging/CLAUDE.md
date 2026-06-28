# _staging — value-source alignment

Intermediate (T1) staging that **melts and standardizes every player-value source into one long
schema**, so `fact_asset_values` (T2) can build from a single uniform frame instead of four
provider-specific ones.

`asset_alignment.py` (`silver-staging-asset-alignment`) unions:
- **KTC daily** (`bronze/ktc/dynasty/daily_load`) — recent ~252 days, 4 formats (1QB/SF × Std/TEP).
- **KTC devy** → a separate `_staging/devy_values.parquet`.
- **KTC historic** (`bronze/ktc/dynasty/full_load`) — the per-player scrape, **continuous 2020 →
  2025-10-01**, with `local_load` as a coverage fallback. Reading `full_load` here (not `local_load`)
  is what closes the **2024-08 → 2025-10 player-value gap**.
- **FantasyCalc** (`bronze/fantasycalc/values/daily`).

Output: `silver/fantasy/_staging/asset_values_long` (+ `devy_values.parquet`).

## Why the type-strictness
Each source is `.cast()` to a strict schema (`valuation_date`→Date, ids→Utf8, values→Int64) **before**
the unpivot/union — sources disagree on dtypes (FantasyCalc dates are strings; KTC values come in as
mixed ints), and an un-cast union produces schema drift that breaks downstream. That's the reason for
the per-source cast blocks, not decoration.

## Gotchas
- **Output is a tidy "long" frame**: one row per `(valuation_date, source_id, asset_name, qb_format,
  te_premium, market_type, source_system, asset_type)` → `value`. `fact_asset_values` pivots it.
- The KTC historic/local naming differs ("Season Tier Round" vs reversed "Tier Season Round") and
  `local_load` carries dirty future dates — handled in the parse, but watch it if you add a source.
- This is a `main()`-guarded job (added so it's deployable). See [../CLAUDE.md](../CLAUDE.md) for the
  pick-value precedence (`daily > full_load > local_load`) and the ownership-independent fact design.
