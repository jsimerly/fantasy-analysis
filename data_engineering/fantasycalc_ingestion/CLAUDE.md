# fantasycalc_ingestion — FantasyCalc values

One script, `daily_ingestion.py` (`fantasycalc-daily`, in the daily DAG), that pulls
[FantasyCalc](https://www.fantasycalc.com) player + pick values from `api.fantasycalc.com/values/current`
into `bronze/fantasycalc/values/daily/load_date=*/data.parquet`.

## What it fetches
All league-setting combinations (dynasty): `numQbs` ∈ {1,2} × `numTeams` ∈ {8,10,12,14} × `ppr` ∈
{0,.5,1} — each row tagged with `n_qb`/`n_teams`/`ppr`. `flatten_player_data` flattens the nested
`player` object (rich ids: `sleeper_id`, `espn_id`, `mfl_id`, `fleaflicker_id` + bio) plus value metrics
(`value`, `redraft_value`, `combined_value`, `overall_rank`, `tier`, `adp`, `trade_frequency`, moving
std-dev). Feeds silver staging ([../silver_fantasy/_staging/](../silver_fantasy/_staging/)).

## This is the scraper-politeness template
The fetch loop here is the **pattern other scrapers copy** (the FantasyPros projections scraper, etc.):
- realistic browser **headers** (UA + Sec-CH-UA client hints),
- **gaussian-jittered** delay between requests (`np.random.normal(5,2)+1`) + a longer
  `random.uniform(10,20)` break every 5 requests (non-linear pacing),
- **429** → wait 90s + retry; **403** → pause 5 min + continue; other errors logged + skipped.

## Gotchas
- **No Early/Mid/Late pick tiers.** FantasyCalc names picks at round level / numbered slot
  (e.g. `"2026 Pick 1.09"`), unlike KTC's tiers — silver stores FC picks at round level (`tier="NA"`).
  See [../silver_fantasy/CLAUDE.md](../silver_fantasy/CLAUDE.md).
- Player↔master mapping is by `sleeper_id`/name in silver (Master has no `fantasycalc_id`).
