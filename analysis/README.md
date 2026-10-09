# analysis/

Exploratory analysis on the lake: notebooks (`01_player_pick_trends`, `02_team_value_over_time`,
`auction_predictor`, `ad_hoc_exp`), the shared helpers in `fantasy_lib.py`, and the trade
analysis below. Everything here reads the lake and the ML bucket; nothing here trains a model
(that is `machine_learning/`) or writes datasets (that is `data_engineering/`). Run with the
root venv: `.venv/Scripts/python.exe -m pip install -r analysis/requirements-analysis.txt`.

## The leagues' trading, scored on value

`trades.py` + `trade_report.py` (machine_learning/BACKLOG.md item 25). Every completed trade in
the three lineages (Sleeper transactions, 2021 on; full-load dump ∪ daily feed, deduplicated)
with both sides' assets, priced in KTC dynasty (SF) value:

* **at N, the trade date,** with KTC's own trade calculator (`ktc_combine`, the `processVNew`
  curve: the trade's best asset is the reference and a package of lesser pieces has to overpay
  in raw value), so a two-for-one is judged the way KTC judges it. `net_v0` is received minus
  given, combined; `fair_v0` is the calculator's lean as a share of the trade;
* **at N+1, N+2, N+3 years and today,** the same assets re-priced: what the market later thought
  of each package. A pick is always a pick, never the player later taken with it: for next year's
  draft it is priced at the tier its original team is likely to land in (`pick_slots.py`, below),
  at the Mid tier for drafts further out, at its actual slot's tier once the order is known
  (January of the draft year), and frozen at its last pre-draft price once the draft has
  happened. Pick prices come from every KTC source in the lake (the silver fact, the archive's
  pick rows, the per-asset history) with the same round and tier of another season at the same
  distance from its draft standing in where the lake has gaps, and a pick KTC had not listed yet
  taking its first listed price. A horizon that has not arrived is null; an asset with no price on
  a date that has (a team defense, a kicker, FAAB) is 0 and is named on the card;
* **wins delivered since** (secondary): weekly points vs the lineage's weekly replacement line
  that season, through its win curve, from the week after the trade to today.

Player prices come from the silver KTC fact (today's ~430 listed players, daily since 2020-04)
with the bronze KTC archive (2020-04 to 2024-08, by Sleeper id) as the fallback for players no
longer listed; a player who dropped off the list between 2024-08 and 2025-10 reads 0 in that
window. The Sleeper id → gsis crosswalk is nflverse's `fantasy_player_ids` (the player master
maps a third and pads ids with spaces).

The report prints per-manager scorecards (verdict at N, packages later, wins), the best and
worst trades by value at `--horizon`, the patterns (does the verdict at the time hold up; picks
for players; consolidation; in-season vs offseason; what positions and pick rounds keep their
value; head-to-head), and `--manager NAME` lists one manager's trades. `--publish` writes the
summary to `backtests/trades/run_date=<today>/summary.json`, which the page's export picks up
for the Trades tab (scorecards, log, head-to-head and patterns; sortable, filtered by league,
manager, horizon and trade age).

```
.venv/Scripts/python analysis/trade_report.py --rebuild --out analysis/_cache/trades --min-seasons 1 --manager "Jacob Simerly" --publish
.venv/Scripts/python -m pytest analysis/tests -q
```

Caveats: roster ids map to today's franchise owners; a side's "wins since" counts everything the
player did afterwards whether or not he was kept; older trades have had more seasons, so the
trade-age filter keeps scorecards comparable; one scoring setting for the wins. Trades undone by
a mirror trade within ten days are flagged as reversals and left out of the scorecards; the
2023-04-02 joke trade is excluded by id (`EXCLUDED_TRANSACTIONS`).

## Team value over time (`team_value.py`, `02_team_value_over_time.ipynb`)

Every franchise's roster value week by week, in KTC's own power-ranking terms: each week the held
players and picks are priced on KTC dynasty (superflex, TE premium once KTC published it; picks at
the round level), ranked within the roster and depth-weighted with KTC's `prProcessV`
(`fantasy_lib.team_power_index`), the top team that week = 99. Alongside: the same without picks,
the raw depth-weighted sum, the share of the league-average team (100 = average), the plain KTC
sum, and FantasyCalc's sum from 2025-10. The regular seasons (NFL week 1 to the league's
`fantasy_end`) and the draft days come from the calendar dims; each lineage starts at its first
week with 95 % of its steady-state asset count, because the first week of a startup is a partial
roster. `--publish` writes `backtests/team_value/run_date=<today>/summary.json`, which the page
export folds into the **Team value** tab (chart with season bands and draft guides, owner colors,
a team highlight, the week's standings on hover, a measure switch, and a standings-now table with
the moves over 4 / 13 / 52 weeks and since the last draft). The notebook draws the same frames with
matplotlib.

```
.venv/Scripts/python analysis/team_value.py --out analysis/_cache/team_value --publish
```

## How the market prices over time (`market_trends.py`)

The KTC dynasty market's own regularities, measured on its daily history since 2020 (SF values,
players priced at 1,000 or more; every relative move is market-adjusted, i.e. minus the median
priced player's move, because KTC rescales and the whole pool drifts with the calendar):
seasonality of prices by position, career stage and age (mean 30-day change by month), momentum
versus reversion (the past 28-day move against the next 56 days, by decile and for 15 %+ moves),
the market's age discount against realized three-year WAR on the market-backtest cohorts (realized
rank minus market rank, wins per 1,000 KTC), the rookie-pick price cycle by months before the draft,
the reaction to one big game (2+ sd above the season rate) over the next week and eight weeks, and
the price path around an injury absence (by injury class and age). `--publish` writes
`backtests/market_trends/run_date=<today>/summary.json` for a page tab.

```
machine_learning/.venv/Scripts/python analysis/market_trends.py --out analysis/_cache/market_trends --players <market backtest>/players.parquet --publish
```

## Expected draft slot (`pick_slots.py`, `pick_slots_report.py`)

The market prices a pick by its expected slot, so a 1st from a 2-win team is not a 1st from the
league leader. `pick_slots.build_standings` turns the 10,000-league crawl's weekly matchups
(`bronze/sleeper_crawl/history/matchups`, 26,769 league-seasons, 2017–2025) into cumulative
records by week and the final regular-season rank; `tier_table` is the empirical
P(Early / Mid / Late third of the draft order | weeks played, record fifth, points-for fifth),
`prior_table` the same given last season's finish (the preseason prior); `slot_table` /
`slot_prior` the full distribution over twelfths of the order. No fitting: the table is the
model, with the expected slot alongside. `slot_curve` is the market's value of each slot: the
median KTC value of the player taken there a month after the draft, over 586k picks in the
crawl's rookie drafts (2021–2025), relative to the round's mean (1.01 ≈ 1.46×, 1.12 ≈ 0.80×, so
the top pick is 1.8 times the last pick of the round; a 2.01 is worth more than a late 1st). KTC
itself only prices individual slots once the order is set, so a pick's value at any date is the
round's price level that day (the mean of KTC's Early / Mid / Late prices) times the expected
curve value over the team's slot distribution — a known slot takes its own curve value. `trades.SlotContext` applies it at any trade date
from our leagues' standings (the crawl for past seasons, Sleeper's daily `team_state` snapshots
for the season in progress). Calibration on our own leagues' seasons: the table names the right
third 59 / 72 / 81 % of the time after 4 / 8 / 12 weeks (44 % from the preseason prior) against
32 % for "Mid for everyone", Brier 0.53 / 0.39 / 0.30 vs 1.37. `pick_slots_report.py --publish`
writes every current team's outlook (P(tiers), expected slot, what that makes its next 1st and
2nd worth at today's tier prices), the tables and the calibration to `backtests/pick_slots/`,
which the page's Draft slots tab shows.

```
.venv/Scripts/python analysis/pick_slots.py                 # rebuild the standings and tables (cached in _cache/)
.venv/Scripts/python analysis/pick_slots_report.py --out analysis/_cache/pick_slots --publish
```

Next, if the table is not enough: an ordered regression for the preseason prior (roster value +
returning record) and smoothing of thin cells; XGBoost only if a held-out test says it beats
that at calling the actual slot.
