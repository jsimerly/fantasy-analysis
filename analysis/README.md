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
  of each package. A pick is a pick (the Mid tier of its round) until its draft and the rookie
  taken with it afterwards (the lineage's linear draft, the original roster's slot in the draft
  order, the pick at that slot). A horizon that has not arrived is null; an asset with no price
  on a date that has (a team defense, a pick before KTC priced picks) is 0;
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
trade-age filter keeps scorecards comparable; one scoring setting for the wins.
