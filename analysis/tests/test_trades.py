"""trades: id parsing, KTC's combine, horizon pricing, the per-side trade table and the manager scorecard."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import trades  # noqa: E402


def test_ids_parses_sleeper_roster_lists():
    assert trades._ids("[3, 9]") == [3, 9]
    assert trades._ids("[5,7,10]") == [5, 7, 10]
    assert trades._ids(None) == [] and trades._ids("") == []


def test_faab_parses_both_lake_shapes():
    assert trades._faab('["4,10,16"]') == [(4, 10, 16)]
    assert trades._faab('[{"amount": 25, "receiver": 10, "sender": 7}]') == [(7, 10, 25)]
    assert trades._faab(None) == [] and trades._faab("[]") == [] and trades._faab("null") == [] and trades._faab('["1,2,0"]') == []


def test_ktc_combine_is_convex_and_penalises_quantity():
    # one 8000 asset beats two 4000 assets under the calculator although the raw sums are equal
    one, two = trades.ktc_combine([8000], 8000), trades.ktc_combine([4000, 4000], 8000)
    assert one > two > 0
    assert trades.ktc_combine([]) == 0.0
    # the page's formula at a known point: pv(v, vmax) with v = vmax = 10099 -> (0.1 + 0.7 * (1/1.05)^1.25 + 0.2) * 10099
    expect = (0.1 + 0.7 * (1 / 1.05) ** 1.25 + 0.2) * 10099
    assert abs(float(trades.pv(np.array([10099.0]), 10099.0)[0]) - expect) < 1e-6


def test_value_at_prices_players_picks_and_future_as_null():
    hv = pl.DataFrame({"pid": ["1", "1", "9"], "valuation_date": [date(2022, 1, 1), date(2023, 1, 1), date(2023, 6, 1)], "ktc": [5000.0, 6000.0, 3000.0]})
    picks = pl.DataFrame({"valuation_date": [date(2022, 1, 1)], "pick_season": [2023], "pick_round": [1], "tier": ["Mid"], "ktc": [4000.0]})
    legs = pl.DataFrame({
        "asset": ["player", "pick", "pick", "player"], "player_id": ["1", None, None, "DEF"],
        "pick_season": [None, 2023, 2023, None], "pick_round": [None, 1, 1, None],
        "drafted_player_id": [None, "9", "9", None], "draft_date": [None, date(2023, 5, 15), date(2023, 5, 15), None],
        "date": [date(2022, 2, 1)] * 4})
    today = date(2024, 1, 1)
    at = pl.Series("at", [date(2022, 2, 1), date(2022, 2, 1), date(2023, 7, 1), date(2022, 2, 1)])
    v = trades.value_at(legs, at, hv, picks, today).to_list()
    assert v[0] == 5000.0                 # the player at the trade date
    assert v[1] == 4000.0                 # the pick as a pick before its draft
    assert v[2] == 3000.0                 # the pick as the drafted player after the draft
    assert v[3] == 0.0                    # a team defense: arrived, no price
    fut = trades.value_at(legs, pl.Series("at", [date(2025, 1, 1)] * 4), hv, picks, today).to_list()
    assert all(x is None for x in fut)    # a horizon that has not arrived


def _fixture():
    trades_df = pl.DataFrame({
        "transaction_id": ["t1", "t2"], "lineage_id": ["L", "L"], "league_id": ["l1", "l1"], "league_name": ["Lg", "Lg"],
        "season": [2022, 2023], "date": [date(2022, 4, 1), date(2023, 10, 20)], "leg": [1, 7], "rosters": [[1, 2], [1, 3]], "creator": ["u", "u"],
        "in_season": [False, True], "n_teams": [2, 2]})
    legs = pl.DataFrame({
        "transaction_id": ["t1", "t1", "t1", "t2", "t2"],
        "roster_id": [1, 2, 2, 3, 1], "from_roster": [2, 1, 1, 1, 3],
        "asset": ["player", "player", "pick", "player", "pick"],
        "name": ["Henry", "Taylor", None, "Adams", None],
        "pick_season": [None, None, 2024, None, 2026], "pick_round": [None, None, 2, None, 1],
        "drafted_name": [None, None, "Rookie X", None, None],
        "v0": [6000.0, 9000.0, 2000.0, 7000.0, 5000.0], "v1": [5000.0, 7000.0, 2500.0, 6000.0, 5500.0], "v_now": [1000.0, 4000.0, 1500.0, None, None],
        "wins_since": [8.0, 4.0, 0.5, 2.0, 0.0], "pts_since": [800.0, 500.0, 60.0, 300.0, 0.0],
        "seasons_since": [4.5, 4.5, 4.5, 3.0, 3.0]})
    fr = pl.DataFrame({"lineage_id": ["L", "L", "L"], "roster_id": [1, 2, 3], "manager": ["Ann", "Bob", "Cy"]})
    return trades_df, legs, fr


def test_trade_table_combines_sides_and_nets_against_the_other_side():
    trades_df, legs, fr = _fixture()
    tt = trades.trade_table(trades_df, legs, fr)
    assert tt.height == 4
    a = tt.filter((pl.col("transaction_id") == "t1") & (pl.col("roster_id") == 1)).row(0, named=True)
    b = tt.filter((pl.col("transaction_id") == "t1") & (pl.col("roster_id") == 2)).row(0, named=True)
    assert a["manager"] == "Ann" and a["recv_assets"] == "Henry" and "Rookie X" in b["recv_assets"]
    assert a["recv_v0_sum"] == 6000 and a["give_v0_sum"] == 11000 and a["net_v0_sum"] == -5000
    # combined: Taylor is the trade's best asset, so Ann's 6000 and Bob's 9000 + 2000 are both measured against 9000
    exp_recv = trades.ktc_combine([6000], 9000)
    exp_give = trades.ktc_combine([9000, 2000], 9000)
    assert abs(a["recv_v0"] - exp_recv) < 1e-6 and abs(a["give_v0"] - exp_give) < 1e-6
    assert abs(a["net_v0"] + b["net_v0"]) < 1e-6 and a["net_v0"] < 0 and abs(a["fair_v0"] - a["net_v0"] / (a["recv_v0"] + a["give_v0"])) < 1e-9
    assert a["recv_wins"] == 8.0 and a["give_wins"] == 4.5 and abs(a["net_wins"] - 3.5) < 1e-9
    assert b["recv_picks"] == 1 and b["give_picks"] == 0
    # a horizon that has not arrived for one leg leaves the side null at that horizon
    c = tt.filter((pl.col("transaction_id") == "t2") & (pl.col("roster_id") == 1)).row(0, named=True)
    assert c["recv_v_now"] is None and c["give_v_now"] is None and c["net_v_now"] is None
    assert c["recv_v1"] == trades.ktc_combine([5500], 6000) and abs(c["give_v1"] - trades.ktc_combine([6000], 6000)) < 1e-6


def test_manager_table_aggregates_by_horizon_and_min_seasons():
    trades_df, legs, fr = _fixture()
    tt = trades.trade_table(trades_df, legs, fr).join(legs.group_by("transaction_id").agg(pl.col("seasons_since").max()), on="transaction_id", how="left")
    mt = trades.manager_table(tt)
    ann = mt.filter(pl.col("manager") == "Ann").row(0, named=True)
    assert ann["trades"] == 2 and abs(ann["net_wins"] - (3.5 + (0.0 - 2.0))) < 1e-9
    assert ann["picks_in"] == 1 and ann["picks_out"] == 1 and ann["net_picks"] == 0
    assert ann["n_v0"] == 2 and ann["n_v_now"] == 1 and 0 <= ann["won_v0"] <= 1
    old = trades.manager_table(tt, min_seasons=4.0)
    assert old.filter(pl.col("manager") == "Ann").row(0, named=True)["trades"] == 1
    assert "Cy" not in old["manager"].to_list()
