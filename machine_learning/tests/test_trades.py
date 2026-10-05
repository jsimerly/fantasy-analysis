"""trades: id parsing, the per-side trade table and the manager scorecard (synthetic legs)."""
from __future__ import annotations

from datetime import date

import polars as pl

import trades


def test_ids_parses_sleeper_roster_lists():
    assert trades._ids("[3, 9]") == [3, 9]
    assert trades._ids("[5,7,10]") == [5, 7, 10]
    assert trades._ids(None) == [] and trades._ids("") == []


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
        "ktc_then": [6000.0, 9000.0, 2000.0, 7000.0, 5000.0], "ktc_now": [1000.0, 4000.0, 1500.0, 3000.0, 6000.0],
        "wins_since": [8.0, 4.0, 0.5, 2.0, 0.0], "pts_since": [800.0, 500.0, 60.0, 300.0, 0.0],
        "seasons_since": [4.5, 4.5, 4.5, 3.0, 3.0]})
    fr = pl.DataFrame({"lineage_id": ["L", "L", "L"], "roster_id": [1, 2, 3], "manager": ["Ann", "Bob", "Cy"]})
    return trades_df, legs, fr


def test_trade_table_scores_both_sides_and_labels_resolved_picks():
    trades_df, legs, fr = _fixture()
    tt = trades.trade_table(trades_df, legs, fr)
    assert tt.height == 4                                   # two sides per trade
    a = tt.filter((pl.col("transaction_id") == "t1") & (pl.col("roster_id") == 1)).row(0, named=True)
    b = tt.filter((pl.col("transaction_id") == "t1") & (pl.col("roster_id") == 2)).row(0, named=True)
    assert a["manager"] == "Ann" and a["recv_assets"] == "Henry" and "Rookie X" in b["recv_assets"]
    assert a["recv_ktc_then"] == 6000 and a["give_ktc_then"] == 11000 and a["net_ktc_then"] == -5000
    assert a["recv_wins"] == 8.0 and a["give_wins"] == 4.5 and abs(a["net_wins"] - 3.5) < 1e-9
    assert abs(b["net_wins"] + 3.5) < 1e-9 and b["recv_picks"] == 1 and b["give_picks"] == 0
    assert a["in_season"] is False and tt.filter(pl.col("transaction_id") == "t2")["in_season"].all()


def test_manager_table_aggregates_and_min_seasons():
    trades_df, legs, fr = _fixture()
    tt = trades.trade_table(trades_df, legs, fr).join(legs.group_by("transaction_id").agg(pl.col("seasons_since").max()), on="transaction_id", how="left")
    mt = trades.manager_table(tt)
    ann = mt.filter(pl.col("manager") == "Ann").row(0, named=True)
    assert ann["trades"] == 2 and abs(ann["net_wins"] - (3.5 + (0.0 - 2.0))) < 1e-9
    assert ann["picks_in"] == 1 and ann["picks_out"] == 1 and ann["net_picks"] == 0
    assert ann["win_rate"] == 0.5 and ann["loss_rate"] == 0.5
    # only trades at least four seasons old
    old = trades.manager_table(tt, min_seasons=4.0)
    assert old.filter(pl.col("manager") == "Ann").row(0, named=True)["trades"] == 1
    assert "Cy" not in old["manager"].to_list()
