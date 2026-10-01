"""Draft picks in wins: curve fit, slot tiers, discounting."""
import numpy as np
import polars as pl

import picks
from league import WinCurve


def test_realized_war_counts_never_played_as_zero_and_discounts_from_the_class():
    curve = WinCurve.normal(134.2, 41.2)
    rep = pl.DataFrame({"season": [2020, 2021], "position": ["WR", "WR"], "rep": [9.0, 9.0]})
    drafted = pl.DataFrame({"player_id": ["a", "b"], "nfl_pick": [5, 200], "nfl_class": [2020, 2020], "position": ["WR", "WR"]})
    sf = pl.DataFrame({"player_id": ["a", "a"], "season": [2020, 2021], "position": ["WR", "WR"], "games": [17, 17], "ppg": [19.0, 19.0]})
    out = picks.realized_war_by_player(sf, rep, drafted, curve, years=10, rate=0.2).sort("player_id")
    a, b = out.row(0, named=True), out.row(1, named=True)
    assert b["war"] == 0.0 and b["war_disc"] == 0.0
    assert a["war"] > 0 and abs(a["war_disc"] - (a["war"] / 2) * (1 + 0.8)) < 1e-9     # two equal seasons: 1 + 0.8


def test_curve_is_monotone_decreasing_in_pick():
    rng = np.random.default_rng(0)
    pick = np.arange(1, 260)
    wins = np.clip(2.5 * pick ** -0.7 + rng.normal(0, 0.05, pick.size), 0, None)
    a, b = picks.fit_pick_curve(pl.DataFrame({"nfl_pick": pick, "war_disc": wins}))
    e = picks.expected_war([1, 10, 50, 200], a, b)
    assert b < 0 and e[0] > e[1] > e[2] > e[3] >= 0


def test_slot_tiers_scale_with_league_size():
    df = pl.DataFrame({"pick_no": [1, 3, 4, 7, 10, 11, 13, 36], "teams": [10, 10, 10, 10, 10, 12, 12, 12]})
    out = df.with_columns(picks.slot_tier(pl.col("pick_no"), pl.col("teams")).alias("tier"))["tier"].to_list()
    # 10 teams: picks 1-3 early, 4-6 mid, 7-10 late; 12 teams: pick 11 is the 11th slot (late), 13 opens round 2 (early), 36 closes round 3
    assert out == ["Early", "Early", "Mid", "Late", "Late", "Late", "Early", "Late"]


def test_pick_table_discounts_future_drafts_and_prices_them():
    by_tier = pl.DataFrame({"round": [1, 1], "tier": ["Early", "Late"], "nfl_picks": [[2, 6, 10], [20, 30, 45]], "n": [3, 3]})
    prices = pl.DataFrame({"season": [2027, 2027], "round": [1, 1], "tier": ["Early", "Late"], "ktc": [7000.0, 5000.0]})
    out = picks.pick_table(by_tier, a=1.0, b=-0.7, seasons=[2027, 2028], now_season=2026, rate=0.2, prices=prices)
    e27 = out.filter((pl.col("season") == 2027) & (pl.col("tier") == "Early")).row(0, named=True)
    e28 = out.filter((pl.col("season") == 2028) & (pl.col("tier") == "Early")).row(0, named=True)
    l27 = out.filter((pl.col("season") == 2027) & (pl.col("tier") == "Late")).row(0, named=True)
    assert e27["wins"] > l27["wins"] > 0
    assert abs(e28["wins"] - e27["wins"] * 0.8) < 1e-9 and abs(e27["wins"] - e27["wins_undiscounted"] * 0.8) < 1e-9
    assert abs(e27["wins_per_1000"] - e27["wins"] / 7.0) < 1e-9 and e28["ktc"] is None
