"""market_trends: the fantasy-season year, forward and backward market-adjusted changes, the market
index, seasonality and momentum tables on a synthetic price history, the pick cycle and the age read."""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import market_trends as mt  # noqa: E402


def _prices():
    # two players over 400 days: A drifts up 0.1 % a day, B is flat; the market index is their median
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(400)]
    rows = []
    for i, d in enumerate(days):
        rows.append(dict(player_key="A", valuation_date=d, ktc_value=int(3000 * np.exp(0.001 * i)), position="WR", age=24.0, exp=1, stage="yr2-3", ppg=15.0, games=17))
        rows.append(dict(player_key="B", valuation_date=d, ktc_value=3000, position="RB", age=28.0, exp=5, stage="yr4-6", ppg=10.0, games=17))
        rows.append(dict(player_key="C", valuation_date=d, ktc_value=500, position="TE", age=30.0, exp=8, stage="yr7+", ppg=5.0, games=17))   # below MIN_VALUE
    return pl.DataFrame(rows).with_columns(pl.col("ktc_value").cast(pl.Float64).log().alias("logv"), mt.season_year(pl.col("valuation_date")).alias("season"))


def test_season_year_runs_march_to_february():
    df = pl.DataFrame({"d": [date(2024, 2, 15), date(2024, 3, 1), date(2024, 12, 31)]})
    assert df.select(mt.season_year(pl.col("d")).alias("s"))["s"].to_list() == [2023, 2024, 2024]


def test_forward_and_backward_changes_are_market_adjusted():
    p = _prices()
    idx = mt.market_index(p)
    assert idx.height == 400 and (idx["n_priced"] == 2).all()                                   # C is under the floor
    f = mt.forward_change(p, 30).filter(pl.col("valuation_date") == date(2024, 1, 1))
    a, b = (f.filter(pl.col("player_key") == k).row(0, named=True) for k in ("A", "B"))
    assert abs(a["fwd"] - 0.03) < 0.005 and abs(b["fwd"]) < 1e-9
    # the index moved by half of A's move (the median of a riser and a flat player), so A's relative move is half and B's is minus half
    assert abs(a["fwd_rel"] - 0.015) < 0.005 and abs(b["fwd_rel"] + 0.015) < 0.005
    bw = mt.backward_change(p, 28).filter(pl.col("valuation_date") == date(2024, 3, 1))
    assert abs(bw.filter(pl.col("player_key") == "A")["bwd"][0] - 0.014) < 0.004


def test_seasonality_and_momentum_tables_have_the_expected_shape():
    p = _prices()
    s = mt.seasonality(p, days=30)
    assert set(r["stage"] for r in s["by_stage"]) == {"yr2-3", "yr4-6"} and len(s["whole_market"]) == 12
    assert all(r["pct"] > 0 for r in s["by_stage"] if r["stage"] == "yr2-3")                   # the riser's relative drift is positive every month
    m = mt.momentum(p)
    assert m["n"] > 0 and len(m["deciles"]) == 10 and "corr_overall" in m


def test_pick_cycle_is_relative_to_the_month_before_the_draft():
    rows = []
    for season in (2025, 2026):
        for k in range(0, 24):
            d = date(season, 5, 1) - timedelta(days=30 * k + 5)
            rows.append(dict(source_system="ktc", tier="Mid", qb_format="SF", te_premium="Standard", season=season, round=1, valuation_date=d, value=4000.0 - 50.0 * k))
    out = mt.pick_cycle(pl.DataFrame(rows))
    t = {r["months_to_draft"]: r["pct_vs_month_before_draft"] for r in out["rows"]}
    assert t[1] == 0.0 and t[12] < t[6] < 0 and out["seasons"] == [2025, 2026]


def test_age_discount_reads_the_backtest_frame():
    p = pl.DataFrame({"cohort": [2020] * 6, "position": ["WR"] * 6, "age_at_season": [22.0, 23.0, 25.0, 27.0, 28.0, 31.0],
                      "ktc_value": [6000.0, 5000.0, 4000.0, 3000.0, 2000.0, 1000.0], "realized_war": [0.5, 0.4, 1.0, 1.5, 1.2, 0.3]})
    a = mt.age_discount(p)
    by = {r["age_band"]: r for r in a["by_age"]}
    assert by["<24"]["finished_worse_by"] > 0 and by["27-29"]["finished_worse_by"] < 0 and a["n"] == 6


def test_price_per_point_uses_the_first_september_price_and_the_value_share():
    # eight WRs and eight RBs priced in September 2024: a WR's price is 300 x his PPG, an RB's 200 x; the RB pool is cheaper
    rows = []
    for k in range(8):
        for d in (date(2024, 9, 1), date(2024, 9, 20)):
            rows.append(dict(player_key=f"W{k}", valuation_date=d, ktc_value=300.0 * (10 + k) * (1.1 if d.day == 20 else 1), position="WR", ppg=10.0 + k, games=17))
            rows.append(dict(player_key=f"R{k}", valuation_date=d, ktc_value=200.0 * (10 + k), position="RB", ppg=10.0 + k, games=17))
    p = pl.DataFrame(rows).with_columns(mt.season_year(pl.col("valuation_date")).alias("season"), pl.col("ktc_value").log().alias("logv"))
    out = mt.price_per_point(p)
    by = {r["position"]: r for r in out["rows"]}
    assert by["WR"]["ktc_per_ppg"] == 300 and by["RB"]["ktc_per_ppg"] == 200                      # the 1 September price, not the 20th
    assert by["WR"]["spearman_price_ppg"] == 1.0 and abs(by["WR"]["value_share"] - 60.0) < 0.1 and by["RB"]["n_priced"] == 8
    assert out["seasons"] == [2024] and out["latest_season"] == 2024
    assert by["WR"]["n_top"] == 8 and by["WR"]["ktc_per_ppg_top"] == 300 and out["top_n"]["WR"] == 36                 # all eight are in the top 36


def test_ktc_crosswalk_falls_back_to_name_and_position_and_prefers_the_recent_namesake():
    hist = pl.DataFrame({"player_key": ["1", "2", "3", "4"], "ktc_name": ["Ja'Marr Chase", "Marvin Harrison Jr.", "Kyle Williams", "Nobody Here"], "ktc_position": ["WR", "WR", "WR", "RB"]})
    xw = pl.DataFrame({"player_key": ["1"], "gsis_id": ["00-0036900"]})
    fps = pl.DataFrame({"player_id": ["00-0036900", "00-0010000", "00-0039900", "00-0020000"], "player_name": ["Ja'Marr Chase", "Marvin Harrison", "Marvin Harrison Jr.", "Kyle Williams"],
                        "position": ["WR", "WR", "WR", "RB"], "season": [2024, 2008, 2024, 2010]})
    m = {r["player_key"]: (r["gsis_id"], r["match"]) for r in mt.ktc_crosswalk(hist, xw, fps).to_dicts()}
    assert m["1"] == ("00-0036900", "id") and m["2"] == ("00-0039900", "name")      # the son, who played last
    assert m["3"] == (None, None) and m["4"] == (None, None)                          # a WR cannot take an RB's name; no such player

