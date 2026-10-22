"""market_backtest: within-cohort ranks and calls, segment scores, and the swap pairing."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import market_backtest as mb  # noqa: E402


def _frame(n=30, cohort=2021):
    rows = []
    for i in range(n):
        # the model likes the even players more than the market; the odd ones less; realized follows the model
        war = n - i + (3 if i % 2 == 0 else -3)
        rows.append({"variant": "xgb", "horizon": 3, "cohort": cohort, "player_id": f"p{i}", "player_name": f"P{i}",
                     "position": ["QB", "RB", "WR", "TE"][i % 4], "age_at_season": 22 + i % 12, "exp_at_season": i % 6,
                     "draft_pick": None, "is_rookie": i % 6 == 0, "games": 15, "ppg": 10.0, "war": float(war), "iv": float(war),
                     "ktc_value": float(9000 - 200 * i), "realized_war": float(war) + (0.5 if i % 3 == 0 else 0.0), "realized_iv": float(war)})
    return pl.DataFrame(rows)


def test_priced_frame_ranks_and_calls():
    df = mb.priced_frame([_frame()])
    assert df.height == 30
    assert df["model_rank"].min() == 1 and df["ktc_rank"].max() == 30
    assert set(df["call"].unique().to_list()) == {"cheap", "rich", "agree"}
    assert df.filter(pl.col("call") == "cheap").height == 10 and df.filter(pl.col("call") == "rich").height == 10
    # the even players (liked more than the market) are the cheap calls
    assert all(int(n[1:]) % 2 == 0 for n in df.filter(pl.col("call") == "cheap")["player_name"].to_list())
    assert set(df["tier"].unique().to_list()) == {"1-24", "25-60"}
    assert set(df["exp_band"].unique().to_list()) == {"rookie", "yr2-3", "vet4+"}
    # unpriced / unobserved rows are dropped
    f = _frame().with_columns(pl.when(pl.col("player_name") == "P0").then(None).otherwise(pl.col("ktc_value")).alias("ktc_value"))
    assert mb.priced_frame([f]).height == 29


def test_score_reports_edge_when_the_model_is_right():
    df = mb.priced_frame([_frame(), _frame(cohort=2022)])
    s = mb.score(df, ["variant", "horizon"])
    assert s.height == 1
    r = s.row(0, named=True)
    assert r["n"] == 60 and r["rho_model"] > r["rho_ktc"] and r["edge_corr"] > 0.5
    assert r["cheap_gap_real"] > 0 > r["rich_gap_real"]
    by_pos = mb.score(df, ["variant", "horizon", "position"])
    assert by_pos.height == 4 and "top_decile_model" not in by_pos.columns or by_pos["top_decile_model"].null_count() == by_pos.height


def test_score_net_of_regression_is_zero_for_a_model_with_no_skill():
    """review 2026-10-13: cheap_gap_real is positive for ANY model that disagrees with the market (regression to the
    mean on the shared KTC rank); cheap_net and cheap_closer are not."""
    import numpy as np
    rng = np.random.default_rng(3)
    n = 300
    truth = rng.standard_normal(n)
    ktc = truth + 0.8 * rng.standard_normal(n)                      # the market: a noisy read of the truth
    noise = rng.standard_normal(n)                                  # a model with no information at all
    rows = [{"variant": "null", "horizon": 3, "cohort": 2021, "player_id": f"p{i}", "player_name": f"P{i}", "position": "WR",
             "age_at_season": 25, "exp_at_season": 3, "draft_pick": None, "is_rookie": False, "games": 15, "ppg": 10.0,
             "war": float(noise[i]), "iv": float(noise[i]), "ktc_value": float(10000 + 1000 * ktc[i]),
             "realized_war": float(truth[i]), "realized_iv": float(truth[i])} for i in range(n)]
    s = mb.score(mb.priced_frame([pl.DataFrame(rows)]), ["variant", "horizon"]).row(0, named=True)
    assert s["cheap_gap_real"] > 5 and s["rich_gap_real"] < -5                    # the artifact: "cheap finished better"
    assert abs(s["cheap_net"]) < s["cheap_gap_real"] / 2 and abs(s["rich_net"]) < -s["rich_gap_real"] / 2   # the honest score: nothing
    assert s["cheap_closer"] < -5                                                  # a noise model sits further from the outcome than KTC
    # and the fixture's skilled model scores above zero on both
    r = mb.score(mb.priced_frame([_frame()]), ["variant", "horizon"]).row(0, named=True)
    assert r["cheap_net"] > 0 > r["rich_net"] and r["cheap_closer"] > 0


def test_swap_pairs_match_on_price_and_gain_follows_realized():
    df = mb.priced_frame([_frame()])
    pairs = mb.swap_pairs(df, tol=0.5, min_gap=1)
    assert pairs.height > 0
    assert (pairs["buy_gap"] >= 1).all() and (pairs["sell_gap"] <= -1).all()
    assert ((pairs["buy_ktc"] - pairs["sell_ktc"]).abs() <= 0.5 * pairs["sell_ktc"]).all()
    assert pairs["buy"].n_unique() == pairs.height          # a cheap player is bought once
    assert (pairs["gain"] == pairs["buy_real"] - pairs["sell_real"]).all()
    assert pairs["gain"].mean() > 0
    s = mb.swap_summary(pairs, ["variant", "horizon"])
    assert s["pairs"][0] == pairs.height and 0 <= s["win_rate"][0] <= 1
    # a tight tolerance leaves fewer pairs, never more
    assert mb.swap_pairs(df, tol=0.02, min_gap=1).height <= pairs.height
    assert mb.swap_pairs(df, tol=0.5, min_gap=100).height == 0


def test_realized_points_and_par_ride_along_for_the_non_modeller():
    f = _frame().with_columns(pl.lit(100.0).alias("h1_fpts"), pl.lit(90.0).alias("h2_fpts"), pl.lit(None, pl.Float64).alias("h3_fpts"),
                              pl.lit(20.0).alias("h1_vorp"), pl.lit(10.0).alias("h2_vorp"), pl.lit(0.0).alias("h3_vorp"))
    df = mb.priced_frame([f])
    assert df["realized_pts"].unique().to_list() == [190.0] and df["realized_par"].unique().to_list() == [30.0]   # an unplayed season is 0, not unknown
    pairs = mb.swap_pairs(df, tol=0.5, min_gap=1)
    assert pairs.height and pairs["gain_pts"].unique().to_list() == [0.0] and pairs["buy_par"].unique().to_list() == [30.0]
    s = mb.swap_summary(pairs, ["variant", "horizon"]).row(0, named=True)
    assert s["gain_pts_mean"] == 0.0 and s["gain_par_mean"] == 0.0 and s["win_rate_par"] == 0.0
    plain = mb.swap_summary(mb.swap_pairs(mb.priced_frame([_frame()]), tol=0.5, min_gap=1), ["variant", "horizon"])
    assert plain.row(0, named=True)["gain_pts_mean"] is None                           # a frame without the columns: the means are unknown, not zero
