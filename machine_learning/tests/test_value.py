"""replacement.py + value.py + market.py: league lineup -> replacement level -> discounted VORP,
realized value for backtests, and the market comparison (pure, no network)."""
from datetime import date

import polars as pl
import pytest

import market
import replacement
import value


class TestLineup:
    def test_starters_include_flex_and_superflex_shares(self):
        slots = dict(qb_slots=1, rb_slots=2, wr_slots=3, te_slots=1, flex_slots=1, superflex_slots=1)
        n = replacement.starters_per_position(slots, teams=10)
        assert n["QB"] == 20                      # 1 QB + the superflex, x10 teams
        assert abs(n["RB"] - (2 + 0.45) * 10) < 1e-9
        assert abs(n["WR"] - (3 + 0.45) * 10) < 1e-9
        assert abs(n["TE"] - (1 + 0.10) * 10) < 1e-9

    def test_league_lineup_reads_latest_current_row(self):
        settings = pl.DataFrame({
            "league_id": ["old", "new"], "league_lineage_id": ["L", "L"], "is_current": [True, True],
            "valid_from": [date(2024, 1, 1), date(2025, 1, 1)], "num_teams": [10, 12],
            "qb_slots": [1, 1], "rb_slots": [2, 2], "wr_slots": [2, 3], "te_slots": [1, 1],
            "flex_slots": [1, 1], "superflex_slots": [1, 1],
        })
        slots, teams = replacement.league_lineup(settings, lineage_id="L")
        assert teams == 12 and slots["wr_slots"] == 3

    def test_missing_lineage_raises(self):
        with pytest.raises(ValueError):
            replacement.league_lineup(pl.DataFrame({"league_lineage_id": ["x"], "is_current": [True]}), "L")


def _seasons():
    rows = []
    for s in (2022, 2023):
        for i in range(6):        # 6 QBs per season, ppg 30,28,...,20
            rows.append(dict(player_id=f"q{i}", season=s, position="QB", games=16, ppg=30.0 - 2 * i, season_complete=True))
        rows.append(dict(player_id="q_lowgames", season=s, position="QB", games=2, ppg=40.0, season_complete=True))
    rows.append(dict(player_id="q0", season=2024, position="QB", games=3, ppg=35.0, season_complete=False))
    return pl.DataFrame(rows)


class TestReplacementLevels:
    def test_rank_past_the_starters_min_games_complete_seasons_only(self):
        # 2 starters -> replacement is the 3rd-best ppg (26); the 2-game 40ppg guy is excluded;
        # the incomplete 2024 season is ignored
        rep = replacement.replacement_levels(_seasons(), {"QB": 2.0}, min_games=8)
        assert rep == {"QB": 26.0}

    def test_fractional_starters_floor(self):
        rep = replacement.replacement_levels(_seasons(), {"QB": 2.9}, min_games=8)
        assert rep["QB"] == 26.0                     # floor(2.9)=2 starters -> 3rd best

    def test_position_without_enough_players_is_zero(self):
        assert replacement.replacement_levels(_seasons(), {"TE": 1.0})["TE"] == 0.0


class TestIntrinsicValue:
    def _proj(self):
        return pl.DataFrame({
            "player_id": ["a", "b"], "position": ["QB", "RB"],
            "h1_ppg_hat": [25.0, 10.0], "h1_games_hat": [16.0, 16.0],
            "h2_ppg_hat": [24.0, 9.0], "h2_games_hat": [8.0, 16.0],
        })

    def test_discounted_sum_of_points_above_replacement(self):
        out = value.intrinsic_value(self._proj(), {"QB": 20.0, "RB": 12.0}, [1, 2], discount=0.5)
        a = out.filter(pl.col("player_id") == "a").to_dicts()[0]
        assert a["h1_vorp_hat"] == 80.0           # (25-20)*16
        assert a["h2_vorp_hat"] == 32.0           # (24-20)*8
        assert abs(a["iv"] - (0.5 * 80 + 0.25 * 32)) < 1e-9
        assert a["iv_undiscounted"] == 112.0

    def test_below_replacement_is_worth_zero_not_negative(self):
        out = value.intrinsic_value(self._proj(), {"QB": 20.0, "RB": 12.0}, [1, 2])
        b = out.filter(pl.col("player_id") == "b").to_dicts()[0]
        assert b["iv"] == 0.0

    def test_expected_excess_prices_the_upside(self):
        import numpy as np
        # no spread -> the clipped point estimate
        assert np.allclose(value.expected_excess([25.0, 10.0], [0.0, 0.0], [20.0, 12.0]), [5.0, 0.0])
        # at replacement with spread 4 -> sigma/sqrt(2pi) per game, not zero
        assert abs(value.expected_excess([20.0], [4.0], [20.0])[0] - 4.0 / np.sqrt(2 * np.pi)) < 1e-9
        # a spread never lowers value and a wider spread raises it when below replacement
        lo, hi = value.expected_excess([10.0, 10.0], [2.0, 6.0], [12.0, 12.0])
        assert 0 < lo < hi

    def test_sigma_columns_switch_on_the_upside_valuation(self):
        proj = self._proj().with_columns(pl.lit(4.0).alias("h1_ppg_sigma"), pl.lit(4.0).alias("h2_ppg_sigma"))
        out = value.intrinsic_value(proj, {"QB": 20.0, "RB": 12.0}, [1, 2])
        b = out.filter(pl.col("player_id") == "b").to_dicts()[0]
        assert b["iv"] > 0.0                                           # below replacement, but not worthless
        a = out.filter(pl.col("player_id") == "a").to_dicts()[0]
        assert a["h1_vorp_hat"] > 80.0                                 # upside adds to an above-replacement player too

    def test_realized_value_zero_when_not_played_and_null_when_censored(self):
        df = pl.DataFrame({
            "player_id": ["a", "c"], "position": ["QB", "QB"],
            "h1_ppg": [25.0, None], "h1_games": [16, 0], "h1_observable": [True, True],
            "h2_ppg": [None, None], "h2_games": [None, 0], "h2_observable": [False, True],
        })
        out = value.realized_value(df, {"QB": 20.0}, [1, 2], discount=0.5)
        a, c = out.to_dicts()
        assert a["h1_vorp"] == 80.0 and a["realized_iv"] is None      # h2 censored
        assert c["realized_iv"] == 0.0                                 # never played


class TestMarketComparison:
    def test_spearman(self):
        assert abs(value.spearman([1, 2, 3, 4], [10, 20, 30, 40]) - 1.0) < 1e-9
        assert abs(value.spearman([1, 2, 3, 4], [40, 30, 20, 10]) + 1.0) < 1e-9

    def test_fair_value_is_monotone_and_mispricing_sums_to_about_zero(self):
        df = pl.DataFrame({"iv": [10.0, 20.0, 30.0, 40.0, 50.0], "ktc_value": [1000, 3000, 2000, 6000, 9000]})
        out, summary = value.compare_to_market(df)
        fair = out["fair_value"].to_list()
        assert fair == sorted(fair)
        assert abs(out["mispricing"].sum()) < 1e-6
        assert summary["n"] == 5 and summary["spearman"] > 0.8
        # the 20->3000 row is paid more than its neighbours justify
        assert out.filter(pl.col("iv") == 20.0)["mispricing"][0] > 0

    def test_rows_without_market_are_excluded(self):
        df = pl.DataFrame({"iv": [1.0, 2.0, 3.0, 4.0], "ktc_value": [100, None, 300, 400]})
        out, summary = value.compare_to_market(df)
        assert summary["n"] == 3 and out.height == 3


class TestKtcAsOf:
    def test_latest_value_on_or_before_date_within_tolerance(self):
        hist = pl.DataFrame({
            "player_key": ["p", "p", "p", "q"],
            "valuation_date": [date(2021, 2, 1), date(2021, 2, 10), date(2021, 3, 1), date(2020, 12, 1)],
            "ktc_value": [5000, 5100, 5300, 4000],
        })
        out = market.ktc_as_of(hist, date(2021, 2, 15), tolerance_days=14)
        assert out.to_dicts() == [{"player_key": "p", "ktc_date": date(2021, 2, 10), "ktc_value": 5100}]


class TestAttachMarket:
    HIST = pl.DataFrame({
        "player_key": ["k1", "k2", "k3"], "ktc_name": ["Josh Allen", "A.J. Brown", "Someone Else"],
        "ktc_position": ["QB", "WR", "RB"], "valuation_date": [date(2026, 9, 30)] * 3, "ktc_value": [9900, 7000, 100],
    })
    XW = pl.DataFrame({"gsis_id": ["00-1"], "player_key": ["k1"], "ktc_id": [365], "master_name": ["Josh Allen"]})

    def test_id_bridge_then_name_fallback(self):
        df = pl.DataFrame({
            "player_id": ["00-1", "00-2", "00-3"], "player_name": ["Josh Allen", "AJ Brown Jr.", "Nobody"],
            "position": ["QB", "WR", "TE"],
        })
        out = market.attach_market(df, date(2026, 10, 1), self.HIST, self.XW)
        by = {r["player_name"]: r for r in out.to_dicts()}
        assert by["Josh Allen"]["ktc_value"] == 9900 and by["Josh Allen"]["market_match"] == "id"
        assert by["AJ Brown Jr."]["ktc_value"] == 7000 and by["AJ Brown Jr."]["market_match"] == "name"
        assert by["AJ Brown Jr."]["player_key"] == "k2"
        assert by["Nobody"]["ktc_value"] is None and by["Nobody"]["market_match"] is None

    def test_name_match_requires_same_position(self):
        df = pl.DataFrame({"player_id": ["00-9"], "player_name": ["Someone Else"], "position": ["WR"]})
        out = market.attach_market(df, date(2026, 10, 1), self.HIST, self.XW)
        assert out["ktc_value"][0] is None

    def test_norm_name(self):
        s = pl.DataFrame({"n": ["A.J. Brown Jr.", "Kenneth Walker III", "D'Andre  Swift"]}).select(market.norm_name("n"))
        assert s["n"].to_list() == ["aj brown", "kenneth walker", "dandre swift"]
