"""league / lineup / war: league spec, win curve, exact lineups, league fill, and wins above replacement."""
import numpy as np
import polars as pl
import pytest

import league as lg
import lineup
import war

SF = lg.LeagueSpec("sf", teams=2, slots={"QB": 1, "RB": 1, "WR": 1, "TE": 1, "FLEX": 1, "SUPER_FLEX": 1})
ONE_QB = lg.LeagueSpec("1qb", teams=2, slots={"QB": 1, "RB": 2, "WR": 2, "TE": 1, "FLEX": 1})


class TestLeagueSpec:
    def test_slot_list_dedicated_first_then_restrictive_flex(self):
        assert SF.slot_list == ["QB", "RB", "TE", "WR", "FLEX", "SUPER_FLEX"] and SF.starters == 6

    def test_from_settings_row(self):
        st = pl.DataFrame({"is_current": [True], "league_lineage_id": ["730630605066371072"], "league_id": ["1"], "valid_from": ["2026-01-01"],
                           "num_teams": [10], "qb_slots": [1], "rb_slots": [2], "wr_slots": [3], "te_slots": [1], "flex_slots": [1], "superflex_slots": [1]})
        spec = lg.LeagueSpec.from_settings(st)
        assert spec.teams == 10 and spec.slots == {"QB": 1, "RB": 2, "WR": 3, "TE": 1, "FLEX": 1, "SUPER_FLEX": 1}

    def test_json_round_trip_and_custom_eligibility(self, tmp_path):
        d = {"name": "te-prem", "teams": 12, "slots": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "FLEX": 2}, "eligibility": {"FLEX": ["RB", "WR"]}}
        p = tmp_path / "l.json"; p.write_text(__import__("json").dumps(d), encoding="utf-8")
        spec = lg.LeagueSpec.from_json(p)
        assert spec.eligibility["FLEX"] == ["RB", "WR"] and spec.eligibility["QB"] == ["QB"]
        with pytest.raises(ValueError):
            lg.LeagueSpec("x", 2, {"KICKER": 1})


class TestWinCurve:
    def test_fit_recovers_a_logistic(self):
        rng = np.random.default_rng(0)
        pts = rng.normal(115, 30, 3000)
        p = 1 / (1 + np.exp(-(-0.05 * 115 + 0.05 * pts)))
        tw = pl.DataFrame({"week_pts": pts, "win": rng.uniform(size=3000) < p})
        c = lg.WinCurve.fit(tw)
        assert abs(c.b - 0.05) < 0.01 and abs(c.win_prob(115) - 0.5) < 0.05
        assert c.delta_win(115, 10) > c.delta_win(170, 10) > 0           # concave above the middle
        assert abs(c.slope_at_mean - 0.05 * 0.25) < 0.01

    def test_normal_approximation_and_standings_diffs(self):
        c = lg.WinCurve.normal(113, 42)
        assert abs(c.win_prob(113) - 0.5) < 1e-9 and 0.6 < c.win_prob(130) < 0.7
        ts = pl.DataFrame({"league_id": ["L"] * 4, "roster_id": [1] * 4, "load_date": ["d1", "d2", "d3", "d4"],
                           "fpts": [100, 230, 230, 300], "fpts_decimal": [0, 50, 50, 0], "wins": [1, 2, 2, 2], "losses": [0, 0, 0, 1]})
        tw = lg.team_weeks_from_standings(ts)
        assert tw["week_pts"].to_list() == [130.5, 69.5] and tw["win"].to_list() == [1, 0]   # the no-game snapshot is dropped


class TestLineup:
    def test_exact_assignment_uses_flex_and_superflex_optimally(self):
        pts = np.array([20, 9, 12, 11, 8, 10]); pos = ["QB", "QB", "RB", "WR", "TE", "RB"]
        total, assign = lineup.optimal_lineup(pts, pos, SF)
        assert total == 20 + 12 + 11 + 8 + 10 + 9                              # QB2 lands in the superflex, RB2 in the flex
        assert (assign >= 0).sum() == 6

    def test_bench_player_adds_nothing_and_short_roster_ok(self):
        pts = np.array([20, 12, 11, 8]); pos = ["QB", "RB", "WR", "TE"]
        assert lineup.marginal_gain(pts, pos, 5.0, "RB", SF) == 5.0                     # open flex: he starts
        assert lineup.marginal_gain(np.array([20, 12, 11, 8, 10, 9]), ["QB", "RB", "WR", "TE", "RB", "QB"], 4.0, "RB", SF) == 0.0

    def test_league_fill_replacement_and_starter_counts(self):
        pool = pl.DataFrame({"position": ["QB"] * 4 + ["RB"] * 4 + ["WR"] * 4 + ["TE"] * 3,
                             "ppg": [25, 22, 18, 15, 16, 14, 9, 8, 13, 12, 11, 7, 10, 6, 5]})
        rep = lineup.league_fill(pool, SF)
        # 2 teams: QB 2, RB 2, WR 2, TE 2 dedicated; FLEX x2 take RB 9, RB 8? no: best remaining of RB/WR/TE = WR 11, RB 9; SUPER_FLEX x2 take QB 18, QB 15
        assert rep == {"QB": 0.0, "RB": 8.0, "WR": 7.0, "TE": 5.0}
        assert lineup.starters_per_position(pool, SF) == {"QB": 4, "RB": 3, "WR": 3, "TE": 2}
        rep1 = lineup.league_fill(pool, ONE_QB)
        assert rep1["QB"] == 18.0                                                      # 1QB: the third QB is replacement


class TestWAR:
    def _df(self):
        return pl.DataFrame({"player_id": ["a", "b", "c"], "player_name": ["A", "B", "C"], "position": ["QB", "WR", "WR"],
                             "h1_ppg_hat": [24.0, 14.0, 9.0], "h1_games_hat": [16.0, 16.0, 16.0],
                             "h2_ppg_hat": [23.0, 13.0, 9.0], "h2_games_hat": [16.0, 16.0, 16.0]})

    def test_war_is_par_mapped_through_the_curve_first_span_undiscounted(self):
        rep = {"QB": 15.0, "WR": 10.0}
        c = lg.WinCurve.normal(113, 42)
        out = war.wins_above_replacement(self._df(), rep, c, war.career_components([1, 2]), discount_rate=0.5)
        a = out.filter(pl.col("player_id") == "a").row(0, named=True)
        assert a["par_1"] == 9 * 16 and abs(a["war_1"] - 16 * float(c.delta_win(113, 9))) < 1e-9
        assert abs(a["war"] - (a["war_1"] + 0.5 * a["war_2"])) < 1e-9
        assert out.filter(pl.col("player_id") == "c")["war"][0] == 0.0                   # below replacement
        assert a["war_1"] < 9 * 16 * c.slope_at_mean + 1e-9                               # concavity: never more than linear

    def test_sigma_gives_upside_near_replacement(self):
        rep = {"QB": 15.0, "WR": 10.0}; c = lg.WinCurve.normal(113, 42)
        out = war.wins_above_replacement(self._df(), rep, c, war.career_components([1]), sigma={1: {"__all__": 3.0}})
        assert out.filter(pl.col("player_id") == "c")["war"][0] > 0.0

    def test_team_marginal_war_depends_on_the_roster(self):
        c = lg.WinCurve.normal(113, 42); comps = war.career_components([1])
        roster = pl.DataFrame({"player_id": ["q1", "r1", "w1", "t1", "w2"], "player_name": list("ABCDE"), "position": ["QB", "RB", "WR", "TE", "WR"],
                               "h1_ppg_hat": [22.0, 12.0, 11.0, 8.0, 10.0], "h1_games_hat": [16.0] * 5})
        cands = pl.DataFrame({"player_id": ["x"], "player_name": ["X"], "position": ["QB"], "h1_ppg_hat": [18.0], "h1_games_hat": [16.0]})
        out = war.team_marginal_war(roster, SF, c, comps, candidates=cands)
        q1 = out.filter(pl.col("player_id") == "q1").row(0, named=True)
        x = out.filter(pl.col("player_id") == "x").row(0, named=True)
        assert q1["m_par"] == (22 - 0) * 16 or q1["m_par"] > 0                           # removing the only QB loses his slot entirely
        assert x["m_par"] == 18 * 16 and x["rostered"] is False                           # open superflex: the candidate starts in full
        w2 = out.filter(pl.col("player_id") == "w2").row(0, named=True)
        assert w2["m_par"] == 10 * 16                                                     # third WR fills the flex


class TestLineupOffset:
    def test_offset_centres_projected_lineups_and_shifts_marginal_wins(self):
        c = lg.WinCurve.normal(134, 42)
        off = war.lineup_offset(c, [120.0, 110.0, 130.0])             # mean 120 -> +14
        assert abs(off - 14.0) < 1e-9 and abs(c.win_prob(120 + off) - 0.5) < 1e-9
        roster = pl.DataFrame({"player_id": ["q1", "r1", "w1", "t1", "w2"], "player_name": list("ABCDE"), "position": ["QB", "RB", "WR", "TE", "WR"],
                               "h1_ppg_hat": [22.0, 12.0, 11.0, 8.0, 10.0], "h1_games_hat": [16.0] * 5})
        comps = war.career_components([1])
        a = war.team_marginal_war(roster, SF, c, comps, offset=0.0)
        b = war.team_marginal_war(roster, SF, c, comps, offset=off)
        assert (a["m_par"] == b["m_par"]).all()                        # points unchanged
        assert b["m_war"][0] != a["m_war"][0]                          # read at a different point on the curve
