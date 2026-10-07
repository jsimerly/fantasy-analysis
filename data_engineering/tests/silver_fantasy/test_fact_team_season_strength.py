"""silver_fantasy/fact_team_season_strength.py: both sides of every game, the market rating from the
spreads, the record from the played games only, the QB situation, the EPA from team_stats, the
franchise-code normalization and the one-season lags."""
import polars as pl

import silver_fantasy.fact_team_season_strength as m


def _sched():
    # 2015: STL (Rams, favored at home by 3 then by 7 away... from STL's side: +3, -4, +7, and a future game) vs others
    rows = [
        dict(season=2015, week=1, game_type="REG", game_id="a", home_team="STL", away_team="SEA", home_score=30.0, away_score=20.0,
             spread_line=3.0, total_line=44.0, home_moneyline=-150.0, away_moneyline=130.0, home_qb_id="qbA", away_qb_id="qbS"),
        dict(season=2015, week=2, game_type="REG", game_id="b", home_team="SEA", away_team="STL", home_score=24.0, away_score=24.0,
             spread_line=4.0, total_line=42.0, home_moneyline=-200.0, away_moneyline=170.0, home_qb_id="qbS", away_qb_id="qbA"),
        dict(season=2015, week=3, game_type="REG", game_id="c", home_team="STL", away_team="SEA", home_score=10.0, away_score=27.0,
             spread_line=7.0, total_line=40.0, home_moneyline=-300.0, away_moneyline=250.0, home_qb_id="qbB", away_qb_id="qbS"),
        dict(season=2015, week=4, game_type="REG", game_id="d", home_team="STL", away_team="SEA", home_score=None, away_score=None,
             spread_line=-2.0, total_line=46.0, home_moneyline=110.0, away_moneyline=-130.0, home_qb_id=None, away_qb_id=None),
        dict(season=2015, week=19, game_type="POST", game_id="e", home_team="STL", away_team="SEA", home_score=50.0, away_score=0.0,
             spread_line=20.0, total_line=60.0, home_moneyline=None, away_moneyline=None, home_qb_id="qbZ", away_qb_id="qbS"),
        dict(season=2016, week=1, game_type="REG", game_id="f", home_team="LA", away_team="SEA", home_score=7.0, away_score=9.0,
             spread_line=-1.0, total_line=41.0, home_moneyline=None, away_moneyline=None, home_qb_id="qbA", away_qb_id="qbS"),
    ]
    return pl.DataFrame(rows)


def _team_stats():
    rows = [
        dict(season=2015, week=1, season_type="REG", team="STL", attempts=30, carries=20, sacks_suffered=0, passing_epa=5.0, rushing_epa=5.0),
        dict(season=2015, week=2, season_type="REG", team="STL", attempts=30, carries=10, sacks_suffered=10, passing_epa=-5.0, rushing_epa=0.0),
        dict(season=2015, week=2, season_type="REG", team="STL", attempts=99, carries=99, sacks_suffered=99, passing_epa=99.0, rushing_epa=99.0),  # duplicate dump
        dict(season=2015, week=19, season_type="POST", team="STL", attempts=50, carries=50, sacks_suffered=0, passing_epa=50.0, rushing_epa=50.0),
        dict(season=2015, week=1, season_type="REG", team="SEA", attempts=20, carries=30, sacks_suffered=0, passing_epa=0.0, rushing_epa=0.0),
    ]
    return pl.DataFrame(rows)


class TestMarketAndRecord:
    def test_rating_from_both_sides_and_record_from_played_games_only(self):
        out = m.build_fact_team_season_strength(_sched(), _team_stats())
        stl = out.filter((pl.col("season") == 2015) & (pl.col("team") == "LA")).row(0, named=True)   # STL normalized to LA
        assert stl["games_played"] == 3 and (stl["wins"], stl["losses"], stl["ties"]) == (1, 1, 1) and abs(stl["win_pct"] - 0.5) < 1e-9
        assert abs(stl["point_diff_pg"] - (10 + 0 - 17) / 3) < 1e-9 and abs(stl["pts_for_pg"] - 64 / 3) < 1e-9
        assert stl["games_lined"] == 4 and abs(stl["mkt_margin"] - (3 - 4 + 7 - 2) / 4) < 1e-9   # the future game's line counts, its score does not
        assert stl["mkt_margin_wk1"] == 3.0 and abs(stl["mkt_margin_last4"] - 1.0) < 1e-9 and abs(stl["mkt_total"] - 43.0) < 1e-9
        assert abs(stl["mkt_implied_pts"] - ((44 + 3) / 2 + (42 - 4) / 2 + (40 + 7) / 2 + (46 - 2) / 2) / 4) < 1e-9
        sea = out.filter((pl.col("season") == 2015) & (pl.col("team") == "SEA")).row(0, named=True)
        assert abs(sea["mkt_margin"] + stl["mkt_margin"]) < 1e-9                                  # the other side of the same lines
        assert "POST" not in out.columns and out.filter(pl.col("season") == 2015)["games_played"].max() == 3   # playoffs excluded

    def test_moneyline_probability_and_qb_situation(self):
        out = m.build_fact_team_season_strength(_sched(), _team_stats())
        stl = out.filter((pl.col("season") == 2015) & (pl.col("team") == "LA")).row(0, named=True)
        p = [150 / 250, 100 / 270, 300 / 400, 100 / 210]
        assert abs(stl["mkt_win_prob"] - sum(p) / 4) < 1e-9
        assert stl["n_starting_qbs"] == 2 and stl["qb_main_id"] == "qbA" and abs(stl["qb_main_share"] - 2 / 3) < 1e-9
        sea = out.filter((pl.col("season") == 2015) & (pl.col("team") == "SEA")).row(0, named=True)
        assert sea["n_starting_qbs"] == 1 and sea["qb_main_share"] == 1.0


class TestOffenseAndLags:
    def test_epa_per_play_pass_rate_and_duplicate_dump_ignored(self):
        out = m.build_fact_team_season_strength(_sched(), _team_stats())
        stl = out.filter((pl.col("season") == 2015) & (pl.col("team") == "LA")).row(0, named=True)
        assert abs(stl["off_epa_per_play"] - 5.0 / 100) < 1e-9          # (10 - 5) over (50 + 50) plays; the duplicate week-2 row and the playoff row are out
        assert abs(stl["pass_rate"] - 70 / 100) < 1e-9 and abs(stl["plays_pg"] - 50.0) < 1e-9
        sea = out.filter((pl.col("season") == 2015) & (pl.col("team") == "SEA")).row(0, named=True)
        assert sea["off_epa_per_play"] == 0.0 and abs(sea["pass_rate"] - 0.4) < 1e-9

    def test_lags_follow_the_normalized_franchise_across_the_move(self):
        out = m.build_fact_team_season_strength(_sched(), _team_stats())
        la16 = out.filter((pl.col("season") == 2016) & (pl.col("team") == "LA")).row(0, named=True)
        assert abs(la16["lag1_mkt_margin"] - 1.0) < 1e-9 and abs(la16["lag1_off_epa_per_play"] - 0.05) < 1e-9
        assert la16["off_epa_per_play"] is None and la16["mkt_margin"] == -1.0
        assert out.filter(pl.col("season") == 2015)["lag1_mkt_margin"].is_null().all()
        assert set(out["team"].unique().to_list()) == {"LA", "SEA"}


class TestWeekGrid:
    def test_to_date_values_after_each_week_and_the_current_weeks_line(self):
        out = m.build_fact_team_week_strength(_sched(), _team_stats())
        stl = {r["week"]: r for r in out.filter((pl.col("season") == 2015) & (pl.col("team") == "LA")).to_dicts()}
        assert sorted(stl) == [1, 2, 3, 4]                                                     # the regular-season span; the playoff week is out
        assert stl[1]["mkt_margin_td"] == 3.0 and stl[1]["point_diff_td"] == 10.0 and stl[1]["games_td"] == 1 and stl[1]["line_this_week"] == 3.0
        assert abs(stl[2]["mkt_margin_td"] - (-0.5)) < 1e-9 and abs(stl[2]["point_diff_td"] - 5.0) < 1e-9 and abs(stl[2]["win_pct_td"] - 0.75) < 1e-9
        assert abs(stl[3]["mkt_margin_td"] - 2.0) < 1e-9 and abs(stl[3]["point_diff_td"] - (-7 / 3)) < 1e-9 and stl[3]["games_td"] == 3
        # week 4 has a line but no score yet: the rating moves, the record does not
        assert abs(stl[4]["mkt_margin_td"] - 1.0) < 1e-9 and abs(stl[4]["point_diff_td"] - (-7 / 3)) < 1e-9 and stl[4]["games_td"] == 3
        assert stl[4]["line_this_week"] == -2.0 and stl[4]["played_this_week"] is False and stl[3]["played_this_week"] is True
        assert abs(stl[1]["off_epa_td"] - 0.2) < 1e-9 and abs(stl[2]["off_epa_td"] - 0.05) < 1e-9 and abs(stl[2]["pass_rate_td"] - 0.7) < 1e-9
        assert abs(stl[4]["off_epa_td"] - 0.05) < 1e-9                                            # carried forward past the last stats row
        assert stl[1]["mkt_margin_prev"] is None

    def test_bye_weeks_carry_the_to_date_values_and_have_no_line(self):
        sched = _sched().filter(pl.col("game_id") != "b")                                        # week 2 becomes a bye for both teams
        out = m.build_fact_team_week_strength(sched, _team_stats())
        stl = {r["week"]: r for r in out.filter((pl.col("season") == 2015) & (pl.col("team") == "LA")).to_dicts()}
        assert stl[2]["line_this_week"] is None and stl[2]["played_this_week"] is False
        assert stl[2]["mkt_margin_td"] == 3.0 and stl[2]["games_td"] == 1 and stl[2]["point_diff_td"] == 10.0
        assert abs(stl[3]["mkt_margin_td"] - 5.0) < 1e-9 and stl[3]["games_td"] == 2
        la16 = out.filter((pl.col("season") == 2016) & (pl.col("team") == "LA")).row(0, named=True)
        assert abs(la16["mkt_margin_prev"] - (3 + 7 - 2) / 3) < 1e-9                               # last season's full rating, normalized code


class TestNormalizeTeam:
    def test_every_old_code_maps_to_the_current_franchise(self):
        df = pl.DataFrame({"team": ["OAK", "SD", "STL", "JAC", "LAR", "KC", " LV "]}).with_columns(m.normalize_team().alias("n"))
        assert df["n"].to_list() == ["LV", "LAC", "LA", "JAX", "LA", "KC", "LV"]
