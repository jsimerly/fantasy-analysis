"""silver_fantasy/fact_player_week.py

Re-scoring math, league-scoring resolution, the schedules -> game_date join, the bio
derivations, and the player-week build.
"""
from datetime import date, datetime

import polars as pl

import silver_fantasy.fact_player_week as fpw

NFL_STAT_COLS = [
    "passing_yards", "passing_tds", "passing_interceptions", "passing_2pt_conversions",
    "attempts", "completions", "rushing_yards", "rushing_tds", "rushing_2pt_conversions",
    "carries", "rushing_first_downs", "receptions", "receiving_yards", "receiving_tds",
    "receiving_2pt_conversions", "receiving_first_downs", "targets", "target_share", "wopr",
    "fantasy_points_ppr", "sack_fumbles_lost", "rushing_fumbles_lost", "receiving_fumbles_lost",
    "sack_fumbles", "rushing_fumbles", "receiving_fumbles",
]


def _weeks(rows):
    base = {c: 0 for c in NFL_STAT_COLS}
    out = []
    for r in rows:
        d = dict(base)
        d.update(season_type="REG", position="RB", season=2023, week=1,
                 player_id="p1", player_display_name="P1", team="KC", opponent_team="DET")
        d.update(r)
        out.append(d)
    return pl.DataFrame(out)


class TestScoring:
    def test_known_qb_line(self):
        df = pl.DataFrame({
            "position": ["QB"],
            "passing_yards": [300], "passing_tds": [3], "passing_interceptions": [1],
            "rushing_yards": [20], "rushing_tds": [1],
            "sack_fumbles_lost": [1], "rushing_fumbles_lost": [0], "receiving_fumbles_lost": [0],
        })
        sc = {"pass_yd": 0.04, "pass_td": 4.0, "pass_int": -1.0,
              "rush_yd": 0.1, "rush_td": 6.0, "fum_lost": -2.0}
        # 12 + 12 - 1 + 2 + 6 - 2 = 29
        assert abs(fpw.score_player_weeks(df, sc)["fpts"][0] - 29.0) < 1e-9

    def test_te_premium_is_position_conditional(self):
        df = pl.DataFrame({"position": ["TE", "WR"], "receptions": [5, 5], "receiving_yards": [50, 50]})
        out = fpw.score_player_weeks(df, {"rec": 0.5, "rec_yd": 0.1, "bonus_rec_te": 0.5})["fpts"].to_list()
        assert abs(out[0] - 10.0) < 1e-9   # TE: 2.5 + 5 + 2.5
        assert abs(out[1] - 7.5) < 1e-9    # WR: 2.5 + 5 + 0

    def test_unmodeled_offense_key_flagged(self):
        _, unmodeled = fpw.score_expr({"pass_yd": 0.04, "pass_td_50p": 1.0})
        assert "pass_td_50p" in unmodeled and "pass_yd" not in unmodeled


class TestResolveScoring:
    def _settings(self):
        return pl.DataFrame({
            "league_id": ["a", "b", "c"],
            "league_lineage_id": ["L1", "L1", "L2"],
            "is_current": [True, True, True],
            "valid_from": [datetime(2024, 1, 1), datetime(2025, 1, 1), datetime(2025, 1, 1)],
            "pass_yd": [0.04, 0.04, 0.04], "pass_int": [-1.0, -1.0, -2.0],
            "rec": [0.5, 0.5, 1.0], "bonus_rec_te": [0.5, 0.5, 0.0],
            "sack": [1.0, 1.0, 1.0],  # defensive key — must be excluded
        })

    def test_picks_primary_lineage_latest_and_offense_only(self):
        cfg = fpw.resolve_league_scoring(self._settings())
        assert cfg["league_lineage_id"] == "L1" and cfg["league_id"] == "b"
        assert "sack" not in cfg["scoring"] and cfg["unmodeled_keys"] == []


class TestTeamDates:
    def test_game_date_for_home_and_away_reg_only(self):
        sched = pl.DataFrame({
            "season": [2023, 2023], "game_type": ["REG", "POST"], "week": [1, 1],
            "gameday": ["2023-09-10", "2024-01-13"], "home_team": ["KC", "BUF"], "away_team": ["DET", "PIT"],
        })
        td = {r["team"]: r for r in fpw.build_team_dates(sched).to_dicts()}
        assert td["KC"]["game_date"] == date(2023, 9, 10) and td["KC"]["opponent"] == "DET"
        assert td["DET"]["game_date"] == date(2023, 9, 10)
        assert "BUF" not in td  # POST filtered out

    def test_era_team_codes_canonicalized(self):
        # schedules use era codes (OAK/SD); player_stats use current (LV/LAC) -> must align
        sched = pl.DataFrame({
            "season": [2015], "game_type": ["REG"], "week": [1],
            "gameday": ["2015-09-13"], "home_team": ["OAK"], "away_team": ["SD"],
        })
        td = {r["team"] for r in fpw.build_team_dates(sched).to_dicts()}
        assert td == {"LV", "LAC"} and "OAK" not in td and "SD" not in td


class TestBuild:
    def test_build_has_game_date_fpts_and_bio(self):
        weeks = _weeks([{"week": 1, "rushing_yards": 100, "rushing_tds": 1, "receptions": 3},
                        {"week": 2, "rushing_yards": 50}])
        sched = pl.DataFrame({
            "season": [2023, 2023], "game_type": ["REG", "REG"], "week": [1, 2],
            "gameday": ["2023-09-10", "2023-09-17"], "home_team": ["KC", "KC"], "away_team": ["DET", "JAX"],
        })
        bio = pl.DataFrame({"gsis_id": ["p1"], "birth_date": ["1999-09-01"], "draft_round": [1],
                            "draft_pick": [5], "rookie_season": [2023], "college_name": ["X"]})
        out = fpw.build_fact_player_week(weeks, sched, bio, {"rush_yd": 0.1, "rush_td": 6.0, "rec": 0.5})
        assert out.height == 2
        r1 = out.filter(pl.col("week") == 1).to_dicts()[0]
        assert abs(r1["fpts"] - (10 + 6 + 1.5)) < 1e-9     # 100*.1 + 1*6 + 3*.5
        assert r1["game_date"] == date(2023, 9, 10)
        assert abs(r1["age_at_season"] - 24.0) < 0.05 and r1["is_rookie"] is True
