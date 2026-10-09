"""draft_rows: one pre-NFL row per drafted skill player at season = draft year - 1, with the gsis id when
he has NFL rows (targets attach) and a cfbd id otherwise (observable seasons = 0 games), age from the
rookie season or the class median, the rookie flags, the college join by cfbd id, and the played-season
rows kept apart for replacement and survival."""
import polars as pl

import career
import draft_rows as dr
import feature_groups as fg


def _xwalk():
    return pl.DataFrame({
        "cfbd_id": [101, 102, 103, 104, 105], "gsis_id": ["00-0031381", "DAV763631", None, "00-0039337", "00-0012345"],
        "name": ["Played", "Never", "Nobody", "Future", "Lineman"], "position": ["WR", "RB", "TE", "WR", "Offensive Tackle"],
        "draft_year": [2020, 2020, 2021, 2026, 2020], "round": [1, 2, 3, 1, 1], "overall": [12, 40, 70, 4, 9], "college": ["A", "B", "C", "D", "E"],
    })


def _fact():
    # Played: rookie 2020 (age 22.1), 2021, 2022; Future: rookie 2026 row (in progress); another 2020 rookie for the class median
    return pl.DataFrame({
        "player_id": ["00-0031381", "00-0031381", "00-0031381", "00-0039337", "00-0099999"],
        "season": [2020, 2021, 2022, 2026, 2020], "rookie_season": [2020, 2020, 2020, 2026, 2020],
        "position": ["WR", "WR", "WR", "WR", "RB"], "fpts": [120.0, 150.0, 90.0, 40.0, 30.0], "games": [15, 16, 10, 4, 12],
        "ppg": [8.0, 9.375, 9.0, 10.0, 2.5], "age_at_season": [22.1, 23.1, 24.1, 21.5, 23.9], "season_complete": [True, True, True, False, True],
    })


class TestRows:
    def test_one_row_per_skill_pick_at_the_season_before_the_rookie_year(self):
        rows = dr.build_draft_rows(_xwalk(), _fact())
        assert rows["player_id"].to_list() == ["00-0031381", "cfbd:102", "cfbd:103", "00-0039337"]     # the lineman is out; sorted by year, pick
        assert rows["season"].to_list() == [2019, 2019, 2020, 2025] and rows["draft_year"].to_list() == [2020, 2020, 2021, 2026]
        assert rows["is_draft_row"].all() and rows["is_rookie"].all() and (rows["exp_at_season"] == 0).all() and rows["season_complete"].all()
        assert rows["draft_pick"].to_list() == [12, 40, 70, 4] and rows["draft_round"].to_list() == [1, 2, 3, 1]
        assert rows["cfbd_id"].to_list() == [101, 102, 103, 104]

    def test_age_from_the_rookie_season_else_the_class_median_else_the_fallback(self):
        a = {r["player_id"]: r["age_at_season"] for r in dr.build_draft_rows(_xwalk(), _fact()).to_dicts()}
        assert abs(a["00-0031381"] - 21.1) < 1e-9                        # his own rookie-season age minus one
        assert abs(a["cfbd:102"] - (23.0 - 1.0)) < 1e-9                  # the 2020 class median (22.1, 23.9) minus one
        assert abs(a["cfbd:103"] - (dr.FALLBACK_ROOKIE_AGE - 1.0)) < 1e-9  # no 2021 rookies in the fact
        assert abs(a["00-0039337"] - 20.5) < 1e-9


class TestTargetsAndJoins:
    def test_targets_attach_to_played_rows_zero_for_the_never_played_and_null_when_censored(self):
        fact = _fact()
        base = career.career_features(fact.with_columns(pl.lit(False).alias("is_draft_row")))
        m = career.attach_horizon_targets(dr.with_draft_rows(base, dr.build_draft_rows(_xwalk(), fact)), [1, 2])
        d = {r["player_id"]: r for r in m.filter(pl.col("is_draft_row")).to_dicts()}
        assert d["00-0031381"]["h1_fpts"] == 120.0 and d["00-0031381"]["h2_games"] == 16 and d["00-0031381"]["h1_played"] is True
        assert d["cfbd:102"]["h1_games"] == 0 and d["cfbd:102"]["h1_played"] is False and d["cfbd:102"]["h1_observable"] is True
        assert d["00-0039337"]["h1_observable"] is False and d["00-0039337"]["h1_fpts"] is None     # 2026 is not complete
        # a draft row never serves as another row's future and never counts as a played season
        assert m.filter(pl.col("player_id") == "00-0031381").filter(~pl.col("is_draft_row")).height == 3
        assert dr.drop_draft_rows(m).height == 5 and career.last_complete_season(m) == 2025

    def test_survival_fit_ignores_draft_rows(self):
        import numpy as np
        rng = np.random.default_rng(0)
        ages = rng.uniform(22, 36, 200)
        tbl = pl.DataFrame({"position": ["WR"] * 200, "age_at_season": ages, "games": [12] * 200, "h1_observable": [True] * 200,
                            "h1_played": rng.random(200) < 0.8, "is_draft_row": [False] * 200})
        young = pl.DataFrame({"position": ["WR"] * 50, "age_at_season": [21.0] * 50, "games": [None] * 50, "h1_observable": [True] * 50,
                              "h1_played": [False] * 50, "is_draft_row": [True] * 50})
        a = career.fit_survival(tbl, "30+").cap_games(pl.DataFrame({"position": ["WR"], "age_at_season": [21.0], "h1_games_hat": [16.0]}), [1])
        b = career.fit_survival(pl.concat([tbl, young], how="diagonal_relaxed"), "30+").cap_games(pl.DataFrame({"position": ["WR"], "age_at_season": [21.0], "h1_games_hat": [16.0]}), [1])
        assert a["h1_games_hat"][0] == b["h1_games_hat"][0]

    def test_college_joins_draft_rows_by_cfbd_id(self):
        fact = pl.DataFrame({"cfbd_id": [101, 102], "season": [2019, 2019], "dominator": [0.3, 0.25], "yards_per_team_play": [2.0, 1.5], "usage_overall": [0.2, 0.15],
                             "touch_share": [0.3, 0.2], "team_sp": [10.0, 5.0], "class_year": [3, 4], "breakout_season_dom": [2018, None]})
        xw = _xwalk().with_columns(pl.lit("1998-01-01").alias("birth_date"))
        mx = pl.DataFrame({"player_id": ["00-0031381", "cfbd:102", "00-0000001"], "season": [2019, 2019, 2019], "cfbd_id": [101, 102, None], "position": ["WR", "RB", "QB"]})
        ctx = fg.Context()
        ctx._college = fg.college_per_player(fact, xw)
        ctx._college_cfbd = fg.college_per_cfbd(fact, xw)
        out = fg.build_college(mx, ctx).sort("player_id")
        d = {r["player_id"]: r for r in out.to_dicts()}
        assert d["00-0031381"]["col_dominator_last"] == 0.3 and d["cfbd:102"]["col_dominator_last"] == 0.25 and d["00-0000001"]["col_dominator_last"] is None
        assert d["00-0031381"]["col_breakout_age"] == 20.0 and d["cfbd:102"]["col_breakout_age"] is None
