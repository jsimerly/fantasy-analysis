"""inseason.py: to-date features, snapshot targets (ROS / next season, censoring), leak guards."""
import numpy as np
import polars as pl
import pytest

import inseason


def _weeks():
    rows = []
    # player A: 2023 weeks 1-6 (season complete), 2024 weeks 1-3 (season in progress)
    for w in range(1, 7):
        rows.append(dict(player_id="A", season=2023, week=w, fpts=10.0 + w, targets=5, rush_att=2, rec=4, pass_att=0,
                         target_share=0.2, wopr=0.3, game_date=None, player_name="A", position="WR", team="KC",
                         age_at_season=25.0, exp_at_season=3, draft_round=2, draft_pick=40, is_undrafted=False, is_rookie=False))
    for w in (1, 2, 3):
        rows.append(dict(player_id="A", season=2024, week=w, fpts=20.0, targets=8, rush_att=0, rec=6, pass_att=0,
                         target_share=0.3, wopr=0.5, game_date=None, player_name="A", position="WR", team="KC",
                         age_at_season=26.0, exp_at_season=4, draft_round=2, draft_pick=40, is_undrafted=False, is_rookie=False))
    # player B: 2023 weeks 1-2 only (then gone for good)
    for w in (1, 2):
        rows.append(dict(player_id="B", season=2023, week=w, fpts=5.0, targets=1, rush_att=5, rec=1, pass_att=0,
                         target_share=0.05, wopr=0.1, game_date=None, player_name="B", position="RB", team="DET",
                         age_at_season=30.0, exp_at_season=8, draft_round=None, draft_pick=None, is_undrafted=True, is_rookie=False))
    return pl.DataFrame(rows)


def _seasons():
    return pl.DataFrame({
        "player_id": ["A", "A", "A", "B"], "season": [2022, 2023, 2024, 2023],
        "fpts": [100.0, 81.0, 60.0, 10.0], "games": [10, 6, 3, 2], "ppg": [10.0, 13.5, 20.0, 5.0],
        "lag1_fpts": [None, 100.0, 81.0, None], "lag1_ppg": [None, 10.0, 13.5, None], "lag1_games": [None, 10, 6, None],
        "best_ppg": [10.0, 13.5, 20.0, 5.0], "career_seasons": [1, 2, 3, 1], "career_fpts": [100.0, 181.0, 241.0, 10.0],
        "career_games": [10, 16, 19, 2], "total_touches": [60, 36, 18, 12], "targets": [50, 30, 24, 2], "pass_yds": [0, 0, 0, 0],
        "season_complete": [True, True, False, True],
    })


class TestToDate:
    def test_aggregates_weeks_up_to_w_only(self):
        td = inseason.to_date_features(_weeks(), 3)
        a = td.filter((pl.col("player_id") == "A") & (pl.col("season") == 2023)).to_dicts()[0]
        assert a["td_games"] == 3 and a["td_fpts"] == 11 + 12 + 13 and abs(a["td_ppg"] - 12.0) < 1e-9
        assert a["td_targets_pg"] == 5.0 and a["td_missed"] == 0 and a["week"] == 3

    def test_last3_and_form(self):
        td = inseason.to_date_features(_weeks(), 6)
        a = td.filter((pl.col("player_id") == "A") & (pl.col("season") == 2023)).to_dicts()[0]
        assert abs(a["last3_ppg"] - (14 + 15 + 16) / 3) < 1e-9
        assert a["td_form"] > 0                      # trending up

    def test_missed_weeks_counts_absences(self):
        td = inseason.to_date_features(_weeks(), 4)
        b = td.filter(pl.col("player_id") == "B").to_dicts()[0]
        assert b["td_games"] == 2 and b["td_missed"] == 2


class TestSnapshots:
    def test_prior_season_joined_and_rookie_null(self):
        snaps = inseason.build_snapshots(_weeks(), _seasons(), weeks=[2])
        a24 = snaps.filter((pl.col("player_id") == "A") & (pl.col("season") == 2024)).to_dicts()[0]
        assert a24["prev_ppg"] == 13.5 and a24["prev_career_seasons"] == 2
        b23 = snaps.filter(pl.col("player_id") == "B").to_dicts()[0]
        assert b23["prev_ppg"] is None

    def test_ros_targets_and_zero_when_gone(self):
        snaps = inseason.build_snapshots(_weeks(), _seasons(), weeks=[2])
        a23 = snaps.filter((pl.col("player_id") == "A") & (pl.col("season") == 2023)).to_dicts()[0]
        assert a23["ros_observable"] is True and a23["ros_games"] == 4 and abs(a23["ros_ppg"] - (13 + 14 + 15 + 16) / 4) < 1e-9
        b23 = snaps.filter(pl.col("player_id") == "B").to_dicts()[0]
        assert b23["ros_games"] == 0 and b23["ros_fpts"] == 0.0 and b23["ros_ppg"] is None

    def test_in_progress_season_is_censored(self):
        snaps = inseason.build_snapshots(_weeks(), _seasons(), weeks=[2])
        a24 = snaps.filter((pl.col("player_id") == "A") & (pl.col("season") == 2024)).to_dicts()[0]
        assert a24["ros_observable"] is False and a24["ros_games"] is None
        assert a24["next_observable"] is False and a24["next_fpts"] is None

    def test_next_season_target_and_attrition(self):
        snaps = inseason.build_snapshots(_weeks(), _seasons(), weeks=[2])
        a23 = snaps.filter((pl.col("player_id") == "A") & (pl.col("season") == 2023)).to_dicts()[0]
        # 2024 is NOT complete -> next target censored even though A has 2024 rows
        assert a23["next_observable"] is False
        b23 = snaps.filter(pl.col("player_id") == "B").to_dicts()[0]
        assert b23["next_observable"] is False

    def test_feature_frame_fixed_width(self):
        snaps = inseason.build_snapshots(_weeks(), _seasons(), weeks=[2, 3])
        assert inseason.feature_frame(snaps).columns == inseason.FEATURES


def _big():
    rng = np.random.default_rng(1)
    wk, ss = [], []
    for p in range(30):
        pos = ["QB", "RB", "WR", "TE"][p % 4]
        for s in range(2015, 2024):
            if rng.random() < 0.15:
                continue
            ppg = float(rng.uniform(5, 20)); g = int(rng.integers(10, 18))
            for w in range(1, g + 1):
                wk.append(dict(player_id=f"p{p}", season=s, week=w, fpts=max(0.0, ppg + rng.normal(0, 4)), targets=5, rush_att=3,
                               rec=3, pass_att=0, target_share=0.2, wopr=0.3, game_date=None, player_name=f"p{p}", position=pos,
                               team="X", age_at_season=22.0 + s - 2015, exp_at_season=s - 2015, draft_round=3, draft_pick=80,
                               is_undrafted=False, is_rookie=(s == 2015)))
            ss.append(dict(player_id=f"p{p}", season=s, fpts=ppg * g, games=g, ppg=ppg, lag1_fpts=None, lag1_ppg=None, lag1_games=None,
                           best_ppg=ppg, career_seasons=s - 2014, career_fpts=ppg * g, career_games=g, total_touches=6 * g,
                           targets=5 * g, pass_yds=0, season_complete=True))
    return pl.DataFrame(wk), pl.DataFrame(ss)


class TestInSeasonValue:
    def test_ros_full_weight_next_discounted_tail_from_career(self):
        snaps = pl.DataFrame({"player_id": ["a", "b"], "position": ["QB", "WR"],
                              "ros_ppg_hat": [25.0, 8.0], "ros_games_hat": [10.0, 10.0],
                              "next_ppg_hat": [24.0, 12.0], "next_games_hat": [16.0, 16.0]})
        # career_pred is projected off season T-1: h1 = this season, h2 = next season (both replaced by
        # the in-season model), h3 = the first tail season, weighted (1 - rate)^2
        career_pred = pl.DataFrame({"player_id": ["a"], "h2_ppg_hat": [22.0], "h2_games_hat": [8.0],
                                    "h3_ppg_hat": [24.0], "h3_games_hat": [4.0], "h4_ppg_hat": [20.0], "h4_games_hat": [4.0]})
        out = inseason.inseason_value(snaps, career_pred, {"QB": 20.0, "WR": 10.0}, None, [1, 2, 3, 4], discount_rate=0.5)
        a = out.filter(pl.col("player_id") == "a").to_dicts()[0]
        assert a["vorp_ros"] == 50.0 and a["vorp_next"] == 64.0 and a["h3_vorp_hat"] == 16.0 and a["h4_vorp_hat"] == 0.0
        assert "h2_vorp_hat" not in out.columns                     # next season is never counted twice
        assert abs(a["iv_inseason"] - (50 + 0.5 * 64 + 0.25 * 16)) < 1e-9
        b = out.filter(pl.col("player_id") == "b").to_dicts()[0]        # no career tail -> only ros/next
        assert b["vorp_ros"] == 0.0 and b["vorp_next"] == 32.0 and abs(b["iv_inseason"] - 16.0) < 1e-9


def _team_week():
    rows = []
    for w in range(1, 7):
        rows.append(dict(season=2023, team="KC", week=w, mkt_margin_td=1.0 * w, mkt_total_td=48.0, mkt_win_prob_td=0.6, point_diff_td=2.0 * w, win_pct_td=0.7,
                         off_epa_td=0.1, pass_rate_td=0.6, line_this_week=3.0 if w != 4 else None, total_this_week=47.0, mkt_margin_prev=2.5, point_diff_prev=4.0))
        rows.append(dict(season=2023, team="DET", week=w, mkt_margin_td=-1.0 * w, mkt_total_td=44.0, mkt_win_prob_td=0.4, point_diff_td=-2.0 * w, win_pct_td=0.3,
                         off_epa_td=-0.05, pass_rate_td=0.55, line_this_week=-3.0, total_this_week=43.0, mkt_margin_prev=None, point_diff_prev=None))
    for w in (1, 2, 3):
        rows.append(dict(season=2024, team="KC", week=w, mkt_margin_td=5.0, mkt_total_td=50.0, mkt_win_prob_td=0.7, point_diff_td=8.0, win_pct_td=1.0,
                         off_epa_td=0.2, pass_rate_td=0.65, line_this_week=6.0, total_this_week=49.0, mkt_margin_prev=6.0, point_diff_prev=5.0))
    return pl.DataFrame(rows)


def _contracts():
    return pl.DataFrame({"gsis_id": ["A", "A"], "season": [2023, 2024], "position": ["WR", "WR"], "apy_cap_pct": [0.05, 0.09], "guaranteed_cap_pct": [0.1, 0.2],
                         "years_left": [0, 3], "contract_year": [True, False], "is_rookie_deal": [True, False], "contract_age": [3, 0], "apy_cap_pct_pos_pctl": [0.6, 0.95]})


class TestExtras:
    def test_team_to_date_joins_the_snapshot_week_and_the_contract_the_season(self):
        snaps = inseason.build_snapshots(_weeks(), _seasons(), weeks=[2, 4], team_week=_team_week(), contracts=_contracts()).sort(["player_id", "season", "week"])
        a23w2 = snaps.filter((pl.col("player_id") == "A") & (pl.col("season") == 2023) & (pl.col("week") == 2)).row(0, named=True)
        a23w4 = snaps.filter((pl.col("player_id") == "A") & (pl.col("season") == 2023) & (pl.col("week") == 4)).row(0, named=True)
        b23w2 = snaps.filter((pl.col("player_id") == "B") & (pl.col("season") == 2023) & (pl.col("week") == 2)).row(0, named=True)
        assert a23w2["tm_mkt_margin_td"] == 2.0 and a23w4["tm_mkt_margin_td"] == 4.0 and a23w4["tm_line_this_week"] is None and a23w2["tm_line_this_week"] == 3.0
        assert a23w2["tm_mkt_margin_prev"] == 2.5 and b23w2["tm_mkt_margin_td"] == -2.0 and b23w2["tm_mkt_margin_prev"] is None
        assert a23w2["ct_apy_cap_pct"] == 0.05 and a23w2["ct_contract_year"] == 1.0 and a23w2["ct_is_rookie_deal"] == 1.0 and a23w2["ct_has_contract"] == 1.0
        assert a23w4["ct_years_left"] == 0.0 and b23w2["ct_has_contract"] == 0.0 and b23w2["ct_apy_cap_pct"] is None
        a24 = snaps.filter((pl.col("player_id") == "A") & (pl.col("season") == 2024) & (pl.col("week") == 2)).row(0, named=True)
        assert a24["tm_mkt_margin_td"] == 5.0 and a24["ct_apy_cap_pct"] == 0.09 and a24["ct_years_left"] == 3.0

    def test_old_team_codes_map_to_the_current_franchise(self):
        tw = _team_week().with_columns(pl.when(pl.col("team") == "KC").then(pl.lit("KC")).otherwise(pl.lit("OAK")).alias("team"))
        wk = _weeks().with_columns(pl.when(pl.col("team") == "DET").then(pl.lit("LV")).otherwise(pl.col("team")).alias("team"))
        snaps = inseason.build_snapshots(wk, _seasons(), weeks=[2], team_week=tw)
        b = snaps.filter(pl.col("player_id") == "B").row(0, named=True)
        assert b["tm_mkt_margin_td"] == -2.0

    def test_feature_frame_and_models_take_the_extra_columns(self):
        snaps = inseason.build_snapshots(_weeks(), _seasons(), weeks=[2], team_week=_team_week(), contracts=_contracts())
        extra = inseason.extra_columns(["team", "contract"])
        X = inseason.feature_frame(snaps, extra)
        assert X.width == len(inseason.FEATURES) + len(extra) and set(extra) <= set(X.columns)
        assert inseason.feature_frame(snaps.drop(extra), extra)[extra[0]].null_count() == snaps.height   # absent columns are null, not an error
        assert inseason.extra_columns(["contract"]) == inseason.CONTRACT_SNAP_COLS and inseason.extra_columns([]) == []
        with pytest.raises(ValueError):
            inseason.extra_columns(["weather"])
        wk, ss = _big()
        big = inseason.build_snapshots(wk, ss, weeks=[3, 8], contracts=_contracts())
        m = inseason.InSeasonModels(n_estimators=5, max_depth=2, extra_features=inseason.CONTRACT_SNAP_COLS).fit(big, as_of_season=2022)
        assert m.models["ros_ppg"].n_features_in_ == len(inseason.FEATURES) + len(inseason.CONTRACT_SNAP_COLS)


class TestTrainingSubset:
    def test_weeks_filter_then_recent_seasons_first_with_a_random_tail(self):
        rows = pl.DataFrame({"season": [2020] * 6 + [2021] * 6 + [2022] * 6, "week": [3, 6, 9, 13, 4, 5] * 3, "x": range(18)})
        sub = inseason.training_subset(rows, train_weeks=[3, 6, 9, 13], max_rows=6)
        assert sub.height == 6 and sub["season"].to_list() == [2022] * 4 + [2021] * 2       # 2022 whole, two of 2021 at random, 2020 out
        assert inseason.training_subset(rows, None, None).height == 18 and inseason.training_subset(rows, [3], None).height == 3
        assert inseason.training_subset(rows, None, 100).height == 18
        assert inseason.training_subset(rows, None, 6)["season"].unique().to_list() == [2022]

    def test_tabpfn_backend_builds_the_career_wrapper_on_the_capped_set(self, monkeypatch):
        import career

        class _Fake:
            def __init__(self, device, seed, params):
                self.n = None

            def fit(self, X, y, sample_weight=None):
                self.n = len(X); return self

            def predict(self, X):
                return np.full(len(X), 5.0)

        monkeypatch.setattr(career, "_TabPFN", _Fake)
        wk, ss = _big()
        snaps = inseason.build_snapshots(wk, ss, weeks=[3, 8])
        m = inseason.InSeasonModels(backend="tabpfn", train_weeks=[3], max_train_rows=40).fit(snaps, as_of_season=2022)
        assert all(n <= 40 for n in m.train_rows.values()) and m.train_rows["ros_games"] == 40
        out = m.predict(snaps.filter(pl.col("season") == 2022))
        assert (out["ros_ppg_hat"] == 5.0).all()
        with pytest.raises(ValueError):
            inseason.InSeasonModels(backend="forest").fit(snaps, as_of_season=2022)


class TestModels:
    def test_fit_predict_and_as_of_guard(self):
        wk, ss = _big()
        snaps = inseason.build_snapshots(wk, ss, weeks=[3, 8])
        m = inseason.InSeasonModels(n_estimators=5, max_depth=2).fit(snaps, as_of_season=2022)
        out = m.predict(snaps.filter(pl.col("season") == 2022))
        assert {"ros_ppg_hat", "ros_games_hat", "next_fpts_hat"} <= set(out.columns)
        assert (out["ros_games_hat"] >= 0).all() and (out["ros_games_hat"] <= inseason.MAX_GAMES).all()
        with pytest.raises(ValueError):
            inseason.InSeasonModels(n_estimators=5).fit(snaps, as_of_season=2015)

    def test_baselines_present(self):
        wk, ss = _big()
        snaps = inseason.baselines(inseason.build_snapshots(wk, ss, weeks=[3]))
        assert snaps["bl_blend_ppg"].null_count() == 0


class TestUsageRoleScheduleGroups:
    """BACKLOG 37: the usage (Next Gen + snaps), role (recency + status) and schedule groups."""

    def test_usage_weights_next_gen_by_targets_and_attempts_and_skips_the_season_row(self):
        rec = pl.DataFrame({"season": [2025] * 3, "season_type": ["REG"] * 3, "week": [0, 1, 2], "player_gsis_id": ["g1"] * 3,
                            "targets": [20, 5, 15], "avg_separation": [9.9, 2.0, 4.0], "avg_cushion": [9.9, 6.0, 6.0], "percent_share_of_intended_air_yards": [50.0, 30.0, 40.0],
                            "avg_intended_air_yards": [9.9, 10.0, 12.0], "avg_yac_above_expectation": [9.9, 1.0, -1.0], "catch_percentage": [99.0, 80.0, 60.0]})
        rush = pl.DataFrame({"season": [2025] * 2, "season_type": ["REG"] * 2, "week": [1, 2], "player_gsis_id": ["g2"] * 2, "rush_attempts": [10, 30],
                             "efficiency": [4.0, 3.0], "rush_yards_over_expected_per_att": [1.0, 0.0], "percent_attempts_gte_eight_defenders": [20.0, 40.0], "avg_time_to_los": [2.8, 2.6]})
        status = pl.DataFrame({"season": [2025] * 4, "week": [1, 2, 3, 4], "gsis_id": ["g1"] * 4, "status": ["played"] * 4, "offense_pct": [0.5, 0.6, 0.8, 0.9]})
        u = inseason.usage_features(rec, rush, status, week=3)
        g1 = u.filter(pl.col("player_id") == "g1").to_dicts()[0]
        assert abs(g1["ng_sep"] - (5 * 2.0 + 15 * 4.0) / 20) < 1e-9 and abs(g1["ng_air_share"] - 35.0) < 1e-9     # week 0 skipped
        assert abs(g1["sn_pct_td"] - (0.5 + 0.6 + 0.8) / 3) < 1e-9 and g1["sn_trend"] == 0.0                     # week 4 excluded
        g2 = u.filter(pl.col("player_id") == "g2").to_dicts()[0]
        assert abs(g2["ng_rush_eff"] - (10 * 4.0 + 30 * 3.0) / 40) < 1e-9 and g2["ng_sep"] is None

    def test_role_windows_and_the_trailing_run_of_played_weeks(self):
        wk = pl.DataFrame({"player_id": ["p"] * 6, "season": [2025] * 6, "week": [1, 2, 3, 4, 5, 6], "fpts": [10.0, 12.0, 8.0, 20.0, 30.0, 99.0],
                           "targets": [5, 5, 5, 10, 10, 99], "rush_att": [0] * 6, "rec": [3, 3, 3, 7, 7, 99]})
        status = pl.DataFrame({"season": [2025] * 7, "week": [1, 2, 3, 4, 5, 6, 7], "gsis_id": ["p"] * 7,
                               "status": ["played", "injured_out", "bye", "played", "played", "played", "played"], "offense_pct": [None] * 7})
        r = inseason.role_features(wk, status, week=5).to_dicts()[0]
        assert r["last1_fpts"] == 30.0 and abs(r["last5_ppg"] - 16.0) < 1e-9 and abs(r["last3_targets_pg"] - 25 / 3) < 1e-9
        assert abs(r["tgt_trend"] - (25 / 3 - 7.0)) < 1e-9
        assert r["games_since_return"] == 2 and r["missed_last3"] == 0 and r["td_injured_weeks"] == 1    # byes neutral, week 6+ unseen
        r3 = inseason.role_features(wk, status, week=3).to_dicts()[0]
        assert r3["games_since_return"] == 0 and r3["missed_last3"] == 1

    def test_schedule_uses_only_played_games_for_opponent_strength(self):
        sch = pl.DataFrame({"season": [2025] * 5, "game_type": ["REG"] * 5, "week": [1, 2, 3, 4, 5],
                            "home_team": ["A", "B", "A", "C", "A"], "away_team": ["B", "C", "C", "A", "B"],
                            "home_score": [30, 20, None, None, None], "away_score": [10, 20, None, None, None]})
        s = inseason.schedule_features(sch, week=2, last_week=5)
        a = s.filter(pl.col("team") == "A").to_dicts()[0]
        assert a["sch_games_left"] == 3                        # weeks 3, 4, 5
        assert abs(a["sch_opp_pd_next"] - 0.0) < 1e-9          # next: C, who drew in week 2
        assert abs(a["sch_opp_pd"] - ((0.0 + 0.0 + -10.0) / 3)) < 1e-9    # C, C, B (B: lost by 20 in week 1, drew week 2 -> -10 per game)
        assert a["sch_bye_ahead"] == 0
        c = s.filter(pl.col("team") == "C").to_dicts()[0]
        assert c["sch_games_left"] == 2 and c["sch_bye_ahead"] == 1       # no week-5 game

    def test_groups_are_registered_and_build_snapshots_joins_them(self):
        assert set(inseason.EXTRA_GROUPS) >= {"usage", "role", "schedule"}
        assert inseason.extra_columns(["role"]) == inseason.ROLE_COLS

    def test_opportunity_expected_points_to_date_and_the_luck_signal(self):
        ffo = pl.DataFrame({"season": [2025] * 4, "week": [1, 2, 3, 4], "player_id": ["g"] * 4,
                            "total_fantasy_points_exp": [10.0, 12.0, 14.0, 99.0], "total_fantasy_points": [15.0, 12.0, 10.0, 99.0],
                            "pass_fantasy_points_exp": [0.0] * 4, "rush_fantasy_points_exp": [2.0, 2.0, 2.0, 9.0], "rec_fantasy_points_exp": [8.0, 10.0, 12.0, 9.0],
                            "total_touchdown_exp": [0.5, 0.5, 0.5, 9.0], "total_touchdown": [1, 0, 0, 9], "total_yards_gained_exp": [60.0, 70.0, 80.0, 9.0],
                            "rec_attempt": [6, 8, 10, 9], "rec_air_yards": [60.0, 80.0, 100.0, 9.0]})
        o = inseason.opportunity_features(ffo, week=3).to_dicts()[0]
        assert abs(o["op_xfp_pg"] - 12.0) < 1e-9 and abs(o["op_rec_xfp_pg"] - 10.0) < 1e-9 and abs(o["op_xfp_last3"] - 12.0) < 1e-9
        assert abs(o["op_fp_oe_pg"] - 1 / 3) < 1e-9 and abs(o["op_td_oe_pg"] - (1 - 1.5) / 3) < 1e-9       # +1 point and -0.5 TD of luck over three games
        assert abs(o["op_targets_pg"] - 8.0) < 1e-9 and abs(o["op_x_yards_pg"] - 70.0) < 1e-9 and o["op_xfp_trend"] == 0.0
        assert "opportunity" in inseason.EXTRA_GROUPS and inseason.extra_columns(["opportunity"]) == inseason.OPP_COLS

    def test_consensus_is_the_coming_weeks_projection_and_the_record_against_it(self):
        proj = pl.DataFrame({"season": [2025] * 6, "week": [1, 2, 3, 4, 4, 4], "player_id": ["s1"] * 4 + ["s2", "s3"], "position": ["WR"] * 5 + ["RB"],
                             "pts_ppr": [10.0, 12.0, 14.0, 20.0, 25.0, 9.0]})
        xwalk = pl.DataFrame({"sleeper_id": ["s1", "s2", "s3", "s9"], "gsis_id": ["g1", "g2", "g3", None]})
        wk = pl.DataFrame({"player_id": ["g1"] * 4, "season": [2025] * 4, "week": [1, 2, 3, 4], "fpts_ppr_nflverse": [15.0, 15.0, 15.0, 99.0]})
        c = {r["player_id"]: r for r in inseason.consensus_features(proj, xwalk, wk, week=3).to_dicts()}
        g1 = c["g1"]
        assert g1["cs_next_ppr"] == 20.0 and g1["cs_next_rank_pos"] == 2.0                       # week 4: s2 (25) ranks first among WRs
        assert abs(g1["cs_td_mean"] - 12.0) < 1e-9 and abs(g1["cs_beat_td"] - 3.0) < 1e-9       # weeks 1-3 only; beat the consensus by 3
        assert abs(g1["cs_next_vs_td"] - 5.0) < 1e-9 and g1["cs_has"] == 1.0
        assert c["g3"]["cs_next_rank_pos"] == 1.0 and c["g3"]["cs_td_mean"] is None             # the only RB; no projections before week 4
        assert "g9" not in c and inseason.extra_columns(["consensus"]) == inseason.CONSENSUS_COLS

