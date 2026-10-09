"""feature_groups: every group adds only end-of-season-T information, keyed correctly, with
coverage-aware nulls; the registry resolves and assembles combinations."""
import polars as pl
import pytest

import feature_groups as fg


def _matrix(rows):
    base = dict(player_id="p1", season=2023, position="WR", team="KC", fpts=100.0, ppg=8.0, games=12)
    return pl.DataFrame([{**base, **r} for r in rows])


def _injury(rows):
    base = dict(season=2023, week=1, game_type="REG", gsis_id="p1", is_out=False, on_injury_report=True,
                on_injured_reserve=False, injury_class="other")
    return pl.DataFrame([{**base, **r} for r in rows])


def _depth(rows):
    base = dict(season=2023, week=1, game_type="REG", gsis_id="p1", position="WR", depth_rank=1, position_rank=None)
    return pl.DataFrame([{**base, **r} for r in rows], schema_overrides={"position_rank": pl.Int32})


def _weeks(rows):
    base = dict(player_id="p1", season=2023, week=1, season_type="REG", fpts=10.0, targets=5, rush_att=0, rec=3)
    return pl.DataFrame([{**base, **r} for r in rows])


class TestInjury:
    def test_counts_by_class_and_lag_without_leaking_next_season(self):
        inj = _injury([
            {"week": 1, "is_out": True, "injury_class": "soft_tissue"}, {"week": 2, "is_out": True, "injury_class": "soft_tissue"},
            {"week": 3, "is_out": True, "injury_class": "knee_achilles"}, {"week": 4, "injury_class": "concussion"},
            {"week": 5, "on_injury_report": False, "on_injured_reserve": True, "is_out": True, "injury_class": "unknown"},
            {"season": 2024, "week": 1, "is_out": True, "injury_class": "soft_tissue"},     # next season: must not leak into 2023
            {"season": 2024, "week": 3, "game_type": "WC", "is_out": True},                # playoffs: ignored
        ])
        out = fg.build_injury(_matrix([{"season": 2023}, {"season": 2024}]), fg.Context(injury=inj)).sort("season")
        r23, r24 = out.to_dicts()
        assert (r23["inj_weeks_out"], r23["inj_weeks_listed"], r23["inj_reserve_weeks"]) == (4, 4, 1)
        assert (r23["inj_soft_tissue_weeks"], r23["inj_structural_weeks"], r23["inj_concussion_weeks"]) == (2, 1, 1)
        assert r23["lag1_inj_weeks_out"] is None                       # 2022 is before the source's coverage
        assert (r24["inj_weeks_out"], r24["inj_soft_tissue_weeks"]) == (1, 1)
        assert (r24["lag1_inj_weeks_out"], r24["lag1_inj_soft_tissue_weeks"], r24["lag1_inj_structural_weeks"]) == (4, 2, 1)

    def test_within_coverage_missing_player_is_zero_before_coverage_is_null(self):
        inj = _injury([{"season": 2023}])
        out = fg.build_injury(_matrix([{"player_id": "p2", "season": 2023}, {"player_id": "p2", "season": 2005}]), fg.Context(injury=inj)).sort("season")
        assert out["inj_weeks_out"].to_list() == [None, 0]


class TestRole:
    def test_season_shape_of_the_depth_chart_joined_by_position(self):
        d = _depth([{"week": 1, "depth_rank": 3}, {"week": 2, "depth_rank": 3}, {"week": 3, "depth_rank": 2}, {"week": 4, "depth_rank": 1},
                    {"week": 5, "depth_rank": 1, "position_rank": 1},
                    {"week": 1, "position": "TE", "depth_rank": 1}])                      # a TE listing is not his WR row
        out = fg.build_role(_matrix([{}]), fg.Context(depth=d)).row(0, named=True)
        assert (out["depth_start"], out["depth_end"], out["depth_best"], out["depth_weeks_listed"]) == (3, 1, 1, 5)
        assert out["depth_moves"] == 2 and out["depth_change_season"] == -2 and out["position_rank_end"] == 1
        assert abs(out["depth_starter_share"] - 0.4) < 1e-9

    def test_unlisted_player_in_covered_season_has_zero_weeks(self):
        out = fg.build_role(_matrix([{"player_id": "p9"}]), fg.Context(depth=_depth([{}]))).row(0, named=True)
        assert out["depth_weeks_listed"] == 0 and out["depth_end"] is None


class TestTrend:
    def test_second_half_vs_first_half_and_last_four(self):
        w = _weeks([{"week": i, "fpts": f, "targets": t} for i, (f, t) in enumerate([(4, 2), (6, 3), (8, 4), (12, 7), (14, 8), (16, 9)], start=1)])
        out = fg.build_trend(_matrix([{}]), fg.Context(weeks=w)).row(0, named=True)
        assert out["ppg_first_half"] == 6.0 and out["ppg_second_half"] == 14.0 and out["trend_ppg"] == 8.0
        assert out["trend_targets_pg"] == 5.0 and out["last4_ppg"] == 12.5 and out["last4_targets_pg"] == 7.0

    def test_playoff_weeks_excluded(self):
        w = _weeks([{"week": 1, "fpts": 10.0}, {"week": 2, "fpts": 10.0}, {"week": 19, "season_type": "POST", "fpts": 50.0}])
        out = fg.build_trend(_matrix([{}]), fg.Context(weeks=w)).row(0, named=True)
        assert out["last4_ppg"] == 10.0


class TestSituation:
    def test_team_change_flags_from_the_matrix_itself(self):
        m = _matrix([{"season": 2021, "team": "SEA"}, {"season": 2022, "team": "SEA"}, {"season": 2023, "team": "KC"}, {"season": 2024, "team": "KC"}])
        out = fg.build_situation(m, fg.Context()).sort("season")
        assert out["moved_this_season"].to_list() == [None, 0, 1, 0]
        assert out["lag1_moved"].to_list() == [None, None, 0, 1]


class TestRookie:
    def test_draft_capital_scaled_by_experience(self):
        m = _matrix([{"player_id": "r", "exp_at_season": 0, "draft_pick": 10, "draft_round": 1},
                     {"player_id": "y", "exp_at_season": 2, "draft_pick": 10, "draft_round": 1},
                     {"player_id": "v", "exp_at_season": 9, "draft_pick": 10, "draft_round": 1},
                     {"player_id": "u", "exp_at_season": 0, "draft_pick": None, "draft_round": None}])
        out = fg.build_rookie(m, fg.Context()).sort("player_id")
        d = {r["player_id"]: r for r in out.to_dicts()}
        assert (d["r"]["pick_x_rookie"], d["r"]["pick_x_young"], d["r"]["pick_over_exp"], d["r"]["round_x_rookie"]) == (10, 10, 10, 1)
        assert (d["y"]["pick_x_rookie"], d["y"]["pick_x_young"], d["y"]["pick_over_exp"]) == (0, 10, 10 / 3)
        assert (d["v"]["pick_x_rookie"], d["v"]["pick_x_young"], d["v"]["pick_over_exp"]) == (0, 0, 1.0)
        assert d["u"]["pick_x_rookie"] == 260 and d["u"]["round_x_rookie"] == 8


class TestRegistry:
    def test_resolve_dedupes_and_rejects_unknown(self):
        groups = fg.resolve("base,career,base")
        assert [g.name for g in groups] == ["base", "career"]
        with pytest.raises(ValueError, match="unknown feature group"):
            fg.resolve(["base", "weather"])

    def test_feature_columns_are_ordered_and_unique(self):
        cols = fg.feature_columns(fg.resolve(["base", "career", "injury"]))
        assert len(cols) == len(set(cols)) and cols[:3] == fg.GROUPS["base"].columns[:3] and "inj_weeks_out" in cols

    def test_assemble_adds_null_columns_when_a_source_is_empty(self):
        ctx = fg.Context(depth=_depth([]).clear(), injury=_injury([]).clear(), weeks=_weeks([]).clear())
        out = fg.assemble(_matrix([{}]), fg.resolve(["base", "injury", "role", "trend", "situation"]), ctx)
        for c in fg.INJURY_COLS + fg.ROLE_COLS + fg.TREND_COLS + fg.SITUATION_COLS:
            assert c in out.columns, c
        assert out["inj_weeks_out"][0] is None and out["depth_end"][0] is None



def _ws(rows):
    base = dict(gsis_id="p1", season=2024, week=1, status="played", injury_class=None, fpts=10.0, opportunities=8.0, offense_pct=0.8)
    return pl.DataFrame([{**base, **r} for r in rows], schema_overrides={"injury_class": pl.Utf8})


class TestWeekly:
    def test_slots_reason_counts_snaps_and_lag(self):
        ws = _ws([dict(week=1), dict(week=2, fpts=20.0, opportunities=12.0, offense_pct=0.9), dict(week=3, status="bye", fpts=None, opportunities=None, offense_pct=None),
                  dict(week=4, status="injured_out", injury_class="knee_achilles", fpts=None, opportunities=None, offense_pct=None),
                  dict(week=5, status="injured_reserve", injury_class="knee_achilles", fpts=None, opportunities=None, offense_pct=None),
                  dict(week=6, status="injured_reserve", injury_class=None, fpts=None, opportunities=None, offense_pct=None),
                  dict(week=7, status="suspended", fpts=None, opportunities=None, offense_pct=None),
                  dict(season=2023, week=1), dict(season=2023, week=2, status="dnp", fpts=None, opportunities=None, offense_pct=None)])
        out = fg.build_weekly(_matrix([{"season": 2024}, {"season": 2023}]), fg.Context()) if False else None
        ctx = fg.Context(); ctx._week_status = ws
        out = fg.build_weekly(_matrix([{"season": 2024}, {"season": 2023}]), ctx).sort("season")
        r = out.row(1, named=True)                                     # 2024
        assert [r[f"wk{w}_status"] for w in range(1, 8)] == [0, 0, 1, 3, 2, 2, 4] and r["wk8_status"] is None
        assert [r[f"wk{w}_inj"] for w in range(1, 8)] == [None, None, None, 2, 2, 0, None]       # 0 = injured, body part unknown
        assert r["wk2_fpts"] == 20.0 and r["wk2_opp"] == 12.0 and r["wk2_snap"] == 0.9 and r["wk3_fpts"] is None
        assert r["wks_played"] == 2 and r["wks_bye"] == 1 and r["wks_inj_out"] == 1 and r["wks_inj_reserve"] == 2 and r["wks_suspended"] == 1
        assert r["inj_wks_knee"] == 2 and r["inj_wks_soft"] == 0 and abs(r["snap_pct_mean"] - 0.85) < 1e-9 and abs(r["snap_pct_last4"] - 0.85) < 1e-9
        assert r["lag1_wks_played"] == 1 and r["lag1_wks_dnp"] == 1                       # 2023 counts on the 2024 row
        r23 = out.row(0, named=True)
        assert r23["wks_played"] == 1 and r23["lag1_wks_played"] is None                   # 2022 is before the table's coverage

    def test_missing_table_leaves_the_matrix_alone_and_assemble_fills_nulls(self):
        ctx = fg.Context(); ctx._week_status = pl.DataFrame()
        m = _matrix([{}])
        assert fg.build_weekly(m, ctx).columns == m.columns
        out = fg.assemble(m, fg.resolve(["weekly"]), ctx)
        assert all(c in out.columns for c in fg.WEEKLY_COLS) and out["wk1_status"][0] is None


def _team_fact(rows):
    base = dict(season=2023, team="KC", mkt_margin=3.0, mkt_margin_last4=2.0, mkt_total=48.0, mkt_implied_pts=25.5, mkt_win_prob=0.6, point_diff_pg=5.0,
                win_pct=0.7, off_epa_per_play=0.1, pass_rate=0.6, plays_pg=64.0, n_starting_qbs=1, qb_main_share=1.0, lag1_mkt_margin=2.5, lag1_off_epa_per_play=0.08)
    return pl.DataFrame([{**base, **r} for r in rows])


class TestTeam:
    def test_joins_the_season_team_and_the_change_from_the_previous_team(self):
        fact = _team_fact([{}, {"season": 2022, "team": "KC", "mkt_margin": 4.0}, {"season": 2022, "team": "OAK", "mkt_margin": -6.0}, {"season": 2023, "team": "LV", "mkt_margin": -5.0}])
        mx = _matrix([{"season": 2022, "team": "LV"}, {"season": 2023, "team": "KC"}, {"player_id": "p2", "season": 2023, "team": "LV"}])
        out = fg.build_team(mx, fg.Context(team=fact)).sort(["player_id", "season"])
        p1_22, p1_23, p2_23 = out.to_dicts()
        assert p1_23["tm_mkt_margin"] == 3.0 and p1_23["tm_mkt_total"] == 48.0 and p1_23["tm_qb_main_share"] == 1.0 and p1_23["tm_lag1_mkt_margin"] == 2.5
        assert p1_23["tm_mkt_margin_vs_prev_team"] == 3.0 - (-6.0)         # KC this year vs the Raiders last year (OAK and LV are one franchise)
        assert p1_22["tm_mkt_margin"] == -6.0 and p1_22["tm_mkt_margin_vs_prev_team"] is None
        assert p2_23["tm_mkt_margin"] == -5.0 and p2_23["tm_mkt_margin_vs_prev_team"] is None

    def test_without_the_table_every_column_is_null(self):
        out = fg.build_team(_matrix([{}]), fg.Context(team=pl.DataFrame()))
        assert all(out[c][0] is None for c in fg.TEAM_COLS)


def _contract_fact(rows):
    base = dict(gsis_id="p1", season=2023, position="WR", apy_cap_pct=0.08, guaranteed_cap_pct=0.1, years_left=2, contract_year=False, contract_age=1,
                contract_years=4, is_rookie_deal=False, cap_pct_season=0.07, cap_pct_next=0.09, guaranteed_salary_season=5.0, apy_cap_pct_pos_pctl=0.9, n_contracts_signed=2)
    return pl.DataFrame([{**base, **r} for r in rows])


class TestContract:
    def test_joins_the_deal_in_force_and_flags_no_deal_on_file(self):
        fact = _contract_fact([{}, {"season": 2022, "years_left": 3, "contract_year": False}])
        mx = _matrix([{"season": 2022}, {"season": 2023}, {"player_id": "p2", "season": 2023}, {"player_id": "p3", "season": 1990}])
        out = fg.build_contract(mx, fg.Context(contracts=fact)).sort(["player_id", "season"])
        p1_22, p1_23, p2_23, p3_90 = out.to_dicts()
        assert p1_23["ct_apy_cap_pct"] == 0.08 and p1_23["ct_years_left"] == 2.0 and p1_23["ct_contract_year"] == 0.0 and p1_23["ct_pos_pctl"] == 0.9
        assert p1_23["ct_has_contract"] == 1.0 and p1_22["ct_years_left"] == 3.0
        assert p2_23["ct_has_contract"] == 0.0 and p2_23["ct_apy_cap_pct"] is None          # covered season, no deal on file
        assert p3_90["ct_has_contract"] is None                                              # before the cap era: unknown, not zero

    def test_registry_resolves_both_groups(self):
        groups = fg.resolve("base,career,team,contract")
        cols = fg.feature_columns(groups)
        assert set(fg.TEAM_COLS) <= set(cols) and set(fg.CONTRACT_COLS) <= set(cols)
        out = fg.assemble(_matrix([{}]), groups, fg.Context(team=pl.DataFrame(), contracts=pl.DataFrame()))
        assert all(c in out.columns for c in fg.TEAM_COLS + fg.CONTRACT_COLS)


class TestNoise:
    def test_noise_is_seeded_per_row_and_carries_no_information(self):
        mx = _matrix([{"season": 2022}, {"season": 2023}, {"player_id": "p2", "season": 2023}])
        a, b = fg.build_noise(mx, fg.Context()), fg.build_noise(mx, fg.Context())
        assert a.select(fg.NOISE_COLS).equals(b.select(fg.NOISE_COLS)) and a.select(fg.NOISE_COLS).n_unique() == 3
        assert "noise" in fg.GROUPS and len(fg.feature_columns(fg.resolve("base,career,noise"))) == len(fg.feature_columns(fg.resolve("base,career"))) + 15
        assert a.filter(pl.col("player_id") == "p2")[fg.NOISE_COLS[0]][0] != a.filter((pl.col("player_id") == "p1") & (pl.col("season") == 2023))[fg.NOISE_COLS[0]][0]
