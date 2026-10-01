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
