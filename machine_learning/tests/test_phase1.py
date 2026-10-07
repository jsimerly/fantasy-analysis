"""Phase 1 unit tests: metrics, walk-forward splits, baselines, model plumbing (no network)."""
import numpy as np
import polars as pl

import baselines
import evaluation
import model


def _df():
    return pl.DataFrame({
        "season": [2018, 2019, 2020, 2018, 2019, 2020],
        "player_id": ["a", "a", "a", "b", "b", "b"],
        "fpts": [100.0, 150.0, 200.0, 50.0, 60.0, 80.0],
        "position": ["RB", "RB", "RB", "WR", "WR", "WR"],
        "age_at_season": [24.0, 25.0, 26.0, 23.0, 24.0, 25.0],
        "target_fpts_next": [150.0, 200.0, 210.0, 60.0, 80.0, 90.0],
    })


class TestMetrics:
    def test_mae_rmse(self):
        assert abs(evaluation.mae([1, 2, 3], [1, 2, 5]) - (2 / 3)) < 1e-9
        assert abs(evaluation.rmse([0, 0], [3, 4]) - np.sqrt(12.5)) < 1e-9


class TestSplits:
    def test_walk_forward_expanding_no_future_leak(self):
        sp = list(evaluation.walk_forward_splits(_df(), start_season=2019))
        assert [s for _, _, s in sp] == [2019, 2020]
        tr, te, s = sp[0]
        assert set(te["season"].to_list()) == {2019}
        assert set(tr["season"].to_list()) == {2018}  # train strictly < s


class TestBaselines:
    def test_carry_forward_is_current_fpts(self):
        pred = baselines.carry_forward()(_df())(_df())
        assert list(pred) == [100.0, 150.0, 200.0, 50.0, 60.0, 80.0]

    def test_age_adjusted_positive(self):
        pred = baselines.age_adjusted_naive("target_fpts_next")(_df())(_df())
        assert len(pred) == 6 and all(p > 0 for p in pred)


class TestModel:
    def test_feature_frame_fixed_schema(self):
        ff = model.make_feature_frame(_df())
        assert ff.width == len(model.FEATURE_COLS)
        assert {"pos_RB", "pos_WR", "is_rookie"} <= set(ff.columns)

    def test_xgb_fits_and_predicts(self):
        pred = model.xgb_regressor("target_fpts_next", n_estimators=10, max_depth=2)(_df())(_df())
        assert len(pred) == 6 and np.isfinite(pred).all()
