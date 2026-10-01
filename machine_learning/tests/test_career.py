"""career.py: horizon targets (zero-fill vs censor), career features, as-of training, baselines."""
import numpy as np
import polars as pl
import pytest

import career


def _season_table():
    # A: 2018-2021 then gone. B: 2019-2022. 2022 is the last complete season; 2023 in progress.
    rows = []
    for pid, seasons, base in [("A", [2018, 2019, 2020, 2021], 100.0), ("B", [2019, 2020, 2021, 2022], 60.0)]:
        for i, s in enumerate(seasons):
            rows.append(dict(player_id=pid, season=s, fpts=base + 10 * i, games=16, ppg=(base + 10 * i) / 16,
                             position="RB", age_at_season=22.0 + i, season_complete=True))
    rows.append(dict(player_id="B", season=2023, fpts=20.0, games=3, ppg=20 / 3, position="RB",
                     age_at_season=26.0, season_complete=False))
    return pl.DataFrame(rows)


class TestHorizonTargets:
    def test_target_is_the_future_season(self):
        out = career.attach_horizon_targets(_season_table(), [1, 2])
        a19 = out.filter((pl.col("player_id") == "A") & (pl.col("season") == 2019)).to_dicts()[0]
        assert a19["h1_fpts"] == 120.0 and a19["h2_fpts"] == 130.0
        assert a19["h1_played"] is True and a19["h1_games"] == 16

    def test_missing_future_row_in_a_complete_season_means_zero_production(self):
        # A played through 2021; 2022 is complete and A has no row -> 0 games, 0 fpts, not played
        out = career.attach_horizon_targets(_season_table(), [1])
        a21 = out.filter((pl.col("player_id") == "A") & (pl.col("season") == 2021)).to_dicts()[0]
        assert a21["h1_observable"] is True
        assert a21["h1_fpts"] == 0.0 and a21["h1_games"] == 0 and a21["h1_played"] is False
        assert a21["h1_ppg"] is None

    def test_future_beyond_last_complete_season_is_censored_not_zero(self):
        out = career.attach_horizon_targets(_season_table(), [1, 2])
        b22 = out.filter((pl.col("player_id") == "B") & (pl.col("season") == 2022)).to_dicts()[0]
        assert b22["h1_observable"] is False and b22["h1_fpts"] is None and b22["h1_played"] is None
        b21 = out.filter((pl.col("player_id") == "B") & (pl.col("season") == 2021)).to_dicts()[0]
        assert b21["h1_observable"] is True and b21["h1_fpts"] == 90.0     # 2022 is complete
        assert b21["h2_observable"] is False                               # 2023 is not

    def test_incomplete_season_never_used_as_an_outcome(self):
        # B's partial 2023 (20 fpts in 3 games) must not appear as anyone's h1
        out = career.attach_horizon_targets(_season_table(), [1])
        assert 20.0 not in out["h1_fpts"].drop_nulls().to_list()


class TestCareerFeatures:
    def test_cumulative_through_current_season_only(self):
        out = career.career_features(_season_table())
        a20 = out.filter((pl.col("player_id") == "A") & (pl.col("season") == 2020)).to_dicts()[0]
        assert a20["career_seasons"] == 3
        assert a20["career_fpts"] == 100 + 110 + 120
        assert a20["career_games"] == 48
        assert abs(a20["best_ppg"] - 120 / 16) < 1e-9

    def test_feature_frame_has_fixed_width(self):
        ff = career.horizon_feature_frame(career.career_features(_season_table()))
        assert ff.columns == career.FEATURES


def _big_table(n_players=40, seasons=range(2010, 2023)):
    rng = np.random.default_rng(0)
    rows = []
    for p in range(n_players):
        start = int(rng.choice([2010, 2012, 2014]))
        for i, s in enumerate(seasons):
            if s < start or i - (start - 2010) > int(rng.integers(3, 9)):
                continue
            g = int(rng.integers(8, 18)); ppg = float(rng.uniform(5, 25))
            rows.append(dict(player_id=f"p{p}", season=s, fpts=g * ppg, games=g, ppg=ppg,
                             position=["QB", "RB", "WR", "TE"][p % 4], age_at_season=22.0 + i,
                             season_complete=True))
    return pl.DataFrame(rows)


class TestHorizonModels:
    def test_fit_predict_shapes_and_bounds(self):
        df = career.attach_horizon_targets(career.career_features(_big_table()), [1, 2])
        m = career.HorizonModels([1, 2], n_estimators=5, max_depth=2).fit(df)
        out = m.predict(df)
        for k in (1, 2):
            assert f"h{k}_fpts_hat" in out.columns
            assert (out[f"h{k}_games_hat"] >= 0).all() and (out[f"h{k}_games_hat"] <= career.MAX_GAMES).all()
            assert (out[f"h{k}_ppg_hat"] >= 0).all()

    def test_as_of_season_hides_later_outcomes(self):
        df = career.attach_horizon_targets(career.career_features(_big_table()), [1])
        m = career.HorizonModels([1], n_estimators=5, max_depth=2)
        # as_of 2011: only rows with season+1 <= 2011 (i.e. season 2010) may train
        rows = df.filter(pl.col("h1_observable") & ((pl.col("season") + 1) <= 2011))
        assert rows["season"].max() == 2010
        m.fit(df, as_of_season=2011)
        assert 1 in m.ppg_models

    def test_estimate_sigma_per_horizon_and_position_then_attached_by_predict(self):
        df = career.attach_horizon_targets(career.career_features(_big_table()), [1, 2])
        m = career.HorizonModels([1, 2], n_estimators=5, max_depth=2).fit(df)
        sigma = m.estimate_sigma(df, holdout=3)
        assert set(sigma) == {1, 2} and all(sigma[k]["__all__"] > 0 for k in (1, 2))
        out = m.predict(df.head(5))
        assert "h1_ppg_sigma" in out.columns and (out["h1_ppg_sigma"] > 0).all()

    def test_as_of_with_no_outcomes_raises(self):
        df = career.attach_horizon_targets(career.career_features(_big_table()), [1])
        with pytest.raises(ValueError):
            career.HorizonModels([1], n_estimators=5).fit(df, as_of_season=2009)


class TestBaselines:
    def test_carry_forward_repeats_this_season(self):
        df = career.attach_horizon_targets(_season_table(), [1, 2])
        out = career.carry_forward_horizons(df, [1, 2])
        assert out["h1_fpts_carry"].to_list() == out["fpts"].to_list()

    def test_decay_uses_pooled_retention_from_train(self):
        df = career.attach_horizon_targets(career.career_features(_big_table()), [1])
        train, test = df.filter(pl.col("season") < 2020), df.filter(pl.col("season") == 2020)
        out = career.decay_baseline(train, test, [1], as_of_season=2020)
        assert "h1_fpts_decay" in out.columns and out["h1_fpts_decay"].null_count() == 0


class TestWalkForward:
    def test_runs_and_reports_all_methods(self):
        df = career.attach_horizon_targets(career.career_features(_big_table()), [1, 2])
        per_fold, agg = career.walk_forward_horizon_eval(df, [1, 2], start_season=2019,
                                                         n_estimators=5, max_depth=2)
        assert set(agg["method"].to_list()) == {"model", "carry_forward", "decay"}
        assert set(agg["horizon"].to_list()) == {1, 2}
        # test season 2021's h2 (2023) is censored -> fewer h2 folds than h1 folds
        assert per_fold.filter(pl.col("horizon") == 2)["test_season"].max() <= 2020
