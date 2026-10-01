"""career.HorizonModels(calibrate=True): a walk-forward line per position and horizon that re-scales
ppg / games projections; mechanics and bounds."""
import numpy as np
import polars as pl

import career
import features


def _fact(seed=0, n_players=60, seasons=range(2012, 2024)):
    rng = np.random.default_rng(seed)
    rows = []
    for p in range(n_players):
        pos = ["QB", "RB", "WR", "TE"][p % 4]
        lvl = rng.uniform(5, 22)
        for s in seasons:
            g = int(rng.integers(8, 18)); ppg = max(0.5, lvl + rng.normal(0, 2))
            rows.append(dict(player_id=f"p{p}", season=s, player_name=f"P{p}", position=pos, team="KC",
                             games=g, fpts=ppg * g, ppg=ppg, pass_yds=0.0, rush_yds=50.0, rec_yds=100.0, targets=40, rec=25,
                             total_touches=60, scrim_yds=150.0, total_tds=3, yds_per_touch=4.0, target_share_avg=0.15, wopr_avg=0.3,
                             draft_round=3, draft_pick=80, age_at_season=22.0 + s - 2012 + p % 5, exp_at_season=s - 2012,
                             is_rookie=(s == 2012), is_undrafted=False, season_complete=True))
    return pl.DataFrame(rows)


def test_line_fit_and_slope_bounds():
    x = np.array([1.0, 2, 3, 4, 5]); y = 2 + 1.5 * x
    a, b = career._line(x, y)
    assert abs(a - 2) < 1e-9 and abs(b - 1.5) < 1e-9
    assert career._line(x, 10 * x)[1] == 2.0 and career._line(x, 0.1 * x)[1] == 0.5     # clipped
    assert career._line(np.ones(5), y) == (0.0, 1.0)                                      # degenerate -> identity


def test_calibrated_model_fits_a_table_and_changes_predictions_within_bounds():
    H = [1, 2]
    m = career.attach_horizon_targets(career.career_features(features.attach_lags_and_target(_fact(), drop_no_target=False)), H)
    plain = career.HorizonModels(H, n_estimators=20, max_depth=2).fit(m, as_of_season=2021)
    cal = career.HorizonModels(H, n_estimators=20, max_depth=2, calibrate=True).fit(m, as_of_season=2021)
    assert cal.calibration and set(cal.calibration) <= {1, 2} and "__all__" in cal.calibration[1]
    for pos, ((ap, bp), (ag, bg)) in cal.calibration[1].items():
        assert 0.5 <= bp <= 2.0 and 0.5 <= bg <= 2.0
    test = m.filter(pl.col("season") == 2021)
    a, b = plain.predict(test), cal.predict(test)
    assert (b["h1_ppg_hat"] >= 0).all() and (b["h1_games_hat"] <= career.MAX_GAMES).all()
    assert not np.allclose(a["h1_ppg_hat"].to_numpy(), b["h1_ppg_hat"].to_numpy())   # the line was applied
    # the calibration is built from the model's own held-out errors: sigma still works alongside it
    cal.estimate_sigma(m, as_of_season=2021)
    assert "h1_ppg_sigma" in cal.predict(test).columns
