"""The range of outcomes (BACKLOG 26): quantile columns from a backend that has them, the games cap on
the band, the harness's coverage / pinball scores, floor / ceiling wins, and the rookie band fallback."""
import numpy as np
import polars as pl

import career
import experiments as ex
import inseason
import war
from league import WinCurve


class _FakeQuantileModel:
    """Predicts a constant point and a symmetric band around it."""

    def __init__(self, point, half=2.0):
        self.point, self.half = point, half

    def fit(self, X, y, sample_weight=None):
        return self

    def predict(self, X):
        return np.full(len(X), self.point)

    def predict_quantiles(self, X, qs):
        return np.column_stack([np.full(len(X), self.point + (q - 0.5) * 2 * self.half) for q in qs])


def _models(range_quantiles):
    m = career.HorizonModels([1, 2], backend="xgb", range_quantiles=range_quantiles)
    m.ppg_models = {1: _FakeQuantileModel(12.0, 3.0), 2: _FakeQuantileModel(10.0, 3.0)}
    m.games_models = {1: _FakeQuantileModel(14.0, 2.0), 2: _FakeQuantileModel(12.0, 2.0)}
    m.feature_frame = lambda df: pl.DataFrame({"x": [0.0] * df.height})
    return m


def _df(n=3):
    return pl.DataFrame({"player_id": [f"p{i}" for i in range(n)], "position": ["WR"] * n, "age_at_season": [25.0] * n, "ppg": [11.0] * n, "games": [15] * n})


def test_predict_keeps_the_band_only_when_asked():
    out = _models((0.2, 0.5, 0.8)).predict(_df())
    assert out["h1_ppg_q20"][0] == 12.0 - 0.6 * 3.0 and out["h1_ppg_q80"][0] == 12.0 + 0.6 * 3.0 and out["h1_ppg_q50"][0] == 12.0
    assert out["h2_games_q20"][0] == 12.0 - 1.2 and out["h2_games_q80"][0] == 13.2
    plain = _models(None).predict(_df())
    assert not any("_q" in c for c in plain.columns)


def test_cap_applies_to_the_games_band_too():
    rng = np.random.default_rng(1)
    ages = rng.uniform(22, 42, 300)
    tbl = pl.DataFrame({"position": ["WR"] * 300, "age_at_season": ages, "games": [10] * 300, "h1_observable": [True] * 300,
                        "h1_played": rng.random(300) < 1 / (1 + np.exp(-(14 - 0.4 * ages)))})
    s = career.AgeSurvival().fit(tbl)
    pred = pl.DataFrame({"position": ["WR"], "age_at_season": [43.0], "h1_games_hat": [16.0], "h1_games_q20": [14.0], "h1_games_q80": [17.0]})
    out = s.cap_games(pred, [1])
    assert out["h1_games_hat"][0] < 10 and out["h1_games_q80"][0] == out["h1_games_hat"][0] and out["h1_games_q20"][0] <= out["h1_games_hat"][0]


def test_range_scores_coverage_and_pinball():
    y = np.array([10.0, 12.0, 14.0, 30.0] * 6)
    cohort = pl.DataFrame({"h1_observable": [True] * 24, "h1_played": [True] * 24, "h1_ppg": y,
                           "h1_ppg_q20": [9.0] * 24, "h1_ppg_q50": [12.0] * 24, "h1_ppg_q80": [15.0] * 24})
    s = ex.range_scores(cohort, [1, 2])          # horizon 2 has no band: skipped
    assert abs(s["ppg_cover_2080"] - 0.75) < 1e-9
    assert s["ppg_pinball"] > 0
    assert ex.range_scores(cohort.drop("h1_ppg_q20"), [1]) == {}


def test_wins_range_floor_and_ceiling():
    curve = WinCurve.normal(130.0, 40.0)
    df = pl.DataFrame({"position": ["WR", "WR"], "ros_ppg_hat": [14.0, 14.0], "ros_games_hat": [10.0, 10.0],
                       "h3_ppg_hat": [12.0, 12.0], "h3_games_hat": [14.0, 14.0],
                       "h3_ppg_q20": [9.0, None], "h3_ppg_q80": [15.0, None], "h3_games_q20": [10.0, None], "h3_games_q80": [16.0, None]})
    comps = [war.Component(1, "ros_ppg_hat", "ros_games_hat", None), war.Component(3, "h3_ppg_hat", "h3_games_hat", None)]
    rep = {"WR": 8.0}
    out = war.wins_range(war.wins_above_replacement(df, rep, curve, comps, 0.2), rep, curve, comps, 0.2)
    a, b = out.to_dicts()
    assert a["war_lo_1"] == a["war_1"] == a["war_hi_1"]                       # no band this season: point on both sides
    assert a["war_lo_3"] < a["war_3"] < a["war_hi_3"] and a["war_lo"] < a["war"] < a["war_hi"]
    assert b["war_lo_3"] == 0.0 and b["war_hi_3"] == 0.0                       # a null band is empty, not the point (filled upstream for rookies)


def test_fill_missing_band_from_the_position_spread():
    df = pl.DataFrame({"position": ["WR", "RB"], "h3_ppg_hat": [12.0, 10.0], "h3_games_hat": [14.0, 12.0],
                       "h3_ppg_q20": [9.0, None], "h3_ppg_q50": [12.0, None], "h3_ppg_q80": [15.0, None], "h3_games_q20": [11.0, None], "h3_games_q80": [16.0, None]})
    out = inseason.fill_missing_band(df, [3, 4], {3: {"__all__": 3.0, "RB": 2.0}})
    r = out.row(1, named=True)
    assert abs(r["h3_ppg_q20"] - (10.0 - inseason.Z_20_80 * 2.0)) < 1e-9 and abs(r["h3_ppg_q80"] - (10.0 + inseason.Z_20_80 * 2.0)) < 1e-9
    assert r["h3_ppg_q50"] == 10.0 and r["h3_games_q20"] == 12.0 and out.row(0, named=True)["h3_ppg_q20"] == 9.0
    assert inseason.fill_missing_band(df, [3], None).equals(df)


def test_inseason_value_keeps_the_band_columns():
    snaps = pl.DataFrame({"player_id": ["a"], "position": ["WR"], "age_at_season": [25.0], "is_rookie": [False], "prev_ppg": [12.0], "prev_games": [15],
                          "ros_ppg_hat": [12.0], "ros_games_hat": [10.0], "next_ppg_hat": [12.0], "next_games_hat": [15.0]})
    tail = pl.DataFrame({"player_id": ["a"], "h3_ppg_hat": [11.0], "h3_games_hat": [14.0], "h3_ppg_q20": [8.0], "h3_ppg_q50": [11.0], "h3_ppg_q80": [14.0], "h3_games_q20": [11.0], "h3_games_q80": [16.0]})
    out = inseason.inseason_value(snaps, tail, {"WR": 8.0}, {3: {"__all__": 3.0}}, [1, 2, 3], 0.2)
    r = out.row(0, named=True)
    assert r["h3_ppg_q20"] == 8.0 and r["h3_ppg_q80"] == 14.0 and r["h3_games_q80"] == 16.0
