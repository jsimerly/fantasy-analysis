"""experiments: metrics, the ledger, and an end-to-end run of a variant on synthetic data
(including a feature the data does not have, which must train as all-null)."""
import numpy as np
import polars as pl

import career
import experiments as ex
import feature_groups as fg
import features


def _fact(seed=0, n_players=40, seasons=range(2015, 2024)):
    rng = np.random.default_rng(seed)
    rows = []
    for p in range(n_players):
        pos = ["QB", "RB", "WR", "TE"][p % 4]
        lvl = rng.uniform(5, 20)
        for s in seasons:
            g = int(rng.integers(8, 18)); ppg = max(0.5, lvl + rng.normal(0, 2))
            rows.append(dict(player_id=f"p{p}", season=s, player_name=f"P{p}", position=pos, team="KC" if p % 2 else "DEN",
                             games=g, fpts=ppg * g, ppg=ppg, pass_yds=0.0, rush_yds=50.0, rec_yds=100.0, targets=40, rec=25,
                             total_touches=60, scrim_yds=150.0, total_tds=3, yds_per_touch=4.0, target_share_avg=0.15, wopr_avg=0.3,
                             draft_round=3, draft_pick=80, age_at_season=22.0 + s - 2015 + p % 5, exp_at_season=s - 2015,
                             is_rookie=(s == 2015), is_undrafted=False, season_complete=True))
    return pl.DataFrame(rows)


def _matrix(H):
    return career.attach_horizon_targets(career.career_features(features.attach_lags_and_target(_fact(), drop_no_target=False)), H)


class TestMetrics:
    def test_top_decile_precision(self):
        score = np.array([10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, -1]); realized = np.array([10, 0, 8, 7, 6, 5, 4, 3, 2, 1, 9, -1])
        assert ex.top_decile_precision(score, realized, frac=0.25) == 2 / 3          # top-3 by score {0,1,2}, by realized {0,2,10}

    def test_ledger_append_and_leaderboard(self):
        s1 = {"name": "a", "groups": "base", "n_features": 3, "horizon": 3, "cohorts": "2018-2020", "n_cohorts": 3, "discount_rate": 0.2,
              "params": "", "timestamp": "t1", "commit": "abc", "spearman_iv_vs_realized": 0.60, "spearman_ktc_vs_realized": 0.62,
              "iv_minus_ktc": -0.02, "top_decile_iv": 0.3, "top_decile_ktc": 0.3, "mae_h1": 40.0}
        s2 = {**s1, "name": "b", "groups": "base,injury", "spearman_iv_vs_realized": 0.65, "iv_minus_ktc": 0.03, "mae_h1": 39.0}
        s3 = {**s1, "name": "c", "horizon": 5, "spearman_iv_vs_realized": 0.70}
        ledger = ex.append_result(None, s1); ledger = ex.append_result(ledger, s2); ledger = ex.append_result(ledger, s3)
        assert ledger.height == 3 and "mae_h1" in ledger.columns
        lb = ex.leaderboard(ledger, horizon=3)
        assert lb["name"].to_list() == ["b", "a"]                       # like-for-like: same horizon only, best first


class TestRunExperiment:
    def test_variant_with_a_group_the_data_lacks_still_trains_and_scores(self):
        H = [1, 2]
        m = _matrix(H)
        ctx = fg.Context(depth=pl.DataFrame(), injury=pl.DataFrame(), weeks=pl.DataFrame())
        cfg = ex.ExperimentConfig(name="t", groups=["base", "career", "injury", "situation"], horizons=H, cohorts=[2020, 2021],
                                  params={"n_estimators": 10, "max_depth": 2})
        rng = np.random.default_rng(1)

        def market_for(cohort, T):               # a noisy market: realized-ish ranks for half the players
            return cohort.with_columns(pl.Series("ktc_value", [float(rng.integers(500, 9999)) if i % 2 else None for i in range(cohort.height)]))

        per_cohort, summary = ex.run_experiment(m, cfg, ctx, rep_for=lambda T: {"QB": 12.0, "RB": 9.0, "WR": 8.0, "TE": 7.0}, market_for=market_for)
        assert per_cohort["cohort"].to_list() == [2020, 2021]
        for c in ("spearman_iv_vs_realized", "spearman_ktc_vs_realized", "top_decile_iv", "mae_h1", "mae_h2"):
            assert c in per_cohort.columns and per_cohort[c].null_count() == 0
        assert summary["groups"] == "base,career,injury,situation" and summary["n_features"] == len(fg.feature_columns(fg.resolve(cfg.groups)))
        assert "iv_minus_ktc" in summary and summary["cohorts"] == "2020-2021"


class TestHorizonModelsFeatureHook:
    def test_explicit_feature_list_including_missing_and_onehot_columns(self):
        H = [1]
        m = _matrix(H)
        feats = ["ppg", "games", "age_at_season", "pos_QB", "pos_WR", "inj_weeks_out"]       # inj_weeks_out is absent
        models = career.HorizonModels(H, features=feats, n_estimators=10, max_depth=2).fit(m, as_of_season=2021)
        X = models.feature_frame(m.head(5))
        assert X.columns == feats and X["inj_weeks_out"].null_count() == 5 and X["pos_QB"].sum() >= 0
        out = models.predict(m.filter(pl.col("season") == 2021))
        assert "h1_fpts_hat" in out.columns and out.height > 0
        default = career.HorizonModels(H, n_estimators=10, max_depth=2).fit(m, as_of_season=2021)
        assert default.feature_frame(m.head(2)).columns == career.FEATURES                 # unchanged default behaviour


class TestMarketEdge:
    def test_disagreement_that_predicts_market_error_scores_positive(self):
        rng = np.random.default_rng(0); n = 60
        market = rng.normal(size=n)
        truth = market + rng.normal(size=n)                 # the market is noisy
        model = truth + 0.3 * rng.normal(size=n)            # we see most of the truth
        e = ex.market_edge(model, market, truth)
        assert e["edge_corr"] > 0.5 and e["edge_spread"] > 5
        e2 = ex.market_edge(market + 0.01 * rng.normal(size=n), market, truth)   # we just echo the market
        assert abs(e2["edge_corr"]) < 0.4 and e2["edge_spread"] < e["edge_spread"]
        assert ex.market_edge(np.arange(5.0), np.arange(5.0), np.arange(5.0)) == {}


def test_paired_comparison_is_cohort_by_cohort(monkeypatch):
    import experiments as ex
    a = pl.DataFrame({"cohort": [2015, 2016, 2017, 2018], "spearman_war_top": [0.50, 0.60, 0.55, 0.65], "mae_war_top": [1.0, 1.0, 1.0, 1.0]})
    b = pl.DataFrame({"cohort": [2015, 2016, 2017, 2018], "spearman_war_top": [0.52, 0.62, 0.57, 0.67], "mae_war_top": [1.0, 1.1, 0.9, 1.0]})
    monkeypatch.setattr(ex, "load_run", lambda ref: a if ref == "a" else b)
    out = ex.paired("a", "b")
    top = out.filter(pl.col("metric") == "spearman_war_top").row(0, named=True)
    assert abs(top["diff_b_minus_a"] - 0.02) < 1e-9 and top["b_wins"] == 4 and top["cohorts"] == 4 and top["t"] > 100
    mae = out.filter(pl.col("metric") == "mae_war_top").row(0, named=True)
    assert abs(mae["diff_b_minus_a"]) < 1e-9 and mae["b_wins"] == 1

