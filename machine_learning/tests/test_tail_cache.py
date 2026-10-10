"""tail_cache: the weekly refresh reuses the career tail while its key holds (BACKLOG 36)."""
from pathlib import Path

import polars as pl

import tail_cache


def _frame():
    return pl.DataFrame({"player_id": ["a", "b", "c", "d"], "season": [2024, 2025, 2026, 2026], "ppg": [10.0, 11.0, 5.0, None],
                         "is_snapshot_row": [None, None, None, 1.0]})


def test_visible_rows_drop_the_in_progress_season_but_keep_its_snapshots():
    seen = tail_cache.visible_rows(_frame(), 2025)
    assert seen["player_id"].to_list() == ["a", "b", "d"]
    assert tail_cache.visible_rows(_frame().drop("is_snapshot_row"), 2025)["player_id"].to_list() == ["a", "b"]


def test_key_ignores_the_in_progress_row_and_row_order_but_not_content_config_or_code(tmp_path):
    code = tmp_path / "m.py"; code.write_text("x = 1")
    cfg = {"backend": "tabpfn", "target": "blend"}
    k = tail_cache.cache_key(_frame(), 2025, cfg, [code])
    assert k == tail_cache.cache_key(_frame().with_columns(pl.when(pl.col("player_id") == "c").then(9.0).otherwise(pl.col("ppg")).alias("ppg")), 2025, cfg, [code])
    assert k == tail_cache.cache_key(_frame().reverse(), 2025, cfg, [code])
    assert k != tail_cache.cache_key(_frame().with_columns(pl.when(pl.col("player_id") == "a").then(9.0).otherwise(pl.col("ppg")).alias("ppg")), 2025, cfg, [code])
    assert k != tail_cache.cache_key(_frame(), 2025, {**cfg, "target": "level"}, [code])
    assert k != tail_cache.cache_key(_frame(), 2026, cfg, [code])
    code.write_text("x = 2")
    assert k != tail_cache.cache_key(_frame(), 2025, cfg, [code])


def test_store_then_load_round_trips_tail_sigma_and_meta(tmp_path):
    tail = pl.DataFrame({"player_id": ["a"], "h1_ppg_hat": [12.5]})
    sigma = {1: {"QB": 3.0, "__all__": 2.5}, 2: {"__all__": 3.5}}
    tail_cache.store(tmp_path, "k1", tail, sigma, {"as_of": 2025})
    got = tail_cache.load(tmp_path, "k1")
    assert got is not None
    t, s, meta = got
    assert t.equals(tail) and s == sigma and meta["as_of"] == 2025 and meta["key"] == "k1" and "written" in meta
    assert tail_cache.load(tmp_path, "missing") is None
    assert tail_cache.CODE and all(Path(f).exists() for f in tail_cache.CODE)


def _script():
    import importlib.util
    p = Path(__file__).resolve().parents[1] / "scripts" / "backtest_inseason.py"
    spec = importlib.util.spec_from_file_location("backtest_inseason", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_career_tail_reuses_the_cache_until_a_visible_row_changes(monkeypatch, tmp_path):
    mod = _script()
    fits = []

    class FakeModels:
        def __init__(self, *a, **k):
            pass

        def fit(self, df, as_of_season=None):
            fits.append(as_of_season); return self

        def estimate_sigma(self, df, as_of_season=None):
            return {1: {"QB": 3.0, "__all__": 2.0}}

        def predict(self, rows):
            return rows.with_columns(pl.lit(1.0).alias("h1_ppg_hat"))

    monkeypatch.setattr(mod.career, "HorizonModels", FakeModels)
    monkeypatch.setattr(mod.career, "fit_survival", lambda df, cap: "survival")
    monkeypatch.setattr(mod.career, "apply_cap", lambda surv, pred, H, cap: pred)
    df = pl.DataFrame({"player_id": ["a", "b"], "season": [2025, 2026], "ppg": [1.0, 2.0]})
    t1, s1, surv, m1 = mod.career_tail(df, 2025, {}, "cpu", cache_dir=tmp_path)
    t2, s2, _, m2 = mod.career_tail(df, 2025, {}, "cpu", cache_dir=tmp_path)
    assert fits == [2025] and m1 is not None and m2 is None and surv == "survival"
    assert t2.equals(t1) and t1["player_id"].to_list() == ["a"] and s2 == s1 == {1: {"QB": 3.0, "__all__": 2.0}}
    in_progress = df.with_columns(pl.when(pl.col("season") == 2026).then(9.0).otherwise(pl.col("ppg")).alias("ppg"))
    mod.career_tail(in_progress, 2025, {}, "cpu", cache_dir=tmp_path)
    assert fits == [2025]                                                    # the in-progress season moving is not a miss
    completed = df.with_columns(pl.when(pl.col("season") == 2025).then(9.0).otherwise(pl.col("ppg")).alias("ppg"))
    mod.career_tail(completed, 2025, {}, "cpu", cache_dir=tmp_path)
    assert fits == [2025, 2025]                                              # a restated completed season is
    mod.career_tail(df, 2025, {}, "cpu", cache_dir=None)
    assert fits == [2025, 2025, 2025]                                        # no cache dir: always computed
