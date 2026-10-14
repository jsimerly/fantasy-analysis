"""weekly_refresh: the production preset expands to the adopted configuration and leaves the defaults alone otherwise."""
import argparse
import importlib.util
from pathlib import Path


def _mod():
    p = Path(__file__).resolve().parents[1] / "scripts" / "weekly_refresh.py"
    spec = importlib.util.spec_from_file_location("weekly_refresh", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_production_preset_sets_the_adopted_configuration():
    m = _mod()
    a = argparse.Namespace(preset="production", backend="xgb", groups="base,career", stacked=False, range=False, inseason_backend="xgb", device="cuda", target="level")
    out = m.apply_preset(a)
    assert out.backend == "tabpfn" and out.stacked is True and out.range is True and out.inseason_backend == "tabpfn"
    assert a.target == "blend"                                   # the level + residual blend (BACKLOG 44)
    assert a.sigma == "band"                                     # each player's own band prices his upside (BACKLOG 45)
    assert a.discount_rate == 0.1                                # the page's dynasty default (owner, 2026-10-12)
    assert a.age_scale == "tabpfn35_set_stacked_blend_w2"       # the age-band level correction on the career spans (review C1; owner, 2026-10-14)
    assert out.groups == "base,career,injury,trend,situation,rookie,college" and out.device == "cuda"
    b = m.apply_preset(argparse.Namespace(preset=None, backend="xgb", stacked=False))
    assert b.backend == "xgb" and b.stacked is False and set(m.PRESETS) == {"production"}


def test_the_projection_step_writes_under_the_refresh_run_date(monkeypatch):
    """A refresh that crosses UTC midnight (8 p.m. Eastern) used to stamp the in-season run with the UTC date
    at its end while WAR and the export read the refresh's own date; the date is now passed down."""
    import sys
    m = _mod()
    seen = []
    monkeypatch.setattr(m, "run", lambda cmd, dry, cwd=None, best_effort=False: seen.append(cmd) or True)
    monkeypatch.setattr(m, "current_season_week", lambda: (2026, 6))
    monkeypatch.setattr(m.power, "keep_awake", lambda *a, **k: True)
    monkeypatch.setattr(sys, "argv", ["weekly_refresh.py", "--dry-run", "--run-date", "2026-10-11", "--skip", "analysis", "export"])
    m.main()
    ins = next(c for c in seen if any("backtest_inseason.py" in str(x) for x in c))
    assert ins[ins.index("--run-date") + 1] == "2026-10-11"
    war = next(c for c in seen if any("build_war.py" in str(x) for x in c))
    assert war[war.index("--run-date") + 1] == "2026-10-11"
    # review 2026-10-13: a refresh on the trees (the defaults: the cloud job, a smoke) tells the exports it is not production
    assert war[war.index("--require-backend") + 1] == "any"


def test_a_production_refresh_lets_the_exports_require_the_production_backend(monkeypatch):
    """review 2026-10-13 (D1-D3): the exports refuse a run whose models are not the production ones; a tabpfn refresh
    passes no override, so a trees run under its run date would be refused."""
    import sys
    m = _mod()
    seen = []
    monkeypatch.setattr(m, "run", lambda cmd, dry, cwd=None, best_effort=False: seen.append(cmd) or True)
    monkeypatch.setattr(m, "current_season_week", lambda: (2026, 6))
    monkeypatch.setattr(m.power, "keep_awake", lambda *a, **k: True)
    monkeypatch.setattr(sys, "argv", ["weekly_refresh.py", "--dry-run", "--run-date", "2026-10-14", "--backend", "tabpfn", "--inseason-backend", "tabpfn",
                                      "--age-scale", "some_run", "--skip", "analysis", "export"])
    m.main()
    war = next(c for c in seen if any("build_war.py" in str(x) for x in c))
    assert "--require-backend" not in war
    ins = next(c for c in seen if any("backtest_inseason.py" in str(x) for x in c))
    assert ins[ins.index("--age-scale") + 1] == "some_run"                       # the age-band correction reaches the in-season run


def test_no_career_todate_overrides_the_preset(monkeypatch):
    """2026-10-14: the to-date career tail computed for ten hours; the fallback runs the preset off the last complete season."""
    import sys
    m = _mod()
    seen = []
    monkeypatch.setattr(m, "run", lambda cmd, dry, cwd=None, best_effort=False: seen.append(cmd) or True)
    monkeypatch.setattr(m, "current_season_week", lambda: (2026, 6))
    monkeypatch.setattr(m.power, "keep_awake", lambda *a, **k: True)
    monkeypatch.setattr(sys, "argv", ["weekly_refresh.py", "--dry-run", "--preset", "production", "--skip", "war", "analysis", "export"])
    m.main()
    ins = next(c for c in seen if any("backtest_inseason.py" in str(x) for x in c))
    assert "--career-todate" in ins and "--age-scale" in ins
    seen.clear()
    monkeypatch.setattr(sys, "argv", ["weekly_refresh.py", "--dry-run", "--preset", "production", "--no-career-todate", "--skip", "war", "analysis", "export"])
    m.main()
    ins = next(c for c in seen if any("backtest_inseason.py" in str(x) for x in c))
    assert "--career-todate" not in ins and "--age-scale" in ins                 # everything else of the preset stays


def test_the_export_shows_the_pinned_market_run_and_never_market_trends(monkeypatch):
    import importlib.util, sys
    m = _mod()
    seen = []
    monkeypatch.setattr(m, "run", lambda cmd, dry, cwd=None, best_effort=False: seen.append(cmd) or True)
    monkeypatch.setattr(m, "current_season_week", lambda: (2026, 6))
    monkeypatch.setattr(m.power, "keep_awake", lambda *a, **k: True)
    monkeypatch.setattr(sys, "argv", ["weekly_refresh.py", "--dry-run", "--preset", "production", "--skip", "inseason", "war", "analysis"])
    m.main()
    exp = next(c for c in seen if any("export_projections.py" in str(x) for x in c))
    assert exp[exp.index("--market-run-date") + 1] == m.PRESETS["production"]["market_run_date"]
    p = Path(__file__).resolve().parents[1] / "scripts" / "export_projections.py"
    spec = importlib.util.spec_from_file_location("export_projections", p)
    ex = importlib.util.module_from_spec(spec); spec.loader.exec_module(ex)
    paths = ["backtests/market/run_date=2026-10-07/summary.json", "backtests/market/run_date=2026-10-09/summary.json",
             "backtests/market_trends/run_date=2026-10-10/summary.json", "backtests/market/run_date=2026-10-09/players.parquet"]
    assert ex.newest_summary(paths, "market") == "backtests/market/run_date=2026-10-09/summary.json"
    assert ex.newest_summary(paths, "market_trends") == "backtests/market_trends/run_date=2026-10-10/summary.json"
    assert ex.newest_summary(paths, "value") is None


def test_the_refresh_reruns_the_title_odds_before_the_export(monkeypatch):
    import sys
    m = _mod()
    seen = []
    monkeypatch.setattr(m, "run", lambda cmd, dry, cwd=None, best_effort=False: seen.append((cmd, best_effort)) or True)
    monkeypatch.setattr(m, "current_season_week", lambda: (2026, 6))
    monkeypatch.setattr(m.power, "keep_awake", lambda *a, **k: True)
    monkeypatch.setattr(sys, "argv", ["weekly_refresh.py", "--dry-run", "--preset", "production", "--skip", "inseason", "war"])
    m.main()
    names = [" ".join(str(x) for x in c) for c, _ in seen]
    t = next(i for i, n in enumerate(names) if "title_odds.py" in n)
    e = next(i for i, n in enumerate(names) if "export_projections.py" in n)
    assert t < e and "--publish" in names[t] and seen[t][1] is True       # before the export, published, best-effort
