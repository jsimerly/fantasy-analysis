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
    assert out.groups == "base,career,injury,trend,situation,rookie,college" and out.device == "cuda"
    b = m.apply_preset(argparse.Namespace(preset=None, backend="xgb", stacked=False))
    assert b.backend == "xgb" and b.stacked is False and set(m.PRESETS) == {"production"}
