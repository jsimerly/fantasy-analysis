"""reprice_run: an in-season run re-priced with another age-level table, no GPU (owner, 2026-10-14)."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import reprice_run as rr  # noqa: E402


def _proj():
    base = pl.DataFrame({"player_id": ["young", "old", "mid"], "player_name": ["Y", "O", "M"], "position": ["WR", "RB", "QB"], "age_at_season": [23.0, 31.0, 27.0],
                         "vorp_ros": [50.0, 40.0, 60.0], "vorp_next": [80.0, 60.0, 90.0],
                         "h3_vorp_hat": [100.0 * 1.27, 50.0 * 0.80, 70.0 * 1.03], "h4_vorp_hat": [90.0 * 1.27, 30.0 * 0.80, 60.0 * 1.03],
                         "iv_inseason": [0.0, 0.0, 0.0], "iv_preseason": [300.0 * 1.27, 120.0 * 0.80, None], "ktc_value": [6000.0, 3000.0, 7000.0]})
    filler = pl.DataFrame({"player_id": [f"f{i}" for i in range(10)], "player_name": [f"F{i}" for i in range(10)], "position": ["WR"] * 10,
                           "age_at_season": [26.0] * 10, "vorp_ros": [10.0 * i for i in range(10)], "vorp_next": [10.0] * 10,
                           "h3_vorp_hat": [5.0 * i for i in range(10)], "h4_vorp_hat": [0.0] * 10, "iv_inseason": [0.0] * 10, "iv_preseason": [20.0] * 10,
                           "ktc_value": [1000.0 + 400.0 * i for i in range(10)]})       # the market comparison needs more than ten priced players
    return pl.concat([base, filler])


def test_the_career_spans_are_rescaled_by_new_over_old_and_the_values_resummed():
    old = {"<=24": 1.27, "25-28": 1.03, "29+": 0.80}
    new = {"<=24": 1.27, "25-28": 1.03, "29+": 0.80, "age:23": 1.19, "age:27": 1.06, "age:31": 0.89}
    out = rr.reprice(_proj(), old, new, discount_rate=0.1)
    y, o, m = (out.filter(pl.col("player_id") == p).row(0, named=True) for p in ("young", "old", "mid"))
    assert y["h3_vorp_hat"] == pytest.approx(100.0 * 1.19) and o["h3_vorp_hat"] == pytest.approx(50.0 * 0.89) and m["h4_vorp_hat"] == pytest.approx(60.0 * 1.06)
    assert y["vorp_ros"] == 50.0 and y["vorp_next"] == 80.0                                   # the in-season spans untouched
    assert y["iv_inseason"] == pytest.approx(50 + 0.9 * 80 + 0.81 * 100 * 1.19 + 0.729 * 90 * 1.19)
    assert y["iv_preseason"] == pytest.approx(300.0 * 1.19) and m["iv_preseason"] is None       # the preseason view rescaled, a null stays null
    assert {"fair_value", "mispricing", "iv_rank", "market_rank"} <= set(out.columns)          # the market comparison redone
    same = rr.reprice(_proj(), old, old, discount_rate=0.1)
    assert same["h3_vorp_hat"].to_list() == pytest.approx(_proj()["h3_vorp_hat"].to_list())    # the same table: nothing moves
    none = rr.reprice(_proj().drop("ktc_value"), None, new, discount_rate=0.1)                 # no old table: factor 1 before
    assert none.filter(pl.col("player_id") == "young")["h3_vorp_hat"][0] == pytest.approx(100.0 * 1.27 * 1.19)
