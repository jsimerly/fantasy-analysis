"""age_gate: realized / priced by age band on the fixed top set (review 2026-10-13, C1)."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import age_gate as ag  # noqa: E402


def _frame(scale_young: float, scale_old: float, cohorts=(2020, 2021)) -> pl.DataFrame:
    rows = []
    for T in cohorts:
        for i in range(40):
            age = 23.0 if i % 2 == 0 else 31.0
            war = 2.0 - i * 0.04
            real = war * (scale_young if age < 25 else scale_old)
            rows.append({"cohort": T, "player_id": f"p{i}", "age_at_season": age, "position": ["QB", "RB", "WR", "TE"][i % 4], "war": war, "realized_war": real})
    return pl.DataFrame(rows)


def test_level_by_band_and_the_comparison_with_the_cohort_as_the_unit():
    a = _frame(1.2, 0.9)                       # the production shape: young under-priced, old over-priced
    b = _frame(1.0, 1.0)                       # a level candidate
    t = ag.age_level({"a": a, "b": b}, top=20)
    ra = t.filter(pl.col("run") == "a").row(0, named=True)
    assert abs(ra["ratio_<=24"] - 1.2) < 1e-9 and abs(ra["ratio_29+"] - 0.9) < 1e-9 and ra["ratio_25-28"] is None and ra["n"] == 20
    assert abs(ra["ratio_QB"] - 1.2) < 1e-9 and abs(ra["ratio_RB"] - 0.9) < 1e-9      # QBs are the young ones in this frame
    c = {r["band"]: r for r in ag.compare(t, "b", "a").to_dicts()}
    assert abs(c["<=24"]["a_minus_b"] + 0.2) < 1e-9 and c["<=24"]["a_closer_to_1"] == 2 and c["<=24"]["cohorts"] == 2
    assert c["29+"]["a_closer_to_1"] == 2 and c["all"]["a_closer_to_1"] == 2


def test_the_set_is_the_union_of_both_runs_top_lists():
    a = _frame(1.0, 1.0, cohorts=(2020,))
    b = a.with_columns((pl.col("war") * pl.when(pl.col("player_id") == "p39").then(10.0).otherwise(1.0)).alias("war"))   # b likes the last player
    t = ag.age_level({"a": a, "b": b}, top=5)
    assert t.filter(pl.col("run") == "a")["n"][0] == 6 and t.filter(pl.col("run") == "b")["n"][0] == 6   # 5 + the one b added
