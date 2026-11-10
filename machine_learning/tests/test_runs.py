"""runs.py: the production exports refuse a run built by a non-production model unless told otherwise (review 2026-10-13)."""
import pytest

import runs


def test_a_trees_run_is_refused_and_a_production_run_passes():
    trees = {"run_date": "2026-10-13", "career_backend": "xgb stacked blend [base,career]", "inseason_backend": "xgb"}
    prod = {"run_date": "2026-10-14", "career_backend": "tabpfn stacked blend [base,career,injury,trend,situation,rookie,college]", "inseason_backend": "tabpfn"}
    with pytest.raises(SystemExit, match="career_backend='xgb stacked blend"):
        runs.check_backend(trees, "tabpfn", "build_war")
    runs.check_backend(prod, "tabpfn", "build_war")                                   # nothing raised
    runs.check_backend(trees, "any", "build_war")                                     # an explicitly non-production run
    runs.check_backend(trees, None, "build_war")


def test_an_unstamped_run_is_not_checked_and_one_bad_backend_is_enough():
    runs.check_backend({"run_date": "2026-10-01", "discount": 0.8}, "tabpfn", "export")   # older runs carry no backend keys
    with pytest.raises(SystemExit, match="inseason_backend='xgb'"):
        runs.check_backend({"career_backend": "tabpfn stacked blend", "inseason_backend": "xgb"}, "tabpfn", "export")
    assert runs.backends_of({"career_backend": "tabpfn", "other": 1}) == {"career_backend": "tabpfn"}
