"""export_projections: the page parses strict JSON, so the payload never carries NaN (sweep 2026-10-13: a veteran with a
row this season but none last season had a NaN preseason value, and JSON.parse rejects the bare token)."""
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))


def test_the_payload_never_carries_nan():
    import export_projections as ex
    assert ex._r(float("nan")) is None and ex._r(None) is None and ex._r(1.26) == 1.3 and ex._r(float("inf")) is None
    out = ex._finite({"a": [1.0, float("nan"), {"b": float("inf"), "c": "x"}], "d": (2, math.nan)})
    assert out == {"a": [1.0, None, {"b": None, "c": "x"}], "d": [2, None]}
    json.dumps(out, allow_nan=False)
