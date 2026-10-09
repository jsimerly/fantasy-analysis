"""fantasy_lib: the SF value blend falls back to Standard per day and player, never per era."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fantasy_lib as F  # noqa: E402


def _vals(rows):
    return pl.DataFrame(rows, schema={"valuation_date": pl.Date, "player_id": pl.Utf8, "name": pl.Utf8, "position": pl.Utf8, "ktc_value": pl.Int64, "fc_value": pl.Int64}, orient="row")


def test_blend_takes_tep_where_it_exists_and_standard_where_the_tep_series_has_a_hole():
    std = _vals([(date(2026, 9, 7), "te1", "T", "TE", 5000, None), (date(2026, 9, 14), "te1", "T", "TE", 5100, None), (date(2026, 9, 21), "te1", "T", "TE", 5200, None),
                 (date(2026, 9, 7), "wr1", "W", "WR", 4000, None), (date(2026, 9, 14), "wr1", "W", "WR", 4000, None), (date(2026, 9, 21), "wr1", "W", "WR", 4000, None)])
    tep = _vals([(date(2026, 9, 7), "te1", "T", "TE", 6000, None), (date(2026, 9, 21), "te1", "T", "TE", 6200, None),      # 09-14 missing: the outage week
                 (date(2026, 9, 7), "wr1", "W", "WR", 4000, None), (date(2026, 9, 21), "wr1", "W", "WR", 4000, None)])
    out = F.blend_values(std, tep)
    v = {(r["valuation_date"], r["player_id"]): r["ktc_value"] for r in out.to_dicts()}
    assert v[(date(2026, 9, 7), "te1")] == 6000 and v[(date(2026, 9, 21), "te1")] == 6200     # TEP where published
    assert v[(date(2026, 9, 14), "te1")] == 5100 and v[(date(2026, 9, 14), "wr1")] == 4000    # Standard fills the hole
    assert out.height == 6 and out.columns == std.columns


def test_blend_keeps_tep_only_rows_and_the_pre_tep_era():
    std = _vals([(date(2025, 1, 1), "a", "A", "RB", 1000, None), (date(2025, 11, 1), "a", "A", "RB", 1100, None)])
    tep = _vals([(date(2025, 11, 1), "a", "A", "RB", 1150, None), (date(2025, 11, 1), "b", "B", "TE", 900, None)])
    out = F.blend_values(std, tep)
    v = {(r["valuation_date"], r["player_id"]): r["ktc_value"] for r in out.to_dicts()}
    assert v[(date(2025, 1, 1), "a")] == 1000 and v[(date(2025, 11, 1), "a")] == 1150 and v[(date(2025, 11, 1), "b")] == 900
    assert F.blend_values(std, tep.clear()).equals(std)
