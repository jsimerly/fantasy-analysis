"""Union/dedup preference, slot assignment + partition pathing (no network)."""
import datetime as dt

from fantasypros_ingestion import _harvest as H


def test_assign_slot():
    wk = {1: dt.date(2015, 9, 13), 2: dt.date(2015, 9, 20), 3: dt.date(2015, 9, 27)}
    assert H._assign_slot(dt.date(2015, 9, 11), wk) == 1     # 2d before wk1 games -> wk1
    assert H._assign_slot(dt.date(2015, 9, 18), wk) == 2     # mid-week before wk2
    assert H._assign_slot(dt.date(2015, 8, 1), wk) is None   # preseason -> season-long
    assert H._assign_slot(dt.date(2015, 12, 1), wk) == "skip"  # after the season


def test_union_prefers_wayback_team():
    wb = [{"fp_id": "1", "player": "A", "team": "GB", "fpts": 20.0}]      # as-of (2012)
    live = [{"fp_id": "1", "player": "A", "team": "PIT", "fpts": 20.0},   # current team
            {"fp_id": "2", "player": "B", "team": "KC", "fpts": 10.0}]    # survivor-only
    out = {r["fp_id"]: r for r in H.union(wb, live)}
    assert out["1"]["team"] == "GB"   # Wayback wins (era-correct team)
    assert "2" in out                 # live-only player still kept


def test_union_keys_on_name_team_when_no_fpid():
    wb = [{"fp_id": None, "player": "Matt Ryan", "team": "ATL", "fpts": 22.0}]
    live = [{"fp_id": None, "player": "Matt Ryan", "team": "ATL", "fpts": 21.0}]
    assert len(H.union(wb, live)) == 1


def test_out_path_backfill_vs_daily():
    assert H.out_path(2013, 8).endswith("weekly/season=2013/week=08/data.parquet")
    assert H.out_path(2015, None).endswith("season/season=2015/data.parquet")
    assert "/as_of=20260907/" in H.out_path(2026, 8, "20260907")   # daily capture sub-partition
