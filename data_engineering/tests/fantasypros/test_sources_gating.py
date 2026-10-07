"""Wayback nearest-pick + live-current in-season gating (no network)."""
import datetime as dt

from fantasypros_ingestion._sources import pick_nearest
from fantasypros_ingestion import live_current as L


class TestPickNearest:
    def test_latest_on_or_before_target(self):
        snaps = [("20230905120000", "u"), ("20230912120000", "u"), ("20230920120000", "u")]
        assert pick_nearest(snaps, "20230913")[0] == "20230912120000"   # pre-game

    def test_fallback_earliest_after(self):
        assert pick_nearest([("20231001120000", "u")], "20230913")[0] == "20231001120000"

    def test_empty(self):
        assert pick_nearest([], "20230913") is None


class TestSeasonGating:
    GD = {(2025, 1): "20250904", (2025, 18): "20260104"}

    def test_in_season(self):
        assert L.season_window(self.GD, dt.date(2025, 10, 1)) == 2025

    def test_offseason_returns_none(self):
        assert L.season_window(self.GD, dt.date(2025, 7, 1)) is None   # before preseason window

    def test_current_week(self):
        assert L.current_week(self.GD, 2025, dt.date(2025, 9, 5)) == 1
        assert L.current_week(self.GD, 2025, dt.date(2025, 8, 1)) is None  # preseason, no week yet
