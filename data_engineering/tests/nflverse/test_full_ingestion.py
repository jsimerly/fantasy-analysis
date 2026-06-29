"""nflverse_ingestion/full_ingestion.py: backfill save helpers.

The full (backfill) loader writes under bronze/nfl/..., but the daily loader and
the silver layer both use bronze/nflverse/... -- so a full backfill lands where
nothing reads it. The path-root spec test documents the intended shared root.
"""
import polars as pl


import nflverse_ingestion.full_ingestion as mod
fetch_and_save_seasonal_dataset = mod.fetch_and_save_seasonal_dataset
fetch_and_save_non_seasonal_dataset = mod.fetch_and_save_non_seasonal_dataset


class TestSeasonalBackfill:
    def test_writes_one_partition_per_season(self, fake_gcs):
        df = pl.DataFrame({"season": [1999, 2000, 2000], "x": [1, 2, 3]})
        res = fetch_and_save_seasonal_dataset(
            "player_stats", lambda seasons: df, 1999, 2000, "player_stats", "b"
        )
        assert res["success"] is True
        assert res["seasons_loaded"] == 2          # 1999 + 2000
        assert len([k for k in fake_gcs if "season=" in k]) == 2

    def test_writes_to_shared_nflverse_root(self, fake_gcs):
        # SPEC: backfill must land under the same bronze/nflverse/ root that the
        # daily loader and silver layer read from (currently writes bronze/nfl/).
        df = pl.DataFrame({"season": [2024], "x": [1]})
        fetch_and_save_seasonal_dataset("player_stats", lambda s: df, 2024, 2024, "player_stats", "b")
        assert all(k.startswith("gs://b/bronze/nflverse/") for k in fake_gcs)


class TestNonSeasonalBackfill:
    def test_writes_load_date_partition(self, fake_gcs):
        df = pl.DataFrame({"x": [1, 2]})
        res = fetch_and_save_non_seasonal_dataset("players", lambda: df, "players", "b")
        assert res["success"] is True
        assert res["rows"] == 2
