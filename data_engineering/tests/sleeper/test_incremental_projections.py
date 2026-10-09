"""sleeper_ingestion/daily/incremental_projections.py: the flattener's shape, the season resolver, the
whole-season write."""
from datetime import datetime

import polars as pl

import sleeper_ingestion.daily.incremental_projections as mod


def _payload():
    return [
        {"season": "2025", "week": 5, "player_id": "4984", "date": "2025-10-05", "company": "rotowire", "updated_at": 1759000000000, "team": "BUF", "opponent": "NE",
         "player": {"position": "QB", "team": "BUF"}, "stats": {"pts_ppr": 22.4, "pts_half_ppr": 22.4, "pts_std": 22.4, "pass_yd": 265.0, "pass_td": 1.9}},
        {"season": "2025", "week": 5, "player_id": "9509", "team": "ATL", "player": {"position": "RB"}, "stats": {"pts_ppr": 18.1, "rec": 4.2}},   # no date, no company
        {"season": "2025", "week": 5, "player": {"position": "WR"}, "stats": {"pts_ppr": 1.0}},                                                   # no id: dropped
    ]


class TestFlattenProjections:
    def test_one_row_per_record_with_the_stat_line_and_the_schema_pinned(self):
        df = mod.flatten_projections(_payload(), 2025, 5, loaded_at=datetime(2025, 10, 1))
        assert df.height == 2 and df.columns == list(mod.SCHEMA)
        allen = df.filter(pl.col("player_id") == "4984").to_dicts()[0]
        assert allen["position"] == "QB" and allen["opponent"] == "NE" and allen["pts_ppr"] == 22.4 and allen["pass_td"] == 1.9 and allen["rec"] is None
        bijan = df.filter(pl.col("player_id") == "9509").to_dicts()[0]
        assert bijan["team"] == "ATL" and bijan["game_date"] is None and bijan["rec"] == 4.2
        assert df.schema["pts_ppr"] == pl.Float64 and df.schema["updated_at"] == pl.Int64 and df.schema["week"] == pl.Int64

    def test_empty_payload_is_a_typed_empty_frame_and_weeks_concat(self):
        empty = mod.flatten_projections([], 2025, 7)
        assert empty.height == 0 and empty.columns == list(mod.SCHEMA)
        both = pl.concat([mod.flatten_projections(_payload(), 2025, 5), empty], how="vertical")
        assert both.height == 2


class TestSeasons:
    def test_current_is_the_season_in_progress_march_to_february(self):
        assert mod.resolve_seasons("current", datetime(2026, 10, 9)) == [2026]
        assert mod.resolve_seasons(None, datetime(2027, 2, 1)) == [2026]
        assert mod.resolve_seasons("2018-2020") == [2018, 2019, 2020] and mod.resolve_seasons("2021, 2024") == [2021, 2024]


class TestWrite:
    def test_whole_season_lands_in_one_partition(self, monkeypatch, fake_gcs):
        df = mod.flatten_projections(_payload(), 2025, 5)
        path = mod.save_season(df, "gs://test-bucket", 2025)
        assert path == "gs://test-bucket/bronze/sleeper/projections/season=2025/data.parquet" and fake_gcs[path].height == 2
