"""nflverse_ingestion/daily_ingestion.py: schedule gating + per-dataset save."""
import polars as pl
import pytest

from tests.de_loader import load_de_module

mod = load_de_module("nflverse_ingestion/daily_ingestion.py", "nflverse_ingestion", "nflverse_daily")
should_run_dataset = mod.should_run_dataset
fetch_and_save_dataset = mod.fetch_and_save_dataset

DAILY = {"schedule": "daily"}
TUESDAY = {"schedule": "Tuesday"}


class TestShouldRunDataset:
    def test_daily_always_runs(self):
        assert should_run_dataset("x", DAILY, "Friday", force_run=False) is True

    def test_weekly_runs_only_on_its_day(self):
        assert should_run_dataset("x", TUESDAY, "Tuesday", force_run=False) is True
        assert should_run_dataset("x", TUESDAY, "Wednesday", force_run=False) is False

    def test_force_run_overrides_schedule(self):
        assert should_run_dataset("x", TUESDAY, "Wednesday", force_run=True) is True


class TestFetchAndSaveDataset:
    def test_seasonal_dataset_written_to_season_partition(self, fake_gcs):
        cfg = {"loader": lambda seasons: pl.DataFrame({"a": [1, 2]}),
               "folder": "player_stats", "seasonal": True, "schedule": "daily"}
        res = fetch_and_save_dataset("player_stats", cfg, current_season=2024, bucket_name="b")
        assert res["success"] is True
        assert res["rows"] == 2
        assert "gs://b/bronze/nflverse/player_stats/season=2024/data.parquet" in fake_gcs

    def test_non_seasonal_uses_load_date_partition(self, fake_gcs):
        cfg = {"loader": lambda: pl.DataFrame({"a": [1]}),
               "folder": "fantasy_player_ids", "seasonal": False, "schedule": "Tuesday"}
        res = fetch_and_save_dataset("ff_player_ids", cfg, current_season=2024, bucket_name="b")
        assert res["success"] is True
        key = next(iter(fake_gcs))
        assert key.startswith("gs://b/bronze/nflverse/fantasy_player_ids/load_date=")

    def test_empty_dataframe_reports_no_data(self, fake_gcs):
        cfg = {"loader": lambda seasons: pl.DataFrame({"a": []}),
               "folder": "x", "seasonal": True, "schedule": "daily"}
        res = fetch_and_save_dataset("x", cfg, current_season=2024, bucket_name="b")
        assert res["success"] is False
        assert res["error"] == "No data returned"
