"""nflverse_ingestion/daily_ingestion.py: schedule gating + per-dataset save."""
import sys
import types

import polars as pl
import pytest


import nflverse_ingestion.daily_ingestion as mod
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


class TestRegistry:
    def test_contracts_is_a_weekly_snapshot(self):
        cfg = mod.DATASETS_CONFIG["contracts"]
        assert cfg["seasonal"] is False and cfg["schedule"] == mod.SCHEDULE_TUESDAY and cfg["folder"] == "contracts"

    def test_the_tuesday_snapshots_are_the_five_the_quality_check_watches(self):
        tuesday = [cfg["folder"] for cfg in mod.DATASETS_CONFIG.values() if cfg["schedule"] == mod.SCHEDULE_TUESDAY]
        assert tuesday == ["fantasy_rankings", "fantasy_rankings_history", "fantasy_player_ids", "nfl_players", "contracts"]


class TestNflreadpyCache:
    def test_the_job_turns_the_in_memory_cache_off_before_loading(self, monkeypatch):
        # nflreadpy's default cache kept every frame of the run alive: the Tuesday OOM at 1Gi (2026-10-13/20/27, contracts in flight)
        calls = []
        fake = types.ModuleType("nflreadpy.config")
        fake.update_config = lambda **kw: calls.append(kw)
        monkeypatch.setitem(sys.modules, "nflreadpy.config", fake)
        mod.disable_nflreadpy_cache()
        assert calls == [{"cache_mode": "off"}]


class TestNextGenSlices:
    def test_receiving_and_rushing_are_their_own_seasonal_datasets(self):
        # the loader's default slice is passing (quarterbacks); receivers and rushers need their own calls
        for slice_ in ("receiving", "rushing"):
            cfg = mod.DATASETS_CONFIG[f"nextgen_stats_{slice_}"]
            assert cfg["loader"].keywords == {"stat_type": slice_}
            assert cfg["seasonal"] is True and cfg["start_season"] == 2016 and cfg["folder"] == f"nextgen_stats_{slice_}"
        assert "stat_type" not in getattr(mod.DATASETS_CONFIG["nextgen_stats"]["loader"], "keywords", {})

    def test_expected_points_is_a_seasonal_dataset_from_2006(self):
        cfg = mod.DATASETS_CONFIG["ff_opportunity"]
        assert cfg["loader"].keywords == {"stat_type": "weekly"} and cfg["seasonal"] is True and cfg["start_season"] == 2006

    def test_rankings_history_is_the_all_file_refreshed_weekly(self):
        cfg = mod.DATASETS_CONFIG["ff_rankings_history"]
        assert cfg["loader"].args == ("all",) and cfg["seasonal"] is False and cfg["schedule"] == mod.SCHEDULE_TUESDAY

