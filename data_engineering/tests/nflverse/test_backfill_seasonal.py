"""nflverse_ingestion/backfill_seasonal.py: one data.parquet per season on the daily job's path,
existing partitions are never replaced silently, one bad season does not stop the rest."""
import polars as pl
import pytest

import nflverse_ingestion.backfill_seasonal as mod

NEVER = lambda *a: False  # noqa: E731
ALWAYS = lambda *a: True  # noqa: E731


def _cfg(frames: dict[int, pl.DataFrame], folder: str = "injuries") -> dict:
    return {"loader": lambda seasons: frames[seasons[0]], "folder": folder, "seasonal": True, "schedule": "daily"}


class TestBackfill:
    def test_writes_the_daily_partition_path_per_season(self, fake_gcs, monkeypatch):
        frames = {2009: pl.DataFrame({"season": [2009.0], "week": [1.0]}),     # old nflverse files: floats
                  2010: pl.DataFrame({"season": [2010], "week": [1]})}
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": _cfg(frames)})
        res = mod.backfill("injuries", 2009, 2010, "b", exists=NEVER)
        assert [r["success"] for r in res] == [True, True]
        assert [r["rows"] for r in res] == [1, 1]
        assert set(fake_gcs) == {"gs://b/bronze/nflverse/injuries/season=2009/data.parquet",
                                 "gs://b/bronze/nflverse/injuries/season=2010/data.parquet"}

    def test_same_path_as_the_daily_job(self):
        # SPEC: backfill and daily must share one file per season (no <name>.parquet twin)
        assert mod.season_path("b", "injuries", 2024) == "gs://b/bronze/nflverse/injuries/season=2024/data.parquet"

    def test_existing_partition_is_skipped_unless_overwrite(self, fake_gcs, monkeypatch):
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": _cfg({2024: pl.DataFrame({"x": [1]})})})
        res = mod.backfill("injuries", 2024, 2024, "b", exists=ALWAYS)
        assert res[0]["success"] is True and res[0]["skipped"] is True
        assert not fake_gcs                                              # nothing written
        res = mod.backfill("injuries", 2024, 2024, "b", overwrite=True, exists=ALWAYS)
        assert res[0]["success"] is True and res[0]["skipped"] is False
        assert len(fake_gcs) == 1

    def test_a_failing_season_does_not_stop_the_others(self, fake_gcs, monkeypatch):
        def loader(seasons):
            if seasons[0] == 2010:
                raise ValueError("Season must be between 2009 and 2026")
            return pl.DataFrame({"x": [1]})
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": {"loader": loader, "folder": "injuries", "seasonal": True}})
        res = mod.backfill("injuries", 2009, 2011, "b", exists=NEVER)
        assert [r["success"] for r in res] == [True, False, True]
        assert "between 2009 and 2026" in res[1]["error"]
        assert len(fake_gcs) == 2

    def test_empty_season_is_a_recorded_failure(self, fake_gcs, monkeypatch):
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": _cfg({2024: pl.DataFrame({"x": []})})})
        res = mod.backfill("injuries", 2024, 2024, "b", exists=NEVER)
        assert res[0]["success"] is False and res[0]["error"] == "No data returned"
        assert not fake_gcs

    def test_only_seasonal_datasets_are_accepted(self, monkeypatch):
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"nfl_players": {"loader": lambda: None, "folder": "nfl_players", "seasonal": False}})
        with pytest.raises(ValueError, match="not a seasonal dataset"):
            mod.backfill("nfl_players", 2020, 2020, "b", exists=NEVER)
        with pytest.raises(ValueError, match="not a seasonal dataset"):
            mod.backfill("no_such_dataset", 2020, 2020, "b", exists=NEVER)

    def test_reversed_range_is_rejected(self, monkeypatch):
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": _cfg({})})
        with pytest.raises(ValueError, match="after end season"):
            mod.backfill("injuries", 2020, 2019, "b", exists=NEVER)


class TestArgs:
    def test_env_vars_drive_the_cloud_run_job(self, monkeypatch):
        monkeypatch.setenv("DATASET", "injuries")
        monkeypatch.setenv("START_SEASON", "2009")
        monkeypatch.delenv("END_SEASON", raising=False)
        monkeypatch.setenv("OVERWRITE", "true")
        a = mod.parse_args([])
        assert (a.dataset, a.start_season, a.end_season, a.overwrite) == ("injuries", 2009, None, True)

    def test_cli_flags_win_over_env(self, monkeypatch):
        monkeypatch.setenv("DATASET", "injuries")
        monkeypatch.setenv("START_SEASON", "2009")
        monkeypatch.delenv("OVERWRITE", raising=False)
        a = mod.parse_args(["--dataset", "snap_counts", "--start-season", "2012", "--end-season", "2013"])
        assert (a.dataset, a.start_season, a.end_season, a.overwrite) == ("snap_counts", 2012, 2013, False)

    def test_missing_required_inputs_exit(self, monkeypatch):
        monkeypatch.delenv("DATASET", raising=False)
        monkeypatch.delenv("START_SEASON", raising=False)
        with pytest.raises(SystemExit):
            mod.parse_args([])


class TestInjuriesInDailyConfig:
    def test_injuries_is_a_daily_seasonal_dataset(self):
        from nflverse_ingestion.daily_ingestion import DATASETS_CONFIG
        cfg = DATASETS_CONFIG["injuries"]
        assert cfg["seasonal"] is True
        assert cfg["schedule"] == "daily"
        assert cfg["folder"] == "injuries"
