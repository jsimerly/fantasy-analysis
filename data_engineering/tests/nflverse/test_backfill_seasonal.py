"""nflverse_ingestion/backfill_seasonal.py: scheduled reconcile fills only missing history seasons;
explicit backfill writes one data.parquet per season on the daily job's path and never replaces
an existing partition silently; one bad season does not stop the rest."""
import polars as pl
import pytest

import nflverse_ingestion.backfill_seasonal as mod


def _cfg(frames: dict[int, pl.DataFrame], folder: str = "injuries", start_season: int | None = 2009) -> dict:
    cfg = {"loader": lambda seasons: frames[seasons[0]], "folder": folder, "seasonal": True, "schedule": "daily"}
    if start_season is not None:
        cfg["start_season"] = start_season
    return cfg


def _existing(by_folder: dict[str, set[int]]):
    return lambda bucket, folder: set(by_folder.get(folder, set()))


class TestReconcile:
    def test_loads_only_missing_seasons_below_the_current_one(self, fake_gcs, monkeypatch):
        frames = {s: pl.DataFrame({"x": [s]}) for s in range(2009, 2027)}
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {
            "injuries": _cfg(frames, "injuries", 2009),
            "snap_counts": _cfg(frames, "snap_counts", 2012),
            "nfl_players": {"loader": lambda: None, "folder": "nfl_players", "seasonal": False},
        })
        have = {"injuries": set(range(2009, 2026)) - {2015, 2020}, "snap_counts": set(range(2012, 2026))}
        res = mod.reconcile("b", current_season=2026, existing=_existing(have))
        assert sorted((r["name"], r["season"]) for r in res) == [("injuries", 2015), ("injuries", 2020)]
        assert all(r["success"] for r in res)
        assert set(fake_gcs) == {"gs://b/bronze/nflverse/injuries/season=2015/data.parquet",
                                 "gs://b/bronze/nflverse/injuries/season=2020/data.parquet"}

    def test_current_season_belongs_to_the_daily_job(self, fake_gcs, monkeypatch):
        frames = {s: pl.DataFrame({"x": [s]}) for s in range(2009, 2027)}
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": _cfg(frames, "injuries", 2009)})
        res = mod.reconcile("b", current_season=2026, existing=_existing({"injuries": set(range(2009, 2026))}))
        assert res == [] and not fake_gcs                                  # 2026 is not "missing"

    def test_dataset_without_start_season_is_left_alone(self, fake_gcs, monkeypatch):
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"officials": _cfg({}, "officials", None)})
        assert mod.reconcile("b", current_season=2026, existing=_existing({})) == []
        assert not fake_gcs

    def test_dry_run_reports_gaps_and_writes_nothing(self, fake_gcs, monkeypatch):
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": _cfg({}, "injuries", 2024)})
        res = mod.reconcile("b", current_season=2026, dry_run=True, existing=_existing({"injuries": {2024}}))
        assert [(r["season"], r["dry_run"]) for r in res] == [(2025, True)]
        assert not fake_gcs

    def test_a_failing_season_does_not_stop_the_others(self, fake_gcs, monkeypatch):
        def loader(seasons):
            if seasons[0] == 2010:
                raise ValueError("Season must be between 2009 and 2026")
            return pl.DataFrame({"x": [1]})
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": {"loader": loader, "folder": "injuries", "seasonal": True, "start_season": 2009}})
        res = mod.reconcile("b", current_season=2012, existing=_existing({}))
        assert [(r["season"], r["success"]) for r in res] == [(2009, True), (2010, False), (2011, True)]
        assert "between 2009 and 2026" in res[1]["error"]
        assert len(fake_gcs) == 2

    def test_empty_upstream_season_is_reported_not_failed(self, fake_gcs, monkeypatch):
        # nflverse serves an empty file for some early seasons (snap_counts 2012): a scheduled
        # reconcile must not fail the DAG every morning over it
        frames = {2012: pl.DataFrame({"x": []}), 2013: pl.DataFrame({"x": [1]})}
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"snap_counts": _cfg(frames, "snap_counts", 2012)})
        res = mod.reconcile("b", current_season=2014, existing=_existing({}))
        assert [(r["season"], r["success"], r.get("empty", False)) for r in res] == [(2012, False, True), (2013, True, False)]
        assert set(fake_gcs) == {"gs://b/bronze/nflverse/snap_counts/season=2013/data.parquet"}


class TestExplicitBackfill:
    def test_writes_the_daily_partition_path_per_season(self, fake_gcs, monkeypatch):
        frames = {2009: pl.DataFrame({"season": [2009.0], "week": [1.0]}),     # old nflverse files: floats
                  2010: pl.DataFrame({"season": [2010], "week": [1]})}
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": _cfg(frames)})
        res = mod.backfill("injuries", 2009, 2010, "b", existing=_existing({}))
        assert [r["success"] for r in res] == [True, True]
        assert [r["rows"] for r in res] == [1, 1]
        assert set(fake_gcs) == {"gs://b/bronze/nflverse/injuries/season=2009/data.parquet",
                                 "gs://b/bronze/nflverse/injuries/season=2010/data.parquet"}

    def test_same_path_as_the_daily_job(self):
        # SPEC: backfill and daily must share one file per season (no <name>.parquet twin)
        assert mod.season_path("b", "injuries", 2024) == "gs://b/bronze/nflverse/injuries/season=2024/data.parquet"

    def test_existing_partition_is_skipped_unless_overwrite(self, fake_gcs, monkeypatch):
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": _cfg({2024: pl.DataFrame({"x": [1]})})})
        res = mod.backfill("injuries", 2024, 2024, "b", existing=_existing({"injuries": {2024}}))
        assert res[0]["success"] is True and res[0]["skipped"] is True
        assert not fake_gcs                                              # nothing written
        res = mod.backfill("injuries", 2024, 2024, "b", overwrite=True, existing=_existing({"injuries": {2024}}))
        assert res[0]["success"] is True and res[0]["skipped"] is False
        assert len(fake_gcs) == 1

    def test_empty_season_is_a_recorded_failure(self, fake_gcs, monkeypatch):
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": _cfg({2024: pl.DataFrame({"x": []})})})
        res = mod.backfill("injuries", 2024, 2024, "b", existing=_existing({}))
        assert res[0]["success"] is False and res[0]["error"] == "No data returned"
        assert not fake_gcs

    def test_only_seasonal_datasets_are_accepted(self, monkeypatch):
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"nfl_players": {"loader": lambda: None, "folder": "nfl_players", "seasonal": False}})
        with pytest.raises(ValueError, match="not a seasonal dataset"):
            mod.backfill("nfl_players", 2020, 2020, "b", existing=_existing({}))
        with pytest.raises(ValueError, match="not a seasonal dataset"):
            mod.backfill("no_such_dataset", 2020, 2020, "b", existing=_existing({}))

    def test_reversed_range_is_rejected(self, monkeypatch):
        monkeypatch.setattr(mod, "DATASETS_CONFIG", {"injuries": _cfg({})})
        with pytest.raises(ValueError, match="after end season"):
            mod.backfill("injuries", 2020, 2019, "b", existing=_existing({}))


class TestExistingSeasons:
    def test_any_object_under_the_season_prefix_counts(self, monkeypatch):
        # a full-load <name>.parquet twin must count as "exists" so we never add a second file
        class Blob:
            def __init__(self, name): self.name = name
        class Client:
            def list_blobs(self, bucket, prefix):
                assert prefix == "bronze/nflverse/player_stats/season="
                return [Blob("bronze/nflverse/player_stats/season=2024/player_stats.parquet"),
                        Blob("bronze/nflverse/player_stats/season=2025/data.parquet"),
                        Blob("bronze/nflverse/player_stats/season=2025/player_stats.parquet"),
                        Blob("bronze/nflverse/player_stats/season=None/player_stats.parquet")]
        import sys, types
        gcs = types.SimpleNamespace(Client=lambda: Client())
        monkeypatch.setitem(sys.modules, "google.cloud.storage", gcs)
        google_cloud = types.ModuleType("google.cloud"); google_cloud.storage = gcs
        monkeypatch.setitem(sys.modules, "google.cloud", google_cloud)
        assert mod.existing_seasons("b", "player_stats") == {2024, 2025}


class TestArgs:
    def test_no_args_means_reconcile(self, monkeypatch):
        for k in ("DATASET", "START_SEASON", "END_SEASON", "OVERWRITE", "DRY_RUN"):
            monkeypatch.delenv(k, raising=False)
        a = mod.parse_args([])
        assert a.dataset is None and a.dry_run is False and a.overwrite is False

    def test_env_vars_drive_the_cloud_run_job(self, monkeypatch):
        monkeypatch.setenv("DATASET", "injuries")
        monkeypatch.setenv("START_SEASON", "2009")
        monkeypatch.delenv("END_SEASON", raising=False)
        monkeypatch.setenv("OVERWRITE", "true")
        monkeypatch.setenv("DRY_RUN", "true")
        a = mod.parse_args([])
        assert (a.dataset, a.start_season, a.end_season, a.overwrite, a.dry_run) == ("injuries", 2009, None, True, True)

    def test_cli_flags_win_over_env(self, monkeypatch):
        monkeypatch.setenv("DATASET", "injuries")
        monkeypatch.setenv("START_SEASON", "2009")
        monkeypatch.delenv("OVERWRITE", raising=False)
        a = mod.parse_args(["--dataset", "snap_counts", "--start-season", "2012", "--end-season", "2013"])
        assert (a.dataset, a.start_season, a.end_season, a.overwrite) == ("snap_counts", 2012, 2013, False)

    def test_dataset_without_start_season_exits(self, monkeypatch):
        monkeypatch.delenv("START_SEASON", raising=False)
        with pytest.raises(SystemExit):
            mod.parse_args(["--dataset", "injuries"])

    def test_range_flags_without_dataset_exit(self, monkeypatch):
        monkeypatch.delenv("DATASET", raising=False)
        with pytest.raises(SystemExit):
            mod.parse_args(["--start-season", "2009"])


class TestDailyConfigSpec:
    def test_injuries_is_a_daily_seasonal_dataset(self):
        from nflverse_ingestion.daily_ingestion import DATASETS_CONFIG
        cfg = DATASETS_CONFIG["injuries"]
        assert cfg["seasonal"] is True
        assert cfg["schedule"] == "daily"
        assert cfg["folder"] == "injuries"
        assert cfg["start_season"] == 2009

    def test_every_seasonal_dataset_declares_where_its_history_starts(self):
        # SPEC: reconcile can only heal datasets that say which seasons nflverse serves
        from nflverse_ingestion.daily_ingestion import DATASETS_CONFIG
        for name, cfg in DATASETS_CONFIG.items():
            if cfg.get("seasonal"):
                assert isinstance(cfg.get("start_season"), int) and 1999 <= cfg["start_season"] <= 2026, name
