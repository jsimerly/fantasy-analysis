"""silver_fantasy/dim_leagues_meta.py"""
import polars as pl
import pytest

from tests.de_loader import load_de_module

mod = load_de_module("silver_fantasy/dim_leagues_meta.py", "silver_fantasy", "dim_leagues_meta")
transform_dim_leagues_meta = mod.transform_dim_leagues_meta


@pytest.fixture
def seeded(monkeypatch, fake_gcs):
    monkeypatch.setenv("GCS_BUCKET_NAME", "test-bucket")

    def fake_path(bucket, entity_path, source="sleeper"):
        return f"path::{entity_path}"
    monkeypatch.setattr(mod, "get_latest_bronze_path", fake_path)

    def fake_latest(bucket, entity_path, join_key="league_id", source="sleeper", columns=None, **kw):
        df = fake_gcs[f"path::{entity_path}"]
        return df.select([c for c in columns if c in df.columns]) if columns else df
    monkeypatch.setattr(mod, "read_latest_incremental_by_key", fake_latest)

    leagues = pl.DataFrame({
        "league_id": ["L1", "L2"],
        "league_lineage_id": ["L1", "ROOT"],
        "league_name": ["Alpha", "Beta"],
        "season": ["2024", "2024"],
        "status": ["in_season", "complete"],
        "season_type": ["regular", "regular"],
        "total_rosters": [12, 12],
        "draft_id": ["D1", "D2"],
        "bracket_id": ["B1", "B2"],
        "previous_league_id": [None, "PREV"],
    })
    settings = pl.DataFrame({
        "league_id": ["L1", "L2"],
        "league_lineage_id": ["L1", "ROOT"],
        "leg": [3, 18],
        "last_scored_leg": [2, 17],
    })
    for ep in ("league/leagues/full_load", "league/leagues/incremental"):
        fake_gcs[f"path::{ep}"] = leagues
    for ep in ("league/settings/full_load", "league/settings/incremental"):
        fake_gcs[f"path::{ep}"] = settings
    return fake_gcs


class TestDimLeaguesMeta:
    def test_is_original_flag(self, seeded):
        df = transform_dim_leagues_meta()
        by_id = {r["league_id"]: r for r in df.to_dicts()}
        assert by_id["L1"]["is_original"] is True    # league_id == lineage_id
        assert by_id["L2"]["is_original"] is False

    def test_is_active_flag(self, seeded):
        df = transform_dim_leagues_meta()
        by_id = {r["league_id"]: r for r in df.to_dicts()}
        assert by_id["L1"]["is_active"] is True       # in_season
        assert by_id["L2"]["is_active"] is False       # complete

    def test_status_columns_joined_from_settings(self, seeded):
        df = transform_dim_leagues_meta()
        by_id = {r["league_id"]: r for r in df.to_dicts()}
        assert by_id["L1"]["leg"] == 3
        assert by_id["L1"]["last_scored_leg"] == 2

    def test_has_source_and_loaded_at(self, seeded):
        df = transform_dim_leagues_meta()
        assert set(["source_system", "loaded_at"]).issubset(df.columns)
        assert df["source_system"].unique().to_list() == ["sleeper"]


class TestStatusFromLatestObservation:
    def test_incremental_overlay_beats_stale_full_load(self, seeded):
        # full_load still says L1 in_season (a 2025 snapshot); the incremental overlay --
        # the latest observation across all partitions -- says complete. The dim must not
        # fall back to the snapshot (that fallback was the 2026-09 status oscillation).
        seeded["path::league/leagues/incremental"] = seeded["path::league/leagues/incremental"].with_columns(
            pl.when(pl.col("league_id") == "L1").then(pl.lit("complete"))
              .otherwise(pl.col("status")).alias("status")
        )
        df = transform_dim_leagues_meta()
        by_id = {r["league_id"]: r for r in df.to_dicts()}
        assert by_id["L1"]["status"] == "complete"
        assert by_id["L1"]["is_active"] is False

    def test_reads_all_partitions_not_newest_file(self, seeded, monkeypatch):
        calls = []
        real = mod.read_latest_incremental_by_key
        def spy(bucket, entity_path, **kw):
            calls.append(entity_path)
            return real(bucket, entity_path, **kw)
        monkeypatch.setattr(mod, "read_latest_incremental_by_key", spy)
        transform_dim_leagues_meta()
        assert calls == ["league/leagues/incremental", "league/settings/incremental"]
