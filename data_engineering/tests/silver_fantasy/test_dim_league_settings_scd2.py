"""silver_fantasy/dim_league_settings_scd2.py

SCD Type-2 builder for the league "constitution" (scoring + roster slots +
settings). Tests cover first load, the no-change heartbeat, and a real change
(expire old version + open a new current version).
"""
import polars as pl
import pytest


import silver_fantasy.dim_league_settings_scd2 as mod
transform = mod.transform_dim_league_settings_scd2

EXISTING_PATH = "gs://test-bucket/silver/fantasy/dim_league_settings/data.parquet"

_ALL_SLOTS = ["QB", "RB", "WR", "TE", "FLEX", "SUPER_FLEX", "BN", "TAXI", "IR", "K", "DEF"]


@pytest.fixture
def seed(monkeypatch, fake_gcs):
    monkeypatch.setenv("GCS_BUCKET_NAME", "test-bucket")
    monkeypatch.setattr(mod, "get_latest_bronze_path",
                        lambda bucket, ep, source="sleeper": f"path::{ep}")

    def _seed_bronze(pass_td=4.0):
        scoring = pl.DataFrame({
            "league_id": ["L1"], "league_lineage_id": ["ROOT"], "pass_td": [pass_td],
        })
        rosters = pl.DataFrame({
            "league_id": ["L1"], "league_lineage_id": ["ROOT"],
            **{slot: [1] for slot in _ALL_SLOTS},
        })
        settings = pl.DataFrame({
            "league_id": ["L1"], "league_lineage_id": ["ROOT"], "num_teams": [12],
        })
        for ep in ("league/scoring/full_load", "league/scoring/incremental"):
            fake_gcs[f"path::{ep}"] = scoring
        for ep in ("league/roster_slots/full_load", "league/roster_slots/incremental"):
            fake_gcs[f"path::{ep}"] = rosters
        for ep in ("league/settings/full_load", "league/settings/incremental"):
            fake_gcs[f"path::{ep}"] = settings

    return _seed_bronze, fake_gcs


class TestFirstLoad:
    def test_all_records_current(self, seed):
        seed_bronze, _ = seed
        seed_bronze(pass_td=4.0)
        df = transform()
        assert df.height == 1
        row = df.to_dicts()[0]
        assert row["is_current"] is True
        assert row["valid_to"] is None
        assert row["valid_from"] is not None
        assert row["pass_td"] == 4.0


class TestNoChange:
    def test_unchanged_league_stays_single_current(self, seed):
        seed_bronze, store = seed
        seed_bronze(pass_td=4.0)
        store[EXISTING_PATH] = transform()        # baseline becomes "existing"

        df = transform()                          # run again, nothing changed
        current = df.filter(pl.col("is_current"))
        assert current.height == 1
        assert df.height == 1                     # no new version opened


class TestChange:
    def test_change_expires_old_and_opens_new(self, seed):
        seed_bronze, store = seed
        seed_bronze(pass_td=4.0)
        store[EXISTING_PATH] = transform()        # baseline at 4.0

        seed_bronze(pass_td=6.0)                  # scoring changed
        df = transform()

        current = df.filter(pl.col("is_current"))
        expired = df.filter(~pl.col("is_current"))
        assert current.height == 1
        assert current.to_dicts()[0]["pass_td"] == 6.0
        assert expired.height == 1
        assert expired.to_dicts()[0]["pass_td"] == 4.0
        assert expired.to_dicts()[0]["valid_to"] is not None
