"""silver_fantasy/dim_franchise_meta.py"""
import polars as pl
import pytest

from tests.de_loader import load_de_module

mod = load_de_module("silver_fantasy/dim_franchise_meta.py", "silver_fantasy", "dim_franchise_meta")
transform_dim_franchises_meta = mod.transform_dim_franchises_meta


@pytest.fixture
def seeded(monkeypatch, fake_gcs):
    monkeypatch.setenv("GCS_BUCKET_NAME", "test-bucket")
    monkeypatch.setattr(mod, "get_latest_bronze_path",
                        lambda bucket, ep, source="sleeper": f"path::{ep}")

    leagues = pl.DataFrame({"league_id": ["L1"], "league_lineage_id": ["ROOT"]})
    users = pl.DataFrame({
        "user_id": ["U1"], "primary_name": ["Alice"], "avatar": ["av1"],
    })
    rosters = pl.DataFrame({
        "league_id": ["L1", "L1"],
        "roster_id": [1, 2],
        "owner_id": ["U1", None],     # roster 2 has no owner -> orphan
    })
    fake_gcs["gs://test-bucket/silver/fantasy/dim_leagues_meta/data.parquet"] = leagues
    fake_gcs["gs://test-bucket/silver/fantasy/dim_users/data.parquet"] = users
    fake_gcs["path::rosters/team_state/daily"] = rosters
    return fake_gcs


class TestDimFranchiseMeta:
    def test_franchise_id_is_lineage_plus_roster(self, seeded):
        df = transform_dim_franchises_meta()
        ids = set(df["franchise_id"].to_list())
        assert ids == {"ROOT_1", "ROOT_2"}

    def test_owner_name_resolved(self, seeded):
        df = transform_dim_franchises_meta()
        row = df.filter(pl.col("roster_id") == 1).to_dicts()[0]
        assert row["current_team_name"] == "Alice"
        assert row["is_orphan"] is False

    def test_orphan_roster_gets_placeholder_name(self, seeded):
        df = transform_dim_franchises_meta()
        row = df.filter(pl.col("roster_id") == 2).to_dicts()[0]
        assert row["is_orphan"] is True
        assert "Orphan" in row["current_team_name"]
