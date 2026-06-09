"""silver_fantasy/dim_users.py"""
import polars as pl
import pytest

from tests.de_loader import load_de_module

mod = load_de_module("silver_fantasy/dim_users.py", "silver_fantasy", "dim_users")
transform_dim_users = mod.transform_dim_users
get_google_sheet_data = mod.get_google_sheet_data


class TestGetGoogleSheetData:
    def test_returns_empty_typed_frame_on_fetch_error(self, monkeypatch):
        def boom(*a, **k):
            raise RuntimeError("network down")
        monkeypatch.setattr(mod.pd, "read_csv", boom)
        df = get_google_sheet_data("sheet-id")
        assert df.height == 0
        assert "user_id" in df.columns
        assert df.schema["user_id"] == pl.Utf8

    def test_casts_user_id_to_string(self, monkeypatch):
        import pandas as pd
        monkeypatch.setattr(
            mod.pd, "read_csv",
            lambda url: pd.DataFrame({"user_id": [123], "real_name": ["Jake"]}),
        )
        df = get_google_sheet_data("sheet-id")
        assert df.schema["user_id"] == pl.Utf8
        assert df.to_dicts()[0]["user_id"] == "123"


class TestTransformDimUsers:
    @pytest.fixture
    def seeded(self, monkeypatch, fake_gcs):
        monkeypatch.setenv("GCS_BUCKET_NAME", "test-bucket")
        monkeypatch.setattr(mod, "get_latest_bronze_path",
                            lambda bucket, ep, source="sleeper": f"path::{ep}")
        users = pl.DataFrame({
            "user_id": ["U1", "U2"],
            "display_name": ["alice", "bob"],
            "avatar": ["a1", "a2"],
        })
        fake_gcs["path::rosters/users/weekly"] = users
        return fake_gcs

    def test_real_name_wins_over_display_name(self, seeded, monkeypatch):
        monkeypatch.setattr(
            mod, "get_google_sheet_data",
            lambda sheet_id: pl.DataFrame({"user_id": ["U1"], "real_name": ["Alice Real"]}),
        )
        df = transform_dim_users()
        by_id = {r["user_id"]: r for r in df.to_dicts()}
        assert by_id["U1"]["primary_name"] == "Alice Real"      # manual real_name
        assert by_id["U2"]["primary_name"] == "bob"             # falls back to display

    def test_unknown_when_no_names(self, seeded, monkeypatch):
        users = pl.DataFrame({"user_id": ["U3"], "display_name": [None], "avatar": [None]})
        seeded["path::rosters/users/weekly"] = users
        monkeypatch.setattr(
            mod, "get_google_sheet_data",
            lambda sheet_id: pl.DataFrame(
                {"user_id": [], "real_name": []},
                schema={"user_id": pl.Utf8, "real_name": pl.Utf8},
            ),
        )
        df = transform_dim_users()
        assert df.to_dicts()[0]["primary_name"] == "Unknown"

    def test_dedupes_users(self, seeded, monkeypatch):
        dupes = pl.DataFrame({
            "user_id": ["U1", "U1"],
            "display_name": ["old", "new"],
            "avatar": ["a", "a"],
        })
        seeded["path::rosters/users/weekly"] = dupes
        monkeypatch.setattr(
            mod, "get_google_sheet_data",
            lambda sheet_id: pl.DataFrame(
                {"user_id": [], "real_name": []},
                schema={"user_id": pl.Utf8, "real_name": pl.Utf8},
            ),
        )
        df = transform_dim_users()
        assert df.filter(pl.col("user_id") == "U1").height == 1
