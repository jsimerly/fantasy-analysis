"""silver_fantasy/utils.py: get_latest_bronze_path + merge_full_and_incremental."""
import types

import polars as pl
import pytest

from tests.de_loader import load_de_module

mod = load_de_module("silver_fantasy/utils.py", "silver_fantasy", "silver_utils")
get_latest_bronze_path = mod.get_latest_bronze_path
merge_full_and_incremental = mod.merge_full_and_incremental


class _Blob:
    def __init__(self, name):
        self.name = name


def _fake_storage(blob_names):
    """Build a fake google.cloud.storage.Client returning the given blob names."""
    class _Bucket:
        def list_blobs(self, prefix=None):
            return [_Blob(n) for n in blob_names]

    class _Client:
        def bucket(self, name):
            return _Bucket()

    stub = types.SimpleNamespace(Client=_Client)
    return stub


class TestGetLatestBronzePath:
    def test_picks_most_recent_load_date(self, monkeypatch):
        names = [
            "bronze/sleeper/league/leagues/full_load/load_date=2024-01-01/data.parquet",
            "bronze/sleeper/league/leagues/full_load/load_date=2024-03-15/data.parquet",
            "bronze/sleeper/league/leagues/full_load/load_date=2024-02-10/data.parquet",
        ]
        monkeypatch.setattr(mod, "storage", _fake_storage(names))
        path = get_latest_bronze_path("nfl-data-bronze", "league/leagues/full_load")
        assert path == (
            "gs://nfl-data-bronze/bronze/sleeper/league/leagues/full_load/"
            "load_date=2024-03-15/data.parquet"
        )

    def test_ignores_non_parquet_and_non_partitioned_blobs(self, monkeypatch):
        names = [
            "bronze/sleeper/league/leagues/full_load/load_date=2024-01-01/data.parquet",
            "bronze/sleeper/league/leagues/full_load/_SUCCESS",
            "bronze/sleeper/league/leagues/full_load/load_date=2024-05-01/data.json",
        ]
        monkeypatch.setattr(mod, "storage", _fake_storage(names))
        path = get_latest_bronze_path("b", "league/leagues/full_load")
        assert "load_date=2024-01-01" in path

    def test_raises_when_no_data(self, monkeypatch):
        monkeypatch.setattr(mod, "storage", _fake_storage([]))
        with pytest.raises(ValueError):
            get_latest_bronze_path("b", "league/leagues/full_load")

    def test_source_segment_is_used(self, monkeypatch):
        names = ["bronze/nflverse/players/load_date=2024-01-01/data.parquet"]
        monkeypatch.setattr(mod, "storage", _fake_storage(names))
        path = get_latest_bronze_path("b", "players", source="nflverse")
        assert "/bronze/nflverse/players/" in path


class TestMergeFullAndIncremental:
    def test_incremental_overrides_full_on_key(self):
        full = pl.DataFrame({"league_id": ["A", "B"], "val": [1, 2]})
        incr = pl.DataFrame({"league_id": ["B"], "val": [99]})
        out = merge_full_and_incremental(full, incr, join_key="league_id")
        by_id = {r["league_id"]: r["val"] for r in out.to_dicts()}
        assert by_id == {"A": 1, "B": 99}

    def test_new_keys_in_incremental_are_added(self):
        full = pl.DataFrame({"league_id": ["A"], "val": [1]})
        incr = pl.DataFrame({"league_id": ["C"], "val": [3]})
        out = merge_full_and_incremental(full, incr, join_key="league_id")
        assert set(out["league_id"].to_list()) == {"A", "C"}

    def test_preserve_columns_taken_from_full(self):
        # league_lineage_id should be preserved from the full load even though
        # the incremental row wins for the other columns.
        full = pl.DataFrame({
            "league_id": ["B"], "val": [2], "league_lineage_id": ["ROOT"],
        })
        incr = pl.DataFrame({"league_id": ["B"], "val": [99]})
        out = merge_full_and_incremental(
            full, incr, join_key="league_id", preserve_columns=["league_lineage_id"]
        )
        row = out.to_dicts()[0]
        assert row["val"] == 99
        assert row["league_lineage_id"] == "ROOT"
