"""Shared fixtures for the ETL test suite."""
from __future__ import annotations

import polars as pl
import pytest


@pytest.fixture
def fake_gcs(monkeypatch):
    """In-memory stand-in for parquet I/O against gs:// paths.

    Patches ``polars.read_parquet`` and ``polars.DataFrame.write_parquet`` so any
    ETL code that round-trips through GCS instead reads/writes an in-process dict
    keyed by path string. Returns the dict so tests can seed inputs and assert on
    outputs.

    Notes:
        * Partitioning kwargs (partition_by/use_pyarrow) are accepted and ignored;
          the whole frame is stored under the given path.
        * Reading a missing path raises FileNotFoundError, matching the
          "first load" branch that several scripts rely on.
    """
    store: dict[str, pl.DataFrame] = {}

    def fake_write(self, path, *args, **kwargs):
        store[str(path)] = self.clone()

    def fake_read(path, *args, **kwargs):
        key = str(path)
        if key in store:
            return store[key].clone()
        raise FileNotFoundError(key)

    monkeypatch.setattr(pl.DataFrame, "write_parquet", fake_write, raising=True)
    monkeypatch.setattr(pl, "read_parquet", fake_read, raising=True)
    return store
