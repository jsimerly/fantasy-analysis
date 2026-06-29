"""Shared fixtures + import-time stubs for the data_engineering test suite.

1. Stub heavyweight / auth-only deps that ingestion scripts import at module load but
   that unit tests never exercise (network/cloud calls are mocked): ``gql`` (Sleeper
   GraphQL) and ``nflreadpy`` (NFL data loader). Any attribute access returns a dummy
   callable that raises if actually invoked — patch it in the test instead.
2. ``fake_gcs`` — in-memory stand-in for parquet I/O against ``gs://`` paths.

Packages are imported by their real names (``from silver_fantasy.utils import ...``)
via ``pythonpath = ["src"]`` in pyproject.toml, so no ``de_loader`` shim is needed.
"""
from __future__ import annotations

import sys
import types

import polars as pl
import pytest


# --------------------------------------------------------------- import-time stubs
def _make_stub(fullname: str) -> types.ModuleType:
    """A module whose every missing attribute is a callable that raises on use."""
    mod = types.ModuleType(fullname)
    mod.__path__ = []  # mark as package so submodule imports are allowed

    def __getattr__(attr: str):  # PEP 562 module-level __getattr__
        def _dummy(*args, **kwargs):
            raise RuntimeError(
                f"{fullname}.{attr} is a test stub and was called for real; "
                f"patch/mock it in your test instead."
            )
        _dummy.__name__ = attr
        _dummy.__qualname__ = f"{fullname}.{attr}"
        return _dummy

    mod.__getattr__ = __getattr__
    return mod


def _install_stub_tree(*fullnames: str) -> None:
    for fullname in fullnames:
        if fullname in sys.modules:
            continue
        stub = _make_stub(fullname)
        sys.modules[fullname] = stub
        if "." in fullname:
            parent_name, child = fullname.rsplit(".", 1)
            parent = sys.modules.get(parent_name)
            if parent is not None:
                setattr(parent, child, stub)


_install_stub_tree("gql", "gql.transport", "gql.transport.requests")
_install_stub_tree("nflreadpy")


# ------------------------------------------------------------------------ fixtures
@pytest.fixture
def fake_gcs(monkeypatch):
    """In-memory stand-in for parquet I/O against gs:// paths.

    Patches ``polars.read_parquet`` and ``polars.DataFrame.write_parquet`` to read/write
    an in-process dict keyed by path string. Returns the dict so tests seed inputs and
    assert outputs. Reading a missing path raises FileNotFoundError (the "first load"
    branch several scripts rely on).
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
