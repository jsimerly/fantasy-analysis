"""Root pytest conftest.

Two jobs:

1. Make the repo root importable so `tests.de_loader` resolves.
2. Install lightweight stand-ins for heavyweight / auth-only third-party
   packages that the ETL scripts import at module load time but that we never
   actually exercise in unit tests (we mock the network/cloud calls instead).

   Stubbed:  gql (Sleeper GraphQL client), nflreadpy (NFL data loader).
   Real:     polars, pandas, numpy, requests, bs4, google-cloud-storage, dotenv.

The stubs are installed at import time (before pytest collects any test module),
so DE modules that do `import nflreadpy as nfl` / `from gql import gql` import
cleanly. Any attribute access on a stub returns a dummy callable that raises if
actually invoked -- a unit test that needs the behaviour must patch it.
"""
import sys
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


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
        # Link child to parent so `from a.b import c` style imports resolve.
        if "." in fullname:
            parent_name, child = fullname.rsplit(".", 1)
            parent = sys.modules.get(parent_name)
            if parent is not None:
                setattr(parent, child, stub)


def _install_stubs() -> None:
    # gql GraphQL client (used by sleeper _utils + league_transactions_ingestion)
    _install_stub_tree("gql", "gql.transport", "gql.transport.requests")
    # nflreadpy (used by nflverse ingestion scripts; referenced at import time)
    _install_stub_tree("nflreadpy")


_install_stubs()
