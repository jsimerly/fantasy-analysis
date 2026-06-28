# tests — ETL spec suite

Full detail is in **[README.md](README.md)** — read it. Quick version:

- **Run:** `.venv/Scripts/python.exe -m pytest` from the repo root (the root venv has pytest +
  polars). A single area: `pytest tests/sleeper`.
- **Spec tests:** each asserts *intended* behavior, so a red test = a real bug to fix in source, not a
  flaky test.
- **Harness:** the ETL scripts aren't importable packages (runtime `sys.path.insert` + bare imports),
  so tests load them via **`tests/de_loader.py::load_de_module(relpath, pkg_dir)`** (it evicts colliding
  module names first). Root `conftest.py` stubs `gql`/`nflreadpy`; **`tests/conftest.py::fake_gcs`**
  patches polars read/write so `gs://` round-trips in memory — no network/GCS needed.
- **When adding a pipeline:** mirror its source folder under `tests/<area>/` and load via `de_loader`.
- CI runs the suite on push/PR (`.github/workflows/tests.yaml`).
