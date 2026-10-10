# tests — ETL spec suite

Full detail is in **[README.md](README.md)** — read it. Quick version:

- **Run:** `uv run pytest -ra` from `data_engineering/` (or that folder's venv:
  `.venv/Scripts/python.exe -m pytest tests -q`). A single area: `uv run pytest tests/sleeper`.
- **Spec tests:** each asserts *intended* behavior, so a red test = a real bug to fix in source, not a
  flaky test.
- **Harness:** pytest's `pythonpath = ["src"]` (`pyproject.toml`, explained in `tests/conftest.py`) puts
  the packages on the path, so tests import them by name (`from silver_fantasy.utils import …`).
  `tests/conftest.py` stubs `gql`/`nflreadpy` at import time; its **`fake_gcs`** fixture patches polars
  read/write so `gs://` round-trips in memory — no network/GCS needed.
- **When adding a pipeline:** mirror its source folder under `tests/<area>/` and import it directly.
- CI runs this suite and the machine_learning suite separately on push/PR (`.github/workflows/tests.yaml`).
