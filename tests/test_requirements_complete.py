"""Guard: every third-party module a pipeline imports must be declared in that
pipeline's requirements.txt.

This is the test that would have caught the silver `pandas` outage: unit tests
run in a venv that already has pandas, so a missing requirements.txt entry only
blows up in the lean Docker image at deploy time. Here we parse the actual
imports and cross-check the actual requirements file.
"""
import ast
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DE = REPO_ROOT / "data_engineering"

PIPELINES = [
    "sleeper_ingestion",
    "silver_fantasy",
    "ktc_ingestion",
    "nflverse_ingestion",
    "fantasycalc_ingestion",
]

# import name -> PyPI distribution name, where they differ
IMPORT_TO_DIST = {
    "google": "google-cloud-storage",
    "dotenv": "python-dotenv",
    "bs4": "beautifulsoup4",
}

# Directories to skip when scanning a pipeline's own source (local dev venvs,
# caches, build artifacts -- not part of the shipped code).
EXCLUDE_DIRS = {".venv", "venv", "env", "site-packages", "__pycache__",
                ".git", ".pytest_cache", ".ipynb_checkpoints", "build", "dist"}

# Imports used only by local/dev-only scripts that are never deployed as Cloud
# Run jobs, so they intentionally stay out of the image requirements.
ALLOWED_MISSING = {
    # dynasty/local_archive_to_cloud.py is a one-off local Postgres->GCS backfill
    "ktc_ingestion": {"adbc_driver_postgresql"},
}


def _is_source(path: Path) -> bool:
    return not any(part in EXCLUDE_DIRS or part.endswith(".egg-info")
                   for part in path.parts)


def _read_text(path: Path) -> str:
    """Decode a file that may be UTF-8, UTF-8-BOM, or UTF-16 (some requirements
    files in this repo are UTF-16)."""
    raw = path.read_bytes()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16")
    if raw[:3] == b"\xef\xbb\xbf":
        return raw.decode("utf-8-sig")
    if b"\x00" in raw[:64]:  # BOM-less UTF-16
        return raw.decode("utf-16-le" if raw[1:2] == b"\x00" else "utf-16-be")
    return raw.decode("utf-8")


def _local_names(pkg_dir: Path) -> set[str]:
    names: set[str] = set()
    for p in pkg_dir.rglob("*.py"):
        if _is_source(p):
            names.add(p.stem)
    for p in pkg_dir.iterdir():
        if p.is_dir() and p.name not in EXCLUDE_DIRS:
            names.add(p.name)
    return names


def _top_level_imports(pkg_dir: Path) -> set[str]:
    mods: set[str] = set()
    for py in pkg_dir.rglob("*.py"):
        if not _is_source(py):
            continue
        try:
            tree = ast.parse(_read_text(py))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    mods.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.level == 0 and node.module:  # absolute import only
                    mods.add(node.module.split(".")[0])
    return mods


def _norm(name: str) -> str:
    return name.lower().replace("_", "-")


def _required_dists(req_path: Path) -> set[str]:
    dists: set[str] = set()
    for line in _read_text(req_path).splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name = re.split(r"[<>=!~\[; ]", line, 1)[0]
        if name:
            dists.add(_norm(name))
    return dists


@pytest.mark.parametrize("pipeline", PIPELINES)
def test_requirements_cover_imports(pipeline):
    pkg = DE / pipeline
    req = pkg / "requirements.txt"
    assert req.exists(), f"{pipeline} has no requirements.txt"

    local = _local_names(pkg)
    stdlib = set(sys.stdlib_module_names)
    required = _required_dists(req)
    allowed = ALLOWED_MISSING.get(pipeline, set())

    missing = []
    for mod in sorted(_top_level_imports(pkg)):
        if mod in local or mod in stdlib or mod in allowed:
            continue
        dist = _norm(IMPORT_TO_DIST.get(mod, mod))
        if dist not in required:
            missing.append(f"import '{mod}' -> needs '{dist}'")

    assert not missing, (
        f"{pipeline}/requirements.txt is missing dependencies: {missing}"
    )
