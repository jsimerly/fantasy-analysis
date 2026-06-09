"""Import ETL scripts that live under data_engineering/ as importable modules.

The ingestion scripts are not packaged for import: they rely on
`sys.path.insert(0, <package_root>)` at runtime and then do bare imports like
`from utils import ...`, `from _utils import ...`, `from api.draft import ...`.
Several packages also reuse the same top-level module names (two `utils.py`,
two `daily_ingestion.py`, etc.), so we cannot simply dump every package dir on
sys.path.

`load_de_module` loads one target file with *its* package root temporarily at
the front of sys.path, evicting any cached sibling modules first so the bare
imports resolve to the correct package.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DE = REPO_ROOT / "data_engineering"


def _namespace_names(pkg_path: Path) -> set[str]:
    """Top-level importable names provided by a package directory."""
    names: set[str] = set()
    for child in pkg_path.iterdir():
        if child.suffix == ".py":
            names.add(child.stem)
        elif child.is_dir() and (child / "__init__.py").exists():
            names.add(child.name)
    return names


def load_de_module(relpath: str, pkg_dir: str, mod_name: str | None = None):
    """Load ``data_engineering/<relpath>`` with ``data_engineering/<pkg_dir>``
    as the import root.

    Args:
        relpath: path to the .py file, relative to data_engineering/.
        pkg_dir: package root (relative to data_engineering/) to put on sys.path
                 so the module's intra-package imports resolve.
        mod_name: optional explicit module name (defaults to the file stem).
    """
    pkg_path = (DE / pkg_dir).resolve()
    file_path = (DE / relpath).resolve()
    if not file_path.exists():
        raise FileNotFoundError(file_path)
    if mod_name is None:
        mod_name = file_path.stem

    # Evict any cached modules that could shadow / be shadowed by this package's
    # namespace so the fresh load binds to the right files.
    collide = _namespace_names(pkg_path) | {mod_name}
    for cached in list(sys.modules):
        root = cached.split(".", 1)[0]
        if cached in collide or root in collide:
            del sys.modules[cached]

    sys.path.insert(0, str(pkg_path))
    try:
        spec = importlib.util.spec_from_file_location(mod_name, file_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        try:
            sys.path.remove(str(pkg_path))
        except ValueError:
            pass
