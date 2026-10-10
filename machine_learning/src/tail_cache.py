"""The career tail, cached per season for the weekly refresh (BACKLOG 36).

The career model's projections and their out-of-sample spread (sigma) are trained on completed
seasons and project every player's latest complete-season row, so from one Monday to the next
they are identical: the in-progress season reaches the model through the in-season stage, not
here. A production run spends hours on them (fit, the sigma refit, the predictions with bands),
so ``backtest_inseason.py --current`` keeps them under ``_cache/career_tail/<key>/`` and reuses
them while the key holds.

The key is the content the tail can depend on: every row the model can see (seasons up to the
as-of season, plus training snapshot rows of any season; the in-progress season's partial rows
are not among them because the tail neither trains on nor projects them), the configuration
(backend, parameters, groups, target, cap, horizons, bands), the polars version, and the bytes of
the modelling modules -- so a code change, a lake restatement or a new completed season all miss
and recompute. Nothing here is a dataset: the cache is local and disposable.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import polars as pl

HERE = Path(__file__).resolve().parent
CODE = tuple(HERE / f for f in ("career.py", "features.py", "feature_groups.py", "draft_rows.py", "unified.py"))


def visible_rows(season_df: pl.DataFrame, as_of: int) -> pl.DataFrame:
    """The rows the career tail can see as of ``as_of``."""
    cond = pl.col("season") <= as_of
    if "is_snapshot_row" in season_df.columns:
        cond = cond | (pl.col("is_snapshot_row").cast(pl.Float64, strict=False).fill_null(0.0) > 0.5)
    return season_df.filter(cond)


def cache_key(season_df: pl.DataFrame, as_of: int, config: dict, code: Iterable[Path] = CODE) -> str:
    seen = visible_rows(season_df, as_of)
    h = hashlib.sha1()
    h.update(json.dumps({"as_of": int(as_of), "config": config, "polars": pl.__version__, "columns": seen.columns, "shape": list(seen.shape)},
                        sort_keys=True, default=str).encode())
    h.update(np.sort(seen.hash_rows().to_numpy().astype(np.uint64)).tobytes())     # row content, order-free
    for f in code:
        h.update(Path(f).read_bytes())
    return h.hexdigest()[:16]


def load(cache_dir: Path, key: str) -> tuple[pl.DataFrame, dict[int, dict[str, float]], dict] | None:
    """(tail, sigma, meta) under ``cache_dir/key``, or None when absent or unreadable."""
    d = Path(cache_dir) / key
    try:
        tail = pl.read_parquet(d / "tail.parquet")
        raw = json.loads((d / "sigma.json").read_text(encoding="utf-8"))
        meta = json.loads((d / "meta.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return tail, {int(k): v for k, v in raw.items()}, meta


def store(cache_dir: Path, key: str, tail: pl.DataFrame, sigma: dict, meta: dict) -> Path:
    d = Path(cache_dir) / key
    d.mkdir(parents=True, exist_ok=True)
    tail.write_parquet(d / "tail.parquet")
    (d / "sigma.json").write_text(json.dumps({str(k): v for k, v in sigma.items()}), encoding="utf-8")
    (d / "meta.json").write_text(json.dumps({**meta, "key": key, "written": datetime.now(timezone.utc).isoformat(timespec="seconds")},
                                           default=str, indent=1), encoding="utf-8")
    return d
