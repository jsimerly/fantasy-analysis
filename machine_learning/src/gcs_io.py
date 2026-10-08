"""GCS access for the dynasty model.

Two buckets, by ownership:
  * LAKE_BUCKET  — the shared data lake. Holds reusable data_engineering datasets (e.g.
    ``silver/fantasy/fact_player_season``). The model only *reads* from here.
  * ML_BUCKET    — model-exclusive artifacts (cached training matrices, model files,
    predictions), namespaced per project under ``<ML_BUCKET>/<PROJECT>/``. Phase 0 reads
    only; we start writing here in Phase 1+ (the bucket may not exist yet).

Reads download blob bytes via ``google.cloud.storage`` (works off ADC) and parse with
polars — no gcsfs/object_store credential plumbing.
"""
from __future__ import annotations

import io
import os
from functools import lru_cache

import polars as pl
from google.cloud import storage

LAKE_BUCKET = os.environ.get("GCS_BUCKET_NAME") or "nfl-data-bronze"
ML_BUCKET = os.environ.get("ML_BUCKET") or "fantasy-football-ml"
PROJECT = "dynasty-value"


@lru_cache(maxsize=1)
def _client() -> storage.Client:
    return storage.Client()


def lake_updated(path: str) -> str:
    """When a lake blob was last written (ISO, UTC); empty when unreachable. A data fingerprint."""
    try:
        b = _client().bucket(LAKE_BUCKET).get_blob(path)
        return b.updated.strftime("%Y-%m-%dT%H:%M:%SZ") if b is not None and b.updated else ""
    except Exception:  # noqa: BLE001
        return ""


def read_lake(path: str) -> pl.DataFrame:
    """Read a parquet blob from the shared lake bucket."""
    data = _client().bucket(LAKE_BUCKET).blob(path).download_as_bytes()
    return pl.read_parquet(io.BytesIO(data))


def ml_path(*parts: str) -> str:
    """gs:// path for a model-exclusive artifact under this project's ML prefix."""
    return f"gs://{ML_BUCKET}/{PROJECT}/" + "/".join(parts)


def read_ml_parquet(*parts: str) -> pl.DataFrame:
    data = _client().bucket(ML_BUCKET).blob(f"{PROJECT}/" + "/".join(parts)).download_as_bytes()
    return pl.read_parquet(io.BytesIO(data))


def read_ml_json(*parts: str) -> dict:
    import json

    return json.loads(_client().bucket(ML_BUCKET).blob(f"{PROJECT}/" + "/".join(parts)).download_as_text())


def write_ml_parquet(df: pl.DataFrame, *parts: str) -> str:
    """Write a frame as parquet under this project's ML prefix; returns the gs:// path."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    blob = _client().bucket(ML_BUCKET).blob(f"{PROJECT}/" + "/".join(parts))
    blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    return ml_path(*parts)


def write_ml_json(obj: dict, *parts: str) -> str:
    import json

    blob = _client().bucket(ML_BUCKET).blob(f"{PROJECT}/" + "/".join(parts))
    blob.upload_from_string(json.dumps(obj, indent=2, default=str), content_type="application/json")
    return ml_path(*parts)


def read_lake_prefix(prefix: str, partition: str | None = None) -> pl.DataFrame:
    """Every parquet under a lake prefix, concatenated (diagonal); with ``partition`` the
    ``<partition>=value`` folder each row came from is added as a column of that name."""
    import re

    frames = []
    for b in _client().list_blobs(LAKE_BUCKET, prefix=prefix):
        if not b.name.endswith(".parquet"):
            continue
        df = pl.read_parquet(io.BytesIO(b.download_as_bytes()))
        if partition:
            m = re.search(rf"/{partition}=([^/]+)/", "/" + b.name)
            df = df.with_columns(pl.lit(m.group(1) if m else None).alias(partition))
        frames.append(df)
    if not frames:
        raise FileNotFoundError(f"no parquet objects under gs://{LAKE_BUCKET}/{prefix}")
    return pl.concat(frames, how="diagonal_relaxed")


def latest_run_date(*parts: str, on_or_before: str | None = None, name: str = "metrics.json") -> str | None:
    """Newest ``run_date=YYYY-MM-DD`` partition under ``parts`` that holds ``name`` (on or before a date, if given)."""
    import re

    pat = re.compile(r"run_date=(\d{4}-\d{2}-\d{2})/" + re.escape(name) + "$")
    dates = {m.group(1) for m in map(pat.search, list_ml(*parts)) if m and (on_or_before is None or m.group(1) <= on_or_before)}
    return max(dates) if dates else None


def list_ml(*parts: str) -> list[str]:
    """Blob names (relative to this project's ML prefix) under ``parts``."""
    prefix = f"{PROJECT}/" + "/".join(parts)
    return [b.name[len(PROJECT) + 1:] for b in _client().list_blobs(ML_BUCKET, prefix=prefix)]
