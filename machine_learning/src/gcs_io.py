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
