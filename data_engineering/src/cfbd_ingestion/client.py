"""Thin client for the College Football Data API (https://api.collegefootballdata.com).

Needs ``CFBD_API_KEY`` (free at https://collegefootballdata.com/key; read from ``.env`` locally,
from the job's env in Cloud Run). Every call is a GET with a Bearer token; 429s back off and
retry, other errors raise. The free tier is metered per month, so the backfill writes one
parquet per (dataset, season) and skips what already exists.
"""
from __future__ import annotations

import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "https://api.collegefootballdata.com"
USER_AGENT = "fantasy-analysis-lake/1.0 (+cfbd_ingestion)"


def api_key() -> str:
    key = os.environ.get("CFBD_API_KEY", "").strip()
    if not key:
        raise RuntimeError("CFBD_API_KEY is not set: get a free key at https://collegefootballdata.com/key and put it in data_engineering/.env")
    return key


def get(path: str, params: dict | None = None, retries: int = 4, timeout: int = 60, session: requests.Session | None = None) -> list | dict:
    """GET ``path`` (e.g. "/stats/player/season") with ``params``; returns the decoded JSON."""
    s = session or requests.Session()
    headers = {"Authorization": f"Bearer {api_key()}", "Accept": "application/json", "User-Agent": USER_AGENT}
    url = BASE + path
    for attempt in range(retries + 1):
        r = s.get(url, params=params or {}, headers=headers, timeout=timeout)
        if r.status_code == 429 and attempt < retries:
            wait = 15 * (attempt + 1)
            print(f"  rate limited on {path}; waiting {wait}s")
            time.sleep(wait)
            continue
        if r.status_code >= 500 and attempt < retries:
            time.sleep(5 * (attempt + 1))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"gave up on {path} {params}")
