"""Fetch layer for FantasyPros projections: polite live site + Wayback Machine.

Two sources, different etiquette:
  * **live** (`fantasypros.com`) blocks scrapers — slow, jittered, rotating realistic headers,
    hard backoff on 429/403/5xx.
  * **Wayback** (`web.archive.org`) tolerates programmatic access — faster, but retry on 503.
    CDX enumerates captures of a page; `id_` fetch returns the raw archived HTML (no banner).
"""
from __future__ import annotations

import random
import time

import requests

BASE = "https://www.fantasypros.com/nfl/projections"
POSITIONS = ["qb", "rb", "wr", "te", "k", "dst"]
SCORING = "PPR"

WB_CDX = "http://web.archive.org/cdx/search/cdx"
WB_WEB = "http://web.archive.org/web"
_WB_UA = {"User-Agent": "Mozilla/5.0 (research; fantasy-analysis bronze ingestion)"}

# rotating realistic browser identities for the live site
_AGENTS = [
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
     '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"', "Windows"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
     '"Google Chrome";v="140", "Not?A_Brand";v="8", "Chromium";v="140"', "macOS"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0", None, None),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
     None, None),
]


def _live_headers() -> dict:
    ua, ch, plat = random.choice(_AGENTS)
    h = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"{BASE}/qb.php", "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive", "Cache-Control": "no-cache",
    }
    if ch:
        h |= {"Sec-CH-UA": ch, "Sec-CH-UA-Mobile": "?0", "Sec-CH-UA-Platform": f'"{plat}"',
              "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "same-origin"}
    return h


_session = requests.Session()
_n_req = 0


def pace(rate_per_min: float) -> None:
    """Gaussian-jittered gap targeting ~rate_per_min, with a longer break every ~10 requests."""
    global _n_req
    _n_req += 1
    mean = 60.0 / max(rate_per_min, 0.5)
    time.sleep(max(mean * 0.45, random.gauss(mean, mean * 0.3)))
    if _n_req % 10 == 0:
        time.sleep(random.uniform(mean * 4, mean * 8))


def live_url(pos: str, year: int, week: int | None) -> str:
    wk = f"&week={week}" if week else ""
    return f"{BASE}/{pos}.php?year={year}{wk}&scoring={SCORING}"


def live_fetch(url: str, rate_per_min: float = 4.0, tries: int = 5) -> str | None:
    """GET the live site with rotating headers + hard backoff. None on a hard miss."""
    for t in range(tries):
        pace(rate_per_min)
        try:
            r = _session.get(url, headers=_live_headers(), timeout=30)
        except requests.RequestException:
            time.sleep(min(30 * (t + 1), 120))
            continue
        if r.status_code == 200:
            return r.text
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            time.sleep(120 * (t + 1)); continue
        if r.status_code == 403:
            time.sleep(300); continue
        if r.status_code >= 500:
            time.sleep(min(30 * (t + 1), 120)); continue
        return None
    return None


# --------------------------------------------------------------------------- wayback
def cdx_snapshots(pos: str, frm: str, to: str, tries: int = 5) -> list[tuple[str, str]]:
    """[(timestamp, original_url)] captures of the projection page in [frm, to] (one/day)."""
    u = (f"{WB_CDX}?url=fantasypros.com/nfl/projections/{pos}.php*&from={frm}&to={to}"
         f"&output=json&collapse=timestamp:8&fl=timestamp,original")
    for t in range(tries):
        try:
            r = requests.get(u, headers=_WB_UA, timeout=90)
            if r.status_code == 200 and r.text.strip().startswith("["):
                return [(row[0], row[1]) for row in r.json()[1:]]
            if r.status_code == 200:        # empty (no captures)
                return []
        except requests.RequestException:
            pass
        time.sleep(5 * (t + 1))             # 503 overload backoff
    return []


def pick_nearest(snaps: list[tuple[str, str]], target_yyyymmdd: str) -> tuple[str, str] | None:
    """Prefer the latest capture on/before ``target`` (pre-game); else the earliest after."""
    if not snaps:
        return None
    on_or_before = [s for s in snaps if s[0][:8] <= target_yyyymmdd]
    if on_or_before:
        return max(on_or_before, key=lambda s: s[0])
    return min(snaps, key=lambda s: s[0])


def wayback_fetch(timestamp: str, original_url: str, tries: int = 4) -> str | None:
    """Raw archived HTML (``id_`` strips the Wayback banner/rewrites)."""
    url = f"{WB_WEB}/{timestamp}id_/{original_url}"
    for t in range(tries):
        try:
            r = requests.get(url, headers=_WB_UA, timeout=90)
            if r.status_code == 200:
                return r.text
        except requests.RequestException:
            pass
        time.sleep(5 * (t + 1))
    return None
