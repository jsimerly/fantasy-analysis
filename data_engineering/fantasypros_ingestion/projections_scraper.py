"""FantasyPros historical fantasy-projection scraper (weekly + season-long).

Pre-game projections — what players were *projected* to score before each game/season — are a gated,
hard-to-find dataset. FantasyPros is the deepest free source we found: a multi-source CONSENSUS
(CBS/ESPN/FFToday/…), publicly reachable on historical pages, serving both weekly (`?year&week`) and
season-long (`?year`) projections back to ~2010. We harvest it carefully into bronze, keyed by the
FantasyPros player id (the `fp-id-NNNN` link class — joins to our `fantasy_rankings` ECR + crosswalks).

Page: https://www.fantasypros.com/nfl/projections/{pos}.php?year={year}[&week={W}]&scoring=PPR
Table `#data` has, per position, either a 2-row header (group row with colspan + stat row — QB/RB/WR/TE,
where labels like ATT/YDS/TDS repeat across PASSING/RUSHING/RECEIVING) or a 1-row header (DST/K). We map
columns by GROUP+STAT label (not position index) so format drift can't silently shift values.

Politeness: this is a content site, so we go slow + human-like — ~4 req/min with gaussian-jittered gaps,
periodic long breaks, rotating realistic browser headers, and hard backoff on 429/403. Resumable: a
(year, week) whose GCS output exists is skipped, so a restart never re-hits done pages.

Run LOCALLY (rate-limited, not compute-bound). Env:
  GCS_BUCKET_NAME (nfl-data-bronze), FP_NO_GCS (1=skip GCS / dry run), FP_RATE_PER_MIN (4),
  FP_YEAR_START (2008), FP_YEAR_END (current), FP_WEEKS (18), FP_SEASON_ONLY/FP_WEEKLY_ONLY.
  `python projections_scraper.py audit`  -> parser audit matrix (years x positions x weekly/season), no writes.
"""
from __future__ import annotations

import datetime as dt
import os
import random
import re
import sys
import time
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BASE = "https://www.fantasypros.com/nfl/projections"
BUCKET = os.environ.get("GCS_BUCKET_NAME", "nfl-data-bronze")
NO_GCS = os.environ.get("FP_NO_GCS") == "1"
RATE_PER_MIN = float(os.environ.get("FP_RATE_PER_MIN", "4"))
YEAR_START = int(os.environ.get("FP_YEAR_START", "2012"))   # FP consensus floor (2010-11 = "not enough sources")
YEAR_END = int(os.environ.get("FP_YEAR_END", str(dt.date.today().year)))
MAX_WEEKS = int(os.environ.get("FP_WEEKS", "18"))
POSITIONS = ["qb", "rb", "wr", "te", "k", "dst"]
SCORING = "PPR"
LOCAL_DIR = Path(__file__).resolve().parents[2] / "analysis" / "_cache" / "fantasypros"
SCRAPED_AT = dt.datetime.now(dt.timezone.utc)

# rotating realistic browser identities (UA + matching client hints)
_AGENTS = [
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
     '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"', "Windows"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
     '"Google Chrome";v="140", "Not?A_Brand";v="8", "Chromium";v="140"', "macOS"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0", None, None),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
     None, None),
]


def _headers() -> dict:
    ua, ch, plat = random.choice(_AGENTS)
    h = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"{BASE}/qb.php",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
    }
    if ch:
        h |= {"Sec-CH-UA": ch, "Sec-CH-UA-Mobile": "?0", "Sec-CH-UA-Platform": f'"{plat}"',
              "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "same-origin"}
    return h


_session = requests.Session()
_n_req = 0


def _pace():
    """Non-linear ~RATE_PER_MIN: gaussian-jittered gap + a longer break every ~10 requests."""
    global _n_req
    _n_req += 1
    mean = 60.0 / max(RATE_PER_MIN, 0.5)
    time.sleep(max(mean * 0.45, random.gauss(mean, mean * 0.3)))
    if _n_req % 10 == 0:
        brk = random.uniform(mean * 4, mean * 8)
        print(f"    …long break {brk:.0f}s", flush=True)
        time.sleep(brk)


def _url(pos: str, year: int, week: int | None) -> str:
    wk = f"&week={week}" if week else ""
    return f"{BASE}/{pos}.php?year={year}{wk}&scoring={SCORING}"


def fetch(url: str, tries: int = 5) -> str | None:
    """GET with rotating headers + hard backoff on 429/403/5xx/network. None on hard miss."""
    for t in range(tries):
        _pace()
        try:
            r = _session.get(url, headers=_headers(), timeout=30)
        except requests.RequestException as e:
            print(f"    net error ({e}); backing off", flush=True)
            time.sleep(min(30 * (t + 1), 120))
            continue
        if r.status_code == 200:
            return r.text
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            print(f"    429 throttled — backing off {120 * (t + 1)}s", flush=True)
            time.sleep(120 * (t + 1))
            continue
        if r.status_code == 403:
            print("    403 forbidden — pausing 5 min (possible soft block)", flush=True)
            time.sleep(300)
            continue
        if r.status_code >= 500:
            time.sleep(min(30 * (t + 1), 120))
            continue
        return None
    return None


# --------------------------------------------------------------------------- parsing
def _norm(group: str, stat: str) -> str:
    if stat.lower() == "player":
        return "player"
    if stat.upper() == "FPTS":
        return "fpts"
    base = f"{group}_{stat}" if group else stat
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", base.lower())).strip("_")


def _header_keys(thead) -> list[str] | None:
    """Map each column to a GROUP_STAT key. Handles 2-row (group+stat, colspan) and 1-row headers."""
    rows = thead.find_all("tr")
    if not rows:
        return None
    if len(rows) == 1:
        labels = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
        groups = [""] * len(labels)
    else:
        groups = []
        for c in rows[0].find_all(["th", "td"]):
            groups += [c.get_text(strip=True)] * int(c.get("colspan") or 1)
        labels = [c.get_text(strip=True) for c in rows[1].find_all(["th", "td"])]
        if len(groups) < len(labels):
            groups += [""] * (len(labels) - len(groups))
    return [_norm(g, l) for g, l in zip(groups, labels)]


def _player_cell(td):
    a = td.find("a", class_="fp-player-link") or td.find("a")
    fp_id = name = team = None
    if a is not None:
        for c in (a.get("class") or []):
            m = re.fullmatch(r"fp-id-(\d+)", c)
            if m:
                fp_id = m.group(1)
                break
        name = a.get("fp-player-name") or a.get_text(strip=True)
        team = td.get_text(strip=True).replace(a.get_text(strip=True), "").strip() or None
    else:
        name = td.get_text(strip=True)
    return fp_id, name, team


def _to_float(s: str):
    s = (s or "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def parse_projection_page(html: str):
    """-> (rows: list[dict], keys: list[str], status). status in {ok, empty, parse_fail}.
    Header-driven + validated, so a layout change fails loud rather than mis-mapping values."""
    if html is None:
        return [], [], "empty"
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id="data")
    if table is None:
        return [], [], "empty"        # e.g. pre-floor years -> "No Player Found"
    thead = table.find("thead")
    keys = _header_keys(thead) if thead else None
    if not keys or keys[0] != "player" or "fpts" not in keys:
        return [], (keys or []), "parse_fail"
    body = table.find("tbody")
    trs = body.find_all("tr") if body else []
    rows = []
    for tr in trs:
        tds = tr.find_all("td")
        if len(tds) <= 1:                   # colspan placeholder ("Not enough sources…", ad row) -> skip
            continue
        if len(tds) != len(keys):           # genuine column/header mismatch -> fail loud, don't guess
            return rows, keys, "parse_fail"
        fp_id, name, team = _player_cell(tds[0])
        if not name:
            continue
        rec = {"fp_id": fp_id, "player": name, "team": team}
        for k, td in zip(keys[1:], tds[1:]):
            rec[k] = _to_float(td.get_text(strip=True))
        rows.append(rec)
    return rows, keys, ("ok" if rows else "empty")


# --------------------------------------------------------------------------- harvest + write
def harvest(year: int, week: int | None):
    """Fetch + parse all positions for one (year, week|season). Returns (DataFrame|None, failures)."""
    frames, failures = [], []
    for pos in POSITIONS:
        html = fetch(_url(pos, year, week))
        rows, keys, status = parse_projection_page(html)
        if status == "parse_fail":
            failures.append((year, week, pos, keys))
            print(f"  !! PARSE FAIL {pos} {year} wk={week} headers={keys}", flush=True)
            continue
        if not rows:
            continue
        df = pl.DataFrame(rows, infer_schema_length=None).with_columns(
            pl.lit(str(year)).alias("season"),
            pl.lit(week, dtype=pl.Int64).alias("week"),
            pl.lit(pos).alias("position"),
            pl.lit(SCORING).alias("scoring"),
            pl.lit(SCRAPED_AT).alias("scraped_at"),
        )
        frames.append(df)
    if not frames:
        return None, failures
    return pl.concat(frames, how="diagonal_relaxed"), failures


def _out_path(year: int, week: int | None) -> str:
    if week:
        return f"bronze/fantasypros/projections/weekly/season={year}/week={week:02d}/data.parquet"
    return f"bronze/fantasypros/projections/season/season={year}/data.parquet"


_gcs_bucket = None


def _bucket():
    global _gcs_bucket
    if _gcs_bucket is None:
        from google.cloud import storage
        _gcs_bucket = storage.Client().bucket(BUCKET)
    return _gcs_bucket


def _exists(year: int, week: int | None) -> bool:
    if NO_GCS:
        return (LOCAL_DIR / _out_path(year, week).replace("/", "__")).exists()
    return _bucket().blob(_out_path(year, week)).exists()


def _write(df: pl.DataFrame, year: int, week: int | None):
    """Local parquet then GCS upload (robust: timeout+retry, never the hang-prone direct gs:// write)."""
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    local = LOCAL_DIR / _out_path(year, week).replace("/", "__")
    df.write_parquet(local)
    if not NO_GCS:
        _bucket().blob(_out_path(year, week)).upload_from_filename(str(local), timeout=300)


# --------------------------------------------------------------------------- audit
def audit():
    """Parser audit matrix: years x positions x {weekly, season}. Prints headers + sample rows +
    assertions so format drift across eras/positions is caught BEFORE the full run. No writes."""
    years = [2010, 2014, 2018, 2022, 2025]
    drift = {}
    print("=== FantasyPros parser audit matrix (read-only) ===", flush=True)
    for year in years:
        for week in (5, None):                    # a mid-season week + season-long
            for pos in POSITIONS:
                html = fetch(_url(pos, year, week))
                rows, keys, status = parse_projection_page(html)
                tag = f"{year} {'wk5' if week else 'SEASON'} {pos:<3}"
                if status == "parse_fail":
                    print(f"  [FAIL] {tag}  headers={keys}", flush=True)
                    continue
                if status == "empty":
                    print(f"  [empty] {tag}", flush=True)
                    continue
                drift.setdefault(pos, set()).add(tuple(keys))
                star = max(rows, key=lambda r: (r.get("fpts") or 0))
                ok = (rows and star.get("fpts") and star.get("fp_id"))
                print(f"  [{'OK' if ok else '??'}] {tag}  n={len(rows):>3}  "
                      f"top={star.get('player')} fpts={star.get('fpts')} fp_id={star.get('fp_id')}", flush=True)
                if (year, pos, week) in [(2014, "qb", 5), (2024, "dst", 5)]:
                    print(f"        keys={keys}", flush=True)
    print("\n=== header-drift report (distinct header layouts seen per position) ===", flush=True)
    for pos in POSITIONS:
        layouts = drift.get(pos, set())
        print(f"  {pos}: {len(layouts)} distinct layout(s)", flush=True)
        for lay in layouts:
            print(f"     {list(lay)}", flush=True)


# --------------------------------------------------------------------------- main
def main():
    weekly = os.environ.get("FP_SEASON_ONLY") != "1"
    season = os.environ.get("FP_WEEKLY_ONLY") != "1"
    weeks = ([None] if season else []) + (list(range(1, MAX_WEEKS + 1)) if weekly else [])
    print(f"FantasyPros projections {YEAR_START}-{YEAR_END} | rate~{RATE_PER_MIN}/min | "
          f"weekly={weekly} season={season} | scoring={SCORING} | {'DRY-RUN' if NO_GCS else 'GCS'}", flush=True)
    t0 = time.monotonic()
    done = skipped = 0
    all_fail = []
    for year in range(YEAR_START, YEAR_END + 1):
        for week in weeks:
            if _exists(year, week):
                skipped += 1
                continue
            df, failures = harvest(year, week)
            all_fail += failures
            if df is not None:
                _write(df, year, week)
                done += 1
                el = time.monotonic() - t0
                print(f"  {year} {'season' if week is None else f'wk{week:02d}'}: "
                      f"{df.height} rows ({df['position'].n_unique()} pos) | done={done} skip={skipped} "
                      f"fail={len(all_fail)} | {el/60:.1f}m", flush=True)
            else:
                print(f"  {year} {'season' if week is None else f'wk{week:02d}'}: empty (no data / pre-floor)", flush=True)
    print(f"\nDONE: wrote {done}, skipped {skipped}, parse-failures {len(all_fail)} in "
          f"{(time.monotonic()-t0)/60:.1f}m", flush=True)
    for f in all_fail:
        print("  parse-fail:", f, flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "audit":
        audit()
    else:
        main()
