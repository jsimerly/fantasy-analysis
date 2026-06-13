"""KTC league-overview crawler — pull KTC's team values for many Sleeper leagues.

KTC's power-rankings page renders a Sleeper league on demand:
  GET https://keeptradecut.com/dynasty/power-rankings/league-overview?leagueId=<id>&platform=2
returning a ~4 MB HTML page. ~98% of that is `playersArray` — KTC's GLOBAL player value
database, byte-identical on every page. The league-specific payload is tiny and is what we
keep per league:
  leagueTeams  -> per-team {teamId, name, total (KTC team value), average,
                  platformPowerRank, playerIds, draftPicks}   (the bag AND the target)
  leagueInfo   -> {name, franchises, isAuction, isStartup, numberOfRosters, starters, ...}
  scalars      -> thisLeagueId, thisPlatform, thisTEP, currentYear, lastUpdatedMilliseconds
The global playersArray is captured ONCE per scrape (separate file), not 10K times.

Input league IDs come from the Sleeper crawl (bronze/sleeper_crawl/leagues). Output:
  bronze/ktc_crawl/league_overview/load_date=<DATE>/data.parquet   (one row per league)
  bronze/ktc_crawl/players/load_date=<DATE>/data.parquet           (global playersArray, once)
Local crash-safe mirror + resume in analysis/_cache/ktc_crawl/ (already-fetched IDs are skipped).

Be a good citizen: a conservative request rate (KTC_RATE_PER_MIN, default 90) with 429/5xx
backoff. Env: GCS_BUCKET_NAME, KTC_PLATFORM (2), KTC_RATE_PER_MIN, KTC_MAX (0=all),
KTC_NO_GCS (1=skip GCS), KTC_LEAGUES_PATH (override input parquet/glob).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import time
from pathlib import Path

import polars as pl
import requests
from google.cloud import storage

URL = "https://keeptradecut.com/dynasty/power-rankings/league-overview"
HDR = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://keeptradecut.com/dynasty/power-rankings",
}
BUCKET = os.environ.get("GCS_BUCKET_NAME", "nfl-data-bronze")
PLATFORM = os.environ.get("KTC_PLATFORM", "2")
RATE_PER_MIN = int(os.environ.get("KTC_RATE_PER_MIN", "90"))
MAX = int(os.environ.get("KTC_MAX", "0"))
NO_GCS = os.environ.get("KTC_NO_GCS") == "1"
LOAD_DATE = dt.date.today().isoformat()
LOCAL_DIR = Path(__file__).resolve().parents[2] / "analysis" / "_cache" / "ktc_crawl"
LEAGUE_VARS = ("leagueTeams", "leagueInfo", "thisLeagueId", "thisPlatform",
               "thisTEP", "currentYear", "lastUpdatedMilliseconds")


class _Rate:
    def __init__(self, per_min: int):
        self.interval = 60.0 / max(per_min, 1)
        self.last = 0.0

    def wait(self):
        gap = time.monotonic() - self.last
        if gap < self.interval:
            time.sleep(self.interval - gap)
        self.last = time.monotonic()


_rate = _Rate(RATE_PER_MIN)


def extract_var(html: str, name: str):
    """Pull `var name = <json>` out of the page via balanced-bracket scan.
    Returns parsed JSON (list/dict) or a stripped scalar string, else None."""
    m = re.search(r"var\s+" + re.escape(name) + r"\s*=\s*", html)
    if not m:
        return None
    i = m.end()
    # scalar (number / quoted string up to ;) vs JSON array/object
    while i < len(html) and html[i] in " \t\r\n":
        i += 1
    if i >= len(html):
        return None
    if html[i] not in "[{":
        end = html.find(";", i)
        return html[i:end].strip().strip('"') if end > 0 else None
    depth = 0
    instr = False
    esc = False
    j = i
    while j < len(html):
        c = html[j]
        if esc:
            esc = False
        elif c == "\\":
            esc = True
        elif c == '"':
            instr = not instr
        elif not instr:
            if c in "[{":
                depth += 1
            elif c in "]}":
                depth -= 1
                if depth == 0:
                    break
        j += 1
    try:
        return json.loads(html[i:j + 1])
    except json.JSONDecodeError:
        return None


def fetch(league_id: str, tries: int = 4) -> tuple[int, str | None]:
    for t in range(tries):
        _rate.wait()
        try:
            r = requests.get(URL, params={"leagueId": league_id, "platform": PLATFORM},
                             headers=HDR, timeout=45)
        except requests.RequestException:
            time.sleep(min(2 ** t, 30))
            continue
        if r.status_code == 200:
            return 200, r.text
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(2 ** t, 30))
            continue
        return r.status_code, None
    return 0, None


def parse_league_row(league_id: str, status: int, html: str | None) -> dict:
    row = {"league_id": str(league_id), "platform": int(PLATFORM),
           "fetched_at": dt.datetime.utcnow().isoformat(), "http_status": status,
           "league_name": None, "n_teams": None, "tep": None, "current_year": None,
           "last_updated_ms": None, "league_info": None, "league_teams": None}
    if not html:
        return row
    teams = extract_var(html, "leagueTeams")
    info = extract_var(html, "leagueInfo")
    row["league_teams"] = json.dumps(teams) if teams is not None else None
    row["league_info"] = json.dumps(info) if info is not None else None
    row["n_teams"] = len(teams) if isinstance(teams, list) else None
    row["league_name"] = info.get("name") if isinstance(info, dict) else None
    row["tep"] = extract_var(html, "thisTEP")
    row["current_year"] = extract_var(html, "currentYear")
    row["last_updated_ms"] = extract_var(html, "lastUpdatedMilliseconds")
    return row


def players_rows(html: str) -> list[dict]:
    """Global playersArray -> one row/player: key scalars + full raw JSON (values nested)."""
    arr = extract_var(html, "playersArray")
    if not isinstance(arr, list):
        return []
    out = []
    for p in arr:
        out.append({"player_id": p.get("playerID"), "slug": p.get("slug"),
                    "player_name": p.get("playerName"), "position": p.get("position"),
                    "team": p.get("team"), "raw": json.dumps(p)})
    return out


def _load_target_leagues() -> list[str]:
    override = os.environ.get("KTC_LEAGUES_PATH")
    if override:
        df = pl.read_parquet(override)
    else:
        client = storage.Client()
        blobs = [b.name for b in client.list_blobs(BUCKET, prefix="bronze/sleeper_crawl/leagues/")
                 if b.name.endswith(".parquet")]
        if not blobs:
            raise SystemExit("no sleeper_crawl leagues found in GCS; run the Sleeper crawl first "
                             "or set KTC_LEAGUES_PATH")
        latest = max(blobs)  # load_date=YYYY-MM-DD sorts lexicographically
        df = pl.read_parquet(f"gs://{BUCKET}/{latest}")
    col = "league_id" if "league_id" in df.columns else df.columns[0]
    ids = df[col].cast(pl.Utf8).unique().drop_nulls().to_list()
    return ids


def _done_ids() -> set[str]:
    f = LOCAL_DIR / "league_overview.parquet"
    if f.exists():
        return set(pl.read_parquet(f)["league_id"].to_list())
    return set()


def _write_local(rows: list[dict], players: list[dict] | None):
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(LOCAL_DIR / "league_overview.parquet")
    if players:
        pl.DataFrame(players).write_parquet(LOCAL_DIR / "players.parquet")


def _write_gcs(rows: list[dict], players: list[dict] | None):
    root = f"gs://{BUCKET}/bronze/ktc_crawl"
    pl.DataFrame(rows).write_parquet(f"{root}/league_overview/load_date={LOAD_DATE}/data.parquet")
    if players:
        pl.DataFrame(players).write_parquet(f"{root}/players/load_date={LOAD_DATE}/data.parquet")
    print(f"  wrote GCS: {root}/league_overview & /players (load_date={LOAD_DATE})", flush=True)


def crawl():
    targets = _load_target_leagues()
    done = _done_ids()
    todo = [l for l in targets if l not in done]
    if MAX:
        todo = todo[:MAX]
    print(f"target leagues={len(targets)} already_done={len(done)} to_fetch={len(todo)} "
          f"| platform={PLATFORM} rate~{RATE_PER_MIN}/min", flush=True)

    # resume: keep prior rows
    prior = LOCAL_DIR / "league_overview.parquet"
    rows = pl.read_parquet(prior).to_dicts() if prior.exists() else []
    players: list[dict] = []
    pf = LOCAL_DIR / "players.parquet"
    have_players = pf.exists()
    t0 = time.monotonic()
    ok = err = 0

    for k, lid in enumerate(todo, 1):
        status, html = fetch(lid)
        rows.append(parse_league_row(lid, status, html))
        if html and status == 200:
            ok += 1
            if not have_players:  # capture global player DB once
                players = players_rows(html)
                have_players = bool(players)
        else:
            err += 1
        if k % 100 == 0:
            el = time.monotonic() - t0
            print(f"  {k:>5}/{len(todo)} ok={ok} err={err} {el/60:.1f}m "
                  f"({k/el*60:.0f}/min) eta~{(len(todo)-k)/(k/el)/60:.0f}m", flush=True)
        if k % 500 == 0:
            _write_local(rows, players if players else None)
            print(f"  [checkpoint] {len(rows)} league rows mirrored locally", flush=True)

    el = time.monotonic() - t0
    print(f"\nDONE: fetched {len(todo)} ({ok} ok, {err} err), {len(rows)} total rows, "
          f"players_captured={bool(players) or have_players} in {el/60:.1f}m", flush=True)
    _write_local(rows, players if players else None)
    if not NO_GCS:
        players_out = players if players else (pl.read_parquet(pf).to_dicts() if pf.exists() else None)
        _write_gcs(rows, players_out)
    return rows


if __name__ == "__main__":
    crawl()
