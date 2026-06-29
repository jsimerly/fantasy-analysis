"""Sleeper league HISTORY crawler — deep harvest of the seeded leagues.

The breadth-first `league_crawler` gave us ~10k leagues for a single season (their IDs,
settings, current rosters). This crawler goes DEEP on that set: for every seeded league it
walks the lineage back through `previous_league_id` and, for each league-season, harvests the
full picture needed for modeling (predict a team's final finish) and for adjacent projects
(trade/waiver markets, draft/ADP, projection accuracy, manager behavior):

  per league-season
    season_meta     league settings/scoring/roster_positions/playoff_week_start/...
    matchups        weeks 1..REG: roster_id, points, players, starters, per-player points
    transactions    weeks 1..TXN: trades, waivers, FAAB bids, adds/drops
    brackets        winners + losers bracket -> final finish
    rosters         final standings (wins/losses/ties/fpts) + roster
    drafts + picks  draft order, slots, picks (ADP + rookie-draft slot labels)
    traded_picks    dynasty pick ownership
    users           manager display names (cross-league manager joins)

Player-level data (stats / projections) is intentionally NOT harvested here — that's
league-agnostic and may come from a richer source; this run is league data only.

Output: a distinct top-level prefix, NOT under bronze/sleeper/ (never swept into the curated
3-league ingestion). Sharded by the seed offset so the crawl can be run in chunks / resumed:
  bronze/sleeper_crawl/history/<entity>/load_date=<DATE>/part=<OFFSET>.parquet
A local mirror is written to analysis/_cache/sleeper_history/ as a crash-safe checkpoint.

Fetches run on a thread pool so we hit Sleeper's rate ceiling instead of being stuck at single-
request latency. A thread-safe limiter reserves request slots >= interval apart GLOBALLY, so the
combined rate across all workers stays under HIST_RATE_PER_MIN no matter the worker count (well
under Sleeper's 1000/min); 429/5xx/network errors back off + retry per request. Env:
  GCS_BUCKET_NAME (nfl-data-bronze), HIST_RATE_PER_MIN (900, global cap), HIST_WORKERS (8),
  HIST_NO_GCS (1=skip GCS), HIST_LEAGUE_OFFSET (0), HIST_MAX_LEAGUES (all from offset),
  HIST_FLUSH_EVERY (250 leagues), HIST_REG_WEEKS (18), HIST_TXN_WEEKS (18).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

import polars as pl
import requests
from google.cloud import storage

BASE_APP = "https://api.sleeper.app/v1"      # leagues, matchups, transactions, drafts, brackets
BUCKET = os.environ.get("GCS_BUCKET_NAME", "nfl-data-bronze")
RATE_PER_MIN = int(os.environ.get("HIST_RATE_PER_MIN", "900"))   # << Sleeper's 1000/min ceiling (global)
WORKERS = int(os.environ.get("HIST_WORKERS", "8"))               # parallel fetchers (rate stays capped)
NO_GCS = os.environ.get("HIST_NO_GCS") == "1"
OFFSET = int(os.environ.get("HIST_LEAGUE_OFFSET", "0"))
MAX_LEAGUES = int(os.environ.get("HIST_MAX_LEAGUES", "0")) or None   # 0/unset -> all from OFFSET
FLUSH_EVERY = int(os.environ.get("HIST_FLUSH_EVERY", "100"))
PROGRESS_EVERY = int(os.environ.get("HIST_PROGRESS_EVERY", "10"))   # progress line cadence (leagues)
REG_WEEKS = int(os.environ.get("HIST_REG_WEEKS", "18"))
TXN_WEEKS = int(os.environ.get("HIST_TXN_WEEKS", "18"))
LOAD_DATE = dt.date.today().isoformat()
LOCAL_DIR = Path(__file__).resolve().parents[2] / "analysis" / "_cache" / "sleeper_history"

# entities written by this crawler
ENTITIES = ["season_meta", "matchups", "transactions", "brackets", "rosters",
            "drafts", "draft_picks", "traded_picks", "users"]


class _Rate:
    """Thread-safe GLOBAL rate cap. Each call reserves the next request slot >= `interval` after
    the previous one (under a lock), then sleeps to it OUTSIDE the lock so requests overlap. With
    N workers the combined issue rate can never exceed RATE_PER_MIN — the slot reservation is the
    hard ceiling, independent of worker count."""

    def __init__(self, per_min: int):
        self.interval = 60.0 / max(per_min, 1)
        self.lock = threading.Lock()
        self.nxt = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            target = self.nxt if self.nxt > now else now
            self.nxt = target + self.interval
        delay = target - time.monotonic()
        if delay > 0:
            time.sleep(delay)


_rate = _Rate(RATE_PER_MIN)
_count_lock = threading.Lock()
_calls = 0
_misses = 0       # endpoints abandoned after exhausting all retries (visibility into 429 storms)
_throttled = 0    # count of 429 responses (Sleeper rate-limiting us -> slowdown is real, not a bug)


def _retry_wait(resp, t: int) -> float:
    """Seconds before the next retry: honor Sleeper's Retry-After header on a 429 if present,
    else exponential backoff (1,2,4,... capped at 60s)."""
    if resp is not None:
        ra = resp.headers.get("Retry-After")
        if ra:
            try:
                return min(float(ra), 120.0)
            except ValueError:
                pass
    return min(2.0 ** t, 60.0)


def _get(url: str, tries: int = 7):
    """GET with proactive rate-limiting + resilient retry. Retries 429 / 5xx / network errors
    with backoff (honoring Retry-After on 429); 404 is a clean miss (no retry). After `tries`
    exhausted it counts a miss + warns (so a 429 storm or outage never fails silently) and
    returns None -> the caller degrades to empty for that entity and the crawl continues."""
    global _calls, _misses, _throttled
    for t in range(tries):
        _rate.wait()
        with _count_lock:
            _calls += 1
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
        except requests.RequestException:
            time.sleep(min(2.0 ** t, 60.0))
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                return None
        if r.status_code == 404:
            return None
        if r.status_code == 429 or r.status_code >= 500:
            if r.status_code == 429:
                with _count_lock:
                    _throttled += 1
            time.sleep(_retry_wait(r, t))
            continue
        return None   # other 4xx — not retryable
    with _count_lock:
        _misses += 1
        miss = _misses
    print(f"  ! gave up after {tries} tries (miss #{miss}): {url}", flush=True)
    return None


def _j(v):
    """JSON-encode a nested value for a flat parquet column (None stays None)."""
    return json.dumps(v) if v is not None else None


# --------------------------------------------------------------------------- inputs
def _seed_leagues() -> list[dict]:
    """The crawled league set (one season per lineage). Local mirror preferred, else GCS."""
    local = Path(__file__).resolve().parents[2] / "analysis" / "_cache" / "sleeper_crawl" / "leagues.parquet"
    if local.exists():
        df = pl.read_parquet(local)
    else:
        from google.cloud import storage as _st
        c = _st.Client()
        names = sorted(b.name for b in c.list_blobs(BUCKET, prefix="bronze/sleeper_crawl/leagues/"))
        df = pl.read_parquet(f"gs://{BUCKET}/{names[-1]}")
    rows = df.to_dicts()
    rows.sort(key=lambda r: str(r.get("league_id")))      # stable order for offset sharding
    return rows


def _as_league_dict(seed_row: dict) -> dict:
    """Normalize a cached seed row (settings are JSON strings) back to a league-like dict."""
    def _load(v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except ValueError:
                return None
        return v
    return {
        "league_id": str(seed_row.get("league_id")),
        "name": seed_row.get("name"),
        "season": str(seed_row.get("season")),
        "status": seed_row.get("status"),
        "sport": seed_row.get("sport"),
        "total_rosters": seed_row.get("total_rosters"),
        "previous_league_id": seed_row.get("previous_league_id"),
        "draft_id": seed_row.get("draft_id"),
        "settings": _load(seed_row.get("settings")),
        "scoring_settings": _load(seed_row.get("scoring_settings")),
        "roster_positions": _load(seed_row.get("roster_positions")),
    }


def _walk_lineage(seed: dict, seen: set[str]) -> list[dict]:
    """seed + every ancestor via previous_league_id (fetched fresh). Skips already-seen ids."""
    chain, lg = [], seed
    while lg and str(lg.get("league_id")) not in seen:
        lid = str(lg.get("league_id"))
        seen.add(lid)
        chain.append(lg)
        prev = lg.get("previous_league_id")
        lg = _get(f"{BASE_APP}/league/{prev}") if prev else None
    return chain


# --------------------------------------------------------------------------- row builders
def _meta_row(lg: dict) -> dict:
    s = lg.get("settings") or {}
    return {
        "league_id": str(lg.get("league_id")), "season": str(lg.get("season")),
        "name": lg.get("name"), "sport": lg.get("sport"), "status": lg.get("status"),
        "total_rosters": lg.get("total_rosters"), "previous_league_id": lg.get("previous_league_id"),
        "draft_id": lg.get("draft_id"), "bracket_id": lg.get("bracket_id"),
        "loser_bracket_id": lg.get("loser_bracket_id"),
        "playoff_week_start": s.get("playoff_week_start"),
        "playoff_teams": s.get("playoff_teams"), "num_teams": s.get("num_teams"),
        "settings": _j(lg.get("settings")), "scoring_settings": _j(lg.get("scoring_settings")),
        "roster_positions": _j(lg.get("roster_positions")),
    }


def _harvest_season(lg: dict, sink: dict[str, list], lineage_id: str):
    """Fetch + flatten every per-league-season entity into `sink`, stamping `league_lineage_id`
    (the chain's root/originating league_id — matches dim_leagues_meta's convention) on every
    row so the crawl is lineage-keyed as it lands (no post-hoc lineage reconstruction)."""
    lid, season = str(lg.get("league_id")), str(lg.get("season"))
    _before = {e: len(sink[e]) for e in sink}
    s = lg.get("settings") or {}
    pws = s.get("playoff_week_start") or 15
    last_reg = min(max(int(pws) - 1, 1), REG_WEEKS)

    sink["season_meta"].append(_meta_row(lg))

    # matchups (regular-season state trajectory)
    for wk in range(1, last_reg + 1):
        for m in _get(f"{BASE_APP}/league/{lid}/matchups/{wk}") or []:
            sink["matchups"].append({
                "league_id": lid, "season": season, "week": wk,
                "roster_id": m.get("roster_id"), "matchup_id": m.get("matchup_id"),
                "points": m.get("points"), "custom_points": m.get("custom_points"),
                "players": _j(m.get("players")), "starters": _j(m.get("starters")),
                "players_points": _j(m.get("players_points")),
                "starters_points": _j(m.get("starters_points")),
            })

    # transactions (all moves: trades / waivers / FAAB / adds-drops)
    for wk in range(1, TXN_WEEKS + 1):
        for t in _get(f"{BASE_APP}/league/{lid}/transactions/{wk}") or []:
            sink["transactions"].append({
                "league_id": lid, "season": season, "leg": wk,
                "transaction_id": t.get("transaction_id"), "type": t.get("type"),
                "status": t.get("status"), "created": t.get("created"),
                "roster_ids": _j(t.get("roster_ids")), "consenter_ids": _j(t.get("consenter_ids")),
                "adds": _j(t.get("adds")), "drops": _j(t.get("drops")),
                "draft_picks": _j(t.get("draft_picks")), "waiver_budget": _j(t.get("waiver_budget")),
                "settings": _j(t.get("settings")), "metadata": _j(t.get("metadata")),
            })

    # brackets (final finish)
    for kind, path in (("winners", "winners_bracket"), ("losers", "losers_bracket")):
        for b in _get(f"{BASE_APP}/league/{lid}/{path}") or []:
            sink["brackets"].append({
                "league_id": lid, "season": season, "bracket": kind,
                "round": b.get("r"), "match_id": b.get("m"), "t1": b.get("t1"), "t2": b.get("t2"),
                "w": b.get("w"), "l": b.get("l"), "p": b.get("p"),
                "t1_from": _j(b.get("t1_from")), "t2_from": _j(b.get("t2_from")),
            })

    # final rosters (standings)
    for r in _get(f"{BASE_APP}/league/{lid}/rosters") or []:
        sink["rosters"].append({
            "league_id": lid, "season": season, "roster_id": r.get("roster_id"),
            "owner_id": r.get("owner_id"), "co_owners": _j(r.get("co_owners")),
            "players": _j(r.get("players")), "starters": _j(r.get("starters")),
            "reserve": _j(r.get("reserve")), "taxi": _j(r.get("taxi")),
            "keepers": _j(r.get("keepers")), "settings": _j(r.get("settings")),
        })

    # users (manager metadata)
    for u in _get(f"{BASE_APP}/league/{lid}/users") or []:
        sink["users"].append({
            "league_id": lid, "season": season, "user_id": u.get("user_id"),
            "display_name": u.get("display_name"), "avatar": u.get("avatar"),
            "is_owner": u.get("is_owner"), "metadata": _j(u.get("metadata")),
        })

    # traded picks (dynasty pick ownership)
    for tp in _get(f"{BASE_APP}/league/{lid}/traded_picks") or []:
        sink["traded_picks"].append({
            "league_id": lid, "season": season, "pick_season": tp.get("season"),
            "round": tp.get("round"), "roster_id": tp.get("roster_id"),
            "previous_owner_id": tp.get("previous_owner_id"), "owner_id": tp.get("owner_id"),
        })

    # drafts + picks (order, slots, ADP, rookie-draft slot labels)
    for d in _get(f"{BASE_APP}/league/{lid}/drafts") or []:
        did = d.get("draft_id")
        sink["drafts"].append({
            "league_id": lid, "season": season, "draft_id": did, "type": d.get("type"),
            "status": d.get("status"), "start_time": d.get("start_time"),
            "settings": _j(d.get("settings")), "metadata": _j(d.get("metadata")),
            "draft_order": _j(d.get("draft_order")), "slot_to_roster_id": _j(d.get("slot_to_roster_id")),
        })
        for p in _get(f"{BASE_APP}/draft/{did}/picks") or []:
            sink["draft_picks"].append({
                "league_id": lid, "season": season, "draft_id": did,
                "pick_no": p.get("pick_no"), "round": p.get("round"), "draft_slot": p.get("draft_slot"),
                "roster_id": p.get("roster_id"), "player_id": p.get("player_id"),
                "picked_by": p.get("picked_by"), "is_keeper": p.get("is_keeper"),
                "metadata": _j(p.get("metadata")),
            })

    # stamp the lineage id on every row added for this league-season
    for ent, lst in sink.items():
        for row in lst[_before[ent]:]:
            row["league_lineage_id"] = lineage_id


# --------------------------------------------------------------------------- output
_part = 0
_gcs_bucket = None


def _bucket():
    global _gcs_bucket
    if _gcs_bucket is None:
        _gcs_bucket = storage.Client().bucket(BUCKET)
    return _gcs_bucket


def _flush(sink: dict[str, list], totals: dict[str, int]):
    """Persist each entity's accumulated rows as a part file, then clear that buffer. The parquet
    is written LOCALLY first (fast, reliable), then uploaded to GCS via the storage client with a
    timeout + built-in retry — NOT polars' direct gs:// path, which can hang forever on a large
    upload with no timeout. A per-entity try/except means a transient write failure is logged and
    the rows are KEPT (retried on the next flush) rather than lost or hanging the whole run. The
    local files double as a crash-safe mirror. Parts read back as one dataset per entity."""
    global _part
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    wrote = []
    for ent in ENTITIES:
        rows = sink.get(ent) or []
        if not rows:
            continue
        n = len(rows)                       # capture before clear (rows aliases sink[ent])
        part = f"{OFFSET:06d}_{_part:04d}"
        local_path = LOCAL_DIR / f"{ent}__{part}.parquet"
        try:
            # infer_schema_length=None scans ALL rows, so a column that's None in the first rows
            # then has a value later (custom_points, is_keeper, ...) types correctly instead of
            # failing to append. strict=False coerces the odd mixed scalar rather than erroring.
            pl.DataFrame(rows, infer_schema_length=None, strict=False).write_parquet(local_path)
            if not NO_GCS:
                blob = _bucket().blob(
                    f"bronze/sleeper_crawl/history/{ent}/load_date={LOAD_DATE}/part={part}.parquet")
                blob.upload_from_filename(str(local_path), timeout=300)   # retries + hard timeout
            totals[ent] += n
            sink[ent].clear()               # only on success -> no data loss on a failed write
            wrote.append(f"{ent}={n}")
        except Exception as e:
            print(f"  ! flush {ent} failed (kept in buffer, retry next flush): {e}", flush=True)
    if wrote:
        print(f"  [flush {OFFSET:06d}_{_part:04d}] " + " ".join(wrote), flush=True)
    _part += 1


def _harvest_league(seed_row: dict) -> dict[str, list]:
    """Worker: walk one seed's lineage + harvest every season into a LOCAL sink (returned to the
    main thread). The only shared state touched is the rate limiter and call counters, both
    thread-safe; the sink is per-worker so there's no cross-thread contention on the buffers."""
    local: dict[str, list] = {e: [] for e in ENTITIES}
    try:
        chain = _walk_lineage(_as_league_dict(seed_row), set())     # fresh per-chain cycle guard
        lineage_id = str(chain[-1]["league_id"]) if chain else None  # root/originating league
        for lg in chain:
            try:
                _harvest_season(lg, local, lineage_id)
            except Exception as e:
                print(f"  ! {lg.get('league_id')} {lg.get('season')}: {e}", flush=True)
    except Exception as e:
        print(f"  ! seed {seed_row.get('league_id')}: {e}", flush=True)
    return local


def _prevent_sleep():
    """Stop the machine from sleeping mid-crawl (Windows). The OS suspends the process on sleep,
    freezing the run for hours (time.monotonic keeps counting, so the rate column looks cratered).
    This holds the SYSTEM awake for the run; the display may still turn off. No-op off Windows."""
    if os.name != "nt":
        return
    try:
        import ctypes
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001)  # ES_CONTINUOUS|ES_SYSTEM_REQUIRED
        print("keep-awake enabled (system won't sleep while crawling; screen may still turn off)", flush=True)
    except Exception as e:
        print(f"(could not enable keep-awake: {e} — set Windows sleep to Never to be safe)", flush=True)


def _take(it, n):
    """Pull up to n items from an iterator (used to prime the bounded sliding window)."""
    out = []
    for _ in range(n):
        x = next(it, None)
        if x is None:
            break
        out.append(x)
    return out


def crawl():
    seeds = _seed_leagues()
    sl = seeds[OFFSET: (OFFSET + MAX_LEAGUES) if MAX_LEAGUES else None]
    print(f"seed leagues={len(seeds)} | this shard offset={OFFSET} n={len(sl)} | "
          f"rate<={RATE_PER_MIN}/min global | workers={WORKERS} | "
          f"reg_weeks<={REG_WEEKS} txn_weeks<={TXN_WEEKS}", flush=True)
    _prevent_sleep()

    sink: dict[str, list] = {e: [] for e in ENTITIES}
    totals: dict[str, int] = {e: 0 for e in ENTITIES}
    t0 = time.monotonic()
    prev_t, prev_calls = t0, 0       # for the recent (not lifetime-average) rate
    done = 0

    # Workers fetch in parallel (rate-capped globally); the MAIN thread alone merges each
    # finished league's rows into the shared buffer + flushes, so the buffer needs no lock.
    # Bounded sliding window: keep only ~2x workers in flight (submit one as each finishes), so a
    # stall or Ctrl-C tears down in seconds instead of waiting on 10k queued futures.
    it = iter(sl)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        inflight = {ex.submit(_harvest_league, r) for r in _take(it, WORKERS * 2)}
        while inflight:
            ready, inflight = wait(inflight, return_when=FIRST_COMPLETED)
            for fut in ready:
                local = fut.result()
                for e in ENTITIES:
                    if local[e]:
                        sink[e].extend(local[e])
                done += 1
                nxt = next(it, None)
                if nxt is not None:
                    inflight.add(ex.submit(_harvest_league, nxt))
                if done % PROGRESS_EVERY == 0:
                    now = time.monotonic(); el = now - t0; gap = now - prev_t
                    recent = (_calls - prev_calls) / max(gap, 1) * 60   # true current rate, not lifetime avg
                    if gap > 300:    # >5 min for one batch -> the process was frozen (machine slept)
                        print(f"  !! {gap/60:.0f} min gap — process was frozen (machine likely slept); "
                              f"data is safe, continuing", flush=True)
                    print(f"  leagues={done:>5}/{len(sl)} seasons={totals['season_meta']+len(sink['season_meta']):>6} "
                          f"matchups={totals['matchups']+len(sink['matchups']):>8} "
                          f"txns={totals['transactions']+len(sink['transactions']):>8} "
                          f"calls={_calls:>7} throttled={_throttled} misses={_misses} "
                          f"{recent:.0f}/min (avg {_calls/max(el,1)*60:.0f}/min) {el/60:.1f}m", flush=True)
                    prev_t, prev_calls = now, _calls
                if done % FLUSH_EVERY == 0:
                    _flush(sink, totals)

    _flush(sink, totals)        # final partial
    el = time.monotonic() - t0
    print(f"\nDONE shard offset={OFFSET}: "
          + ", ".join(f"{e}={totals[e]}" for e in ENTITIES)
          + f" | {_calls} calls, {_throttled} throttled, {_misses} misses in {el/60:.1f}m", flush=True)
    return totals


if __name__ == "__main__":
    crawl()
