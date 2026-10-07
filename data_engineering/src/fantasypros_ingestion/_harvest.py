"""Harvest + union FantasyPros projections into bronze parquet.

**Snapshot-driven** Wayback: one CDX query per (pos, season) lists every capture; each is
assigned to the NFL week it pre-dates (latest per week kept = most-informed pre-game), plus a
season-long preseason slot. This harvests exactly what's archived (coverage is patchy year to
year — e.g. 2015 ≈ 18 weeks, 2013 ≈ a handful) without wasted per-week queries. The live site
is unioned in for survivor density (Wayback preferred: real pre-game date + as-of team).
Resumable: existing partitions are skipped.
"""
from __future__ import annotations

import datetime as dt
import io
import os

import polars as pl
from google.cloud import storage

from fantasypros_ingestion import _sources as S
from fantasypros_ingestion._parse import parse_projection_page


def _bucket() -> str:
    return os.environ.get("GCS_BUCKET_NAME") or "nfl-data-bronze"


def _d(s: str) -> dt.date:
    return dt.datetime.strptime(s[:8], "%Y%m%d").date()


# --------------------------------------------------- schedule week -> gameday map
def load_week_gamedays() -> dict[tuple[int, int], str]:
    """(season, week) -> first REG gameday 'YYYYMMDD', from bronze nflverse schedules."""
    cl = storage.Client()
    names = [b.name for b in cl.list_blobs(_bucket(), prefix="bronze/nflverse/schedules/")
             if b.name.endswith(".parquet")]
    sch = pl.concat([pl.read_parquet(io.BytesIO(cl.bucket(_bucket()).blob(n).download_as_bytes()))
                     for n in names], how="diagonal_relaxed")
    sch = sch.filter(pl.col("game_type") == "REG").select(
        pl.col("season").cast(pl.Int64), pl.col("week").cast(pl.Int64),
        pl.col("gameday").cast(pl.Utf8).str.slice(0, 10).str.replace_all("-", "").alias("g"))
    g = sch.group_by("season", "week").agg(pl.col("g").min().alias("g"))
    return {(r["season"], r["week"]): r["g"] for r in g.iter_rows(named=True)}


# ------------------------------------------------------- snapshot -> week slot
def _assign_slot(date: dt.date, wk_gd: dict[int, dt.date]):
    """Which slot a snapshot taken on ``date`` belongs to: an int week (pre-game for the next
    games, within ~9d, else the next week as a stale fill), ``None`` (preseason/season-long),
    or ``'skip'`` (after the season)."""
    g1 = min(wk_gd.values())
    future = sorted(((w, g) for w, g in wk_gd.items() if g >= date), key=lambda x: x[1])
    if not future:
        return "skip"
    w, g = future[0]
    if (g - date).days <= 9:
        return w
    if date < g1 - dt.timedelta(days=9):
        return None
    return w


def harvest_wayback_season(pos, season, gamedays) -> dict:
    """{slot -> rows} for one (pos, season) from all Wayback captures (slot = int week | None).
    One CDX query; fetch only the latest capture per slot."""
    wk_gd = {w: _d(gamedays[(season, w)]) for w in range(1, 19) if (season, w) in gamedays}
    if not wk_gd:
        return {}
    g1, glast = min(wk_gd.values()), max(wk_gd.values())
    frm = (g1 - dt.timedelta(days=45)).strftime("%Y%m%d")     # include preseason
    to = (glast + dt.timedelta(days=10)).strftime("%Y%m%d")
    chosen: dict = {}                                          # slot -> (ts, orig), latest ts
    for ts, orig in S.cdx_snapshots(pos, frm, to):
        slot = _assign_slot(_d(ts), wk_gd)
        if slot == "skip":
            continue
        if slot not in chosen or ts > chosen[slot][0]:
            chosen[slot] = (ts, orig)
    out = {}
    for slot, (ts, orig) in chosen.items():
        rows = _rows(S.wayback_fetch(ts, orig), "wayback", ts[:8])
        if rows:
            out[slot] = rows
    return out


# ----------------------------------------------------------------- harvest a page
def _rows(html, source, as_of):
    rows, _, status = parse_projection_page(html)
    if status != "ok":
        return []
    for r in rows:
        r["source"] = source
        r["as_of_date"] = as_of
    return rows


def harvest_live(pos, season, week, rate=4.0, as_of=None):
    """Live FantasyPros page for (pos, year, week|None), parsed. Survivors only."""
    return _rows(S.live_fetch(S.live_url(pos, season, week), rate_per_min=rate), "live", as_of)


def _key(r):
    return r.get("fp_id") or (str(r.get("player", "")).lower().strip(), str(r.get("team", "")).upper())


def union(wb_rows, live_rows):
    """wayback ∪ live by player key; Wayback wins (as-of team + guaranteed pre-game)."""
    out = {_key(r): r for r in live_rows}
    for r in wb_rows:
        out[_key(r)] = r
    return list(out.values())


def _to_frame(rows, season, slot, pos):
    return pl.DataFrame(rows, infer_schema_length=None).with_columns(
        pl.lit(season).alias("season"), pl.lit(slot, dtype=pl.Int64).alias("week"),
        pl.lit(pos).alias("position"), pl.lit(S.SCORING).alias("scoring"))


def assemble_slot(season, slot, wb_by_pos, do_live=False, live_rate=4.0, live_as_of=None):
    """One (season, slot) across positions -> a unioned DataFrame (or None). ``wb_by_pos`` is
    {pos: {slot: rows}} from `harvest_wayback_season` (empty for live-only capture)."""
    frames = []
    for pos in S.POSITIONS:
        wb = wb_by_pos.get(pos, {}).get(slot, [])
        lv = harvest_live(pos, season, slot, live_rate, live_as_of) if do_live else []
        merged = union(wb, lv)
        if merged:
            frames.append(_to_frame(merged, season, slot, pos))
    return pl.concat(frames, how="diagonal_relaxed") if frames else None


# ----------------------------------------------------------------- GCS write/resume
def out_path(season, week, as_of=None) -> str:
    """Backfill (as_of=None): one partition per (season, week). Going-forward daily capture
    (as_of='YYYYMMDD'): an extra `as_of=` sub-partition so each day's snapshot is kept."""
    base = (f"bronze/fantasypros/projections/weekly/season={season}/week={week:02d}"
            if week else f"bronze/fantasypros/projections/season/season={season}")
    return f"{base}/as_of={as_of}/data.parquet" if as_of else f"{base}/data.parquet"


def exists(season, week, as_of=None) -> bool:
    return storage.Client().bucket(_bucket()).blob(out_path(season, week, as_of)).exists()


def write(df: pl.DataFrame, season, week, as_of=None) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf)
    storage.Client().bucket(_bucket()).blob(out_path(season, week, as_of)).upload_from_string(
        buf.getvalue(), content_type="application/octet-stream")
