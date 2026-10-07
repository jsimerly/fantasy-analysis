"""In-season daily live capture of FantasyPros projections (Cloud Run job).

Captures the season-long + current-week LIVE projections, tagged `as_of_date=today` (written
to an `as_of=` sub-partition), so the **evolving** consensus through the week is recorded.
Full-roster for the in-progress season (nobody has left it yet) — no survivorship problem; this
is how future seasons never go sparse. **Self-gates to the NFL season** (preseason → playoffs)
and exits in the offseason, so a plain daily scheduler is fine.

Run: `python -m fantasypros_ingestion.live_current`
"""
from __future__ import annotations

import datetime as dt

from fantasypros_ingestion import _harvest as H


def _d(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y%m%d").date()


def season_window(gd: dict, today: dt.date):
    """(season) if `today` is in-season (preseason ~30d before Wk1 → ~35d after the last week),
    else None. Season label: July+ = this year, else last year."""
    season = today.year if today.month >= 7 else today.year - 1
    wk1 = gd.get((season, 1))
    last = gd.get((season, 18)) or gd.get((season, 17))
    if not wk1:
        return None
    start = _d(wk1) - dt.timedelta(days=30)
    end = (_d(last) + dt.timedelta(days=35)) if last else dt.date(today.year, 2, 15)
    return season if start <= today <= end else None


def current_week(gd: dict, season: int, today: dt.date):
    """Latest week whose lead-up (gameday-6d) has started by `today` (None preseason)."""
    cur = None
    for w in range(1, 19):
        g = gd.get((season, w))
        if g and _d(g) <= today + dt.timedelta(days=6):
            cur = w
    return cur


def main() -> None:
    gd = H.load_week_gamedays()
    today = dt.date.today()
    season = season_window(gd, today)
    if season is None:
        print(f"{today}: offseason — nothing to capture", flush=True)
        return
    week = current_week(gd, season, today)
    as_of = today.strftime("%Y%m%d")
    targets = [None, week] if week else [None]   # season-long always; current week once underway
    for wk in targets:
        if H.exists(season, wk, as_of=as_of):    # already captured today
            continue
        df = H.assemble_slot(season, wk, {}, do_live=True, live_as_of=as_of)
        tag = "season" if wk is None else f"wk{wk:02d}"
        if df is not None:
            H.write(df, season, wk, as_of=as_of)
            print(f"{season} {tag} live: {df.height} rows as_of={as_of}", flush=True)
        else:
            print(f"{season} {tag}: empty", flush=True)


if __name__ == "__main__":
    main()
