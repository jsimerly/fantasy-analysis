"""Is the lake ready for the weekly refresh? The gate the Tuesday task runs before it touches the GPU.

The refresh takes the week as the last one with stats in ``fact_player_week`` and never checks that the week is
over, so a run on a partial week (Thursday's game in, Sunday's not) would project the season from half a week.
This says, from the lake alone, whether the newest week is complete and still unpublished:

  ready (exit 0)  every game of the newest week in the schedule has its score, every team that played has stats
                  in silver for that week, and nothing is on the page for it yet
  wait  (exit 2)  a game of the week is unplayed, or its teams' stats have not reached silver (the daily DAG at
                  10:00 UTC carries nflverse's overnight build; MNF lands by ~06:15 Eastern), or the lake is unreachable
  done  (exit 3)  ``pages/season=S/week=W/`` already holds a projections.json: this week's refresh ran

One line on stdout, the reason; the task script polls on exit 2 and stops on 3 (the Wednesday trigger is the safety
net for a late DAG or a postponed Monday game). Owner, 2026-10-27: "lets get it so that we can grab the data as soon
as the week is over".
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

import gcs_io  # noqa: E402

WEEK_PATH = "silver/fantasy/fact_player_week/data.parquet"
SCHEDULE_PATH = "bronze/nflverse/schedules/season={season}/data.parquet"
READY, WAIT, DONE = 0, 2, 3
_PUBLISHED = re.compile(r"/run_date=(\d{4}-\d{2}-\d{2})/projections\.json$")


def newest_week(wk: pl.DataFrame) -> tuple[int, int]:
    """The refresh's own choice: the last week with stats in the weekly fact (``weekly_refresh.current_season_week``)."""
    season = int(wk["season"].max())
    return season, int(wk.filter(pl.col("season") == season)["week"].max())


def decide(wk: pl.DataFrame, sched: pl.DataFrame, published: list[str]) -> tuple[int, str]:
    """(exit code, reason) for the newest week in ``wk``. ``sched`` is that season's nflverse schedule (week, gameday,
    away_team, home_team, away_score, home_score); ``published`` the blob names under the week's pages prefix."""
    season, week = newest_week(wk)
    tag = f"{season} week {week}"
    games = sched.filter(pl.col("week") == week)
    if "season" in sched.columns:
        games = games.filter(pl.col("season") == season)
    if games.is_empty():
        return WAIT, f"{tag}: no games in the schedule"
    unplayed = games.filter(pl.col("away_score").is_null() | pl.col("home_score").is_null())
    if not unplayed.is_empty():
        g = unplayed.sort("gameday").row(0, named=True)
        return WAIT, f"{tag}: {unplayed.height} of {games.height} games unplayed (next {g['away_team']} @ {g['home_team']} {g['gameday']})"
    teams = set(games["away_team"].to_list()) | set(games["home_team"].to_list())
    have = set(wk.filter((pl.col("season") == season) & (pl.col("week") == week))["team"].to_list())
    missing = sorted(teams - have)
    if missing:
        return WAIT, f"{tag}: every game played but no stats in silver yet for {', '.join(missing)}"
    runs = sorted({m.group(1) for m in map(_PUBLISHED.search, published) if m})
    if runs:
        return DONE, f"{tag}: already published (run_date {runs[-1]})"
    return READY, f"{tag}: complete, {games.height} games and {len(teams)} teams with stats; not yet published"


def main() -> int:
    try:
        wk = gcs_io.read_lake(WEEK_PATH)
        season, week = newest_week(wk)
        sched = gcs_io.read_lake(SCHEDULE_PATH.format(season=season))
        published = gcs_io.list_ml("pages", f"season={season}", f"week={week}")
    except Exception as e:  # noqa: BLE001 -- unreachable lake = not ready, the caller retries
        print(f"lake unreachable: {type(e).__name__}: {e}", flush=True)
        return WAIT
    code, reason = decide(wk, sched, published)
    print(reason, flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
