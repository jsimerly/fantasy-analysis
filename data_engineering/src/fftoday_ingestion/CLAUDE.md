# fftoday_ingestion — FFToday weekly projections (2010 on)

FFToday serves its weekly projections for every season from 2010 on its own pages
(`rankings/playerwkproj.php?Season=&GameWeek=&PosID=`; 2009 and earlier return an empty shell), with
the stat line, the FFToday player id and the injury designation of the day. That makes it the weekly
consensus history before Sleeper's 2018 floor, point-in-time and with no survivorship (every player
projected that week is on the page) — the gap the model's consensus group needed (BACKLOG 38).

| File | What |
| --- | --- |
| `_parse.py` | pure parser: `parse_week_page(html, season, week, position)` → one row per player (id, name, team, opponent, injury, the position's stat line, FPts); spec-tested on the site's own markup in `tests/fftoday/` |
| `backfill.py` | the local, run-by-hand scraper: `FFT_SEASONS=2010-2017` (or `current`), `FFT_DRY_RUN=1`; polite pacing in the style of the other scrapers; writes `bronze/fftoday/projections/season=<Y>/data.parquet` |

## Notes
- A page lists the projected players of the position for that week (28 QBs, ~50 RBs / WRs / TEs in 2010;
  no pagination). FFToday ids map to nflverse by name + team + season (no id bridge); the parser keeps
  `fft_id` so a crosswalk can be built once and reused.
- Single-source projections (FFToday's own), not a multi-expert consensus; Sleeper's (2018 on) and the
  FantasyPros ECR history (2019-12 on, nflverse `fantasy_rankings_history`) are the other lenses.
- Not in the daily DAG: the history is static once a season is over; `FFT_SEASONS=current` in season is
  the owner's call.
