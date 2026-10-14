# adp_ingestion — average draft position by season (2009 on)

The preseason consensus with no survivorship problem: every player taken in a draft appears, every
year. Two public sources, both free and keyed well enough to join the lake (BACKLOG 38):

| Source | Years | Size | Key |
| --- | --- | --- | --- |
| Fantasy Football Calculator API (12-team; standard 2009-2011, PPR 2012 on) | 2009+ | ~200 players, 300-1,300 drafts a year | name + position (+ an FFC id) |
| MyFantasyLeague ADP export (PPR, 12-team, redraft) | 2011+ | 320-460 players, 2,000-9,000 drafts a year | MFL id → gsis through nflverse `fantasy_player_ids` (92 %) |

`backfill.py` writes `bronze/adp/<source>/season=<Y>/data.parquet`. **What each provider serves (checked
2026-10-13):** FFC has ONE window per year -- for a past year the frozen final preseason week (2025: 8,470 drafts,
Aug 25 - Sep 1), for the current year its latest window, which in season is a few dozen mock drafts (2026: 109,
Sep 18-25) -- so an FFC partition is written only from a preseason window (ends by Sep 10, 300+ drafts; the rows
carry `start_date` / `end_date`). MFL's export takes a period: `ALL` keeps growing all season, so the job pulls
`AUG15` (the late-preseason window, 600+ drafts a year, frozen at kickoff; the rows carry `period`). **Daily**
(`adp-daily`, bronze tier of the DAG, `ADP_SEASONS=live`): last season and this one every day -- last season is the
canary (a 400 / 404 on the coming season counts as "not published yet" only when last season answered) and FFC's
frozen final week catches up once the year is past; the model's `adp_next` and the in-season `preseason` group get
the new season the day it lands. By hand: `adp-yearly` (`ADP_SEASONS=current` or a range); `ADP_DRY_RUN=1`.
**Lake state:** 2009-2026 were backfilled 2026-10-12 with MFL `ALL`; MFL 2011-2026 were re-pulled 2026-10-14 at `AUG15` on
the owner's go (every partition carries `period`), FFC's past seasons rewritten from their frozen preseason windows
with their dates; FFC 2026 is still the 2026-10-12 in-season window (109 drafts, the quality check's known-open
row) and is unused -- the model's preseason group takes MFL wherever a season has it; FFC 2026 corrects itself when FFC freezes it (the
`bronze.adp.windows_are_preseason` check says which partitions are not a preseason view).
The model reads both (`inseason.preseason_features`: MFL by id where a season has it, FFC by name
otherwise) as the `preseason` group, and the career model will take the same columns per season.
