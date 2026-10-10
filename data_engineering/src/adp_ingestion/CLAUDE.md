# adp_ingestion — average draft position by season (2009 on)

The preseason consensus with no survivorship problem: every player taken in a draft appears, every
year. Two public sources, both free and keyed well enough to join the lake (BACKLOG 38):

| Source | Years | Size | Key |
| --- | --- | --- | --- |
| Fantasy Football Calculator API (12-team; standard 2009-2011, PPR 2012 on) | 2009+ | ~200 players, 300-1,300 drafts a year | name + position (+ an FFC id) |
| MyFantasyLeague ADP export (PPR, 12-team, redraft) | 2011+ | 320-460 players, 2,000-9,000 drafts a year | MFL id → gsis through nflverse `fantasy_player_ids` (92 %) |

`backfill.py` writes `bronze/adp/<source>/season=<Y>/data.parquet`; a yearly job (the view is final once
the season starts): `ADP_SEASONS=current` (default, Cloud Run job `adp-yearly`, run by hand or on a
yearly trigger after Labor Day) or `ADP_SEASONS=2009-2026` for the one-time backfill; `ADP_DRY_RUN=1`.
The model reads both (`inseason.preseason_features`: MFL by id where a season has it, FFC by name
otherwise) as the `preseason` group, and the career model will take the same columns per season.
