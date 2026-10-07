"""replacement.league_lineup: the newest league of the lineage wins over a later-stamped row keyed by
the lineage's root league id (the legacy SCD2 trigger's artifact, which carried 2 WR and a -1 fumble)."""
from datetime import datetime

import polars as pl

import replacement


def test_newest_league_wins_over_newest_stamp():
    s = pl.DataFrame({
        "league_id": ["730630605066371072", "1342619554194423808", "1180221337891495936"],
        "league_lineage_id": ["730630605066371072"] * 3,
        "is_current": [True, True, True],
        "valid_from": [datetime(2026, 10, 7, 11, 2), datetime(2026, 10, 1, 9, 4), datetime(2026, 10, 7, 10, 8)],
        "qb_slots": [1, 1, 1], "rb_slots": [2, 2, 2], "wr_slots": [2, 3, 3], "te_slots": [1, 1, 1],
        "flex_slots": [1, 1, 1], "superflex_slots": [1, 1, 1], "num_teams": [10, 10, 10],
    })
    slots, teams = replacement.league_lineup(s, "730630605066371072")
    assert slots.get("WR") == 3 and teams == 10
