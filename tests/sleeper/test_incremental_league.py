"""sleeper_ingestion/daily/incremental_league.py

flatten_league_to_parquets splits one league payload into leagues / settings /
scoring / roster-slot frames. The scoring frame is currently built from a stray
singular variable, so a league with no scoring_settings raises -> spec fails.
"""
import polars as pl

from tests.de_loader import load_de_module

mod = load_de_module("sleeper_ingestion/daily/incremental_league.py", "sleeper_ingestion")
flatten_league_to_parquets = mod.flatten_league_to_parquets


def _league(with_scoring=True):
    league = {
        "league_id": "L1", "name": "My League", "season": "2024",
        "status": "in_season", "season_type": "regular", "total_rosters": 12,
        "draft_id": "D1", "bracket_id": "B1", "previous_league_id": None,
        "settings": {"leg": 3, "last_scored_leg": 2, "taxi_slots": 4, "reserve_slots": 2},
        "roster_positions": ["QB", "RB", "RB", "WR", "WR", "FLEX", "BN", "BN"],
    }
    if with_scoring:
        league["scoring_settings"] = {"pass_td": 4.0, "rec": 1.0}
    return league


class TestFlattenLeague:
    def test_returns_four_frames(self):
        out = flatten_league_to_parquets(_league())
        assert len(out) == 4
        for df in out:
            assert isinstance(df, pl.DataFrame)

    def test_leagues_frame_header(self):
        leagues_df, _, _, _ = flatten_league_to_parquets(_league())
        row = leagues_df.to_dicts()[0]
        assert row["league_id"] == "L1"
        assert row["league_name"] == "My League"
        assert row["leg"] == 3
        assert row["last_scored_leg"] == 2

    def test_roster_slot_counts(self):
        _, _, _, rosters_df = flatten_league_to_parquets(_league())
        row = rosters_df.to_dicts()[0]
        assert row["RB"] == 2          # two RB positions counted
        assert row["BN"] == 2
        assert row["TAXI"] == 4        # from settings.taxi_slots
        assert row["IR"] == 2          # from settings.reserve_slots

    def test_scoring_frame_present_when_scoring_exists(self):
        _, _, scoring_df, _ = flatten_league_to_parquets(_league(with_scoring=True))
        row = scoring_df.to_dicts()[0]
        assert row["league_id"] == "L1"
        assert row["pass_td"] == 4.0

    def test_missing_scoring_settings_does_not_crash(self):
        # SPEC: a league without scoring_settings should still yield a scoring
        # frame carrying at least the league_id (no NameError on a stray var).
        _, _, scoring_df, _ = flatten_league_to_parquets(_league(with_scoring=False))
        assert scoring_df.height == 1
        assert scoring_df.to_dicts()[0]["league_id"] == "L1"
