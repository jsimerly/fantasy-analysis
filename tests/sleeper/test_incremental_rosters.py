"""sleeper_ingestion/daily/incremental_rosters.py

Covers roster flattening, current traded-pick normalization, and the
week-start helper (which currently has a wrong weekday map -> spec test fails).
"""
import polars as pl
import pytest

from tests.de_loader import load_de_module

mod = load_de_module("sleeper_ingestion/daily/incremental_rosters.py", "sleeper_ingestion")
flatten_rosters = mod.flatten_rosters
flatten_traded_picks_current = mod.flatten_traded_picks_current
_get_week_start_from_str = mod._get_week_start_from_str


def _roster():
    return {
        "league_id": "L1",
        "roster_id": 1,
        "owner_id": "U1",
        "players": ["100", "200", "300", "400"],
        "starters": ["100", "200"],
        "taxi": ["300"],
        "reserve": ["400"],
        "metadata": {
            "team_name": "Team A",
            "record": "WWL",
            "streak": "2W",
            "p_nick_100": "Showtime",   # player nickname (digits)
            "p_nick_KC": "Chiefs",      # team nickname (alpha)
        },
        "settings": {
            "division": 1, "wins": 2, "losses": 1, "ties": 0,
            "fpts": 100, "fpts_decimal": 50, "waiver_budget_used": 10,
        },
    }


class TestFlattenRosters:
    def test_returns_three_frames(self):
        players, team_state, nicknames = flatten_rosters([_roster()])
        assert isinstance(players, pl.DataFrame)
        assert isinstance(team_state, pl.DataFrame)
        assert isinstance(nicknames, pl.DataFrame)

    def test_one_player_row_per_roster_player(self):
        players, _, _ = flatten_rosters([_roster()])
        assert players.height == 4

    def test_starter_taxi_reserve_flags(self):
        players, _, _ = flatten_rosters([_roster()])
        by_id = {r["player_id"]: r for r in players.to_dicts()}
        assert by_id["100"]["is_starter"] is True
        assert by_id["100"]["is_active"] is True
        assert by_id["300"]["is_taxi"] is True
        assert by_id["300"]["is_active"] is False   # taxi -> not active
        assert by_id["400"]["is_reserve"] is True
        assert by_id["400"]["is_active"] is False   # reserve -> not active

    def test_player_id_is_stringified(self):
        players, _, _ = flatten_rosters([_roster()])
        assert players.schema["player_id"] == pl.Utf8

    def test_team_state_is_one_row_per_roster(self):
        _, team_state, _ = flatten_rosters([_roster()])
        assert team_state.height == 1
        row = team_state.to_dicts()[0]
        assert row["team_name"] == "Team A"
        assert row["wins"] == 2

    def test_nicknames_classify_player_vs_team(self):
        _, _, nicknames = flatten_rosters([_roster()])
        by_key = {r["meta_key"]: r for r in nicknames.to_dicts()}
        assert by_key["p_nick_100"]["subject_type"] == "player"
        assert by_key["p_nick_100"]["subject_id"] == "100"
        assert by_key["p_nick_KC"]["subject_type"] == "team"
        assert by_key["p_nick_KC"]["subject_id"] == "KC"

    def test_empty_input_yields_typed_empty_frames(self):
        players, team_state, nicknames = flatten_rosters([])
        assert players.height == 0
        assert players.schema["player_id"] == pl.Utf8
        assert team_state.height == 0
        assert nicknames.height == 0


class TestFlattenTradedPicksCurrent:
    def test_maps_roster_fields_to_named_columns(self):
        traded = [{"season": "2024", "round": 1, "roster_id": 3,
                   "owner_id": 5, "previous_owner_id": 3}]
        df = flatten_traded_picks_current(traded, league_id="L1")
        row = df.to_dicts()[0]
        assert row["league_id"] == "L1"
        assert row["original_roster_id"] == 3
        assert row["owner_roster_id"] == 5
        assert row["previous_owner_roster_id"] == 3

    def test_dedupes_on_natural_key(self):
        traded = [
            {"season": "2024", "round": 1, "roster_id": 3, "owner_id": 5, "previous_owner_id": 3},
            {"season": "2024", "round": 1, "roster_id": 3, "owner_id": 7, "previous_owner_id": 5},
        ]
        df = flatten_traded_picks_current(traded, league_id="L1")
        assert df.height == 1
        assert df.to_dicts()[0]["owner_roster_id"] == 7  # keep last

    def test_empty_returns_typed_empty_frame(self):
        df = flatten_traded_picks_current([], league_id="L1")
        assert df.height == 0
        assert "original_roster_id" in df.columns


class TestWeekStart:
    # 2024-01-10 is a Wednesday.
    def test_tuesday_start_rolls_back_to_tuesday(self):
        assert _get_week_start_from_str("2024-01-10", week_start="tuesday") == "2024-01-09"

    def test_same_day_when_start_matches_weekday(self):
        # Wednesday date with a Wednesday week-start must return the same day.
        # SPEC: Wednesday -> weekday index 2. (Current map uses 3, so this fails.)
        assert _get_week_start_from_str("2024-01-10", week_start="wednesday") == "2024-01-10"

    def test_supports_all_weekdays(self):
        # SPEC: every weekday is a valid start. Thursday(3)/Friday(4)/Saturday(5)
        # are currently missing from the map (silently fall back to Tuesday).
        # 2024-01-11 is a Thursday -> Thursday-start should return the same day.
        assert _get_week_start_from_str("2024-01-11", week_start="thursday") == "2024-01-11"
