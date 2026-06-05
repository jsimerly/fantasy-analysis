"""sleeper_ingestion/daily/incremental_users.py"""
import polars as pl

from tests.de_loader import load_de_module

mod = load_de_module("sleeper_ingestion/daily/incremental_users.py", "sleeper_ingestion")
flatten_users = mod.flatten_users
_get_week_start_from_str = mod._get_week_start_from_str


class TestFlattenUsers:
    def test_basic_fields_and_metadata_team_name(self):
        users = [{
            "user_id": "U1", "display_name": "alice",
            "avatar": "av1", "metadata": {"team_name": "Alice's Team"},
        }]
        df = flatten_users(users, league_id="L1")
        row = df.to_dicts()[0]
        assert row["league_id"] == "L1"
        assert row["user_id"] == "U1"
        assert row["display_name"] == "alice"
        assert row["user_team_name"] == "Alice's Team"

    def test_blank_strings_normalized_to_null(self):
        users = [{"user_id": "U1", "display_name": "  ", "avatar": None, "metadata": {}}]
        df = flatten_users(users, league_id="L1")
        row = df.to_dicts()[0]
        assert row["display_name"] is None
        assert row["avatar"] is None

    def test_dedupes_by_league_and_user_keep_last(self):
        users = [
            {"user_id": "U1", "display_name": "old", "metadata": {}},
            {"user_id": "U1", "display_name": "new", "metadata": {}},
        ]
        df = flatten_users(users, league_id="L1")
        assert df.height == 1
        assert df.to_dicts()[0]["display_name"] == "new"

    def test_empty_returns_typed_empty_frame(self):
        df = flatten_users([], league_id="L1")
        assert df.height == 0
        assert df.schema["user_id"] == pl.Utf8


class TestWeekStart:
    # 2024-01-10 is a Wednesday; this module has the full, correct weekday map.
    def test_tuesday_start(self):
        assert _get_week_start_from_str("2024-01-10", week_start="tuesday") == "2024-01-09"

    def test_wednesday_start_is_same_day(self):
        assert _get_week_start_from_str("2024-01-10", week_start="wednesday") == "2024-01-10"

    def test_thursday_supported(self):
        # 2024-01-11 is a Thursday
        assert _get_week_start_from_str("2024-01-11", week_start="thursday") == "2024-01-11"
