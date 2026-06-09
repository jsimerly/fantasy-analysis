"""sleeper_ingestion/historical/league_draft_picks_ingestion.py"""
import polars as pl

from tests.de_loader import load_de_module

mod = load_de_module(
    "sleeper_ingestion/historical/league_draft_picks_ingestion.py", "sleeper_ingestion"
)
flatten_draft_picks = mod.flatten_draft_picks
flatten_traded_draft_picks = mod.flatten_traded_draft_picks
_to_int_or_none = mod._to_int_or_none
_ms_to_iso_or_none = mod._ms_to_iso_or_none


class TestCoercers:
    def test_to_int_handles_blanks_and_padding(self):
        assert _to_int_or_none("003") == 3
        assert _to_int_or_none(" 12 ") == 12
        assert _to_int_or_none("") is None
        assert _to_int_or_none("NA") is None
        assert _to_int_or_none(None) is None

    def test_ms_to_iso(self):
        assert _ms_to_iso_or_none(1609459200000).startswith("2021-01-01T00:00:00")
        assert _ms_to_iso_or_none("") is None


def _pick(pick_no=1):
    return {
        "draft_id": "D1", "pick_no": pick_no, "round": 1, "draft_slot": 1,
        "picked_by": "U1", "roster_id": 1, "player_id": "100", "is_keeper": None,
        "metadata": {"first_name": "Pat", "last_name": "M", "position": "QB",
                     "team": "KC", "years_exp": "7", "number": "15",
                     "news_updated": "1609459200000"},
    }


class TestFlattenDraftPicks:
    def test_row_and_types(self):
        df = flatten_draft_picks([_pick()])
        assert df.height == 1
        row = df.to_dicts()[0]
        assert row["player_id"] == "100"
        assert row["player_years_exp"] == 7
        assert row["player_number"] == 15
        assert df.schema["pick_no"] == pl.Int64
        assert df.schema["player_id"] == pl.Utf8

    def test_dedupes_on_draft_and_pick_no(self):
        df = flatten_draft_picks([_pick(1), _pick(1)])
        assert df.height == 1

    def test_empty_returns_typed_schema(self):
        df = flatten_draft_picks([])
        assert df.height == 0
        assert "pick_no" in df.columns
        assert df.schema["pick_no"] == pl.Int64


class TestFlattenTradedDraftPicks:
    def test_renames_owner_columns(self):
        trades = [{"draft_id": "D1", "season": "2024", "round": 1,
                   "roster_id": 2, "owner_id": 5, "previous_owner_id": 2}]
        df = flatten_traded_draft_picks(trades)
        row = df.to_dicts()[0]
        assert row["owner_roster_id"] == 5
        assert row["previous_owner_roster_id"] == 2

    def test_empty_returns_empty_frame(self):
        df = flatten_traded_draft_picks([])
        assert df.height == 0
