"""sleeper_ingestion/historical/league_drafts_ingestion.py"""
import polars as pl

from tests.de_loader import load_de_module

mod = load_de_module(
    "sleeper_ingestion/historical/league_drafts_ingestion.py", "sleeper_ingestion"
)
flatten_drafts = mod.flatten_drafts


def _draft():
    return {
        "draft_id": "D1", "league_id": "L1",
        "created": 1609459200000,          # 2021-01-01T00:00:00Z in ms
        "last_message_time": None, "last_picked": None, "start_time": None,
        "season": "2021", "season_type": "regular", "status": "complete",
        "type": "snake", "sport": "nfl",
        "metadata": {"name": "Startup", "description": "d", "scoring_type": "ppr"},
        "settings": {"rounds": 5, "teams": 12, "pick_timer": 0},
        "draft_order": {"U1": 1, "U2": 2},
        "creators": [],
    }


class TestFlattenDrafts:
    def test_returns_drafts_and_order_frames(self):
        drafts_df, order_df = flatten_drafts([_draft()])
        assert drafts_df.height == 1
        assert order_df.height == 2          # one row per (user, slot)

    def test_epoch_ms_converted_to_iso(self):
        drafts_df, _ = flatten_drafts([_draft()])
        row = drafts_df.to_dicts()[0]
        assert row["created_iso"].startswith("2021-01-01T00:00:00")

    def test_null_timestamps_stay_null(self):
        drafts_df, _ = flatten_drafts([_draft()])
        row = drafts_df.to_dicts()[0]
        assert row["last_picked_iso"] is None

    def test_settings_and_metadata_unpacked(self):
        drafts_df, _ = flatten_drafts([_draft()])
        row = drafts_df.to_dicts()[0]
        assert row["rounds"] == 5
        assert row["draft_name"] == "Startup"

    def test_draft_order_rows_carry_ids(self):
        _, order_df = flatten_drafts([_draft()])
        rows = {r["user_id"]: r["slot"] for r in order_df.to_dicts()}
        assert rows == {"U1": 1, "U2": 2}

    def test_empty_input_yields_empty_frames(self):
        drafts_df, order_df = flatten_drafts([])
        assert drafts_df.height == 0
        assert order_df.height == 0
