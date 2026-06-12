"""sleeper_ingestion/historical/league_transactions_ingestion.py

The GraphQL-backed historical loader. We exercise the pure flatten step:
the draft-pick id string parse, and the (unwanted) per-iteration debug-file
write that should not happen.
"""
import polars as pl

from tests.de_loader import load_de_module

mod = load_de_module(
    "sleeper_ingestion/historical/league_transactions_ingestion.py", "sleeper_ingestion"
)
flatten_transactions = mod.flatten_transactions


def _txn():
    return {
        "transaction_id": "t1", "league_id": "L1", "type": "trade",
        "status": "complete", "created": 1000, "status_updated": 1001,
        "creator": "U1", "leg": 1,
        "settings": {"waiver_bid": None, "seq": None},
        "metadata": {"notes": None},
        "consenter_ids": ["U1", "U2"], "roster_ids": [1, 2], "waiver_budget": [],
        "adds": {"100": 1}, "drops": {"200": 2},
        "player_map": {"100": {"first_name": "Pat", "position": "QB"}},
        # GraphQL pick string: "orig_roster,season,round,NEW_OWNER,PREV_OWNER"
        # -> orig roster 1's 2023 1st, traded TO roster 5 (new), FROM roster 1 (prev).
        "draft_picks": ["1,2023,1,5,1"],
    }


class TestFlattenTransactions:
    def test_draft_pick_owner_semantics(self, tmp_path, monkeypatch):
        # SPEC: the 4th field is the NEW owner; emit owner_id/previous_owner_id to
        # match the daily REST feed (a prior bug labeled these from/to and swapped
        # them, breaking reconstruction).
        monkeypatch.chdir(tmp_path)
        _, _, picks = flatten_transactions([_txn()])
        row = picks.to_dicts()[0]
        assert row["draft_pick_id"] == "1,2023,1,5,1"
        assert row["roster_id"] == 1
        assert row["season"] == "2023"
        assert row["round"] == 1
        assert row["owner_id"] == 5            # new owner (4th field)
        assert row["previous_owner_id"] == 1   # previous owner (5th field)
        assert "from_team_id" not in row       # old swapped labels removed

    def test_adds_and_drops_rows(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _, players, _ = flatten_transactions([_txn()])
        assert sorted(r["action"] for r in players.to_dicts()) == ["add", "drop"]

    def test_does_not_write_debug_file(self, tmp_path, monkeypatch):
        # SPEC: flattening must not litter a debug_transactions.json into cwd.
        monkeypatch.chdir(tmp_path)
        flatten_transactions([_txn()])
        assert not (tmp_path / "debug_transactions.json").exists()
