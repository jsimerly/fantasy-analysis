"""sleeper_ingestion/daily/incremental_transactions.py

Covers transaction flattening, the incremental week-selection logic, and the
bronze de-duplicated save (whose draft-pick branch currently never dedupes).
"""
import polars as pl
import pytest

from tests.de_loader import load_de_module

mod = load_de_module("sleeper_ingestion/daily/incremental_transactions.py", "sleeper_ingestion")
flatten_transactions = mod.flatten_transactions
get_recent_transactions_incremental = mod.get_recent_transactions_incremental
save_transactions_to_bronze_deduplicated = mod.save_transactions_to_bronze_deduplicated


def _txn():
    return {
        "transaction_id": "t1", "type": "waiver", "status": "complete",
        "created": 1000, "status_updated": 1001, "creator": "U1",
        "leg": 2, "api_week": 2,
        "settings": {"waiver_bid": 5, "seq": 1},
        "metadata": {"notes": "ok"},
        "consenter_ids": ["U1"], "roster_ids": [1], "waiver_budget": [],
        "adds": {"100": 1}, "drops": {"200": 1},
        "player_map": {"100": {"first_name": "Pat", "last_name": "M", "position": "QB"}},
        "draft_picks": [{"season": "2024", "round": 1, "roster_id": 2,
                         "previous_owner_id": 2, "owner_id": 1}],
    }


class TestFlattenTransactions:
    def test_header_row_and_settings(self):
        txns, players, picks = flatten_transactions([_txn()], league_id="L1")
        assert txns.height == 1
        row = txns.to_dicts()[0]
        assert row["transaction_id"] == "t1"
        assert row["league_id"] == "L1"
        assert row["waiver_bid"] == 5
        assert row["api_week"] == 2

    def test_adds_and_drops_each_produce_a_player_row(self):
        _, players, _ = flatten_transactions([_txn()], league_id="L1")
        actions = sorted(r["action"] for r in players.to_dicts())
        assert actions == ["add", "drop"]
        add_row = next(r for r in players.to_dicts() if r["action"] == "add")
        assert add_row["player_id"] == "100"
        assert add_row["player_first_name"] == "Pat"

    def test_draft_pick_row(self):
        _, _, picks = flatten_transactions([_txn()], league_id="L1")
        assert picks.height == 1
        assert picks.to_dicts()[0]["season"] == "2024"

    def test_none_frames_when_no_players_or_picks(self):
        bare = {**_txn(), "adds": None, "drops": None, "draft_picks": None}
        txns, players, picks = flatten_transactions([bare], league_id="L1")
        assert txns.height == 1
        assert players is None
        assert picks is None

    def test_missing_optional_header_keys_tolerated(self):
        # SPEC: optional header fields Sleeper may omit (creator on system/commissioner
        # moves, status_updated) must flatten to null, not KeyError. transaction_id stays
        # required (it's the dedup key) and is asserted present.
        t = _txn()
        del t["creator"]
        del t["status_updated"]
        txns, _, _ = flatten_transactions([t], league_id="L1")
        row = txns.to_dicts()[0]
        assert row["transaction_id"] == "t1"
        assert row["creator"] is None
        assert row["status_updated"] is None


class TestWeekSelection:
    @pytest.fixture(autouse=True)
    def _no_sleep(self, monkeypatch):
        monkeypatch.setattr(mod.time, "sleep", lambda *a, **k: None)

    def _capture_weeks(self, monkeypatch):
        seen = []

        def fake_get(*, league_id, week):
            seen.append(week)
            return []
        monkeypatch.setattr(mod, "get_transactions", fake_get)
        return seen

    def test_in_season_leg_zero_fetches_weeks_1_and_2(self, monkeypatch):
        seen = self._capture_weeks(monkeypatch)
        get_recent_transactions_incremental("L1", "in_season", 0)
        assert seen == [1, 2]

    def test_in_season_midseason_fetches_one_prev_current_next(self, monkeypatch):
        seen = self._capture_weeks(monkeypatch)
        get_recent_transactions_incremental("L1", "in_season", 5)
        assert seen == [1, 5, 6]

    def test_complete_sweeps_all_legs(self, monkeypatch):
        # SPEC: once a league is complete we must keep capturing late-season +
        # offseason legs (not just week 1), or the daily feed drops everything
        # after completion. Sweep a generous leg range.
        seen = self._capture_weeks(monkeypatch)
        get_recent_transactions_incremental("L1", "complete", 0)
        assert seen == list(range(1, 23))
        assert 1 in seen and 18 in seen          # late-season legs included

    def test_offseason_status_also_sweeps(self, monkeypatch):
        seen = self._capture_weeks(monkeypatch)
        get_recent_transactions_incremental("L1", "pre_draft", 0)
        assert seen == list(range(1, 23))


class TestSaveDeduplicated:
    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        monkeypatch.setenv("GCS_BUCKET_NAME", "test-bucket")

    def _picks_df(self):
        return pl.DataFrame([{
            "transaction_id": "t1", "league_id": "L1", "season": "2024",
            "round": 1, "roster_id": 2, "previous_owner_id": 2, "owner_id": 1,
        }])

    def test_transactions_table_dedupes_across_runs(self, fake_gcs):
        txns = pl.DataFrame([{
            "transaction_id": "t1", "league_id": "L1", "type": "waiver",
            "status": "complete", "created": 1000,
        }])
        save_transactions_to_bronze_deduplicated("L1", txns, None, None)
        save_transactions_to_bronze_deduplicated("L1", txns, None, None)
        path = "gs://test-bucket/bronze/sleeper/transactions/transactions/daily/league_id=L1/data.parquet"
        assert fake_gcs[path].height == 1

    def test_draft_picks_table_dedupes_across_runs(self, fake_gcs):
        # SPEC: re-running the daily load must not create duplicate pick rows.
        # (Current code checks table name 'transaction_draft_picks' but the key
        #  is 'draft_picks', so the pick table never dedupes -> this fails.)
        picks = self._picks_df()
        save_transactions_to_bronze_deduplicated("L1", None, None, picks)
        save_transactions_to_bronze_deduplicated("L1", None, None, picks)
        path = "gs://test-bucket/bronze/sleeper/transactions/draft_picks/daily/league_id=L1/data.parquet"
        assert fake_gcs[path].height == 1
