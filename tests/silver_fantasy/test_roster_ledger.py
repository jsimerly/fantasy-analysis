"""silver_fantasy/fact_roster_membership.py — SCD2 ledger builders.

Covers build_snapshot_intervals: collapsing per-day presence into [valid_from,
valid_to) holding stints via gaps-and-islands (handles gaps + is_current).
"""
import polars as pl

from tests.de_loader import load_de_module

mod = load_de_module(
    "silver_fantasy/fact_roster_membership.py", "silver_fantasy", "fact_roster_membership"
)
build_snapshot_intervals = mod.build_snapshot_intervals
build_event_intervals = mod.build_event_intervals
combine_eras = mod.combine_eras
reconstruct_pick_intervals = mod.reconstruct_pick_intervals


def _present(rows):
    # rows: (snapshot_date, roster_id, player_id)
    return pl.DataFrame({
        "snapshot_date": [r[0] for r in rows],
        "roster_id": [r[1] for r in rows],
        "player_id": [r[2] for r in rows],
    })


KEY = ["roster_id", "player_id"]


class TestBuildSnapshotIntervals:
    def _data(self):
        # 3 snapshot days. A present all 3 (continuous). B present day1+day3 (gap day2).
        return _present([
            ("2025-01-01", 1, "A"), ("2025-01-02", 1, "A"), ("2025-01-03", 1, "A"),
            ("2025-01-01", 1, "B"), ("2025-01-03", 1, "B"),
        ])

    def test_continuous_presence_is_one_open_interval(self):
        iv = build_snapshot_intervals(self._data(), KEY)
        a = iv.filter(pl.col("player_id") == "A").to_dicts()
        assert len(a) == 1
        assert a[0]["valid_from"] == "2025-01-01"
        assert a[0]["valid_to"] is None         # present at the latest snapshot
        assert a[0]["is_current"] is True

    def test_gap_splits_into_two_stints(self):
        iv = build_snapshot_intervals(self._data(), KEY).filter(pl.col("player_id") == "B").sort("valid_from")
        rows = iv.to_dicts()
        assert len(rows) == 2
        # first stint closed when absent on day 2
        assert rows[0]["valid_from"] == "2025-01-01"
        assert rows[0]["valid_to"] == "2025-01-02"
        assert rows[0]["is_current"] is False
        # second stint reopens day 3 and is current
        assert rows[1]["valid_from"] == "2025-01-03"
        assert rows[1]["valid_to"] is None
        assert rows[1]["is_current"] is True

    def test_left_before_end_closes_interval(self):
        # C present day1+day2 then gone day3 -> closed at day3, not current
        data = _present([
            ("2025-01-01", 2, "C"), ("2025-01-02", 2, "C"),
            ("2025-01-03", 2, "Z"),   # keeps day3 in the date universe
        ])
        c = build_snapshot_intervals(data, KEY).filter(pl.col("player_id") == "C").to_dicts()
        assert len(c) == 1
        assert c[0]["valid_from"] == "2025-01-01"
        assert c[0]["valid_to"] == "2025-01-03"
        assert c[0]["is_current"] is False

    def test_empty_input_typed_frame(self):
        out = build_snapshot_intervals(_present([]).clear(), KEY)
        assert out.height == 0
        assert set(["valid_from", "valid_to", "is_current"]).issubset(out.columns)


def _events(rows):
    # rows: (ts_ms, date, roster_id, player_id, action)
    return pl.DataFrame({
        "ts": [r[0] for r in rows],
        "date": [r[1] for r in rows],
        "roster_id": [r[2] for r in rows],
        "player_id": [r[3] for r in rows],
        "action": [r[4] for r in rows],
    })


class TestBuildEventIntervals:
    def test_add_drop_readd_yields_two_stints(self):
        ev = _events([
            (100, "2021-01-01", 1, "A", "add"),
            (500, "2021-05-01", 1, "A", "drop"),
            (900, "2021-09-01", 1, "A", "add"),
        ])
        iv = build_event_intervals(ev, KEY).sort("valid_from")
        rows = iv.to_dicts()
        assert len(rows) == 2
        assert rows[0]["valid_from"] == "2021-01-01" and rows[0]["valid_to"] == "2021-05-01"
        assert rows[0]["is_open"] is False
        assert rows[1]["valid_from"] == "2021-09-01" and rows[1]["valid_to"] is None
        assert rows[1]["is_open"] is True

    def test_consecutive_adds_stay_one_stint(self):
        # draft add then a redundant waiver add (no drop between) = single stint
        ev = _events([
            (100, "2021-01-01", 2, "B", "add"),
            (300, "2021-03-01", 2, "B", "add"),
        ])
        iv = build_event_intervals(ev, KEY)
        assert iv.height == 1
        assert iv.to_dicts()[0]["valid_from"] == "2021-01-01"
        assert iv.to_dicts()[0]["is_open"] is True

    def test_same_ts_drop_before_add(self):
        # a trade: drop from old then add (same instant); must net to held-open
        ev = _events([
            (100, "2021-01-01", 3, "C", "add"),
            (500, "2021-05-01", 3, "C", "drop"),
            (500, "2021-05-01", 3, "C", "add"),   # re-acquired same instant
        ])
        iv = build_event_intervals(ev, KEY)
        # drop-before-add tiebreak -> ends at 05-01, reopens 05-01 -> still currently held
        assert iv.filter(pl.col("is_open")).height == 1

    def test_empty_typed(self):
        out = build_event_intervals(_events([]).clear(), KEY)
        assert out.height == 0
        assert "is_open" in out.columns


class TestCombineEras:
    BOUNDARY = "2025-10-16"

    def test_history_handed_off_to_snapshot_at_boundary(self):
        # A: drafted 2021, never moved -> reconstructed open; snapshot has it from
        # boundary onward. Expect a reconstructed interval ending at boundary + a
        # current snapshot interval.
        events = _events([(100, "2021-09-01", 1, "A", "add")])
        present = _present([(self.BOUNDARY, 1, "A"), ("2025-10-17", 1, "A")])
        out = combine_eras(present, events, self.BOUNDARY, KEY).sort("valid_from")
        rows = out.filter(pl.col("player_id") == "A").to_dicts()
        assert len(rows) == 2
        assert rows[0]["valid_from"] == "2021-09-01"
        assert rows[0]["valid_to"] == self.BOUNDARY      # reconstructed capped at boundary
        assert rows[0]["is_current"] is False
        assert rows[1]["valid_from"] == self.BOUNDARY    # snapshot owns boundary onward
        assert rows[1]["is_current"] is True

    def test_history_only_asset_dropped_before_boundary(self):
        # B: drafted then dropped in 2022, never in snapshots -> only a closed
        # reconstructed interval, no snapshot interval.
        events = _events([(100, "2021-09-01", 2, "B", "add"), (200, "2022-03-01", 2, "B", "drop")])
        present = _present([(self.BOUNDARY, 2, "Z")])  # B absent from snapshots
        out = combine_eras(present, events, self.BOUNDARY, KEY)
        b = out.filter(pl.col("player_id") == "B").to_dicts()
        assert len(b) == 1
        assert b[0]["valid_from"] == "2021-09-01" and b[0]["valid_to"] == "2022-03-01"
        assert b[0]["is_current"] is False

    def test_reconstruction_after_boundary_is_dropped(self):
        # an event-implied open interval must not bleed past the boundary (snapshots own it)
        events = _events([(100, "2025-12-01", 3, "C", "add")])  # event after boundary
        present = _present([(self.BOUNDARY, 3, "Z")])
        out = combine_eras(present, events, self.BOUNDARY, KEY)
        assert out.filter(pl.col("player_id") == "C").height == 0


def _lifecycle(rows):
    # rows: (franchise_id_orig, pick_id, mint_ts, mint_date, consume_date)
    return pl.DataFrame({
        "franchise_id": [r[0] for r in rows],
        "pick_id": [r[1] for r in rows],
        "mint_ts": [r[2] for r in rows],
        "mint_date": [r[3] for r in rows],
        "consume_date": [r[4] for r in rows],
    })


def _trades(rows):
    # rows: (pick_id, prev_franchise, new_franchise, ts, date)
    return pl.DataFrame({
        "pick_id": [r[0] for r in rows],
        "prev_franchise": [r[1] for r in rows],
        "new_franchise": [r[2] for r in rows],
        "ts": [r[3] for r in rows],
        "date": [r[4] for r in rows],
    })


class TestReconstructPickIntervals:
    def test_traded_then_consumed(self):
        # pick minted to L_1 (2021), traded to L_2 (2023), drafted/consumed 2024.
        lc = _lifecycle([("L_1", "2024:1:1", 100, "2021-09-07", "2024-05-08")])
        tr = _trades([("2024:1:1", "L_1", "L_2", 500, "2023-01-01")])
        out = reconstruct_pick_intervals(lc, tr).sort("valid_from")
        rows = out.to_dicts()
        assert len(rows) == 2
        assert rows[0]["franchise_id"] == "L_1"
        assert rows[0]["valid_from"] == "2021-09-07" and rows[0]["valid_to"] == "2023-01-01"
        assert rows[1]["franchise_id"] == "L_2"
        assert rows[1]["valid_from"] == "2023-01-01" and rows[1]["valid_to"] == "2024-05-08"
        assert all(r["is_current"] is False for r in rows)   # consumed

    def test_never_traded_pick_one_interval_to_consume(self):
        lc = _lifecycle([("L_3", "2023:2:3", 100, "2020-09-01", "2023-05-11")])
        out = reconstruct_pick_intervals(lc, _trades([]).clear()).to_dicts()
        assert len(out) == 1
        assert out[0]["franchise_id"] == "L_3"
        assert out[0]["valid_from"] == "2020-09-01" and out[0]["valid_to"] == "2023-05-11"

    def test_unconsumed_pick_stays_open(self):
        # a future pick (no draft yet) -> open/current interval
        lc = _lifecycle([("L_5", "2027:1:5", 100, "2024-05-08", None)])
        out = reconstruct_pick_intervals(lc, _trades([]).clear()).to_dicts()
        assert len(out) == 1
        assert out[0]["valid_to"] is None and out[0]["is_current"] is True
