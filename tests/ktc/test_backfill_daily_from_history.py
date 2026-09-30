"""ktc_ingestion/backfill_daily_from_history.py

Rebuilds missing ``daily_load`` partitions from the per-player value-history pages. The
pure pieces (date gap, history -> rows, schema conformance, partition assembly) are spec'd
here; network and GCS are injected / patched.
"""
from datetime import date

import polars as pl
import pytest

from tests.de_loader import load_de_module

mod = load_de_module("ktc_ingestion/backfill_daily_from_history.py", "ktc_ingestion", "ktc_backfill")


# A trimmed stand-in for the real 80-column daily snapshot schema (dtypes as in bronze).
SCHEMA = {
    "playerName": pl.String, "playerID": pl.Int64, "slug": pl.String, "position": pl.String,
    "positionID": pl.Int32, "isTrending": pl.Boolean,
    "oneqb_value": pl.Float32, "oneqb_tep_value": pl.Float32, "oneqb_rank": pl.Int32,
    "oneqb_positionalRank": pl.Int32,
    "sf_value": pl.Float32, "sf_tep_value": pl.Float32, "sf_rank": pl.Int32, "sf_positionalRank": pl.Int32,
}

PLAYER = {"playerName": "Josh Allen", "playerID": 365, "slug": "josh-allen-365",
          "position": "QB", "positionID": 1, "isTrending": True}
PICK = {"playerName": "2027 Early 1st", "playerID": 90001, "slug": "2027-early-1st-90001",
        "position": "RDP", "positionID": 7, "isTrending": False}


def _hist(values, ranks=None, pos=None):
    blob = {"overallValue": [{"d": d, "v": v} for d, v in values]}
    if ranks is not None:
        blob["overallRankHistory"] = [{"d": d, "v": v} for d, v in ranks]
    if pos is not None:
        blob["positionalRankHistory"] = [{"d": d, "v": v} for d, v in pos]
    return blob


class TestMissingDates:
    def test_default_start_is_day_after_newest_partition(self):
        existing = [date(2026, 9, 6), date(2026, 9, 7)]
        assert mod.missing_dates(existing, None, date(2026, 9, 10)) == [
            date(2026, 9, 8), date(2026, 9, 9), date(2026, 9, 10)]

    def test_explicit_start_skips_days_that_already_exist(self):
        existing = [date(2026, 9, 7), date(2026, 9, 9)]
        assert mod.missing_dates(existing, date(2026, 9, 6), date(2026, 9, 10)) == [
            date(2026, 9, 6), date(2026, 9, 8), date(2026, 9, 10)]

    def test_nothing_missing(self):
        assert mod.missing_dates([date(2026, 9, 7)], None, date(2026, 9, 7)) == []

    def test_no_partitions_at_all_yields_only_end(self):
        assert mod.missing_dates([], None, date(2026, 9, 7)) == [date(2026, 9, 7)]


class TestHistoryToDailyRows:
    def test_ktc_date_format(self):
        assert mod.parse_ktc_date("260930") == date(2026, 9, 30)

    def test_values_and_ranks_by_date(self):
        one = _hist([("260908", 7800), ("260909", 7810)], ranks=[("260908", 7), ("260909", 6)],
                    pos=[("260908", 1), ("260909", 1)])
        sf = _hist([("260908", 9990), ("260909", 9994)], ranks=[("260908", 3), ("260909", 2)],
                   pos=[("260908", 1), ("260909", 1)])
        rows = mod.history_to_daily_rows(PLAYER, one, sf, [date(2026, 9, 8), date(2026, 9, 9)])
        assert [r["_load_date"] for r in rows] == [date(2026, 9, 8), date(2026, 9, 9)]
        assert rows[1]["oneqb_value"] == 7810 and rows[1]["oneqb_rank"] == 6
        assert rows[1]["sf_value"] == 9994 and rows[1]["sf_rank"] == 2
        assert rows[0]["slug"] == "josh-allen-365" and rows[0]["playerID"] == 365

    def test_only_wanted_dates(self):
        one = _hist([("260907", 1), ("260908", 2), ("260909", 3)])
        rows = mod.history_to_daily_rows(PLAYER, one, {}, [date(2026, 9, 8)])
        assert [r["oneqb_value"] for r in rows] == [2]

    def test_days_without_any_value_are_skipped(self):
        # a player who entered the rankings on 09-09 has no 09-08 row
        one = _hist([("260909", 5)])
        rows = mod.history_to_daily_rows(PLAYER, one, {}, [date(2026, 9, 8), date(2026, 9, 9)])
        assert [r["_load_date"] for r in rows] == [date(2026, 9, 9)]

    def test_duplicate_history_dates_keep_the_last(self):
        # KTC sometimes lists a date twice (intraday update); the later point is the final one
        one = _hist([("260908", 7800), ("260908", 7850)])
        rows = mod.history_to_daily_rows(PLAYER, one, {}, [date(2026, 9, 8)])
        assert rows[0]["oneqb_value"] == 7850

    def test_pick_without_positional_ranks(self):
        one = _hist([("260908", 4000)], ranks=[("260908", 40)])   # no positionalRankHistory
        sf = _hist([("260908", 4200)], ranks=[("260908", 38)])
        rows = mod.history_to_daily_rows(PICK, one, sf, [date(2026, 9, 8)])
        assert rows[0]["oneqb_positionalRank"] is None
        assert rows[0]["sf_positionalRank"] is None
        assert rows[0]["position"] == "RDP"

    def test_isTrending_is_not_carried_from_the_rankings_page(self):
        # a "now" property; unknowable for a past day -> left for conform_to_schema to null
        rows = mod.history_to_daily_rows(PLAYER, _hist([("260908", 1)]), {}, [date(2026, 9, 8)])
        assert "isTrending" not in rows[0]


class TestConformToSchema:
    def test_adds_missing_columns_as_typed_nulls_in_schema_order(self):
        df = pl.DataFrame({"slug": ["a"], "oneqb_value": [7800], "playerName": ["A"],
                           "playerID": [1], "position": ["QB"], "positionID": [1],
                           "oneqb_rank": [None], "oneqb_positionalRank": [None],
                           "sf_value": [9000], "sf_rank": [1], "sf_positionalRank": [1]})
        out = mod.conform_to_schema(df, SCHEMA)
        assert out.columns == list(SCHEMA)
        assert dict(out.schema) == SCHEMA
        assert out["oneqb_tep_value"].to_list() == [None]     # not recoverable -> null
        assert out["isTrending"].to_list() == [None]
        assert out["oneqb_value"].to_list() == [7800.0]        # cast to the bronze Float32


class TestBuildPartitions:
    WANTED = [date(2026, 9, 8), date(2026, 9, 9)]

    def _fetch(self, player):
        if player["slug"] == "broken-1":
            raise ValueError("Could not extract player data")
        one = _hist([("260908", 100), ("260909", 110)], ranks=[("260908", 2), ("260909", 2)],
                    pos=[("260908", 1), ("260909", 1)])
        sf = _hist([("260908", 200), ("260909", 210)], ranks=[("260908", 1), ("260909", 1)],
                   pos=[("260908", 1), ("260909", 1)])
        return one, sf

    def test_one_frame_per_day_with_every_player(self):
        players = [PLAYER, PICK]
        frames, errors = mod.build_partitions(players, self.WANTED, self._fetch, SCHEMA, log=lambda s: None)
        assert errors == []
        assert sorted(frames) == self.WANTED
        for d, df in frames.items():
            assert df.height == 2
            assert dict(df.schema) == SCHEMA
            assert set(df["slug"].to_list()) == {"josh-allen-365", "2027-early-1st-90001"}

    def test_failed_player_is_skipped_and_reported(self):
        broken = {**PLAYER, "slug": "broken-1", "playerName": "Broken"}
        frames, errors = mod.build_partitions([PLAYER, broken], self.WANTED, self._fetch, SCHEMA, log=lambda s: None)
        assert [e["slug"] for e in errors] == ["broken-1"]
        assert all(df.height == 1 for df in frames.values())

    def test_no_rows_gives_no_frames(self):
        frames, errors = mod.build_partitions([], self.WANTED, self._fetch, SCHEMA, log=lambda s: None)
        assert frames == {} and errors == []


class TestBackfillMarketGuards:
    """The market driver refuses to write when too many player pages failed, and never
    overwrites an existing partition."""

    def _patch_common(self, monkeypatch, existing, schema=SCHEMA):
        monkeypatch.setattr(mod, "list_existing_partition_dates", lambda b, m: existing)
        monkeypatch.setattr(mod, "reference_schema", lambda b, m, latest: schema)
        monkeypatch.setattr(mod, "fetch_soup", lambda url: url)          # soup stand-in
        monkeypatch.setattr(mod, "parse_rankings_players", lambda soup: [PLAYER, PICK, {**PLAYER, "slug": "x-1"}])

    def test_aborts_before_writing_when_failure_rate_too_high(self, monkeypatch):
        self._patch_common(monkeypatch, [date(2026, 9, 7)])
        # every player page "fails" to parse
        monkeypatch.setattr(mod, "parse_historic_1QBplayer_data", lambda soup: (_ for _ in ()).throw(ValueError("nope")))
        writes = []
        monkeypatch.setattr(mod, "write_partition", lambda *a, **k: writes.append(a))
        rc = mod.backfill_market("dynasty", "b", None, date(2026, 9, 9), None, None)
        assert rc == 1
        assert writes == []

    def test_writes_each_missing_day(self, monkeypatch):
        self._patch_common(monkeypatch, [date(2026, 9, 7)])
        one = _hist([("260908", 100), ("260909", 110)])
        sf = _hist([("260908", 200), ("260909", 210)])
        monkeypatch.setattr(mod, "parse_historic_1QBplayer_data", lambda soup: one)
        monkeypatch.setattr(mod, "parse_historic_SFplayer_data", lambda soup: sf)
        writes = []
        monkeypatch.setattr(mod, "write_partition",
                            lambda df, bucket, market, d, n, dry: writes.append((market, d, df.height)) or "ok")
        rc = mod.backfill_market("dynasty", "b", None, date(2026, 9, 9), None, None)
        assert rc == 0
        assert writes == [("dynasty", date(2026, 9, 8), 3), ("dynasty", date(2026, 9, 9), 3)]

    def test_nothing_missing_is_a_noop(self, monkeypatch):
        self._patch_common(monkeypatch, [date(2026, 9, 9)])
        monkeypatch.setattr(mod, "write_partition", lambda *a, **k: pytest.fail("must not write"))
        assert mod.backfill_market("dynasty", "b", None, date(2026, 9, 9), None, None) == 0

    def test_existing_partition_is_never_overwritten(self, monkeypatch, tmp_path):
        # the GCS path: an existing player_data.parquet must raise, not be replaced
        class _Blob:
            def __init__(self, name): self.name = name
            def exists(self): return self.name.endswith("player_data.parquet")
            def upload_from_string(self, *a, **k): pytest.fail("must not upload")
        class _Bucket:
            def blob(self, name): return _Blob(name)
        class _Client:
            def bucket(self, name): return _Bucket()
        monkeypatch.setattr(mod.storage, "Client", lambda: _Client())
        df = pl.DataFrame({"slug": ["a"]})
        with pytest.raises(FileExistsError):
            mod.write_partition(df, "b", "dynasty", date(2026, 9, 8), 1, None)

    def test_dry_run_writes_locally_with_marker(self, tmp_path):
        df = pl.DataFrame({"slug": ["a"]})
        where = mod.write_partition(df, "b", "dynasty", date(2026, 9, 8), 1, tmp_path)
        assert (tmp_path / "bronze/ktc/dynasty/daily_load/load_date=2026-09-08/player_data.parquet").exists()
        assert (tmp_path / "bronze/ktc/dynasty/daily_load/load_date=2026-09-08/_RECONSTRUCTED.json").exists()
        assert "load_date=2026-09-08" in where
