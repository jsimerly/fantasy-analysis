"""data_quality/expectations.py: each pure expectation on a synthetic frame, the pass and the fail."""
from datetime import date, datetime

import polars as pl

from data_quality import expectations as x


def test_unique_key_reports_duplicates_with_a_sample():
    df = pl.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
    assert x.unique_key(df, ["a", "b"]).passed is False
    r = x.unique_key(df, ["a", "b"])
    assert r.value == 1 and "1,x" in r.detail
    assert x.unique_key(df.unique(), ["a", "b"]).passed


def test_not_null_counts_per_column():
    df = pl.DataFrame({"a": [1, None, None], "b": ["x", "y", "z"]})
    r = x.not_null(df, ["a", "b"])
    assert not r.passed and "a=2" in r.observed and r.value == 2
    assert x.not_null(df, ["b"]).passed


def test_fresh_accepts_date_datetime_and_string_columns():
    today = date(2026, 10, 9)
    for col in (pl.Series("d", [date(2026, 10, 8)]), pl.Series("d", [datetime(2026, 10, 8, 5)]), pl.Series("d", ["2026-10-08"])):
        r = x.fresh(pl.DataFrame([col]), "d", today, 1)
        assert r.passed and r.value == 1
    stale = x.fresh(pl.DataFrame({"d": ["2026-09-30"]}), "d", today, 1, "ktc")
    assert not stale.passed and "9 days old" in stale.observed
    assert not x.fresh(pl.DataFrame({"d": []}, schema={"d": pl.Date}), "d", today, 1).passed


def test_partition_fresh_uses_the_newest_load_date():
    parts = [("2026-10-01", "a"), ("2026-10-08", "b")]
    assert x.partition_fresh(parts, date(2026, 10, 9), 1, "ktc").passed
    assert not x.partition_fresh(parts, date(2026, 10, 12), 1, "ktc").passed
    assert not x.partition_fresh([], date(2026, 10, 9), 1, "ktc").passed


def test_no_date_gaps_lists_the_missing_days_and_honours_grace():
    today = date(2026, 10, 9)
    days = [date(2026, 10, 1) + pl.duration(days=k) for k in range(0, 0)]  # noqa: F841 (illustrative)
    have = pl.DataFrame({"d": [date(2026, 10, k) for k in (2, 3, 4, 6, 7, 8)]})
    r = x.no_date_gaps(have, "d", today, 7, grace_days=1)      # window 2..8: the 5th is missing
    assert not r.passed and "2026-10-05" in r.observed and r.value == 1
    assert x.no_date_gaps(have, "d", today, 3, grace_days=1).passed


def test_monthly_presence_flags_a_thin_month():
    rows = [{"d": date(2024, m, 1), "k": f"p{i}"} for m in range(1, 7) for i in range(5)]
    rows += [{"d": date(2024, 7, 1), "k": "p0"}]                     # July has one player
    r = x.monthly_presence(pl.DataFrame(rows), "d", "k", date(2024, 1, 1), date(2024, 8, 31), 3)
    assert not r.passed and "2024-07=1" in r.observed and "2024-08=0" in r.observed and r.value == 2
    assert x.monthly_presence(pl.DataFrame(rows), "d", "k", date(2024, 1, 1), date(2024, 6, 30), 3).passed


def test_scd2_finds_overlaps_double_current_and_inverted_intervals():
    good = pl.DataFrame({"k": ["a", "a", "b"], "f": ["2024-01-01", "2024-02-01", "2024-01-01"], "t": ["2024-02-01", None, None], "cur": [False, True, True]})
    assert x.scd2(good, ["k"], "f", "t", "cur").passed
    overlap = pl.DataFrame({"k": ["a", "a"], "f": ["2024-01-01", "2024-01-15"], "t": ["2024-02-01", None], "cur": [False, True]})
    r = x.scd2(overlap, ["k"], "f", "t", "cur")
    assert not r.passed and "overlapping" in r.observed
    two_current = pl.DataFrame({"k": ["a", "a"], "f": ["2024-01-01", "2024-03-01"], "t": [None, None], "cur": [True, True]})
    assert "several current" in x.scd2(two_current, ["k"], "f", "t", "cur").observed
    inverted = pl.DataFrame({"k": ["a"], "f": ["2024-03-01"], "t": ["2024-01-01"], "cur": [False]})
    assert "from >= to" in x.scd2(inverted, ["k"], "f", "t", "cur").observed


def test_share_within_not_below_and_unchanged():
    assert x.share_at_least(90, 100, 0.9, "x").passed and not x.share_at_least(89, 100, 0.9, "x").passed
    assert x.within_pct(110, 100, 0.25, "x").passed and not x.within_pct(130, 100, 0.25, "x").passed
    assert x.within_pct(130, None, 0.25, "x").passed                           # first run: nothing to compare
    assert x.not_below(100, 100, "x").passed and not x.not_below(99, 100, "x").passed and x.not_below(5, None, "x").passed
    first = x.unchanged("qb1 rb2", None, "lineup")
    assert first.passed and first.state == "qb1 rb2"
    assert x.unchanged("qb1 rb2", "qb1 rb2", "lineup").passed
    changed = x.unchanged("qb1 rb3", "qb1 rb2", "lineup")
    assert not changed.passed and "CHANGED" in changed.observed and changed.state == "qb1 rb3"


def test_row_count_value_range_allowed_values_and_future_dates():
    df = pl.DataFrame({"v": [1, 5, 11], "s": ["a", "b", "zz"]})
    assert x.row_count(df, 1, 3).passed and not x.row_count(df, 4).passed and not x.row_count(df, 1, 2).passed
    assert not x.value_range(df, "v", 0, 10).passed and x.value_range(df, "v", 0, 11).passed
    assert not x.allowed_values(df, "s", {"a", "b"}).passed and x.allowed_values(df, "s", {"a", "b", "zz"}).passed
    assert x.no_newer_than(date(2026, 10, 9), date(2026, 10, 9), "d").passed
    assert not x.no_newer_than(datetime(2027, 1, 1), date(2026, 10, 9), "d").passed


def test_all_of_joins_results():
    r = x.all_of(x.ok("a"), x.fail("b", detail="k1"))
    assert not r.passed and r.observed == "a; b" and r.detail == "k1"
    assert x.all_of(x.ok("a"), x.ok("b")).passed
