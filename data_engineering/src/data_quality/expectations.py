"""Pure expectations: each takes DataFrames / values and returns a ``Result``. No IO, so every one is
spec-tested on synthetic frames in ``tests/data_quality/test_expectations.py``. The catalogue in
``suite.py`` composes these against the lake."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

import polars as pl


@dataclass
class Result:
    passed: bool
    observed: str                 # one line a human reads in the run log
    value: float | None = None    # a number the next run can compare against (drift checks)
    detail: str = ""              # a sample of the offending keys, for the fix
    state: str = ""               # a description the next run must find unchanged (regime checks)


def ok(observed: str, value: float | None = None) -> Result:
    return Result(True, observed, value)


def fail(observed: str, value: float | None = None, detail: str = "") -> Result:
    return Result(False, observed, value, detail)


def all_of(*results: Result) -> Result:
    """Several expectations as one check: passes when all pass; the observed line joins theirs."""
    passed = all(r.passed for r in results)
    return Result(passed, "; ".join(r.observed for r in results), None, " | ".join(r.detail for r in results if r.detail))


def as_date(col: str, df: pl.DataFrame) -> pl.Expr:
    """A date expression whatever the column's storage: Date, Datetime, or an ISO string."""
    dt = df.schema[col]
    if dt == pl.Date:
        return pl.col(col)
    if isinstance(dt, pl.Datetime):
        return pl.col(col).dt.date()
    return pl.col(col).cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False)


def _sample(df: pl.DataFrame, cols: list[str], n: int = 5) -> str:
    return "; ".join(",".join(str(v) for v in row) for row in df.select(cols).head(n).iter_rows())


# ------------------------------------------------------------------------------ keys and nulls
def unique_key(df: pl.DataFrame, cols: list[str]) -> Result:
    dup = df.group_by(cols).len().filter(pl.col("len") > 1)
    if dup.height == 0:
        return ok(f"unique on {cols} ({df.height:,} rows)", 0)
    return fail(f"{dup.height:,} duplicate keys on {cols}", dup.height, _sample(dup, cols))


def not_null(df: pl.DataFrame, cols: list[str]) -> Result:
    counts = {c: int(df[c].null_count()) for c in cols}
    bad = {c: n for c, n in counts.items() if n}
    if not bad:
        return ok(f"no nulls in {cols}", 0)
    return fail("nulls: " + ", ".join(f"{c}={n:,}" for c, n in bad.items()), sum(bad.values()))


def columns_present(df: pl.DataFrame, cols: list[str]) -> Result:
    missing = [c for c in cols if c not in df.columns]
    if not missing:
        return ok(f"columns present {cols}")
    return fail(f"missing columns {missing}", len(missing))


def row_count(df: pl.DataFrame, lo: int, hi: int | None = None) -> Result:
    n = df.height
    if n < lo or (hi is not None and n > hi):
        return fail(f"{n:,} rows, expected {lo:,}{'-' + format(hi, ',') if hi else '+'}", n)
    return ok(f"{n:,} rows", n)


def value_range(df: pl.DataFrame, col: str, lo: float, hi: float) -> Result:
    out = df.filter((pl.col(col) < lo) | (pl.col(col) > hi))
    if out.height == 0:
        return ok(f"{col} within [{lo}, {hi}]", 0)
    return fail(f"{out.height:,} rows with {col} outside [{lo}, {hi}]", out.height)


def allowed_values(df: pl.DataFrame, col: str, allowed: set) -> Result:
    bad = df.filter(~pl.col(col).is_in(list(allowed)))
    if bad.height == 0:
        return ok(f"{col} in {sorted(allowed)}", 0)
    return fail(f"{bad.height:,} rows with {col} outside {sorted(allowed)}: {sorted(bad[col].unique().to_list())[:5]}", bad.height)


# ------------------------------------------------------------------------------ time
def fresh(df: pl.DataFrame, date_col: str, today: date, max_age_days: int, label: str = "") -> Result:
    """The newest date in the column is at most ``max_age_days`` old."""
    if df.height == 0:
        return fail(f"{label or date_col}: no rows")
    newest = df.select(as_date(date_col, df).max()).item()
    if newest is None:
        return fail(f"{label or date_col}: no parseable dates")
    age = (today - newest).days
    msg = f"{label or date_col}: newest {newest} ({age} days old)"
    return ok(msg, age) if age <= max_age_days else fail(msg + f", allowed {max_age_days}", age)


def partition_fresh(partitions: list[tuple[str, str]], today: date, max_age_days: int, label: str = "") -> Result:
    """``partitions`` are ``(load_date, blob)`` pairs; the newest must be at most ``max_age_days`` old."""
    if not partitions:
        return fail(f"{label}: no partitions")
    newest = max(date.fromisoformat(d[:10]) for d, _ in partitions)
    age = (today - newest).days
    msg = f"{label}: newest partition {newest} ({age} days old)"
    return ok(msg, age) if age <= max_age_days else fail(msg + f", allowed {max_age_days}", age)


def no_date_gaps(df: pl.DataFrame, date_col: str, today: date, lookback_days: int, grace_days: int = 1) -> Result:
    """Every calendar day in ``[today - lookback, today - grace]`` has at least one row."""
    have = set(df.select(as_date(date_col, df).alias("d")).drop_nulls()["d"].unique().to_list())
    want = [today - timedelta(days=k) for k in range(grace_days, lookback_days + 1)]
    missing = sorted(d for d in want if d not in have)
    if not missing:
        return ok(f"every day present, last {lookback_days} days", 0)
    return fail(f"{len(missing)} missing days in the last {lookback_days}: {', '.join(str(d) for d in missing[:6])}", len(missing))


def monthly_presence(df: pl.DataFrame, date_col: str, key_col: str, start: date, end: date, min_keys: int) -> Result:
    """Every month between ``start`` and ``end`` has at least ``min_keys`` distinct keys (a history with
    no hole, e.g. the KTC player series across the 2024-08 -> 2025-10 local_load gap)."""
    d = df.select(as_date(date_col, df).alias("_d"), pl.col(key_col)).drop_nulls()
    per = d.with_columns(pl.col("_d").dt.truncate("1mo").alias("_m")).group_by("_m").agg(pl.col(key_col).n_unique().alias("k"))
    have = {r["_m"]: r["k"] for r in per.to_dicts()}
    months, m = [], date(start.year, start.month, 1)
    while m <= end:
        months.append(m)
        m = date(m.year + (m.month == 12), m.month % 12 + 1, 1)
    thin = [(mm, have.get(mm, 0)) for mm in months if have.get(mm, 0) < min_keys]
    if not thin:
        return ok(f"{len(months)} months each with {min_keys}+ {key_col}s", 0)
    return fail(f"{len(thin)} months under {min_keys} {key_col}s: " + ", ".join(f"{mm:%Y-%m}={k}" for mm, k in thin[:6]), len(thin))


# ------------------------------------------------------------------------------ slowly changing dimensions
def scd2(df: pl.DataFrame, key_cols: list[str], from_col: str, to_col: str, current_col: str) -> Result:
    """One open (current) interval per key at most, no overlapping intervals, from < to."""
    d = df.with_columns(as_date(from_col, df).alias("_f"), as_date(to_col, df).alias("_t"))
    inverted = d.filter(pl.col("_t").is_not_null() & (pl.col("_f") >= pl.col("_t")))
    multi = d.filter(pl.col(current_col)).group_by(key_cols).len().filter(pl.col("len") > 1)
    s = d.sort(key_cols + ["_f"]).with_columns(pl.col("_t").shift(1).over(key_cols).alias("_prev_t"))
    overlap = s.filter(pl.col("_prev_t").is_not_null() & (pl.col("_f") < pl.col("_prev_t")))
    problems = []
    if inverted.height:
        problems.append(f"{inverted.height:,} intervals with from >= to")
    if multi.height:
        problems.append(f"{multi.height:,} keys with several current rows")
    if overlap.height:
        problems.append(f"{overlap.height:,} overlapping intervals")
    if not problems:
        return ok(f"SCD2 intact on {key_cols} ({d.height:,} rows)", 0)
    return fail("; ".join(problems), inverted.height + multi.height + overlap.height,
                _sample(overlap if overlap.height else (multi if multi.height else inverted), key_cols))


# ------------------------------------------------------------------------------ ratios and drift
def share_at_least(num: int, den: int, threshold: float, label: str) -> Result:
    share = num / den if den else 0.0
    msg = f"{label}: {num:,} of {den:,} ({share:.0%}), need {threshold:.0%}"
    return ok(msg, share) if share >= threshold else fail(msg, share)


def within_pct(value: float, reference: float | None, pct: float, label: str) -> Result:
    """``value`` within ``pct`` (0.25 = 25 %) of ``reference``; a missing reference passes (first run)."""
    if reference is None or reference == 0:
        return ok(f"{label}: {value:,.0f} (no reference yet)", value)
    rel = abs(value - reference) / abs(reference)
    msg = f"{label}: {value:,.0f} vs {reference:,.0f} ({rel:+.0%})"
    return ok(msg, value) if rel <= pct else fail(msg + f", allowed {pct:.0%}", value)


def not_below(value: float, reference: float | None, label: str, slack: float = 0.0) -> Result:
    """A count that must never shrink against the last run (a rebuild that lost history)."""
    if reference is None:
        return ok(f"{label}: {value:,.0f} (no reference yet)", value)
    msg = f"{label}: {value:,.0f} vs last run {reference:,.0f}"
    return ok(msg, value) if value >= reference * (1 - slack) else fail(msg + ", shrank", value)


def unchanged(observed: str, previous: str | None, label: str) -> Result:
    """A description that must equal the last run's (a regime that must not silently change)."""
    if previous is None:
        return Result(True, f"{label}: recorded ({observed})", None, "", observed)
    if observed == previous:
        return Result(True, f"{label}: unchanged ({observed})", None, "", observed)
    return Result(False, f"{label}: CHANGED from [{previous}] to [{observed}]", None, "", observed)


def no_newer_than(d: date | datetime | None, today: date, label: str) -> Result:
    """Dates in the future are a parser or clock bug (KTC local_load carried rows dated into 2027)."""
    if d is None:
        return ok(f"{label}: no dates")
    dd = d.date() if isinstance(d, datetime) else d
    return ok(f"{label}: max {dd}") if dd <= today else fail(f"{label}: max date {dd} is in the future")
