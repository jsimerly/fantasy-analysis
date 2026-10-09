"""Run the data-quality suite against the lake (the ``silver-data-quality`` Cloud Run job, last step of
the daily DAG) and store the results.

    python -m data_quality.run                 # the job: run, write results, exit 1 on any failed error-check
    python -m data_quality.run --no-write      # local dry run (reads the lake, writes nothing)
    python -m data_quality.run --only asset_values --no-write

Results: ``silver/_quality/run_date=<d>/results.parquet`` (one row per check) and
``silver/_quality/latest.parquet`` (the previous run the drift / regime checks compare against).
"""
from __future__ import annotations

import argparse
import io
import sys
from datetime import date

import polars as pl

from data_quality.core import RESULTS_PREFIX, Context, previous_from, run_checks, summarize
from data_quality.suite import suite


def load_previous(ctx: Context) -> dict:
    try:
        return previous_from(ctx.blob(f"{RESULTS_PREFIX}/latest.parquet"))
    except Exception:  # noqa: BLE001 - first run, or the store is unreadable: drift checks record instead of compare
        return {}


def write_results(ctx: Context, results: pl.DataFrame) -> None:
    from google.cloud import storage
    bucket = storage.Client().bucket(ctx.bucket)
    for name in (f"{RESULTS_PREFIX}/run_date={ctx.today.isoformat()}/results.parquet", f"{RESULTS_PREFIX}/latest.parquet"):
        buf = io.BytesIO()
        results.write_parquet(buf)
        bucket.blob(name).upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    print(f"wrote {results.height} results to gs://{ctx.bucket}/{RESULTS_PREFIX}/run_date={ctx.today.isoformat()}/ (+ latest.parquet)")


def print_report(results: pl.DataFrame) -> None:
    width = max(len(c) for c in results["check"].to_list())
    for r in results.sort(["passed", "effective_severity", "check"]).to_dicts():
        mark = "PASS" if r["passed"] else ("FAIL" if r["effective_severity"] == "error" else "WARN")
        note = f"  [known open: {r['known_open']}]" if (not r["passed"] and r["known_open"]) else ""
        print(f"{mark:4} {r['severity']:5} {r['check']:{width}}  {r['observed']}{note}")
        if not r["passed"] and r["detail"]:
            print(f"{'':11}{'':{width}}  e.g. {r['detail'][:300]}")
        if r["error"]:
            print(f"{'':11}{'':{width}}  {r['error'].strip().splitlines()[-1][:200]}")
    s = summarize(results)
    known = f" ({s['known_open']} known open)" if s["known_open"] else ""
    print(f"\n{s['checks']} checks: {s['passed']} passed, {s['failed_errors']} failed (error), {s['failed_warnings']} warnings{known}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-write", action="store_true", help="dry run: read the lake, write no results")
    ap.add_argument("--only", default=None, help="run the checks whose name or table contains this")
    ap.add_argument("--today", default=None, help="YYYY-MM-DD, for re-running a past day's freshness logic")
    args = ap.parse_args(argv)
    ctx = Context(today=date.fromisoformat(args.today) if args.today else None)
    ctx.previous = load_previous(ctx)
    results = run_checks(suite(args.only), ctx)
    print_report(results)
    if not args.no_write:
        write_results(ctx, results)
    s = summarize(results)
    return 1 if s["failed_errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
