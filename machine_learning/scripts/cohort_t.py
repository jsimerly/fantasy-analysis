"""Paired t with the COHORT (test season) as the unit (review 2026-10-13, docs/MODEL_REVIEW_2026-10-13.md).

The checkpoint weeks of one season share players and outcomes (intraclass correlation 0.6 for ordering, 0.9 for PAR
error), so a t over cohort-weeks is about 1.7x too large: item 51's "t +6.92 over 32 cohort-weeks" is t +3.99 over the
8 cohorts. Average each cohort's weeks first, then t over cohorts; the cohort-week t is printed beside it for the record.

  python scripts/cohort_t.py seam _cache/seam_eval/seam_35.parquet --a todate --b last_season
  python scripts/cohort_t.py table _cache/inseason_backtests/par35_all5.parquet _cache/inseason_backtests/par35_base.parquet --metrics "ros|par" "next|par"

``seam``: one table with T, W, variant and metric columns (seam_eval.py); ``--a`` / ``--b`` are the two variants.
``table``: two tables with T, W and metric columns (backtest_inseason.py --tag); a minus b.
Metrics default to spearman / mae / ratio for a seam table and every shared ``next|`` / ``ros|`` column for a pair.
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
import polars as pl


def cohort_t(a: pl.DataFrame, b: pl.DataFrame, metric: str) -> dict:
    """a minus b on ``metric``, joined on (T, W): the mean, the t over cohorts (weeks averaged within each cohort first),
    cohorts won, and the naive t over cohort-weeks."""
    j = (a.select("T", "W", pl.col(metric).cast(pl.Float64).alias("_a"))
          .join(b.select("T", "W", pl.col(metric).cast(pl.Float64).alias("_b")), on=["T", "W"], how="inner")
          .with_columns((pl.col("_a") - pl.col("_b")).alias("_d")).drop_nulls("_d"))
    if j.height == 0:
        return {"metric": metric, "n_cohorts": 0}
    per = j.group_by("T").agg(pl.col("_d").mean()).sort("T")["_d"].to_numpy()
    wk = j["_d"].to_numpy()

    def t_of(x: np.ndarray) -> float | None:
        return float(x.mean() / (x.std(ddof=1) / np.sqrt(len(x)))) if len(x) > 1 and x.std(ddof=1) > 0 else None
    return {"metric": metric, "mean": float(per.mean()), "t_cohort": t_of(per), "n_cohorts": int(len(per)), "cohorts_won": int((per > 0).sum()),
            "t_cohort_week": t_of(wk), "n_cohort_weeks": int(len(wk)), "weeks_won": int((wk > 0).sum())}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["seam", "table"])
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--a", default="todate", help="seam: the candidate variant")
    ap.add_argument("--b", default="last_season", help="seam: the baseline variant")
    ap.add_argument("--metrics", nargs="*", default=None)
    args = ap.parse_args()
    if args.mode == "seam":
        d = pl.read_parquet(args.paths[0])
        a, b = d.filter(pl.col("variant") == args.a), d.filter(pl.col("variant") == args.b)
        metrics = args.metrics or [m for m in ("spearman", "mae", "ratio") if m in d.columns]
        label = f"{args.a} - {args.b}"
    else:
        a, b = pl.read_parquet(args.paths[0]), pl.read_parquet(args.paths[1])
        shared = [c for c in a.columns if c in b.columns and (c.startswith("next|") or c.startswith("ros|")) and a[c].dtype.is_numeric()]
        metrics = args.metrics or shared
        label = f"{args.paths[0]} - {args.paths[1]}"
    rows = [cohort_t(a, b, m) for m in metrics]
    with pl.Config(tbl_rows=-1, tbl_cols=12, tbl_width_chars=200, float_precision=3):
        print(label)
        print(pl.DataFrame(rows))
    print("the unit of evidence is the cohort: read t_cohort; t_cohort_week counts the weeks of one season as if independent")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    main()
