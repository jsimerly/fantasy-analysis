"""The career projection over several horizons from one saved run (BACKLOG 49): one GPU run, every window scored offline.

    uv run python scripts/window_report.py RUN [--windows 3 5 7] [--vs BASELINE_RUN] [--rate 0.2]

RUN must have been run with ``--save-cohorts`` and ``--horizon`` at least the longest window (``run_experiment.py
--horizon 7 --save-cohorts ...``). A window is scored on the cohorts whose seasons are all complete (with cohorts
2015-2022 and seasons through 2025: 3 years on 2015-22, 5 on 2015-20, 7 on 2015-18). ``--vs`` pairs two runs window by
window, cohort by cohort (difference, t, cohorts won), on the same windows.
"""
from __future__ import annotations

import argparse
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")      # the tables use box characters; the Windows console code page is cp1252
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import numpy as np  # noqa: E402
import polars as pl  # noqa: E402

import experiments as ex  # noqa: E402
import value  # noqa: E402


def summarize(per: pl.DataFrame) -> pl.DataFrame:
    cols = [c for c in per.columns if c not in ("cohort", "window")]
    return per.group_by("window").agg(pl.len().alias("cohorts"), *[pl.col(c).mean().round(3) for c in cols]).sort("window")


def paired(a: pl.DataFrame, b: pl.DataFrame, metrics=("spearman_top", "mae_top", "bias_top", "spearman_all")) -> pl.DataFrame:
    j = a.join(b, on=["cohort", "window"], suffix="_b")
    rows = []
    for w in sorted(j["window"].unique().to_list()):
        jw = j.filter(pl.col("window") == w)
        for m in metrics:
            if m not in a.columns or f"{m}_b" not in jw.columns:
                continue
            d = (jw[m] - jw[f"{m}_b"]).drop_nulls().to_numpy()
            if len(d) < 2:
                continue
            se = float(np.std(d, ddof=1) / np.sqrt(len(d)))
            rows.append({"window": w, "metric": m, "a": float(jw[m].mean()), "b": float(jw[f"{m}_b"].mean()), "a_minus_b": float(d.mean()),
                         "t": float(d.mean() / se) if se > 0 else float("inf"), "a_wins": int((d > 0).sum()), "cohorts": len(d)})
    return pl.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run")
    ap.add_argument("--windows", nargs="+", type=int, default=[3, 5, 7])
    ap.add_argument("--vs", default=None)
    ap.add_argument("--rate", type=float, default=value.DEFAULT_DISCOUNT_RATE)
    ap.add_argument("--by-age", type=int, default=0, metavar="W", help="also break window W down by age band (<=24, 25-28, 29+)")
    args = ap.parse_args()
    a = ex.window_scores(ex.load_cohorts(args.run), args.windows, args.rate)
    with pl.Config(tbl_rows=-1, tbl_width_chars=240, float_precision=3):
        print(f"== {args.run}: windows {args.windows}, discount {args.rate}")
        print(summarize(a))
        if args.by_age:
            ag = ex.age_band_scores(ex.load_cohorts(args.run), args.by_age, args.rate)
            print(f"== {args.run}: window {args.by_age} by age band")
            print(ag.group_by("band").agg(pl.len().alias("cohorts"), pl.col("n").mean().round(0), pl.col("spearman").mean().round(3), pl.col("mae").mean().round(3),
                                          pl.col("bias").mean().round(3), pl.col("ratio").mean().round(2)).sort("band"))
            if args.vs:
                bg = ex.age_band_scores(ex.load_cohorts(args.vs), args.by_age, args.rate)
                j = ag.join(bg, on=["cohort", "band"], suffix="_b")
                rows = []
                for band in sorted(j["band"].unique().to_list()):
                    jb = j.filter(pl.col("band") == band)
                    for m in ("spearman", "mae", "bias"):
                        d = (jb[m] - jb[f"{m}_b"]).to_numpy()
                        se = float(np.std(d, ddof=1) / np.sqrt(len(d))) if len(d) > 1 else float("nan")
                        rows.append({"band": band, "metric": m, "a_minus_b": float(d.mean()), "t": float(d.mean() / se) if se and se > 0 else None, "a_wins": int((d > 0).sum()), "cohorts": len(d)})
                print(f"== {args.run} minus {args.vs}, by age band (mae: negative = less error)")
                print(pl.DataFrame(rows))
        if args.vs:
            b = ex.window_scores(ex.load_cohorts(args.vs), args.windows, args.rate)
            print(f"== {args.run} minus {args.vs} (for mae_top, negative = less error)")
            print(paired(a, b))


if __name__ == "__main__":
    main()
