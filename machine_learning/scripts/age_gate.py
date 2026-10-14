"""The level by age band: realized / priced wins on the players a manager would weigh, from a run's saved cohort frames
(review 2026-10-13, C1: the dominant systematic error is an age gradient -- under-25s realize ~1.2x what they are priced
at, 29+ ~0.9x, rookies 2-3x at seasons T+2 / T+3 -- and the harness's co-primaries cannot see it). A gate to read beside
every verdict, and the thing a pricing change (joint, band widening) must not make worse on either end.

  python scripts/age_gate.py tabpfn35_set_stacked_blend_w2
  python scripts/age_gate.py candidate baseline          # the difference per band with the cohort as the unit

The set is the union of both runs' top ``--top`` players by projected WAR per cohort (the fixed-set reading of item 45),
so a variant cannot score by changing who is in the list.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

BANDS = [("<25", 0.0, 25.0), ("25-28", 25.0, 29.0), ("29+", 29.0, 99.0)]
POSITIONS = ["QB", "RB", "WR", "TE"]


def age_level(frames: dict[str, pl.DataFrame], top: int = 150, proj: str = "war", real: str = "realized_war") -> pl.DataFrame:
    """Per (run, cohort): realized / priced on the union of the runs' top-``top`` by ``proj``, overall, by age band and
    by position. ``frames``: run name -> its cohort frames (columns cohort, player_id, age_at_season, position, proj, real)."""
    rows = []
    cohorts = sorted(set.union(*[set(f["cohort"].unique().to_list()) for f in frames.values()]))
    for T in cohorts:
        per = {n: f.filter((pl.col("cohort") == T) & pl.col(real).is_not_null()) for n, f in frames.items()}
        union: set[str] = set()
        for d in per.values():
            union |= set(d.sort(proj, descending=True).head(top)["player_id"].to_list())
        for n, d in per.items():
            s = d.filter(pl.col("player_id").is_in(list(union)))
            if s.height == 0:
                continue
            r = {"run": n, "cohort": int(T), "n": s.height, "ratio": _ratio(s, proj, real)}
            for b, lo, hi in BANDS:
                r[f"ratio_{b}"] = _ratio(s.filter((pl.col("age_at_season") >= lo) & (pl.col("age_at_season") < hi)), proj, real)
            for p in POSITIONS:
                r[f"ratio_{p}"] = _ratio(s.filter(pl.col("position") == p), proj, real)
            rows.append(r)
    t = pl.DataFrame(rows)
    return t.with_columns([pl.col(c).cast(pl.Float64) for c in t.columns if c.startswith("ratio")])   # an empty band is a null float, not a Null column


def _ratio(s: pl.DataFrame, proj: str, real: str) -> float | None:
    if s.height == 0:
        return None
    p = float(s[proj].fill_null(0.0).sum())
    return float(s[real].fill_null(0.0).sum()) / p if p > 0 else None


def compare(table: pl.DataFrame, a: str, b: str) -> pl.DataFrame:
    """Per ratio column: A minus B with the cohort as the unit (mean, t over cohorts, cohorts where A is closer to 1)."""
    cols = [c for c in table.columns if c.startswith("ratio")]
    ta, tb = table.filter(pl.col("run") == a), table.filter(pl.col("run") == b)
    j = ta.join(tb, on="cohort", suffix="_b")
    out = []
    for c in cols:
        if j[c].null_count() == j.height or j[f"{c}_b"].null_count() == j.height:     # an empty band on either side
            continue
        d = (j[c] - j[f"{c}_b"]).drop_nulls().to_numpy()
        closer = ((j[c] - 1).abs() < (j[f"{c}_b"] - 1).abs()).sum()
        t = float(d.mean() / (d.std(ddof=1) / np.sqrt(len(d)))) if len(d) > 1 and d.std(ddof=1) > 0 else None
        out.append({"band": c.replace("ratio_", "").replace("ratio", "all"), "a": float(j[c].mean()), "b": float(j[f"{c}_b"].mean()),
                    "a_minus_b": float(d.mean()) if len(d) else None, "t_cohort": t, "a_closer_to_1": int(closer), "cohorts": int(len(d))})
    return pl.DataFrame(out)


def main() -> None:
    import experiments as ex
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="+", help="one run (its level), or candidate then baseline (the difference)")
    ap.add_argument("--top", type=int, default=150)
    args = ap.parse_args()
    frames = {r: ex.load_cohorts(r) for r in args.runs}
    table = age_level(frames, args.top)
    with pl.Config(tbl_rows=-1, tbl_cols=20, tbl_width_chars=220, float_precision=3):
        print("realized / priced (wins) on the fixed top set, mean over cohorts; 1.00 = level, above = under-priced")
        print(table.group_by("run", maintain_order=True).agg(pl.col("n").mean(), *[pl.col(c).mean() for c in table.columns if c.startswith("ratio")]))
        if len(args.runs) == 2:
            print(f"\n{args.runs[0]} minus {args.runs[1]}, the cohort as the unit")
            print(compare(table, args.runs[0], args.runs[1]))


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    main()
