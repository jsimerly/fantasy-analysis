"""Walk-forward evaluation of the career projection, one row per horizon.

For each test season s >= --start-season: train on earlier rows using only outcomes known by
the end of s, then score the season-s rows on every horizon whose outcome is observable.
Methods: the model (ppg x games), naive carry-forward, and the age-aware decay baseline.

Usage (from machine_learning/):
    uv run python scripts/evaluate_career.py [--horizons 5] [--start-season 2010] [--device cpu|cuda]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

import career  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizons", type=int, default=5)
    ap.add_argument("--start-season", type=int, default=2010)
    ap.add_argument("--device", default="cpu")
    args = ap.parse_args()
    horizons = list(range(1, args.horizons + 1))

    t0 = time.time()
    df = career.build_career_matrix(horizons)
    print(f"career matrix: {df.shape}, last complete season {career.last_complete_season(df)}")
    per_fold, agg = career.walk_forward_horizon_eval(df, horizons, args.start_season, device=args.device)
    print(f"walk-forward done in {time.time() - t0:.0f}s\n")

    table = agg.pivot(values="mae", index="horizon", on="method").with_columns(
        (100 * (1 - pl.col("model") / pl.col("carry_forward"))).round(1).alias("model_%_vs_carry"),
        (100 * (1 - pl.col("model") / pl.col("decay"))).round(1).alias("model_%_vs_decay"),
    ).join(agg.filter(pl.col("method") == "model").select("horizon", "n_total", "folds"), on="horizon").sort("horizon")
    print("MAE in season fantasy points by horizon (season T+k outcome; 0 when the player did not play):")
    with pl.Config(tbl_rows=-1, float_precision=2):
        print(table)
    print("\nRMSE by horizon:")
    with pl.Config(tbl_rows=-1, float_precision=2):
        print(agg.pivot(values="rmse", index="horizon", on="method").sort("horizon"))


if __name__ == "__main__":
    main()
