"""Phase 1: walk-forward eval of the baselines vs the xgboost 1-year projection.

Usage (from machine_learning/):
    uv run python scripts/evaluate_phase1.py [--start-season 2010] [--device cpu|cuda]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

import baselines  # noqa: E402
import evaluation  # noqa: E402
import features  # noqa: E402
import model  # noqa: E402

TARGET = "target_fpts_next"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-season", type=int, default=2010)
    ap.add_argument("--device", default="cpu", help="cpu | cuda")
    args = ap.parse_args()

    df = features.build_training_matrix()
    methods = {
        "carry_forward": baselines.carry_forward(),
        "age_adjusted": baselines.age_adjusted_naive(TARGET),
        "xgboost": model.xgb_regressor(TARGET, device=args.device),
    }
    per_fold, agg = evaluation.run_walk_forward(df, methods, TARGET, start_season=args.start_season)

    print(f"\nWalk-forward CV — target={TARGET}, rows={df.height}, "
          f"test seasons {args.start_season}+ ({agg['folds'][0]} folds), device={args.device}")
    print("\nAggregate (MAE/RMSE in fantasy points, lower is better):")
    with pl.Config(tbl_rows=-1, float_precision=2):
        print(agg)
        skill = agg.filter(pl.col("method") != "carry_forward").with_columns(
            (100 * (1 - pl.col("mae") / agg.filter(pl.col("method") == "carry_forward")["mae"][0]))
            .round(1).alias("mae_%_better_than_carry"))
        print("\nSkill vs naive carry-forward:")
        print(skill.select("method", "mae", "mae_%_better_than_carry"))
        print("\nPer-fold MAE (xgboost vs carry_forward):")
        print(per_fold.filter(pl.col("method").is_in(["xgboost", "carry_forward"]))
              .pivot(values="mae", index="test_season", on="method").sort("test_season"))


if __name__ == "__main__":
    main()
