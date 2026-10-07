"""Baselines the projection model must beat (MAE/RMSE under walk-forward CV).

1. **naive carry-forward** — predict next-season fpts = this-season fpts.
2. **age-adjusted naive** — this-season fpts x a (position, age-bucket) year-over-year ratio
   learned *from the training fold only* (so it stays leak-safe and improves on flat carry).

Each returns a ``fit(train) -> predict(test) -> np.ndarray`` so they plug into
``evaluation.run_walk_forward`` exactly like the model.
"""
from __future__ import annotations

import numpy as np
import polars as pl

from evaluation import FitFn


def carry_forward() -> FitFn:
    def fit(train: pl.DataFrame):
        def predict(test: pl.DataFrame) -> np.ndarray:
            return test["fpts"].to_numpy().astype(float)
        return predict
    return fit


def age_adjusted_naive(target_col: str = "target_fpts_next", bucket: int = 2) -> FitFn:
    def fit(train: pl.DataFrame):
        scored = train.filter(pl.col("fpts") > 0).with_columns(
            (pl.col("age_at_season") // bucket * bucket).alias("_ab"),
        )
        # POOLED ratio (sum/sum) per bucket, not mean-of-row-ratios: a player with ~0 fpts
        # makes target/fpts explode, so averaging row ratios is unstable. Pooling is robust.
        ratios = scored.group_by("position", "_ab").agg(
            (pl.col(target_col).sum() / pl.col("fpts").sum()).alias("_r"))
        denom = float(scored["fpts"].sum())
        global_ratio = float(scored[target_col].sum() / denom) if denom else 1.0

        def predict(test: pl.DataFrame) -> np.ndarray:
            t = (
                test.with_columns((pl.col("age_at_season") // bucket * bucket).alias("_ab"))
                .join(ratios, on=["position", "_ab"], how="left")
                .with_columns(pl.col("_r").fill_null(global_ratio))
            )
            return (t["fpts"] * t["_r"]).to_numpy().astype(float)
        return predict
    return fit
