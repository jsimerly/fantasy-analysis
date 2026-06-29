"""Gradient-boosted 1-year fantasy-points projection (xgboost).

xgboost is chosen for hardware portability: ``device="cuda"`` vs ``"cpu"`` flips GPU/CPU by a
single param, so the same code trains on a local NVIDIA GPU or on GCP (Vertex) — see the
``ml-model-portability`` note. xgboost also handles missing values natively, so rookie lag
nulls need no imputation. No market/consensus features here → divergence-test-safe.

Returns a ``fit(train) -> predict(test)`` so it drops into ``evaluation.run_walk_forward``.
"""
from __future__ import annotations

from typing import Callable

import numpy as np
import polars as pl
from xgboost import XGBRegressor

from evaluation import FitFn

# year-T features (NaN = missing; xgboost handles it)
NUMERIC_FEATURES = [
    "age_at_season", "exp_at_season", "draft_round", "draft_pick",
    "fpts", "ppg", "games", "pass_yds", "rush_yds", "rec_yds", "targets", "rec",
    "total_touches", "scrim_yds", "total_tds", "yds_per_touch", "target_share_avg", "wopr_avg",
    "lag1_fpts", "lag1_ppg", "lag1_games", "lag1_pass_yds", "lag1_rush_yds", "lag1_rec_yds",
    "lag1_targets", "lag1_rec", "lag1_total_touches", "lag2_fpts", "lag2_ppg", "lag2_games",
    "d_fpts_1", "d_ppg_1",
]
POSITIONS = ["QB", "RB", "WR", "TE"]
FEATURE_COLS = NUMERIC_FEATURES + ["is_undrafted", "is_rookie"] + [f"pos_{p}" for p in POSITIONS]

DEFAULT_PARAMS = dict(
    n_estimators=400, max_depth=5, learning_rate=0.05, subsample=0.8,
    colsample_bytree=0.8, min_child_weight=5, reg_lambda=1.0,
)


def make_feature_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Fixed-schema feature frame (numeric + flags + one-hot position); resilient to
    missing columns so it's testable on small frames and robust to schema drift."""
    out = []
    for c in NUMERIC_FEATURES:
        out.append(pl.col(c).cast(pl.Float64, strict=False) if c in df.columns
                   else pl.lit(None, pl.Float64).alias(c))
    for b in ["is_undrafted", "is_rookie"]:
        out.append((pl.col(b).cast(pl.Int8) if b in df.columns else pl.lit(0, pl.Int8)).alias(b))
    for p in POSITIONS:
        out.append((pl.col("position") == p).cast(pl.Int8).alias(f"pos_{p}") if "position" in df.columns
                   else pl.lit(0, pl.Int8).alias(f"pos_{p}"))
    return df.select(out)


def xgb_regressor(
    target_col: str = "target_fpts_next", device: str = "cpu", seed: int = 0, **params
) -> FitFn:
    p = {**DEFAULT_PARAMS, **params}

    def fit(train: pl.DataFrame):
        model = XGBRegressor(tree_method="hist", device=device, random_state=seed, n_jobs=-1, **p)
        model.fit(make_feature_frame(train).to_numpy(), train[target_col].to_numpy().astype(float))

        def predict(test: pl.DataFrame) -> np.ndarray:
            return np.clip(model.predict(make_feature_frame(test).to_numpy()), 0.0, None)
        return predict
    return fit
