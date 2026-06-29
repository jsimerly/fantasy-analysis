"""Walk-forward (expanding-window) evaluation for the 1-year projection.

Time-respecting CV: for each test feature-season ``s``, train on every season ``< s`` and
test on ``s`` (whose outcome is season ``s+1``). A player's row is wholly in train or test by
its season, and we never train on the future — the leak-safe split the methodology requires.
Metrics are MAE + RMSE, aggregated across folds weighted by test size.
"""
from __future__ import annotations

from typing import Callable, Iterator

import numpy as np
import polars as pl

# A "method" is fit(train_df) -> predict(test_df) -> np.ndarray
FitFn = Callable[[pl.DataFrame], Callable[[pl.DataFrame], np.ndarray]]


def mae(y_true, y_pred) -> float:
    return float(np.mean(np.abs(np.asarray(y_true, float) - np.asarray(y_pred, float))))


def rmse(y_true, y_pred) -> float:
    return float(np.sqrt(np.mean((np.asarray(y_true, float) - np.asarray(y_pred, float)) ** 2)))


def walk_forward_splits(
    df: pl.DataFrame, season_col: str = "season", start_season: int | None = None
) -> Iterator[tuple[pl.DataFrame, pl.DataFrame, int]]:
    """Yield ``(train, test, test_season)`` for each season >= ``start_season`` (train = < s)."""
    seasons = sorted(df[season_col].unique().to_list())
    if start_season is None and seasons:
        start_season = seasons[len(seasons) // 2]  # default: evaluate the back half
    for s in seasons:
        if start_season is not None and s < start_season:
            continue
        train = df.filter(pl.col(season_col) < s)
        test = df.filter(pl.col(season_col) == s)
        if train.height and test.height:
            yield train, test, s


def run_walk_forward(
    df: pl.DataFrame, methods: dict[str, FitFn], target_col: str,
    season_col: str = "season", start_season: int | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Run every method across the walk-forward folds. Returns ``(per_fold, aggregate)``."""
    splits = list(walk_forward_splits(df, season_col, start_season))
    rows = []
    for name, fit in methods.items():
        for train, test, s in splits:
            predict = fit(train)
            y_pred = np.asarray(predict(test), dtype=float)
            y_true = test[target_col].to_numpy().astype(float)
            rows.append({"method": name, "test_season": s, "n": test.height,
                         "mae": mae(y_true, y_pred), "rmse": rmse(y_true, y_pred)})
    per_fold = pl.DataFrame(rows)
    agg = (
        per_fold.group_by("method").agg(
            ((pl.col("mae") * pl.col("n")).sum() / pl.col("n").sum()).alias("mae"),
            ((pl.col("rmse") * pl.col("n")).sum() / pl.col("n").sum()).alias("rmse"),
            pl.col("n").sum().alias("n_total"),
            pl.len().alias("folds"),
        ).sort("mae")
    )
    return per_fold, agg
