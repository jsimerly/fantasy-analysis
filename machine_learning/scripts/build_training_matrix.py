"""Build the in-memory training matrix (player-season features + T+1 target) and summarize.

Reads the reusable ``fact_player_season`` dataset from the lake and attaches the
model-specific lags/target. Nothing is persisted — this is regenerated each training run.

Usage (from machine_learning/):
    uv run python scripts/build_training_matrix.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

import features  # noqa: E402


def main() -> None:
    table = features.build_training_matrix()
    print(f"training matrix shape: {table.shape}  (from gs://.../{features.FACT_PATH})")

    print("\nrows by season:")
    with pl.Config(tbl_rows=-1):
        print(table.group_by("season").len().sort("season"))

    print("\nrows by position:")
    print(table.group_by("position").len().sort("len", descending=True))

    print("\ntop 15 player-seasons by re-scored fantasy points (superflex QB sanity):")
    cols = ["season", "player_name", "position", "fpts", "ppg", "games", "target_fpts_next"]
    with pl.Config(tbl_rows=20, fmt_str_lengths=30):
        print(table.sort("fpts", descending=True).select(cols).head(15))


if __name__ == "__main__":
    main()
