"""Backtest: does intrinsic value predict REALIZED future value better than the market did?

For each cohort season T:
  * train the horizon models as of T (only outcomes known by the end of season T),
  * intrinsic value IV_T for every player with a season-T row (H horizons, discounted),
  * realized value = the same discounted points-above-replacement formula applied to what
    actually happened in T+1..T+H,
  * the market's view = KTC superflex value as of Feb 15 of T+1 (season over, before the
    rookie draft / free agency),
and compare rank correlations with realized value on the players KTC priced at the time.

Usage (from machine_learning/):
    uv run python scripts/backtest_value.py [--horizon 3] [--discount 0.8] [--device cpu]
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import numpy as np  # noqa: E402
import polars as pl  # noqa: E402

import career  # noqa: E402
import gcs_io  # noqa: E402
import market  # noqa: E402
import replacement  # noqa: E402
import value  # noqa: E402

SETTINGS_PATH = "silver/fantasy/dim_league_settings/data.parquet"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", type=int, default=3)
    ap.add_argument("--discount", type=float, default=value.DEFAULT_DISCOUNT)
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--first-cohort", type=int, default=2020)
    args = ap.parse_args()
    H = list(range(1, args.horizon + 1))

    df = career.build_career_matrix(H)
    last = career.last_complete_season(df)
    slots, teams = replacement.league_lineup(gcs_io.read_lake(SETTINGS_PATH))
    starters = replacement.starters_per_position(slots, teams)
    hist, crosswalk = market.load_ktc_history(), market.load_crosswalk()
    print(f"lineup {slots} x {teams} teams -> starters {dict((k, round(v, 1)) for k, v in starters.items())}")

    rows, examples = [], []
    for T in range(args.first_cohort, last - args.horizon + 1):
        as_of = date(T + 1, 2, 15)
        rep = replacement.replacement_levels(df, starters, seasons=list(range(T - 4, T + 1)))
        models = career.HorizonModels(H, device=args.device).fit(df, as_of_season=T)
        models.estimate_sigma(df, as_of_season=T)
        cohort = models.predict(df.filter(pl.col("season") == T))
        cohort = value.realized_value(value.intrinsic_value(cohort, rep, H, args.discount), rep, H, args.discount)
        cohort = market.attach_market(cohort, as_of, hist, crosswalk)
        both = cohort.filter(pl.col("ktc_value").is_not_null() & pl.col("realized_iv").is_not_null())
        iv, kt, rz = (both[c].to_numpy().astype(float) for c in ("iv", "ktc_value", "realized_iv"))
        blend = pl.Series(iv).rank().to_numpy() + pl.Series(kt).rank().to_numpy()
        rows.append({
            "cohort_T": T, "ktc_as_of": as_of, "n_players": both.height,
            "spearman_iv_vs_realized": value.spearman(iv, rz),
            "spearman_ktc_vs_realized": value.spearman(kt, rz),
            "spearman_blend_vs_realized": value.spearman(blend, rz),
            "spearman_iv_vs_ktc": value.spearman(iv, kt),
        })
        cmp, _ = value.compare_to_market(both)
        ex = cmp.with_columns(pl.col("realized_iv").rank(method="ordinal", descending=True).alias("realized_rank")).select(
            pl.lit(T).alias("T"), "player_name", "position", pl.col("age_at_season").round(0).alias("age"),
            "ktc_value", pl.col("fair_value").round(0), pl.col("mispricing_pct").round(2), "market_rank", "iv_rank", "realized_rank")
        examples.append(ex.sort("mispricing_pct").head(5))        # market cheapest vs fundamentals
        examples.append(ex.sort("mispricing_pct", descending=True).head(5))

    out = pl.DataFrame(rows)
    print(f"\nRank correlation with REALIZED discounted value over the next {args.horizon} seasons "
          f"(discount {args.discount}), on the players KTC priced at the time:")
    with pl.Config(tbl_rows=-1, float_precision=3, tbl_width_chars=160):
        print(out)
        print("\nmean over cohorts:")
        print(out.select(pl.col("^spearman_.*$").mean()))
    print("\nPer cohort: the 5 players the market priced LOWEST vs fundamentals, then the 5 HIGHEST "
          "(realized_rank = where they actually finished):")
    with pl.Config(tbl_rows=-1, tbl_width_chars=170, fmt_str_lengths=24):
        print(pl.concat(examples))


if __name__ == "__main__":
    main()
