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
    uv run python scripts/backtest_value.py [--horizon 3] [--discount-rate 0.2] [--device cpu]
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
    ap.add_argument("--discount-rate", type=float, default=value.DEFAULT_DISCOUNT_RATE)
    ap.add_argument("--no-write", action="store_true", help="do not persist the summary to the ML bucket")
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

    rows, examples, terciles, boot_all = [], [], [], []
    for T in range(args.first_cohort, last - args.horizon + 1):
        as_of = date(T + 1, 2, 15)
        rep = replacement.replacement_levels(df, starters, seasons=list(range(T - 4, T + 1)))
        models = career.HorizonModels(H, device=args.device).fit(df, as_of_season=T)
        models.estimate_sigma(df, as_of_season=T)
        cohort = models.predict(df.filter(pl.col("season") == T))
        cohort = value.realized_value(value.intrinsic_value(cohort, rep, H, args.discount_rate), rep, H, args.discount_rate)
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
        # how much better / worse than the market, with uncertainty: paired bootstrap over players
        rng = np.random.default_rng(0)
        draws = []
        for _ in range(1000):
            idx = rng.integers(0, both.height, both.height)
            draws.append(value.spearman(iv[idx], rz[idx]) - value.spearman(kt[idx], rz[idx]))
        rows[-1].update({"iv_minus_ktc": float(np.mean(draws)),
                         "ci90_lo": float(np.percentile(draws, 5)), "ci90_hi": float(np.percentile(draws, 95))})
        boot_all.extend(draws)

        cmp, _ = value.compare_to_market(both)
        cmp = cmp.with_columns(pl.col("realized_iv").rank(method="ordinal", descending=True).cast(pl.Int64).alias("realized_rank")).with_columns(
            (pl.col("market_rank") - pl.col("realized_rank")).alias("beat_market_by"),   # + = finished better than the market ranked him
            pl.col("mispricing_pct").qcut(3, labels=["market cheap vs IV", "fairly priced", "market rich vs IV"]).alias("tercile"),
        )
        terciles.append(cmp.group_by("tercile").agg(pl.len().alias("n"), pl.col("beat_market_by").mean().alias("beat_market_by"))
                        .with_columns(pl.lit(T).alias("T")))
        ex = cmp.select(
            pl.lit(T).alias("T"), "player_name", "position", pl.col("age_at_season").round(0).alias("age"),
            "ktc_value", pl.col("fair_value").round(0), pl.col("mispricing_pct").round(2), "market_rank", "iv_rank", "realized_rank")
        ex = ex.with_columns(pl.col("fair_value").round(0))
        examples.append(ex.sort("mispricing_pct").head(5))        # market cheapest vs fundamentals
        examples.append(ex.sort("mispricing_pct", descending=True).head(5))

    out = pl.DataFrame(rows)
    print(f"\nRank correlation with REALIZED discounted value over the next {args.horizon} seasons "
          f"(discount rate {args.discount_rate:.0%}), on the players KTC priced at the time:")
    with pl.Config(tbl_rows=-1, float_precision=3, tbl_width_chars=160):
        print(out)
        print("\nmean over cohorts:")
        print(out.select(pl.col("^spearman_.*$").mean()))
        print(f"\nIV minus KTC rank correlation, paired bootstrap pooled over cohorts: "
              f"mean {np.mean(boot_all):+.3f}, 90% CI [{np.percentile(boot_all, 5):+.3f}, {np.percentile(boot_all, 95):+.3f}]  "
              f"(a CI straddling 0 = statistically a tie)")
        print("\nIs the mispricing signal actionable? Players bucketed by (market - fair) / fair; "
              "beat_market_by = market rank - realized rank (positive = finished better than the market had him):")
        tc = pl.concat(terciles)
        print(tc.pivot(values="beat_market_by", index="tercile", on="T").sort("tercile"))
        print(tc.group_by("tercile").agg(pl.col("n").sum().alias("n"),
                                         pl.col("beat_market_by").mean().alias("mean_beat_market_by")).sort("tercile"))
    print("\nPer cohort: the 5 players the market priced LOWEST vs fundamentals, then the 5 HIGHEST "
          "(realized_rank = where they actually finished):")
    with pl.Config(tbl_rows=-1, tbl_width_chars=170, fmt_str_lengths=24):
        print(pl.concat(examples))
    if not args.no_write:                       # persist for the performance panel / later comparison
        from datetime import datetime, timezone
        run = datetime.now(timezone.utc).date().isoformat()
        summary = {
            "run_date": run, "horizon": args.horizon, "first_cohort": args.first_cohort, "discount_rate": args.discount_rate,
            "per_cohort": out.with_columns(pl.col("ktc_as_of").cast(pl.Utf8)).to_dicts(),
            "mean": out.select(pl.col("^spearman_.*$").mean()).to_dicts()[0],
            "bootstrap": {"iv_minus_ktc": float(np.mean(boot_all)), "ci90_lo": float(np.percentile(boot_all, 5)), "ci90_hi": float(np.percentile(boot_all, 95))},
            "terciles": tc.group_by("tercile").agg(pl.col("n").sum().alias("n"), pl.col("beat_market_by").mean().alias("mean_beat_market_by"))
                          .with_columns(pl.col("tercile").cast(pl.Utf8)).sort("tercile").to_dicts(),
        }
        p = gcs_io.write_ml_json(summary, "backtests", "value", f"run_date={run}", "summary.json")
        print("\nwrote", p)


if __name__ == "__main__":
    main()
