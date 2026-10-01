"""Build today's intrinsic values: project every player's next H seasons from their latest
complete season, convert to discounted points above replacement, compare with the market.

Steps: train the horizon models on every complete season -> project the players who played
in the latest complete season -> replacement levels from the league's lineup -> IV ->
join KTC (superflex) as of today -> fair value / mispricing -> write to the ML bucket.

Players with no row in the latest complete season (2026 rookies, players who missed the whole
year) are not projected yet -- that needs college / prior-year inputs (next phase).

Usage (from machine_learning/):
    uv run python scripts/build_intrinsic_value.py [--horizon 7] [--discount 0.8] [--device cpu] [--no-write]
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

import career  # noqa: E402
import gcs_io  # noqa: E402
import market  # noqa: E402
import replacement  # noqa: E402
import value  # noqa: E402

SETTINGS_PATH = "silver/fantasy/dim_league_settings/data.parquet"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", type=int, default=10,
                    help="seasons projected; elite QBs play 15+, so the cap, not the discount, is what "
                         "would under-value them -- the discount already prices distance in time")
    ap.add_argument("--discount", type=float, default=value.DEFAULT_DISCOUNT)
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--sensitivity", action="store_true",
                    help="also print the top 10 under horizon x discount alternatives")
    args = ap.parse_args()
    H = list(range(1, args.horizon + 1))
    today = datetime.now(timezone.utc).date()

    df = career.build_career_matrix(H)
    last = career.last_complete_season(df)
    slots, teams = replacement.league_lineup(gcs_io.read_lake(SETTINGS_PATH))
    starters = replacement.starters_per_position(slots, teams)
    rep = replacement.replacement_levels(df, starters)
    print(f"lineup {slots} x {teams} teams")
    print("starters per position:", {k: round(v, 1) for k, v in starters.items()})
    print("replacement ppg:", {k: round(v, 2) for k, v in rep.items()})

    models = career.HorizonModels(H, device=args.device).fit(df, as_of_season=last)
    sigma = models.estimate_sigma(df, as_of_season=last)
    print("out-of-sample ppg spread by horizon (all positions):",
          {k: round(v["__all__"], 2) for k, v in sigma.items()})
    current = df.filter(pl.col("season") == last)
    proj = value.intrinsic_value(models.predict(current), rep, H, args.discount)
    proj = market.attach_market(proj, today)
    cmp, summary = value.compare_to_market(proj)
    print(f"\nprojected {proj.height} players from season {last}; {summary['n']} have a KTC value "
          f"(as of {today}); spearman(IV, KTC) = {summary['spearman']:.3f}")

    show = ["player_name", "position", "age", "iv", "h1", "h2", "h3", "ktc_value", "fair_value", "mispricing_pct", "market_rank", "iv_rank"]
    view = cmp.with_columns(
        pl.col("age_at_season").round(0).alias("age"), pl.col("iv").round(0),
        pl.col("h1_fpts_hat").round(0).alias("h1"), pl.col("h2_fpts_hat").round(0).alias("h2"),
        pl.col("h3_fpts_hat").round(0).alias("h3"), pl.col("fair_value").round(0), pl.col("mispricing_pct").round(2),
    )
    with pl.Config(tbl_rows=-1, tbl_width_chars=180, fmt_str_lengths=22):
        print("\nTop 25 by intrinsic value (discounted points above replacement over the next "
              f"{args.horizon} seasons; h1..h3 = projected season points):")
        print(view.sort("iv", descending=True).select(show).head(25))
        liquid = view.filter(pl.col("ktc_value") >= 1500)
        print("\nMost UNDER-valued by the market (KTC >= 1500; mispricing_pct = (market - fair) / fair):")
        print(liquid.sort("mispricing_pct").select(show).head(15))
        print("\nMost OVER-valued by the market:")
        print(liquid.sort("mispricing_pct", descending=True).select(show).head(15))
        print("\nTop 10 by IV among players with NO market value (unmatched or unpriced):")
        print(proj.filter(pl.col("ktc_value").is_null()).sort("iv", descending=True)
              .select("player_name", "position", pl.col("age_at_season").round(0).alias("age"), pl.col("iv").round(0), "market_match").head(10))

    if args.sensitivity:
        pred = models.predict(current)
        watch = ["Drake Maye", "Jared Goff", "Josh Allen", "Caleb Williams", "Puka Nacua"]
        print("\nSensitivity: top 10 by IV under alternative horizon / discount (same projections):")
        for h in sorted({5, 7, args.horizon}):
            for disc in (0.7, 0.8, 0.9):
                hh = [k for k in H if k <= h]
                alt = value.intrinsic_value(pred, rep, hh, disc).sort("iv", descending=True)
                names = alt["player_name"].head(10).to_list()
                ranks = {n: int(alt.with_row_index("r").filter(pl.col("player_name") == n)["r"][0]) + 1
                         for n in watch if n in alt["player_name"].to_list()}
                print(f"  H={h:2d} discount={disc}: {', '.join(names)}  | ranks {ranks}")

    if not args.no_write:
        run = today.isoformat()
        p1 = gcs_io.write_ml_parquet(proj, "intrinsic_value", f"as_of_season={last}", f"run_date={run}", "projections.parquet")
        p2 = gcs_io.write_ml_json({
            "run_date": run, "as_of_season": last, "horizon": args.horizon, "discount": args.discount,
            "lineup": slots, "teams": teams, "starters": starters, "replacement_ppg": rep,
            "n_projected": proj.height, "n_with_market": summary["n"], "spearman_iv_vs_ktc": summary["spearman"],
            "ppg_sigma": sigma, "features": career.FEATURES, "model_params": models.params,
        }, "intrinsic_value", f"as_of_season={last}", f"run_date={run}", "metrics.json")
        print(f"\nwrote {p1}\n      {p2}")


if __name__ == "__main__":
    main()
