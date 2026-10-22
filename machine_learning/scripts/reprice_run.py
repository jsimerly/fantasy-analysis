"""Re-price an in-season run with another age-level table, no GPU (owner, 2026-10-14: "why can't we layer this curve on the
post projection run data? Why do we have to wait until new projections come out of the GPU?").

The career spans' projections (ppg, games, the band) are the GPU's work; the age-level correction is a scale on their
points above replacement, applied afterwards. This reads the run's ``projections.parquet`` and ``metrics.json``, rescales
``h{k}_vorp_hat`` (k >= 3) by new factor / old factor per player, recomputes ``iv_inseason`` and ``iv_preseason`` and the
market comparison, and writes the run back with the new table in its metrics (the originals are kept beside them as
``projections.before_<stamp>.parquet`` / ``metrics.before_<stamp>.json``). ``weekly_refresh --skip inseason`` then
rebuilds the WAR, the analysis summaries and the exports from it.

  python scripts/reprice_run.py --season 2026 --week 5 --run-date 2026-10-14 --age-scale tabpfn35_set_stacked_blend_w2 --age-scale-recent --age-scale-curve
  (add --dry-run to see the table and the effect without writing)
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import gcs_io  # noqa: E402
import value  # noqa: E402


def reprice(proj: pl.DataFrame, old_table: dict | None, new_table: dict | None, discount_rate: float) -> pl.DataFrame:
    """The pure step: every career span's points above replacement times new / old age factor, the values re-summed,
    the preseason view rescaled, the market comparison redone. The in-season spans (rest of season, next season) are
    untouched: the correction is the career model's."""
    fac_old, fac_new = value.age_scale_factors(proj, old_table), value.age_scale_factors(proj, new_table)
    ratio = np.where(fac_old > 0, fac_new / fac_old, 1.0)
    ks = sorted(int(c[1:-9]) for c in proj.columns if c.startswith("h") and c.endswith("_vorp_hat") and c[1:-9].isdigit() and int(c[1:-9]) >= 3)
    out = proj.with_columns([pl.Series(f"h{k}_vorp_hat", proj[f"h{k}_vorp_hat"].fill_null(0.0).to_numpy().astype(float) * ratio) for k in ks])
    iv = (out["vorp_ros"].fill_null(0.0).to_numpy().astype(float) + value.discount_weight(2, discount_rate) * out["vorp_next"].fill_null(0.0).to_numpy().astype(float)
          + sum(value.discount_weight(k, discount_rate) * out[f"h{k}_vorp_hat"].to_numpy().astype(float) for k in ks))
    out = out.with_columns(pl.Series("iv_inseason", iv), pl.Series("_ratio", ratio))
    if "iv_preseason" in out.columns:
        out = out.with_columns((pl.col("iv_preseason") * pl.col("_ratio")).alias("iv_preseason"))
    cmp_cols = ["fair_value", "mispricing", "mispricing_pct", "iv_rank", "market_rank"]
    if "ktc_value" in out.columns and out["ktc_value"].is_not_null().sum() > 10:
        cmp, _ = value.compare_to_market(out.drop([c for c in cmp_cols if c in out.columns]), iv_col="iv_inseason")
        out = out.drop([c for c in cmp_cols if c in out.columns]).join(cmp.select(["player_id"] + [c for c in cmp_cols if c in cmp.columns]), on="player_id", how="left")
    return out.drop("_ratio")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--run-date", required=True)
    ap.add_argument("--age-scale", required=True, metavar="RUN", help="the run whose saved cohort frames the table is fitted on")
    ap.add_argument("--age-scale-by-position", action="store_true")
    ap.add_argument("--age-scale-curve", action="store_true")
    ap.add_argument("--age-scale-recent", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    import experiments as ex
    parts = ("inseason", f"season={args.season}", f"week={args.week}", f"run_date={args.run_date}")
    proj, meta = gcs_io.read_ml_parquet(*parts, "projections.parquet"), gcs_io.read_ml_json(*parts, "metrics.json")
    old = meta.get("age_scale") or None
    new = ex.age_scale_table(args.age_scale, by_position=args.age_scale_by_position, curve=args.age_scale_curve, recent=args.age_scale_recent)
    rate = float(meta.get("discount_rate", 0.2))
    print(f"run {args.run_date} ({meta.get('career_backend')}): {proj.height} players; discount {rate}")
    print("old table:", {k: round(v, 2) for k, v in (old or {}).items()} or "none (factor 1)")
    print("new table:", ", ".join(f"{k} x{v:.2f}" for k, v in sorted(new.items()) if not (k.endswith("_mean") or k.endswith("_se"))))
    out = reprice(proj, old, new, rate)
    moved = out.select("player_id", "player_name", "position", "age_at_season", "iv_inseason").join(proj.select("player_id", pl.col("iv_inseason").alias("iv_before")), on="player_id")
    moved = moved.with_columns((pl.col("iv_inseason") / pl.col("iv_before")).alias("x")).filter(pl.col("iv_before") > 50).sort("x")
    with pl.Config(tbl_rows=8, float_precision=2, tbl_width_chars=160):
        print("most marked down:"); print(moved.head(6)); print("most marked up:"); print(moved.tail(6))
    print(f"iv_inseason: NaN {int(np.isnan(out['iv_inseason'].to_numpy()).sum())}; mean before {proj['iv_inseason'].mean():.1f} after {out['iv_inseason'].mean():.1f}")
    if args.dry_run:
        print("dry run: nothing written"); return
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    gcs_io.write_ml_parquet(proj, *parts, f"projections.before_{stamp}.parquet")
    gcs_io.write_ml_json(meta, *parts, f"metrics.before_{stamp}.json")
    meta = dict(meta, age_scale=new, age_scale_run=args.age_scale, age_scale_by_position=args.age_scale_by_position, age_scale_curve=args.age_scale_curve,
                age_scale_recent=args.age_scale_recent, repriced={"at": stamp, "from_age_scale": old, "n_with_market": int(out["ktc_value"].is_not_null().sum()) if "ktc_value" in out.columns else None})
    print("wrote", gcs_io.write_ml_parquet(out, *parts, "projections.parquet"))
    print("wrote", gcs_io.write_ml_json(meta, *parts, "metrics.json"))
    print("next: weekly_refresh.py --preset production --skip inseason (WAR, the analysis summaries, the exports), then publish")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    main()
