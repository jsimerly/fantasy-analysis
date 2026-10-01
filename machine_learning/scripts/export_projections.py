"""Export the latest intrinsic-value run as JSON for a browsable table (page data file).

Usage (from machine_learning/):
    uv run python scripts/export_projections.py --run-date 2026-10-01 --out projections.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

import gcs_io  # noqa: E402
import value  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of-season", type=int, default=2025)
    ap.add_argument("--run-date", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    base = ("intrinsic_value", f"as_of_season={args.as_of_season}", f"run_date={args.run_date}")

    proj = gcs_io.read_ml_parquet(*base, "projections.parquet")
    meta = gcs_io.read_ml_json(*base, "metrics.json")
    if "fair_value" not in proj.columns:                      # older runs: derive the comparison here
        cmp, summary = value.compare_to_market(proj)
        proj = proj.join(
            cmp.select("player_id", "fair_value", "mispricing", "mispricing_pct", "iv_rank", "market_rank", "rank_gap"),
            on="player_id", how="left",
        ).with_columns(pl.col("iv").rank(method="ordinal", descending=True).alias("iv_rank_all"))
        meta["spearman_iv_vs_ktc"] = summary["spearman"]

    hcols = [c for c in proj.columns if c.startswith("h") and c.endswith("_fpts_hat")]
    horizons = sorted(int(c[1:].split("_")[0]) for c in hcols)
    rows = []
    for r in proj.sort("iv", descending=True).iter_rows(named=True):
        rows.append({
            "name": r["player_name"], "pos": r["position"], "team": r.get("team"),
            "age": round(r["age_at_season"], 1) if r.get("age_at_season") is not None else None,
            "fpts": round(r["fpts"]), "games": r["games"],
            "iv": round(r["iv"], 1), "iv_rank_all": int(r["iv_rank_all"]),
            "ktc": r.get("ktc_value"), "market_rank": r.get("market_rank"), "iv_rank": r.get("iv_rank"),
            "rank_gap": r.get("rank_gap"),
            "fair": round(r["fair_value"]) if r.get("fair_value") is not None else None,
            "mis_pct": round(r["mispricing_pct"], 3) if r.get("mispricing_pct") is not None else None,
            "match": r.get("market_match"),
            "h": [round(r[f"h{k}_fpts_hat"]) for k in horizons],
            "h1_ppg": round(r["h1_ppg_hat"], 1), "h1_games": round(r["h1_games_hat"], 1),
        })
    out = {
        "as_of_season": meta["as_of_season"], "run_date": meta["run_date"], "horizon": meta["horizon"],
        "horizons": horizons, "discount": meta["discount"], "replacement_ppg": meta["replacement_ppg"],
        "n": len(rows), "n_priced": sum(1 for x in rows if x["ktc"] is not None),
        "spearman": meta.get("spearman_iv_vs_ktc"), "rows": rows,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")
    print(f"wrote {args.out} ({args.out.stat().st_size // 1024} KB, {len(rows)} players, {out['n_priced']} priced)")


if __name__ == "__main__":
    main()
