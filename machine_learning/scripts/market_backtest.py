"""Model vs market backtest (BACKLOG 23): are the projections beating KTC, and where.

Each cohort T is scored the way a manager would have used the model: the career model is trained
on seasons <= T, its projection of the next ``horizon`` seasons (in wins above replacement, the
owner's league) is set beside KTC's dynasty value the following February, and both are compared
with the WAR those players then delivered. KTC is daily from 2020-04, so cohorts run 2020..last
observable (2022 at three years, 2024 at one).

Per variant (``--backends xgb tabpfn blend``) and horizon:
  * per cohort: rank agreement with realized WAR for the model and for KTC on the same priced
    players, top-decile hit rates, the disagreement test (does our gap to the market predict the
    market's error) and the swap test;
  * by segment (position, age band, experience, market tier): the same, pooled over cohorts on
    within-cohort percentile ranks;
  * the swap test: each February, pair every player the model calls rich with the closest-priced
    player it calls cheap (same cohort, KTC within ``--tol``, both at least ``--min-gap`` ranks of
    disagreement) and count the realized WAR the swap gained -- the trade a manager with the model
    would have made against one without it.

Writes ``players.parquet``, ``pairs.parquet`` and ``summary.json`` to ``--out``. ``--from DIR ...``
re-scores saved runs instead of training (so variants run separately can be merged into one
report), and ``--publish`` writes the summary to the ML bucket
(``backtests/market/run_date=<today>/summary.json``), where the export picks it up for the page's
Model performance tab.

Usage:
  scripts/market_backtest.py --horizons 3 1 --backends xgb --replacement weekly --out <dir>
  scripts/market_backtest.py --horizons 3 --backends tabpfn --device cuda --tabpfn-params model_version=v2 n_estimators=4 --out <dir2>
  scripts/market_backtest.py --from <dir> <dir2> --out <merged> --publish
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np  # noqa: E402
import polars as pl  # noqa: E402

import experiments as ex  # noqa: E402
import gcs_io  # noqa: E402
import power  # noqa: E402
import value  # noqa: E402
from run_experiment import build_context  # noqa: E402

KEEP = ["variant", "horizon", "cohort", "player_id", "player_name", "position", "age_at_season", "exp_at_season",
        "draft_pick", "is_rookie", "games", "ppg", "war", "iv", "ktc_value", "realized_war", "realized_iv"]
BY = ["variant", "horizon", "cohort"]


def age_band(age: pl.Expr) -> pl.Expr:
    return (pl.when(age < 25).then(pl.lit("<25")).when(age < 29).then(pl.lit("25-28"))
            .when(age < 33).then(pl.lit("29-32")).otherwise(pl.lit("33+")))


def exp_band(exp: pl.Expr) -> pl.Expr:
    return pl.when(exp <= 0).then(pl.lit("rookie")).when(exp <= 2).then(pl.lit("yr2-3")).otherwise(pl.lit("vet4+"))


def tier(rank: pl.Expr) -> pl.Expr:
    return (pl.when(rank <= 24).then(pl.lit("1-24")).when(rank <= 60).then(pl.lit("25-60"))
            .when(rank <= 120).then(pl.lit("61-120")).otherwise(pl.lit("121+")))


def priced_frame(frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Priced, observable players with within-cohort ranks (1 = best) and percentile ranks."""
    df = pl.concat([f.select([c for c in KEEP if c in f.columns]) for f in frames], how="diagonal")
    df = df.filter(pl.col("ktc_value").is_not_null() & pl.col("realized_war").is_not_null())
    n = pl.len().over(BY)
    df = df.with_columns(
        pl.col("war").rank(descending=True, method="average").over(BY).alias("model_rank"),
        pl.col("ktc_value").rank(descending=True, method="average").over(BY).alias("ktc_rank"),
        pl.col("realized_war").rank(descending=True, method="average").over(BY).alias("real_rank"),
        n.alias("n_cohort"),
    ).with_columns(
        ((pl.col("model_rank") - 1) / (pl.col("n_cohort") - 1)).alias("model_pct"),
        ((pl.col("ktc_rank") - 1) / (pl.col("n_cohort") - 1)).alias("ktc_pct"),
        ((pl.col("real_rank") - 1) / (pl.col("n_cohort") - 1)).alias("real_pct"),
        (pl.col("ktc_rank") - pl.col("model_rank")).alias("gap_model"),      # + : we like him more than the market
        (pl.col("ktc_rank") - pl.col("real_rank")).alias("gap_real"),        # + : he beat the market's rank
        age_band(pl.col("age_at_season")).alias("age_band"),
        exp_band(pl.col("exp_at_season").fill_null(0)).alias("exp_band"),
        tier(pl.col("ktc_rank")).alias("tier"),
    )
    third = (pl.col("n_cohort") // 3).cast(pl.Int64)
    gap_rank = pl.col("gap_model").rank(descending=True, method="ordinal").over(BY)
    return df.with_columns(
        pl.when(gap_rank <= third).then(pl.lit("cheap")).when(gap_rank > pl.col("n_cohort") - third).then(pl.lit("rich"))
        .otherwise(pl.lit("agree")).alias("call"))


def _sp(a, b) -> float | None:
    a, b = np.asarray(a, float), np.asarray(b, float)
    return value.spearman(a, b) if len(a) >= 8 else None


def score(df: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    """Rank agreement of the model and of KTC with realized WAR, the disagreement test, and the
    realized wins per 1,000 KTC of the players we called cheap vs rich, per group."""
    rows = []
    for key, g in df.group_by(keys, maintain_order=True):
        key = key if isinstance(key, tuple) else (key,)
        cheap, rich = g.filter(pl.col("call") == "cheap"), g.filter(pl.col("call") == "rich")
        per_k = lambda h: float(h["realized_war"].sum() / h["ktc_value"].sum() * 1000) if h.height and h["ktc_value"].sum() > 0 else None
        row = dict(zip(keys, key))
        row.update({
            "n": g.height,
            "rho_model": _sp(-g["model_pct"], -g["real_pct"]), "rho_ktc": _sp(-g["ktc_pct"], -g["real_pct"]),
            "edge_corr": _sp(g["gap_model"], g["gap_real"]),
            "cheap_n": cheap.height, "cheap_gap_real": float(cheap["gap_real"].mean()) if cheap.height else None,
            "rich_n": rich.height, "rich_gap_real": float(rich["gap_real"].mean()) if rich.height else None,
            "cheap_wins_per_1k": per_k(cheap), "rich_wins_per_1k": per_k(rich), "all_wins_per_1k": per_k(g),
            "model_mae": float((g["realized_war"] - g["war"]).abs().mean()), "model_bias": float((g["realized_war"] - g["war"]).mean()),
        })
        if "cohort" in keys or g.select(pl.col("cohort").n_unique()).item() == 1:
            m, k, r = (g[c].to_numpy().astype(float) for c in ("war", "ktc_value", "realized_war"))
            row["top_decile_model"], row["top_decile_ktc"] = ex.top_decile_precision(m, r), ex.top_decile_precision(k, r)
        rows.append(row)
    return pl.DataFrame(rows)


def swap_pairs(df: pl.DataFrame, tol: float, min_gap: float) -> pl.DataFrame:
    """Within each cohort: every 'rich' player paired with the unmatched 'cheap' player closest in
    KTC (within ``tol``), both with at least ``min_gap`` ranks of disagreement. Greedy from the
    strongest disagreement down. ``gain`` = realized WAR of the cheap side minus the rich side."""
    out = []
    for key, g in df.group_by(BY, maintain_order=True):
        rich = g.filter(pl.col("gap_model") <= -min_gap).sort("gap_model")
        cheap = g.filter(pl.col("gap_model") >= min_gap).sort("gap_model", descending=True)
        used = set()
        cv, cn = cheap["ktc_value"].to_numpy().astype(float), cheap["player_name"].to_list()
        for r in rich.iter_rows(named=True):
            best, best_d = None, None
            for i in range(len(cv)):
                if cn[i] in used:
                    continue
                d = abs(cv[i] - r["ktc_value"]) / max(r["ktc_value"], 1)
                if d <= tol and (best is None or d < best_d):
                    best, best_d = i, d
            if best is None:
                continue
            used.add(cn[best])
            c = cheap.row(best, named=True)
            out.append({**dict(zip(BY, key if isinstance(key, tuple) else (key,))),
                        "sell": r["player_name"], "sell_pos": r["position"], "sell_age": r["age_band"], "sell_exp": r["exp_band"], "sell_tier": r["tier"],
                        "sell_ktc": r["ktc_value"], "sell_gap": r["gap_model"], "sell_real": r["realized_war"], "sell_war": r["war"],
                        "buy": c["player_name"], "buy_pos": c["position"], "buy_age": c["age_band"], "buy_exp": c["exp_band"], "buy_tier": c["tier"],
                        "buy_ktc": c["ktc_value"], "buy_gap": c["gap_model"], "buy_real": c["realized_war"], "buy_war": c["war"],
                        "gain": c["realized_war"] - r["realized_war"], "model_gain": c["war"] - r["war"]})
    return pl.DataFrame(out)


def swap_summary(pairs: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    if pairs.height == 0:
        return pl.DataFrame()
    return (pairs.group_by(keys, maintain_order=True)
            .agg(pl.len().alias("pairs"), pl.col("gain").mean().alias("gain_mean"), pl.col("gain").median().alias("gain_median"),
                 (pl.col("gain") > 0).mean().alias("win_rate"), pl.col("gain").sum().alias("gain_total"),
                 pl.col("model_gain").mean().alias("model_said"), pl.col("sell_ktc").mean().alias("ktc_mean"))
            .sort(keys))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--horizons", nargs="+", type=int, default=[3])
    ap.add_argument("--backends", nargs="+", default=["xgb"], choices=["xgb", "tabpfn", "blend"])
    ap.add_argument("--groups", default="base,career")
    ap.add_argument("--stacked", action="store_true", help="pooled horizons: one games and one ppg model over every horizon")
    ap.add_argument("--first-cohort", type=int, default=2020)
    ap.add_argument("--last-cohort", type=int, default=None)
    ap.add_argument("--replacement", choices=["share", "fill", "weekly"], default="weekly")
    ap.add_argument("--target", choices=["level", "residual", "opportunity", "blend"], default="level")
    ap.add_argument("--draft-rows", action="store_true", help="a pre-NFL row per drafted skill player (draft_rows.py)")
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--tabpfn-params", nargs="*", default=[])
    ap.add_argument("--calibrate", default=False, help="career.HorizonModels calibrate option, e.g. games_table:tiers:0.5")
    ap.add_argument("--suffix", default="", help="appended to the variant name (to tell calibrated runs apart)")
    ap.add_argument("--tol", type=float, default=0.15, help="KTC tolerance for a swap pair (fraction of the sold player's value)")
    ap.add_argument("--min-gap", type=float, default=10, help="minimum model-vs-market rank gap on both sides of a swap")
    ap.add_argument("--out", required=True)
    ap.add_argument("--from", dest="from_dirs", nargs="*", default=[], help="re-score these runs' players.parquet instead of training")
    ap.add_argument("--publish", action="store_true", help="write the summary to the ML bucket (backtests/market/run_date=<today>/summary.json)")
    args = ap.parse_args()
    power.keep_awake()                      # hours of GPU work: do not let the machine sleep under it
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    tp = {}
    for kv in args.tabpfn_params:
        k, v = kv.split("=", 1)
        try:
            tp[k] = int(v)
        except ValueError:
            tp[k] = v

    frames: list[pl.DataFrame] = []
    ledger_rows = []
    if args.from_dirs:
        for d in args.from_dirs:
            f = pl.read_parquet(Path(d) / "players.parquet")
            frames.append(f.select([c for c in KEEP if c in f.columns]))
            print(f"{d}: {f.height} priced rows, variants {f['variant'].unique().to_list()}, horizons {f['horizon'].unique().to_list()}")
        args.horizons = sorted({int(h) for f in frames for h in f["horizon"].unique().to_list()})
    for h in (args.horizons if not args.from_dirs else []):
        H = list(range(1, h + 1))
        bc = build_context(H, args.first_cohort, args.last_cohort, args.replacement, draft_rows=args.draft_rows)
        print(f"horizon {h}: cohorts {bc.cohorts[0]}-{bc.cohorts[-1]}")
        for backend in args.backends:
            cfg = ex.ExperimentConfig(name=f"{backend}{args.suffix}_h{h}", groups=args.groups.split(","), horizons=H, cohorts=bc.cohorts, device=args.device,
                                      curve=bc.curve, replacement=args.replacement, target=args.target, backend=backend, tabpfn_params=tp, calibrate=args.calibrate or False,
                                      stacked=args.stacked)
            got: list[pl.DataFrame] = []
            per_cohort, summary = ex.run_experiment(bc.matrix, cfg, bc.ctx, bc.rep_for, bc.market_for, collect=got)
            frames += [g.with_columns(pl.lit(h).alias("horizon"), pl.lit(backend + args.suffix).alias("variant")) for g in got]
            ledger_rows.append(summary)
            with pl.Config(tbl_rows=-1, tbl_width_chars=220, float_precision=3):
                print(f"\n== {backend} h{h} ==")
                print(per_cohort.select([c for c in ["cohort", "n_all", "n_priced", "spearman_war_top", "spearman_war_all", "spearman_iv_vs_realized",
                                                      "spearman_ktc_vs_realized", "top_decile_iv", "top_decile_ktc", "edge_corr", "edge_spread"] if c in per_cohort.columns]))

    df = priced_frame(frames)
    pairs = swap_pairs(df, args.tol, args.min_gap)
    df.write_parquet(out / "players.parquet")
    pairs.write_parquet(out / "pairs.parquet")

    tables = {
        "by_cohort": score(df, ["variant", "horizon", "cohort"]),
        "overall": score(df, ["variant", "horizon"]),
        "by_position": score(df, ["variant", "horizon", "position"]),
        "by_age": score(df, ["variant", "horizon", "age_band"]),
        "by_exp": score(df, ["variant", "horizon", "exp_band"]),
        "by_tier": score(df, ["variant", "horizon", "tier"]),
        "swap_overall": swap_summary(pairs, ["variant", "horizon"]),
        "swap_by_cohort": swap_summary(pairs, ["variant", "horizon", "cohort"]),
        "swap_by_buy_pos": swap_summary(pairs, ["variant", "horizon", "buy_pos"]),
        "swap_by_sell_pos": swap_summary(pairs, ["variant", "horizon", "sell_pos"]),
        "swap_by_buy_age": swap_summary(pairs, ["variant", "horizon", "buy_age"]),
        "swap_by_sell_age": swap_summary(pairs, ["variant", "horizon", "sell_age"]),
        "swap_by_buy_exp": swap_summary(pairs, ["variant", "horizon", "buy_exp"]),
        "swap_by_sell_exp": swap_summary(pairs, ["variant", "horizon", "sell_exp"]),
        "swap_by_sell_tier": swap_summary(pairs, ["variant", "horizon", "sell_tier"]),
    }
    with pl.Config(tbl_rows=-1, tbl_width_chars=220, float_precision=3):
        for name, t in tables.items():
            print(f"\n-- {name}")
            print(t)
        if pairs.height:
            print("\n-- biggest swap wins / losses (h = max horizon)")
            hm = max(args.horizons)
            p = pairs.filter(pl.col("horizon") == hm).sort("gain")
            print(p.select("variant", "cohort", "sell", "sell_ktc", "sell_real", "buy", "buy_ktc", "buy_real", "gain").head(8))
            print(p.select("variant", "cohort", "sell", "sell_ktc", "sell_real", "buy", "buy_ktc", "buy_real", "gain").tail(8))
    hm = max(args.horizons)
    top = pairs.filter(pl.col("horizon") == hm).sort("gain") if pairs.height else pairs
    cols = ["variant", "cohort", "sell", "sell_pos", "sell_ktc", "sell_real", "buy", "buy_pos", "buy_ktc", "buy_real", "gain"]
    summary = {k: t.to_dicts() for k, t in tables.items()} | {
        "ledger": ledger_rows,
        "top_swaps": (top.select(cols).head(10).to_dicts() + top.select(cols).tail(10).to_dicts()) if pairs.height else [],
        "meta": {"horizons": args.horizons, "variants": sorted(df["variant"].unique().to_list()), "cohorts": f"{df['cohort'].min()}-{df['cohort'].max()}",
                 "n_priced": df.height, "tol": args.tol, "min_gap": args.min_gap, "replacement": args.replacement,
                 "run_date": datetime.now(timezone.utc).date().isoformat()},
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    print(f"\nwrote {out}")
    if args.publish:
        print("published", gcs_io.write_ml_json(summary, "backtests", "market", f"run_date={summary['meta']['run_date']}", "summary.json"))


if __name__ == "__main__":
    main()
