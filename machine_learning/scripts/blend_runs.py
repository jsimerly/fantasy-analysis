"""Blend two runs from their saved cohort frames, or a run with the market -- no GPU (owner, 2026-10-25: "Lets try the
blends and see if they outperform. Again performance of the model is #1").

Run A (the stronger model, TabPFN) and run B (the trees) projected the same players in the same cohorts; their errors are
not the same errors, so a weighted mean of their projections can beat either. Every projected column the two share --
the value columns (war, iv, par, their per-season parts) and the per-horizon season points, ppg, games and points above
replacement -- becomes w * A + (1 - w) * B; the band stays A's. The blend is scored by the harness as a run of its own
and paired with A under the co-primary rule.

  blend_runs.py A B --grid                    every weight 0..1 by 0.1 scored: a map (the weight is chosen in-sample), not a verdict
  blend_runs.py A B --walk [--metric mae]     walk-forward: cohort T's weight is the one that did best on the cohorts complete by T
                                              (cohort + horizon <= T); A alone where none is complete yet -- the honest candidate
  blend_runs.py A B --weight 0.7              one fixed weight on A (the same caveat as --grid)
  blend_runs.py A --market                    the model and the market (KTC) as one ordering of the priced players: the blended
                                              rank at every weight, scored on the realized value and wins -- a map on the priced
                                              cohorts (2020-22); no cohort has a complete priced predecessor yet, so no walk-forward
  --as NAME (default: blend_<A>_<B>_<mode>)  --no-ledger (a GPU chain may be appending)  --no-save (frames + ledger untouched)
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Callable

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import experiments as ex  # noqa: E402
import feature_groups as fg  # noqa: E402
import value  # noqa: E402

BLEND_RE = re.compile(r"^(iv|war|par)$|^(iv|war|par)_\d+$|^h\d+_(fpts|ppg|games|vorp)_hat$")
GRID = [round(0.1 * i, 1) for i in range(11)]
OUTCOME_TOL = 0.01          # wins: realized WAR moves by a hair between season-fact writes (the league's win curve); more is a regime change


def blend_columns(a: pl.DataFrame, b: pl.DataFrame) -> list[str]:
    """The projected columns both frames hold: these are blended, everything else stays A's."""
    return [c for c in a.columns if c in b.columns and BLEND_RE.match(c) and a[c].dtype.is_numeric()]


def blend_frames(a: pl.DataFrame, b: pl.DataFrame, w_for: Callable[[int], float]) -> list[pl.DataFrame]:
    """Per cohort, A's frame with every shared projected column replaced by w * A + (1 - w) * B (``w_for(T)`` the
    weight on A), joined on player; a player B lacks keeps A's value (and vice versa); ``blend_w`` records the weight."""
    cols = blend_columns(a, b)
    out = []
    for T in sorted(int(t) for t in a["cohort"].unique().to_list()):
        w = float(w_for(T))
        fa = a.filter(pl.col("cohort") == T)
        fb = b.filter(pl.col("cohort") == T).select(["player_id"] + cols).rename({c: f"{c}__b" for c in cols})
        f = fa.join(fb, on="player_id", how="left")
        f = f.with_columns([pl.coalesce([w * pl.col(c) + (1.0 - w) * pl.col(f"{c}__b"), pl.col(c), pl.col(f"{c}__b")]).alias(c) for c in cols])
        out.append(f.drop([f"{c}__b" for c in cols]).with_columns(pl.lit(w).alias("blend_w")))
    return out


def top_metrics(frame: pl.DataFrame, top_n: int = 150) -> tuple[float, float]:
    """(ordering, wins error) among the top ``top_n`` by projected WAR on the observable players -- score_cohort's co-primary pair."""
    obs = frame.filter(pl.col("realized_iv").is_not_null()).sort("war", descending=True).head(top_n)
    p, r = obs["war"].to_numpy().astype(float), obs["realized_war"].to_numpy().astype(float)
    return value.spearman(p, r), float(np.mean(np.abs(r - p)))


def walk_weight(a: pl.DataFrame, b: pl.DataFrame, T: int, horizon: int, metric: str = "mae", grid: list[float] = GRID, top_n: int = 150) -> float:
    """The weight on A for cohort T, chosen on the cohorts whose outcomes are complete by T (cohort + horizon <= T):
    the one with the best mean ``metric`` ('mae' = lowest wins error, 'spearman' = highest ordering) over them, ties to
    the weight nearest 1 (A alone). 1.0 when no earlier cohort is complete: no evidence, no blend."""
    done = sorted(int(t) for t in a["cohort"].unique().to_list() if int(t) + horizon <= T)
    if not done:
        return 1.0
    aa, bb = a.filter(pl.col("cohort").is_in(done)), b.filter(pl.col("cohort").is_in(done))
    best, best_w = None, 1.0
    for w in sorted(grid, reverse=True):                     # from A alone downwards: a tie keeps the larger weight on A
        frames = blend_frames(aa, bb, lambda _t, w=w: w)
        vals = [top_metrics(f, top_n)[0 if metric == "spearman" else 1] for f in frames]
        score = float(np.mean(vals)) * (1 if metric == "spearman" else -1)
        if best is None or score > best + 1e-12:
            best, best_w = score, w
    return best_w


def market_map(a: pl.DataFrame, grid: list[float] = GRID, min_priced: int = 20) -> pl.DataFrame:
    """The model and the market as one ordering: within each cohort, among the priced players, the blended rank
    w * rank(iv) + (1 - w) * rank(ktc), scored by how it orders the realized value and the realized wins (Spearman),
    averaged over the priced cohorts. In-sample on the weight: a map of whether the market adds to the model, not a
    verdict (a swap at equal price is unchanged by any weight: the blend only reorders players at different prices)."""
    pr = a.filter(pl.col("ktc_value").is_not_null() & pl.col("realized_iv").is_not_null())
    rows = []
    for w in grid:
        vals = []
        for T in sorted(int(t) for t in pr["cohort"].unique().to_list()):
            f = pr.filter(pl.col("cohort") == T)
            if f.height < min_priced:
                continue
            s = w * f["iv"].rank().to_numpy().astype(float) + (1.0 - w) * f["ktc_value"].rank().to_numpy().astype(float)
            vals.append((value.spearman(s, f["realized_iv"].to_numpy().astype(float)), value.spearman(s, f["realized_war"].to_numpy().astype(float)), f.height))
        if vals:
            rows.append({"w_on_model": w, "spearman_value": float(np.mean([v[0] for v in vals])), "spearman_wins": float(np.mean([v[1] for v in vals])),
                         "cohorts": len(vals), "players": int(sum(v[2] for v in vals))})
    return pl.DataFrame(rows)


def _cfg(name: str, src_name: str, frames: pl.DataFrame) -> "ex.ExperimentConfig":
    ledger = ex.load_ledger()
    meta = ledger.filter(pl.col("name") == src_name).sort("timestamp").tail(1).to_dicts() if ledger is not None else []
    meta = meta[0] if meta else {}
    groups = (meta.get("groups") or ",".join(fg.DEFAULT)).split(",")
    H = sorted(int(c[1:-9]) for c in frames.columns if c.startswith("h") and c.endswith("_fpts_hat") and c[1:-9].isdigit())
    backend = str(meta.get("backend") or "tabpfn")
    return ex.ExperimentConfig(name=name, groups=groups, horizons=H, cohorts=sorted(int(t) for t in frames["cohort"].unique().to_list()),
                               discount_rate=float(meta.get("discount_rate") or value.DEFAULT_DISCOUNT_RATE),
                               replacement=str(meta.get("replacement") or "weekly").split("/")[0], target=str(meta.get("target") or "blend"),
                               backend=backend.split("(")[0].split(" ")[0], stacked=" stacked" in backend)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("a", help="run A (its frames carry everything the blend does not change: the band, the market, the outcomes)")
    ap.add_argument("b", nargs="?", help="run B, the partner (not with --market)")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--grid", action="store_true")
    mode.add_argument("--walk", action="store_true")
    mode.add_argument("--weight", type=float)
    mode.add_argument("--market", action="store_true")
    ap.add_argument("--metric", choices=["mae", "spearman"], default="mae", help="--walk: what the weight is chosen on")
    ap.add_argument("--as", dest="as_name")
    ap.add_argument("--no-ledger", action="store_true")
    ap.add_argument("--no-save", action="store_true")
    args = ap.parse_args()
    a = ex.load_cohorts(args.a)
    if args.market:
        with pl.Config(tbl_rows=-1, float_precision=4):
            print(f"== {args.a} + the market (KTC), the priced players, rank blend ==")
            print(market_map(a))
        print("(w = 1 is the model alone, 0 the market alone; in-sample on the weight, three priced cohorts: a map, not a verdict)")
        return
    if not args.b:
        ap.error("run B is needed (or --market)")
    b = ex.load_cohorts(args.b)
    cols = blend_columns(a, b)
    H = sorted(int(c[1:-9]) for c in a.columns if c.startswith("h") and c.endswith("_fpts_hat") and c[1:-9].isdigit())
    chk = a.select("cohort", "player_id", "realized_war", "realized_iv").join(
        b.select("cohort", "player_id", pl.col("realized_war").alias("_rw"), pl.col("realized_iv").alias("_ri")), on=["cohort", "player_id"], how="inner")
    dw = float(chk.select((pl.col("realized_war") - pl.col("_rw")).abs().max()).item() or 0.0)
    di = float(chk.select((pl.col("realized_iv") - pl.col("_ri")).abs().max()).item() or 0.0)
    print(f"A {args.a}: {a.height} rows; B {args.b}: {b.height} rows; {chk.height} shared player-cohorts; realized value differs by up to {di:.3f} points,"
          f" realized wins by up to {dw:.3f}; horizons {H}; blending {len(cols)} columns")
    if di > 1e-6 or dw > OUTCOME_TOL:
        print("WARNING: the two runs do not agree on the realized outcomes beyond the win curve's drift -- different regimes? the pairing is not like for like")
    if args.grid:
        rows = []
        for w in GRID:
            ms = [top_metrics(f) for f in blend_frames(a, b, lambda _t, w=w: w)]
            rows.append({"w_on_A": w, "spearman_war_top": float(np.mean([m[0] for m in ms])), "mae_war_top": float(np.mean([m[1] for m in ms]))})
        with pl.Config(tbl_rows=-1, float_precision=4):
            print(pl.DataFrame(rows))
        print("(the weight is chosen on the same cohorts it is scored on: a map of where a blend could help, not a verdict -- --walk gives the verdict)")
        return
    if args.walk:
        weights = {T: walk_weight(a, b, T, max(H), args.metric) for T in sorted(int(t) for t in a["cohort"].unique().to_list())}
        w_for, tag = (lambda T: weights[T]), f"walk_{args.metric}"
        print("walk-forward weights on A:", weights)
    else:
        w_for, tag = (lambda _T: args.weight), f"w{int(round(args.weight * 100)):02d}"
    name = args.as_name or f"blend_{args.a}_{args.b}_{tag}"
    frames = blend_frames(a, b, w_for)
    cfg = _cfg(name, args.a.partition("@")[0], a)
    try:
        src = ex.load_run(args.a)
    except FileNotFoundError:
        src = None
    got: list = []
    per_cohort, summary = ex.rescore(pl.concat(frames, how="diagonal"), cfg, src_per_cohort=src, collect=got)
    per_cohort = per_cohort.with_columns(pl.Series("blend_w", [float(w_for(T)) for T in per_cohort["cohort"].to_list()]))
    summary["regime"] = str(src["regime"][0]) if src is not None and "regime" in src.columns else ""
    summary["backend"] = f"blend<{args.a}+{args.b}:{tag}>"
    per_cohort = per_cohort.with_columns(pl.lit(summary["regime"]).alias("regime"))
    with pl.Config(tbl_rows=-1, tbl_width_chars=200, float_precision=3):
        print(f"== {name} ==")
        print(per_cohort.select([c for c in ("cohort", "n_all", "blend_w", "spearman_war_top", "mae_war_top", "bias_war_top") if c in per_cohort.columns]))
    if args.no_save:
        print("not saved (--no-save)"); return
    print("per-cohort ->", ex.save_run(per_cohort, summary))
    print("cohort frames ->", ex.save_cohorts(name, got, summary["timestamp"]))
    if args.no_ledger:
        print("ledger untouched (--no-ledger)")
    else:
        ex.save_ledger(ex.append_result(ex.load_ledger(), summary))
    with pl.Config(tbl_rows=-1, tbl_width_chars=200, float_precision=3):
        table = ex.paired(name, args.a)
        print(table)
        print(ex.verdict(table))


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    main()
