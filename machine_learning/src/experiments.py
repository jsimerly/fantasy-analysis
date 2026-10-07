"""Walk-forward experiments over feature-group variants of the career model, with a results ledger.

One experiment = one feature-group combination (see ``feature_groups``) trained and scored exactly
the way the production value is: for every cohort season T, fit the horizon models on outcomes
known by the end of T, project the season-T rows, convert to intrinsic value, and compare with
what those players actually did over the next H seasons (and with KTC the following February).

Metrics per cohort (``run_experiment`` returns one row per cohort + a summary):
  * ``spearman_iv_vs_realized``      rank agreement of IV with realized H-season PAR, KTC-priced players
  * ``spearman_ktc_vs_realized``     the market's, on the same players (the bar to clear)
  * ``spearman_iv_vs_realized_all``  every projected player, not just the priced ones
  * ``top_decile_iv`` / ``top_decile_ktc``  share of the top 10 % by IV (KTC) that finished top 10 %
  * ``mae_h{k}``                     points error of the season T+k projection, all observable rows

Every run can be appended to the ledger (``experiments/ledger.parquet`` in the ML bucket) with its
config and git commit, so variants are compared on the same cohorts over time.
"""
from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

import numpy as np
import polars as pl

import career
import feature_groups as fg
import gcs_io
import value

LEDGER = ("experiments", "ledger.parquet")


@dataclass
class ExperimentConfig:
    name: str
    groups: list[str]
    horizons: list[int]
    cohorts: list[int]
    discount_rate: float = value.DEFAULT_DISCOUNT_RATE
    device: str = "cpu"
    params: dict = field(default_factory=dict)
    calibrate: object = False        # walk-forward recalibration: True/"both", "ppg", "games" or False (career.HorizonModels)
    quantile_sigma: bool = False     # player-specific spread from quantile models instead of one sigma per position
    curve: object = None             # league.WinCurve for WAR units; None = the owner's league's known curve
    top_n: int = 150                 # "relevant players" cut for the market-free metrics: top N by projected WAR
    position_scale: bool = False     # scale each position's value by (realized / projected) PAR share on the holdout seasons
    replacement: str = "share"       # how rep_for was built: share | fill | weekly (recorded in the ledger; the harness picks rep_for)
    realized_replacement: str | None = None   # score realized WAR on a different replacement (cross-check that a rep change helps the ORDERING, not just the metric)
    fixed_scale: dict = field(default_factory=dict)   # position -> factor on value (iv / war / par), a fixed cross-position calibration to test
    target: str = "level"            # career.HorizonModels target: level | residual | opportunity
    weight: str | None = None        # career.HorizonModels relevance weights: None | ppg | ppg2
    backend: str = "xgb"             # career.HorizonModels estimator: xgb | tabpfn | blend
    stacked: bool = False            # one games / ppg model over all horizons with years-ahead as a feature
    cap: str = career.DEFAULT_CAP    # age-survival cap on projected games: career.apply_cap specs ("30+t" production, "30+", "30+t34", all, none)
    range_quantiles: tuple | None = None   # keep these quantiles of the predictive distribution (TabPFN): scored as coverage / pinball
    tabpfn_params: dict = field(default_factory=dict)   # TabPFNRegressor constructor overrides (n_estimators, ...)


def top_decile_precision(score: np.ndarray, realized: np.ndarray, frac: float = 0.1) -> float:
    """Share of the top ``frac`` by ``score`` that are also top ``frac`` by ``realized``."""
    score, realized = np.asarray(score, float), np.asarray(realized, float)
    k = max(1, int(round(frac * len(score))))
    top_s = set(np.argsort(-score)[:k].tolist())
    top_r = set(np.argsort(-realized)[:k].tolist())
    return len(top_s & top_r) / k


def market_edge(model: np.ndarray, market: np.ndarray, realized: np.ndarray) -> dict:
    """Beating the market, not agreeing with it. Ranks among the priced players:
    ``gap_model`` = market rank − model rank (positive: we like him more than the market),
    ``gap_real``  = market rank − realized rank (positive: he finished better than the market had him).
    ``edge_corr``  Spearman(gap_model, gap_real): does our disagreement predict the market's error?
    ``edge_spread`` realized gap of the third we like most minus the third we like least, in ranks."""
    def rank(v):
        return pl.Series(-np.asarray(v, float)).rank(method="average").to_numpy()
    gm = rank(market) - rank(model)
    gr = rank(market) - rank(realized)
    n = len(gm)
    if n < 9:
        return {}
    order = np.argsort(-gm)
    third = max(1, n // 3)
    cheap, rich = gr[order[:third]].mean(), gr[order[-third:]].mean()
    return {"edge_corr": value.spearman(gm, gr), "edge_cheap": float(cheap), "edge_rich": float(rich), "edge_spread": float(cheap - rich)}


def position_scales(df: pl.DataFrame, models, rep: dict, H: list[int], T: int, cfg: "ExperimentConfig",
                    holdout: int = 3, lo: float = 0.6, hi: float = 1.6) -> dict[str, float]:
    """Walk-forward position calibration of VALUE: on the holdout seasons (outcomes in (T-holdout, T]),
    temporary models' projected PAR share by position vs the realized PAR share; the ratio scales
    that position's value for cohort T. Targets the gap the points projections do not have but the
    value construction does (shrunk projections + a replacement line in the dense part of a
    position's distribution under-count that position's PAR)."""
    cut = T - holdout
    tmp = career.HorizonModels(H, device=cfg.device, features=fg.feature_columns(fg.resolve(cfg.groups)), calibrate=cfg.calibrate,
                               backend=cfg.backend, tabpfn_params=cfg.tabpfn_params,
                               quantile_sigma=cfg.quantile_sigma, target=cfg.target, weight=cfg.weight, **cfg.params).fit(df, as_of_season=cut)
    tmp.estimate_sigma(df, as_of_season=cut)
    out = {}
    for k in H:
        rows = df.filter(pl.col(f"h{k}_observable") & ((pl.col("season") + k) > cut) & ((pl.col("season") + k) <= T))
        if rows.height < 50:
            continue
        pred = value.intrinsic_value(tmp.predict(rows), rep, [k], cfg.discount_rate)
        pred = value.realized_value(pred, rep, [k], cfg.discount_rate)
        g = pred.group_by("position").agg(pl.col(f"h{k}_vorp_hat").sum().alias("p"), pl.col(f"h{k}_vorp").sum().alias("r"))
        tp, tr = g["p"].sum(), g["r"].sum()
        for pos, p_, r_ in g.iter_rows():
            if p_ > 0 and tp > 0 and tr > 0:
                out.setdefault(pos, []).append((r_ / tr) / (p_ / tp))
    return {pos: float(min(max(np.mean(v), lo), hi)) for pos, v in out.items()}


def git_commit() -> str | None:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, check=True, text=True).stdout.strip()
    except Exception:  # noqa: BLE001
        return None


def run_experiment(
    matrix: pl.DataFrame, cfg: ExperimentConfig, ctx: fg.Context,
    rep_for: Callable[[int], dict[str, float]],
    market_for: Callable[[pl.DataFrame, int], pl.DataFrame],
    realized_rep_for: Callable[[int], dict[str, float]] | None = None,
    collect: list | None = None,
) -> tuple[pl.DataFrame, dict]:
    """``matrix`` is the career matrix (``career.build_career_matrix``) for the configured horizons;
    ``rep_for(T)`` gives replacement ppg as of T; ``market_for(cohort, T)`` attaches ``ktc_value``.
    ``realized_rep_for`` scores realized value / WAR on another replacement level (default: the same).
    ``collect`` (a list) receives every scored cohort frame (projection, market, realized per player)
    for analyses beyond the ledger's metrics, e.g. the market backtest."""
    groups = fg.resolve(cfg.groups)
    cols = fg.feature_columns(groups)
    df = fg.assemble(matrix, groups, ctx)
    H = list(cfg.horizons)
    rows = []
    for T in cfg.cohorts:
        t_cohort = time.perf_counter()
        rep = rep_for(T)
        rep_real = realized_rep_for(T) if realized_rep_for is not None else rep
        models = career.HorizonModels(H, device=cfg.device, features=cols, calibrate=cfg.calibrate, quantile_sigma=cfg.quantile_sigma,
                                      target=cfg.target, weight=cfg.weight, backend=cfg.backend, tabpfn_params=cfg.tabpfn_params,
                                      stacked=cfg.stacked, range_quantiles=cfg.range_quantiles, **cfg.params).fit(df, as_of_season=T)
        models.estimate_sigma(df, as_of_season=T)
        t_fit = time.perf_counter() - t_cohort
        survival = career.fit_survival(df.filter((pl.col("season") + 1) <= T), cfg.cap)
        pred = models.predict(df.filter(pl.col("season") == T))
        cohort = career.apply_cap(survival, pred, H, cfg.cap)
        cohort = value.realized_value(value.intrinsic_value(cohort, rep, H, cfg.discount_rate), rep_real, H, cfg.discount_rate)
        # the same thing in wins: projected WAR and realized WAR on the league's curve
        import war as _war
        from league import WinCurve, OWNER_CURVE_FALLBACK
        curve = cfg.curve or WinCurve.normal(*OWNER_CURVE_FALLBACK)
        cohort = _war.wins_above_replacement(cohort, rep, curve, _war.career_components(H), cfg.discount_rate, sigma=getattr(models, "sigma", None))
        cohort = _war.realized_wins(cohort, rep_real, curve, H, cfg.discount_rate)
        if cfg.position_scale:
            scales = position_scales(df, models, rep, H, T, cfg)
            sc = pl.col("position").replace_strict(scales, default=1.0, return_dtype=pl.Float64)
            cohort = cohort.with_columns((pl.col("iv") * sc).alias("iv"), (pl.col("war") * sc).alias("war"), (pl.col("par") * sc).alias("par"))
        if cfg.fixed_scale:
            sc = pl.col("position").replace_strict({k: float(v) for k, v in cfg.fixed_scale.items()}, default=1.0, return_dtype=pl.Float64)
            cohort = cohort.with_columns((pl.col("iv") * sc).alias("iv"), (pl.col("war") * sc).alias("war"), (pl.col("par") * sc).alias("par"))
        cohort = market_for(cohort, T)
        print(f"  {cfg.name}: cohort {T} done in {time.perf_counter() - t_cohort:.0f}s "
              f"(fit + spread {t_fit:.0f}s, predict + score {time.perf_counter() - t_cohort - t_fit:.0f}s; {len(H)} horizons, {cfg.backend})", flush=True)
        if collect is not None:
            collect.append(cohort.with_columns(pl.lit(T).alias("cohort"), pl.lit(cfg.name).alias("variant")))
        obs = cohort.filter(pl.col("realized_iv").is_not_null())
        priced = obs.filter(pl.col("ktc_value").is_not_null())
        row = {"name": cfg.name, "cohort": T, "n_all": obs.height, "n_priced": priced.height}
        # position shares of projected vs realized WAR on the observable cohort (cross-position calibration)
        if "realized_war" in obs.columns and obs["realized_war"].sum() > 0 and obs["war"].sum() > 0:
            g = obs.group_by("position").agg(pl.col("war").sum().alias("p"), pl.col("realized_war").sum().alias("r"))
            tp, tr = g["p"].sum(), g["r"].sum()
            err = 0.0
            for pos, p_, r_ in g.iter_rows():
                row[f"share_proj_{pos}"], row[f"share_real_{pos}"] = p_ / tp, r_ / tr
                err += abs(p_ / tp - r_ / tr)
            row["share_abs_err"] = err
        if obs.height:
            row["spearman_iv_vs_realized_all"] = value.spearman(obs["iv"].to_numpy(), obs["realized_iv"].to_numpy())
            # ---- primary, market-free: projected WAR vs realized WAR
            pw, rw = obs["war"].to_numpy().astype(float), obs["realized_war"].to_numpy().astype(float)
            row["spearman_war_all"] = value.spearman(pw, rw)
            row["mae_war_all"] = float(np.mean(np.abs(rw - pw)))
            row["bias_war_all"] = float(np.mean(rw - pw))
            row["top_decile_war_all"] = top_decile_precision(pw, rw)
            top = obs.sort("war", descending=True).head(cfg.top_n)            # the players a manager would actually weigh
            if top.height >= 20:
                tw, tr = top["war"].to_numpy().astype(float), top["realized_war"].to_numpy().astype(float)
                row["spearman_war_top"] = value.spearman(tw, tr)
                row["mae_war_top"] = float(np.mean(np.abs(tr - tw)))
                row["bias_war_top"] = float(np.mean(tr - tw))
            prior = obs.filter(pl.col("games") >= 8).with_columns(pl.col("ppg").rank(descending=True).over("position").alias("_pr"))
            t12 = prior.filter(pl.col("_pr") <= 12)
            if t12.height:
                row["bias_war_top12"] = float((t12["realized_war"].to_numpy() - t12["war"].to_numpy()).mean())
        if priced.height:
            iv, kt, rz = (priced[c].to_numpy().astype(float) for c in ("iv", "ktc_value", "realized_iv"))
            row.update({"spearman_iv_vs_realized": value.spearman(iv, rz), "spearman_ktc_vs_realized": value.spearman(kt, rz),
                        "top_decile_iv": top_decile_precision(iv, rz), "top_decile_ktc": top_decile_precision(kt, rz)})
            row.update(market_edge(iv, kt, rz))
        for k in H:
            o = cohort.filter(pl.col(f"h{k}_observable"))
            if o.height:
                row[f"mae_h{k}"] = float(np.mean(np.abs(o[f"h{k}_fpts"].to_numpy().astype(float) - o[f"h{k}_fpts_hat"].to_numpy().astype(float))))
        row.update(range_scores(cohort, H))
        # magnitude bias (realized - projected season points, + = model too low): everyone, and prior top-12 by position
        prior = cohort.filter(pl.col("games") >= 8).with_columns(pl.col("ppg").rank(descending=True).over("position").alias("_pr"))
        for k in sorted({1, max(H)}):
            o = prior.filter(pl.col(f"h{k}_observable"))
            if o.height:
                res = o[f"h{k}_fpts"].to_numpy().astype(float) - o[f"h{k}_fpts_hat"].to_numpy().astype(float)
                top = o["_pr"].to_numpy() <= 12
                row[f"bias_all_h{k}"] = float(res.mean())
                row[f"bias_top12_h{k}"] = float(res[top].mean()) if top.any() else None
        rows.append(row)
    per_cohort = pl.DataFrame(rows)
    metric_cols = [c for c in per_cohort.columns if c.startswith(("spearman", "top_decile", "mae_", "bias_", "edge_", "share_"))]
    # warn when market context is missing entirely (the primary metrics never need it)
    summary = {"name": cfg.name, "groups": ",".join(g.name for g in groups), "n_features": len(cols),
               "horizon": max(H), "cohorts": f"{min(cfg.cohorts)}-{max(cfg.cohorts)}", "n_cohorts": len(cfg.cohorts),
               "discount_rate": cfg.discount_rate, "params": repr(cfg.params) if cfg.params else "", "calibrate": cfg.calibrate,
               "quantile_sigma": cfg.quantile_sigma, "position_scale": cfg.position_scale,
               "replacement": cfg.replacement if not cfg.realized_replacement else f"{cfg.replacement}/{cfg.realized_replacement}",
               "fixed_scale": ",".join(f"{k}={v:g}" for k, v in cfg.fixed_scale.items()) if cfg.fixed_scale else "",
               "target": cfg.target, "weight": cfg.weight or "",
               "backend": cfg.backend + ("(" + ",".join(f"{k}={v}" for k, v in cfg.tabpfn_params.items()) + ")" if cfg.tabpfn_params else "") + (" stacked" if cfg.stacked else "") + (f" cap={cfg.cap}" if cfg.cap != "30+" else "") + (" range" if cfg.range_quantiles else ""),
               "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"), "commit": git_commit()}
    for c in metric_cols:
        summary[c] = float(per_cohort[c].mean())
    if "spearman_iv_vs_realized" in summary and "spearman_ktc_vs_realized" in summary:
        summary["iv_minus_ktc"] = summary["spearman_iv_vs_realized"] - summary["spearman_ktc_vs_realized"]
    return per_cohort, summary


# ------------------------------------------------------------------------------ ledger
LEDGER_COLS = ["timestamp", "name", "groups", "n_features", "horizon", "cohorts", "n_cohorts", "discount_rate", "params", "calibrate", "quantile_sigma", "position_scale", "replacement", "fixed_scale", "target", "weight", "backend", "commit",
               # primary, market-free: projected WAR vs realized WAR (all projected players / top-N by projected WAR)
               "spearman_war_all", "spearman_war_top", "mae_war_all", "mae_war_top", "bias_war_all", "bias_war_top", "bias_war_top12", "top_decile_war_all", "share_abs_err",
               # context: the market on the same (priced) players
               "spearman_iv_vs_realized", "spearman_ktc_vs_realized", "iv_minus_ktc", "spearman_iv_vs_realized_all",
               "top_decile_iv", "top_decile_ktc", "edge_corr", "edge_cheap", "edge_rich", "edge_spread",
               # the range of outcomes (TabPFN quantiles): share of realized ppg inside the 20-80 band, pinball loss at 20/50/80
               "ppg_cover_2080", "ppg_pinball"]


def range_scores(cohort: pl.DataFrame, horizons: list[int]) -> dict:
    """Score the projected band where the run kept one: for every horizon with ``h{k}_ppg_q20`` and
    ``h{k}_ppg_q80``, on the players who played that season, the share of realized ppg inside the
    band (0.6 is calibrated) and the mean pinball loss over the quantiles present (the proper
    scoring rule for quantiles: hedging and false confidence both cost). Averaged over horizons."""
    cover, pin = [], []
    for k in horizons:
        lo, hi = f"h{k}_ppg_q20", f"h{k}_ppg_q80"
        if lo not in cohort.columns or hi not in cohort.columns:
            continue
        o = cohort.filter(pl.col(f"h{k}_observable") & pl.col(f"h{k}_played") & pl.col(lo).is_not_null())
        if o.height < 20:
            continue
        y = o[f"h{k}_ppg"].to_numpy().astype(float)
        cover.append(float(np.mean((y >= o[lo].to_numpy()) & (y <= o[hi].to_numpy()))))
        losses = []
        for q in (0.2, 0.5, 0.8):
            c = f"h{k}_ppg_{career.q_name(q)}"
            if c in o.columns:
                d = y - o[c].to_numpy().astype(float)
                losses.append(float(np.mean(np.maximum(q * d, (q - 1) * d))))
        if losses:
            pin.append(float(np.mean(losses)))
    out = {}
    if cover:
        out["ppg_cover_2080"] = float(np.mean(cover))
    if pin:
        out["ppg_pinball"] = float(np.mean(pin))
    return out


def append_result(ledger: pl.DataFrame | None, summary: dict) -> pl.DataFrame:
    row = pl.DataFrame([summary]).select([pl.col(c) if c in summary else pl.lit(None).alias(c)
                                           for c in LEDGER_COLS + sorted(k for k in summary if k.startswith(("mae_h", "bias_all_h", "bias_top12_h", "share_proj_", "share_real_")))])
    return row if ledger is None or ledger.height == 0 else pl.concat([ledger, row], how="diagonal_relaxed")


PRIMARY = "spearman_war_top"
RUNS_PREFIX = ("experiments", "runs")
PAIRED_METRICS = ["spearman_war_top", "spearman_war_all", "top_decile_war_all", "mae_war_top", "bias_war_top12", "share_abs_err"]


def save_run(per_cohort: pl.DataFrame, summary: dict) -> str:
    """Persist a run's per-cohort rows (the unit of a paired comparison) next to the ledger."""
    stamp = str(summary.get("timestamp", "")).replace(":", "-")
    return gcs_io.write_ml_parquet(per_cohort.with_columns(pl.lit(stamp).alias("run_ts")), *RUNS_PREFIX, f"{summary['name']}_{stamp}.parquet")


def load_run(ref: str) -> pl.DataFrame:
    """``name`` (latest run of that name) or ``name@timestamp``."""
    name, _, stamp = ref.partition("@")
    paths = sorted(x for x in gcs_io.list_ml(*RUNS_PREFIX) if x.rsplit("/", 1)[-1].startswith(name + "_"))
    if stamp:
        paths = [x for x in paths if stamp.replace(":", "-") in x]
    if not paths:
        raise FileNotFoundError(f"no per-cohort results for {ref!r}")
    tail = paths[-1].split("/")[-3:]
    return gcs_io.read_ml_parquet(*tail)


def paired(ref_a: str, ref_b: str, metrics: list[str] = PAIRED_METRICS) -> pl.DataFrame:
    """Paired comparison of two runs cohort by cohort: mean difference (B - A), its standard error
    over cohorts, the t statistic and how many cohorts B wins. Eight cohorts is few: |t| above ~2.4
    is the 5 % two-sided line, below ~1 is noise."""
    a, b = load_run(ref_a), load_run(ref_b)
    j = a.join(b, on="cohort", suffix="_b")
    rows = []
    for m in metrics:
        if m not in a.columns or m not in b.columns:
            continue
        d = (j[f"{m}_b"] - j[m]).drop_nulls()
        if d.len() < 2:
            continue
        mean, se = float(d.mean()), float(d.std() / np.sqrt(d.len()))
        rows.append({"metric": m, "a": float(j[m].mean()), "b": float(j[f"{m}_b"].mean()), "diff_b_minus_a": mean, "se": se,
                     "t": mean / se if se > 0 else float("inf"), "b_wins": int((d > 0).sum()), "cohorts": int(d.len())})
    return pl.DataFrame(rows)


T_GAIN, T_NO_WORSE = 2.4, -1.0


def verdict(table: pl.DataFrame) -> str:
    """The acceptance rule on a ``paired(A, B)`` table, A the candidate and B the baseline (owner,
    2026-10-06: ordering and error are co-primaries, and both are always shown). ADOPT when A
    improves either ``spearman_war_top`` (ordering of realized WAR among the top 150 projected) or
    ``mae_war_top`` (wins error on the same players) past |t| = 2.4 while the other is no worse
    (t > -1, within noise). TRADE-OFF when one gains past the line and the other dips past the
    noise band: the owner decides on the magnitudes (a slight ordering dip against a large error
    gain is a win). NO otherwise. Both are losses against realized outcomes; the market is never in them."""
    rows = {r["metric"]: r for r in table.to_dicts()}
    if "spearman_war_top" not in rows or "mae_war_top" not in rows:
        return "co-primary verdict: n/a (spearman_war_top and mae_war_top needed)"
    o, e = rows["spearman_war_top"], rows["mae_war_top"]
    # the table's t is for B - A: a positive t on spearman means B orders better, on mae that A errs less
    t_order, t_err = -o["t"], e["t"]
    gain_o, gain_e = t_order >= T_GAIN, t_err >= T_GAIN
    worse_o, worse_e = t_order < T_NO_WORSE, t_err < T_NO_WORSE
    if (gain_o and not worse_e) or (gain_e and not worse_o):
        state = "ADOPT"
    elif (gain_o and worse_e) or (gain_e and worse_o):
        state = "TRADE-OFF (owner's call)"
    else:
        state = "NO"
    stats = (f"ordering (spearman, realized WAR, top 150) A {o['a']:.3f} vs B {o['b']:.3f}, t = {t_order:+.2f}"
             f" | wins error (MAE, top 150) A {e['a']:.3f} vs B {e['b']:.3f}, t = {t_err:+.2f}")
    why = ("gain on " + " and ".join(n for n, g in (("ordering", gain_o), ("wins error", gain_e)) if g)) if (gain_o or gain_e) else "no gain past the line on either"
    dips = ", ".join(n for n, w in (("ordering", worse_o), ("wins error", worse_e)) if w)
    if dips:
        why += f"; {dips} worse"
    return (f"co-primary verdict (A = candidate vs B = baseline): {state} | {stats} | {why}"
            f" | rule: a gain past t = {T_GAIN} on either with the other above t = {T_NO_WORSE}")


def leaderboard(ledger: pl.DataFrame, horizon: int | None = None, cohorts: str | None = None) -> pl.DataFrame:
    """Variants ranked by the market-free primary metric (rank agreement of projected WAR with
    realized WAR among the top-N projected players); filter to one horizon / cohort span so the
    comparison is like for like. Market columns are context only."""
    lb = ledger
    if horizon is not None:
        lb = lb.filter(pl.col("horizon") == horizon)
    if cohorts is not None:
        lb = lb.filter(pl.col("cohorts") == cohorts)
    show = [c for c in ["name", "groups", "n_features", "calibrate", "quantile_sigma", "position_scale", "replacement", "fixed_scale", "target", "weight", "params", "horizon", "cohorts",
                        "spearman_war_top", "spearman_war_all", "mae_war_top", "bias_war_top", "bias_war_top12", "top_decile_war_all", "share_abs_err",
                        "spearman_iv_vs_realized", "spearman_ktc_vs_realized", "edge_corr", "edge_spread", "timestamp", "commit"] if c in lb.columns]
    key = next((c for c in (PRIMARY, "spearman_iv_vs_realized_all", "spearman_iv_vs_realized")
                if c in lb.columns and lb[c].null_count() < lb.height), "name")
    return lb.select(show).sort(key, descending=True, nulls_last=True)


def load_ledger() -> pl.DataFrame | None:
    import gcs_io
    try:
        return gcs_io.read_ml_parquet(*LEDGER)
    except Exception:  # noqa: BLE001 - first run
        return None


def save_ledger(ledger: pl.DataFrame) -> str:
    import gcs_io
    return gcs_io.write_ml_parquet(ledger, *LEDGER)
