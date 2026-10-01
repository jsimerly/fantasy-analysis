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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

import numpy as np
import polars as pl

import career
import feature_groups as fg
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


def top_decile_precision(score: np.ndarray, realized: np.ndarray, frac: float = 0.1) -> float:
    """Share of the top ``frac`` by ``score`` that are also top ``frac`` by ``realized``."""
    score, realized = np.asarray(score, float), np.asarray(realized, float)
    k = max(1, int(round(frac * len(score))))
    top_s = set(np.argsort(-score)[:k].tolist())
    top_r = set(np.argsort(-realized)[:k].tolist())
    return len(top_s & top_r) / k


def git_commit() -> str | None:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, check=True, text=True).stdout.strip()
    except Exception:  # noqa: BLE001
        return None


def run_experiment(
    matrix: pl.DataFrame, cfg: ExperimentConfig, ctx: fg.Context,
    rep_for: Callable[[int], dict[str, float]],
    market_for: Callable[[pl.DataFrame, int], pl.DataFrame],
) -> tuple[pl.DataFrame, dict]:
    """``matrix`` is the career matrix (``career.build_career_matrix``) for the configured horizons;
    ``rep_for(T)`` gives replacement ppg as of T; ``market_for(cohort, T)`` attaches ``ktc_value``."""
    groups = fg.resolve(cfg.groups)
    cols = fg.feature_columns(groups)
    df = fg.assemble(matrix, groups, ctx)
    H = list(cfg.horizons)
    rows = []
    for T in cfg.cohorts:
        rep = rep_for(T)
        models = career.HorizonModels(H, device=cfg.device, features=cols, **cfg.params).fit(df, as_of_season=T)
        models.estimate_sigma(df, as_of_season=T)
        survival = career.AgeSurvival().fit(df.filter((pl.col("season") + 1) <= T))
        cohort = survival.cap_games(models.predict(df.filter(pl.col("season") == T)), H)
        cohort = value.realized_value(value.intrinsic_value(cohort, rep, H, cfg.discount_rate), rep, H, cfg.discount_rate)
        cohort = market_for(cohort, T)
        obs = cohort.filter(pl.col("realized_iv").is_not_null())
        priced = obs.filter(pl.col("ktc_value").is_not_null())
        row = {"name": cfg.name, "cohort": T, "n_all": obs.height, "n_priced": priced.height}
        if obs.height:
            row["spearman_iv_vs_realized_all"] = value.spearman(obs["iv"].to_numpy(), obs["realized_iv"].to_numpy())
        if priced.height:
            iv, kt, rz = (priced[c].to_numpy().astype(float) for c in ("iv", "ktc_value", "realized_iv"))
            row.update({"spearman_iv_vs_realized": value.spearman(iv, rz), "spearman_ktc_vs_realized": value.spearman(kt, rz),
                        "top_decile_iv": top_decile_precision(iv, rz), "top_decile_ktc": top_decile_precision(kt, rz)})
        for k in H:
            o = cohort.filter(pl.col(f"h{k}_observable"))
            if o.height:
                row[f"mae_h{k}"] = float(np.mean(np.abs(o[f"h{k}_fpts"].to_numpy().astype(float) - o[f"h{k}_fpts_hat"].to_numpy().astype(float))))
        rows.append(row)
    per_cohort = pl.DataFrame(rows)
    metric_cols = [c for c in per_cohort.columns if c.startswith(("spearman", "top_decile", "mae_"))]
    summary = {"name": cfg.name, "groups": ",".join(g.name for g in groups), "n_features": len(cols),
               "horizon": max(H), "cohorts": f"{min(cfg.cohorts)}-{max(cfg.cohorts)}", "n_cohorts": len(cfg.cohorts),
               "discount_rate": cfg.discount_rate, "params": repr(cfg.params) if cfg.params else "",
               "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"), "commit": git_commit()}
    for c in metric_cols:
        summary[c] = float(per_cohort[c].mean())
    if "spearman_iv_vs_realized" in summary and "spearman_ktc_vs_realized" in summary:
        summary["iv_minus_ktc"] = summary["spearman_iv_vs_realized"] - summary["spearman_ktc_vs_realized"]
    return per_cohort, summary


# ------------------------------------------------------------------------------ ledger
LEDGER_COLS = ["timestamp", "name", "groups", "n_features", "horizon", "cohorts", "n_cohorts", "discount_rate", "params", "commit",
               "spearman_iv_vs_realized", "spearman_ktc_vs_realized", "iv_minus_ktc", "spearman_iv_vs_realized_all",
               "top_decile_iv", "top_decile_ktc"]


def append_result(ledger: pl.DataFrame | None, summary: dict) -> pl.DataFrame:
    row = pl.DataFrame([summary]).select([pl.col(c) if c in summary else pl.lit(None).alias(c)
                                           for c in LEDGER_COLS + sorted(k for k in summary if k.startswith("mae_"))])
    return row if ledger is None or ledger.height == 0 else pl.concat([ledger, row], how="diagonal_relaxed")


def leaderboard(ledger: pl.DataFrame, horizon: int | None = None, cohorts: str | None = None) -> pl.DataFrame:
    """Variants ranked by rank agreement with realized value; filter to one horizon / cohort span
    so the comparison is like for like."""
    lb = ledger
    if horizon is not None:
        lb = lb.filter(pl.col("horizon") == horizon)
    if cohorts is not None:
        lb = lb.filter(pl.col("cohorts") == cohorts)
    show = [c for c in ["name", "groups", "n_features", "horizon", "cohorts", "spearman_iv_vs_realized", "spearman_ktc_vs_realized",
                        "iv_minus_ktc", "top_decile_iv", "mae_h1", "timestamp", "commit"] if c in lb.columns]
    return lb.select(show).sort("spearman_iv_vs_realized", descending=True, nulls_last=True)


def load_ledger() -> pl.DataFrame | None:
    import gcs_io
    try:
        return gcs_io.read_ml_parquet(*LEDGER)
    except Exception:  # noqa: BLE001 - first run
        return None


def save_ledger(ledger: pl.DataFrame) -> str:
    import gcs_io
    return gcs_io.write_ml_parquet(ledger, *LEDGER)
