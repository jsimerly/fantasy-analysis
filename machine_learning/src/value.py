"""Intrinsic value = discounted points above replacement over the projected career.

    VORP_k  = max(ppg_hat_k - replacement_ppg[position], 0) x games_hat_k
    IV      = sum_k  (1 - rate)^(k-1) x VORP_k                (k = 1..H; k = 1 is the coming season)

The discount rate is the dynasty manager's time preference, per year: at 5 % a point above
replacement this season counts 1.00, next season 0.95, the one after 0.9025. 0 % weighs every
season the same; 100 % counts only the coming season. It is a parameter, not a fact -- the owner
should tune it (the projections table exposes it as a slider); 20 % is the default.

``realized_value`` applies the same formula to what actually happened (for backtests), and
``compare_to_market`` lines intrinsic value up against KTC: a monotone (isotonic) fit maps
IV onto KTC's 1-9999 scale so the *residual* (market - fair value) is a mispricing in market
units, and rank correlation says how much of the market's ordering the fundamentals explain.
"""
from __future__ import annotations

from typing import Iterable

import numpy as np
import polars as pl
from scipy.stats import norm
from sklearn.isotonic import IsotonicRegression

DEFAULT_DISCOUNT_RATE = 0.2


def discount_weight(k: int, rate: float) -> float:
    """Weight of season k (k = 1 is the coming season, at full weight) under a per-year ``rate``."""
    if not 0.0 <= rate <= 1.0:
        raise ValueError(f"discount rate must be in [0, 1], got {rate}")
    return (1.0 - rate) ** (k - 1)


def _rep_expr(rep: dict[str, float]) -> pl.Expr:
    return pl.col("position").replace_strict(rep, default=0.0, return_dtype=pl.Float64)


def vorp_expr(ppg_col: str, games_col: str, rep: dict[str, float]) -> pl.Expr:
    return (pl.max_horizontal(pl.col(ppg_col) - _rep_expr(rep), pl.lit(0.0)) * pl.col(games_col)).cast(pl.Float64)


def expected_excess(mu: np.ndarray, sigma: np.ndarray, rep: np.ndarray) -> np.ndarray:
    """E[max(X - rep, 0)] for X ~ Normal(mu, sigma): the value of a projection INCLUDING its
    upside. A player projected right at replacement is worth sigma/sqrt(2*pi) per game, not
    zero -- the "option value" the point estimate throws away. sigma -> 0 recovers max(mu-rep, 0)."""
    mu, sigma, rep = (np.asarray(a, float) for a in (mu, sigma, rep))
    sigma = np.maximum(sigma, 1e-9)
    z = (mu - rep) / sigma
    return (mu - rep) * norm.cdf(z) + sigma * norm.pdf(z)


def intrinsic_value(
    df: pl.DataFrame, rep: dict[str, float], horizons: Iterable[int], discount_rate: float = DEFAULT_DISCOUNT_RATE,
) -> pl.DataFrame:
    """Add ``h{k}_vorp_hat`` per horizon plus ``iv`` (discounted) and ``iv_undiscounted``.

    When ``h{k}_ppg_sigma`` is present (see ``career.HorizonModels.estimate_sigma``) the
    points above replacement are the expected excess under that spread; otherwise the plain
    clipped point estimate."""
    horizons = list(horizons)
    rep_arr = df.select(_rep_expr(rep)).to_series().to_numpy()
    cols = []
    for k in horizons:
        mu = df[f"h{k}_ppg_hat"].to_numpy().astype(float)
        games = df[f"h{k}_games_hat"].to_numpy().astype(float)
        if f"h{k}_ppg_sigma" in df.columns:
            excess = expected_excess(mu, df[f"h{k}_ppg_sigma"].to_numpy(), rep_arr)
        else:
            excess = np.maximum(mu - rep_arr, 0.0)
        cols.append(pl.Series(f"h{k}_vorp_hat", excess * games))
    out = df.with_columns(cols)
    return out.with_columns(
        pl.sum_horizontal([pl.col(f"h{k}_vorp_hat") * discount_weight(k, discount_rate) for k in horizons]).alias("iv"),
        pl.sum_horizontal([pl.col(f"h{k}_vorp_hat") for k in horizons]).alias("iv_undiscounted"),
    )


def realized_value(
    df: pl.DataFrame, rep: dict[str, float], horizons: Iterable[int], discount_rate: float = DEFAULT_DISCOUNT_RATE,
) -> pl.DataFrame:
    """Same formula on actual outcomes (``h{k}_ppg`` / ``h{k}_games``; a season not played is 0).
    Rows with any censored horizon get a null ``realized_iv``."""
    horizons = list(horizons)
    out = df.with_columns([
        vorp_expr(f"h{k}_ppg", f"h{k}_games", rep).fill_null(0.0).alias(f"h{k}_vorp") for k in horizons
    ])
    observable = pl.all_horizontal([pl.col(f"h{k}_observable") for k in horizons])
    return out.with_columns(
        pl.when(observable)
        .then(pl.sum_horizontal([pl.col(f"h{k}_vorp") * discount_weight(k, discount_rate) for k in horizons]))
        .otherwise(None).alias("realized_iv")
    )


# ------------------------------------------------------------------------- market view
def spearman(a: np.ndarray, b: np.ndarray) -> float:
    a, b = np.asarray(a, float), np.asarray(b, float)
    ok = np.isfinite(a) & np.isfinite(b)
    if ok.sum() < 3:
        return float("nan")
    ra = pl.Series(a[ok]).rank(method="average").to_numpy()
    rb = pl.Series(b[ok]).rank(method="average").to_numpy()
    return float(np.corrcoef(ra, rb)[0, 1])


def fair_value_curve(iv: np.ndarray, market: np.ndarray, method: str = "power") -> np.ndarray:
    """Monotone map from intrinsic value onto the market's scale.

    ``power`` (default): least-squares fit of ``log(market) = a + b*log(iv + 1)`` -- smooth and
    strictly increasing, so two players with different IV never get the same fair value.
    ``isotonic``: the non-parametric step fit; wherever the market's ordering disagrees with
    IV's it pools players into one flat step (that is why the top of the list came out tied),
    so it is kept only as an option."""
    iv, market = np.asarray(iv, float), np.asarray(market, float)
    if method == "isotonic":
        return IsotonicRegression(increasing=True, out_of_bounds="clip").fit(iv, market).predict(iv)
    ok = market > 0
    b, a = np.polyfit(np.log1p(iv[ok]), np.log(market[ok]), 1)
    b = max(b, 1e-6)                                   # keep it increasing
    return np.exp(a) * np.power(1.0 + iv, b)


def compare_to_market(df: pl.DataFrame, iv_col: str = "iv", market_col: str = "ktc_value",
                      method: str = "power", group_col: str | None = None) -> tuple[pl.DataFrame, dict]:
    """Rows with both an IV and a market value, plus:
    ``fair_value`` -- monotone map of IV onto the market's scale (see ``fair_value_curve``),
    ``mispricing`` -- market - fair_value (positive = market pays more than fundamentals),
    ``mispricing_pct`` -- mispricing / fair_value,
    ``iv_rank`` / ``market_rank`` / ``rank_gap`` -- ordering disagreement (positive = market ranks higher).
    With ``group_col`` (e.g. "position") the curve is fitted per group, so the mispricing says
    "vs other players at the position" and the market's positional premium is factored out.
    Summary: n and spearman (pooled)."""
    both = df.filter(pl.col(iv_col).is_not_null() & pl.col(market_col).is_not_null())
    if both.height < 3:
        return both, {"n": both.height, "spearman": float("nan")}
    iv = both[iv_col].to_numpy().astype(float)
    mk = both[market_col].to_numpy().astype(float)
    if group_col is None:
        fair = fair_value_curve(iv, mk, method)
    else:
        fair = np.empty(both.height)
        groups = both[group_col].to_numpy()
        for g in np.unique(groups):
            m = groups == g
            fair[m] = fair_value_curve(iv[m], mk[m], method) if m.sum() >= 3 else fair_value_curve(iv, mk, method)[m]
    out = both.with_columns(
        pl.Series("fair_value", fair),
        (pl.col(market_col) - pl.Series("fair_value", fair)).alias("mispricing"),
    ).with_columns(
        (pl.col("mispricing") / pl.col("fair_value").clip(lower_bound=1.0)).alias("mispricing_pct"),
        # ranks come back unsigned; cast before differencing or (1 - 9) wraps to 4294967288
        pl.col(iv_col).rank(method="ordinal", descending=True).cast(pl.Int64).alias("iv_rank"),
        pl.col(market_col).rank(method="ordinal", descending=True).cast(pl.Int64).alias("market_rank"),
    ).with_columns((pl.col("iv_rank") - pl.col("market_rank")).alias("rank_gap"))
    return out, {"n": both.height, "spearman": spearman(iv, mk)}
