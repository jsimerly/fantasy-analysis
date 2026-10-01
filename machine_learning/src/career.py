"""Career projection: expected production for each of the next H seasons.

This is the "project the career" half of intrinsic value. For every player-season T we train
one pair of gradient-boosted models per horizon k = 1..H:

  * ``ppg_h{k}``   -- points per game in season T+k, trained only on players who PLAYED in T+k
                     (a *rate*: how good is the player when on the field),
  * ``games_h{k}`` -- games played in season T+k, with 0 for players who did not play at all
                     (an *availability* that already carries injury risk and leaving the league).

E[production at T+k] = ppg_hat x games_hat. ``value.py`` turns that into points above the
league's replacement level and discounts it to the present.

Why direct multi-horizon models instead of rolling a one-year model forward: each horizon is
its own supervised problem with real outcomes (no simulated features, no compounding error),
attrition is *learnt* instead of assumed, and outcomes that are not yet observable (T+k after
the last complete season) are simply censored rather than guessed.

Leakage rules (enforced here, not left to the caller):
  * a horizon target for row (player, T) is season T+k, taken only from COMPLETE seasons;
  * a missing T+k row for an observable horizon means 0 games / 0 points (the player did not
    play in the NFL that year), never "drop the row" -- that is the survivorship bias that
    makes one-year models over-value fringe players;
  * ``HorizonModels.fit(train, as_of_season)`` uses only outcomes observed by the end of
    ``as_of_season``, so a backtest "as of 2020" cannot peek at 2021+.
"""
from __future__ import annotations

from typing import Iterable

import numpy as np
import polars as pl
from xgboost import XGBRegressor

import model as one_year

# career-to-date aggregates (all <= T, so leakage-safe by construction)
CAREER_FEATURES = ["career_seasons", "career_fpts", "career_games", "best_ppg"]
FEATURES = one_year.FEATURE_COLS + CAREER_FEATURES

MAX_GAMES = 17

# A little more regularised than the one-year model: far horizons are noisier.
DEFAULT_PARAMS = {**one_year.DEFAULT_PARAMS, "n_estimators": 300, "max_depth": 4}


# --------------------------------------------------------------------------- features
def career_features(df: pl.DataFrame) -> pl.DataFrame:
    """Add cumulative career-to-date columns (seasons, points, games, best ppg) through season T."""
    return (
        df.sort(["player_id", "season"])
        .with_columns(
            pl.col("season").cum_count().over("player_id").cast(pl.Int64).alias("career_seasons"),
            pl.col("fpts").cum_sum().over("player_id").alias("career_fpts"),
            pl.col("games").cum_sum().over("player_id").cast(pl.Int64).alias("career_games"),
            pl.col("ppg").cum_max().over("player_id").alias("best_ppg"),
        )
    )


def horizon_feature_frame(df: pl.DataFrame) -> pl.DataFrame:
    """One-year feature frame + career features, fixed schema (missing columns -> null)."""
    base = one_year.make_feature_frame(df)
    extra = df.select([
        pl.col(c).cast(pl.Float64, strict=False) if c in df.columns else pl.lit(None, pl.Float64).alias(c)
        for c in CAREER_FEATURES
    ])
    return pl.concat([base, extra], how="horizontal")


# ---------------------------------------------------------------------------- targets
def last_complete_season(df: pl.DataFrame) -> int:
    if "season_complete" in df.columns:
        done = df.filter(pl.col("season_complete"))
        if done.height:
            return int(done["season"].max())
    return int(df["season"].max())


def attach_horizon_targets(df: pl.DataFrame, horizons: Iterable[int]) -> pl.DataFrame:
    """For each horizon k add ``h{k}_fpts`` / ``h{k}_games`` / ``h{k}_ppg`` / ``h{k}_played``
    (season T+k outcomes) and ``h{k}_observable`` (T+k is a complete season).

    Observable + no T+k row  -> 0 games, 0 fpts, played=False, ppg=null (did not play).
    Not observable (censored) -> every target null.
    """
    done = df.filter(pl.col("season_complete")) if "season_complete" in df.columns else df
    last = last_complete_season(df)
    out = df
    for k in horizons:
        fut = done.select(
            "player_id", (pl.col("season") - k).alias("season"),
            pl.col("fpts").alias(f"h{k}_fpts"), pl.col("games").alias(f"h{k}_games"),
            pl.col("ppg").alias(f"h{k}_ppg"),
        )
        observable = (pl.col("season") + k) <= last
        out = (
            out.join(fut, on=["player_id", "season"], how="left")
            .with_columns(
                observable.alias(f"h{k}_observable"),
                pl.when(observable).then(pl.col(f"h{k}_fpts").fill_null(0.0)).otherwise(None).alias(f"h{k}_fpts"),
                pl.when(observable).then(pl.col(f"h{k}_games").fill_null(0).cast(pl.Int64)).otherwise(None).alias(f"h{k}_games"),
                pl.when(observable).then(pl.col(f"h{k}_ppg")).otherwise(None).alias(f"h{k}_ppg"),
            )
            .with_columns(
                pl.when(observable).then(pl.col(f"h{k}_games") > 0).otherwise(None).alias(f"h{k}_played")
            )
        )
    return out


# ----------------------------------------------------------------------------- models
class HorizonModels:
    """One (ppg, games) model pair per horizon. ``fit`` then ``predict``."""

    def __init__(self, horizons: Iterable[int], device: str = "cpu", seed: int = 0, **params):
        self.horizons = list(horizons)
        self.device = device
        self.seed = seed
        self.params = {**DEFAULT_PARAMS, **params}
        self.ppg_models: dict[int, XGBRegressor] = {}
        self.games_models: dict[int, XGBRegressor] = {}

    def _new(self) -> XGBRegressor:
        return XGBRegressor(tree_method="hist", device=self.device, random_state=self.seed,
                            n_jobs=-1, **self.params)

    def fit(self, train: pl.DataFrame, as_of_season: int | None = None) -> "HorizonModels":
        """Train every horizon. ``as_of_season`` restricts outcomes to those observed by the end
        of that season (row season + k <= as_of): the leak-safe way to train "as of" a past year."""
        for k in self.horizons:
            rows = train.filter(pl.col(f"h{k}_observable"))
            if as_of_season is not None:
                rows = rows.filter((pl.col("season") + k) <= as_of_season)
            played = rows.filter(pl.col(f"h{k}_played"))
            if rows.height == 0 or played.height == 0:
                raise ValueError(f"horizon {k}: no observable training outcomes (as_of={as_of_season})")
            g = self._new().fit(horizon_feature_frame(rows).to_numpy(),
                                rows[f"h{k}_games"].to_numpy().astype(float))
            p = self._new().fit(horizon_feature_frame(played).to_numpy(),
                                played[f"h{k}_ppg"].to_numpy().astype(float))
            self.games_models[k], self.ppg_models[k] = g, p
        return self

    def estimate_sigma(self, train: pl.DataFrame, as_of_season: int | None = None,
                       holdout: int = 3) -> dict[int, dict[str, float]]:
        """Out-of-sample spread of the ppg projection, per horizon and position.

        Fits a temporary set of models as of ``as_of - holdout`` and measures the ppg residual
        std on the outcomes of the last ``holdout`` seasons (rows the temporary models never
        saw). ``value.py`` uses it to price the UPSIDE of a projection -- a player projected
        near replacement is not worth zero, he is worth the expected excess over replacement
        given how wrong projections at that horizon usually are. Stored on ``self.sigma`` and
        attached by ``predict`` as ``h{k}_ppg_sigma``.
        """
        as_of = as_of_season if as_of_season is not None else last_complete_season(train)
        cut = as_of - holdout
        tmp = HorizonModels(self.horizons, self.device, self.seed, **self.params).fit(train, as_of_season=cut)
        sigma: dict[int, dict[str, float]] = {}
        for k in self.horizons:
            rows = train.filter(
                pl.col(f"h{k}_observable") & pl.col(f"h{k}_played")
                & ((pl.col("season") + k) > cut) & ((pl.col("season") + k) <= as_of)
            )
            if rows.height == 0:
                raise ValueError(f"horizon {k}: no holdout outcomes between {cut} and {as_of}")
            pred = tmp.predict(rows).with_columns((pl.col(f"h{k}_ppg_hat") - pl.col(f"h{k}_ppg")).alias("_r"))
            per_pos = {pos: float(s) for pos, s in pred.group_by("position").agg(pl.col("_r").std()).iter_rows()
                       if s is not None}
            per_pos["__all__"] = float(pred["_r"].std())
            sigma[k] = per_pos
        self.sigma = sigma
        return sigma

    def predict(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add ``h{k}_games_hat`` / ``h{k}_ppg_hat`` / ``h{k}_fpts_hat`` for every horizon
        (plus ``h{k}_ppg_sigma`` when ``estimate_sigma`` has run)."""
        X = horizon_feature_frame(df).to_numpy()
        cols = []
        sigma = getattr(self, "sigma", None)
        for k in self.horizons:
            games = np.clip(self.games_models[k].predict(X), 0.0, MAX_GAMES)
            ppg = np.clip(self.ppg_models[k].predict(X), 0.0, None)
            cols += [pl.Series(f"h{k}_games_hat", games), pl.Series(f"h{k}_ppg_hat", ppg),
                     pl.Series(f"h{k}_fpts_hat", games * ppg)]
            if sigma:
                s = sigma[k]
                cols.append(df["position"].replace_strict(
                    {p: v for p, v in s.items() if p != "__all__"}, default=s["__all__"], return_dtype=pl.Float64
                ).alias(f"h{k}_ppg_sigma"))
        return df.with_columns(cols)


# ------------------------------------------------------------------- age-survival prior
class AgeSurvival:
    """Population probability of still playing k seasons out, by position and age.

    The gradient-boosted games models learn attrition well where data is dense, but at the
    oldest ages the only training examples are the survivors (every 43-year-old QB season in
    the data is Tom Brady's), so they extrapolate a 43-year-old with 13 ppg as if he were
    Brady. A smooth logistic fit of P(played next season | age) per position, dominated by
    the hundreds of 30-38-year-old seasons, is a far better prior there; projected games at
    horizon k are capped at ``MAX_GAMES x survival(age, k)``. The cap only binds where the
    model is over-optimistic (it is ~15 games for anyone under 30).
    """

    def __init__(self) -> None:
        self.coef: dict[str, tuple[float, float]] = {}      # position -> (intercept, slope on age)

    def fit(self, season_df: pl.DataFrame) -> "AgeSurvival":
        from sklearn.linear_model import LogisticRegression

        rows = season_df.filter(pl.col("h1_observable") & (pl.col("games") > 0) & pl.col("age_at_season").is_not_null())
        for pos in rows["position"].unique().to_list():
            sub = rows.filter(pl.col("position") == pos)
            if sub.height < 50 or sub["h1_played"].n_unique() < 2:
                continue
            lr = LogisticRegression().fit(sub[["age_at_season"]].to_numpy(), sub["h1_played"].to_numpy().astype(int))
            self.coef[pos] = (float(lr.intercept_[0]), float(lr.coef_[0][0]))
        return self

    def p_next(self, position: str, age: np.ndarray) -> np.ndarray:
        if position not in self.coef:
            return np.ones_like(np.asarray(age, float))
        b0, b1 = self.coef[position]
        return 1.0 / (1.0 + np.exp(-(b0 + b1 * np.asarray(age, float))))

    def survival(self, position: str, age: np.ndarray, k: int) -> np.ndarray:
        """P(plays in season age+k | plays now) = product of the yearly continuation odds."""
        age = np.asarray(age, float)
        s = np.ones_like(age)
        for j in range(k):
            s = s * self.p_next(position, age + j)
        return s

    def cap_games(self, df: pl.DataFrame, horizons: Iterable[int], age_col: str = "age_at_season",
                  col: str = "h{k}_games_hat") -> pl.DataFrame:
        """Cap each horizon's projected games at MAX_GAMES x survival (per row's position/age)."""
        cols = []
        pos = df["position"].to_numpy()
        age = df[age_col].fill_null(27.0).to_numpy().astype(float)
        for k in horizons:
            name = col.format(k=k)
            if name not in df.columns:
                continue
            cap = np.empty(df.height)
            for p in np.unique(pos):
                m = pos == p
                cap[m] = MAX_GAMES * self.survival(str(p), age[m], k)
            cols.append(pl.Series(name, np.minimum(df[name].to_numpy().astype(float), cap)))
        return df.with_columns(cols)


# --------------------------------------------------------------------------- baselines
def carry_forward_horizons(test: pl.DataFrame, horizons: Iterable[int]) -> pl.DataFrame:
    """Naive: next k seasons look like this one."""
    return test.with_columns([pl.col("fpts").alias(f"h{k}_fpts_carry") for k in horizons])


def decay_baseline(train: pl.DataFrame, test: pl.DataFrame, horizons: Iterable[int],
                   as_of_season: int | None = None, bucket: int = 2) -> pl.DataFrame:
    """Age-aware carry: this season's points x the pooled (position, age bucket, k) retention
    ratio sum(h{k}_fpts)/sum(fpts) from the training rows. The multi-horizon version of the
    one-year ``age_adjusted_naive`` baseline; it already encodes aging + attrition on average."""
    out = test.with_columns((pl.col("age_at_season") // bucket * bucket).alias("_ab"))
    for k in horizons:
        rows = train.filter(pl.col(f"h{k}_observable") & (pl.col("fpts") > 0))
        if as_of_season is not None:
            rows = rows.filter((pl.col("season") + k) <= as_of_season)
        rows = rows.with_columns((pl.col("age_at_season") // bucket * bucket).alias("_ab"))
        ratios = rows.group_by("position", "_ab").agg(
            (pl.col(f"h{k}_fpts").sum() / pl.col("fpts").sum()).alias("_r"))
        denom = float(rows["fpts"].sum())
        global_r = float(rows[f"h{k}_fpts"].sum() / denom) if denom else 1.0
        out = (
            out.join(ratios, on=["position", "_ab"], how="left")
            .with_columns((pl.col("fpts") * pl.col("_r").fill_null(global_r)).alias(f"h{k}_fpts_decay"))
            .drop("_r")
        )
    return out.drop("_ab")


# ------------------------------------------------------------------------- assembly
def build_career_matrix(horizons: Iterable[int]) -> pl.DataFrame:
    """fact_player_season (lake) -> one-year lags -> career features -> horizon targets.
    Rows from the in-progress season are kept (they have no outcomes yet) so the latest
    complete season can be projected and the partial one inspected."""
    import features

    season = features.load_fact_player_season()
    df = features.attach_lags_and_target(season, drop_no_target=False)
    return attach_horizon_targets(career_features(df), list(horizons))


# -------------------------------------------------------------------------- evaluation
def walk_forward_horizon_eval(
    df: pl.DataFrame, horizons: Iterable[int], start_season: int = 2010,
    device: str = "cpu", **params,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Expanding-window CV per horizon: for each test season s train on seasons < s using only
    outcomes known by the end of s, then score season-s rows on each observable horizon.
    Returns ``(per_fold, aggregate)`` with MAE/RMSE for model / carry / decay."""
    horizons = list(horizons)
    seasons = sorted(df["season"].unique().to_list())
    rows = []
    for s in seasons:
        if s < start_season:
            continue
        train, test = df.filter(pl.col("season") < s), df.filter(pl.col("season") == s)
        if not train.height or not test.height:
            continue
        try:
            models = HorizonModels(horizons, device=device, **params).fit(train, as_of_season=s)
        except ValueError:
            continue
        pred = decay_baseline(train, carry_forward_horizons(models.predict(test), horizons), horizons, as_of_season=s)
        for k in horizons:
            obs = pred.filter(pl.col(f"h{k}_observable"))
            if not obs.height:
                continue
            y = obs[f"h{k}_fpts"].to_numpy().astype(float)
            for method, col in [("model", f"h{k}_fpts_hat"), ("carry_forward", f"h{k}_fpts_carry"),
                                ("decay", f"h{k}_fpts_decay")]:
                p = obs[col].to_numpy().astype(float)
                rows.append({"horizon": k, "method": method, "test_season": s, "n": obs.height,
                             "mae": float(np.mean(np.abs(y - p))), "rmse": float(np.sqrt(np.mean((y - p) ** 2)))})
    per_fold = pl.DataFrame(rows)
    agg = (
        per_fold.group_by("horizon", "method").agg(
            ((pl.col("mae") * pl.col("n")).sum() / pl.col("n").sum()).alias("mae"),
            ((pl.col("rmse") * pl.col("n")).sum() / pl.col("n").sum()).alias("rmse"),
            pl.col("n").sum().alias("n_total"), pl.len().alias("folds"),
        ).sort("horizon", "mae")
    )
    return per_fold, agg
