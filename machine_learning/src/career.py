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


OPPORTUNITY_COLS = ["targets", "rush_att", "pass_att"]     # an opportunity: a target, a carry or a pass attempt


def attach_horizon_targets(df: pl.DataFrame, horizons: Iterable[int]) -> pl.DataFrame:
    """For each horizon k add ``h{k}_fpts`` / ``h{k}_games`` / ``h{k}_ppg`` / ``h{k}_played``
    (season T+k outcomes) and ``h{k}_observable`` (T+k is a complete season).

    Observable + no T+k row  -> 0 games, 0 fpts, played=False, ppg=null (did not play).
    Not observable (censored) -> every target null.
    """
    done = df.filter(pl.col("season_complete")) if "season_complete" in df.columns else df
    last = last_complete_season(df)
    out = df
    opp_cols = [c for c in OPPORTUNITY_COLS if c in done.columns]
    for k in horizons:
        fut = done.select(
            "player_id", (pl.col("season") - k).alias("season"),
            pl.col("fpts").alias(f"h{k}_fpts"), pl.col("games").alias(f"h{k}_games"),
            pl.col("ppg").alias(f"h{k}_ppg"),
            (pl.sum_horizontal([pl.col(c).fill_null(0) for c in opp_cols]) if opp_cols else pl.lit(None, pl.Float64)).cast(pl.Float64).alias(f"h{k}_opp"),
        )
        observable = (pl.col("season") + k) <= last
        out = (
            out.join(fut, on=["player_id", "season"], how="left")
            .with_columns(
                observable.alias(f"h{k}_observable"),
                pl.when(observable).then(pl.col(f"h{k}_fpts").fill_null(0.0)).otherwise(None).alias(f"h{k}_fpts"),
                pl.when(observable).then(pl.col(f"h{k}_games").fill_null(0).cast(pl.Int64)).otherwise(None).alias(f"h{k}_games"),
                pl.when(observable).then(pl.col(f"h{k}_ppg")).otherwise(None).alias(f"h{k}_ppg"),
                pl.when(observable).then(pl.col(f"h{k}_opp")).otherwise(None).alias(f"h{k}_opp"),
            )
            .with_columns(
                pl.when(observable).then(pl.col(f"h{k}_games") > 0).otherwise(None).alias(f"h{k}_played")
            )
        )
    return out


# ----------------------------------------------------------------------------- models
BACKENDS = ("xgb", "tabpfn", "blend")


class _TabPFN:
    """TabPFN regressor behind the xgboost-shaped ``fit(X, y, sample_weight) / predict(X)`` the
    horizon models use. A pretrained tabular foundation model: ``fit`` stores the training set and
    ``predict`` runs in-context regression over it, so there is nothing to tune and the GPU does the
    work at prediction time. Missing values pass through as NaN. Sample weights are not supported
    by the model and are ignored (``weight`` variants are an xgboost-only experiment)."""

    def __init__(self, device: str = "cpu", seed: int = 0, params: dict | None = None):
        self.device, self.seed, self.params = device, seed, dict(params or {})
        self.model = None

    def fit(self, X, y, sample_weight=None):
        import os
        params = dict(self.params)
        # which pretrained weights: "v2" (open), "v2.5" / "v3" / "v3.5" (one-time licence acceptance,
        # TABPFN_TOKEN for headless runs); the package reads TABPFN_MODEL_VERSION at import
        version = params.pop("model_version", None)
        if version:
            os.environ["TABPFN_MODEL_VERSION"] = str(version)
        from tabpfn import TabPFNRegressor
        # the career matrix has ~12k training rows per horizon, past the model's advertised 10k.
        # fit_preprocessors re-encodes the training set on every predict (~1 min for 600 rows on an
        # RTX 2060) but holds no GPU cache; fit_with_cache is faster per predict yet every fitted
        # model keeps its cache on the GPU, and a harness cohort holds twelve of them (6 GB OOM)
        params.setdefault("ignore_pretraining_limits", True)
        params.setdefault("fit_mode", "fit_preprocessors")
        self.model = TabPFNRegressor(device=self.device, random_state=self.seed, **params)
        self.model.fit(np.asarray(X, dtype=np.float32), np.asarray(y, dtype=np.float32))
        return self

    def predict(self, X):
        return np.asarray(self.model.predict(np.asarray(X, dtype=np.float32)), dtype=float)


class _Blend:
    """Mean of several estimators' predictions (the xgb + tabpfn blend of the bake-off)."""

    def __init__(self, members):
        self.members = list(members)

    def fit(self, X, y, sample_weight=None):
        for m in self.members:
            m.fit(X, y, sample_weight=sample_weight)
        return self

    def predict(self, X):
        return np.mean([np.asarray(m.predict(X), dtype=float) for m in self.members], axis=0)


class HorizonModels:
    """One (ppg, games) model pair per horizon. ``fit`` then ``predict``."""

    def __init__(self, horizons: Iterable[int], device: str = "cpu", seed: int = 0,
                 features: list[str] | None = None, calibrate: bool = False, holdout: int = 3,
                 quantile_sigma: bool = False, quantiles: tuple[float, float] = (0.16, 0.84),
                 target: str = "level", weight: str | None = None,
                 backend: str = "xgb", tabpfn_params: dict | None = None, stacked: bool = False, **params):
        self.horizons = list(horizons)
        # stacked: every horizon's training rows in one frame with "years ahead" (and age + years ahead)
        # as features, one games model and one ppg model for all horizons; the decay over time is then
        # learnt from the features jointly instead of per horizon (each horizon's model hedging on its own)
        self.stacked = stacked
        self.stacked_models: dict[str, object] = {}
        self.device = device
        self.seed = seed
        # backend: "xgb" (production trees), "tabpfn" (pretrained tabular foundation model, in-context
        # regression on the same frame and targets) or "blend" (mean of the two). The games and ppg
        # models of every horizon use the same backend; quantile models stay xgboost.
        if backend not in BACKENDS:
            raise ValueError(f"backend must be one of {BACKENDS}, got {backend!r}")
        self.backend, self.tabpfn_params = backend, dict(tabpfn_params or {})
        self.features = list(features) if features is not None else None   # None = FEATURES (production)
        # True / "both": ppg and games lines; "ppg": ppg only (games lines hurt: a least-squares line
        # through a 0-or-14 target pulls starters' games down); "games": games only; False: none
        # "tier": additive ppg adjustment per position x prior-season tier (rank by ppg within position),
        # keyed on what the player WAS rather than on the projection -- the conditioning under which the
        # shrinkage of good players shows up out of sample
        # "games_table": projected games per horizon replaced by the empirical mean realized games of
        # the training rows in the same (position, age bucket, prior tier) — attrition and injury
        # read off history for players like this one, instead of the games model's horizon decay
        # "games_table[:scope[:weight]]": scope "all" | "tiers" (starter and mid tiers only, the fringe keeps
        # the model), weight = share of the table in a blend with the model's games (1 = replace)
        self.games_table: dict | None = None
        self.games_scope, self.games_weight = "all", 1.0
        if isinstance(calibrate, str) and calibrate.startswith("games_table"):
            parts = calibrate.split(":")
            self.games_scope = parts[1] if len(parts) > 1 and parts[1] else "all"
            self.games_weight = float(parts[2]) if len(parts) > 2 and parts[2] else 1.0
            calibrate = "games_table"
        self._calibrate_arg = calibrate
        self.calibrate = "both" if calibrate is True else (calibrate or False)
        self.holdout = holdout
        self.calibration: dict | None = None
        self.tier_adjust: dict | None = None
        self.quantile_sigma = quantile_sigma         # per-player spread from quantile models (else per position)
        self.quantiles = quantiles
        self.quantile_models: dict[int, tuple[XGBRegressor, XGBRegressor]] = {}
        self.params = {**DEFAULT_PARAMS, **params}
        # target: "level" fits h{k}_ppg directly; "residual" fits h{k}_ppg - ppg (this season's rate) and adds
        # it back, so the ensemble's default is "stays the same" and any regression to the mean is learnt.
        # weight: None | "ppg" (1 + ppg / 10) | "ppg2" (its square): relevance weights so the loss is dominated
        # by the players whose ordering matters rather than the long tail of near-zero rows.
        # "opportunity": ppg = (opportunities per game) x (points per opportunity), each its own model —
        # volume persists, efficiency regresses, and the product does not shrink a high-volume player's
        # rate the way one model of the level does.
        if target not in ("level", "residual", "opportunity"):
            raise ValueError(f"target must be level, residual or opportunity, got {target!r}")
        if weight not in (None, "ppg", "ppg2"):
            raise ValueError(f"weight must be None, ppg or ppg2, got {weight!r}")
        self.target, self.weight = target, weight
        self.ppg_models: dict[int, XGBRegressor] = {}
        self.games_models: dict[int, XGBRegressor] = {}
        self.opp_models: dict[int, XGBRegressor] = {}
        self.eff_models: dict[int, XGBRegressor] = {}

    def _xgb(self) -> XGBRegressor:
        return XGBRegressor(tree_method="hist", device=self.device, random_state=self.seed,
                            n_jobs=-1, **self.params)

    def _new(self):
        if self.backend == "tabpfn":
            return _TabPFN(self.device, self.seed, self.tabpfn_params)
        if self.backend == "blend":
            return _Blend([self._xgb(), _TabPFN(self.device, self.seed, self.tabpfn_params)])
        return self._xgb()

    def _kw(self) -> dict:
        """Constructor arguments that a temporary (holdout) copy of these models must share."""
        return dict(features=self.features, target=self.target, weight=self.weight,
                    backend=self.backend, tabpfn_params=self.tabpfn_params, stacked=self.stacked,
                    calibrate=(f"games_table:{self.games_scope}:{self.games_weight}" if self.calibrate == "games_table" else False), **self.params)

    def feature_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        """The design matrix. Default: the production feature set (``horizon_feature_frame``).
        With an explicit ``features`` list (experiment variants, see ``feature_groups``) every
        named column is taken from the standard frame if it lives there (so position one-hots
        work), else from ``df``, else all-null -- a group whose source has no coverage still
        trains, as nulls."""
        if self.features is None:
            return horizon_feature_frame(df)
        std = horizon_feature_frame(df)
        cols = []
        for c in self.features:
            if c in std.columns:
                cols.append(std[c].cast(pl.Float64, strict=False).alias(c))
            elif c in df.columns:
                cols.append(df[c].cast(pl.Float64, strict=False).alias(c))
            else:
                cols.append(pl.Series(c, [None] * df.height, dtype=pl.Float64))
        return pl.DataFrame(cols)

    def _stack_X(self, df: pl.DataFrame, k: int) -> np.ndarray:
        X = self.feature_frame(df).to_numpy()
        age = df["age_at_season"].cast(pl.Float64, strict=False).fill_null(27.0).to_numpy().astype(float) if "age_at_season" in df.columns else np.full(df.height, 27.0)
        return np.column_stack([X, np.full(df.height, float(k)), age + k])

    def _fit_stacked(self, train: pl.DataFrame, as_of_season: int | None) -> None:
        Xg, yg, wg, Xp, yp, wp = [], [], [], [], [], []
        for k in self.horizons:
            rows = train.filter(pl.col(f"h{k}_observable"))
            if as_of_season is not None:
                rows = rows.filter((pl.col("season") + k) <= as_of_season)
            played = rows.filter(pl.col(f"h{k}_played"))
            if rows.height == 0 or played.height == 0:
                raise ValueError(f"horizon {k}: no observable training outcomes (as_of={as_of_season})")
            Xg.append(self._stack_X(rows, k)); yg.append(rows[f"h{k}_games"].to_numpy().astype(float))
            w = self._weights(rows); wg.append(w if w is not None else np.ones(rows.height))
            y = played[f"h{k}_ppg"].to_numpy().astype(float)
            if self.target == "residual":
                y = y - played["ppg"].fill_null(0.0).to_numpy().astype(float)
            Xp.append(self._stack_X(played, k)); yp.append(y)
            w = self._weights(played); wp.append(w if w is not None else np.ones(played.height))
        g = self._new().fit(np.vstack(Xg), np.concatenate(yg), sample_weight=np.concatenate(wg) if self.weight else None)
        p = self._new().fit(np.vstack(Xp), np.concatenate(yp), sample_weight=np.concatenate(wp) if self.weight else None)
        self.stacked_models = {"games": g, "ppg": p}
        for k in self.horizons:                      # the per-horizon slots point at the shared models
            self.games_models[k], self.ppg_models[k] = g, p

    def fit(self, train: pl.DataFrame, as_of_season: int | None = None) -> "HorizonModels":
        """Train every horizon. ``as_of_season`` restricts outcomes to those observed by the end
        of that season (row season + k <= as_of): the leak-safe way to train "as of" a past year."""
        if self.stacked:
            self._fit_stacked(train, as_of_season)
            if self.calibrate == "tier":
                self._fit_tier_adjust(train, as_of_season)
            elif self.calibrate == "games_table":
                self._fit_games_table(train, as_of_season)
            elif self.calibrate:
                self._fit_calibration(train, as_of_season)
            return self
        for k in self.horizons:
            rows = train.filter(pl.col(f"h{k}_observable"))
            if as_of_season is not None:
                rows = rows.filter((pl.col("season") + k) <= as_of_season)
            played = rows.filter(pl.col(f"h{k}_played"))
            if rows.height == 0 or played.height == 0:
                raise ValueError(f"horizon {k}: no observable training outcomes (as_of={as_of_season})")
            g = self._new().fit(self.feature_frame(rows).to_numpy(),
                                rows[f"h{k}_games"].to_numpy().astype(float), sample_weight=self._weights(rows))
            y = played[f"h{k}_ppg"].to_numpy().astype(float)
            if self.target == "residual":
                y = y - played["ppg"].fill_null(0.0).to_numpy().astype(float)
            if self.target == "opportunity":
                if f"h{k}_opp" not in played.columns or played[f"h{k}_opp"].null_count() == played.height:
                    raise ValueError("opportunity target needs h{k}_opp (targets / rush_att / pass_att in the season table)")
                # rows whose opportunities were recorded (the season fact has no targets / attempts for a quarter
                # of 2000-09 rows: those read as 0 and would teach both models nonsense); >= 1 per game for efficiency
                opp = played.with_columns((pl.col(f"h{k}_opp") / pl.col(f"h{k}_games")).alias("_opp_pg")).filter(pl.col(f"h{k}_opp") > 0)
                used = opp.filter(pl.col("_opp_pg") >= 1.0)
                if opp.height < 50 or used.height < 50:
                    raise ValueError(f"horizon {k}: too few rows with recorded opportunities for the opportunity target")
                self.opp_models[k] = self._new().fit(self.feature_frame(opp).to_numpy(), opp["_opp_pg"].to_numpy().astype(float), sample_weight=self._weights(opp))
                self.eff_models[k] = self._new().fit(self.feature_frame(used).to_numpy(), (used[f"h{k}_fpts"] / used[f"h{k}_opp"]).to_numpy().astype(float), sample_weight=self._weights(used))
            p = self._new().fit(self.feature_frame(played).to_numpy(), y, sample_weight=self._weights(played))
            self.games_models[k], self.ppg_models[k] = g, p
            if self.quantile_sigma:
                Xp, yp = self.feature_frame(played).to_numpy(), played[f"h{k}_ppg"].to_numpy().astype(float)
                lo = XGBRegressor(tree_method="hist", device=self.device, random_state=self.seed, n_jobs=-1,
                                  objective="reg:quantileerror", quantile_alpha=self.quantiles[0], **self.params).fit(Xp, yp)
                hi = XGBRegressor(tree_method="hist", device=self.device, random_state=self.seed, n_jobs=-1,
                                  objective="reg:quantileerror", quantile_alpha=self.quantiles[1], **self.params).fit(Xp, yp)
                self.quantile_models[k] = (lo, hi)
        if self.calibrate == "tier":
            self._fit_tier_adjust(train, as_of_season)
        elif self.calibrate == "games_table":
            self._fit_games_table(train, as_of_season)
        elif self.calibrate:
            self._fit_calibration(train, as_of_season)
        return self

    @staticmethod
    def _bucket_exprs() -> list[pl.Expr]:
        age = pl.col("age_at_season")
        ab = pl.when(age < 24).then(pl.lit("<24")).when(age < 27).then(pl.lit("24-26")).when(age < 30).then(pl.lit("27-29")).otherwise(pl.lit("30+"))
        ppg, games = pl.col("ppg").fill_null(0.0), pl.col("games").fill_null(0)
        tier = pl.when((ppg >= 12) & (games >= 10)).then(pl.lit("starter")).when((ppg >= 8) & (games >= 8)).then(pl.lit("mid")).otherwise(pl.lit("fringe"))
        return [ab.alias("_ab"), tier.alias("_tier")]

    def _fit_games_table(self, train: pl.DataFrame, as_of_season: int | None, min_n: int = 20) -> None:
        """Mean realized games per horizon by (position, age bucket, tier), then (position, tier),
        then (tier), from training rows whose outcome was known by ``as_of_season``."""
        rows = train.with_columns(self._bucket_exprs())
        table: dict[int, dict] = {}
        for k in self.horizons:
            obs = rows.filter(pl.col(f"h{k}_observable"))
            if as_of_season is not None:
                obs = obs.filter((pl.col("season") + k) <= as_of_season)
            obs = obs.with_columns(pl.col(f"h{k}_games").fill_null(0).cast(pl.Float64).alias("_g"))
            lvl3 = {(p_, a, ti): (float(m), int(n)) for p_, a, ti, m, n in
                    obs.group_by(["position", "_ab", "_tier"]).agg(pl.col("_g").mean(), pl.len()).iter_rows() if n >= min_n}
            lvl2 = {(p_, ti): float(m) for p_, ti, m, n in obs.group_by(["position", "_tier"]).agg(pl.col("_g").mean(), pl.len()).iter_rows() if n >= min_n}
            lvl1 = {ti: float(m) for ti, m in obs.group_by("_tier").agg(pl.col("_g").mean()).iter_rows()}
            table[k] = {"l3": lvl3, "l2": lvl2, "l1": lvl1}
        self.games_table = table

    def _apply_games_table(self, df: pl.DataFrame, k: int, games: np.ndarray) -> np.ndarray:
        if not self.games_table or k not in self.games_table:
            return games
        tb = self.games_table[k]
        b = df.with_columns(self._bucket_exprs())
        out = games.copy()
        for i, (p_, a, ti) in enumerate(zip(b["position"].to_list(), b["_ab"].to_list(), b["_tier"].to_list())):
            if self.games_scope == "tiers" and ti == "fringe":
                continue
            v = tb["l3"].get((p_, a, ti))
            v = v[0] if v is not None else tb["l2"].get((p_, ti), tb["l1"].get(ti))
            if v is not None:
                out[i] = self.games_weight * v + (1.0 - self.games_weight) * games[i]
        return np.clip(out, 0.0, MAX_GAMES)

    def _weights(self, rows: pl.DataFrame) -> np.ndarray | None:
        if not self.weight:
            return None
        w = 1.0 + np.clip(rows["ppg"].fill_null(0.0).to_numpy().astype(float), 0.0, None) / 10.0
        return w * w if self.weight == "ppg2" else w

    def _ppg_hat(self, k: int, X: np.ndarray, df: pl.DataFrame) -> np.ndarray:
        if self.stacked:
            raw = self.stacked_models["ppg"].predict(self._stack_X(df, k))
            if self.target == "residual":
                raw = raw + df["ppg"].fill_null(0.0).to_numpy().astype(float)
            return np.clip(raw, 0.0, None)
        if self.target == "opportunity" and k in self.opp_models:
            return np.clip(self.opp_models[k].predict(X), 0.0, None) * np.clip(self.eff_models[k].predict(X), 0.0, None)
        raw = self.ppg_models[k].predict(X)
        if self.target == "residual":
            raw = raw + df["ppg"].fill_null(0.0).to_numpy().astype(float)
        return np.clip(raw, 0.0, None)

    @staticmethod
    def _tier_expr() -> pl.Expr:
        """Prior-season tier from ppg rank within (season, position) among players with >= 8 games."""
        r = pl.when(pl.col("games") >= 8).then(pl.col("ppg")).otherwise(None).rank(descending=True).over(["season", "position"])
        return (pl.when(r <= 5).then(pl.lit("1-5")).when(r <= 12).then(pl.lit("6-12")).when(r <= 24).then(pl.lit("13-24"))
                  .when(r <= 36).then(pl.lit("25-36")).otherwise(pl.lit("37+")))

    def _fit_tier_adjust(self, train: pl.DataFrame, as_of_season: int | None, min_n: int = 15) -> None:
        as_of = as_of_season if as_of_season is not None else last_complete_season(train)
        cut = as_of - self.holdout
        tmp = HorizonModels(self.horizons, self.device, self.seed, **self._kw()).fit(train, as_of_season=cut)
        adj: dict[int, dict[tuple[str, str], float]] = {}
        for k in self.horizons:
            rows = train.filter(pl.col(f"h{k}_observable") & pl.col(f"h{k}_played") & ((pl.col("season") + k) > cut) & ((pl.col("season") + k) <= as_of))
            if rows.height < min_n:
                continue
            pred = tmp.predict(rows).with_columns(self._tier_expr().alias("_tier"), (pl.col(f"h{k}_ppg") - pl.col(f"h{k}_ppg_hat")).alias("_r"))
            g = pred.group_by("position", "_tier").agg(pl.len().alias("n"), pl.col("_r").mean().alias("m")).filter(pl.col("n") >= min_n)
            adj[k] = {(pos, tier): float(m) for pos, tier, _, m in g.iter_rows()}
        self.tier_adjust = adj

    def _apply_tier_adjust(self, df: pl.DataFrame, k: int, ppg: np.ndarray) -> np.ndarray:
        per = (self.tier_adjust or {}).get(k)
        if not per:
            return ppg
        tiers = df.with_columns(self._tier_expr().alias("_tier")).select("position", "_tier").rows()
        delta = np.array([per.get((pos, tier), 0.0) for pos, tier in tiers])
        return np.clip(ppg + delta, 0.0, None)

    def _fit_calibration(self, train: pl.DataFrame, as_of_season: int | None) -> None:
        """Walk-forward recalibration. Temporary models fit as of (as_of - holdout) are scored on
        the holdout seasons' outcomes (rows the temporary models never saw), and per position and
        horizon a line ``realized = a + b * projected`` is fitted for ppg (players who played) and
        for games (everyone; 0 when gone). ``predict`` applies it. Out of sample the tree ensemble
        projects prior top-12 players 10-20 points a season low (WR 6-12 ~30): the slope b > 1
        undoes that shrinkage where the holdout says it exists, position by position."""
        as_of = as_of_season if as_of_season is not None else last_complete_season(train)
        cut = as_of - self.holdout
        tmp = HorizonModels(self.horizons, self.device, self.seed, **self._kw()).fit(train, as_of_season=cut)
        cal: dict[int, dict[str, tuple]] = {}
        for k in self.horizons:
            rows = train.filter(pl.col(f"h{k}_observable") & ((pl.col("season") + k) > cut) & ((pl.col("season") + k) <= as_of))
            if rows.height < 30:
                continue
            pred = tmp.predict(rows)
            per: dict[str, tuple] = {}
            for pos in list(pred["position"].unique().to_list()) + ["__all__"]:
                sub = pred if pos == "__all__" else pred.filter(pl.col("position") == pos)
                played = sub.filter(pl.col(f"h{k}_played"))
                if sub.height < 30 or played.height < 20:
                    continue
                per[pos] = (_line(played[f"h{k}_ppg_hat"].to_numpy(), played[f"h{k}_ppg"].to_numpy()),
                            _line(sub[f"h{k}_games_hat"].to_numpy(), sub[f"h{k}_games"].to_numpy()))
            if "__all__" in per:
                cal[k] = per
        self.calibration = cal

    def _apply_calibration(self, df: pl.DataFrame, k: int, games: np.ndarray, ppg: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        per = (self.calibration or {}).get(k)
        if not per:
            return games, ppg
        pos = df["position"].to_numpy()
        games, ppg = games.copy(), ppg.copy()
        for p in np.unique(pos):
            (ap, bp), (ag, bg) = per.get(p, per["__all__"])
            m = pos == p
            if self.calibrate in ("both", "ppg"):
                ppg[m] = np.clip(ap + bp * ppg[m], 0.0, None)
            if self.calibrate in ("both", "games"):
                games[m] = np.clip(ag + bg * games[m], 0.0, MAX_GAMES)
        return games, ppg

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
        tmp = HorizonModels(self.horizons, self.device, self.seed, **self._kw()).fit(train, as_of_season=cut)
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
        X = self.feature_frame(df).to_numpy()
        cols = []
        sigma = getattr(self, "sigma", None)
        for k in self.horizons:
            games = np.clip((self.stacked_models["games"].predict(self._stack_X(df, k)) if self.stacked else self.games_models[k].predict(X)), 0.0, MAX_GAMES)
            ppg = self._ppg_hat(k, X, df)
            if self.games_table:
                games = self._apply_games_table(df, k, games)
            if self.calibration:
                games, ppg = self._apply_calibration(df, k, games, ppg)
            if self.tier_adjust:
                ppg = self._apply_tier_adjust(df, k, ppg)
            cols += [pl.Series(f"h{k}_games_hat", games), pl.Series(f"h{k}_ppg_hat", ppg),
                     pl.Series(f"h{k}_fpts_hat", games * ppg)]
            if k in self.quantile_models:
                # player-specific spread: half the 16th-84th percentile width of the ppg projection,
                # floored so a projection is never treated as certain; rescaled with the calibration slope
                lo, hi = self.quantile_models[k]
                width = np.clip(hi.predict(X) - lo.predict(X), 0.0, None)
                s_row = np.maximum(width / 2.0, 1.0)
                if self.calibration and k in self.calibration:
                    per = self.calibration[k]
                    slopes = np.array([per.get(p, per["__all__"])[0][1] for p in df["position"].to_list()])
                    s_row = s_row * slopes
                cols.append(pl.Series(f"h{k}_ppg_sigma", s_row))
            elif sigma:
                s = sigma[k]
                cols.append(df["position"].replace_strict(
                    {p: v for p, v in s.items() if p != "__all__"}, default=s["__all__"], return_dtype=pl.Float64
                ).alias(f"h{k}_ppg_sigma"))
        return df.with_columns(cols)


def _line(x: np.ndarray, y: np.ndarray, lo: float = 0.5, hi: float = 2.0) -> tuple[float, float]:
    """Least-squares ``y = a + b x`` with the slope kept in [lo, hi] (a calibration, not a model)."""
    x, y = np.asarray(x, float), np.asarray(y, float)
    if len(x) < 2 or x.std() < 1e-9:
        return (0.0, 1.0)
    b = float(np.cov(x, y, bias=True)[0, 1] / x.var())
    b = min(max(b, lo), hi)
    return (float(y.mean() - b * x.mean()), b)


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

    def __init__(self, tiered: bool = False) -> None:
        # tiered: a curve per (position, prior tier) as well: an elite 31-year-old is held to the elite
        # 31-year-olds' continuation odds, not the roster filler's (the good ones last longer, and the data says so)
        self.coef: dict[str, tuple[float, float]] = {}      # position -> (intercept, slope on age)
        self.tier_coef: dict[tuple[str, str], tuple[float, float]] = {}
        self.tiered = tiered

    @staticmethod
    def tier_expr() -> pl.Expr:
        ppg, games = pl.col("ppg").fill_null(0.0), pl.col("games").fill_null(0)
        return pl.when((ppg >= 12) & (games >= 10)).then(pl.lit("starter")).when((ppg >= 8) & (games >= 8)).then(pl.lit("mid")).otherwise(pl.lit("fringe"))

    def fit(self, season_df: pl.DataFrame) -> "AgeSurvival":
        from sklearn.linear_model import LogisticRegression

        rows = season_df.filter(pl.col("h1_observable") & (pl.col("games") > 0) & pl.col("age_at_season").is_not_null())
        for pos in rows["position"].unique().to_list():
            sub = rows.filter(pl.col("position") == pos)
            if sub.height < 50 or sub["h1_played"].n_unique() < 2:
                continue
            lr = LogisticRegression().fit(sub[["age_at_season"]].to_numpy(), sub["h1_played"].to_numpy().astype(int))
            self.coef[pos] = (float(lr.intercept_[0]), float(lr.coef_[0][0]))
        if self.tiered:
            rows = rows.with_columns(self.tier_expr().alias("_tier"))
            for (pos, tier), sub in rows.group_by(["position", "_tier"]):
                if sub.height < 50 or sub["h1_played"].n_unique() < 2:
                    continue
                lr = LogisticRegression().fit(sub[["age_at_season"]].to_numpy(), sub["h1_played"].to_numpy().astype(int))
                self.tier_coef[(str(pos), str(tier))] = (float(lr.intercept_[0]), float(lr.coef_[0][0]))
        return self

    def p_next(self, position: str, age: np.ndarray, tier: str | None = None) -> np.ndarray:
        c = self.tier_coef.get((position, tier)) if tier is not None else None
        if c is None:
            c = self.coef.get(position)
        if c is None:
            return np.ones_like(np.asarray(age, float))
        b0, b1 = c
        return 1.0 / (1.0 + np.exp(-(b0 + b1 * np.asarray(age, float))))

    def survival(self, position: str, age: np.ndarray, k: int, tier: str | None = None) -> np.ndarray:
        """P(plays in season age+k | plays now) = product of the yearly continuation odds."""
        age = np.asarray(age, float)
        s = np.ones_like(age)
        for j in range(k):
            s = s * self.p_next(position, age + j, tier)
        return s

    def cap_games(self, df: pl.DataFrame, horizons: Iterable[int], age_col: str = "age_at_season",
                  col: str = "h{k}_games_hat", min_age: float | None = None) -> pl.DataFrame:
        """Cap each horizon's projected games at MAX_GAMES x survival (per row's position/age).

        The survival curve is the population's yearly continuation odds (every player, fringe
        included: ~0.8 a year at 23), so an unconditional cap binds on young starters from year two
        (0.8^3 x 17 = 8 games at year three for players who actually play 11). ``min_age`` applies
        the cap only from that age, where the games model has little data and survivorship is the
        real question; None keeps the historical behaviour (every row)."""
        cols = []
        pos = df["position"].to_numpy()
        age = df[age_col].fill_null(27.0).to_numpy().astype(float)
        for k in horizons:
            name = col.format(k=k)
            if name not in df.columns:
                continue
            cap = np.full(df.height, float(MAX_GAMES))
            tiers = df.select(self.tier_expr().alias("_t"))["_t"].to_numpy() if (self.tiered and "ppg" in df.columns) else None
            for p in np.unique(pos):
                m = pos == p
                if tiers is None:
                    cap[m] = MAX_GAMES * self.survival(str(p), age[m], k)
                else:
                    for t_ in np.unique(tiers[m]):
                        mm = m & (tiers == t_)
                        cap[mm] = MAX_GAMES * self.survival(str(p), age[mm], k, str(t_))
            if min_age is not None:
                cap = np.where(age >= min_age, cap, float(MAX_GAMES))
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
