"""In-season update model: project from a mid-season snapshot.

A snapshot is (player, season T, through week W). Its features are what was known at that
moment: the player's prior complete seasons (the season-level feature set as of T-1) plus
this season to date (games, per-game rate, usage, recent form, missed weeks). Targets:

  * ``ros_ppg`` / ``ros_games``   -- the rest of season T (weeks > W); 0 games if he never played again
  * ``next_ppg`` / ``next_games`` -- season T+1, 0 games if he did not play (attrition learnt, as in
                                     the career model)

Trained on historical snapshots from every season and every week, so the model learns how far
a 3-week sample should move a projection versus a 10-week one -- which is the question the
market answers emotionally in September.

Leakage rules: a snapshot's to-date features use weeks <= W only; ROS targets exist only for
complete seasons; next-season targets only when T+1 is complete; ``fit(as_of_season)`` trains
only on outcomes known by the end of that season (for a backtest "at week W of T", train on
seasons < T).
"""
from __future__ import annotations

from typing import Iterable

import numpy as np
import polars as pl
from xgboost import XGBRegressor

import career

MAX_GAMES = 17
WEEKS = list(range(2, 17))

# season-level columns (as of T-1) carried as prior-history features
PREV_COLS = ["fpts", "ppg", "games", "lag1_fpts", "lag1_ppg", "lag1_games", "best_ppg",
             "career_seasons", "career_fpts", "career_games", "total_touches", "targets", "pass_yds"]
TD_COLS = ["week", "td_games", "td_fpts", "td_ppg", "td_targets_pg", "td_touches_pg", "td_pass_att_pg",
           "td_target_share", "td_wopr", "last3_ppg", "td_missed", "td_form"]
BIO_COLS = ["age_at_season", "exp_at_season", "draft_round", "draft_pick"]
FLAG_COLS = ["is_undrafted", "is_rookie"]
POSITIONS = ["QB", "RB", "WR", "TE"]
FEATURES = TD_COLS + [f"prev_{c}" for c in PREV_COLS] + BIO_COLS + FLAG_COLS + [f"pos_{p}" for p in POSITIONS]

DEFAULT_PARAMS = {**career.DEFAULT_PARAMS}


# ---------------------------------------------------------------------------- snapshots
def to_date_features(wk: pl.DataFrame, week: int) -> pl.DataFrame:
    """One row per (player, season) with aggregates of weeks <= ``week`` (players with >= 1 game)."""
    sub = wk.filter(pl.col("week") <= week)
    agg = sub.group_by("player_id", "season").agg(
        pl.col("player_name").first(), pl.col("position").first(), pl.col("team").sort_by("week").last(),
        pl.col("age_at_season").first(), pl.col("exp_at_season").first(),
        pl.col("draft_round").first(), pl.col("draft_pick").first(),
        pl.col("is_undrafted").first(), pl.col("is_rookie").first(),
        pl.len().alias("td_games"),
        pl.col("fpts").sum().alias("td_fpts"),
        pl.col("targets").sum().alias("_tg"), pl.col("rush_att").sum().alias("_ra"),
        pl.col("rec").sum().alias("_rc"), pl.col("pass_att").sum().alias("_pa"),
        pl.col("target_share").mean().alias("td_target_share"), pl.col("wopr").mean().alias("td_wopr"),
        pl.col("fpts").sort_by("week").tail(3).mean().alias("last3_ppg"),
        pl.col("game_date").max().alias("td_last_date"),
    )
    return agg.with_columns(
        pl.lit(week).alias("week"),
        (pl.col("td_fpts") / pl.col("td_games")).alias("td_ppg"),
        (pl.col("_tg") / pl.col("td_games")).alias("td_targets_pg"),
        ((pl.col("_ra") + pl.col("_rc")) / pl.col("td_games")).alias("td_touches_pg"),
        (pl.col("_pa") / pl.col("td_games")).alias("td_pass_att_pg"),
        (pl.lit(week) - pl.col("td_games")).alias("td_missed"),
    ).with_columns(
        (pl.col("last3_ppg") - pl.col("td_ppg")).alias("td_form"),      # recent form vs season rate
    ).drop("_tg", "_ra", "_rc", "_pa")


def prior_season_features(season_df: pl.DataFrame) -> pl.DataFrame:
    """Season-level rows re-keyed to the NEXT season so they join a snapshot as 'prior season'."""
    cols = [c for c in PREV_COLS if c in season_df.columns]
    return season_df.select(
        "player_id", (pl.col("season") + 1).alias("season"),
        *[pl.col(c).alias(f"prev_{c}") for c in cols],
    )


def build_snapshots(wk: pl.DataFrame, season_df: pl.DataFrame, weeks: Iterable[int] = WEEKS) -> pl.DataFrame:
    """Stack a snapshot for every (player, season, week): to-date features + prior-season
    features + ROS / next-season targets (null where not observable)."""
    prev = prior_season_features(season_df)
    done = season_df.filter(pl.col("season_complete")) if "season_complete" in season_df.columns else season_df
    complete = set(done["season"].unique().to_list())
    last_complete = max(complete) if complete else int(season_df["season"].max())
    nxt = done.select("player_id", (pl.col("season") - 1).alias("season"),
                      pl.col("fpts").alias("next_fpts"), pl.col("games").alias("next_games"), pl.col("ppg").alias("next_ppg"))
    out = []
    for w in weeks:
        snap = to_date_features(wk, w).join(prev, on=["player_id", "season"], how="left")
        ros = wk.filter(pl.col("week") > w).group_by("player_id", "season").agg(
            pl.len().alias("ros_games"), pl.col("fpts").sum().alias("ros_fpts"))
        snap = snap.join(ros, on=["player_id", "season"], how="left").join(nxt, on=["player_id", "season"], how="left")
        season_done = pl.col("season").is_in(sorted(complete))
        next_done = (pl.col("season") + 1) <= last_complete
        snap = snap.with_columns(
            season_done.alias("ros_observable"),
            pl.when(season_done).then(pl.col("ros_games").fill_null(0)).otherwise(None).cast(pl.Int64).alias("ros_games"),
            pl.when(season_done).then(pl.col("ros_fpts").fill_null(0.0)).otherwise(None).alias("ros_fpts"),
            next_done.alias("next_observable"),
            pl.when(next_done).then(pl.col("next_games").fill_null(0)).otherwise(None).cast(pl.Int64).alias("next_games"),
            pl.when(next_done).then(pl.col("next_fpts").fill_null(0.0)).otherwise(None).alias("next_fpts"),
            pl.when(next_done).then(pl.col("next_ppg")).otherwise(None).alias("next_ppg"),
        ).with_columns(
            pl.when(pl.col("ros_games") > 0).then(pl.col("ros_fpts") / pl.col("ros_games")).otherwise(None).alias("ros_ppg"),
        )
        out.append(snap)
    return pl.concat(out, how="diagonal_relaxed")


def feature_frame(df: pl.DataFrame) -> pl.DataFrame:
    cols = []
    for c in TD_COLS + [f"prev_{c}" for c in PREV_COLS] + BIO_COLS:
        cols.append(pl.col(c).cast(pl.Float64, strict=False) if c in df.columns else pl.lit(None, pl.Float64).alias(c))
    for b in FLAG_COLS:
        cols.append((pl.col(b).cast(pl.Int8) if b in df.columns else pl.lit(0, pl.Int8)).alias(b))
    for p in POSITIONS:
        cols.append((pl.col("position") == p).cast(pl.Int8).alias(f"pos_{p}"))
    return df.select(cols)


# ------------------------------------------------------------------------------ models
class InSeasonModels:
    """Four regressors: ros_ppg (on rows that played again), ros_games, next_ppg (on rows that
    played next year), next_games."""

    def __init__(self, device: str = "cpu", seed: int = 0, **params):
        self.device, self.seed = device, seed
        self.params = {**DEFAULT_PARAMS, **params}
        self.models: dict[str, XGBRegressor] = {}

    def _new(self) -> XGBRegressor:
        return XGBRegressor(tree_method="hist", device=self.device, random_state=self.seed, n_jobs=-1, **self.params)

    def fit(self, snaps: pl.DataFrame, as_of_season: int | None = None) -> "InSeasonModels":
        ros = snaps.filter(pl.col("ros_observable"))
        nxt = snaps.filter(pl.col("next_observable"))
        if as_of_season is not None:
            ros = ros.filter(pl.col("season") < as_of_season)              # season T itself is still playing
            nxt = nxt.filter((pl.col("season") + 1) < as_of_season)        # T+1 outcome known only after it ends
        specs = {
            "ros_games": (ros, "ros_games"), "ros_ppg": (ros.filter(pl.col("ros_games") > 0), "ros_ppg"),
            "next_games": (nxt, "next_games"), "next_ppg": (nxt.filter(pl.col("next_games") > 0), "next_ppg"),
        }
        for name, (rows, target) in specs.items():
            if rows.height == 0:
                raise ValueError(f"{name}: no training outcomes (as_of={as_of_season})")
            self.models[name] = self._new().fit(feature_frame(rows).to_numpy(), rows[target].to_numpy().astype(float))
        return self

    def predict(self, snaps: pl.DataFrame) -> pl.DataFrame:
        X = feature_frame(snaps).to_numpy()
        ros_g = np.clip(self.models["ros_games"].predict(X), 0, MAX_GAMES)
        ros_p = np.clip(self.models["ros_ppg"].predict(X), 0, None)
        nx_g = np.clip(self.models["next_games"].predict(X), 0, MAX_GAMES)
        nx_p = np.clip(self.models["next_ppg"].predict(X), 0, None)
        return snaps.with_columns(
            pl.Series("ros_games_hat", ros_g), pl.Series("ros_ppg_hat", ros_p), pl.Series("ros_fpts_hat", ros_g * ros_p),
            pl.Series("next_games_hat", nx_g), pl.Series("next_ppg_hat", nx_p), pl.Series("next_fpts_hat", nx_g * nx_p),
        )


# ------------------------------------------------------------------- in-season value
def inseason_value(
    snaps: pl.DataFrame, career_pred: pl.DataFrame, rep: dict[str, float],
    sigma: dict[int, dict[str, float]] | None, horizons: Iterable[int], discount: float = 0.8,
) -> pl.DataFrame:
    """Intrinsic value updated mid-season: the rest of THIS season (undiscounted) + next season
    from the in-season model, then seasons T+2.. from the career model's projection off the
    player's latest complete season (``career_pred`` carries ``h{k}_ppg_hat`` / ``h{k}_games_hat``
    keyed by player_id; its h1 is the next season, so k >= 2 are used).

    This is what the dynasty market should be compared with in-season: next-season points alone
    make every productive 38-year-old look cheap against a price that already discounts his
    remaining career.
    """
    import value as _value

    horizons = [k for k in horizons if k >= 2]
    tail = career_pred.select(["player_id"] + [c for k in horizons for c in (f"h{k}_ppg_hat", f"h{k}_games_hat")])
    df = snaps.join(tail, on="player_id", how="left")
    rep_arr = df["position"].replace_strict(rep, default=0.0, return_dtype=pl.Float64).to_numpy()

    def excess(mu_col: str, k: int) -> np.ndarray:
        mu = df[mu_col].fill_null(0.0).to_numpy().astype(float)
        if sigma and k in sigma:
            s = df["position"].replace_strict({p: v for p, v in sigma[k].items() if p != "__all__"},
                                              default=sigma[k]["__all__"], return_dtype=pl.Float64).to_numpy()
            return _value.expected_excess(mu, s, rep_arr)
        return np.maximum(mu - rep_arr, 0.0)

    ros = excess("ros_ppg_hat", 1) * df["ros_games_hat"].to_numpy()
    nxt = excess("next_ppg_hat", 1) * df["next_games_hat"].to_numpy()
    cols = [pl.Series("vorp_ros", ros), pl.Series("vorp_next", nxt)]
    total = ros + discount * nxt
    for k in horizons:
        v = excess(f"h{k}_ppg_hat", k) * df[f"h{k}_games_hat"].fill_null(0.0).to_numpy()
        cols.append(pl.Series(f"h{k}_vorp_hat", v))
        total = total + (discount ** k) * v
    cols.append(pl.Series("iv_inseason", total))
    return df.with_columns(cols)


# ---------------------------------------------------------------------------- baselines
def baselines(snaps: pl.DataFrame) -> pl.DataFrame:
    """What a reactive manager and a stubborn one would do: this season to date, last season
    only, and a 50/50 of the two (per game)."""
    return snaps.with_columns(
        pl.col("td_ppg").alias("bl_todate_ppg"),
        pl.col("prev_ppg").fill_null(0.0).alias("bl_prior_ppg"),
        ((pl.col("td_ppg") + pl.col("prev_ppg").fill_null(pl.col("td_ppg"))) / 2).alias("bl_blend_ppg"),
    )
