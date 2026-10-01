"""Backtest the in-season update model by week, against the market.

For each cohort season T and checkpoint week W:
  * train the in-season model on snapshots from seasons < T (ROS outcomes) and < T-1
    (next-season outcomes), and the career model as of T-1 for the seasons beyond next,
  * project every player with a game by week W of T: rest-of-season ppg, next-season points,
    and the in-season intrinsic value (ROS + next + career tail, discounted),
  * take KTC's superflex value the day after week W's games (k_W) and at Feb 15 of T+1 (k_end),
and report, on the players KTC priced at the time:
  1. skill: rank correlation with what actually happened, model vs market vs naive baselines
     (last season only, this season to date, 50/50 blend);
  2. market lag: does the gap between the in-season value and the market at week W predict
     where the market moves by season's end? (mispricing at W vs log(k_end / k_W)).

Usage (from machine_learning/):
    uv run python scripts/backtest_inseason.py [--weeks 3,6,9,13] [--first-cohort 2021] [--device cpu]
    uv run python scripts/backtest_inseason.py --current          # project the in-progress season now
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import numpy as np  # noqa: E402
import polars as pl  # noqa: E402

import career  # noqa: E402
import features  # noqa: E402
import gcs_io  # noqa: E402
import inseason  # noqa: E402
import market  # noqa: E402
import replacement  # noqa: E402
import value  # noqa: E402

WEEK_PATH = "silver/fantasy/fact_player_week/data.parquet"
SETTINGS_PATH = "silver/fantasy/dim_league_settings/data.parquet"
H = list(range(1, 11))


def load_inputs() -> tuple[pl.DataFrame, pl.DataFrame]:
    wk = gcs_io.read_lake(WEEK_PATH)
    season = career.attach_horizon_targets(
        career.career_features(features.attach_lags_and_target(features.load_fact_player_season(), drop_no_target=False)), H)
    return wk, season


def week_end_date(wk: pl.DataFrame, season: int, week: int) -> date:
    d = wk.filter((pl.col("season") == season) & (pl.col("week") == week))["game_date"].max()
    return (d + timedelta(days=1)) if d is not None else date(season, 9, 1) + timedelta(weeks=week)


def career_tail(season_df: pl.DataFrame, as_of: int, rep: dict, device: str):
    """Career-model projections off every player's row in season ``as_of`` (their latest
    complete season), plus the out-of-sample spread, trained only on outcomes known by then.
    Projected games are capped by the population age-survival prior (survivorship at the
    oldest ages). Returns (predictions, sigma, survival)."""
    m = career.HorizonModels(H, device=device).fit(season_df, as_of_season=as_of)
    sigma = m.estimate_sigma(season_df, as_of_season=as_of)
    survival = career.AgeSurvival().fit(season_df.filter((pl.col("season") + 1) <= as_of))
    pred = survival.cap_games(m.predict(season_df.filter(pl.col("season") == as_of)), H)
    return pred, sigma, survival


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--weeks", default="3,6,9,13")
    ap.add_argument("--first-cohort", type=int, default=2021)
    ap.add_argument("--discount", type=float, default=value.DEFAULT_DISCOUNT)
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--current", action="store_true", help="train on everything and project the in-progress season")
    args = ap.parse_args()
    weeks = [int(w) for w in args.weeks.split(",")]

    wk, season = load_inputs()
    snaps = inseason.baselines(inseason.build_snapshots(wk, season))
    last_complete = career.last_complete_season(season)
    hist, xw = market.load_ktc_history(), market.load_crosswalk()
    slots, teams = replacement.league_lineup(gcs_io.read_lake(SETTINGS_PATH))
    starters = replacement.starters_per_position(slots, teams)
    print(f"snapshots: {snaps.shape} (seasons {snaps['season'].min()}..{snaps['season'].max()}, weeks {inseason.WEEKS[0]}..{inseason.WEEKS[-1]}); last complete season {last_complete}")

    if args.current:
        cur = int(wk["season"].max())
        w_now = int(wk.filter(pl.col("season") == cur)["week"].max())
        rep = replacement.replacement_levels(season, starters)
        tail, sigma, survival = career_tail(season, last_complete, rep, args.device)
        m = inseason.InSeasonModels(device=args.device).fit(snaps)
        snap = m.predict(snaps.filter((pl.col("season") == cur) & (pl.col("week") == w_now)))
        snap = survival.cap_games(snap, [1], col="next_games_hat")
        snap = inseason.inseason_value(snap, tail, rep, sigma, H, args.discount)
        snap = market.attach_market(snap, datetime.now(timezone.utc).date(), hist, xw)
        # preseason view for the same players: the career model's IV off their 2025 row
        pre = value.intrinsic_value(tail, rep, H, args.discount).select("player_id", pl.col("iv").alias("iv_preseason"))
        snap = snap.join(pre, on="player_id", how="left")
        cmp, summary = value.compare_to_market(snap, iv_col="iv_inseason")
        pre_rank = snap.filter(pl.col("ktc_value").is_not_null()).with_columns(
            pl.col("iv_preseason").rank(method="ordinal", descending=True).cast(pl.Int64).alias("pre_rank")).select("player_id", "pre_rank")
        cmp = cmp.join(pre_rank, on="player_id", how="left").with_columns((pl.col("pre_rank") - pl.col("iv_rank")).alias("moved_up"))
        print(f"\n== {cur} through week {w_now}: {snap.height} players projected, {summary['n']} priced by KTC; "
              f"spearman(in-season IV, KTC) = {summary['spearman']:.3f}")
        show = ["player_name", "position", "age", "td_ppg", "prev_ppg", "ros_ppg_hat", "next_fpts_hat", "iv_inseason", "ktc_value", "fair_value", "mispricing_pct", "market_rank", "iv_rank", "moved_up"]
        v = cmp.with_columns(pl.col("age_at_season").round(0).alias("age"), pl.col("td_ppg").round(1), pl.col("prev_ppg").round(1),
                             pl.col("ros_ppg_hat").round(1), pl.col("next_fpts_hat").round(0), pl.col("iv_inseason").round(0),
                             pl.col("fair_value").round(0), pl.col("mispricing_pct").round(2))
        with pl.Config(tbl_rows=-1, tbl_width_chars=200, fmt_str_lengths=22):
            liquid = v.filter(pl.col("ktc_value") >= 1500)
            print("\nMarket CHEAPEST vs in-season intrinsic value (KTC >= 1500):"); print(liquid.sort("mispricing_pct").select(show).head(20))
            print("\nMarket RICHEST vs in-season intrinsic value:"); print(liquid.sort("mispricing_pct", descending=True).select(show).head(20))
            movers = liquid.filter(pl.col("moved_up").is_not_null())      # rookies have no preseason rank
            print("\nBiggest movers since preseason in the model's own ranking (moved_up = preseason IV rank - in-season IV rank):")
            print(movers.sort("moved_up", descending=True).select(show).head(10)); print(movers.sort("moved_up").select(show).head(10))
        run = datetime.now(timezone.utc).date().isoformat()
        p = gcs_io.write_ml_parquet(snap.join(cmp.select("player_id", "fair_value", "mispricing", "mispricing_pct", "iv_rank", "market_rank"), on="player_id", how="left"),
                                    "inseason", f"season={cur}", f"week={w_now}", f"run_date={run}", "projections.parquet")
        print("\nwrote", p)
        return

    rows, lag_rows = [], []
    for T in range(args.first_cohort, last_complete):               # next-season outcome must be complete
        rep = replacement.replacement_levels(season, starters, seasons=list(range(T - 5, T)))
        tail, sigma, survival = career_tail(season, T - 1, rep, args.device)
        m = inseason.InSeasonModels(device=args.device).fit(snaps, as_of_season=T)
        k_end = market.ktc_as_of(hist, date(T + 1, 2, 15)).select("player_key", pl.col("ktc_value").alias("k_end"))
        for W in weeks:
            snap = m.predict(snaps.filter((pl.col("season") == T) & (pl.col("week") == W)))
            snap = survival.cap_games(snap, [1], col="next_games_hat")
            snap = snap.with_columns((pl.col("next_games_hat") * pl.col("next_ppg_hat")).alias("next_fpts_hat"))
            snap = inseason.inseason_value(snap, tail, rep, sigma, H, args.discount)
            snap = market.attach_market(snap, week_end_date(wk, T, W), hist, xw).join(k_end, on="player_key", how="left")
            priced = snap.filter(pl.col("ktc_value").is_not_null() & pl.col("next_observable"))
            nx = priced["next_fpts"].to_numpy().astype(float)
            r = {"T": T, "W": W, "n": priced.height,
                 "next|model": value.spearman(priced["next_fpts_hat"].to_numpy(), nx),
                 "next|iv_inseason": value.spearman(priced["iv_inseason"].to_numpy(), nx),
                 "next|ktc": value.spearman(priced["ktc_value"].to_numpy(), nx),
                 "next|last_season": value.spearman(priced["bl_prior_ppg"].to_numpy(), nx),
                 "next|to_date": value.spearman(priced["bl_todate_ppg"].to_numpy(), nx),
                 "next|blend": value.spearman(priced["bl_blend_ppg"].to_numpy(), nx)}
            played = priced.filter(pl.col("ros_games") > 0)
            rp = played["ros_ppg"].to_numpy().astype(float)
            r.update({"ros|model": value.spearman(played["ros_ppg_hat"].to_numpy(), rp),
                      "ros|ktc": value.spearman(played["ktc_value"].to_numpy(), rp),
                      "ros|last_season": value.spearman(played["bl_prior_ppg"].to_numpy(), rp),
                      "ros|to_date": value.spearman(played["bl_todate_ppg"].to_numpy(), rp),
                      "ros|blend": value.spearman(played["bl_blend_ppg"].to_numpy(), rp)})
            rows.append(r)

            lag = priced.filter(pl.col("k_end").is_not_null() & (pl.col("ktc_value") >= 1000))
            for label, col in (("iv", "iv_inseason"), ("next_pts", "next_fpts_hat")):
                cmp, _ = value.compare_to_market(lag, iv_col=col)
                cmp = cmp.with_columns(
                    (pl.col("k_end").log() - pl.col("ktc_value").log()).alias("move"),
                    pl.col("mispricing_pct").qcut(3, labels=["cheap", "fair", "rich"]).alias("tercile"),
                    (pl.col("bl_todate_ppg").rank() - pl.col("ktc_value").rank()).alias("hot_start_gap"),
                )
                lag_rows.append({"T": T, "W": W, "signal": label, "n": cmp.height,
                                 "spearman(mispricing, move)": value.spearman(cmp["mispricing_pct"].to_numpy(), cmp["move"].to_numpy()),
                                 "spearman(hot_start, move)": value.spearman(cmp["hot_start_gap"].to_numpy(), cmp["move"].to_numpy()),
                                 **{f"move|{t}": float(np.expm1(cmp.filter(pl.col("tercile") == t)["move"].mean())) for t in ["cheap", "fair", "rich"]}})

    res = pl.DataFrame(rows)
    with pl.Config(tbl_rows=-1, float_precision=3, tbl_width_chars=220):
        print("\n== Rank correlation with NEXT-season points, by checkpoint week (mean over cohorts "
              f"{args.first_cohort}-{last_complete - 1}; players KTC priced that week) ==")
        print(res.group_by("W").agg(pl.col("n").sum(), pl.col("^next\\|.*$").mean()).sort("W"))
        print("\n== Rank correlation with REST-OF-SEASON ppg (players who played again) ==")
        print(res.group_by("W").agg(pl.col("n").sum(), pl.col("^ros\\|.*$").mean()).sort("W"))
        lagdf = pl.DataFrame(lag_rows)
        print("\n== Does the market LAG fundamentals? (KTC >= 1000 at week W; move = KTC change from week W to Feb 15 of T+1) ==")
        print("spearman(mispricing at W, move) < 0: players the market priced cheap vs the model subsequently rose. "
              "move|tercile = mean subsequent KTC change for that third (0.05 = +5%).")
        print(lagdf.group_by("signal", "W").agg(pl.col("n").sum(), pl.col("^spearman.*$").mean(), pl.col("^move\\|.*$").mean()).sort("signal", "W"))
        print("\n== per cohort x week (in-season IV signal) ==")
        print(lagdf.filter(pl.col("signal") == "iv").sort("T", "W"))


if __name__ == "__main__":
    main()
