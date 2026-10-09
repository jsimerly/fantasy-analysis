"""How the KTC dynasty market prices players over time, and where it is systematically off (owner,
2026-10-09): seasonality of prices by position and career stage, momentum versus reversion of price
moves, the market's age discount against what players of that age actually delivered, the price
path around injuries, the rookie-pick cycle, and the reaction to single big weeks.

Everything is read from the lake and the ML bucket; nothing is modelled, these are the market's own
regularities, measured. Market-wide drift (KTC rescales its 1-9999 scale, and the whole pool moves
with the calendar) is removed where the question is about relative moves.

Run:  machine_learning/.venv/Scripts/python analysis/market_trends.py --out analysis/_cache/market_trends [--publish]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "machine_learning" / "src"))

import numpy as np  # noqa: E402
import polars as pl  # noqa: E402

import gcs_io  # noqa: E402
import market  # noqa: E402

POS = ["QB", "RB", "WR", "TE"]
MIN_VALUE = 1000          # below this a player is barely priced and the percentages are noise
STAGES = [("rookie", 0, 0), ("yr2-3", 1, 2), ("yr4-6", 3, 5), ("yr7+", 6, 99)]


# ------------------------------------------------------------------------------ data
def season_year(d: pl.Expr) -> pl.Expr:
    """The fantasy season a date belongs to: March to February."""
    return pl.when(d.dt.month() >= 3).then(d.dt.year()).otherwise(d.dt.year() - 1)


def ktc_crosswalk(hist: pl.DataFrame, xw: pl.DataFrame, fps: pl.DataFrame) -> pl.DataFrame:
    """KTC ``player_key`` -> nflverse ``gsis_id``: by id through the players master where it has one
    (a third of the priced pool: the master's gsis_id is sparse), else by normalised name and
    position against the season fact's names, an ambiguous name resolved to the player who played
    most recently. Lifts coverage of the priced pool from about a fifth to nearly all of it."""
    keys = hist.select("player_key", "ktc_name", "ktc_position").unique("player_key")
    names = (fps.filter(pl.col("position").is_in(POS)).group_by("player_id", "player_name", "position").agg(pl.col("season").max().alias("_last"))
                .with_columns(market.norm_name("player_name").alias("_n")).sort("_last", descending=True)
                .unique(["_n", "position"], keep="first", maintain_order=True).select("_n", pl.col("position").alias("ktc_position"), pl.col("player_id").alias("_gsis_name")))
    m = (keys.join(xw.select("player_key", "gsis_id").unique("player_key"), on="player_key", how="left")
             .with_columns(market.norm_name("ktc_name").alias("_n")).join(names, on=["_n", "ktc_position"], how="left")
             .with_columns(pl.coalesce(["gsis_id", "_gsis_name"]).alias("gsis_id"),
                           pl.when(pl.col("gsis_id").is_not_null()).then(pl.lit("id")).when(pl.col("_gsis_name").is_not_null()).then(pl.lit("name")).otherwise(None).alias("match")))
    return m.select("player_key", "gsis_id", "match")


def load_prices() -> pl.DataFrame:
    """KTC dynasty values (SF) per player and date with the player's season context: position, age,
    experience, draft capital and that season's points per game (the fantasy season of the date;
    a rookie before his first game takes his rookie season's row)."""
    h = market.load_ktc_history().filter(pl.col("ktc_value") > 0)
    fps = gcs_io.read_lake("silver/fantasy/fact_player_season/data.parquet").select(
        "player_id", "player_name", "season", "position", "age_at_season", "exp_at_season", "draft_round", "draft_pick", "ppg", "games", "fpts", "rookie_season")
    xw = ktc_crosswalk(h, market.load_crosswalk(), fps)
    print(f"crosswalk: {xw.height} KTC players, by id {xw.filter(pl.col('match') == 'id').height}, by name {xw.filter(pl.col('match') == 'name').height}, unmatched {xw.filter(pl.col('gsis_id').is_null()).height}")
    fps = fps.drop("player_name")
    h = (h.join(xw.select("player_key", "gsis_id"), on="player_key", how="left").with_columns(season_year(pl.col("valuation_date")).alias("season"))
          .filter(pl.col("ktc_position").is_in(POS)))
    j = h.join(fps, left_on=["gsis_id", "season"], right_on=["player_id", "season"], how="left")
    # a rookie priced before his first season: take the row of the season after
    nxt = fps.select(pl.col("player_id"), (pl.col("season") - 1).alias("season"), *[pl.col(c).alias(f"{c}_n") for c in ("position", "age_at_season", "exp_at_season", "draft_round", "draft_pick", "rookie_season")])
    j = j.join(nxt, left_on=["gsis_id", "season"], right_on=["player_id", "season"], how="left")
    j = j.with_columns(
        pl.coalesce([pl.col("position"), pl.col("position_n"), pl.col("ktc_position")]).alias("position"),
        pl.coalesce([pl.col("age_at_season"), pl.col("age_at_season_n") - 1]).alias("age"),
        pl.coalesce([pl.col("exp_at_season"), pl.col("exp_at_season_n") - 1]).alias("exp"),
        pl.coalesce([pl.col("draft_round"), pl.col("draft_round_n")]).alias("draft_round"),
        pl.coalesce([pl.col("draft_pick"), pl.col("draft_pick_n")]).alias("draft_pick"),
    ).drop([c for c in j.columns if c.endswith("_n")])
    stage = pl.when(pl.col("exp") <= 0).then(pl.lit("rookie")).when(pl.col("exp") <= 2).then(pl.lit("yr2-3")).when(pl.col("exp") <= 5).then(pl.lit("yr4-6")).otherwise(pl.lit("yr7+"))
    return (j.with_columns(pl.when(pl.col("exp").is_null()).then(None).otherwise(stage).alias("stage"),
                           pl.col("ktc_value").cast(pl.Float64).log().alias("logv"))
             .sort(["player_key", "valuation_date"]))


def market_index(prices: pl.DataFrame) -> pl.DataFrame:
    """The market as a whole per date: the median log value of players above MIN_VALUE, so a player's
    relative move is his log change minus this index's change (KTC rescales and the pool drifts)."""
    return (prices.filter(pl.col("ktc_value") >= MIN_VALUE).group_by("valuation_date").agg(pl.col("logv").median().alias("idx"), pl.len().alias("n_priced"))
                  .sort("valuation_date"))


def forward_change(prices: pl.DataFrame, days: int, col: str = "fwd") -> pl.DataFrame:
    """Per player and date: the log value change over the next ``days`` days (nearest later date
    within a week of the target), market-adjusted (``{col}_rel``)."""
    idx = market_index(prices).select("valuation_date", "idx")
    p = prices.drop([c for c in ("idx", "idx_right") if c in prices.columns]).join(idx, on="valuation_date", how="left").sort(["player_key", "valuation_date"])
    later = p.select("player_key", (pl.col("valuation_date") - pl.duration(days=days)).alias("valuation_date"),
                     pl.col("logv").alias(f"_logv_{col}"), pl.col("idx").alias(f"_idx_{col}"), pl.col("valuation_date").alias(f"_date_{col}")).sort("valuation_date")
    out = p.sort("valuation_date").join_asof(later.sort("valuation_date"), on="valuation_date", by="player_key", strategy="forward", tolerance="7d")
    return out.with_columns(
        (pl.col(f"_logv_{col}") - pl.col("logv")).alias(col),
        ((pl.col(f"_logv_{col}") - pl.col("logv")) - (pl.col(f"_idx_{col}") - pl.col("idx"))).alias(f"{col}_rel"),
    ).drop([f"_logv_{col}", f"_idx_{col}", f"_date_{col}"])


def backward_change(prices: pl.DataFrame, days: int, col: str = "bwd") -> pl.DataFrame:
    """Per player and date: the market-adjusted log change over the previous ``days`` days."""
    idx = market_index(prices).select("valuation_date", "idx")
    p = prices.drop([c for c in ("idx", "idx_right") if c in prices.columns]).join(idx, on="valuation_date", how="left")
    earlier = p.select("player_key", (pl.col("valuation_date") + pl.duration(days=days)).alias("valuation_date"),
                       pl.col("logv").alias("_logv_b"), pl.col("idx").alias("_idx_b")).sort("valuation_date")
    out = p.sort("valuation_date").join_asof(earlier, on="valuation_date", by="player_key", strategy="backward", tolerance="7d")
    return out.with_columns(((pl.col("logv") - pl.col("_logv_b")) - (pl.col("idx") - pl.col("_idx_b"))).alias(col)).drop(["_logv_b", "_idx_b"])


# ------------------------------------------------------------------------------ analyses
def seasonality(prices: pl.DataFrame, days: int = 30) -> dict:
    """Mean market-adjusted log change over the next ``days`` days by calendar month, for the market
    as a whole (raw, the index itself) and by position and career stage (relative); one observation
    per player per week to keep the daily series from over-weighting itself."""
    f = forward_change(prices, days).filter(pl.col("ktc_value") >= MIN_VALUE)
    f = f.with_columns(pl.col("valuation_date").dt.month().alias("month"), pl.col("valuation_date").dt.week().alias("_wk"), pl.col("valuation_date").dt.year().alias("_yr"))
    f = f.unique(["player_key", "_yr", "_wk"], keep="first", maintain_order=True)
    idx = market_index(prices).with_columns(pl.col("valuation_date").dt.month().alias("month"))
    idx_fwd = forward_change(prices.filter(pl.col("ktc_value") >= MIN_VALUE).group_by("valuation_date").agg(pl.col("logv").median().alias("logv"), pl.lit("__index__").alias("player_key"), pl.lit(10_000).alias("ktc_value")).with_columns(pl.col("player_key").cast(pl.Utf8)), days)
    whole = idx_fwd.with_columns(pl.col("valuation_date").dt.month().alias("month")).group_by("month").agg(pl.len().alias("n"), (pl.col("fwd").mean() * 100).round(2).alias("pct")).sort("month")
    by_pos = f.group_by("month", "position").agg(pl.len().alias("n"), (pl.col("fwd_rel").mean() * 100).round(2).alias("pct")).sort(["position", "month"])
    by_stage = f.filter(pl.col("stage").is_not_null()).group_by("month", "stage").agg(pl.len().alias("n"), (pl.col("fwd_rel").mean() * 100).round(2).alias("pct")).sort(["stage", "month"])
    by_age = f.filter(pl.col("age").is_not_null()).with_columns(pl.when(pl.col("age") < 25).then(pl.lit("<25")).when(pl.col("age") < 29).then(pl.lit("25-28")).otherwise(pl.lit("29+")).alias("age_band")) \
              .group_by("month", "age_band").agg(pl.len().alias("n"), (pl.col("fwd_rel").mean() * 100).round(2).alias("pct")).sort(["age_band", "month"])
    return {"days": days, "whole_market": whole.to_dicts(), "by_position": by_pos.to_dicts(), "by_stage": by_stage.to_dicts(), "by_age": by_age.to_dicts()}


def momentum(prices: pl.DataFrame, back: int = 28, fwd: int = 56) -> dict:
    """Does a price move continue or revert? Market-adjusted change over the previous ``back`` days
    against the change over the next ``fwd`` days: correlation and deciles of the past move with the
    mean subsequent move, by position and overall; one observation per player per week."""
    p = backward_change(prices, back)
    p = forward_change(p, fwd).filter(pl.col("ktc_value") >= MIN_VALUE).drop_nulls(["bwd", "fwd_rel"])
    p = p.with_columns(pl.col("valuation_date").dt.week().alias("_wk"), pl.col("valuation_date").dt.year().alias("_yr")).unique(["player_key", "_yr", "_wk"], keep="first", maintain_order=True)
    out = {"back_days": back, "fwd_days": fwd, "n": p.height}
    out["corr_overall"] = float(np.corrcoef(p["bwd"].to_numpy(), p["fwd_rel"].to_numpy())[0, 1])
    out["corr_by_position"] = {pos: float(np.corrcoef(g["bwd"].to_numpy(), g["fwd_rel"].to_numpy())[0, 1]) for pos, g in ((q, p.filter(pl.col("position") == q)) for q in POS) if g.height > 100}
    dec = p.with_columns(pl.col("bwd").qcut(10, labels=[str(i) for i in range(1, 11)]).alias("decile"))
    out["deciles"] = dec.group_by("decile").agg(pl.len().alias("n"), (pl.col("bwd").mean() * 100).round(1).alias("past_pct"), (pl.col("fwd_rel").mean() * 100).round(2).alias("next_pct")).sort("decile").to_dicts()
    big = p.filter(pl.col("bwd").abs() > 0.15)
    out["big_moves"] = big.with_columns(pl.when(pl.col("bwd") > 0).then(pl.lit("up >15%")).otherwise(pl.lit("down >15%")).alias("move")) \
                          .group_by("move", "position").agg(pl.len().alias("n"), (pl.col("fwd_rel").mean() * 100).round(2).alias("next_pct"), (pl.col("fwd_rel") > 0).mean().round(2).alias("share_up")).sort(["move", "position"]).to_dicts()
    return out


def age_discount(players: pl.DataFrame) -> dict:
    """The market's pricing by age and stage against what the players delivered: on the backtest
    cohorts (every KTC-priced player each February, realized WAR over the next three seasons),
    the market's rank among priced players minus the realized rank (positive = priced too rich),
    and the realized wins per 1,000 KTC, by age band, stage and position."""
    p = players.filter(pl.col("ktc_value").is_not_null() & pl.col("realized_war").is_not_null())
    if "age" not in p.columns and "age_at_season" in p.columns:
        p = p.with_columns(pl.col("age_at_season").alias("age"))
    p = p.with_columns(pl.col("ktc_value").rank(descending=True).over("cohort").alias("_mr"), pl.col("realized_war").rank(descending=True).over("cohort").alias("_rr"),
                       pl.when(pl.col("age") < 24).then(pl.lit("<24")).when(pl.col("age") < 27).then(pl.lit("24-26")).when(pl.col("age") < 30).then(pl.lit("27-29")).otherwise(pl.lit("30+")).alias("age_band"),
                       (pl.col("realized_war") / pl.col("ktc_value") * 1000).alias("wins_per_1k"))
    p = p.with_columns((pl.col("_rr") - pl.col("_mr")).alias("rank_gap"))     # realized rank minus market rank: + = finished worse than priced
    def tbl(by):
        return (p.group_by(by).agg(pl.len().alias("n"), pl.col("rank_gap").mean().round(1).alias("finished_worse_by"), pl.col("wins_per_1k").mean().round(3).alias("wins_per_1k"),
                                   pl.col("ktc_value").mean().round(0).alias("ktc_mean"), pl.col("realized_war").mean().round(2).alias("war_mean")).sort(by).to_dicts())
    return {"cohorts": sorted(p["cohort"].unique().to_list()), "n": p.height, "by_age": tbl("age_band"), "by_position": tbl("position"),
            "by_age_position": tbl(["position", "age_band"]), "by_exp": tbl("exp_band") if "exp_band" in p.columns else []}


def price_per_point(prices: pl.DataFrame, min_games: int = 8) -> dict:
    """What the market pays for a point, by position and season: each player's first September
    price (the preseason snapshot) against the points per game he then scored that season (at least
    ``min_games`` games): the median KTC per point of PPG, how well the preseason price ranked the
    season's scorers (Spearman), and each position's share of all priced value in that snapshot
    (the market's taste over the years). The current, incomplete season is in the composition only."""
    sept = prices.filter((pl.col("valuation_date").dt.month() == 9) & (pl.col("ktc_value") >= MIN_VALUE)).sort("valuation_date")
    snap = sept.group_by(["player_key", "season"]).agg(pl.all().first()).with_columns(pl.col("ktc_value").cast(pl.Float64))
    comp = (snap.group_by(["season", "position"]).agg(pl.len().alias("n_priced"), pl.col("ktc_value").sum().alias("_v"))
                .with_columns((pl.col("_v") / pl.col("_v").sum().over("season") * 100).round(1).alias("value_share")).drop("_v"))
    done = snap.filter(pl.col("ppg").is_not_null() & (pl.col("games") >= min_games))
    done = done.with_columns(pl.col("ktc_value").rank().over(["season", "position"]).alias("_pr"), pl.col("ppg").rank().over(["season", "position"]).alias("_sr"))
    per = (done.group_by(["season", "position"]).agg(pl.len().alias("n"), pl.col("ktc_value").median().round(0).alias("ktc_median"), pl.col("ppg").median().round(2).alias("ppg_median"),
                                                     (pl.col("ktc_value") / pl.col("ppg")).median().round(0).alias("ktc_per_ppg"), pl.corr("_pr", "_sr").round(3).alias("spearman_price_ppg"))
               .filter(pl.col("n") >= 8))
    # the priced pool deepens every year (KTC prices more players), so the medians drift towards lesser players; the top of each
    # position by price (the starters) is the like-for-like read
    top_n = {"QB": 12, "RB": 24, "WR": 36, "TE": 12}
    top = (done.with_columns(pl.col("ktc_value").rank(descending=True).over(["season", "position"]).alias("_top"), pl.col("position").replace_strict(top_n, default=24).alias("_n_top"))
               .filter(pl.col("_top") <= pl.col("_n_top"))
               .group_by(["season", "position"]).agg(pl.len().alias("n_top"), (pl.col("ktc_value") / pl.col("ppg")).median().round(0).alias("ktc_per_ppg_top"), pl.col("ppg").median().round(2).alias("ppg_median_top")))
    rows = comp.join(per, on=["season", "position"], how="left").join(top, on=["season", "position"], how="left").sort(["position", "season"])
    latest = snap.filter(pl.col("season") == snap["season"].max())
    return {"min_games": min_games, "top_n": top_n, "rows": rows.to_dicts(), "seasons": sorted(rows["season"].unique().to_list()), "latest_season": int(latest["season"].max()) if latest.height else None}


def pick_cycle(pick_values: pl.DataFrame) -> dict:
    """A rookie pick's price by months before its draft (round-level KTC, Mid tier): the average
    log value relative to the value one month before the draft, by round."""
    pk = (pick_values.filter((pl.col("source_system") == "ktc") & (pl.col("tier") == "Mid") & (pl.col("qb_format") == "SF") & (pl.col("te_premium") == "Standard"))
                     .select(pl.col("season").cast(pl.Int64), pl.col("round").cast(pl.Int64), pl.col("valuation_date").cast(pl.Date), pl.col("value").cast(pl.Float64)))
    draft = pl.date(pl.col("season"), 5, 1)      # the rookie draft sits around the first of May
    pk = pk.with_columns(((draft - pl.col("valuation_date")).dt.total_days() / 30.4).floor().cast(pl.Int64).alias("months_to_draft"), pl.col("value").log().alias("logv"))
    pk = pk.filter(pl.col("months_to_draft").is_between(0, 30))
    ref = pk.filter(pl.col("months_to_draft") == 1).group_by("season", "round").agg(pl.col("logv").mean().alias("_ref"))
    rel = pk.join(ref, on=["season", "round"], how="inner").with_columns((pl.col("logv") - pl.col("_ref")).alias("rel"))
    out = rel.group_by("round", "months_to_draft").agg(pl.len().alias("n"), (pl.col("rel").mean() * 100).round(1).alias("pct_vs_month_before_draft")).sort(["round", "months_to_draft"])
    return {"rows": out.to_dicts(), "seasons": sorted(pk["season"].unique().to_list())}


def big_weeks(prices: pl.DataFrame, weeks: pl.DataFrame, fwd_short: int = 7, fwd_long: int = 56) -> dict:
    """The market's reaction to one big game: weeks where a player scored 2+ standard deviations
    above his season rate so far (at least four games in), the market-adjusted change in the
    following week and the following eight weeks, by position."""
    w = (weeks.filter(pl.col("season_type") == "REG") if "season_type" in weeks.columns else weeks).sort(["player_id", "season", "week"])
    w = w.with_columns(pl.col("fpts").cum_count().over(["player_id", "season"]).alias("_n"),
                       (pl.col("fpts").cum_sum().over(["player_id", "season"]) - pl.col("fpts")).alias("_prev_sum"))
    w = w.with_columns((pl.col("_prev_sum") / (pl.col("_n") - 1)).alias("_prev_mean"))
    sd = w.group_by(["player_id", "season"]).agg(pl.col("fpts").std().alias("_sd"))
    w = w.join(sd, on=["player_id", "season"]).filter((pl.col("_n") >= 5) & (pl.col("_sd") > 0))
    w = w.with_columns(((pl.col("fpts") - pl.col("_prev_mean")) / pl.col("_sd")).alias("z"))
    big = w.filter(pl.col("z") >= 2.0).select("player_id", "season", "week", pl.col("game_date").cast(pl.Date).alias("game_date"), "position", "z", "fpts")
    xw = prices.select("gsis_id", "player_key").drop_nulls().unique("gsis_id")      # the full crosswalk the prices carry (id, else name + position)
    big = big.join(xw, left_on="player_id", right_on="gsis_id", how="inner")
    p_short = forward_change(prices, fwd_short, "s"); p_long = forward_change(p_short, fwd_long, "l")
    px = p_long.select("player_key", "valuation_date", "ktc_value", "s_rel", "l_rel").sort("valuation_date")
    j = big.sort("game_date").join_asof(px, left_on="game_date", right_on="valuation_date", by="player_key", strategy="forward", tolerance="4d")
    j = j.filter(pl.col("ktc_value") >= MIN_VALUE).drop_nulls(["s_rel"])
    out = j.group_by("position").agg(pl.len().alias("n"), (pl.col("s_rel").mean() * 100).round(2).alias("next_week_pct"), (pl.col("l_rel").mean() * 100).round(2).alias("next_8w_pct"),
                                     ((pl.col("l_rel") - pl.col("s_rel")).mean() * 100).round(2).alias("after_the_pop_pct")).sort("position")
    return {"events": j.height, "by_position": out.to_dicts()}


def injuries(prices: pl.DataFrame, status: pl.DataFrame) -> dict:
    """The price path around an injury absence: the first week a player misses as injured (out or
    reserve) after playing the week before; the market-adjusted change from the last price before
    the miss to one week, four weeks and sixteen weeks after, by injury class and by age band."""
    s = status.sort(["gsis_id", "season", "week"])
    s = s.with_columns(pl.col("status").shift(1).over(["gsis_id", "season"]).alias("_prev"))
    onset = s.filter(pl.col("status").is_in(["injured_out", "injured_reserve"]) & (pl.col("_prev") == "played"))
    xw = prices.select("gsis_id", "player_key").drop_nulls().unique("gsis_id")
    onset = onset.join(xw, on="gsis_id", how="inner")
    # the status table has no dates: the week's first game date comes from the weekly points fact
    wk = gcs_io.read_lake("silver/fantasy/fact_player_week/data.parquet").select("season", "week", pl.col("game_date").cast(pl.Date)).drop_nulls()
    dates = wk.group_by(["season", "week"]).agg(pl.col("game_date").min())
    onset = onset.join(dates, on=["season", "week"], how="inner")
    p1 = forward_change(prices, 7, "w1"); p4 = forward_change(p1, 28, "w4"); p16 = forward_change(p4, 112, "w16")
    px = p16.select("player_key", "valuation_date", "ktc_value", "age", "w1_rel", "w4_rel", "w16_rel").sort("valuation_date")
    j = onset.sort("game_date").join_asof(px, left_on="game_date", right_on="valuation_date", by="player_key", strategy="backward", tolerance="10d")
    j = j.filter(pl.col("ktc_value") >= MIN_VALUE).drop_nulls(["w1_rel"])
    j = j.with_columns(pl.when(pl.col("age") < 25).then(pl.lit("<25")).when(pl.col("age") < 29).then(pl.lit("25-28")).otherwise(pl.lit("29+")).alias("age_band"))
    agg = lambda by: j.group_by(by).agg(pl.len().alias("n"), (pl.col("w1_rel").mean() * 100).round(2).alias("week1_pct"), (pl.col("w4_rel").mean() * 100).round(2).alias("week4_pct"), (pl.col("w16_rel").mean() * 100).round(2).alias("week16_pct")).sort(by).to_dicts()  # noqa: E731
    return {"events": j.height, "by_class": agg("injury_class"), "by_age": agg("age_band"), "by_position": agg("position")}


# ------------------------------------------------------------------------------ main
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--players", default=None, help="a market backtest's players.parquet (per-player realized WAR vs KTC) for the age discount")
    args = ap.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    pl.Config.set_tbl_formatting("ASCII_MARKDOWN"); pl.Config.set_tbl_rows(60); pl.Config.set_tbl_width_chars(200)
    prices = load_prices()
    print(f"prices: {prices.height:,} player-dates, {prices['valuation_date'].min()} -> {prices['valuation_date'].max()}, context on {prices['age'].is_not_null().mean():.0%}")
    summary = {"meta": {"run_date": datetime.now(timezone.utc).date().isoformat(), "from": str(prices["valuation_date"].min()), "through": str(prices["valuation_date"].max()), "min_value": MIN_VALUE}}
    summary["seasonality"] = seasonality(prices)
    print("\n-- seasonality: market-adjusted 30-day change (%) by month and stage")
    print(pl.DataFrame(summary["seasonality"]["by_stage"]).pivot(on="stage", index="month", values="pct").sort("month"))
    print("-- by position"); print(pl.DataFrame(summary["seasonality"]["by_position"]).pivot(on="position", index="month", values="pct").sort("month"))
    print("-- the whole market (raw 30-day change of the median priced player, %)"); print(pl.DataFrame(summary["seasonality"]["whole_market"]))
    summary["momentum"] = momentum(prices)
    print("\n-- momentum: past 28-day move vs next 56-day market-adjusted move"); print("corr", round(summary["momentum"]["corr_overall"], 3), summary["momentum"]["corr_by_position"])
    print(pl.DataFrame(summary["momentum"]["deciles"])); print(pl.DataFrame(summary["momentum"]["big_moves"]))
    if args.players:
        players = pl.read_parquet(args.players)
        summary["age_discount"] = age_discount(players)
        print("\n-- the market by age: realized rank minus market rank (+ = priced too rich), wins per 1,000 KTC")
        print(pl.DataFrame(summary["age_discount"]["by_age"])); print(pl.DataFrame(summary["age_discount"]["by_age_position"]))
    summary["price_per_point"] = price_per_point(prices)
    print("\n-- what the market pays per point: September price vs that season's PPG, by position and season")
    print(pl.DataFrame(summary["price_per_point"]["rows"]))
    summary["pick_cycle"] = pick_cycle(gcs_io.read_lake("silver/fantasy/fact_pick_values"))
    print("\n-- rookie picks: value by months before the draft vs one month before (%)")
    print(pl.DataFrame(summary["pick_cycle"]["rows"]).pivot(on="round", index="months_to_draft", values="pct_vs_month_before_draft").sort("months_to_draft"))
    summary["big_weeks"] = big_weeks(prices, gcs_io.read_lake("silver/fantasy/fact_player_week/data.parquet"))
    print("\n-- one big game (2+ sd above the season rate): market-adjusted change next week and over 8 weeks"); print(pl.DataFrame(summary["big_weeks"]["by_position"]))
    try:
        summary["injuries"] = injuries(prices, gcs_io.read_lake("silver/fantasy/fact_player_week_status/data.parquet"))
        print("\n-- injuries: price change from the last price before the first missed week"); print(pl.DataFrame(summary["injuries"].get("by_class", []))); print(pl.DataFrame(summary["injuries"].get("by_age", [])))
    except Exception as e:  # noqa: BLE001
        summary["injuries"] = {"events": 0, "note": str(e)[:200]}; print("injuries: skipped", str(e)[:120])
    (out / "summary.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    print(f"\nwrote {out}")
    if args.publish:
        print("published", gcs_io.write_ml_json(summary, "backtests", "market_trends", f"run_date={summary['meta']['run_date']}", "summary.json"))


if __name__ == "__main__":
    main()
