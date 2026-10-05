"""The leagues' trading, scored on value (machine_learning/BACKLOG.md item 25): who trades well, the
worst trades, where managers slip. Analysis, not a model: lives in analysis/ with the notebooks.

Every trade side is priced with KTC's trade calculator (the combine, so a two-for-one is judged
the way KTC judges it) at the trade date (N: the market's verdict at the time) and again with
the same assets at N+1, N+2, N+3 years and today (what the market later thought of each
package; a pick becomes the rookie it turned into). Wins delivered since are a secondary column.

Prints per-manager scorecards per league, the best and worst trades by value at ``--horizon``
years, and the patterns (does the verdict at the time hold up later; picks vs players;
consolidation; timing; positions; head-to-head). ``--manager NAME`` lists one manager's trades.
``--publish`` writes the summary to the ML bucket (``backtests/trades/run_date=<today>/summary.json``)
for the page's Trades tab.

Run with the root venv:
  .venv/Scripts/python analysis/trade_report.py --rebuild --out analysis/_cache/trades --min-seasons 1 --manager "Jacob Simerly" --publish
  .venv/Scripts/python -m pytest analysis/tests -q
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "machine_learning" / "src"))

import polars as pl  # noqa: E402

import gcs_io  # noqa: E402
import trades  # noqa: E402

H = ["v0", "v1", "v2", "v3", "v_now"]


def summarise(df: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    """Per group: sides, the verdict at the time (mean net at N, combined, and the fair share), the
    package values later (mean net at N+1 / N+2 / today, share won), and the wins (secondary)."""
    aggs = [pl.len().alias("sides"), pl.col("net_v0").mean().alias("net_v0_mean"), pl.col("fair_v0").mean().alias("fair_v0_mean")]
    for c in ("v1", "v2", "v_now"):
        aggs += [pl.col(f"net_{c}").mean().alias(f"net_{c}_mean"), (pl.col(f"net_{c}") > 0).sum().alias(f"_w_{c}"), pl.col(f"net_{c}").is_not_null().sum().alias(f"n_{c}")]
    aggs += [pl.col("net_wins").mean().alias("net_wins_mean"), (pl.col("net_wins") > 0.05).mean().alias("wins_won")]
    out = df.group_by(keys, maintain_order=True).agg(aggs)
    return out.with_columns([(pl.col(f"_w_{c}") / pl.col(f"n_{c}").clip(lower_bound=1)).alias(f"won_{c}") for c in ("v1", "v2", "v_now")]).drop([f"_w_{c}" for c in ("v1", "v2", "v_now")]).sort(keys)


def patterns(tt: pl.DataFrame, legs: pl.DataFrame, min_seasons: float) -> dict[str, pl.DataFrame]:
    old = tt.filter(pl.col("seasons_since") >= min_seasons)
    two = old.filter(pl.col("n_teams") == 2)
    out = {}
    # the calculator's verdict at the time vs what the market said later and what the players delivered
    v = two.filter(pl.col("recv_v0").is_not_null() & pl.col("give_v0").is_not_null()).with_columns(
        pl.when(pl.col("fair_v0") > 0.05).then(pl.lit("favoured by the calculator")).when(pl.col("fair_v0") < -0.05).then(pl.lit("disfavoured")).otherwise(pl.lit("called even")).alias("verdict"))
    out["verdict"] = summarise(v, ["verdict"])
    out["timing"] = summarise(two.with_columns(pl.when(pl.col("in_season")).then(pl.lit("in-season")).otherwise(pl.lit("offseason")).alias("when")), ["when"])
    pk = two.with_columns(pl.when((pl.col("recv_picks") > 0) & (pl.col("give_picks") == 0)).then(pl.lit("took picks for players"))
                          .when((pl.col("give_picks") > 0) & (pl.col("recv_picks") == 0)).then(pl.lit("gave picks for players")).otherwise(pl.lit("mixed / players only")).alias("pick_side"))
    out["picks"] = summarise(pk, ["pick_side"])
    best = legs.filter(pl.col("v0").is_not_null()).sort("v0", descending=True).unique("transaction_id", keep="first").select("transaction_id", pl.col("roster_id").alias("best_to"))
    c = two.join(best, on="transaction_id", how="inner").with_columns(pl.when(pl.col("roster_id") == pl.col("best_to")).then(pl.lit("got the best asset")).otherwise(pl.lit("gave the best asset")).alias("side"))
    out["consolidation"] = summarise(c, ["side"])
    # players bought, by position: what was paid, what the player was worth a year and two later, wins per 1,000 paid
    pl_legs = legs.filter((pl.col("asset") == "player") & (pl.col("v0") > 0) & (pl.col("seasons_since") >= min_seasons))
    out["by_position_bought"] = (pl_legs.group_by("pos").agg(
        pl.len().alias("legs"), pl.col("v0").mean().alias("paid_mean"),
        (pl.col("v1") / pl.col("v0")).mean().alias("value_kept_1y"), (pl.col("v2") / pl.col("v0")).mean().alias("value_kept_2y"),
        pl.col("wins_since").mean().alias("wins_mean"), (pl.col("wins_since").sum() / pl.col("v0").sum() * 1000).alias("wins_per_1k")).sort("pos"))
    # picks bought: what was paid for the pick and what the rookie was worth a year and two after the trade
    pk_legs = legs.filter((pl.col("asset") == "pick") & (pl.col("v0") > 0) & (pl.col("seasons_since") >= min_seasons))
    out["picks_bought_by_round"] = (pk_legs.group_by("pick_round").agg(
        pl.len().alias("legs"), pl.col("v0").mean().alias("paid_mean"), (pl.col("v1") / pl.col("v0")).mean().alias("value_kept_1y"),
        (pl.col("v2") / pl.col("v0")).mean().alias("value_kept_2y"), pl.col("wins_since").mean().alias("wins_mean")).sort("pick_round"))
    other = two.select("transaction_id", pl.col("roster_id").alias("other_roster"), pl.col("manager").alias("partner"))
    p = two.join(other, on="transaction_id", how="inner").filter(pl.col("roster_id") != pl.col("other_roster"))
    out["partners"] = (p.group_by(["league_name", "manager", "partner"]).agg(
        pl.len().alias("trades"), pl.col("net_v0").sum().alias("net_v0"), pl.col("net_v2").sum().alias("net_v2"), pl.col("net_v_now").sum().alias("net_v_now"),
        (pl.col("net_v2") > 0).mean().alias("won_v2"), pl.col("net_wins").sum().alias("net_wins")).sort(["league_name", "manager", "net_v2"]))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--rebuild", action="store_true", help="rebuild from the lake even if --out has the parquet files")
    ap.add_argument("--min-seasons", type=float, default=1.0, help="a trade must be this old (seasons) to count in the scored tables")
    ap.add_argument("--horizon", default="v2", choices=H, help="which horizon ranks the best / worst trades")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--manager", help="also print every trade of this manager in full")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    if args.rebuild or not (out / "trade_table.parquet").exists():
        R = trades.build()
        for k, v in R.items():
            v.write_parquet(out / f"{k}.parquet")
    else:
        R = {k: pl.read_parquet(out / f"{k}.parquet") for k in ("trades", "legs", "trade_table", "managers", "replacement")}
    tt, legs = R["trade_table"], R["legs"]
    scored = tt.filter(pl.col("seasons_since") >= args.min_seasons)
    managers = trades.manager_table(tt, min_seasons=args.min_seasons)
    hz = args.horizon
    cols = ["league_name", "date", "manager", "recv_assets", "give_assets", "recv_v0_sum", "give_v0_sum", "net_v0", "fair_v0", "net_v1", "net_v2", "net_v_now", "net_wins", "seasons_since"]
    ranked = scored.filter(pl.col(f"net_{hz}").is_not_null())
    worst = ranked.sort(f"net_{hz}").head(15).select(cols)
    best = ranked.sort(f"net_{hz}", descending=True).head(15).select(cols)
    pat = patterns(tt, legs, args.min_seasons)
    with pl.Config(tbl_rows=80, tbl_cols=-1, tbl_width_chars=320, float_precision=2, tbl_hide_dataframe_shape=True, tbl_hide_column_data_types=True, fmt_str_lengths=80):
        npk = legs.filter(pl.col("asset") == "pick")
        print(f"trades {R['trades'].height}, legs {legs.height} (players {legs.filter(pl.col('asset') == 'player').height}, picks {npk.height}, "
              f"resolved {npk['drafted_player_id'].is_not_null().sum()}); priced at N: {(legs['v0'] > 0).sum()} of {legs.height}; "
              f"sides scored at >= {args.min_seasons} seasons: {scored.height} of {tt.height}")
        print("\n-- managers (trades at least %.0f season(s) old): the calculator's verdict at N, the packages later, wins since" % args.min_seasons)
        print(managers.select("league_name", "manager", "trades", "in_season_share", "net_v0_per_trade", "won_v0", "net_v1", "won_v1", "net_v2", "won_v2", "net_v_now", "won_v_now", "net_wins", "wins_won", "net_picks"))
        print(f"\n-- worst trades by value at {hz} (the side that lost)")
        print(worst)
        print(f"\n-- best trades by value at {hz}")
        print(best)
        for k, v in pat.items():
            print(f"\n-- {k}")
            print(v if k != "partners" else v.filter(pl.col("trades") >= 3).sort("net_v2").head(12))
        if args.manager:
            mine = tt.filter(pl.col("manager") == args.manager).sort("date")
            print(f"\n-- every trade of {args.manager} ({mine.height} sides)")
            print(mine.select("league_name", "date", "leg", "recv_assets", "give_assets", "recv_v0_sum", "give_v0_sum", "net_v0", "fair_v0", "net_v1", "net_v2", "net_v3", "net_v_now", "net_wins", "seasons_since"))
    # the full log, one row per trade side, for the page (sortable, filterable client-side)
    others = tt.select("transaction_id", "roster_id", "manager")
    partners = (others.join(others.rename({"roster_id": "r2", "manager": "m2"}), on="transaction_id", how="inner")
                .filter(pl.col("roster_id") != pl.col("r2")).group_by(["transaction_id", "roster_id"]).agg(pl.col("m2").sort().str.join(" & ").alias("partners")))
    log_cols = (["transaction_id", "league_name", "season", "date", "leg", "in_season", "n_teams", "roster_id", "manager", "partners", "recv_assets", "give_assets",
                 "recv_n", "give_n", "recv_picks", "give_picks", "recv_v0_sum", "give_v0_sum", "fair_v0"]
                + [f"{s}_{c}" for c in H for s in ("recv", "give")] + ["recv_wins", "give_wins", "seasons_since"])
    log = tt.join(partners, on=["transaction_id", "roster_id"], how="left").select([c for c in log_cols if c in tt.columns or c == "partners"])
    log = log.with_columns([pl.col(c).round(3) for c, d in zip(log.columns, log.dtypes) if d == pl.Float64])
    summary = {"managers": managers.to_dicts(), "worst": worst.to_dicts(), "best": best.to_dicts(), **{k: v.to_dicts() for k, v in pat.items()},
               "trade_log": log.sort("date", descending=True).to_dicts(),
               "meta": {"n_trades": R["trades"].height, "n_legs": legs.height, "min_seasons": args.min_seasons, "horizons": H, "rank_horizon": hz,
                        "through": str(legs["date"].max()), "run_date": datetime.now(timezone.utc).date().isoformat()}}
    (out / "summary.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    print(f"\nwrote {out}")
    if args.publish:
        print("published", gcs_io.write_ml_json(summary, "backtests", "trades", f"run_date={summary['meta']['run_date']}", "summary.json"))


if __name__ == "__main__":
    main()
