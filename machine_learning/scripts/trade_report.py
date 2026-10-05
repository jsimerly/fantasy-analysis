"""The leagues' trading, scored (BACKLOG item 25): who trades well, the worst trades, where managers slip.

Builds (or loads) the trade tables from ``src/trades.py`` and prints:
  * per-manager scorecards per league (value given vs received at the time in KTC, wins above
    replacement delivered since by each side, net wins, win rate, pick balance);
  * the best and worst trades of all time by realized wins (both sides), with the KTC verdict at
    the time, restricted to trades at least ``--min-seasons`` old;
  * league-wide patterns: in-season vs offseason, the side that took picks vs players, the side
    that took the single best asset (consolidation), whether KTC's verdict at the time predicted
    the realized winner, and each manager's record against each partner.

Writes ``trades.parquet``, ``legs.parquet``, ``trade_table.parquet``, ``managers.parquet`` and
``summary.json`` to ``--out``. ``--publish`` writes the summary to the ML bucket
(``backtests/trades/run_date=<today>/summary.json``) for the page.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import polars as pl  # noqa: E402

import gcs_io  # noqa: E402
import trades  # noqa: E402

PAIR = ["recv_ktc_then", "give_ktc_then", "recv_wins", "give_wins", "net_wins", "net_ktc_then", "net_ktc_now"]


def patterns(tt: pl.DataFrame, legs: pl.DataFrame, min_seasons: float) -> dict[str, pl.DataFrame]:
    old = tt.filter(pl.col("seasons_since") >= min_seasons)
    two = old.filter(pl.col("n_teams") == 2)
    out = {}
    # the KTC verdict at the time vs the realized winner (two-team trades, both sides priced)
    v = two.filter((pl.col("recv_ktc_then") > 0) & (pl.col("give_ktc_then") > 0)).with_columns(
        (pl.col("net_ktc_then") > 0).alias("ktc_favoured"), (pl.col("net_wins") > 0.05).alias("won"), (pl.col("net_wins") < -0.05).alias("lost"))
    out["ktc_verdict"] = v.group_by("ktc_favoured").agg(pl.len().alias("sides"), pl.col("won").mean().alias("win_rate"), pl.col("lost").mean().alias("loss_rate"),
                                                       pl.col("net_wins").mean().alias("net_wins_mean"), pl.col("net_ktc_then").mean().alias("net_ktc_mean")).sort("ktc_favoured")
    # in-season vs offseason, from the side that made the trade bigger (received more KTC)
    out["timing"] = two.group_by("in_season").agg(pl.len().alias("sides"), (pl.col("net_wins") > 0.05).mean().alias("win_rate"), pl.col("net_wins").abs().mean().alias("swing_mean")).sort("in_season")
    # picks vs players: the side that received picks and gave players
    pk = two.with_columns(pl.when((pl.col("recv_picks") > 0) & (pl.col("give_picks") == 0)).then(pl.lit("took picks"))
                          .when((pl.col("give_picks") > 0) & (pl.col("recv_picks") == 0)).then(pl.lit("gave picks")).otherwise(pl.lit("mixed / none")).alias("pick_side"))
    out["picks"] = pk.group_by("pick_side").agg(pl.len().alias("sides"), (pl.col("net_wins") > 0.05).mean().alias("win_rate"), (pl.col("net_wins") < -0.05).mean().alias("loss_rate"),
                                                 pl.col("net_wins").mean().alias("net_wins_mean"), pl.col("net_ktc_then").mean().alias("net_ktc_mean")).sort("pick_side")
    # consolidation: the side that received the single most valuable asset (by KTC then)
    best = legs.filter(pl.col("ktc_then").is_not_null()).sort("ktc_then", descending=True).unique("transaction_id", keep="first").select("transaction_id", pl.col("roster_id").alias("best_to"), pl.col("ktc_then").alias("best_ktc"))
    c = two.join(best, on="transaction_id", how="inner").with_columns((pl.col("roster_id") == pl.col("best_to")).alias("got_best"))
    out["consolidation"] = c.group_by("got_best").agg(pl.len().alias("sides"), (pl.col("net_wins") > 0.05).mean().alias("win_rate"), (pl.col("net_wins") < -0.05).mean().alias("loss_rate"),
                                                      pl.col("net_wins").mean().alias("net_wins_mean"), pl.col("net_ktc_then").mean().alias("net_ktc_mean")).sort("got_best")
    # positions: what each manager buys and sells (legs), and the realized return per 1,000 KTC by position bought
    pl_legs = legs.filter((pl.col("asset") == "player") & pl.col("ktc_then").is_not_null() & (pl.col("seasons_since") >= min_seasons))
    out["by_position_bought"] = pl_legs.group_by("pos").agg(pl.len().alias("legs"), pl.col("ktc_then").mean().alias("ktc_mean"), pl.col("wins_since").mean().alias("wins_mean"),
                                                            (pl.col("wins_since").sum() / pl.col("ktc_then").sum() * 1000).alias("wins_per_1k")).sort("pos")
    # partners: each manager's record against each counterparty (two-team trades)
    other = two.select("transaction_id", pl.col("roster_id").alias("other_roster"), pl.col("manager").alias("partner"))
    p = two.join(other, on="transaction_id", how="inner").filter(pl.col("roster_id") != pl.col("other_roster"))
    out["partners"] = (p.group_by(["league_name", "manager", "partner"]).agg(pl.len().alias("trades"), pl.col("net_wins").sum().alias("net_wins"), (pl.col("net_wins") > 0.05).mean().alias("win_rate"))
                       .sort(["league_name", "manager", "net_wins"]))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--rebuild", action="store_true", help="rebuild from the lake even if --out has the parquet files")
    ap.add_argument("--min-seasons", type=float, default=1.0, help="a trade must be this old (seasons) to count in the scored tables")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--manager", help="also print every trade of this manager in full (both sides, prices, wins)")
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
    cols = ["league_name", "season", "date", "manager", "recv_assets", "give_assets", "recv_ktc_then", "give_ktc_then", "recv_wins", "give_wins", "net_wins", "net_ktc_now", "seasons_since"]
    worst = scored.sort("net_wins").head(15).select(cols)
    best = scored.sort("net_wins", descending=True).head(15).select(cols)
    pat = patterns(tt, legs, args.min_seasons)
    with pl.Config(tbl_rows=80, tbl_cols=-1, tbl_width_chars=300, float_precision=2, tbl_hide_dataframe_shape=True, tbl_hide_column_data_types=True, fmt_str_lengths=90):
        print(f"trades {R['trades'].height}, legs {legs.height} (picks resolved {legs.filter(pl.col('asset') == 'pick')['drafted_player_id'].is_not_null().sum()} of {legs.filter(pl.col('asset') == 'pick').height}), "
              f"sides scored at >= {args.min_seasons} seasons: {scored.height} of {tt.height}")
        print("\n-- managers (trades at least %.0f season(s) old)" % args.min_seasons)
        print(managers.select("league_name", "manager", "trades", "in_season_share", "ktc_in", "ktc_out", "net_ktc_per_trade", "wins_in", "wins_out", "net_wins", "win_rate", "loss_rate", "best", "worst", "net_picks", "net_ktc_now"))
        print("\n-- worst trades (by wins delivered since, the side that lost)")
        print(worst)
        print("\n-- best trades")
        print(best)
        for k, v in pat.items():
            print(f"\n-- {k}")
            print(v if k != "partners" else v.filter(pl.col("trades") >= 3).sort("net_wins").head(12))
        if args.manager:
            mine = tt.filter(pl.col("manager") == args.manager).sort("date")
            print(f"\n-- every trade of {args.manager} ({mine.height} sides)")
            print(mine.select("league_name", "date", "leg", "recv_assets", "give_assets", "recv_ktc_then", "give_ktc_then", "recv_wins", "give_wins", "net_wins", "net_ktc_now", "seasons_since"))
    summary = {"managers": managers.to_dicts(), "worst": worst.to_dicts(), "best": best.to_dicts(), **{k: v.to_dicts() for k, v in pat.items()},
               "meta": {"n_trades": R["trades"].height, "n_legs": legs.height, "min_seasons": args.min_seasons, "through": str(legs["date"].max()),
                        "run_date": datetime.now(timezone.utc).date().isoformat()}}
    (out / "summary.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    print(f"\nwrote {out}")
    if args.publish:
        print("published", gcs_io.write_ml_json(summary, "backtests", "trades", f"run_date={summary['meta']['run_date']}", "summary.json"))


if __name__ == "__main__":
    main()
