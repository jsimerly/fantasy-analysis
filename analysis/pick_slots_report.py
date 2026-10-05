"""Expected draft slot: the report behind the page's Draft slots tab (analysis, not a model).

From ``pick_slots`` (the empirical tier table and the preseason prior, built from the
10,000-league crawl) and today's standings: for every team in the three leagues, where its next
pick is likely to land (P(Early / Mid / Late), the expected slot) and what that makes its next
1st and 2nd worth at today's KTC tier prices; the tables themselves, collapsed to weeks played x
record fifth; and a calibration check on our own leagues' past seasons (predicted tier at weeks
4 / 8 / 12 vs the actual final standing, against "Mid for everyone").

Run with the root venv:
  .venv/Scripts/python analysis/pick_slots_report.py --out analysis/_cache/pick_slots --publish
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "machine_learning" / "src"))

import polars as pl  # noqa: E402

import gcs_io  # noqa: E402
import pick_slots  # noqa: E402
import trades  # noqa: E402


def by_week(table: pl.DataFrame) -> pl.DataFrame:
    """The tier table collapsed over the points-for fifth: P(tiers) by weeks played x record fifth."""
    g = table.group_by(["played", "rk5"]).agg(pl.col("n").sum().alias("n"),
                                              ((pl.col("p_early") * pl.col("n")).sum() / pl.col("n").sum()).alias("p_early"),
                                              ((pl.col("p_mid") * pl.col("n")).sum() / pl.col("n").sum()).alias("p_mid"),
                                              ((pl.col("p_late") * pl.col("n")).sum() / pl.col("n").sum()).alias("p_late"),
                                              ((pl.col("slot_pct_mean") * pl.col("n")).sum() / pl.col("n").sum()).alias("slot_pct_mean"))
    return g.filter(pl.col("played") <= 14).sort(["played", "rk5"])


def teams_now(ctx: trades.SlotContext, today: date) -> pl.DataFrame:
    """Every current roster in the three leagues: standing today, P(tiers) for next year's pick,
    expected slot, and the value of its next 1st / 2nd at today's tier prices."""
    meta = gcs_io.read_lake("silver/fantasy/dim_leagues_meta/data.parquet").select("league_id", "league_name", pl.col("season").cast(pl.Int64), "league_lineage_id", pl.col("total_rosters").cast(pl.Int64))
    s = ctx.season_of(today)
    cur = meta.filter(pl.col("season") == s)
    fr = trades.franchises()
    prices = trades.load_pick_prices(today)
    last = prices["valuation_date"].max()
    px = {(r["pick_round"], r["tier"]): r["ktc"] for r in prices.filter((pl.col("valuation_date") == last) & (pl.col("pick_season") == s + 1)).iter_rows(named=True)}
    rows = []
    for lg in cur.iter_rows(named=True):
        lin, lid, teams = lg["league_lineage_id"], lg["league_id"], lg["total_rosters"]
        st = None
        if ctx.current is not None and ctx.current.height:
            st = ctx.current.filter((pl.col("league_id") == lid) & (pl.col("as_of") <= today)).sort("as_of")
            if st.height:
                last_as_of = st["as_of"].max()
                st = st.filter(pl.col("as_of") == last_as_of)
        if st is None or st.height == 0:
            continue
        for r in st.iter_rows(named=True):
            p = ctx.probs(lin, s + 1, int(r["roster_id"]), today) or (0.0, 1.0, 0.0)
            mgr = fr.filter((pl.col("lineage_id") == lin) & (pl.col("roster_id") == r["roster_id"]))
            exp_slot = (p[0] * (teams / 6.0) + p[1] * (teams / 2.0) + p[2] * (5 * teams / 6.0))      # midpoints of the thirds
            v1 = sum(p[i] * px.get((1, tier), 0.0) for i, tier in enumerate(("Early", "Mid", "Late")))
            v2 = sum(p[i] * px.get((2, tier), 0.0) for i, tier in enumerate(("Early", "Mid", "Late")))
            rows.append({"league_name": lg["league_name"], "lineage_id": lin, "roster_id": int(r["roster_id"]), "manager": mgr["manager"][0] if mgr.height else f"roster {r['roster_id']}",
                         "as_of": str(r["as_of"]), "played": int(r["played"]), "wins": float(r["wins"]), "pf": float(r["pf"]), "rank_now": int(r["rank_now"]), "pf_rank": int(r["pf_rank"]), "teams": int(teams or r["teams"]),
                         "p_early": p[0], "p_mid": p[1], "p_late": p[2], "exp_slot": exp_slot, "pick_season": s + 1,
                         "first_value": v1, "second_value": v2, "first_mid": px.get((1, "Mid")), "second_mid": px.get((2, "Mid"))})
    return pl.DataFrame(rows)


def calibration(ctx: trades.SlotContext, weeks: tuple[int, ...] = (4, 8, 12)) -> pl.DataFrame:
    """On our leagues' past seasons: the predicted tier at k weeks vs the actual final tier."""
    if ctx.hist is None or ctx.table is None:
        return pl.DataFrame()
    h = ctx.hist
    fin = h.filter(pl.col("week") == pl.col("last_week")).select("league_id", "season", "roster_id", "teams", "final_rank")
    fin = fin.with_columns(pick_slots.tier_of(pl.col("final_rank"), pl.col("teams")).alias("actual"))
    rows = []
    onehot = {"Early": (1, 0, 0), "Mid": (0, 1, 0), "Late": (0, 0, 1)}
    for k in weeks:
        at = h.filter(pl.col("played") == k).join(fin.select("league_id", "season", "roster_id", "actual"), on=["league_id", "season", "roster_id"], how="inner")
        n = hit = 0
        brier = base_brier = 0.0
        base_hit = 0
        for r in at.iter_rows(named=True):
            p = pick_slots.tier_probs(ctx.table, int(r["played"]), int(r["rank_now"]), int(r["pf_rank"]), int(r["teams"]))
            if p is None:
                continue
            y = onehot[r["actual"]]
            n += 1
            hit += int(("Early", "Mid", "Late")[max(range(3), key=lambda i: p[i])] == r["actual"])
            brier += sum((p[i] - y[i]) ** 2 for i in range(3))
            base_hit += int(r["actual"] == "Mid")
            base_brier += sum(((0, 1, 0)[i] - y[i]) ** 2 for i in range(3))
        if n:
            rows.append({"played": k, "n": n, "accuracy": hit / n, "brier": brier / n, "mid_for_all_accuracy": base_hit / n, "mid_for_all_brier": base_brier / n})
    # the preseason prior
    if ctx.prior is not None:
        lin = {}
        for (l, s), (lid, teams) in ctx.league_of.items():
            lin[lid] = (l, s)
        prev = fin.with_columns(pl.col("league_id").replace_strict({k: v[0] for k, v in lin.items()}, default=None).alias("lineage_id"))
        nxt = prev.select("lineage_id", (pl.col("season") + 1).alias("season"), "roster_id", pl.col("final_rank").alias("prev_rank"), pl.col("teams").alias("prev_teams"))
        both = prev.join(nxt, on=["lineage_id", "season", "roster_id"], how="inner")
        n = hit = 0
        brier = base_brier = 0.0
        base_hit = 0
        for r in both.iter_rows(named=True):
            p = pick_slots.prior_probs(ctx.prior, int(r["prev_rank"]), int(r["prev_teams"]))
            if p is None:
                continue
            y = onehot[r["actual"]]
            n += 1
            hit += int(("Early", "Mid", "Late")[max(range(3), key=lambda i: p[i])] == r["actual"])
            brier += sum((p[i] - y[i]) ** 2 for i in range(3))
            base_hit += int(r["actual"] == "Mid")
            base_brier += sum(((0, 1, 0)[i] - y[i]) ** 2 for i in range(3))
        if n:
            rows.insert(0, {"played": 0, "n": n, "accuracy": hit / n, "brier": brier / n, "mid_for_all_accuracy": base_hit / n, "mid_for_all_brier": base_brier / n})
    return pl.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--publish", action="store_true")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    today = date.today()
    ctx = trades.SlotContext()
    table, prior = ctx.table, ctx.prior
    st = pl.read_parquet(pick_slots.CACHE / "crawl_standings.parquet")
    tn = teams_now(ctx, today)
    cal = calibration(ctx)
    bw = by_week(table)
    with pl.Config(tbl_rows=60, tbl_cols=-1, tbl_width_chars=220, float_precision=2, tbl_hide_dataframe_shape=True, tbl_hide_column_data_types=True):
        print("-- teams now"); print(tn.select("league_name", "manager", "played", "wins", "rank_now", "pf_rank", "p_early", "p_mid", "p_late", "exp_slot", "first_value", "second_value"))
        print("-- calibration"); print(cal)
        print("-- prior"); print(prior)
    summary = {"meta": {"run_date": datetime.now(timezone.utc).date().isoformat(), "league_seasons": int(st.select("league_id", "season").n_unique()), "team_weeks": int(st.height),
                        "seasons": [int(st["season"].min()), int(st["season"].max())], "pick_season": int(ctx.season_of(today) + 1)},
               "teams": tn.to_dicts(), "calibration": cal.to_dicts(), "by_week": bw.to_dicts(), "prior": prior.to_dicts() if prior is not None else [],
               "table": table.filter(pl.col("played") <= 14).to_dicts()}
    (out / "summary.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    print(f"wrote {out}")
    if args.publish:
        print("published", gcs_io.write_ml_json(summary, "backtests", "pick_slots", f"run_date={summary['meta']['run_date']}", "summary.json"))


if __name__ == "__main__":
    main()
