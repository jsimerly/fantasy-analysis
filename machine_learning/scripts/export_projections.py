"""Export a value run as JSON for the browsable projections table (page data file).

Two sources:

* ``--source inseason`` (default): the in-season run written by ``backtest_inseason.py --current``
  (``inseason/season=S/week=W/run_date=D/projections.parquet``). Value components are exported in
  the order the page discounts them -- [rest of THIS season, next season, season + 2, ...] -- and
  the page weights component k by (1 - rate)^(k-1), so the rest of this season is never
  discounted and a 100 % rate means "rest of this season only". Includes this season's games to
  date and players without a complete prior season (rookies).
* ``--source career``: the preseason career run (``intrinsic_value/as_of_season=S/run_date=D``),
  components [season 1, season 2, ...] off the last complete season.

Usage (from machine_learning/):
    uv run python scripts/export_projections.py --season 2026 --week 3 --run-date 2026-10-01 --out projections.json
    uv run python scripts/export_projections.py --source career --run-date 2026-10-01 --out projections.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

import gcs_io  # noqa: E402
import market  # noqa: E402
import picks  # noqa: E402
import value  # noqa: E402


def _r(x, d=1):
    return None if x is None else round(x, d)


def fantasycalc_values() -> pl.DataFrame:
    """Same-day FantasyCalc SF dynasty value per player_key (a second market reference)."""
    fav = gcs_io.read_lake(market.FACT_ASSET_VALUES_PATH)
    day = fav["valuation_date"].max()
    return (fav.filter((pl.col("valuation_date") == day) & (pl.col("market_type") == "DYNASTY")
                       & (pl.col("qb_format") == "SF") & (pl.col("te_premium") == "Standard") & (pl.col("fc_value") > 0))
            .group_by("player_id").agg(pl.col("fc_value").max()).rename({"player_id": "player_key"}))


def redraft_values() -> pl.DataFrame:
    """Latest KTC REDRAFT values per player_key: superflex (``rd_sf``) and 1QB (``rd_1qb``), standard TE.
    The ROS market for the page's ROS view (dynasty values lag redraft reality by design)."""
    fav = gcs_io.read_lake(market.FACT_ASSET_VALUES_PATH)
    rd = fav.filter((pl.col("market_type") == "REDRAFT") & (pl.col("te_premium") == "Standard") & (pl.col("ktc_value") > 0))
    out = None
    for fmt_, col in (("SF", "rd_sf"), ("1QB", "rd_1qb")):
        sub = rd.filter(pl.col("qb_format") == fmt_)
        if sub.height == 0:
            continue
        day = sub["valuation_date"].max()
        one = sub.filter(pl.col("valuation_date") == day).group_by("player_id").agg(pl.col("ktc_value").max().alias(col)).rename({"player_id": "player_key"})
        out = one if out is None else out.join(one, on="player_key", how="full", coalesce=True)
    return out if out is not None else pl.DataFrame({"player_key": [], "rd_sf": [], "rd_1qb": []})


def within_position(proj: pl.DataFrame, iv_col: str) -> pl.DataFrame:
    """The market's premium for a whole position factored out: fair value fitted per position."""
    within, _ = value.compare_to_market(proj, iv_col=iv_col, group_col="position")
    return proj.join(within.select("player_id", pl.col("fair_value").alias("fair_pos"), pl.col("mispricing_pct").alias("mis_pct_pos")),
                     on="player_id", how="left")


def _common(r: dict) -> dict:
    return {
        "name": r["player_name"], "pos": r["position"], "team": r.get("team"), "age": _r(r.get("age_at_season")),
        "ktc": r.get("ktc_value"), "market_rank": r.get("market_rank"), "iv_rank": r.get("iv_rank"), "rank_gap": r.get("rank_gap"),
        "fair": _r(r.get("fair_value"), 0), "mis_pct": _r(r.get("mispricing_pct"), 3),
        "fair_pos": _r(r.get("fair_pos"), 0), "mis_pct_pos": _r(r.get("mis_pct_pos"), 3),
        "match": r.get("market_match"), "fc": r.get("fc_value"), "rd_sf": r.get("rd_sf"), "rd_1qb": r.get("rd_1qb"),
    }


def rows_career(proj: pl.DataFrame, horizons: list[int]) -> list[dict]:
    out = []
    for r in proj.sort("iv", descending=True).iter_rows(named=True):
        out.append({**_common(r),
                    "fpts": _r(r["fpts"], 0), "games": r["games"],
                    "iv": _r(r["iv"]), "iv_rank_all": int(r["iv_rank_all"]),
                    "h": [_r(r[f"h{k}_fpts_hat"], 0) for k in horizons],
                    # undiscounted points above replacement per season: the page recomputes IV for any discount
                    "v": [_r(r[f"h{k}_vorp_hat"], 2) for k in horizons],
                    "h1_ppg": _r(r["h1_ppg_hat"]), "h1_games": _r(r["h1_games_hat"])})
    return out


def rows_inseason(proj: pl.DataFrame, tail: list[int]) -> list[dict]:
    out = []
    for r in proj.sort("iv_inseason", descending=True).iter_rows(named=True):
        h = [_r(r["ros_ppg_hat"] * r["ros_games_hat"], 0), _r(r["next_fpts_hat"], 0)]
        v = [_r(r["vorp_ros"], 2), _r(r["vorp_next"], 2)]
        for k in tail:                                              # career tail: seasons + 2 and on
            ppg, g = r.get(f"h{k}_ppg_hat"), r.get(f"h{k}_games_hat")
            h.append(_r(ppg * g, 0) if ppg is not None and g is not None else 0)
            v.append(_r(r.get(f"h{k}_vorp_hat") or 0.0, 2))
        pg = [_r(r["ros_ppg_hat"], 2), _r(r["next_ppg_hat"], 2)] + [_r(r.get(f"h{k}_ppg_hat") or 0.0, 2) for k in tail]
        g = [_r(r["ros_games_hat"], 2), _r(r["next_games_hat"], 2)] + [_r(r.get(f"h{k}_games_hat") or 0.0, 2) for k in tail]
        out.append({**_common(r),
                    "fpts": _r(r.get("prev_fpts"), 0), "games": r.get("prev_games"),
                    "td_games": r.get("td_games"), "td_ppg": _r(r.get("td_ppg")), "td_touches": _r(r.get("td_touches_pg")),
                    "iv": _r(r["iv_inseason"]), "iv_pre": _r(r.get("iv_preseason")), "iv_rank_all": int(r["iv_rank_all"]),
                    "h": h, "v": v, "pg": pg, "g": g,                        # per-span projected rate / games (primary scoring)
                    "h1_ppg": _r(r["ros_ppg_hat"]), "h1_games": _r(r["ros_games_hat"])})
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["inseason", "career"], default="inseason")
    ap.add_argument("--season", type=int, default=None, help="in-season: the season in progress")
    ap.add_argument("--week", type=int, default=None, help="in-season: snapshot week")
    ap.add_argument("--as-of-season", type=int, default=2025, help="career run (replacement level; the data for --source career)")
    ap.add_argument("--run-date", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    career_base = ("intrinsic_value", f"as_of_season={args.as_of_season}", f"run_date={args.run_date}")
    meta = gcs_io.read_ml_json(*career_base, "metrics.json")
    rate = meta.get("discount_rate", round(1 - meta.get("discount", 0.8), 2))
    fc = fantasycalc_values()
    rd = redraft_values()
    fc = fc.join(rd, on="player_key", how="full", coalesce=True)

    if args.source == "career":
        proj = gcs_io.read_ml_parquet(*career_base, "projections.parquet")
        if "fair_value" not in proj.columns:                      # older runs: derive the comparison here
            cmp, summary = value.compare_to_market(proj)
            proj = proj.join(cmp.select("player_id", "fair_value", "mispricing", "mispricing_pct", "iv_rank", "market_rank", "rank_gap"),
                             on="player_id", how="left")
            meta["spearman_iv_vs_ktc"] = summary["spearman"]
        if "iv_rank_all" not in proj.columns:
            proj = proj.with_columns(pl.col("iv").rank(method="ordinal", descending=True).alias("iv_rank_all"))
        proj = within_position(proj, "iv").join(fc, on="player_key", how="left")
        horizons = sorted(int(c[1:].split("_")[0]) for c in proj.columns if c.startswith("h") and c.endswith("_fpts_hat"))
        rows = rows_career(proj, horizons)
        first = args.as_of_season + 1
        labels = [f"’{(first + i) % 100:02d}" for i in range(len(horizons))]
        as_of = f"preseason, projected from the {args.as_of_season} season"
        spearman = meta.get("spearman_iv_vs_ktc")
    else:
        if args.season is None or args.week is None:
            ap.error("--season and --week are required for --source inseason")
        proj = gcs_io.read_ml_parquet("inseason", f"season={args.season}", f"week={args.week}", f"run_date={args.run_date}", "projections.parquet")
        proj = (within_position(proj, "iv_inseason")
                .with_columns(pl.col("iv_inseason").rank(method="ordinal", descending=True).alias("iv_rank_all"),
                              (pl.col("iv_rank").cast(pl.Int64) - pl.col("market_rank").cast(pl.Int64)).alias("rank_gap")))
        proj = proj.join(fc, on="player_key", how="left") if "player_key" in proj.columns else proj.with_columns(pl.lit(None).alias("fc_value"))
        tail = sorted(int(c[1:].split("_")[0]) for c in proj.columns if c.startswith("h") and c.endswith("_vorp_hat"))
        rows = rows_inseason(proj, tail)
        labels = [f"ROS ’{args.season % 100:02d}"] + [f"’{(args.season + i) % 100:02d}" for i in range(1, len(tail) + 2)]
        as_of = f"in-season, {args.season} through week {args.week}"
        priced = proj.filter(pl.col("ktc_value").is_not_null())
        spearman = float(value.spearman(priced["iv_inseason"].to_numpy(), priced["ktc_value"].to_numpy())) if priced.height else None

    # ---- WAR per league (build_war.py outputs): per-player value components per league + roster views
    leagues, teams, league_ids = [], {}, {}
    tail_tag = f"season={args.season or args.as_of_season}/week={args.week or 0}/run_date={args.run_date}"
    by_name = {r["name"]: r for r in rows}
    for path in sorted(p for p in gcs_io.list_ml("war") if p.endswith(f"{tail_tag}/meta.json")):
        parts = path.split("/")
        lid = parts[1].split("=", 1)[1]
        meta_l = gcs_io.read_ml_json(*parts[:-1], "meta.json")
        if "display_name" not in meta_l:                 # output of an older build_war; not a league of the owner's
            continue
        wp = gcs_io.read_ml_parquet(*parts[:-1], "projections.parquet")
        ks = sorted(int(c.split("_")[1]) for c in wp.columns if c.startswith("war_") and c.split("_")[1].isdigit())
        for r in wp.iter_rows(named=True):
            row = by_name.get(r["player_name"])
            if row is None:
                continue
            base = row["pg"][0] if row.get("pg") else None
            scale = (r["ros_ppg_hat"] / base) if base and r.get("ros_ppg_hat") is not None and base > 0 else 1.0
            row.setdefault("L", {})[lid] = {"w": [_r(r[f"war_{k}"], 4) for k in ks], "v": [_r(r[f"par_{k}"], 2) for k in ks], "s": _r(scale, 4)}
        curve = meta_l["win_curve"]
        p0 = 1 / (1 + 2.718281828 ** (-(curve["a"] + curve["b"] * curve["mean_points"])))
        try:
            td = gcs_io.read_ml_parquet(*parts[:-1], "teams.parquet")
        except Exception:  # noqa: BLE001
            td = None
        offset = float(td["lineup_offset"][0]) if td is not None and "lineup_offset" in td.columns else 0.0
        league_ids[lid] = meta_l.get("league_id")
        leagues.append({"id": lid, "name": meta_l.get("display_name", lid), "teams": meta_l["league"]["teams"], "slots": meta_l["league"]["slots"],
                        "replacement": {k: _r(v, 1) for k, v in meta_l["replacement_ppg"].items()}, "starters": meta_l.get("starters"),
                        "curve": {"mean": _r(curve["mean_points"], 1), "sd": _r(curve["sd_points"], 1), "n": curve.get("n", 0),
                                  "per10": _r(10 * curve["b"] * p0 * (1 - p0), 3), "a": curve["a"], "b": curve["b"]},
                        "offset": _r(offset, 2), "note": meta_l.get("note", ""), "primary": meta_l.get("lineage_id") == "730630605066371072"})
        if td is not None:
            tl = []
            for rid, tname in td.select("roster_id", "team_name").unique().sort("roster_id").iter_rows():
                t = td.filter(pl.col("roster_id") == rid)
                own = t.filter(pl.col("rostered")).sort("m_war", descending=True)
                trade = t.filter(~pl.col("rostered") & pl.col("owned_by").is_not_null()).sort("m_war", descending=True).head(12)
                free = t.filter(~pl.col("rostered") & pl.col("owned_by").is_null()).sort("m_war", descending=True).head(6)
                tl.append({"rid": rid, "name": tname, "owner": bool(t["is_owner"][0]) if "is_owner" in t.columns else False,
                           "ppg": _r(t["lineup_ppg_now"][0]), "wp": _r(t["win_prob_now"][0], 3),
                           "players": [[x["player_name"], x["position"], _r(x["m_war"], 2), _r(x["m_par"], 0), _r(x["league_war"], 2), x["ktc_value"], _r(x.get("m_war_1"), 2), x.get("status") or "active"] for x in own.iter_rows(named=True)],
                           "targets": [[x["player_name"], x["position"], x["owned_by"], _r(x["m_war"], 2), _r(x["league_war"], 2), x["ktc_value"], _r(x.get("m_war_1"), 2), x.get("status") or "active"] for x in trade.iter_rows(named=True)],
                           "free": [[x["player_name"], x["position"], _r(x["m_war"], 2)] for x in free.iter_rows(named=True)]})
            teams[lid] = tl
    # ---- model performance: latest persisted backtest summaries + the experiment leaderboard
    def latest_summary(name: str):
        paths = sorted(p for p in gcs_io.list_ml("backtests", name) if p.endswith("summary.json"))
        return gcs_io.read_ml_json(*paths[-1].split("/")) if paths else None
    performance = {k: latest_summary(k) for k in ("career_eval", "value", "inseason", "market")}
    try:
        import experiments
        led = experiments.load_ledger()
        performance["experiments"] = experiments.leaderboard(led, horizon=3).to_dicts() if led is not None else None
    except Exception as e:  # noqa: BLE001
        print("experiment ledger unavailable:", str(e)[:80]); performance["experiments"] = None
    print("performance summaries:", {k: (v is not None) for k, v in performance.items()})

    default_league = next((l["id"] for l in leagues if l["primary"]), leagues[0]["id"] if leagues else None)
    if default_league:
        rows.sort(key=lambda x: -(sum(w * (1 - rate) ** i for i, w in enumerate(x["L"][default_league]["w"])) if default_league in x.get("L", {}) else -1))
        for i, x in enumerate(rows):
            x["iv_rank_all"] = i + 1
    print(f"leagues with WAR: {[l['name'] for l in leagues]}; roster views: {list(teams)}")

    try:
        now_season = int(args.season or args.as_of_season + 1)
        pick_tab, pick_meta = picks.build_from_lake(now_season, [now_season + 1, now_season + 2, now_season + 3], rate=rate)
        pick_rows = [[int(r["season"]), int(r["round"]), r["tier"], int(r["n_slots"]), _r(r["wins_undiscounted"], 3), r.get("ktc")] for r in pick_tab.iter_rows(named=True)]
        picks_out = {"now": now_season, "rows": pick_rows, "meta": pick_meta}
        print(f"picks: curve a={pick_meta['a']:.2f} b={pick_meta['b']:.2f} on {pick_meta['n_players']} drafted players, {pick_meta['n_rookie_picks']} league rookie picks")
    except Exception as e:  # noqa: BLE001
        print("picks: skipped:", str(e)[:200]); picks_out = None
    # owned picks per roster, with the slot tier projected from the original roster's lineup strength
    if picks_out is not None and teams:
        try:
            client = gcs_io._client()
            tp_blobs = sorted(b.name for b in client.list_blobs("nfl-data-bronze", prefix="bronze/sleeper/rosters/traded_picks/daily/") if b.name.endswith(".parquet"))
            traded_all = gcs_io.read_lake(tp_blobs[-1])
            value_of = {(int(r["season"]), int(r["round"]), r["tier"]): (r["wins_undiscounted"], r.get("ktc")) for r in pick_tab.iter_rows(named=True)}
            future = [now_season + 1, now_season + 2, now_season + 3]
            for lid, tl in teams.items():
                sleeper_id = league_ids.get(lid)
                traded = traded_all.filter(pl.col("league_id") == str(sleeper_id)) if sleeper_id else traded_all.head(0)
                order = sorted(tl, key=lambda x: -(x["ppg"] or 0))
                rank = {x["rid"]: i + 1 for i, x in enumerate(order)}
                names = {x["rid"]: x["name"] for x in tl}
                owned = picks.owned_picks(traded, [x["rid"] for x in tl], future)
                for x in tl:
                    mine = owned.filter(pl.col("owner_roster_id") == x["rid"]).sort("season", "round", "original_roster_id")
                    out_p = []
                    for r in mine.iter_rows(named=True):
                        # next two drafts from projected strength; the third is anyone's guess -> Mid
                        tier = picks.projected_tier(rank[r["original_roster_id"]], len(tl)) if r["season"] <= now_season + 2 else "Mid"
                        wins, ktc = value_of.get((r["season"], r["round"], tier), (None, None))
                        out_p.append([r["season"], r["round"], tier, names.get(r["original_roster_id"], ""), _r(wins, 3), ktc])
                    x["picks"] = out_p
            print("owned picks attached:", {lid: sum(len(x.get("picks", [])) for x in tl) for lid, tl in teams.items()})
        except Exception as e:  # noqa: BLE001
            print("owned picks: skipped:", str(e)[:200])
    out = {
        "picks": picks_out,
        "mode": args.source, "as_of": as_of, "season": args.season, "week": args.week, "as_of_season": args.as_of_season,
        "leagues": leagues, "default_league": default_league, "teams": teams, "performance": performance,
        "run_date": args.run_date, "labels": labels, "prev_label": f"Pts ’{args.as_of_season % 100:02d}", "discount_rate": rate,
        "replacement_ppg": meta["replacement_ppg"], "n": len(rows), "n_priced": sum(1 for x in rows if x["ktc"] is not None),
        "spearman": spearman, "rows": rows,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")
    print(f"wrote {args.out} ({args.out.stat().st_size // 1024} KB, {len(rows)} players, {out['n_priced']} priced; {as_of})")


if __name__ == "__main__":
    main()
