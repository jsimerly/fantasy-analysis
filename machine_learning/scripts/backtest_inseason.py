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
import feature_groups as fg  # noqa: E402
import features  # noqa: E402
import gcs_io  # noqa: E402
import power  # noqa: E402
import inseason  # noqa: E402
import market  # noqa: E402
import replacement  # noqa: E402
import tail_cache  # noqa: E402
import value  # noqa: E402

WEEK_PATH = "silver/fantasy/fact_player_week/data.parquet"
SETTINGS_PATH = "silver/fantasy/dim_league_settings/data.parquet"
H = list(range(1, 11))
TAIL_CACHE_DIR = ROOT / "_cache" / "career_tail"      # the --current career tail, reused weekly (tail_cache)


def load_inputs(draft_rows: bool = False, snapshot_weeks: list[int] | None = None, snapshot_from: int = 2010) -> tuple[pl.DataFrame, pl.DataFrame]:
    wk = gcs_io.read_lake(WEEK_PATH)
    fact = features.load_fact_player_season()
    df = career.career_features(features.attach_lags_and_target(fact, drop_no_target=False)).with_columns(pl.lit(18, pl.Int64).alias("row_week"))
    if draft_rows:
        import draft_rows as dr
        df = dr.with_draft_rows(df, dr.build_draft_rows(gcs_io.read_lake(career.XWALK_PATH), fact))
    if snapshot_weeks:
        import draft_rows as dr
        import unified
        df = unified.with_snapshot_rows(df, unified.snapshot_rows(wk, df.filter(~dr.is_aux(df)), snapshot_weeks, snapshot_from))
    return wk, career.attach_horizon_targets(df, H)


def week_end_date(wk: pl.DataFrame, season: int, week: int) -> date:
    d = wk.filter((pl.col("season") == season) & (pl.col("week") == week))["game_date"].max()
    return (d + timedelta(days=1)) if d is not None else date(season, 9, 1) + timedelta(weeks=week)


SNAP_TIER_COLS = ("prev_ppg", "prev_games")   # a snapshot's prior tier is last season's (the row the career tail is off), not 3 weeks of this one


def career_tail(season_df: pl.DataFrame, as_of: int, rep: dict, device: str, backend: str = "xgb", tabpfn_params: dict | None = None,
                cap: str = career.DEFAULT_CAP, range_quantiles: tuple | None = None, features: list[str] | None = None, stacked: bool = False,
                target: str = "level", cache_dir: Path | None = None):
    """Career-model projections off every player's row in season ``as_of`` (their latest
    complete season), plus the out-of-sample spread, trained only on outcomes known by then.
    Projected games are capped by the age-survival prior per ``cap`` (career.apply_cap: from age
    30, tier-aware, by default). Returns (predictions, sigma, survival, model).

    With ``cache_dir`` the capped predictions and sigma are reused from ``cache_dir/<key>`` while
    the key holds (tail_cache: the visible rows, this configuration, the modelling code) and the
    model comes back as None -- the weekly refresh, where the tail is identical until a season
    completes or the code or the lake changes."""
    survival = career.fit_survival(season_df.filter((pl.col("season") + 1) <= as_of), cap)
    cfg = dict(horizons=H, backend=backend, tabpfn_params=tabpfn_params or {}, cap=cap, range_quantiles=range_quantiles,
               features=features, stacked=stacked, target=target)
    key = tail_cache.cache_key(season_df, as_of, cfg) if cache_dir else None
    if key:
        hit = tail_cache.load(cache_dir, key)
        if hit:
            print(f"career tail: cached as of {as_of} (key {key}, {hit[0].height} players, written {hit[2].get('written')}); "
                  f"--no-tail-cache recomputes", flush=True)
            return hit[0], hit[1], survival, None
        print(f"career tail: no cache for key {key}; computing", flush=True)
    m = career.HorizonModels(H, device=device, backend=backend, tabpfn_params=tabpfn_params, range_quantiles=range_quantiles,
                             features=features, stacked=stacked, target=target).fit(season_df, as_of_season=as_of)
    sigma = m.estimate_sigma(season_df, as_of_season=as_of)
    import draft_rows as dr
    rows = season_df.filter(pl.col("season") == as_of)
    if "is_snapshot_row" in rows.columns:                   # project the season rows (and draft rows), not the training snapshots
        rows = rows.filter(~dr._flag(rows, "is_snapshot_row"))
    pred = career.apply_cap(survival, m.predict(rows), H, cap)
    if key:
        print("career tail: cached under", tail_cache.store(cache_dir, key, pred, sigma, {"as_of": as_of, "config": cfg}), flush=True)
    return pred, sigma, survival, m


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--weeks", default="3,6,9,13")
    ap.add_argument("--first-cohort", type=int, default=2021)
    ap.add_argument("--discount-rate", type=float, default=value.DEFAULT_DISCOUNT_RATE)
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--current", action="store_true", help="train on everything and project the in-progress season")
    ap.add_argument("--no-tail-cache", action="store_true", help="--current: recompute the career tail even when _cache/career_tail has it for this key")
    ap.add_argument("--sigma", choices=["band"], default=None, help="--current: price each career span's upside with the player's own 20/80 band width (items 6 / 36b; needs --range) instead of the position's holdout sigma")
    ap.add_argument("--no-write", action="store_true", help="do not persist the backtest summary to the ML bucket")
    ap.add_argument("--inseason-groups", default="", help="in-season model groups (inseason.EXTRA_GROUPS): team (the team to date at the snapshot week), contract (the contract in force)")
    ap.add_argument("--inseason-backend", choices=["xgb", "tabpfn"], default="xgb", help="in-season model estimator (the career tail has --backend); tabpfn shares --tabpfn-params")
    ap.add_argument("--inseason-next-one-per-season", action="store_true", help="the next-season models see one random snapshot week per player-season (a repeated label counts once)")
    ap.add_argument("--adp-dir", default=None, help="a directory with mfl.parquet / ffc.parquet (average draft position pulled locally) for the preseason group, instead of the lake")
    ap.add_argument("--proj-dir", default=None, help="a directory with weekly.parquet (Sleeper weekly projections pulled locally) for the consensus group, instead of the lake")
    ap.add_argument("--ffo-dir", default=None, help="a directory with weekly.parquet (nflverse ff_opportunity pulled locally) for the opportunity group, instead of the lake")
    ap.add_argument("--ngs-dir", default=None, help="a directory with receiving.parquet / rushing.parquet (Next Gen Stats pulled locally) for the usage group, instead of the lake")
    ap.add_argument("--inseason-train-weeks", default="", help="snapshot weeks the in-season model trains on, e.g. 3,6,9,13 (default: all for xgb, the checkpoint weeks for tabpfn)")
    ap.add_argument("--inseason-max-rows", type=int, default=50000, help="in-context cap for the tabpfn in-season model (recent seasons first)")
    ap.add_argument("--depth", action="store_true", help="add the depth-chart standing features (BACKLOG 11; neutral in the 2021-24 backtest, so opt-in)")
    ap.add_argument("--backend", choices=["xgb", "tabpfn", "blend"], default="xgb", help="career model estimator for the multi-year tail (the in-season model itself stays xgboost)")
    ap.add_argument("--tabpfn-params", nargs="*", default=[], help="TabPFNRegressor overrides, e.g. model_version=v2")
    ap.add_argument("--range", action="store_true", help="keep the 20/50/80 quantiles of the career tail's predictive distribution (TabPFN): the range of outcomes on the page")
    ap.add_argument("--groups", default="base,career", help="feature groups for the career tail (feature_groups.GROUPS), e.g. base,career,injury,trend,situation,rookie,college")
    ap.add_argument("--stacked", action="store_true", help="pooled horizons for the career tail: one games and one ppg model over every horizon")
    ap.add_argument("--draft-rows", action="store_true", help="drafted rookies get a pre-NFL row (college + draft capital) the career tail projects, instead of the rookie tail table")
    ap.add_argument("--snapshot-weeks", default="", help="mid-season snapshot rows in the career tail's training table (unified.py), e.g. 9")
    ap.add_argument("--snapshot-from", type=int, default=2010)
    ap.add_argument("--max-horizon", type=int, default=10, help="career tail horizons 1..N (10 = production; fewer for a cheaper unified-model test)")
    ap.add_argument("--target", choices=["level", "residual", "blend"], default="level", help="career ppg target: the level, the change from this season's rate, or the mean of both")
    ap.add_argument("--cap", default=career.DEFAULT_CAP, help="age-survival cap on projected games: 30+t (default: tier-aware, from age 30) | 30+ | all (the pre-2026-10-05 behaviour) | none")
    args = ap.parse_args()
    power.keep_awake()                      # hours of GPU work: do not let the machine sleep under it
    tabpfn_params = {}
    for kv in args.tabpfn_params:
        k, v = kv.split("=", 1)
        try:
            tabpfn_params[k] = int(v)
        except ValueError:
            tabpfn_params[k] = v

    weeks = [int(w) for w in args.weeks.split(",")]
    global H
    if args.max_horizon != len(H):
        H = list(range(1, args.max_horizon + 1))
        print(f"career tail horizons 1..{args.max_horizon}", flush=True)

    import draft_rows as dr
    snap_weeks = [int(w) for w in args.snapshot_weeks.split(",") if w]
    wk, season = load_inputs(args.draft_rows, snap_weeks or None, args.snapshot_from)
    groups = fg.resolve(args.groups.split(","))
    feature_cols = fg.feature_columns(groups)
    if args.groups != "base,career":
        season = fg.assemble(season, groups, fg.Context())
        print(f"feature groups {[g.name for g in groups]}: {len(feature_cols)} columns", flush=True)
    group_cols = [c for c in feature_cols if c not in career.FEATURES]
    if snap_weeks:
        import unified
        season = unified.mask_group_columns(season, group_cols)   # a mid-season row must not carry the whole season's groups
    base = dr.drop_draft_rows(season)                 # played seasons: snapshots, replacement, rookie tables; the career tail sees every row
    if args.draft_rows:
        print(f"draft rows: {season.height - base.height} drafted skill players carry a pre-NFL row; the career tail projects them directly", flush=True)
    depth = gcs_io.read_lake("silver/fantasy/fact_depth_chart_week/data.parquet") if args.depth else None
    is_groups = [g for g in args.inseason_groups.split(",") if g]
    is_tabpfn = args.inseason_backend == "tabpfn"
    is_train_weeks = [int(w) for w in args.inseason_train_weeks.split(",") if w] or ([int(w) for w in args.weeks.split(",")] if is_tabpfn else None)
    is_kw = dict(backend=args.inseason_backend, tabpfn_params=tabpfn_params if is_tabpfn else None, next_one_per_season=args.inseason_next_one_per_season,
                 train_weeks=is_train_weeks, max_train_rows=args.inseason_max_rows if is_tabpfn else None)
    if is_tabpfn:
        print(f"in-season model: tabpfn {tabpfn_params or {}} on weeks {is_train_weeks}, at most {args.inseason_max_rows:,} rows per fit", flush=True)
    extra_cols = inseason.extra_columns(is_groups)
    team_week = gcs_io.read_lake(inseason.TEAM_WEEK_PATH) if "team" in is_groups else None
    contracts = gcs_io.read_lake(inseason.CONTRACT_PATH) if "contract" in is_groups else None
    status = gcs_io.read_lake(inseason.STATUS_PATH) if ("usage" in is_groups or "role" in is_groups) else None
    ngs_rec = ngs_rush = None
    if "usage" in is_groups:
        if args.ngs_dir:                                              # the slices pulled locally (nflreadpy) until the lake has them
            ngs_rec, ngs_rush = pl.read_parquet(f"{args.ngs_dir}/receiving.parquet"), pl.read_parquet(f"{args.ngs_dir}/rushing.parquet")
        else:
            ngs_rec, ngs_rush = gcs_io.read_lake_prefix(inseason.NGS_REC_PATH), gcs_io.read_lake_prefix(inseason.NGS_RUSH_PATH)
    schedules = gcs_io.read_lake_prefix(inseason.SCHEDULES_PATH) if "schedule" in is_groups else None
    ffo = None
    if "opportunity" in is_groups:
        ffo = pl.read_parquet(f"{args.ffo_dir}/weekly.parquet") if args.ffo_dir else gcs_io.read_lake_prefix(inseason.FFO_PATH)
    proj = xw_ids = ids = preseason = None
    if "consensus" in is_groups or "consensus_line" in is_groups or "preseason" in is_groups:
        ids = gcs_io.read_lake_prefix(inseason.FF_IDS_PATH, partition="load_date")
        ids = ids.filter(pl.col("load_date") == ids["load_date"].max())
    if "consensus" in is_groups or "consensus_line" in is_groups:
        proj = pl.read_parquet(f"{args.proj_dir}/weekly.parquet") if args.proj_dir else gcs_io.read_lake_prefix(inseason.PROJ_PATH)
        xw_ids = ids.select("sleeper_id", "gsis_id")
    if "preseason" in is_groups:
        if args.adp_dir:
            mfl, ffc = pl.read_parquet(f"{args.adp_dir}/mfl.parquet"), pl.read_parquet(f"{args.adp_dir}/ffc.parquet")
        else:
            mfl, ffc = gcs_io.read_lake_prefix(f"{inseason.ADP_PATH}/mfl"), gcs_io.read_lake_prefix(f"{inseason.ADP_PATH}/ffc")
        preseason = inseason.preseason_features(mfl, ffc, ids.select("mfl_id", "gsis_id", "name", "position"))
        print(f"preseason consensus: {preseason.height:,} player-seasons {preseason['season'].min()}..{preseason['season'].max()}", flush=True)
    snaps = inseason.baselines(inseason.build_snapshots(wk, base, depth=depth, team_week=team_week, contracts=contracts, ngs_receiving=ngs_rec, ngs_rushing=ngs_rush,
                                                        status=status, schedules=schedules, role="role" in is_groups, opportunity=ffo,
                                                        consensus=proj, consensus_xwalk=xw_ids, preseason=preseason))
    if extra_cols:
        print(f"in-season groups {is_groups}: {len(extra_cols)} columns; " + ", ".join(f"{c} {snaps[c].is_not_null().mean():.0%}" for c in extra_cols[:1] + extra_cols[-1:]), flush=True)
    if depth is not None:
        print(f"depth-chart features on: {snaps['td_depth_rank'].is_not_null().mean():.0%} of snapshots listed")
    last_complete = career.last_complete_season(season)
    hist, xw = market.load_ktc_history(), market.load_crosswalk()
    slots, teams = replacement.league_lineup(gcs_io.read_lake(SETTINGS_PATH))
    starters = replacement.starters_per_position(slots, teams)
    print(f"snapshots: {snaps.shape} (seasons {snaps['season'].min()}..{snaps['season'].max()}, weeks {inseason.WEEKS[0]}..{inseason.WEEKS[-1]}); last complete season {last_complete}")

    if args.current:
        cur = int(wk["season"].max())
        w_now = int(wk.filter(pl.col("season") == cur)["week"].max())
        rep = replacement.replacement_levels(base, starters)
        tail, sigma, survival, _ = career_tail(season, last_complete, rep, args.device, args.backend, tabpfn_params, args.cap,
                                            range_quantiles=(0.2, 0.5, 0.8) if args.range else None,
                                            features=feature_cols, stacked=args.stacked, target=args.target,
                                            cache_dir=None if args.no_tail_cache else TAIL_CACHE_DIR)
        backend_tag = (args.backend + ("(" + ",".join(f"{k}={v}" for k, v in tabpfn_params.items()) + ")" if tabpfn_params else "")
                       + (" stacked" if args.stacked else "") + (f" {args.target}" if args.target != "level" else "") + (f" [{args.groups}]" if args.groups != "base,career" else ""))
        m = inseason.InSeasonModels(device=args.device, extra_features=extra_cols, **is_kw).fit(snaps)
        snap = m.predict(snaps.filter((pl.col("season") == cur) & (pl.col("week") == w_now)))
        snap = career.apply_cap(survival, snap, [1], args.cap, col="next_games_hat", tier_cols=SNAP_TIER_COLS)
        rookie_tbl = inseason.rookie_tail_table(base, H, through=last_complete)
        if args.sigma == "band" and not args.range:
            raise SystemExit("--sigma band needs --range (the band comes from the TabPFN career tail)")
        snap = inseason.inseason_value(snap, tail, rep, sigma, H, args.discount_rate, rookie_table=rookie_tbl, sigma_mode=args.sigma)
        print("rookie tails from realized trajectories:", snap.filter(pl.col("tail_source") == "rookie_table").height, "players")
        snap = market.attach_market(snap, datetime.now(timezone.utc).date(), hist, xw)
        # preseason view for the same players: the career model's IV off their 2025 row
        pre = value.intrinsic_value(tail, rep, H, args.discount_rate).select("player_id", pl.col("iv").alias("iv_preseason"))
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
        p = gcs_io.write_ml_parquet(snap.join(cmp.select("player_id", "fair_value", "mispricing", "mispricing_pct", "iv_rank", "market_rank"), on="player_id", how="left")
                                    .with_columns(pl.lit(backend_tag).alias("career_backend")),
                                    "inseason", f"season={cur}", f"week={w_now}", f"run_date={run}", "projections.parquet")
        p2 = gcs_io.write_ml_json({
            "run_date": run, "season": cur, "week": w_now, "as_of_season": last_complete, "career_backend": backend_tag, "cap": args.cap,
            "range_quantiles": [0.2, 0.5, 0.8] if args.range else None, "groups": args.groups, "stacked": args.stacked, "target": args.target, "draft_rows": args.draft_rows, "snapshot_weeks": args.snapshot_weeks, "inseason_groups": args.inseason_groups, "inseason_backend": args.inseason_backend,
            "sigma_mode": args.sigma,
            "discount_rate": args.discount_rate, "ppg_sigma": sigma, "replacement_ppg": rep,
            "n_projected": snap.height, "n_with_market": summary["n"], "spearman_iv_vs_ktc": summary["spearman"],
        }, "inseason", f"season={cur}", f"week={w_now}", f"run_date={run}", "metrics.json")
        print("\nwrote", p, "\n     ", p2)
        return

    rows, lag_rows = [], []
    for T in range(args.first_cohort, last_complete):               # next-season outcome must be complete
        rep = replacement.replacement_levels(base, starters, seasons=list(range(T - 5, T)))
        tail, sigma, survival, tail_model = career_tail(season, T - 1, rep, args.device, args.backend, tabpfn_params, args.cap, features=feature_cols, stacked=args.stacked, target=args.target)
        rookie_tbl = inseason.rookie_tail_table(base, H, through=T - 1)
        m = inseason.InSeasonModels(device=args.device, extra_features=extra_cols, **is_kw).fit(snaps, as_of_season=T)
        k_end = market.ktc_as_of(hist, date(T + 1, 2, 15)).select("player_key", pl.col("ktc_value").alias("k_end"))
        for W in weeks:
            snap = m.predict(snaps.filter((pl.col("season") == T) & (pl.col("week") == W)))
            snap = career.apply_cap(survival, snap, [1], args.cap, col="next_games_hat", tier_cols=SNAP_TIER_COLS)
            snap = snap.with_columns((pl.col("next_games_hat") * pl.col("next_ppg_hat")).alias("next_fpts_hat"))
            snap = inseason.inseason_value(snap, tail, rep, sigma, H, args.discount_rate, rookie_table=rookie_tbl)
            if snap_weeks:                                     # next season from this week's own snapshot through the unified career model
                import unified
                urows = unified.snapshot_rows(wk.filter(pl.col("season") == T), base, weeks=[W], from_season=T)
                urows = urows.with_columns([pl.lit(None, pl.Float64).alias(c) for c in group_cols if c not in urows.columns])
                up = career.apply_cap(survival, tail_model.predict(urows), [1], args.cap)
                snap = snap.join(up.select("player_id", (pl.col("h1_ppg_hat") * pl.col("h1_games_hat")).alias("next_fpts_unified")), on="player_id", how="left")
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
            if "cs_next_ppr" in priced.columns:                                    # the provider's view as rankings: the coming week (a bye or an out week falls back to the mean to date) and the mean to date
                r["next|consensus"] = value.spearman(priced.select(pl.col("cs_next_ppr").fill_null(pl.col("cs_td_mean")).fill_null(0.0))["cs_next_ppr"].to_numpy(), nx)
                r["next|consensus_td"] = value.spearman(priced["cs_td_mean"].fill_null(0.0).to_numpy(), nx)
            if "next_fpts_unified" in priced.columns:
                r["next|unified"] = value.spearman(priced["next_fpts_unified"].fill_null(0.0).to_numpy(), nx)
            played = priced.filter(pl.col("ros_games") > 0)
            rp = played["ros_ppg"].to_numpy().astype(float)
            r.update({"ros|model": value.spearman(played["ros_ppg_hat"].to_numpy(), rp),
                      "ros|ktc": value.spearman(played["ktc_value"].to_numpy(), rp),
                      "ros|last_season": value.spearman(played["bl_prior_ppg"].to_numpy(), rp),
                      "ros|to_date": value.spearman(played["bl_todate_ppg"].to_numpy(), rp),
                      "ros|blend": value.spearman(played["bl_blend_ppg"].to_numpy(), rp)})
            if "cs_next_ppr" in played.columns:
                r["ros|consensus"] = value.spearman(played.select(pl.col("cs_next_ppr").fill_null(pl.col("cs_td_mean")).fill_null(0.0))["cs_next_ppr"].to_numpy(), rp)
                r["ros|consensus_td"] = value.spearman(played["cs_td_mean"].fill_null(0.0).to_numpy(), rp)
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
    if not args.no_write:                       # persist for the performance panel / later comparison
        run = datetime.now(timezone.utc).date().isoformat()
        lag_mean = lagdf.group_by("signal", "W").agg(pl.col("n").sum(), pl.col("^spearman.*$").mean(), pl.col("^move\\|.*$").mean()).sort("signal", "W")
        w3 = lag_mean.filter((pl.col("signal") == "next_pts") & (pl.col("W") == weeks[0])).to_dicts()
        note = (f"at week {weeks[0]} the gap between the next-season projection and KTC predicts KTC's move to February "
                f"(Spearman {w3[0]['spearman(mispricing, move)']:+.2f}; the third priced cheapest vs the model moved {w3[0]['move|cheap']:+.1%}, "
                f"the richest third {w3[0]['move|rich']:+.1%}); the multi-year gap does not") if w3 else ""
        summary = {"run_date": run, "inseason_groups": args.inseason_groups, "inseason_backend": args.inseason_backend, "first_cohort": args.first_cohort, "last_cohort": last_complete - 1, "weeks": weeks,
                   "cohorts": f"{args.first_cohort}-{last_complete - 1}",
                   "next": res.group_by("W").agg(pl.col("n").sum(), pl.col("^next\\|.*$").mean()).sort("W").to_dicts(),
                   "ros": res.group_by("W").agg(pl.col("n").sum(), pl.col("^ros\\|.*$").mean()).sort("W").to_dicts(),
                   "market_lag": lag_mean.to_dicts(), "market_lag_note": note}
        p = gcs_io.write_ml_json(summary, "backtests", "inseason", f"run_date={run}", "summary.json")
        print("\nwrote", p)


if __name__ == "__main__":
    main()
