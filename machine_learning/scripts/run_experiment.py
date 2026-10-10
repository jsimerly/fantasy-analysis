"""Train and score feature-group variants of the career model in the same walk-forward backtest.

    uv run python scripts/run_experiment.py --list-groups
    uv run python scripts/run_experiment.py --variants "current=base,career;injury=base,career,injury" --horizon 3 --first-cohort 2015
    uv run python scripts/run_experiment.py --groups base,career,role,trend --name role_trend
    uv run python scripts/run_experiment.py --leaderboard [--horizon 3]

Every run prints the per-cohort table and (unless --no-log) appends its summary to the ledger in
the ML bucket, so combinations can be compared over time on identical cohorts.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

import career  # noqa: E402
import experiments as ex  # noqa: E402
import feature_groups as fg  # noqa: E402
import gcs_io  # noqa: E402
import power  # noqa: E402
import market  # noqa: E402
import replacement  # noqa: E402
import value  # noqa: E402

SETTINGS_PATH = "silver/fantasy/dim_league_settings/data.parquet"


def parse_variants(spec: str) -> list[tuple[str, list[str]]]:
    out = []
    for part in spec.split(";"):
        part = part.strip()
        if not part:
            continue
        name, groups = part.split("=", 1)
        out.append((name.strip(), [g.strip() for g in groups.split(",") if g.strip()]))
    return out


class HarnessContext:
    """Everything a walk-forward run needs besides the model: the career matrix, the cohorts, the
    feature context, the owner's win curve, and the replacement / market callbacks."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


def build_context(H: list[int], first_cohort: int, last_cohort: int | None, replacement_kind: str,
                  realized_replacement: str | None = None, draft_rows: bool = False, snapshot_weeks: list[int] | None = None,
                  snapshot_from: int = 2010) -> HarnessContext:
    """The harness setup shared by every script that scores projections walk-forward: cohorts
    ``first_cohort``..``last_cohort`` (default: last complete season minus the horizon), replacement
    as of each cohort (``share`` / ``fill`` / ``weekly``) and KTC the following February."""
    import draft_rows as dr
    matrix = career.build_career_matrix(H, draft_rows=draft_rows, snapshot_weeks=snapshot_weeks, snapshot_from=snapshot_from)
    base = dr.drop_draft_rows(matrix)                 # played seasons: replacement levels and the regime stamp
    if snapshot_weeks:
        print(f"snapshot rows: {int(matrix.filter(pl.col('is_snapshot_row'))['player_id'].len())} at weeks {list(snapshot_weeks)} from {snapshot_from}")
    last = career.last_complete_season(matrix)
    horizon = max(H)
    last_cohort = last_cohort if last_cohort is not None else last - horizon
    cohorts = list(range(first_cohort, last_cohort + 1))
    slots, teams = replacement.league_lineup(gcs_io.read_lake(SETTINGS_PATH))
    starters = replacement.starters_per_position(slots, teams)
    regime = ex.regime_stamp(starters, gcs_io.lake_updated("silver/fantasy/fact_player_season/data.parquet"), base.height)
    if draft_rows:
        print(f"draft rows: {matrix.height - base.height} (drafted skill players {int(matrix.filter(pl.col('is_draft_row'))['draft_year'].min())}-{int(matrix.filter(pl.col('is_draft_row'))['draft_year'].max())})")
    print(f"regime: {regime}")
    hist, xw = market.load_ktc_history(), market.load_crosswalk()
    ctx = fg.Context()
    import league as lg
    curve = lg.curve_for_lineage(replacement.PRIMARY_LINEAGE)
    print(f"WAR units: owner's league curve, weekly mean {curve.mean_points:.1f}, spread {curve.sd_points:.1f} ({curve.n} team-weeks)")

    spec = pool = None
    if "share" != replacement_kind or (realized_replacement and realized_replacement != "share"):
        import lineup
        spec = lg.LeagueSpec.from_settings(gcs_io.read_lake(SETTINGS_PATH))
        pool = base.filter(pl.col("position").is_in(["QB", "RB", "WR", "TE"]))

    def rep_by(kind: str, T: int) -> dict:
        if kind == "fill":
            import lineup
            return lineup.replacement_from_history(pool, spec, list(range(T - 4, T + 1)))
        if kind == "weekly":
            import lineup
            return lineup.replacement_weekly(pool, ctx.weeks, spec, list(range(T - 4, T + 1)))
        return replacement.replacement_levels(base, starters, seasons=list(range(T - 4, T + 1)))

    def rep_for(T: int) -> dict:
        return rep_by(replacement_kind, T)

    realized_rep_for = (lambda T: rep_by(realized_replacement, T)) if realized_replacement else None

    def market_for(cohort: pl.DataFrame, T: int) -> pl.DataFrame:
        return market.attach_market(cohort, date(T + 1, 2, 15), hist, xw)

    return HarnessContext(matrix=matrix, cohorts=cohorts, ctx=ctx, curve=curve, rep_for=rep_for, market_for=market_for,
                          realized_rep_for=realized_rep_for, hist=hist, crosswalk=xw, regime=regime)


def rescore_cli(args) -> None:
    """``--rescore RUN``: the saved cohort frames scored again under a value-side change, saved as
    a run of their own and compared with the source, cohort by cohort."""
    fixed = {k: float(v) for k, v in (kv.split("=") for kv in args.fixed_scale.split(",") if kv)}
    src_name = args.rescore.partition("@")[0]
    frames = ex.load_cohorts(args.rescore)
    try:
        src = ex.load_run(args.rescore)
    except FileNotFoundError:                     # a --no-log run: frames only, no in-sample scores or regime to carry
        src = None
    ledger = ex.load_ledger()
    meta = ledger.filter(pl.col("name") == src_name).sort("timestamp").tail(1).to_dicts() if ledger is not None else []
    meta = meta[0] if meta else {}
    groups = (args.groups or meta.get("groups") or ",".join(fg.DEFAULT)).split(",")
    H = sorted(int(c[1:-9]) for c in frames.columns if c.startswith("h") and c.endswith("_fpts_hat") and c[1:-9].isdigit())   # the horizons the frames hold
    suffix = ("_fs" if fixed else "") + ("_ws" if args.walk_scale else "") + ({"band": "_sb", "none": "_s0"}.get(args.sigma, "")) + (f"_top{args.top_n}" if args.top_n else "")
    name = args.as_name or (src_name + (suffix or "_rescored"))
    backend = str(meta.get("backend") or args.backend)
    cfg = ex.ExperimentConfig(name=name, groups=groups, horizons=H, cohorts=sorted(int(t) for t in frames["cohort"].unique().to_list()),
                              discount_rate=float(meta.get("discount_rate") or args.discount_rate), fixed_scale=fixed, position_scale=args.walk_scale,
                              replacement=str(meta.get("replacement") or args.replacement).split("/")[0], target=str(meta.get("target") or args.target),
                              backend=backend.split("(")[0].split(" ")[0], stacked=" stacked" in backend, **({"top_n": args.top_n} if args.top_n else {}))
    rep_for = curve = None
    if args.sigma:                                # value and WAR rebuilt: the cohort's replacement levels and the owner's curve, as the run had them
        bc = build_context(H, min(cfg.cohorts), max(cfg.cohorts), cfg.replacement)
        rep_for, curve = bc.rep_for, bc.curve
    per_cohort, summary = ex.rescore(frames, cfg, fixed_scale=fixed, walk_scale=args.walk_scale, src_per_cohort=src, sigma_mode=args.sigma, rep_for=rep_for, curve=curve)
    summary["regime"] = str(src["regime"][0]) if src is not None and "regime" in src.columns else ""
    summary["backend"] = backend + f" rescored<{args.rescore}>"
    per_cohort = per_cohort.with_columns(pl.lit(summary["regime"]).alias("regime"))
    change = " ".join(x for x in (("fixed " + args.fixed_scale) if fixed else "", "walk-forward scale" if args.walk_scale else "", f"sigma {args.sigma}" if args.sigma else "", f"top {args.top_n}" if args.top_n else "") if x)
    with pl.Config(tbl_rows=-1, tbl_width_chars=200, float_precision=3):
        print(f"== {name}: {args.rescore} rescored ({change or 'as is'}) ==")
        print(per_cohort.select([c for c in per_cohort.columns if c in ("cohort", "n_all", "spearman_war_top", "mae_war_top", "share_abs_err") or c.startswith("scale_")]))
    if args.no_log:
        return
    ledger = ex.append_result(ledger, summary)
    print("per-cohort ->", ex.save_run(per_cohort, summary))
    ex.save_ledger(ledger)
    with pl.Config(tbl_rows=-1, tbl_width_chars=200, float_precision=3):
        table = ex.paired(name, args.rescore)
        print(table)
        print(ex.verdict(table))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--variants", help='"name=g1,g2;name2=g1,g3" (several variants on one matrix build)')
    ap.add_argument("--groups", help="one variant: comma-separated feature groups")
    ap.add_argument("--name", help="one variant: its name (default = the group list)")
    ap.add_argument("--horizon", type=int, default=3)
    ap.add_argument("--first-cohort", type=int, default=2015)
    ap.add_argument("--last-cohort", type=int, default=None, help="default: last complete season minus horizon")
    ap.add_argument("--discount-rate", type=float, default=value.DEFAULT_DISCOUNT_RATE)
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--replacement", choices=["share", "fill", "weekly"], default="share",
                    help="replacement level: production flex-share line, an explicit fill of the league's lineup (lineup.league_fill), "
                         "or the injury-aware weekly fill over players who actually played each week (lineup.replacement_weekly)")
    ap.add_argument("--realized-replacement", choices=["share", "fill", "weekly"], default=None,
                    help="score realized value on a different replacement level (ledger shows projected/realized)")
    ap.add_argument("--fixed-scale", default="", help="fixed value scale per position, e.g. QB=0.8,TE=1.1 (applied to iv / war / par)")
    ap.add_argument("--target", choices=["level", "residual", "opportunity", "blend"], default="level",
                    help="ppg target: the level, the change from this season's rate, or opportunities per game x points per opportunity")
    ap.add_argument("--weight", choices=["ppg", "ppg2"], default=None, help="relevance sample weights for the career models")
    ap.add_argument("--backend", choices=["xgb", "tabpfn", "blend"], default="xgb",
                    help="estimator: xgb (production trees), tabpfn (TabPFN foundation model, use --device cuda) or blend (mean of both)")
    ap.add_argument("--tabpfn-params", nargs="*", default=[], help="TabPFNRegressor overrides, e.g. n_estimators=4")
    ap.add_argument("--stacked", action="store_true", help="one games / ppg model over all horizons, years-ahead as a feature (instead of one pair per horizon)")
    ap.add_argument("--draft-rows", action="store_true", help="add a pre-NFL row per drafted skill player (college + draft capital; draft_rows.py) to the matrix")
    ap.add_argument("--snapshot-weeks", default="", help="mid-season snapshot rows in the training table (unified.py), e.g. 9 or 6,13")
    ap.add_argument("--snapshot-from", type=int, default=2010, help="first season of the snapshot rows")
    ap.add_argument("--range", action="store_true", help="keep the 20/50/80 quantiles of the predictive distribution (TabPFN) and score the band: coverage, pinball")
    ap.add_argument("--cap", default=career.DEFAULT_CAP, help="age-survival cap on projected games: 30+t (default, as production: tier-aware from 30) | 30+ | 30+t34 | all (the pre-2026-10-05 behaviour) | none")
    ap.add_argument("--params", nargs="*", default=[], help="xgboost overrides for every variant in this run, e.g. max_depth=6 min_child_weight=1")
    ap.add_argument("--calibrate", nargs="?", const="both", default=False,
                    help="walk-forward recalibration: both (default when given) | ppg | games | tier | games_table[:all|tiers[:weight]] (empirical games by position x age x tier; tiers = starters and mid only; weight = the table's share of a blend)")
    ap.add_argument("--quantile-sigma", action="store_true", help="player-specific projection spread from quantile models")
    ap.add_argument("--position-scale", action="store_true", help="scale each position's value by its holdout realized/projected PAR share")
    ap.add_argument("--no-log", action="store_true", help="do not append to the ledger")
    ap.add_argument("--save-cohorts", action="store_true", help="keep every scored cohort frame (experiments/cohorts) for --rescore")
    ap.add_argument("--rescore", metavar="RUN", help="score the saved cohort frames of RUN again (no GPU) under --fixed-scale, --walk-scale or --top-n; name it with --as")
    ap.add_argument("--as", dest="as_name", help="the rescored run name (default: RUN plus a suffix for the change)")
    ap.add_argument("--walk-scale", action="store_true", help="--rescore: walk-forward per-position value scale from the cohorts complete by each cohort")
    ap.add_argument("--sigma", choices=["band", "none"], default=None, help="--rescore: price upside with each player's own 20/80 band width (items 6 / 36b) or with no spread; value and WAR rebuilt")
    ap.add_argument("--top-n", type=int, default=None, help="the top-N projected players the ordering and error metrics score (default: the config)")
    ap.add_argument("--list-groups", action="store_true")
    ap.add_argument("--leaderboard", action="store_true")
    ap.add_argument("--paired", nargs=2, metavar=("RUN_A", "RUN_B"),
                    help="compare two runs' per-cohort results (names or name@timestamp; latest run of each name) and exit")
    args = ap.parse_args()
    power.keep_awake()                      # hours of GPU work: do not let the machine sleep under it

    if args.list_groups:
        for g in fg.GROUPS.values():
            print(f"{g.name:10s} {len(g.columns):3d} cols  <- {g.source}")
        return
    if args.paired:
        with pl.Config(tbl_rows=-1, tbl_width_chars=200, float_precision=3):
            table = ex.paired(*args.paired)
            print(table)
            print(ex.verdict(table))
        return
    if args.leaderboard:
        ledger = ex.load_ledger()
        if ledger is None:
            print("no ledger yet")
            return
        with pl.Config(tbl_rows=-1, tbl_width_chars=220, float_precision=3):
            print(ex.leaderboard(ledger, horizon=args.horizon))
        return

    if args.rescore:
        rescore_cli(args)
        return

    variants = parse_variants(args.variants) if args.variants else [(args.name or (args.groups or ",".join(fg.DEFAULT)), (args.groups or ",".join(fg.DEFAULT)).split(","))]
    H = list(range(1, args.horizon + 1))
    snap_weeks = [int(w) for w in args.snapshot_weeks.split(",") if w]
    bc = build_context(H, args.first_cohort, args.last_cohort, args.replacement, args.realized_replacement, draft_rows=args.draft_rows,
                       snapshot_weeks=snap_weeks or None, snapshot_from=args.snapshot_from)
    matrix, cohorts, ctx, curve = bc.matrix, bc.cohorts, bc.ctx, bc.curve
    rep_for, market_for, realized_rep_for = bc.rep_for, bc.market_for, bc.realized_rep_for

    print(f"matrix {matrix.shape}; cohorts {cohorts[0]}-{cohorts[-1]}; horizon {args.horizon}; variants: " + "; ".join(f"{n}=[{','.join(g)}]" for n, g in variants))
    ledger = None if args.no_log else ex.load_ledger()
    summaries = []
    params = {}
    for kv in args.params:
        k, v = kv.split("=", 1)
        try:
            params[k] = int(v)
        except ValueError:
            try:
                params[k] = float(v)
            except ValueError:
                params[k] = v
    tabpfn_params = {}
    for kv in args.tabpfn_params:
        k, v = kv.split("=", 1)
        try:
            tabpfn_params[k] = int(v)
        except ValueError:
            try:
                tabpfn_params[k] = float(v)
            except ValueError:
                tabpfn_params[k] = v
    for name, groups in variants:
        cfg = ex.ExperimentConfig(name=name, groups=groups, horizons=H, cohorts=cohorts, discount_rate=args.discount_rate, device=args.device,
                                  params=params, calibrate=args.calibrate, quantile_sigma=args.quantile_sigma, curve=curve,
                                  position_scale=args.position_scale, replacement=args.replacement, realized_replacement=args.realized_replacement,
                                  fixed_scale={k: float(v) for k, v in (kv.split("=") for kv in args.fixed_scale.split(",") if kv)},
                                  target=args.target, weight=args.weight, backend=args.backend, tabpfn_params=tabpfn_params, stacked=args.stacked, cap=args.cap,
                                  range_quantiles=(0.2, 0.5, 0.8) if args.range else None, draft_rows=args.draft_rows, snapshot_weeks=args.snapshot_weeks)
        got: list[pl.DataFrame] = []
        per_cohort, summary = ex.run_experiment(matrix, cfg, ctx, rep_for, market_for, realized_rep_for=realized_rep_for, collect=got if args.save_cohorts else None)
        summary["regime"] = bc.regime
        if args.save_cohorts:
            print("cohort frames ->", ex.save_cohorts(name, got, summary["timestamp"]))
        per_cohort = per_cohort.with_columns(pl.lit(bc.regime).alias("regime"))
        summaries.append(summary)
        with pl.Config(tbl_rows=-1, tbl_width_chars=200, float_precision=3):
            print(f"\n== {name}: {summary['groups']} ({summary['n_features']} features) ==")
            print(per_cohort)
        if not args.no_log:
            ledger = ex.append_result(ledger, summary)
            print("per-cohort ->", ex.save_run(per_cohort, summary))
    with pl.Config(tbl_rows=-1, tbl_width_chars=220, float_precision=3):
        print("\n== summary (mean over cohorts) ==")
        print(pl.DataFrame(summaries).select([c for c in ["name", "groups", "n_features", "spearman_war_top", "spearman_war_all", "mae_war_top", "bias_war_top",
                                                         "bias_war_top12", "top_decile_war_all", "spearman_iv_vs_realized", "spearman_ktc_vs_realized", "edge_corr"]
                                              + [f"mae_h{k}" for k in H] if c in summaries[0]]))
    if not args.no_log and ledger is not None:
        print("ledger ->", ex.save_ledger(ledger))


if __name__ == "__main__":
    main()
