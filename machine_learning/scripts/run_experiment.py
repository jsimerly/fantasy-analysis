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
                  realized_replacement: str | None = None) -> HarnessContext:
    """The harness setup shared by every script that scores projections walk-forward: cohorts
    ``first_cohort``..``last_cohort`` (default: last complete season minus the horizon), replacement
    as of each cohort (``share`` / ``fill`` / ``weekly``) and KTC the following February."""
    matrix = career.build_career_matrix(H)
    last = career.last_complete_season(matrix)
    horizon = max(H)
    last_cohort = last_cohort if last_cohort is not None else last - horizon
    cohorts = list(range(first_cohort, last_cohort + 1))
    slots, teams = replacement.league_lineup(gcs_io.read_lake(SETTINGS_PATH))
    starters = replacement.starters_per_position(slots, teams)
    hist, xw = market.load_ktc_history(), market.load_crosswalk()
    ctx = fg.Context()
    import league as lg
    curve = lg.curve_for_lineage(replacement.PRIMARY_LINEAGE)
    print(f"WAR units: owner's league curve, weekly mean {curve.mean_points:.1f}, spread {curve.sd_points:.1f} ({curve.n} team-weeks)")

    spec = pool = None
    if "share" != replacement_kind or (realized_replacement and realized_replacement != "share"):
        import lineup
        spec = lg.LeagueSpec.from_settings(gcs_io.read_lake(SETTINGS_PATH))
        pool = matrix.filter(pl.col("position").is_in(["QB", "RB", "WR", "TE"]))

    def rep_by(kind: str, T: int) -> dict:
        if kind == "fill":
            import lineup
            return lineup.replacement_from_history(pool, spec, list(range(T - 4, T + 1)))
        if kind == "weekly":
            import lineup
            return lineup.replacement_weekly(pool, ctx.weeks, spec, list(range(T - 4, T + 1)))
        return replacement.replacement_levels(matrix, starters, seasons=list(range(T - 4, T + 1)))

    def rep_for(T: int) -> dict:
        return rep_by(replacement_kind, T)

    realized_rep_for = (lambda T: rep_by(realized_replacement, T)) if realized_replacement else None

    def market_for(cohort: pl.DataFrame, T: int) -> pl.DataFrame:
        return market.attach_market(cohort, date(T + 1, 2, 15), hist, xw)

    return HarnessContext(matrix=matrix, cohorts=cohorts, ctx=ctx, curve=curve, rep_for=rep_for, market_for=market_for,
                          realized_rep_for=realized_rep_for, hist=hist, crosswalk=xw)


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
    ap.add_argument("--target", choices=["level", "residual", "opportunity"], default="level",
                    help="ppg target: the level, the change from this season's rate, or opportunities per game x points per opportunity")
    ap.add_argument("--weight", choices=["ppg", "ppg2"], default=None, help="relevance sample weights for the career models")
    ap.add_argument("--backend", choices=["xgb", "tabpfn", "blend"], default="xgb",
                    help="estimator: xgb (production trees), tabpfn (TabPFN foundation model, use --device cuda) or blend (mean of both)")
    ap.add_argument("--tabpfn-params", nargs="*", default=[], help="TabPFNRegressor overrides, e.g. n_estimators=4")
    ap.add_argument("--params", nargs="*", default=[], help="xgboost overrides for every variant in this run, e.g. max_depth=6 min_child_weight=1")
    ap.add_argument("--calibrate", nargs="?", const="both", default=False, choices=["both", "ppg", "games", "tier"],
                    help="walk-forward recalibration per position and horizon: both (default when given), ppg or games")
    ap.add_argument("--quantile-sigma", action="store_true", help="player-specific projection spread from quantile models")
    ap.add_argument("--position-scale", action="store_true", help="scale each position's value by its holdout realized/projected PAR share")
    ap.add_argument("--no-log", action="store_true", help="do not append to the ledger")
    ap.add_argument("--list-groups", action="store_true")
    ap.add_argument("--leaderboard", action="store_true")
    ap.add_argument("--paired", nargs=2, metavar=("RUN_A", "RUN_B"),
                    help="compare two runs' per-cohort results (names or name@timestamp; latest run of each name) and exit")
    args = ap.parse_args()

    if args.list_groups:
        for g in fg.GROUPS.values():
            print(f"{g.name:10s} {len(g.columns):3d} cols  <- {g.source}")
        return
    if args.paired:
        with pl.Config(tbl_rows=-1, tbl_width_chars=200, float_precision=3):
            print(ex.paired(*args.paired))
        return
    if args.leaderboard:
        ledger = ex.load_ledger()
        if ledger is None:
            print("no ledger yet")
            return
        with pl.Config(tbl_rows=-1, tbl_width_chars=220, float_precision=3):
            print(ex.leaderboard(ledger, horizon=args.horizon))
        return

    variants = parse_variants(args.variants) if args.variants else [(args.name or (args.groups or ",".join(fg.DEFAULT)), (args.groups or ",".join(fg.DEFAULT)).split(","))]
    H = list(range(1, args.horizon + 1))
    bc = build_context(H, args.first_cohort, args.last_cohort, args.replacement, args.realized_replacement)
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
                                  target=args.target, weight=args.weight, backend=args.backend, tabpfn_params=tabpfn_params)
        per_cohort, summary = ex.run_experiment(matrix, cfg, ctx, rep_for, market_for, realized_rep_for=realized_rep_for)
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
