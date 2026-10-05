"""Intrinsic value v2: wins above replacement (WAR), per league, league-average and per roster.

    uv run python scripts/build_war.py --season 2026 --week 3 --run-date 2026-10-01 --all-leagues --teams
    uv run python scripts/build_war.py --season 2026 --week 3 --run-date 2026-10-01                 # primary league only
    uv run python scripts/build_war.py ... --league leagues/12team_1qb.json                        # any league from a JSON spec
    uv run python scripts/build_war.py --source career --run-date 2026-10-01                       # preseason career run

League pieces (``league.py`` / ``lineup.py``): the lineup from ``dim_league_settings`` (or the JSON),
replacement from an explicit fill of that lineup on the last five real seasons, and the win curve
from the league's own standings (a normal-margin curve from its weekly mean / spread until it has
300 team-weeks). Value (``war.py``): per span, sigma-aware points above replacement turned into
wins on that curve; the rest of this season is the first, undiscounted span. ``--teams`` adds the
per-roster view: marginal wins of every rostered player on his own lineup, trade targets (other
rosters) and free agents, from the latest Sleeper roster snapshot.

Projections are scored under the primary league's rules; the owner's other leagues share the
offensive scoring except interceptions (-2 vs -1), noted in each league's meta.

Writes ``war/league=<slug>/season=S/week=W/run_date=D/{projections.parquet, teams.parquet, meta.json}``.
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import numpy as np  # noqa: E402
import polars as pl  # noqa: E402

import features  # noqa: E402
import gcs_io  # noqa: E402
import league as lg  # noqa: E402
import lineup  # noqa: E402
import replacement as _rep  # noqa: E402
import scoring  # noqa: E402
import value  # noqa: E402
import war  # noqa: E402

WEEK_PATH = "silver/fantasy/fact_player_week/data.parquet"

SETTINGS_PATH = "silver/fantasy/dim_league_settings/data.parquet"
LEAGUES_META_PATH = "silver/fantasy/dim_leagues_meta/data.parquet"
FRANCHISES_PATH = "silver/fantasy/dim_franchises_meta/data.parquet"
ROSTERS_PREFIX = "bronze/sleeper/rosters/roster_players/daily/"
IDS_PREFIX = "bronze/nflverse/fantasy_player_ids/"
POSITIONS = ["QB", "RB", "WR", "TE"]
MIN_TEAM_WEEKS_FOR_FIT = 300


def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def win_curve_for(lineage_id: str | None, fallback: lg.WinCurve | None = None) -> lg.WinCurve:
    """The league's win curve: a logistic fit with 300+ of its own team-weeks, otherwise a
    normal-margin curve from its own weekly mean / spread, otherwise the fallback."""
    try:
        ts = lg.load_team_state()
        if lineage_id:
            ids = gcs_io.read_lake(LEAGUES_META_PATH).filter(pl.col("league_lineage_id") == lineage_id)["league_id"].to_list()
            ts = ts.filter(pl.col("league_id").is_in(ids))
        tw = lg.team_weeks_from_standings(ts)
        if tw.height >= MIN_TEAM_WEEKS_FOR_FIT:
            return lg.WinCurve.fit(tw)
        mean, sd = float(tw["week_pts"].mean()), float(tw["week_pts"].std())
        c = lg.WinCurve.normal(mean, sd * 2 ** 0.5)
        c.n = tw.height
        return c
    except Exception as e:  # noqa: BLE001
        print(f"  win curve: could not use standings ({str(e)[:80]}); using the fallback")
        return fallback or lg.WinCurve.normal(113.0, 42.0)


def sigma_from_career_meta(meta: dict) -> dict[int, dict[str, float]]:
    return {int(k): v for k, v in meta.get("ppg_sigma", {}).items()}


def run_meta(args) -> tuple[dict, str]:
    """The run's metrics (sigma, discount, replacement line) and the career run date to read from.

    An in-season refresh writes its own ``metrics.json`` (since 2026-10-05); older runs, and the
    career source, use the career build of the run date or, failing that, the latest one before it
    (the refresh does not rebuild the preseason career model every week)."""
    if args.source == "inseason":
        own = ("inseason", f"season={args.season}", f"week={args.week}", f"run_date={args.run_date}")
        if any(p.endswith("metrics.json") for p in gcs_io.list_ml(*own)):
            return gcs_io.read_ml_json(*own, "metrics.json"), args.run_date
    base = ("intrinsic_value", f"as_of_season={args.as_of_season}")
    run = gcs_io.latest_run_date(*base, on_or_before=args.run_date)
    if run is None:
        raise SystemExit(f"no career build under {'/'.join(base)} on or before {args.run_date}")
    if run != args.run_date:
        print(f"career build for run_date {args.run_date} not found; using {run}", flush=True)
    return gcs_io.read_ml_json(*base, f"run_date={run}", "metrics.json"), run


def _latest(prefix: str) -> str:
    from google.cloud import storage
    return sorted(b.name for b in storage.Client().list_blobs(gcs_io.LAKE_BUCKET, prefix=prefix) if b.name.endswith(".parquet"))[-1]


def current_rosters(league_id: str) -> pl.DataFrame:
    """Latest roster snapshot of a league, mapped to gsis ids via the nflverse crosswalk."""
    rp = gcs_io.read_lake(_latest(ROSTERS_PREFIX)).filter(pl.col("league_id") == league_id)
    xw = (gcs_io.read_lake(_latest(IDS_PREFIX)).select(pl.col("sleeper_id").cast(pl.Utf8), "gsis_id")
            .filter(pl.col("sleeper_id").is_not_null() & pl.col("gsis_id").is_not_null()).unique("sleeper_id"))
    fr = gcs_io.read_lake(FRANCHISES_PATH).filter(pl.col("league_id") == league_id).select("roster_id", "current_team_name", "owner_id")
    return (rp.join(xw, left_on="player_id", right_on="sleeper_id", how="left").join(fr, on="roster_id", how="left")
              .select("roster_id", pl.col("current_team_name").alias("team_name"), "owner_id", pl.col("gsis_id").alias("player_id"), "is_taxi", "is_reserve"))


def owner_in_every_league(leagues: pl.DataFrame) -> str | None:
    """The Sleeper user who owns a roster in every in-season league (the owner of this repo)."""
    fr = gcs_io.read_lake(FRANCHISES_PATH).filter(pl.col("league_id").is_in(leagues["league_id"].implode()))
    per = fr.group_by("owner_id").agg(pl.col("league_id").n_unique().alias("n")).filter(pl.col("n") == leagues.height)
    return per["owner_id"][0] if per.height == 1 else None


def build_league(spec: lg.LeagueSpec, curve: lg.WinCurve, proj: pl.DataFrame, comps: list[war.Component], sigma: dict,
                 season_fact: pl.DataFrame, rate: float, rosters: pl.DataFrame | None, owner_id: str | None, span: str,
                 note: str = "", scale: pl.DataFrame | None = None, weeks: pl.DataFrame | None = None,
                 replacement: str = "weekly") -> tuple[pl.DataFrame, pl.DataFrame | None, dict]:
    """``replacement``: "weekly" fills the league's lineups each week from the players who actually played
    (injuries and byes push the marginal starter deeper; lineup.replacement_weekly, needs ``weeks``),
    "fill" is the full-season fill (every starter assumed to play every week)."""
    if scale is not None:                     # this league's scoring, applied after the one model (per-player ratio)
        proj = scoring.apply_scale(proj, scale, [c for c in proj.columns if c.endswith("_ppg_hat") or c.endswith("_ppg_sigma")])
        season_fact = scoring.apply_scale(season_fact, scale, ["ppg"])
    last = int(season_fact.filter(pl.col("season_complete"))["season"].max())
    hist = list(range(last - 4, last + 1))
    pool_hist = season_fact.filter(pl.col("position").is_in(POSITIONS))
    rep_full = lineup.replacement_from_history(pool_hist, spec, hist)
    if replacement == "weekly":
        if weeks is None:
            raise ValueError("weekly replacement needs the weekly fact")
        rep = lineup.replacement_weekly(pool_hist, weeks, spec, hist)
    else:
        rep = rep_full
    starters = lineup.starters_per_position(pool_hist.filter((pl.col("season") == last) & (pl.col("games") >= 8)), spec)
    print(f"\n== {spec.name}: {spec.teams} teams, slots {spec.slots}")
    print(f"  starters by position (explicit fill, {last}): {starters}; replacement ({replacement}, {hist[0]}-{hist[-1]}): " + ", ".join(f"{p} {v:.1f}" for p, v in rep.items())
          + ("" if replacement != "weekly" else "; full-season fill would be " + ", ".join(f"{p} {v:.1f}" for p, v in rep_full.items())))
    print(f"  win curve: mean {curve.mean_points:.1f} sd {curve.sd_points:.1f} n={curve.n}; +10 ppg for an average team = +{10 * curve.slope_at_mean:.3f} win/week")
    out = war.wins_above_replacement(proj, rep, curve, comps, rate, sigma=sigma)
    out = out.with_columns(pl.col("war").rank(method="ordinal", descending=True).cast(pl.Int64).alias("war_rank_all"))
    cmp_cols = ["player_id", "fair_value", "mispricing", "mispricing_pct", "iv_rank", "market_rank", "rank_gap"]
    summary = {"n": 0, "spearman": None}
    if "ktc_value" in out.columns and out["ktc_value"].is_not_null().sum() > 10:
        cmp, summary = value.compare_to_market(out, iv_col="war", method="rank")
        out = out.drop([c for c in cmp_cols[1:] if c in out.columns]).join(cmp.select(cmp_cols), on="player_id", how="left")
        priced = out.filter(pl.col("ktc_value").is_not_null())
        sh = priced.group_by("position").agg(pl.col("war").sum().alias("w"), pl.col("ktc_value").sum().alias("k"))
        tot = sh.select(pl.col("w").sum(), pl.col("k").sum()).row(0)
        print(f"  {summary['n']} priced; spearman(WAR, KTC) = {summary['spearman']:.3f}; share by position (WAR | KTC): "
              + ", ".join(f"{r['position']} {r['w']/tot[0]:.0%}|{r['k']/tot[1]:.0%}" for r in sh.sort("position").iter_rows(named=True)))
    show = ["player_name", "position", "war_1", "war", "par", "ktc_value", "market_rank", "iv_rank", "mispricing_pct"]
    with pl.Config(tbl_rows=12, tbl_width_chars=180, fmt_str_lengths=22, float_precision=2):
        print(out.sort("war", descending=True).select([c for c in show if c in out.columns]).head(12))

    teams_df = None
    if rosters is not None:
        rosters = rosters.filter(pl.col("player_id").is_not_null()).with_columns(
            pl.when(pl.col("is_taxi")).then(pl.lit("taxi")).when(pl.col("is_reserve")).then(pl.lit("IR")).otherwise(pl.lit("active")).alias("status"))
        # ownership counts every roster spot (taxi and IR players are owned and tradeable); only active players can start
        owner_of = rosters.select("player_id", pl.col("team_name").alias("owned_by"), "status").unique("player_id")
        pool = out.join(owner_of, on="player_id", how="left")
        teams_list = rosters.select("roster_id", "team_name", "owner_id").unique().sort("roster_id").rows()
        per_team = {}
        for roster_id, name, oid in teams_list:
            mine = rosters.filter(pl.col("roster_id") == roster_id)["player_id"].implode()
            active = rosters.filter((pl.col("roster_id") == roster_id) & (pl.col("status") == "active"))["player_id"].implode()
            r = pool.filter(pl.col("player_id").is_in(active))
            if r.height:
                per_team[roster_id] = (name, oid, mine, r, war.roster_total(r, spec, comps[0].ppg, floor=rep))
        # projected lineups vs the curve: centre the curve on the league's average projected lineup
        offset = war.lineup_offset(curve, [v[4] for v in per_team.values()])
        print(f"  projected lineups: mean {np.mean([v[4] for v in per_team.values()]):.1f} ppg vs curve mean {curve.mean_points:.1f} -> offset {offset:+.1f}")
        rows = []
        for roster_id, (name, oid, mine, r, total) in per_team.items():
            cands = pool.filter(~pl.col("player_id").is_in(mine)).sort("war", descending=True).head(80)
            t = war.team_marginal_war(r, spec, curve, comps, rate, candidates=cands, offset=offset, floor=rep)
            # taxi / IR players of this roster: owned, not in the lineup, no marginal wins
            bench = pool.filter(pl.col("player_id").is_in(mine) & ~pl.col("player_id").is_in(r["player_id"].implode()))
            if bench.height:
                t = pl.concat([t, bench.select("player_id", "player_name", "position", pl.lit(True).alias("rostered")).with_columns(
                    [pl.lit(0.0).alias(c) for c in t.columns if c.startswith("m_")])], how="diagonal_relaxed")
            t = (t.join(cands.select("player_id", "owned_by"), on="player_id", how="left")
                   .join(owner_of.select("player_id", "status"), on="player_id", how="left")
                   .join(out.select("player_id", pl.col("war").alias("league_war"), "ktc_value"), on="player_id", how="left")
                   .with_columns(pl.lit(roster_id).alias("roster_id"), pl.lit(name).alias("team_name"), pl.lit(total).alias("lineup_ppg_now"),
                                 pl.lit(float(curve.win_prob(total + offset))).alias("win_prob_now"), pl.lit(offset).alias("lineup_offset"),
                                 pl.lit(bool(owner_id and oid == owner_id)).alias("is_owner")))
            rows.append(t)
        teams_df = pl.concat(rows, how="diagonal_relaxed")
        print(f"  mean win probability across rosters: {teams_df.group_by('roster_id').agg(pl.col('win_prob_now').first())['win_prob_now'].mean():.3f}")
        with pl.Config(tbl_rows=14, tbl_width_chars=180, fmt_str_lengths=24, float_precision=2):
            print(teams_df.group_by("roster_id", "team_name", "is_owner").agg(pl.col("lineup_ppg_now").first(), pl.col("win_prob_now").first())
                  .sort("lineup_ppg_now", descending=True))
    meta = {"league": spec.to_dict(), "win_curve": curve.to_dict(), "replacement_ppg": rep, "starters": starters, "discount_rate": rate,
            "span": span, "components": [c.k for c in comps], "n_priced": summary["n"], "spearman_war_vs_ktc": summary["spearman"],
            "note": note, "built": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    return out, teams_df, meta


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["inseason", "career"], default="inseason")
    ap.add_argument("--season", type=int)
    ap.add_argument("--week", type=int)
    ap.add_argument("--as-of-season", type=int, default=2025)
    ap.add_argument("--run-date", required=True)
    ap.add_argument("--league", help="JSON league spec (no standings / rosters)")
    ap.add_argument("--all-leagues", action="store_true", help="every in-season league the owner is in (from dim_leagues_meta)")
    ap.add_argument("--discount-rate", type=float, default=value.DEFAULT_DISCOUNT_RATE)
    ap.add_argument("--teams", action="store_true", help="per-roster marginal WAR, trade targets and free agents")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--replacement", choices=["weekly", "fill"], default="weekly",
                    help="replacement line: weekly = the marginal starter among players who actually played each week (injuries, byes); fill = full-season fill")
    ap.add_argument("--dump", help="also write each league's projections / teams parquet to this local directory (for comparisons)")
    args = ap.parse_args()

    if args.source == "inseason" and (args.season is None or args.week is None):
        ap.error("--season/--week required for --source inseason")
    career_meta, career_run = run_meta(args)
    sigma = sigma_from_career_meta(career_meta)
    if args.source == "career":
        proj = gcs_io.read_ml_parquet("intrinsic_value", f"as_of_season={args.as_of_season}", f"run_date={career_run}", "projections.parquet")
        H = sorted(int(c[1:].split("_")[0]) for c in proj.columns if c.startswith("h") and c.endswith("_ppg_hat"))
        comps, span = war.career_components(H), f"preseason off {args.as_of_season}"
    else:
        proj = gcs_io.read_ml_parquet("inseason", f"season={args.season}", f"week={args.week}", f"run_date={args.run_date}", "projections.parquet")
        tail = sorted(int(c[1:].split("_")[0]) for c in proj.columns if c.startswith("h") and c.endswith("_vorp_hat"))
        comps, span = war.inseason_components(tail), f"{args.season} through week {args.week}"
    proj = proj.filter(pl.col("position").is_in(POSITIONS))
    season_fact = features.load_fact_player_season()
    settings = gcs_io.read_lake(SETTINGS_PATH)
    weeks = gcs_io.read_lake(WEEK_PATH)
    tag_base = ("war",)
    tag_tail = (f"season={args.season or args.as_of_season}", f"week={args.week or 0}", f"run_date={args.run_date}")

    targets: list[tuple[lg.LeagueSpec, lg.WinCurve, str | None, str | None, str, pl.DataFrame | None]] = []   # spec, curve, lineage, league_id, note, scale
    if args.league:
        spec = lg.LeagueSpec.from_json(args.league)
        targets.append((spec, win_curve_for(None), None, None, "custom league: no standings or rosters; win curve from all leagues in the lake; primary league's scoring", None))
    else:
        all_leagues = gcs_io.read_lake(LEAGUES_META_PATH).filter(pl.col("status") == "in_season")
        leagues = all_leagues if args.all_leagues else all_leagues.filter(pl.col("league_lineage_id") == _rep.PRIMARY_LINEAGE)
        primary_id = all_leagues.filter(pl.col("league_lineage_id") == _rep.PRIMARY_LINEAGE)["league_id"][0]
        primary_sc = scoring.league_scoring(settings, primary_id)
        last_season = int(weeks["season"].max())
        for lid, name, lineage in leagues.select("league_id", "league_name", "league_lineage_id").iter_rows():
            spec = lg.LeagueSpec.from_settings(settings, lineage, name=slug(name))
            if lineage == _rep.PRIMARY_LINEAGE:
                note, scale = "the model is trained on this league's scoring", None
            else:
                sc = scoring.league_scoring(settings, lid)
                diff = scoring.scoring_diff(primary_sc, sc)
                scale = scoring.ppg_scale(weeks, primary_sc, sc, seasons=[last_season - 2, last_season - 1, last_season]) if diff else None
                note = ("same offensive scoring as the primary league" if not diff else
                        "scoring adjusted per player from the primary league's rules: " + ", ".join(f"{k} {a:g} -> {b:g}" for k, (a, b) in sorted(diff.items())))
            targets.append((spec, win_curve_for(lineage), lineage, lid, note, scale))
        owner_id = owner_in_every_league(leagues) if args.teams else None
    for spec, curve, lineage, lid, note, scale in targets:
        rosters = current_rosters(lid) if (args.teams and lid) else None
        display = next((n for i, n, _ in gcs_io.read_lake(LEAGUES_META_PATH).select("league_id", "league_name", "league_lineage_id").iter_rows() if i == lid), spec.name) if lid else spec.name
        out, teams_df, meta = build_league(spec, curve, proj, comps, sigma, season_fact, args.discount_rate, rosters,
                                           owner_id if args.teams and not args.league else None, span, note, scale=scale,
                                           weeks=weeks, replacement=args.replacement)
        meta.update({"display_name": display, "lineage_id": lineage, "league_id": lid, "replacement": args.replacement})
        if args.dump:
            Path(args.dump).mkdir(parents=True, exist_ok=True)
            out.write_parquet(Path(args.dump) / f"{spec.name}_projections.parquet")
            if teams_df is not None:
                teams_df.write_parquet(Path(args.dump) / f"{spec.name}_teams.parquet")
        if not args.no_write:
            tag = tag_base + (f"league={spec.name}",) + tag_tail
            p = gcs_io.write_ml_parquet(out, *tag, "projections.parquet")
            gcs_io.write_ml_json(meta, *tag, "meta.json")
            if teams_df is not None:
                gcs_io.write_ml_parquet(teams_df, *tag, "teams.parquet")
            print("  wrote", p)


if __name__ == "__main__":
    main()
