"""Intrinsic value v2: wins above replacement (WAR) for a league, league-average and per roster.

    uv run python scripts/build_war.py --season 2026 --week 3 --run-date 2026-10-01            # owner's league, in-season run
    uv run python scripts/build_war.py ... --league leagues/12team_1qb.json                   # any other league (slots / teams)
    uv run python scripts/build_war.py ... --teams                                            # per-roster marginal WAR + trade targets
    uv run python scripts/build_war.py --source career --run-date 2026-10-01                  # preseason career run

League pieces (``league.py`` / ``lineup.py``): the lineup from ``dim_league_settings`` (or the JSON),
replacement from an explicit fill of that lineup on the last five real seasons, and the win curve
fitted on the league's own standings (a normal-margin curve when a league has no standings yet).
Value (``war.py``): per span, sigma-aware points above replacement turned into wins on that curve;
the rest of this season is the first, undiscounted span.

Writes ``war/league=<name>/season=S/week=W/run_date=D/{projections.parquet, teams.parquet, meta.json}``
to the ML bucket unless --no-write.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

import features  # noqa: E402
import gcs_io  # noqa: E402
import league as lg  # noqa: E402
import lineup  # noqa: E402
import market  # noqa: E402
import value  # noqa: E402
import war  # noqa: E402

SETTINGS_PATH = "silver/fantasy/dim_league_settings/data.parquet"
LEAGUES_META_PATH = "silver/fantasy/dim_leagues_meta/data.parquet"
FRANCHISES_PATH = "silver/fantasy/dim_franchises_meta/data.parquet"
PLAYERS_MASTER_PATH = "silver/fantasy/dim_players_master/data.parquet"
ROSTERS_PREFIX = "bronze/sleeper/rosters/roster_players/daily/"
IDS_PREFIX = "bronze/nflverse/fantasy_player_ids/"
POSITIONS = ["QB", "RB", "WR", "TE"]


MIN_TEAM_WEEKS_FOR_FIT = 300


def win_curve_for(lineage_id: str | None, fallback: lg.WinCurve | None = None) -> lg.WinCurve:
    """The league's win curve. With enough of its own team-weeks a logistic fit; with fewer, a
    normal-margin curve from its own weekly mean / spread (a logistic fit on one season of ten
    teams is too steep to trust); with no standings at all, the fallback."""
    try:
        ts = lg.load_team_state()
        if lineage_id:
            ids = gcs_io.read_lake(LEAGUES_META_PATH).filter(pl.col("league_lineage_id") == lineage_id)["league_id"].to_list()
            ts = ts.filter(pl.col("league_id").is_in(ids))
        tw = lg.team_weeks_from_standings(ts)
        if tw.height >= MIN_TEAM_WEEKS_FOR_FIT:
            return lg.WinCurve.fit(tw)
        mean, sd = float(tw["week_pts"].mean()), float(tw["week_pts"].std())
        print(f"  win curve: {tw.height} team-weeks (< {MIN_TEAM_WEEKS_FOR_FIT}); normal-margin curve from the league's mean {mean:.1f} / sd {sd:.1f}")
        c = lg.WinCurve.normal(mean, sd * 2 ** 0.5)
        c.n = tw.height
        return c
    except Exception as e:  # noqa: BLE001
        print(f"  win curve: could not use standings ({str(e)[:80]}); using the fallback")
        return fallback or lg.WinCurve.normal(113.0, 42.0)


def sigma_from_career_meta(meta: dict) -> dict[int, dict[str, float]]:
    return {int(k): v for k, v in meta.get("ppg_sigma", {}).items()}


def current_rosters(lineage_id: str) -> pl.DataFrame:
    """Latest roster snapshot of the lineage's in-season league, mapped to gsis ids."""
    leagues = gcs_io.read_lake(LEAGUES_META_PATH).filter(pl.col("league_lineage_id") == lineage_id)
    cur = leagues.filter(pl.col("status") == "in_season")
    league_id = (cur if cur.height else leagues.sort("season", descending=True))["league_id"][0]
    from google.cloud import storage
    names = sorted(b.name for b in storage.Client().list_blobs(gcs_io.LAKE_BUCKET, prefix=ROSTERS_PREFIX) if b.name.endswith(".parquet"))
    rp = gcs_io.read_lake(names[-1]).filter(pl.col("league_id") == league_id)
    # Sleeper id -> gsis id via the nflverse crosswalk (every QB/RB/WR/TE on a roster maps; K/DEF do not)
    ids = sorted(b.name for b in storage.Client().list_blobs(gcs_io.LAKE_BUCKET, prefix=IDS_PREFIX) if b.name.endswith(".parquet"))
    xw = (gcs_io.read_lake(ids[-1]).select(pl.col("sleeper_id").cast(pl.Utf8), "gsis_id")
            .filter(pl.col("sleeper_id").is_not_null() & pl.col("gsis_id").is_not_null()).unique("sleeper_id"))
    fr = gcs_io.read_lake(FRANCHISES_PATH).filter(pl.col("league_id") == league_id).select("roster_id", "current_team_name")
    return (rp.join(xw, left_on="player_id", right_on="sleeper_id", how="left").join(fr, on="roster_id", how="left")
              .select("roster_id", pl.col("current_team_name").alias("team_name"), pl.col("gsis_id").alias("player_id"), "is_taxi", "is_reserve"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["inseason", "career"], default="inseason")
    ap.add_argument("--season", type=int)
    ap.add_argument("--week", type=int)
    ap.add_argument("--as-of-season", type=int, default=2025)
    ap.add_argument("--run-date", required=True)
    ap.add_argument("--league", help="JSON league spec; default = the owner's league from dim_league_settings")
    ap.add_argument("--lineage", default=None, help="league lineage for standings/rosters (default: the primary one)")
    ap.add_argument("--discount-rate", type=float, default=value.DEFAULT_DISCOUNT_RATE)
    ap.add_argument("--teams", action="store_true", help="per-roster marginal WAR and trade targets (owner's league only)")
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args()

    settings = gcs_io.read_lake(SETTINGS_PATH)
    import replacement as _rep
    lineage = args.lineage or _rep.PRIMARY_LINEAGE
    if args.league:
        spec = lg.LeagueSpec.from_json(args.league)
        curve = win_curve_for(None)          # all standings in the lake, or the fallback
    else:
        spec = lg.LeagueSpec.from_settings(settings, lineage, name="owner")
        curve = win_curve_for(lineage)
    season = features.load_fact_player_season()
    last = int(season.filter(pl.col("season_complete"))["season"].max())
    hist_seasons = list(range(last - 4, last + 1))
    rep = lineup.replacement_from_history(season.filter(pl.col("position").is_in(POSITIONS)), spec, hist_seasons)
    starters = lineup.starters_per_position(season.filter((pl.col("season") == last) & (pl.col("games") >= 8) & pl.col("position").is_in(POSITIONS)), spec)
    print(f"league {spec.name}: {spec.teams} teams, slots {spec.slots}")
    print(f"  starters by position (explicit fill, {last}): {starters}")
    print(f"  replacement ppg ({hist_seasons[0]}-{hist_seasons[-1]} avg): " + ", ".join(f"{p} {v:.1f}" for p, v in rep.items()))
    print(f"  win curve: mean {curve.mean_points:.1f} sd {curve.sd_points:.1f} n={curve.n}; +10 ppg for an average team = +{10 * curve.slope_at_mean:.3f} win/week")

    career_meta = gcs_io.read_ml_json("intrinsic_value", f"as_of_season={args.as_of_season}", f"run_date={args.run_date}", "metrics.json")
    sigma = sigma_from_career_meta(career_meta)
    if args.source == "career":
        proj = gcs_io.read_ml_parquet("intrinsic_value", f"as_of_season={args.as_of_season}", f"run_date={args.run_date}", "projections.parquet")
        H = sorted(int(c[1:].split("_")[0]) for c in proj.columns if c.startswith("h") and c.endswith("_ppg_hat"))
        comps = war.career_components(H)
        span = f"preseason off {args.as_of_season}"
    else:
        if args.season is None or args.week is None:
            ap.error("--season/--week required for --source inseason")
        proj = gcs_io.read_ml_parquet("inseason", f"season={args.season}", f"week={args.week}", f"run_date={args.run_date}", "projections.parquet")
        tail = sorted(int(c[1:].split("_")[0]) for c in proj.columns if c.startswith("h") and c.endswith("_vorp_hat"))
        comps = war.inseason_components(tail)
        span = f"{args.season} through week {args.week}"
    proj = proj.filter(pl.col("position").is_in(POSITIONS))
    out = war.wins_above_replacement(proj, rep, curve, comps, args.discount_rate, sigma=sigma)
    out = out.with_columns(pl.col("war").rank(method="ordinal", descending=True).cast(pl.Int64).alias("war_rank_all"))

    cmp_cols = ["player_id", "fair_value", "mispricing", "mispricing_pct", "iv_rank", "market_rank", "rank_gap"]
    if "ktc_value" in out.columns and out["ktc_value"].is_not_null().sum() > 10:
        cmp, summary = value.compare_to_market(out, iv_col="war", method="rank")
        out = out.drop([c for c in cmp_cols[1:] if c in out.columns]).join(cmp.select(cmp_cols), on="player_id", how="left")
        print(f"  {summary['n']} priced by KTC; spearman(WAR, KTC) = {summary['spearman']:.3f}")
        priced = out.filter(pl.col("ktc_value").is_not_null())
        sh = priced.group_by("position").agg(pl.col("war").sum().alias("w"), pl.col("par").sum().alias("p"), pl.col("ktc_value").sum().alias("k"))
        tot = sh.select(pl.col("w").sum(), pl.col("p").sum(), pl.col("k").sum()).row(0)
        print("  share of value by position (WAR | PAR | KTC): " + ", ".join(f"{r['position']} {r['w']/tot[0]:.0%}|{r['p']/tot[1]:.0%}|{r['k']/tot[2]:.0%}" for r in sh.sort("position").iter_rows(named=True)))
    show = ["player_name", "position", "age", "war_1", "war_2", "war", "par", "ktc_value", "market_rank", "iv_rank", "mispricing_pct"]
    with pl.Config(tbl_rows=30, tbl_width_chars=200, fmt_str_lengths=22, float_precision=2):
        print(f"\nTop 25 by WAR ({span}; war_1 = the current span, undiscounted; WAR in wins):")
        print(out.sort("war", descending=True).with_columns(pl.col("age_at_season").round(0).alias("age")).select([c for c in show if c in out.columns]).head(25))

    teams_df = None
    if args.teams and not args.league:
        rosters = current_rosters(lineage).filter(~pl.col("is_taxi") & pl.col("player_id").is_not_null())
        owner_of = rosters.select("player_id", pl.col("team_name").alias("owned_by")).unique("player_id")
        pool = out.join(owner_of, on="player_id", how="left")                      # owned_by null = free agent
        rows = []
        for roster_id, name in rosters.select("roster_id", "team_name").unique().sort("roster_id").iter_rows():
            mine = rosters.filter(pl.col("roster_id") == roster_id)["player_id"].implode()
            r = pool.filter(pl.col("player_id").is_in(mine))
            if r.height == 0:
                continue
            # candidates: the best 80 players NOT on this roster (other teams' players = trade targets, unowned = free agents)
            cands = pool.filter(~pl.col("player_id").is_in(mine)).sort("war", descending=True).head(80)
            total = war.roster_total(r, spec, comps[0].ppg)
            t = war.team_marginal_war(r, spec, curve, comps, args.discount_rate, candidates=cands)
            t = (t.join(cands.select("player_id", "owned_by"), on="player_id", how="left")
                   .with_columns(pl.lit(roster_id).alias("roster_id"), pl.lit(name).alias("team_name"), pl.lit(total).alias("lineup_ppg_now"),
                                 pl.lit(float(curve.win_prob(total))).alias("win_prob_now")))
            rows.append(t)
        teams_df = pl.concat(rows)
        with pl.Config(tbl_rows=12, tbl_width_chars=200, fmt_str_lengths=24, float_precision=2):
            print("\nRosters: projected starting lineup now (per week) and win probability on the league's curve:")
            print(teams_df.group_by("roster_id", "team_name").agg(pl.col("lineup_ppg_now").first(), pl.col("win_prob_now").first(),
                                                                   pl.col("m_war").filter(pl.col("rostered")).sum().alias("roster_marginal_war")).sort("lineup_ppg_now", descending=True))
            for roster_id, name in rosters.select("roster_id", "team_name").unique().sort("roster_id").iter_rows():
                t = teams_df.filter(pl.col("roster_id") == roster_id)
                own = t.filter(pl.col("rostered")).sort("m_war", descending=True).head(5)
                trade = t.filter(~pl.col("rostered") & pl.col("owned_by").is_not_null()).sort("m_war", descending=True).head(5)
                free = t.filter(~pl.col("rostered") & pl.col("owned_by").is_null()).sort("m_war", descending=True).head(3)
                print(f"\n  {name} (roster {roster_id}): most valuable to THIS roster: " + ", ".join(f"{n} {w:.2f}" for n, w in zip(own['player_name'], own['m_war'])))
                print("    trade targets (what they would add here): " + ", ".join(f"{n} +{w:.2f} ({o})" for n, w, o in zip(trade['player_name'], trade['m_war'], trade['owned_by'])))
                print("    free agents: " + ", ".join(f"{n} +{w:.2f}" for n, w in zip(free['player_name'], free['m_war'])))

    if not args.no_write:
        tag = ("war", f"league={spec.name}", f"season={args.season or args.as_of_season}", f"week={args.week or 0}", f"run_date={args.run_date}")
        p = gcs_io.write_ml_parquet(out, *tag, "projections.parquet")
        gcs_io.write_ml_json({"league": spec.to_dict(), "win_curve": curve.to_dict(), "replacement_ppg": rep, "starters": starters,
                              "discount_rate": args.discount_rate, "source": args.source, "span": span, "components": [c.k for c in comps],
                              "built": datetime.now(timezone.utc).isoformat(timespec="seconds")}, *tag, "meta.json")
        if teams_df is not None:
            gcs_io.write_ml_parquet(teams_df, *tag, "teams.parquet")
        print("\nwrote", p)


if __name__ == "__main__":
    main()
