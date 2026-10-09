"""Title odds per league: the chance of the playoffs, a bye and the title for every roster from a
Monte Carlo of the rest of the season, and each player's marginal title odds (BACKLOG 40). The lens
next to WAR on the page's Rosters tab: WAR prices a win as a win; this prices the right wins.

Inputs: the latest per-league roster view the page already uses (``war/league=<slug>/.../teams.parquet``
+ ``meta.json`` in the ML bucket: each roster's projected lineup points, each player's lineup gain per
game over the rest of the season ``m_par_1``, the league's weekly spread), and Sleeper live (records,
points for, the remaining matchups, the playoff format).

Run:  machine_learning/.venv/Scripts/python analysis/title_odds.py --out analysis/_cache/title_odds [--publish] [--live-json <saved>]
      --publish writes backtests/title_odds/run_date=<today>/summary.json to the ML bucket, which
      machine_learning/scripts/export_projections.py folds into the page export as ``title_odds``.
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
import title  # noqa: E402

SHIFTS = np.arange(-20.0, 20.5, 1.0)        # points per week, the grid a player's marginal odds interpolate on
NFL_LAST_WEEK = 17                           # the rest-of-season projection runs through the fantasy championship week
HEADERS = {"User-Agent": "Mozilla/5.0 fantasy-analysis/1.0"}


# ------------------------------------------------------------------------------ inputs
def latest_team_views() -> dict[str, tuple[list[str], pl.DataFrame, dict]]:
    """league slug -> (bucket parts, teams.parquet, meta.json) of the newest in-season roster view."""
    out = {}
    for p in sorted(p for p in gcs_io.list_ml("war") if p.endswith("teams.parquet")):
        parts = p.split("/")[:-1]
        slug = next(s for s in parts if s.startswith("league=")).split("=", 1)[1]
        out[slug] = parts                                    # sorted: the last (newest week / run_date) wins
    return {slug: (parts, gcs_io.read_ml_parquet(*parts, "teams.parquet"), gcs_io.read_ml_json(*parts, "meta.json")) for slug, parts in out.items()}


def sleeper_live(league_id: str) -> dict:
    """Records, points for, the playoff format and every week's matchups from Sleeper."""
    import requests
    lg = requests.get(f"https://api.sleeper.app/v1/league/{league_id}", headers=HEADERS, timeout=30).json()
    rs = requests.get(f"https://api.sleeper.app/v1/league/{league_id}/rosters", headers=HEADERS, timeout=30).json()
    st = lg.get("settings") or {}
    standings = [{"roster_id": r["roster_id"], "owner_id": r.get("owner_id"), "wins": (r.get("settings") or {}).get("wins", 0), "losses": (r.get("settings") or {}).get("losses", 0),
                  "ties": (r.get("settings") or {}).get("ties", 0), "fpts": (r.get("settings") or {}).get("fpts", 0) + (r.get("settings") or {}).get("fpts_decimal", 0) / 100} for r in rs]
    weeks = {}
    for wk in range(1, int(st.get("playoff_week_start") or 15)):
        m = requests.get(f"https://api.sleeper.app/v1/league/{league_id}/matchups/{wk}", headers=HEADERS, timeout=30).json()
        weeks[wk] = [{"roster_id": x["roster_id"], "matchup_id": x.get("matchup_id"), "points": x.get("points")} for x in m]
    return {"settings": {k: st.get(k) for k in ("playoff_teams", "playoff_week_start", "playoff_type", "num_teams", "leg", "last_scored_leg")}, "standings": standings, "matchups": weeks}


# ------------------------------------------------------------------------------ the season from the inputs
def build_season(teams: pl.DataFrame, meta: dict, live: dict) -> tuple[title.Season, list[dict], list[int]]:
    """A ``title.Season`` for the simulation plus the roster list (in team-index order) and the weeks simulated."""
    st = live["settings"]
    last_scored = int(st.get("last_scored_leg") or 0)
    rosters = sorted({int(r) for r in teams["roster_id"].to_list()})
    index = {rid: i for i, rid in enumerate(rosters)}
    offset = float(teams["lineup_offset"][0]) if "lineup_offset" in teams.columns else 0.0
    per = teams.group_by("roster_id").agg(pl.col("lineup_ppg_now").first(), pl.col("team_name").first(), pl.col("is_owner").first())
    mu = np.zeros(len(rosters)); names = {}; owner = {}
    for r in per.to_dicts():
        mu[index[int(r["roster_id"])]] = float(r["lineup_ppg_now"]) + offset
        names[int(r["roster_id"])] = r["team_name"]; owner[int(r["roster_id"])] = bool(r["is_owner"])
    wins = np.zeros(len(rosters)); pf = np.zeros(len(rosters)); rec = {}
    for s in live["standings"]:
        rid = int(s["roster_id"])
        if rid in index:
            wins[index[rid]] = s["wins"] + 0.5 * s.get("ties", 0); pf[index[rid]] = s["fpts"]; rec[rid] = s
    weeks = sorted(int(w) for w in live["matchups"] if last_scored < int(w) < int(st["playoff_week_start"]))
    matchups = []
    for w in weeks:
        by_m: dict = {}
        for x in live["matchups"][str(w)] if str(w) in live["matchups"] else live["matchups"][w]:
            if x.get("matchup_id") is None or int(x["roster_id"]) not in index:
                continue
            by_m.setdefault(x["matchup_id"], []).append(index[int(x["roster_id"])])
        matchups.append((w, [(p[0], p[1]) for p in by_m.values() if len(p) == 2]))
    season = title.Season(mu=mu, sd=float(meta["win_curve"]["sd_points"]), wins=wins, pf=pf, matchups=matchups, playoff_teams=int(st["playoff_teams"]))
    roster_list = [{"rid": rid, "name": names.get(rid), "owner": owner.get(rid, False), "wins": int(rec.get(rid, {}).get("wins", 0)), "losses": int(rec.get(rid, {}).get("losses", 0)),
                    "ties": int(rec.get(rid, {}).get("ties", 0)), "pf": round(float(pf[index[rid]]), 1), "mu": round(float(mu[index[rid]]), 1)} for rid in rosters]
    return season, roster_list, weeks


def league_summary(slug: str, teams: pl.DataFrame, meta: dict, live: dict, n_sims: int = 20_000, seed: int = 0) -> dict:
    season, roster_list, weeks = build_season(teams, meta, live)
    base = title.simulate(season, n_sims=n_sims, seed=seed)
    last_scored = int(live["settings"].get("last_scored_leg") or 0)
    weeks_left = max(NFL_LAST_WEEK - last_scored, 1)
    out_teams = []
    for i, t in enumerate(roster_list):
        curve = title.title_curve(season, i, SHIFTS, n_sims=n_sims, seed=seed)
        rows = teams.filter(pl.col("roster_id") == t["rid"])
        own = rows.filter(pl.col("rostered")).sort("m_par_1", descending=True)
        p0_title, p0_play = title.interp(curve, "p_title", 0.0), title.interp(curve, "p_playoffs", 0.0)
        players = []
        for x in own.iter_rows(named=True):
            d_mu = float(x.get("m_par_1") or 0.0) / weeks_left
            players.append({"name": x["player_name"], "pos": x["position"], "d_mu": round(d_mu, 2), "ros_wins": round(float(x.get("m_war_1") or 0.0), 2),
                            "d_title": round(p0_title - title.interp(curve, "p_title", -d_mu), 4), "d_playoffs": round(p0_play - title.interp(curve, "p_playoffs", -d_mu), 4),
                            "status": x.get("status") or "active"})
        targets = []
        for x in rows.filter(~pl.col("rostered")).sort("m_par_1", descending=True).head(30).iter_rows(named=True):
            d_mu = float(x.get("m_par_1") or 0.0) / weeks_left
            targets.append({"name": x["player_name"], "pos": x["position"], "owned_by": x.get("owned_by"), "d_mu": round(d_mu, 2), "ros_wins": round(float(x.get("m_war_1") or 0.0), 2),
                            "ktc": x.get("ktc_value"), "d_title": round(title.interp(curve, "p_title", d_mu) - p0_title, 4), "d_playoffs": round(title.interp(curve, "p_playoffs", d_mu) - p0_play, 4)})
        targets.sort(key=lambda z: -z["d_title"])
        out_teams.append({**t, "p_playoffs": round(float(base["p_playoffs"][i]), 4), "p_bye": round(float(base["p_bye"][i]), 4), "p_title": round(float(base["p_title"][i]), 4),
                          "exp_wins": round(float(base["exp_wins"][i]), 2), "exp_seed": round(float(base["exp_seed"][i]), 2),
                          "curve": {"shift": [float(s) for s in curve["shift"]], "p_title": [round(float(v), 4) for v in curve["p_title"]], "p_playoffs": [round(float(v), 4) for v in curve["p_playoffs"]]},
                          "players": players, "targets": targets[:30]})
    out_teams.sort(key=lambda z: -z["p_title"])
    st = live["settings"]
    return {"slug": slug, "id": meta.get("league_id"), "name": meta.get("display_name", slug), "playoff_teams": int(st["playoff_teams"]), "playoff_week_start": int(st["playoff_week_start"]),
            "byes": title.BYES[int(st["playoff_teams"])], "weeks_simulated": weeks, "last_scored_week": last_scored, "weeks_left_ros": weeks_left, "sd": round(season.sd, 1),
            "top_heaviness": round(float(np.sort(base["p_title"])[-1] - np.sort(base["p_title"])[-2]), 3), "teams": out_teams}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--live-json", default=None, help="a saved dict league_id -> Sleeper live payload (offline development); otherwise fetched")
    ap.add_argument("--n-sims", type=int, default=20_000)
    args = ap.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    live_all = json.loads(Path(args.live_json).read_text(encoding="utf-8")) if args.live_json else {}
    leagues = []
    for slug, (parts, teams, meta) in latest_team_views().items():
        lid = meta.get("league_id")
        if not lid:
            continue
        live = live_all.get(lid) or sleeper_live(lid)
        print(f"{slug}: view {'/'.join(parts[1:])}; {teams['roster_id'].n_unique()} rosters; weeks {live['settings'].get('last_scored_leg')}+1..{int(live['settings']['playoff_week_start']) - 1}", flush=True)
        s = league_summary(slug, teams, meta, live, n_sims=args.n_sims)
        leagues.append(s)
        with pl.Config(tbl_rows=14, tbl_width_chars=160):
            print(pl.DataFrame([{k: t[k] for k in ("name", "wins", "losses", "pf", "mu", "p_playoffs", "p_bye", "p_title", "exp_wins")} for t in s["teams"]]))
    summary = {"run_date": datetime.now(timezone.utc).date().isoformat(), "n_sims": args.n_sims, "shift_grid": [float(x) for x in SHIFTS], "leagues": leagues,
               "lens": "Monte Carlo of the remaining regular-season matchups from each roster's projected lineup points and the league's weekly spread; seeds by record then points for; Sleeper's default bracket, one week per round"}
    (out / "summary.json").write_text(json.dumps(summary, indent=1), encoding="utf-8")
    print(f"wrote {out}")
    if args.publish:
        print("published", gcs_io.write_ml_json(summary, "backtests", "title_odds", f"run_date={summary['run_date']}", "summary.json"))


if __name__ == "__main__":
    main()
