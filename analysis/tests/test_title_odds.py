"""title_odds: the season built from a roster view and a Sleeper payload; a player's marginal odds."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import title_odds as T  # noqa: E402


def _inputs():
    teams = pl.DataFrame({
        "roster_id": [1, 1, 2, 2, 3, 4], "team_name": ["A", "A", "B", "B", "C", "D"], "is_owner": [True, True, False, False, False, False],
        "lineup_ppg_now": [130.0, 130.0, 120.0, 120.0, 110.0, 100.0], "lineup_offset": [5.0] * 6,
        "player_name": ["a1", "a2", "b1", "x", "c1", "d1"], "position": ["QB", "RB", "WR", "WR", "TE", "RB"],
        "rostered": [True, True, True, False, True, True], "m_par_1": [65.0, 13.0, 26.0, 39.0, 13.0, 0.0], "m_war_1": [1.0, 0.2, 0.4, 0.6, 0.2, 0.0],
        "owned_by": [None, None, None, "C", None, None], "status": ["active"] * 6, "ktc_value": [5000, 2000, 3000, 2500, 1000, 500],
    })
    meta = {"win_curve": {"sd_points": 20.0}, "league_id": "L", "display_name": "Toy"}
    live = {"settings": {"playoff_teams": 4, "playoff_week_start": 7, "last_scored_leg": 4},
            "standings": [{"roster_id": 1, "wins": 3, "losses": 1, "ties": 0, "fpts": 520.0}, {"roster_id": 2, "wins": 2, "losses": 2, "ties": 0, "fpts": 480.0},
                          {"roster_id": 3, "wins": 2, "losses": 2, "ties": 0, "fpts": 470.0}, {"roster_id": 4, "wins": 1, "losses": 3, "ties": 0, "fpts": 400.0}],
            "matchups": {"5": [{"roster_id": 1, "matchup_id": 1}, {"roster_id": 2, "matchup_id": 1}, {"roster_id": 3, "matchup_id": 2}, {"roster_id": 4, "matchup_id": 2}],
                         "6": [{"roster_id": 1, "matchup_id": 1}, {"roster_id": 3, "matchup_id": 1}, {"roster_id": 2, "matchup_id": 2}, {"roster_id": 4, "matchup_id": 2}],
                         "4": [{"roster_id": 1, "matchup_id": 1}, {"roster_id": 2, "matchup_id": 1}]}}       # already scored: ignored
    return teams, meta, live


def test_build_season_uses_the_offset_the_records_and_only_the_unscored_regular_season_weeks():
    teams, meta, live = _inputs()
    season, rosters, weeks = T.build_season(teams, meta, live)
    assert weeks == [5, 6] and [m[0] for m in season.matchups] == [5, 6] and season.matchups[0][1] == [(0, 1), (2, 3)]
    assert np.allclose(season.mu, [135.0, 125.0, 115.0, 105.0]) and season.wins.tolist() == [3, 2, 2, 1] and season.sd == 20.0
    assert rosters[0]["name"] == "A" and rosters[0]["owner"] is True and rosters[3]["pf"] == 400.0


def test_league_summary_ranks_teams_and_prices_players_and_targets_in_title_odds():
    teams, meta, live = _inputs()
    s = T.league_summary("toy", teams, meta, live, n_sims=3000, seed=0)
    assert s["playoff_teams"] == 4 and s["weeks_left_ros"] == 13 and [t["name"] for t in s["teams"]][0] == "A"
    a = s["teams"][0]
    assert abs(sum(t["p_title"] for t in s["teams"]) - 1.0) < 1e-6 and a["p_playoffs"] == 1.0            # four of four make it
    qb, rb = a["players"][0], a["players"][1]
    assert qb["name"] == "a1" and abs(qb["d_mu"] - 5.0) < 1e-9 and qb["d_title"] > rb["d_title"] >= 0.0   # 65 points over 13 weeks
    b = next(t for t in s["teams"] if t["name"] == "B")                                                  # x is a candidate in B's view, owned by C
    assert b["targets"][0]["name"] == "x" and b["targets"][0]["owned_by"] == "C" and b["targets"][0]["d_title"] >= 0.0 and len(a["curve"]["shift"]) == len(T.SHIFTS)
