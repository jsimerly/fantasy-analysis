"""fact_college_player_season: the wide box score, shares against team volume, breakout flags, and the crosswalk."""
from __future__ import annotations

import polars as pl

from silver_fantasy import fact_college_player_season as m


def _stats():
    rows = []
    for season, (yds, td, rec) in {2021: (600, 4, 40), 2022: (1300, 12, 90)}.items():
        for st, v in (("YDS", yds), ("TD", td), ("REC", rec)):
            rows.append({"season": season, "playerId": 7, "player": "Marvin Harrison Jr.", "position": "WR", "team": "Ohio State", "conference": "B1G", "category": "receiving", "statType": st, "stat": str(v)})
    rows.append({"season": 2022, "playerId": 7, "player": "Marvin Harrison Jr.", "position": "WR", "team": "Ohio State", "conference": "B1G", "category": "rushing", "statType": "YDS", "stat": "20"})
    rows.append({"season": 2022, "playerId": 7, "player": "Marvin Harrison Jr.", "position": "WR", "team": "Ohio State", "conference": "B1G", "category": "receiving", "statType": "LONG", "stat": "75"})
    return pl.DataFrame(rows)


def _team_stats():
    rows = []
    for season in (2021, 2022):
        for name, v in (("passAttempts", 400), ("rushingAttempts", 500), ("totalYards", 6000), ("passingTDs", 30), ("rushingTDs", 25), ("games", 13)):
            rows.append({"season": season, "team": "Ohio State", "conference": "B1G", "statName": name, "statValue": v})
    return pl.DataFrame(rows)


def test_build_fact_wide_shares_and_breakout():
    usage = pl.DataFrame({"season": [2021, 2022], "id": [7, 7], "name": ["M H"] * 2, "position": ["WR"] * 2, "team": ["Ohio State"] * 2, "usage_overall": [0.12, 0.26], "usage_pass": [0.2, 0.4]})
    rosters = pl.DataFrame({"season": [2021, 2022], "id": [7, 7], "year": [1, 2], "height": [76, 76], "weight": [205, 209]})
    sp = pl.DataFrame({"season": [2021, 2022], "team": ["Ohio State"] * 2, "rating": [28.0, 30.5], "ranking": [4, 2], "offense_rating": [44.0, 46.0]})
    f = m.build_fact(_stats(), usage, rosters, sp, _team_stats())
    assert f.height == 2
    r22 = f.filter(pl.col("season") == 2022).row(0, named=True)
    assert r22["rec_yds"] == 1300 and r22["rec_td"] == 12 and r22["rec"] == 90 and r22["rush_yds"] == 20
    assert r22["team_plays"] == 900 and r22["team_td"] == 55 and r22["class_year"] == 2 and r22["team_sp"] == 30.5 and r22["team_sp_rank"] == 2
    assert abs(r22["yards_share"] - 1320 / 6000) < 1e-9 and abs(r22["td_share"] - 12 / 55) < 1e-9
    assert abs(r22["dominator"] - (1320 / 6000 + 12 / 55) / 2) < 1e-9 and abs(r22["yards_per_team_play"] - 1320 / 900) < 1e-9
    assert r22["college_season_no"] == 2 and r22["first_season"] == 2021 and r22["last_season"] == 2022
    assert r22["breakout_season_dom"] == 2022 and r22["breakout_season_usage"] == 2022     # 2021: 600/6000 + 4/55 -> 0.086; 2022: 0.219
    assert "LONG" not in f.columns


def test_build_fact_without_optional_feeds():
    f = m.build_fact(_stats(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame())
    assert f.height == 2 and f["dominator"].null_count() == 2 and "usage_overall" not in f.columns


def test_crosswalk_by_pick_then_name():
    picks = pl.DataFrame({"collegeAthleteId": [7, 8, 9], "year": [2024, 2024, 2023], "overall": [4, 300, 50], "round": [1, 7, 2],
                          "name": ["Marvin Harrison Jr.", "Some Guy", "Other Player"], "position": ["WR", "RB", "TE"], "collegeTeam": ["Ohio State", "X", "Y"]})
    ids = pl.DataFrame({"gsis_id": ["00-1", "00-2", None], "draft_year": [2024, 2024, 2023], "draft_pick": [4, 301, 50], "name": ["Marvin Harrison", "Some Guy", "Other Player"],
                        "position": ["WR", "RB", "TE"], "birthdate": ["2002-08-07", "2001-01-01", None]})
    x = m.build_crosswalk(picks, ids)
    r = {row["cfbd_id"]: row for row in x.iter_rows(named=True)}
    assert r["7"]["gsis_id"] == "00-1" and r["7"]["match"] == "pick" and r["7"]["birth_date"] == "2002-08-07"
    assert r["8"]["gsis_id"] == "00-2" and r["8"]["match"] == "name"          # pick 300 vs 301: the name carries it
    assert r["9"]["gsis_id"] is None and r["9"]["match"] is None
    assert x["draft_year"].to_list() and x["college"].to_list()
