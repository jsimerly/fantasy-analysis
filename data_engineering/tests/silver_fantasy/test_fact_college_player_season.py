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


def test_crosswalk_reads_the_season_facts_birth_date_column():
    """Production passes our fact_player_season (birth_date, a date), not nflverse's ids (birthdate); looking only for the
    latter left every crosswalk birth date empty until 2026-10-12 (and the model's breakout ages with them)."""
    from datetime import date
    picks = pl.DataFrame({"collegeAthleteId": [7], "year": [2024], "overall": [13], "round": [1], "name": ["Brock Bowers"], "position": ["TE"], "collegeTeam": ["Georgia"]})
    ids = pl.DataFrame({"gsis_id": ["00-0039338"], "draft_year": [2024], "draft_pick": [13], "name": ["Brock Bowers"], "position": ["TE"], "birth_date": [date(2002, 12, 13)]})
    r = m.build_crosswalk(picks, ids).row(0, named=True)
    assert r["gsis_id"] == "00-0039338" and r["birth_date"] == "2002-12-13"


def test_the_id_table_main_builds_carries_birth_dates_from_both_sources():
    """Replay of the 2026-10-12 defect's second half: main assembled the crosswalk's id table from our season fact and
    nflverse's ids but selected neither birth-date column, so the crosswalk had nothing to read."""
    from datetime import date
    season_fact = pl.DataFrame({"player_id": ["g1", "g1"], "season": [2024, 2025], "rookie_season": [2024, 2024], "draft_pick": [13, 13],
                                "player_name": ["Brock Bowers"] * 2, "position": ["TE"] * 2, "birth_date": [date(2002, 12, 13)] * 2})
    nfl_ids = pl.DataFrame({"gsis_id": ["g1", "g2"], "draft_year": [2024, 2023], "draft_pick": [13, 200], "name": ["Brock Bowers", "Late Guy"],
                            "position": ["TE", "WR"], "birthdate": ["2002-12-13", "2000-05-01"], "db_season": [2026, 2026]})
    ids = m.crosswalk_ids(season_fact, nfl_ids)
    got = dict(zip(ids["gsis_id"].to_list(), ids["birth_date"].to_list()))
    assert got == {"g1": "2002-12-13", "g2": "2000-05-01"}
    picks = pl.DataFrame({"collegeAthleteId": [7, 8], "year": [2024, 2023], "overall": [13, 200], "round": [1, 7], "name": ["Brock Bowers", "Late Guy"],
                          "position": ["TE", "WR"], "collegeTeam": ["Georgia", "Somewhere"]})
    xw = m.build_crosswalk(picks, ids)
    assert xw["birth_date"].null_count() == 0


def test_nflverse_ids_join_the_college_pick_by_the_overall_pick_not_the_pick_within_the_round():
    """fantasy_player_ids.draft_pick is the pick within the round (Puka Nacua: round 5, pick 42, overall 177); the CFBD
    join needs the overall pick (draft_ovr). Before the fix a first-round defender at overall 42 took Nacua's id."""
    season_fact = pl.DataFrame({"player_id": ["g-ours"], "season": [2024], "rookie_season": [2024], "draft_pick": [13], "player_name": ["Brock Bowers"],
                                "position": ["TE"], "birth_date": [None]}).with_columns(pl.col("birth_date").cast(pl.Utf8))
    nfl_ids = pl.DataFrame({"gsis_id": ["g-puka", "g-dl"], "draft_year": [2023, 2023], "draft_round": [5, 1], "draft_pick": [42, 10], "draft_ovr": [177, 10],
                            "name": ["Puka Nacua", "Some Lineman"], "position": ["WR", "DL"], "birthdate": ["2001-05-29", "2000-01-01"], "db_season": [2026, 2026]})
    ids = m.crosswalk_ids(season_fact, nfl_ids)
    picks = pl.DataFrame({"collegeAthleteId": [1, 2], "year": [2023, 2023], "overall": [177, 42], "round": [5, 2], "name": ["Puka Nacua", "Other Guy"],
                          "position": ["Wide Receiver", "Wide Receiver"], "collegeTeam": ["BYU", "Somewhere"]})
    xw = {r["cfbd_id"]: r for r in m.build_crosswalk(picks, ids).to_dicts()}
    assert xw["1"]["gsis_id"] == "g-puka" and xw["1"]["match"] == "pick" and xw["1"]["birth_date"] == "2001-05-29"
    assert xw["2"]["gsis_id"] is None                                       # overall 42 is nobody in these ids
