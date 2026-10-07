"""silver_fantasy/fact_player_week_status.py: the weekly status grid (played / bye / injured_reserve /
injured_out / suspended / practice_squad / inactive / dnp / not_rostered), the body-part class carried
through a reserve stint, team fill, snap shares through the PFR id, and the season grid."""
import polars as pl

import silver_fantasy.fact_player_week_status as m


def _sched(season=2024, weeks=6, bye=("KC", 3)):
    rows = []
    teams = ["KC", "BUF", "SF", "DAL"]
    for w in range(1, weeks + 1):
        pairs = [("KC", "BUF"), ("SF", "DAL")]
        for h, a in pairs:
            if (h, w) == bye or (a, w) == bye:
                continue
            rows.append(dict(season=season, week=w, game_type="REG", game_id=f"{season}_{w}_{a}_{h}", home_team=h, away_team=a))
    return pl.DataFrame(rows)


def _roster(rows, season=2024):
    base = dict(season=season, week=1, game_type="REG", team="KC", gsis_id="p1", position="WR", status="ACT", status_description_abbr="A01", pfr_id="PfrP1", full_name="Player One")
    return pl.DataFrame([{**base, **r} for r in rows]) if rows else pl.DataFrame([base]).clear()


def _stats(rows, season=2024):
    base = dict(season=season, week=1, player_id="p1", player_name="Player One", position="WR", team="KC", fpts=10.0, targets=6, rush_att=0, rec=4)
    return pl.DataFrame([{**base, **r} for r in rows]) if rows else pl.DataFrame([base]).clear()


def _injury(rows, season=2024):
    base = dict(season=season, week=1, gsis_id="p1", on_injury_report=True, report_status="Out", injury_class="knee_achilles", is_out=True, on_injured_reserve=False)
    df = pl.DataFrame([{**base, **r} for r in rows]) if rows else pl.DataFrame(schema={"season": pl.Int64, "week": pl.Int64, "gsis_id": pl.Utf8, "on_injury_report": pl.Boolean, "report_status": pl.Utf8, "injury_class": pl.Utf8, "is_out": pl.Boolean, "on_injured_reserve": pl.Boolean})
    return df


def _snaps(rows, season=2024):
    base = dict(season=season, week=1, game_type="REG", game_id="g", pfr_player_id="PfrP1", offense_snaps=40.0, offense_pct=0.8)
    return pl.DataFrame([{**base, **r} for r in rows])


def test_one_season_every_status_and_the_carried_body_part():
    # p1 (WR, KC): plays weeks 1-2, bye week 3, Out (knee) week 4, IR weeks 5-6 (no report rows of their own)
    roster = _roster([dict(week=w) for w in (1, 2, 3, 4)] + [dict(week=5, status="RES", status_description_abbr="R01"), dict(week=6, status="RES", status_description_abbr="R01")])
    stats = _stats([dict(week=1), dict(week=2, fpts=14.5, targets=9, rec=7)])
    injury = _injury([dict(week=4)] + [dict(week=5, on_injury_report=False, report_status=None, injury_class="unknown", is_out=True, on_injured_reserve=True),
                                      dict(week=6, on_injury_report=False, report_status=None, injury_class="unknown", is_out=True, on_injured_reserve=True)])
    out = m.build_fact_player_week_status(stats, roster, injury, _sched(), _snaps([dict(week=1), dict(week=2, offense_snaps=55.0, offense_pct=0.95)]))
    p1 = out.filter(pl.col("gsis_id") == "p1").sort("week")
    assert p1["week"].to_list() == [1, 2, 3, 4, 5, 6]                                   # the full regular-season grid
    assert p1["status"].to_list() == ["played", "played", "bye", "injured_out", "injured_reserve", "injured_reserve"]
    assert p1["injury_class"].to_list() == [None, None, None, "knee_achilles", "knee_achilles", "knee_achilles"]   # carried through IR
    assert p1["offense_pct"].to_list()[:2] == [0.8, 0.95] and p1["touches"].to_list()[:2] == [4.0, 7.0] and p1["opportunities"][1] == 9.0
    assert p1["team"].to_list() == ["KC"] * 6 and p1["played"].to_list() == [True, True, False, False, False, False]


def test_suspension_practice_squad_inactive_dnp_and_release():
    roster = _roster([dict(week=1, gsis_id="p2", status="SUS", status_description_abbr="R48", pfr_id=None, full_name="Player Two"),
                      dict(week=2, gsis_id="p2", status="DEV", status_description_abbr="P01", pfr_id=None, full_name="Player Two"),
                      dict(week=3, gsis_id="p2", status="INA", status_description_abbr="A01", pfr_id=None, full_name="Player Two"),
                      dict(week=4, gsis_id="p2", status="ACT", pfr_id=None, full_name="Player Two"),
                      dict(week=5, gsis_id="p2", status="CUT", status_description_abbr="W03", pfr_id=None, full_name="Player Two")])
    out = m.build_fact_player_week_status(_stats([]), roster, _injury([]), _sched(bye=("SF", 3)), None)
    p2 = out.filter(pl.col("gsis_id") == "p2").sort("week")
    assert p2["status"].to_list() == ["suspended", "practice_squad", "inactive", "dnp", "not_rostered", "not_rostered"]   # week 6: no roster row
    assert p2["injury_class"].drop_nulls().len() == 0 and p2["player_name"][0] == "Player Two"


def test_inactive_on_the_report_is_an_injury_and_questionable_who_played_is_played():
    roster = _roster([dict(week=1, status="INA"), dict(week=2, status="ACT")])
    stats = _stats([dict(week=2)])
    injury = _injury([dict(week=1, report_status="Questionable", injury_class="soft_tissue", is_out=False),
                      dict(week=2, report_status="Questionable", injury_class="soft_tissue", is_out=False)])
    out = m.build_fact_player_week_status(stats, roster, injury, _sched(weeks=2), None).filter(pl.col("gsis_id") == "p1").sort("week")
    assert out["status"].to_list() == ["injured_out", "played"]
    assert out["injury_class"].to_list() == ["soft_tissue", None] and out["on_injury_report"].to_list() == [True, True]


def test_universe_and_team_fill_from_the_stat_lines_only():
    # a player with stat lines but no roster rows at all: still on the grid, team from the stat line, misses read not_rostered
    stats = _stats([dict(week=1, player_id="p3", player_name="Three", team="SF"), dict(week=2, player_id="p3", player_name="Three", team="SF")])
    out = m.build_fact_player_week_status(stats, _roster([]).clear(), _injury([]), _sched(weeks=4, bye=("SF", 3)), None).filter(pl.col("gsis_id") == "p3").sort("week")
    assert out["team"].to_list() == ["SF"] * 4 and out["status"].to_list() == ["played", "played", "bye", "not_rostered"]


def test_non_skill_positions_and_pre_2002_are_left_out():
    roster = _roster([dict(week=1, gsis_id="k1", position="K", full_name="Kicker")] + [dict(week=1, gsis_id="old", season=2001)])
    out = m.build_fact_player_week_status(_stats([]).clear(), roster, _injury([]), pl.concat([_sched(weeks=1), _sched(season=2001, weeks=1)]), None)
    assert "k1" not in out["gsis_id"].to_list() and "old" not in out["gsis_id"].to_list()


def test_a_listing_played_through_does_not_name_the_later_reserve_stint():
    # Questionable (shoulder) in week 1 and played; hurt in the week-2 game; IR from week 3 with no report: class unknown
    roster = _roster([dict(week=1), dict(week=2), dict(week=3, status="RES", status_description_abbr="R01"), dict(week=4, status="RES", status_description_abbr="R01")])
    stats = _stats([dict(week=1), dict(week=2)])
    injury = _injury([dict(week=1, report_status="Questionable", injury_class="upper_body", is_out=False),
                      dict(week=3, on_injury_report=False, report_status=None, injury_class="unknown", is_out=True, on_injured_reserve=True),
                      dict(week=4, on_injury_report=False, report_status=None, injury_class="unknown", is_out=True, on_injured_reserve=True)])
    out = m.build_fact_player_week_status(stats, roster, injury, _sched(weeks=4, bye=("SF", 3)), None).filter(pl.col("gsis_id") == "p1").sort("week")
    assert out["status"].to_list() == ["played", "played", "injured_reserve", "injured_reserve"]
    assert out["injury_class"].to_list() == [None, None, None, None]
