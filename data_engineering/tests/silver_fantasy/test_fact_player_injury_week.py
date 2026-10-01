"""silver_fantasy/fact_player_injury_week.py: report normalization, de-duplication, body-part
classes, reserve-list rows from weekly rosters, and the is_out rule."""
from datetime import datetime

import polars as pl

import silver_fantasy.fact_player_injury_week as m


def _inj(rows):
    base = dict(season=2024, week=1, game_type="REG", team="KC", gsis_id="p1", full_name="P One", position="WR",
                report_status=None, report_primary_injury=None, report_secondary_injury=None,
                practice_status=None, practice_primary_injury=None, practice_secondary_injury=None,
                date_modified=None)
    overrides = {"date_modified": pl.Datetime("us"), "report_status": pl.Utf8, "report_primary_injury": pl.Utf8,
                 "report_secondary_injury": pl.Utf8, "practice_status": pl.Utf8, "practice_primary_injury": pl.Utf8,
                 "practice_secondary_injury": pl.Utf8}
    df = pl.DataFrame([{**base, **r} for r in (rows or [{}])], schema_overrides=overrides)
    return df if rows else df.clear()


def _rw(rows):
    base = dict(season=2024, week=1, game_type="REG", team="KC", gsis_id="p1", full_name="P One", position="WR", status="ACT")
    return pl.DataFrame([{**base, **r} for r in rows])


class TestNormalizeReport:
    def test_keys_cast_from_old_float_files(self):
        inj = _inj([{"season": 2012.0, "week": 3.0, "report_status": "Out"}])
        out = m.normalize_report(inj.with_columns(pl.col("season").cast(pl.Float64), pl.col("week").cast(pl.Float64)))
        assert out.schema["season"] == pl.Int32 and out.schema["week"] == pl.Int32
        assert out.row(0, named=True)["week"] == 3

    def test_latest_update_wins_then_severity(self):
        inj = _inj([
            {"report_status": "Questionable", "date_modified": datetime(2024, 9, 7, 10)},
            {"report_status": "Out", "date_modified": datetime(2024, 9, 8, 12)},       # later update
        ])
        out = m.normalize_report(inj)
        assert out.height == 1 and out["report_status"][0] == "Out"
        tie = _inj([{"report_status": "Questionable"}, {"report_status": "Doubtful"}])  # no timestamps (2026 files)
        assert m.normalize_report(tie)["report_status"][0] == "Doubtful"

    def test_primary_injury_falls_back_to_practice_report_and_blank_is_null(self):
        inj = _inj([{"gsis_id": "a", "report_primary_injury": "", "practice_primary_injury": "Hamstring"},
                    {"gsis_id": "b", "report_primary_injury": "Knee"}])
        out = m.normalize_report(inj).sort("gsis_id")
        assert out["primary_injury"].to_list() == ["Hamstring", "Knee"]
        assert out["injury_class"].to_list() == ["soft_tissue", "knee_achilles"]

    def test_classes_and_practice_participation(self):
        parts = ["Concussion", "Not injury related - personal matter", "Illness", "Groin", "Achilles", "Ankle",
                 "Shoulder", "Ribs", "Eye", None]
        inj = _inj([{"gsis_id": f"p{i}", "report_primary_injury": p} for i, p in enumerate(parts)])
        out = m.normalize_report(inj).sort("gsis_id")
        assert out["injury_class"].to_list() == ["concussion", "not_injury", "not_injury", "soft_tissue", "knee_achilles",
                                                 "ankle_foot", "upper_body", "upper_body", "other", "none"]
        prac = ["Full Participation in Practice", "Limited Participation in Practice",
                "Did Not Participate In Practice", "Out (Definitely Will Not Play)", "", None]
        inj = _inj([{"gsis_id": f"p{i}", "practice_status": p} for i, p in enumerate(prac)])
        out = m.normalize_report(inj).sort("gsis_id")
        assert out["practice_participation"].to_list() == ["full", "limited", "dnp", "out", None, None]

    def test_game_status_rank(self):
        inj = _inj([{"gsis_id": f"p{i}", "report_status": s} for i, s in enumerate(["Out", "Doubtful", "Questionable", "Probable", None])])
        assert m.normalize_report(inj).sort("gsis_id")["game_status_rank"].to_list() == [4, 3, 2, 1, 0]

    def test_team_codes_canonicalized_like_fact_player_week(self):
        out = m.normalize_report(_inj([{"team": "OAK"}]))
        assert out["team"][0] == "LV"


class TestBuild:
    def test_reserve_list_players_are_added_with_unknown_cause(self):
        inj = _inj([{"gsis_id": "p1", "report_status": "Questionable", "report_primary_injury": "Knee"}])
        rw = _rw([{"gsis_id": "p1", "status": "ACT"},
                  {"gsis_id": "ir", "full_name": "On Reserve", "position": "RB", "status": "RES"},
                  {"gsis_id": "fit", "status": "ACT"}])                           # healthy, not listed -> no row
        out = m.build_fact_player_injury_week(inj, rw).sort("gsis_id")
        assert out["gsis_id"].to_list() == ["ir", "p1"]
        ir = out.filter(pl.col("gsis_id") == "ir").row(0, named=True)
        assert ir["on_injury_report"] is False and ir["on_injured_reserve"] is True and ir["is_out"] is True
        assert ir["injury_class"] == "unknown" and ir["report_status"] is None
        assert ir["player_name"] == "On Reserve" and ir["position"] == "RB" and ir["team"] == "KC"
        p1 = out.filter(pl.col("gsis_id") == "p1").row(0, named=True)
        assert p1["on_injury_report"] is True and p1["roster_status"] == "ACT" and p1["is_out"] is False

    def test_is_out_rule(self):
        inj = _inj([{"gsis_id": "o", "report_status": "Out"}, {"gsis_id": "d", "report_status": "Doubtful"},
                    {"gsis_id": "q", "report_status": "Questionable"}])
        rw = _rw([{"gsis_id": "o"}, {"gsis_id": "d"}, {"gsis_id": "q"}, {"gsis_id": "pup", "status": "PUP"}])
        out = m.build_fact_player_injury_week(inj, rw).sort("gsis_id")
        assert dict(zip(out["gsis_id"], out["is_out"])) == {"d": False, "o": True, "pup": True, "q": False}

    def test_one_row_per_player_week_and_sorted(self):
        inj = _inj([{"week": 2, "report_status": "Out"}, {"week": 1, "report_status": "Questionable"}])
        rw = _rw([{"week": 1}, {"week": 2}, {"week": 1, "team": "DEN"}])             # duplicate roster row
        out = m.build_fact_player_injury_week(inj, rw)
        assert out.select("season", "week", "gsis_id").is_unique().all()
        assert out["week"].to_list() == [1, 2]

    def test_works_without_roster_names(self):
        rw = _rw([{"gsis_id": "ir", "status": "RES"}]).drop("full_name")
        out = m.build_fact_player_injury_week(_inj([]), rw)
        assert out.height == 1 and out["player_name"][0] is None
