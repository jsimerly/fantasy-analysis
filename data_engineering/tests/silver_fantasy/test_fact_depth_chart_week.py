"""silver_fantasy/fact_depth_chart_week.py: the two nflverse eras normalized to one weekly grain,
snapshot -> game-week assignment, best-rank collapse, and movement columns."""
from datetime import datetime

import polars as pl

import silver_fantasy.fact_depth_chart_week as m


def _weekly(rows):
    base = dict(season=2024, week=1, game_type="REG", formation="Offense", club_code="KC", team=None,
                depth_team="1", depth_position="WR", gsis_id="p1", full_name="P One", partition_season=2024)
    return pl.DataFrame([{**base, **r} for r in rows])


def _snap(rows):
    base = dict(dt="2026-09-08T06:00:00Z", team="KC", gsis_id="p1", player_name="P One",
                pos_name="Wide Receiver", pos_abb="WR", pos_slot=1, pos_rank=1, partition_season=2026)
    return pl.DataFrame([{**base, **r} for r in rows])


def _sched(rows):
    base = dict(game_id="g", season=2026, game_type="REG", week=1, gameday="2026-09-10", home_team="KC", away_team="DEN")
    return pl.DataFrame([{**base, **r} for r in rows])


class TestWeeklyEra:
    def test_normalizes_slots_ranks_and_team(self):
        df = _weekly([{"depth_position": "LWR", "depth_team": "2"}, {"gsis_id": "q", "depth_position": "QB", "depth_team": "1", "club_code": "OAK"}])
        out = m.weekly_era(df).sort("gsis_id")
        assert out["position"].to_list() == ["WR", "QB"]
        assert out["depth_rank"].to_list() == [2, 1]
        assert out["position_rank"].to_list() == [None, None]          # unknown in the weekly era
        assert out["team"].to_list() == ["KC", "LV"]
        assert out["source"].to_list() == ["weekly", "weekly"]
        assert out.schema["depth_rank"] == pl.Int32 and out.schema["week"] == pl.Int32

    def test_best_rank_across_slots_and_only_offense_skill_slots(self):
        df = _weekly([{"depth_position": "LWR", "depth_team": "2"}, {"depth_position": "SWR", "depth_team": "1"},
                      {"depth_position": "KR", "depth_team": "1", "formation": "Special Teams"},
                      {"depth_position": "WR", "depth_team": "1", "formation": "Defense"}])
        out = m.weekly_era(df)
        assert out.height == 1 and out["depth_rank"][0] == 1 and out["slot"][0] == "SWR"

    def test_rows_without_week_are_dropped(self):
        out = m.weekly_era(_weekly([{"week": None}, {"week": 3}]))
        assert out["week"].to_list() == [3]


class TestSnapshotEra:
    def test_last_snapshot_before_each_game_week(self):
        sched = _sched([{"week": 1, "gameday": "2026-09-10"}, {"week": 2, "gameday": "2026-09-17", "game_id": "g2"}])
        snaps = _snap([
            {"dt": "2026-03-01T06:00:00Z", "pos_rank": 3},        # offseason -> week 1, but superseded
            {"dt": "2026-09-08T06:00:00Z", "pos_rank": 1},        # last before the week-1 game
            {"dt": "2026-09-12T06:00:00Z", "pos_rank": 2},        # between games -> week 2
            {"dt": "2026-09-15T06:00:00Z", "pos_rank": 2},        # last before the week-2 game
            {"dt": "2026-09-20T06:00:00Z", "pos_rank": 1},        # after the final game -> dropped
        ])
        out = m.snapshot_era(snaps, sched).sort("week")
        assert out["week"].to_list() == [1, 2]
        assert out["depth_rank"].to_list() == [1, 2]
        assert out["snapshot_at"].to_list() == [datetime(2026, 9, 8, 6), datetime(2026, 9, 15, 6)]
        assert out["source"].to_list() == ["snapshot", "snapshot"]
        assert out["season"].to_list() == [2026, 2026]            # from the bronze partition

    def test_whole_team_snapshot_is_kept_together(self):
        # the team's last snapshot decides for every player, even one who vanished from a later-but-partial listing
        sched = _sched([{"week": 1, "gameday": "2026-09-10"}])
        snaps = _snap([{"gsis_id": "a", "dt": "2026-09-07T06:00:00Z", "pos_rank": 1},
                       {"gsis_id": "b", "dt": "2026-09-07T06:00:00Z", "pos_rank": 2},
                       {"gsis_id": "a", "dt": "2026-09-09T06:00:00Z", "pos_rank": 2},
                       {"gsis_id": "b", "dt": "2026-09-09T06:00:00Z", "pos_rank": 1}])
        out = m.snapshot_era(snaps, sched).sort("gsis_id")
        assert out["depth_rank"].to_list() == [2, 1]

    def test_position_from_pos_name_and_non_skill_rows_dropped(self):
        sched = _sched([{"week": 1}])
        snaps = _snap([{"gsis_id": "qb", "pos_name": "Quarterback", "pos_abb": "QB"},
                       {"gsis_id": "lt", "pos_name": "Left Tackle", "pos_abb": "LT"},
                       {"gsis_id": "rb", "pos_name": "Running Back", "pos_abb": "RB"}])
        out = m.snapshot_era(snaps, sched).sort("gsis_id")
        assert out["position"].to_list() == ["QB", "RB"]

    def test_team_without_schedule_rows_is_dropped(self):
        out = m.snapshot_era(_snap([{"team": "XXX"}]), _sched([{"week": 1}]))
        assert out.height == 0

    def test_depth_rank_is_within_slot_and_position_rank_is_overall(self):
        # feed semantics: slot 1 holds overall WR ranks 1, 4, 7...; slot 2 holds 2, 5, 8...
        snaps = _snap([{"gsis_id": "wr1", "pos_slot": 1, "pos_rank": 1}, {"gsis_id": "wr4", "pos_slot": 1, "pos_rank": 4},
                       {"gsis_id": "wr2", "pos_slot": 2, "pos_rank": 2}, {"gsis_id": "wr5", "pos_slot": 2, "pos_rank": 5},
                       {"gsis_id": "qb2", "pos_name": "Quarterback", "pos_abb": "QB", "pos_slot": 9, "pos_rank": 2}])
        out = m.snapshot_era(snaps, _sched([{"week": 1}])).sort("gsis_id")
        assert dict(zip(out["gsis_id"], out["depth_rank"])) == {"qb2": 2, "wr1": 1, "wr2": 1, "wr4": 2, "wr5": 2}
        assert dict(zip(out["gsis_id"], out["position_rank"])) == {"qb2": 2, "wr1": 1, "wr2": 2, "wr4": 4, "wr5": 5}
        assert out.schema["position_rank"] == pl.Int32


class TestBuildAndMovement:
    def test_both_eras_combine_with_movement(self):
        weekly = _weekly([{"week": 1, "depth_team": "2"}, {"week": 2, "depth_team": "1"}, {"week": 4, "depth_team": "1"}])
        depth = pl.concat([weekly, _snap([{"dt": "2026-09-08T06:00:00Z", "pos_rank": 2}])], how="diagonal_relaxed")
        sched = _sched([{"week": 1, "gameday": "2026-09-10"}])
        out = m.build_fact_depth_chart_week(depth, sched)
        w = out.filter(pl.col("season") == 2024).sort("week")
        assert w["prev_depth_rank"].to_list() == [None, 2, 1]
        assert w["depth_rank_change"].to_list() == [None, -1, 0]          # negative = moved up
        assert w["prev_week"].to_list() == [None, 1, 2]
        assert w["is_starter"].to_list() == [False, True, True]
        s = out.filter(pl.col("season") == 2026)
        assert s.height == 1 and s["source"][0] == "snapshot" and s["prev_depth_rank"][0] is None

    def test_movement_does_not_cross_seasons_or_positions(self):
        weekly = _weekly([{"season": 2023, "week": 17, "depth_team": "3", "partition_season": 2023},
                          {"season": 2024, "week": 1, "depth_team": "1"},
                          {"season": 2024, "week": 1, "depth_position": "TE", "depth_team": "2"}])
        out = m.build_fact_depth_chart_week(weekly, _sched([]))
        assert out.filter(pl.col("season") == 2024)["prev_depth_rank"].to_list() == [None, None]

    def test_unique_key(self):
        weekly = _weekly([{"depth_position": "LWR"}, {"depth_position": "RWR"}, {"week": 2}])
        out = m.build_fact_depth_chart_week(weekly, _sched([]))
        assert out.select(m.KEY).is_unique().all()
