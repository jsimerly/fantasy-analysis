"""silver_fantasy/dim_league_events.py — league-specific dated events.

Covers draft_events (startup vs rookie by round count) and fantasy_end_events
(playoff_week_start + ceil(log2(teams)) -> final week -> schedule date).
"""
import polars as pl

from tests.de_loader import load_de_module

mod = load_de_module("silver_fantasy/dim_league_events.py", "silver_fantasy", "dim_league_events")
draft_events = mod.draft_events
fantasy_end_events = mod.fantasy_end_events

_LEAGUES = pl.DataFrame({
    "league_id": ["L21", "L22"],
    "league_lineage_id": ["LIN", "LIN"],
    "season": [2021, 2022],
})


def _drafts(rows):
    # rows: (draft_id, league_id, season, rounds, start_time_ms, last_picked_ms)
    return pl.DataFrame({
        "draft_id": [r[0] for r in rows], "league_id": [r[1] for r in rows],
        "season": [r[2] for r in rows], "rounds": [r[3] for r in rows],
        "start_time": [r[4] for r in rows], "last_picked": [r[5] for r in rows],
    })


class TestDraftEvents:
    def test_startup_vs_rookie_by_round_count(self):
        # 2021 = 25-round startup; 2022 = 4-round rookie. ts: 2021-09-07, 2022-07-27 (ms epoch)
        out = draft_events(_drafts([
            ("d21", "L21", 2021, 25, 1631016000000, 1631016000000),
            ("d22", "L22", 2022, 4, 1658880000000, 1658880000000),
        ]), _LEAGUES)
        types = {r["season"]: r["event_type"] for r in out.to_dicts()}
        assert types[2021] == "startup_draft"
        assert types[2022] == "rookie_draft"

    def test_uses_start_time_then_last_picked(self):
        # start_time missing -> fall back to last_picked
        out = draft_events(_drafts([("d22", "L22", 2022, 4, None, 1658880000000)]), _LEAGUES)
        assert out.to_dicts()[0]["event_date"].year == 2022


def _settings(rows):
    # rows: (league_id, playoff_week_start, playoff_teams)
    return pl.DataFrame({
        "league_id": [r[0] for r in rows],
        "playoff_week_start": [r[1] for r in rows],
        "playoff_teams": [r[2] for r in rows],
        "is_current": [True for _ in rows],
    })


def _schedules():
    # season 2022 REG weeks 16 & 17 with end dates
    rows = [(2022, "REG", 16, "2022-12-25"), (2022, "REG", 17, "2023-01-01"),
            (2022, "REG", 17, "2022-12-31")]
    return pl.DataFrame({"season": [r[0] for r in rows], "game_type": [r[1] for r in rows],
                         "week": [r[2] for r in rows], "gameday": [r[3] for r in rows]})


class TestFantasyEndEvents:
    def test_final_week_from_playoff_settings(self):
        # 4 teams -> ceil(log2 4)=2 rounds; pws=16 -> final week 17; date = max gameday wk17
        out = fantasy_end_events(_settings([("L22", 16, 4)]), _LEAGUES, _schedules())
        from datetime import date
        r = out.to_dicts()[0]
        assert r["event_type"] == "fantasy_end"
        assert r["season"] == 2022
        assert r["event_date"] == date(2023, 1, 1)   # latest wk17 gameday

    def test_six_teams_three_rounds(self):
        # 6 teams -> ceil(log2 6)=3 rounds; pws=15 -> final week 17
        sch = _schedules()
        out = fantasy_end_events(_settings([("L22", 15, 6)]), _LEAGUES, sch)
        # final_week should be 17 -> resolves to a date (not null)
        assert out.to_dicts()[0]["event_date"] is not None
