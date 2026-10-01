"""Injury-aware weekly replacement: starters who are out push the marginal available player deeper."""
import polars as pl

import lineup
from league import LeagueSpec


def _spec():
    return LeagueSpec(name="t", teams=2, slots={"RB": 1}, eligibility={"RB": ["RB"]})


def _season():
    return pl.DataFrame({"player_id": [f"p{i}" for i in range(5)], "season": [2024] * 5, "position": ["RB"] * 5,
                         "games": [17] * 5, "ppg": [20.0, 18.0, 15.0, 12.0, 9.0]})


def test_matches_full_fill_when_everyone_plays():
    weeks = pl.DataFrame({"season": [2024] * 10, "week": [1] * 5 + [2] * 5, "player_id": [f"p{i}" for i in range(5)] * 2})
    out = lineup.replacement_weekly(_season(), weeks, _spec(), [2024])
    assert out == {"RB": 15.0}
    assert out == lineup.replacement_from_history(_season(), _spec(), [2024])


def test_out_starter_pushes_replacement_deeper():
    # week 1: all play (rep = 3rd best, 15); week 2: the best RB is out, so the 2 starters are p1, p2 and rep = p3 (12)
    weeks = pl.DataFrame({"season": [2024] * 9, "week": [1] * 5 + [2] * 4,
                          "player_id": [f"p{i}" for i in range(5)] + [f"p{i}" for i in range(1, 5)]})
    out = lineup.replacement_weekly(_season(), weeks, _spec(), [2024])
    assert out == {"RB": (15.0 + 12.0) / 2}


def test_postseason_weeks_are_ignored():
    weeks = pl.DataFrame({"season": [2024] * 7, "week": [1] * 5 + [19] * 2, "player_id": [f"p{i}" for i in range(5)] + ["p0", "p1"]})
    assert lineup.replacement_weekly(_season(), weeks, _spec(), [2024]) == {"RB": 15.0}
