"""build_war: Sleeper's fantasy position for rostered players and the roster placeholders (review 2026-10-13, D4: a
rostered player with no projection -- a devy pick, a rookie without an NFL row -- vanished from the roster view)."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import build_war as bw  # noqa: E402


def test_sleeper_names_resolve_the_fantasy_position():
    dim = pl.DataFrame({"player_key": ["1", "2", "3", None], "display_name": ["Travis Hunter", "Kyle Juszczyk", "Some Corner", "Ghost"],
                        "position": ["DB", "FB", "CB", "WR"], "fantasy_positions": ["DB/WR", "RB", "CB", "WR"]})
    out = {r["_sid"]: r for r in bw.sleeper_names(dim).to_dicts()}
    assert set(out) == {"1", "2", "3"}                                            # a row without a Sleeper id is no key
    assert out["1"]["sleeper_position"] == "WR" and out["1"]["sleeper_name"] == "Travis Hunter"   # the first skill position he may fill
    assert out["2"]["sleeper_position"] == "RB" and out["3"]["sleeper_position"] is None
    # without the fantasy_positions column the primary position alone decides
    assert bw.sleeper_names(dim.drop("fantasy_positions")).filter(pl.col("_sid") == "1")["sleeper_position"][0] is None


def test_placeholder_rows_are_the_rostered_skill_players_without_a_projection():
    rosters = pl.DataFrame({"roster_id": [1, 1, 2, 2], "team_name": ["A", "A", "B", "B"], "owner_id": ["o1", "o1", "o2", "o2"],
                            "player_id": ["00-1", "sleeper:9", "00-2", "00-3"], "is_taxi": [False, True, False, False], "is_reserve": [False, False, False, False],
                            "player_name": ["Projected", "Devy QB", "Unprojected Vet", "A Kicker"], "position": ["RB", "QB", "WR", "K"]}).with_columns(pl.lit("active").alias("status"))
    ph = bw.placeholder_rows(rosters, ["00-1"])
    assert ph["player_id"].to_list() == ["sleeper:9", "00-2"] and ph["owned_by"].to_list() == ["A", "B"]   # not the projected one, not the kicker
    assert ph["ktc_value"].null_count() == 2 and ph.schema["ktc_value"] == pl.Float64
    assert bw.placeholder_rows(rosters, ["00-1", "sleeper:9", "00-2"]).height == 0
    assert bw.placeholder_rows(rosters.drop("player_name"), ["00-1"]).height == 0            # an older roster frame: nothing to place


def test_placeholder_rows_take_the_ktc_value_by_name():
    rosters = pl.DataFrame({"roster_id": [1], "team_name": ["A"], "owner_id": ["o1"], "player_id": ["sleeper:9"], "is_taxi": [True], "is_reserve": [False],
                            "player_name": ["Jordyn Tyson"], "position": ["WR"], "status": ["taxi"]})
    # the shape market.load_ktc_history / load_crosswalk return: a devy player has no gsis id, so the name path prices him
    hist = pl.DataFrame({"player_key": ["9"], "ktc_name": ["Jordyn Tyson"], "ktc_position": ["WR"], "valuation_date": [bw.datetime(2026, 10, 12).date()], "ktc_value": [4832.0]})
    xw = pl.DataFrame(schema={"gsis_id": pl.Utf8, "player_key": pl.Utf8})
    ph = bw.placeholder_rows(rosters, [], (hist, xw, bw.datetime(2026, 10, 13).date()))
    assert ph.height == 1 and ph["player_name"][0] == "Jordyn Tyson" and ph["ktc_value"][0] == 4832.0 and ph["status"][0] == "taxi"
