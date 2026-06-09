"""sleeper_ingestion/historical/league_lineage_ingestion.py

Covers the lineage flattener and the lineage walk. The current-season league
is assembled without its scoring_settings, so the walk drops them -> spec fails.
"""
import polars as pl
import pytest

from tests.de_loader import load_de_module

mod = load_de_module(
    "sleeper_ingestion/historical/league_lineage_ingestion.py", "sleeper_ingestion"
)
flatten_lineage_to_parquets = mod.flatten_lineage_to_parquets
get_user_leagues_lineage = mod.get_user_leagues_lineage


def _lineage_league(lid, scoring=True):
    d = {
        "league_id": lid, "league_name": "L", "season": "2024",
        "status": "in_season", "season_type": "regular", "total_rosters": 12,
        "draft_id": "D", "bracket_id": "B", "previous_league_id": None,
        "settings": {"leg": 1, "last_scored_leg": 0, "taxi_slots": 3, "reserve_slots": 1},
        "roster_positions": ["QB", "RB", "RB", "WR"],
    }
    if scoring:
        d["scoring_settings"] = {"pass_td": 4.0}
    return d


class TestFlattenLineage:
    def test_propagates_lineage_id_and_counts_positions(self):
        groups = [{"league_name": "Dynasty", "league_lineage_id": "ROOT",
                   "lineage": [_lineage_league("L2"), _lineage_league("ROOT")]}]
        leagues_df, settings_df, scoring_df, rosters_df = flatten_lineage_to_parquets(groups)
        assert set(leagues_df["league_lineage_id"].to_list()) == {"ROOT"}
        rr = {r["league_id"]: r for r in rosters_df.to_dicts()}
        assert rr["L2"]["RB"] == 2
        assert rr["L2"]["TAXI"] == 3
        assert rr["L2"]["IR"] == 1


def _raw_api_league(lid):
    """A league as returned by the Sleeper REST API (note the raw 'name' key)."""
    return {
        "league_id": lid, "name": "Dynasty", "season": "2024",
        "status": "in_season", "season_type": "regular", "total_rosters": 12,
        "draft_id": "D", "bracket_id": "B", "previous_league_id": None,
        "settings": {"leg": 1, "last_scored_leg": 0, "taxi_slots": 3, "reserve_slots": 1},
        "roster_positions": ["QB", "RB", "RB", "WR"],
        "scoring_settings": {"pass_td": 4.0, "rec": 1.0},
    }


class TestLineageWalk:
    def test_current_season_keeps_scoring_settings(self, monkeypatch):
        # SPEC: the active (head-of-lineage) league's scoring_settings must
        # survive into the lineage. The walk currently builds the first entry
        # without scoring_settings, so this fails.
        current = _raw_api_league("CUR")
        monkeypatch.setattr(mod, "get_sport_state", lambda sport: {"season": "2024"})
        monkeypatch.setattr(
            mod, "get_user_leagues_for_year",
            lambda user_id, sport, year: [current],
        )
        monkeypatch.setattr(mod, "get_league", lambda league_id: current)

        result = get_user_leagues_lineage(user_id="u")
        first_entry = result[0]["lineage"][0]
        assert "scoring_settings" in first_entry
