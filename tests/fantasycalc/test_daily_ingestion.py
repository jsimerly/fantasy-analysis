"""fantasycalc_ingestion/daily_ingestion.py: flatten the nested API payload."""
import polars as pl

from tests.de_loader import load_de_module

mod = load_de_module("fantasycalc_ingestion/daily_ingestion.py", "fantasycalc_ingestion", "fc_daily")
flatten_player_data = mod.flatten_player_data


def _item():
    return {
        "player": {
            "id": 1, "name": "Patrick Mahomes", "mflId": "m1", "sleeperId": "100",
            "position": "QB", "espnId": "e1", "fleaflickerId": "f1",
            "maybeBirthday": "1995-09-17", "maybeHeight": "75", "maybeWeight": "225",
            "maybeCollege": "Texas Tech", "maybeTeam": "KC", "maybeAge": 28, "maybeYoe": 7,
        },
        "value": 5000, "overallRank": 1, "positionRank": 1, "trend30Day": 10,
        "redraftDynastyValueDifference": 200, "redraftDynastyValuePercDifference": 0.04,
        "redraftValue": 4800, "combinedValue": 9800, "maybeTier": 1, "maybeAdp": 12.0,
        "maybeTradeFrequency": 0.5, "maybeMovingStandardDeviation": 1.0,
        "maybeMovingStandardDeviationPerc": 0.1, "maybeMovingStandardDeviationAdjusted": 0.9,
    }


class TestFlattenPlayerData:
    def test_returns_dataframe_with_one_row_per_item(self):
        df = flatten_player_data([_item(), _item()])
        assert isinstance(df, pl.DataFrame)
        assert df.height == 2

    def test_player_fields_flattened_from_nested_object(self):
        row = flatten_player_data([_item()]).to_dicts()[0]
        assert row["id"] == 1
        assert row["name"] == "Patrick Mahomes"
        assert row["sleeper_id"] == "100"
        assert row["position"] == "QB"
        assert row["espn_id"] == "e1"

    def test_value_metrics_flattened_from_root(self):
        row = flatten_player_data([_item()]).to_dicts()[0]
        assert row["value"] == 5000
        assert row["redraft_value"] == 4800
        assert row["combined_value"] == 9800
        assert row["overall_rank"] == 1

    def test_missing_optional_fields_become_null(self):
        bare = {"player": {"id": 2, "name": "Rookie", "position": "WR"}, "value": 100}
        row = flatten_player_data([bare]).to_dicts()[0]
        assert row["id"] == 2
        assert row["sleeper_id"] is None
        assert row["adp"] is None
