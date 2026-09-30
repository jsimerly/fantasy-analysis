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


class TestSettingsTags:
    """Each daily file holds all 24 league-setting combinations; the tags fetch_all_combinations
    puts on every item must survive flattening (they were dropped until 2026-09, which left the
    24 rows per player-day indistinguishable for silver)."""

    def test_tags_flattened_with_pinned_dtypes(self):
        item = _item()
        item.update({"n_qb": 2, "n_teams": 12, "ppr": 1.0})
        df = flatten_player_data([item])
        row = df.to_dicts()[0]
        assert (row["n_qb"], row["n_teams"], row["ppr"]) == (2, 12, 1.0)
        assert df.schema["n_qb"] == pl.Int64
        assert df.schema["n_teams"] == pl.Int64
        assert df.schema["ppr"] == pl.Float64

    def test_untagged_items_keep_the_columns_as_typed_nulls(self):
        df = flatten_player_data([_item()])
        assert df["n_qb"].to_list() == [None]
        assert df.schema["n_qb"] == pl.Int64      # never a Null-dtype column
        assert df.schema["ppr"] == pl.Float64

    def test_combinations_are_the_documented_grid_in_fetch_order(self):
        combos = flatten_player_data.__globals__["SETTINGS_COMBINATIONS"]
        assert len(combos) == 24
        assert combos[0] == ("1", "8", "0")
        assert combos[-1] == ("2", "14", "1")
