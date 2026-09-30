"""ktc_ingestion/utils.py: page parsing, historic transform, daily flatten, dtype casting."""
import json

import polars as pl
import pytest
from bs4 import BeautifulSoup

from tests.de_loader import load_de_module

mod = load_de_module("ktc_ingestion/utils.py", "ktc_ingestion", "ktc_utils")
transform_player_data = mod.transform_player_data
flatten_player_data = mod.flatten_player_data
set_dtypes = mod.set_dtypes


class TestTransformPlayerData:
    def _data(self, position="QB"):
        data = {
            "player_id": "p1", "player_name": "Pat", "position": position, "slug": "pat-1",
            "one_qb_value": [{"d": "240101", "v": 5000}, {"d": "240108", "v": 5100}],
            "one_qb_overall_rank": [{"d": "240101", "v": 1}],
            "sf_value": [{"d": "240101", "v": 6000}],
            "sf_overall_rank": [{"d": "240101", "v": 1}],
        }
        if position != "RDP":
            data["one_qb_pos_rank"] = [{"d": "240101", "v": 1}]
            data["sf_pos_rank"] = [{"d": "240101", "v": 1}]
        return data

    def test_one_row_per_distinct_date(self):
        df = transform_player_data(self._data(), "2024-01-10")
        assert df.height == 2

    def test_value_and_date_types(self):
        df = transform_player_data(self._data(), "2024-01-10")
        assert df.schema["ranking_date"] == pl.Date
        assert df.schema["one_qb_value"] == pl.Float64
        assert df.schema["scrape_date"] == pl.Date

    def test_values_aligned_to_dates(self):
        df = transform_player_data(self._data(), "2024-01-10").sort("ranking_date")
        rows = df.to_dicts()
        assert rows[0]["one_qb_value"] == 5000.0
        assert rows[1]["one_qb_value"] == 5100.0
        assert rows[1]["sf_value"] is None       # no sf entry for 2024-01-08

    def test_rdp_has_no_positional_rank_columns(self):
        df = transform_player_data(self._data(position="RDP"), "2024-01-10")
        assert "one_qb_pos_rank" not in df.columns
        assert "sf_pos_rank" not in df.columns


class TestFlattenPlayerData:
    def _player(self):
        return {
            "playerName": "Pat", "playerID": 1, "slug": "pat-1",
            "position": "QB", "positionID": 1, "isTrending": False,
            "oneQBValues": {
                "value": 5000, "rank": 1, "positionalRank": 1, "overallTier": 1,
                "startSitValue": 10.0, "tep": {"value": 5100, "rank": 2},
            },
            "superflexValues": {
                "value": 6000, "rank": 1, "tep": {"value": 6100},
            },
        }

    def test_core_values_flattened(self):
        flat = flatten_player_data(self._player())
        assert flat["playerName"] == "Pat"
        assert flat["oneqb_value"] == 5000
        assert flat["sf_value"] == 6000

    def test_tep_nested_values_flattened(self):
        flat = flatten_player_data(self._player())
        assert flat["oneqb_tep_value"] == 5100
        assert flat["sf_tep_value"] == 6100

    def test_missing_value_blocks_are_skipped(self):
        flat = flatten_player_data({"playerName": "X", "playerID": 2, "slug": "x",
                                    "position": "WR", "positionID": 2, "isTrending": False,
                                    "oneQBValues": None, "superflexValues": None})
        assert flat["playerName"] == "X"
        assert "oneqb_value" not in flat


class TestSetDtypes:
    def test_casts_present_columns(self):
        df = pl.DataFrame({
            "positionID": [1], "oneqb_value": [5000.0], "isTrending": [True],
            "sf_rank": [3],
        })
        out = set_dtypes(df)
        assert out.schema["positionID"] == pl.Int32
        assert out.schema["oneqb_value"] == pl.Float32
        assert out.schema["isTrending"] == pl.Boolean
        assert out.schema["sf_rank"] == pl.Int32

    def test_ignores_absent_columns(self):
        df = pl.DataFrame({"positionID": [1]})
        out = set_dtypes(df)   # must not raise on the many missing columns
        assert out.schema["positionID"] == pl.Int32


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


class TestRankingsPageParsing:
    """KTC changed the rankings pages on ~2026-09-08: `playersArray` now ships as a JSON blob in
    <script type="application/json" id="ktc-players"> which the inline JS JSON.parse()s, so the
    old `playersArray = [...]` regex can never match again (the three ktc-incremental-* jobs
    failed every day from 2026-09-08). The parser must read the element first and keep the
    legacy inline layout as a fallback."""

    PLAYERS = [{
        "playerName": "Josh Allen", "playerID": 365, "slug": "josh-allen-365",
        "position": "QB", "positionID": 1, "isTrending": False,
        "oneQBValues": {"value": 7806}, "superflexValues": {"value": 9994},
    }]

    def test_reads_json_element_layout(self):
        html = f"""<html><body>
        <script type="application/json" id="ktc-players">{json.dumps(self.PLAYERS)}</script>
        <script>var leagueType = 1;
        var playersArray = JSON.parse(document.getElementById('ktc-players').textContent);
        var oneQBPlayers = [{{"playerName": "Other"}}];</script></body></html>"""
        assert mod.parse_rankings_players(_soup(html)) == self.PLAYERS

    def test_legacy_inline_layout_still_parses(self):
        html = ("<html><script>var leagueType = 1;\n    var playersArray = "
                f"{json.dumps(self.PLAYERS)};\n    var oneQBSC = false;</script></html>")
        assert mod.parse_rankings_players(_soup(html)) == self.PLAYERS

    def test_inline_reference_without_element_raises(self):
        # the exact production failure mode: a script *mentions* playersArray but the data
        # element is missing -> must raise, not return garbage
        html = ("<html><script>var playersArray = JSON.parse("
                "document.getElementById('ktc-players').textContent);</script></html>")
        with pytest.raises(ValueError):
            mod.parse_rankings_players(_soup(html))

    def test_raises_when_neither_layout_present(self):
        with pytest.raises(ValueError):
            mod.parse_rankings_players(_soup("<html><script>var x = 1;</script></html>"))

    def test_get_dynasty_playersArray_fetches_then_parses(self, monkeypatch):
        html = f'<html><script type="application/json" id="ktc-players">{json.dumps(self.PLAYERS)}</script></html>'
        seen = []
        monkeypatch.setattr(mod, "fetch_soup", lambda url: seen.append(url) or _soup(html))
        assert mod.get_dynasty_playersArray("https://keeptradecut.com/devy-rankings") == self.PLAYERS
        assert seen == ["https://keeptradecut.com/devy-rankings"]


class TestPlayerPageParsing:
    """Per-player pages moved the value history the same way: id="pd-oneqb" / id="pd-superflex"
    JSON elements instead of inline `var playerOneQB = {...}` (used by the ktc-full-* backfills)."""

    ONEQB = {"overallValue": [{"d": "260930", "v": 7806}],
             "overallRankHistory": [{"d": "260930", "v": 7}],
             "positionalRankHistory": [{"d": "260930", "v": 1}]}
    SF = {"overallValue": [{"d": "260930", "v": 9994}],
          "overallRankHistory": [{"d": "260930", "v": 3}],
          "positionalRankHistory": [{"d": "260930", "v": 1}]}

    def test_reads_json_element_layout(self):
        html = f"""<html>
        <script type="application/json" id="pd-superflex">{json.dumps(self.SF)}</script>
        <script type="application/json" id="pd-oneqb">{json.dumps(self.ONEQB)}</script>
        <script>var playerSuperflex = JSON.parse(document.getElementById('pd-superflex').textContent);
        var playerOneQB = JSON.parse(document.getElementById('pd-oneqb').textContent);
        var leagueType = 1;</script></html>"""
        assert mod.parse_historic_1QBplayer_data(_soup(html)) == self.ONEQB
        assert mod.parse_historic_SFplayer_data(_soup(html)) == self.SF

    def test_legacy_inline_layout_still_parses(self):
        html = (f"<html><script>var playerSuperflex = {json.dumps(self.SF)};\n"
                f"    var playerOneQB = {json.dumps(self.ONEQB)};\n"
                "    var leagueType = 1;</script></html>")
        assert mod.parse_historic_1QBplayer_data(_soup(html)) == self.ONEQB
        assert mod.parse_historic_SFplayer_data(_soup(html)) == self.SF

    def test_raises_when_absent(self):
        with pytest.raises(ValueError):
            mod.parse_historic_1QBplayer_data(_soup("<html></html>"))
        with pytest.raises(ValueError):
            mod.parse_historic_SFplayer_data(_soup("<html></html>"))
