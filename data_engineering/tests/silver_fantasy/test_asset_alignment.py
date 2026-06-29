"""silver_fantasy/_staging/asset_alignment.py

Pure reshape helpers: melt_ktc_values (wide KTC -> long) and standardize_simple
(single-value source -> long, with PICK/PLAYER classification).
"""
import polars as pl


import silver_fantasy._staging.asset_alignment as mod
melt_ktc_values = mod.melt_ktc_values
standardize_simple = mod.standardize_simple


class TestMeltKtcValues:
    def _lf(self):
        return pl.DataFrame({
            "valuation_date": ["2024-01-01"],
            "slug": ["patrick-mahomes-1"],
            "playerName": ["Patrick Mahomes"],
            "oneqb_value": [5000],
            "oneqb_tep_value": [5100],
            "sf_value": [6000],
            "sf_tep_value": [6100],
        }).lazy()

    def test_unpivots_to_four_scoring_rows(self):
        out = melt_ktc_values(self._lf(), "DYNASTY").collect()
        assert out.height == 4
        assert out["source_system"].unique().to_list() == ["KTC"]
        assert out["market_type"].unique().to_list() == ["DYNASTY"]
        assert out["asset_type"].unique().to_list() == ["PLAYER"]

    def test_qb_format_and_te_premium_parsed_from_code(self):
        out = melt_ktc_values(self._lf(), "DYNASTY").collect()
        rows = {(r["qb_format"], r["te_premium"]): r["value"] for r in out.to_dicts()}
        assert rows[("1QB", "Standard")] == 5000
        assert rows[("1QB", "TEP")] == 5100
        assert rows[("SF", "Standard")] == 6000
        assert rows[("SF", "TEP")] == 6100

    def test_valuation_date_coerced_to_date(self):
        out = melt_ktc_values(self._lf(), "DYNASTY").collect()
        assert out.schema["valuation_date"] == pl.Date

    def test_source_id_taken_from_slug(self):
        out = melt_ktc_values(self._lf(), "DYNASTY").collect()
        assert out["source_id"].unique().to_list() == ["patrick-mahomes-1"]

    def test_null_values_dropped(self):
        lf = pl.DataFrame({
            "valuation_date": ["2024-01-01"], "slug": ["x-1"], "playerName": ["X"],
            "oneqb_value": [None], "oneqb_tep_value": [None],
            "sf_value": [6000], "sf_tep_value": [None],
        }).lazy()
        out = melt_ktc_values(lf, "DYNASTY").collect()
        assert out.height == 1   # only the single non-null sf_value survives


class TestStandardizeSimple:
    def _lf(self, name, value=5000):
        return pl.DataFrame({
            "valuation_date": ["2024-01-01"],
            "source_id": ["123"],
            "asset_name": [name],
            "value": [value],
        }).lazy()

    def test_player_classified_as_player(self):
        out = standardize_simple(self._lf("Patrick Mahomes"), "KTC", "DYNASTY", "SF", "Standard", "value").collect()
        assert out["asset_type"].item() == "PLAYER"

    def test_pick_classified_by_ordinal(self):
        out = standardize_simple(self._lf("Early 1st"), "KTC", "DYNASTY", "SF", "Standard", "value").collect()
        assert out["asset_type"].item() == "PICK"

    def test_pick_classified_by_future_year(self):
        out = standardize_simple(self._lf("2026 Mid 1st"), "KTC", "DYNASTY", "SF", "Standard", "value").collect()
        assert out["asset_type"].item() == "PICK"

    def test_constant_fields_and_types(self):
        out = standardize_simple(self._lf("Patrick Mahomes"), "FANTASYCALC", "REDRAFT", "1QB", "Standard", "value").collect()
        row = out.to_dicts()[0]
        assert row["source_system"] == "FANTASYCALC"
        assert row["market_type"] == "REDRAFT"
        assert row["qb_format"] == "1QB"
        assert row["te_premium"] == "Standard"
        assert row["value"] == 5000
        assert out.schema["valuation_date"] == pl.Date
        assert out.schema["source_id"] == pl.Utf8
