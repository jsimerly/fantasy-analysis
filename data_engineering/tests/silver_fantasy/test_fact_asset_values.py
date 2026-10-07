"""silver_fantasy/fact_asset_values.py

resolve_and_split links staging valuations to the master player dim; build_fact
pivots the two sources wide. build_fact currently assumes both sources are
present after the pivot -> the single-source spec test fails.
"""
import polars as pl


import silver_fantasy.fact_asset_values as mod
resolve_and_split = mod.resolve_and_split
build_fact = mod.build_fact


def _dim():
    return pl.DataFrame({
        "player_id": ["100", "200"],
        "name": ["Patrick Mahomes", "Justin Jefferson"],
        "ktc_id": [1, 2],
        "espn_id": [11, 22],
        "position": ["QB", "WR"],
    })


def _staging_row(source_system, source_id, asset_name, value):
    return {
        "valuation_date": "2024-01-01", "source_system": source_system,
        "source_id": source_id, "asset_name": asset_name, "value": value,
        "market_type": "DYNASTY", "qb_format": "SF", "te_premium": "Standard",
    }


class TestResolveAndSplit:
    def test_ktc_slug_resolves_via_trailing_id(self):
        staging = pl.DataFrame([_staging_row("KTC", "patrick-mahomes-1", "Patrick Mahomes", 5000)])
        valid, unmapped = resolve_and_split(staging, _dim())
        assert valid.height == 1
        assert valid.to_dicts()[0]["player_id"] == "100"
        assert unmapped.height == 0

    def test_fantasycalc_resolves_via_name(self):
        staging = pl.DataFrame([_staging_row("FANTASYCALC", "fc-9", "Justin Jefferson", 4800)])
        valid, unmapped = resolve_and_split(staging, _dim())
        assert valid.to_dicts()[0]["player_id"] == "200"

    def test_unmatched_rows_go_to_quarantine(self):
        # SPEC: the quarantine report must identify the unmapped asset by name.
        # The KTC branch sources `name` from the (failed) dim join and never
        # falls back to asset_name, so unmapped KTC rows currently report
        # name=None -- making the report unusable for the largest source.
        staging = pl.DataFrame([_staging_row("KTC", "nobody-999", "Nobody Special", 1)])
        valid, unmapped = resolve_and_split(staging, _dim())
        assert valid.height == 0
        assert unmapped.height == 1
        assert unmapped.to_dicts()[0]["name"] == "Nobody Special"


def _valid_row(source_system, value, player_id="100"):
    return {
        "valuation_date": "2024-01-01", "player_id": player_id,
        "name": "Patrick Mahomes", "position": "QB", "market_type": "DYNASTY",
        "qb_format": "SF", "te_premium": "Standard",
        "source_system": source_system, "value": value,
    }


class TestBuildFact:
    def test_pivots_both_sources_and_computes_diff(self):
        valid = pl.DataFrame([_valid_row("KTC", 5000), _valid_row("FANTASYCALC", 4800)])
        fact = build_fact(valid)
        assert fact.height == 1
        row = fact.to_dicts()[0]
        assert row["ktc_value"] == 5000
        assert row["fc_value"] == 4800
        assert row["value_diff"] == 200

    def test_single_source_still_yields_both_value_columns(self):
        # SPEC: a slice with only KTC values must still produce an fc_value (0),
        # not raise because the FANTASYCALC pivot column is absent.
        valid = pl.DataFrame([_valid_row("KTC", 5000)])
        fact = build_fact(valid)
        row = fact.to_dicts()[0]
        assert row["ktc_value"] == 5000
        assert row["fc_value"] == 0
        assert row["value_diff"] == 5000
