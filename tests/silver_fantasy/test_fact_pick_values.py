"""silver_fantasy/fact_pick_values.py

Ownership-independent pick value time-series. Covers parsing both KTC name
formats (daily ``Season Tier Round`` 4-format; archive ``Tier Season Round``
SF-Standard), future-date clipping on the archive, and the union/dedup that lets
the daily feed win on overlap.
"""
import polars as pl

from tests.de_loader import load_de_module

mod = load_de_module(
    "silver_fantasy/fact_pick_values.py", "silver_fantasy", "fact_pick_values"
)
parse_ktc_daily_pick_values = mod.parse_ktc_daily_pick_values
parse_ktc_local_pick_values = mod.parse_ktc_local_pick_values
parse_fc_pick_values = mod.parse_fc_pick_values
build_pick_values = mod.build_pick_values


# --- daily feed: "Season Tier Round", 4 formats -----------------------------
def _daily(rows):
    # rows: (playerName, oneqb_value, sf_value); valuation_date fixed
    return pl.DataFrame({
        "valuation_date": ["2026-06-01"] * len(rows),
        "playerName": [r[0] for r in rows],
        "oneqb_value": [r[1] for r in rows],
        "oneqb_tep_value": [r[1] + 50 for r in rows],
        "sf_value": [r[2] for r in rows],
        "sf_tep_value": [r[2] + 50 for r in rows],
    })


class TestParseDaily:
    def test_only_pick_rows_kept(self):
        out = parse_ktc_daily_pick_values(
            _daily([("2026 Early 1st", 6299, 5629), ("Kenny Pickett", 900, 950)])
        )
        # "Kenny Pickett" contains 'pick' but is not a structured pick name
        assert set(out["season"].to_list()) == {2026}
        assert set(out["tier"].to_list()) == {"Early"}
        assert set(out["round"].to_list()) == {1}

    def test_four_formats_unpivoted(self):
        out = parse_ktc_daily_pick_values(_daily([("2026 Early 1st", 6299, 5629)]))
        vals = {(r["qb_format"], r["te_premium"]): r["value"] for r in out.to_dicts()}
        assert vals[("1QB", "Standard")] == 6299.0
        assert vals[("SF", "Standard")] == 5629.0
        assert vals[("1QB", "TEP")] == 6349.0
        assert vals[("SF", "TEP")] == 5679.0
        assert out["market_type"].unique().to_list() == ["DYNASTY"]

    def test_round_words_to_numbers(self):
        out = parse_ktc_daily_pick_values(_daily([("2027 Mid 3rd", 2833, 2479)]))
        assert out["round"].unique().to_list() == [3]

    def test_fullload_shape_missing_tep_cols(self):
        # full_load picks arrive (after rename) with only oneqb_value + sf_value (no TEP).
        # The parser's `present` filter must yield exactly those two Standard formats.
        df = pl.DataFrame({
            "valuation_date": ["2024-08-02"],
            "playerName": ["2026 Early 1st"],
            "oneqb_value": [5997], "sf_value": [4963],
        })
        out = parse_ktc_daily_pick_values(df)
        formats = {(r["qb_format"], r["te_premium"]) for r in out.to_dicts()}
        assert formats == {("1QB", "Standard"), ("SF", "Standard")}
        assert out["season"].unique().to_list() == [2026]


# --- FantasyCalc picks: round-generic "Season Round", tier='NA' --------------
def _fc(rows):
    # rows: (position, name, value); valuation_date fixed
    return pl.DataFrame({
        "valuation_date": ["2026-06-01"] * len(rows),
        "position": [r[0] for r in rows],
        "name": [r[1] for r in rows],
        "value": [r[2] for r in rows],
    })


class TestParseFCPicks:
    def test_round_generic_pick_parsed_to_na_tier(self):
        out = parse_fc_pick_values(_fc([("PICK", "2026 1st", 3235)]))
        row = out.to_dicts()[0]
        assert row["season"] == 2026 and row["round"] == 1
        assert row["tier"] == "NA"
        assert (row["qb_format"], row["te_premium"], row["market_type"]) == ("SF", "Standard", "DYNASTY")
        assert row["value"] == 3235.0

    def test_non_pick_and_exact_slot_rows_excluded(self):
        out = parse_fc_pick_values(_fc([
            ("RB", "Bijan Robinson", 9000),     # not a pick
            ("PICK", "2026 Pick 1.09", 2741),   # exact-slot form -> skipped
            ("PICK", "2027 2nd", 1619),         # round-generic -> kept
        ]))
        assert out.height == 1
        assert out.to_dicts()[0]["season"] == 2027 and out.to_dicts()[0]["round"] == 2


# --- historical archive: "Tier Season Round", SF Standard only --------------
def _local(rows):
    # rows: (display_name, date, value)
    from datetime import datetime
    return pl.DataFrame({
        "display_name": [r[0] for r in rows],
        "date": [datetime.fromisoformat(r[1]) for r in rows],
        "value": [r[2] for r in rows],
    }, schema_overrides={"value": pl.Int16})


class TestParseLocal:
    def test_reversed_names_parsed_to_sf_standard(self):
        out = parse_ktc_local_pick_values(
            _local([("Early 2026 1st", "2024-08-01", 4990)]), max_date="2026-06-09"
        )
        row = out.to_dicts()[0]
        assert row["season"] == 2026
        assert row["tier"] == "Early"
        assert row["round"] == 1
        assert row["qb_format"] == "SF"
        assert row["te_premium"] == "Standard"
        assert row["market_type"] == "DYNASTY"
        assert row["value"] == 4990.0
        assert row["valuation_date"] == "2024-08-01"

    def test_future_dates_clipped(self):
        out = parse_ktc_local_pick_values(
            _local([("Early 2026 1st", "2024-08-01", 4990),
                    ("Early 2026 1st", "2027-05-31", 9999)]),   # dirty future date
            max_date="2026-06-09",
        )
        assert out["valuation_date"].to_list() == ["2024-08-01"]

    def test_non_pick_rows_excluded(self):
        out = parse_ktc_local_pick_values(
            _local([("Ja'Marr Chase", "2024-08-01", 8000)]), max_date="2026-06-09"
        )
        assert out.height == 0


# --- union / dedup ----------------------------------------------------------
def _long(rows):
    # rows: (valuation_date, season, round, tier, qb_format, te_premium, value)
    return pl.DataFrame({
        "valuation_date": [r[0] for r in rows],
        "season": [r[1] for r in rows],
        "round": [r[2] for r in rows],
        "tier": [r[3] for r in rows],
        "market_type": ["DYNASTY"] * len(rows),
        "qb_format": [r[4] for r in rows],
        "te_premium": [r[5] for r in rows],
        "value": [float(r[6]) for r in rows],
    })


class TestBuildPickValues:
    def test_daily_wins_on_overlap(self):
        # same key in both feeds -> daily value kept
        daily = _long([("2026-06-01", 2026, 1, "Early", "SF", "Standard", 5629)])
        local = _long([("2026-06-01", 2026, 1, "Early", "SF", "Standard", 9999)])
        out = build_pick_values(daily, local)
        assert out.height == 1
        assert out.to_dicts()[0]["value"] == 5629.0     # daily, not 9999

    def test_non_overlapping_history_is_kept(self):
        daily = _long([("2026-06-01", 2026, 1, "Early", "SF", "Standard", 5629)])
        local = _long([("2024-08-01", 2026, 1, "Early", "SF", "Standard", 4990)])  # older date
        out = build_pick_values(daily, local)
        assert out.height == 2
        assert set(out["valuation_date"].to_list()) == {"2026-06-01", "2024-08-01"}

    def test_other_formats_only_from_daily(self):
        # archive only supplies SF/Standard; 1QB stays daily-only
        daily = _long([("2026-06-01", 2026, 1, "Early", "1QB", "Standard", 6299)])
        local = _long([("2024-08-01", 2026, 1, "Early", "SF", "Standard", 4990)])
        out = build_pick_values(daily, local)
        formats = {(r["qb_format"], r["te_premium"]) for r in out.to_dicts()}
        assert formats == {("1QB", "Standard"), ("SF", "Standard")}
