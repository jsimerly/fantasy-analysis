"""team_value: the weekly grid, the startup-ramp cutoff, the power / value series, owner colors
and the page summary (series aligned to the grid, ranks and changes)."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import team_value as TV  # noqa: E402


def _ledger():
    # two franchises of one lineage; B's second player arrives a week after the startup, C's pick too
    rows = [
        ("L1_1", "player", "p1", "2024-01-01", None), ("L1_1", "player", "p2", "2024-01-01", None), ("L1_1", "pick", "2025:1:1", "2024-01-08", None),
        ("L1_2", "player", "p3", "2024-01-01", None), ("L1_2", "player", "p4", "2024-01-08", None), ("L1_2", "pick", "2025:1:2", "2024-01-08", None),
    ]
    return pl.DataFrame(rows, schema={"franchise_id": pl.Utf8, "asset_type": pl.Utf8, "asset_id": pl.Utf8, "valid_from": pl.Utf8, "valid_to": pl.Utf8}, orient="row")


def _meta():
    return pl.DataFrame({"franchise_id": ["L1_1", "L1_2"], "league_lineage_id": ["L1", "L1"], "current_team_name": ["Alpha", "Bravo"],
                         "roster_id": [1, 2], "owner_id": ["o1", "o2"]})


def _values():
    rows = []
    for d in (date(2024, 1, 1), date(2024, 1, 8), date(2024, 1, 15), date(2024, 1, 22)):
        for pid, v in (("p1", 8000), ("p2", 4000), ("p3", 6000), ("p4", 5000)):
            rows.append({"valuation_date": d, "player_id": pid, "name": pid, "position": "WR", "ktc_value": v + (500 if pid == "p4" and d >= date(2024, 1, 15) else 0), "fc_value": v})
    return pl.DataFrame(rows)


def _picks():
    return pl.DataFrame({"season": ["2025"] * 4, "round": [1] * 4, "valuation_date": [date(2024, 1, 1), date(2024, 1, 8), date(2024, 1, 15), date(2024, 1, 22)],
                         "value": [3000] * 4})


def _events():
    return pl.DataFrame({"league_lineage_id": ["L1", "L1", "L1"], "season": [2024, 2024, 2025], "event_type": ["startup_draft", "fantasy_end", "rookie_draft"],
                         "event_date": [date(2024, 1, 1), date(2024, 12, 20), date(2025, 5, 10)]})


class TestGridAndRamp:
    def test_weekly_grid_spans_the_overlap_and_ends_today(self):
        dates = TV.weekly_grid(_ledger(), _values())
        assert dates[0] == "2024-01-01" and dates[-1] == "2024-01-22" and len(dates) == 4

    def test_startup_cutoff_is_the_first_full_week(self):
        dates = TV.weekly_grid(_ledger(), _values())
        cut = TV.startup_cutoffs(_ledger(), dates, _meta())
        assert cut.to_dicts() == [{"league_lineage_id": "L1", "start": date(2024, 1, 8)}]   # week 1 holds 3 of the steady 6 assets
        power = TV.build_power(_ledger(), dates, _values(), _picks(), _meta())
        trimmed = TV.trim_startup_ramp(power, cut)
        assert trimmed["date"].min() == date(2024, 1, 8) and trimmed.height == 6


class TestSeries:
    def test_power_with_and_without_picks_and_the_league_average(self):
        dates = TV.weekly_grid(_ledger(), _values())
        power = TV.build_power(_ledger(), dates, _values(), _picks(), _meta()).sort(["date", "franchise_id"])
        wk2 = power.filter(pl.col("date") == date(2024, 1, 8))
        assert wk2["power_index"].max() == 99 and wk2["power_players"].max() == 99
        assert abs(wk2["rel_mean"].mean() - 100) < 1e-9 and wk2.filter(pl.col("franchise_id") == "L1_1")["rel_mean"][0] > 100
        assert set(power.columns) >= {"current_team_name", "owner_id", "adj_total", "power_players", "rel_mean"}

    def test_plain_value_sums_players_and_picks(self):
        dates = TV.weekly_grid(_ledger(), _values())
        v = TV.build_value(_ledger(), dates, _values(), _picks()).sort(["date", "franchise_id"])
        assert v.filter((pl.col("date") == date(2024, 1, 8)) & (pl.col("franchise_id") == "L1_1"))["value"][0] == 8000 + 4000 + 3000
        assert v.filter((pl.col("date") == date(2024, 1, 1)) & (pl.col("franchise_id") == "L1_2"))["value"][0] == 6000

    def test_calendar_pieces(self):
        bands = TV.season_bands(_events(), pl.DataFrame({"season": [2024], "season_start": [date(2024, 9, 5)]}))
        assert bands.to_dicts() == [{"league_lineage_id": "L1", "season": 2024, "season_start": date(2024, 9, 5), "season_end": date(2024, 12, 20)}]
        assert TV.draft_days(_events())["event_type"].to_list() == ["startup_draft", "rookie_draft"]


class TestColorsAndSummary:
    def test_owner_colors_unique_and_deterministic(self):
        owners = [f"owner{i}" for i in range(12)]
        c1, c2 = TV.owner_colors(owners), TV.owner_colors(list(reversed(owners)))
        assert c1 == c2 and len(set(c1.values())) == 12 and all(v in TV.PALETTE for v in c1.values())

    def test_summary_aligns_series_to_the_grid_and_ranks_now(self):
        dates = TV.weekly_grid(_ledger(), _values())
        cut = TV.startup_cutoffs(_ledger(), dates, _meta())
        power = TV.trim_startup_ramp(TV.build_power(_ledger(), dates, _values(), _picks(), _meta()), cut)
        power = power.join(TV.build_value(_ledger(), dates, _values(), _picks()), on=["franchise_id", "date"], how="left")
        events = _events()
        data = {"power": power, "fc": pl.DataFrame(schema={"franchise_id": pl.Utf8, "date": pl.Date, "value": pl.Float64, "league_lineage_id": pl.Utf8}),
                "events": events, "bands": TV.season_bands(events, pl.DataFrame({"season": [2024], "season_start": [date(2024, 9, 5)]})),
                "drafts": TV.draft_days(events), "fr_meta": _meta(), "dates": dates, "names": {"L1": "Test League"}}
        s = TV.summary(data)
        lg = s["leagues"][0]
        assert lg["name"] == "Test League" and lg["dates"] == ["2024-01-08", "2024-01-15", "2024-01-22"]
        assert set(lg["series"]) == set(TV.METRICS) and all(len(v) == 3 for v in lg["series"]["power_index"].values())
        assert lg["now"][0]["name"] == "Alpha" and lg["now"][0]["rank"] == 1 and lg["now"][0]["power_index"] == 99
        assert lg["now"][1]["since_draft"] is not None and lg["now"][1]["d4w"] is None          # 3 weeks of history: no 4-week change yet
        assert lg["bands"] == [["2024-09-05", "2024-12-20", 2024]] and lg["drafts"][0] == ["2024-01-01", "startup_draft", 2024]
        assert lg["fc"] is None and lg["latest_draft"] == "2024-01-01"
        assert {t["color"] for t in lg["teams"]} <= set(TV.PALETTE) and s["meta"]["through"] == "2024-01-22"
