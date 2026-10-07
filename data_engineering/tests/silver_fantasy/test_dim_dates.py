"""silver_fantasy/dim_dates.py — date dimension with NFL season/phase/flags.

Covers season_boundaries (REG/POST -> per-season dates) and build_dim_dates
(daily spine labelling: phase, offseason->upcoming-season attribution, event flags).
"""
from datetime import date

import polars as pl


import silver_fantasy.dim_dates as mod
season_boundaries = mod.season_boundaries
build_dim_dates = mod.build_dim_dates


def _sched():
    # season 2024: REG 2024-09-08..2024-12-29, playoffs WC 2025-01-11, SB 2025-02-09;
    # season 2025: REG 2025-09-07.. (in progress, no POST yet)
    rows = [
        (2024, "REG", "2024-09-08"), (2024, "REG", "2024-12-29"),
        (2024, "WC", "2025-01-11"), (2024, "SB", "2025-02-09"),
        (2025, "REG", "2025-09-07"), (2025, "REG", "2025-12-28"),
    ]
    return pl.DataFrame({"season": [r[0] for r in rows], "game_type": [r[1] for r in rows],
                         "gameday": [r[2] for r in rows]})


class TestSeasonBoundaries:
    def test_boundaries_per_season(self):
        b = season_boundaries(_sched()).sort("season")
        r24 = b.filter(pl.col("season") == 2024).to_dicts()[0]
        assert r24["season_start"] == date(2024, 9, 8)
        assert r24["reg_end"] == date(2024, 12, 29)
        assert r24["playoff_start"] == date(2025, 1, 11)
        assert r24["playoff_end"] == date(2025, 2, 9)

    def test_in_progress_season_has_null_playoffs(self):
        b = season_boundaries(_sched())
        r25 = b.filter(pl.col("season") == 2025).to_dicts()[0]
        assert r25["playoff_start"] is None and r25["playoff_end"] is None


class TestBuildDimDates:
    def _d(self):
        return build_dim_dates(_sched(), end_date=date(2025, 10, 1))

    def _row(self, d, iso):
        return d.filter(pl.col("date") == date.fromisoformat(iso)).to_dicts()[0]

    def test_regular_season_phase_and_season(self):
        r = self._row(self._d(), "2024-10-15")
        assert r["nfl_phase"] == "regular" and r["nfl_season"] == 2024

    def test_playoffs_phase_stays_in_season_year(self):
        r = self._row(self._d(), "2025-01-20")   # after reg_end, before SB
        assert r["nfl_phase"] == "playoffs" and r["nfl_season"] == 2024

    def test_offseason_attributed_to_upcoming_season(self):
        r = self._row(self._d(), "2025-04-01")    # after SB, before 2025 kickoff
        assert r["nfl_phase"] == "offseason" and r["nfl_season"] == 2025

    def test_event_flags_on_exact_dates(self):
        d = self._d()
        assert self._row(d, "2024-09-08")["is_nfl_season_start"] is True
        assert self._row(d, "2025-01-11")["is_nfl_playoff_start"] is True
        assert self._row(d, "2025-02-09")["is_nfl_playoff_end"] is True
        # a non-event date carries no flags
        off = self._row(d, "2024-10-15")
        assert not (off["is_nfl_season_start"] or off["is_nfl_playoff_start"] or off["is_nfl_playoff_end"])

    def test_spine_is_contiguous_daily(self):
        d = self._d()
        assert d.height == (date(2025, 10, 1) - date(2024, 9, 8)).days + 1
        assert d["date"].is_duplicated().sum() == 0
