"""week_ready: the Tuesday task's lake gate -- the newest week complete, in silver, and not yet on the page."""
import importlib.util
from pathlib import Path

import polars as pl


def _mod():
    p = Path(__file__).resolve().parents[1] / "scripts" / "week_ready.py"
    spec = importlib.util.spec_from_file_location("week_ready", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _schedule(week, games):
    """games: (away, home, gameday, away_score, home_score)"""
    return pl.DataFrame({"season": [2026] * len(games), "week": [week] * len(games),
                         "gameday": [g[2] for g in games], "away_team": [g[0] for g in games], "home_team": [g[1] for g in games],
                         "away_score": pl.Series([g[3] for g in games], dtype=pl.Int32),
                         "home_score": pl.Series([g[4] for g in games], dtype=pl.Int32)})


def _weeks(rows):
    """rows: (season, week, team)"""
    return pl.DataFrame({"player_id": [f"p{i}" for i in range(len(rows))], "season": [r[0] for r in rows],
                         "week": [r[1] for r in rows], "team": [r[2] for r in rows]})


W7 = [("NE", "CHI", "2026-10-22", 24, 30), ("KC", "SEA", "2026-10-25", 3, 20), ("DAL", "PHI", "2026-10-26", 38, 21)]


def test_a_complete_week_with_every_team_in_silver_is_ready():
    m = _mod()
    wk = _weeks([(2026, 6, "DAL"), (2026, 7, "NE"), (2026, 7, "CHI"), (2026, 7, "KC"), (2026, 7, "SEA"), (2026, 7, "DAL"), (2026, 7, "PHI")])
    code, why = m.decide(wk, _schedule(7, W7), [])
    assert code == m.READY == 0
    assert "2026 week 7" in why and "3 games" in why and "6 teams" in why


def test_an_unplayed_game_means_wait_and_names_it():
    m = _mod()
    games = W7[:2] + [("DAL", "PHI", "2026-10-26", None, None)]
    wk = _weeks([(2026, 7, "NE"), (2026, 7, "CHI"), (2026, 7, "KC"), (2026, 7, "SEA")])
    code, why = m.decide(wk, _schedule(7, games), [])
    assert code == m.WAIT == 2
    assert "1 of 3 games unplayed" in why and "DAL @ PHI 2026-10-26" in why


def test_the_monday_game_played_but_not_in_silver_means_wait():
    """The schedule reconciles before player_stats in the same nflverse job: scores can be in while the stats are not."""
    m = _mod()
    wk = _weeks([(2026, 7, "NE"), (2026, 7, "CHI"), (2026, 7, "KC"), (2026, 7, "SEA")])
    code, why = m.decide(wk, _schedule(7, W7), [])
    assert code == m.WAIT and "no stats in silver yet for DAL, PHI" in why


def test_a_partial_next_week_never_runs():
    """Friday: Thursday's game is in silver, so the weekly fact's newest week is 8 with one game played."""
    m = _mod()
    wk = _weeks([(2026, 7, "DAL"), (2026, 7, "PHI"), (2026, 8, "BAL"), (2026, 8, "MIA")])
    sched = pl.concat([_schedule(7, W7), _schedule(8, [("BAL", "MIA", "2026-10-29", 20, 17), ("GB", "DET", "2026-11-01", None, None)])])
    code, why = m.decide(wk, sched, [])
    assert code == m.WAIT and "2026 week 8" in why and "1 of 2 games unplayed" in why


def test_a_week_already_on_the_page_is_done():
    m = _mod()
    wk = _weeks([(2026, 7, "NE"), (2026, 7, "CHI"), (2026, 7, "KC"), (2026, 7, "SEA"), (2026, 7, "DAL"), (2026, 7, "PHI")])
    published = ["pages/season=2026/week=7/run_date=2026-10-28/projections.json", "pages/season=2026/week=7/run_date=2026-10-28/scorecard.json"]
    code, why = m.decide(wk, _schedule(7, W7), published)
    assert code == m.DONE == 3 and "run_date 2026-10-28" in why
    # a scorecard alone is not a refresh: only the page payload counts
    code, _ = m.decide(wk, _schedule(7, W7), ["pages/season=2026/week=7/run_date=2026-10-28/scorecard.json"])
    assert code == m.READY


def test_the_season_in_the_schedule_is_the_newest_one():
    """Two seasons in the weekly fact: the newest season's last week, and a schedule row of another season never counts."""
    m = _mod()
    wk = _weeks([(2025, 18, "DAL"), (2026, 7, "DAL"), (2026, 7, "PHI")])
    sched = pl.concat([_schedule(7, [("DAL", "PHI", "2026-10-26", 38, 21)]), _schedule(7, [("X", "Y", "2025-10-20", None, None)]).with_columns(season=pl.lit(2025, dtype=pl.Int64))])
    code, _ = m.decide(wk, sched, [])
    assert code == m.READY


def test_main_reads_the_lake_and_exits_with_the_decision(monkeypatch, capsys):
    m = _mod()
    wk = _weeks([(2026, 7, "NE"), (2026, 7, "CHI"), (2026, 7, "KC"), (2026, 7, "SEA"), (2026, 7, "DAL"), (2026, 7, "PHI")])
    seen = []

    def read_lake(path):
        seen.append(path)
        return wk if path == m.WEEK_PATH else _schedule(7, W7)

    monkeypatch.setattr(m.gcs_io, "read_lake", read_lake)
    monkeypatch.setattr(m.gcs_io, "list_ml", lambda *parts: seen.append("/".join(parts)) or [])
    assert m.main() == m.READY
    assert seen[1] == "bronze/nflverse/schedules/season=2026/data.parquet" and seen[2] == "pages/season=2026/week=7"
    assert "not yet published" in capsys.readouterr().out
    # the lake out of reach is a wait, not a crash: the task retries
    monkeypatch.setattr(m.gcs_io, "read_lake", lambda path: (_ for _ in ()).throw(OSError("no network")))
    assert m.main() == m.WAIT
    assert "lake unreachable" in capsys.readouterr().out
