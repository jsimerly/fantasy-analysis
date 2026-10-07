"""cfbd_ingestion.backfill: flattening, tagging, skip-if-exists, and the per-dataset endpoints."""
from __future__ import annotations

import polars as pl
import pytest

from cfbd_ingestion import backfill, client


def test_flat_handles_two_levels_and_lists():
    r = {"id": 1, "usage": {"overall": 0.2, "pass": 0.1}, "offense": {"rating": 30.1, "ppa": {"x": 1}}, "tags": ["a"], "empty": []}
    f = backfill._flat(r)
    assert f["id"] == 1 and f["usage_overall"] == 0.2 and f["offense_rating"] == 30.1 and f["offense_ppa_x"] == 1
    assert f["tags"] == "['a']" and f["empty"] is None


def test_tag_adds_season_and_casts_null_columns():
    df = pl.DataFrame({"a": [1, 2], "b": [None, None]})
    out = backfill.tag(df, 2022)
    assert out["season"].to_list() == [2022, 2022] and out["b"].dtype == pl.Utf8 and "loaded_at" in out.columns
    assert backfill.tag(pl.DataFrame(), 2022).is_empty()


def test_fetch_routes_each_dataset(monkeypatch):
    calls = []

    def fake_get(path, params=None, **kw):
        calls.append((path, params))
        if path == "/player/usage":
            return [{"id": 7, "name": "A B", "position": "WR", "team": "T", "usage": {"overall": 0.25, "pass": 0.3}}]
        if path == "/ratings/sp":
            return [{"team": "T", "rating": 20.5, "ranking": 3, "offense": {"rating": 35.0, "ranking": 2}, "defense": {"rating": 14.5}}]
        return [{"season": 2022, "playerId": "7", "player": "A B", "category": "receiving", "statType": "YDS", "stat": "1200"}]

    monkeypatch.setattr(client, "get", fake_get)
    u = backfill.fetch("player_usage", 2022)
    assert u["usage_overall"][0] == 0.25 and u["season"][0] == 2022
    sp = backfill.fetch("team_sp", 2022)
    assert sp["offense_rating"][0] == 35.0 and sp["defense_rating"][0] == 14.5
    st = backfill.fetch("player_season_stats", 2022)
    assert st["stat"][0] == "1200" and st["category"][0] == "receiving"
    assert [c[0] for c in calls] == ["/player/usage", "/ratings/sp", "/stats/player/season"]
    with pytest.raises(ValueError):
        backfill.fetch("nope", 2022)


def test_run_skips_existing_and_writes_new(monkeypatch, fake_gcs):
    monkeypatch.setattr(client, "get", lambda path, params=None, **kw: [{"team": "T", "rating": 1.0}])
    fake_gcs[backfill.out_path("team_sp", 2021)] = pl.DataFrame({"team": ["T"], "season": [2021]})
    monkeypatch.setattr(backfill.time, "sleep", lambda s: None)
    written = backfill.run(["team_sp"], 2021, 2022)
    assert written == [backfill.out_path("team_sp", 2022)]
    assert fake_gcs[backfill.out_path("team_sp", 2022)]["season"][0] == 2022
    # --force rewrites
    written = backfill.run(["team_sp"], 2021, 2021, force=True)
    assert written == [backfill.out_path("team_sp", 2021)]


def test_api_key_required(monkeypatch):
    monkeypatch.delenv("CFBD_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        client.api_key()
    monkeypatch.setenv("CFBD_API_KEY", "abc")
    assert client.api_key() == "abc"


def test_last_completed_season_is_the_previous_calendar_year():
    from datetime import datetime
    import cfbd_ingestion.backfill as b
    assert b.last_completed_season(datetime(2027, 2, 5)) == 2026      # the yearly run, two weeks after the CFP
    assert b.last_completed_season(datetime(2026, 10, 6)) == 2025     # mid-season: the finished one
