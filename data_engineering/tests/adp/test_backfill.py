"""adp_ingestion/backfill.py: the two flatteners, the scoring switch, the season resolver, the write."""
from datetime import datetime

import polars as pl

import adp_ingestion.backfill as mod


def test_ffc_flattens_players_with_the_scoring_of_the_year_and_the_draft_count():
    payload = {"meta": {"type": "PPR", "teams": 12, "total_drafts": 303},
               "players": [{"player_id": 1, "name": "Arian Foster", "position": "RB", "team": "HOU", "adp": 1.4, "times_drafted": 68, "bye": 8},
                           {"player_id": 2, "name": None, "position": "WR"}]}
    df = mod.flatten_ffc(payload, 2012, loaded_at=datetime(2026, 10, 9))
    assert df.height == 1 and df.columns == list(mod.FFC_SCHEMA)
    r = df.to_dicts()[0]
    assert r["scoring"] == "ppr" and r["adp"] == 1.4 and r["total_drafts"] == 303 and r["ffc_id"] == "1"
    assert mod.ffc_format(2011) == "standard" and mod.ffc_format(2012) == "ppr"
    assert mod.flatten_ffc({"players": []}, 2007).height == 0


def test_mfl_flattens_the_adp_export_by_mfl_id():
    payload = {"adp": {"totalDrafts": "4685", "player": [{"id": "11192", "draftsSelectedIn": "4673", "maxPick": "121", "minPick": "1", "averagePick": "2.63", "rank": "1"},
                                                          {"id": "x", "averagePick": None}]}}
    df = mod.flatten_mfl(payload, 2015, loaded_at=datetime(2026, 10, 9))
    assert df.height == 1 and df.columns == list(mod.MFL_SCHEMA)
    r = df.to_dicts()[0]
    assert r["mfl_id"] == "11192" and abs(r["adp"] - 2.63) < 1e-9 and r["drafts_selected"] == 4673 and r["total_drafts"] == 4685 and r["max_pick"] == 121


def test_seasons_and_the_partition_path(fake_gcs):
    assert mod.resolve_seasons("current", datetime(2026, 10, 9)) == [2026] and mod.resolve_seasons("2009-2011") == [2009, 2010, 2011]
    df = mod.flatten_ffc({"players": [{"player_id": 1, "name": "A", "position": "RB", "adp": 1.0}]}, 2012)
    assert mod.save(df, "gs://test-bucket", "ffc", 2012) == "gs://test-bucket/bronze/adp/ffc/season=2012/data.parquet"


def test_live_pulls_the_coming_season_all_year_and_skips_one_not_published_yet():
    from datetime import datetime
    import requests

    assert mod.resolve_seasons("live", datetime(2027, 1, 20)) == [2026, 2027]      # last season (the canary) and the coming one, every day
    assert mod.resolve_seasons("live", datetime(2027, 5, 1)) == [2026, 2027]
    assert mod.resolve_seasons("live", datetime(2026, 10, 12)) == [2025, 2026]

    def http_error(code):
        r = requests.Response(); r.status_code = code
        return requests.HTTPError(f"{code}", response=r)
    assert mod.not_published_yet(http_error(404), 2027, datetime(2027, 1, 20))
    assert mod.not_published_yet(http_error(400), 2027, datetime(2026, 12, 1))
    assert not mod.not_published_yet(http_error(404), 2025, datetime(2026, 10, 12))    # a past season missing is a real failure
    assert not mod.not_published_yet(http_error(500), 2027, datetime(2027, 1, 20))


def _ffc_payload(n, start, end):
    return {"meta": {"total_drafts": n, "start_date": start, "end_date": end}, "players": [{"player_id": 1, "name": "A", "position": "RB", "team": "X", "adp": 1.5, "times_drafted": n, "bye": 7}]}


def test_ffc_is_written_only_from_a_preseason_window():
    """FFC serves one window per year: the frozen final preseason week for a past year, the latest (in-season) window
    for the current one. 2026-10-13: year=2025 -> 8,470 drafts Aug 25 - Sep 1; year=2026 -> 109 drafts Sep 18-25."""
    ok, desc = mod.ffc_window_is_preseason(mod.flatten_ffc(_ffc_payload(8470, "2025-08-25", "2025-09-01"), 2025), 2025)
    assert ok and "8470 drafts" in desc
    ok, _ = mod.ffc_window_is_preseason(mod.flatten_ffc(_ffc_payload(109, "2026-09-18", "2026-09-25"), 2026), 2026)
    assert not ok                                                          # in season: not written
    ok, _ = mod.ffc_window_is_preseason(mod.flatten_ffc(_ffc_payload(2000, "2026-08-20", "2026-08-27"), 2026), 2026)
    assert ok                                                              # a preseason week of the current year
    df = mod.flatten_ffc(_ffc_payload(8470, "2025-08-25", "2025-09-01"), 2025)
    assert df["start_date"].item() == "2025-08-25" and df["end_date"].item() == "2025-09-01" and "period" in mod.MFL_SCHEMA


def test_a_season_not_published_yet_is_skipped_only_when_the_provider_answered_for_last_season(monkeypatch):
    """The canary: in live mode last season is fetched first; a 400 / 404 on the coming season is 'not published yet'
    only if last season came back, so a provider outage (or a moved endpoint) fails the run instead of hiding."""
    import requests
    import pytest

    def http_error(code):
        r = requests.Response(); r.status_code = code
        return requests.HTTPError(f"{code}", response=r)

    def run(ffc_this_season_ok):          # this season answers (the canary); next season is not published yet
        def fake_ffc(season, session):
            if season == 2026 and ffc_this_season_ok:
                return _ffc_payload(8470, "2026-08-25", "2026-09-01")
            raise http_error(400)
        monkeypatch.setattr(mod, "fetch_ffc", fake_ffc)
        monkeypatch.setattr(mod, "fetch_mfl", lambda season, session: {"adp": {"totalDrafts": "600", "player": [{"id": "1", "averagePick": "2.0", "rank": "1", "draftsSelectedIn": "1", "minPick": "1", "maxPick": "3"}]}})
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
        monkeypatch.setattr(mod, "resolve_seasons", lambda spec: [2026, 2027])
        monkeypatch.setenv("ADP_DRY_RUN", "1")
        monkeypatch.setenv("ADP_SEASONS", "live")
        mod.main()

    run(ffc_this_season_ok=True)                                           # 2026 answered, 2027 not published yet: fine
    with pytest.raises(SystemExit):
        run(ffc_this_season_ok=False)                                      # FFC down: the run fails
