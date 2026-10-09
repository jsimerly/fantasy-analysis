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
