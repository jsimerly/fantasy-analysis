"""silver_fantasy/dim_player_master.py"""
import polars as pl
import pytest


import silver_fantasy.dim_player_master as mod
transform_dim_players_master = mod.transform_dim_players_master


@pytest.fixture
def seeded(monkeypatch, fake_gcs):
    monkeypatch.setenv("GCS_BUCKET_NAME", "test-bucket")

    def fake_path(bucket, entity_path, source="sleeper"):
        return f"path::{source}::{entity_path}"
    monkeypatch.setattr(mod, "get_latest_bronze_path", fake_path)

    sleeper = pl.DataFrame({
        "player_id": ["100", "200"],
        "full_name": ["Patrick Mahomes", None],
        "first_name": ["Patrick", "Justin"],
        "last_name": ["Mahomes", "Jefferson"],
        "position": ["QB", "WR"],
        "team": ["KC", "MIN"],
        "age": [28, 24],
        "height": ["75", "73"],
        "weight": ["225", "195"],
        "years_exp": [7, 4],
        "status": ["Active", "Active"],
        "injury_status": [None, None],
        "swish_id": ["s100", "s200"],
        "birth_date": ["1995-09-17", "1999-06-16"],
    })
    ff_ids = pl.DataFrame({
        "sleeper_id": ["100", "200"],
        "gsis_id": ["G100", "G200"],
        "ktc_id": [1, 2],
        "fantasy_data_id": [11, 22],
        "rotoworld_id": [111, 222],
        "espn_id": [1111, 2222],
        "yahoo_id": [11111, 22222],
        "pff_id": ["p1", "p2"],
    })
    nfl_players = pl.DataFrame({
        "gsis_id": ["G100", "G200"],
        "headshot": ["http://img/100.png", "http://img/200.png"],
        "college_name": ["Texas Tech", "LSU"],
        "draft_year": [2017, 2020],
        "draft_round": [1, 1],
        "draft_pick": [10, 22],
    })
    fake_gcs["path::sleeper::league/players/incremental"] = sleeper
    fake_gcs["path::nflverse::fantasy_player_ids"] = ff_ids
    fake_gcs["path::nflverse::players"] = nfl_players
    return fake_gcs


class TestDimPlayerMaster:
    def test_player_key_and_ids_joined(self, seeded):
        df = transform_dim_players_master()
        row = df.filter(pl.col("player_key") == "100").to_dicts()[0]
        assert row["gsis_id"] == "G100"
        assert row["ktc_id"] == 1
        assert row["espn_id"] == 1111

    def test_display_name_coalesces_full_then_first_last(self, seeded):
        df = transform_dim_players_master()
        by_key = {r["player_key"]: r for r in df.to_dicts()}
        assert by_key["100"]["display_name"] == "Patrick Mahomes"   # full_name
        assert by_key["200"]["display_name"] == "Justin Jefferson"  # first + last

    def test_nfl_enrichment_columns_present(self, seeded):
        df = transform_dim_players_master()
        row = df.filter(pl.col("player_key") == "100").to_dicts()[0]
        assert row["college_name"] == "Texas Tech"
        assert row["nfl_draft_pick"] == 10

    def test_bridge_gsis_wins_over_sleepers_sparse_gsis_column(self, seeded):
        # Sleeper's own feed carries a gsis_id column, mostly null; the bridge's must not be lost to a
        # same-named join column (the master mapped a fifth of the KTC-priced pool until 2026-10-09)
        seeded["path::sleeper::league/players/incremental"] = seeded["path::sleeper::league/players/incremental"].with_columns(
            pl.Series("gsis_id", [None, " G200 "]))
        seeded["path::nflverse::fantasy_player_ids"] = seeded["path::nflverse::fantasy_player_ids"].with_columns(pl.Series("gsis_id", ["G100", None]))
        by_key = {r["player_key"]: r for r in transform_dim_players_master().to_dicts()}
        assert by_key["100"]["gsis_id"] == "G100"      # the bridge
        assert by_key["200"]["gsis_id"] == "G200"      # Sleeper's, stripped, when the bridge has none

    def test_one_player_per_gsis_placeholders_and_inactive_namesakes_lose_it(self, seeded):
        sl = seeded["path::sleeper::league/players/incremental"]
        extra = pl.concat([
            sl.filter(pl.col("player_id") == "100").with_columns(pl.lit("300").alias("player_id"), pl.lit("Duplicate Player").alias("full_name"), pl.lit("Inactive").alias("status")),
            sl.filter(pl.col("player_id") == "100").with_columns(pl.lit("400").alias("player_id"), pl.lit("Pat Mahomes").alias("full_name"), pl.lit("Inactive").alias("status")),
        ])
        seeded["path::sleeper::league/players/incremental"] = pl.concat([sl, extra])
        ff = seeded["path::nflverse::fantasy_player_ids"]
        seeded["path::nflverse::fantasy_player_ids"] = pl.concat([ff, ff.filter(pl.col("sleeper_id") == "100").with_columns(pl.lit("300").alias("sleeper_id")),
                                                                  ff.filter(pl.col("sleeper_id") == "100").with_columns(pl.lit("400").alias("sleeper_id"))])
        df = transform_dim_players_master()
        by_key = {r["player_key"]: r for r in df.to_dicts()}
        assert by_key["100"]["gsis_id"] == "G100" and by_key["300"]["gsis_id"] is None and by_key["400"]["gsis_id"] is None
        assert df.height == 4                                                  # the rows stay: the ledger keys on player_key
        assert df.filter(pl.col("gsis_id").is_not_null()).select("gsis_id").n_unique() == df.filter(pl.col("gsis_id").is_not_null()).height

    def test_avatar_url_is_a_url_not_a_raw_id(self, seeded):
        # SPEC: when only a swish_id is available, avatar_url should be a usable
        # Sleeper CDN URL, not the bare id. Currently it coalesces the raw
        # swish_id, so this documents the intended behaviour.
        no_headshot = seeded["path::nflverse::players"].with_columns(
            pl.lit(None).alias("headshot")
        )
        seeded["path::nflverse::players"] = no_headshot
        df = transform_dim_players_master()
        row = df.filter(pl.col("player_key") == "100").to_dicts()[0]
        assert str(row["avatar_url"]).startswith("http")
