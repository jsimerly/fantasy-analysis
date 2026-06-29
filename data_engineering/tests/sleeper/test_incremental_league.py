"""sleeper_ingestion/daily/incremental_league.py

flatten_league_to_parquets splits one league payload into leagues / settings /
scoring / roster-slot frames. The scoring frame is currently built from a stray
singular variable, so a league with no scoring_settings raises -> spec fails.
"""
import polars as pl
import pytest


import sleeper_ingestion.daily.incremental_league as mod
flatten_league_to_parquets = mod.flatten_league_to_parquets


def _league(with_scoring=True):
    league = {
        "league_id": "L1", "name": "My League", "season": "2024",
        "status": "in_season", "season_type": "regular", "total_rosters": 12,
        "draft_id": "D1", "bracket_id": "B1", "previous_league_id": None,
        "settings": {"leg": 3, "last_scored_leg": 2, "taxi_slots": 4, "reserve_slots": 2},
        "roster_positions": ["QB", "RB", "RB", "WR", "WR", "FLEX", "BN", "BN"],
    }
    if with_scoring:
        league["scoring_settings"] = {"pass_td": 4.0, "rec": 1.0}
    return league


class TestFlattenLeague:
    def test_returns_four_frames(self):
        out = flatten_league_to_parquets(_league())
        assert len(out) == 4
        for df in out:
            assert isinstance(df, pl.DataFrame)

    def test_leagues_frame_header(self):
        leagues_df, _, _, _ = flatten_league_to_parquets(_league())
        row = leagues_df.to_dicts()[0]
        assert row["league_id"] == "L1"
        assert row["league_name"] == "My League"
        assert row["leg"] == 3
        assert row["last_scored_leg"] == 2

    def test_roster_slot_counts(self):
        _, _, _, rosters_df = flatten_league_to_parquets(_league())
        row = rosters_df.to_dicts()[0]
        assert row["RB"] == 2          # two RB positions counted
        assert row["BN"] == 2
        assert row["TAXI"] == 4        # from settings.taxi_slots
        assert row["IR"] == 2          # from settings.reserve_slots

    def test_scoring_frame_present_when_scoring_exists(self):
        _, _, scoring_df, _ = flatten_league_to_parquets(_league(with_scoring=True))
        row = scoring_df.to_dicts()[0]
        assert row["league_id"] == "L1"
        assert row["pass_td"] == 4.0

    def test_missing_scoring_settings_does_not_crash(self):
        # SPEC: a league without scoring_settings should still yield a scoring
        # frame carrying at least the league_id (no NameError on a stray var).
        _, _, scoring_df, _ = flatten_league_to_parquets(_league(with_scoring=False))
        assert scoring_df.height == 1
        assert scoring_df.to_dicts()[0]["league_id"] == "L1"


class TestOffseasonSettingsDrift:
    """Sleeper omits season-derived settings keys until the games are played: a fresh
    season is `in_season` at leg 1 with NO `last_scored_leg` key yet. flatten must
    tolerate that (None), not KeyError. Regression for the 2026-06-27 Cloud Run failure
    on 'Stuck in High School'. We tolerate *legitimately-absent* optional keys; truly
    required identity (league_id) is covered separately below."""

    def _unscored(self):
        lg = _league()
        lg["settings"] = {"taxi_slots": 3, "reserve_slots": 2, "leg": 1}  # no last_scored_leg
        return lg

    def test_missing_last_scored_leg_does_not_crash(self):
        leagues_df, _, _, _ = flatten_league_to_parquets(self._unscored())
        row = leagues_df.to_dicts()[0]
        assert row["leg"] == 1
        assert row["last_scored_leg"] is None        # absent -> null, not a crash

    def test_missing_leg_also_tolerated(self):
        lg = _league()
        lg["settings"] = {"taxi_slots": 3, "reserve_slots": 2}  # neither leg nor last_scored_leg
        leagues_df, _, _, _ = flatten_league_to_parquets(lg)
        row = leagues_df.to_dicts()[0]
        assert row["leg"] is None and row["last_scored_leg"] is None

    def test_leg_columns_are_nullable_int64(self):
        # SPEC: the leg columns must be Int64 (nullable) even when the value is absent,
        # NOT a Null-dtype column — otherwise vstacking a scored league onto an unscored
        # one fails on a dtype mismatch (the second-order bug behind the same incident).
        leagues_df, _, _, _ = flatten_league_to_parquets(self._unscored())
        assert leagues_df.schema["leg"] == pl.Int64
        assert leagues_df.schema["last_scored_leg"] == pl.Int64

    def test_scored_and_unscored_leagues_concat_cleanly(self):
        # The real failure path: main() vertically concats per-league frames. A league
        # that has scored (Int64) and one that hasn't (was Null) must concat without a
        # schema mismatch.
        scored, _, _, _ = flatten_league_to_parquets(_league())          # last_scored_leg=2
        unscored, _, _, _ = flatten_league_to_parquets(self._unscored())  # last_scored_leg absent
        combined = pl.concat([scored, unscored])                          # must not raise
        assert combined.height == 2
        assert combined["last_scored_leg"].to_list() == [2, None]

    def test_missing_league_id_still_raises(self):
        # Boundary: we are flexible with optional/season-derived keys, but a payload
        # missing a required identity field is a genuine break and SHOULD fail loudly
        # rather than silently write a keyless row.
        lg = _league()
        del lg["league_id"]
        with pytest.raises(KeyError):
            flatten_league_to_parquets(lg)


class TestMainNoActiveLeagues:
    def test_offseason_no_active_leagues_is_a_clean_noop(self, monkeypatch):
        # SPEC: when every league is complete (offseason) there are 0 active
        # leagues, so main() must no-op cleanly instead of pl.concat([])-ing an
        # empty list. It must not attempt any GCS writes.
        monkeypatch.setenv("GCS_BUCKET_NAME", "test-bucket")
        only_complete = pl.DataFrame({
            "league_id": ["L1"], "status": ["complete"], "source_system": ["sleeper"],
        })
        monkeypatch.setattr(mod, "get_fantasy_leagues", lambda: only_complete)
        # discovery is exercised separately; here it finds nothing (no rollover)
        monkeypatch.setattr(mod, "discover_new_season_leagues", lambda *a, **k: [])

        saved = []
        monkeypatch.setattr(mod, "save_df_to_gcs",
                            lambda *a, **k: saved.append(a))
        # If the guard is missing this raises "cannot concat empty list".
        assert mod.main() is None
        assert saved == []


def _next_season_league(lid="C", prev="B", season="2026", status="in_season"):
    return {
        "league_id": lid, "name": "Rolled Over", "season": season, "status": status,
        "season_type": "regular", "total_rosters": 12, "draft_id": "D", "bracket_id": None,
        "previous_league_id": prev,
        "settings": {"leg": 1, "last_scored_leg": 0}, "roster_positions": ["QB", "BN"],
        "scoring_settings": {},
    }


class TestDiscoverNewSeasonLeagues:
    """Roll-forward discovery: walk previous_league_id FORWARD from a lineage's latest
    known league to the new-season league Sleeper never points to."""

    KNOWN = pl.DataFrame({
        "league_id": ["A", "B"], "season": ["2024", "2025"],
        "league_lineage_id": ["A", "A"], "source_system": ["sleeper", "sleeper"],
    })

    def _patch(self, monkeypatch, user_leagues):
        monkeypatch.setattr(mod, "get_sport_state", lambda sport="nfl": {"season": "2026"})
        monkeypatch.setattr(mod, "get_rosters", lambda league_id: [{"owner_id": "u1"}])
        monkeypatch.setattr(mod, "get_user_leagues_for_year", user_leagues)

    def test_finds_rolled_over_league(self, monkeypatch):
        # latest known is B (2025); member u1's 2026 leagues include C whose previous is B
        self._patch(monkeypatch, lambda uid, sport, yr: [_next_season_league()] if yr == 2026 else [])
        out = mod.discover_new_season_leagues(self.KNOWN)
        assert [l["league_id"] for l in out] == ["C"]
        assert out[0]["previous_league_id"] == "B"

    def test_idempotent_when_already_known(self, monkeypatch):
        # C is already tracked -> nothing new (no duplicate ingestion)
        known = pl.concat([self.KNOWN, pl.DataFrame({
            "league_id": ["C"], "season": ["2026"], "league_lineage_id": ["A"],
            "source_system": ["sleeper"]})], how="vertical")
        self._patch(monkeypatch, lambda uid, sport, yr: [_next_season_league()] if yr == 2026 else [])
        assert mod.discover_new_season_leagues(known) == []

    def test_unrelated_league_not_picked_up(self, monkeypatch):
        # a league whose previous_league_id is NOT in our lineage must be ignored
        self._patch(monkeypatch,
                    lambda uid, sport, yr: [_next_season_league(lid="Z", prev="OTHER")] if yr == 2026 else [])
        assert mod.discover_new_season_leagues(self.KNOWN) == []

    def test_chains_multiple_seasons_behind(self, monkeypatch):
        # behind by two seasons: B(2025) -> C(2026) -> D(2027) discovered in one pass
        def user_leagues(uid, sport, yr):
            if yr == 2026: return [_next_season_league(lid="C", prev="B", season="2026")]
            if yr == 2027: return [_next_season_league(lid="D", prev="C", season="2027")]
            return []
        self._patch(monkeypatch, user_leagues)
        ids = sorted(l["league_id"] for l in mod.discover_new_season_leagues(self.KNOWN))
        assert ids == ["C", "D"]
