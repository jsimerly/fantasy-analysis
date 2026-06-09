"""silver_fantasy/fact_roster_membership.py

Asset-level (player + pick) daily ownership with reconciliation. Players come
from Sleeper's authoritative snapshot; pick ownership is rebuilt by overlaying
original -> traded -> txn event log -> commissioner overrides. The reconcile
step must conserve assets (no double ownership, every league/season/round
resolves to exactly total_rosters picks, every moved pick lands on a real
franchise) and quarantine anything that doesn't instead of dropping it.
"""
from datetime import datetime

import polars as pl

from tests.de_loader import load_de_module

mod = load_de_module(
    "silver_fantasy/fact_roster_membership.py", "silver_fantasy", "fact_roster_membership"
)
build_player_membership = mod.build_player_membership
resolve_pick_ownership = mod.resolve_pick_ownership
build_pick_membership = mod.build_pick_membership
build_pick_universe = mod.build_pick_universe
reconcile = mod.reconcile


# --- shared fixtures -------------------------------------------------------
def _dim_franchise(n=6, league="L1"):
    return pl.DataFrame({
        "league_id": [league] * n,
        "roster_id": list(range(1, n + 1)),
        "franchise_id": [f"F{i}" for i in range(1, n + 1)],
    })


def _leagues(total_rosters=6, league="L1"):
    # Single league, lineage = itself, far-future season + in_season so the
    # future-pick filter keeps every test pick (no completed season in lineage).
    return pl.DataFrame({
        "league_id": [league],
        "league_lineage_id": [league],
        "season": ["2099"],
        "status": ["in_season"],
        "total_rosters": [total_rosters],
    })


def _dim_player():
    return pl.DataFrame({
        "player_key": ["100", "200"],
        "position": ["QB", "WR"],
    })


def _traded(rows, league="L1"):
    # rows: list of (season, round, original, owner)
    return pl.DataFrame({
        "league_id": [league] * len(rows),
        "season": [r[0] for r in rows],
        "round": [r[1] for r in rows],
        "original_roster_id": [r[2] for r in rows],
        "owner_roster_id": [r[3] for r in rows],
        "previous_owner_roster_id": [None] * len(rows),
        "timestamp": [datetime(2025, 1, 1)] * len(rows),
    })


def _overrides(rows, league="L1"):
    # rows: list of (season, round, original_roster_id, to_team_id)
    return pl.DataFrame({
        "transaction_id": [None] * len(rows),
        "league_id": [league] * len(rows),
        "draft_pick_id": [f"{r[2]},{r[0]},{r[1]},x,{r[3]}" for r in rows],
        "roster_id": [r[2] for r in rows],
        "season": [r[0] for r in rows],
        "round": [r[1] for r in rows],
        "from_team_id": [0] * len(rows),
        "to_team_id": [r[3] for r in rows],
        "created": [1649198791290 + i for i in range(len(rows))],
    })


_EMPTY = pl.DataFrame()


# --- players ---------------------------------------------------------------
class TestPlayerMembership:
    def _rosters(self):
        return pl.DataFrame({
            "league_id": ["L1", "L1", "L1"],
            "roster_id": [1, 1, 2],
            "owner_id": ["U1", "U1", "U2"],
            "player_id": ["100", "200", "999"],   # 999 not in player dim
            "is_starter": [True, False, True],
            "is_taxi": [False, False, False],
            "is_reserve": [False, True, False],
            "is_active": [True, True, True],
            "timestamp": [datetime(2025, 1, 1)] * 3,
        })

    def test_player_resolves_to_franchise_and_keeps_flags(self):
        out = build_player_membership(self._rosters(), _dim_franchise(), _dim_player())
        row = out.filter(pl.col("asset_id") == "100").to_dicts()[0]
        assert row["asset_type"] == "player"
        assert row["franchise_id"] == "F1"
        assert row["player_key"] == "100"
        assert row["position"] == "QB"
        assert row["is_starter"] is True
        assert row["is_reserve"] is False

    def test_unmapped_player_has_null_player_key(self):
        out = build_player_membership(self._rosters(), _dim_franchise(), _dim_player())
        row = out.filter(pl.col("asset_id") == "999").to_dicts()[0]
        assert row["franchise_id"] == "F2"     # franchise still resolves
        assert row["player_key"] is None        # but not in the player dim


# --- pick resolution -------------------------------------------------------
class TestResolvePickOwnership:
    def test_never_traded_pick_defaults_to_original_owner(self):
        # (2024,1) is observed via roster 3's traded pick, so the whole round is
        # enumerated; roster 2's pick was never traded -> owner is itself.
        traded = _traded([("2024", 1, 3, 1)])
        resolved = resolve_pick_ownership(traded, _EMPTY, _EMPTY, _leagues())
        row = resolved.filter(
            (pl.col("season") == "2024") & (pl.col("round") == 1)
            & (pl.col("original_roster_id") == 2)
        ).to_dicts()[0]
        assert row["owner_roster_id"] == 2

    def test_traded_pick_resolves_to_new_owner(self):
        traded = _traded([("2024", 1, 3, 1)])   # roster 3's pick now owned by 1
        resolved = resolve_pick_ownership(traded, _EMPTY, _EMPTY, _leagues())
        row = resolved.filter(
            (pl.col("season") == "2024") & (pl.col("round") == 1)
            & (pl.col("original_roster_id") == 3)
        ).to_dicts()[0]
        assert row["owner_roster_id"] == 1

    def test_commissioner_override_beats_sleeper_traded_state(self):
        # Real 2022 3-way trade row: pick 1,2023,1 -> ends with roster 1, even
        # though Sleeper's traded state still shows it held by roster 5.
        traded = _traded([("2023", 1, 1, 5)])             # Sleeper says owner 5
        overrides = _overrides([("2023", 1, 1, 1)])        # commissioner says 1
        resolved = resolve_pick_ownership(traded, _EMPTY, overrides, _leagues())
        row = resolved.filter(
            (pl.col("season") == "2023") & (pl.col("round") == 1)
            & (pl.col("original_roster_id") == 1)
        ).to_dicts()[0]
        assert row["owner_roster_id"] == 1


# --- lineage automation (survives season rollover) -------------------------
def _lineage_leagues():
    # One dynasty lineage "ROOT": an OLD 2024 league (complete) succeeded by the
    # CUR 2025 league (complete). Current league = CUR; latest completed = 2025,
    # so only 2026+ picks survive the future-pick filter.
    return pl.DataFrame({
        "league_id": ["OLD", "CUR"],
        "league_lineage_id": ["ROOT", "ROOT"],
        "season": ["2024", "2025"],
        "status": ["complete", "complete"],
        "total_rosters": [6, 6],
    })


class TestLineageAutomation:
    def test_override_under_old_league_id_applies_to_current_league(self):
        # A commissioner override hand-coded under the OLD (2024) league_id for a
        # future 2026 pick must resolve onto the CURRENT league (CUR) — this is
        # what keeps overrides working after a season rollover without edits.
        traded = _traded([("2026", 1, 5, 4)], league="CUR")     # makes (2026,1) observed
        overrides = _overrides([("2026", 1, 3, 2)], league="OLD")
        resolved = resolve_pick_ownership(traded, _EMPTY, overrides, _lineage_leagues())
        row = resolved.filter(
            (pl.col("league_id") == "CUR") & (pl.col("season") == "2026")
            & (pl.col("round") == 1) & (pl.col("original_roster_id") == 3)
        ).to_dicts()[0]
        assert row["owner_roster_id"] == 2

    def test_already_drafted_seasons_are_filtered_out(self):
        # 2025 is the latest completed season in the lineage, so its picks are
        # already drafted and must not appear in current ownership.
        traded = _traded([("2025", 1, 3, 1), ("2026", 1, 3, 1)], league="CUR")
        resolved = resolve_pick_ownership(traded, _EMPTY, _EMPTY, _lineage_leagues())
        seasons = set(resolved["season"].to_list())
        assert "2025" not in seasons
        assert "2026" in seasons

    def test_draft_status_drops_picks_before_league_completes(self):
        # In-season hole: the 2025 league is still in_season (league-status would
        # keep 2025 picks), but the 2025 rookie draft is already complete -> its
        # picks are spent. Draft status must drop them.
        leagues = pl.DataFrame({
            "league_id": ["OLD", "CUR"],
            "league_lineage_id": ["ROOT", "ROOT"],
            "season": ["2024", "2025"],
            "status": ["complete", "in_season"],     # CUR not complete yet
            "total_rosters": [6, 6],
        })
        drafts = pl.DataFrame({
            "league_id": ["OLD", "CUR"],
            "season": ["2024", "2025"],
            "status": ["complete", "complete"],       # but 2025 draft IS done
        })
        traded = _traded([("2025", 1, 3, 1), ("2026", 1, 3, 1)], league="CUR")

        # Without drafts -> league-status fallback keeps 2025 (the bug).
        no_drafts = resolve_pick_ownership(traded, _EMPTY, _EMPTY, leagues)
        assert "2025" in set(no_drafts["season"].to_list())

        # With drafts -> draft-status cutoff drops 2025, keeps 2026.
        with_drafts = resolve_pick_ownership(traded, _EMPTY, _EMPTY, leagues, drafts)
        seasons = set(with_drafts["season"].to_list())
        assert "2025" not in seasons
        assert "2026" in seasons


# --- deterministic pick universe (untraded picks are still accounted) ------
def _drafts(rows, status="complete"):
    # rows: list of (league_id, season, rounds)
    return pl.DataFrame({
        "league_id": [r[0] for r in rows],
        "season": [r[1] for r in rows],
        "status": [status] * len(rows),
        "rounds": [r[2] for r in rows],
    })


class TestPickUniverse:
    def test_full_grid_enumerated_with_zero_trades(self):
        # 3 future seasons x 3 rounds x 6 rosters = 54 picks, even with no trades.
        current_meta = pl.DataFrame({
            "league_id": ["CUR"], "total_rosters": [6],
            "cutoff_season": [2025], "rookie_rounds": [3],
        })
        empty = pl.DataFrame(schema={"league_id": pl.Utf8, "season": pl.Utf8, "round": pl.Int64})
        universe = build_pick_universe(empty, current_meta, years_ahead=3)
        assert universe.height == 3 * 3 * 6
        assert set(universe["season"].to_list()) == {"2026", "2027", "2028"}
        assert set(universe["round"].to_list()) == {1, 2, 3}
        # every (season, round) carries all 6 original rosters
        per = universe.group_by(["season", "round"]).len()
        assert per["len"].to_list() == [6] * 9

    def test_rounds_are_dynamic_per_league(self):
        current_meta = pl.DataFrame({
            "league_id": ["A", "B"], "total_rosters": [12, 10],
            "cutoff_season": [2025, 2025], "rookie_rounds": [4, 2],   # different
        })
        empty = pl.DataFrame(schema={"league_id": pl.Utf8, "season": pl.Utf8, "round": pl.Int64})
        u = build_pick_universe(empty, current_meta, years_ahead=3)
        a = u.filter(pl.col("league_id") == "A")
        b = u.filter(pl.col("league_id") == "B")
        assert set(a["round"].to_list()) == {1, 2, 3, 4}
        assert a.height == 3 * 4 * 12
        assert set(b["round"].to_list()) == {1, 2}
        assert b.height == 3 * 2 * 10


class TestResolveWithDrafts:
    def test_untraded_rounds_resolve_to_original_owner(self):
        # CUR is a 6-team league with 3 rookie rounds; 2025 draft complete. With
        # ZERO trades, every one of the 54 future picks must still appear, owned
        # by its original roster.
        leagues = pl.DataFrame({
            "league_id": ["CUR"], "league_lineage_id": ["CUR"],
            "season": ["2025"], "status": ["complete"], "total_rosters": [6],
        })
        drafts = _drafts([("CUR", "2025", 3)])
        resolved = resolve_pick_ownership(_EMPTY, _EMPTY, _EMPTY, leagues, drafts)
        assert resolved.height == 3 * 3 * 6
        # a never-traded pick is owned by its original roster
        row = resolved.filter(
            (pl.col("season") == "2027") & (pl.col("round") == 3)
            & (pl.col("original_roster_id") == 4)
        ).to_dicts()[0]
        assert row["owner_roster_id"] == 4


# --- pick membership / franchise mapping -----------------------------------
class TestBuildPickMembership:
    def test_asset_id_and_franchise(self):
        resolved = resolve_pick_ownership(_traded([("2024", 1, 3, 1)]), _EMPTY, _EMPTY, _leagues())
        out = build_pick_membership(resolved, _dim_franchise())
        row = out.filter(pl.col("asset_id") == "2024:1:3").to_dicts()[0]
        assert row["asset_type"] == "pick"
        assert row["franchise_id"] == "F1"     # roster 3's pick owned by roster 1
        assert row["pick_original_roster_id"] == 3

    def test_pick_owned_by_unknown_roster_has_null_franchise(self):
        resolved = resolve_pick_ownership(_traded([("2024", 1, 3, 99)]), _EMPTY, _EMPTY, _leagues())
        out = build_pick_membership(resolved, _dim_franchise())
        row = out.filter(pl.col("asset_id") == "2024:1:3").to_dicts()[0]
        assert row["franchise_id"] is None     # roster 99 not in franchise dim


# --- reconciliation --------------------------------------------------------
def _good_pipeline():
    rosters = pl.DataFrame({
        "league_id": ["L1", "L1"], "roster_id": [1, 2], "owner_id": ["U1", "U2"],
        "player_id": ["100", "200"],
        "is_starter": [True, True], "is_taxi": [False, False],
        "is_reserve": [False, False], "is_active": [True, True],
        "timestamp": [datetime(2025, 1, 1)] * 2,
    })
    players = build_player_membership(rosters, _dim_franchise(), _dim_player())
    resolved = resolve_pick_ownership(
        _traded([("2024", 1, 3, 1)]), _EMPTY, _overrides([("2024", 1, 5, 2)]), _leagues()
    )
    picks = build_pick_membership(resolved, _dim_franchise())
    return players, picks


class TestReconcile:
    def test_good_data_quarantine_empty_and_all_rows_kept(self):
        players, picks = _good_pipeline()
        fact, quarantine = reconcile(players, picks, _leagues())
        assert quarantine.height == 0
        # 2 players + 6 picks (one observed season/round x total_rosters)
        assert fact.filter(pl.col("asset_type") == "pick").height == 6
        assert fact.filter(pl.col("asset_type") == "player").height == 2

    def test_pick_count_conserved_per_season_round(self):
        _, picks = _good_pipeline()
        fact, _ = reconcile(_good_pipeline()[0], picks, _leagues())
        counts = (
            fact.filter(pl.col("asset_type") == "pick")
            .group_by(["pick_season", "pick_round"]).len()
        )
        assert counts["len"].to_list() == [6]   # == total_rosters, none lost/dup

    def test_duplicate_ownership_is_quarantined_and_excluded(self):
        players, picks = _good_pipeline()
        # Seed the same pick asset owned by a second franchise.
        dup = picks.head(1).with_columns(pl.lit("F4").alias("franchise_id"))
        picks_bad = pl.concat([picks, dup], how="diagonal_relaxed")
        fact, quarantine = reconcile(players, picks_bad, _leagues())
        bad_asset = dup.to_dicts()[0]["asset_id"]
        assert "duplicate_ownership" in quarantine["quarantine_reason"].to_list()
        # the conflicted asset must not appear in the published fact
        assert fact.filter(pl.col("asset_id") == bad_asset).height == 0

    def test_orphan_pick_with_null_franchise_is_quarantined(self):
        players, _ = _good_pipeline()
        resolved = resolve_pick_ownership(_traded([("2024", 1, 3, 99)]), _EMPTY, _EMPTY, _leagues())
        picks = build_pick_membership(resolved, _dim_franchise())
        fact, quarantine = reconcile(players, picks, _leagues())
        assert "unresolved_franchise" in quarantine["quarantine_reason"].to_list()
        # no fact pick row may carry a null franchise
        assert fact.filter(
            (pl.col("asset_type") == "pick") & pl.col("franchise_id").is_null()
        ).height == 0

    def test_unmapped_player_warned_but_kept_in_fact(self):
        rosters = pl.DataFrame({
            "league_id": ["L1"], "roster_id": [1], "owner_id": ["U1"],
            "player_id": ["999"],   # not in player dim
            "is_starter": [True], "is_taxi": [False], "is_reserve": [False],
            "is_active": [True], "timestamp": [datetime(2025, 1, 1)],
        })
        players = build_player_membership(rosters, _dim_franchise(), _dim_player())
        fact, quarantine = reconcile(players, players.clear(), _leagues())
        assert "unmapped_player" in quarantine["quarantine_reason"].to_list()
        # franchise is known, so the row stays in the fact (not dropped silently)
        assert fact.filter(pl.col("asset_id") == "999").height == 1
