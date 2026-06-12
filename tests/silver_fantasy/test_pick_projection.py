"""silver_fantasy/_pick_projection.py

Measure-layer projection (v1 of algo(team, pick_data)): reverse-standings draft
slot -> Early/Mid/Late tier for the next draft. Lives outside any fact on purpose.
"""
import polars as pl

from tests.de_loader import load_de_module

mod = load_de_module(
    "silver_fantasy/_pick_projection.py", "silver_fantasy", "pick_projection"
)
compute_draft_slots = mod.compute_draft_slots


def _leagues(total=6, season="2025", league="CUR"):
    return pl.DataFrame({
        "league_id": [league], "league_lineage_id": [league],
        "season": [season], "status": ["complete"], "total_rosters": [total],
    })


def _team_state(records, league="CUR"):
    # records: list of (roster_id, wins, fpts)
    return pl.DataFrame({
        "league_id": [league] * len(records),
        "roster_id": [r[0] for r in records],
        "wins": [r[1] for r in records],
        "fpts": [r[2] for r in records],
    })


class TestComputeDraftSlots:
    def _standings6(self):
        return _team_state([(1, 12, 2000), (2, 10, 1900), (3, 8, 1800),
                            (4, 6, 1700), (5, 4, 1600), (6, 2, 1500)])

    def test_worst_team_drafts_first(self):
        slots = compute_draft_slots(self._standings6(), _leagues(total=6))
        by_roster = {r["roster_id"]: r for r in slots.to_dicts()}
        assert by_roster[6]["draft_slot"] == 1     # worst record -> 1.01
        assert by_roster[1]["draft_slot"] == 6     # best record -> last

    def test_tiers_are_terciles(self):
        slots = compute_draft_slots(self._standings6(), _leagues(total=6))
        by_roster = {r["roster_id"]: r["tier"] for r in slots.to_dicts()}
        assert by_roster[6] == "Early"   # slot 1
        assert by_roster[5] == "Early"   # slot 2
        assert by_roster[4] == "Mid"     # slot 3
        assert by_roster[2] == "Late"    # slot 5
        assert by_roster[1] == "Late"    # slot 6

    def test_applies_to_next_draft_season(self):
        slots = compute_draft_slots(self._standings6(), _leagues(season="2025"))
        assert set(slots["draft_season"].to_list()) == {2026}   # 2025 standings -> 2026 draft

    def test_fpts_breaks_tie_worst_first(self):
        ts = _team_state([(1, 5, 2000), (2, 5, 1000)])   # same wins, roster 2 fewer pts
        slots = compute_draft_slots(ts, _leagues(total=2))
        by_roster = {r["roster_id"]: r["draft_slot"] for r in slots.to_dicts()}
        assert by_roster[2] == 1     # fewer points -> drafts first
