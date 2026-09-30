"""silver_fantasy/fact_player_season.py

Season rollup of the player-week fact: sums weeks to season, last team by week, ppg,
and carries bio through unchanged.
"""
import polars as pl

import silver_fantasy.fact_player_season as fps


def _wk(rows):
    base = dict(
        fpts_ppr_nflverse=0.0, pass_att=0, pass_cmp=0, pass_yds=0, pass_tds=0, pass_int=0,
        rush_att=0, rush_yds=0, rush_tds=0, targets=0, rec=0, rec_yds=0, rec_tds=0,
        target_share=0.0, wopr=0.0, birth_date="1999-09-01", draft_round=1, draft_pick=5, rookie_season=2020,
        college_name="X", age_at_season=24.0, exp_at_season=3, is_rookie=False,
        is_undrafted=False, scored_under_league_id="L", player_name="P1", position="RB",
    )
    out = []
    for r in rows:
        d = dict(base)
        d.update(player_id="p1", season=2023, team="KC")
        d.update(r)
        out.append(d)
    return pl.DataFrame(out)


class TestRollup:
    def test_sums_weeks_to_season(self):
        wk = _wk([{"week": 1, "fpts": 20.0, "rush_yds": 100, "rush_tds": 1, "team": "KC"},
                  {"week": 2, "fpts": 10.0, "rush_yds": 50, "team": "DET"}])
        row = fps.rollup_to_season(wk).to_dicts()[0]
        assert row["games"] == 2
        assert abs(row["fpts"] - 30.0) < 1e-9 and abs(row["ppg"] - 15.0) < 1e-9
        assert row["rush_yds"] == 150
        assert row["team"] == "DET"          # last team by week
        assert row["age_at_season"] == 24.0  # bio carried through unchanged

    def test_one_row_per_player_season(self):
        wk = _wk([{"week": 1, "fpts": 5.0}, {"week": 2, "fpts": 7.0},
                  {"player_id": "p2", "week": 1, "fpts": 3.0}])
        assert fps.rollup_to_season(wk).height == 2


class TestSeasonComplete:
    """Season-level flag: the final regular-season week has been played (18 since 2021, 17
    before). The in-progress season is in the lake too and must not become a T+1 target."""

    def test_flagged_by_final_week_of_the_season(self):
        wk = pl.concat([
            _wk([{"season": 2020, "week": 17, "fpts": 1.0}]),
            _wk([{"season": 2024, "week": 18, "fpts": 1.0}]),
            _wk([{"season": 2024, "week": 5, "fpts": 1.0, "player_id": "p2"}]),   # played fewer weeks
            _wk([{"season": 2026, "week": 3, "fpts": 1.0}]),                       # in progress
        ])
        out = fps.rollup_to_season(wk)
        by = {(r["player_id"], r["season"]): r["season_complete"] for r in out.to_dicts()}
        assert by[("p1", 2020)] is True
        assert by[("p1", 2024)] is True
        assert by[("p2", 2024)] is True        # season-level, not per player
        assert by[("p1", 2026)] is False

    def test_2021_plus_needs_week_18(self):
        wk = _wk([{"season": 2022, "week": 17, "fpts": 1.0}])
        assert fps.rollup_to_season(wk)["season_complete"][0] is False
