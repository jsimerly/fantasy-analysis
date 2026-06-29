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
