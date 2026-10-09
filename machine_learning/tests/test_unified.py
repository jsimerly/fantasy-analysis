"""unified: snapshot rows in the career schema (to-date season columns, last season in the lags, the
career to date, row_week), appended without touching the played rows, never a future season, never a
test row, and dropped from the played-season view."""
import polars as pl

import career
import draft_rows as dr
import unified


def _weeks():
    rows = []
    for s, n, base in ((2022, 12, 8.0), (2023, 10, 12.0)):
        for w in range(1, n + 1):
            rows.append(dict(player_id="A", season=s, week=w, fpts=base + (w % 3), targets=6, rush_att=1, rec=4, pass_att=0, target_share=0.2, wopr=0.4,
                             game_date=None, player_name="A", position="WR", team="KC", age_at_season=24.0 + s - 2022, exp_at_season=2 + s - 2022,
                             draft_round=1, draft_pick=20, is_undrafted=False, is_rookie=False))
    return pl.DataFrame(rows)


def _seasons():
    return pl.DataFrame({
        "player_id": ["A", "A", "A"], "season": [2021, 2022, 2023], "position": ["WR"] * 3, "player_name": ["A"] * 3, "team": ["KC"] * 3,
        "fpts": [80.0, 110.0, 130.0], "games": [14, 12, 10], "ppg": [80 / 14, 110 / 12, 13.0], "age_at_season": [23.0, 24.0, 25.0], "exp_at_season": [1, 2, 3],
        "draft_round": [1] * 3, "draft_pick": [20] * 3, "is_undrafted": [False] * 3, "is_rookie": [False] * 3,
        "lag1_fpts": [None, 80.0, 110.0], "lag1_ppg": [None, 80 / 14, 110 / 12], "lag1_games": [None, 14, 12],
        "best_ppg": [80 / 14, 110 / 12, 13.0], "career_seasons": [1, 2, 3], "career_fpts": [80.0, 190.0, 320.0], "career_games": [14, 26, 36],
        "total_touches": [60, 50, 40], "targets": [70, 60, 50], "pass_yds": [0, 0, 0], "season_complete": [True, True, True],
    })


class TestSnapshotRows:
    def test_schema_mapping_at_a_checkpoint_week(self):
        rows = unified.snapshot_rows(_weeks(), _seasons(), weeks=[6], from_season=2022).sort("season")
        assert rows["season"].to_list() == [2022, 2023] and rows["row_week"].to_list() == [6, 6] and rows["is_snapshot_row"].all()
        r = rows.filter(pl.col("season") == 2023).row(0, named=True)
        td = [12.0 + (w % 3) for w in range(1, 7)]
        assert r["games"] == 6 and abs(r["fpts"] - sum(td)) < 1e-9 and abs(r["ppg"] - sum(td) / 6) < 1e-9 and r["targets"] == 36
        assert r["lag1_fpts"] == 110.0 and r["lag1_games"] == 12 and r["lag2_fpts"] == 80.0           # last season, the one before
        assert r["career_seasons"] == 3 and abs(r["career_fpts"] - (190.0 + sum(td))) < 1e-9 and r["career_games"] == 26 + 6
        assert abs(r["best_ppg"] - max(110 / 12, sum(td) / 6)) < 1e-9 and r["season_complete"] is True and r["age_at_season"] == 25.0

    def test_appended_rows_leave_played_rows_alone_and_never_serve_as_a_future(self):
        base = career.career_features(_seasons().with_columns(pl.lit(False).alias("is_draft_row")))
        rows = unified.snapshot_rows(_weeks(), _seasons(), weeks=[6], from_season=2022)
        m = career.attach_horizon_targets(unified.with_snapshot_rows(base, rows), [1])
        played = m.filter(~pl.col("is_snapshot_row"))
        assert played.height == 3 and (played["row_week"] == 18).all()
        p22 = played.filter(pl.col("season") == 2022).row(0, named=True)
        assert p22["h1_fpts"] == 130.0 and p22["h1_games"] == 10                                     # the real 2023 season, not the week-6 snapshot
        s22 = m.filter(pl.col("is_snapshot_row") & (pl.col("season") == 2022)).row(0, named=True)
        assert s22["h1_fpts"] == 130.0                                                               # the snapshot shares the played row's targets
        assert dr.drop_draft_rows(m).height == 3                                                     # the played-season view drops snapshots too
        assert m.select(dr.is_aux(m).alias("x"))["x"].sum() == 2                                      # the two snapshot rows are auxiliary


class TestMask:
    def test_group_columns_are_blanked_on_snapshot_rows_only(self):
        df = pl.DataFrame({"is_snapshot_row": [True, False, None], "inj_weeks_out": [3.0, 2.0, 1.0], "ppg": [9.0, 8.0, 7.0]})
        out = unified.mask_group_columns(df, ["inj_weeks_out", "ppg_not_there"])
        assert out["inj_weeks_out"].to_list() == [None, 2.0, 1.0] and out["ppg"].to_list() == [9.0, 8.0, 7.0]
        assert unified.mask_group_columns(df.drop("is_snapshot_row"), ["inj_weeks_out"]).equals(df.drop("is_snapshot_row"))
