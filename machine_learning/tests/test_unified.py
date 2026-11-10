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


# ------------------------------------------------------------ season-to-date rows (BACKLOG 51)
def _td_weeks(player="a", season=2025, weeks=range(1, 18), fpts=10.0, **extra):
    import polars as pl
    return pl.DataFrame([{"player_id": player, "season": season, "week": w, "player_name": player, "position": "WR", "team": "X",
                          "fpts": fpts, "pass_att": 0, "pass_cmp": 0, "pass_yds": 0.0, "pass_tds": 0, "pass_int": 0, "rush_att": 1,
                          "rush_yds": 5.0, "rush_tds": 0, "targets": 8, "rec": 5, "rec_yds": 60.0, "rec_tds": 0, "target_share": 0.2, "wopr": 0.5,
                          "birth_date": None, "draft_round": 1, "draft_pick": 10, "rookie_season": 2024, "age_at_season": 23.0,
                          "exp_at_season": season - 2024, "is_rookie": season == 2024, "is_undrafted": False, **extra} for w in weeks])


def test_a_season_seen_through_week_w_reads_like_a_season_with_last_season_as_its_lags():
    import polars as pl
    import unified
    wk = pl.concat([_td_weeks(season=2024, fpts=8.0), _td_weeks(season=2025, fpts=10.0), _td_weeks(season=2026, weeks=range(1, 6), fpts=20.0)])
    season = unified.rollup_to_date(wk).with_columns(pl.col("season").is_in([2024, 2025]).alias("season_complete"))
    td = unified.season_to_date_rows(wk, season, [5, 9], from_season=2025)
    r = {(x["season"], x["row_week"]): x for x in td.to_dicts()}
    assert (2026, 9) not in r                                   # the current season has no week 9 yet
    now = r[(2026, 5)]
    assert now["games"] == 5 and now["ppg"] == 20.0 and now["lag1_ppg"] == 10.0 and now["lag2_ppg"] == 8.0
    assert now["career_seasons"] == 3 and now["career_games"] == 17 + 17 + 5 and now["best_ppg"] == 20.0
    assert now["is_snapshot_row"] and now["d_ppg_1"] == 10.0
    assert r[(2025, 9)]["games"] == 9 and r[(2025, 9)]["lag1_ppg"] == 8.0 and r[(2025, 9)]["career_seasons"] == 2


def test_the_survival_cap_tier_reads_the_games_pace_on_a_to_date_row():
    import polars as pl
    import unified
    df = pl.DataFrame({"games": [3, 3, 16], "row_week": [3, 9, 18], "is_snapshot_row": [True, True, False]})
    assert unified.with_tier_games(df)["tier_games"].to_list() == [17.0, 3 * 17 / 9, 16.0]


def test_a_to_date_projection_lands_one_season_later_in_the_tail():
    import polars as pl
    import unified
    pred = pl.DataFrame({"player_id": ["a"], "h1_ppg_hat": [15.0], "h2_ppg_hat": [14.0], "h2_games_hat": [12.0], "h2_ppg_q20": [10.0], "ppg": [20.0]})
    out = unified.shift_to_tail(pred, [1, 2, 3])
    assert out.columns == ["player_id", "h2_ppg_hat", "h3_ppg_hat", "h3_games_hat", "h3_ppg_q20"]
    assert out["h3_ppg_hat"].item() == 14.0


def test_one_random_week_per_player_season_and_the_training_table_targets():
    import polars as pl
    import unified
    wk = pl.concat([_td_weeks(season=s, fpts=8.0 + s - 2022) for s in (2022, 2023, 2024, 2025)])
    season = unified.rollup_to_date(wk).with_columns(pl.lit(True).alias("season_complete"), pl.lit(18).alias("row_week"))
    td = unified.season_to_date_rows(wk, season, list(range(2, 17)), from_season=2023)
    one = unified.one_week_per_season(td)
    assert one.group_by("player_id", "season").len()["len"].max() == 1 and one.height == 3
    table = unified.todate_training_table(season, td, [1, 2], as_of=2024)
    snap = table.filter(pl.col("is_snapshot_row").cast(pl.Float64, strict=False).fill_null(0.0) > 0.5)
    assert set(snap["season"].to_list()) == {2023, 2024}         # nothing after as_of
    r23 = snap.filter(pl.col("season") == 2023).row(0, named=True)
    assert r23["h1_ppg"] == 10.0 and r23["h2_ppg"] == 11.0     # a to-date row's targets are the complete seasons after it
    seasons = table.filter(pl.col("is_snapshot_row").cast(pl.Float64, strict=False).fill_null(0.0) < 0.5)
    assert seasons.height == 4 and seasons.filter(pl.col("season") == 2022)["h1_ppg"].item() == 9.0   # a to-date row is never a target


def test_the_merged_tail_keeps_the_base_row_and_takes_this_seasons_projections():
    """BACKLOG 51, the Tuesday 2026-10-13 refresh failed on this: the merged tail had lost every column but the
    projections (the preseason view needs position). Base columns stay; h2 on come from this season's row; a player
    missing from the base keeps his identity from the to-date row."""
    import polars as pl
    import unified
    base = pl.DataFrame({"player_id": ["a", "b"], "position": ["WR", "RB"], "ppg": [15.0, 9.0], "h1_ppg_hat": [14.0, 8.0],
                         "h2_ppg_hat": [13.0, 7.0], "h2_games_hat": [14.0, 12.0], "h3_ppg_hat": [12.0, 6.0], "h3_games_hat": [13.0, 11.0]})
    td = pl.DataFrame({"player_id": ["a", "c"], "position": ["WR", "TE"], "player_name": ["A", "C"], "exp_at_season": [3, 2],
                       "h1_ppg_hat": [16.0, 10.0], "h1_games_hat": [15.0, 12.0], "h2_ppg_hat": [15.5, 9.5], "h2_games_hat": [14.5, 11.5]})
    out = {r["player_id"]: r for r in unified.merge_todate_tail(base, td, [1, 2, 3]).to_dicts()}
    assert out["a"]["position"] == "WR" and out["a"]["ppg"] == 15.0 and out["a"]["h1_ppg_hat"] == 14.0      # the base row and its h1 stay
    assert out["a"]["h2_ppg_hat"] == 16.0 and out["a"]["h3_ppg_hat"] == 15.5 and out["a"]["h3_games_hat"] == 14.5   # h2 on from this season
    assert out["a"]["tail_origin"] == "this_season"
    assert out["b"]["h2_ppg_hat"] == 7.0 and out["b"]["tail_origin"] == "last_season"
    assert out["c"]["position"] == "TE" and out["c"]["h2_ppg_hat"] == 10.0 and out["c"]["h1_ppg_hat"] is None    # no base row: identity from the to-date row
