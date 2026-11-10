"""Unit tests for lag/target assembly (pure, no network)."""
import polars as pl

import features


def _toy() -> pl.DataFrame:
    # Player A: 2020,2021,2022 ; Player B: 2021,2022
    cols = {c: [0, 0, 0, 0, 0] for c in features.LAG1_COLS}
    cols.update({
        "player_id": ["A", "A", "A", "B", "B"],
        "season": [2020, 2021, 2022, 2021, 2022],
        "fpts": [100, 150, 200, 50, 80],
        "ppg": [10.0, 15.0, 20.0, 5.0, 8.0],
        "games": [10, 10, 10, 10, 10],
    })
    return pl.DataFrame(cols)


def test_one_row_per_player_season():
    out = features.attach_lags_and_target(_toy())
    # rows with a T+1 target: A2020, A2021, B2021  (A2022 + B2022 dropped)
    assert out.height == 3
    assert out.select(["player_id", "season"]).n_unique() == 3


def test_target_is_next_season_not_current():
    out = features.attach_lags_and_target(_toy())
    a21 = out.filter((pl.col("player_id") == "A") & (pl.col("season") == 2021))
    assert a21["fpts"][0] == 150            # current-year feature
    assert a21["target_fpts_next"][0] == 200  # T+1 target (A 2022)
    assert a21["lag1_fpts"][0] == 100         # T-1 lag (A 2020)


def test_rows_without_target_are_dropped():
    out = features.attach_lags_and_target(_toy())
    assert out.filter((pl.col("player_id") == "A") & (pl.col("season") == 2022)).height == 0
    assert out.filter((pl.col("player_id") == "B") & (pl.col("season") == 2022)).height == 0


def test_missing_lags_are_null_not_dropped():
    out = features.attach_lags_and_target(_toy())
    b21 = out.filter((pl.col("player_id") == "B") & (pl.col("season") == 2021))
    assert b21.height == 1                    # kept even though B has no 2020 season
    assert b21["lag1_fpts"][0] is None


def test_keep_all_when_drop_disabled():
    out = features.attach_lags_and_target(_toy(), drop_no_target=False)
    assert out.height == 5


def test_incomplete_next_season_is_never_a_target():
    # 2022 is in progress (season_complete=False): rows that would target it get no target and
    # are dropped; A2020 (targets the complete 2021) survives.
    df = _toy().with_columns(pl.Series("season_complete", [True, True, False, True, False]))
    out = features.attach_lags_and_target(df)
    assert out.select("player_id", "season").rows() == [("A", 2020)]


def test_frames_without_the_flag_still_work():
    assert features.attach_lags_and_target(_toy()).height == 3


class TestRookieSeasonRepair:
    """2026-11-10: nflverse moved the 2012 rookie class to rookie_season 2013; silver's exp_at_season went to -1 on
    their 2012 rows and the refresh died on pick / (1 + exp). The model repairs what it reads."""

    def _frame(self):
        import polars as pl
        return pl.DataFrame({"player_id": ["rw", "rw", "ok", "ok", "nul"], "season": [2012, 2013, 2012, 2013, 2012],
                             "rookie_season": [2013, 2013, 2011, 2011, None], "exp_at_season": [-1, 0, 1, 2, None],
                             "is_rookie": [False, True, False, False, None]},
                            schema_overrides={"rookie_season": pl.Int64, "exp_at_season": pl.Int64})

    def test_a_rookie_season_after_the_first_season_played_is_pulled_back(self):
        import lake_repair
        out = lake_repair.repair(self._frame())
        assert out["rookie_season"].to_list() == [2012, 2012, 2011, 2011, None]
        assert out["exp_at_season"].to_list() == [0, 1, 1, 2, None]
        assert out["is_rookie"].to_list() == [True, False, False, False, None]
        assert out.schema == self._frame().schema

    def test_a_frame_without_the_columns_is_untouched(self):
        import polars as pl
        import lake_repair
        df = pl.DataFrame({"player_id": ["a"], "season": [2020]})
        assert lake_repair.repair(df).equals(df)
