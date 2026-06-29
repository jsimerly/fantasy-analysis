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
