"""One model for the in-season and the career horizons (BACKLOG 32), step one: mid-season snapshots
as rows of the career matrix.

A career row is a player's state at the end of season T with the seasons T+1, T+2.. as targets. A
snapshot row is the same player's state after week W of season T, with the same targets: the
to-date rate, games and usage in the season's columns, last season in the lag columns, the career
to date including the partial season, and ``row_week`` = W (a complete season reads 18, a draft row
0) so the model learns how far a partial season should move the projection. ``is_snapshot_row``
marks them. Snapshot rows never serve as another row's future season, never form a test cohort,
and never feed replacement levels or survival (``draft_rows.drop_draft_rows`` drops them too).

Step two (the in-season use: next season from a snapshot through this model instead of the
in-season model) builds on the same rows.
"""
from __future__ import annotations

from typing import Iterable

import polars as pl

import inseason

SEASON_ROW_WEEK = 18          # row_week of a complete season
DEFAULT_SNAPSHOT_WEEKS = (9,)
DEFAULT_SNAPSHOT_FROM = 2010  # the in-context cap (50k rows on TabPFN 3.5) decides how many snapshots fit


def snapshot_rows(wk: pl.DataFrame, season_df: pl.DataFrame, weeks: Iterable[int] = DEFAULT_SNAPSHOT_WEEKS,
                  from_season: int = DEFAULT_SNAPSHOT_FROM) -> pl.DataFrame:
    """Career-schema rows from ``inseason.build_snapshots`` at the given checkpoint weeks (players
    with at least one game by then), seasons ``from_season`` on. ``season_df`` is the played-season
    frame (lags and career columns on it are used as last season's values)."""
    weeks = [int(w) for w in weeks]
    snaps = inseason.build_snapshots(wk.filter(pl.col("season") >= from_season), season_df, weeks=weeks)
    done = set(season_df.filter(pl.col("season_complete"))["season"].unique().to_list()) if "season_complete" in season_df.columns else None

    def col(c: str) -> pl.Expr:
        return pl.col(c).cast(pl.Float64, strict=False) if c in snaps.columns else pl.lit(None, pl.Float64)

    out = snaps.select(
        pl.col("player_id"), pl.col("season").cast(pl.Int64), pl.col("player_name"), pl.col("position"), pl.col("team"),
        col("age_at_season").alias("age_at_season"), col("exp_at_season").alias("exp_at_season"),
        col("draft_round").alias("draft_round"), col("draft_pick").alias("draft_pick"),
        pl.col("is_undrafted"), pl.col("is_rookie"),
        pl.col("week").cast(pl.Int64).alias("row_week"), pl.lit(True).alias("is_snapshot_row"),
        # this season to date, in the season's columns
        col("td_fpts").alias("fpts"), col("td_ppg").alias("ppg"), col("td_games").alias("games"),
        (col("td_targets_pg") * col("td_games")).alias("targets"), (col("td_touches_pg") * col("td_games")).alias("total_touches"),
        col("td_target_share").alias("target_share_avg"), col("td_wopr").alias("wopr_avg"),
        # last season and the one before, in the lag columns
        col("prev_fpts").alias("lag1_fpts"), col("prev_ppg").alias("lag1_ppg"), col("prev_games").alias("lag1_games"),
        col("prev_targets").alias("lag1_targets"), col("prev_total_touches").alias("lag1_total_touches"), col("prev_pass_yds").alias("lag1_pass_yds"),
        col("prev_lag1_fpts").alias("lag2_fpts"), col("prev_lag1_ppg").alias("lag2_ppg"), col("prev_lag1_games").alias("lag2_games"),
        # the career to date, the partial season included
        (col("prev_career_seasons").fill_null(0.0) + 1.0).alias("career_seasons"),
        (col("prev_career_fpts").fill_null(0.0) + col("td_fpts").fill_null(0.0)).alias("career_fpts"),
        (col("prev_career_games").fill_null(0.0) + col("td_games").fill_null(0.0)).alias("career_games"),
        pl.max_horizontal(col("prev_best_ppg"), col("td_ppg")).alias("best_ppg"),
    )
    if done is not None:
        out = out.with_columns(pl.col("season").is_in(sorted(done)).alias("season_complete"))
    else:
        out = out.with_columns(pl.lit(True).alias("season_complete"))
    return out.sort(["season", "player_id", "row_week"])


def mask_group_columns(df: pl.DataFrame, cols: Iterable[str]) -> pl.DataFrame:
    """Null the season-level feature-group columns on snapshot rows: those groups (injury weeks, the
    second-half trend, the team move) describe the whole season, which a mid-season row has not seen."""
    cols = [c for c in cols if c in df.columns]
    if not cols or "is_snapshot_row" not in df.columns:
        return df
    snap = pl.col("is_snapshot_row").cast(pl.Float64, strict=False).fill_null(0.0) > 0.5
    return df.with_columns([pl.when(snap).then(None).otherwise(pl.col(c)).alias(c) for c in cols])


def with_snapshot_rows(matrix: pl.DataFrame, rows: pl.DataFrame) -> pl.DataFrame:
    """Append snapshot rows to a career matrix before the horizon targets are attached; played-season
    rows get ``row_week`` = 18 and ``is_snapshot_row`` = False."""
    m = matrix
    if "row_week" not in m.columns:
        m = m.with_columns(pl.lit(SEASON_ROW_WEEK, pl.Int64).alias("row_week"))
    else:
        m = m.with_columns(pl.col("row_week").fill_null(SEASON_ROW_WEEK).cast(pl.Int64))
    if "is_snapshot_row" not in m.columns:
        m = m.with_columns(pl.lit(False).alias("is_snapshot_row"))
    r = rows.with_columns(pl.col("season").cast(m.schema["season"]))
    return pl.concat([m, r], how="diagonal_relaxed")


# --------------------------------------------------------------- season-to-date rows (BACKLOG 51)
ROLLUP_SUMS = ["fpts", "pass_att", "pass_cmp", "pass_yds", "pass_tds", "pass_int", "rush_att", "rush_yds", "rush_tds",
               "targets", "rec", "rec_yds", "rec_tds"]
ROLLUP_BIO = ["player_name", "position", "birth_date", "draft_round", "draft_pick", "rookie_season", "age_at_season",
              "exp_at_season", "is_rookie", "is_undrafted"]


def rollup_to_date(wk: pl.DataFrame) -> pl.DataFrame:
    """fact_player_season's rollup (data_engineering silver_fantasy.fact_player_season.rollup_to_season) on whatever weeks
    ``wk`` holds: one row per (player_id, season) in the season fact's own columns, so a season to date reads exactly like
    a season."""
    sums = [pl.col(c).sum().alias(c) for c in ROLLUP_SUMS if c in wk.columns]
    bio = [pl.col(c).first().alias(c) for c in ROLLUP_BIO if c in wk.columns]
    means = [pl.col(src).mean().alias(dst) for src, dst in (("target_share", "target_share_avg"), ("wopr", "wopr_avg")) if src in wk.columns]
    out = wk.group_by(["player_id", "season"]).agg(
        *bio, pl.col("team").sort_by("week").last().alias("team"), pl.len().alias("games"), *sums, *means)
    out = out.with_columns(
        (pl.col("fpts") / pl.col("games")).alias("ppg"),
        (pl.col("rush_att").fill_null(0) + pl.col("rec").fill_null(0)).alias("total_touches"),
        (pl.col("rush_yds").fill_null(0) + pl.col("rec_yds").fill_null(0)).alias("scrim_yds"),
        (pl.col("pass_tds").fill_null(0) + pl.col("rush_tds").fill_null(0) + pl.col("rec_tds").fill_null(0)).alias("total_tds"))
    return out.with_columns(pl.when(pl.col("total_touches") > 0).then(pl.col("scrim_yds") / pl.col("total_touches")).otherwise(None).alias("yds_per_touch"))


def season_to_date_rows(wk: pl.DataFrame, season_full: pl.DataFrame, weeks: Iterable[int], from_season: int = DEFAULT_SNAPSHOT_FROM) -> pl.DataFrame:
    """Career-matrix rows for a season seen through week W (BACKLOG 51: the career model must see the current season).
    For each W: the season fact's rollup of weeks 1..W, last season and the one before as the lags (complete seasons from
    ``season_full``), the career to date through the partial season, ``row_week`` = W, ``is_snapshot_row``. A season
    whose latest played week is before W gets no row at W (the current season only up to its latest week).
    ``season_full`` is the season fact (complete and in-progress seasons; only complete ones feed lags and careers)."""
    import features
    full = season_full.filter(pl.col("season_complete")) if "season_complete" in season_full.columns else season_full
    w = wk.filter(pl.col("season") >= from_season)
    last_week = w.group_by("season").agg(pl.col("week").max().alias("_last"))
    parts = []
    for W in sorted({int(x) for x in weeks}):
        have = last_week.filter(pl.col("_last") >= W)["season"].to_list()
        td = rollup_to_date(w.filter((pl.col("week") <= W) & pl.col("season").is_in(have)))
        parts.append(td.with_columns(pl.lit(W, pl.Int64).alias("row_week")))
    if not parts:
        return pl.DataFrame()
    td = pl.concat(parts, how="diagonal_relaxed").with_columns(pl.col("season").cast(pl.Int64))
    full = full.with_columns(pl.col("season").cast(pl.Int64))
    lag1 = features._shift(full, 1, features.LAG1_COLS, "lag1_")
    lag2 = features._shift(full, 2, features.LAG2_COLS, "lag2_")
    car = (full.sort(["player_id", "season"]).with_columns(
               pl.col("season").cum_count().over("player_id").alias("_cs"), pl.col("fpts").cum_sum().over("player_id").alias("_cf"),
               pl.col("games").cum_sum().over("player_id").alias("_cg"), pl.col("ppg").cum_max().over("player_id").alias("_cb"))
           .select("player_id", "season", "_cs", "_cf", "_cg", "_cb"))
    # the career through the last complete season before s: the latest complete season < s (an absent year carries over)
    td = td.sort("season").join_asof(car.sort("season").with_columns((pl.col("season") + 1).alias("_s1")).drop("season"),
                                     left_on="season", right_on="_s1", by="player_id", strategy="backward")
    td = (td.join(lag1, on=["player_id", "season"], how="left").join(lag2, on=["player_id", "season"], how="left")
            .with_columns((pl.col("fpts") - pl.col("lag1_fpts")).alias("d_fpts_1"), (pl.col("ppg") - pl.col("lag1_ppg")).alias("d_ppg_1"),
                          (pl.col("_cs").fill_null(0) + 1).cast(pl.Int64).alias("career_seasons"),
                          (pl.col("_cf").fill_null(0.0) + pl.col("fpts")).alias("career_fpts"),
                          (pl.col("_cg").fill_null(0) + pl.col("games")).cast(pl.Int64).alias("career_games"),
                          pl.max_horizontal(pl.col("_cb"), pl.col("ppg")).alias("best_ppg"),
                          pl.lit(True).alias("is_snapshot_row"), pl.lit(False).alias("season_complete"))
            .drop(["_cs", "_cf", "_cg", "_cb", "_s1"]))
    return td.sort(["season", "player_id", "row_week"])


def with_tier_games(df: pl.DataFrame, full_season: int = 17) -> pl.DataFrame:
    """``tier_games``: the games a row's season is on pace for, for the survival cap's tier (a starter three weeks in has
    3 games; read as a season he would be a fringe player and his later seasons capped as one). Season rows keep
    their games; a to-date row at week W reads games x 17 / W, at most 17."""
    snap = pl.col("is_snapshot_row").cast(pl.Float64, strict=False).fill_null(0.0) > 0.5 if "is_snapshot_row" in df.columns else pl.lit(False)
    wk = pl.col("row_week").cast(pl.Float64, strict=False) if "row_week" in df.columns else pl.lit(18.0)
    pace = (pl.col("games").cast(pl.Float64) * full_season / pl.max_horizontal(wk, pl.lit(1.0))).clip(upper_bound=float(full_season))
    return df.with_columns(pl.when(snap & (wk < 18)).then(pace).otherwise(pl.col("games").cast(pl.Float64)).alias("tier_games"))


# groups that describe one player (not one season): valid on a to-date row as they are on a season row
STATIC_GROUPS = {"rookie", "college", "college2", "college_qb", "draft_age", "adp_this",
                 "is_usage", "is_role", "is_opportunity", "is_consensus", "is_preseason", "is_injury_now"}   # the last five are computed to each row's week


def mask_season_groups(df: pl.DataFrame, groups) -> pl.DataFrame:
    """Season-level group columns (injury weeks, the second-half trend, team changes, next season's ADP) describe a whole
    season: null on the to-date rows, which have not seen it. One-player groups (``STATIC_GROUPS``) stay."""
    import career
    cols = [c for g in groups if g.name not in STATIC_GROUPS for c in g.columns if c not in career.FEATURES and c in df.columns]
    if not cols or "is_snapshot_row" not in df.columns:
        return df
    snap = pl.col("is_snapshot_row").cast(pl.Float64, strict=False).fill_null(0.0) > 0.5
    return df.with_columns([pl.when(snap).then(None).otherwise(pl.col(c)).alias(c) for c in cols])


def one_week_per_season(rows: pl.DataFrame, k: int = 1, seed: int = 7) -> pl.DataFrame:
    """K random to-date weeks per player-season (seeded): every week of the season in the training table at about the
    season table's size (BACKLOG 51: one random week did as well as eight fixed ones on the trees)."""
    key = pl.struct("player_id", "season", "row_week").hash(seed)
    return rows.with_columns(key.alias("_rk")).filter(pl.col("_rk").rank("ordinal").over(["player_id", "season"]) <= k).drop("_rk")


def todate_training_table(season_matrix: pl.DataFrame, todate: pl.DataFrame, horizons, as_of: int, k: int = 1, seed: int = 7) -> pl.DataFrame:
    """The to-date model's table: every season row plus K random to-date weeks per complete player-season through
    ``as_of``, the horizon targets attached afresh (a to-date row's targets are the complete seasons after it)."""
    import career
    hcols = [c for c in season_matrix.columns if c[:1] == "h" and c.split("_", 1)[0][1:].isdigit()]
    train_td = one_week_per_season(todate.filter(pl.col("season") <= as_of), k, seed)
    both = pl.concat([season_matrix.drop(hcols), train_td], how="diagonal_relaxed")
    return career.attach_horizon_targets(both, list(horizons))


def shift_to_tail(pred: pl.DataFrame, horizons) -> pl.DataFrame:
    """A to-date row of season T projects h1 = T+1, h2 = T+2 ..; the in-season tail is keyed off the last complete season
    (h3 = T+2 ..). Rename h{j} -> h{j+1} for the projection, band and grid columns, keep player_id."""
    keep = ["player_id"]
    ren = {}
    for c in pred.columns:
        head, _, rest = c.partition("_")
        if head[:1] == "h" and head[1:].isdigit() and (rest.endswith("_hat") or "_q" in rest or rest.endswith("_sigma")):
            j = int(head[1:])
            if j + 1 in set(horizons) or j + 1 <= max(horizons):
                ren[c] = f"h{j + 1}_{rest}"
    return pred.select(keep + list(ren)).rename(ren)
