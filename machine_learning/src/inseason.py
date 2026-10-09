"""In-season update model: project from a mid-season snapshot.

A snapshot is (player, season T, through week W). Its features are what was known at that
moment: the player's prior complete seasons (the season-level feature set as of T-1) plus
this season to date (games, per-game rate, usage, recent form, missed weeks). Targets:

  * ``ros_ppg`` / ``ros_games``   -- the rest of season T (weeks > W); 0 games if he never played again
  * ``next_ppg`` / ``next_games`` -- season T+1, 0 games if he did not play (attrition learnt, as in
                                     the career model)

Trained on historical snapshots from every season and every week, so the model learns how far
a 3-week sample should move a projection versus a 10-week one -- which is the question the
market answers emotionally in September.

Leakage rules: a snapshot's to-date features use weeks <= W only; ROS targets exist only for
complete seasons; next-season targets only when T+1 is complete; ``fit(as_of_season)`` trains
only on outcomes known by the end of that season (for a backtest "at week W of T", train on
seasons < T).
"""
from __future__ import annotations

from typing import Iterable

import numpy as np
import polars as pl
from xgboost import XGBRegressor

import career

MAX_GAMES = 17
WEEKS = list(range(2, 17))

# season-level columns (as of T-1) carried as prior-history features
PREV_COLS = ["fpts", "ppg", "games", "lag1_fpts", "lag1_ppg", "lag1_games", "best_ppg",
             "career_seasons", "career_fpts", "career_games", "total_touches", "targets", "pass_yds"]
TD_COLS = ["week", "td_games", "td_fpts", "td_ppg", "td_targets_pg", "td_touches_pg", "td_pass_att_pg",
           "td_target_share", "td_wopr", "last3_ppg", "td_missed", "td_form"]
BIO_COLS = ["age_at_season", "exp_at_season", "draft_round", "draft_pick"]
FLAG_COLS = ["is_undrafted", "is_rookie"]
# depth-chart standing entering the snapshot week (fact_depth_chart_week): rank within position on his
# team (1 = starter), its change since three weeks earlier (+ = moved up), starter flag; null = not listed
DEPTH_COLS = ["td_depth_rank", "td_depth_change", "td_is_starter"]
POSITIONS = ["QB", "RB", "WR", "TE"]
FEATURES = TD_COLS + DEPTH_COLS + [f"prev_{c}" for c in PREV_COLS] + BIO_COLS + FLAG_COLS + [f"pos_{p}" for p in POSITIONS]

# optional in-season groups (BACKLOG 31), joined to a snapshot when the lake tables are given:
#   team     - the player's team to date at the snapshot week (fact_team_week_strength): the market's
#              rating so far (mean closing line), total, win probability, point differential, record,
#              offensive EPA per play and pass rate to date, this week's own line, last season's rating
#   contract - the contract in force that season (fact_player_contract_season): cap share, guarantees,
#              years left, contract year, rookie deal, age of the deal, rank among the position's deals
TEAM_WEEK_PATH = "silver/fantasy/fact_team_week_strength/data.parquet"
CONTRACT_PATH = "silver/fantasy/fact_player_contract_season/data.parquet"
TEAM_WEEK_SRC = ["mkt_margin_td", "mkt_total_td", "mkt_win_prob_td", "point_diff_td", "win_pct_td", "off_epa_td", "pass_rate_td",
                 "line_this_week", "total_this_week", "mkt_margin_prev", "point_diff_prev"]
TEAM_WEEK_COLS = [f"tm_{c}" for c in TEAM_WEEK_SRC]
CONTRACT_SRC = ["apy_cap_pct", "guaranteed_cap_pct", "years_left", "contract_year", "is_rookie_deal", "contract_age", "apy_cap_pct_pos_pctl"]
CONTRACT_SNAP_COLS = [f"ct_{c}" for c in CONTRACT_SRC] + ["ct_has_contract"]
# usage    - Next Gen Stats to date (receiving: separation, cushion, share of intended air yards, depth of
#            target, YAC over expected, catch rate; rushing: efficiency, yards over expected per attempt,
#            stacked-box rate, time to the line) and snap share to date + its three-week trend
# role     - recency windows beyond the three-game form (last game, last five, targets and touches over the
#            last three vs the season rate) and the status table (games since returning from a missed
#            week, misses in the last three, injured and dnp weeks to date)
# schedule - the remaining regular-season schedule: games left, the remaining opponents' point
#            differential to date (mean, and the next opponent's), a bye still ahead
NGS_REC_SRC = {"avg_separation": "sep", "avg_cushion": "cushion", "percent_share_of_intended_air_yards": "air_share",
               "avg_intended_air_yards": "adot", "avg_yac_above_expectation": "yac_oe", "catch_percentage": "catch_pct"}
NGS_RUSH_SRC = {"efficiency": "rush_eff", "rush_yards_over_expected_per_att": "ryoe_att",
                "percent_attempts_gte_eight_defenders": "box8", "avg_time_to_los": "time_los"}
USAGE_COLS = [f"ng_{v}" for v in NGS_REC_SRC.values()] + [f"ng_{v}" for v in NGS_RUSH_SRC.values()] + ["sn_pct_td", "sn_pct_last3", "sn_trend"]
ROLE_COLS = ["last1_fpts", "last5_ppg", "last3_targets_pg", "last3_touches_pg", "tgt_trend", "touch_trend",
             "games_since_return", "missed_last3", "td_injured_weeks", "td_dnp_weeks"]
SCHEDULE_COLS = ["sch_games_left", "sch_opp_pd", "sch_opp_pd_next", "sch_bye_ahead"]
# opportunity - nflverse's expected fantasy points (ff_opportunity, 2006 on): expected points per game to
#               date and by phase (pass / rush / receiving), the last three weeks' expected and its trend,
#               actual minus expected per game (points, touchdowns: the luck / regression signal), expected
#               yards, targets and air yards per game
OPP_COLS = ["op_xfp_pg", "op_pass_xfp_pg", "op_rush_xfp_pg", "op_rec_xfp_pg", "op_xfp_last3", "op_xfp_trend", "op_fp_oe_pg",
            "op_x_td_pg", "op_td_oe_pg", "op_x_yards_pg", "op_targets_pg", "op_air_yards_pg"]
EXTRA_GROUPS = {"team": TEAM_WEEK_COLS, "contract": CONTRACT_SNAP_COLS, "usage": USAGE_COLS, "role": ROLE_COLS, "schedule": SCHEDULE_COLS,
                "opportunity": OPP_COLS}
FFO_PATH = "bronze/nflverse/ff_opportunity"                  # season partitions (gcs_io.read_lake_prefix)
NGS_REC_PATH = "bronze/nflverse/nextgen_stats_receiving"      # season partitions (gcs_io.read_lake_prefix)
NGS_RUSH_PATH = "bronze/nflverse/nextgen_stats_rushing"
STATUS_PATH = "silver/fantasy/fact_player_week_status/data.parquet"
SCHEDULES_PATH = "bronze/nflverse/schedules"
MISSED_STATUSES = ["injured_out", "injured_reserve", "inactive", "dnp", "suspended"]
INJURED_STATUSES = ["injured_out", "injured_reserve"]
TEAM_CODE = {"OAK": "LV", "SD": "LAC", "STL": "LA", "JAC": "JAX", "LAR": "LA"}
CONTRACT_FIRST_SEASON = 1994

DEFAULT_PARAMS = {**career.DEFAULT_PARAMS}


def extra_columns(groups: Iterable[str]) -> list[str]:
    """The snapshot feature columns of the named in-season groups, in registry order."""
    names = [g.strip() for g in groups if g and g.strip()]
    unknown = [g for g in names if g not in EXTRA_GROUPS]
    if unknown:
        raise ValueError(f"unknown in-season group(s) {unknown}; known: {sorted(EXTRA_GROUPS)}")
    return [c for g in EXTRA_GROUPS if g in names for c in EXTRA_GROUPS[g]]


def _norm_team(col: str = "team") -> pl.Expr:
    return pl.col(col).cast(pl.Utf8).str.strip_chars().replace(TEAM_CODE)


def team_week_features(team_week: pl.DataFrame, week: int) -> pl.DataFrame:
    """(season, team) -> the team's to-date columns after ``week`` (current franchise codes)."""
    t = team_week.filter(pl.col("week") == week).with_columns(pl.col("season").cast(pl.Int64), _norm_team().alias("team"))
    return t.select(["season", "team"] + [pl.col(c).cast(pl.Float64, strict=False).alias(f"tm_{c}") for c in TEAM_WEEK_SRC if c in t.columns])


def contract_features(contracts: pl.DataFrame) -> pl.DataFrame:
    """(player_id, season) -> the contract in force that season, with a has-contract flag."""
    c = contracts.with_columns(pl.col("season").cast(pl.Int64), pl.col("gsis_id").cast(pl.Utf8).str.strip_chars().alias("player_id"))
    return (c.select(["player_id", "season"] + [pl.col(x).cast(pl.Float64, strict=False).alias(f"ct_{x}") for x in CONTRACT_SRC if x in c.columns]
                     + [pl.lit(1.0).alias("ct_has_contract")])
             .unique(["player_id", "season"], keep="first", maintain_order=True))


def _wmean(x: str, w: str) -> pl.Expr:
    """A mean of ``x`` weighted by ``w`` (null when nothing was weighted)."""
    return (pl.col(x) * pl.col(w)).sum() / pl.when(pl.col(w).sum() > 0).then(pl.col(w).sum()).otherwise(None)


def usage_features(ngs_rec: pl.DataFrame | None, ngs_rush: pl.DataFrame | None, status: pl.DataFrame | None, week: int) -> pl.DataFrame:
    """(player_id, season) -> Next Gen receiving / rushing to date (weeks 1..``week``, regular season,
    weighted by targets / attempts; week 0 is the season aggregate and is skipped) and the snap share to
    date with its three-week trend. Sources are optional; a missing one leaves its columns absent."""
    parts = []
    if ngs_rec is not None and ngs_rec.height:
        r = ngs_rec.filter((pl.col("week") >= 1) & (pl.col("week") <= week) & ((pl.col("season_type") == "REG") if "season_type" in ngs_rec.columns else True))
        r = r.with_columns(pl.col("player_gsis_id").cast(pl.Utf8).str.strip_chars().alias("player_id"), pl.col("season").cast(pl.Int64), pl.col("targets").cast(pl.Float64))
        parts.append(r.group_by("player_id", "season").agg(
            *[_wmean(src, "targets").alias(f"ng_{dst}") for src, dst in NGS_REC_SRC.items() if src in r.columns and dst != "air_share"],
            *([pl.col("percent_share_of_intended_air_yards").mean().alias("ng_air_share")] if "percent_share_of_intended_air_yards" in r.columns else [])))
    if ngs_rush is not None and ngs_rush.height:
        u = ngs_rush.filter((pl.col("week") >= 1) & (pl.col("week") <= week) & ((pl.col("season_type") == "REG") if "season_type" in ngs_rush.columns else True))
        u = u.with_columns(pl.col("player_gsis_id").cast(pl.Utf8).str.strip_chars().alias("player_id"), pl.col("season").cast(pl.Int64), pl.col("rush_attempts").cast(pl.Float64))
        parts.append(u.group_by("player_id", "season").agg(*[_wmean(src, "rush_attempts").alias(f"ng_{dst}") for src, dst in NGS_RUSH_SRC.items() if src in u.columns]))
    if status is not None and status.height and "offense_pct" in status.columns:
        s = (status.filter((pl.col("week") <= week) & pl.col("offense_pct").is_not_null())
                   .with_columns(pl.col("gsis_id").cast(pl.Utf8).str.strip_chars().alias("player_id"), pl.col("season").cast(pl.Int64), pl.col("offense_pct").cast(pl.Float64))
                   .sort("week"))
        parts.append(s.group_by("player_id", "season").agg(pl.col("offense_pct").mean().alias("sn_pct_td"), pl.col("offense_pct").tail(3).mean().alias("sn_pct_last3"))
                      .with_columns((pl.col("sn_pct_last3") - pl.col("sn_pct_td")).alias("sn_trend")))
    if not parts:
        return pl.DataFrame(schema={"player_id": pl.Utf8, "season": pl.Int64})
    out = parts[0]
    for q in parts[1:]:
        out = out.join(q, on=["player_id", "season"], how="full", coalesce=True)
    return out


def opportunity_features(ffo: pl.DataFrame, week: int) -> pl.DataFrame:
    """(player_id, season) -> expected fantasy points to date from the nflverse opportunity model
    (weeks 1..``week``): per game overall and by phase, the last three weeks' expected per game and its
    trend, actual minus expected per game for points and touchdowns, expected yards, targets and air
    yards per game. ``player_id`` is the gsis id."""
    f = ffo.filter((pl.col("week") >= 1) & (pl.col("week") <= week)).with_columns(
        pl.col("player_id").cast(pl.Utf8).str.strip_chars(), pl.col("season").cast(pl.Int64), pl.col("week").cast(pl.Int64)).sort("week")
    num = lambda c: pl.col(c).cast(pl.Float64, strict=False).fill_null(0.0)  # noqa: E731
    return f.group_by("player_id", "season").agg(
        pl.len().alias("_n"),
        num("total_fantasy_points_exp").sum().alias("_xfp"), num("pass_fantasy_points_exp").sum().alias("_pass"),
        num("rush_fantasy_points_exp").sum().alias("_rush"), num("rec_fantasy_points_exp").sum().alias("_rec"),
        num("total_fantasy_points_exp").tail(3).mean().alias("op_xfp_last3"),
        (num("total_fantasy_points") - num("total_fantasy_points_exp")).sum().alias("_oe"),
        num("total_touchdown_exp").sum().alias("_xtd"), (num("total_touchdown") - num("total_touchdown_exp")).sum().alias("_tdoe"),
        num("total_yards_gained_exp").sum().alias("_xyd"), num("rec_attempt").sum().alias("_tg"), num("rec_air_yards").sum().alias("_air"),
    ).with_columns(
        (pl.col("_xfp") / pl.col("_n")).alias("op_xfp_pg"), (pl.col("_pass") / pl.col("_n")).alias("op_pass_xfp_pg"),
        (pl.col("_rush") / pl.col("_n")).alias("op_rush_xfp_pg"), (pl.col("_rec") / pl.col("_n")).alias("op_rec_xfp_pg"),
        (pl.col("_oe") / pl.col("_n")).alias("op_fp_oe_pg"), (pl.col("_xtd") / pl.col("_n")).alias("op_x_td_pg"),
        (pl.col("_tdoe") / pl.col("_n")).alias("op_td_oe_pg"), (pl.col("_xyd") / pl.col("_n")).alias("op_x_yards_pg"),
        (pl.col("_tg") / pl.col("_n")).alias("op_targets_pg"), (pl.col("_air") / pl.col("_n")).alias("op_air_yards_pg"),
    ).with_columns((pl.col("op_xfp_last3") - pl.col("op_xfp_pg")).alias("op_xfp_trend")).drop("_n", "_xfp", "_pass", "_rush", "_rec", "_oe", "_xtd", "_tdoe", "_xyd", "_tg", "_air")


def role_features(wk: pl.DataFrame, status: pl.DataFrame | None, week: int) -> pl.DataFrame:
    """(player_id, season) -> recency windows from the played weeks <= ``week`` (last game, last five, the
    last three games' targets and touches per game against the season rate) and, from the status
    table, games since the player last missed a week (byes are neutral), misses in the last three
    weeks, injured and dnp weeks to date."""
    sub = wk.filter(pl.col("week") <= week).sort("week").with_columns((pl.col("rush_att") + pl.col("rec")).alias("_touches"))
    out = sub.group_by("player_id", "season").agg(
        pl.col("fpts").last().alias("last1_fpts"), pl.col("fpts").tail(5).mean().alias("last5_ppg"),
        pl.col("targets").tail(3).mean().alias("last3_targets_pg"), pl.col("_touches").tail(3).mean().alias("last3_touches_pg"),
        pl.col("targets").mean().alias("_tg"), pl.col("_touches").mean().alias("_to"),
    ).with_columns((pl.col("last3_targets_pg") - pl.col("_tg")).alias("tgt_trend"), (pl.col("last3_touches_pg") - pl.col("_to")).alias("touch_trend")).drop("_tg", "_to")
    if status is None or status.height == 0:
        return out
    s = (status.filter((pl.col("week") <= week) & (pl.col("status") != "bye"))
               .with_columns(pl.col("gsis_id").cast(pl.Utf8).str.strip_chars().alias("player_id"), pl.col("season").cast(pl.Int64),
                             pl.col("status").is_in(MISSED_STATUSES).alias("_missed"), pl.col("status").is_in(INJURED_STATUSES).alias("_inj"),
                             pl.col("status").is_in(["dnp", "inactive"]).alias("_dnp"), (pl.col("status") == "played").alias("_played"))
               .sort("week"))
    st = s.group_by("player_id", "season").agg(
        pl.col("_played").cast(pl.Int8).reverse().cum_prod().sum().alias("games_since_return"),   # the trailing run of played weeks
        pl.col("_missed").filter(pl.col("week") > week - 3).sum().alias("missed_last3"),
        pl.col("_inj").sum().alias("td_injured_weeks"), pl.col("_dnp").sum().alias("td_dnp_weeks"),
    )
    return out.join(st, on=["player_id", "season"], how="full", coalesce=True)


def schedule_features(schedules: pl.DataFrame, week: int, last_week: int = 18) -> pl.DataFrame:
    """(season, team) -> the remaining regular-season schedule after ``week``: games left, the remaining
    opponents' mean point differential per game to date (their games <= ``week``, so nothing from the
    future), the next opponent's, and whether a bye is still ahead. Team codes are the current ones."""
    g = schedules.filter((pl.col("game_type") == "REG") if "game_type" in schedules.columns else True).with_columns(pl.col("season").cast(pl.Int64), pl.col("week").cast(pl.Int64))
    long = pl.concat([
        g.select("season", "week", _norm_team("home_team").alias("team"), _norm_team("away_team").alias("opp"), (pl.col("home_score") - pl.col("away_score")).cast(pl.Float64).alias("pd")),
        g.select("season", "week", _norm_team("away_team").alias("team"), _norm_team("home_team").alias("opp"), (pl.col("away_score") - pl.col("home_score")).cast(pl.Float64).alias("pd")),
    ])
    strength = long.filter((pl.col("week") <= week) & pl.col("pd").is_not_null()).group_by("season", "team").agg(pl.col("pd").mean().alias("_opp_pd")).rename({"team": "opp"})
    ahead = long.filter(pl.col("week") > week).join(strength, on=["season", "opp"], how="left").sort("week")
    season_last = g.group_by("season").agg(pl.col("week").max().alias("_last"))
    return (ahead.group_by("season", "team").agg(pl.len().alias("sch_games_left"), pl.col("_opp_pd").mean().alias("sch_opp_pd"),
                                                 pl.col("_opp_pd").first().alias("sch_opp_pd_next"), pl.col("week").n_unique().alias("_weeks"))
                 .join(season_last, on="season", how="left")
                 .with_columns(((pl.col("_last").fill_null(last_week) - week) > pl.col("_weeks")).cast(pl.Int8).alias("sch_bye_ahead"))
                 .drop("_weeks", "_last"))


# ---------------------------------------------------------------------------- snapshots
def to_date_features(wk: pl.DataFrame, week: int) -> pl.DataFrame:
    """One row per (player, season) with aggregates of weeks <= ``week`` (players with >= 1 game)."""
    sub = wk.filter(pl.col("week") <= week)
    agg = sub.group_by("player_id", "season").agg(
        pl.col("player_name").first(), pl.col("position").first(), pl.col("team").sort_by("week").last(),
        pl.col("age_at_season").first(), pl.col("exp_at_season").first(),
        pl.col("draft_round").first(), pl.col("draft_pick").first(),
        pl.col("is_undrafted").first(), pl.col("is_rookie").first(),
        pl.len().alias("td_games"),
        pl.col("fpts").sum().alias("td_fpts"),
        pl.col("targets").sum().alias("_tg"), pl.col("rush_att").sum().alias("_ra"),
        pl.col("rec").sum().alias("_rc"), pl.col("pass_att").sum().alias("_pa"),
        pl.col("target_share").mean().alias("td_target_share"), pl.col("wopr").mean().alias("td_wopr"),
        pl.col("fpts").sort_by("week").tail(3).mean().alias("last3_ppg"),
        pl.col("game_date").max().alias("td_last_date"),
    )
    return agg.with_columns(
        pl.lit(week).alias("week"),
        (pl.col("td_fpts") / pl.col("td_games")).alias("td_ppg"),
        (pl.col("_tg") / pl.col("td_games")).alias("td_targets_pg"),
        ((pl.col("_ra") + pl.col("_rc")) / pl.col("td_games")).alias("td_touches_pg"),
        (pl.col("_pa") / pl.col("td_games")).alias("td_pass_att_pg"),
        (pl.lit(week) - pl.col("td_games")).alias("td_missed"),
    ).with_columns(
        (pl.col("last3_ppg") - pl.col("td_ppg")).alias("td_form"),      # recent form vs season rate
    ).drop("_tg", "_ra", "_rc", "_pa")


def depth_features(depth: pl.DataFrame, week: int) -> pl.DataFrame:
    """Per (player, season): the latest depth-chart listing at week <= ``week`` (regular season),
    the rank three weeks earlier for the change, and the starter flag. Players without a listing
    get no row (null features), which the trees read as 'not on a chart'."""
    d = depth.filter((pl.col("week") <= week) & ((pl.col("game_type") == "REG") if "game_type" in depth.columns else True))
    d = d.select(pl.col("gsis_id").alias("player_id"), "season", "week", "depth_rank", "is_starter").sort("week")
    now = d.group_by("player_id", "season").agg(pl.col("depth_rank").last().alias("td_depth_rank"),
                                                pl.col("is_starter").last().cast(pl.Int8).alias("td_is_starter"))
    prev = d.filter(pl.col("week") <= max(week - 3, 1)).group_by("player_id", "season").agg(pl.col("depth_rank").last().alias("_prev"))
    return (now.join(prev, on=["player_id", "season"], how="left")
            .with_columns((pl.col("_prev") - pl.col("td_depth_rank")).alias("td_depth_change")).drop("_prev"))


def prior_season_features(season_df: pl.DataFrame) -> pl.DataFrame:
    """Season-level rows re-keyed to the NEXT season so they join a snapshot as 'prior season'."""
    cols = [c for c in PREV_COLS if c in season_df.columns]
    return season_df.select(
        "player_id", (pl.col("season") + 1).alias("season"),
        *[pl.col(c).alias(f"prev_{c}") for c in cols],
    )


def build_snapshots(wk: pl.DataFrame, season_df: pl.DataFrame, weeks: Iterable[int] = WEEKS,
                    depth: pl.DataFrame | None = None, team_week: pl.DataFrame | None = None,
                    contracts: pl.DataFrame | None = None, ngs_receiving: pl.DataFrame | None = None,
                    ngs_rushing: pl.DataFrame | None = None, status: pl.DataFrame | None = None,
                    schedules: pl.DataFrame | None = None, role: bool = False, opportunity: pl.DataFrame | None = None) -> pl.DataFrame:
    """Stack a snapshot for every (player, season, week): to-date features + prior-season
    features (+ depth-chart standing when ``depth`` is given, + the team to date when
    ``team_week`` is given, + the contract in force when ``contracts`` is given) + ROS /
    next-season targets (null where not observable)."""
    prev = prior_season_features(season_df)
    ct = contract_features(contracts) if contracts is not None else None
    done = season_df.filter(pl.col("season_complete")) if "season_complete" in season_df.columns else season_df
    complete = set(done["season"].unique().to_list())
    last_complete = max(complete) if complete else int(season_df["season"].max())
    nxt = done.select("player_id", (pl.col("season") - 1).alias("season"),
                      pl.col("fpts").alias("next_fpts"), pl.col("games").alias("next_games"), pl.col("ppg").alias("next_ppg"))
    out = []
    for w in weeks:
        snap = to_date_features(wk, w).join(prev, on=["player_id", "season"], how="left")
        if depth is not None:
            snap = snap.join(depth_features(depth, w), on=["player_id", "season"], how="left")
        if team_week is not None:
            snap = (snap.with_columns(_norm_team().alias("_tm")).join(team_week_features(team_week, w).rename({"team": "_tm"}), on=["season", "_tm"], how="left")
                        .drop("_tm"))
        if ct is not None:
            snap = snap.join(ct, on=["player_id", "season"], how="left").with_columns(
                pl.when(pl.col("season") >= CONTRACT_FIRST_SEASON).then(pl.col("ct_has_contract").fill_null(0.0)).otherwise(None).alias("ct_has_contract"))
        if ngs_receiving is not None or ngs_rushing is not None or status is not None:
            usage = usage_features(ngs_receiving, ngs_rushing, status, w)
            if usage.width > 2:
                snap = snap.join(usage, on=["player_id", "season"], how="left")
        if role:
            snap = snap.join(role_features(wk, status, w), on=["player_id", "season"], how="left")
        if opportunity is not None:
            snap = snap.join(opportunity_features(opportunity, w), on=["player_id", "season"], how="left")
        if schedules is not None:
            snap = (snap.with_columns(_norm_team().alias("_tm")).join(schedule_features(schedules, w).rename({"team": "_tm"}), on=["season", "_tm"], how="left")
                        .drop("_tm"))
        ros = wk.filter(pl.col("week") > w).group_by("player_id", "season").agg(
            pl.len().alias("ros_games"), pl.col("fpts").sum().alias("ros_fpts"))
        snap = snap.join(ros, on=["player_id", "season"], how="left").join(nxt, on=["player_id", "season"], how="left")
        season_done = pl.col("season").is_in(sorted(complete))
        next_done = (pl.col("season") + 1) <= last_complete
        snap = snap.with_columns(
            season_done.alias("ros_observable"),
            pl.when(season_done).then(pl.col("ros_games").fill_null(0)).otherwise(None).cast(pl.Int64).alias("ros_games"),
            pl.when(season_done).then(pl.col("ros_fpts").fill_null(0.0)).otherwise(None).alias("ros_fpts"),
            next_done.alias("next_observable"),
            pl.when(next_done).then(pl.col("next_games").fill_null(0)).otherwise(None).cast(pl.Int64).alias("next_games"),
            pl.when(next_done).then(pl.col("next_fpts").fill_null(0.0)).otherwise(None).alias("next_fpts"),
            pl.when(next_done).then(pl.col("next_ppg")).otherwise(None).alias("next_ppg"),
        ).with_columns(
            pl.when(pl.col("ros_games") > 0).then(pl.col("ros_fpts") / pl.col("ros_games")).otherwise(None).alias("ros_ppg"),
        )
        out.append(snap)
    return pl.concat(out, how="diagonal_relaxed")


def feature_frame(df: pl.DataFrame, extra: Iterable[str] = ()) -> pl.DataFrame:
    cols = []
    for c in TD_COLS + DEPTH_COLS + [f"prev_{c}" for c in PREV_COLS] + BIO_COLS + list(extra):
        cols.append(pl.col(c).cast(pl.Float64, strict=False) if c in df.columns else pl.lit(None, pl.Float64).alias(c))
    for b in FLAG_COLS:
        cols.append((pl.col(b).cast(pl.Int8) if b in df.columns else pl.lit(0, pl.Int8)).alias(b))
    for p in POSITIONS:
        cols.append((pl.col("position") == p).cast(pl.Int8).alias(f"pos_{p}"))
    return df.select(cols)


# ------------------------------------------------------------------------------ models
def training_subset(rows: pl.DataFrame, train_weeks: Iterable[int] | None = None, max_rows: int | None = None, seed: int = 0) -> pl.DataFrame:
    """The in-context set for a size-limited estimator: optionally only the snapshots of some
    checkpoint weeks, then at most ``max_rows`` rows keeping the most recent seasons whole and a
    random share of the oldest season that fits (the trees take everything: both None)."""
    out = rows
    if train_weeks is not None:
        out = out.filter(pl.col("week").is_in([int(w) for w in train_weeks]))
    if max_rows is None or out.height <= max_rows:
        return out
    per = out.group_by("season").len().sort("season", descending=True)
    kept, budget = [], max_rows
    for s, n in zip(per["season"].to_list(), per["len"].to_list()):
        if n <= budget:
            kept.append(out.filter(pl.col("season") == s)); budget -= n
        else:
            if budget > 0:
                kept.append(out.filter(pl.col("season") == s).sample(n=budget, seed=seed))
            break
    return pl.concat(kept) if kept else out.head(0)


class InSeasonModels:
    """Four regressors: ros_ppg (on rows that played again), ros_games, next_ppg (on rows that
    played next year), next_games. ``backend`` "xgb" (the trees, every snapshot) or "tabpfn" (the
    foundation model: in-context regression over a training set capped by ``max_train_rows`` and
    optionally the checkpoint weeks ``train_weeks``, recent seasons first)."""

    def __init__(self, device: str = "cpu", seed: int = 0, extra_features: Iterable[str] = (), backend: str = "xgb",
                 tabpfn_params: dict | None = None, train_weeks: Iterable[int] | None = None, max_train_rows: int | None = None, **params):
        self.device, self.seed = device, seed
        self.extra_features = list(extra_features)       # in-season group columns (extra_columns) on top of FEATURES
        self.backend, self.tabpfn_params = backend, dict(tabpfn_params or {})
        self.train_weeks = [int(w) for w in train_weeks] if train_weeks is not None else None
        self.max_train_rows = max_train_rows
        self.params = {**DEFAULT_PARAMS, **params}
        self.models: dict = {}

    def _new(self):
        if self.backend == "tabpfn":
            return career._TabPFN(self.device, self.seed, self.tabpfn_params)
        if self.backend != "xgb":
            raise ValueError(f"unknown in-season backend {self.backend!r} (xgb | tabpfn)")
        return XGBRegressor(tree_method="hist", device=self.device, random_state=self.seed, n_jobs=-1, **self.params)

    def fit(self, snaps: pl.DataFrame, as_of_season: int | None = None) -> "InSeasonModels":
        ros = snaps.filter(pl.col("ros_observable"))
        nxt = snaps.filter(pl.col("next_observable"))
        if as_of_season is not None:
            ros = ros.filter(pl.col("season") < as_of_season)              # season T itself is still playing
            nxt = nxt.filter((pl.col("season") + 1) < as_of_season)        # T+1 outcome known only after it ends
        specs = {
            "ros_games": (ros, "ros_games"), "ros_ppg": (ros.filter(pl.col("ros_games") > 0), "ros_ppg"),
            "next_games": (nxt, "next_games"), "next_ppg": (nxt.filter(pl.col("next_games") > 0), "next_ppg"),
        }
        self.train_rows: dict[str, int] = {}
        for name, (rows, target) in specs.items():
            rows = training_subset(rows, self.train_weeks, self.max_train_rows, self.seed)
            if rows.height == 0:
                raise ValueError(f"{name}: no training outcomes (as_of={as_of_season})")
            self.train_rows[name] = rows.height
            self.models[name] = self._new().fit(feature_frame(rows, self.extra_features).to_numpy(), rows[target].to_numpy().astype(float))
        return self

    def predict(self, snaps: pl.DataFrame) -> pl.DataFrame:
        X = feature_frame(snaps, self.extra_features).to_numpy()
        ros_g = np.clip(self.models["ros_games"].predict(X), 0, MAX_GAMES)
        ros_p = np.clip(self.models["ros_ppg"].predict(X), 0, None)
        nx_g = np.clip(self.models["next_games"].predict(X), 0, MAX_GAMES)
        nx_p = np.clip(self.models["next_ppg"].predict(X), 0, None)
        return snaps.with_columns(
            pl.Series("ros_games_hat", ros_g), pl.Series("ros_ppg_hat", ros_p), pl.Series("ros_fpts_hat", ros_g * ros_p),
            pl.Series("next_games_hat", nx_g), pl.Series("next_ppg_hat", nx_p), pl.Series("next_fpts_hat", nx_g * nx_p),
        )


# ------------------------------------------------------------------- in-season value
AGE_BUCKETS = [(0, 23), (24, 25), (26, 27), (28, 29), (30, 99)]


def _age_bucket(age: pl.Expr) -> pl.Expr:
    expr = pl.lit(None, pl.Utf8)
    for lo, hi in reversed(AGE_BUCKETS):
        expr = pl.when(age.is_between(lo, hi)).then(pl.lit(f"{lo}-{hi}")).otherwise(expr)
    return expr


ROOKIE_TIERS = ("starter", "mid", "fringe", "regular", "__all__")   # regular = starter + mid pooled; __all__ = the position


def _tier_expr(ppg: pl.Expr, games: pl.Expr) -> pl.Expr:
    return (pl.when((ppg >= 12) & (games >= 10)).then(pl.lit("starter"))
              .when((ppg >= 8) & (games >= 8)).then(pl.lit("mid")).otherwise(pl.lit("fringe")))


def rookie_tail_table(season_df: pl.DataFrame, horizons: Iterable[int], through: int | None = None,
                      first_season: int = 2008, min_n: int = 10) -> pl.DataFrame:
    """Realized trajectories of past rookies, as ratios to their year +1: for in-season horizon k
    (season T-1+k off a rookie's season T, so h3 is year +2 after next season), the mean games in
    year +(k-1) over the mean games in year +1 (``_rg{k}``; absent seasons count 0) and the same for
    ppg among seasons played (``_rp{k}``), by position and rookie-year tier (starter / mid /
    fringe on the rookie season's ppg and games), plus a starter+mid pool (``regular``) and the
    position (``__all__``) as fallbacks. Only rookies whose year +(k-1) is complete by ``through``
    (default: every complete season) enter horizon k, so a backtest cannot see its own future.
    Groups with fewer than ``min_n`` rookies are dropped (the caller coalesces down the fallbacks).

    Why realized, not projected: the previous rule took the median ratio among players WITH a
    career tail in the same position and age bucket, and the young buckets are mostly fringe
    players whose projected tails collapse (0.27 of next-season games by year four for a
    first-round running back who, in the record, keeps 0.7-0.8); see BACKLOG 24."""
    ks = [k for k in horizons if k >= 3]
    if not ks or "is_rookie" not in season_df.columns:
        return pl.DataFrame({"position": [], "_tier": []})
    last = int(season_df.filter(pl.col("season_complete") if "season_complete" in season_df.columns else pl.lit(True))["season"].max()) if through is None else int(through)
    base = season_df.filter(pl.col("position").is_in(POSITIONS) & pl.col("is_rookie").fill_null(False) & (pl.col("season") >= first_season) & (pl.col("season") + 1 <= last))
    base = base.select("player_id", "position", "season", _tier_expr(pl.col("ppg").fill_null(0.0), pl.col("games").fill_null(0)).alias("_tier"))
    out = season_df.select("player_id", "season", pl.col("games").fill_null(0).cast(pl.Float64).alias("_g"), pl.col("ppg").cast(pl.Float64).alias("_p"))
    anchor = base.join(out.with_columns((pl.col("season") - 1).alias("season")).rename({"_g": "_g1", "_p": "_p1"}), on=["player_id", "season"], how="left").with_columns(pl.col("_g1").fill_null(0.0))
    frames = []
    for k in ks:
        j = k - 1                                          # years after the rookie season
        rows = anchor.filter(pl.col("season") + j <= last).join(
            out.with_columns((pl.col("season") - j).alias("season")).rename({"_g": "_gk", "_p": "_pk"}), on=["player_id", "season"], how="left").with_columns(pl.col("_gk").fill_null(0.0))
        rows = pl.concat([rows, rows.filter(pl.col("_tier").is_in(["starter", "mid"])).with_columns(pl.lit("regular").alias("_tier")), rows.with_columns(pl.lit("__all__").alias("_tier"))])
        agg = rows.group_by("position", "_tier").agg(
            pl.len().alias("_n"),
            pl.col("_pk").is_not_null().sum().alias("_np"),
            (pl.col("_gk").mean() / pl.col("_g1").mean()).alias(f"_rg{k}"),
            # ppg among the seasons played: only the survivors carry a rate, so it needs its own count and is
            # clipped (a handful of 10-year survivors at a thin position are not a rate to project from)
            (pl.col("_pk").mean() / pl.col("_p1").mean()).clip(0.6, 1.15).alias(f"_rp{k}"),
        ).filter((pl.col("_n") >= min_n) & pl.col(f"_rg{k}").is_finite()).with_columns(
            pl.when((pl.col("_np") >= min_n) & pl.col(f"_rp{k}").is_finite()).then(pl.col(f"_rp{k}")).otherwise(None).alias(f"_rp{k}")).drop("_n", "_np")
        frames.append(agg)
    table = frames[0]
    for fr in frames[1:]:
        table = table.join(fr, on=["position", "_tier"], how="full", coalesce=True)
    return table


def fill_missing_tail(df: pl.DataFrame, horizons: list[int], min_group: int = 8, rookie_table: pl.DataFrame | None = None) -> pl.DataFrame:
    """Give players with no career tail (rookies and anyone without a complete prior season) a tail
    extrapolated from their own next-season projection: season k = next-season ppg / games times a
    ratio. Rookies take the realized ratios of past rookies of their position and projected tier
    (``rookie_table`` from ``rookie_tail_table``; tier on the next-season projection; starter+mid
    pool, then the position, as fallbacks). Everyone else, and rookies without a table, take the
    median ratio h{k} / next among players of the same position and age bucket who do have a career
    tail (falls back to the position when a bucket is thin). Without any of this a rookie's value
    stopped after next season while a veteran's ran ten years, which made every rookie look rich.
    Adds ``tail_source`` ('career' | 'extrapolated' | 'none')."""
    ks = [k for k in horizons if k >= 3 and f"h{k}_ppg_hat" in df.columns]
    if not ks or "age_at_season" not in df.columns:
        return df.with_columns(pl.lit("career").alias("tail_source"))
    has = pl.all_horizontal([pl.col(f"h{k}_ppg_hat").is_not_null() for k in ks])
    d = df.with_columns(has.alias("_has_tail"), _age_bucket(pl.col("age_at_season")).alias("_ab"))
    rk_cols: dict[int, tuple[pl.Expr, pl.Expr]] = {}
    if rookie_table is not None and rookie_table.height and "is_rookie" in d.columns:
        d = d.with_columns(_tier_expr(pl.col("next_ppg_hat").fill_null(0.0), pl.col("next_games_hat").fill_null(0.0)).alias("_rt"))
        d = d.with_columns(pl.when(pl.col("_rt").is_in(["starter", "mid"])).then(pl.lit("regular")).otherwise(pl.lit("fringe")).alias("_rt2"))
        for lvl, key in (("_rt", "_t1"), ("_rt2", "_t2")):
            t = rookie_table.rename({c: f"{c}{key}" for c in rookie_table.columns if c.startswith("_r")}).rename({"_tier": lvl})
            d = d.join(t, on=["position", lvl], how="left")
        t = rookie_table.filter(pl.col("_tier") == "__all__").drop("_tier").rename({c: f"{c}_t3" for c in rookie_table.columns if c.startswith("_r")})
        d = d.join(t, on="position", how="left")
        for k in ks:
            if f"_rg{k}_t1" in d.columns:
                rk_cols[k] = (pl.coalesce([pl.col(f"_rp{k}_t1"), pl.col(f"_rp{k}_t2"), pl.col(f"_rp{k}_t3")]),
                              pl.coalesce([pl.col(f"_rg{k}_t1"), pl.col(f"_rg{k}_t2"), pl.col(f"_rg{k}_t3")]))
    base = d.filter(pl.col("_has_tail") & (pl.col("next_ppg_hat") > 0) & (pl.col("next_games_hat") > 0))
    ratio_exprs = []
    for k in ks:
        ratio_exprs += [(pl.col(f"h{k}_ppg_hat") / pl.col("next_ppg_hat")).median().alias(f"_rp{k}"),
                        (pl.col(f"h{k}_games_hat") / pl.col("next_games_hat")).median().alias(f"_rg{k}")]
    by_bucket = base.group_by("position", "_ab").agg([pl.len().alias("_n")] + ratio_exprs).filter(pl.col("_n") >= min_group).drop("_n")
    by_pos = base.group_by("position").agg(ratio_exprs)
    d = d.join(by_bucket, on=["position", "_ab"], how="left")
    d = d.join(by_pos, on="position", how="left", suffix="_pos")
    fills = []
    is_rookie = pl.col("is_rookie").fill_null(False) if "is_rookie" in d.columns else pl.lit(False)
    for k in ks:
        rp = pl.coalesce([pl.col(f"_rp{k}"), pl.col(f"_rp{k}_pos")])
        rg = pl.coalesce([pl.col(f"_rg{k}"), pl.col(f"_rg{k}_pos")])
        if k in rk_cols:                                  # a rookie takes the realized rookie ratios where the table has them
            rp = pl.when(is_rookie).then(pl.coalesce([rk_cols[k][0], rp])).otherwise(rp)
            rg = pl.when(is_rookie).then(pl.coalesce([rk_cols[k][1], rg])).otherwise(rg)
        fills += [pl.when(pl.col("_has_tail")).then(pl.col(f"h{k}_ppg_hat")).otherwise(pl.col("next_ppg_hat") * rp).alias(f"h{k}_ppg_hat"),
                  pl.when(pl.col("_has_tail")).then(pl.col(f"h{k}_games_hat")).otherwise(pl.col("next_games_hat") * rg).alias(f"h{k}_games_hat")]
    d = d.with_columns(fills).with_columns(
        pl.when(pl.col("_has_tail")).then(pl.lit("career"))
          .when(pl.col(f"h{ks[0]}_ppg_hat").is_not_null() & is_rookie & pl.lit(bool(rk_cols))).then(pl.lit("rookie_table"))
          .when(pl.col(f"h{ks[0]}_ppg_hat").is_not_null()).then(pl.lit("extrapolated")).otherwise(pl.lit("none")).alias("tail_source"))
    return d.drop([c for c in d.columns if c.startswith("_rp") or c.startswith("_rg") or c in ("_has_tail", "_ab", "_rt", "_rt2")])


Z_20_80 = 0.8416     # a 20-80 band is +- 0.84 standard deviations of a normal spread


def fill_missing_band(df: pl.DataFrame, horizons: Iterable[int], sigma: dict[int, dict[str, float]] | None) -> pl.DataFrame:
    """Where the run carries a projected band (``h{k}_ppg_q20`` / ``_q50`` / ``_q80`` from TabPFN) but a
    player has none (a rookie's or returning veteran's extrapolated tail), give him one from the
    position's out-of-sample spread: point +- 0.84 sigma, floored at 0; games band = the point. A run
    without a band is left alone."""
    if not sigma:
        return df
    cols = []
    for k in horizons:
        lo, mid, hi = f"h{k}_ppg_q20", f"h{k}_ppg_q50", f"h{k}_ppg_q80"
        if lo not in df.columns or hi not in df.columns:
            continue
        s = df["position"].replace_strict({p: v for p, v in sigma.get(k, {}).items() if p != "__all__"},
                                          default=sigma.get(k, {}).get("__all__", 3.0), return_dtype=pl.Float64)
        point = pl.col(f"h{k}_ppg_hat")
        cols += [pl.coalesce([pl.col(lo), (point - Z_20_80 * s).clip(0.0, None)]).alias(lo),
                 pl.coalesce([pl.col(hi), point + Z_20_80 * s]).alias(hi)]
        if mid in df.columns:
            cols.append(pl.coalesce([pl.col(mid), point]).alias(mid))
        for g in (f"h{k}_games_q20", f"h{k}_games_q50", f"h{k}_games_q80"):
            if g in df.columns:
                cols.append(pl.coalesce([pl.col(g), pl.col(f"h{k}_games_hat")]).alias(g))
    return df.with_columns(cols) if cols else df


def inseason_value(
    snaps: pl.DataFrame, career_pred: pl.DataFrame, rep: dict[str, float],
    sigma: dict[int, dict[str, float]] | None, horizons: Iterable[int], discount_rate: float = 0.2,
    rookie_table: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Intrinsic value updated mid-season: the rest of THIS season (full weight) + next season
    from the in-season model at (1 - rate), then seasons T+2.. from the career model's projection
    off the player's latest complete season T-1 (``career_pred`` carries ``h{k}_ppg_hat`` /
    ``h{k}_games_hat`` keyed by player_id). Off a T-1 row, h1 is THIS season and h2 is next
    season -- both already covered by the in-season model -- so only k >= 3 form the tail, season
    T-1+k weighted (1 - rate)^(k-1) exactly as in ``value.intrinsic_value``.

    This is what the dynasty market should be compared with in-season: next-season points alone
    make every productive 38-year-old look cheap against a price that already discounts his
    remaining career.
    """
    import value as _value

    horizons = [k for k in horizons if k >= 3]
    band_cols = [c for c in career_pred.columns if any(c.startswith(f"h{k}_ppg_q") or c.startswith(f"h{k}_games_q") for k in horizons)]
    tail = career_pred.select(["player_id"] + [c for k in horizons for c in (f"h{k}_ppg_hat", f"h{k}_games_hat")] + band_cols)
    df = fill_missing_tail(snaps.join(tail, on="player_id", how="left"), horizons, rookie_table=rookie_table)
    df = fill_missing_band(df, horizons, sigma)
    rep_arr = df["position"].replace_strict(rep, default=0.0, return_dtype=pl.Float64).to_numpy()

    def excess(mu_col: str, k: int) -> np.ndarray:
        mu = df[mu_col].fill_null(0.0).to_numpy().astype(float)
        if sigma and k in sigma:
            s = df["position"].replace_strict({p: v for p, v in sigma[k].items() if p != "__all__"},
                                              default=sigma[k]["__all__"], return_dtype=pl.Float64).to_numpy()
            return _value.expected_excess(mu, s, rep_arr)
        return np.maximum(mu - rep_arr, 0.0)

    ros = excess("ros_ppg_hat", 1) * df["ros_games_hat"].to_numpy()
    nxt = excess("next_ppg_hat", 1) * df["next_games_hat"].to_numpy()
    cols = [pl.Series("vorp_ros", ros), pl.Series("vorp_next", nxt)]
    total = ros + _value.discount_weight(2, discount_rate) * nxt
    for k in horizons:
        v = excess(f"h{k}_ppg_hat", k) * df[f"h{k}_games_hat"].fill_null(0.0).to_numpy()
        cols.append(pl.Series(f"h{k}_vorp_hat", v))
        total = total + _value.discount_weight(k, discount_rate) * v
    cols.append(pl.Series("iv_inseason", total))
    return df.with_columns(cols)


# ---------------------------------------------------------------------------- baselines
def baselines(snaps: pl.DataFrame) -> pl.DataFrame:
    """What a reactive manager and a stubborn one would do: this season to date, last season
    only, and a 50/50 of the two (per game)."""
    return snaps.with_columns(
        pl.col("td_ppg").alias("bl_todate_ppg"),
        pl.col("prev_ppg").fill_null(0.0).alias("bl_prior_ppg"),
        ((pl.col("td_ppg") + pl.col("prev_ppg").fill_null(pl.col("td_ppg"))) / 2).alias("bl_blend_ppg"),
    )
