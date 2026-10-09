"""data_quality/suite.py: the catalogue on a seeded, healthy lake passes; each bug we have hit, replayed
on the seed, is caught by the check that guards it. The Context is seeded with frames and partition
listings, so nothing reaches GCS."""
from datetime import date, datetime, timedelta

import polars as pl
import pytest

from data_quality.core import Context, previous_from, run_checks, summarize
from data_quality.suite import SUITE, suite

TODAY = date(2026, 10, 9)
LEAGUES = [("1342619554194423808", "730630605066371072", 10), ("1314347683258843136", "1131624152349323264", 12)]


def _dates(start: date, end: date):
    return [start + timedelta(days=k) for k in range((end - start).days + 1)]


def healthy() -> tuple[dict, dict]:
    """A small lake that satisfies every check."""
    frames, parts = {}, {}
    days = _dates(date(2026, 9, 1), TODAY)
    # calendar spine
    spine = _dates(date(2025, 1, 1), date(2028, 1, 1))
    frames["dim_dates"] = pl.DataFrame({"date": spine, "nfl_season": [2026 if d >= date(2026, 3, 1) else 2025 for d in spine],
                                        "nfl_phase": ["regular" if 9 <= d.month <= 12 else "offseason" for d in spine]})
    # leagues: two lineages, 2025 complete + 2026 in season
    lm = []
    for lg, lin, n in LEAGUES:
        lm.append({"league_id": lin, "season": "2025", "status": "complete", "total_rosters": n, "league_lineage_id": lin, "source_system": "sleeper"})
        lm.append({"league_id": lg, "season": "2026", "status": "in_season", "total_rosters": n, "league_lineage_id": lin, "source_system": "sleeper"})
    frames["dim_leagues_meta"] = pl.DataFrame(lm)
    frames["dim_franchises_meta"] = pl.DataFrame([{"league_id": lg, "roster_id": r, "franchise_id": f"{lg}_{r}"} for lg, _, n in LEAGUES for r in range(1, n + 1)])
    frames["dim_league_settings"] = pl.DataFrame([{"league_id": lg, "league_lineage_id": lin, "qb_slots": 1, "rb_slots": 2, "wr_slots": 3, "te_slots": 1, "flex_slots": 1,
                                                   "superflex_slots": 1, "num_teams": n, "draft_rounds": 3, "valid_from": datetime(2026, 1, 1), "valid_to": None, "is_current": True}
                                                  for lg, lin, n in LEAGUES] +
                                                 [{"league_id": lin, "league_lineage_id": lin, "qb_slots": 1, "rb_slots": 2, "wr_slots": 3, "te_slots": 1, "flex_slots": 1,
                                                   "superflex_slots": 1, "num_teams": n, "draft_rounds": 3, "valid_from": datetime(2025, 1, 1), "valid_to": datetime(2026, 1, 1), "is_current": False}
                                                  for lg, lin, n in LEAGUES])
    # players: 400 priced, all with a gsis id and a CDN avatar
    pids = [str(1000 + i) for i in range(400)]
    frames["dim_players_master"] = pl.DataFrame({"player_key": pids, "gsis_id": [f"00-00{i:05d}" for i in range(400)], "avatar_url": ["https://sleepercdn.com/x"] * 400})
    # values: every day since 2020-06 for 400 players (SF Standard DYNASTY), ktc + fc
    hist_days = [date(y, m, 1) for y in range(2020, 2027) for m in range(1, 13) if date(2020, 6, 1) <= date(y, m, 1) < date(2026, 9, 1)] + days
    fav = pl.DataFrame({"valuation_date": [d for d in hist_days for _ in pids], "player_id": pids * len(hist_days)})
    fav = fav.with_columns(pl.lit("P").alias("name"), pl.lit("WR").alias("position"), pl.lit("DYNASTY").alias("market_type"), pl.lit("SF").alias("qb_format"),
                           pl.lit("Standard").alias("te_premium"), pl.lit(3000).alias("ktc_value"), pl.lit(3000).alias("fc_value")).unique(["valuation_date", "player_id"])
    frames["fact_asset_values"] = fav
    frames["quarantine_unmapped_players"] = pl.DataFrame({"valuation_date": [TODAY] * 3, "name": ["a", "b", "c"]})
    # picks: 3 tiers for 2027-2028 rounds 1-3
    frames["fact_pick_values"] = pl.DataFrame([{"valuation_date": TODAY.isoformat(), "season": s, "round": r, "tier": t, "source_system": "ktc", "value": 1000.0}
                                               for s in (2027, 2028) for r in (1, 2, 3) for t in ("Early", "Mid", "Late")])
    # roster snapshot + ledger: 20 players per roster, picks conserved for 2027-2029
    snap, led = [], []
    for lg, lin, n in LEAGUES:
        for r in range(1, n + 1):
            for k in range(20):
                pid = pids[(r * 20 + k) % 400]
                snap.append({"league_id": lg, "roster_id": r, "player_id": pid})
                led.append({"franchise_id": f"{lin}_{r}", "asset_type": "player", "asset_id": pid, "valid_from": "2026-09-01", "valid_to": None, "is_current": True})
            for s in (2027, 2028, 2029):
                for rd in (1, 2, 3):
                    led.append({"franchise_id": f"{lin}_{r}", "asset_type": "pick", "asset_id": f"{s}:{rd}:{r}", "valid_from": "2026-09-01", "valid_to": None, "is_current": True})
    parts["bronze/sleeper/rosters/roster_players/daily/"] = [(TODAY.isoformat(), "rosters/today")]
    frames["rosters/today"] = pl.DataFrame(snap)
    frames["fact_roster_membership"] = pl.DataFrame(led)
    # bronze feeds
    for prefix in ("bronze/ktc/dynasty/daily_load/", "bronze/ktc/redraft/daily_load/", "bronze/ktc/devy/daily_load/", "bronze/fantasycalc/values/daily/", "bronze/sleeper/drafts/drafts/", "bronze/nflverse/contracts/"):
        parts[prefix] = [(TODAY.isoformat(), prefix + "today")]
    frames["bronze/ktc/dynasty/daily_load/today"] = pl.DataFrame({"playerName": ["a"] * 500, "playerID": list(range(500)), "sf_value": [1.0] * 500, "oneqb_value": [1.0] * 500})
    frames["bronze/fantasycalc/values/daily/today"] = pl.DataFrame({"n_qb": [2] * 400 + [1] * 400, "n_teams": [12] * 800, "ppr": [1] * 800, "value": [1] * 800, "sleeper_id": ["1"] * 800})
    frames["bronze/sleeper/drafts/drafts/today"] = pl.DataFrame({"league_id": [lg for lg, _, _ in LEAGUES], "season": ["2026"] * 2, "status": ["complete"] * 2})
    for lg, _, _ in LEAGUES:
        frames[f"bronze/sleeper/transactions/transactions/daily/league_id={lg}/data.parquet"] = pl.DataFrame({"created": [int(datetime(2026, 10, 5).timestamp() * 1000)]})
    # production facts
    frames["fact_player_week"] = pl.DataFrame({"player_id": ["p1"] * 5, "season": [2026] * 5, "week": [1, 2, 3, 4, 5], "game_date": [date(2026, 9, 10) + timedelta(days=7 * k) for k in range(5)]})
    frames["fact_player_season"] = pl.DataFrame({"player_id": ["p1", "p1"], "season": [2025, 2026], "games": [17, 5], "fpts": [170.0, 50.0], "ppg": [10.0, 10.0],
                                                 "scored_under_league_id": ["L", "L"], "season_complete": [True, False]})
    for t in ("fact_player_week_status", "fact_player_injury_week", "fact_depth_chart_week", "fact_team_week_strength"):
        frames[t] = pl.DataFrame({"season": [2026] * 18, "week": list(range(1, 19)), "played": [True] * 5 + [False] * 13, "played_this_week": [True] * 5 + [False] * 13})
    frames["fact_team_season_strength"] = pl.DataFrame({"season": [2026] * 32, "team": [f"T{i}" for i in range(32)]})
    frames["fact_player_contract_season"] = pl.DataFrame({"season": [2026] * 1600, "gsis_id": [str(i) for i in range(1600)]})
    frames["fact_college_player_season"] = pl.DataFrame({"season": [2024, 2025], "cfbd_id": ["a", "b"]})
    return frames, parts


def run(frames, parts, previous=None, only=None):
    ctx = Context(bucket="test", today=TODAY, frames=frames, partitions=parts, previous=previous)
    return run_checks(suite(only), ctx)


def failed(results):
    return {r["check"]: r for r in results.filter(~pl.col("passed")).to_dicts()}


def test_the_healthy_lake_passes_every_enforced_check():
    frames, parts = healthy()
    res = run(frames, parts)
    bad = {k: v["observed"] for k, v in failed(res).items() if not v["known_open"]}
    assert bad == {}, bad
    s = summarize(res)
    assert s["checks"] == len(SUITE) and s["failed_errors"] == 0 and res.filter(pl.col("error") != "").height == 0


def test_every_check_names_the_bug_it_guards_and_has_a_unique_dotted_name():
    names = [c.name for c in SUITE]
    assert len(names) == len(set(names)) and all(n.count(".") == 2 for n in names)
    assert all(len(c.guards) > 20 for c in SUITE) and all(c.severity in ("error", "warn") for c in SUITE)


def test_known_open_failures_are_reported_as_warnings_with_the_note():
    frames, parts = healthy()
    frames["dim_league_settings"] = frames["dim_league_settings"].with_columns(pl.lit(None, dtype=pl.Utf8).alias("league_lineage_id"))
    r = failed(run(frames, parts, only="league_settings.lineage"))["dim.league_settings.lineage_assigned"]
    assert r["severity"] == "error" and r["effective_severity"] == "warn" and "PR #12" in r["known_open"]
    assert summarize(run(frames, parts, only="league_settings.lineage"))["failed_errors"] == 0


# ------------------------------------------------------------------ the bugs, replayed
def test_ktc_outage_the_parser_break_of_2026_09():
    frames, parts = healthy()
    parts["bronze/ktc/dynasty/daily_load/"] = [("2026-09-07", "bronze/ktc/dynasty/daily_load/today")]
    f = failed(run(frames, parts, only="ktc_dynasty"))
    assert "bronze.ktc_dynasty.partition_fresh" in f and "32 days old" in f["bronze.ktc_dynasty.partition_fresh"]["observed"]
    frames["bronze/ktc/dynasty/daily_load/today"] = pl.DataFrame({"playerName": ["a"] * 12, "playerID": [1] * 12})   # a parser returning scraps
    assert "bronze.ktc_dynasty.partition_shape" in failed(run(frames, parts, only="ktc_dynasty"))


def test_stale_value_fact_and_a_missing_day():
    frames, parts = healthy()
    fav = frames["fact_asset_values"]
    frames["fact_asset_values"] = fav.filter(pl.col("valuation_date") != date(2026, 10, 3))
    f = failed(run(frames, parts, only="asset_values"))
    assert "fact.asset_values.no_gaps_30d" in f and "2026-10-03" in f["fact.asset_values.no_gaps_30d"]["observed"]
    frames["fact_asset_values"] = fav.filter(pl.col("valuation_date") < date(2026, 10, 1))
    f = failed(run(frames, parts, only="asset_values"))
    assert {"fact.asset_values.fresh_ktc", "fact.asset_values.fresh_fantasycalc"} <= set(f)


def test_the_local_load_player_gap_2024_08_to_2025_10():
    frames, parts = healthy()
    fav = frames["fact_asset_values"]
    frames["fact_asset_values"] = fav.filter(~((pl.col("valuation_date") >= date(2024, 8, 3)) & (pl.col("valuation_date") < date(2025, 10, 1))))
    r = failed(run(frames, parts, only="history_continuous"))["fact.asset_values.history_continuous"]
    assert "2024-09=0" in r["observed"] and r["value"] >= 13


def test_the_sparse_gsis_crosswalk():
    frames, parts = healthy()
    pm = frames["dim_players_master"]
    frames["dim_players_master"] = pm.with_columns(pl.when(pl.int_range(pl.len()) < 300).then(None).otherwise(pl.col("gsis_id")).alias("gsis_id"))
    r = failed(run(frames, parts, only="gsis_coverage"))["dim.players_master.gsis_coverage_of_priced"]
    assert "25%" in r["observed"] and r["effective_severity"] == "warn" and r["known_open"]


def test_the_league_dim_oscillation_and_the_frozen_rollover():
    frames, parts = healthy()
    lm = frames["dim_leagues_meta"]
    prev = previous_from(run(frames, parts, only="drift.leagues_meta"))
    frames["dim_leagues_meta"] = pl.concat([lm, lm.head(3)])                       # 7 rows: stale rows re-ingested
    f = failed(run(frames, parts, previous=prev, only="leagues_meta"))
    assert {"dim.leagues_meta.unique_league", "drift.leagues_meta.row_count"} <= set(f)
    frames["dim_leagues_meta"] = lm.filter(pl.col("season") == "2025")               # 2026 never rolled forward
    frames["dim_franchises_meta"] = frames["dim_franchises_meta"].with_columns(pl.col("league_id").replace({lg: lin for lg, lin, _ in LEAGUES}))
    f = failed(run(frames, parts, only="current_season_per_lineage"))
    assert "lineages behind season 2026" in f["dim.leagues_meta.current_season_per_lineage"]["observed"]


def test_the_lineup_regime_change_is_caught_on_the_second_run():
    frames, parts = healthy()
    prev = previous_from(run(frames, parts, only="lineup_regime"))
    assert run(frames, parts, previous=prev, only="lineup_regime")["passed"].all()
    frames["dim_league_settings"] = frames["dim_league_settings"].with_columns(pl.when(pl.col("is_current")).then(2).otherwise(pl.col("superflex_slots")).alias("superflex_slots"))
    r = failed(run(frames, parts, previous=prev, only="lineup_regime"))["drift.league_settings.lineup_regime"]
    assert "CHANGED" in r["observed"] and r["effective_severity"] == "error"


def test_the_ledger_catches_an_empty_fact_and_the_pick_double_count():
    frames, parts = healthy()
    led = frames["fact_roster_membership"]
    frames["fact_roster_membership"] = led.filter(pl.col("asset_type") == "pick")      # the scoping bug: players silently gone
    r = failed(run(frames, parts, only="current_players_match_snapshot"))["fact.roster_membership.current_players_match_snapshot"]
    assert "22 rosters" in r["observed"] and "0 vs 20" in r["detail"]
    extra = led.filter((pl.col("asset_type") == "pick") & (pl.col("asset_id") == "2027:1:1") & (pl.col("franchise_id") == f"{LEAGUES[0][1]}_1")).with_columns(pl.lit(f"{LEAGUES[0][1]}_2").alias("franchise_id"))
    frames["fact_roster_membership"] = pl.concat([led, extra])                           # the same pick held twice
    f = failed(run(frames, parts, only="roster_membership"))
    assert "fact.roster_membership.scd2" in f and "several current" in f["fact.roster_membership.scd2"]["observed"]
    assert "2027 R1=11/10" in f["fact.roster_membership.picks_conserved"]["detail"]


def test_transactions_freeze_and_a_missing_draft():
    frames, parts = healthy()
    lg = LEAGUES[0][0]
    frames[f"bronze/sleeper/transactions/transactions/daily/league_id={lg}/data.parquet"] = pl.DataFrame({"created": [int(datetime(2025, 12, 9).timestamp() * 1000)]})
    r = failed(run(frames, parts, only="in_season_activity"))["bronze.sleeper_transactions.in_season_activity"]
    assert lg in r["observed"] and r["effective_severity"] == "warn"
    frames["bronze/sleeper/drafts/drafts/today"] = pl.DataFrame({"league_id": [lg], "season": ["2025"], "status": ["complete"]})
    f = failed(run(frames, parts, only="drafts"))
    assert "bronze.sleeper_drafts.current_season_present" in f


def test_production_facts_keep_up_and_stay_consistent():
    frames, parts = healthy()
    frames["fact_player_week_status"] = pl.DataFrame({"season": [2026] * 18, "week": list(range(1, 19)), "played": [True] * 3 + [False] * 15})   # the schedule is there, the games are not
    assert "fact.player_week_status.keeps_up" in failed(run(frames, parts, only="keeps_up"))
    frames["fact_player_season"] = frames["fact_player_season"].with_columns(pl.lit(9.0).alias("ppg"))
    assert "fact.player_season.ppg_consistent" in failed(run(frames, parts, only="player_season"))
    frames["fact_player_week"] = frames["fact_player_week"].filter(pl.col("week") <= 2)   # production two weeks behind
    assert "fact.player_week.current_week_present" in failed(run(frames, parts, only="player_week"))


def test_a_crashing_check_is_a_failed_row_not_a_crashed_run():
    frames, parts = healthy()
    del frames["fact_team_season_strength"]
    ctx = Context(bucket="test", today=TODAY, frames=frames, partitions=parts)
    ctx._bucket = lambda: (_ for _ in ()).throw(RuntimeError("no gcs in tests"))
    res = run_checks(suite("team_season_strength"), ctx)
    r = res.to_dicts()[0]
    assert not r["passed"] and "check raised" in r["observed"] and r["error"]


def test_never_shrinks_uses_the_previous_run():
    frames, parts = healthy()
    prev = previous_from(run(frames, parts, only="never_shrinks"))
    frames["fact_asset_values"] = frames["fact_asset_values"].head(100)
    r = failed(run(frames, parts, previous=prev, only="never_shrinks"))["drift.fact_asset_values.never_shrinks"]
    assert "shrank" in r["observed"]
