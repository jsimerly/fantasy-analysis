"""The catalogue of checks on the lake. Every check names the bug or trap it exists for (``guards``), so
the list reads as the history of what has gone wrong and is now watched. Add a check with every data
bug fixed: the fix proves the code, the check proves the lake stays fixed.

Layers: ``bronze.*`` (the provider feeds landed and look like themselves), ``dim.*`` / ``fact.*`` (the
silver tables keep their keys, freshness, history and invariants), ``drift.*`` (what must not change
between runs, or must not shrink). Severity ``error`` fails the daily job; ``warn`` reports.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl

from data_quality import expectations as x
from data_quality.core import Check, Context

SUITE: list[Check] = []
SF_STD = (pl.col("market_type") == "DYNASTY") & (pl.col("qb_format") == "SF") & (pl.col("te_premium") == "Standard")


def check(name: str, table: str, severity: str, guards: str, known_open: str | None = None):
    def deco(fn):
        SUITE.append(Check(name, table, severity, guards, fn, known_open))
        return fn
    return deco


def _ktc_sf(ctx: Context) -> pl.DataFrame:
    return ctx.table("fact_asset_values").filter(SF_STD & pl.col("ktc_value").is_not_null())


# ====================================================================== bronze: the feeds landed
@check("bronze.ktc_dynasty.partition_fresh", "bronze/ktc/dynasty/daily_load", "error",
       "KTC's page layout changed 2026-09-08 and the daily scrape failed for weeks before anyone noticed (daily-pipeline-failures)")
def ktc_dynasty_fresh(ctx):
    return x.partition_fresh(ctx.partitions("bronze/ktc/dynasty/daily_load/"), ctx.today, 1, "ktc dynasty")


@check("bronze.ktc_dynasty.partition_shape", "bronze/ktc/dynasty/daily_load", "error",
       "a parser that still runs but returns a handful of rows or no value columns is the same outage with a green job")
def ktc_dynasty_shape(ctx):
    _, df = ctx.latest_partition("bronze/ktc/dynasty/daily_load/")
    if df is None:
        return x.fail("no partition")
    return x.all_of(x.row_count(df, 400, 2000), x.columns_present(df, ["playerName", "playerID", "sf_value", "oneqb_value"]))


@check("bronze.ktc_redraft.partition_fresh", "bronze/ktc/redraft/daily_load", "warn", "same scraper as dynasty; redraft gaps are unrecoverable (no per-player history pages)")
def ktc_redraft_fresh(ctx):
    return x.partition_fresh(ctx.partitions("bronze/ktc/redraft/daily_load/"), ctx.today, 1, "ktc redraft")


@check("bronze.ktc_devy.partition_fresh", "bronze/ktc/devy/daily_load", "warn", "same scraper as dynasty")
def ktc_devy_fresh(ctx):
    return x.partition_fresh(ctx.partitions("bronze/ktc/devy/daily_load/"), ctx.today, 1, "ktc devy")


@check("bronze.fantasycalc.partition_fresh", "bronze/fantasycalc/values/daily", "error", "the FantasyCalc series feeds the page's second market lens and the trade report")
def fc_fresh(ctx):
    return x.partition_fresh(ctx.partitions("bronze/fantasycalc/values/daily/"), ctx.today, 1, "fantasycalc")


@check("bronze.fantasycalc.settings_tagged", "bronze/fantasycalc/values/daily", "error",
       "24 untagged rows per player-day let staging keep the 2QB/14-team/1PPR series for both lenses (PR #27 tags n_qb/n_teams/ppr)")
def fc_tagged(ctx):
    _, df = ctx.latest_partition("bronze/fantasycalc/values/daily/")
    if df is None:
        return x.fail("no partition")
    cols = x.columns_present(df, ["n_qb", "n_teams", "ppr", "value", "sleeper_id"])
    if not cols.passed:
        return cols
    dyn = df.filter((pl.col("n_qb") == 2) & (pl.col("n_teams") == 12) & (pl.col("ppr") == 1))
    r = x.row_count(dyn, 300)
    return x.all_of(cols, x.Result(r.passed, f"rows tagged 2QB/12-team/1PPR: {r.observed}", r.value))


@check("bronze.sleeper_rosters.partition_fresh", "bronze/sleeper/rosters/roster_players/daily", "error", "the daily roster snapshot is the authoritative spine of the ownership ledger (snapshot era)")
def rosters_fresh(ctx):
    return x.partition_fresh(ctx.partitions("bronze/sleeper/rosters/roster_players/daily/"), ctx.today, 1, "roster snapshot")


@check("bronze.sleeper_rosters.current_leagues_covered", "bronze/sleeper/rosters/roster_players/daily", "error",
       "the lineage froze at 2025 for 8 months (ingestion-freshness-gap): a current league missing from the snapshot is the first symptom")
def rosters_cover(ctx):
    _, snap = ctx.latest_partition("bronze/sleeper/rosters/roster_players/daily/")
    if snap is None:
        return x.fail("no snapshot")
    per = snap.group_by("league_id").len()
    missing, thin = [], []
    for lg in ctx.current_leagues().to_dicts():
        n = per.filter(pl.col("league_id") == lg["league_id"])["len"].sum()
        if n == 0:
            missing.append(lg["league_id"])
        elif lg["total_rosters"] and n < lg["total_rosters"] * 10:
            thin.append(f"{lg['league_id']}={n}")
    if missing or thin:
        return x.fail(f"snapshot missing leagues {missing}; thin {thin}", len(missing) + len(thin))
    return x.ok(f"all {ctx.current_leagues().height} current leagues in the snapshot ({snap.height:,} roster rows)", snap.height)


@check("bronze.sleeper_rosters.one_league_per_lineage", "bronze/sleeper/rosters/roster_players/daily", "error",
       "the roster job re-ingested the completed 2025 leagues beside the 2026 ones on alternate days through 2026-09, booking a frozen second roster per franchise in the ledger (BACKLOG 35)")
def rosters_one_per_lineage(ctx):
    _, snap = ctx.latest_partition("bronze/sleeper/rosters/roster_players/daily/")
    if snap is None:
        return x.fail("no snapshot")
    lm = ctx.table("dim_leagues_meta").select(pl.col("league_id").cast(pl.Utf8), "league_lineage_id")
    per = snap.select(pl.col("league_id").cast(pl.Utf8)).unique().join(lm, on="league_id", how="left").group_by("league_lineage_id").agg(pl.col("league_id").n_unique().alias("n"), pl.col("league_id").alias("ids"))
    multi = per.filter(pl.col("n") > 1)
    if multi.height:
        return x.fail(f"{multi.height} lineages with several leagues in the snapshot", multi.height, "; ".join(f"{r['league_lineage_id']}: {r['ids']}" for r in multi.to_dicts()))
    return x.ok(f"one league per lineage in the snapshot ({per.height} lineages)", 0)


@check("bronze.sleeper_projections.coming_week_present", "bronze/sleeper/projections", "error",
       "the consensus feature group reads the provider's projection for the coming week; a season partition that stops at last week means the Monday model ran without it",
       known_open="the dataset lands with the deploy of sleeper-incremental-projections and the PROJ_SEASONS=2018-2025 backfill (BACKLOG 38); until then there is nothing to check")
def projections_fresh(ctx):
    if ctx.nfl_phase() == "offseason":
        return x.ok("offseason: not enforced")
    season = ctx.nfl_season()
    try:
        pj = ctx.blob(f"bronze/sleeper/projections/season={season}/data.parquet")
    except Exception:  # noqa: BLE001
        return x.fail(f"no projections partition for {season}")
    wk = _current_week(ctx) or 0
    have = sorted(pj["week"].unique().to_list())
    n_next = pj.filter(pl.col("week") == wk + 1).height
    if n_next < 1500:
        return x.fail(f"week {wk + 1} has {n_next} projections (weeks present {have[:3]}..{have[-1:]}), need 1500+", n_next)
    return x.ok(f"week {wk + 1} has {n_next:,} projections", n_next)


@check("bronze.sleeper_transactions.in_season_activity", "bronze/sleeper/transactions/transactions/daily", "warn",
       "transactions silently stopped at leg 14 of 2025 (incremental fetched weeks=[1] once complete); trades missing = picks and players mis-owned")
def txn_activity(ctx):
    if ctx.nfl_phase() == "offseason":
        return x.ok("offseason: not enforced")
    stale, seen = [], 0
    for lg in ctx.current_leagues().to_dicts():
        try:
            tx = ctx.blob(f"bronze/sleeper/transactions/transactions/daily/league_id={lg['league_id']}/data.parquet")
        except Exception:  # noqa: BLE001
            stale.append(f"{lg['league_id']}=no feed")
            continue
        seen += 1
        newest = tx.select(pl.col("created").max()).item()
        newest_d = datetime.fromtimestamp(newest / 1000).date() if newest else None
        if newest_d is None or (ctx.today - newest_d).days > 21:
            stale.append(f"{lg['league_id']}={newest_d}")
    if stale:
        return x.fail(f"no transaction in 21 days for {stale}", len(stale))
    return x.ok(f"every current league ({seen}) transacted within 21 days", 0)


@check("bronze.sleeper_drafts.current_season_present", "bronze/sleeper/drafts/drafts", "error",
       "the 2026 rookie draft was absent for months, so 2026 picks never consumed into rookies and every team carried the pick AND the player (ingestion-freshness-gap)")
def drafts_current(ctx):
    _, dr = ctx.latest_partition("bronze/sleeper/drafts/drafts/")
    if dr is None:
        return x.fail("no drafts partition")
    missing = []
    for lg in ctx.current_leagues().to_dicts():
        rows = dr.filter((pl.col("league_id") == lg["league_id"]) & (pl.col("season").cast(pl.Int64) == lg["season"]))
        if rows.height == 0:
            missing.append(f"{lg['league_id']} ({lg['season']})")
    if missing:
        r = x.fail(f"no draft for {missing}", len(missing))
        return r if ctx.nfl_phase() != "offseason" else x.Result(True, "offseason, not enforced: " + r.observed, len(missing))
    return x.ok("every current league has its season's draft", 0)


@check("bronze.nflverse.model_feeds_current_season", "bronze/nflverse", "error",
       "the in-season feature groups (usage, opportunity, consensus) read these feeds; a season partition that never lands means the Monday model silently runs without them",
       known_open="nextgen_stats_receiving / nextgen_stats_rushing / ff_opportunity (seasonal) and fantasy_rankings_history land with the deploy of PR #34's nflverse config and the daily reconcile (BACKLOG 37-38)")
def model_feeds(ctx):
    season = ctx.nfl_season()
    missing = []
    for ds in ("nextgen_stats_receiving", "nextgen_stats_rushing", "ff_opportunity"):
        parts = ctx.partitions(f"bronze/nflverse/{ds}/", key="season")
        if not any(v == str(season) for v, _ in parts):
            missing.append(f"{ds} season={season}")
    if not ctx.partitions("bronze/nflverse/fantasy_rankings_history/"):
        missing.append("fantasy_rankings_history")
    return x.fail(f"missing feeds: {missing}", len(missing)) if missing else x.ok("every model feed has its current-season partition", 0)


@check("bronze.nflverse_contracts.snapshot_fresh", "bronze/nflverse/contracts", "warn", "the Over The Cap snapshot refreshes on Tuesdays; a stale one means fact_player_contract_season stops moving")
def contracts_fresh(ctx):
    return x.partition_fresh(ctx.partitions("bronze/nflverse/contracts/"), ctx.today, 10, "contracts")


# ====================================================================== dims
@check("dim.players_master.unique_player_key", "dim_players_master", "error", "the id bridge: a duplicate player_key double-joins every value row")
def pm_unique(ctx):
    return x.unique_key(ctx.table("dim_players_master"), ["player_key"])


@check("dim.players_master.unique_gsis", "dim_players_master", "error", "two Sleeper players on one gsis_id double-join nflverse stats")
def pm_unique_gsis(ctx):
    pm = ctx.table("dim_players_master").filter(pl.col("gsis_id").is_not_null() & (pl.col("gsis_id").str.strip_chars() != ""))
    return x.unique_key(pm.with_columns(pl.col("gsis_id").str.strip_chars()), ["gsis_id"])


@check("dim.players_master.gsis_coverage_of_priced", "dim_players_master", "error",
       "gsis_id is set for a third of players, so an id-only KTC->nflverse join reached 62 of 307 priced players and skewed the market-trends read (2026-10-09)")
def pm_gsis_coverage(ctx):
    sf = _ktc_sf(ctx)
    latest = sf.filter(pl.col("valuation_date") == sf["valuation_date"].max()).filter(pl.col("ktc_value") >= 1000)
    pm = ctx.table("dim_players_master").select("player_key", pl.col("gsis_id").str.strip_chars().alias("gsis_id"))
    j = latest.join(pm, left_on="player_id", right_on="player_key", how="left")
    have = j.filter(pl.col("gsis_id").is_not_null() & (pl.col("gsis_id") != "")).height
    return x.share_at_least(have, j.height, 0.9, "priced players (KTC SF 1000+) with a gsis_id")


@check("dim.players_master.avatar_is_url", "dim_players_master", "warn", "avatar_url once coalesced the raw swish_id instead of building the Sleeper CDN URL (fantasy-etl-known-bugs)")
def pm_avatar(ctx):
    pm = ctx.table("dim_players_master").filter(pl.col("avatar_url").is_not_null())
    return x.share_at_least(pm.filter(pl.col("avatar_url").str.starts_with("http")).height, pm.height, 0.95, "avatar_url that are URLs")


@check("dim.leagues_meta.unique_league", "dim_leagues_meta", "error", "the dim oscillated between 3 and 6 rows in 2026-09 (incremental overlay kept stale rows)")
def lm_unique(ctx):
    return x.unique_key(ctx.table("dim_leagues_meta"), ["league_id"])


@check("dim.leagues_meta.status_values", "dim_leagues_meta", "error", "an unknown status breaks the active-league filters in every daily ingestion")
def lm_status(ctx):
    return x.allowed_values(ctx.table("dim_leagues_meta"), "status", {"pre_draft", "drafting", "in_season", "complete"})


@check("dim.leagues_meta.lineage_assigned", "dim_leagues_meta", "error", "2026 leagues orphaned with a null lineage, so nothing rolled forward (ingestion-freshness-gap, PR #12)")
def lm_lineage(ctx):
    return x.not_null(ctx.table("dim_leagues_meta").filter(pl.col("source_system") == "sleeper"), ["league_lineage_id"])


@check("dim.leagues_meta.current_season_per_lineage", "dim_leagues_meta", "error",
       "every lineage we track must have the NFL season's league once the season starts; the rollover never happened for 2026 until it was done by hand")
def lm_current(ctx):
    season = ctx.nfl_season()
    lm = ctx.table("dim_leagues_meta").with_columns(pl.col("season").cast(pl.Int64))
    behind = []
    for lin in ctx.current_leagues()["league_lineage_id"].drop_nulls().unique().to_list():
        mx = lm.filter(pl.col("league_lineage_id") == lin)["season"].max()
        if mx is None or mx < season:
            behind.append(f"{lin}={mx}")
    if behind:
        r = x.fail(f"lineages behind season {season}: {behind}", len(behind))
        return r if ctx.nfl_phase() != "offseason" else x.Result(True, "offseason, not enforced: " + r.observed, len(behind))
    return x.ok(f"every tracked lineage has a {season} league", 0)


@check("drift.leagues_meta.row_count", "dim_leagues_meta", "warn", "the 3/6-row oscillation: a league count that moves by more than one row a day is the dim flapping")
def lm_rows_drift(ctx):
    n = ctx.table("dim_leagues_meta").height
    prev = ctx.previous_value("drift.leagues_meta.row_count")
    if prev is None or abs(n - prev) <= 1:
        return x.ok(f"{n} leagues (last run {prev})", n)
    return x.fail(f"{n} leagues vs {prev:.0f} last run", n)


@check("dim.league_settings.scd2", "dim_league_settings", "error", "the league constitution is SCD2: one current row per league, no overlapping validity")
def ls_scd2(ctx):
    return x.scd2(ctx.table("dim_league_settings"), ["league_id"], "valid_from", "valid_to", "is_current")


@check("dim.league_settings.lineage_assigned", "dim_league_settings", "error", "the settings rows of the current leagues carry no lineage (found 2026-10-09 by this suite's first run)")
def ls_lineage(ctx):
    return x.not_null(ctx.table("dim_league_settings").filter(pl.col("is_current")), ["league_lineage_id"])


@check("drift.league_settings.lineup_regime", "dim_league_settings", "error",
       "the lineup settings define replacement level and the WAR scale: a silent change voided a day of model comparisons (BACKLOG 22, the regime lesson)")
def ls_regime(ctx):
    cur = ctx.table("dim_league_settings").filter(pl.col("is_current"))
    ids = ctx.current_leagues()["league_id"].to_list()
    cur = cur.filter(pl.col("league_id").is_in(ids)).sort("league_id")
    slots = ["qb_slots", "rb_slots", "wr_slots", "te_slots", "flex_slots", "superflex_slots", "num_teams", "draft_rounds"]
    desc = " ".join(f"{r['league_id'][-6:]}:" + ",".join(f"{c[:2]}{r[c]}" for c in slots) for r in cur.select(["league_id"] + slots).to_dicts())
    return x.unchanged(desc, ctx.previous_state("drift.league_settings.lineup_regime"), "lineups of the current leagues")


@check("dim.franchises_meta.one_row_per_roster", "dim_franchises_meta", "error", "roster_id is the lineage-stable key everything joins on")
def fm_unique(ctx):
    return x.unique_key(ctx.table("dim_franchises_meta"), ["league_id", "roster_id"])


@check("dim.franchises_meta.roster_count_matches_league", "dim_franchises_meta", "error", "a league with fewer franchises than rosters means the users/rosters feed missed a team")
def fm_counts(ctx):
    fm = ctx.table("dim_franchises_meta").group_by("league_id").len()
    bad = []
    for lg in ctx.current_leagues().to_dicts():
        n = fm.filter(pl.col("league_id") == lg["league_id"])["len"].sum()
        if lg["total_rosters"] and n != lg["total_rosters"]:
            bad.append(f"{lg['league_id']}={n}/{lg['total_rosters']}")
    return x.fail(f"franchise count != total_rosters: {bad}", len(bad)) if bad else x.ok("franchises per league match total_rosters", 0)


@check("dim.dates.spine", "dim_dates", "error", "the calendar spine every season/phase lookup keys off: unique days, no holes, a year ahead")
def dates_spine(ctx):
    d = ctx.table("dim_dates")
    mx = d["date"].max()
    ahead = x.ok(f"spine to {mx}") if mx and mx >= ctx.today + timedelta(days=365) else x.fail(f"spine ends {mx}, need a year ahead")
    return x.all_of(x.unique_key(d, ["date"]), x.no_date_gaps(d, "date", ctx.today, 365, 0), ahead)


# ====================================================================== facts: values
@check("fact.asset_values.fresh_ktc", "fact_asset_values", "error", "the KTC series went stale for weeks in 2026-09 while every job stayed scheduled")
def fav_fresh_ktc(ctx):
    return x.fresh(ctx.table("fact_asset_values").filter(pl.col("ktc_value").is_not_null()), "valuation_date", ctx.today, 1, "ktc")


@check("fact.asset_values.fresh_fantasycalc", "fact_asset_values", "error", "the stranded FantasyCalc fix (PR #28): the series must keep landing")
def fav_fresh_fc(ctx):
    return x.fresh(ctx.table("fact_asset_values").filter(pl.col("fc_value").is_not_null()), "valuation_date", ctx.today, 1, "fantasycalc")


@check("fact.asset_values.unique_key", "fact_asset_values", "error", "a duplicate player-day double-counts team value and breaks as-of joins")
def fav_unique(ctx):
    return x.unique_key(ctx.table("fact_asset_values"), ["player_id", "valuation_date", "market_type", "qb_format", "te_premium"])


@check("fact.asset_values.names_and_ids_present", "fact_asset_values", "error", "unmapped KTC rows once landed with name=None instead of the asset name / the quarantine (fantasy-etl-known-bugs)")
def fav_nulls(ctx):
    return x.not_null(ctx.table("fact_asset_values"), ["name", "player_id", "position"])


@check("fact.asset_values.no_future_dates", "fact_asset_values", "error", "the KTC local_load archive carried rows dated into 2027 (clip_future_dates, PR #26)")
def fav_future(ctx):
    return x.no_newer_than(ctx.table("fact_asset_values")["valuation_date"].max(), ctx.today, "valuation_date")


@check("fact.asset_values.no_gaps_30d", "fact_asset_values", "error", "a missed day is a hole in every as-of market join; the 2026-09 outage left weeks of them")
def fav_gaps(ctx):
    return x.no_date_gaps(_ktc_sf(ctx), "valuation_date", ctx.today, 30, 1)


@check("fact.asset_values.tep_lens_no_gaps_30d", "fact_asset_values", "warn",
       "the TE-premium series (the power-ranking lens) has holes the Standard series does not: the 2026-09 outage was backfilled from history pages that carry Standard only, and the team-value blend had no values for four weeks (BACKLOG 35)",
       known_open="2026-09-08 -> 09-30 has no TEP rows and cannot be recovered; the analysis blend now falls back to Standard per day; this note comes off after 2026-10-31")
def fav_tep_gaps(ctx):
    tep = ctx.table("fact_asset_values").filter((pl.col("market_type") == "DYNASTY") & (pl.col("qb_format") == "SF") & (pl.col("te_premium") == "TEP") & pl.col("ktc_value").is_not_null())
    if tep.height == 0:
        return x.ok("no TEP series yet")
    return x.no_date_gaps(tep, "valuation_date", ctx.today, 30, 1)


@check("fact.asset_values.history_continuous", "fact_asset_values", "error",
       "the fact was once built from local_load, which has NO player values 2024-08 -> 2025-10 (gcs-data-inventory); the per-player full_load fills it")
def fav_history(ctx):
    end = date(ctx.today.year, ctx.today.month, 1) - timedelta(days=1)
    return x.monthly_presence(_ktc_sf(ctx), "valuation_date", "player_id", date(2020, 6, 1), end, 80)   # KTC priced ~100 players in 2020, 280+ by 2025


@check("fact.asset_values.priced_pool_stable", "fact_asset_values", "warn", "the count of players KTC prices at 1000+ moves slowly; a jump is a rescale or a half-parsed page")
def fav_pool(ctx):
    sf = _ktc_sf(ctx).filter(pl.col("ktc_value") >= 1000)
    per = sf.group_by("valuation_date").len().sort("valuation_date")
    if per.height < 2:
        return x.ok("not enough days", per.height)
    today_n = per["len"][-1]
    ref = per["len"][-8:-1].median() if per.height >= 8 else per["len"][:-1].median()
    return x.within_pct(float(today_n), float(ref), 0.25, f"priced pool on {per['valuation_date'][-1]}")


@check("fact.asset_values.unmapped_quarantine_small", "quarantine_unmapped_players", "warn", "the staging crosswalk drifts: unmapped KTC/FC names pile up in quarantine instead of the fact")
def quarantine_small(ctx):
    if not ctx.has("quarantine_unmapped_players"):
        return x.ok("no quarantine file", 0)
    q = ctx.table("quarantine_unmapped_players")
    latest = q.filter(pl.col("valuation_date") == q["valuation_date"].max())
    n = latest.height
    return x.ok(f"{n} unmapped names on {q['valuation_date'].max()}", n) if n <= 50 else x.fail(f"{n} unmapped names on {q['valuation_date'].max()}, allowed 50", n)


@check("fact.pick_values.fresh", "fact_pick_values", "error", "pick tier prices feed the team-value and draft-slot tabs daily")
def pv_fresh(ctx):
    return x.fresh(ctx.table("fact_pick_values").filter(pl.col("source_system") == "ktc"), "valuation_date", ctx.today, 1, "ktc picks")


@check("fact.pick_values.tiers_present", "fact_pick_values", "warn",
       "KTC re-keys picks Early/Mid/Late -> 'Pick R.SS' once a draft order sets; a season/round with no tier rows loses value and ownership (ktc-pick-tier-to-slot)")
def pv_tiers(ctx):
    pv = ctx.table("fact_pick_values").filter(pl.col("source_system") == "ktc")
    latest = pv.filter(pl.col("valuation_date") == pv["valuation_date"].max())
    season = ctx.nfl_season()
    thin = []
    for s in (season + 1, season + 2):
        for rd in (1, 2, 3):
            n = latest.filter((pl.col("season") == s) & (pl.col("round") == rd))["tier"].n_unique()
            if n < 3:
                thin.append(f"{s} R{rd}={n} tiers")
    return x.fail(f"pick tiers missing on {pv['valuation_date'].max()}: {thin}", len(thin)) if thin else x.ok(f"3 tiers for every round of {season + 1}-{season + 2}", 0)


# ====================================================================== facts: ownership
def _lineage_of(ctx: Context, league_id_col: str = "league_id") -> pl.Expr:
    """league_id -> lineage: a lineage root id maps to itself, a season's league to its lineage (the
    ledger's franchise_id is ``<lineage root>_<roster_id>`` because roster_id is lineage-stable)."""
    lm = ctx.table("dim_leagues_meta").select("league_id", "league_lineage_id").drop_nulls()
    m = {r["league_id"]: r["league_lineage_id"] for r in lm.to_dicts()}
    m.update({lin: lin for lin in lm["league_lineage_id"].unique().to_list()})
    return pl.col(league_id_col).replace_strict(m, default=pl.col(league_id_col))


def _ledger_with_league(ctx: Context) -> pl.DataFrame:
    rm = ctx.table("fact_roster_membership")
    return (rm.with_columns(pl.col("franchise_id").str.split("_").list.first().alias("league_id"),
                            pl.col("franchise_id").str.split("_").list.last().cast(pl.Int64, strict=False).alias("roster_id"))
              .with_columns(_lineage_of(ctx).alias("lineage")))


@check("fact.roster_membership.scd2", "fact_roster_membership", "error", "the ownership ledger: an asset is held by one franchise of a lineage at a time, intervals never overlap",
       known_open="after the 2026-10-10 rebuild 6 overlapping intervals remain, all draft picks (the same pick booked twice at a transaction boundary, e.g. 2023:1:1); the player overlaps are gone. Was 88 on the first run 2026-10-09 (two franchises holding the same player or pick at once, most for a day or two at a transaction boundary, a few for months, e.g. player 11370 in lineage ...304 2026-06-30 to 08-24); BACKLOG 34")
def rm_scd2(ctx):
    return x.scd2(_ledger_with_league(ctx), ["lineage", "asset_type", "asset_id"], "valid_from", "valid_to", "is_current")


@check("fact.roster_membership.current_players_match_snapshot", "fact_roster_membership", "error",
       "the snapshot era is authoritative: current player holdings must equal today's roster snapshot per roster; an is_active scoping bug once emptied the fact silently")
def rm_snapshot(ctx):
    _, snap = ctx.latest_partition("bronze/sleeper/rosters/roster_players/daily/")
    if snap is None:
        return x.fail("no roster snapshot")
    led = _ledger_with_league(ctx).filter(pl.col("is_current") & (pl.col("asset_type") == "player"))
    a = led.group_by("lineage", "roster_id").len().rename({"len": "ledger"})
    b = snap.with_columns(_lineage_of(ctx).alias("lineage")).group_by("lineage", "roster_id").len().rename({"len": "snapshot"})
    cur = ctx.current_leagues().with_columns(_lineage_of(ctx).alias("lineage"))["lineage"].to_list()
    j = a.join(b, on=["lineage", "roster_id"], how="full", coalesce=True).filter(pl.col("lineage").is_in(cur)).fill_null(0)
    bad = j.filter(pl.col("ledger") != pl.col("snapshot"))
    if bad.height:
        return x.fail(f"{bad.height} rosters where the ledger != the snapshot", bad.height,
                      "; ".join(f"{r['lineage'][-6:]}/{r['roster_id']}: {r['ledger']} vs {r['snapshot']}" for r in bad.head(5).to_dicts()))
    return x.ok(f"{j.height} rosters match the snapshot ({int(j['ledger'].sum()):,} players)", 0)


@check("fact.roster_membership.picks_conserved", "fact_roster_membership", "error",
       "pick conservation: every current league holds exactly total_rosters picks per future season and round; the 2026 double-count broke this")
def rm_picks(ctx):
    led = _ledger_with_league(ctx).filter(pl.col("is_current") & (pl.col("asset_type") == "pick"))
    led = led.with_columns(pl.col("asset_id").str.split(":").list.get(0).cast(pl.Int64, strict=False).alias("pick_season"),
                           pl.col("asset_id").str.split(":").list.get(1).cast(pl.Int64, strict=False).alias("pick_round"))
    settings = ctx.table("dim_league_settings").filter(pl.col("is_current")).select("league_id", "draft_rounds")
    season = ctx.nfl_season()
    bad = []
    for lg in ctx.current_leagues().with_columns(_lineage_of(ctx).alias("lineage")).join(settings, on="league_id", how="left").to_dicts():
        rounds = int(lg["draft_rounds"] or 3)
        for s in (season + 1, season + 2, season + 3):
            for rd in range(1, rounds + 1):
                n = led.filter((pl.col("lineage") == lg["lineage"]) & (pl.col("pick_season") == s) & (pl.col("pick_round") == rd)).height
                if n != (lg["total_rosters"] or 0):
                    bad.append(f"{lg['league_id'][-6:]} {s} R{rd}={n}/{lg['total_rosters']}")
    return x.fail(f"{len(bad)} season-rounds off conservation", len(bad), "; ".join(bad[:8])) if bad else x.ok(f"picks conserved for {season + 1}-{season + 3}", 0)


# ====================================================================== facts: production
@check("fact.player_week.unique_key", "fact_player_week", "error", "the atomic production fact: one row per player-week")
def fpw_unique(ctx):
    return x.unique_key(ctx.table("fact_player_week"), ["player_id", "season", "week"])


@check("fact.player_week.current_week_present", "fact_player_week", "error", "in season the fact must carry last weekend's games, or every ROS projection and the in-season model run a week behind")
def fpw_current(ctx):
    if ctx.nfl_phase() not in ("regular", "playoffs"):
        return x.ok("offseason: not enforced")
    fpw = ctx.table("fact_player_week").filter(pl.col("season") == ctx.nfl_season())
    return x.fresh(fpw, "game_date", ctx.today, 9, f"games of {ctx.nfl_season()}")


@check("fact.player_week.no_future_games", "fact_player_week", "error", "a game_date after today means an unplayed week was scored as zero")
def fpw_future(ctx):
    return x.no_newer_than(ctx.table("fact_player_week")["game_date"].max(), ctx.today, "game_date")


@check("fact.player_season.unique_key", "fact_player_season", "error", "one row per player-season")
def fps_unique(ctx):
    return x.unique_key(ctx.table("fact_player_season"), ["player_id", "season"])


@check("fact.player_season.ppg_consistent", "fact_player_season", "error", "ppg must equal fpts/games; the rollup once carried stale games")
def fps_ppg(ctx):
    f = ctx.table("fact_player_season").filter(pl.col("games") > 0)
    bad = f.filter((pl.col("ppg") - pl.col("fpts") / pl.col("games")).abs() > 0.01)
    return x.fail(f"{bad.height:,} rows with ppg != fpts/games", bad.height, _sample_rows(bad, ["player_id", "season"])) if bad.height else x.ok("ppg == fpts/games", 0)


@check("fact.player_season.single_scoring_regime", "fact_player_season", "error",
       "every season is scored under one league's rules (the model trains on one scoring); two regimes in one table is a silent rescale")
def fps_regime(ctx):
    vals = ctx.table("fact_player_season")["scored_under_league_id"].drop_nulls().unique().to_list()
    return x.ok(f"scored under {vals}", 1) if len(vals) == 1 else x.fail(f"scored under {len(vals)} leagues: {vals[:4]}", len(vals))


@check("fact.player_season.last_season_complete", "fact_player_season", "error", "the previous NFL season must be flagged complete (the model's last training season) once the new one starts")
def fps_complete(ctx):
    f = ctx.table("fact_player_season").filter(pl.col("season") == ctx.nfl_season() - 1)
    if f.height == 0:
        return x.fail(f"no rows for {ctx.nfl_season() - 1}")
    return x.ok(f"{ctx.nfl_season() - 1} complete") if f["season_complete"].all() else x.fail(f"{ctx.nfl_season() - 1} not flagged complete on {f.filter(~pl.col('season_complete')).height} rows")


def _current_week(ctx: Context) -> int | None:
    fpw = ctx.table("fact_player_week").filter(pl.col("season") == ctx.nfl_season())
    return None if fpw.height == 0 else int(fpw["week"].max())


def _keeps_up(ctx: Context, table: str, lag: int = 1, played_col: str | None = None) -> x.Result:
    """The table's newest current-season week (of played rows, when the table carries the whole schedule)
    is within ``lag`` weeks of production's."""
    wk = _current_week(ctx)
    if wk is None:
        return x.ok("no current-season production yet")
    t = ctx.table(table).filter(pl.col("season") == ctx.nfl_season())
    if played_col and played_col in t.columns:
        t = t.filter(pl.col(played_col))
    mx = None if t.height == 0 else int(t["week"].max())
    msg = f"{table} at week {mx}, production at week {wk}"
    return x.ok(msg, mx or 0) if mx is not None and mx >= wk - lag else x.fail(msg + f", allowed lag {lag}", mx or 0)


@check("fact.player_week_status.keeps_up", "fact_player_week_status", "warn", "the status fact (played / injured / inactive) must follow production week by week")
def status_keeps_up(ctx):
    return _keeps_up(ctx, "fact_player_week_status", 0, "played")


@check("fact.player_injury_week.keeps_up", "fact_player_injury_week", "warn", "the injury fact feeds the market-trends injury read and the model's injury group")
def injury_keeps_up(ctx):
    return _keeps_up(ctx, "fact_player_injury_week", 1)


@check("fact.depth_chart_week.keeps_up", "fact_depth_chart_week", "warn", "depth charts feed the situation group")
def depth_keeps_up(ctx):
    return _keeps_up(ctx, "fact_depth_chart_week", 1)


@check("fact.team_week_strength.keeps_up", "fact_team_week_strength", "warn", "team strength to date feeds the in-season team group")
def tws_keeps_up(ctx):
    return _keeps_up(ctx, "fact_team_week_strength", 1, "played_this_week")


@check("fact.team_season_strength.current_season_32_teams", "fact_team_season_strength", "error", "every NFL team must have a current-season row (closing lines, record) for the team group")
def tss_current(ctx):
    t = ctx.table("fact_team_season_strength").filter(pl.col("season") == ctx.nfl_season())
    n = t["team"].n_unique()
    return x.ok(f"{n} teams in {ctx.nfl_season()}", n) if n == 32 else x.fail(f"{n} teams in {ctx.nfl_season()}, expected 32", n)


@check("fact.player_contract_season.current_season_present", "fact_player_contract_season", "warn", "the contract in force per player must exist for the current season")
def contracts_current(ctx):
    return x.row_count(ctx.table("fact_player_contract_season").filter(pl.col("season") == ctx.nfl_season()), 1000)


@check("fact.college_player_season.last_season_present", "fact_college_player_season", "warn", "the yearly CFBD workflow must have landed last season's college stats for the rookie / college groups")
def college_current(ctx):
    mx = ctx.table("fact_college_player_season")["season"].max()
    return x.ok(f"college through {mx}", mx) if mx and mx >= ctx.nfl_season() - 1 else x.fail(f"college ends {mx}, need {ctx.nfl_season() - 1}", mx or 0)


# ====================================================================== drift: history never shrinks
def _never_shrinks(name: str, table: str, slack: float = 0.0):
    def fn(ctx):
        n = ctx.table(table).height
        return x.not_below(float(n), ctx.previous_value(name), f"{table} rows", slack)
    return fn


for _t in ("fact_asset_values", "fact_player_week", "fact_roster_membership", "fact_pick_values", "dim_players_master"):
    SUITE.append(Check(f"drift.{_t}.never_shrinks", _t, "warn", "a rebuild that lost history (a source that stopped being read, a partition skipped) shows as a table shrinking",
                       _never_shrinks(f"drift.{_t}.never_shrinks", _t)))


def _sample_rows(df: pl.DataFrame, cols: list[str], n: int = 5) -> str:
    return "; ".join(",".join(str(v) for v in row) for row in df.select(cols).head(n).iter_rows())


def suite(only: str | None = None) -> list[Check]:
    return [c for c in SUITE if not only or only in c.name or only in c.table]


FFTODAY_SEASONS = range(2010, 2018)     # FFToday's own weekly projections, the consensus before Sleeper's 2018 floor (backfilled 2026-10-10)


@check("bronze.fftoday.history_complete", "bronze/fftoday/projections", "warn",
       "the weekly consensus before 2018 comes only from this one-time scrape; a season partition lost or truncated silently shortens the consensus group's history and the in-season model trains on fewer labelled weeks")
def fftoday_history(ctx):
    short = []
    for season in FFTODAY_SEASONS:
        try:
            df = ctx.blob(f"bronze/fftoday/projections/season={season}/data.parquet")
        except Exception:  # noqa: BLE001
            short.append(f"{season}: missing"); continue
        weeks = df["week"].n_unique() if "week" in df.columns else 0
        if df.height < 2000 or weeks < 16:
            short.append(f"{season}: {df.height} rows, {weeks} weeks")
    if short:
        return x.fail(f"{len(short)} of {len(FFTODAY_SEASONS)} seasons short", len(short), "; ".join(short))
    return x.ok(f"{len(FFTODAY_SEASONS)} seasons, 16+ weeks and 2,000+ rows each", 0)
