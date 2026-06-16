"""Shared loaders + the team-value measure for the exploratory analysis notebooks.

The heavy data lives in two places:
  * GCS (`gs://nfl-data-bronze/...`) — player values + dims, read live.
  * `analysis/_cache/*.parquet` — the SCD2 ownership ledger and KTC pick values,
    pre-built locally because the silver versions aren't deployed yet
    (PR #4 jobs haven't run). Rebuild via the prebuild script if stale.

Value lenses: KTC (deep history, ~2022→ players / ~2020→ picks) and FantasyCalc
(better market signal, only ~2025-10→). Picks are valued at the ROUND level for both
(KTC tier=Mid; FC round-generic "YYYY Nth") so the measure stays transparent — the
standings-based tier/slot refinement is a documented v2 knob, not baked in here.
"""
from __future__ import annotations

import re
from pathlib import Path

import polars as pl
from google.cloud import storage

BUCKET = "nfl-data-bronze"
CACHE = Path(__file__).resolve().parent / "_cache"

# default valuation lens (these leagues are superflex; verified via dim_league_settings)
DEFAULT_QB_FORMAT = "SF"
DEFAULT_TE_PREMIUM = "Standard"
DEFAULT_MARKET = "DYNASTY"


# --------------------------------------------------------------------------- IO
def _read_prefix(prefix: str, suffix: str = ".parquet", how: str = "diagonal_relaxed") -> pl.DataFrame:
    """Read+concat every blob under a GCS prefix (robust vs glob path-expansion).
    Recovers hive partition columns (``key=value`` path segments) that aren't in the files."""
    client = storage.Client()
    names = sorted(b.name for b in client.bucket(BUCKET).list_blobs(prefix=prefix)
                   if b.name.endswith(suffix))
    if not names:
        return pl.DataFrame()
    frames = []
    for n in names:
        df = pl.read_parquet(f"gs://{BUCKET}/{n}")
        for seg in n.split("/"):
            if "=" in seg:
                k, v = seg.split("=", 1)
                if k not in df.columns:
                    df = df.with_columns(pl.lit(v).alias(k))
        frames.append(df)
    return pl.concat(frames, how=how)


def load_ledger() -> pl.DataFrame:
    """SCD2 ownership ledger from production silver (franchise_id, asset_type, asset_id,
    valid_from, valid_to, is_current). Bare-blob parquet."""
    return pl.read_parquet(f"gs://{BUCKET}/silver/fantasy/fact_roster_membership")


def load_calendar() -> tuple[pl.DataFrame, pl.DataFrame]:
    """(dim_dates, dim_league_events) for graph overlays.

    Reads the production silver calendar dims if they exist; otherwise builds them on the
    fly from bronze using the silver modules (so this works before PR #7 is deployed)."""
    try:
        dd = pl.read_parquet(f"gs://{BUCKET}/silver/fantasy/dim_dates/data.parquet")
        le = pl.read_parquet(f"gs://{BUCKET}/silver/fantasy/dim_league_events/data.parquet")
        return dd, le
    except Exception:
        pass
    import importlib.util
    sf = Path(__file__).resolve().parent.parent / "data_engineering" / "silver_fantasy"

    def _imp(name):
        spec = importlib.util.spec_from_file_location(name, sf / f"{name}.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        return m

    DD, LE = _imp("dim_dates"), _imp("dim_league_events")
    sched = _read_prefix("bronze/nflverse/schedules/").select("season", "game_type", "week", "gameday")
    leagues = pl.read_parquet(f"gs://{BUCKET}/silver/fantasy/dim_leagues_meta/data.parquet")
    settings = pl.read_parquet(f"gs://{BUCKET}/silver/fantasy/dim_league_settings/data.parquet")
    drafts = _read_prefix("bronze/sleeper/drafts/drafts/")
    dd = DD.build_dim_dates(sched)
    le = LE.build_dim_league_events(drafts, settings, leagues, sched)
    return dd, le


def load_dims() -> tuple[pl.DataFrame, pl.DataFrame]:
    """(franchises, players). franchises: franchise_id -> current_team_name, lineage."""
    fr = pl.read_parquet(f"gs://{BUCKET}/silver/fantasy/dim_franchises_meta/data.parquet")
    pm = pl.read_parquet(f"gs://{BUCKET}/silver/fantasy/dim_players_master/data.parquet")
    return fr, pm


def load_league_events() -> pl.DataFrame:
    """League calendar events from `dim_league_events`
    -> (league_lineage_id, season:Int, event_type, event_date:Date), sorted by date.

    event_type in {startup_draft, rookie_draft, fantasy_end} — the offseason draft days
    (the inaugural startup draft = the lineage's start) and each season's end."""
    df = pl.read_parquet(f"gs://{BUCKET}/silver/fantasy/dim_league_events/**/*.parquet")
    return (df.select("league_lineage_id", pl.col("season").cast(pl.Int64),
                      "event_type", pl.col("event_date").cast(pl.Date))
            .sort("event_date"))


def load_nfl_season_starts() -> pl.DataFrame:
    """NFL regular-season Week 1 date per season from nflverse schedules
    -> (season:Int, season_start:Date). Pair with `fantasy_end` from dim_league_events
    to bracket each in-season window."""
    sch = _read_prefix("bronze/nflverse/schedules/")
    return (sch.filter(pl.col("game_type") == "REG")
            .with_columns(pl.col("season").cast(pl.Int64, strict=False),
                          pl.col("gameday").str.to_date())
            .group_by("season").agg(pl.col("gameday").min().alias("season_start"))
            .sort("season"))


def load_player_values(qb_format: str = DEFAULT_QB_FORMAT,
                       te_premium: str = DEFAULT_TE_PREMIUM) -> pl.DataFrame:
    """Dynasty player values from production `fact_asset_values_daily`
    -> (valuation_date, player_id, name, position, ktc_value, fc_value).

    Defaults to SF / Standard. Pass te_premium='TEP' (and qb_format='SF') for a superflex
    TE-premium league — the lens KTC's power rankings use. KTC history is continuous back to
    2020 (the fact reads `dynasty/full_load`); FC is the daily era (~2025-10+)."""
    df = pl.read_parquet(f"gs://{BUCKET}/silver/fantasy/fact_asset_values_daily")
    return (
        df.filter((pl.col("market_type") == DEFAULT_MARKET) & (pl.col("qb_format") == qb_format)
                  & (pl.col("te_premium") == te_premium))
        .select("valuation_date", "player_id", "name", "position", "ktc_value", "fc_value")
        .with_columns(pl.col("valuation_date").cast(pl.Date))
    )


def load_player_values_blend(qb_format: str = "SF") -> pl.DataFrame:
    """SF + TE-Premium player values with a Standard fallback for the pre-TEP era.

    KTC only began publishing TE-premium (TEP) values ~2025-10; before that only SF Standard
    exists. This returns one continuous series: SF/TEP from the TEP era onward, SF/Standard
    before it — the lens KTC's power rankings use, extended back over the full history. Shape
    matches load_player_values (valuation_date, player_id, name, position, ktc_value, fc_value)."""
    std = load_player_values(qb_format, "Standard")
    tep = load_player_values(qb_format, "TEP")
    if tep.is_empty():
        return std
    tstart = tep["valuation_date"].min()
    return pl.concat([std.filter(pl.col("valuation_date") < tstart), tep], how="vertical_relaxed")


def load_pick_values_round(source: str, qb_format: str = DEFAULT_QB_FORMAT,
                           te_premium: str = DEFAULT_TE_PREMIUM,
                           carry_forward_seasons: int = 2) -> pl.DataFrame:
    """Round-level pick value series for one source from production `fact_pick_values`
    -> (season:Utf8, round:Int64, valuation_date:Date, value:Int64).

    KTC is tiered -> use tier='Mid' as the round proxy; FantasyCalc is natively round-level
    (tier='NA'). Both selected via the fact's `source_system` column.

    carry_forward_seasons: the source only prices a draft class ~3yr out (KTC's furthest is
    2028 as of 2026-06), but the ledger holds picks further out (2029+). Rather than value
    those held picks at $0, proxy each further-out class from the FURTHEST priced class's
    value HISTORY, time-shifted forward by k years for the k-th class out. Rationale: a pick
    k seasons further out has, at any date, the time-to-maturity the furthest class had k
    years earlier, and longer-to-maturity picks are worth LESS — so a brand-new 2029 pick is
    anchored to the *beginning* of 2028's trajectory (its 3yr-out value), not 2028's current
    (2yr-out) value. When the source later publishes a real class it becomes the new furthest
    season and the proxy shifts out automatically (no overlap), so real values backfill with
    no code change. Set 0 to disable (strictly faithful to published data)."""
    df = pl.read_parquet(f"gs://{BUCKET}/silver/fantasy/fact_pick_values")
    if source == "ktc":
        f = df.filter((pl.col("source_system") == "ktc") & (pl.col("market_type") == DEFAULT_MARKET)
                      & (pl.col("qb_format") == qb_format) & (pl.col("te_premium") == te_premium)
                      & (pl.col("tier") == "Mid"))
    elif source == "fc":
        f = df.filter(pl.col("source_system") == "fantasycalc")
    else:
        raise ValueError(f"unknown source {source!r}")
    out = f.select(pl.col("season").cast(pl.Utf8), pl.col("round").cast(pl.Int64),
                   pl.col("valuation_date").cast(pl.Date), pl.col("value").cast(pl.Int64))
    if carry_forward_seasons and out.height:
        max_s = int(out["season"].cast(pl.Int64).max())             # furthest priced class
        furthest = out.filter(pl.col("season").cast(pl.Int64) == max_s)
        # shift that class's history forward by k years (k-th class out) so a further-out
        # pick is valued at the furthest class's same-time-to-maturity (lower) point; dates
        # before the shifted series resolve to its first/"beginning" value via the consumer's
        # first-priced backfill.
        proxies = [furthest.with_columns(pl.col("valuation_date").dt.offset_by(f"{k}y"),
                                         pl.lit(str(max_s + k)).alias("season"))
                   for k in range(1, carry_forward_seasons + 1)]
        out = pl.concat([out, *proxies], how="vertical")
    return out


_ORD = {"1st": 1, "2nd": 2, "3rd": 3, "4th": 4}


def _fc_pick_values_round() -> pl.DataFrame:
    """Parse FantasyCalc round-generic pick rows ('2027 1st') into a value series.
    (FC also has exact-slot rows '2026 Pick 1.09' — see fc_pick_values_slot.)"""
    fc = _read_prefix("bronze/fantasycalc/values/daily/")
    picks = fc.filter(pl.col("position") == "PICK").select(
        "name", "value", pl.col("load_date").cast(pl.Date).alias("valuation_date"))
    rows = []
    for name, value, vd in picks.iter_rows():
        m = re.fullmatch(r"(\d{4})\s+(1st|2nd|3rd|4th)", str(name).strip())
        if not m:
            continue  # skip exact-slot "Pick R.SS" rows for the round-level series
        rows.append({"season": m.group(1), "round": _ORD[m.group(2)],
                     "valuation_date": vd, "value": int(value)})
    return pl.DataFrame(rows, schema={"season": pl.Utf8, "round": pl.Int64,
                                      "valuation_date": pl.Date, "value": pl.Int64})


def load_rookie_draft_picks() -> pl.DataFrame:
    """Actual rookie-draft selections -> (season:Int, round, pick_no, player_id, draft_slot,
    is_startup). `is_startup` flags inaugural startup drafts (auction OR linear/snake) by their
    ROUND COUNT — startups draft whole rosters (~15-25 rounds), rookie drafts are 3-4 rounds. Using
    round count is robust where `type` isn't (newer lineages ran 22-round linear/snake startups)."""
    drafts = _read_prefix("bronze/sleeper/drafts/drafts/").unique("draft_id").select(
        "draft_id", pl.col("season").cast(pl.Int64).alias("season"), "type")
    picks = _read_prefix("bronze/sleeper/drafts/draft_picks/").unique(["draft_id", "pick_no"]).select(
        "draft_id", pl.col("round").cast(pl.Int64), pl.col("pick_no").cast(pl.Int64),
        pl.col("player_id").cast(pl.Utf8), pl.col("draft_slot").cast(pl.Int64))
    max_round = picks.group_by("draft_id").agg(pl.col("round").max().alias("_maxr"))
    return (
        picks.join(drafts, on="draft_id", how="inner").join(max_round, on="draft_id", how="left")
        .with_columns((pl.col("_maxr") > 7).alias("is_startup"))   # >7 rounds => startup
        .select("season", "round", "pick_no", "draft_slot", "player_id", "draft_id", "is_startup")
    )


def fc_pick_values_slot() -> pl.DataFrame:
    """FantasyCalc exact-slot pick values '2026 Pick R.SS' -> (season, round, slot, date, value).
    Used for the pick-EV analysis (not the round-level team-value measure)."""
    fc = _read_prefix("bronze/fantasycalc/values/daily/")
    picks = fc.filter(pl.col("position") == "PICK").select(
        "name", "value", pl.col("load_date").cast(pl.Date).alias("valuation_date"))
    rows = []
    for name, value, vd in picks.iter_rows():
        m = re.fullmatch(r"(\d{4})\s+Pick\s+(\d+)\.(\d+)", str(name).strip())
        if not m:
            continue
        rows.append({"season": m.group(1), "round": int(m.group(2)), "slot": int(m.group(3)),
                     "valuation_date": vd, "value": int(value)})
    return pl.DataFrame(rows, schema={"season": pl.Utf8, "round": pl.Int64, "slot": pl.Int64,
                                      "valuation_date": pl.Date, "value": pl.Int64})


# ---------------------------------------------------------------------- measure
def held_on(ledger: pl.DataFrame, date: str) -> pl.DataFrame:
    """Assets held on a given ISO date: valid_from <= date < valid_to (null = still open)."""
    return ledger.filter((pl.col("valid_from") <= date)
                         & (pl.col("valid_to").is_null() | (pl.col("valid_to") > date)))


def weekly_dates(start: str, end: str) -> list[str]:
    rng = pl.date_range(pl.lit(start).str.to_date(), pl.lit(end).str.to_date(),
                        interval="1w", eager=True)
    return [d.isoformat() for d in rng]


def _holdings_by_date(ledger: pl.DataFrame, dates: list[str]) -> pl.DataFrame:
    """Explode the SCD2 intervals onto a date grid: (franchise_id, asset_type, asset_id, date)
    for every (asset, date) the asset was held. Cross+filter — small enough (intervals x dates)."""
    dgrid = pl.DataFrame({"date": dates})
    return (
        ledger.join(dgrid, how="cross")
        .filter((pl.col("valid_from") <= pl.col("date"))
                & (pl.col("valid_to").is_null() | (pl.col("valid_to") > pl.col("date"))))
        .select("franchise_id", "asset_type", "asset_id", pl.col("date").str.to_date().alias("date"))
    )


def team_value_timeseries(ledger: pl.DataFrame, source: str, dates: list[str],
                          player_values: pl.DataFrame | None = None,
                          pick_values: pl.DataFrame | None = None) -> pl.DataFrame:
    """Team value per (franchise_id, date) for one value source ('ktc' | 'fc').

    Joins each held player to its value (as-of nearest date <= grid date) and each held
    pick to its round-level value, then sums. Returns
    (franchise_id, date, player_value, pick_value, total_value).
    """
    value_col = "ktc_value" if source == "ktc" else "fc_value"
    pv = (player_values if player_values is not None else load_player_values())
    pv = pv.select("valuation_date", "player_id", pl.col(value_col).alias("v")).drop_nulls("v") \
           .filter(pl.col("v") > 0).sort("valuation_date")
    pk = (pick_values if pick_values is not None else load_pick_values_round(source)).sort("valuation_date")

    hold = _holdings_by_date(ledger, dates).sort("date")

    # players: as-of join held player -> value
    players = (
        hold.filter(pl.col("asset_type") == "player")
        .join_asof(pv, left_on="date", right_on="valuation_date", by_left="asset_id",
                   by_right="player_id", strategy="backward")
        .select("franchise_id", "date", pl.col("v").fill_null(0).alias("player_value"))
    )
    # picks: parse asset_id "season:round:orig" -> (season, round); as-of join round value.
    # BACKFILL: KTC only starts pricing a draft class ~3yr out, but the ledger holds the pick
    # from mint, so a backward as-of reads null (->0) before KTC's window and snaps to full
    # value in one week. Coalesce those pre-window nulls to the pick's FIRST-priced value so the
    # series is smooth; inside the window the point-in-time backward value still applies.
    first_priced = pk.sort("valuation_date").group_by("season", "round").agg(
        pl.col("value").first().alias("_first"))
    picks = (
        hold.filter(pl.col("asset_type") == "pick")
        .with_columns(pl.col("asset_id").str.split(":").alias("_p"))
        .with_columns(pl.col("_p").list.get(0).alias("season"),
                      pl.col("_p").list.get(1).cast(pl.Int64).alias("round"))
        .sort("date")
        .join_asof(pk, left_on="date", right_on="valuation_date", by=["season", "round"],
                   strategy="backward")
        .join(first_priced, on=["season", "round"], how="left")
        .select("franchise_id", "date",
                pl.coalesce(["value", "_first"]).fill_null(0).alias("pick_value"))
    )
    pl_sum = players.group_by("franchise_id", "date").agg(
        pl.col("player_value").sum(), (pl.col("player_value") > 0).sum().alias("n_player"))
    pk_sum = picks.group_by("franchise_id", "date").agg(
        pl.col("pick_value").sum(), (pl.col("pick_value") > 0).sum().alias("n_pick"))
    return (
        pl_sum.join(pk_sum, on=["franchise_id", "date"], how="full", coalesce=True)
        .with_columns(pl.col("player_value").fill_null(0), pl.col("pick_value").fill_null(0),
                      pl.col("n_player").fill_null(0), pl.col("n_pick").fill_null(0))
        .with_columns((pl.col("player_value") + pl.col("pick_value")).alias("total_value"))
        .sort("franchise_id", "date")
    )


def _pr_adjval(val: pl.Expr, slotavg: pl.Expr) -> pl.Expr:
    """KTC's `prProcessV`, vectorized (MAXPLAYERVAL=10000 -> t=10100, t+100=10200).
    Recovered verbatim from keeptradecut.com/js/site.min.js."""
    a = pl.max_horizontal(slotavg, pl.lit(0.1))
    t = 10100.0
    r = (0.05 * (val / t).pow(1.3) + 0.05 * (val / (1.05 * a)) + 0.1) * val
    s = (val / 10200.0).pow(1.3)
    return r * ((2.0 * s + (a / 10200.0).pow(1.2)) / 3.0 * 0.7 + 0.3)


def team_power_index(ledger: pl.DataFrame, dates: list[str], player_values: pl.DataFrame,
                     fr_meta: pl.DataFrame, value_col: str = "ktc_value",
                     pick_values: pl.DataFrame | None = None) -> pl.DataFrame:
    """KTC's league POWER RANKING (the /power-rankings/teams page), replicated from KTC's JS.

    This is NOT total roster value: each team's assets are ranked by value, each is adjusted
    relative to the league-average value at its roster slot (`prProcessV`) — a non-linear depth
    discount — summed to `adj_total`, then scaled ``floor(adj_total / top-team adj_total * 99)``
    within each league/date. Pass SF/TEP `player_values` to match a superflex TE-premium league.

    Picks: if `pick_values` (round-level) is given, picks are folded into the SAME prProcessV
    pipeline as extra assets (KTC's default ``hrdp=0`` "Include Picks" view — verified vs the
    live page). Omit it for the players-only view (KTC's ``hrdp=1``). Returns
    (franchise_id, league_lineage_id, date, adj_total, power_index).
    """
    pv = (player_values.select("valuation_date", "player_id", pl.col(value_col).alias("val"))
          .drop_nulls("val").filter(pl.col("val") > 0).sort("valuation_date"))
    lin = fr_meta.select("franchise_id", "league_lineage_id").unique(subset=["franchise_id"])
    hold = _holdings_by_date(ledger, dates).sort("date").join(lin, on="franchise_id", how="left")
    players = (
        hold.filter(pl.col("asset_type") == "player")
        .join_asof(pv, left_on="date", right_on="valuation_date", by_left="asset_id",
                   by_right="player_id", strategy="backward")
        .select("franchise_id", "league_lineage_id", "date", pl.col("val"))
    )
    parts = [players]
    if pick_values is not None:
        # mirror team_value_timeseries' pick valuation: parse asset_id "season:round:orig",
        # as-of join the round value, backfill pre-window nulls to the first-priced value.
        pk = pick_values.sort("valuation_date")
        first_priced = pk.group_by("season", "round").agg(pl.col("value").first().alias("_first"))
        picks = (
            hold.filter(pl.col("asset_type") == "pick")
            .with_columns(pl.col("asset_id").str.split(":").alias("_p"))
            .with_columns(pl.col("_p").list.get(0).alias("season"),
                          pl.col("_p").list.get(1).cast(pl.Int64).alias("round"))
            .sort("date")
            .join_asof(pk, left_on="date", right_on="valuation_date", by=["season", "round"],
                       strategy="backward")
            .join(first_priced, on=["season", "round"], how="left")
            .select("franchise_id", "league_lineage_id", "date",
                    pl.coalesce(["value", "_first"]).alias("val"))
        )
        parts.append(picks)
    assets = (
        pl.concat(parts, how="vertical").filter(pl.col("val") > 0)
        # slot index within each team (0 = best), then the league-average value at that slot
        .with_columns((pl.col("val").rank("ordinal", descending=True).over("date", "franchise_id") - 1)
                      .alias("slot"))
        .with_columns(pl.col("val").mean().over("league_lineage_id", "date", "slot").alias("slotavg"))
        .with_columns(_pr_adjval(pl.col("val"), pl.col("slotavg")).alias("adjval"))
    )
    adj = assets.group_by("franchise_id", "league_lineage_id", "date").agg(
        pl.col("adjval").sum().alias("adj_total"))
    return (
        adj.with_columns(
            (pl.col("adj_total") / pl.col("adj_total").max().over("league_lineage_id", "date") * 99)
            .floor().alias("power_index"))
        .sort("franchise_id", "date")
    )


def league_diagnostics(tv: pl.DataFrame, fr_meta: pl.DataFrame):
    """Per (lineage, date) aggregates for spotting GLOBAL (synchronized) moves vs real ones.
    Returns (tv_plus, agg) where tv_plus adds `share` (value / league-mean that week), `idx`
    (value / the franchise's first non-zero value), and `ktc_index` (KTC's 1-99 scale: top
    team = 99 each date, computed on player_value to mirror KTC), and `agg` has the league
    mean/total, the league-wide valued-player count, avg value per valued player, and week-over-week %."""
    if "league_lineage_id" not in tv.columns:
        tv = tv.join(fr_meta.select("franchise_id", "league_lineage_id", "current_team_name"),
                     on="franchise_id", how="left")
    agg = (
        tv.group_by("league_lineage_id", "date").agg(
            pl.col("total_value").mean().alias("league_mean"),
            pl.col("total_value").sum().alias("league_total"),
            pl.col("n_player").sum().alias("lg_players_valued"),
            pl.len().alias("n_teams"),
        )
        .with_columns((pl.col("league_total") / pl.col("lg_players_valued").clip(1)).alias("val_per_valued"))
        .sort("league_lineage_id", "date")
        .with_columns(
            (pl.col("league_mean").pct_change().over("league_lineage_id") * 100).round(1).alias("wow_pct"),
            pl.col("lg_players_valued").diff().over("league_lineage_id").alias("d_players"),
        )
    )
    base = (tv.filter(pl.col("total_value") > 0).sort("date")
            .group_by("franchise_id").agg(pl.col("total_value").first().alias("_base")))
    tv_plus = (
        tv.join(agg.select("league_lineage_id", "date", "league_mean"), on=["league_lineage_id", "date"])
        .join(base, on="franchise_id", how="left")
        .with_columns((pl.col("total_value") / pl.col("league_mean")).alias("share"),
                      (pl.col("total_value") / pl.col("_base") * 100).alias("idx"),
                      # KTC's own scale: per (lineage, date) the top team = 99 and every
                      # other team scales off it. Computed on player_value (picks excluded)
                      # to mirror KTC's team index exactly.
                      (99 * pl.col("player_value")
                       / pl.col("player_value").max().over("league_lineage_id", "date")
                       ).round(0).alias("ktc_index"),
                      # full-roster value (players + picks) on the same 1-99 scale
                      (99 * pl.col("total_value")
                       / pl.col("total_value").max().over("league_lineage_id", "date")
                       ).round(0).alias("total_index"))
        .sort("franchise_id", "date")
    )
    return tv_plus, agg


def team_bags(lineage_id: str | None = None, as_of: str | None = None,
              ledger: pl.DataFrame | None = None, player_values: pl.DataFrame | None = None,
              pick_values: pl.DataFrame | None = None) -> dict:
    """Each current franchise's held-asset values (players + picks) as a value LIST, as-of a date.

    Returns {franchise_id: {"name", "values" (desc), "naive_sum", "n"}} — the per-team value
    distribution, which the summed/indexed measures (team_value_timeseries, team_power_index)
    don't expose. Handy for ad-hoc "what does each team hold and what's it worth" inspection.
    Picks are valued at round level. Loaders are called lazily; pass frames to reuse them."""
    led = ledger if ledger is not None else load_ledger()
    fr, _ = load_dims()
    pv = player_values if player_values is not None else load_player_values()
    pk = pick_values if pick_values is not None else load_pick_values_round("ktc")
    as_of = as_of or pv["valuation_date"].max().isoformat()
    cutoff = pl.lit(as_of).str.to_date()

    held = held_on(led, as_of)
    if lineage_id:
        held = held.filter(pl.col("franchise_id").str.starts_with(lineage_id))
    pl_latest = (pv.filter(pl.col("valuation_date") <= cutoff).sort("valuation_date")
                 .group_by("player_id").agg(pl.col("ktc_value").last().alias("v")))
    players = (held.filter(pl.col("asset_type") == "player")
               .join(pl_latest, left_on="asset_id", right_on="player_id", how="left"))
    pk_latest = (pk.filter(pl.col("valuation_date") <= cutoff).sort("valuation_date")
                 .group_by("season", "round").agg(pl.col("value").last().alias("v")))
    picks = (held.filter(pl.col("asset_type") == "pick")
             .with_columns(pl.col("asset_id").str.split(":").alias("_p"))
             .with_columns(pl.col("_p").list.get(0).alias("season"),
                           pl.col("_p").list.get(1).cast(pl.Int64).alias("round"))
             .join(pk_latest, on=["season", "round"], how="left"))
    allv = pl.concat([players.select("franchise_id", "v"), picks.select("franchise_id", "v")],
                     how="vertical")
    names = dict(zip(fr["franchise_id"].to_list(), fr["current_team_name"].to_list()))
    out = {}
    for fid in allv["franchise_id"].unique().to_list():
        vals = [float(x) for x in allv.filter(pl.col("franchise_id") == fid)["v"].drop_nulls().to_list()
                if x and x > 0]
        out[fid] = {"name": names.get(fid, fid), "values": sorted(vals, reverse=True),
                    "naive_sum": float(sum(vals)), "n": len(vals)}
    return out
