"""League trades, priced three ways (BACKLOG item 25).

Every completed trade in the owner's three dynasty lineages (Sleeper transactions, 2021 on) with
both sides' assets:

* ``load_trades`` -> (trades, legs): one row per trade and one row per asset moved, with the
  receiving and giving rosters, the league season, the date and the fantasy week (``leg``);
  pick legs carry the pick's season, round and original roster.
* ``resolve_picks``: a traded pick whose draft has happened becomes the player taken with it
  (drafts + draft order), so the pick can be scored on what it delivered.
* ``price_ktc``: KTC dynasty (SF, standard) value of every asset at the trade date and today;
  picks at the Mid tier of their round until the draft, the drafted player's value after.
* ``realized_wins``: what every asset delivered after the trade, in wins above replacement in
  the lineage's own units (weekly points vs the lineage's replacement line, through the win
  curve), counted from the week after the trade to today.
* ``manager_table``: per manager and lineage, value given vs received on each basis, net wins,
  win rate, partners; ``trade_table`` scores each trade for each side.

Caveats: roster ids are mapped to today's franchise owners (an orphaned team's earlier trades are
credited to its current owner); a 2026 pick traded before its draft is priced at the Mid tier of
its round; points are scored under one scoring setting (fact_player_week).
"""
from __future__ import annotations

import json
from datetime import date, timedelta

import numpy as np
import polars as pl

import gcs_io
import league as lg
import lineup
import market

SETTINGS_PATH = "silver/fantasy/dim_league_settings/data.parquet"
TX = "bronze/sleeper/transactions/transactions"
TX_PLAYERS = "bronze/sleeper/transactions/transaction_players"
TX_PICKS = "bronze/sleeper/transactions/draft_picks"
POS = ["QB", "RB", "WR", "TE"]


def _both(prefix: str) -> pl.DataFrame:
    parts = []
    for sub in ("full_load", "daily"):
        try:
            parts.append(gcs_io.read_lake_prefix(f"{prefix}/{sub}"))
        except Exception:  # noqa: BLE001 - a missing sub-store is fine
            pass
    return pl.concat(parts, how="diagonal_relaxed")


def _ids(s: str | None) -> list[int]:
    if not s:
        return []
    try:
        return [int(x) for x in json.loads(s)]
    except Exception:  # noqa: BLE001
        return [int(x) for x in s.strip("[]").split(",") if x.strip()]


# ------------------------------------------------------------------------------ loading
def load_trades() -> tuple[pl.DataFrame, pl.DataFrame]:
    """(trades, legs). ``legs``: transaction_id, lineage_id, league_id, season, date, leg,
    roster_id (receives), from_roster (gives), asset ('player' | 'pick'), player_id (Sleeper),
    pick_season, pick_round, pick_orig (original roster of the pick)."""
    meta = gcs_io.read_lake("silver/fantasy/dim_leagues_meta/data.parquet").select(
        "league_id", "league_name", pl.col("season").cast(pl.Int64), "league_lineage_id")
    tx = _both(TX).unique("transaction_id").filter((pl.col("type") == "trade") & (pl.col("status") == "complete"))
    tx = tx.with_columns(pl.from_epoch(pl.col("created") // 1000, time_unit="s").dt.date().alias("date"))
    tx = tx.join(meta, on="league_id", how="inner")
    tx = tx.with_columns(pl.col("roster_ids").map_elements(_ids, return_dtype=pl.List(pl.Int64)).alias("rosters"))
    trades = tx.select("transaction_id", pl.col("league_lineage_id").alias("lineage_id"), "league_id", "league_name", "season", "date",
                       pl.col("leg").fill_null(1).alias("leg"), "rosters", "creator")
    trades = trades.with_columns((pl.col("leg") > 1).alias("in_season"), pl.col("rosters").list.len().alias("n_teams"))

    tp = _both(TX_PLAYERS).unique(["transaction_id", "player_id", "roster_id", "action"]).filter(pl.col("transaction_id").is_in(trades["transaction_id"]))
    adds = tp.filter(pl.col("action") == "add").select("transaction_id", "player_id", pl.col("roster_id"))
    drops = tp.filter(pl.col("action") == "drop").select("transaction_id", "player_id", pl.col("roster_id").alias("from_roster"))
    players = adds.join(drops, on=["transaction_id", "player_id"], how="left").with_columns(pl.lit("player").alias("asset"))

    dp = _both(TX_PICKS).filter(pl.col("transaction_id").is_in(trades["transaction_id"]))
    # full_load (GraphQL dump) carries from/to with from_team_id = the NEW owner; the daily REST feed owner_id = new owner
    new_owner = pl.coalesce([pl.col("owner_id"), pl.col("from_team_id")]) if "owner_id" in dp.columns else pl.col("from_team_id")
    prev_owner = pl.coalesce([pl.col("previous_owner_id"), pl.col("to_team_id")]) if "previous_owner_id" in dp.columns else pl.col("to_team_id")
    picks = dp.select("transaction_id", new_owner.cast(pl.Int64).alias("roster_id"), prev_owner.cast(pl.Int64).alias("from_roster"),
                      pl.col("season").cast(pl.Int64).alias("pick_season"), pl.col("round").cast(pl.Int64).alias("pick_round"),
                      pl.col("roster_id").cast(pl.Int64).alias("pick_orig")).unique().with_columns(pl.lit("pick").alias("asset"))
    legs = pl.concat([players, picks], how="diagonal_relaxed").join(
        trades.select("transaction_id", "lineage_id", "league_id", "season", "date", "leg"), on="transaction_id", how="inner")
    return trades, legs


def franchises() -> pl.DataFrame:
    """(lineage_id, roster_id) -> manager display name, from today's franchises."""
    fr = gcs_io.read_lake("silver/fantasy/dim_franchises_meta/data.parquet")
    us = gcs_io.read_lake("silver/fantasy/dim_users/data.parquet")
    return (fr.join(us.select(pl.col("user_id").alias("owner_id"), "display_name", "primary_name"), on="owner_id", how="left")
            .select(pl.col("league_lineage_id").alias("lineage_id"), pl.col("roster_id").cast(pl.Int64),
                    pl.coalesce([pl.col("primary_name"), pl.col("display_name"), pl.col("current_team_name")]).alias("manager"),
                    "current_team_name", "is_orphan")
            .unique(["lineage_id", "roster_id"]))


# ------------------------------------------------------------------------------ picks -> players
def resolve_picks(legs: pl.DataFrame) -> pl.DataFrame:
    """Add ``drafted_player_id`` (Sleeper) and ``draft_slot`` to pick legs whose draft has happened:
    the lineage's linear draft of that season, the original roster's slot in the draft order, the
    player taken at (round, slot)."""
    meta = gcs_io.read_lake("silver/fantasy/dim_leagues_meta/data.parquet").select("league_id", pl.col("season").cast(pl.Int64), "league_lineage_id")
    dr = (gcs_io.read_lake_prefix("bronze/sleeper/drafts/drafts").unique("draft_id").filter(pl.col("type") == "linear")
          .join(meta, on="league_id", how="inner"))
    # owner -> roster by lineage (today's franchises; roster ids persist across a lineage's seasons)
    fr = (gcs_io.read_lake("silver/fantasy/dim_franchises_meta/data.parquet")
          .select(pl.col("league_lineage_id").alias("lineage_id"), pl.col("roster_id").cast(pl.Int64), pl.col("owner_id").cast(pl.Utf8))
          .unique(["lineage_id", "owner_id"]))
    rows = []
    for d in dr.iter_rows(named=True):
        v = d["draft_order_raw"]            # struct (user_id -> slot) in the lake; a JSON string in older dumps
        try:
            order = v if isinstance(v, dict) else (json.loads(v) if v else {})
        except Exception:  # noqa: BLE001
            order = {}
        for user, slot in order.items():
            if slot is None:
                continue
            rows.append({"lineage_id": d["league_lineage_id"], "pick_season": int(d["season"]), "draft_id": d["draft_id"], "owner_id": str(user), "draft_slot": int(slot)})
    if not rows:
        return legs.with_columns(pl.lit(None, pl.Utf8).alias("drafted_player_id"), pl.lit(None, pl.Int64).alias("draft_slot"))
    slots = pl.DataFrame(rows).join(fr, on=["lineage_id", "owner_id"], how="left")
    dp = gcs_io.read_lake_prefix("bronze/sleeper/drafts/draft_picks").unique(["draft_id", "pick_no"]).select(
        "draft_id", pl.col("round").cast(pl.Int64).alias("pick_round"), pl.col("draft_slot").cast(pl.Int64), pl.col("player_id").alias("drafted_player_id"))
    res = (slots.select("lineage_id", "pick_season", "draft_id", pl.col("roster_id").alias("pick_orig"), "draft_slot")
           .join(dp, on=["draft_id", "draft_slot"], how="inner")
           .select("lineage_id", "pick_season", "pick_round", "pick_orig", "draft_slot", "drafted_player_id").unique(["lineage_id", "pick_season", "pick_round", "pick_orig"]))
    return legs.join(res, on=["lineage_id", "pick_season", "pick_round", "pick_orig"], how="left")


# ------------------------------------------------------------------------------ KTC prices
def load_pick_prices() -> pl.DataFrame:
    """KTC pick tiers by date: (valuation_date, pick_season, pick_round, tier, ktc)."""
    import io
    pv = pl.read_parquet(io.BytesIO(gcs_io._client().bucket(gcs_io.LAKE_BUCKET).blob("silver/fantasy/fact_pick_values").download_as_bytes()))
    pv = pv.filter((pl.col("source_system") == "ktc") & (pl.col("market_type") == "DYNASTY") & (pl.col("qb_format") == "SF") & (pl.col("te_premium") == "Standard"))
    return pv.select(pl.col("valuation_date").cast(pl.Utf8).str.slice(0, 10).str.to_date().alias("valuation_date"), pl.col("season").cast(pl.Int64).alias("pick_season"), pl.col("round").cast(pl.Int64).alias("pick_round"),
                     "tier", pl.col("value").cast(pl.Float64).alias("ktc"))


def load_ktc_archive() -> pl.DataFrame:
    """KTC's historical dynasty (SF) player values from the bronze archive, 2020-04 to 2024-08, keyed
    by Sleeper id: the silver fact only carries today's ~430 listed players, so retired or dropped
    players (Elliott, Cook, Carr) are priced from here."""
    try:
        df = gcs_io.read_lake_prefix("bronze/ktc/dynasty/local_load")
    except Exception:  # noqa: BLE001
        return pl.DataFrame({"pid": [], "valuation_date": [], "ktc": []}, schema={"pid": pl.Utf8, "valuation_date": pl.Date, "ktc": pl.Float64})
    return (df.filter(pl.col("sleeper_id").is_not_null() & (pl.col("value") > 0))
            .select(pl.col("sleeper_id").cast(pl.Utf8).alias("pid"), pl.col("date").cast(pl.Date).alias("valuation_date"), pl.col("value").cast(pl.Float64).alias("ktc"))
            .unique(["pid", "valuation_date"]))


def _asof(values: pl.DataFrame, keys: list[str], at: pl.DataFrame, tolerance_days: int = 30, out: str = "ktc") -> pl.DataFrame:
    """Latest value on or before ``at.date`` within the tolerance, joined on ``keys``."""
    v = values.sort("valuation_date")
    a = at.sort("date")
    j = a.join_asof(v.rename({"valuation_date": "_vd"}), left_on="date", right_on="_vd", by=keys, strategy="backward",
                    tolerance=f"{tolerance_days}d")
    return j.rename({"ktc": out}).drop("_vd")


def price_ktc(legs: pl.DataFrame, hist: pl.DataFrame | None = None, pick_prices: pl.DataFrame | None = None) -> pl.DataFrame:
    """``ktc_then`` (at the trade date) and ``ktc_now`` (latest) per leg. Players by Sleeper id
    (fact_asset_values_daily is keyed on the master player_key); picks at the Mid tier of their
    round before the draft, the drafted player after."""
    hist = market.load_ktc_history() if hist is None else hist
    hv = hist.select(pl.col("player_key").alias("pid"), "valuation_date", pl.col("ktc_value").cast(pl.Float64).alias("ktc"))
    hv = pl.concat([hv, load_ktc_archive()]).unique(["pid", "valuation_date"], keep="first")     # the fact first, the archive where it has nothing
    latest = hv.filter(pl.col("valuation_date") >= hv["valuation_date"].max() - timedelta(days=7)).sort("valuation_date").unique("pid", keep="last").select("pid", pl.col("ktc").alias("ktc_now"))
    # "then": a player by his id at the trade date; a pick as a pick (it was one at the time), Mid tier of its round
    # "now": the player's latest value; a pick's drafted player if the draft has happened, else the pick tier today
    legs = legs.with_row_index("_i").with_columns(
        pl.when(pl.col("asset") == "player").then(pl.col("player_id")).otherwise(pl.col("drafted_player_id") if "drafted_player_id" in legs.columns else None).alias("pid"))
    pl_legs = legs.filter((pl.col("asset") == "player") & pl.col("pid").is_not_null())
    priced = _asof(hv, ["pid"], pl_legs.select("_i", "pid", "date"), out="ktc_then").select("_i", "ktc_then")
    legs = legs.join(priced, on="_i", how="left").join(latest, on="pid", how="left")
    if pick_prices is None:
        try:
            pick_prices = load_pick_prices()
        except Exception:  # noqa: BLE001
            pick_prices = None
    if pick_prices is not None and pick_prices.height:
        mid = pick_prices.filter(pl.col("tier").str.to_lowercase() == "mid").drop("tier")
        pk = legs.filter(pl.col("asset") == "pick").select("_i", "pick_season", "pick_round", "date")
        if pk.height:
            p2 = _asof(mid, ["pick_season", "pick_round"], pk, tolerance_days=60, out="pick_then").select("_i", "pick_then")
            legs = legs.join(p2, on="_i", how="left").with_columns(pl.coalesce([pl.col("ktc_then"), pl.col("pick_then")]).alias("ktc_then")).drop("pick_then")
        last_day = mid["valuation_date"].max()
        now_p = mid.filter(pl.col("valuation_date") == last_day).select("pick_season", "pick_round", pl.col("ktc").alias("pick_now"))
        legs = legs.join(now_p, on=["pick_season", "pick_round"], how="left").with_columns(
            pl.when(pl.col("ktc_now").is_null() & (pl.col("asset") == "pick")).then(pl.col("pick_now")).otherwise(pl.col("ktc_now")).alias("ktc_now")).drop("pick_now")
    return legs.drop("_i")


# ------------------------------------------------------------------------------ realized wins
def _crosswalk() -> pl.DataFrame:
    """Sleeper id -> gsis id, name, position. nflverse's fantasy_player_ids first (it maps ~97 % of
    traded players); dim_players_master as the fallback (it maps a third, and pads some gsis ids
    with a space, which is stripped here)."""
    client = gcs_io._client()
    names = sorted(b.name for b in client.list_blobs(gcs_io.LAKE_BUCKET, prefix="bronze/nflverse/fantasy_player_ids/") if b.name.endswith(".parquet"))
    nv = (gcs_io.read_lake(names[-1]).filter(pl.col("sleeper_id").is_not_null() & pl.col("gsis_id").is_not_null())
          .select(pl.col("sleeper_id").cast(pl.Utf8).alias("pid"), "gsis_id", "name", pl.col("position").alias("pos")).unique("pid"))
    pm = gcs_io.read_lake("silver/fantasy/dim_players_master/data.parquet")
    pm = (pm.filter(pl.col("gsis_id").is_not_null())
          .select(pl.col("player_key").alias("pid"), pl.col("gsis_id").str.strip_chars().alias("gsis_id"), pl.col("display_name").alias("name"), pl.col("position").alias("pos"))
          .unique("pid"))
    return pl.concat([nv, pm.filter(~pl.col("pid").is_in(nv["pid"]))])


def replacement_by_season(spec: lg.LeagueSpec, seasons: list[int], season_df: pl.DataFrame, weeks: pl.DataFrame) -> dict[int, dict[str, float]]:
    """The lineage's weekly replacement line per season (the last complete season's for a season in progress)."""
    out = {}
    complete = sorted({int(s) for s in season_df.filter(pl.col("season_complete") == True)["season"].unique().to_list()}) if "season_complete" in season_df.columns else seasons  # noqa: E712
    for s in seasons:
        use = s if s in complete else max([c for c in complete if c < s] or [s])
        out[s] = lineup.replacement_weekly(season_df.filter(pl.col("position").is_in(POS)), weeks, spec, [use])
    return out


def realized_wins(legs: pl.DataFrame, weeks: pl.DataFrame, rep: dict[str, dict[int, dict[str, float]]], curves: dict[str, lg.WinCurve],
                  through: date | None = None) -> pl.DataFrame:
    """Per leg: ``wins_since`` (wins above replacement delivered from the week after the trade to
    ``through``), ``pts_since``, ``games_since``, ``seasons_since`` (elapsed seasons, fractional).
    ``rep[lineage][season][pos]``; ``curves[lineage]``."""
    xw = _crosswalk()
    legs = legs.with_row_index("_i")
    pid = pl.col("player_id") if "drafted_player_id" not in legs.columns else pl.coalesce([pl.col("player_id"), pl.col("drafted_player_id")])
    L = legs.with_columns(pid.alias("pid")).join(xw, on="pid", how="left")
    w = weeks.select(pl.col("player_id").alias("gsis_id"), pl.col("season").cast(pl.Int64), pl.col("week").cast(pl.Int64), "fpts", "position", "game_date")
    w = w.filter(pl.col("position").is_in(POS))
    if through is not None:
        w = w.filter(pl.col("game_date") <= through)
    j = L.filter(pl.col("gsis_id").is_not_null()).select("_i", "gsis_id", "lineage_id", "season", "leg").join(w, on="gsis_id", how="inner")
    # weeks that count: later seasons entirely; the trade season from the week after the trade (an offseason trade = all of it)
    j = j.filter((pl.col("season_right") > pl.col("season")) | ((pl.col("season_right") == pl.col("season")) & (pl.col("week") > pl.when(pl.col("leg") > 1).then(pl.col("leg")).otherwise(0))))
    rows = []
    for (lin, s), g in j.group_by(["lineage_id", "season_right"], maintain_order=True):
        r = rep.get(lin, {}).get(int(s)) or {}
        curve = curves[lin]
        rp = g["position"].replace_strict(r, default=0.0, return_dtype=pl.Float64).to_numpy()
        pts = g["fpts"].fill_null(0.0).to_numpy().astype(float)
        ex = np.maximum(pts - rp, 0.0)
        g = g.with_columns(pl.Series("_w", curve.delta_win(curve.mean_points, ex)))
        rows.append(g.select("_i", "_w", "fpts"))
    if not rows:
        return legs.with_columns(pl.lit(0.0).alias("wins_since"), pl.lit(0.0).alias("pts_since"), pl.lit(0).alias("games_since")).drop("_i")
    agg = pl.concat(rows).group_by("_i").agg(pl.col("_w").sum().alias("wins_since"), pl.col("fpts").sum().alias("pts_since"), pl.len().alias("games_since"))
    out = legs.join(agg, on="_i", how="left").with_columns(pl.col("wins_since").fill_null(0.0), pl.col("pts_since").fill_null(0.0), pl.col("games_since").fill_null(0))
    return out.drop("_i")


# ------------------------------------------------------------------------------ scoring
def trade_table(trades: pl.DataFrame, legs: pl.DataFrame, fr: pl.DataFrame) -> pl.DataFrame:
    """One row per (trade, roster): what the roster received and gave on each basis."""
    def side(df: pl.DataFrame, col: str) -> pl.DataFrame:
        return df.group_by(["transaction_id", col]).agg(
            pl.len().alias("n"), (pl.col("asset") == "pick").sum().alias("picks"),
            pl.col("ktc_then").sum().alias("ktc_then"), pl.col("ktc_now").sum().alias("ktc_now"),
            pl.col("wins_since").sum().alias("wins"), pl.col("pts_since").sum().alias("pts"),
            pl.col("label").str.join(" + ").alias("assets")).rename({col: "roster_id"})
    name = pl.when(pl.col("asset") == "pick").then(
        pl.format("{} R{} pick", pl.col("pick_season"), pl.col("pick_round")) + pl.when(pl.col("drafted_name").is_not_null()).then(pl.format(" ({})", pl.col("drafted_name"))).otherwise(pl.lit(""))
    ).otherwise(pl.col("name"))
    L = legs.with_columns(name.alias("label"))
    recv = side(L, "roster_id").rename({c: f"recv_{c}" for c in ["n", "picks", "ktc_then", "ktc_now", "wins", "pts", "assets"]})
    give = side(L, "from_roster").rename({c: f"give_{c}" for c in ["n", "picks", "ktc_then", "ktc_now", "wins", "pts", "assets"]})
    t = recv.join(give, on=["transaction_id", "roster_id"], how="full", coalesce=True)
    t = t.join(trades.select("transaction_id", "lineage_id", "league_name", "season", "date", "leg", "in_season", "n_teams"), on="transaction_id", how="left")
    t = t.join(fr.select("lineage_id", "roster_id", "manager"), on=["lineage_id", "roster_id"], how="left")
    num = ["recv_ktc_then", "give_ktc_then", "recv_ktc_now", "give_ktc_now", "recv_wins", "give_wins", "recv_pts", "give_pts"]
    t = t.with_columns([pl.col(c).fill_null(0.0) for c in num])
    return t.with_columns((pl.col("recv_ktc_then") - pl.col("give_ktc_then")).alias("net_ktc_then"),
                          (pl.col("recv_ktc_now") - pl.col("give_ktc_now")).alias("net_ktc_now"),
                          (pl.col("recv_wins") - pl.col("give_wins")).alias("net_wins")).sort("date", "transaction_id")


def manager_table(tt: pl.DataFrame, min_seasons: float = 0.0) -> pl.DataFrame:
    """Per lineage and manager: trades, value given / received, net wins, win rate, tendencies."""
    scored = tt.filter(pl.col("seasons_since") >= min_seasons) if "seasons_since" in tt.columns else tt
    return (scored.group_by(["lineage_id", "league_name", "manager"]).agg(
        pl.len().alias("trades"), pl.col("in_season").mean().alias("in_season_share"),
        pl.col("recv_ktc_then").sum().alias("ktc_in"), pl.col("give_ktc_then").sum().alias("ktc_out"),
        pl.col("net_ktc_then").mean().alias("net_ktc_per_trade"),
        pl.col("recv_wins").sum().alias("wins_in"), pl.col("give_wins").sum().alias("wins_out"), pl.col("net_wins").sum().alias("net_wins"),
        (pl.col("net_wins") > 0.05).mean().alias("win_rate"), (pl.col("net_wins") < -0.05).mean().alias("loss_rate"),
        pl.col("net_wins").max().alias("best"), pl.col("net_wins").min().alias("worst"),
        pl.col("recv_picks").sum().cast(pl.Int64).alias("picks_in"), pl.col("give_picks").sum().cast(pl.Int64).alias("picks_out"),
        pl.col("net_ktc_now").sum().alias("net_ktc_now"))
        .with_columns((pl.col("picks_in") - pl.col("picks_out")).alias("net_picks"))
        .sort(["lineage_id", "net_wins"], descending=[False, True]))


def build(through: date | None = None) -> dict[str, pl.DataFrame]:
    """Everything: trades, legs (priced and scored), per-side trade table, manager table."""
    import feature_groups as fg
    trades, legs = load_trades()
    legs = resolve_picks(legs)
    fr = franchises()
    xw = _crosswalk()
    legs = legs.join(xw.select("pid", "name", "pos"), left_on="player_id", right_on="pid", how="left")
    legs = legs.join(xw.select(pl.col("pid").alias("drafted_player_id"), pl.col("name").alias("drafted_name"), pl.col("pos").alias("drafted_pos")), on="drafted_player_id", how="left")
    legs = price_ktc(legs)
    ctx = fg.Context()
    settings = gcs_io.read_lake(SETTINGS_PATH)
    season_df = ctx.matrix if hasattr(ctx, "matrix") else gcs_io.read_lake("silver/fantasy/fact_player_season/data.parquet")
    seasons = sorted(set(int(s) for s in legs["season"].to_list()) | {int(legs["season"].max()) + 1})
    seasons = [s for s in seasons if s <= (through or date.today()).year]
    rep, curves = {}, {}
    for lin in trades["lineage_id"].unique().to_list():
        try:
            spec = lg.LeagueSpec.from_settings(settings, lin, name=lin)
        except Exception:  # noqa: BLE001
            spec = lg.LeagueSpec.from_settings(settings, None, name=lin)
        rep[lin] = replacement_by_season(spec, seasons, season_df, ctx.weeks)
        curves[lin] = lg.curve_for_lineage(lin)
    legs = realized_wins(legs, ctx.weeks, rep, curves, through=through)
    end = through or date.today()
    legs = legs.with_columns(((pl.lit(end) - pl.col("date")).dt.total_days() / 365.25).alias("seasons_since"))
    tt = trade_table(trades, legs, fr).join(legs.group_by("transaction_id").agg(pl.col("seasons_since").max()), on="transaction_id", how="left")
    mt = manager_table(tt)
    return {"trades": trades, "legs": legs, "trade_table": tt, "managers": mt, "replacement": pl.DataFrame(
        [{"lineage_id": lin, "season": s, **{p: v.get(p) for p in POS}} for lin, by in rep.items() for s, v in by.items()])}
