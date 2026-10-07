"""League trades, priced at the trade date and at N+1, N+2, ... years after it (machine_learning/BACKLOG.md
item 25; analysis, not a model).

Every completed trade in the owner's three dynasty lineages (Sleeper transactions, 2021 on) with
both sides' assets:

* ``load_trades`` -> (trades, legs): one row per trade and one row per asset moved, with the
  receiving and giving rosters, the league season, the date and the fantasy week (``leg``);
  pick legs carry the pick's season, round and original roster.
* ``resolve_picks``: a traded pick whose draft has happened becomes the player taken with it
  (drafts + draft order), with the draft date.
* ``price_horizons``: KTC dynasty (SF, standard) value of every asset at the trade date (N) and at
  N+1, N+2, N+3 years and today. A pick is always a pick, priced at what the market paid for a pick
  like it at the time (never at the player later taken with it): the Mid tier of its round until
  the draft order is known (January of the draft year), the tier of its actual slot after that, and
  once the draft has happened its value is frozen at the last pre-draft price. Where KTC's history
  has no price for that pick on that date (the lake has gaps), the same round and tier of another
  season at the same distance from its draft stands in. A horizon that has not arrived yet is null
  ("not yet"), an asset with no price on a date that has (a team defense, FAAB) is 0.
* ``ktc_combine``: KTC's trade-calculator combine (``processVNew``): a package's value is not the
  sum of its parts, the best asset counts for more, so a two-for-one has to overpay in raw value.
  Side values are combined with the trade's best asset as the reference.
* ``realized_wins`` (secondary): what every asset delivered after the trade, in wins above
  replacement in the lineage's own units (weekly points vs the lineage's weekly replacement line,
  through its win curve), from the week after the trade to today.
* ``trade_table``: one row per (trade, side) with the side's package value at each horizon, raw
  and combined, the net against the other side, and the wins; ``manager_table``: per manager.

Caveats: roster ids map to today's franchise owners (an orphaned team's earlier trades are
credited to its current owner); the silver KTC fact lists only today's ~430 players, so the bronze
archive (2020-04 to 2024-08) prices the rest and a player who dropped off KTC between 2024-08 and
2025-10 reads 0 in that window; one scoring setting for the wins.
"""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl

# the wins yardstick (league spec, weekly replacement line, win curve) and the KTC history loader live in the ML package
ML_SRC = Path(__file__).resolve().parents[1] / "machine_learning" / "src"
if str(ML_SRC) not in sys.path:
    sys.path.insert(0, str(ML_SRC))
import gcs_io  # noqa: E402
import league as lg  # noqa: E402
import pick_slots  # noqa: E402
import lineup  # noqa: E402
import market  # noqa: E402

SETTINGS_PATH = "silver/fantasy/dim_league_settings/data.parquet"
TX = "bronze/sleeper/transactions/transactions"
TX_PLAYERS = "bronze/sleeper/transactions/transaction_players"
TX_PICKS = "bronze/sleeper/transactions/draft_picks"
POS = ["QB", "RB", "WR", "TE"]
# trades the owner asked to drop from every table (transaction_id -> why)
EXCLUDED_TRANSACTIONS = {
    "948106660671827968": "2023-04-02 Stuck, Simerly-Becker: a joke trade (owner, 2026-10-05)",
}
HORIZONS = (0, 1, 2, 3)          # years after the trade at which the packages are re-priced
TOLERANCE_DAYS = 90              # a value is "at" a date if KTC priced the asset within this many days before it
FULL_LOAD_CACHE = Path(__file__).resolve().parent / "_cache" / "ktc_full_load.parquet"   # KTC per-asset daily history, 2020-04 .. 2025-10
TIERS = {"e": "Early", "m": "Mid", "l": "Late", "early": "Early", "mid": "Mid", "late": "Late"}


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


def _faab(s) -> list[tuple[int, int, int]]:
    """Sleeper's waiver_budget on a trade -> [(sender, receiver, amount)]. Two shapes in the lake:
    the GraphQL dump's ["4,10,16"] strings and the REST feed's [{"sender": 7, "receiver": 10, "amount": 25}]."""
    if s is None or (isinstance(s, str) and s.strip() in ("", "[]", "null")):
        return []
    try:
        items = json.loads(s) if isinstance(s, str) else list(s)
    except Exception:  # noqa: BLE001
        return []
    out = []
    for it in items:
        try:
            if isinstance(it, dict):
                out.append((int(it["sender"]), int(it["receiver"]), int(it["amount"])))
            else:
                a, b, c = [int(x) for x in str(it).split(",")]
                out.append((a, b, c))
        except Exception:  # noqa: BLE001
            continue
    return [x for x in out if x[2] > 0]


# ------------------------------------------------------------------------------ loading
def load_trades() -> tuple[pl.DataFrame, pl.DataFrame]:
    """(trades, legs). ``legs``: transaction_id, lineage_id, league_id, season, date, leg,
    roster_id (receives), from_roster (gives), asset ('player' | 'pick'), player_id (Sleeper),
    pick_season, pick_round, pick_orig (original roster of the pick)."""
    meta = gcs_io.read_lake("silver/fantasy/dim_leagues_meta/data.parquet").select(
        "league_id", "league_name", pl.col("season").cast(pl.Int64), "league_lineage_id")
    tx = _both(TX).unique("transaction_id").filter((pl.col("type") == "trade") & (pl.col("status") == "complete"))
    tx = tx.filter(~pl.col("transaction_id").is_in(list(EXCLUDED_TRANSACTIONS)))
    tx = tx.with_columns(pl.from_epoch(pl.col("created") // 1000, time_unit="s").dt.date().alias("date"))
    tx = tx.join(meta, on="league_id", how="inner")
    tx = tx.with_columns(pl.col("roster_ids").map_elements(_ids, return_dtype=pl.List(pl.Int64)).alias("rosters"))
    trades = tx.select("transaction_id", pl.col("league_lineage_id").alias("lineage_id"), "league_id", "league_name", "season", "date",
                       pl.col("leg").fill_null(1).alias("leg"), "rosters", "creator")
    trades = trades.with_columns((pl.col("leg") > 1).alias("in_season"), pl.col("rosters").list.len().alias("n_teams"))

    tp = _both(TX_PLAYERS).unique(["transaction_id", "player_id", "roster_id", "action"]).filter(pl.col("transaction_id").is_in(trades["transaction_id"]))
    adds = tp.filter(pl.col("action") == "add").select("transaction_id", "player_id", pl.col("roster_id"),
                                                       pl.concat_str([pl.col("player_first_name"), pl.col("player_last_name")], separator=" ", ignore_nulls=True).alias("tp_name"),
                                                       pl.col("player_position").alias("tp_pos"))
    drops = tp.filter(pl.col("action") == "drop").select("transaction_id", "player_id", pl.col("roster_id").alias("from_roster"))
    players = adds.join(drops, on=["transaction_id", "player_id"], how="left").with_columns(pl.lit("player").alias("asset"))
    players = players.unique(["transaction_id", "player_id", "roster_id"])

    dp = _both(TX_PICKS).filter(pl.col("transaction_id").is_in(trades["transaction_id"]))
    # full_load (GraphQL dump) carries from/to with from_team_id = the NEW owner; the daily REST feed owner_id = new owner
    new_owner = pl.coalesce([pl.col("owner_id"), pl.col("from_team_id")]) if "owner_id" in dp.columns else pl.col("from_team_id")
    prev_owner = pl.coalesce([pl.col("previous_owner_id"), pl.col("to_team_id")]) if "previous_owner_id" in dp.columns else pl.col("to_team_id")
    picks = dp.select("transaction_id", new_owner.cast(pl.Int64).alias("roster_id"), prev_owner.cast(pl.Int64).alias("from_roster"),
                      pl.col("season").cast(pl.Int64).alias("pick_season"), pl.col("round").cast(pl.Int64).alias("pick_round"),
                      pl.col("roster_id").cast(pl.Int64).alias("pick_orig")).unique().with_columns(pl.lit("pick").alias("asset"))
    # FAAB dollars sent in a trade: an asset leg with no market price, so the side that paid in budget still shows what it gave
    faab_rows = [{"transaction_id": tid, "roster_id": recv, "from_roster": send, "faab": amt, "asset": "faab"}
                 for tid, wb in tx.select("transaction_id", "waiver_budget").iter_rows() for send, recv, amt in _faab(wb)]
    faab = pl.DataFrame(faab_rows, schema={"transaction_id": pl.Utf8, "roster_id": pl.Int64, "from_roster": pl.Int64, "faab": pl.Int64, "asset": pl.Utf8})
    legs = pl.concat([players, picks, faab], how="diagonal_relaxed").join(
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
    """Add ``drafted_player_id`` (Sleeper), ``draft_slot`` and ``draft_date`` to pick legs whose draft
    has happened: the lineage's linear draft of that season, the original roster's slot in the draft
    order, the player taken at (round, slot)."""
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
        when = d.get("last_picked") or d.get("start_time")
        ddate = date.fromtimestamp(when / 1000) if when else date(int(d["season"]), 5, 15)
        for user, slot in order.items():
            if slot is None:
                continue
            rows.append({"lineage_id": d["league_lineage_id"], "pick_season": int(d["season"]), "draft_id": d["draft_id"], "owner_id": str(user),
                         "draft_slot": int(slot), "draft_date": ddate})
    empty = legs.with_columns(pl.lit(None, pl.Utf8).alias("drafted_player_id"), pl.lit(None, pl.Int64).alias("draft_slot"), pl.lit(None, pl.Date).alias("draft_date"))
    if not rows:
        return empty
    slots = pl.DataFrame(rows).join(fr, on=["lineage_id", "owner_id"], how="left")
    dp = gcs_io.read_lake_prefix("bronze/sleeper/drafts/draft_picks").unique(["draft_id", "pick_no"]).select(
        "draft_id", pl.col("round").cast(pl.Int64).alias("pick_round"), pl.col("draft_slot").cast(pl.Int64), pl.col("player_id").alias("drafted_player_id"))
    res = (slots.select("lineage_id", "pick_season", "draft_id", pl.col("roster_id").alias("pick_orig"), "draft_slot", "draft_date")
           .join(dp, on=["draft_id", "draft_slot"], how="inner")
           .select("lineage_id", "pick_season", "pick_round", "pick_orig", "draft_slot", "draft_date", "drafted_player_id")
           .unique(["lineage_id", "pick_season", "pick_round", "pick_orig"]))
    return legs.join(res, on=["lineage_id", "pick_season", "pick_round", "pick_orig"], how="left")


# ------------------------------------------------------------------------------ KTC prices
PICK_SCHEMA = {"valuation_date": pl.Date, "pick_season": pl.Int64, "pick_round": pl.Int64, "tier": pl.Utf8, "ktc": pl.Float64}


def _full_load() -> pl.DataFrame | None:
    """KTC's per-asset daily history (players and the picks listed in 2025-10), cached locally by
    the owner from bronze/ktc/dynasty/full_load; None when the cache is absent."""
    if not FULL_LOAD_CACHE.exists():
        return None
    return pl.read_parquet(FULL_LOAD_CACHE)


def load_pick_prices(today: date | None = None) -> pl.DataFrame:
    """KTC pick tiers by date (valuation_date, pick_season, pick_round, tier, ktc) from every source in
    the lake: the silver fact, the bronze archive's pick rows (ids like "m,2023,1") and the per-asset
    history (slugs like "2026-mid-1st-1528"). The fact wins where sources overlap."""
    today = today or date.today()
    parts = []
    try:
        import io
        pv = pl.read_parquet(io.BytesIO(gcs_io._client().bucket(gcs_io.LAKE_BUCKET).blob("silver/fantasy/fact_pick_values").download_as_bytes()))
        pv = pv.filter((pl.col("source_system") == "ktc") & (pl.col("market_type") == "DYNASTY") & (pl.col("qb_format") == "SF") & (pl.col("te_premium") == "Standard"))
        parts.append(pv.select(pl.col("valuation_date").cast(pl.Utf8).str.slice(0, 10).str.to_date().alias("valuation_date"),
                               pl.col("season").cast(pl.Int64).alias("pick_season"), pl.col("round").cast(pl.Int64).alias("pick_round"),
                               pl.col("tier").cast(pl.Utf8), pl.col("value").cast(pl.Float64).alias("ktc")))
    except Exception:  # noqa: BLE001
        pass
    fl = _full_load()
    if fl is not None:
        pk = fl.filter(pl.col("slug").str.contains(r"^\d{4}-(early|mid|late)-\d(st|nd|rd|th)-"))
        parts.append(pk.select(pl.col("ranking_date").alias("valuation_date"),
                               pl.col("slug").str.extract(r"^(\d{4})-", 1).cast(pl.Int64).alias("pick_season"),
                               pl.col("slug").str.extract(r"-(\d)(st|nd|rd|th)-", 1).cast(pl.Int64).alias("pick_round"),
                               pl.col("slug").str.extract(r"^\d{4}-(early|mid|late)-", 1).replace_strict(TIERS, default=None).alias("tier"),
                               pl.col("sf_value").alias("ktc")))
    try:
        ar = gcs_io.read_lake_prefix("bronze/ktc/dynasty/local_load")
        ar = ar.filter(pl.col("sleeper_id").is_not_null() & pl.col("sleeper_id").cast(pl.Utf8).str.contains(r"^[eml],\d{4},\d$") & (pl.col("value") > 0))
        sp = pl.col("sleeper_id").cast(pl.Utf8).str.split(",")
        parts.append(ar.select(pl.col("date").cast(pl.Date).alias("valuation_date"), sp.list.get(1).cast(pl.Int64).alias("pick_season"), sp.list.get(2).cast(pl.Int64).alias("pick_round"),
                               sp.list.get(0).replace_strict(TIERS, default=None).alias("tier"), pl.col("value").cast(pl.Float64).alias("ktc")))
    except Exception:  # noqa: BLE001
        pass
    if not parts:
        return pl.DataFrame(schema=PICK_SCHEMA)
    out = pl.concat([x.select(list(PICK_SCHEMA)).cast(PICK_SCHEMA) for x in parts], how="vertical")
    return (out.filter(pl.col("tier").is_not_null() & (pl.col("ktc") > 0) & (pl.col("valuation_date") <= today))
            .unique(["valuation_date", "pick_season", "pick_round", "tier"], keep="first").sort("valuation_date"))


def slot_tier(slot: int | None, teams: int | None) -> str:
    """Early / Mid / Late by thirds of the draft order."""
    if slot is None or not teams:
        return "Mid"
    third = teams / 3.0
    return "Early" if slot <= third else ("Mid" if slot <= 2 * third else "Late")


def load_ktc_archive(today: date | None = None) -> pl.DataFrame:
    """KTC's historical dynasty (SF) player values from the bronze archive, 2020-04 to 2024-08, keyed
    by Sleeper id: the silver fact only carries today's ~430 listed players, so retired or dropped
    players (Elliott, Cook, Carr) are priced from here. Pick-tier rows (non-numeric ids, with
    dirty future dates) are dropped."""
    empty = pl.DataFrame({"pid": [], "valuation_date": [], "ktc": []}, schema={"pid": pl.Utf8, "valuation_date": pl.Date, "ktc": pl.Float64})
    try:
        df = gcs_io.read_lake_prefix("bronze/ktc/dynasty/local_load")
    except Exception:  # noqa: BLE001
        return empty
    today = today or date.today()
    return (df.filter(pl.col("sleeper_id").is_not_null() & pl.col("sleeper_id").cast(pl.Utf8).str.contains(r"^\d+$") & (pl.col("value") > 0))
            .select(pl.col("sleeper_id").cast(pl.Utf8).alias("pid"), pl.col("date").cast(pl.Date).alias("valuation_date"), pl.col("value").cast(pl.Float64).alias("ktc"))
            .filter(pl.col("valuation_date") <= today)
            .unique(["pid", "valuation_date"]))


def _ktc_to_sleeper(fl: pl.DataFrame | None = None) -> pl.DataFrame:
    """KTC id -> Sleeper id: nflverse fantasy_player_ids, then dim_players_master, then (for the
    per-asset history) the player's name and position against nflverse's player list."""
    client = gcs_io._client()
    names = sorted(b.name for b in client.list_blobs(gcs_io.LAKE_BUCKET, prefix="bronze/nflverse/fantasy_player_ids/") if b.name.endswith(".parquet"))
    ids = gcs_io.read_lake(names[-1])
    nv = ids.filter(pl.col("ktc_id").is_not_null() & pl.col("sleeper_id").is_not_null()).select(
        pl.col("ktc_id").cast(pl.Int64), pl.col("sleeper_id").cast(pl.Utf8).alias("pid"))
    pm = gcs_io.read_lake("silver/fantasy/dim_players_master/data.parquet").filter(pl.col("ktc_id").is_not_null()).select(
        pl.col("ktc_id").cast(pl.Int64), pl.col("player_key").cast(pl.Utf8).alias("pid"))
    out = pl.concat([nv, pm]).unique("ktc_id", keep="first")
    if fl is not None and "slug" in fl.columns:
        have = set(out["ktc_id"].to_list())
        rest = (fl.filter(~pl.col("ktc_id").is_in(list(have))).select("ktc_id", pl.col("slug").str.replace(r"-\d+$", "").str.replace_all("-", " ").alias("player_name"), "position")
                .with_columns(market.norm_name("player_name").alias("_n")).unique("ktc_id"))
        by_name = ids.filter(pl.col("sleeper_id").is_not_null()).select(market.norm_name("name").alias("_n"), "position", pl.col("sleeper_id").cast(pl.Utf8).alias("pid")).unique(["_n", "position"])
        more = rest.join(by_name, on=["_n", "position"], how="inner").select("ktc_id", "pid")
        out = pl.concat([out, more]).unique("ktc_id", keep="first")
    return out


def ktc_history(today: date | None = None) -> pl.DataFrame:
    """Player values by Sleeper id and date: the silver fact first, then KTC's per-asset history
    (2020-04 .. 2025-10, players listed in 2025-10), then the archive (to 2024-08, every player)."""
    hist = market.load_ktc_history()
    parts = [hist.select(pl.col("player_key").alias("pid"), "valuation_date", pl.col("ktc_value").cast(pl.Float64).alias("ktc"))]
    fl = _full_load()
    if fl is not None:
        players = fl.filter(~pl.col("slug").str.contains(r"^\d{4}-(early|mid|late)-") & (pl.col("sf_value") > 0)).join(_ktc_to_sleeper(fl), on="ktc_id", how="inner")
        parts.append(players.select("pid", pl.col("ranking_date").alias("valuation_date"), pl.col("sf_value").alias("ktc")))
    parts.append(load_ktc_archive(today))
    return pl.concat(parts).unique(["pid", "valuation_date"], keep="first")


def _asof(values: pl.DataFrame, keys: list[str], at: pl.DataFrame, tolerance_days: int = TOLERANCE_DAYS, out: str = "ktc") -> pl.DataFrame:
    """Latest value on or before ``at.at`` within the tolerance, joined on ``keys``."""
    v = values.sort("valuation_date")
    a = at.sort("at")
    j = a.join_asof(v.rename({"valuation_date": "_vd"}), left_on="at", right_on="_vd", by=keys, strategy="backward", tolerance=f"{tolerance_days}d")
    return j.rename({"ktc": out}).drop("_vd")


class SlotContext:
    """What the expected-slot pricing needs: the empirical tier table and prior, our leagues'
    week-by-week standings (crawl for past seasons, Sleeper's daily team_state for the season in
    progress), the week calendar, and (lineage, season) -> league id."""

    def __init__(self):
        rd = lambda n: pl.read_parquet(pick_slots.CACHE / n) if (pick_slots.CACHE / n).exists() else None   # noqa: E731
        self.table, self.prior = rd("pick_tier_table.parquet"), rd("pick_prior_table.parquet")
        self.slots, self.slot_prior_t = rd("pick_slot_table.parquet"), rd("pick_slot_prior.parquet")
        curve = rd("pick_slot_curve.parquet")
        # the market's slot curve: value of a slot relative to its round's mean (1.01 ~ 1.46, 1.12 ~ 0.80)
        self.curve = {(int(r["round"]), int(r["bin"])): float(r["rel"]) for r in curve.iter_rows(named=True)} if curve is not None else {}
        meta = gcs_io.read_lake("silver/fantasy/dim_leagues_meta/data.parquet").select("league_id", pl.col("season").cast(pl.Int64), "league_lineage_id", pl.col("total_rosters").cast(pl.Int64))
        self.league_of = {(r["league_lineage_id"], r["season"]): (r["league_id"], r["total_rosters"]) for r in meta.iter_rows(named=True)}
        ours = set(meta["league_id"].to_list())
        f = pick_slots.CACHE / "crawl_standings.parquet"
        self.hist = pl.read_parquet(f).filter(pl.col("league_id").is_in(list(ours))) if f.exists() else None
        try:
            self.current = pick_slots.current_standings()
        except Exception:  # noqa: BLE001
            self.current = None
        # week calendar: the last game date of each (season, week)
        w = gcs_io.read_lake("silver/fantasy/fact_player_week/data.parquet").group_by("season", "week").agg(pl.col("game_date").max().alias("week_end"))
        self.weeks = {(int(r["season"]), int(r["week"])): r["week_end"] for r in w.iter_rows(named=True) if r["week_end"] is not None}

    @staticmethod
    def season_of(d: date) -> int:
        return d.year if d.month >= 3 else d.year - 1

    def played_by(self, season: int, d: date) -> int:
        return sum(1 for (s, w), end in self.weeks.items() if s == season and end is not None and end < d)

    def rel(self, rnd: int, b: int) -> float:
        return self.curve.get((int(rnd), int(b)), 1.0)

    def slot_dist(self, lineage: str, pick_season: int, pick_orig: int, at: date) -> list[float] | None:
        """P(slot twelfth 1..12) for the original team's pick as of ``at``, from its standing (or last
        season's finish before week 1); None = no view."""
        if self.slots is None:
            return None
        row, prev_rank, teams = self._standing(lineage, pick_season, pick_orig, at)
        if row is not None:
            return pick_slots.slot_probs(self.slots, int(row["played"]), int(row["rank_now"]), int(row["pf_rank"]), int(row["teams"] or teams or 12))
        if prev_rank is not None and self.slot_prior_t is not None:
            return pick_slots.slot_prior_probs(self.slot_prior_t, prev_rank, teams or 12)
        return None

    def _standing(self, lineage: str, pick_season: int, pick_orig: int, at: date):
        """(standing row this season | None, last season's final rank | None, teams) for next year's pick; (None, None, None) otherwise."""
        s = self.season_of(at)
        if pick_season != s + 1:
            return None, None, None
        lid = self.league_of.get((lineage, s))
        if lid is None:
            return None, None, None
        league_id, teams = lid
        played = self.played_by(s, at)
        row = None
        if played >= 1:
            if self.hist is not None:
                h = self.hist.filter((pl.col("league_id") == league_id) & (pl.col("season") == s) & (pl.col("roster_id") == pick_orig) & (pl.col("played") <= played)).sort("played")
                if h.height:
                    row = h.row(-1, named=True)
            if row is None and self.current is not None and self.current.height:
                c = self.current.filter((pl.col("league_id") == league_id) & (pl.col("roster_id") == pick_orig) & (pl.col("as_of") <= at)).sort("as_of")
                if c.height and c.row(-1, named=True).get("played", 0) >= 1:
                    row = c.row(-1, named=True)
        prev_rank, prev_teams = None, None
        prev = self.league_of.get((lineage, s - 1))
        if prev is not None and self.hist is not None:
            h = self.hist.filter((pl.col("league_id") == prev[0]) & (pl.col("season") == s - 1) & (pl.col("roster_id") == pick_orig) & (pl.col("week") == pl.col("last_week")))
            if h.height:
                prev_rank, prev_teams = int(h["final_rank"][0]), int(h["teams"][0] or prev[1] or 12)
        return row, prev_rank, (int(row["teams"]) if row is not None and row.get("teams") else (prev_teams or teams))

    def probs(self, lineage: str, pick_season: int, pick_orig: int, at: date) -> tuple[float, float, float] | None:
        """P(Early, Mid, Late) for the original team's pick, as of ``at``; None = no view (Mid)."""
        if self.table is None:
            return None
        s = self.season_of(at)
        if pick_season != s + 1:
            return None                                   # two or more drafts out: the market prices the tier flat
        lid = self.league_of.get((lineage, s))
        if lid is None:
            return None
        league_id, teams = lid
        played = self.played_by(s, at)
        if played >= 1:
            row = None
            if self.hist is not None:
                h = self.hist.filter((pl.col("league_id") == league_id) & (pl.col("season") == s) & (pl.col("roster_id") == pick_orig) & (pl.col("played") <= played)).sort("played")
                if h.height:
                    row = h.row(-1, named=True)
            if row is None and self.current is not None and self.current.height:
                c = self.current.filter((pl.col("league_id") == league_id) & (pl.col("roster_id") == pick_orig) & (pl.col("as_of") <= at)).sort("as_of")
                if c.height:
                    row = c.row(-1, named=True)
            if row is not None and row.get("played", 0) >= 1:
                return pick_slots.tier_probs(self.table, int(row["played"]), int(row["rank_now"]), int(row["pf_rank"]), int(row["teams"] or teams or 12))
        # preseason: last season's finish as the prior
        if self.prior is not None and self.hist is not None:
            prev = self.league_of.get((lineage, s - 1))
            if prev is not None:
                h = self.hist.filter((pl.col("league_id") == prev[0]) & (pl.col("season") == s - 1) & (pl.col("roster_id") == pick_orig) & (pl.col("week") == pl.col("last_week")))
                if h.height:
                    return pick_slots.prior_probs(self.prior, int(h["final_rank"][0]), int(h["teams"][0] or prev[1] or 12))
        return None


def pick_value_at(pk: pl.DataFrame, pick_prices: pl.DataFrame, offsets: tuple[int, ...] = (0, 1, -1, 2, -2, 3)) -> pl.DataFrame:
    """``pk``: (_i, pick_season, pick_round, tier, at) -> (_i, v). The pick's own season first; where
    the lake has no price for it on that date, the same round and tier of another season at the same
    distance from its draft (the 2024 Mid 1st a year later stands in for the 2023 Mid 1st)."""
    got = pl.DataFrame({"_i": pl.Series([], dtype=pk["_i"].dtype), "v": pl.Series([], dtype=pl.Float64)})
    left = pk
    for k in offsets:
        if left.height == 0:
            break
        q = left.select("_i", (pl.col("pick_season") + k).alias("pick_season"), "pick_round", "tier",
                        (pl.col("at") + pl.duration(days=round(365.25 * k))).alias("at") if k else pl.col("at"))
        r = _asof(pick_prices, ["pick_season", "pick_round", "tier"], q, out="v").select("_i", "v")
        hit = r.filter(pl.col("v").is_not_null())
        got = pl.concat([got, hit.cast({"_i": got["_i"].dtype})])
        left = left.filter(~pl.col("_i").is_in(hit["_i"].implode()))
    if left.height:
        # not listed yet on that date (KTC adds a draft's picks ~2-3 years out): its first listed price, within a year
        v = pick_prices.sort("valuation_date")
        r = (left.select("_i", "pick_season", "pick_round", "tier", "at").sort("at")
             .join_asof(v.rename({"valuation_date": "_vd"}), left_on="at", right_on="_vd", by=["pick_season", "pick_round", "tier"], strategy="forward", tolerance="365d")
             .select("_i", pl.col("ktc").alias("v")))
        got = pl.concat([got, r.filter(pl.col("v").is_not_null()).cast({"_i": got["_i"].dtype})])
    return got


def value_at(legs: pl.DataFrame, at: pl.Series, hv: pl.DataFrame, pick_prices: pl.DataFrame | None, today: date | None = None,
             teams_by_league: dict[str, int] | None = None, slots: "SlotContext | None" = None) -> pl.Series:
    """Each leg's KTC value at its own date ``at``: null when the date is in the future, 0 when it has
    arrived but the asset has no price, a player by his id, a pick as a pick -- for next year's draft
    the tier its original team is likely to land in (P(Early / Mid / Late) from that team's record and
    scoring so far, last season's finish before week 1; ``slots``), the Mid tier for drafts further
    out, its actual slot's tier once the order is known (January of the draft year), and frozen at
    the last pre-draft price once the draft has happened."""
    today = today or date.today()
    L = legs.with_row_index("_i").with_columns(at.alias("at"))
    L = L.with_columns(pl.when(pl.col("asset") == "player").then(pl.col("player_id")).otherwise(None).alias("pid"))
    arrived = L.filter(pl.col("at") <= today)
    out = pl.DataFrame({"_i": L["_i"], "v": pl.Series([None] * L.height, dtype=pl.Float64)})
    pp = arrived.filter(pl.col("pid").is_not_null()).select("_i", "pid", "at")
    vals = []
    if pp.height:
        vals.append(_asof(hv, ["pid"], pp, out="v").select("_i", "v"))
    pk = arrived.filter(pl.col("asset") == "pick")
    if pk.height and pick_prices is not None and pick_prices.height:
        teams = teams_by_league or {}
        has_slot = "draft_slot" in pk.columns
        has_draft = "draft_date" in pk.columns
        # the price date: never past the draft (the pick stops existing); a generic pre-draft date when the draft date is unknown
        draft_day = pl.col("draft_date") if has_draft else pl.lit(None, pl.Date)
        generic = pl.date(pl.col("pick_season"), 5, 1)
        cutoff = pl.coalesce([draft_day, generic]) - pl.duration(days=1)
        pk = pk.with_columns(pl.min_horizontal(pl.col("at"), cutoff).alias("at_eff"))
        # the tier: Mid until the order is known, the actual slot's tier after
        known = pl.col("at_eff") >= pl.date(pl.col("pick_season"), 1, 1)
        rows = pk.select("_i", "pick_season", "pick_round", "league_id" if "league_id" in pk.columns else pl.lit(None, pl.Utf8).alias("league_id"),
                         pl.col("draft_slot") if has_slot else pl.lit(None, pl.Int64).alias("draft_slot"), known.alias("known"), pl.col("at_eff").alias("at"))
        tiers = [slot_tier(s, teams.get(lg)) if (k and s is not None) else "Mid" for lg, s, k in zip(rows["league_id"].to_list(), rows["draft_slot"].to_list(), rows["known"].to_list())]
        rows = rows.with_columns(pl.Series("tier", tiers))
        # expected tier for next year's draft from the original team's standing (one-hot where the slot is known)
        # the multiplier on the round's level: the slot's own curve value once the order is known, the expected
        # curve value over the team's slot distribution for next year's draft, 1 (the Mid price) further out
        mult, pe, pm, pl_ = [], [], [], []
        src = pk.select("_i", "lineage_id" if "lineage_id" in pk.columns else pl.lit(None, pl.Utf8).alias("lineage_id"),
                        "pick_orig" if "pick_orig" in pk.columns else pl.lit(None, pl.Int64).alias("pick_orig"), "pick_round",
                        "league_id" if "league_id" in pk.columns else pl.lit(None, pl.Utf8).alias("league_id"),
                        "draft_slot" if "draft_slot" in pk.columns else pl.lit(None, pl.Int64).alias("draft_slot")).join(rows.select("_i", "pick_season", "tier", "known", "at"), on="_i")
        for r in src.iter_rows(named=True):
            m, p = None, None
            if slots is not None and r["known"] and r["draft_slot"] is not None and teams.get(r["league_id"]):
                b = (int(r["draft_slot"]) - 1) * pick_slots.BINS // int(teams[r["league_id"]]) + 1
                m = slots.rel(r["pick_round"], b)
            elif slots is not None and not r["known"] and r["lineage_id"] is not None and r["pick_orig"] is not None:
                dist = slots.slot_dist(r["lineage_id"], int(r["pick_season"]), int(r["pick_orig"]), r["at"])
                if dist is not None:
                    m = sum(pb * slots.rel(r["pick_round"], b + 1) for b, pb in enumerate(dist))
                    p = (sum(dist[:4]), sum(dist[4:8]), sum(dist[8:]))
            if p is None:
                p = {"Early": (1.0, 0.0, 0.0), "Mid": (0.0, 1.0, 0.0), "Late": (0.0, 0.0, 1.0)}[r["tier"]] if m is None else p
            mult.append(m); pe.append(p[0] if p else None); pm.append(p[1] if p else None); pl_.append(p[2] if p else None)
        probs = src.select("_i").with_columns(pl.Series("mult", mult, dtype=pl.Float64), pl.Series("p_early", pe, dtype=pl.Float64), pl.Series("p_mid", pm, dtype=pl.Float64), pl.Series("p_late", pl_, dtype=pl.Float64))
        base = rows.select("_i", "pick_season", "pick_round", "at")
        by_tier = {tier: pick_value_at(base.with_columns(pl.lit(tier).alias("tier")), pick_prices).rename({"v": f"v_{tier}"}) for tier in ("Early", "Mid", "Late")}
        ex = probs
        for tier, d in by_tier.items():
            ex = ex.join(d, on="_i", how="left")
        ex = ex.with_columns(pl.coalesce([pl.col("v_Mid"), pl.col("v_Early"), pl.col("v_Late")]).alias("_any"))
        ex = ex.with_columns([pl.coalesce([pl.col(f"v_{t}"), pl.col("_any")]).alias(f"v_{t}") for t in ("Early", "Mid", "Late")])
        # the round's level that day: the mean of KTC's three tier prices; the value is level x multiplier where the
        # slot view exists, else the tier expectation (one-hot Mid / the known tier) as before
        ex = ex.with_columns(pl.mean_horizontal(["v_Early", "v_Mid", "v_Late"]).alias("level"))
        ex = ex.with_columns(pl.when(pl.col("mult").is_not_null()).then(pl.col("level") * pl.col("mult"))
                             .otherwise(pl.col("p_early") * pl.col("v_Early") + pl.col("p_mid") * pl.col("v_Mid") + pl.col("p_late") * pl.col("v_Late")).alias("v"))
        vals.append(ex.select("_i", "v").filter(pl.col("v").is_not_null()))
    if vals:
        got = pl.concat(vals).unique("_i")
        out = out.drop("v").join(got, on="_i", how="left")
    # arrived but unpriced -> 0; not arrived -> null
    out = out.join(arrived.select("_i", pl.lit(True).alias("_arr")), on="_i", how="left")
    return out.sort("_i").with_columns(pl.when(pl.col("_arr")).then(pl.col("v").fill_null(0.0)).otherwise(None).alias("v"))["v"]


def price_horizons(legs: pl.DataFrame, hv: pl.DataFrame | None = None, pick_prices: pl.DataFrame | None = None,
                   horizons: tuple[int, ...] = HORIZONS, today: date | None = None, teams_by_league: dict[str, int] | None = None,
                   slots: "SlotContext | None" = None) -> pl.DataFrame:
    """``v0`` (the trade date), ``v1`` .. (N+1, N+2 ... years later) and ``v_now`` per leg."""
    today = today or date.today()
    hv = ktc_history(today) if hv is None else hv
    if pick_prices is None:
        try:
            pick_prices = load_pick_prices(today)
        except Exception:  # noqa: BLE001
            pick_prices = None
    cols = []
    for m in horizons:
        at = legs.select((pl.col("date") + pl.duration(days=round(365.25 * m))).alias("at"))["at"] if m else legs["date"].alias("at")
        cols.append(value_at(legs, at, hv, pick_prices, today, teams_by_league, slots).alias(f"v{m}"))
    cols.append(value_at(legs, pl.Series("at", [today] * legs.height, dtype=pl.Date), hv, pick_prices, today, teams_by_league, slots).alias("v_now"))
    return legs.with_columns(cols)


# ------------------------------------------------------------------------------ KTC's combine
def pv(v: np.ndarray, vmax: np.ndarray | float) -> np.ndarray:
    """KTC's trade calculator (processVNew): an asset's contribution to a package, given the best
    asset in the trade (``vmax``). The curve is convex, so the best asset counts for more than its
    share and a package of lesser pieces has to overpay in raw value."""
    v = np.asarray(v, float)
    vmax = np.maximum(np.asarray(vmax, float), 1.0)
    return (0.1 * np.power(v / 10099.0, 1.4) + 0.7 * np.power(v / (1.05 * vmax), 1.25) + 0.2) * v


def ktc_combine(values, vmax: float | None = None) -> float:
    """A package's value under KTC's calculator; ``vmax`` defaults to the package's own best asset
    (in a trade, pass the best asset across both sides)."""
    v = np.asarray([x for x in values if x is not None], float)
    if v.size == 0:
        return 0.0
    return float(pv(v, vmax if vmax is not None else v.max()).sum())


# ------------------------------------------------------------------------------ realized wins (secondary)
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
    ``through``), ``pts_since``, ``games_since``. ``rep[lineage][season][pos]``; ``curves[lineage]``."""
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
def _vcols(legs: pl.DataFrame) -> list[str]:
    return [c for c in legs.columns if c == "v_now" or (c.startswith("v") and c[1:].isdigit())]


def reversal_pairs(legs: pl.DataFrame, days: int = 10) -> pl.DataFrame:
    """Transactions undone by a mirror trade between the same rosters within ``days``: each asset
    goes back where it came from. Returns (transaction_id, reversed_by)."""
    empty = pl.DataFrame(schema={"transaction_id": pl.Utf8, "reversed_by": pl.Utf8})
    if not {"lineage_id", "date"} <= set(legs.columns):
        return empty
    part = lambda c: pl.coalesce([pl.col(c).cast(pl.Utf8), pl.lit("")]) if c in legs.columns else pl.lit("")   # noqa: E731
    key = pl.concat_str([pl.col("asset"), part("player_id"), part("pick_season"), part("pick_round"), part("pick_orig"), part("faab")], separator="|")
    L = legs.with_columns(key.alias("_k"))
    sig = L.group_by("transaction_id").agg(pl.col("lineage_id").first(), pl.col("date").first(),
                                           pl.concat_str([pl.col("_k"), pl.col("from_roster").cast(pl.Utf8), pl.col("roster_id").cast(pl.Utf8)], separator=">").sort().str.join(";").alias("fwd"),
                                           pl.concat_str([pl.col("_k"), pl.col("roster_id").cast(pl.Utf8), pl.col("from_roster").cast(pl.Utf8)], separator=">").sort().str.join(";").alias("rev"))
    j = sig.join(sig.select(pl.col("transaction_id").alias("reversed_by"), pl.col("lineage_id"), pl.col("date").alias("date2"), pl.col("fwd").alias("fwd2")), on="lineage_id", how="inner")
    j = j.filter((pl.col("transaction_id") != pl.col("reversed_by")) & (pl.col("rev") == pl.col("fwd2")) & ((pl.col("date2") - pl.col("date")).dt.total_days().abs() <= days))
    return j.select("transaction_id", "reversed_by").unique("transaction_id")


def trade_table(trades: pl.DataFrame, legs: pl.DataFrame, fr: pl.DataFrame) -> pl.DataFrame:
    """One row per (trade, roster): the package received and given at every horizon, raw (``_sum``)
    and under KTC's combine (the trade's best asset at that horizon as the reference), the net
    (received minus given, combined), the wins delivered since, and the assets as text."""
    vcols = _vcols(legs)
    name = pl.when(pl.col("asset") == "pick").then(
        pl.format("{} R{} pick", pl.col("pick_season"), pl.col("pick_round")) + pl.when(pl.col("drafted_name").is_not_null()).then(pl.format(" ({})", pl.col("drafted_name"))).otherwise(pl.lit(""))
    ).when(pl.col("asset") == "faab").then(pl.format("${} FAAB", pl.col("faab")) if "faab" in legs.columns else pl.lit("FAAB")
    ).otherwise(pl.col("name"))
    L = legs.with_columns(name.alias("label"))
    # the trade's best asset at each horizon (both sides) is the combine's reference; a horizon is defined for a side only when every leg has arrived
    for c in vcols:
        L = L.with_columns(pl.col(c).max().over("transaction_id").alias(f"_max_{c}"))
        L = L.with_columns(pl.Series(f"_pv_{c}", pv(L[c].fill_null(0.0).to_numpy(), L[f"_max_{c}"].fill_null(1.0).to_numpy())))
        L = L.with_columns(pl.when(pl.col(c).is_null()).then(None).otherwise(pl.col(f"_pv_{c}")).alias(f"_pv_{c}"))

    def side(col: str) -> pl.DataFrame:
        aggs = [pl.len().alias("n"), (pl.col("asset") == "pick").sum().alias("picks"),
                pl.col("wins_since").sum().alias("wins") if "wins_since" in L.columns else pl.lit(0.0).alias("wins"),
                pl.col("label").str.join(" + ").alias("assets"),
                pl.col("label").filter(pl.col("v0") == 0).str.join(" + ").alias("unpriced_names") if "v0" in L.columns else pl.lit("").alias("unpriced_names")]
        for c in vcols:
            aggs += [pl.when(pl.col(c).is_null().any()).then(None).otherwise(pl.col(c).sum()).alias(f"{c}_sum"),
                     pl.when(pl.col(f"_pv_{c}").is_null().any()).then(None).otherwise(pl.col(f"_pv_{c}").sum()).alias(c),
                     ((pl.col(c) == 0) & pl.col(c).is_not_null()).sum().alias(f"{c}_unpriced")]
        return L.group_by(["transaction_id", col]).agg(aggs).rename({col: "roster_id"})

    keep = ["n", "picks", "wins", "assets", "unpriced_names"] + [f"{c}{s}" for c in vcols for s in ("", "_sum", "_unpriced")]
    recv = side("roster_id").rename({c: f"recv_{c}" for c in keep})
    give = side("from_roster").rename({c: f"give_{c}" for c in keep})
    t = recv.join(give, on=["transaction_id", "roster_id"], how="full", coalesce=True)
    t = t.join(trades.select("transaction_id", "lineage_id", "league_name", "season", "date", "leg", "in_season", "n_teams"), on="transaction_id", how="left")
    t = t.join(fr.select("lineage_id", "roster_id", "manager"), on=["lineage_id", "roster_id"], how="left")
    t = t.join(reversal_pairs(legs), on="transaction_id", how="left").with_columns(pl.col("reversed_by").is_not_null().alias("reversal"))
    t = t.with_columns([pl.col(c).fill_null(0.0) for c in ("recv_wins", "give_wins")] + [pl.col(c).fill_null(0) for c in ("recv_n", "give_n", "recv_picks", "give_picks")])
    nets = [(pl.col(f"recv_{c}") - pl.col(f"give_{c}")).alias(f"net_{c}") for c in vcols]
    nets += [(pl.col(f"recv_{c}_sum") - pl.col(f"give_{c}_sum")).alias(f"net_{c}_sum") for c in vcols]
    nets.append((pl.col("recv_wins") - pl.col("give_wins")).alias("net_wins"))
    # fairness at the time: the calculator's verdict as a share of the trade (+ = this side got the better of it)
    t = t.with_columns(nets)
    if "net_v0" in t.columns:
        t = t.with_columns((pl.col("net_v0") / (pl.col("recv_v0") + pl.col("give_v0")).clip(lower_bound=1.0)).alias("fair_v0"))
    return t.sort("date", "transaction_id")


def manager_table(tt: pl.DataFrame, min_seasons: float = 0.0) -> pl.DataFrame:
    """Per lineage and manager: trades, the calculator's verdict at the time, the package values at
    N+1 .. and today, win rates by value at each horizon, net wins delivered."""
    scored = tt.filter(pl.col("seasons_since") >= min_seasons) if "seasons_since" in tt.columns else tt
    if "reversal" in scored.columns:
        scored = scored.filter(~pl.col("reversal"))          # a trade undone within days is not a trade
    vcols = [c[4:] for c in tt.columns if c.startswith("net_v") and not c.endswith("_sum")]
    aggs = [pl.len().alias("trades"), pl.col("in_season").mean().alias("in_season_share"),
            pl.col("recv_wins").sum().alias("wins_in"), pl.col("give_wins").sum().alias("wins_out"), pl.col("net_wins").sum().alias("net_wins"),
            (pl.col("net_wins") > 0.05).mean().alias("wins_won"),
            pl.col("recv_picks").sum().cast(pl.Int64).alias("picks_in"), pl.col("give_picks").sum().cast(pl.Int64).alias("picks_out")]
    for c in vcols:
        aggs += [pl.col(f"recv_{c}").sum().alias(f"in_{c}"), pl.col(f"give_{c}").sum().alias(f"out_{c}"),
                 pl.col(f"net_{c}").sum().alias(f"net_{c}"), pl.col(f"net_{c}").mean().alias(f"net_{c}_per_trade"),
                 (pl.col(f"net_{c}") > 0).sum().alias(f"won_{c}"), pl.col(f"net_{c}").is_not_null().sum().alias(f"n_{c}")]
    out = scored.group_by(["lineage_id", "league_name", "manager"]).agg(aggs)
    out = out.with_columns([(pl.col(f"won_{c}") / pl.col(f"n_{c}").clip(lower_bound=1)).alias(f"won_{c}") for c in vcols] + [(pl.col("picks_in") - pl.col("picks_out")).alias("net_picks")])
    return out.sort(["lineage_id", "net_v0" if "net_v0" in out.columns else "net_wins"], descending=[False, True])


def build(through: date | None = None) -> dict[str, pl.DataFrame]:
    """Everything: trades, legs (priced at every horizon and scored), per-side trade table, manager table."""
    today = through or date.today()
    trades, legs = load_trades()
    legs = resolve_picks(legs)
    fr = franchises()
    xw = _crosswalk()
    legs = legs.join(xw.select("pid", "name", "pos"), left_on="player_id", right_on="pid", how="left")
    # every player leg gets a label: the crosswalk's name, else Sleeper's own, else "<TEAM> DEF" for a defense
    is_def = pl.col("player_id").is_not_null() & ~pl.col("player_id").str.contains(r"^\d+$")
    legs = legs.with_columns(
        pl.coalesce([pl.col("name"), pl.when(is_def).then(pl.col("player_id") + pl.lit(" DEF")).otherwise(None), pl.when(pl.col("tp_name") != "").then(pl.col("tp_name")).otherwise(None), pl.col("player_id")]).alias("name"),
        pl.coalesce([pl.col("pos"), pl.when(is_def).then(pl.lit("DEF")).otherwise(None), pl.col("tp_pos")]).alias("pos"))
    legs = legs.join(xw.select(pl.col("pid").alias("drafted_player_id"), pl.col("name").alias("drafted_name"), pl.col("pos").alias("drafted_pos")), on="drafted_player_id", how="left")
    meta = gcs_io.read_lake("silver/fantasy/dim_leagues_meta/data.parquet")
    teams_by_league = {r["league_id"]: int(r["total_rosters"]) for r in meta.select("league_id", "total_rosters").iter_rows(named=True) if r["total_rosters"]}
    try:
        slots = SlotContext()
    except Exception as e:  # noqa: BLE001
        print("expected-slot pricing unavailable:", str(e)[:120]); slots = None
    legs = price_horizons(legs, today=today, teams_by_league=teams_by_league, slots=slots)
    settings = gcs_io.read_lake(SETTINGS_PATH)
    weeks = gcs_io.read_lake("silver/fantasy/fact_player_week/data.parquet")
    season_df = gcs_io.read_lake("silver/fantasy/fact_player_season/data.parquet")
    seasons = sorted(set(int(s) for s in legs["season"].to_list()) | {int(legs["season"].max()) + 1})
    seasons = [s for s in seasons if s <= today.year]
    rep, curves = {}, {}
    for lin in trades["lineage_id"].unique().to_list():
        try:
            spec = lg.LeagueSpec.from_settings(settings, lin, name=lin)
        except Exception:  # noqa: BLE001
            spec = lg.LeagueSpec.from_settings(settings, None, name=lin)
        rep[lin] = replacement_by_season(spec, seasons, season_df, weeks)
        curves[lin] = lg.curve_for_lineage(lin)
    legs = realized_wins(legs, weeks, rep, curves, through=through)
    legs = legs.with_columns(((pl.lit(today) - pl.col("date")).dt.total_days() / 365.25).alias("seasons_since"))
    tt = trade_table(trades, legs, fr).join(legs.group_by("transaction_id").agg(pl.col("seasons_since").max()), on="transaction_id", how="left")
    mt = manager_table(tt)
    return {"trades": trades, "legs": legs, "trade_table": tt, "managers": mt, "replacement": pl.DataFrame(
        [{"lineage_id": lin, "season": s, **{p: v.get(p) for p in POS}} for lin, by in rep.items() for s, v in by.items()])}
