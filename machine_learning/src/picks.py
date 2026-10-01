"""Draft picks priced in wins (BACKLOG item 15).

A rookie pick is a claim on the player taken at that slot. Its value is built in two steps that
each have plenty of data:

1. **Wins by NFL draft capital.** Every drafted QB / RB / WR / TE (nflverse players table) of the
   classes with a full career window gets his realized WAR per season in the owner's league's
   units (weekly replacement line of that season, the league's win curve), summed over the first
   ``years`` seasons and discounted from the draft year like the page discounts a player. Players
   who never recorded a stat line count as zero, so the curve is the full-population expectation.
   A log-log line through it, E[WAR | pick] = exp(a + b·log pick) − c, is monotone and smooth.
2. **NFL capital by fantasy slot.** The leagues' own rookie drafts say which NFL picks actually go
   at each slot (1.01–1.03 take a median NFL pick of 6, the late first ~25, the second round ~60,
   the third ~95). The expected wins of a slot tier is the mean of the curve over the NFL picks
   that were taken there, which carries the uncertainty of who will be available.

A pick in draft year Y, valued in season S, is discounted a further (1 − rate)^(Y − S): its player
plays his first season in Y. KTC's pick tiers (Early / Mid / Late thirds of a round) are joined on
for wins per 1,000 KTC, the same yardstick the roster tab uses for players.
"""
from __future__ import annotations

import numpy as np
import polars as pl

POSITIONS = ["QB", "RB", "WR", "TE"]
TIERS = {"Early": (1, 3), "Mid": (4, 6), "Late": (7, 10)}      # thirds of a round on a 10-slot scale
FLOOR = 0.05                                                     # keeps the log-log fit defined at zero wins


def realized_war_by_player(season_fact: pl.DataFrame, rep: pl.DataFrame, drafted: pl.DataFrame, curve,
                           years: int = 10, rate: float = 0.2) -> pl.DataFrame:
    """Per drafted player: realized WAR over his first ``years`` seasons, undiscounted and discounted
    from the draft year. ``rep`` has (season, position, rep); ``drafted`` has
    (player_id, nfl_pick, nfl_class, position); ``curve`` is a league.WinCurve."""
    d = (season_fact.filter(pl.col("position").is_in(POSITIONS))
         .join(drafted.select("player_id", "nfl_class"), on="player_id", how="inner")
         .join(rep, on=["season", "position"], how="left")
         .filter((pl.col("season") >= pl.col("nfl_class")) & (pl.col("season") < pl.col("nfl_class") + years)))
    if d.height:
        excess = np.clip((d["ppg"] - d["rep"]).fill_null(0.0).to_numpy().astype(float), 0.0, None)
        war = d["games"].fill_null(0).to_numpy().astype(float) * curve.delta_win(np.full(d.height, curve.mean_points), excess)
        d = d.with_columns(pl.Series("war", war), (pl.col("season") - pl.col("nfl_class")).alias("k"))
        agg = d.group_by("player_id").agg(pl.col("war").sum().alias("war"), (pl.col("war") * (1 - rate) ** pl.col("k")).sum().alias("war_disc"))
    else:
        agg = pl.DataFrame({"player_id": [], "war": [], "war_disc": []})
    return drafted.join(agg, on="player_id", how="left").with_columns(pl.col("war").fill_null(0.0), pl.col("war_disc").fill_null(0.0))


def fit_pick_curve(df: pl.DataFrame, col: str = "war_disc") -> tuple[float, float]:
    """log(wins + FLOOR) = a + b·log(pick), least squares over every drafted player."""
    x = np.log(df["nfl_pick"].to_numpy().astype(float))
    y = np.log(df[col].to_numpy().astype(float) + FLOOR)
    b, a = np.polyfit(x, y, 1)
    return float(a), float(b)


def expected_war(nfl_pick, a: float, b: float) -> np.ndarray:
    p = np.asarray(nfl_pick, dtype=float)
    return np.clip(np.exp(a + b * np.log(p)) - FLOOR, 0.0, None)


def slot_tier(pick_no: pl.Expr, teams: pl.Expr) -> pl.Expr:
    """Round-relative slot on a 10-team scale -> Early / Mid / Late."""
    slot10 = (((pick_no - 1) % teams) / teams * 10).floor() + 1
    return (pl.when(slot10 <= TIERS["Early"][1]).then(pl.lit("Early"))
              .when(slot10 <= TIERS["Mid"][1]).then(pl.lit("Mid")).otherwise(pl.lit("Late")))


def nfl_picks_by_tier(rookie_picks: pl.DataFrame) -> pl.DataFrame:
    """From the leagues' rookie drafts (round, pick_no, teams, nfl_pick): the NFL picks taken in
    each (round, tier). Undrafted players (null nfl_pick) count as pick 260 (worth ~0)."""
    return (rookie_picks.with_columns(slot_tier(pl.col("pick_no"), pl.col("teams")).alias("tier"), pl.col("nfl_pick").fill_null(260))
            .group_by("round", "tier").agg(pl.col("nfl_pick").alias("nfl_picks"), pl.len().alias("n")))


def pick_table(by_tier: pl.DataFrame, a: float, b: float, seasons: list[int], now_season: int, rate: float = 0.2,
               prices: pl.DataFrame | None = None) -> pl.DataFrame:
    """Expected wins per (season, round, tier): the curve averaged over the tier's observed NFL picks,
    discounted (1 − rate)^(season − now). ``prices`` (season, round, tier, ktc) adds wins per 1,000 KTC."""
    rows = []
    for rnd, tier, picks, n in by_tier.select("round", "tier", "nfl_picks", "n").iter_rows():
        base = float(np.mean(expected_war(np.asarray(picks), a, b)))
        for s in seasons:
            disc = (1 - rate) ** max(s - now_season, 0)
            rows.append({"season": s, "round": int(rnd), "tier": tier, "n_slots": int(n), "wins_undiscounted": base, "wins": base * disc})
    out = pl.DataFrame(rows).sort("season", "round", "tier")
    if prices is not None and prices.height:
        out = out.join(prices.select("season", "round", "tier", "ktc"), on=["season", "round", "tier"], how="left")
        out = out.with_columns(pl.when(pl.col("ktc") > 0).then(pl.col("wins") / pl.col("ktc") * 1000).otherwise(None).alias("wins_per_1000"))
    return out


def owned_picks(traded: pl.DataFrame, roster_ids: list[int], seasons: list[int], rounds: list[int] = (1, 2, 3)) -> pl.DataFrame:
    """Every (season, round, original roster) pick with its current owner: each roster owns its own
    picks unless Sleeper's traded-pick state (season, round, original_roster_id, owner_roster_id)
    says otherwise."""
    base = pl.DataFrame({"season": [s for s in seasons for _ in rounds for _ in roster_ids],
                         "round": [r for _ in seasons for r in rounds for _ in roster_ids],
                         "original_roster_id": [rid for _ in seasons for _ in rounds for rid in roster_ids]})
    tr = traded.select(pl.col("season").cast(pl.Int64), pl.col("round").cast(pl.Int64), pl.col("original_roster_id").cast(pl.Int64),
                       pl.col("owner_roster_id").cast(pl.Int64)).unique(subset=["season", "round", "original_roster_id"], keep="last")
    return (base.with_columns(pl.col("season").cast(pl.Int64), pl.col("round").cast(pl.Int64), pl.col("original_roster_id").cast(pl.Int64))
            .join(tr, on=["season", "round", "original_roster_id"], how="left")
            .with_columns(pl.col("owner_roster_id").fill_null(pl.col("original_roster_id"))))


def projected_tier(strength_rank: int, n_teams: int) -> str:
    """Slot tier of a roster's own pick from its projected strength rank (1 = best lineup, picks last)."""
    slot = n_teams - strength_rank + 1
    slot10 = int((slot - 1) / n_teams * 10) + 1
    return "Early" if slot10 <= TIERS["Early"][1] else "Mid" if slot10 <= TIERS["Mid"][1] else "Late"


def build_from_lake(now_season: int, seasons: list[int], rate: float = 0.2, first_class: int = 2010, last_class: int = 2016,
                    years: int = 10) -> tuple[pl.DataFrame, dict]:
    """The whole chain from the lake: realized wins by NFL pick (classes with a full window), the
    leagues' rookie drafts for the slot -> NFL pick mapping, KTC's tier prices. Returns the pick table
    (undiscounted and discounted to ``now_season``) and the fitted curve."""
    import io

    import features
    import gcs_io
    import league as lg
    import lineup
    import replacement
    from feature_groups import Context

    client = gcs_io._client()
    spec = lg.LeagueSpec.from_settings(gcs_io.read_lake("silver/fantasy/dim_league_settings/data.parquet"))
    curve = lg.curve_for_lineage(replacement.PRIMARY_LINEAGE)
    sf = features.load_fact_player_season().filter(pl.col("position").is_in(POSITIONS))
    weeks = Context().weeks
    rep = pl.DataFrame([{"season": s, "position": q, "rep": v} for s in range(first_class, now_season)
                        for q, v in lineup.replacement_weekly(sf, weeks, spec, [s]).items()])
    blobs = sorted(b.name for b in client.list_blobs("nfl-data-bronze", prefix="bronze/nflverse/nfl_players/") if b.name.endswith(".parquet"))
    npl = gcs_io.read_lake(blobs[-1]).filter(pl.col("position").is_in(POSITIONS) & pl.col("draft_pick").is_not_null())
    drafted_all = npl.select(pl.col("gsis_id").alias("player_id"), pl.col("draft_pick").cast(pl.Int64).alias("nfl_pick"),
                             pl.col("draft_year").cast(pl.Int64).alias("nfl_class"), "position")
    per = realized_war_by_player(sf, rep, drafted_all.filter((pl.col("nfl_class") >= first_class) & (pl.col("nfl_class") <= last_class)), curve, years=years, rate=rate)
    a, b = fit_pick_curve(per)
    # the leagues' rookie drafts (linear = rookie; auction / snake = startups)
    dp = gcs_io.read_lake_prefix("bronze/sleeper/drafts/draft_picks").unique(subset=["draft_id", "pick_no"])
    dr = gcs_io.read_lake_prefix("bronze/sleeper/drafts/drafts").unique(subset=["draft_id"]).filter(pl.col("type") == "linear")
    ids = sorted(b.name for b in client.list_blobs("nfl-data-bronze", prefix="bronze/nflverse/fantasy_player_ids/") if b.name.endswith(".parquet"))
    xw = (gcs_io.read_lake(ids[-1]).select(pl.col("sleeper_id").cast(pl.Utf8), "gsis_id")
          .filter(pl.col("sleeper_id").is_not_null() & pl.col("gsis_id").is_not_null()).unique("sleeper_id"))
    rk = (dp.join(dr.select("draft_id", "teams"), on="draft_id", how="inner")
            .join(xw, left_on="player_id", right_on="sleeper_id", how="left")
            .join(drafted_all.select(pl.col("player_id").alias("gsis_id"), "nfl_pick"), on="gsis_id", how="left")
            .filter(pl.col("round") <= 3))
    by_tier = nfl_picks_by_tier(rk)
    pv = pl.read_parquet(io.BytesIO(client.bucket("nfl-data-bronze").blob("silver/fantasy/fact_pick_values").download_as_bytes()))
    ktc = pv.filter((pl.col("source_system") == "ktc") & (pl.col("market_type") == "DYNASTY") & (pl.col("qb_format") == "SF") & (pl.col("te_premium") == "Standard"))
    day = ktc["valuation_date"].max()
    prices = ktc.filter(pl.col("valuation_date") == day).select("season", "round", "tier", pl.col("value").alias("ktc"))
    tab = pick_table(by_tier, a, b, seasons, now_season, rate, prices)
    return tab, {"a": a, "b": b, "n_players": per.height, "classes": [first_class, last_class], "n_rookie_picks": rk.height, "ktc_date": str(day)}

