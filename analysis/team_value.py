"""Team value over time: every franchise's roster value week by week, in KTC's own power-ranking
terms, with the league calendar laid under it. ``02_team_value_over_time.ipynb`` draws it; this
module builds the series (the data prep that used to live in the notebook) and publishes the
summary the page's Team value tab reads.

Measures per (lineage, franchise, week), all on the SF / TE-premium lens (Standard before the TEP
era, ``fantasy_lib.load_player_values_blend``), picks at the round level:

  power_index    KTC's /power-rankings/teams scale: the prProcessV depth-adjusted sum, picks
                 included, the top team that week = 99 (matches the live page to +-1)
  power_players  the same without picks (KTC's "exclude picks" view)
  adj_total      the raw depth-adjusted sum: absolute strength, so league-wide events show
  rel_mean       adj_total as a share of the league's average team that week (100 = average)
  value          the plain sum of KTC player + pick values (the league-overview number)
  fc_value       FantasyCalc players + picks, its own history (2025-10 on)

Calendar: NFL week 1 -> the lineage's fantasy_end is the regular season; startup and rookie draft
days are guides. Each lineage starts at its first week holding >= 95 % of its steady-state asset
count (the first week of a startup is a partial roster and would read as a fake jump).

Run:  .venv/Scripts/python analysis/team_value.py --out analysis/_cache/team_value [--publish]
      --publish writes backtests/team_value/run_date=<today>/summary.json to the ML bucket, which
      machine_learning/scripts/export_projections.py folds into the page export.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "machine_learning" / "src"))

import polars as pl  # noqa: E402

import fantasy_lib as F  # noqa: E402

LEAGUE_NAMES = {   # fallback when dim_leagues_meta is unreachable
    "730630605066371072": "Stuck in High School",
    "1131624152349323264": "Football Guys of Indianapolis",
    "1061511485920354304": "Sigma Chi Dynasty League",
}
METRICS = {"power_index": "Power index (1-99, picks included)", "power_players": "Power index, players only",
           "rel_mean": "Value vs league average (100 = average team)", "adj_total": "Adjusted value (depth-weighted sum)",
           "value": "Roster value (plain KTC sum, picks included)"}
RAMP_SHARE = 0.95
FC_START = "2025-10-13"
# 16 well-spread, line-visible colors (Trubetskoy's distinct set, lightest dropped)
PALETTE = ["#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4", "#42d4f4", "#f032e6", "#bfef45",
           "#469990", "#9a6324", "#800000", "#808000", "#000075", "#a9a9a9", "#d81b60", "#5d3a9b"]


# ------------------------------------------------------------------------------ pieces
def weekly_grid(ledger: pl.DataFrame, player_values: pl.DataFrame) -> list[str]:
    """Weekly ISO dates from the later of the ledger's and the values' start to the latest valuation
    date, which is appended so the series ends on today's value."""
    start = max(str(ledger["valid_from"].min())[:10], player_values["valuation_date"].min().isoformat())
    today = player_values["valuation_date"].max().isoformat()
    return sorted(set(F.weekly_dates(start, today) + [today]))


def startup_cutoffs(ledger: pl.DataFrame, dates: list[str], fr_meta: pl.DataFrame, share: float = RAMP_SHARE) -> pl.DataFrame:
    """(league_lineage_id, start: Date): the first grid date on which the lineage holds at least ``share``
    of its median asset count. The first week of a startup draft holds a partial roster and the
    picks are minted about a week later, so the week-1 -> week-2 jump is the roster filling, not value."""
    lin = fr_meta.select("franchise_id", "league_lineage_id").unique(subset=["franchise_id"])
    counts = (F._holdings_by_date(ledger, dates).join(lin, on="franchise_id", how="left")
               .group_by("league_lineage_id", "date").agg(pl.len().alias("n")))
    med = counts.group_by("league_lineage_id").agg(pl.col("n").median().alias("med"))
    return (counts.join(med, on="league_lineage_id").filter(pl.col("n") >= share * pl.col("med"))
                  .group_by("league_lineage_id").agg(pl.col("date").min().alias("start")))


def trim_startup_ramp(frame: pl.DataFrame, cutoffs: pl.DataFrame) -> pl.DataFrame:
    """Drop each lineage's rows before its cutoff (frames keyed by league_lineage_id and a Date ``date``)."""
    return (frame.join(cutoffs, on="league_lineage_id", how="left")
                 .filter(pl.col("start").is_null() | (pl.col("date") >= pl.col("start"))).drop("start"))


def build_power(ledger: pl.DataFrame, dates: list[str], player_values: pl.DataFrame, pick_values: pl.DataFrame,
                fr_meta: pl.DataFrame) -> pl.DataFrame:
    """KTC's power ranking with picks (power_index, adj_total), without (power_players), and the
    share of the league's average team (rel_mean), per (franchise, week), with names and owners."""
    meta = fr_meta.select("franchise_id", "league_lineage_id", "current_team_name", "owner_id").unique(subset=["franchise_id"])
    with_picks = F.team_power_index(ledger, dates, player_values, meta, pick_values=pick_values)
    players_only = (F.team_power_index(ledger, dates, player_values, meta)
                     .select("franchise_id", "date", pl.col("power_index").alias("power_players")))
    out = (with_picks.join(players_only, on=["franchise_id", "date"], how="left")
                     .join(meta.select("franchise_id", "current_team_name", "owner_id"), on="franchise_id", how="left"))
    return out.with_columns((100 * pl.col("adj_total") / pl.col("adj_total").mean().over("league_lineage_id", "date")).alias("rel_mean"))


def build_value(ledger: pl.DataFrame, dates: list[str], player_values: pl.DataFrame, pick_values: pl.DataFrame, source: str = "ktc") -> pl.DataFrame:
    """The plain sum of held player and pick values per (franchise, week) -> (franchise_id, date, value)."""
    tv = F.team_value_timeseries(ledger, source, dates, player_values=player_values, pick_values=pick_values)
    return tv.select("franchise_id", "date", pl.col("total_value").alias("value"))


def season_bands(events: pl.DataFrame, season_starts: pl.DataFrame) -> pl.DataFrame:
    """(league_lineage_id, season, season_start, season_end): NFL week 1 to the lineage's fantasy end."""
    return (events.filter(pl.col("event_type") == "fantasy_end").join(season_starts, on="season", how="inner")
                  .select("league_lineage_id", "season", "season_start", pl.col("event_date").alias("season_end")).sort("season_start"))


def draft_days(events: pl.DataFrame) -> pl.DataFrame:
    """(league_lineage_id, season, event_type, event_date) for the startup and rookie drafts."""
    return events.filter(pl.col("event_type").is_in(["startup_draft", "rookie_draft"])).sort("event_date")


def _ohash(owner_id) -> int:
    return int(hashlib.md5(str(owner_id).encode()).hexdigest(), 16)


def owner_colors(owner_ids) -> dict:
    """Each owner's line color: the hash picks a palette slot, a collision bumps to the nearest free
    one. Unique within a league; the same manager usually keeps a color across leagues."""
    n = len(PALETTE)
    taken, out = {}, {}
    for o in sorted(set(owner_ids), key=_ohash):
        p = _ohash(o) % n
        if p in taken:
            for d in range(1, n):
                if (p + d) % n not in taken:
                    p = (p + d) % n
                    break
                if (p - d) % n not in taken:
                    p = (p - d) % n
                    break
        taken[p] = o
        out[o] = PALETTE[p]
    return out


# ------------------------------------------------------------------------------ build
def league_names() -> dict:
    """lineage id -> the lineage's current league name, from dim_leagues_meta when it is reachable."""
    try:
        lm = pl.read_parquet(f"gs://{F.BUCKET}/silver/fantasy/dim_leagues_meta/data.parquet")
    except Exception:  # noqa: BLE001
        return dict(LEAGUE_NAMES)
    name_col = next((c for c in ("league_name", "display_name", "name") if c in lm.columns), None)
    if name_col is None or "league_lineage_id" not in lm.columns:
        return dict(LEAGUE_NAMES)
    order = "season" if "season" in lm.columns else name_col
    latest = lm.sort(order).group_by("league_lineage_id").agg(pl.col(name_col).last())
    return {**LEAGUE_NAMES, **{str(k): str(v) for k, v in zip(latest["league_lineage_id"], latest[name_col]) if v}}


def build() -> dict:
    """Everything the summary needs, from the lake."""
    ledger = F.load_ledger()
    franchises, _ = F.load_dims()
    fr_meta = franchises.select("franchise_id", "league_lineage_id", "current_team_name", "roster_id", "owner_id")
    player_values = F.load_player_values_blend(qb_format="SF")
    pick_values = F.load_pick_values_round("ktc", "SF", "Standard")
    dates = weekly_grid(ledger, player_values)
    cut = startup_cutoffs(ledger, dates, fr_meta)
    power = trim_startup_ramp(build_power(ledger, dates, player_values, pick_values, fr_meta), cut)
    value = build_value(ledger, dates, player_values, pick_values)
    power = power.join(value, on=["franchise_id", "date"], how="left")
    events = F.load_league_events()
    bands = season_bands(events, F.load_nfl_season_starts())
    fc_dates = sorted(set(F.weekly_dates(FC_START, dates[-1]) + [dates[-1]]))
    fc = (build_value(ledger, fc_dates, F.load_player_values("SF", "Standard"), F.load_pick_values_round("fc"), source="fc")
          .join(fr_meta.select("franchise_id", "league_lineage_id").unique(subset=["franchise_id"]), on="franchise_id", how="left"))
    return {"power": power, "fc": fc, "events": events, "bands": bands, "drafts": draft_days(events), "fr_meta": fr_meta,
            "dates": dates, "names": league_names()}


# ------------------------------------------------------------------------------ summary
def _series(frame: pl.DataFrame, dates: list, col: str, ndigits: int | None) -> dict:
    """team id -> one value per grid date (null where the team has no row)."""
    idx = {d: i for i, d in enumerate(dates)}
    out = {}
    for fid, sub in frame.group_by("franchise_id", maintain_order=True):
        fid = fid[0] if isinstance(fid, tuple) else fid
        arr = [None] * len(dates)
        for d, v in zip(sub["date"].to_list(), sub[col].to_list()):
            if d in idx and v is not None:
                arr[idx[d]] = round(float(v), ndigits) if ndigits else int(round(float(v)))
        out[str(fid)] = arr
    return out


def _change(arr: list, dates: list, back_days: int):
    """Latest value minus the value at the latest grid date at least ``back_days`` earlier."""
    now_i = max((i for i, v in enumerate(arr) if v is not None), default=None)
    if now_i is None:
        return None
    target = dates[now_i].toordinal() - back_days
    j = max((i for i in range(now_i) if dates[i].toordinal() <= target and arr[i] is not None), default=None)
    return None if j is None else round(arr[now_i] - arr[j], 1)


def summary(data: dict) -> dict:
    power, fc, bands, drafts, fr_meta, names = data["power"], data["fc"], data["bands"], data["drafts"], data["fr_meta"], data["names"]
    meta = fr_meta.select("franchise_id", "league_lineage_id", "current_team_name", "owner_id").unique(subset=["franchise_id"])
    leagues = []
    for lid in sorted(power["league_lineage_id"].unique().to_list(), key=lambda x: names.get(str(x), str(x))):
        p = power.filter(pl.col("league_lineage_id") == lid).sort(["franchise_id", "date"])
        dates = sorted(p["date"].unique().to_list())
        teams_meta = meta.filter(pl.col("league_lineage_id") == lid)
        colors = owner_colors(teams_meta["owner_id"].to_list())
        series = {m: _series(p, dates, m, None if m in ("power_index", "power_players", "adj_total", "value") else 1) for m in METRICS}
        latest_draft = max((d for d in drafts.filter(pl.col("league_lineage_id") == lid)["event_date"].to_list() if d <= dates[-1]), default=None)
        now = []
        for fid, name, owner in teams_meta.select("franchise_id", "current_team_name", "owner_id").iter_rows():
            arr = series["power_index"].get(str(fid))
            if not arr or all(v is None for v in arr):
                continue
            cur = next(v for v in reversed(arr) if v is not None)
            vals = [v for v in arr if v is not None]
            peak_i = max(range(len(arr)), key=lambda i: (arr[i] if arr[i] is not None else -1))
            since_draft = None
            if latest_draft is not None:
                # from the last value before the draft; a startup draft has none, so from the first value after it
                k = max((i for i, d in enumerate(dates) if d <= latest_draft and arr[i] is not None), default=None)
                if k is None:
                    k = min((i for i, d in enumerate(dates) if d > latest_draft and arr[i] is not None), default=None)
                since_draft = None if k is None else round(cur - arr[k], 1)
            now.append({"id": str(fid), "name": name, "owner": str(owner), "color": colors.get(owner, "#777"), "power_index": cur,
                        "rel_mean": next((v for v in reversed(series["rel_mean"].get(str(fid), [])) if v is not None), None),
                        "value": next((v for v in reversed(series["value"].get(str(fid), [])) if v is not None), None),
                        "d4w": _change(arr, dates, 28), "d13w": _change(arr, dates, 91), "d52w": _change(arr, dates, 364), "since_draft": since_draft,
                        "peak": max(vals), "peak_date": dates[peak_i].isoformat(), "trough": min(vals)})
        now.sort(key=lambda r: -r["power_index"])
        for i, r in enumerate(now):
            r["rank"] = i + 1
        f = fc.filter(pl.col("league_lineage_id") == lid).sort(["franchise_id", "date"]) if fc.height else fc
        fc_dates = sorted(f["date"].unique().to_list()) if f.height else []
        leagues.append({
            "id": str(lid), "name": names.get(str(lid), str(lid)), "dates": [d.isoformat() for d in dates],
            "teams": [{"id": r["id"], "name": r["name"], "owner": r["owner"], "color": r["color"]} for r in now],
            "series": series,
            "fc": {"dates": [d.isoformat() for d in fc_dates], "series": _series(f, fc_dates, "value", None)} if fc_dates else None,
            "bands": [[s.isoformat(), e.isoformat(), int(season)] for _, season, s, e in bands.filter(pl.col("league_lineage_id") == lid).iter_rows()],
            "drafts": [[d.isoformat(), t, int(season)] for _, season, t, d in drafts.filter(pl.col("league_lineage_id") == lid).iter_rows()],
            "latest_draft": latest_draft.isoformat() if latest_draft is not None else None,
            "now": now,
        })
    return {"leagues": leagues, "metrics": METRICS,
            "meta": {"run_date": datetime.now(timezone.utc).date().isoformat(), "through": data["dates"][-1], "weeks": len(data["dates"]),
                     "lens": "KTC dynasty, superflex, TE premium from 2025-10 (Standard before); picks at the round level (KTC Mid tier)",
                     "fc_from": FC_START, "ramp_share": RAMP_SHARE}}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--publish", action="store_true")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    data = build()
    data["power"].write_parquet(out / "power.parquet")
    data["fc"].write_parquet(out / "fc.parquet")
    s = summary(data)
    (out / "summary.json").write_text(json.dumps(s, indent=1, default=str), encoding="utf-8")
    for lg in s["leagues"]:
        top = ", ".join(f"{r['name']} {r['power_index']}" for r in lg["now"][:3])
        print(f"{lg['name']}: {len(lg['dates'])} weeks {lg['dates'][0]} -> {lg['dates'][-1]}, {len(lg['teams'])} teams; top now: {top}")
    print(f"wrote {out}")
    if args.publish:
        import gcs_io  # noqa: E402
        print("published", gcs_io.write_ml_json(s, "backtests", "team_value", f"run_date={s['meta']['run_date']}", "summary.json"))


if __name__ == "__main__":
    main()
