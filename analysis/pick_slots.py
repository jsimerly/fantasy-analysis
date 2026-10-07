"""Expected draft slot of a team's future pick, from its record so far (machine_learning/BACKLOG.md
item 25, picks). Analysis, not a model in the ML package.

The market prices a pick by its expected slot. KTC lists picks as Early / Mid / Late tiers until
the draft order is set, so a traded pick should be valued at the tier its original team is
*likely* to land in, given that team's record and scoring at the trade date, not at Mid for
everyone. This module builds, from the 10,000-league crawl (``bronze/sleeper_crawl/history``),
the empirical answer: P(final tier | wins so far, points-for rank so far, weeks played, league size).

* ``build_standings``: (league_id, season, week, roster_id) -> wins / losses / points for so far
  and the final regular-season rank, from weekly matchups (a roster's points vs its matchup
  opponent's), for every league-season with a complete regular season.
* ``tier_table``: P(Early / Mid / Late) by (teams, weeks played, wins so far, points-for rank bucket),
  plus the expected slot; a lookup the trade analysis uses at any date in the season.
* Offseason (before week 1): the previous season's final rank stands in (the market's prior).

Draft order in these leagues is reverse standings (worst record picks first); the crawl's own
draft order per league-season (``drafts`` + ``draft_picks``) is the ground truth where present,
the final regular-season rank otherwise.
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ML_SRC = Path(__file__).resolve().parents[1] / "machine_learning" / "src"
if str(ML_SRC) not in sys.path:
    sys.path.insert(0, str(ML_SRC))
import gcs_io  # noqa: E402

CACHE = Path(__file__).resolve().parent / "_cache"
PREFIX = "bronze/sleeper_crawl/history"


def _parts(entity: str) -> list[str]:
    c = gcs_io._client()
    return sorted(b.name for b in c.list_blobs(gcs_io.LAKE_BUCKET, prefix=f"{PREFIX}/{entity}/") if b.name.endswith(".parquet"))


def load_matchups(max_parts: int | None = None) -> pl.DataFrame:
    """Every (league, season, week, roster): points and matchup id, from the crawl."""
    cols = ["league_id", "season", "week", "roster_id", "matchup_id", "points"]
    parts = _parts("matchups")[: max_parts or None]
    out = []
    for i, n in enumerate(parts):
        try:
            out.append(gcs_io.read_lake(n).select([pl.col(c) for c in cols]).with_columns(pl.col("season").cast(pl.Int64), pl.col("week").cast(pl.Int64), pl.col("roster_id").cast(pl.Int64), pl.col("points").cast(pl.Float64)))
        except Exception as e:  # noqa: BLE001
            print("skip", n, str(e)[:80])
        if i % 20 == 0:
            print("matchups part", i, flush=True)
    return pl.concat(out, how="vertical_relaxed").unique(["league_id", "season", "week", "roster_id"])


def load_season_meta() -> pl.DataFrame:
    """Per league-season: teams, playoff week start (regular season = weeks before it)."""
    out = []
    for n in _parts("season_meta"):
        try:
            d = gcs_io.read_lake(n)
            keep = [c for c in ("league_id", "season", "total_rosters", "settings", "status", "league_lineage_id") if c in d.columns]
            out.append(d.select(keep))
        except Exception as e:  # noqa: BLE001
            print("skip", n, str(e)[:80])
    m = pl.concat(out, how="diagonal_relaxed").unique(["league_id", "season"])
    # playoff_week_start lives in the settings struct / json when present
    if "settings" in m.columns:
        s = m["settings"]
        if s.dtype == pl.Utf8:
            m = m.with_columns(pl.col("settings").str.json_path_match(r"$.playoff_week_start").cast(pl.Int64, strict=False).alias("playoff_week_start"))
        elif s.dtype == pl.Struct and "playoff_week_start" in s.struct.fields:
            m = m.with_columns(pl.col("settings").struct.field("playoff_week_start").cast(pl.Int64, strict=False).alias("playoff_week_start"))
    if "playoff_week_start" not in m.columns:
        m = m.with_columns(pl.lit(None, pl.Int64).alias("playoff_week_start"))
    return m.select("league_id", pl.col("season").cast(pl.Int64), pl.col("total_rosters").cast(pl.Int64).alias("teams"), "playoff_week_start")


def build_standings(matchups: pl.DataFrame, meta: pl.DataFrame) -> pl.DataFrame:
    """Cumulative record by week and the final regular-season rank per roster."""
    m = matchups.join(meta, on=["league_id", "season"], how="left")
    m = m.with_columns(pl.col("playoff_week_start").fill_null(15).alias("pws"), pl.col("teams").fill_null(pl.col("roster_id").max().over(["league_id", "season"])).alias("teams"))
    reg = m.filter((pl.col("week") >= 1) & (pl.col("week") < pl.col("pws")) & pl.col("matchup_id").is_not_null())
    # opponent's points: the other roster in the same matchup
    opp = reg.group_by(["league_id", "season", "week", "matchup_id"]).agg(pl.col("points").sum().alias("_tot"), pl.len().alias("_n"))
    reg = reg.join(opp, on=["league_id", "season", "week", "matchup_id"], how="left").filter(pl.col("_n") == 2)
    reg = reg.with_columns((pl.col("_tot") - pl.col("points")).alias("opp_points"))
    reg = reg.with_columns(pl.when(pl.col("points") > pl.col("opp_points")).then(1.0).when(pl.col("points") < pl.col("opp_points")).then(0.0).otherwise(0.5).alias("win"))
    reg = reg.sort(["league_id", "season", "roster_id", "week"])
    key = ["league_id", "season", "roster_id"]
    reg = reg.with_columns(pl.col("win").cum_sum().over(key).alias("wins"), pl.col("points").cum_sum().over(key).alias("pf"), pl.col("week").cum_count().over(key).alias("played"))
    # the season's regular-season length actually played (max week with games) and the final standing
    last = reg.group_by(["league_id", "season"]).agg(pl.col("week").max().alias("last_week"), pl.col("teams").first().alias("teams_"))
    fin = reg.join(last, on=["league_id", "season"], how="inner").filter(pl.col("week") == pl.col("last_week"))
    fin = fin.with_columns(pl.struct(["wins", "pf"]).rank(descending=True, method="ordinal").over(["league_id", "season"]).alias("final_rank"))
    reg = reg.join(fin.select(key + ["final_rank", "last_week"]), on=key, how="inner")
    # rank so far by (wins, points for) and the points-for rank so far
    reg = reg.with_columns(pl.struct(["wins", "pf"]).rank(descending=True, method="ordinal").over(["league_id", "season", "week"]).alias("rank_now"),
                           pl.col("pf").rank(descending=True, method="ordinal").over(["league_id", "season", "week"]).alias("pf_rank"))
    return reg.select("league_id", "season", "week", "roster_id", "teams", "played", "wins", "pf", "rank_now", "pf_rank", "final_rank", "last_week")


def tier_of(rank: pl.Expr, teams: pl.Expr) -> pl.Expr:
    """Draft slot = reverse standings: the worst team picks first. Early / Mid / Late by thirds."""
    slot = teams - rank + 1
    return pl.when(slot <= teams / 3.0).then(pl.lit("Early")).when(slot <= 2 * teams / 3.0).then(pl.lit("Mid")).otherwise(pl.lit("Late"))


def tier_table(st: pl.DataFrame) -> pl.DataFrame:
    """P(final tier) and the expected slot by (teams, played, rank_now bucket, pf_rank bucket), with
    the counts behind each cell. Buckets are rank percentiles in fifths so 10- and 12-team leagues share cells."""
    s = st.with_columns(tier_of(pl.col("final_rank"), pl.col("teams")).alias("final_tier"),
                        (pl.col("teams") - pl.col("final_rank") + 1).alias("final_slot"),
                        ((pl.col("rank_now") - 1) * 5 // pl.col("teams")).alias("rk5"), ((pl.col("pf_rank") - 1) * 5 // pl.col("teams")).alias("pf5"))
    g = s.group_by(["played", "rk5", "pf5"]).agg(pl.len().alias("n"), (pl.col("final_tier") == "Early").mean().alias("p_early"),
                                                 (pl.col("final_tier") == "Mid").mean().alias("p_mid"), (pl.col("final_tier") == "Late").mean().alias("p_late"),
                                                 (pl.col("final_slot") / pl.col("teams")).mean().alias("slot_pct_mean"))
    return g.sort(["played", "rk5", "pf5"])


def prior_table(st: pl.DataFrame) -> pl.DataFrame:
    """Before week 1 the market's prior is last season's finish: P(final tier this season | last
    season's final-rank fifth), over consecutive seasons of the same league lineage and roster id."""
    fin = st.filter(pl.col("week") == pl.col("last_week")).select("league_id", "season", "roster_id", "teams", "final_rank")
    lin = load_lineages()
    fin = fin.join(lin, on=["league_id", "season"], how="left").with_columns(pl.coalesce([pl.col("lineage_id"), pl.col("league_id")]).alias("lineage_id"))
    prev = fin.select("lineage_id", (pl.col("season") + 1).alias("season"), "roster_id", ((pl.col("final_rank") - 1) * 5 // pl.col("teams")).alias("prev5"))
    both = fin.join(prev, on=["lineage_id", "season", "roster_id"], how="inner")
    s = both.with_columns(tier_of(pl.col("final_rank"), pl.col("teams")).alias("final_tier"), (pl.col("teams") - pl.col("final_rank") + 1).alias("final_slot"))
    return (s.group_by("prev5").agg(pl.len().alias("n"), (pl.col("final_tier") == "Early").mean().alias("p_early"), (pl.col("final_tier") == "Mid").mean().alias("p_mid"),
                                   (pl.col("final_tier") == "Late").mean().alias("p_late"), (pl.col("final_slot") / pl.col("teams")).mean().alias("slot_pct_mean")).sort("prev5"))


def load_lineages() -> pl.DataFrame:
    """(league_id, season) -> lineage id, from the crawl's season meta (its own lineage key)."""
    out = []
    for n in _parts("season_meta"):
        try:
            d = gcs_io.read_lake(n)
            if "league_lineage_id" in d.columns:
                out.append(d.select("league_id", pl.col("season").cast(pl.Int64), pl.col("league_lineage_id").alias("lineage_id")))
        except Exception:  # noqa: BLE001
            pass
    return pl.concat(out).unique(["league_id", "season"]) if out else pl.DataFrame(schema={"league_id": pl.Utf8, "season": pl.Int64, "lineage_id": pl.Utf8})


def current_standings() -> pl.DataFrame:
    """The season in progress from Sleeper's daily team_state snapshots (not in the crawl yet):
    (league_id, season, as_of, roster_id, teams, played, wins, pf, rank_now, pf_rank)."""
    c = gcs_io._client()
    names = sorted(b.name for b in c.list_blobs(gcs_io.LAKE_BUCKET, prefix="bronze/sleeper/rosters/team_state/daily/") if b.name.endswith(".parquet"))
    out = []
    for n in names:
        try:
            d = gcs_io.read_lake(n)
            load_date = n.split("load_date=")[1][:10]
            out.append(d.select("league_id", pl.col("roster_id").cast(pl.Int64), pl.col("wins").cast(pl.Float64), pl.col("losses").cast(pl.Float64), pl.col("ties").cast(pl.Float64).fill_null(0.0),
                                (pl.col("fpts").cast(pl.Float64) + pl.col("fpts_decimal").cast(pl.Float64).fill_null(0.0) / 100.0).alias("pf")).with_columns(pl.lit(load_date).str.to_date().alias("as_of")))
        except Exception:  # noqa: BLE001
            pass
    if not out:
        return pl.DataFrame()
    ts = pl.concat(out, how="diagonal_relaxed")
    ts = ts.with_columns((pl.col("wins") + pl.col("ties") / 2).alias("wins"), (pl.col("wins") + pl.col("losses") + pl.col("ties")).cast(pl.Int64).alias("played"))
    ts = ts.with_columns(pl.col("roster_id").n_unique().over(["league_id", "as_of"]).alias("teams"),
                         pl.struct(["wins", "pf"]).rank(descending=True, method="ordinal").over(["league_id", "as_of"]).alias("rank_now"),
                         pl.col("pf").rank(descending=True, method="ordinal").over(["league_id", "as_of"]).alias("pf_rank"))
    return ts.select("league_id", "as_of", "roster_id", "teams", "played", "wins", "pf", "rank_now", "pf_rank")


def tier_probs(table: pl.DataFrame, played: int, rank_now: int, pf_rank: int, teams: int) -> tuple[float, float, float] | None:
    """P(Early, Mid, Late) from the empirical table; None when the cell is missing or thin."""
    rk5, pf5 = (rank_now - 1) * 5 // teams, (pf_rank - 1) * 5 // teams
    row = table.filter((pl.col("played") == played) & (pl.col("rk5") == rk5) & (pl.col("pf5") == pf5))
    if row.height == 0 or row["n"][0] < 30:
        row = table.filter((pl.col("played") == played) & (pl.col("rk5") == rk5))
        if row.height == 0:
            return None
        n = row["n"].sum()
        return (float((row["p_early"] * row["n"]).sum() / n), float((row["p_mid"] * row["n"]).sum() / n), float((row["p_late"] * row["n"]).sum() / n))
    return (float(row["p_early"][0]), float(row["p_mid"][0]), float(row["p_late"][0]))


def prior_probs(prior: pl.DataFrame, prev_rank: int, teams: int) -> tuple[float, float, float] | None:
    row = prior.filter(pl.col("prev5") == (prev_rank - 1) * 5 // teams)
    return (float(row["p_early"][0]), float(row["p_mid"][0]), float(row["p_late"][0])) if row.height else None


BINS = 12   # slot resolution: twelfths of the draft order (a 12-team league's slots; a 10-team league maps onto them)


def load_crawl_drafts() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Rookie drafts (linear, a few rounds) in the crawl and their picks: (draft_id, league_id, season, teams,
    draft_date, superflex) and (draft_id, round, draft_slot, player_id)."""
    dr_parts, dp_parts = [], []
    for n in _parts("drafts"):
        try:
            d = gcs_io.read_lake(n)
            keep = [c for c in ("draft_id", "league_id", "season", "type", "start_time", "settings") if c in d.columns]
            dr_parts.append(d.select(keep))
        except Exception as e:  # noqa: BLE001
            print("skip", n, str(e)[:60])
    for n in _parts("draft_picks"):
        try:
            d = gcs_io.read_lake(n)
            keep = [c for c in ("draft_id", "round", "draft_slot", "pick_no", "player_id", "roster_id") if c in d.columns]
            dp_parts.append(d.select(keep))
        except Exception as e:  # noqa: BLE001
            print("skip", n, str(e)[:60])
    dr = pl.concat(dr_parts, how="diagonal_relaxed").unique("draft_id")
    st = pl.col("settings").cast(pl.Utf8)      # Sleeper's draft settings JSON: rounds, teams, slots_super_flex ...
    dr = dr.with_columns(st.str.json_path_match(r"$.rounds").cast(pl.Int64, strict=False).alias("rounds"),
                         st.str.json_path_match(r"$.teams").cast(pl.Int64, strict=False).alias("teams"),
                         (st.str.json_path_match(r"$.slots_super_flex").cast(pl.Int64, strict=False) > 0).alias("superflex"),
                         pl.col("season").cast(pl.Int64), pl.col("start_time").cast(pl.Int64).alias("_ms"))
    dr = dr.filter((pl.col("type") == "linear") & (pl.col("rounds") <= 6) & pl.col("_ms").is_not_null())
    dr = dr.with_columns(pl.from_epoch(pl.col("_ms") // 1000, time_unit="s").dt.date().alias("draft_date"))
    dr = dr.select("draft_id", "league_id", "season", "teams", "draft_date", "superflex")
    dp = pl.concat(dp_parts, how="diagonal_relaxed").unique(["draft_id", "pick_no"]).select(
        "draft_id", pl.col("round").cast(pl.Int64), pl.col("draft_slot").cast(pl.Int64), pl.col("player_id").cast(pl.Utf8))
    return dr, dp


def slot_curve(hv: pl.DataFrame, seasons: tuple[int, ...] = (2021, 2022, 2023, 2024, 2025), days_after: int = 30, min_n: int = 25) -> pl.DataFrame:
    """The market's value of a draft slot: the median KTC value (superflex) of the player taken at each
    slot, ``days_after`` the draft, over the crawl's rookie drafts; by round and twelfth of the order.
    ``rel`` is that value relative to the round's mean across slots, so it can be applied to KTC's
    tier prices on any date (the shape from here, the level from the market that day)."""
    dr, dp = load_crawl_drafts()
    dr = dr.filter(pl.col("season").is_in(list(seasons)) & pl.col("draft_date").is_not_null() & pl.col("teams").is_between(8, 16))
    picks = dp.join(dr, on="draft_id", how="inner").filter(pl.col("round") <= 4)
    picks = picks.with_columns(((pl.col("draft_slot") - 1) * BINS // pl.col("teams") + 1).alias("bin"), (pl.col("draft_date") + pl.duration(days=days_after)).alias("at"))
    v = hv.sort("valuation_date")
    picks = picks.sort("at").join_asof(v.rename({"pid": "player_id", "valuation_date": "_vd"}), left_on="at", right_on="_vd", by="player_id", strategy="backward", tolerance="45d")
    picks = picks.filter(pl.col("ktc").is_not_null() & (pl.col("ktc") > 0))
    curve = picks.group_by(["round", "bin"]).agg(pl.len().alias("n"), pl.col("ktc").median().alias("value"), pl.col("ktc").mean().alias("value_mean"),
                                                 pl.col("superflex").mean().alias("sf_share")).sort(["round", "bin"])
    curve = curve.filter(pl.col("n") >= min_n)
    return curve.with_columns((pl.col("value") / pl.col("value").mean().over("round")).alias("rel"))


def slot_table(st: pl.DataFrame) -> pl.DataFrame:
    """P(final slot bin | weeks played, record fifth, points-for fifth): the full distribution the tier table summarises."""
    s = st.with_columns(((pl.col("teams") - pl.col("final_rank")) * BINS // pl.col("teams") + 1).alias("bin"),
                        ((pl.col("rank_now") - 1) * 5 // pl.col("teams")).alias("rk5"), ((pl.col("pf_rank") - 1) * 5 // pl.col("teams")).alias("pf5"))
    g = s.group_by(["played", "rk5", "pf5", "bin"]).agg(pl.len().alias("n"))
    return g.with_columns((pl.col("n") / pl.col("n").sum().over(["played", "rk5", "pf5"])).alias("p")).sort(["played", "rk5", "pf5", "bin"])


def slot_prior(st: pl.DataFrame) -> pl.DataFrame:
    """P(final slot bin | last season's final-rank fifth)."""
    fin = st.filter(pl.col("week") == pl.col("last_week")).select("league_id", "season", "roster_id", "teams", "final_rank")
    lin = load_lineages()
    fin = fin.join(lin, on=["league_id", "season"], how="left").with_columns(pl.coalesce([pl.col("lineage_id"), pl.col("league_id")]).alias("lineage_id"))
    prev = fin.select("lineage_id", (pl.col("season") + 1).alias("season"), "roster_id", ((pl.col("final_rank") - 1) * 5 // pl.col("teams")).alias("prev5"))
    both = fin.join(prev, on=["lineage_id", "season", "roster_id"], how="inner").with_columns(((pl.col("teams") - pl.col("final_rank")) * BINS // pl.col("teams") + 1).alias("bin"))
    g = both.group_by(["prev5", "bin"]).agg(pl.len().alias("n"))
    return g.with_columns((pl.col("n") / pl.col("n").sum().over("prev5")).alias("p")).sort(["prev5", "bin"])


def slot_probs(table: pl.DataFrame, played: int, rank_now: int, pf_rank: int, teams: int, min_n: int = 30) -> list[float] | None:
    """P(bin 1..BINS) from the slot table; the points-for dimension is dropped for thin cells."""
    rk5, pf5 = (rank_now - 1) * 5 // teams, (pf_rank - 1) * 5 // teams
    cell = table.filter((pl.col("played") == played) & (pl.col("rk5") == rk5) & (pl.col("pf5") == pf5))
    if cell.height == 0 or cell["n"].sum() < min_n:
        cell = table.filter((pl.col("played") == played) & (pl.col("rk5") == rk5)).group_by("bin").agg(pl.col("n").sum())
        if cell.height == 0:
            return None
        cell = cell.with_columns((pl.col("n") / pl.col("n").sum()).alias("p"))
    out = [0.0] * BINS
    for b, p_ in zip(cell["bin"].to_list(), cell["p"].to_list()):
        if 1 <= b <= BINS:
            out[b - 1] = float(p_)
    s = sum(out)
    return [x / s for x in out] if s > 0 else None


def slot_prior_probs(prior: pl.DataFrame, prev_rank: int, teams: int) -> list[float] | None:
    cell = prior.filter(pl.col("prev5") == (prev_rank - 1) * 5 // teams)
    if cell.height == 0:
        return None
    out = [0.0] * BINS
    for b, p_ in zip(cell["bin"].to_list(), cell["p"].to_list()):
        if 1 <= b <= BINS:
            out[b - 1] = float(p_)
    s = sum(out)
    return [x / s for x in out] if s > 0 else None


def build(max_parts: int | None = None, cache: bool = True) -> dict[str, pl.DataFrame]:
    CACHE.mkdir(exist_ok=True)
    f = CACHE / "crawl_standings.parquet"
    if cache and f.exists():
        st = pl.read_parquet(f)
    else:
        st = build_standings(load_matchups(max_parts), load_season_meta())
        if cache:
            st.write_parquet(f)
    return {"standings": st, "tiers": tier_table(st), "prior": prior_table(st), "slots": slot_table(st), "slot_prior": slot_prior(st)}


if __name__ == "__main__":
    R = build()
    st, tt = R["standings"], R["tiers"]
    print("standings rows", st.shape, "| league-seasons", st.select("league_id", "season").n_unique(), "| seasons", sorted(st["season"].unique().to_list()))
    with pl.Config(tbl_rows=30, tbl_width_chars=160):
        print(tt.filter(pl.col("played").is_in([0, 4, 8, 12])).sort(["played", "rk5", "pf5"]).head(30))
    tt.write_parquet(CACHE / "pick_tier_table.parquet")
    R["prior"].write_parquet(CACHE / "pick_prior_table.parquet")
    R["slots"].write_parquet(CACHE / "pick_slot_table.parquet")
    R["slot_prior"].write_parquet(CACHE / "pick_slot_prior.parquet")
    with pl.Config(tbl_rows=10):
        print(R["prior"])
    # the market's slot curve needs the KTC player history: built by trades.ktc_history()
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import trades
    curve = slot_curve(trades.ktc_history())
    curve.write_parquet(CACHE / "pick_slot_curve.parquet")
    with pl.Config(tbl_rows=60):
        print(curve)
