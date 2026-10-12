"""Does the career projection improve when it sees the current season? (BACKLOG 51)

Production projects the career seasons (two and more seasons out) from the last COMPLETE season's row, so a player's
current season never reaches them. This scores, walk-forward on past seasons T, the seasons T+2 and T+3 projected at
week W of season T four ways, on the same players:

  last_season   the production path: the model trained on season rows (as of T-1), projecting from the T-1 row
                (seasons T+2, T+3 = horizons 3, 4)
  todate_plain  the same model projecting from the season-to-date row at week W (horizons 2, 3; it never saw a partial row)
  todate        a model trained on season rows AND season-to-date rows at the training weeks (``row_week`` = W tells it how
                much of the season is in), projecting from the week-W row
  season_end    the reference: that model from the complete season-T row (what the offseason sees)

Players: a played T-1 season and a row at week W of T (rookies are scored apart on the todate model alone). Value:
PAR = max(ppg - replacement, 0) x games per season, replacement as of T, summed over T+2 and T+3. Scores per (T, W):
ordering (Spearman) on the players either path puts in its top 150, PAR error, bias, and realized / priced by age.

    python scripts/seam_eval.py --backend xgb --device cpu --name seam_trees
    python scripts/seam_eval.py --backend tabpfn --device cuda --name seam_35
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import career  # noqa: E402
import feature_groups as fg  # noqa: E402
import unified  # noqa: E402
import value  # noqa: E402
from run_experiment import build_context  # noqa: E402

H = [1, 2, 3, 4]
SET = "base,career,injury,trend,situation,rookie,college"
# groups that describe one player (not one season): valid on a mid-season row as they are on a season row
STATIC_GROUPS = {"rookie", "college", "college2", "college_qb", "draft_age", "adp_this"}
AGE_BANDS = (("<=24", 0.0, 24.99), ("25-28", 25.0, 28.99), ("29+", 29.0, 99.0))


def mask_season_groups(df: pl.DataFrame, groups) -> pl.DataFrame:
    """Season-level group columns (injury weeks, the second-half trend, team changes, next season's ADP) describe a whole
    season: null on the to-date rows, which have not seen it."""
    cols = [c for g in groups if g.name not in STATIC_GROUPS for c in g.columns if c not in career.FEATURES and c in df.columns]
    snap = pl.col("is_snapshot_row").cast(pl.Float64, strict=False).fill_null(0.0) > 0.5
    return df.with_columns([pl.when(snap).then(None).otherwise(pl.col(c)).alias(c) for c in cols]) if cols else df


def par(df: pl.DataFrame, rep: dict, ks: list[int], hat: bool) -> np.ndarray:
    """Sum over the horizons ks of max(ppg - rep, 0) x games, projected (hat) or realized."""
    r = df["position"].replace_strict(rep, default=0.0, return_dtype=pl.Float64).to_numpy()
    tot = np.zeros(df.height)
    for k in ks:
        if hat:
            p, g = df[f"h{k}_ppg_hat"].fill_null(0.0).to_numpy(), df[f"h{k}_games_hat"].fill_null(0.0).to_numpy()
        else:
            p, g = df[f"h{k}_ppg"].fill_null(0.0).to_numpy(), df[f"h{k}_games"].fill_null(0).to_numpy().astype(float)
        tot += np.maximum(p - r, 0.0) * g
    return tot


def score(proj: dict[str, np.ndarray], real: np.ndarray, age: np.ndarray, top_n: int = 150) -> list[dict]:
    """Per variant: ordering and PAR error on the union of every variant's top ``top_n`` (one player set for all),
    the bias, and realized / priced by age band on the same set."""
    union = np.zeros(len(real), dtype=bool)
    for p in proj.values():
        union[np.argsort(-p)[:top_n]] = True
    out = []
    for name, p in proj.items():
        pp, rr, aa = p[union], real[union], age[union]
        row = {"variant": name, "n": int(union.sum()), "spearman": value.spearman(pp, rr), "mae": float(np.mean(np.abs(rr - pp))),
               "bias": float(np.mean(rr - pp)), "ratio": float(rr.sum() / pp.sum()) if pp.sum() else None}
        for band, lo, hi in AGE_BANDS:
            m = (aa >= lo) & (aa <= hi)
            row[f"ratio_{band}"] = float(rr[m].sum() / pp[m].sum()) if m.any() and pp[m].sum() else None
            row[f"spearman_{band}"] = value.spearman(pp[m], rr[m]) if m.sum() >= 20 else None
        out.append(row)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--name", required=True)
    ap.add_argument("--backend", default="xgb", choices=["xgb", "tabpfn"])
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--groups", default=SET)
    ap.add_argument("--target", default="blend")
    ap.add_argument("--first-cohort", type=int, default=2015)
    ap.add_argument("--last-cohort", type=int, default=2022)
    ap.add_argument("--train-weeks", default="2,4,6,8,10,12,14,16", help="the to-date rows the todate model trains on")
    ap.add_argument("--test-weeks", default="3,5,9,13", help="the weeks of season T the projections are made at")
    ap.add_argument("--train-random-weeks", type=int, default=0,
                    help="K > 0: train on K random weeks (2-16) per player-season instead of --train-weeks (the context stays near the season table's size)")
    ap.add_argument("--no-plain", action="store_true", help="skip the todate_plain variant (saves predictions on the GPU)")
    ap.add_argument("--todate-from", type=int, default=2006)
    ap.add_argument("--cap", default=career.DEFAULT_CAP)
    ap.add_argument("--out", default=str(ROOT / "_cache" / "seam_eval"))
    args = ap.parse_args()
    train_w = list(range(2, 17)) if args.train_random_weeks else [int(w) for w in args.train_weeks.split(",") if w]
    test_w = [int(w) for w in args.test_weeks.split(",") if w]
    hc = build_context(H, args.first_cohort, args.last_cohort, "weekly", todate_weeks=sorted(set(train_w) | set(test_w)))
    groups = fg.resolve(args.groups)
    cols = fg.feature_columns(groups)
    df = mask_season_groups(fg.assemble(hc.matrix, groups, hc.ctx), groups)
    snap = pl.col("is_snapshot_row").cast(pl.Float64, strict=False).fill_null(0.0) > 0.5
    seasons_only = df.filter(~snap)
    train_all = df.filter(~snap | pl.col("row_week").is_in(train_w))
    if args.train_random_weeks:          # K random weeks per player-season (seeded), every week of the season represented
        key = pl.struct("player_id", "season", "row_week").hash(7)
        train_all = train_all.with_columns(key.alias("_rk")).filter(
            ~snap | (pl.col("_rk").rank("ordinal").over(["player_id", "season", "is_snapshot_row"]) <= args.train_random_weeks)).drop("_rk")
    print(f"rows: {seasons_only.height:,} season rows, {df.filter(snap).height:,} to-date rows ({train_all.filter(snap).height:,} at the training weeks)", flush=True)
    kw = dict(device=args.device, features=cols, target=args.target, backend=args.backend, stacked=True)
    rows, players = [], []
    for T in range(args.first_cohort, args.last_cohort + 1):
        t0 = time.perf_counter()
        rep = hc.rep_for(T)
        m_season = career.HorizonModels(H, **kw).fit(seasons_only, as_of_season=T - 1)
        m_todate = career.HorizonModels(H, **kw).fit(train_all, as_of_season=T - 1)
        survival = career.fit_survival(seasons_only.filter((pl.col("season") + 1) <= T - 1), args.cap)
        # the survival cap's tier from the season's games pace, not the games so far (unified.with_tier_games)
        cap = lambda d: career.apply_cap(survival, unified.with_tier_games(d), H, args.cap, tier_cols=("ppg", "tier_games"))  # noqa: E731
        last = cap(m_season.predict(seasons_only.filter((pl.col("season") == T - 1) & (pl.col("games") > 0))))
        end = cap(m_todate.predict(seasons_only.filter(pl.col("season") == T)))
        end_plain = cap(m_season.predict(seasons_only.filter(pl.col("season") == T)))      # the season model at season end (production's offseason)
        import inseason
        rk_table = inseason.rookie_tail_table(seasons_only, [3, 4], through=T - 1)            # production's rookie tail: realized ratios to year +1
        for W in test_w:
            tdr = df.filter(snap & (pl.col("season") == T) & (pl.col("row_week") == W))
            td = cap(m_todate.predict(tdr))
            plain = td if args.no_plain else cap(m_season.predict(tdr))
            # one player set: a played T-1 season and a week-W row; seasons T+2 and T+3 from each origin
            base = last.select("player_id", "position", "age_at_season", "player_name", "h3_ppg_hat", "h3_games_hat", "h4_ppg_hat", "h4_games_hat",
                               "h3_ppg", "h3_games", "h4_ppg", "h4_games", "h3_observable", "h4_observable")
            j = (base.join(plain.select("player_id", *[pl.col(f"h{k}_{x}_hat").alias(f"p{k}_{x}") for k in (2, 3) for x in ("ppg", "games")]), on="player_id", how="inner")
                     .join(td.select("player_id", *[pl.col(f"h{k}_{x}_hat").alias(f"t{k}_{x}") for k in (2, 3) for x in ("ppg", "games")]), on="player_id", how="inner")
                     .join(end.select("player_id", *[pl.col(f"h{k}_{x}_hat").alias(f"e{k}_{x}") for k in (2, 3) for x in ("ppg", "games")]), on="player_id", how="left")
                     .join(end_plain.select("player_id", *[pl.col(f"h{k}_{x}_hat").alias(f"s{k}_{x}") for k in (2, 3) for x in ("ppg", "games")]), on="player_id", how="left")
                     .filter(pl.col("h3_observable") & pl.col("h4_observable")))
            if j.height < 50:
                continue
            ren = lambda pre: j.select("position", *[pl.col(f"{pre}{k}_{x}").alias(f"h{k + 1}_{x}_hat") for k in (2, 3) for x in ("ppg", "games")])  # noqa: E731
            real = par(j, rep, [3, 4], hat=False)
            proj = {"last_season": par(j, rep, [3, 4], hat=True), "todate_plain": par(ren("p"), rep, [3, 4], hat=True),
                    "todate": par(ren("t"), rep, [3, 4], hat=True), "season_end": par(ren("e"), rep, [3, 4], hat=True),
                    "season_end_plain": par(ren("s"), rep, [3, 4], hat=True)}
            age = j["age_at_season"].fill_null(27.0).to_numpy().astype(float)
            for r in score(proj, real, age):
                rows.append({"T": T, "W": W, **r})
            players.append(j.select("player_id", "player_name", "position", "age_at_season").with_columns(
                pl.lit(T).alias("T"), pl.lit(W).alias("W"), pl.Series("real", real), *[pl.Series(k, v) for k, v in proj.items()]))
            # rookies (no T-1 season): the todate model alone, ordering on seasons T+2, T+3
            rk = td.filter(pl.col("exp_at_season").fill_null(9) == 0).filter(pl.col("h2_observable") & pl.col("h3_observable"))
            if rk.height >= 20:
                # production's way for comparison: next season (here the todate model's own h1) times past rookies' realized
                # ratios (rookie_tail_table: in-season h3 = year +2 -> our h2, h4 -> h3), tier on the next-season projection
                tb = (rk.with_columns(pl.col("h1_ppg_hat").alias("next_ppg_hat"), pl.col("h1_games_hat").alias("next_games_hat"),
                                      *[pl.lit(None, pl.Float64).alias(f"h{k}_{x}_hat") for k in (3, 4) for x in ("ppg", "games")], pl.lit(True).alias("is_rookie"))
                        .drop([f"h{k}_{x}_hat" for k in (2,) for x in ("ppg", "games")]))
                tb = inseason.fill_missing_tail(tb.with_columns(pl.lit(None, pl.Float64).alias("h3_ppg_hat")), [3, 4], rookie_table=rk_table) if rk_table.height else None
                real_rk = par(rk, rep, [2, 3], hat=False)
                proj_rk = {"rookies_todate": par(rk, rep, [2, 3], hat=True)}
                if tb is not None:
                    proj_rk["rookies_table"] = par(tb.select("position", *[pl.col(f"h{k + 1}_{x}_hat").alias(f"h{k}_{x}_hat") for k in (2, 3) for x in ("ppg", "games")]), rep, [2, 3], hat=True)
                for r in score(proj_rk, real_rk, rk["age_at_season"].fill_null(22.0).to_numpy().astype(float), top_n=60):
                    rows.append({"T": T, "W": W, **r})
        print(f"  cohort {T} done in {time.perf_counter() - t0:.0f}s", flush=True)
        part = pl.DataFrame(rows)                    # written as it goes: a run stopped at a deadline keeps its finished cohorts
        Path(args.out).mkdir(parents=True, exist_ok=True)
        part.write_parquet(Path(args.out) / f"{args.name}.parquet")
        cur = part.filter((pl.col("T") == T) & pl.col("variant").is_in(["last_season", "todate", "season_end_plain"]))
        print("   " + " | ".join(f"W{w} " + ", ".join(f"{v} {x:.3f}" for v, x in cur.filter(pl.col("W") == w).select("variant", "spearman").iter_rows())
                                 for w in sorted(cur["W"].unique().to_list())), flush=True)
    res = pl.DataFrame(rows)
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    res.write_parquet(out / f"{args.name}.parquet")
    pl.concat(players, how="vertical_relaxed").write_parquet(out / f"{args.name}_players.parquet")
    (out / f"{args.name}.json").write_text(json.dumps(vars(args), indent=1))
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=250, float_precision=3):
        print(f"\n== {args.name}: seasons T+2 and T+3 projected at week W of season T (mean over cohorts {args.first_cohort}-{args.last_cohort}) ==")
        print(res.filter(~pl.col("variant").str.starts_with("rookies")).group_by("W", "variant").agg(
            pl.len().alias("cohorts"), pl.col("n").mean(), pl.col("spearman").mean(), pl.col("mae").mean(), pl.col("bias").mean(), pl.col("ratio").mean(),
            *[pl.col(f"ratio_{b}").mean() for b, _, _ in AGE_BANDS], *[pl.col(f"spearman_{b}").mean() for b, _, _ in AGE_BANDS]).sort("W", "variant"))
        piv = res.filter(pl.col("variant").is_in(["last_season", "todate"])).pivot(on="variant", index=["T", "W"], values="spearman")
        d = (piv["todate"] - piv["last_season"]).to_numpy()
        print(f"todate vs last_season, ordering: {d.mean():+.4f} (t {d.mean() / (d.std(ddof=1) / np.sqrt(len(d))):+.2f}, {int((d > 0).sum())} of {len(d)} cohort-weeks)")
        print("rookies (no prior season): seasons T+2, T+3 from week W -- the todate model vs production's rookie table")
        print(res.filter(pl.col("variant").str.starts_with("rookies")).group_by("W", "variant").agg(pl.col("n").mean(), pl.col("spearman").mean(), pl.col("mae").mean(), pl.col("ratio").mean()).sort("W", "variant"))


if __name__ == "__main__":
    main()
