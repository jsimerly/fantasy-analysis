"""Wins above replacement (WAR): intrinsic value v2, in wins, league-dependent by construction.

For each value component k (in-season: rest of THIS season, next season, season + 2, ...; preseason:
season 1, 2, ...):

    excess_k = E[max(ppg_k - replacement_pos, 0)]           the sigma-aware points above replacement per
                                                           game (exactly PAR's per-game term)
    war_k    = games_k * [W(mu + excess_k) - W(mu)]          wins that excess buys on the league's win
                                                           curve for an average team scoring mu
    WAR      = sum_k (1 - rate)^(k-1) * war_k                the first component is never discounted

PAR is the linear special case (W linear). Because W is concave above the middle, a very large
per-game edge earns slightly less than proportional wins; the measured curve in the owner's league
is near-linear over the range one player can move a lineup, so rankings barely change -- what
changes is the unit (wins) and that replacement and the curve come from the league.

Team-specific value (``team_marginal_war``): instead of the league replacement, the player's
marginal starting points on an actual roster (optimal lineup with him minus without him), mapped
through the curve at that roster's own weekly total. A contender near the top of the curve gets
fewer regular-season wins per point than an average team; title equity is a separate layer.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl

import lineup
import value
from league import LeagueSpec, WinCurve


@dataclass(frozen=True)
class Component:
    """One value span: which columns carry its projection."""
    k: int                 # 1 = the current span (never discounted)
    ppg: str
    games: str
    sigma: str | None = None


def inseason_components(tail: list[int]) -> list[Component]:
    comps = [Component(1, "ros_ppg_hat", "ros_games_hat", "h1_ppg_sigma" if False else None),
             Component(2, "next_ppg_hat", "next_games_hat", None)]
    return comps + [Component(k, f"h{k}_ppg_hat", f"h{k}_games_hat", f"h{k}_ppg_sigma") for k in tail if k >= 3]


def career_components(horizons: list[int]) -> list[Component]:
    return [Component(k, f"h{k}_ppg_hat", f"h{k}_games_hat", f"h{k}_ppg_sigma") for k in horizons]


def _excess(df: pl.DataFrame, comp: Component, rep_arr: np.ndarray, sigma: dict | None) -> np.ndarray:
    mu = df[comp.ppg].fill_null(0.0).to_numpy().astype(float)
    if comp.sigma and comp.sigma in df.columns:
        return value.expected_excess(mu, df[comp.sigma].fill_null(0.0).to_numpy().astype(float), rep_arr)
    if sigma and comp.k in sigma:
        s = df["position"].replace_strict({p: v for p, v in sigma[comp.k].items() if p != "__all__"},
                                          default=sigma[comp.k]["__all__"], return_dtype=pl.Float64).to_numpy()
        return value.expected_excess(mu, s, rep_arr)
    return np.maximum(mu - rep_arr, 0.0)


def wins_above_replacement(df: pl.DataFrame, rep: dict[str, float], curve: WinCurve, components: list[Component],
                           discount_rate: float = value.DEFAULT_DISCOUNT_RATE, sigma: dict | None = None) -> pl.DataFrame:
    """Adds ``war_k`` (wins) and ``par_k`` (points above replacement) per component, ``war`` and ``par`` totals."""
    rep_arr = df["position"].replace_strict(rep, default=0.0, return_dtype=pl.Float64).to_numpy()
    cols, war_total, par_total = [], np.zeros(df.height), np.zeros(df.height)
    for comp in components:
        ex = _excess(df, comp, rep_arr, sigma)
        games = df[comp.games].fill_null(0.0).to_numpy().astype(float)
        par = ex * games
        war = games * curve.delta_win(curve.mean_points, ex)
        w = value.discount_weight(comp.k, discount_rate)
        cols += [pl.Series(f"par_{comp.k}", par), pl.Series(f"war_{comp.k}", war)]
        war_total += w * war
        par_total += w * par
    return df.with_columns(cols + [pl.Series("war", war_total), pl.Series("par", par_total)])


def wins_range(df: pl.DataFrame, rep: dict[str, float], curve: WinCurve, components: list[Component],
               discount_rate: float = value.DEFAULT_DISCOUNT_RATE, lo: str = "q20", hi: str = "q80") -> pl.DataFrame:
    """Floor and ceiling wins per span and in total: the same arithmetic as ``wins_above_replacement``
    run on the band's ppg and games (``h{k}_ppg_q20`` / ``_q80``, no upside term) where a span carries
    one; a span without a band (this season, next season, a tree backend) keeps its point wins on
    both sides. Adds ``war_lo_k`` / ``war_hi_k`` and ``war_lo`` / ``war_hi``."""
    rep_arr = df["position"].replace_strict(rep, default=0.0, return_dtype=pl.Float64).to_numpy()
    cols, lo_total, hi_total = [], np.zeros(df.height), np.zeros(df.height)
    for comp in components:
        w = value.discount_weight(comp.k, discount_rate)
        base = comp.ppg.replace("_ppg_hat", "")
        both = []
        for side, q in (("lo", lo), ("hi", hi)):
            pcol, gcol = f"{base}_ppg_{q}", f"{base}_games_{q}"
            if pcol in df.columns and df[pcol].null_count() < df.height:
                ppg = df[pcol].fill_null(0.0).to_numpy().astype(float)
                games = (df[gcol] if gcol in df.columns else df[comp.games]).fill_null(0.0).to_numpy().astype(float)
                ex = np.maximum(ppg - rep_arr, 0.0)
                wins = games * curve.delta_win(curve.mean_points, ex)
            else:
                wins = df[f"war_{comp.k}"].fill_null(0.0).to_numpy().astype(float) if f"war_{comp.k}" in df.columns else np.zeros(df.height)
            both.append(wins)
            cols.append(pl.Series(f"war_{side}_{comp.k}", wins))
        lo_total += w * np.minimum(both[0], both[1])
        hi_total += w * np.maximum(both[0], both[1])
    return df.with_columns(cols + [pl.Series("war_lo", lo_total), pl.Series("war_hi", hi_total)])


def realized_wins(df: pl.DataFrame, rep: dict[str, float], curve: WinCurve, horizons: list[int],
                  discount_rate: float = value.DEFAULT_DISCOUNT_RATE) -> pl.DataFrame:
    """What a player actually delivered, in the same wins: per observable season k,
    games_k x [W(mu + max(ppg_k - rep, 0)) - W(mu)], discounted like ``wins_above_replacement``.
    Null when any horizon is censored (not yet played), as ``value.realized_value`` does."""
    rep_arr = df["position"].replace_strict(rep, default=0.0, return_dtype=pl.Float64).to_numpy()
    total = np.zeros(df.height)
    cols = []
    for k in horizons:
        ppg = df[f"h{k}_ppg"].fill_null(0.0).to_numpy().astype(float)
        games = df[f"h{k}_games"].fill_null(0).to_numpy().astype(float)
        ex = np.maximum(ppg - rep_arr, 0.0)
        w = games * curve.delta_win(curve.mean_points, ex)
        cols.append(pl.Series(f"war_real_{k}", w))
        total += value.discount_weight(k, discount_rate) * w
    observable = pl.all_horizontal([pl.col(f"h{k}_observable") for k in horizons])
    return df.with_columns(cols + [pl.Series("_rw", total)]).with_columns(
        pl.when(observable).then(pl.col("_rw")).otherwise(None).alias("realized_war")).drop("_rw")


def lineup_offset(curve: WinCurve, lineup_totals: list[float]) -> float:
    """Where projected lineups sit on the curve. The curve is fitted on the league's ACTUAL weekly
    totals (every position, that league's scoring); projected lineups cover QB/RB/WR/TE in model
    units and are shrunk toward the mean, so they run lower. Centring the curve on the league's
    average projected lineup puts an average roster at the curve's 50 % point, and the rosters'
    win probabilities average one half, as they must."""
    if not lineup_totals:
        return 0.0
    return float(curve.mean_points - float(np.mean(lineup_totals)))


def team_marginal_war(roster: pl.DataFrame, spec: LeagueSpec, curve: WinCurve, components: list[Component],
                      discount_rate: float = value.DEFAULT_DISCOUNT_RATE, candidates: pl.DataFrame | None = None,
                      offset: float = 0.0, floor: dict[str, float] | None = None) -> pl.DataFrame:
    """Marginal wins each rostered player adds to HIS roster (lineup with him minus without him),
    and, if ``candidates`` is given, what each candidate would add to this roster.

    ``roster`` / ``candidates``: one row per player with ``position`` and the component columns.
    ``offset`` (see ``lineup_offset``) shifts projected lineup totals onto the curve's scale.
    Returns one row per player with ``m_par_k`` / ``m_war_k`` per component and ``m_war`` / ``m_par``."""
    pos = roster["position"].to_list()
    out_rows = []

    def value_of(points_by_comp: dict[int, np.ndarray], cand_pos: str, cand_pts: dict[int, float], games: dict[int, float], remove_idx: int | None):
        row = {}
        war_total = par_total = 0.0
        for comp in components:
            pts = points_by_comp[comp.k]
            if remove_idx is None:                                       # external candidate: add him
                base, _ = lineup.optimal_lineup(pts, pos, spec, floor)
                with_, _ = lineup.optimal_lineup(np.append(pts, cand_pts[comp.k]), pos + [cand_pos], spec, floor)
                gain, team_total = max(with_ - base, 0.0), base
            else:                                                        # rostered: remove him
                with_, _ = lineup.optimal_lineup(pts, pos, spec, floor)
                keep = np.ones(len(pts), bool); keep[remove_idx] = False
                base, _ = lineup.optimal_lineup(pts[keep], [p for i, p in enumerate(pos) if keep[i]], spec, floor)
                gain, team_total = max(with_ - base, 0.0), with_
            g = games[comp.k]
            par = gain * g
            war = g * float(curve.delta_win(team_total + offset, gain))
            w = value.discount_weight(comp.k, discount_rate)
            row[f"m_par_{comp.k}"], row[f"m_war_{comp.k}"] = par, war
            war_total += w * war; par_total += w * par
        row["m_war"], row["m_par"] = war_total, par_total
        return row

    points_by_comp = {c.k: roster[c.ppg].fill_null(0.0).to_numpy().astype(float) for c in components}
    for i in range(roster.height):
        games = {c.k: float(roster[c.games].fill_null(0.0)[i]) for c in components}
        r = value_of(points_by_comp, pos[i], {}, games, i)
        out_rows.append({"player_id": roster["player_id"][i], "player_name": roster["player_name"][i] if "player_name" in roster.columns else None,
                         "position": pos[i], "rostered": True, **r})
    if candidates is not None:
        for j in range(candidates.height):
            cand_pts = {c.k: float(candidates[c.ppg].fill_null(0.0)[j]) for c in components}
            games = {c.k: float(candidates[c.games].fill_null(0.0)[j]) for c in components}
            r = value_of(points_by_comp, candidates["position"][j], cand_pts, games, None)
            out_rows.append({"player_id": candidates["player_id"][j], "player_name": candidates["player_name"][j] if "player_name" in candidates.columns else None,
                             "position": candidates["position"][j], "rostered": False, **r})
    return pl.DataFrame(out_rows)


def roster_total(roster: pl.DataFrame, spec: LeagueSpec, ppg_col: str, floor: dict[str, float] | None = None) -> float:
    total, _ = lineup.optimal_lineup(roster[ppg_col].fill_null(0.0).to_numpy().astype(float), roster["position"].to_list(), spec, floor)
    return total
