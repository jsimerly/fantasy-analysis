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


def team_marginal_war(roster: pl.DataFrame, spec: LeagueSpec, curve: WinCurve, components: list[Component],
                      discount_rate: float = value.DEFAULT_DISCOUNT_RATE, candidates: pl.DataFrame | None = None) -> pl.DataFrame:
    """Marginal wins each rostered player adds to HIS roster (lineup with him minus without him),
    and, if ``candidates`` is given, what each candidate would add to this roster.

    ``roster`` / ``candidates``: one row per player with ``position`` and the component columns.
    Returns one row per player with ``m_par_k`` / ``m_war_k`` per component and ``m_war`` / ``m_par``."""
    pos = roster["position"].to_list()
    out_rows = []

    def value_of(points_by_comp: dict[int, np.ndarray], cand_pos: str, cand_pts: dict[int, float], games: dict[int, float], remove_idx: int | None):
        row = {}
        war_total = par_total = 0.0
        for comp in components:
            pts = points_by_comp[comp.k]
            if remove_idx is None:                                       # external candidate: add him
                base, _ = lineup.optimal_lineup(pts, pos, spec)
                with_, _ = lineup.optimal_lineup(np.append(pts, cand_pts[comp.k]), pos + [cand_pos], spec)
                gain, team_total = max(with_ - base, 0.0), base
            else:                                                        # rostered: remove him
                with_, _ = lineup.optimal_lineup(pts, pos, spec)
                keep = np.ones(len(pts), bool); keep[remove_idx] = False
                base, _ = lineup.optimal_lineup(pts[keep], [p for i, p in enumerate(pos) if keep[i]], spec)
                gain, team_total = max(with_ - base, 0.0), with_
            g = games[comp.k]
            par = gain * g
            war = g * float(curve.delta_win(team_total, gain))
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


def roster_total(roster: pl.DataFrame, spec: LeagueSpec, ppg_col: str) -> float:
    total, _ = lineup.optimal_lineup(roster[ppg_col].fill_null(0.0).to_numpy().astype(float), roster["position"].to_list(), spec)
    return total
