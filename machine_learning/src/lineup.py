"""Optimal lineups and league-wide replacement from an explicit fill of the league's slots.

``optimal_lineup``   best assignment of a roster to a league's starting slots (exact, via the
                     Hungarian algorithm), so FLEX / SUPER_FLEX are filled by whoever is worth most
                     there, not by a fixed position share.
``league_fill``      fill every team's slots from one pool (the league's starters), dedicated
                     slots first, then the more restrictive flex slots; replacement at a position
                     is the best player left over. This replaces the fixed FLEX_SHARE / "superflex
                     = QB" guesses with the league's actual configuration.
``replacement_from_history``  that fill on the last few real seasons, averaged (stable like the
                     production replacement level, but configuration-driven).
"""
from __future__ import annotations

import numpy as np
import polars as pl
from scipy.optimize import linear_sum_assignment

from league import LeagueSpec

BIG = 1e6


def optimal_lineup(points: np.ndarray, positions: list[str], spec: LeagueSpec) -> tuple[float, np.ndarray]:
    """Returns (total starting points, slot index per player or -1 for bench)."""
    points = np.asarray(points, float)
    slots = spec.slot_list
    n, m = len(points), len(slots)
    if n == 0 or m == 0:
        return 0.0, np.full(n, -1)
    cost = np.full((n, m), BIG)
    for i, pos in enumerate(positions):
        for j, slot in enumerate(slots):
            if pos in spec.eligibility[slot]:
                cost[i, j] = -max(points[i], 0.0)
    rows, cols = linear_sum_assignment(cost)
    assign = np.full(n, -1)
    total = 0.0
    for i, j in zip(rows, cols):
        if cost[i, j] < BIG:
            assign[i] = j
            total += -cost[i, j]
    return float(total), assign


def marginal_gain(points: np.ndarray, positions: list[str], cand_points: float, cand_pos: str, spec: LeagueSpec) -> float:
    """Starting points a roster gains by adding one player (0 if he would not start)."""
    base, _ = optimal_lineup(points, positions, spec)
    with_, _ = optimal_lineup(np.append(points, cand_points), list(positions) + [cand_pos], spec)
    return max(with_ - base, 0.0)


def league_fill(pool: pl.DataFrame, spec: LeagueSpec, ppg_col: str = "ppg", pos_col: str = "position") -> dict[str, float]:
    """Replacement ppg per position after the league's starters are drawn from ``pool``."""
    p = pool.select(pl.col(pos_col).alias("pos"), pl.col(ppg_col).cast(pl.Float64).alias("v")).drop_nulls().sort("v", descending=True)
    taken = np.zeros(p.height, dtype=bool)
    pos = p["pos"].to_numpy()
    for slot in sorted(spec.slots, key=lambda s: (len(spec.eligibility[s]), s)):
        need = spec.slots[slot] * spec.teams
        elig = np.isin(pos, spec.eligibility[slot]) & ~taken
        idx = np.flatnonzero(elig)[:need]
        taken[idx] = True
    v = p["v"].to_numpy()
    out = {}
    for q in POSITIONS_OF(spec):
        left = np.flatnonzero((pos == q) & ~taken)
        out[q] = float(v[left[0]]) if len(left) else 0.0
    return out


def POSITIONS_OF(spec: LeagueSpec) -> list[str]:
    seen: list[str] = []
    for s in spec.slot_list:
        for q in spec.eligibility[s]:
            if q not in seen:
                seen.append(q)
    return seen


def starters_per_position(pool: pl.DataFrame, spec: LeagueSpec, ppg_col: str = "ppg", pos_col: str = "position") -> dict[str, int]:
    """How many of each position the fill actually starts (diagnostic; replaces FLEX_SHARE)."""
    p = pool.select(pl.col(pos_col).alias("pos"), pl.col(ppg_col).cast(pl.Float64).alias("v")).drop_nulls().sort("v", descending=True)
    taken = np.zeros(p.height, dtype=bool)
    pos = p["pos"].to_numpy()
    for slot in sorted(spec.slots, key=lambda s: (len(spec.eligibility[s]), s)):
        need = spec.slots[slot] * spec.teams
        idx = np.flatnonzero(np.isin(pos, spec.eligibility[slot]) & ~taken)[:need]
        taken[idx] = True
    return {q: int(((pos == q) & taken).sum()) for q in POSITIONS_OF(spec)}


def replacement_from_history(season_df: pl.DataFrame, spec: LeagueSpec, seasons: list[int], min_games: int = 8) -> dict[str, float]:
    """League-fill replacement on real seasons (players with >= min_games), averaged."""
    acc: dict[str, list[float]] = {}
    for s in seasons:
        pool = season_df.filter((pl.col("season") == s) & (pl.col("games") >= min_games))
        if pool.height == 0:
            continue
        for q, v in league_fill(pool, spec).items():
            acc.setdefault(q, []).append(v)
    return {q: float(np.mean(v)) for q, v in acc.items()}
