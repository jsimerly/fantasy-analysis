"""Title odds: a Monte Carlo of the rest of a league's season (BACKLOG 40).

WAR prices a win as a win. A title is winning the right games: the playoff cut, a bye, three
bracket weeks. Title probability is S-shaped in team strength and depends on where a team sits, so
the same roster move is worth different title odds to a bubble team, a locked-in seed and a
juggernaut, and in a top-heavy league every other team's odds are compressed. This module
simulates the remaining regular-season matchups from each roster's projected lineup points and the
league's weekly spread, seeds the bracket by record then points for, plays it out, and reads off
each team's chance of the playoffs, a bye and the title; ``title_curve`` repeats that over a grid
of shifts to one team's weekly points (common random numbers across the grid), which is what a
player's marginal title odds interpolate on.

Pure numpy; ``analysis/title_odds.py`` feeds it from the lake, the roster views and Sleeper.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

# fixed brackets by seed (1-based), one week per round, no reseeding: Sleeper's default
BRACKETS = {
    4: [[(1, 4), (2, 3)], [("w0", "w1")]],
    6: [[(3, 6), (4, 5)], [(1, "w1"), (2, "w0")], [("w0", "w1")]],
    8: [[(1, 8), (2, 7), (3, 6), (4, 5)], [("w0", "w3"), ("w1", "w2")], [("w0", "w1")]],
}
BYES = {4: 0, 6: 2, 8: 0}


@dataclass
class Season:
    mu: np.ndarray                      # (T,) projected lineup points per week
    sd: float                           # the league's weekly spread of a team's score
    wins: np.ndarray                    # (T,) to date
    pf: np.ndarray                      # (T,) points for to date
    matchups: list[tuple[int, list[tuple[int, int]]]]   # remaining regular-season weeks: (week, [(a, b), ...] 0-based team indices)
    playoff_teams: int

    @property
    def n(self) -> int:
        return len(self.mu)


def _seeds(wins: np.ndarray, pf: np.ndarray) -> np.ndarray:
    """(S, T) seed per team per simulation (0 = top): wins, then points for."""
    key = wins * 1e7 + pf
    order = np.argsort(-key, axis=1, kind="stable")
    seeds = np.empty_like(order)
    rows = np.arange(order.shape[0])[:, None]
    seeds[rows, order] = np.arange(order.shape[1])[None, :]
    return seeds


def _play_bracket(order: np.ndarray, mu: np.ndarray, sd: float, playoff_teams: int, rng: np.random.Generator,
                  draws: np.ndarray | None = None) -> np.ndarray:
    """``order`` (S, T): team index by seed. Returns the champion's team index per simulation."""
    bracket = BRACKETS[playoff_teams]
    S = order.shape[0]
    n_rounds = len(bracket)
    max_games = max(len(r) for r in bracket)
    if draws is None:
        draws = rng.standard_normal((S, n_rounds, max_games, 2))
    winners_prev: list[np.ndarray] = []
    for r, games in enumerate(bracket):
        winners = []
        for g, (a, b) in enumerate(games):
            ta = order[:, a - 1] if isinstance(a, int) else winners_prev[int(a[1:])]
            tb = order[:, b - 1] if isinstance(b, int) else winners_prev[int(b[1:])]
            sa = mu[ta] + sd * draws[:, r, g, 0]
            sb = mu[tb] + sd * draws[:, r, g, 1]
            winners.append(np.where(sa >= sb, ta, tb))
        winners_prev = winners
    return winners_prev[0]


def simulate(season: Season, n_sims: int = 20_000, seed: int = 0, mu_shift: np.ndarray | None = None,
             draws: tuple[np.ndarray, np.ndarray] | None = None) -> dict[str, np.ndarray]:
    """Per team: p_playoffs, p_bye, p_title, exp_wins, exp_seed (1-based). ``mu_shift`` (T,) adds to
    the projected points; ``draws`` (regular-season (S, W, T) and bracket) reuse random numbers so a
    grid of shifts is evaluated on the same simulated seasons."""
    rng = np.random.default_rng(seed)
    T, W = season.n, len(season.matchups)
    mu = season.mu + (mu_shift if mu_shift is not None else 0.0)
    if draws is None:
        reg = rng.standard_normal((n_sims, max(W, 1), T))
        br = rng.standard_normal((n_sims, len(BRACKETS[season.playoff_teams]), max(len(r) for r in BRACKETS[season.playoff_teams]), 2))
    else:
        reg, br = draws
        n_sims = reg.shape[0]
    wins = np.tile(season.wins.astype(float), (n_sims, 1))
    pf = np.tile(season.pf.astype(float), (n_sims, 1))
    for w, (week, pairs) in enumerate(season.matchups):
        scores = mu[None, :] + season.sd * reg[:, w, :]
        pf += scores
        for a, b in pairs:
            a_wins = scores[:, a] > scores[:, b]
            wins[:, a] += a_wins
            wins[:, b] += ~a_wins
    seeds = _seeds(wins, pf)
    order = np.argsort(seeds, axis=1)
    champ = _play_bracket(order, mu, season.sd, season.playoff_teams, rng, br)
    idx = np.arange(T)
    return {
        "p_playoffs": (seeds < season.playoff_teams).mean(axis=0),
        "p_bye": (seeds < BYES[season.playoff_teams]).mean(axis=0),
        "p_title": (champ[:, None] == idx[None, :]).mean(axis=0),
        "exp_wins": wins.mean(axis=0),
        "exp_seed": seeds.mean(axis=0) + 1,
    }


def title_curve(season: Season, team: int, shifts: np.ndarray, n_sims: int = 20_000, seed: int = 0) -> dict[str, np.ndarray]:
    """One team's chance of the title and the playoffs as its weekly points move by ``shifts``
    (points per week), on common random numbers so the curve is smooth."""
    rng = np.random.default_rng(seed)
    T, W = season.n, len(season.matchups)
    reg = rng.standard_normal((n_sims, max(W, 1), T))
    br = rng.standard_normal((n_sims, len(BRACKETS[season.playoff_teams]), max(len(r) for r in BRACKETS[season.playoff_teams]), 2))
    p_title, p_playoffs, p_bye = [], [], []
    for s in shifts:
        shift = np.zeros(T); shift[team] = float(s)
        r = simulate(season, mu_shift=shift, draws=(reg, br))
        p_title.append(r["p_title"][team]); p_playoffs.append(r["p_playoffs"][team]); p_bye.append(r["p_bye"][team])
    return {"shift": np.asarray(shifts, float), "p_title": np.asarray(p_title), "p_playoffs": np.asarray(p_playoffs), "p_bye": np.asarray(p_bye)}


def interp(curve: dict[str, np.ndarray], key: str, shift: float) -> float:
    return float(np.interp(shift, curve["shift"], curve[key]))
