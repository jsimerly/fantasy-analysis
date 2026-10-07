"""What makes value league-dependent: the lineup a league starts and how its points turn into wins.

``LeagueSpec``  teams + starting slots + slot eligibility (the owner's league from
                ``dim_league_settings``, or any other league from a dict / JSON file).
``WinCurve``    P(win a week | your team scores t), a logistic fit on the league's own team-weeks
                (``team_weeks_from_standings`` derives them from the Sleeper standings snapshots:
                weekly points = the change in cumulative points, win = the change in wins).
                A point is worth what it does to that curve, which is why the same projection is
                worth different wins in different leagues.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import polars as pl

POSITIONS = ["QB", "RB", "WR", "TE"]
DEFAULT_ELIGIBILITY = {"QB": ["QB"], "RB": ["RB"], "WR": ["WR"], "TE": ["TE"],
                       "FLEX": ["RB", "WR", "TE"], "SUPER_FLEX": ["QB", "RB", "WR", "TE"]}
SETTINGS_SLOT_COLS = {"qb_slots": "QB", "rb_slots": "RB", "wr_slots": "WR", "te_slots": "TE",
                      "flex_slots": "FLEX", "superflex_slots": "SUPER_FLEX"}
TEAM_STATE_PREFIX = "bronze/sleeper/rosters/team_state/daily/"


@dataclass
class LeagueSpec:
    name: str
    teams: int
    slots: dict[str, int]                                   # slot -> count per team
    eligibility: dict[str, list[str]] = field(default_factory=lambda: dict(DEFAULT_ELIGIBILITY))

    def __post_init__(self):
        self.slots = {s: int(n) for s, n in self.slots.items() if int(n) > 0}
        unknown = [s for s in self.slots if s not in self.eligibility]
        if unknown:
            raise ValueError(f"slots without eligibility: {unknown}")

    @property
    def slot_list(self) -> list[str]:
        """One entry per starting slot, dedicated positions first, then the more restrictive flex."""
        order = sorted(self.slots, key=lambda s: (len(self.eligibility[s]), s))
        return [s for s in order for _ in range(self.slots[s])]

    @property
    def starters(self) -> int:
        return sum(self.slots.values())

    @classmethod
    def from_settings(cls, settings_df: pl.DataFrame, lineage_id: str | None = None, name: str = "owner") -> "LeagueSpec":
        import replacement
        slots, teams = replacement.league_lineup(settings_df, lineage_id) if lineage_id else replacement.league_lineup(settings_df)
        return cls(name=name, teams=teams, slots={SETTINGS_SLOT_COLS[c]: n for c, n in slots.items() if c in SETTINGS_SLOT_COLS})

    @classmethod
    def from_dict(cls, d: dict) -> "LeagueSpec":
        return cls(name=d["name"], teams=int(d["teams"]), slots=dict(d["slots"]),
                   eligibility={**DEFAULT_ELIGIBILITY, **d.get("eligibility", {})})

    @classmethod
    def from_json(cls, path: str | Path) -> "LeagueSpec":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))

    def to_dict(self) -> dict:
        return {"name": self.name, "teams": self.teams, "slots": dict(self.slots), "eligibility": dict(self.eligibility)}


# ---------------------------------------------------------------------------- win curve
@dataclass
class WinCurve:
    """P(win) = 1 / (1 + exp(-(a + b * points))), fitted on this league's team-weeks."""
    a: float
    b: float
    mean_points: float
    sd_points: float
    n: int = 0

    def win_prob(self, points) -> np.ndarray:
        z = self.a + self.b * np.asarray(points, float)
        return 1.0 / (1.0 + np.exp(-z))

    def delta_win(self, base_points, add) -> np.ndarray:
        """Extra win probability from scoring ``add`` more in a week where you would score ``base_points``."""
        base = np.asarray(base_points, float)
        return self.win_prob(base + np.asarray(add, float)) - self.win_prob(base)

    @property
    def slope_at_mean(self) -> float:
        """Win probability per point for an average team (the linear PAR-to-wins rate)."""
        p = float(self.win_prob(self.mean_points))
        return self.b * p * (1 - p)

    @classmethod
    def fit(cls, team_weeks: pl.DataFrame, points_col: str = "week_pts", win_col: str = "win", iters: int = 50) -> "WinCurve":
        """Logistic regression by IRLS (no extra dependency); ``team_weeks`` holds one row per team-week."""
        x = team_weeks[points_col].to_numpy().astype(float)
        y = team_weeks[win_col].to_numpy().astype(float)
        if len(x) < 20 or y.min() == y.max():
            raise ValueError("need at least 20 team-weeks with both wins and losses to fit a win curve")
        mu, sd = x.mean(), x.std()
        xs = (x - mu) / sd                                      # standardize for a stable fit
        X = np.column_stack([np.ones_like(xs), xs])
        beta = np.zeros(2)
        for _ in range(iters):
            p = 1 / (1 + np.exp(-(X @ beta)))
            w = p * (1 - p) + 1e-9
            grad = X.T @ (y - p)
            hess = (X * w[:, None]).T @ X + 1e-6 * np.eye(2)
            step = np.linalg.solve(hess, grad)
            beta += step
            if np.abs(step).max() < 1e-8:
                break
        b = beta[1] / sd
        a = beta[0] - b * mu
        return cls(a=float(a), b=float(b), mean_points=float(mu), sd_points=float(sd), n=int(len(x)))

    @classmethod
    def normal(cls, mean_points: float, sd_points: float) -> "WinCurve":
        """A curve from a margin model alone: opponent ~ same distribution, so P(win | t) =
        Phi((t - mean) / sd) ~ logistic with b = 1.7 / sd. Useful for a league with no standings yet."""
        b = 1.7 / sd_points
        return cls(a=-b * mean_points, b=b, mean_points=mean_points, sd_points=sd_points, n=0)

    def to_dict(self) -> dict:
        return {"a": self.a, "b": self.b, "mean_points": self.mean_points, "sd_points": self.sd_points, "n": self.n}

    @classmethod
    def from_dict(cls, d: dict) -> "WinCurve":
        return cls(**{k: d[k] for k in ("a", "b", "mean_points", "sd_points")}, n=int(d.get("n", 0)))


def team_weeks_from_standings(team_state: pl.DataFrame, lo: float = 20.0, hi: float = 400.0) -> pl.DataFrame:
    """Sleeper standings snapshots (cumulative wins / fpts per roster, one row per load date) ->
    one row per team-week with ``week_pts`` and ``win`` (0/1). Snapshots that do not straddle
    exactly one game week (ties, double weeks, offseason) are dropped."""
    dec = pl.col("fpts_decimal").cast(pl.Float64).fill_null(0) / 100 if "fpts_decimal" in team_state.columns else pl.lit(0.0)
    k = (team_state.select("league_id", "roster_id", "load_date", (pl.col("fpts").cast(pl.Float64) + dec).alias("total"),
                           pl.col("wins").cast(pl.Int64), pl.col("losses").cast(pl.Int64))
         .sort(["league_id", "roster_id", "load_date"])
         .with_columns(pl.col("total").diff().over(["league_id", "roster_id"]).alias("week_pts"),
                       pl.col("wins").diff().over(["league_id", "roster_id"]).alias("dwin"),
                       pl.col("losses").diff().over(["league_id", "roster_id"]).alias("dloss")))
    return (k.filter((pl.col("week_pts") > lo) & (pl.col("week_pts") < hi) & ((pl.col("dwin") + pl.col("dloss")) == 1))
             .select("league_id", "roster_id", "load_date", "week_pts", pl.col("dwin").alias("win")))


OWNER_CURVE_FALLBACK = (134.2, 41.2)        # Stuck in High School, 2025-26 team-weeks: weekly mean, margin spread


def curve_for_lineage(lineage_id: str | None, min_fit: int = 300) -> "WinCurve":
    """A league's win curve from its own standings in the lake: a logistic fit with ``min_fit``
    team-weeks, otherwise a normal-margin curve from its weekly mean / spread; the owner's
    league's known values when the lake cannot be read."""
    try:
        ts = load_team_state()
        if lineage_id:
            import gcs_io
            ids = gcs_io.read_lake("silver/fantasy/dim_leagues_meta/data.parquet").filter(pl.col("league_lineage_id") == lineage_id)["league_id"].to_list()
            ts = ts.filter(pl.col("league_id").is_in(ids))
        tw = team_weeks_from_standings(ts)
        if tw.height >= min_fit:
            return WinCurve.fit(tw)
        c = WinCurve.normal(float(tw["week_pts"].mean()), float(tw["week_pts"].std()) * 2 ** 0.5)
        c.n = tw.height
        return c
    except Exception:  # noqa: BLE001
        return WinCurve.normal(*OWNER_CURVE_FALLBACK)


def load_team_state() -> pl.DataFrame:
    """Every standings snapshot in the lake, tagged with its load_date."""
    import gcs_io
    return gcs_io.read_lake_prefix(TEAM_STATE_PREFIX, partition="load_date")
