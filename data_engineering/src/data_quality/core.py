"""The data-quality framework: a ``Check`` is a named expectation on the lake with a severity and the
bug it guards against; a ``Context`` hands checks the tables (silver facts and dims, bronze partitions)
with caching and lets tests seed frames instead of reading GCS; ``run_checks`` turns a list of checks
into one results frame (one row per check) that the runner prints and stores.

Severity: ``error`` fails the job (the daily DAG records it and the run goes red), ``warn`` is reported
only. A check with ``known_open`` set documents a defect we know about (a BACKLOG item): its failure is
reported as a warning with that note until the fix lands, then the note comes off and it enforces.
"""
from __future__ import annotations

import io
import os
import time
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable

import polars as pl

from data_quality.expectations import Result

# every silver table by short name -> blob (a few facts are bare blobs, not <table>/data.parquet)
TABLES = {
    "dim_dates": "silver/fantasy/dim_dates/data.parquet",
    "dim_leagues_meta": "silver/fantasy/dim_leagues_meta/data.parquet",
    "dim_league_settings": "silver/fantasy/dim_league_settings/data.parquet",
    "dim_league_events": "silver/fantasy/dim_league_events/data.parquet",
    "dim_players_master": "silver/fantasy/dim_players_master/data.parquet",
    "dim_franchises_meta": "silver/fantasy/dim_franchises_meta/data.parquet",
    "dim_users": "silver/fantasy/dim_users/data.parquet",
    "dim_college_crosswalk": "silver/fantasy/dim_college_crosswalk/data.parquet",
    "fact_asset_values": "silver/fantasy/fact_asset_values_daily",
    "fact_pick_values": "silver/fantasy/fact_pick_values",
    "fact_roster_membership": "silver/fantasy/fact_roster_membership",
    "fact_player_week": "silver/fantasy/fact_player_week/data.parquet",
    "fact_player_season": "silver/fantasy/fact_player_season/data.parquet",
    "fact_player_week_status": "silver/fantasy/fact_player_week_status/data.parquet",
    "fact_player_injury_week": "silver/fantasy/fact_player_injury_week/data.parquet",
    "fact_depth_chart_week": "silver/fantasy/fact_depth_chart_week/data.parquet",
    "fact_team_season_strength": "silver/fantasy/fact_team_season_strength/data.parquet",
    "fact_team_week_strength": "silver/fantasy/fact_team_week_strength/data.parquet",
    "fact_player_contract_season": "silver/fantasy/fact_player_contract_season/data.parquet",
    "fact_college_player_season": "silver/fantasy/fact_college_player_season/data.parquet",
    "quarantine_unmapped_players": "silver/fantasy/quarantine/unmapped_players.parquet",
}
RESULTS_PREFIX = "silver/_quality"          # run_date=<d>/results.parquet + latest.parquet


@dataclass
class Check:
    name: str                      # dotted: <layer>.<table>.<what>
    table: str
    severity: str                  # "error" | "warn"
    guards: str                    # the bug or trap this check exists for
    fn: Callable[["Context"], Result]
    known_open: str | None = None  # a documented open defect: failures downgrade to warn with this note

    def effective_severity(self) -> str:
        return "warn" if self.known_open else self.severity


class Context:
    """Lazy, cached access to the lake. ``frames`` seeds tables (by short name or blob path) and
    ``partitions`` seeds partition listings (prefix -> [(key, blob)]), so a test never touches GCS."""

    def __init__(self, bucket: str | None = None, today: date | None = None, frames: dict[str, pl.DataFrame] | None = None,
                 partitions: dict[str, list[tuple[str, str]]] | None = None, previous: dict[str, dict] | None = None):
        self.bucket = bucket or os.environ.get("GCS_BUCKET_NAME") or "nfl-data-bronze"
        self.today = today or date.today()
        self._frames: dict[str, pl.DataFrame] = dict(frames or {})
        self._partitions: dict[str, list[tuple[str, str]]] = dict(partitions or {})
        self.previous: dict[str, dict] = dict(previous or {})     # check name -> {"value", "observed"} from the last run
        self._client = None

    # ---------------------------------------------------------------- IO
    def _bucket(self):
        if self._client is None:
            from google.cloud import storage
            self._client = storage.Client()
        return self._client.bucket(self.bucket)

    def blob(self, name: str) -> pl.DataFrame:
        if name not in self._frames:
            self._frames[name] = pl.read_parquet(io.BytesIO(self._bucket().blob(name).download_as_bytes()))
        return self._frames[name]

    def table(self, name: str) -> pl.DataFrame:
        if name in self._frames:
            return self._frames[name]
        return self.blob(TABLES.get(name, name))

    def has(self, name: str) -> bool:
        if name in self._frames or TABLES.get(name, name) in self._frames:
            return True
        return self._bucket().blob(TABLES.get(name, name)).exists()

    def partitions(self, prefix: str, key: str = "load_date") -> list[tuple[str, str]]:
        """``(key value, blob name)`` pairs for every ``<prefix>.../<key>=<value>/...parquet``, oldest first."""
        if prefix not in self._partitions:
            found: dict[str, str] = {}
            for b in self._bucket().list_blobs(prefix=prefix):
                if f"{key}=" in b.name and b.name.endswith(".parquet") and (b.size or 0) > 0:
                    found[b.name.split(f"{key}=")[1].split("/")[0]] = b.name
            self._partitions[prefix] = sorted(found.items())
        return self._partitions[prefix]

    def latest_partition(self, prefix: str, key: str = "load_date") -> tuple[str | None, pl.DataFrame | None]:
        parts = self.partitions(prefix, key)
        if not parts:
            return None, None
        value, blob = parts[-1]
        return value, self.blob(blob)

    # ---------------------------------------------------------------- the calendar and the leagues
    def nfl_season(self) -> int:
        try:
            row = self.table("dim_dates").filter(pl.col("date") == self.today)
            if row.height:
                return int(row["nfl_season"][0])
        except Exception:  # noqa: BLE001 - the spine itself is checked elsewhere
            pass
        return self.today.year if self.today.month >= 3 else self.today.year - 1

    def nfl_phase(self) -> str:
        try:
            row = self.table("dim_dates").filter(pl.col("date") == self.today)
            if row.height:
                return str(row["nfl_phase"][0])
        except Exception:  # noqa: BLE001
            pass
        return "regular" if 9 <= self.today.month <= 12 else ("playoffs" if self.today.month == 1 else "offseason")

    def current_leagues(self) -> pl.DataFrame:
        """The leagues we track now: ``dim_franchises_meta``'s leagues with their season, size and lineage
        (never ``is_active`` / ``status`` from ``dim_leagues_meta``: in the offseason every league reads
        ``complete``, the scoping trap in silver_fantasy/CLAUDE.md)."""
        fr = self.table("dim_franchises_meta").select("league_id").unique()
        lm = self.table("dim_leagues_meta").select("league_id", pl.col("season").cast(pl.Int64), "total_rosters", "league_lineage_id", "status")
        return fr.join(lm, on="league_id", how="left").sort("league_id")

    def previous_value(self, check_name: str) -> float | None:
        p = self.previous.get(check_name)
        return None if not p or p.get("value") is None else float(p["value"])

    def previous_state(self, check_name: str) -> str | None:
        p = self.previous.get(check_name)
        return None if not p else (p.get("state") or None)


def run_checks(checks: list[Check], ctx: Context) -> pl.DataFrame:
    rows = []
    for c in checks:
        t0 = time.perf_counter()
        try:
            r = c.fn(ctx)
            error = None
        except Exception as e:  # noqa: BLE001 - a crashing check is a failed check, never a crashed run
            r = Result(False, f"check raised {type(e).__name__}: {str(e)[:160]}")
            error = traceback.format_exc()[-1500:]
        rows.append({
            "run_date": ctx.today, "check": c.name, "table": c.table, "severity": c.severity, "effective_severity": c.effective_severity(),
            "passed": bool(r.passed), "observed": r.observed, "value": None if r.value is None else float(r.value), "detail": r.detail or "",
            "state": r.state or "", "guards": c.guards, "known_open": c.known_open or "", "seconds": round(time.perf_counter() - t0, 2), "error": error or "",
            "run_at": datetime.now(timezone.utc).replace(tzinfo=None),
        })
    schema = {"run_date": pl.Date, "check": pl.Utf8, "table": pl.Utf8, "severity": pl.Utf8, "effective_severity": pl.Utf8, "passed": pl.Boolean,
              "observed": pl.Utf8, "value": pl.Float64, "detail": pl.Utf8, "state": pl.Utf8, "guards": pl.Utf8, "known_open": pl.Utf8, "seconds": pl.Float64,
              "error": pl.Utf8, "run_at": pl.Datetime("us")}
    return pl.DataFrame(rows, schema=schema)


def summarize(results: pl.DataFrame) -> dict:
    failed = results.filter(~pl.col("passed"))
    return {
        "checks": results.height,
        "passed": results.filter(pl.col("passed")).height,
        "failed_errors": failed.filter(pl.col("effective_severity") == "error").height,
        "failed_warnings": failed.filter(pl.col("effective_severity") == "warn").height,
        "known_open": failed.filter(pl.col("known_open") != "").height,
    }


def previous_from(results: pl.DataFrame | None) -> dict[str, dict]:
    """The last run's results as the ``previous`` map a Context takes (drift / regime checks)."""
    if results is None or results.height == 0:
        return {}
    cols = ["check", "value", "state"] if "state" in results.columns else ["check", "value"]
    return {r["check"]: {"value": r["value"], "state": r.get("state") or None} for r in results.select(cols).to_dicts()}
