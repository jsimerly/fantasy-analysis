"""silver_fantasy/fact_roster_membership.py

Daily ownership of roster *assets* — players and draft picks — per franchise.

Players come from Sleeper's authoritative roster snapshot. Pick ownership is not
authoritative in any single source: it is reconstructed by overlaying, in
precedence order, the original owner -> Sleeper traded-pick state -> the
transaction draft-pick event log -> commissioner overrides (the out-of-band
trades Sleeper never recorded). A reconciliation step enforces conservation
invariants (no asset owned twice, every league/season/round resolves to exactly
``total_rosters`` picks, every moved pick lands on a real franchise) and routes
any violation to a quarantine parquet rather than dropping it silently.

Output grain: one row per ``snapshot_date x franchise_id x asset_id`` with
``asset_type in {player, pick}``.
"""
import os
from datetime import datetime

import polars as pl
from dotenv import load_dotenv

from utils import get_latest_bronze_path

load_dotenv()

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------
# Canonical key types shared by every pick source. Pick identity is
# (league_id, season, round, original_roster_id) everywhere.
_PICK_KEY = ["league_id", "season", "round", "original_roster_id"]

# Unified asset-row columns (player + pick rows are diagonally concatenated).
_FACT_COLS = [
    "league_id", "franchise_id", "roster_id", "asset_type", "asset_id",
    "player_key", "position",
    "pick_season", "pick_round", "pick_original_roster_id",
    "is_starter", "is_taxi", "is_reserve",
]


# -------------------------------------------------------------------------
# PLAYERS
# -------------------------------------------------------------------------
def build_player_membership(
    roster_players_df: pl.DataFrame,
    dim_franchise: pl.DataFrame,
    dim_player: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve the player snapshot to franchises (and best-effort player_key)."""
    players = roster_players_df.with_columns(
        pl.col("league_id").cast(pl.Utf8),
        pl.col("roster_id").cast(pl.Int64),
        pl.col("player_id").cast(pl.Utf8),
    )

    franchise_lookup = dim_franchise.select(
        pl.col("league_id").cast(pl.Utf8),
        pl.col("roster_id").cast(pl.Int64),
        "franchise_id",
    ).unique(subset=["league_id", "roster_id"], keep="first")

    player_lookup = (
        dim_player.select(pl.col("player_key").cast(pl.Utf8), "position")
        .unique(subset=["player_key"], keep="first")
        .with_columns(pl.lit(True).alias("_player_mapped"))
    )

    out = (
        players
        .join(franchise_lookup, on=["league_id", "roster_id"], how="left")
        .join(player_lookup, left_on="player_id", right_on="player_key", how="left")
        .with_columns(
            pl.lit("player").alias("asset_type"),
            pl.col("player_id").alias("asset_id"),
            # player_key is only populated when the player exists in the dim; an
            # unmapped snapshot player must surface as null (-> quarantine warn).
            pl.when(pl.col("_player_mapped").fill_null(False))
              .then(pl.col("player_id"))
              .otherwise(None)
              .alias("player_key"),
            # picks-only fields are null for players
            pl.lit(None, dtype=pl.Utf8).alias("pick_season"),
            pl.lit(None, dtype=pl.Int64).alias("pick_round"),
            pl.lit(None, dtype=pl.Int64).alias("pick_original_roster_id"),
        )
    )
    # Ensure flag columns exist even if an empty snapshot lacked them.
    for flag in ("is_starter", "is_taxi", "is_reserve"):
        if flag not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=pl.Boolean).alias(flag))

    return out.select(_FACT_COLS)


# -------------------------------------------------------------------------
# PICKS
# -------------------------------------------------------------------------
def _norm_pick_keys(df: pl.DataFrame, original_col: str) -> pl.DataFrame:
    """Cast a pick source to the canonical (league_id, season, round,
    original_roster_id) key types."""
    return df.with_columns(
        pl.col("league_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Utf8),
        pl.col("round").cast(pl.Int64),
        pl.col(original_col).cast(pl.Int64).alias("original_roster_id"),
    )


def build_lineage_map(leagues_df: pl.DataFrame) -> pl.DataFrame:
    """Map every league_id to the *current* league of its dynasty lineage.

    The current league is the max-season league per ``league_lineage_id``, derived
    straight from dim_leagues_meta so it self-updates when a new season's league
    appears. Pick records pinned to an older season's league_id (notably the
    hand-coded commissioner overrides) are thereby translated onto the current
    league and keep resolving correctly across season rollovers — no manual edit
    needed when a new league season starts.

    Returns one row per known league_id with ``current_league_id``,
    ``current_total_rosters`` and ``latest_completed_season`` (the max completed
    season in the lineage, used to drop already-drafted picks).
    """
    lin = leagues_df.select(
        pl.col("league_id").cast(pl.Utf8),
        pl.col("league_lineage_id").cast(pl.Utf8).alias("lineage_id"),
        pl.col("season").cast(pl.Int64).alias("season_int"),
        pl.col("status").cast(pl.Utf8),
        pl.col("total_rosters").cast(pl.Int64),
    )
    current = (
        lin.sort("season_int")
        .group_by("lineage_id")
        .agg(
            pl.col("league_id").last().alias("current_league_id"),
            pl.col("total_rosters").last().alias("current_total_rosters"),
            pl.col("season_int").filter(pl.col("status") == "complete").max()
              .alias("latest_completed_season"),
        )
    )
    return lin.join(current, on="lineage_id", how="left").select(
        "league_id", "lineage_id", "current_league_id",
        "current_total_rosters", "latest_completed_season",
    )


def _drafted_cutoffs(drafts_df, lineage_map: pl.DataFrame):
    """Per current league, the latest season whose rookie draft is `complete`.

    This is the precise "these picks are spent" signal: a draft finishes months
    before its league flips to status=complete, so using draft status (not league
    status) avoids resurrecting already-drafted current-season picks during the
    in-season window. Returns None when no drafts data is available (callers then
    fall back to league status).
    """
    if drafts_df is None or drafts_df.height == 0:
        return None
    d = (
        drafts_df
        .filter(pl.col("status") == "complete")
        .select(pl.col("league_id").cast(pl.Utf8), pl.col("season").cast(pl.Int64).alias("drafted_season"))
        .join(lineage_map.select("league_id", "current_league_id"), on="league_id", how="left")
        .filter(pl.col("current_league_id").is_not_null())
    )
    if d.height == 0:
        return None
    return (
        d.group_by("current_league_id")
        .agg(pl.col("drafted_season").max().alias("latest_drafted_season"))
        .select(pl.col("current_league_id").alias("league_id"), "latest_drafted_season")
    )


def _rookie_rounds(drafts_df, lineage_map: pl.DataFrame):
    """Per current league, the number of rounds in its rookie draft.

    Taken from the most recent *completed* draft in the lineage — that's the
    rookie-draft format (early-season startup drafts have far more rounds and are
    superseded). Number of rounds is a per-league setting, so it must be read from
    the data rather than assumed. Returns None when no drafts data is available.
    """
    if drafts_df is None or drafts_df.height == 0 or "rounds" not in drafts_df.columns:
        return None
    d = (
        drafts_df
        .filter(pl.col("status") == "complete")
        .select(
            pl.col("league_id").cast(pl.Utf8),
            pl.col("season").cast(pl.Int64).alias("dseason"),
            pl.col("rounds").cast(pl.Int64),
        )
        .join(lineage_map.select("league_id", "current_league_id"), on="league_id", how="left")
        .filter(pl.col("current_league_id").is_not_null())
    )
    if d.height == 0:
        return None
    return (
        d.sort("dseason").group_by("current_league_id")
        .agg(pl.col("rounds").last().alias("rookie_rounds"))
        .select(pl.col("current_league_id").alias("league_id"), "rookie_rounds")
    )


def build_pick_universe(
    observed: pl.DataFrame,
    current_meta: pl.DataFrame,
    years_ahead: int = 3,
) -> pl.DataFrame:
    """Enumerate every pick that should exist, keyed by ``_PICK_KEY``.

    Dynasty picks are minted ``years_ahead`` seasons out at each draft, so the
    live set is every league x the next ``years_ahead`` undrafted seasons x
    ``1..rookie_rounds`` x ``1..total_rosters``. This deterministic grid is what
    guarantees *untraded* picks are still accounted for (the prior trade-inferred
    universe silently dropped any round nobody had traded).

    ``current_meta`` carries, per current league: ``total_rosters``,
    ``cutoff_season`` (latest already-drafted season) and ``rookie_rounds``. When
    ``rookie_rounds``/``cutoff_season`` are unknown (e.g. no drafts data) the grid
    is skipped and the function falls back to the observed ``(season, round)`` set
    crossed with rosters, preserving older behavior.
    """
    parts: list[pl.DataFrame] = []

    # deterministic grid: full rolling window of future picks
    grid_src = current_meta.filter(
        pl.col("cutoff_season").is_not_null() & pl.col("rookie_rounds").is_not_null()
    )
    if grid_src.height:
        grid = (
            grid_src
            .with_columns(
                pl.int_ranges(pl.col("cutoff_season") + 1,
                              pl.col("cutoff_season") + 1 + years_ahead).alias("_season")
            )
            .explode("_season")
            .with_columns(pl.int_ranges(1, pl.col("rookie_rounds") + 1).alias("round"))
            .explode("round")
            .with_columns(pl.int_ranges(1, pl.col("total_rosters") + 1).alias("original_roster_id"))
            .explode("original_roster_id")
            .select(
                pl.col("league_id"),
                pl.col("_season").cast(pl.Utf8).alias("season"),
                pl.col("round").cast(pl.Int64),
                pl.col("original_roster_id").cast(pl.Int64),
            )
        )
        parts.append(grid)

    # observed-based fallback / supplement: any (season, round) actually seen,
    # crossed with rosters (covers leagues without drafts data, or picks traded
    # beyond the assumed window).
    if observed is not None and observed.height:
        obs = (
            observed
            .join(current_meta.select("league_id", "total_rosters"), on="league_id", how="inner")
            .with_columns(pl.int_ranges(1, pl.col("total_rosters") + 1).alias("original_roster_id"))
            .explode("original_roster_id")
            .select(
                pl.col("league_id"),
                pl.col("season").cast(pl.Utf8),
                pl.col("round").cast(pl.Int64),
                pl.col("original_roster_id").cast(pl.Int64),
            )
        )
        parts.append(obs)

    if not parts:
        return pl.DataFrame(schema={
            "league_id": pl.Utf8, "season": pl.Utf8,
            "round": pl.Int64, "original_roster_id": pl.Int64,
        })

    universe = pl.concat(parts, how="vertical").unique()

    # drop spent seasons (draft already happened): keep season > cutoff_season
    universe = (
        universe
        .join(current_meta.select("league_id", "cutoff_season"), on="league_id", how="left")
        .with_columns(pl.col("season").cast(pl.Int64).alias("_s"))
        .filter(pl.col("cutoff_season").is_null() | (pl.col("_s") > pl.col("cutoff_season")))
        .select(_PICK_KEY)
    )
    return universe


def resolve_pick_ownership(
    traded_picks_df: pl.DataFrame,
    txn_draft_picks_df: pl.DataFrame,
    overrides_df: pl.DataFrame,
    leagues_df: pl.DataFrame,
    drafts_df: pl.DataFrame | None = None,
    years_ahead: int = 3,
) -> pl.DataFrame:
    """Resolve the current owner roster for every pick in the universe.

    Every source's ``league_id`` is first translated to its lineage's *current*
    league (see :func:`build_lineage_map`), so a pick referenced under any
    season's league_id collapses onto the league we actually track. The universe
    of picks that should exist is then enumerated deterministically by
    :func:`build_pick_universe` (every league x the next ``years_ahead`` undrafted
    seasons x ``1..rookie_rounds`` x ``1..total_rosters``), so untraded picks are
    still accounted for. Ownership is overlaid in precedence order (highest wins):
    commissioner override -> transaction event log -> Sleeper traded state ->
    original owner.

    The "draft has not happened" cutoff prefers per-season draft *status* from
    ``drafts_df`` (a draft completes months before its league does); without it,
    it falls back to the lineage's latest completed *league* season. ``rounds`` is
    read per-league from ``drafts_df`` (a league setting, not assumed).

    Returns one row per pick with columns ``_PICK_KEY + [owner_roster_id]`` where
    ``league_id`` is the current league of the lineage.
    """
    lineage_map = build_lineage_map(leagues_df)
    l2c = lineage_map.select("league_id", "current_league_id")

    def _relabel(df):
        """Translate a source's league_id to its lineage's current league_id."""
        if df is None or df.height == 0:
            return df
        return (
            df.with_columns(pl.col("league_id").cast(pl.Utf8))
            .join(l2c, on="league_id", how="left")
            .filter(pl.col("current_league_id").is_not_null())
            .with_columns(pl.col("current_league_id").alias("league_id"))
            .drop("current_league_id")
        )

    traded_c = _relabel(traded_picks_df)
    txn_c = _relabel(txn_draft_picks_df)
    ovr_c = _relabel(overrides_df)

    # current-league metadata keyed by the current league_id, with the season
    # cutoff for "draft already happened" (draft status preferred, league status
    # as fallback).
    current_meta = lineage_map.select(
        pl.col("current_league_id").alias("league_id"),
        pl.col("current_total_rosters").alias("total_rosters"),
        "latest_completed_season",
    ).unique(subset=["league_id"], keep="first")

    cutoffs = _drafted_cutoffs(drafts_df, lineage_map)
    if cutoffs is not None:
        current_meta = current_meta.join(cutoffs, on="league_id", how="left").with_columns(
            pl.coalesce(["latest_drafted_season", "latest_completed_season"]).alias("cutoff_season")
        )
    else:
        current_meta = current_meta.with_columns(
            pl.col("latest_completed_season").alias("cutoff_season")
        )

    rounds_map = _rookie_rounds(drafts_df, lineage_map)
    if rounds_map is not None:
        current_meta = current_meta.join(rounds_map, on="league_id", how="left")
    else:
        current_meta = current_meta.with_columns(pl.lit(None, dtype=pl.Int64).alias("rookie_rounds"))

    # --- observed (current_league, season, round) across every pick source ---
    def _lsr(df, season="season", rnd="round"):
        if df is None or df.height == 0:
            return pl.DataFrame(schema={"league_id": pl.Utf8, "season": pl.Utf8, "round": pl.Int64})
        return df.select(
            pl.col("league_id").cast(pl.Utf8),
            pl.col(season).cast(pl.Utf8).alias("season"),
            pl.col(rnd).cast(pl.Int64).alias("round"),
        )

    observed = pl.concat(
        [_lsr(traded_c), _lsr(txn_c), _lsr(ovr_c)],
        how="vertical",
    ).unique()

    # --- universe: every pick that should exist (deterministic grid of the
    #     rolling future-pick window), so untraded picks are still accounted ---
    universe = build_pick_universe(observed, current_meta, years_ahead=years_ahead)

    if universe.height == 0:
        return pl.DataFrame(schema={
            "league_id": pl.Utf8, "season": pl.Utf8, "round": pl.Int64,
            "original_roster_id": pl.Int64, "owner_roster_id": pl.Int64,
        })

    # --- overlay sources (relabeled), each deduped to one owner per pick key ---
    traded = (
        _norm_pick_keys(traded_c, "original_roster_id")
        .sort("timestamp") if (traded_c is not None and traded_c.height) else None
    )
    if traded is not None and traded.height:
        traded = (
            traded.unique(subset=_PICK_KEY, keep="last")
            .select(_PICK_KEY + [pl.col("owner_roster_id").cast(pl.Int64).alias("traded_owner")])
        )
    else:
        traded = pl.DataFrame(schema={**{k: universe.schema[k] for k in _PICK_KEY}, "traded_owner": pl.Int64})

    if txn_c is not None and txn_c.height:
        sort_col = "load_date" if "load_date" in txn_c.columns else None
        txn = _norm_pick_keys(txn_c, "roster_id")
        txn = txn.sort(sort_col) if sort_col else txn
        txn = (
            txn.unique(subset=_PICK_KEY, keep="last")
            .select(_PICK_KEY + [pl.col("owner_id").cast(pl.Int64).alias("txn_owner")])
        )
    else:
        txn = pl.DataFrame(schema={**{k: universe.schema[k] for k in _PICK_KEY}, "txn_owner": pl.Int64})

    if ovr_c is not None and ovr_c.height:
        ovr = _norm_pick_keys(ovr_c, "roster_id")
        ovr = ovr.sort("created") if "created" in ovr.columns else ovr
        ovr = (
            ovr.unique(subset=_PICK_KEY, keep="last")
            .select(_PICK_KEY + [pl.col("to_team_id").cast(pl.Int64).alias("override_owner")])
        )
    else:
        ovr = pl.DataFrame(schema={**{k: universe.schema[k] for k in _PICK_KEY}, "override_owner": pl.Int64})

    resolved = (
        universe
        .join(traded, on=_PICK_KEY, how="left")
        .join(txn, on=_PICK_KEY, how="left")
        .join(ovr, on=_PICK_KEY, how="left")
        # precedence (highest first): override > txn event > traded state > original
        .with_columns(
            pl.coalesce([
                pl.col("override_owner"),
                pl.col("txn_owner"),
                pl.col("traded_owner"),
                pl.col("original_roster_id"),
            ]).cast(pl.Int64).alias("owner_roster_id")
        )
        .select(_PICK_KEY + ["owner_roster_id"])
    )
    return resolved


def build_pick_membership(
    resolved_picks: pl.DataFrame,
    dim_franchise: pl.DataFrame,
) -> pl.DataFrame:
    """Attach the owning franchise and a stable asset_id to resolved picks."""
    franchise_lookup = dim_franchise.select(
        pl.col("league_id").cast(pl.Utf8),
        pl.col("roster_id").cast(pl.Int64),
        "franchise_id",
    ).unique(subset=["league_id", "roster_id"], keep="first")

    out = (
        resolved_picks
        .join(
            franchise_lookup,
            left_on=["league_id", "owner_roster_id"],
            right_on=["league_id", "roster_id"],
            how="left",
        )
        .with_columns(
            pl.lit("pick").alias("asset_type"),
            pl.concat_str([
                pl.col("season"),
                pl.col("round").cast(pl.Utf8),
                pl.col("original_roster_id").cast(pl.Utf8),
            ], separator=":").alias("asset_id"),
            pl.col("owner_roster_id").alias("roster_id"),
            pl.col("season").alias("pick_season"),
            pl.col("round").alias("pick_round"),
            pl.col("original_roster_id").alias("pick_original_roster_id"),
            # player-only fields are null for picks
            pl.lit(None, dtype=pl.Utf8).alias("player_key"),
            pl.lit(None, dtype=pl.Utf8).alias("position"),
            pl.lit(None, dtype=pl.Boolean).alias("is_starter"),
            pl.lit(None, dtype=pl.Boolean).alias("is_taxi"),
            pl.lit(None, dtype=pl.Boolean).alias("is_reserve"),
        )
    )
    return out.select(_FACT_COLS)


# -------------------------------------------------------------------------
# RECONCILIATION
# -------------------------------------------------------------------------
def reconcile(
    player_membership: pl.DataFrame,
    pick_membership: pl.DataFrame,
    leagues_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Enforce conservation invariants; return ``(fact_df, quarantine_df)``.

    Hard violations (an asset owned by two franchises, or an asset that resolved
    to no franchise) are removed from the fact and quarantined. Players present
    in the snapshot but missing from the player dim (no ``player_key``) keep their
    fact row but are also logged to quarantine as a dim-coverage warning.
    """
    fact_all = pl.concat([player_membership, pick_membership], how="diagonal_relaxed")

    quarantine_parts: list[pl.DataFrame] = []

    def _flag(df: pl.DataFrame, reason: str) -> pl.DataFrame:
        return df.with_columns(pl.lit(reason).alias("quarantine_reason"))

    # 1. Duplicate ownership: an asset resolving to >1 franchise within a league.
    dup_keys = (
        fact_all.group_by(["league_id", "asset_type", "asset_id"])
        .agg(pl.col("franchise_id").n_unique().alias("n_owners"))
        .filter(pl.col("n_owners") > 1)
        .select(["league_id", "asset_type", "asset_id"])
    )
    dup_rows = fact_all.join(dup_keys, on=["league_id", "asset_type", "asset_id"], how="inner")
    if dup_rows.height:
        quarantine_parts.append(_flag(dup_rows, "duplicate_ownership"))

    # 2. Unresolved franchise: ownership resolved to a roster with no franchise.
    null_fr = fact_all.filter(pl.col("franchise_id").is_null())
    if null_fr.height:
        quarantine_parts.append(_flag(null_fr, "unresolved_franchise"))

    # 3. Unmapped player: in the snapshot but absent from the player dim (warn only).
    unmapped = fact_all.filter(
        (pl.col("asset_type") == "player") & (pl.col("player_key").is_null())
    )
    if unmapped.height:
        quarantine_parts.append(_flag(unmapped, "unmapped_player"))

    # Hard-violation rows are excluded from the fact; warnings are kept.
    bad = pl.concat([dup_rows, null_fr], how="diagonal_relaxed") if (dup_rows.height or null_fr.height) else None
    if bad is not None and bad.height:
        fact_df = fact_all.join(
            bad.select(["league_id", "asset_type", "asset_id"]).unique(),
            on=["league_id", "asset_type", "asset_id"], how="anti",
        )
    else:
        fact_df = fact_all

    if quarantine_parts:
        quarantine_df = pl.concat(quarantine_parts, how="diagonal_relaxed")
    else:
        quarantine_df = fact_all.clear().with_columns(pl.lit(None, dtype=pl.Utf8).alias("quarantine_reason"))

    return fact_df, quarantine_df


# -------------------------------------------------------------------------
# IO / MAIN
# -------------------------------------------------------------------------
def _read_prefix_concat(bucket_name: str, prefix: str) -> pl.DataFrame:
    """Read and vertically concat every parquet under a bronze prefix (used for
    sources partitioned by something other than load_date, e.g. league_id)."""
    from google.cloud import storage

    client = storage.Client()
    blobs = [
        b.name for b in client.bucket(bucket_name).list_blobs(prefix=prefix)
        if b.name.endswith(".parquet")
    ]
    if not blobs:
        return pl.DataFrame()
    frames = [pl.read_parquet(f"gs://{bucket_name}/{name}") for name in blobs]
    return pl.concat(frames, how="diagonal_relaxed")


def _latest_file(bucket_name: str, prefix: str) -> str | None:
    """Return the gs:// path of the lexically-latest parquet directly under a
    prefix (handles load_date=<date>.parquet style files)."""
    from google.cloud import storage

    client = storage.Client()
    names = [
        b.name for b in client.bucket(bucket_name).list_blobs(prefix=prefix)
        if b.name.endswith(".parquet")
    ]
    if not names:
        return None
    return f"gs://{bucket_name}/{max(names)}"


def _snapshot_date_from_path(path: str) -> str:
    if "load_date=" in path:
        return path.split("load_date=")[1].split("/")[0].replace(".parquet", "")
    return datetime.now().strftime("%Y-%m-%d")


# -------------------------------------------------------------------------
# SCD2 LEDGER  (gaps-and-islands over daily presence)
# -------------------------------------------------------------------------
def build_snapshot_intervals(present: pl.DataFrame, key_cols: list[str],
                             date_col: str = "snapshot_date") -> pl.DataFrame:
    """Collapse per-day presence rows into SCD2 intervals.

    ``present`` is a long frame (``key_cols`` + ``date_col``), one row per asset that
    is present on a given snapshot day. Returns one row per contiguous holding stint:
    ``key_cols + [valid_from, valid_to, is_current]`` where the interval is
    ``[valid_from, valid_to)``. A stint ends at the snapshot date the asset is first
    absent; a stint that reaches the latest snapshot is ``is_current=True`` with
    ``valid_to = None``. "Contiguous" means consecutive *snapshot* dates (so gaps in
    snapshot collection don't split a stint), via gaps-and-islands.
    """
    schema_out = {**{k: present.schema.get(k, pl.Utf8) for k in key_cols},
                  "valid_from": pl.Utf8, "valid_to": pl.Utf8, "is_current": pl.Boolean}
    if present.height == 0:
        return pl.DataFrame(schema=schema_out)

    dates = present.select(pl.col(date_col).cast(pl.Utf8)).unique().sort(date_col).with_row_index("didx")
    max_didx = dates["didx"].max()

    p = (
        present.with_columns(pl.col(date_col).cast(pl.Utf8))
        .join(dates, on=date_col, how="left")
        .sort(key_cols + ["didx"])
        # consecutive didx within a key share an island id (didx - running count)
        .with_columns((pl.col("didx") - pl.col("didx").cum_count().over(key_cols)).alias("_isl"))
    )
    iv = p.group_by(key_cols + ["_isl"]).agg(
        pl.col(date_col).min().alias("valid_from"),
        pl.col("didx").max().alias("_last"),
    )
    next_date = dates.select(pl.col("didx").alias("_nd"), pl.col(date_col).alias("_to"))
    iv = (
        iv.with_columns((pl.col("_last") + 1).alias("_nd"))
        .join(next_date, on="_nd", how="left")
        .with_columns(
            pl.col("_to").alias("valid_to"),
            (pl.col("_last") == max_didx).alias("is_current"),
        )
    )
    return iv.select(key_cols + ["valid_from", "valid_to", "is_current"])


def build_event_intervals(events: pl.DataFrame, key_cols: list[str],
                          ts_col: str = "ts", date_col: str = "date",
                          action_col: str = "action") -> pl.DataFrame:
    """Build SCD2 holding intervals from an add/drop event stream.

    For each key, order events by ``ts_col`` (ms; ``action_col`` drop-before-add on
    ties). An asset is "held" after an add until the next drop; consecutive adds with
    no drop between stay one stint. Emits ``key_cols + [valid_from, valid_to,
    is_open]`` (interval ``[valid_from, valid_to)``); a still-held stint has
    ``valid_to = None`` and ``is_open = True``. Used to reconstruct history before
    the daily-snapshot era; the snapshot era overrides it from the anchor forward.
    """
    schema_out = {**{k: events.schema.get(k, pl.Utf8) for k in key_cols},
                  "valid_from": pl.Utf8, "valid_to": pl.Utf8, "is_open": pl.Boolean}
    if events.height == 0:
        return pl.DataFrame(schema=schema_out)

    e = (
        events
        .with_columns(
            pl.col(date_col).cast(pl.Utf8),
            (pl.col(action_col) == "add").alias("_add"),
            pl.when(pl.col(action_col) == "add").then(1).otherwise(0).alias("_tb"),  # drop<add on ties
        )
        .sort(key_cols + [ts_col, "_tb"])
        .with_columns(pl.col("_add").shift(1).over(key_cols).alias("_prev"))
    )
    # transitions: stint starts (not-held -> add) and ends (held -> drop)
    starts = (
        e.filter(pl.col("_add") & (pl.col("_prev").is_null() | ~pl.col("_prev")))
        .select(key_cols + [pl.col(date_col).alias("valid_from")])
        .with_columns(pl.col("valid_from").cum_count().over(key_cols).alias("_rank"))
    )
    ends = (
        e.filter(~pl.col("_add") & pl.col("_prev").fill_null(False))
        .select(key_cols + [pl.col(date_col).alias("valid_to")])
        .with_columns(pl.col("valid_to").cum_count().over(key_cols).alias("_rank"))
    )
    iv = (
        starts.join(ends, on=key_cols + ["_rank"], how="left")
        .with_columns(pl.col("valid_to").is_null().alias("is_open"))
    )
    return iv.select(key_cols + ["valid_from", "valid_to", "is_open"])


def combine_eras(present: pl.DataFrame, events: pl.DataFrame, boundary: str,
                 key_cols: list[str]) -> pl.DataFrame:
    """Merge the snapshot era (>= boundary, authoritative) with the reconstructed
    era (< boundary, from events) into one SCD2 interval set.

    The snapshot intervals own ``[boundary, present]``; reconstructed intervals are
    truncated to end at ``boundary`` so the two eras don't overlap. Adjacent
    intervals are left un-stitched (a continuously-held asset shows a boundary split
    at ``boundary``) — correct for as-of queries; stitching is a later refinement.
    """
    snap = build_snapshot_intervals(present, key_cols).select(
        key_cols + ["valid_from", "valid_to", "is_current"]
    )
    recon = build_event_intervals(events, key_cols)
    recon = (
        recon
        .filter(pl.col("valid_from") < boundary)
        .with_columns(
            pl.when(pl.col("valid_to").is_null() | (pl.col("valid_to") > boundary))
              .then(pl.lit(boundary)).otherwise(pl.col("valid_to")).alias("valid_to"),
            pl.lit(False).alias("is_current"),
        )
        .filter(pl.col("valid_from") < pl.col("valid_to"))
        .select(key_cols + ["valid_from", "valid_to", "is_current"])
    )
    return pl.concat([recon, snap], how="vertical")


def reconstruct_pick_intervals(lifecycle: pl.DataFrame, trades: pl.DataFrame) -> pl.DataFrame:
    """Reconstruct historical pick-ownership SCD2 intervals from synthesized minting
    + trades.

    ``lifecycle`` (one row per pick): ``franchise_id`` (the ORIGINAL owner's franchise),
    ``pick_id``, ``mint_ts``/``mint_date`` (pick created), ``consume_date`` (its draft;
    None if not drafted yet). ``trades`` (one row per ownership change): ``pick_id``,
    ``prev_franchise``, ``new_franchise``, ``ts``, ``date``.

    Each pick becomes add/drop events — mint=add(original); trade=drop(prev)+add(new) —
    fed through :func:`build_event_intervals`; a still-open final interval is then capped
    at the pick's ``consume_date`` (drafted -> pick ceases to exist).
    """
    mint_ev = lifecycle.select(
        "franchise_id", "pick_id",
        pl.col("mint_ts").alias("ts"), pl.col("mint_date").alias("date"),
        pl.lit("add").alias("action"),
    )
    drop_ev = trades.select(
        pl.col("prev_franchise").alias("franchise_id"), "pick_id", "ts", "date",
        pl.lit("drop").alias("action"),
    )
    add_ev = trades.select(
        pl.col("new_franchise").alias("franchise_id"), "pick_id", "ts", "date",
        pl.lit("add").alias("action"),
    )
    events = pl.concat([mint_ev, drop_ev, add_ev], how="vertical_relaxed")

    iv = build_event_intervals(events, ["franchise_id", "pick_id"])  # valid_from, valid_to, is_open

    # consume_date must be keyed by the LINEAGE's pick, not pick_id alone:
    # pick_id ("season:round:original_roster") COLLIDES across lineages, so joining on
    # pick_id alone would apply one lineage's draft date to every lineage's matching pick
    # (and which one wins is row-order-dependent -> non-deterministic). Scope by lineage,
    # which is the prefix of franchise_id ("<lineage>_<roster>").
    _lin = pl.col("franchise_id").str.split("_").list.get(0).alias("_lin")
    consume = (
        lifecycle.with_columns(_lin).select("_lin", "pick_id", "consume_date")
        .unique(subset=["_lin", "pick_id"], keep="first")
    )
    iv = (
        iv.with_columns(_lin)
        .join(consume, on=["_lin", "pick_id"], how="left")
        .with_columns(
            # a still-open interval whose pick has been drafted ends at the draft
            pl.when(pl.col("is_open") & pl.col("consume_date").is_not_null())
              .then(pl.col("consume_date"))
              .otherwise(pl.col("valid_to"))
              .alias("valid_to")
        )
        .filter(pl.col("valid_to").is_null() | (pl.col("valid_from") < pl.col("valid_to")))
        .with_columns(pl.col("valid_to").is_null().alias("is_current"))
    )
    return iv.select("franchise_id", "pick_id", "valid_from", "valid_to", "is_current")


def _lineage_franchise(df: pl.DataFrame, lineage_map: pl.DataFrame) -> pl.DataFrame:
    """Attach franchise_id = ``<lineage_id>_<roster_id>`` (matches dim_franchises_meta),
    mapping each row's league_id onto its dynasty lineage so the snapshot era (current
    league_id) and the reconstructed era (historical league_ids) share one key."""
    return (
        df.with_columns(pl.col("league_id").cast(pl.Utf8))
        .join(lineage_map, on="league_id", how="inner")
        .with_columns(
            pl.concat_str([pl.col("league_lineage_id"), pl.col("roster_id").cast(pl.Utf8)],
                          separator="_").alias("franchise_id")
        )
    )


def _read_player_presence(bucket_name: str, lineage_map: pl.DataFrame) -> pl.DataFrame:
    """All daily roster_players snapshots -> (franchise_id, player_id, snapshot_date)."""
    from google.cloud import storage
    client = storage.Client()
    names = sorted(
        bl.name for bl in client.bucket(bucket_name).list_blobs(
            prefix="bronze/sleeper/rosters/roster_players/daily/")
        if bl.name.endswith(".parquet")
    )
    frames = []
    for n in names:
        d = n.split("load_date=")[1].split("/")[0]
        df = pl.read_parquet(f"gs://{bucket_name}/{n}").select(
            pl.col("league_id").cast(pl.Utf8), pl.col("roster_id").cast(pl.Int64),
            pl.col("player_id").cast(pl.Utf8),
        ).with_columns(pl.lit(d).alias("snapshot_date"))
        frames.append(df)
    present = pl.concat(frames, how="vertical")
    return _lineage_franchise(present, lineage_map).select("franchise_id", "player_id", "snapshot_date")


def _read_player_events(bucket_name: str, lineage_map: pl.DataFrame) -> pl.DataFrame:
    """Deduped add/drop event stream (drafts + transactions, full_load UNION daily) ->
    (franchise_id, player_id, ts, date, action)."""
    def both(sub, cols):
        return pl.concat([
            _read_prefix_concat(bucket_name, f"bronze/sleeper/transactions/{sub}/full_load/").select(cols)
            if _read_prefix_concat(bucket_name, f"bronze/sleeper/transactions/{sub}/full_load/").height else pl.DataFrame(schema={c: pl.Utf8 for c in cols}),
            _read_prefix_concat(bucket_name, f"bronze/sleeper/transactions/{sub}/daily/").select(cols)
            if _read_prefix_concat(bucket_name, f"bronze/sleeper/transactions/{sub}/daily/").height else pl.DataFrame(schema={c: pl.Utf8 for c in cols}),
        ], how="diagonal_relaxed")

    tx = both("transactions", ["transaction_id", "created", "status"]).unique("transaction_id").filter(pl.col("status") == "complete")
    tp = both("transaction_players", ["transaction_id", "league_id", "player_id", "roster_id", "action"]).unique(["transaction_id", "player_id", "roster_id", "action"])
    txn = (
        tp.join(tx.select("transaction_id", "created"), on="transaction_id", how="inner")
        .with_columns(
            pl.col("created").cast(pl.Int64).alias("ts"),
            pl.from_epoch(pl.col("created").cast(pl.Int64), time_unit="ms").dt.date().cast(pl.Utf8).alias("date"),
        )
    )
    # draft adds
    drafts = _read_prefix_concat(bucket_name, "bronze/sleeper/drafts/drafts/").unique("draft_id").select("draft_id", "league_id", "start_time")
    dpk = (
        _read_prefix_concat(bucket_name, "bronze/sleeper/drafts/draft_picks/").unique(["draft_id", "pick_no"])
        .select("draft_id", "player_id", "roster_id")
        .join(drafts, on="draft_id", how="inner")
        .with_columns(
            pl.col("start_time").cast(pl.Int64).alias("ts"),
            pl.from_epoch(pl.col("start_time").cast(pl.Int64), time_unit="ms").dt.date().cast(pl.Utf8).alias("date"),
            pl.lit("add").alias("action"),
        )
    )
    ev = pl.concat([
        txn.select("league_id", "roster_id", "player_id", "ts", "date", "action"),
        dpk.select("league_id", "roster_id", "player_id", "ts", "date", "action"),
    ], how="vertical_relaxed").with_columns(pl.col("roster_id").cast(pl.Int64), pl.col("player_id").cast(pl.Utf8))
    return _lineage_franchise(ev, lineage_map).select("franchise_id", "player_id", "ts", "date", "action")


def _read_pick_presence(bucket_name: str, lineage_map: pl.DataFrame, leagues_df: pl.DataFrame,
                        overrides_df: pl.DataFrame, drafts_df) -> pl.DataFrame:
    """Per-day pick ownership over the daily traded_picks snapshots ->
    (franchise_id, pick_id, snapshot_date).

    Each day's traded_picks IS Sleeper's net pick state for that day, so we resolve
    ownership as-of that day with an EMPTY txn event log (no future-trade leakage);
    the resolver still applies the deterministic future-pick universe + draft cutoff.
    The pick accrues to the OWNER's franchise (lineage + owner_roster_id)."""
    from google.cloud import storage
    client = storage.Client()
    names = sorted(
        bl.name for bl in client.bucket(bucket_name).list_blobs(
            prefix="bronze/sleeper/rosters/traded_picks/daily/")
        if bl.name.endswith(".parquet")
    )
    empty_txn = pl.DataFrame()
    frames = []
    for n in names:
        d = n.split("load_date=")[1].split("/")[0]
        traded = pl.read_parquet(f"gs://{bucket_name}/{n}")
        resolved = resolve_pick_ownership(traded, empty_txn, overrides_df, leagues_df, drafts_df)
        if resolved.height == 0:
            continue
        df = (
            resolved.with_columns(pl.col("league_id").cast(pl.Utf8))
            .join(lineage_map, on="league_id", how="inner")
            .with_columns(
                pl.concat_str([pl.col("league_lineage_id"), pl.col("owner_roster_id").cast(pl.Utf8)],
                              separator="_").alias("franchise_id"),
                pl.concat_str([pl.col("season").cast(pl.Utf8), pl.col("round").cast(pl.Utf8),
                               pl.col("original_roster_id").cast(pl.Utf8)], separator=":").alias("pick_id"),
                pl.lit(d).alias("snapshot_date"),
            )
            .select("franchise_id", "pick_id", "snapshot_date")
        )
        frames.append(df)
    return pl.concat(frames, how="vertical") if frames else pl.DataFrame(
        schema={"franchise_id": pl.Utf8, "pick_id": pl.Utf8, "snapshot_date": pl.Utf8})


def _read_pick_trade_events(bucket_name: str, lineage_map: pl.DataFrame) -> pl.DataFrame:
    """Unify pick ownership-change events with the CORRECT new/prev owner per source.

    DATA QUIRK: `transactions/draft_picks/full_load` (GraphQL dump) has from/to SWAPPED
    — its `from_team_id` is the NEW owner. The `daily` feed (`owner_id`) and
    `commission_overrides` (`to_team_id`) are correct. Returns one row per ownership
    change: ``pick_id, prev_franchise, new_franchise, ts, date`` (lineage-keyed)."""
    txns = pl.concat([
        _read_prefix_concat(bucket_name, "bronze/sleeper/transactions/transactions/full_load/")
            .select("transaction_id", "created", "status"),
        _read_prefix_concat(bucket_name, "bronze/sleeper/transactions/transactions/daily/")
            .select("transaction_id", "created", "status"),
    ], how="vertical_relaxed").unique("transaction_id").filter(pl.col("status") == "complete")

    # full_load: corrected schema uses owner_id (new); legacy data (before the
    # ingestion fix) used from_team_id as the new owner (labels were swapped).
    fl = _read_prefix_concat(bucket_name, "bronze/sleeper/transactions/draft_picks/full_load/")
    if fl.height:
        if "owner_id" in fl.columns:                       # corrected schema
            new_col, prev_col = "owner_id", "previous_owner_id"
        else:                                              # legacy swapped schema
            new_col, prev_col = "from_team_id", "to_team_id"
        fl = fl.select(
            "transaction_id", "league_id", "season", pl.col("round").cast(pl.Int64),
            pl.col("roster_id").cast(pl.Int64), pl.col(new_col).cast(pl.Int64).alias("new_owner"),
            pl.col(prev_col).cast(pl.Int64).alias("prev_owner"),
        )
    # daily: NEW owner = owner_id, prev = previous_owner_id
    dl = _read_prefix_concat(bucket_name, "bronze/sleeper/transactions/draft_picks/daily/")
    dl = dl.select(
        "transaction_id", "league_id", "season", pl.col("round").cast(pl.Int64),
        pl.col("roster_id").cast(pl.Int64), pl.col("owner_id").cast(pl.Int64).alias("new_owner"),
        pl.col("previous_owner_id").cast(pl.Int64).alias("prev_owner"),
    ) if dl.height else dl
    txn_picks = pl.concat([f for f in (fl, dl) if f.height], how="vertical_relaxed")
    txn_picks = (
        txn_picks.unique(["transaction_id", "season", "round", "roster_id"])
        .join(txns.select("transaction_id", "created"), on="transaction_id", how="inner")
        .with_columns(pl.col("created").cast(pl.Int64).alias("ts"))
        .drop("transaction_id")
    ) if txn_picks.height else pl.DataFrame()

    # commission_overrides: NEW owner = to_team_id, prev = from_team_id
    ov_path = _latest_file(bucket_name, "bronze/sleeper/transactions/commission_overrides/")
    if ov_path:
        ov = pl.read_parquet(ov_path).select(
            "league_id", "season", pl.col("round").cast(pl.Int64), pl.col("roster_id").cast(pl.Int64),
            pl.col("to_team_id").cast(pl.Int64).alias("new_owner"),
            pl.col("from_team_id").cast(pl.Int64).alias("prev_owner"),
            pl.col("created").cast(pl.Int64).alias("ts"),
        )
    else:
        ov = pl.DataFrame()

    allev = pl.concat([e for e in (txn_picks, ov) if e.height], how="diagonal_relaxed")
    if allev.height == 0:
        return pl.DataFrame(schema={"pick_id": pl.Utf8, "prev_franchise": pl.Utf8,
                                    "new_franchise": pl.Utf8, "ts": pl.Int64, "date": pl.Utf8})
    return (
        allev.with_columns(pl.col("league_id").cast(pl.Utf8))
        .join(lineage_map, on="league_id", how="inner")
        .with_columns(
            pl.concat_str([pl.col("season").cast(pl.Utf8), pl.col("round").cast(pl.Utf8),
                           pl.col("roster_id").cast(pl.Utf8)], separator=":").alias("pick_id"),
            pl.concat_str([pl.col("league_lineage_id"), pl.col("prev_owner").cast(pl.Utf8)],
                          separator="_").alias("prev_franchise"),
            pl.concat_str([pl.col("league_lineage_id"), pl.col("new_owner").cast(pl.Utf8)],
                          separator="_").alias("new_franchise"),
            pl.from_epoch(pl.col("ts"), time_unit="ms").dt.date().cast(pl.Utf8).alias("date"),
        )
        .select("pick_id", "prev_franchise", "new_franchise", "ts", "date")
    )


def synthesize_pick_lifecycle(drafts_df: pl.DataFrame, leagues_df: pl.DataFrame,
                              lineage_map: pl.DataFrame, rounds_df: pl.DataFrame,
                              years_out: int = 3) -> pl.DataFrame:
    """Synthesize every pick's mint/consume (no creation transaction exists).

    Sleeper mints, at each draft completion, the season-``(D+years_out)`` rookie picks
    (rounds ``1..R``) for every roster; they are consumed at their own season's draft.
    Returns one row per pick: ``franchise_id`` (original owner), ``pick_id``,
    ``mint_ts``/``mint_date``, ``consume_date`` (None if its draft hasn't happened)."""
    # draft completions per lineage/season (max ts per season picks the real rookie draft)
    comp = (
        drafts_df.unique("draft_id")
        .with_columns(pl.col("league_id").cast(pl.Utf8))
        .join(lineage_map, on="league_id", how="inner")
        .with_columns(
            pl.col("season").cast(pl.Int64).alias("dseason"),
            pl.coalesce([pl.col("last_picked"), pl.col("start_time")]).cast(pl.Int64).alias("dts"),
        )
        .group_by(["league_lineage_id", "dseason"]).agg(pl.col("dts").max().alias("dts"))
        .with_columns(pl.from_epoch("dts", time_unit="ms").dt.date().cast(pl.Utf8).alias("ddate"))
    )
    # per lineage: startup (first) season + roster count
    lin_meta = (
        leagues_df.select(
            pl.col("league_lineage_id"), pl.col("season").cast(pl.Int64).alias("season"),
            pl.col("total_rosters").cast(pl.Int64),
        )
        .group_by("league_lineage_id").agg(
            pl.col("season").min().alias("startup"),
            pl.col("total_rosters").max().alias("N"),
        )
    )
    latest = comp.group_by("league_lineage_id").agg(pl.col("dseason").max().alias("latest_draft"))
    R = rounds_df.select(pl.col("league_lineage_id"), pl.col("rounds").cast(pl.Int64).alias("R")).unique("league_lineage_id")
    meta = lin_meta.join(latest, on="league_lineage_id").join(R, on="league_lineage_id", how="left").with_columns(pl.col("R").fill_null(3))

    # pick universe per lineage: seasons (startup+1 .. latest_draft+years_out) x rounds x rosters
    uni = (
        meta.with_columns(pl.int_ranges(pl.col("startup") + 1, pl.col("latest_draft") + years_out + 1).alias("S"))
        .explode("S")
        .with_columns(pl.int_ranges(1, pl.col("R") + 1).alias("rnd")).explode("rnd")
        .with_columns(pl.int_ranges(1, pl.col("N") + 1).alias("roster")).explode("roster")
        .with_columns((pl.max_horizontal(pl.col("S") - years_out, pl.col("startup"))).alias("mint_season"))
    )
    # attach mint/consume completion dates
    mint = comp.select(pl.col("league_lineage_id"), pl.col("dseason").alias("mint_season"),
                       pl.col("ddate").alias("mint_date"), pl.col("dts").alias("mint_ts"))
    consume = comp.select(pl.col("league_lineage_id"), pl.col("dseason").alias("S"),
                          pl.col("ddate").alias("consume_date"))
    out = (
        uni.join(mint, on=["league_lineage_id", "mint_season"], how="inner")
        .join(consume, on=["league_lineage_id", "S"], how="left")
        .with_columns(
            pl.concat_str([pl.col("league_lineage_id"), pl.col("roster").cast(pl.Utf8)], separator="_").alias("franchise_id"),
            pl.concat_str([pl.col("S").cast(pl.Utf8), pl.col("rnd").cast(pl.Utf8), pl.col("roster").cast(pl.Utf8)], separator=":").alias("pick_id"),
        )
    )
    return out.select("franchise_id", "pick_id", "mint_ts", "mint_date", "consume_date")


def reconstruct_rollover_presence(present: pl.DataFrame, leagues_df: pl.DataFrame,
                                  bucket_name: str) -> pl.DataFrame:
    """Repair the season-rollover gap in PLAYER presence.

    When a lineage rolls to a new season, Sleeper keeps serving the OLD (completed) league, so
    the daily snapshot job records a FROZEN roster for it, while the NEW league's offseason
    (rookie draft + trades) goes unsnapshotted until its first real snapshot. The timeline then
    reads flat (frozen old roster) and then spikes (new roster lands all at once).

    We replace the stale window ``[R, S_new)`` -- R = the new league's first move, S_new = its
    first real snapshot -- with the new league's roster reconstructed by replaying its events
    (transactions + draft) forward from the carry-over (the frozen roster just before R),
    ORDERED BY the millisecond timestamp (drop-before-add on ties) so same-day add/drop
    round-trips net correctly. The real S_new snapshot then takes over (anchor to truth).
    Validated to reproduce the new league's first snapshot bar a couple of same-day round-trip
    fringe players. Returns corrected presence (franchise_id, player_id, snapshot_date)."""
    import datetime as _dt
    from collections import defaultdict
    from google.cloud import storage
    client = storage.Client()

    def _read(prefix):
        names = [b.name for b in client.bucket(bucket_name).list_blobs(prefix=prefix)
                 if b.name.endswith(".parquet")]
        return (pl.concat([pl.read_parquet(f"gs://{bucket_name}/{n}") for n in names],
                          how="diagonal_relaxed") if names else pl.DataFrame())

    drafts_all = _read("bronze/sleeper/drafts/drafts/")
    dpicks_all = _read("bronze/sleeper/drafts/draft_picks/")
    current = (
        leagues_df.with_columns(pl.col("season").cast(pl.Int64, strict=False).alias("_s"))
        .sort("_s").group_by("league_lineage_id")
        .agg(pl.col("league_id").last().cast(pl.Utf8).alias("cur_league"))
    )

    synth: list[tuple] = []
    windows: list[tuple] = []
    for row in current.iter_rows(named=True):
        lin, L, pref = row["league_lineage_id"], row["cur_league"], row["league_lineage_id"] + "_"
        dates = sorted(present.filter(pl.col("franchise_id").str.starts_with(pref))["snapshot_date"].unique().to_list())
        if len(dates) < 2:
            continue
        gaps = [(_dt.date.fromisoformat(dates[i + 1]) - _dt.date.fromisoformat(dates[i])).days
                for i in range(len(dates) - 1)]
        gi = max(range(len(gaps)), key=lambda i: gaps[i])
        if gaps[gi] < 3:               # no rollover gap -> nothing to reconstruct
            continue
        last_old, S_new = dates[gi], dates[gi + 1]   # old league's last snapshot; new league's first

        # current-league events (transactions add/drop) with millisecond ts -- MAY be empty
        # (e.g. a pre-draft new league with no offseason moves yet). Even then we must carry the
        # frozen roster forward across the gap, or this lineage alone is absent on the synthetic
        # days the OTHER lineages' reconstruction adds to the global date grid -> a spurious hole.
        tp = _read(f"bronze/sleeper/transactions/transaction_players/daily/league_id={L}/")
        tx = _read(f"bronze/sleeper/transactions/transactions/daily/league_id={L}/")
        if tp.is_empty() or tx.is_empty():
            ev = pl.DataFrame(schema={"ts": pl.Int64, "roster_id": pl.Int64,
                                      "player_id": pl.Utf8, "action": pl.Utf8})
        else:
            txc = tx.filter(pl.col("status") == "complete").select(
                "transaction_id", pl.col("created").cast(pl.Int64).alias("ts"))
            ev = tp.join(txc, on="transaction_id", how="inner").select(
                "ts", pl.col("roster_id").cast(pl.Int64), pl.col("player_id").cast(pl.Utf8), "action")
        # draft adds with the draft's ts so they interleave with same-day transactions
        dr = (drafts_all.filter(pl.col("league_id") == L)
              .select("draft_id", pl.coalesce([pl.col("start_time"), pl.col("last_picked")]).cast(pl.Int64).alias("ts"))
              .unique("draft_id"))
        if not dr.is_empty() and not dpicks_all.is_empty():
            dadd = dpicks_all.join(dr, on="draft_id", how="inner").select(
                "ts", pl.col("roster_id").cast(pl.Int64), pl.col("player_id").cast(pl.Utf8),
                pl.lit("add").alias("action"))
            ev = pl.concat([ev, dadd], how="vertical_relaxed")
        one = _dt.timedelta(days=1)
        if ev.is_empty():
            R = (_dt.date.fromisoformat(last_old) + one).isoformat()   # no events -> just bridge the gap
        else:
            ev = (ev.drop_nulls("ts")
                  .with_columns(pl.from_epoch(pl.col("ts"), time_unit="ms").dt.date().cast(pl.Utf8).alias("d"),
                                pl.when(pl.col("action") == "add").then(1).otherwise(0).alias("_tb"))
                  .sort(["ts", "_tb"]))        # ts asc; drop(0) before add(1) on identical ts
            R = ev["d"].min()
        if R >= S_new:
            continue
        pre = present.filter(pl.col("franchise_id").str.starts_with(pref) & (pl.col("snapshot_date") < R))
        if pre.is_empty():
            continue
        carry_date = pre["snapshot_date"].max()       # frozen roster just before the first move
        roster: dict = defaultdict(set)
        for x in pre.filter(pl.col("snapshot_date") == carry_date).iter_rows(named=True):
            roster[x["franchise_id"]].add(x["player_id"])

        by_day: dict = defaultdict(list)
        for e in ev.iter_rows(named=True):             # already ts-ordered
            by_day[e["d"]].append(e)
        d, end, one = _dt.date.fromisoformat(R), _dt.date.fromisoformat(S_new), _dt.timedelta(days=1)
        while d < end:
            ds = d.isoformat()
            for e in by_day.get(ds, []):
                fid = f"{lin}_{e['roster_id']}"
                if e["action"] == "add":
                    roster[fid].add(e["player_id"])
                else:
                    roster[fid].discard(e["player_id"])
            for fid, s in roster.items():
                for pid in s:
                    synth.append((fid, pid, ds))
            d += one
        windows.append((pref, R, S_new))

    if not synth:
        return present, []
    synth_df = pl.DataFrame({"franchise_id": [s[0] for s in synth],
                             "player_id": [s[1] for s in synth],
                             "snapshot_date": [s[2] for s in synth]})
    out = present
    for pref, R, S_new in windows:      # drop the stale frozen-old-league window
        out = out.filter(~(pl.col("franchise_id").str.starts_with(pref)
                           & (pl.col("snapshot_date") >= R) & (pl.col("snapshot_date") < S_new)))
    corrected = pl.concat([out, synth_df], how="vertical_relaxed").unique(["franchise_id", "player_id", "snapshot_date"])
    return corrected, windows


def reconstruct_rollover_pick_presence(pick_present: pl.DataFrame, recon_intervals: pl.DataFrame,
                                       windows: list) -> pl.DataFrame:
    """Apply the same rollover repair to PICK presence. The reconstructed pick intervals
    (`reconstruct_pick_intervals`, keyed on mint/consume/trades) already CONSUME each season's
    picks at that season's draft. We project them onto daily presence across each rollover
    window [R, S_new) and replace the stale frozen-old-league pick snapshots there -- so the new
    season's picks turn into rookies (drop) at the draft instead of lingering until the first
    new-league snapshot (which would double-count vs the reconstructed rookies)."""
    import datetime as _dt
    if not windows or recon_intervals is None or recon_intervals.is_empty():
        return pick_present
    synth: list[tuple] = []
    for pref, R, S_new in windows:
        rows = recon_intervals.filter(
            pl.col("franchise_id").str.starts_with(pref) & pl.col("valid_from").is_not_null()
        ).select("franchise_id", "pick_id", "valid_from", "valid_to").to_dicts()
        if not rows:
            continue
        d, end, one = _dt.date.fromisoformat(R), _dt.date.fromisoformat(S_new), _dt.timedelta(days=1)
        while d < end:
            ds = d.isoformat()
            for r in rows:
                if r["valid_from"] <= ds and (r["valid_to"] is None or r["valid_to"] > ds):
                    synth.append((r["franchise_id"], r["pick_id"], ds))
            d += one
    if not synth:
        return pick_present
    sdf = pl.DataFrame({"franchise_id": [s[0] for s in synth], "pick_id": [s[1] for s in synth],
                        "snapshot_date": [s[2] for s in synth]})
    out = pick_present
    for pref, R, S_new in windows:
        out = out.filter(~(pl.col("franchise_id").str.starts_with(pref)
                           & (pl.col("snapshot_date") >= R) & (pl.col("snapshot_date") < S_new)))
    return pl.concat([out, sdf], how="vertical_relaxed").unique(["franchise_id", "pick_id", "snapshot_date"])


def main():
    bucket_name = os.environ.get("GCS_BUCKET_NAME")

    leagues_df = pl.read_parquet(f"gs://{bucket_name}/silver/fantasy/dim_leagues_meta/data.parquet")
    lineage_map = leagues_df.select(
        pl.col("league_id").cast(pl.Utf8), pl.col("league_lineage_id").cast(pl.Utf8)
    ).unique()

    print("Reading daily roster snapshots...")
    present = _read_player_presence(bucket_name, lineage_map)
    print(f"  {present.height:,} player-days (raw)")
    print("Reconstructing season-rollover gaps from events (offseason draft + trades)...")
    present, rollover_windows = reconstruct_rollover_presence(present, leagues_df, bucket_name)
    boundary = present["snapshot_date"].min()
    print(f"  {present.height:,} player-days after rollover repair; snapshot era starts {boundary}")

    print("Reading event stream (drafts + transactions, full_load + daily)...")
    events = _read_player_events(bucket_name, lineage_map)
    print(f"  {events.height:,} add/drop events; {events['date'].min()} -> {events['date'].max()}")

    print("Building SCD2 player ledger (snapshot era + reconstructed era)...")
    player_ledger = combine_eras(present, events, boundary, ["franchise_id", "player_id"]).with_columns(
        pl.lit("player").alias("asset_type"),
        pl.col("player_id").alias("asset_id"),
    ).select("franchise_id", "asset_type", "asset_id", "valid_from", "valid_to", "is_current")

    # --- picks (asset_type='pick'): full history. Snapshot era from the traded_picks
    #     snapshots; reconstructed era from synthesized minting + corrected trade events. ---
    overrides_path = _latest_file(bucket_name, "bronze/sleeper/transactions/commission_overrides/")
    overrides_df = pl.read_parquet(overrides_path) if overrides_path else pl.DataFrame()
    try:
        drafts_df = pl.read_parquet(get_latest_bronze_path(bucket_name, "drafts/drafts", source="sleeper"))
    except ValueError:
        drafts_df = None

    print("Resolving per-day pick ownership over traded_picks snapshots...")
    pick_present = _read_pick_presence(bucket_name, lineage_map, leagues_df, overrides_df, drafts_df)

    print("Reconstructing pre-snapshot pick ownership (mint/consume + trades)...")
    rounds_df = (
        drafts_df.unique("draft_id").with_columns(pl.col("league_id").cast(pl.Utf8))
        .join(lineage_map, on="league_id", how="inner")
        .filter(pl.col("status") == "complete").with_columns(pl.col("season").cast(pl.Int64))
        .sort("season").group_by("league_lineage_id").agg(pl.col("rounds").last().alias("rounds"))
    )
    lifecycle = synthesize_pick_lifecycle(drafts_df, leagues_df, lineage_map, rounds_df, years_out=3)
    trades = _read_pick_trade_events(bucket_name, lineage_map)
    recon_pick_full = reconstruct_pick_intervals(lifecycle, trades)
    # rollover repair: in the offseason window, replace stale frozen-old-league pick snapshots
    # with the reconstructed holdings so the new season's picks consume at the draft.
    pick_present = reconstruct_rollover_pick_presence(pick_present, recon_pick_full, rollover_windows)
    snap_pick = build_snapshot_intervals(pick_present, ["franchise_id", "pick_id"])
    # truncate reconstruction at the snapshot boundary (snapshots own >= boundary)
    recon_pick = (
        recon_pick_full.filter(pl.col("valid_from") < boundary)
        .with_columns(
            pl.when(pl.col("valid_to").is_null() | (pl.col("valid_to") > boundary))
              .then(pl.lit(boundary)).otherwise(pl.col("valid_to")).alias("valid_to"),
            pl.lit(False).alias("is_current"),
        )
        .filter(pl.col("valid_from") < pl.col("valid_to"))
        .select("franchise_id", "pick_id", "valid_from", "valid_to", "is_current")
    )

    pick_ledger = pl.concat([recon_pick, snap_pick], how="vertical").with_columns(
        pl.lit("pick").alias("asset_type"),
        pl.col("pick_id").alias("asset_id"),
    ).select("franchise_id", "asset_type", "asset_id", "valid_from", "valid_to", "is_current")
    print(f"  player intervals: {player_ledger.height:,} | pick intervals: {pick_ledger.height:,}")

    ledger = pl.concat([player_ledger, pick_ledger], how="vertical").with_columns(
        pl.lit("sleeper").alias("source_system"),
        pl.lit(datetime.now()).alias("loaded_at"),
    )

    fact_path = os.environ.get(
        "LEDGER_OUTPUT_PATH", f"gs://{bucket_name}/silver/fantasy/fact_roster_membership")
    print(f"Writing {ledger.height:,} SCD2 intervals (full history) to {fact_path}...")
    # Full-history rebuild each run (idempotent) — NOT the old latest-only overwrite.
    ledger.write_parquet(fact_path, partition_by=["asset_type"], use_pyarrow=True)
    print("Done.")


if __name__ == "__main__":
    main()
