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


def main():
    bucket_name = os.environ.get("GCS_BUCKET_NAME")

    # --- load sources ---
    roster_path = get_latest_bronze_path(bucket_name, "rosters/roster_players/daily", source="sleeper")
    snapshot_date = _snapshot_date_from_path(roster_path)
    roster_players_df = pl.read_parquet(roster_path)

    traded_path = get_latest_bronze_path(bucket_name, "rosters/traded_picks/daily", source="sleeper")
    traded_picks_df = pl.read_parquet(traded_path)

    txn_draft_picks_df = _read_prefix_concat(
        bucket_name, "bronze/sleeper/transactions/draft_picks/daily/"
    )
    overrides_path = _latest_file(bucket_name, "bronze/sleeper/transactions/commission_overrides/")
    overrides_df = pl.read_parquet(overrides_path) if overrides_path else pl.DataFrame()

    # drafts give per-season draft status -> the precise "picks are spent" cutoff
    try:
        drafts_df = pl.read_parquet(get_latest_bronze_path(bucket_name, "drafts/drafts", source="sleeper"))
    except ValueError:
        drafts_df = None

    dim_franchise = pl.read_parquet(f"gs://{bucket_name}/silver/fantasy/dim_franchises_meta/data.parquet")
    dim_player = pl.read_parquet(f"gs://{bucket_name}/silver/fantasy/dim_players_master/data.parquet")
    leagues_df = pl.read_parquet(f"gs://{bucket_name}/silver/fantasy/dim_leagues_meta/data.parquet")

    # --- build ---
    print("Building player membership...")
    player_membership = build_player_membership(roster_players_df, dim_franchise, dim_player)

    # The resolver translates every pick source onto the *current* league of its
    # dynasty lineage (max-season league per lineage) and only emits future picks,
    # so it needs the full dim_leagues_meta (all seasons) to build the lineage map.
    # This is what keeps commissioner overrides resolving across season rollovers
    # without manual edits.
    print("Resolving pick ownership (lineage-mapped: override > txn > traded > original)...")
    resolved_picks = resolve_pick_ownership(
        traded_picks_df, txn_draft_picks_df, overrides_df, leagues_df, drafts_df
    )
    pick_membership = build_pick_membership(resolved_picks, dim_franchise)

    print("Reconciling conservation invariants...")
    fact_df, quarantine_df = reconcile(player_membership, pick_membership, leagues_df)

    fact_df = fact_df.with_columns(
        pl.lit(snapshot_date).alias("snapshot_date"),
        pl.lit("sleeper+nflverse").alias("source_system"),
        pl.lit(datetime.now()).alias("loaded_at"),
    )

    if quarantine_df.height:
        print(f"WARNING: {quarantine_df.height} asset rows quarantined. Reasons:")
        print(quarantine_df.group_by("quarantine_reason").len().sort("len", descending=True))
        q_path = f"gs://{bucket_name}/silver/fantasy/quarantine/unaccounted_assets.parquet"
        quarantine_df.with_columns(pl.lit(snapshot_date).alias("snapshot_date")).write_parquet(q_path)
        print(f"Wrote quarantine to {q_path}")
    else:
        print("Success: every asset accounted for (quarantine empty).")

    fact_path = f"gs://{bucket_name}/silver/fantasy/fact_roster_membership"
    print(f"Writing {fact_df.height} asset rows to {fact_path}...")
    fact_df.write_parquet(fact_path, partition_by=["asset_type"], use_pyarrow=True)
    print("Done.")


if __name__ == "__main__":
    main()
