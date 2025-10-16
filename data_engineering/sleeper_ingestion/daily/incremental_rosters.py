import os
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
import time

import polars as pl

env_path = sys.path.insert(0, str(Path(__file__).parent.parent))
from _utils import get_fantasy_leagues
from api.league import get_rosters


def flatten_rosters(rosters_data: list[dict]) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    timestamp = datetime.now()

    player_records: list[dict] = []
    team_state_records: list[dict] = []
    nickname_records: list[dict] = []

    for roster in rosters_data or []:
        league_id = roster.get("league_id")
        roster_id = roster.get("roster_id")
        owner_id = roster.get("owner_id")

        metadata: dict = roster.get("metadata", {}) or {}
        settings: dict = roster.get("settings", {}) or {}

        players = roster.get("players", []) or []
        starters = set(roster.get("starters", []) or [])
        taxi = set(roster.get("taxi", []) or [])
        reserve = set(roster.get("reserve", []) or [])

        # ---- per-player rows (raw-ish)
        for player_id in players:
            player_records.append(
                {
                    "league_id": league_id,
                    "roster_id": roster_id,
                    "owner_id": owner_id,
                    "player_id": str(player_id),      
                    "is_starter": player_id in starters,
                    "is_taxi": player_id in taxi,
                    "is_reserve": player_id in reserve,
                    "is_active": (player_id not in taxi) and (player_id not in reserve),
                    "timestamp": timestamp,
                }
            )

        # ---- per-roster state (once per roster)
        team_state_records.append(
            {
                "league_id": league_id,
                "roster_id": roster_id,
                "owner_id": owner_id,
                "timestamp": timestamp,

                # From metadata
                "record": metadata.get("record"),
                "streak": metadata.get("streak"),

                # From settings 
                "division": settings.get("division"),
                "wins": settings.get("wins"),
                "losses": settings.get("losses"),
                "ties": settings.get("ties"),
                "fpts": settings.get("fpts"),
                "fpts_decimal": settings.get("fpts_decimal"),
                "fpts_against": settings.get("fpts_against"),
                "fpts_against_decimal": settings.get("fpts_against_decimal"),
                "ppts": settings.get("ppts"),
                "ppts_decimal": settings.get("ppts_decimal"),
                "total_moves": settings.get("total_moves"),
                "waiver_position": settings.get("waiver_position"),
                "waiver_budget_used": settings.get("waiver_budget_used"),
            }
        )

        # ---- nicknames
        # Keep ALL p_nick_* entries as-is (including empty strings), plus a very light parse.
        for k, v in (metadata.items() if isinstance(metadata, dict) else []):
            if not (isinstance(k, str) and k.startswith("p_nick_")):
                continue

            suffix = k[len("p_nick_"):]  # could be player_id (digits) or team code (e.g., KC)
            subject_type = "player" if suffix.isdigit() else "team"

            nickname_records.append(
                {
                    "league_id": league_id,
                    "roster_id": roster_id,
                    "owner_id": owner_id,
                    "meta_key": k,          # raw key (e.g., "p_nick_11566")
                    "nickname_raw": v,      # raw value
                    "subject_id": suffix,   # light parse for convenience
                    "subject_type": subject_type,
                    "timestamp": timestamp,
                }
            )

    # ---- DataFrames (let Polars infer types; bronze prefers raw)
    players_df = pl.from_dicts(player_records) if player_records else pl.DataFrame(schema={
        "league_id": pl.Utf8, "roster_id": pl.Int64, "owner_id": pl.Utf8, "player_id": pl.Utf8,
        "is_starter": pl.Boolean, "is_taxi": pl.Boolean, "is_reserve": pl.Boolean, "is_active": pl.Boolean,
        "timestamp": pl.Datetime,
    })

    team_state_df = pl.from_dicts(team_state_records) if team_state_records else pl.DataFrame(schema={
        "league_id": pl.Utf8, "roster_id": pl.Int64, "owner_id": pl.Utf8, "timestamp": pl.Datetime,
        "record": pl.Utf8, "streak": pl.Utf8, "division": pl.Int64, "wins": pl.Int64, "losses": pl.Int64,
        "ties": pl.Int64, "fpts": pl.Int64, "fpts_decimal": pl.Int64, "fpts_against": pl.Int64,
        "fpts_against_decimal": pl.Int64, "ppts": pl.Int64, "ppts_decimal": pl.Int64,
        "total_moves": pl.Int64, "waiver_position": pl.Int64, "waiver_budget_used": pl.Int64,
    })

    nicknames_df = pl.from_dicts(nickname_records) if nickname_records else pl.DataFrame(schema={
        "league_id": pl.Utf8, "roster_id": pl.Int64, "owner_id": pl.Utf8,
        "meta_key": pl.Utf8, "nickname_raw": pl.Utf8,
        "subject_id": pl.Utf8, "subject_type": pl.Utf8,
        "timestamp": pl.Datetime,
    })

    return players_df, team_state_df, nicknames_df

def _get_week_start_from_str(date_str: str, week_start: str = "tuesday") -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


    mapping = {"monday": 0, "tuesday": 1, "wednesday": 3, "sunday": 6}
    start_idx = mapping.get(week_start.lower(), 1)  
    wd = dt.weekday()
    # distance back to start
    offset = (wd - start_idx) % 7
    week_start_dt = (dt - timedelta(days=offset)).replace(hour=0, minute=0, second=0, microsecond=0)
    return week_start_dt.strftime("%Y-%m-%d")


def save_df_to_gcs(
    df: pl.DataFrame,
    bucket_name: str,
    base_date: str,    
    entity: str,
    partition_mode: str = "daily",   # 'daily' | 'weekly' (daily per your note)
    week_start: str = "tuesday",
) -> str:
    # Auto-skip empties
    if df is None or df.height == 0:
        print(f"⚠️  Skipping {entity}: empty DataFrame")
        return ""

    if partition_mode == "daily":
        partition = f"load_date={base_date}"
    elif partition_mode == "weekly":
        wk_start = _get_week_start_from_str(base_date, week_start=week_start)
        partition = f"week_start={wk_start}"
    else:
        raise ValueError("partition_mode must be 'daily' or 'weekly'")

    file_path = f"gs://{bucket_name}/bronze/sleeper/rosters/{entity}/daily/{partition}/data.parquet"

    try:
        df.write_parquet(file_path)
        print(f"✅ Saved {entity} to {file_path}")
    except Exception as e:
        print(f"❌ Failed to save {entity} to GCS: {e}")
        raise

    return file_path

def _concat_or_empty(dfs: list[pl.DataFrame]) -> pl.DataFrame:
    dfs = [df for df in dfs if isinstance(df, pl.DataFrame) and df.height > 0]
    if not dfs:
        return pl.DataFrame()
    return pl.concat(dfs, how="vertical_relaxed").rechunk()

def main():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    leagues = get_fantasy_leagues()

    active_leagues = leagues.filter(
        (pl.col("status") != "complete") &
        (pl.col("source_system") == "sleeper")
    )

    print(f"Found {len(active_leagues)} active leagues to process.\n")

    players_all: list[pl.DataFrame] = []
    team_state_all: list[pl.DataFrame] = []
    nicknames_all: list[pl.DataFrame] = []

    for row in active_leagues.iter_rows(named=True):
        league_id = row['league_id']
        league_name = row.get('league_name', league_id)

        try:
            print("=" * 60)
            print(f"Processing league: {league_name} ({league_id}) | run_date={current_date}")

            rosters = get_rosters(league_id=league_id)
            players_df, team_state_df, nicknames_df = flatten_rosters(rosters)

            for df_name, df in [("players", players_df), ("team_state", team_state_df), ("nicknames", nicknames_df)]:
                if df is not None and df.height > 0 and "league_id" not in df.columns:
                    df = df.with_columns(pl.lit(league_id).alias("league_id"))
                    if df_name == "players":
                        players_df = df
                    elif df_name == "team_state":
                        team_state_df = df
                    else:
                        nicknames_df = df

            players_all.append(players_df)
            team_state_all.append(team_state_df)
            nicknames_all.append(nicknames_df)

        except Exception as e:
            print(f"   ❌ Error processing {league_name}: {e}")

        time.sleep(0.1)

    # Union across leagues
    players_df_all = _concat_or_empty(players_all)
    team_state_df_all = _concat_or_empty(team_state_all)
    nicknames_df_all = _concat_or_empty(nicknames_all)

    print("\n=== Aggregate sizes ===")
    print(f"roster_players: {players_df_all.height:,} rows")
    print(f"team_state:     {team_state_df_all.height:,} rows")
    print(f"nicknames:      {nicknames_df_all.height:,} rows")

    # Save all three as DAILY snapshots
    save_df_to_gcs(
        players_df,
        bucket_name=bucket_name,
        base_date=current_date,
        entity=f"roster_players",
        partition_mode="daily",
    )
    save_df_to_gcs(
        team_state_df,
        bucket_name=bucket_name,
        base_date=current_date,
        entity=f"team_state",
        partition_mode="daily",
    )
    save_df_to_gcs(
        nicknames_df,
        bucket_name=bucket_name,
        base_date=current_date,
        entity=f"nicknames",
        partition_mode="daily",
    )



if __name__ == "__main__":
    main()
        

