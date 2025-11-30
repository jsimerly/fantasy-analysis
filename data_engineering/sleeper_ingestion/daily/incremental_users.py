import os
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
import time
from typing import Iterable, Any

import polars as pl

env_path = sys.path.insert(0, str(Path(__file__).parent.parent))
from _utils import get_fantasy_leagues
from api.league import get_users_in_league


# --------------------------
# Helpers
# --------------------------
def _get_week_start_from_str(date_str: str, week_start: str = "tuesday") -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    mapping = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6}
    start_idx = mapping.get(week_start.lower(), 1)  # default Tuesday
    wd = dt.weekday()
    offset = (wd - start_idx) % 7
    week_start_dt = (dt - timedelta(days=offset)).replace(hour=0, minute=0, second=0, microsecond=0)
    return week_start_dt.strftime("%Y-%m-%d")


def save_df_to_gcs(
    df: pl.DataFrame,
    bucket_name: str,
    base_date: str,
    entity: str,
    partition_mode: str = "weekly",    # users is weekly like team_state/nicknames
    week_start: str = "tuesday",
) -> str:
    if df is None or df.height == 0:
        print(f"⚠️  Skipping {entity}: empty DataFrame")
        return ""

    if partition_mode == "weekly":
        wk_start = _get_week_start_from_str(base_date, week_start=week_start)
        partition = f"week_start={wk_start}"
        subdir = "weekly"
    elif partition_mode == "daily":
        partition = f"load_date={base_date}"
        subdir = "daily"
    else:
        raise ValueError("partition_mode must be 'daily' or 'weekly'")

    file_path = f"gs://{bucket_name}/bronze/sleeper/rosters/{entity}/{subdir}/{partition}/data.parquet"
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved {entity} to {file_path}")
    except Exception as e:
        print(f"❌ Failed to save {entity} to GCS: {e}")
        raise
    return file_path


# --------------------------
# Bronze flattener
# --------------------------
def flatten_users(users: list[dict], league_id: str) -> pl.DataFrame:
    ts = datetime.now(timezone.utc)

    def _norm_str(x):
        if x is None:
            return None
        s = str(x).strip()
        return s if s else None

    rows = []
    for u in users or []:
        meta = (u.get("metadata") or {})
        rows.append({
            "league_id": str(league_id),
            "user_id": _norm_str(u.get("user_id")),
            "display_name": _norm_str(u.get("display_name")),
            "user_team_name": _norm_str(meta.get("team_name")),
            "avatar": _norm_str(u.get("avatar")),
            "timestamp": ts,
        })

    if not rows:
        return pl.DataFrame(schema={
            "league_id": pl.Utf8,
            "user_id": pl.Utf8,
            "display_name": pl.Utf8,
            "user_team_name": pl.Utf8,
            "avatar": pl.Utf8,
            "timestamp": pl.Datetime,
        })

    df = pl.from_dicts(rows).cast({
        "league_id": pl.Utf8,
        "user_id": pl.Utf8,
        "display_name": pl.Utf8,
        "user_team_name": pl.Utf8,
        "avatar": pl.Utf8,
        "timestamp": pl.Datetime,
    }, strict=False)

    df = df.sort(["league_id", "user_id", "timestamp"]).unique(
        subset=["league_id", "user_id"], keep="last"
    )
    return df


# --------------------------
# Main
# --------------------------
def main():
    bucket_name = os.environ.get("GCS_BUCKET_NAME", "YOUR_BUCKET")
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    leagues = get_fantasy_leagues()
    active_leagues = leagues.filter(
        (pl.col("status") != "complete") &
        (pl.col("source_system") == "sleeper")
    )

    print(f"Found {len(active_leagues)} active leagues to process.\n")

    users_all: list[pl.DataFrame] = []

    for row in active_leagues.iter_rows(named=True):
        league_id = row["league_id"]
        league_name = row.get("league_name", league_id)

        try:
            print("=" * 60)
            print(f"Processing users for league: {league_name} ({league_id}) | run_date={current_date}")

            users = get_users_in_league(league_id=league_id)  
            df = flatten_users(users, league_id=str(league_id))
            if df.height > 0:
                users_all.append(df)

        except Exception as e:
            print(f"   ❌ Error processing {league_name}: {e}")

        time.sleep(0.1)

    if users_all:
        users_df_all = pl.concat(users_all, how="vertical_relaxed").rechunk()
        users_df_all = users_df_all.sort(["league_id", "user_id", "timestamp"]).unique(
            subset=["league_id", "user_id"], keep="last"
        )
    else:
        users_df_all = pl.DataFrame(schema={
            "league_id": pl.Utf8,
            "user_id": pl.Utf8,
            "display_name": pl.Utf8,
            "user_team_name": pl.Utf8,
            "avatar": pl.Utf8,
            "timestamp": pl.Datetime,
        })

    print("\n=== Aggregate sizes ===")
    print(f"users: {users_df_all.height:,} rows")

    save_df_to_gcs(
        users_df_all,
        bucket_name=bucket_name,
        base_date=current_date,
        entity="users",
        partition_mode="weekly",
    )


if __name__ == "__main__":
    main()
