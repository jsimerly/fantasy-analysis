import os
from datetime import datetime, timezone
import sys
from pathlib import Path
import time

import polars as pl
from dotenv import load_dotenv

env_path = sys.path.insert(0, str(Path(__file__).parent.parent))
from api.draft import get_drafts_in_league
from _utils import get_fantasy_leagues

def flatten_drafts(drafts_list: list[dict]) -> tuple[pl.DataFrame, pl.DataFrame]:
    drafts_rows = []
    order_rows = []

    for d in drafts_list or []:
        draft_id = d.get("draft_id")
        league_id = d.get("league_id")
        created = d.get("created")
        last_message_time = d.get("last_message_time")
        last_picked = d.get("last_picked")
        start_time = d.get("start_time")
        metadata = d.get("metadata", {}) or {}
        settings = d.get("settings", {}) or {}

        drafts_rows.append({
            "draft_id": draft_id,
            "league_id": league_id,
            "created": created,
            "last_message_time": last_message_time,
            "last_picked": last_picked,
            "start_time": start_time,
            "created_iso": None if created is None else datetime.fromtimestamp(created/1000, tz=timezone.utc).isoformat(),
            "last_message_time_iso": None if last_message_time is None else datetime.fromtimestamp(last_message_time/1000, tz=timezone.utc).isoformat(),
            "last_picked_iso": None if last_picked is None else datetime.fromtimestamp(last_picked/1000, tz=timezone.utc).isoformat(),
            "start_time_iso": None if start_time is None else datetime.fromtimestamp(start_time/1000, tz=timezone.utc).isoformat(),
            "season": d.get("season"),
            "season_type": d.get("season_type"),
            "status": d.get("status"),
            "type": d.get("type"),
            "sport": d.get("sport"),
            "draft_name": metadata.get("name"),
            "draft_description": metadata.get("description"),
            "scoring_type": metadata.get("scoring_type"),
            "rounds": settings.get("rounds"),
            "teams": settings.get("teams"),
            "pick_timer": settings.get("pick_timer"),
            "nomination_timer": settings.get("nomination_timer"),
            "enforce_position_limits": settings.get("enforce_position_limits"),
            "reversal_round": settings.get("reversal_round"),
            "player_type": settings.get("player_type"),
            "creators_raw": d.get("creators", []),
            "draft_order_raw": d.get("draft_order", {}),
            "timestamp": datetime.now(timezone.utc),
        })

        # separate, long-form table for draft order
        for user_id, slot in (d.get("draft_order", {}) or {}).items():
            order_rows.append({
                "draft_id": draft_id,
                "league_id": league_id,
                "user_id": str(user_id),
                "slot": slot,
            })

    drafts_df = pl.from_dicts(drafts_rows) if drafts_rows else pl.DataFrame()
    draft_order_df = pl.from_dicts(order_rows) if order_rows else pl.DataFrame()
    return drafts_df, draft_order_df

def save_df_to_gcs_drafts(
    df: pl.DataFrame,
    bucket_name: str,
    base_date: str,         
    entity: str,           
) -> str:
    # auto-skip empties
    if df is None or df.height == 0:
        print(f"⚠️  Skipping {entity}: empty DataFrame")
        return ""

    file_path = f"gs://{bucket_name}/bronze/sleeper/drafts/{entity}/load_date={base_date}/data.parquet"

    try:
        df.write_parquet(file_path)
        print(f"✅ Saved {entity} to {file_path}")
    except Exception as e:
        print(f"❌ Failed to save {entity} to GCS: {e}")
        raise

    return file_path

def main():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    leagues = get_fantasy_leagues()
    active_leagues = leagues.filter(pl.col("source_system") == "sleeper")
    league_ids = active_leagues.select("league_id").to_series().to_list()

    drafts_all, draft_order_all = [], []

    for league_id in league_ids:
        try:
            drafts_list = get_drafts_in_league(league_id=league_id)
            d_df, o_df = flatten_drafts(drafts_list)
            drafts_all.append(d_df)
            draft_order_all.append(o_df)
        except Exception as e:
            print(f"❌ Error for league {league_id}: {e}")
        time.sleep(.1)

    drafts_df_all = pl.concat([df for df in drafts_all if df.height > 0], how="vertical_relaxed") if drafts_all else pl.DataFrame()
    draft_order_df_all = pl.concat([df for df in draft_order_all if df.height > 0], how="vertical_relaxed") if draft_order_all else pl.DataFrame()

    print("\n=== Aggregate sizes (drafts) ===")
    print(f"drafts:       {drafts_df_all.height:,} rows")
    print(f"draft_order:  {draft_order_df_all.height:,} rows")

    save_df_to_gcs_drafts(drafts_df_all,      bucket_name, current_date, entity="drafts")
    save_df_to_gcs_drafts(draft_order_df_all, bucket_name, current_date, entity="draft_order")




if __name__ == "__main__":
    main()

