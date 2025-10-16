import os
from datetime import datetime, timezone
import sys
from pathlib import Path
import time
from typing import Iterable, Any
import traceback
import json

import polars as pl
from dotenv import load_dotenv

env_path = sys.path.insert(0, str(Path(__file__).parent.parent))
from api.draft import get_player_draft_picks, get_traded_draft_picks
from _utils import get_latest_blob_path

load_dotenv()

# Utils
def _to_int_or_none(x: Any) -> int | None:
    if x in (None, "", "NA", "N/A"):
        return None
    try:
        # handle strings like "003", "12 ", etc.
        return int(str(x).strip())
    except Exception:
        return None

def _to_str_or_none(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s if s != "" else None

def _ms_to_iso_or_none(ms: Any) -> str | None:
    ms_i = _to_int_or_none(ms)
    if ms_i is None:
        return None
    return datetime.fromtimestamp(ms_i / 1000, tz=timezone.utc).isoformat()

_PICK_SCHEMA = {
    "draft_id": pl.Utf8,
    "pick_no": pl.Int64,
    "round": pl.Int64,
    "draft_slot": pl.Int64,
    "picked_by": pl.Utf8,
    "roster_id": pl.Int64,
    "player_id": pl.Utf8,
    "is_keeper": pl.Boolean,
    "player_first_name": pl.Utf8,
    "player_last_name": pl.Utf8,
    "player_position": pl.Utf8,
    "player_team": pl.Utf8,
    "player_status": pl.Utf8,
    "player_years_exp": pl.Int64,
    "player_number": pl.Int64,
    "player_injury_status": pl.Utf8,
    "player_news_updated_ms": pl.Int64,
    "player_news_updated_iso": pl.Utf8,
    "player_team_changed_at": pl.Utf8,
    "player_team_abbr": pl.Utf8,
    "timestamp": pl.Datetime,
}

def flatten_draft_picks(picks: Iterable[dict]) -> pl.DataFrame:
    rows: list[dict] = []
    now_ts = datetime.now(timezone.utc)

    for idx, p in enumerate(picks or []):
        try:
            meta = p.get("metadata") or {}

            # coerce ALL possibly-numeric fields safely
            draft_id   = _to_str_or_none(p.get("draft_id"))
            pick_no    = _to_int_or_none(p.get("pick_no"))
            rnd        = _to_int_or_none(p.get("round"))
            slot       = _to_int_or_none(p.get("draft_slot"))
            picked_by  = _to_str_or_none(p.get("picked_by"))       # user id as str
            roster_id  = _to_int_or_none(p.get("roster_id"))
            player_id  = _to_str_or_none(p.get("player_id"))
            is_keeper  = p.get("is_keeper") if isinstance(p.get("is_keeper"), bool) else None

            years_exp  = _to_int_or_none(meta.get("years_exp"))
            number     = _to_int_or_none(meta.get("number"))
            news_ms    = _to_int_or_none(meta.get("news_updated"))   # "" -> None
            news_iso   = _ms_to_iso_or_none(news_ms)

            rows.append({
                "draft_id": draft_id,
                "pick_no": pick_no,
                "round": rnd,
                "draft_slot": slot,
                "picked_by": picked_by,
                "roster_id": roster_id,
                "player_id": player_id,
                "is_keeper": is_keeper,

                "player_first_name": _to_str_or_none(meta.get("first_name")),
                "player_last_name": _to_str_or_none(meta.get("last_name")),
                "player_position": _to_str_or_none(meta.get("position")),
                "player_team": _to_str_or_none(meta.get("team")),
                "player_status": _to_str_or_none(meta.get("status")),
                "player_years_exp": years_exp,
                "player_number": number,
                "player_injury_status": _to_str_or_none(meta.get("injury_status")),
                "player_news_updated_ms": news_ms,
                "player_news_updated_iso": news_iso,
                "player_team_changed_at": _to_str_or_none(meta.get("team_changed_at")),
                "player_team_abbr": _to_str_or_none(meta.get("team_abbr")),

                "timestamp": now_ts,
            })

        except Exception:
            # Print full context for the exact offending pick, then re-raise
            print(f"❌ Error flattening pick index {idx} in draft {_to_str_or_none(p.get('draft_id'))}")
            try:
                print("   Raw pick:", json.dumps(p, ensure_ascii=False))
            except Exception:
                print("   Raw pick (repr):", repr(p))
            traceback.print_exc()
            raise

    if not rows:
        return pl.DataFrame(schema=_PICK_SCHEMA)

    df = pl.from_dicts(rows)

    # de-dupe: (draft_id, pick_no)
    if {"draft_id", "pick_no"}.issubset(df.columns):
        df = df.unique(subset=["draft_id", "pick_no"], keep="last")

    # cast with strict=False so any oddities become nulls instead of crashing
    df = df.cast({k: v for k, v in _PICK_SCHEMA.items() if k in df.columns}, strict=False)
    return df

def flatten_traded_draft_picks(trades: Iterable[dict]) -> pl.DataFrame:
    if not trades:
        return pl.DataFrame()

    df = pl.from_dicts(trades)

    for col in ("draft_id", "season", "round", "roster_id", "owner_id", "previous_owner_id"):
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    df = df.select(
        pl.col("draft_id").cast(pl.Utf8).alias("draft_id"),
        pl.col("season").cast(pl.Utf8).alias("season"),
        pl.col("round").cast(pl.Int64).alias("round"),
        pl.col("roster_id").cast(pl.Int64).alias("roster_id"),
        pl.col("owner_id").cast(pl.Int64).alias("owner_roster_id"),
        pl.col("previous_owner_id").cast(pl.Int64).alias("previous_owner_roster_id"),
    ).with_columns(
        pl.lit(datetime.now(timezone.utc)).alias("timestamp")
    )

    df = df.unique(subset=["draft_id", "season", "round", "roster_id"], keep="last")

    return df

def save_df_to_gcs(
    df: pl.DataFrame,
    bucket_name: str,
    base_date: str,           
    entity: str,             
) -> str:
    """Save to gs://<bucket>/bronze/sleeper/drafts/<entity>/load_date=YYYY-MM-DD/data.parquet"""
    if df is None or df.height == 0:
        print(f"⚠️  Skipping save for {entity}: empty DataFrame")
        return ""

    file_path = f"gs://{bucket_name}/bronze/sleeper/drafts/{entity}/load_date={base_date}/data.parquet"
    df.write_parquet(file_path)
    print(f"✅ Saved {entity} -> {file_path}")
    return file_path


def main():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    base_prefix = "bronze/sleeper/drafts/drafts/load_date="

    latest_uri = get_latest_blob_path(bucket_name, base_prefix)
    if not latest_uri:
        print(f"❌ No parquet found for drafts at {latest_uri}.")
        return


    df = pl.read_parquet(latest_uri)
    if "draft_id" not in df.columns:
        print("❌ 'draft_id' column not found in parquet. Path: ")
        return

    draft_ids = df.select("draft_id").unique().to_series().to_list()
    print(f"✅ Found {len(draft_ids)} draft_id(s): {draft_ids}")

    all_picks_flat = []
    all_trades_flat = []

    for i, draft_id in enumerate(draft_ids, start=1):
        print("=" * 60)
        print(f"[{i}] Draft ID: {draft_id}")

        # --- fetch
        picks_raw = get_player_draft_picks(draft_id=draft_id)
        trades_raw = get_traded_draft_picks(draft_id=draft_id)

        # --- flatten
        picks_flat = flatten_draft_picks(picks_raw)
        trades_flat = flatten_traded_draft_picks(trades_raw)

        print(f"📝 picks: {len(picks_raw)} -> flat rows: {picks_flat.height}")
        print(f"🔁 traded picks: {len(trades_raw)} -> flat rows: {trades_flat.height}")

        if picks_flat.height > 0:
            all_picks_flat.append(picks_flat)
        if trades_flat.height > 0:
            all_trades_flat.append(trades_flat)

 
        time.sleep(0.1) 

    # --- combine across drafts and save (daily)
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    picks_df_all = (
        pl.concat(all_picks_flat, how="vertical_relaxed").rechunk()
        if all_picks_flat else pl.DataFrame()
    )
    trades_df_all = (
        pl.concat(all_trades_flat, how="vertical_relaxed").rechunk()
        if all_trades_flat else pl.DataFrame()
    )

    save_df_to_gcs(picks_df_all,  bucket_name, current_date, entity="draft_picks")
    save_df_to_gcs(trades_df_all, bucket_name, current_date, entity="traded_draft_picks")

if __name__ == "__main__":
    main()