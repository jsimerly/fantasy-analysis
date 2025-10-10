import polars as pl
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

commissioner_pick_transactions = [
    # 3 way Jake, Tim, Me trade. April 2022 
    {
        'transaction_id': None, 'league_id': '784444797430657024', 'draft_pick_id': '1,2023,1,5,1', 
        'roster_id': 1, 
        'season': '2023', 
        'round': 1, 
        'from_team_id': 5, 
        'to_team_id': 1, 
        'created': 1649198791290
    }, 
    {
        'transaction_id': None, 'league_id': '784444797430657024', 'draft_pick_id': '1,2024,1,5,1', 'roster_id': 1, 
        'season': '2024', 
        'round': 1, 
        'from_team_id': 5, 
        'to_team_id': 1, 
        'created': 1649198791290
    }, 
    {
        'transaction_id': None, 'league_id': '784444797430657024', 'draft_pick_id': '4,2024,1,5,1', 'roster_id': 4, 
        'season': '2024', 
        'round': 1, 'from_team_id': 5, 
        'to_team_id': 1, 
        'created': 1649198791290
    }, 
    # Timmy Taking my Picks
    {
        'transaction_id': None, 'league_id': '784444797430657024', 'draft_pick_id': '2,2024,2,1,2', 'roster_id': 2, 
        'season': '2024', 
        'round': 2, 
        'from_team_id': 1, 
        'to_team_id': 2, 
        'created': 1649198791290
    }, 
    {
        'transaction_id': None, 'league_id': '784444797430657024', 'draft_pick_id': '8,2023,2,2,5', 'roster_id': 8, 
        'season': '2023', 
        'round': 2, 
        'from_team_id': 2, 
        'to_team_id': 5, 
        'created': 1649198791290
    }, 
    # Tim and I Lamar for Mahomes
    {
        'transaction_id': None, 'league_id': '936333864522399744', 'draft_pick_id': '1,2023,1,2,1', 'roster_id': 1, 
        'season': '2023', 
        'round': 1, 
        'from_team_id': 2, 
        'to_team_id': 1, 
        'created': 1658964982917
    },
    {
        'transaction_id': None, 'league_id': '936333864522399744', 'draft_pick_id': '10,2025,1,6,2', 'roster_id': 10, 
        'season': '2025', 
        'round': 1, 
        'from_team_id': 6, 
        'to_team_id': 2, 
        'created': 1680459672733
    }
]

def save_picks_to_gcs(commissioner_picks: list[dict], bucket_name: str, base_date: str):
    df = pl.DataFrame(commissioner_picks)
    
    # Build GCS path matching your transaction_draft_picks structure
    file_path = f"gs://{bucket_name}/bronze/sleeper/transactions/commission_overrides/load_date={base_date}.parquet" 
    
    try:
        df.write_parquet(file_path)
        print(f"✅ Saved {len(df)} commissioner pick transactions to {file_path}")
    except Exception as e:
        print(f"Failed to save commissioner picks to GCS: {e}")
        raise

if __name__ == "__main__":
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date = datetime.now().strftime("%Y-%m-%d")
    save_picks_to_gcs(commissioner_pick_transactions, bucket_name, current_date)