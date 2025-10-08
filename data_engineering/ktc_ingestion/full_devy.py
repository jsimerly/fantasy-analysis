import os
import time
import json
from datetime import datetime, timezone

import numpy as np
from dotenv import load_dotenv
import polars as pl

from utils import (
    fetch_soup,
    get_dynasty_playersArray,
    parse_historic_1QBplayer_data,
    parse_historic_SFplayer_data,
    transform_player_data,
)

load_dotenv()

def save_player_to_gcs(df: pl.DataFrame, bucket_name: str, slug: str, base_date: str):
    file_path = f"gs://{bucket_name}/bronze/ktc/devy/full_load/load_date={base_date}/{slug}.parquet"  

    try:
        df.write_parquet(file_path)
        
    except Exception as e:
        print(f"Failed to save {slug} to GCS: {e}")
        raise

def save_errors_to_gcs(errors: list, bucket_name: str, base_date: str):
    """Save collected errors to GCS as JSON"""
    if not errors:
        return
    
    error_path = f"gs://{bucket_name}/bronze/ktc/devy/full_load/errors/load_date={base_date}/errors.json"
    
    try:
        error_data = {
            'load_date': base_date,
            'error_count': len(errors),
            'errors': errors
        }
        
        with open(error_path.replace('gs://', '/gcs/'), 'w') as f:
            json.dump(error_data, f, indent=2)
            
        print(f"✗ Saved {len(errors)} errors to {error_path}")
        
    except Exception as e:
        print(f"Failed to save errors to GCS: {e}")
        with open('failed_players.txt', 'w') as f:
            for error in errors:
                f.write(f"{error['slug']}\n")



def main():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date =  datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    players = get_dynasty_playersArray()

    errors = []
    for i, player in enumerate(players):
        slug = player['slug']

        try: 
            data = {
                'slug': slug, 
                'player_id': player['playerID'], 
                'player_name': player['playerName'], 
                'position': player['position'], # main used for identifying picks
            }

            url = f"https://keeptradecut.com/dynasty-rankings/players/{slug}"
            soup = fetch_soup(url)

            one_qb_data = parse_historic_1QBplayer_data(soup)
            sf_data = parse_historic_SFplayer_data(soup)

            data['one_qb_value'] = one_qb_data['overallValue']
            data['one_qb_overall_rank'] = one_qb_data['overallRankHistory']

            data['sf_value'] = sf_data['overallValue']
            data['sf_overall_rank'] = sf_data['overallRankHistory']

            if data['position'] != 'RDP':
                data['one_qb_pos_rank'] = one_qb_data['positionalRankHistory']
                data['sf_pos_rank'] = sf_data['positionalRankHistory']

            df = transform_player_data(data, current_date)
            save_player_to_gcs(df, bucket_name, slug, current_date)
            print(f"✓ Saved {slug} ({i + 1}/{len(players)})")
            time.sleep(abs(np.random.normal(2, 1))+1)

        except Exception as e:
            print(f"✗ Error for {slug}: {e}")
            errors.append({
                'slug': slug,
                'player_name': player.get('playerName', 'Unknown'),
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })

    if errors:
        save_errors_to_gcs(errors, bucket_name, current_date)
        print(f"\n⚠ Total errors: {len(errors)}/{len(players)}")
    else:
        print(f"\n✓ All {len(players)} players processed successfully!")

if __name__ == "__main__":
    main()





