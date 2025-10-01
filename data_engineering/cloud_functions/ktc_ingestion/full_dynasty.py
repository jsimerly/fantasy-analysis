import os
import time
import re 
import json
from datetime import datetime, timezone

import numpy as np
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google.cloud import storage
import polars as pl


from utils import (
    fetch_soup,
    find_content_in_tags,
    get_dynasty_playersArray
)

load_dotenv()

def parse_historic_1QBplayer_data(soup: BeautifulSoup) -> dict:
    scripts = soup.find_all('script')
    player_array_str = find_content_in_tags(scripts, 'var playerOneQB')

    if not player_array_str:
        raise ValueError('player not found in any script tag')
    
    match = re.search(r'var playerOneQB = (\{.*?\});\s+var leagueType', player_array_str, re.DOTALL)
    if match:
        json_str = match.group(1)
        return json.loads(json_str)
    else:
        raise ValueError("Could not extract player data from script content: No match returned")
    
def parse_historic_SFplayer_data(soup: BeautifulSoup) -> dict:
    scripts = soup.find_all('script')
    player_array_str = find_content_in_tags(scripts, 'var playerSuperflex')

    if not player_array_str:
        raise ValueError('player not found in any script tag')
    
    match = re.search(r'var playerSuperflex = (\{.*?\});\s+var playerOneQB', player_array_str, re.DOTALL)
    if match:
        json_str = match.group(1)
        return json.loads(json_str)
    else:
        raise ValueError("Could not extract player data from script content: No match returned")
    
def save_player_to_gcs(df: pl.DataFrame, bucket_name: str, slug: str, base_date: str):
    file_path = f"gs://{bucket_name}/bronze/ktc/full_load/load_date={base_date}/{slug}.parquet"  

    try:
        df.write_parquet(file_path)
        
    except Exception as e:
        print(f"Failed to save {slug} to GCS: {e}")
        raise

def transform_player_data(data: dict, scrape_date: str) -> pl.DataFrame:
    player_id = data.get('player_id')
    player_name = data.get("player_name")
    position = data.get("position")
    slug_name = data.get("slug")

    # flatten historic data into date : value from {'d': date, 'v': value}
    one_qb_value_lookup = {item['d']: item['v'] for item in (data.get('one_qb_value') or [])}
    one_qb_overall_lookup = {item['d']: item['v'] for item in (data.get('one_qb_overall_rank') or [])}
    sf_value_lookup = {item['d']: item['v'] for item in (data.get('sf_value') or [])}
    sf_overall_lookup = {item['d']: item['v'] for item in (data.get('sf_overall_rank') or [])}
    if position != 'RDP':
        one_qb_pos_lookup = {item['d']: item['v'] for item in (data.get('one_qb_pos_rank') or [])}
        sf_pos_lookup = {item['d']: item['v'] for item in (data.get('sf_pos_rank') or [])}

    all_dates = set(one_qb_value_lookup.keys()) | set(one_qb_overall_lookup.keys()) | \
                set(sf_value_lookup.keys()) | set(sf_overall_lookup.keys())
    
    if position != 'RDP':
        all_dates |= set(one_qb_pos_lookup.keys()) | set(sf_pos_lookup.keys())

    flattened_rows = []
    for date in sorted(all_dates):
        row = {
            'player_id': player_id,
            'player_name': player_name,
            'position': position,
            'slug': slug_name,
            'ranking_date': date,
            'one_qb_value': one_qb_value_lookup.get(date),
            'one_qb_overall_rank': one_qb_overall_lookup.get(date),
            'sf_value': sf_value_lookup.get(date),
            'sf_overall_rank': sf_overall_lookup.get(date),
            'scrape_date': scrape_date, 
            'processed_at': datetime.now(timezone.utc)
        }
        
        if position != 'RDP':
            row['one_qb_pos_rank'] = one_qb_pos_lookup.get(date)
            row['sf_pos_rank'] = sf_pos_lookup.get(date)
        
        flattened_rows.append(row)

    df = pl.DataFrame(flattened_rows)

    type_casts = [
        pl.col('ranking_date').str.to_date('%y%m%d'),
        pl.col('one_qb_value').cast(pl.Float64),
        pl.col('one_qb_overall_rank').cast(pl.Int64),
        pl.col('sf_value').cast(pl.Float64),
        pl.col('sf_overall_rank').cast(pl.Int64),
        pl.col('scrape_date').str.to_date('%Y-%m-%d'),
    ]
    if position != 'RDP':
        type_casts.extend([
            pl.col('one_qb_pos_rank').cast(pl.Int64),
            pl.col('sf_pos_rank').cast(pl.Int64),
        ])
    
    df = df.with_columns(type_casts)

    return df


def main():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date =  datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    players = get_dynasty_playersArray()

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
            time.sleep(abs(np.random.normal(3, 1)))

        except Exception as e:
            print(f"There was an error for player {slug}: {e}")
            with open('failed_players.txt', 'a') as f:
                f.write(f"{slug}\n")

        

if __name__ == "__main__":
    main()





