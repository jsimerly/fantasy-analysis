import os
import time
import re 
import json
from datetime import datetime

import numpy as np
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google.cloud import storage


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
    
def save_player_to_gcs(data, bucket_name: str, slug: str, base_date: str):
    file_path = f"bronze/ktc/initial_load/load_date={base_date}/{slug}.json"

    try:
        client = storage.Client() 
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type='application/json'
        )
        
    except Exception as e:
        print(f"Failed to save {slug} to GCS: {e}")
        raise

test_player_url = "https://keeptradecut.com/dynasty-rankings/players/brian-thomas-jr-1589"
test_pick_url = "https://keeptradecut.com/dynasty-rankings/players/2027-early-1st-1702"

if __name__ == "__main__":
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date =  datetime.now().strftime('%Y-%m-%d')
    
    players = get_dynasty_playersArray()

    dataset = []
    for i, player in enumerate(players):
        try: 
            slug = player['slug']
            data = {'ktc_id': slug}

            url = f"https://keeptradecut.com/dynasty-rankings/players/{slug}"
            soup = fetch_soup(url)

            one_qb_data = parse_historic_1QBplayer_data(soup)
            sf_data = parse_historic_SFplayer_data(soup)

            data['player_id'] = # figure this out.
            data['one_qb_value'] = one_qb_data['overallValue']
            data['one_qb_overall_rank'] = one_qb_data['overallRankHistory']
            data['one_qb_pos_rank'] = one_qb_data['positionalRankHistory']
            data['sf_value'] = sf_data['overallValue']
            data['sf_overall_rank'] = sf_data['overallRankHistory']
            data['sf_pos_rank'] = sf_data['positionalRankHistory']

            if o

            save_player_to_gcs(data, bucket_name, slug, current_date)
            print(f"✓ Saved {slug} ({i + 1}/{len(players)})")

        except Exception as e:
            print(f"There was an error for player {slug}: {e}")
            with open('failed_players.txt', 'a') as f:
                f.write(f"{slug}\n")


        time.sleep(np.random.normal(3, 1))






