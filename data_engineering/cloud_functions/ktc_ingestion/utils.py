from bs4 import BeautifulSoup, Tag
import time
import random
import requests
import re
import json

def fetch_soup(url: str) -> BeautifulSoup:
    time.sleep(random.uniform(2,6))
    resp = requests.get(url)
    return BeautifulSoup(resp.text, 'html.parser')

def get_dynasty_playersArray(url: str = None) -> list[dict]:
    url = 'https://keeptradecut.com/dynasty-rankings' if not url else url

    soup = fetch_soup(url)
    scripts = soup.find_all('script')

    players_array_str = find_content_in_tags(scripts, 'playersArray')

    if not players_array_str:
        raise ValueError("playersArray not found in any script tag")

    match = re.search(r'playersArray = (\[.*?\]);', players_array_str, re.DOTALL)
    if match:
        json_str = match.group(1)
        return json.loads(json_str)
    else:
        raise ValueError("Could not extract playersArray data from script content")
    
def find_content_in_tags(tags: list[Tag], search_str: str) -> str | None:
    for tag in tags:
        content = tag.string if tag.string else tag.get_text()
        if content and search_str in content:
            return content
        
    
def get_dynasty_urls() -> list[str]:
    ''' This returns a list of all of the top 500 players urls for ktc'''
    playersArray = get_dynasty_playersArray()
    urls = []
    for player_data in playersArray:
        url = f"https://keeptradecut.com/dynasty-rankings/players/{player_data['slug']}"
        urls.append(url)

    return urls

