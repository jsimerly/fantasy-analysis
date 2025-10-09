from typing import Any
import os
import polars as pl

import requests
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

def fetch(url) -> dict[str, Any]:
    resp = requests.get(url)
    if str(resp.status_code)[0] == '2':
        return resp.json()  
    raise ValueError(f"Request failed with status code {resp.status_code}: {resp.text}")

def get_sport_state(sport: str = "nfl") -> dict[str, Any]:
    url = f"https://api.sleeper.app/v1/state/{sport}"
    sport_state = fetch(url)
    return sport_state
        
# TODO: need to solve the captcha problem
def sleeper_login(email: str, password: str) -> str:
    """Login via GraphQL endpoint."""

    transport = RequestsHTTPTransport(
        url='https://sleeper.com/graphql',
        use_json=True,
        headers={'Content-Type': 'application/json'}
    )
    
    client = Client(transport=transport)
    
    query = gql("""
    query login($email_or_phone_or_username: String!, $password: String!, $captcha: String) {
        login(email_or_phone_or_username: $email_or_phone_or_username, password: $password, captcha: $captcha) {
            token
        }
    }
    """)
    
    variables = {
        'email_or_phone_or_username': email,
        'password': password,
        'captcha': None
    }
    
    result = client.execute(query, variable_values=variables)
    token = result['login']['token']
    
    return f"Bearer {token}"
    
def get_latest_blob_path(bucket_name: str, base_prefix: str) -> str | None:
    storage_client = storage.Client()

    if not base_prefix.endswith('/'):
        base_prefix += '/'

    blobs = storage_client.list_blobs(bucket_name, prefix=base_prefix)
    
    most_recent_blob = None
    
    for blob in blobs:
        if blob.name.endswith('/'): 
            continue 
            
        if most_recent_blob is None or blob.time_created > most_recent_blob.time_created:
            most_recent_blob = blob

    if most_recent_blob:
        full_path = f"gs://{bucket_name}/{most_recent_blob.name}"
        return full_path
    else:
        return None

def get_league_ids():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    storage_path = get_latest_blob_path(bucket_name, "bronze/sleeper/league/leagues/")

    df = pl.read_parquet(storage_path)
    league_ids = df.select("league_id").unique().to_series().to_list()
    return league_ids

if __name__ == "__main__":
    ...