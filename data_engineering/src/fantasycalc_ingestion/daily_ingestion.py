import requests
import polars as pl
import numpy as np
from itertools import product
import time
import os
from datetime import datetime, timezone
import random
from dotenv import load_dotenv

load_dotenv()

# The league-setting combinations fetched each day, in fetch order. Every row is tagged with
# its combination (n_qb / n_teams / ppr) so silver can pick ONE setting per value series --
# without the tags the 24 rows per player-day are indistinguishable (they were, until 2026-09:
# fetch_all_combinations tagged the items but flatten_player_data dropped the tags).
N_QB_VALUES = ['1', '2']
N_TEAM_VALUES = ['8', '10', '12', '14']
PPR_VALUES = ['0', '.5', '1']
SETTINGS_COMBINATIONS = list(product(N_QB_VALUES, N_TEAM_VALUES, PPR_VALUES))

def flatten_player_data(player_data: list[dict]) -> pl.DataFrame:
    flattened_data = []

    for item in player_data:
        player: dict = item.get('player', {})

        flattened_record = {
            # Player info (flatten from nested 'player' object)
            'id': player.get('id'),
            'name': player.get('name'),
            'mfl_id': player.get('mflId'),
            'sleeper_id': player.get('sleeperId'),
            'position': player.get('position'),
            'espn_id': player.get('espnId'),
            'fleaflicker_id': player.get('fleaflickerId'),
            'birthday': player.get('maybeBirthday'),
            'height': player.get('maybeHeight'),
            'weight': player.get('maybeWeight'),
            'college': player.get('maybeCollege'),
            'team': player.get('maybeTeam'),
            'age': player.get('maybeAge'),
            'years_exp': player.get('maybeYoe'),
            
            # Value metrics (from root level)
            'value': item.get('value'),
            'overall_rank': item.get('overallRank'),
            'position_rank': item.get('positionRank'),
            'trend_30_day': item.get('trend30Day'),
            'redraft_dynasty_value_difference': item.get('redraftDynastyValueDifference'),
            'redraft_dynasty_value_perc_difference': item.get('redraftDynastyValuePercDifference'),
            'redraft_value': item.get('redraftValue'),
            'combined_value': item.get('combinedValue'),
            'tier': item.get('maybeTier'),
            'adp': item.get('maybeAdp'),
            'trade_frequency': item.get('maybeTradeFrequency'),
            
            # Standard deviation metrics
            'moving_std_dev': item.get('maybeMovingStandardDeviation'),
            'moving_std_dev_perc': item.get('maybeMovingStandardDeviationPerc'),
            'moving_std_dev_adjusted': item.get('maybeMovingStandardDeviationAdjusted'),

            # League settings this row was fetched under (tagged in fetch_all_combinations)
            'n_qb': item.get('n_qb'),
            'n_teams': item.get('n_teams'),
            'ppr': item.get('ppr'),
        }

        flattened_data.append(flattened_record)

    df = pl.DataFrame(flattened_data)
    if df.is_empty():
        return df
    # pin the tag dtypes so every partition agrees (a None-only column would be Null dtype)
    return df.with_columns(
        pl.col('n_qb').cast(pl.Int64),
        pl.col('n_teams').cast(pl.Int64),
        pl.col('ppr').cast(pl.Float64),
    )

def fetch_all_combinations() -> pl.DataFrame:
    combinations = SETTINGS_COMBINATIONS
    total_combinations = len(combinations)
    
    all_data = []

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Host': 'api.fantasycalc.com',
        'Origin': 'https://www.fantasycalc.com',
        'Pragma': 'no-cache',
        'Referer': 'https://www.fantasycalc.com/',
        'Sec-CH-UA': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'Sec-CH-UA-Mobile': '?0',
        'Sec-CH-UA-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
    }

    print(f"📊 Fetching {total_combinations} combinations...")
    for i, (qbs, teams, ppr) in enumerate(combinations, 1):
        url = f"https://api.fantasycalc.com/values/current?isDynasty=true&numQbs={qbs}&numTeams={teams}&ppr={ppr}&includeAdp=false"

        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 429:
                print(f"    ⚠️  Rate limited! Waiting 90 seconds...")
                time.sleep(90)
                response = requests.get(url, headers=headers, timeout=30)

            response.raise_for_status()
            
            data = response.json()
            
            # Add league setting columns to each record
            for item in data:
                item['n_qb'] = int(qbs)
                item['n_teams'] = int(teams)
                item['ppr'] = float(ppr)
            
            all_data.extend(data)
            
            print(f"    ✅ Fetched {len(data)} players (qbs: {qbs}, teams: {teams}, ppr: {ppr})")

            if i < total_combinations:
                sleep_time = max(1, np.random.normal(5, 2) + 1)
                time.sleep(sleep_time)

            # Every 5 requests, take a longer break
            if i % 5 == 0:
                long_break = random.uniform(10, 20)
                print(f"Extra break: {long_break:.1f}s...")
                time.sleep(long_break)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print(f"    🚫 Access forbidden (403) - might be blocked")
                print(f"    ⏸️  Pausing for 5 minutes before retrying...")
                time.sleep(300)
                continue
            else:
                print(f"    ❌ HTTP Error: {e}")
                continue
        except Exception as e:
            print(f"    ❌ Error: {e}")
            continue

    print(f"\n✅ Total records fetched: {len(all_data)}")
    
    df = flatten_player_data(all_data)
    return df


def save_df_to_gcs(df: pl.DataFrame):
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    load_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    # Add metadata columns
    df = df.with_columns([
        pl.lit(load_date).alias('load_date'),
        pl.lit(datetime.now(timezone.utc)).alias('loaded_at')
    ])
    
    file_path = (
        f"gs://{bucket_name}/bronze/fantasycalc/values/daily/load_date={load_date}/data.parquet"
    )
    
    print(file_path)
    df.write_parquet(file_path)
    
    print(f"✅ Saved {len(df)} records to {file_path}")

def main():
    df = fetch_all_combinations()
    save_df_to_gcs(df)


if __name__ == "__main__":
    main()
    
