import os
from datetime import datetime, timezone

import polars as pl
from dotenv import load_dotenv

from utils import (
    get_dynasty_playersArray
)

load_dotenv()

def flatten_player_data(player: dict) -> dict:
    # Std player fields
    flat = {
        'playerName': player.get('playerName'),
        'playerID': player.get('playerID'),
        'slug': player.get('slug'),
        'position': player.get('position'),
        'positionID': player.get('positionID'),
        'isTrending': player.get('isTrending'),
    }
    
    # OneQB Values
    oneqb: dict = player.get('oneQBValues')
    if oneqb:
        flat.update({
            'oneqb_startSitValue': oneqb.get('startSitValue'),
            'oneqb_overallTrend': oneqb.get('overallTrend'),
            'oneqb_positionalTrend': oneqb.get('positionalTrend'),
            'oneqb_overall7DayTrend': oneqb.get('overall7DayTrend'),
            'oneqb_positional7DayTrend': oneqb.get('positional7DayTrend'),
            'oneqb_kept': oneqb.get('kept'),
            'oneqb_traded': oneqb.get('traded'),
            'oneqb_cut': oneqb.get('cut'),
            'oneqb_diff': oneqb.get('diff'),
            'oneqb_isOutThisWeek': oneqb.get('isOutThisWeek'),
            'oneqb_adp': oneqb.get('adp'),
            'oneqb_avgAuctionPercentage': oneqb.get('avgAuctionPercentage'),
            'oneqb_startupAdp': oneqb.get('startupAdp'),
            'oneqb_startupAvgAuctionPercentage': oneqb.get('startupAvgAuctionPercentage'),
            'oneqb_rawLiquidity': oneqb.get('rawLiquidity'),
            'oneqb_stdLiquidity': oneqb.get('stdLiquidity'),
            'oneqb_tradeCount': oneqb.get('tradeCount'),
            'oneqb_value': oneqb.get('value'),
            'oneqb_rank': oneqb.get('rank'),
            'oneqb_positionalRank': oneqb.get('positionalRank'),
            'oneqb_overallTier': oneqb.get('overallTier'),
            'oneqb_positionalTier': oneqb.get('positionalTier'),
        })
        
        # OneQB TEP
        tep = oneqb.get('tep')
        if tep:
            flat.update({
                'oneqb_tep_value': tep.get('value'),
                'oneqb_tep_rank': tep.get('rank'),
                'oneqb_tep_positionalRank': tep.get('positionalRank'),
                'oneqb_tep_overallTier': tep.get('overallTier'),
                'oneqb_tep_positionalTier': tep.get('positionalTier'),
            })
        
        # OneQB TEPP
        tepp = oneqb.get('tepp')
        if tepp:
            flat.update({
                'oneqb_tepp_value': tepp.get('value'),
                'oneqb_tepp_rank': tepp.get('rank'),
                'oneqb_tepp_positionalRank': tepp.get('positionalRank'),
                'oneqb_tepp_overallTier': tepp.get('overallTier'),
                'oneqb_tepp_positionalTier': tepp.get('positionalTier'),
            })
        
        # OneQB TEPPP
        teppp = oneqb.get('teppp')
        if teppp:
            flat.update({
                'oneqb_teppp_value': teppp.get('value'),
                'oneqb_teppp_rank': teppp.get('rank'),
                'oneqb_teppp_positionalRank': teppp.get('positionalRank'),
                'oneqb_teppp_overallTier': teppp.get('overallTier'),
                'oneqb_teppp_positionalTier': teppp.get('positionalTier'),
            })
    
    # Superflex Values 
    sf: dict = player.get('superflexValues')
    if sf:
        flat.update({
            'sf_startSitValue': sf.get('startSitValue'),
            'sf_overallTrend': sf.get('overallTrend'),
            'sf_positionalTrend': sf.get('positionalTrend'),
            'sf_overall7DayTrend': sf.get('overall7DayTrend'),
            'sf_positional7DayTrend': sf.get('positional7DayTrend'),
            'sf_kept': sf.get('kept'),
            'sf_traded': sf.get('traded'),
            'sf_cut': sf.get('cut'),
            'sf_diff': sf.get('diff'),
            'sf_isOutThisWeek': sf.get('isOutThisWeek'),
            'sf_adp': sf.get('adp'),
            'sf_avgAuctionPercentage': sf.get('avgAuctionPercentage'),
            'sf_startupAdp': sf.get('startupAdp'),
            'sf_startupAvgAuctionPercentage': sf.get('startupAvgAuctionPercentage'),
            'sf_rawLiquidity': sf.get('rawLiquidity'),
            'sf_stdLiquidity': sf.get('stdLiquidity'),
            'sf_tradeCount': sf.get('tradeCount'),
            'sf_value': sf.get('value'),
            'sf_rank': sf.get('rank'),
            'sf_positionalRank': sf.get('positionalRank'),
            'sf_overallTier': sf.get('overallTier'),
            'sf_positionalTier': sf.get('positionalTier'),
        })
        
        # SF TEP
        tep: dict = sf.get('tep')
        if tep:
            flat.update({
                'sf_tep_value': tep.get('value'),
                'sf_tep_rank': tep.get('rank'),
                'sf_tep_positionalRank': tep.get('positionalRank'),
                'sf_tep_overallTier': tep.get('overallTier'),
                'sf_tep_positionalTier': tep.get('positionalTier'),
            })
        
        # SF TEPP
        tepp: dict = sf.get('tepp')
        if tepp:
            flat.update({
                'sf_tepp_value': tepp.get('value'),
                'sf_tepp_rank': tepp.get('rank'),
                'sf_tepp_positionalRank': tepp.get('positionalRank'),
                'sf_tepp_overallTier': tepp.get('overallTier'),
                'sf_tepp_positionalTier': tepp.get('positionalTier'),
            })
        
        # SF TEPPP
        teppp: dict = sf.get('teppp')
        if teppp:
            flat.update({
                'sf_teppp_value': teppp.get('value'),
                'sf_teppp_rank': teppp.get('rank'),
                'sf_teppp_positionalRank': teppp.get('positionalRank'),
                'sf_teppp_overallTier': teppp.get('overallTier'),
                'sf_teppp_positionalTier': teppp.get('positionalTier'),
            })
    
    return flat

def set_dtypes(df: pl.DataFrame) -> pl.DataFrame:  
    # Integer columns (use Int16/Int32 for nullable integers)
    int_cols = [
        'positionID',
        'oneqb_kept', 'oneqb_traded', 'oneqb_cut', 'oneqb_tradeCount',
        'oneqb_rank', 'oneqb_positionalRank', 'oneqb_overallTier', 'oneqb_positionalTier',
        'oneqb_tep_rank', 'oneqb_tep_positionalRank', 'oneqb_tep_overallTier', 'oneqb_tep_positionalTier',
        'oneqb_tepp_rank', 'oneqb_tepp_positionalRank', 'oneqb_tepp_overallTier', 'oneqb_tepp_positionalTier',
        'oneqb_teppp_rank', 'oneqb_teppp_positionalRank', 'oneqb_teppp_overallTier', 'oneqb_teppp_positionalTier',
        'sf_kept', 'sf_traded', 'sf_cut', 'sf_tradeCount',
        'sf_rank', 'sf_positionalRank', 'sf_overallTier', 'sf_positionalTier',
        'sf_tep_rank', 'sf_tep_positionalRank', 'sf_tep_overallTier', 'sf_tep_positionalTier',
        'sf_tepp_rank', 'sf_tepp_positionalRank', 'sf_tepp_overallTier', 'sf_tepp_positionalTier',
        'sf_teppp_rank', 'sf_teppp_positionalRank', 'sf_teppp_overallTier', 'sf_teppp_positionalTier',
    ]
    
    # Float columns
    float_cols = [
        'oneqb_startSitValue', 'oneqb_overallTrend', 'oneqb_positionalTrend',
        'oneqb_overall7DayTrend', 'oneqb_positional7DayTrend', 'oneqb_diff',
        'oneqb_adp', 'oneqb_avgAuctionPercentage', 'oneqb_startupAdp',
        'oneqb_startupAvgAuctionPercentage', 'oneqb_rawLiquidity', 'oneqb_stdLiquidity',
        'oneqb_value', 'oneqb_tep_value', 'oneqb_tepp_value', 'oneqb_teppp_value',
        'sf_startSitValue', 'sf_overallTrend', 'sf_positionalTrend',
        'sf_overall7DayTrend', 'sf_positional7DayTrend', 'sf_diff',
        'sf_adp', 'sf_avgAuctionPercentage', 'sf_startupAdp',
        'sf_startupAvgAuctionPercentage', 'sf_rawLiquidity', 'sf_stdLiquidity',
        'sf_value', 'sf_tep_value', 'sf_tepp_value', 'sf_teppp_value',
    ]
    
    # Boolean columns
    bool_cols = ['isTrending', 'oneqb_isOutThisWeek', 'sf_isOutThisWeek']

    cast_exprs = []
    for col in int_cols:
        if col in df.columns:
            cast_exprs.append(pl.col(col).cast(pl.Int32))
    
    for col in float_cols:
        if col in df.columns:
            cast_exprs.append(pl.col(col).cast(pl.Float32))
    
    for col in bool_cols:
        if col in df.columns:
            cast_exprs.append(pl.col(col).cast(pl.Boolean))
    
    if cast_exprs:
        df = df.with_columns(cast_exprs)
    
    return df

def save_player_to_gcs(df: pl.DataFrame, bucket_name: str,  base_date: str):
    file_path = f"gs://{bucket_name}/bronze/ktc/dynasty/daily_load/load_date={base_date}/player_data.parquet"  
    
    try:
        df.write_parquet(file_path)
        
    except Exception as e:
        print(f"Failed to save to GCS: {e}")
        raise


def main():
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    current_date =  datetime.now(timezone.utc).strftime('%Y-%m-%d')

    playerArray = get_dynasty_playersArray()
    flattened = [flatten_player_data(player) for player in playerArray]
    df = pl.from_dicts(flattened)
    df = set_dtypes(df)
    save_player_to_gcs(df, bucket_name, current_date)
    print(f"✓ Saved {len(playerArray)} players.")

if __name__ == "__main__":
    main()