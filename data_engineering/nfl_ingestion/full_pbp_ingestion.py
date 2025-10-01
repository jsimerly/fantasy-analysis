from datetime import datetime
import os
from typing import Any, Optional, List

import polars as pl
import nflreadpy as nfl
from dotenv import load_dotenv

load_dotenv()

def fetch_all_pbp_data(seasons: Optional[List[int]] = None, chunk_size: int = 3) -> dict[str, pl.DataFrame]:
    """
    Fetch all available NFL play-by-play data.
    
    Args:
        seasons: List of seasons to fetch. If None, fetches all available seasons (1999-current)
        chunk_size: Number of seasons to process at once to manage memory usage
    
    Returns:
        Dictionary containing PBP datasets
    """
    datasets = {}
    current_year = datetime.now().year
    
    if seasons is None:
        available_seasons = list(range(1999, current_year + 1))
    else:
        available_seasons = seasons
    
    try:
        print(f"Fetching NFL play-by-play data for {len(available_seasons)} seasons...")
        print(f"Seasons: {min(available_seasons)}-{max(available_seasons)}")
        
        # Process seasons in chunks to manage memory
        all_pbp_data = []
        
        for i in range(0, len(available_seasons), chunk_size):
            chunk_seasons = available_seasons[i:i + chunk_size]
            print(f"- Processing seasons {chunk_seasons}...")
            
            try:
                chunk_pbp = nfl.load_pbp(chunk_seasons)
                
                if chunk_pbp is not None and chunk_pbp.height > 0:
                    print(f"  ✓ Loaded {chunk_pbp.height:,} plays for seasons {chunk_seasons}")
                    all_pbp_data.append(chunk_pbp)
                else:
                    print(f"  ⚠ No data found for seasons {chunk_seasons}")
                    
            except Exception as e:
                print(f"  ✗ Error loading seasons {chunk_seasons}: {e}")
                continue
        
        # Combine all chunks with schema harmonization
        if all_pbp_data:
            print("- Combining all play-by-play data...")
            
            try:
                # First, let's check and align schemas
                print("  → Checking schemas for compatibility...")
                
                # Get all unique column names and their types
                all_columns = set()
                schemas = []
                
                for i, chunk in enumerate(all_pbp_data):
                    schema = dict(chunk.schema)
                    schemas.append(schema)
                    all_columns.update(schema.keys())
                    print(f"    Chunk {i+1}: {chunk.height:,} rows, {len(schema)} columns")
                
                print(f"  → Found {len(all_columns)} unique columns across all chunks")
                
                # Harmonize schemas by determining common types across all chunks
                harmonized_chunks = []
                
                # First pass: collect all column types across chunks
                column_types = {}
                for chunk in all_pbp_data:
                    for col_name, col_dtype in chunk.schema.items():
                        if col_name not in column_types:
                            column_types[col_name] = set()
                        column_types[col_name].add(col_dtype)
                
                # Determine target types for each column
                target_types = {}
                for col_name, dtypes in column_types.items():
                    if len(dtypes) == 1:
                        # All chunks have same type, keep it
                        target_types[col_name] = list(dtypes)[0]
                    else:
                        # Type conflict - choose the most compatible type
                        if any(dt in [pl.Float64, pl.Float32] for dt in dtypes):
                            target_types[col_name] = pl.Float64
                        elif any(dt in [pl.Int64, pl.Int32] for dt in dtypes):
                            target_types[col_name] = pl.Int64
                        elif pl.Utf8 in dtypes:
                            target_types[col_name] = pl.Utf8
                        else:
                            # Default to string for unknown conflicts
                            target_types[col_name] = pl.Utf8
                        
                        print(f"    Type conflict in '{col_name}': {dtypes} → {target_types[col_name]}")
                
                # Second pass: harmonize each chunk to target types
                for i, chunk in enumerate(all_pbp_data):
                    print(f"  → Harmonizing chunk {i+1}...")
                    
                    harmonized_chunk = chunk
                    
                    for col_name in chunk.columns:
                        current_dtype = chunk[col_name].dtype
                        target_dtype = target_types[col_name]
                        
                        if current_dtype != target_dtype:
                            try:
                                # Handle nullable integer conversions carefully
                                if current_dtype in [pl.Int32, pl.Int64] and target_dtype == pl.Float64:
                                    harmonized_chunk = harmonized_chunk.with_columns(
                                        pl.col(col_name).cast(pl.Float64)
                                    )
                                elif current_dtype == pl.Float64 and target_dtype in [pl.Int32, pl.Int64]:
                                    # Convert float to int, handling nulls
                                    harmonized_chunk = harmonized_chunk.with_columns(
                                        pl.col(col_name).cast(target_dtype, strict=False)
                                    )
                                else:
                                    # General case conversion
                                    harmonized_chunk = harmonized_chunk.with_columns(
                                        pl.col(col_name).cast(target_dtype, strict=False)
                                    )
                                    
                            except Exception as e:
                                print(f"    ⚠ Could not convert {col_name} from {current_dtype} to {target_dtype}: {e}")
                                # As fallback, convert to string
                                try:
                                    harmonized_chunk = harmonized_chunk.with_columns(
                                        pl.col(col_name).cast(pl.Utf8)
                                    )
                                except:
                                    pass  # Keep original type if all else fails
                    
                    harmonized_chunks.append(harmonized_chunk)
                
                # Now try to combine with rechunk for better performance
                print("  → Concatenating harmonized chunks...")
                datasets['pbp_all_seasons'] = pl.concat(harmonized_chunks, rechunk=True)
                print(f"  ✓ Total PBP records: {datasets['pbp_all_seasons'].height:,}")
                
                # Optional: Create season-specific datasets if needed
                if len(available_seasons) > 1:
                    print("- Creating season-specific PBP datasets...")
                    for season in available_seasons[-3:]:  # Only create separate files for last 3 seasons
                        try:
                            if 'season' in datasets['pbp_all_seasons'].columns:
                                season_data = datasets['pbp_all_seasons'].filter(pl.col('season') == season)
                                if season_data.height > 0:
                                    datasets[f'pbp_{season}'] = season_data
                                    print(f"  ✓ Season {season}: {season_data.height:,} plays")
                        except Exception as e:
                            print(f"  ⚠ Could not create dataset for season {season}: {e}")
                            
            except Exception as concat_error:
                print(f"  ✗ Error combining chunks: {concat_error}")
                print("  → Saving chunks individually instead...")
                
                # Fallback: Save each chunk individually
                for i, chunk in enumerate(all_pbp_data):
                    start_idx = i * chunk_size
                    end_idx = min(start_idx + chunk_size, len(available_seasons))
                    chunk_seasons = available_seasons[start_idx:end_idx]
                    dataset_name = f'pbp_seasons_{min(chunk_seasons)}_{max(chunk_seasons)}'
                    datasets[dataset_name] = chunk
                    print(f"  ✓ Saved chunk as {dataset_name}: {chunk.height:,} plays")
                    
        else:
            print("No play-by-play data was successfully loaded.")
            
        print(f"Successfully created {len(datasets)} PBP datasets.")
        return datasets
        
    except Exception as e:
        print(f"An error occurred while fetching PBP data: {e}")
        return datasets

def save_pbp_datasets_to_gcs(datasets: dict[str, pl.DataFrame], bucket_name: str, base_date: str) -> dict[str, bool]:
    """
    Save PBP datasets to GCS with optimized partitioning for large data.
    
    Args:
        datasets: Dictionary of PBP DataFrames
        bucket_name: GCS bucket name
        base_date: Date string for partitioning
    
    Returns:
        Dictionary tracking save success for each dataset
    """
    results = {}
    
    for dataset_name, df in datasets.items():
        if df is None or df.height == 0:
            print(f"Skipping empty dataset: {dataset_name}")
            results[dataset_name] = False
            continue
        
        file_path = f'bronze/play_by_play/load_date={base_date}/{dataset_name}.parquet'
        
        try:
            gcs_path = f"gs://{bucket_name}/{file_path}"
            
            print(f"  → Saving {dataset_name}: {df.height:,} rows, {df.width} columns")
            
            # For very large datasets, consider partitioning by season
            if df.height > 1_000_000 and 'all_seasons' in dataset_name:
                print(f"    Large dataset detected, saving with compression...")
                df.write_parquet(
                    gcs_path,
                    compression='zstd',  # Better compression for large files
                    statistics=True,     # Enable statistics for faster queries
                    row_group_size=100_000  # Optimize row group size
                )
            else:
                df.write_parquet(gcs_path)
                
            print(f"✓ Saved {dataset_name} to {file_path}")
            results[dataset_name] = True
            
        except Exception as e:
            print(f"✗ Failed to save {dataset_name}: {e}")
            results[dataset_name] = False
    
    return results

def main_pbp_pipeline(request: Any = None):
    """
    Main function for PBP data pipeline - can be deployed as separate Cloud Function
    """
    try:
        bucket_name = os.environ.get('GCS_BUCKET_NAME')
        
        if not bucket_name:
            print("ERROR: GCS_BUCKET_NAME environment variable not set.")
            return 'Deployment Error: Missing GCS_BUCKET_NAME', 500

        # Fetch all PBP data
        print("=== STARTING PBP DATA PIPELINE ===")
        datasets = fetch_all_pbp_data()
        
        if not datasets:
            print("No PBP datasets fetched successfully.")
            return 'No PBP data fetched', 200
        
        # Save to GCS
        current_date = datetime.now().strftime('%Y-%m-%d')
        results = save_pbp_datasets_to_gcs(datasets, bucket_name, current_date)
        
        successful_saves = sum(results.values())
        total_datasets = len(datasets)
        
        print(f"\n=== PBP PIPELINE SUMMARY ===")
        print(f"PBP datasets fetched: {total_datasets}")
        print(f"Successfully saved: {successful_saves}")
        print(f"Failed saves: {total_datasets - successful_saves}")
        
        # Calculate total records processed
        total_records = sum(df.height for df in datasets.values() if df is not None)
        print(f"Total PBP records processed: {total_records:,}")
        
        return f'PBP Pipeline completed: {successful_saves}/{total_datasets} datasets saved', 200
            
    except Exception as e:
        print(f"FATAL ERROR in PBP pipeline: {e}")
        return 'Internal Server Error', 500

# Integration function to add PBP to your existing pipeline
def fetch_all_nfl_data_with_pbp() -> dict[str, pl.DataFrame]:
    """
    Enhanced version of your original function that includes PBP data
    """
    datasets = {}
    current_year = datetime.now().year
    available_seasons = list(range(1999, current_year+1))

    try:
        print("Fetching comprehensive NFL data from nflverse (including PBP)...")
        
        # Core player data
        print("- Fetching players...")
        datasets['players'] = nfl.load_players()
        print(f"  ✓ Players: {len(datasets['players'])} records")
        
        # Fantasy player IDs
        print("- Fetching fantasy player IDs...")
        datasets['ff_playerids'] = nfl.load_ff_playerids()
        print(f"  ✓ Fantasy IDs: {len(datasets['ff_playerids'])} records")
        
        # Player stats
        print("- Fetching ALL seasons player stats...")
        datasets['player_stats_all_seasons'] = nfl.load_player_stats(available_seasons)
        print(f"  ✓ All Seasons Stats: {len(datasets['player_stats_all_seasons'])} records")
        
        # Rosters
        print("- Fetching ALL seasons rosters...")
        datasets['rosters_all_seasons'] = nfl.load_rosters(available_seasons)
        print(f"  ✓ All Seasons Rosters: {len(datasets['rosters_all_seasons'])} records")
        
        # Schedules
        print("- Fetching ALL seasons schedules...")
        datasets['schedules_all_seasons'] = nfl.load_schedules(available_seasons)
        print(f"  ✓ All Seasons Schedules: {len(datasets['schedules_all_seasons'])} records")
        
        # Team stats
        print("- Fetching team stats...")
        datasets['team_stats'] = nfl.load_team_stats(seasons=True) 
        print(f"  ✓ Team Stats: {len(datasets['team_stats'])} records")
        
        # ADD PBP DATA
        print("\n=== FETCHING PLAY-BY-PLAY DATA ===")
        pbp_datasets = fetch_all_pbp_data()
        datasets.update(pbp_datasets)
        
        print(f"\nSuccessfully fetched {len(datasets)} datasets covering all available seasons.")
        return datasets
        
    except Exception as e:
        print(f"An error occurred while fetching data: {e}")
        return datasets

if __name__ == "__main__":
    # Run just the PBP pipeline
    main_pbp_pipeline()