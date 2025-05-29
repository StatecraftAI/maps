"""
process_census_households.py

This script processes census household data, focusing on demographic analysis within the
Portland Public Schools (PPS) district. It merges American Community Survey (ACS) data
with census block group geometries, performs spatial analysis, and prepares the data
for visualization and backend integration.

Now refactored to use the spatial_utils module for all spatial operations,
processing_utils for common infrastructure, and data_utils for data loading.

Key Functionality:
1. Data Loading and Validation:
   - Loads ACS household data from a JSON file.
   - Validates and processes demographic fields (e.g., households without minors).

2. Geospatial Processing:
   - Loads and validates census block group geometries.
   - Merges ACS data with block group geometries using standardized GEOIDs.
   - Filters block groups to those within PPS district boundaries.

3. Data Enrichment:
   - Calculates household density and percentage of households without minors.
   - Adds spatial metrics (e.g., area in square kilometers).

4. Data Export:
   - Exports optimized GeoJSON files for web visualization.
   - Uploads geospatial data to Supabase PostGIS database (optional).

5. Reporting:
   - Generates a detailed markdown report with statistics and analysis.

Usage:
- This script is typically used as part of the data pipeline for StatecraftAI's maps component.
- It prepares demographic data for visualization and analysis in the context of school board elections.

Input:
- ACS household data (JSON file).
- Census block group boundaries (GeoJSON file).
- PPS district boundaries (GeoJSON file).
- Configuration file (e.g., config.yaml) for file paths and processing settings.

Output:
- Optimized GeoJSON files for web mapping (household demographics within PPS district).
- Detailed markdown report with analysis and statistics.
- Uploaded geospatial data to Supabase PostGIS database (optional).

Example:
    python process_census_households.py --config config.yaml

Dependencies:
- geopandas, pandas, loguru, and other standard Python libraries.
- Supabase integration (optional) requires sqlalchemy and psycopg2-binary.
"""


import geopandas as gpd
import pandas as pd
from calculation_helpers import calculate_percentage
from data_utils import load_geojson, load_json_array, log_data_info
from loguru import logger

# Import processing and data utilities to eliminate boilerplate
from processing_utils import ProcessingContext, log_processing_step, log_success

# Import spatial operations from spatial_utils module
from spatial_utils import (
    filter_to_pps_district,
    merge_acs_with_geometries,
)


def load_and_process_acs_data(config) -> pd.DataFrame:
    """
    Load and process ACS household data using data_utils.
    Replaces the spatial_utils version with cleaner data_utils approach.
    """
    acs_path = config.get_input_path("acs_households_json")

    # Use data_utils for consistent loading
    acs_df = load_json_array(acs_path, header_row=0, data_start_row=1)
    if acs_df is None:
        return None

    log_data_info(acs_df, "ACS household data")

    # Process the data (same logic as before)
    try:
        # Rename columns for clarity
        acs_df = acs_df.rename(
            columns={
                acs_df.columns[0]: "GEOID",
                acs_df.columns[1]: "total_households",
                acs_df.columns[2]: "households_no_minors",
            }
        )

        # Convert to numeric
        acs_df["total_households"] = (
            pd.to_numeric(acs_df["total_households"], errors="coerce").fillna(0).astype(int)
        )
        acs_df["households_no_minors"] = (
            pd.to_numeric(acs_df["households_no_minors"], errors="coerce").fillna(0).astype(int)
        )

        # Calculate percentage
        acs_df["pct_households_no_minors"] = calculate_percentage(
            acs_df["households_no_minors"], acs_df["total_households"]
        )

        log_success(f"Processed ACS data: {len(acs_df):,} block groups")
        return acs_df

    except Exception as e:
        logger.error(f"❌ Error processing ACS data: {e}")
        return None


def load_block_group_boundaries(config) -> gpd.GeoDataFrame:
    """
    Load block group boundaries using data_utils.
    Replaces the spatial_utils version with cleaner data_utils approach.
    """
    bg_path = config.get_input_path("census_blocks_geojson")

    # Use data_utils for consistent loading with Multnomah County filtering
    bg_gdf = load_geojson(
        bg_path,
        validate_crs=True,
        fix_geometries=True,
        filter_county="051",  # Multnomah County
    )

    if bg_gdf is not None:
        log_data_info(bg_gdf, "Block group boundaries")

    return bg_gdf


def main() -> None:
    """Main execution function with comprehensive error handling."""

    # Use ProcessingContext to handle all infrastructure boilerplate
    with ProcessingContext("Household Demographics Analysis with Optimized Export") as ctx:
        # 1. Load and Process ACS Data
        log_processing_step("Loading and processing ACS household data")
        acs_df = load_and_process_acs_data(ctx.config)
        if acs_df is None:
            return

        # 2. Load Block Group Geometries
        log_processing_step("Loading and validating block group geometries")
        bg_gdf = load_block_group_boundaries(ctx.config)
        if bg_gdf is None:
            return

        # 3. Merge Data with Geometries
        log_processing_step("Merging ACS data with block group geometries")
        merged_gdf = merge_acs_with_geometries(acs_df, bg_gdf)
        if merged_gdf is None:
            return

        # 4. Filter to PPS District
        log_processing_step("Filtering to PPS district boundaries")
        pps_gdf = filter_to_pps_district(merged_gdf, ctx.config)
        if pps_gdf is None:
            return

        # 5. Upload to Supabase using the context manager's upload method
        ctx.upload_to_supabase(
            pps_gdf,
            table_name="household_demographics_pps",
            description="Household demographics by block group within PPS district - focused on households without minors for school board election analysis",
        )


if __name__ == "__main__":
    main()
