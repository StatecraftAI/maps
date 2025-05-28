"""
process_census_households.py

This script processes census household data, focusing on demographic analysis within the
Portland Public Schools (PPS) district. It merges American Community Survey (ACS) data
with census block group geometries, performs spatial analysis, and prepares the data
for visualization and backend integration.

Now refactored to use the universal GeoJSON processor for all common operations.

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

import sys
from pathlib import Path

from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))
from ops import Config

# Import all functions from the universal processor
from process_geojson_universal import (
    load_and_process_acs_data,
    load_block_group_boundaries,
    merge_acs_with_geometries,
    filter_to_pps_district,
    SUPABASE_AVAILABLE,
    SupabaseUploader,
    SupabaseDatabase,
    SpatialQueryManager
)


def main() -> None:
    """Main execution function with comprehensive error handling."""
    logger.info("🏠 Household Demographics Analysis with Optimized Export")
    logger.info("=" * 65)

    # Load configuration
    try:
        config = Config()
        logger.info(f"📋 Project: {config.get('project_name')}")
        logger.info(f"📋 Description: {config.get('description')}")
    except Exception as e:
        logger.critical(f"❌ Configuration error: {e}")
        logger.info("💡 Make sure config.yaml exists in the analysis directory")
        sys.exit(1)

    # 1. Load and Process ACS Data
    logger.info("📊 Loading and processing ACS household data...")
    acs_df = load_and_process_acs_data(config)
    if acs_df is None:
        sys.exit(1)

    # 2. Load Block Group Geometries
    logger.info("🗺️ Loading and validating block group geometries...")
    bg_gdf = load_block_group_boundaries(config)
    if bg_gdf is None:
        sys.exit(1)

    # 3. Merge Data with Geometries
    logger.info("🔗 Merging ACS data with block group geometries...")
    merged_gdf = merge_acs_with_geometries(acs_df, bg_gdf)
    if merged_gdf is None:
        sys.exit(1)

    # 4. Filter to PPS District
    logger.info("🎯 Filtering to PPS district boundaries...")
    pps_gdf = filter_to_pps_district(merged_gdf, config)
    if pps_gdf is None:
        sys.exit(1)

    # 5. Upload to Supabase (Optional)
    if SUPABASE_AVAILABLE:
        logger.info("🚀 Uploading to Supabase PostGIS database...")

        try:
            # Use SupabaseUploader directly for uploads
            uploader = SupabaseUploader(config)

            # Upload household demographics for PPS district
            upload_success = uploader.upload_geodataframe(
                pps_gdf,
                table_name="household_demographics_pps",
                description="Household demographics by block group within PPS district - focused on households without minors for school board election analysis",
            )

            if upload_success:
                logger.success("   ✅ Uploaded household demographics to Supabase")

                # Verify upload using query manager for data validation
                try:
                    db = SupabaseDatabase(config)
                    query_manager = SpatialQueryManager(db)

                    # Check if table exists and get sample records
                    if query_manager.table_exists("household_demographics_pps"):
                        sample_records = query_manager.get_sample_records(
                            "household_demographics_pps", limit=5
                        )
                        logger.debug(f"   📊 Verified upload: {len(sample_records)} sample records")
                        logger.info(
                            "🌐 Backend: Data is now available via Supabase PostGIS for fast spatial queries"
                        )

                        if len(sample_records) == 0:
                            logger.warning("   ⚠️ Table exists but contains no data")
                    else:
                        logger.error("   ❌ Table was not created despite successful upload")

                except Exception as verification_error:
                    logger.warning(
                        f"   ⚠️ Upload succeeded but verification failed: {verification_error}"
                    )
                    logger.info("     💡 This might be a temporary connectivity issue")
            else:
                logger.error("   ❌ Upload failed - table was not created")
                logger.info("   💡 Common issues and solutions:")
                logger.info(
                    "      1. Check Supabase credentials (SUPABASE_DB_HOST, SUPABASE_DB_PASSWORD)"
                )
                logger.info(
                    "      2. Ensure PostGIS extension is enabled: CREATE EXTENSION postgis;"
                )
                logger.info("      3. Verify database connectivity and permissions")
                logger.info("      4. Check if the database has sufficient storage space")

        except Exception as e:
            logger.error(f"❌ Supabase upload failed: {e}")
            logger.info("   💡 Check your Supabase credentials and connection")
            # Add more specific error handling
            import traceback

            logger.trace("Detailed upload error:")
            logger.trace(traceback.format_exc())
    else:
        logger.info("📊 Supabase integration not available - skipping database upload")
        logger.info("   💡 Install dependencies with: pip install sqlalchemy psycopg2-binary")


if __name__ == "__main__":
    main()
