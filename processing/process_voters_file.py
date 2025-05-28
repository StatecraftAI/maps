"""
process_voters_file.py

This script processes voter registration data for spatial analysis within the
Portland Public Schools (PPS) district. It creates various spatial aggregations
of voter data including hexagonal grids, block group analysis, and district classification.

Now refactored to use the spatial_utils module for all spatial operations.
"""

import sys
from pathlib import Path

from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))
from ops import Config

# Import all functions from the spatial_utils module
from spatial_utils import (
    load_and_validate_voter_data,
    load_pps_district_boundaries,
    load_block_group_boundaries,
    create_voter_geodataframe,
    create_hexagonal_aggregation,
    filter_to_pps_district,
    classify_by_spatial_join,
    create_grid_aggregation,
    analyze_points_by_polygons,
    validate_and_reproject_to_wgs84,
    optimize_geojson_properties,
    clean_numeric,
    SUPABASE_AVAILABLE,
    SupabaseUploader,
    SupabaseDatabase,
    SpatialQueryManager
)


def main() -> None:
    """Main execution function with comprehensive error handling."""
    logger.info("🗳️ Voter Registration Analysis with Spatial Aggregation")
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

    # 1. Load and validate voter data
    logger.info("📊 Loading and validating voter data...")
    voters_df = load_and_validate_voter_data(config)
    if voters_df is None:
        sys.exit(1)

    # 2. Create voter GeoDataFrame
    logger.info("🗺️ Creating voter GeoDataFrame...")
    voters_gdf = create_voter_geodataframe(voters_df)
    if voters_gdf is None:
        sys.exit(1)

    # 3. Load PPS district boundaries
    logger.info("🎯 Loading PPS district boundaries...")
    districts_gdf = load_pps_district_boundaries(config)
    if districts_gdf is None:
        sys.exit(1)

    # 4. Classify voters by district using the generalized spatial join function
    logger.info("🎯 Classifying voters by PPS district...")
    voters_classified = classify_by_spatial_join(voters_gdf, districts_gdf, "within_pps")
    if voters_classified is None:
        sys.exit(1)

    # 5. Create hexagonal aggregation
    logger.info("🔷 Creating hexagonal aggregation...")
    hex_gdf = create_hexagonal_aggregation(voters_classified, config, resolution=8)
    if hex_gdf is not None:
        logger.success(f"  ✅ Created hexagonal aggregation with {len(hex_gdf):,} hexagons")

    # 6. Load block groups and analyze using the generalized function
    logger.info("🏘️ Loading block group boundaries...")
    block_groups_gdf = load_block_group_boundaries(config)
    if block_groups_gdf is not None:
        bg_analysis = analyze_points_by_polygons(
            voters_classified, 
            block_groups_gdf,
            polygon_id_col="GEOID",
            point_id_col="VOTER_ID", 
            count_col="voter_count",
            density_col="voter_density"
        )
        if bg_analysis is not None:
            logger.success(f"  ✅ Completed block group analysis")

    # 7. Create grid aggregation using the generalized function
    logger.info("📐 Creating grid aggregation...")
    grid_gdf = create_grid_aggregation(voters_classified, grid_size=0.01, count_col="voter_count")
    if grid_gdf is not None:
        logger.success(f"  ✅ Created grid aggregation with {len(grid_gdf):,} cells")

    # 8. Upload to Supabase (Optional)
    if SUPABASE_AVAILABLE and hex_gdf is not None:
        logger.info("🚀 Uploading voter hexagons to Supabase...")

        try:
            uploader = SupabaseUploader(config)

            # Upload hexagonal voter aggregation
            upload_success = uploader.upload_geodataframe(
                hex_gdf,
                table_name="voter_hexagons",
                description="Hexagonal aggregation of voter registration data within PPS district",
            )

            if upload_success:
                logger.success("   ✅ Uploaded voter hexagons to Supabase")
            else:
                logger.error("   ❌ Upload failed")

        except Exception as e:
            logger.error(f"❌ Supabase upload failed: {e}")
    else:
        logger.info("📊 Supabase integration not available or no data to upload")


if __name__ == "__main__":
    main()
