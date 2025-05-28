"""
process_voters_file.py

This script processes voter registration data for spatial analysis within the
Portland Public Schools (PPS) district. It creates various spatial aggregations
of voter data including hexagonal grids, block group analysis, and district classification.

Now refactored to use the spatial_utils module for all spatial operations
and processing_utils for common infrastructure.
"""

import sys
from pathlib import Path

# Import processing utilities to eliminate boilerplate
from processing_utils import ProcessingContext, log_processing_step, log_success

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
)


def main() -> None:
    """Main execution function with comprehensive error handling."""
    
    # Use ProcessingContext to handle all infrastructure boilerplate
    with ProcessingContext("Voter Registration Analysis with Spatial Aggregation") as ctx:
        
        # 1. Load and validate voter data
        log_processing_step("Loading and validating voter data")
        voters_df = load_and_validate_voter_data(ctx.config)
        if voters_df is None:
            return

        # 2. Create voter GeoDataFrame
        log_processing_step("Creating voter GeoDataFrame")
        voters_gdf = create_voter_geodataframe(voters_df)
        if voters_gdf is None:
            return

        # 3. Load PPS district boundaries
        log_processing_step("Loading PPS district boundaries")
        districts_gdf = load_pps_district_boundaries(ctx.config)
        if districts_gdf is None:
            return

        # 4. Classify voters by district using the generalized spatial join function
        log_processing_step("Classifying voters by PPS district")
        voters_classified = classify_by_spatial_join(voters_gdf, districts_gdf, "within_pps")
        if voters_classified is None:
            return

        # 5. Create hexagonal aggregation
        log_processing_step("Creating hexagonal aggregation")
        hex_gdf = create_hexagonal_aggregation(voters_classified, ctx.config, resolution=8)
        if hex_gdf is not None:
            log_success(f"Created hexagonal aggregation with {len(hex_gdf):,} hexagons")

        # 6. Load block groups and analyze using the generalized function
        log_processing_step("Loading block group boundaries")
        block_groups_gdf = load_block_group_boundaries(ctx.config)
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
                log_success("Completed block group analysis")

        # 7. Create grid aggregation using the generalized function
        log_processing_step("Creating grid aggregation")
        grid_gdf = create_grid_aggregation(voters_classified, grid_size=0.01, count_col="voter_count")
        if grid_gdf is not None:
            log_success(f"Created grid aggregation with {len(grid_gdf):,} cells")

        # 8. Upload to Supabase using the context manager's upload method
        if hex_gdf is not None:
            ctx.upload_to_supabase(
                hex_gdf,
                table_name="voter_hexagons",
                description="Hexagonal aggregation of voter registration data within PPS district"
            )


if __name__ == "__main__":
    main()
