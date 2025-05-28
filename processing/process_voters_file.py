"""
process_voters_file.py

This script processes voter registration data for spatial analysis within the
Portland Public Schools (PPS) district. It creates various spatial aggregations
of voter data including hexagonal grids, block group analysis, and district classification.

Now refactored to use the universal GeoJSON processor for all common operations.
"""

import sys
from pathlib import Path

from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))
from ops import Config

# Import all functions from the universal processor
from process_geojson_universal import (
    load_and_validate_voter_data,
    load_pps_district_boundaries,
    load_block_group_boundaries,
    create_voter_geodataframe,
    create_hexagonal_aggregation,
    filter_to_pps_district,
    SUPABASE_AVAILABLE,
    SupabaseUploader,
    SupabaseDatabase,
    SpatialQueryManager
)

# Import remaining functions that are unique to this script
try:
    from process_election_results import (
        clean_numeric,
        optimize_geojson_properties,
        validate_and_reproject_to_wgs84,
    )
    logger.debug("✅ Imported optimization functions from process_election_results")
except ImportError as e:
    logger.warning(f"⚠️ Could not import optimization functions: {e}")
    
    # Fallback implementations
    def validate_and_reproject_to_wgs84(gdf, config, source_description="GeoDataFrame"):
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs("EPSG:4326")
        return gdf

    def optimize_geojson_properties(gdf, config):
        return gdf

    def clean_numeric(series, is_percent=False):
        return pd.to_numeric(series, errors="coerce").fillna(0)


def classify_voters_by_district(voters_gdf, districts_gdf):
    """
    Classify voters by PPS district using spatial join.
    """
    logger.info("🎯 Classifying voters by PPS district...")

    try:
        # Ensure consistent CRS
        if voters_gdf.crs != districts_gdf.crs:
            districts_gdf = districts_gdf.to_crs(voters_gdf.crs)

        # Spatial join to classify voters
        voters_with_district = voters_gdf.sjoin(districts_gdf, how="left", predicate="within")
        
        # Add district classification
        voters_with_district["within_pps"] = ~voters_with_district.index_right.isna()
        
        logger.success(f"  ✅ Classified {len(voters_with_district):,} voters")
        
        # Summary statistics
        pps_voters = voters_with_district[voters_with_district["within_pps"]]
        logger.info(f"     📊 Voters within PPS: {len(pps_voters):,}")
        logger.info(f"     📊 PPS coverage: {len(pps_voters) / len(voters_with_district) * 100:.1f}%")
        
        return voters_with_district

    except Exception as e:
        logger.critical(f"❌ Error classifying voters by district: {e}")
        return None


def create_grid_aggregation(voters_gdf, config, grid_size=0.01):
    """
    Create grid-based aggregation of voter data.
    """
    import geopandas as gpd
    from shapely.geometry import box
    
    logger.info(f"📐 Creating grid aggregation (grid size: {grid_size}°)...")

    try:
        # Get bounds of voter data
        bounds = voters_gdf.total_bounds
        minx, miny, maxx, maxy = bounds

        # Create grid
        grid_cells = []
        x = minx
        while x < maxx:
            y = miny
            while y < maxy:
                grid_cells.append(box(x, y, x + grid_size, y + grid_size))
                y += grid_size
            x += grid_size

        # Create grid GeoDataFrame
        grid_gdf = gpd.GeoDataFrame(geometry=grid_cells, crs=voters_gdf.crs)
        grid_gdf["grid_id"] = range(len(grid_gdf))

        # Spatial join to count voters per grid cell
        voter_counts = voters_gdf.sjoin(grid_gdf, how="right", predicate="within")
        grid_stats = voter_counts.groupby("grid_id").size().reset_index(name="voter_count")

        # Merge back with grid
        result_gdf = grid_gdf.merge(grid_stats, on="grid_id", how="left")
        result_gdf["voter_count"] = result_gdf["voter_count"].fillna(0).astype(int)

        # Filter to non-empty cells
        result_gdf = result_gdf[result_gdf["voter_count"] > 0]

        logger.success(f"  ✅ Created {len(result_gdf):,} grid cells with voter data")
        return result_gdf

    except Exception as e:
        logger.critical(f"❌ Error creating grid aggregation: {e}")
        return None


def analyze_voters_by_block_groups(voters_gdf, block_groups_gdf):
    """
    Analyze voter distribution by census block groups.
    """
    logger.info("🏘️ Analyzing voters by block groups...")

    try:
        # Ensure consistent CRS
        if voters_gdf.crs != block_groups_gdf.crs:
            block_groups_gdf = block_groups_gdf.to_crs(voters_gdf.crs)

        # Spatial join
        voters_with_bg = voters_gdf.sjoin(block_groups_gdf, how="left", predicate="within")
        
        # Aggregate by block group
        bg_stats = voters_with_bg.groupby("GEOID").agg({
            "VOTER_ID": "count"
        }).rename(columns={"VOTER_ID": "voter_count"}).reset_index()

        # Merge with block group geometries
        result_gdf = block_groups_gdf.merge(bg_stats, on="GEOID", how="left")
        result_gdf["voter_count"] = result_gdf["voter_count"].fillna(0).astype(int)

        # Calculate voter density
        result_gdf_proj = result_gdf.to_crs("EPSG:3857")
        result_gdf["area_km2"] = (result_gdf_proj.geometry.area / 1e6).round(3)
        result_gdf["voter_density"] = (result_gdf["voter_count"] / result_gdf["area_km2"]).round(1)
        result_gdf["voter_density"] = result_gdf["voter_density"].replace([float("inf"), -float("inf")], 0)

        logger.success(f"  ✅ Analyzed {len(result_gdf):,} block groups")
        
        # Summary statistics
        with_voters = result_gdf[result_gdf["voter_count"] > 0]
        logger.info(f"     📊 Block groups with voters: {len(with_voters):,}")
        logger.info(f"     📊 Average voter density: {with_voters['voter_density'].mean():.1f}/km²")
        
        return result_gdf

    except Exception as e:
        logger.critical(f"❌ Error analyzing voters by block groups: {e}")
        return None


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

    # 4. Classify voters by district
    logger.info("🎯 Classifying voters by PPS district...")
    voters_classified = classify_voters_by_district(voters_gdf, districts_gdf)
    if voters_classified is None:
        sys.exit(1)

    # 5. Create hexagonal aggregation
    logger.info("🔷 Creating hexagonal aggregation...")
    hex_gdf = create_hexagonal_aggregation(voters_classified, config, resolution=8)
    if hex_gdf is not None:
        logger.success(f"  ✅ Created hexagonal aggregation with {len(hex_gdf):,} hexagons")

    # 6. Load block groups and analyze
    logger.info("🏘️ Loading block group boundaries...")
    block_groups_gdf = load_block_group_boundaries(config)
    if block_groups_gdf is not None:
        bg_analysis = analyze_voters_by_block_groups(voters_classified, block_groups_gdf)
        if bg_analysis is not None:
            logger.success(f"  ✅ Completed block group analysis")

    # 7. Upload to Supabase (Optional)
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
