#!/usr/bin/env python3
"""
Voter Data Analysis and Visualization

This script analyzes voter registration patterns and creates geographic visualizations
of voter demographics within the Portland Public Schools (PPS) district.

Key Features:
- Maps voters inside vs outside PPS district boundaries
- Shows voter density patterns
- Analyzes political registration by geography
- Creates interactive heatmaps for exploration

The script requires:
- Voter location data (CSV with lat/lng)
- PPS district boundary shapefile
- Block group geographic data

Output:
- Static maps showing voter patterns
- Interactive HTML heatmap for detailed exploration
"""

import sys
import time

import folium
import geopandas as gpd
import pandas as pd
from folium.plugins import HeatMap
from loguru import logger
from shapely.geometry import Point

from ops import Config


def load_region_data(config: Config):
    """Load PPS district geometry data."""
    region_path = config.get_input_path('district_boundaries_geojson')
    print(f"📍 Loading PPS district boundaries from {region_path}")
    
    if not region_path.exists():
        print(f"❌ Error: PPS district file not found: {region_path}")
        return None
    
    try:
        regions = gpd.read_file(region_path)
        print(f"  ✓ Loaded {len(regions)} region features")
        return regions
    except Exception as e:
        print(f"❌ Error loading region data: {e}")
        return None


def load_voter_data(config: Config):
    """Load and clean voter CSV data."""
    voter_path = config.get_input_path('voter_locations_csv')
    print(f"👥 Loading voter data from {voter_path}")
    
    if not voter_path.exists():
        print(f"❌ Error: Voters file not found: {voter_path}")
        return None
    
    try:
        df = pd.read_csv(voter_path, low_memory=False)
        print(f"  ✓ Loaded {len(df):,} voter records")
        
        # Clean column names
        cols = df.columns.str.strip().str.lower().str.replace(r"[^0-9a-z]+", "_", regex=True)
        df.columns = cols
        
        # Get coordinate column names from config
        lat_col = config.get_column_name('latitude')
        lon_col = config.get_column_name('longitude')
        
        # Standardize coordinate column names
        coordinate_mapping = {}
        for col in cols:
            if col in ("lat", "latitude", lat_col.lower()):
                coordinate_mapping[col] = "latitude"
            elif col in ("lon", "lng", "longitude", lon_col.lower()):
                coordinate_mapping[col] = "longitude"
        
        if coordinate_mapping:
            df = df.rename(columns=coordinate_mapping)
            print(f"  ✓ Standardized coordinate columns: {list(coordinate_mapping.values())}")
        
        # Validate required columns exist
        if "latitude" not in df.columns or "longitude" not in df.columns:
            print("❌ Error: Could not find latitude/longitude columns in voter data")
            print(f"   Available columns: {list(df.columns)}")
            return None
        
        # Remove invalid coordinates
        initial_count = len(df)
        df = df.dropna(subset=["latitude", "longitude"])
        
        # Remove obviously invalid coordinates
        df = df[
            (df["latitude"].between(-90, 90)) & 
            (df["longitude"].between(-180, 180))
        ]
        
        valid_count = len(df)
        removed_count = initial_count - valid_count
        
        if removed_count > 0:
            print(f"  ⚠️ Removed {removed_count:,} records with invalid coordinates")
        
        print(f"  ✓ Retained {valid_count:,} valid voter locations")
        return df
        
    except Exception as e:
        print(f"❌ Error loading voter data: {e}")
        return None


def load_and_process_data(
    config: Config,
) -> tuple[pd.DataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Load voter location data and geographic boundaries."""
    logger.info("📊 Loading voter and geographic data...")

    # Load voter location data
    voter_csv_path = config.get_input_path("voters_csv")
    if not voter_csv_path.exists():
        logger.critical(f"❌ Voter CSV file not found: {voter_csv_path}")
        raise FileNotFoundError(f"Required file missing: {voter_csv_path}")

    voters_df = pd.read_csv(voter_csv_path)
    logger.success(f"✅ Loaded voter data: {len(voters_df):,} voters")

    # Basic data validation
    required_cols = ["latitude", "longitude"]
    missing_cols = [col for col in required_cols if col not in voters_df.columns]
    if missing_cols:
        logger.critical(f"❌ Missing required columns in voter data: {missing_cols}")
        raise ValueError(f"Voter data missing required columns: {missing_cols}")

    # Check for missing coordinates
    missing_coords = voters_df[["latitude", "longitude"]].isna().any(axis=1).sum()
    if missing_coords > 0:
        logger.warning(f"⚠️ Found {missing_coords:,} voters with missing coordinates")
        logger.debug(
            f"   Percentage with missing coords: {missing_coords / len(voters_df) * 100:.1f}%"
        )

    # Load PPS district boundaries
    boundaries_path = config.get_path("pps_district_boundary_shapefile")
    if not boundaries_path.exists():
        logger.critical(f"❌ PPS boundary shapefile not found: {boundaries_path}")
        raise FileNotFoundError(f"Required file missing: {boundaries_path}")

    pps_boundary = gpd.read_file(boundaries_path)
    logger.success(f"✅ Loaded PPS boundaries: {len(pps_boundary)} polygons")

    # Load block groups for demographic context
    blockgroups_path = config.get_path("block_groups_shapefile")
    blockgroups_gdf = gpd.read_file(blockgroups_path)
    logger.success(f"✅ Loaded block groups: {len(blockgroups_gdf)} areas")

    logger.debug("📍 Voter coordinate range:")
    logger.debug(
        f"   Latitude: {voters_df['latitude'].min():.6f} to {voters_df['latitude'].max():.6f}"
    )
    logger.debug(
        f"   Longitude: {voters_df['longitude'].min():.6f} to {voters_df['longitude'].max():.6f}"
    )

    return voters_df, pps_boundary, blockgroups_gdf


def create_voter_geodataframe(voters_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert voter DataFrame to GeoDataFrame with spatial operations."""
    logger.info("🗺️ Creating spatial representation of voter data...")

    # Filter out voters with missing coordinates
    valid_coords = voters_df.dropna(subset=["latitude", "longitude"])
    filtered_count = len(voters_df) - len(valid_coords)

    if filtered_count > 0:
        logger.warning(f"⚠️ Filtered out {filtered_count:,} voters with invalid coordinates")

    # Create geometry points
    logger.debug("📍 Converting coordinates to spatial points...")
    try:
        geometry = gpd.points_from_xy(valid_coords.longitude, valid_coords.latitude)
        voters_gdf = gpd.GeoDataFrame(valid_coords, geometry=geometry, crs="EPSG:4326")

        logger.success(f"✅ Created spatial voter dataset: {len(voters_gdf):,} valid locations")
        logger.debug(f"   Coordinate system: {voters_gdf.crs}")

        return voters_gdf

    except Exception as e:
        logger.error(f"❌ Failed to create voter geometry: {e}")
        logger.trace("Detailed geometry creation error:")
        import traceback

        logger.trace(traceback.format_exc())
        raise


def perform_spatial_analysis(
    voters_gdf: gpd.GeoDataFrame, pps_boundary: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Perform spatial join to identify voters inside/outside PPS district."""
    logger.info("🔍 Performing spatial analysis: voters vs PPS boundaries...")

    # Ensure both datasets use the same CRS
    if voters_gdf.crs != pps_boundary.crs:
        logger.debug(f"🔄 Reprojecting PPS boundaries from {pps_boundary.crs} to {voters_gdf.crs}")
        pps_boundary = pps_boundary.to_crs(voters_gdf.crs)

    # Perform spatial join
    logger.debug("🔗 Executing spatial join operation...")
    start_time = time.time()

    try:
        # Use spatial join to identify voters within PPS district
        voters_with_district = gpd.sjoin(voters_gdf, pps_boundary, how="left", predicate="within")

        elapsed = time.time() - start_time
        logger.success(f"✅ Spatial join completed in {elapsed:.1f}s")

        # Analyze results
        inside_pps = voters_with_district["index_right"].notna().sum()
        outside_pps = voters_with_district["index_right"].isna().sum()
        total_voters = len(voters_with_district)

        logger.info("📊 Spatial analysis results:")
        logger.info(f"   👥 Total voters analyzed: {total_voters:,}")
        logger.success(
            f"   🏫 Inside PPS district: {inside_pps:,} ({inside_pps / total_voters * 100:.1f}%)"
        )
        logger.info(
            f"   🏠 Outside PPS district: {outside_pps:,} ({outside_pps / total_voters * 100:.1f}%)"
        )

        # Add clean boolean flag
        voters_with_district["inside_pps"] = voters_with_district["index_right"].notna()

        if logger.level == "TRACE":
            logger.trace("🔍 Detailed spatial join validation:")
            logger.trace(
                f"   Sample inside PPS (first 5): {voters_with_district[voters_with_district['inside_pps']].index[:5].tolist()}"
            )
            logger.trace(
                f"   Sample outside PPS (first 5): {voters_with_district[~voters_with_district['inside_pps']].index[:5].tolist()}"
            )

        return voters_with_district

    except Exception as e:
        logger.error(f"❌ Spatial join failed: {e}")
        logger.trace("Detailed spatial join error:")
        import traceback

        logger.trace(traceback.format_exc())
        raise


def create_density_analysis(
    voters_gdf: gpd.GeoDataFrame, blockgroups_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Create voter density analysis by block group."""
    logger.info("📊 Analyzing voter density by block group...")

    try:
        # Ensure consistent CRS
        if voters_gdf.crs != blockgroups_gdf.crs:
            logger.debug(
                f"🔄 Reprojecting block groups from {blockgroups_gdf.crs} to {voters_gdf.crs}"
            )
            blockgroups_gdf = blockgroups_gdf.to_crs(voters_gdf.crs)

        # Spatial join voters to block groups
        logger.debug("🔗 Joining voters to block groups...")
        voters_with_bg = gpd.sjoin(voters_gdf, blockgroups_gdf, how="left", predicate="within")

        # Count voters per block group
        logger.debug("📈 Calculating voter density statistics...")
        density_stats = (
            voters_with_bg.groupby("index_right")
            .agg(
                {
                    "inside_pps": ["count", "sum"],
                    "latitude": "count",  # Total voters in block group
                }
            )
            .round(2)
        )

        # Flatten column names
        density_stats.columns = ["_".join(col).strip() for col in density_stats.columns]
        density_stats = density_stats.rename(
            columns={
                "inside_pps_count": "total_voters_bg",
                "inside_pps_sum": "pps_voters_bg",
                "latitude_count": "total_voters_check",
            }
        )

        # Calculate percentages
        density_stats["pps_voter_pct"] = (
            density_stats["pps_voters_bg"] / density_stats["total_voters_bg"] * 100
        ).round(1)

        # Join back to block groups
        blockgroups_with_density = blockgroups_gdf.merge(
            density_stats, left_index=True, right_index=True, how="left"
        )

        # Fill NaN values for block groups with no voters
        fill_cols = ["total_voters_bg", "pps_voters_bg", "pps_voter_pct"]
        blockgroups_with_density[fill_cols] = blockgroups_with_density[fill_cols].fillna(0)

        logger.success("✅ Density analysis complete:")
        logger.info(
            f"   📍 Block groups with voters: {(blockgroups_with_density['total_voters_bg'] > 0).sum()}"
        )
        logger.info(
            f"   👥 Average voters per block group: {blockgroups_with_density['total_voters_bg'].mean():.1f}"
        )
        logger.debug(
            f"   📊 Max voters in single block group: {blockgroups_with_density['total_voters_bg'].max()}"
        )

        return blockgroups_with_density

    except Exception as e:
        logger.error(f"❌ Density analysis failed: {e}")
        logger.trace("Detailed density analysis error:")
        import traceback

        logger.trace(traceback.format_exc())
        raise


def classify_voters(df, regions):
    """Classify voters as inside or outside PPS district."""
    logger.debug("🗺️ Classifying voter locations...")

    try:
        # Create GeoDataFrame from voter coordinates
        gdf = gpd.GeoDataFrame(
            df,
            geometry=[Point(xy) for xy in zip(df.longitude, df.latitude)],
            crs="EPSG:4326",
        )

        # Ensure regions and points use the same CRS
        if regions.crs != gdf.crs:
            logger.debug(f"  🔄 Reprojecting regions from {regions.crs} to {gdf.crs}")
            regions = regions.to_crs(gdf.crs)

        # Create union of all region geometries
        union = regions.geometry.union_all()

        # Classify points
        gdf["inside_pps"] = gdf.geometry.within(union)

        inside_count = gdf["inside_pps"].sum()
        outside_count = len(gdf) - inside_count

        logger.debug("  ✓ Classification complete:")
        logger.debug(f"    • Inside PPS: {inside_count:,} voters")
        logger.debug(f"    • Outside PPS: {outside_count:,} voters")
        logger.debug(f"    • PPS coverage: {inside_count / len(gdf):.1%}")

        return gdf

    except Exception as e:
        logger.debug(f"❌ Error classifying voters: {e}")
        return None


def export_classification_data(gdf, config: Config):
    """Export inside/outside classification to CSV files."""
    logger.debug("💾 Exporting classification data...")

    try:
        # Get output paths from config
        inside_path = config.get_voters_inside_csv_path()
        outside_path = config.get_voters_outside_csv_path()

        # Export voters inside PPS
        inside_voters = gdf[gdf["inside_pps"]].drop(columns="geometry")
        inside_voters.to_csv(inside_path, index=False)
        logger.debug(f"  ✓ Inside PPS: {len(inside_voters):,} voters → {inside_path}")

        # Export voters outside PPS
        outside_voters = gdf[~gdf["inside_pps"]].drop(columns="geometry")
        outside_voters.to_csv(outside_path, index=False)
        logger.debug(f"  ✓ Outside PPS: {len(outside_voters):,} voters → {outside_path}")

        return True

    except Exception as e:
        logger.debug(f"❌ Error exporting data: {e}")
        return False


def create_heatmap(gdf, regions, config: Config):
    """Create interactive Folium heatmap."""
    logger.debug("🗺️ Creating interactive heatmap...")

    try:
        # Get output path from config
        output_path = config.get_voter_heatmap_path()

        # Calculate map center
        center_lat = gdf.latitude.mean()
        center_lon = gdf.longitude.mean()
        center = [center_lat, center_lon]

        logger.debug(f"  📍 Map center: {center[0]:.4f}, {center[1]:.4f}")

        # Create base map
        m = folium.Map(location=center, zoom_start=10, tiles="cartodbpositron")

        # Add PPS district boundaries
        folium.GeoJson(
            regions.__geo_interface__,
            name="PPS District",
            style_function=lambda f: {
                "color": "#0066cc",
                "weight": 3,
                "fill": False,
                "opacity": 0.8,
            },
            tooltip=folium.Tooltip("PPS District Boundary"),
        ).add_to(m)

        # Prepare heatmap data
        heat_data = gdf[["latitude", "longitude"]].values.tolist()

        # Add heatmap layer
        HeatMap(heat_data, radius=10, blur=15, max_zoom=12, min_opacity=0.3).add_to(m)

        # Add layer control
        folium.LayerControl().add_to(m)

        # Save map
        m.save(output_path)
        logger.debug(f"  ✓ Interactive heatmap saved: {output_path}")

        return True

    except Exception as e:
        logger.debug(f"❌ Error creating heatmap: {e}")
        return False


def main():
    """Main execution function."""
    logger.debug("👥 Voter Location Analysis")
    logger.debug("=" * 50)

    # Load configuration
    try:
        config = Config()
        logger.debug(f"📋 Project: {config.get('project_name')}")
        logger.debug(f"📋 Description: {config.get('description')}")
    except Exception as e:
        logger.debug(f"❌ Configuration error: {e}")
        logger.debug("💡 Make sure config.yaml exists in the analysis directory")
        sys.exit(1)

    # Load region data
    regions = load_region_data(config)
    if regions is None:
        sys.exit(1)

    # Load voter data
    df = load_voter_data(config)
    if df is None:
        sys.exit(1)

    # Classify voters
    gdf = classify_voters(df, regions)
    if gdf is None:
        sys.exit(1)

    # Export classification data
    if not export_classification_data(gdf, config):
        sys.exit(1)

    # Create heatmap
    if not create_heatmap(gdf, regions, config):
        sys.exit(1)

    logger.debug("✅ Voter location analysis completed successfully!")
    logger.debug("📊 Outputs:")
    inside_path = config.get_voters_inside_csv_path()
    outside_path = config.get_voters_outside_csv_path()
    heatmap_path = config.get_voter_heatmap_path()
    logger.debug(f"   • Inside PPS CSV: {inside_path}")
    logger.debug(f"   • Outside PPS CSV: {outside_path}")
    logger.debug(f"   • Interactive heatmap: {heatmap_path}")


if __name__ == "__main__":
    main()
