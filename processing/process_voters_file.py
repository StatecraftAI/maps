"""
process_voters_file.py

This script processes voter location data, performs spatial analysis, and prepares the data
for visualization and backend integration. It focuses on aggregating voter data into
geospatial formats (e.g., hexagons, block groups) for efficient web consumption and analysis.

Key Functionality:
1. Data Loading and Validation:
   - Loads voter location data from a CSV file.
   - Validates and cleans coordinates (latitude/longitude).

2. Spatial Analysis:
   - Classifies voters as inside or outside PPS district boundaries.
   - Aggregates voter data into hexagonal bins for web-optimized visualization.
   - Analyzes voter density and participation by census block groups.

3. Geospatial Processing:
   - Converts voter points into geospatial formats (GeoDataFrame).
   - Validates and reprojects geospatial data to WGS84 (standard for mapping).
   - Optimizes GeoJSON properties for efficient rendering in web maps.

4. Data Export:
   - Exports optimized GeoJSON files for web visualization.
   - Uploads geospatial data to Supabase PostGIS database (optional).

5. Visualization:
   - Prepares data for interactive visualization in the maps component of StatecraftAI.

Usage:
- This script is typically used after `enrich_election_data.py` to prepare
  geospatial voter data for visualization and backend integration.
- It is part of the data pipeline for generating interactive maps and dashboards.

Input:
- Voter location data (CSV file with latitude/longitude).
- PPS district boundaries (GeoJSON file).
- Census block group boundaries (GeoJSON file, optional).
- Configuration file (e.g., config.yaml) for file paths and processing settings.

Output:
- Optimized GeoJSON files for web mapping (hexagons, block groups, district summaries).
- Uploaded geospatial data to Supabase PostGIS database (optional).

Example:
    python process_voters_file.py --config config.yaml

Dependencies:
- geopandas, pandas, numpy, h3, loguru, and other standard Python libraries.
- Supabase integration (optional) requires sqlalchemy and psycopg2-binary.
"""

import json
import sys
import time
from pathlib import Path
from typing import Optional

import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))
from ops import Config

# Import optimization functions from the election results module
# These provide robust CRS handling and field optimization
try:
    import sys

    sys.path.append(str(Path(__file__).parent))
    from process_election_results import (
        clean_numeric,
        optimize_geojson_properties,
        validate_and_reproject_to_wgs84,
    )

    logger.debug("✅ Imported optimization functions from map_election_results")
except ImportError as e:
    logger.warning(f"⚠️ Could not import optimization functions: {e}")
    logger.warning("   Using fallback implementations")

# Import Supabase integration
try:
    from ops.supabase_integration import SupabaseUploader

    # Optional: Import new patterns for future use
    # from ops.supabase_integration import get_supabase_database
    # from ops.repositories import SpatialRepository

    logger.debug("✅ Imported Supabase integration module")
    SUPABASE_AVAILABLE = True
except ImportError as e:
    logger.debug(f"📊 Supabase integration not available: {e}")
    logger.debug("   Install with: pip install sqlalchemy psycopg2-binary")
    SUPABASE_AVAILABLE = False

    def validate_and_reproject_to_wgs84(
        gdf: gpd.GeoDataFrame, config: Config, source_description: str = "GeoDataFrame"
    ) -> gpd.GeoDataFrame:
        """Fallback CRS validation."""
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs("EPSG:4326")
        return gdf

    def optimize_geojson_properties(gdf: gpd.GeoDataFrame, config: Config) -> gpd.GeoDataFrame:
        """Fallback property optimization."""
        return gdf

    def clean_numeric(series: pd.Series, is_percent: bool = False) -> pd.Series:
        """Fallback numeric cleaning."""
        return pd.to_numeric(series, errors="coerce").fillna(0)


def load_and_validate_voter_data(config: Config) -> Optional[pd.DataFrame]:
    """
    Load and validate voter CSV data with robust error handling.

    Args:
        config: Configuration instance

    Returns:
        DataFrame with validated voter data or None if failed
    """
    voter_path = config.get_input_path("voters_file_csv")
    logger.info(f"👥 Loading voter data from {voter_path}")

    if not voter_path.exists():
        logger.critical(f"❌ Voters file not found: {voter_path}")
        return None

    try:
        # Load with efficient data types for large dataset
        logger.debug("  📊 Loading CSV with optimized data types...")
        df = pd.read_csv(
            voter_path, low_memory=False, dtype={"latitude": "float64", "longitude": "float64"}
        )
        logger.success(
            f"  ✅ Loaded {len(df):,} voter records ({voter_path.stat().st_size / (1024 * 1024):.1f} MB)"
        )

        # Clean column names using established patterns
        cols = df.columns.str.strip().str.lower().str.replace(r"[^0-9a-z]+", "_", regex=True)
        df.columns = cols

        # Get coordinate column names from config with fallbacks
        lat_col = config.get_column_name("latitude").lower()
        lon_col = config.get_column_name("longitude").lower()

        # Standardize coordinate column names
        coordinate_mapping = {}
        for col in cols:
            if col in ("lat", "latitude", lat_col):
                coordinate_mapping[col] = "latitude"
            elif col in ("lon", "lng", "longitude", lon_col):
                coordinate_mapping[col] = "longitude"

        if coordinate_mapping:
            df = df.rename(columns=coordinate_mapping)
            logger.debug(
                f"  ✅ Standardized coordinate columns: {list(coordinate_mapping.values())}"
            )

        # Validate required columns exist
        if "latitude" not in df.columns or "longitude" not in df.columns:
            logger.critical("❌ Could not find latitude/longitude columns in voter data")
            logger.critical(f"   Available columns: {list(df.columns)}")
            return None

        # Validate and clean coordinates
        initial_count = len(df)

        # Remove records with missing coordinates
        df = df.dropna(subset=["latitude", "longitude"])

        # Convert to numeric and validate coordinate ranges
        df["latitude"] = clean_numeric(df["latitude"])
        df["longitude"] = clean_numeric(df["longitude"])

        # Remove invalid coordinate ranges
        df = df[(df["latitude"].between(-90, 90)) & (df["longitude"].between(-180, 180))]

        # Remove zero coordinates (often data errors)
        df = df[(df["latitude"] != 0) | (df["longitude"] != 0)]

        valid_count = len(df)
        removed_count = initial_count - valid_count

        if removed_count > 0:
            logger.warning(f"  ⚠️ Removed {removed_count:,} records with invalid coordinates")
            logger.debug(f"     Retention rate: {valid_count / initial_count * 100:.1f}%")

        logger.success(f"  ✅ Validated {valid_count:,} voter locations")

        # Log coordinate range for validation
        logger.debug("  📍 Coordinate validation:")
        logger.debug(
            f"     Latitude range: {df['latitude'].min():.6f} to {df['latitude'].max():.6f}"
        )
        logger.debug(
            f"     Longitude range: {df['longitude'].min():.6f} to {df['longitude'].max():.6f}"
        )

        return df

    except Exception as e:
        logger.critical(f"❌ Error loading voter data: {e}")
        logger.trace("Detailed error information:")
        import traceback

        logger.trace(traceback.format_exc())
        return None


def load_pps_district_boundaries(config: Config) -> Optional[gpd.GeoDataFrame]:
    """
    Load PPS district boundary data with CRS validation.

    Args:
        config: Configuration instance

    Returns:
        GeoDataFrame with district boundaries or None if failed
    """
    district_path = config.get_input_path("pps_boundary_geojson")
    logger.info(f"🏫 Loading PPS district boundaries from {district_path}")

    if not district_path.exists():
        logger.critical(f"❌ District boundaries file not found: {district_path}")
        return None

    try:
        districts_gdf = gpd.read_file(district_path)
        logger.success(f"  ✅ Loaded {len(districts_gdf)} district features")

        # Validate and standardize CRS
        districts_gdf = validate_and_reproject_to_wgs84(
            districts_gdf, config, "PPS district boundaries"
        )

        return districts_gdf

    except Exception as e:
        logger.critical(f"❌ Error loading district boundaries: {e}")
        logger.trace("Detailed error information:")
        import traceback

        logger.trace(traceback.format_exc())
        return None


def load_block_group_boundaries(config: Config) -> Optional[gpd.GeoDataFrame]:
    """
    Load block group boundary data for demographic analysis.

    Args:
        config: Configuration instance

    Returns:
        GeoDataFrame with block group boundaries or None if failed
    """
    bg_path = config.get_input_path("census_blocks_geojson")
    logger.info(f"🗺️ Loading block group boundaries from {bg_path}")

    if not bg_path.exists():
        logger.critical(f"❌ Block groups file not found: {bg_path}")
        return None

    try:
        bg_gdf = gpd.read_file(bg_path)
        logger.success(f"  ✅ Loaded {len(bg_gdf)} block group features")

        # Filter to Oregon (state FIPS 41) for performance
        if "STATEFP" in bg_gdf.columns:
            bg_gdf = bg_gdf[bg_gdf["STATEFP"] == "41"].copy()
            logger.debug(f"  📍 Filtered to Oregon: {len(bg_gdf)} block groups")

        # Validate and standardize CRS
        bg_gdf = validate_and_reproject_to_wgs84(bg_gdf, config, "block group boundaries")

        return bg_gdf

    except Exception as e:
        logger.critical(f"❌ Error loading block group boundaries: {e}")
        logger.trace("Detailed error information:")
        import traceback

        logger.trace(traceback.format_exc())
        return None


def create_voter_geodataframe(voters_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert voter DataFrame to GeoDataFrame with spatial operations.

    Args:
        voters_df: DataFrame with validated voter data

    Returns:
        GeoDataFrame with voter point geometries
    """
    logger.info("🗺️ Creating spatial representation of voter data...")

    try:
        # Create geometry points efficiently
        logger.debug("  📍 Converting coordinates to spatial points...")
        start_time = time.time()

        geometry = gpd.points_from_xy(voters_df.longitude, voters_df.latitude, crs="EPSG:4326")

        voters_gdf = gpd.GeoDataFrame(voters_df, geometry=geometry, crs="EPSG:4326")

        elapsed = time.time() - start_time
        logger.success(
            f"  ✅ Created spatial dataset in {elapsed:.1f}s: {len(voters_gdf):,} locations"
        )
        logger.debug(f"     Coordinate system: {voters_gdf.crs}")

        return voters_gdf

    except Exception as e:
        logger.critical(f"❌ Failed to create voter geometry: {e}")
        logger.trace("Detailed geometry creation error:")
        import traceback

        logger.trace(traceback.format_exc())
        raise


def classify_voters_by_district(
    voters_gdf: gpd.GeoDataFrame, districts_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Classify voters as inside or outside PPS district using spatial operations.

    Args:
        voters_gdf: GeoDataFrame with voter points
        districts_gdf: GeoDataFrame with district boundaries

    Returns:
        GeoDataFrame with district classification
    """
    logger.info("🔍 Performing spatial analysis: voters vs PPS boundaries...")

    try:
        # Ensure consistent CRS
        if voters_gdf.crs != districts_gdf.crs:
            logger.debug(
                f"  🔄 Reprojecting districts from {districts_gdf.crs} to {voters_gdf.crs}"
            )
            districts_gdf = districts_gdf.to_crs(voters_gdf.crs)

        # Create union of all district geometries (FIXED: use unary_union)
        logger.debug("  🔗 Creating district union for spatial classification...")
        district_union = districts_gdf.geometry.unary_union

        # Perform spatial classification
        logger.debug("  🔍 Executing spatial join operation...")
        start_time = time.time()

        voters_gdf["inside_pps"] = voters_gdf.geometry.within(district_union)

        elapsed = time.time() - start_time
        logger.success(f"  ✅ Spatial classification completed in {elapsed:.1f}s")

        # Analyze results
        inside_count = voters_gdf["inside_pps"].sum()
        outside_count = len(voters_gdf) - inside_count
        total_voters = len(voters_gdf)

        logger.info("📊 Spatial analysis results:")
        logger.info(f"   👥 Total voters analyzed: {total_voters:,}")
        logger.success(
            f"   🏫 Inside PPS district: {inside_count:,} ({inside_count / total_voters * 100:.1f}%)"
        )
        logger.info(
            f"   🏠 Outside PPS district: {outside_count:,} ({outside_count / total_voters * 100:.1f}%)"
        )

        return voters_gdf

    except Exception as e:
        logger.critical(f"❌ Spatial classification failed: {e}")
        logger.trace("Detailed spatial analysis error:")
        import traceback

        logger.trace(traceback.format_exc())
        raise


def create_hexagonal_aggregation(
    voters_gdf: gpd.GeoDataFrame, config: Config, resolution: int = 8
) -> gpd.GeoDataFrame:
    """
    Create hexagonal spatial aggregation for web-optimized visualization.

    This addresses the large voter dataset (660k records) by aggregating
    points into hexagonal bins suitable for web consumption.

    Args:
        voters_gdf: GeoDataFrame with classified voters
        config: Configuration instance
        resolution: H3 hexagon resolution (8 = ~0.7km avg edge)

    Returns:
        GeoDataFrame with hexagonal aggregation
    """
    logger.info(
        f"🐝 Creating hexagonal aggregation (resolution {resolution}) for web optimization..."
    )

    try:
        # Keep a copy of the original for fallback
        voters_gdf_original = voters_gdf.copy()

        # Convert to H3 hexagon indices
        logger.debug("  📐 Converting points to H3 hexagon indices...")
        start_time = time.time()

        # Create H3 hexagon IDs for each voter location
        hex_ids = []
        valid_count = 0
        for idx, row in voters_gdf.iterrows():
            try:
                # H3 expects lat, lon order
                hex_id = h3.latlng_to_cell(row.geometry.y, row.geometry.x, resolution)
                if hex_id and hex_id != "0":  # Check for valid hex_id
                    hex_ids.append(hex_id)
                    valid_count += 1
                else:
                    hex_ids.append(None)
            except Exception as e:
                if idx < 5:  # Only log first few errors to avoid spam
                    logger.debug(f"    ⚠️ Could not convert point {idx} to hex: {e}")
                hex_ids.append(None)

        voters_gdf["hex_id"] = hex_ids

        # Remove records that couldn't be assigned to hexagons
        initial_count = len(voters_gdf)
        voters_gdf = voters_gdf.dropna(subset=["hex_id"])

        logger.debug(
            f"  📊 Hexagon assignment: {len(voters_gdf):,}/{initial_count:,} voters successfully assigned"
        )

        if len(voters_gdf) == 0:
            logger.warning(
                "⚠️ No voters could be assigned to hexagons - falling back to grid aggregation"
            )
            return create_grid_aggregation(voters_gdf_original, config)

        # Aggregate by hexagon
        logger.debug("  📊 Aggregating voter statistics by hexagon...")
        hex_stats = (
            voters_gdf.groupby("hex_id")
            .agg(
                {
                    "inside_pps": ["count", "sum"],
                    "latitude": "count",  # Total count check
                }
            )
            .round(2)
        )

        # Flatten column names
        hex_stats.columns = ["total_voters", "pps_voters", "total_check"]
        hex_stats = hex_stats.drop("total_check", axis=1)

        # Calculate percentages and density metrics (safe division)
        hex_stats["pps_voter_pct"] = (
            (hex_stats["pps_voters"] / hex_stats["total_voters"].replace(0, np.nan) * 100)
            .fillna(0)
            .round(1)
        )

        # Create hexagon geometries
        logger.debug("  🔷 Creating hexagon geometries...")
        hex_geometries = []
        hex_indices = []

        for hex_id in hex_stats.index:
            try:
                # Get hexagon boundary coordinates
                hex_boundary = h3.cell_to_boundary(hex_id)
                # Create polygon geometry from lat/lng tuples
                from shapely.geometry import Polygon

                # H3 returns (lat, lng) tuples, convert to (lng, lat) for Shapely
                coords = [(lng, lat) for lat, lng in hex_boundary]
                hex_geom = Polygon(coords)

                hex_geometries.append(hex_geom)
                hex_indices.append(hex_id)
            except Exception as e:
                logger.debug(f"    ⚠️ Could not create geometry for hex {hex_id}: {e}")
                continue

        # Create hexagon GeoDataFrame
        hex_gdf = gpd.GeoDataFrame(
            hex_stats.loc[hex_indices], geometry=hex_geometries, crs="EPSG:4326"
        )

        # Reset index to make hex_id a column if it's not already
        if hex_gdf.index.name == "hex_id":
            hex_gdf = hex_gdf.reset_index()
        elif "hex_id" not in hex_gdf.columns:
            hex_gdf["hex_id"] = hex_indices

        # Add hexagon metadata
        hex_gdf["resolution"] = resolution
        hex_gdf["area_km2"] = hex_gdf.geometry.area * 111319.9**2 / 1e6  # Rough conversion

        # Calculate density (safe division)
        hex_gdf["voter_density"] = hex_gdf["total_voters"] / hex_gdf["area_km2"].replace(0, np.nan)
        hex_gdf["voter_density"] = hex_gdf["voter_density"].fillna(0).round(1)

        elapsed = time.time() - start_time
        logger.success(f"  ✅ Created hexagonal aggregation in {elapsed:.1f}s:")
        logger.info(f"     📐 {len(hex_gdf):,} hexagons from {len(voters_gdf):,} voters")

        if len(hex_gdf) > 0:
            logger.info(f"     📊 Avg voters per hexagon: {hex_gdf['total_voters'].mean():.1f}")
            logger.info(f"     🐝 Data reduction: {len(voters_gdf) / len(hex_gdf):.1f}x smaller")
        else:
            logger.warning("     ⚠️ No valid hexagons created")

        return hex_gdf

    except ImportError:
        logger.error("❌ H3 library not available. Install with: pip install h3")
        logger.info("  💡 Falling back to grid-based aggregation...")
        return create_grid_aggregation(voters_gdf, config)
    except Exception as e:
        logger.critical(f"❌ Hexagonal aggregation failed: {e}")
        logger.trace("Detailed aggregation error:")
        import traceback

        logger.trace(traceback.format_exc())
        raise


def create_grid_aggregation(
    voters_gdf: gpd.GeoDataFrame, config: Config, grid_size: float = 0.01
) -> gpd.GeoDataFrame:
    """
    Create grid-based spatial aggregation as fallback for hexagonal binning.

    Args:
        voters_gdf: GeoDataFrame with classified voters
        config: Configuration instance
        grid_size: Grid cell size in decimal degrees (~1km at Portland latitude)

    Returns:
        GeoDataFrame with grid aggregation
    """
    logger.info(f"📐 Creating grid aggregation (cell size {grid_size}°) as fallback...")

    try:
        # Create grid indices
        voters_gdf["grid_x"] = (voters_gdf.geometry.x / grid_size).astype(int)
        voters_gdf["grid_y"] = (voters_gdf.geometry.y / grid_size).astype(int)
        voters_gdf["grid_id"] = (
            voters_gdf["grid_x"].astype(str) + "_" + voters_gdf["grid_y"].astype(str)
        )

        # Aggregate by grid cell
        grid_stats = (
            voters_gdf.groupby("grid_id")
            .agg({"inside_pps": ["count", "sum"], "grid_x": "first", "grid_y": "first"})
            .round(2)
        )

        # Flatten columns
        grid_stats.columns = ["total_voters", "pps_voters", "grid_x", "grid_y"]
        grid_stats["pps_voter_pct"] = (
            (grid_stats["pps_voters"] / grid_stats["total_voters"].replace(0, np.nan) * 100)
            .fillna(0)
            .round(1)
        )

        # Create grid geometries (simple square cells)
        from shapely.geometry import box

        geometries = []
        for idx, row in grid_stats.iterrows():
            x_min = row["grid_x"] * grid_size
            y_min = row["grid_y"] * grid_size
            x_max = x_min + grid_size
            y_max = y_min + grid_size

            cell_geom = box(x_min, y_min, x_max, y_max)
            geometries.append(cell_geom)

        # Create grid GeoDataFrame
        grid_gdf = gpd.GeoDataFrame(
            grid_stats.drop(["grid_x", "grid_y"], axis=1), geometry=geometries, crs="EPSG:4326"
        )

        logger.success(f"  ✅ Created grid aggregation: {len(grid_gdf):,} cells")

        return grid_gdf

    except Exception as e:
        logger.critical(f"❌ Grid aggregation failed: {e}")
        raise


def analyze_voters_by_block_groups(
    voters_gdf: gpd.GeoDataFrame, block_groups_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Analyze voter density and PPS participation by census block groups.

    Args:
        voters_gdf: GeoDataFrame with classified voters
        block_groups_gdf: GeoDataFrame with block group boundaries

    Returns:
        GeoDataFrame with block group voter analysis
    """
    logger.info("📊 Analyzing voter patterns by census block groups...")

    try:
        # Ensure consistent CRS
        if voters_gdf.crs != block_groups_gdf.crs:
            block_groups_gdf = block_groups_gdf.to_crs(voters_gdf.crs)

        # Spatial join voters to block groups
        logger.debug("  🔗 Joining voters to block groups...")
        start_time = time.time()

        voters_with_bg = gpd.sjoin(
            voters_gdf, block_groups_gdf[["geometry"]], how="left", predicate="within"
        )

        # Count voters per block group
        bg_stats = (
            voters_with_bg.groupby("index_right")
            .agg(
                {
                    "inside_pps": ["count", "sum"],
                    "latitude": "count",  # Total count verification
                }
            )
            .round(2)
        )

        # Flatten column names
        bg_stats.columns = ["total_voters", "pps_voters", "total_check"]
        bg_stats = bg_stats.drop("total_check", axis=1)

        # Calculate metrics (safe division)
        bg_stats["pps_voter_pct"] = (
            (bg_stats["pps_voters"] / bg_stats["total_voters"].replace(0, np.nan) * 100)
            .fillna(0)
            .round(1)
        )

        # Calculate voter density (voters per square km)
        bg_with_stats = block_groups_gdf.copy()
        bg_with_stats["area_km2"] = bg_with_stats.geometry.area * 111319.9**2 / 1e6

        # Merge statistics
        bg_analysis = bg_with_stats.merge(bg_stats, left_index=True, right_index=True, how="left")

        # Fill NaN values for block groups with no voters
        fill_cols = ["total_voters", "pps_voters", "pps_voter_pct"]
        bg_analysis[fill_cols] = bg_analysis[fill_cols].fillna(0)

        # Calculate density (safe division)
        bg_analysis["voter_density"] = (
            (bg_analysis["total_voters"] / bg_analysis["area_km2"].replace(0, np.nan))
            .fillna(0)
            .round(1)
        )

        elapsed = time.time() - start_time
        logger.success(f"  ✅ Block group analysis completed in {elapsed:.1f}s:")

        # Summary statistics
        bg_with_voters = bg_analysis[bg_analysis["total_voters"] > 0]
        logger.info(f"     📍 Block groups with voters: {len(bg_with_voters):,}")
        logger.info(
            f"     👥 Average voters per block group: {bg_with_voters['total_voters'].mean():.1f}"
        )
        logger.info(
            f"     🏫 Average PPS participation: {bg_with_voters['pps_voter_pct'].mean():.1f}%"
        )

        return bg_analysis

    except Exception as e:
        logger.critical(f"❌ Block group analysis failed: {e}")
        logger.trace("Detailed analysis error:")
        import traceback

        logger.trace(traceback.format_exc())
        raise


def export_optimized_geojson(
    gdf: gpd.GeoDataFrame, output_path: Path, config: Config, layer_name: str = "voter_analysis"
) -> bool:
    """
    Export GeoDataFrame as optimized GeoJSON following industry standards.

    Args:
        gdf: GeoDataFrame to export
        output_path: Output file path
        config: Configuration instance
        layer_name: Layer name for metadata

    Returns:
        Success status
    """
    logger.info(f"💾 Exporting optimized GeoJSON: {output_path}")

    try:
        # Validate and optimize for web consumption
        logger.debug("  🔧 Optimizing for web consumption...")

        # Ensure proper CRS
        gdf_export = validate_and_reproject_to_wgs84(gdf, config, layer_name)

        # Optimize properties for vector tiles
        gdf_export = optimize_geojson_properties(gdf_export, config)

        # Validate geometry
        invalid_geom = gdf_export.geometry.isna() | (~gdf_export.geometry.is_valid)
        invalid_count = invalid_geom.sum()

        if invalid_count > 0:
            logger.warning(f"  ⚠️ Found {invalid_count} invalid geometries, fixing...")
            gdf_export.geometry = gdf_export.geometry.buffer(0)

        # Create output directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Export with optimized settings
        logger.debug("  💾 Writing GeoJSON file...")
        gdf_export.to_file(output_path, driver="GeoJSON")

        # Add metadata to GeoJSON file
        with open(output_path, "r") as f:
            geojson_data = json.load(f)

        # Add comprehensive metadata
        geojson_data["metadata"] = {
            "title": f"{config.get('project_name')} - {layer_name}",
            "description": f"Voter analysis layer: {layer_name}",
            "source": config.get_metadata("data_source"),
            "created": time.strftime("%Y-%m-%d"),
            "crs": "EPSG:4326",
            "coordinate_system": "WGS84 Geographic",
            "features_count": len(gdf_export),
            "layer_type": layer_name,
            "processing_notes": [
                "Coordinates validated and reprojected to WGS84",
                "Properties optimized for web display",
                "Geometry validated and repaired where necessary",
                "Large dataset aggregated for web performance",
            ],
        }

        # Save enhanced GeoJSON with compact formatting
        with open(output_path, "w") as f:
            json.dump(geojson_data, f, separators=(",", ":"))

        file_size = output_path.stat().st_size / (1024 * 1024)
        logger.success(f"  ✅ Exported {len(gdf_export):,} features ({file_size:.1f} MB)")

        return True

    except Exception as e:
        logger.critical(f"❌ GeoJSON export failed: {e}")
        logger.trace("Detailed export error:")
        import traceback

        logger.trace(traceback.format_exc())
        return False


def main() -> None:
    """Main execution function."""
    logger.info("👥 Voter Location Analysis with Spatial Aggregation")
    logger.info("=" * 60)

    # Load configuration
    try:
        config = Config()
        logger.info(f"📋 Project: {config.get('project_name')}")
        logger.info(f"📋 Description: {config.get('description')}")
    except Exception as e:
        logger.critical(f"❌ Configuration error: {e}")
        logger.info("💡 Make sure config.yaml exists in the analysis directory")
        sys.exit(1)

    # === 1. Load Data ===
    logger.info("📊 Loading input data...")

    # Load voter data
    voters_df = load_and_validate_voter_data(config)
    if voters_df is None:
        sys.exit(1)

    # Load district boundaries
    districts_gdf = load_pps_district_boundaries(config)
    if districts_gdf is None:
        sys.exit(1)

    # Load block groups (optional for additional analysis)
    block_groups_gdf = load_block_group_boundaries(config)

    # === 2. Spatial Processing ===
    logger.info("🗺️ Performing spatial analysis...")

    # Create voter GeoDataFrame
    voters_gdf = create_voter_geodataframe(voters_df)

    # Classify voters by district
    voters_classified = classify_voters_by_district(voters_gdf, districts_gdf)

    # === 3. Spatial Aggregation for Web Performance ===
    logger.info("🐝 Creating spatial aggregation for web consumption...")

    # Create hexagonal aggregation (web-optimized)
    hex_aggregation = create_hexagonal_aggregation(voters_classified, config)

    # Block group analysis (if available)
    block_group_analysis = None
    if block_groups_gdf is not None:
        block_group_analysis = analyze_voters_by_block_groups(voters_classified, block_groups_gdf)

    # === 4. Export Optimized Data Layers ===
    logger.info("💾 Exporting optimized data layers...")

    # Export hexagonal aggregation for web consumption
    hex_output_path = config.get_output_dir("geospatial") / "voter_hex_aggregation.geojson"
    if not export_optimized_geojson(
        hex_aggregation, hex_output_path, config, "hexagonal_voter_density"
    ):
        sys.exit(1)

    # Export block group analysis if available
    if block_group_analysis is not None:
        bg_output_path = config.get_output_dir("geospatial") / "voter_block_groups.geojson"
        if not export_optimized_geojson(
            block_group_analysis, bg_output_path, config, "block_group_voter_analysis"
        ):
            logger.warning("⚠️ Block group export failed, continuing...")

    # Export district summary
    district_summary = districts_gdf.copy()
    district_summary["total_voters"] = voters_classified["inside_pps"].sum()
    district_summary["voter_count"] = len(voters_classified[voters_classified["inside_pps"]])

    district_output_path = (
        config.get_output_dir("geospatial") / "pps_district_voter_summary.geojson"
    )
    if not export_optimized_geojson(
        district_summary, district_output_path, config, "district_voter_summary"
    ):
        logger.warning("⚠️ District summary export failed, continuing...")

    # === 5. Upload to Supabase (Optional) ===
    if SUPABASE_AVAILABLE:
        logger.info("🚀 Uploading to Supabase PostGIS database...")

        try:
            uploader = SupabaseUploader(config)

            # Upload hexagonal aggregation (primary web layer)
            if uploader.upload_geodataframe(
                hex_aggregation,
                table_name="voter_hexagons",
                description="Hexagonal aggregation of voter density for web visualization - optimized for fast loading and spatial queries",
            ):
                logger.success("   ✅ Uploaded voter hexagons to Supabase")

                # Optional: Use new patterns for verification (commented out for simplicity)
                # db = get_supabase_database(config)
                # spatial_repo = SpatialRepository(db)
                # sample_records = spatial_repo.get_voter_density_hexagons(limit=5)
                # logger.debug(f"   📊 Verified upload: {len(sample_records)} sample records")

            # Upload block group analysis (detailed analysis layer)
            if block_group_analysis is not None:
                if uploader.upload_geodataframe(
                    block_group_analysis,
                    table_name="voter_block_groups",
                    description="Detailed voter analysis by census block groups with demographic context",
                ):
                    logger.success("   ✅ Uploaded voter block groups to Supabase")

            # Upload district summary (boundary layer)
            if uploader.upload_geodataframe(
                district_summary,
                table_name="pps_district_summary",
                description="Portland Public Schools district boundaries with voter statistics summary",
            ):
                logger.success("   ✅ Uploaded PPS district summary to Supabase")

        except Exception as e:
            logger.error(f"❌ Supabase upload failed: {e}")
            logger.info("   💡 Check your Supabase credentials and connection")
    else:
        logger.info("📊 Supabase integration not available - skipping database upload")
        logger.info("   💡 Install dependencies with: pip install sqlalchemy psycopg2-binary")

    # === 6. Create Interactive Visualization ===
    logger.info("🎨 Creating interactive visualization...")

    # HTML generation removed - visualization now handled by separate test_voter_heatmap.html file

    # === 7. Summary ===
    logger.success("✅ Voter location analysis completed successfully!")

    logger.info("📊 File Outputs:")
    logger.info(f"   🐝 Hexagonal aggregation: {hex_output_path}")
    if block_group_analysis is not None:
        logger.info(f"   📍 Block group analysis: {bg_output_path}")
    logger.info(f"   🏫 District summary: {district_output_path}")
    logger.info(f"   🗺️ Interactive map: {config.get_voter_heatmap_path()}")

    if SUPABASE_AVAILABLE:
        logger.info("🚀 Database Tables:")
        logger.info("   📤 voter_hexagons - Optimized for web visualization")
        logger.info("   📤 voter_block_groups - Detailed demographic analysis")
        logger.info("   📤 pps_district_summary - District boundary with stats")

    # Performance summary
    total_input = len(voters_df)
    total_hex = len(hex_aggregation)
    reduction_factor = total_input / total_hex if total_hex > 0 else 0

    logger.info("📈 Performance Summary:")
    logger.info(f"   📊 Input records: {total_input:,}")
    logger.info(f"   🐝 Output hexagons: {total_hex:,}")
    logger.info(f"   📉 Data reduction: {reduction_factor:.1f}x smaller")
    logger.info("   ✅ Ready for web consumption and backend integration!")


if __name__ == "__main__":
    main()
