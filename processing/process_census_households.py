"""
process_census_households.py

This script processes census household data, focusing on demographic analysis within the
Portland Public Schools (PPS) district. It merges American Community Survey (ACS) data
with census block group geometries, performs spatial analysis, and prepares the data
for visualization and backend integration.

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

import json
import sys
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))
from ops import Config

# Import optimization functions from the election results module
try:
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
    from ops.repositories import SpatialQueryManager
    from ops.supabase_integration import SupabaseDatabase, SupabaseUploader

    logger.debug("✅ Imported Supabase integration with improved design patterns")
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

    # Fallback classes for when Supabase integration is not available
    class SupabaseUploader:
        def __init__(self, config):
            pass

        def upload_geodataframe(self, *args, **kwargs):
            return False

    class SupabaseDatabase:
        def __init__(self, config):
            pass

    class SpatialQueryManager:
        def __init__(self, db):
            pass

        def get_sample_records(self, *args, **kwargs):
            return []

    # Backward compatibility alias
    SpatialRepository = SpatialQueryManager


def load_and_process_acs_data(config: Config) -> Optional[pd.DataFrame]:
    """
    Load and process ACS household data from JSON with robust validation.

    Args:
        config: Configuration instance

    Returns:
        DataFrame with processed ACS data or None if failed
    """
    acs_path = config.get_input_path("acs_households_json")
    logger.info(f"📊 Loading ACS household data from {acs_path}")

    if not acs_path.exists():
        logger.critical(f"❌ ACS JSON file not found: {acs_path}")
        return None

    try:
        with open(acs_path) as f:
            data_array = json.load(f)

        # Validate JSON structure
        if not isinstance(data_array, list) or len(data_array) < 2:
            logger.critical("❌ Invalid ACS JSON structure - expected array with header and data")
            return None

        # First row is header, rest are data records
        header = data_array[0]
        records = data_array[1:]

        df = pd.DataFrame(records, columns=header)
        logger.success(f"  ✅ Loaded {len(df):,} ACS records")

        # Process and standardize ACS field names
        logger.debug("  🔧 Processing ACS field mappings...")
        df = df.rename(
            columns={
                "B11001_001E": "total_households",
                "B11001_002E": "households_no_minors",
            }
        )

        # Validate required columns exist
        required_cols = ["total_households", "households_no_minors"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.critical(f"❌ Missing required ACS columns: {missing_cols}")
            logger.critical(f"   Available columns: {list(df.columns)}")
            return None

        # Convert to numeric with robust error handling
        logger.debug("  🔢 Converting fields to numeric...")
        df["total_households"] = clean_numeric(df["total_households"]).astype(int)
        df["households_no_minors"] = clean_numeric(df["households_no_minors"]).astype(int)

        # Validate geographic identifier components
        geo_cols = ["state", "county", "tract", "block group"]
        missing_geo_cols = [col for col in geo_cols if col not in df.columns]
        if missing_geo_cols:
            logger.critical(f"❌ Missing geographic identifier columns: {missing_geo_cols}")
            return None

        # Create standardized GEOID from component parts
        logger.debug("  🗺️ Creating standardized GEOID...")
        df["GEOID"] = (
            df["state"].astype(str)
            + df["county"].astype(str)
            + df["tract"].astype(str)
            + df["block group"].astype(str)
        )

        # Validate GEOID format (should be 12 digits for block groups)
        invalid_geoids = df[df["GEOID"].str.len() != 12]
        if len(invalid_geoids) > 0:
            logger.warning(f"  ⚠️ Found {len(invalid_geoids)} records with invalid GEOID format")
            logger.debug(f"     Example invalid GEOIDs: {invalid_geoids['GEOID'].head().tolist()}")

        # Calculate percentage field with validation
        df["pct_households_no_minors"] = df.apply(
            lambda row: round(100 * row["households_no_minors"] / row["total_households"], 1)
            if row["total_households"] > 0
            else 0.0,
            axis=1,
        )

        # Data quality validation
        total_households_sum = df["total_households"].sum()
        no_minors_sum = df["households_no_minors"].sum()

        logger.success(f"  ✅ Processed household data for {len(df):,} block groups:")
        logger.info(f"     📊 Total households: {total_households_sum:,}")
        logger.info(f"     👥 Households without minors: {no_minors_sum:,}")
        logger.info(f"     📈 Overall rate: {no_minors_sum / total_households_sum * 100:.1f}%")

        return df

    except Exception as e:
        logger.critical(f"❌ Error loading ACS data: {e}")
        logger.trace("Detailed ACS loading error:")
        import traceback

        logger.trace(traceback.format_exc())
        return None


def load_and_validate_block_group_geometries(config: Config) -> Optional[gpd.GeoDataFrame]:
    """
    Load and validate block group geometries with CRS standardization.

    Args:
        config: Configuration instance

    Returns:
        GeoDataFrame with validated block group geometries or None if failed
    """
    bg_path = config.get_input_path("census_blocks_geojson")
    logger.info(f"🗺️ Loading block group geometries from {bg_path}")

    if not bg_path.exists():
        logger.critical(f"❌ Block groups shapefile not found: {bg_path}")
        return None

    try:
        gdf = gpd.read_file(bg_path)
        logger.success(f"  ✅ Loaded {len(gdf):,} block groups from shapefile")

        # Validate required columns
        required_cols = ["GEOID", "STATEFP", "COUNTYFP"]
        missing_cols = [col for col in required_cols if col not in gdf.columns]
        if missing_cols:
            logger.warning(f"  ⚠️ Missing expected columns: {missing_cols}")
            logger.debug(f"     Available columns: {list(gdf.columns)}")

        # Filter to Multnomah County (Oregon=41, Multnomah=051) for performance
        if "STATEFP" in gdf.columns and "COUNTYFP" in gdf.columns:
            multnomah_gdf = gdf[(gdf["STATEFP"] == "41") & (gdf["COUNTYFP"] == "051")].copy()
            logger.success(f"  ✅ Filtered to {len(multnomah_gdf):,} Multnomah County block groups")
        else:
            logger.warning("  ⚠️ Could not filter by county - using all block groups")
            multnomah_gdf = gdf.copy()

        # Validate and standardize CRS
        multnomah_gdf = validate_and_reproject_to_wgs84(
            multnomah_gdf, config, "block group geometries"
        )

        # Validate geometry
        invalid_geom = multnomah_gdf.geometry.isna() | (~multnomah_gdf.geometry.is_valid)
        invalid_count = invalid_geom.sum()

        if invalid_count > 0:
            logger.warning(f"  ⚠️ Found {invalid_count} invalid geometries, fixing...")
            multnomah_gdf.geometry = multnomah_gdf.geometry.buffer(0)
            logger.debug("  ✅ Geometry validation completed")

        return multnomah_gdf

    except Exception as e:
        logger.critical(f"❌ Error loading block group geometries: {e}")
        logger.trace("Detailed geometry loading error:")
        import traceback

        logger.trace(traceback.format_exc())
        return None


def merge_acs_with_geometries(
    acs_df: pd.DataFrame, bg_gdf: gpd.GeoDataFrame
) -> Optional[gpd.GeoDataFrame]:
    """
    Merge ACS data with block group geometries using robust join logic.

    Args:
        acs_df: DataFrame with ACS household data
        bg_gdf: GeoDataFrame with block group geometries

    Returns:
        GeoDataFrame with merged data or None if failed
    """
    logger.info("🔗 Merging ACS data with block group geometries...")

    try:
        # Validate join key consistency
        logger.debug("  🔍 Validating join keys...")
        acs_geoids = set(acs_df["GEOID"].astype(str))
        bg_geoids = set(bg_gdf["GEOID"].astype(str))

        # Analyze join coverage
        common_geoids = acs_geoids & bg_geoids
        acs_only = acs_geoids - bg_geoids
        bg_only = bg_geoids - acs_geoids

        logger.debug(f"     ACS GEOIDs: {len(acs_geoids):,}")
        logger.debug(f"     Block group GEOIDs: {len(bg_geoids):,}")
        logger.debug(f"     Common GEOIDs: {len(common_geoids):,}")

        if len(acs_only) > 0:
            logger.warning(f"  ⚠️ {len(acs_only):,} ACS records without geometry")
            logger.debug(f"     Example ACS-only: {list(acs_only)[:5]}")

        if len(bg_only) > 0:
            logger.debug(f"  📍 {len(bg_only):,} block groups without ACS data (expected)")

        # Perform merge with left join to preserve all geometries
        gdf = bg_gdf.merge(
            acs_df[
                ["GEOID", "total_households", "households_no_minors", "pct_households_no_minors"]
            ],
            on="GEOID",
            how="left",
        )

        # Handle missing values with appropriate defaults
        logger.debug("  🔧 Processing missing values...")
        fill_cols = ["total_households", "households_no_minors", "pct_households_no_minors"]
        gdf[fill_cols] = gdf[fill_cols].fillna(0)

        # Ensure proper data types
        gdf["total_households"] = gdf["total_households"].astype(int)
        gdf["households_no_minors"] = gdf["households_no_minors"].astype(int)
        gdf["pct_households_no_minors"] = gdf["pct_households_no_minors"].round(1)

        # Calculate additional metrics for analysis
        logger.debug("  📊 Calculating additional metrics...")

        # Add area calculation for density metrics
        # Use projected coordinates for accurate area calculation
        gdf_proj = gdf.to_crs("EPSG:3857")  # Web Mercator for area calculation
        gdf["area_km2"] = (gdf_proj.geometry.area / 1e6).round(3)  # Convert to km²

        # Household density
        gdf["household_density"] = (gdf["total_households"] / gdf["area_km2"]).round(1)

        # Replace infinite values (division by zero) with 0
        gdf["household_density"] = gdf["household_density"].replace(
            [float("inf"), -float("inf")], 0
        )

        logger.success(f"  ✅ Merged data for {len(gdf):,} block groups")

        # Summary statistics
        merged_with_data = gdf[gdf["total_households"] > 0]
        logger.info(f"     📍 Block groups with household data: {len(merged_with_data):,}")
        logger.info(
            f"     📊 Average % without minors: {merged_with_data['pct_households_no_minors'].mean():.1f}%"
        )
        logger.info(
            f"     🏠 Average household density: {merged_with_data['household_density'].mean():.1f}/km²"
        )

        return gdf

    except Exception as e:
        logger.critical(f"❌ Error merging ACS data with geometries: {e}")
        logger.trace("Detailed merge error:")
        import traceback

        logger.trace(traceback.format_exc())
        return None


def filter_to_pps_district(gdf: gpd.GeoDataFrame, config: Config) -> Optional[gpd.GeoDataFrame]:
    """
    Filter block groups to those within PPS district using robust spatial operations.

    Args:
        gdf: GeoDataFrame with block group data
        config: Configuration instance

    Returns:
        GeoDataFrame filtered to PPS district or None if failed
    """
    pps_path = config.get_input_path("pps_boundary_geojson")
    logger.info(f"🎯 Filtering to PPS district using {pps_path}")

    if not pps_path.exists():
        logger.critical(f"❌ PPS district file not found: {pps_path}")
        return None

    try:
        # Load PPS district boundaries
        pps_region = gpd.read_file(pps_path)
        logger.debug("  ✅ Loaded PPS district boundaries")

        # Validate and standardize CRS
        pps_region = validate_and_reproject_to_wgs84(pps_region, config, "PPS district boundaries")

        # Ensure consistent CRS between datasets
        if gdf.crs != pps_region.crs:
            logger.debug(f"  🔄 Aligning CRS: {gdf.crs} -> {pps_region.crs}")
            gdf = gdf.to_crs(pps_region.crs)

        # Project to appropriate CRS for geometric operations
        # Using Oregon North State Plane for accurate geometric operations
        target_crs = "EPSG:2913"  # NAD83(HARN) / Oregon North
        logger.debug(f"  📐 Projecting to {target_crs} for geometric operations...")

        pps_proj = pps_region.to_crs(target_crs)
        gdf_proj = gdf.to_crs(target_crs)

        # Create union of PPS boundaries for spatial filtering
        pps_union = pps_proj.geometry.unary_union

        # Use centroid-based filtering for block groups
        # This is more appropriate than intersection for administrative units
        logger.debug("  🎯 Filtering block groups by centroid intersection...")
        centroids = gdf_proj.geometry.centroid
        mask = centroids.within(pps_union)

        # Apply filter and reproject back to WGS84
        pps_gdf = gdf_proj[mask].to_crs("EPSG:4326")

        logger.success(f"  ✅ Filtered to {len(pps_gdf):,} block groups within PPS district")
        logger.info(
            f"     📊 PPS coverage: {len(pps_gdf) / len(gdf) * 100:.1f}% of Multnomah block groups"
        )

        # Add PPS district flag for reference
        pps_gdf["within_pps"] = True

        return pps_gdf

    except Exception as e:
        logger.critical(f"❌ Error filtering to PPS district: {e}")
        logger.trace("Detailed filtering error:")
        import traceback

        logger.trace(traceback.format_exc())
        return None


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
    bg_gdf = load_and_validate_block_group_geometries(config)
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
