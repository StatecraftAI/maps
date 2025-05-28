#!/usr/bin/env python3
"""
prepare_voterfile_data.py - MVP Voter Registration Preprocessor

The preprocessing step for voter registration spatial analysis.

Usage: 
    python prepare_voterfile_data.py
    
Input:  
    - config.yaml (file paths and settings)
    - Voter registration CSV (with lat/lon coordinates)
    - PPS district boundaries GeoJSON
    - Census block group boundaries GeoJSON (optional)
    
Output: 
    - Clean voter registration geodata ready for geo_upload.py
    - Optional: Spatial aggregations (hexagons, grids, block groups)

Result: From voter CSV + boundaries to spatial voter geodata.
"""

import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd
import numpy as np
from loguru import logger
import h3
from shapely.geometry import Point, Polygon

# Add parent directory for config
sys.path.append(str(Path(__file__).parent.parent))
from ops.config_loader import Config


def load_voter_registration_data(config: Config) -> pd.DataFrame:
    """Load and validate voter registration data."""
    logger.info("📊 Loading voter registration data...")
    
    # Get voter file path from config (we'll need to add this)
    try:
        voter_path = config.get_input_path("voters_file_csv")
        logger.info(f"  📄 Voter data: {voter_path}")
    except:
        logger.warning("  ⚠️ No voters_file_csv in config, using placeholder")
        return None
    
    # Load voter data
    df = pd.read_csv(voter_path)
    logger.info(f"  ✅ Loaded {len(df)} voter registration records")
    
    return df


def process_voter_coordinates(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """Process and validate voter coordinates."""
    logger.info("📍 Processing voter coordinates...")
    
    # Get coordinate column names from config
    lat_col = config.get_column_name("latitude")
    lon_col = config.get_column_name("longitude")
    
    # Convert coordinates to numeric
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
    
    # Count valid coordinates
    valid_coords = df[lat_col].notna() & df[lon_col].notna()
    valid_count = valid_coords.sum()
    
    logger.info(f"  ✅ Valid coordinates: {valid_count:,}/{len(df):,} records ({valid_count/len(df)*100:.1f}%)")
    
    # Filter to valid coordinates only
    df_valid = df[valid_coords].copy()
    
    # Basic coordinate validation (rough Oregon bounds)
    oregon_mask = (
        (df_valid[lat_col] >= 42) & (df_valid[lat_col] <= 46.5) &
        (df_valid[lon_col] >= -125) & (df_valid[lon_col] <= -116)
    )
    
    oregon_count = oregon_mask.sum()
    logger.info(f"  🎯 Oregon coordinates: {oregon_count:,}/{valid_count:,} records ({oregon_count/valid_count*100:.1f}%)")
    
    return df_valid[oregon_mask].copy()


def create_voter_geodataframe(df: pd.DataFrame, config: Config) -> gpd.GeoDataFrame:
    """Convert voter DataFrame to GeoDataFrame with point geometries."""
    logger.info("🗺️ Creating voter GeoDataFrame...")
    
    lat_col = config.get_column_name("latitude")
    lon_col = config.get_column_name("longitude")
    
    # Create Point geometries
    geometry = [Point(lon, lat) for lon, lat in zip(df[lon_col], df[lat_col])]
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    
    logger.info(f"  ✅ Created GeoDataFrame: {len(gdf):,} voter points")
    
    return gdf


def filter_to_pps_district(voters_gdf: gpd.GeoDataFrame, config: Config) -> gpd.GeoDataFrame:
    """Filter voters to those within PPS district boundaries."""
    logger.info("🎯 Filtering voters to PPS district...")
    
    # Load PPS boundaries
    pps_path = config.get_input_path("pps_boundary_geojson")
    pps_gdf = gpd.read_file(pps_path)
    logger.info(f"  📄 Loaded PPS boundaries: {len(pps_gdf)} features")
    
    # Ensure same CRS
    if voters_gdf.crs != pps_gdf.crs:
        logger.info(f"  🔄 Reprojecting PPS boundaries from {pps_gdf.crs} to {voters_gdf.crs}")
        pps_gdf = pps_gdf.to_crs(voters_gdf.crs)
    
    # Spatial filter - keep voters within PPS district
    pps_union = pps_gdf.geometry.unary_union
    within_pps = voters_gdf.geometry.within(pps_union)
    
    pps_voters = voters_gdf[within_pps].copy()
    
    logger.info(f"  ✅ PPS voters: {len(pps_voters):,}/{len(voters_gdf):,} ({len(pps_voters)/len(voters_gdf)*100:.1f}%)")
    
    # Add PPS flag
    pps_voters["within_pps"] = True
    
    return pps_voters


def add_voter_analysis_fields(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add voter analysis fields."""
    logger.info("📈 Adding voter analysis fields...")
    
    # Party registration analysis (if party columns exist)
    party_cols = ["DEM", "REP", "NAV"]  # Common party abbreviations
    available_parties = [col for col in party_cols if col in gdf.columns]
    
    if available_parties:
        logger.info(f"  📊 Found party registration data: {available_parties}")
        
        # Convert party columns to numeric
        for party in available_parties:
            gdf[party] = pd.to_numeric(gdf[party], errors="coerce").fillna(0)
        
        # Calculate party percentages (if we have total voter count)
        if "TOTAL" in gdf.columns:
            gdf["TOTAL"] = pd.to_numeric(gdf["TOTAL"], errors="coerce").fillna(0)
            
            for party in available_parties:
                pct_col = f"pct_{party.lower()}"
                gdf[pct_col] = np.where(
                    gdf["TOTAL"] > 0,
                    (gdf[party] / gdf["TOTAL"]) * 100,
                    0
                )
                logger.info(f"    ✅ Added {pct_col}")
            
            # Democratic advantage
            if "DEM" in available_parties and "REP" in available_parties:
                gdf["dem_advantage"] = gdf["pct_dem"] - gdf["pct_rep"]
                logger.info("    ✅ Added dem_advantage")
    
    # Voter classification by party dominance (if party data available)
    if "pct_dem" in gdf.columns and "pct_rep" in gdf.columns:
        gdf["voter_lean"] = "Independent"
        gdf.loc[gdf["pct_dem"] > gdf["pct_rep"] + 10, "voter_lean"] = "Democratic"
        gdf.loc[gdf["pct_rep"] > gdf["pct_dem"] + 10, "voter_lean"] = "Republican"
        gdf.loc[abs(gdf["pct_dem"] - gdf["pct_rep"]) <= 10, "voter_lean"] = "Competitive"
        
        logger.info("  ✅ Added voter_lean classification")
    
    # Data quality flags
    gdf["has_coordinates"] = True  # All records have coordinates at this point
    gdf["within_oregon"] = True  # All passed Oregon bounds check
    gdf["in_pps_district"] = gdf.get("within_pps", False)
    
    logger.info("  ✅ Added data quality flags")
    
    analytical_fields = len([col for col in gdf.columns if col.startswith(("pct_", "dem_", "voter_", "has_", "within_", "in_"))])
    logger.info(f"  📊 Added {analytical_fields} voter analysis fields")
    
    return gdf


def create_hexagonal_aggregation(voters_gdf: gpd.GeoDataFrame, resolution: int = 8) -> gpd.GeoDataFrame:
    """Create hexagonal aggregation of voter data."""
    logger.info(f"🔷 Creating hexagonal aggregation (resolution {resolution})...")
    
    # Get coordinates
    voters_gdf["lat"] = voters_gdf.geometry.y
    voters_gdf["lon"] = voters_gdf.geometry.x
    
    # Convert to H3 hexagons
    voters_gdf["h3_hex"] = voters_gdf.apply(
        lambda row: h3.geo_to_h3(row["lat"], row["lon"], resolution), axis=1
    )
    
    # Aggregate by hexagon
    hex_agg = voters_gdf.groupby("h3_hex").agg({
        "VOTER_ID": "count" if "VOTER_ID" in voters_gdf.columns else "size",
        "DEM": "sum" if "DEM" in voters_gdf.columns else lambda x: 0,
        "REP": "sum" if "REP" in voters_gdf.columns else lambda x: 0,
        "NAV": "sum" if "NAV" in voters_gdf.columns else lambda x: 0,
    }).reset_index()
    
    # Rename count column
    count_col = "VOTER_ID" if "VOTER_ID" in voters_gdf.columns else "size"
    hex_agg = hex_agg.rename(columns={count_col: "voter_count"})
    
    # Create hexagon geometries
    hex_agg["geometry"] = hex_agg["h3_hex"].apply(
        lambda h: Polygon(h3.h3_to_geo_boundary(h, geo_json=False))
    )
    
    # Convert to GeoDataFrame
    hex_gdf = gpd.GeoDataFrame(hex_agg, crs="EPSG:4326")
    
    # Calculate voter density
    hex_gdf["area_sq_km"] = hex_gdf.geometry.to_crs("EPSG:3857").area / 1e6
    hex_gdf["voter_density"] = hex_gdf["voter_count"] / hex_gdf["area_sq_km"]
    
    # Add analysis fields
    if "DEM" in hex_gdf.columns and "REP" in hex_gdf.columns:
        hex_gdf["total_party_voters"] = hex_gdf["DEM"] + hex_gdf["REP"] + hex_gdf["NAV"]
        hex_gdf["pct_dem"] = np.where(
            hex_gdf["total_party_voters"] > 0,
            (hex_gdf["DEM"] / hex_gdf["total_party_voters"]) * 100,
            0
        )
        hex_gdf["pct_rep"] = np.where(
            hex_gdf["total_party_voters"] > 0,
            (hex_gdf["REP"] / hex_gdf["total_party_voters"]) * 100,
            0
        )
        hex_gdf["dem_advantage"] = hex_gdf["pct_dem"] - hex_gdf["pct_rep"]
    
    logger.info(f"  ✅ Created {len(hex_gdf):,} hexagons with voter data")
    
    return hex_gdf


def analyze_by_block_groups(voters_gdf: gpd.GeoDataFrame, config: Config) -> gpd.GeoDataFrame:
    """Aggregate voter data by census block groups."""
    logger.info("📊 Analyzing voters by census block groups...")
    
    # Load block group boundaries
    bg_path = config.get_input_path("census_blocks_geojson")
    bg_gdf = gpd.read_file(bg_path)
    
    # Filter to Multnomah County
    if "COUNTYFP" in bg_gdf.columns:
        bg_gdf = bg_gdf[bg_gdf["COUNTYFP"] == "051"].copy()
        logger.info(f"  📄 Loaded {len(bg_gdf)} Multnomah County block groups")
    
    # Ensure same CRS
    if voters_gdf.crs != bg_gdf.crs:
        bg_gdf = bg_gdf.to_crs(voters_gdf.crs)
    
    # Spatial join voters to block groups
    voters_with_bg = gpd.sjoin(voters_gdf, bg_gdf[["GEOID", "geometry"]], how="left", predicate="within")
    
    # Aggregate by block group
    bg_agg = voters_with_bg.groupby("GEOID").agg({
        "VOTER_ID": "count" if "VOTER_ID" in voters_with_bg.columns else "size",
        "DEM": "sum" if "DEM" in voters_with_bg.columns else lambda x: 0,
        "REP": "sum" if "REP" in voters_with_bg.columns else lambda x: 0,
        "NAV": "sum" if "NAV" in voters_with_bg.columns else lambda x: 0,
    }).reset_index()
    
    count_col = "VOTER_ID" if "VOTER_ID" in voters_with_bg.columns else "size"
    bg_agg = bg_agg.rename(columns={count_col: "voter_count"})
    
    # Merge back with block group geometries
    bg_result = bg_gdf.merge(bg_agg, on="GEOID", how="left")
    bg_result["voter_count"] = bg_result["voter_count"].fillna(0)
    
    # Calculate density
    bg_result["area_sq_km"] = bg_result.geometry.to_crs("EPSG:3857").area / 1e6
    bg_result["voter_density"] = np.where(
        bg_result["area_sq_km"] > 0,
        bg_result["voter_count"] / bg_result["area_sq_km"],
        0
    )
    
    logger.info(f"  ✅ Analyzed {len(bg_result)} block groups with voter data")
    
    return bg_result


def clean_and_validate(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Clean data types and validate geometries."""
    logger.info("🧹 Cleaning and validating...")
    
    # Ensure WGS84 for output
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        logger.info(f"  🔄 Reprojecting from {gdf.crs} to WGS84")
        gdf = gdf.to_crs("EPSG:4326")
    
    # Clean numeric columns
    numeric_cols = [col for col in gdf.columns if col.startswith(("pct_", "DEM", "REP", "NAV", "TOTAL", "voter_count", "voter_density", "dem_advantage"))]
    
    for col in numeric_cols:
        if col in gdf.columns:
            gdf[col] = pd.to_numeric(gdf[col], errors="coerce").fillna(0)
    
    # Clean categorical columns for GeoJSON compatibility
    categorical_cols = ["voter_lean"]
    for col in categorical_cols:
        if col in gdf.columns:
            gdf[col] = gdf[col].astype(str).replace(["nan", "None", "<NA>"], "No Data")
    
    # Validate geometries
    invalid_geom = gdf[~gdf.geometry.is_valid]
    if len(invalid_geom) > 0:
        logger.warning(f"  ⚠️ Found {len(invalid_geom)} invalid geometries, fixing...")
        gdf.geometry = gdf.geometry.buffer(0)
    
    logger.info("  ✅ Data cleaned and validated")
    return gdf


def prepare_voterfile_data() -> tuple:
    """Main function - prepare voter registration data for upload."""
    logger.info("🗳️ Voter Registration Data Preparation - MVP")
    logger.info("=" * 50)
    
    # Load configuration
    config = Config()
    logger.info(f"📋 Project: {config.get('project_name')}")
    
    # Process voter data step by step
    df = load_voter_registration_data(config)
    if df is None:
        logger.warning("  ⚠️ No voter data available, skipping voter file processing")
        return None, None, None
    
    df = process_voter_coordinates(df, config)
    gdf = create_voter_geodataframe(df, config)
    gdf = filter_to_pps_district(gdf, config)
    gdf = add_voter_analysis_fields(gdf)
    gdf = clean_and_validate(gdf)
    
    # Save individual voters for upload
    voters_output_file = Path("data/processed_voters_data.geojson")
    voters_output_file.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(voters_output_file, driver="GeoJSON")
    
    logger.success(f"✅ Voter data prepared: {voters_output_file}")
    logger.info(f"  📊 {len(gdf)} voters ready for upload")
    logger.info(f"  🎯 PPS district voters processed")
    
    # Create hexagonal aggregation
    hex_gdf = None
    hex_output_file = None
    try:
        hex_gdf = create_hexagonal_aggregation(gdf, resolution=8)
        hex_gdf = clean_and_validate(hex_gdf)
        
        hex_output_file = Path("data/processed_voter_hexagons.geojson")
        hex_output_file.parent.mkdir(parents=True, exist_ok=True)
        hex_gdf.to_file(hex_output_file, driver="GeoJSON")
        
        logger.success(f"✅ Voter hexagons prepared: {hex_output_file}")
        logger.info(f"  🔷 {len(hex_gdf)} hexagons with voter aggregations")
        hex_output_file = str(hex_output_file)
        
    except Exception as e:
        logger.warning(f"  ⚠️ Could not create hexagonal aggregation: {e}")
        hex_output_file = None
    
    # Create block group analysis
    bg_gdf = None
    bg_output_file = None
    try:
        bg_gdf = analyze_by_block_groups(gdf, config)
        bg_gdf = clean_and_validate(bg_gdf)
        
        bg_output_file = Path("data/processed_voter_blockgroups.geojson")
        bg_output_file.parent.mkdir(parents=True, exist_ok=True)
        bg_gdf.to_file(bg_output_file, driver="GeoJSON")
        
        logger.success(f"✅ Voter block groups prepared: {bg_output_file}")
        logger.info(f"  📊 {len(bg_gdf)} block groups with voter analysis")
        bg_output_file = str(bg_output_file)
        
    except Exception as e:
        logger.warning(f"  ⚠️ Could not create block group analysis: {e}")
        bg_output_file = None
    
    # Summary stats
    if len(gdf) > 0:
        if "pct_dem" in gdf.columns and "pct_rep" in gdf.columns:
            avg_dem = gdf["pct_dem"].mean()
            avg_rep = gdf["pct_rep"].mean()
            logger.info(f"  📈 Avg Democratic registration: {avg_dem:.1f}%")
            logger.info(f"  📈 Avg Republican registration: {avg_rep:.1f}%")
        
        lean_dist = gdf["voter_lean"].value_counts() if "voter_lean" in gdf.columns else None
        if lean_dist is not None:
            logger.info(f"  📊 Voter lean distribution: {dict(lean_dist)}")
    
    return str(voters_output_file), hex_output_file, bg_output_file


if __name__ == "__main__":
    prepare_voterfile_data() 