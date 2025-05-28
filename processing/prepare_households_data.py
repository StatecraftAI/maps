#!/usr/bin/env python3
"""
prepare_households_data.py - MVP Household Demographics Preprocessor

The preprocessing step for household demographics analysis.

Usage: 
    python prepare_households_data.py
    
Input:  
    - config.yaml (file paths and settings)
    - ACS household data JSON
    - Census block group boundaries GeoJSON
    - PPS district boundaries GeoJSON
    
Output: 
    - Clean household demographics geodata ready for geo_upload.py

Result: From ACS JSON + boundaries to demographic geodata in ~100 lines.
"""

import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd
import numpy as np
import json
from loguru import logger

# Add parent directory for config
sys.path.append(str(Path(__file__).parent.parent))
from ops.config_loader import Config


def load_acs_household_data(config: Config) -> pd.DataFrame:
    """Load and process ACS household data from JSON."""
    logger.info("📊 Loading ACS household data...")
    
    acs_path = config.get_input_path("acs_households_json")
    logger.info(f"  📄 ACS data: {acs_path}")
    
    # Load JSON data
    with open(acs_path, 'r') as f:
        data = json.load(f)
    
    # Convert to DataFrame (skip header row, use second row as column names)
    if len(data) < 2:
        logger.error("❌ ACS data file needs at least 2 rows (header + data)")
        return None
    
    # First row is headers, rest is data
    headers = data[0]
    rows = data[1:]
    
    df = pd.DataFrame(rows, columns=headers)
    logger.info(f"  ✅ Loaded {len(df)} block groups from ACS data")
    
    return df


def process_household_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate household demographic metrics."""
    logger.info("🏠 Processing household metrics...")
    
    # The JSON has columns: NAME, B11001_001E (total households), B11001_002E (households no minors), state, county, tract, block group
    df = df.rename(columns={
        "NAME": "name",
        "B11001_001E": "total_households", 
        "B11001_002E": "households_no_minors",
        "state": "state",
        "county": "county", 
        "tract": "tract",
        "block group": "block_group"
    })
    
    # Construct full GEOID from components (state + county + tract + block group)
    df["GEOID"] = (
        df["state"].astype(str) + 
        df["county"].astype(str) + 
        df["tract"].astype(str) + 
        df["block_group"].astype(str)
    )
    
    # Convert household data to numeric
    df["total_households"] = pd.to_numeric(df["total_households"], errors="coerce").fillna(0).astype(int)
    df["households_no_minors"] = pd.to_numeric(df["households_no_minors"], errors="coerce").fillna(0).astype(int)
    
    # Calculate percentage of households without minors
    df["pct_households_no_minors"] = np.where(
        df["total_households"] > 0,
        (df["households_no_minors"] / df["total_households"]) * 100,
        0
    )
    
    logger.info(f"  ✅ Calculated metrics for {len(df)} block groups")
    logger.info(f"  📊 Avg % households without minors: {df['pct_households_no_minors'].mean():.1f}%")
    logger.info(f"  📋 Sample GEOID: {df['GEOID'].iloc[0]}")
    
    return df


def load_block_group_boundaries(config: Config) -> gpd.GeoDataFrame:
    """Load census block group boundaries and filter to Multnomah County."""
    logger.info("🗺️ Loading block group boundaries...")
    
    bg_path = config.get_input_path("census_blocks_geojson")
    logger.info(f"  📄 Boundaries: {bg_path}")
    
    # Load boundaries
    gdf = gpd.read_file(bg_path)
    logger.info(f"  ✅ Loaded {len(gdf)} block group boundaries")
    
    # Filter to Multnomah County (county code 051)
    if "COUNTYFP" in gdf.columns:
        multnomah_gdf = gdf[gdf["COUNTYFP"] == "051"].copy()
        logger.info(f"  🎯 Filtered to {len(multnomah_gdf)} Multnomah County block groups")
        return multnomah_gdf
    else:
        logger.warning("  ⚠️ No COUNTYFP column found, using all block groups")
        return gdf


def merge_acs_with_boundaries(df: pd.DataFrame, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Merge ACS data with block group geometries."""
    logger.info("🔗 Merging ACS data with boundaries...")
    
    # Ensure GEOID columns match
    df["GEOID"] = df["GEOID"].astype(str)
    gdf["GEOID"] = gdf["GEOID"].astype(str)
    
    # Merge
    merged_gdf = gdf.merge(df, on="GEOID", how="left")
    
    # Fill missing values
    merged_gdf["total_households"] = merged_gdf["total_households"].fillna(0)
    merged_gdf["households_no_minors"] = merged_gdf["households_no_minors"].fillna(0)
    merged_gdf["pct_households_no_minors"] = merged_gdf["pct_households_no_minors"].fillna(0)
    
    logger.info(f"  ✅ Merged: {len(merged_gdf)} block groups with household data")
    
    # Report data coverage
    has_data = merged_gdf["total_households"] > 0
    logger.info(f"  📊 Data coverage: {has_data.sum()}/{len(merged_gdf)} block groups have household data")
    
    return merged_gdf


def filter_to_pps_district(gdf: gpd.GeoDataFrame, config: Config) -> gpd.GeoDataFrame:
    """Filter block groups to those within PPS district boundaries."""
    logger.info("🎯 Filtering to PPS district...")
    
    # Load PPS boundaries
    pps_path = config.get_input_path("pps_boundary_geojson")
    pps_gdf = gpd.read_file(pps_path)
    logger.info(f"  📄 Loaded PPS boundaries: {len(pps_gdf)} features")
    
    # Ensure same CRS
    if gdf.crs != pps_gdf.crs:
        logger.info(f"  🔄 Reprojecting PPS boundaries from {pps_gdf.crs} to {gdf.crs}")
        pps_gdf = pps_gdf.to_crs(gdf.crs)
    
    # Spatial intersection - keep block groups that overlap with PPS
    pps_union = pps_gdf.geometry.unary_union
    pps_mask = gdf.geometry.intersects(pps_union)
    
    pps_gdf_filtered = gdf[pps_mask].copy()
    
    logger.info(f"  ✅ Filtered: {len(pps_gdf_filtered)}/{len(gdf)} block groups within PPS district")
    
    return pps_gdf_filtered


def add_demographic_analysis(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add demographic analysis fields."""
    logger.info("📈 Adding demographic analysis fields...")
    
    # Calculate household density (households per square km)
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        # Use projected CRS for area calculation
        area_gdf = gdf.to_crs("EPSG:3857")  # Web Mercator for area
        gdf["area_sq_km"] = area_gdf.geometry.area / 1e6  # Convert to sq km
    else:
        # Rough area calculation for WGS84 (good enough for analysis)
        gdf["area_sq_km"] = gdf.geometry.area * 111**2  # Rough conversion
    
    gdf["household_density"] = np.where(
        gdf["area_sq_km"] > 0,
        gdf["total_households"] / gdf["area_sq_km"],
        0
    )
    
    logger.info("  ✅ Added household_density (households per sq km)")
    
    # Categorize household density
    valid_density = gdf[gdf["household_density"] > 0]["household_density"]
    if len(valid_density) > 0:
        q1 = valid_density.quantile(0.33)
        q3 = valid_density.quantile(0.67)
        
        gdf["density_category"] = "No Data"
        mask = gdf["household_density"] > 0
        gdf.loc[mask & (gdf["household_density"] <= q1), "density_category"] = "Low Density"
        gdf.loc[mask & (gdf["household_density"] > q1) & (gdf["household_density"] <= q3), "density_category"] = "Medium Density"
        gdf.loc[mask & (gdf["household_density"] > q3), "density_category"] = "High Density"
        
        logger.info(f"  ✅ Added density_category (Low: ≤{q1:.0f}, Med: {q1:.0f}-{q3:.0f}, High: >{q3:.0f})")
    
    # Categorize % households without minors for education analysis
    gdf["family_composition"] = "No Data"
    mask = gdf["total_households"] > 0
    
    # School-age relevant categories
    gdf.loc[mask & (gdf["pct_households_no_minors"] < 50), "family_composition"] = "Many Families"
    gdf.loc[mask & (gdf["pct_households_no_minors"] >= 50) & (gdf["pct_households_no_minors"] < 75), "family_composition"] = "Mixed"
    gdf.loc[mask & (gdf["pct_households_no_minors"] >= 75), "family_composition"] = "Few Families"
    
    logger.info("  ✅ Added family_composition categories for school analysis")
    
    # School-relevant population indicator
    gdf["school_relevance_score"] = np.where(
        gdf["total_households"] > 0,
        (100 - gdf["pct_households_no_minors"]) * np.log(gdf["total_households"] + 1),
        0
    )
    
    logger.info("  ✅ Added school_relevance_score (families with children × household scale)")
    
    # Data quality flags
    gdf["has_household_data"] = gdf["total_households"] > 0
    gdf["reliable_data"] = gdf["total_households"] >= 10  # Minimum for reliable percentages
    
    logger.info("  ✅ Added data quality flags")
    
    analytical_fields = ["household_density", "density_category", "family_composition", "school_relevance_score"]
    logger.info(f"  📊 Added {len(analytical_fields)} demographic analysis fields")
    
    return gdf


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
    numeric_cols = ["total_households", "households_no_minors", "pct_households_no_minors", 
                   "household_density", "school_relevance_score", "area_sq_km"]
    
    for col in numeric_cols:
        if col in gdf.columns:
            gdf[col] = pd.to_numeric(gdf[col], errors="coerce").fillna(0)
    
    # Clean categorical columns for GeoJSON compatibility
    categorical_cols = ["density_category", "family_composition"]
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


def prepare_households_data() -> str:
    """Main function - prepare household demographics data for upload."""
    logger.info("🏠 Household Demographics Preparation - MVP")
    logger.info("=" * 50)
    
    # Load configuration
    config = Config()
    logger.info(f"📋 Project: {config.get('project_name')}")
    
    # Process data step by step
    df = load_acs_household_data(config)
    if df is None:
        return None
    
    df = process_household_metrics(df)
    
    gdf = load_block_group_boundaries(config)
    if gdf is None:
        return None
    
    gdf = merge_acs_with_boundaries(df, gdf)
    gdf = filter_to_pps_district(gdf, config)
    gdf = add_demographic_analysis(gdf)
    gdf = clean_and_validate(gdf)
    
    # Save for upload
    output_file = Path("data/processed_households_data.geojson")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(output_file, driver="GeoJSON")
    
    logger.success(f"✅ Household demographics prepared: {output_file}")
    logger.info(f"  📊 {len(gdf)} block groups ready for upload")
    logger.info(f"  🏠 {gdf['has_household_data'].sum()} block groups with household data")
    
    # Summary stats
    if gdf["has_household_data"].any():
        avg_pct_no_minors = gdf[gdf["has_household_data"]]["pct_households_no_minors"].mean()
        avg_density = gdf[gdf["has_household_data"]]["household_density"].mean()
        logger.info(f"  📈 Avg households without minors: {avg_pct_no_minors:.1f}%")
        logger.info(f"  📈 Avg household density: {avg_density:.1f} per sq km")
    
    return str(output_file)


if __name__ == "__main__":
    prepare_households_data() 