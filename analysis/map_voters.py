#!/usr/bin/env python3
"""
Voter Location Analysis Script

This script analyzes voter locations relative to PPS district boundaries,
creating heatmaps and classification data.

Dependencies:
- folium, geopandas, pandas

Usage:
    python map_voters.py
"""

import sys

import folium
import geopandas as gpd
import pandas as pd
from config_loader import Config
from folium.plugins import HeatMap
from shapely.geometry import Point
from loguru import logger


def load_region_data(config: Config):
    """Load PPS district geometry data."""
    region_path = config.get_input_path("district_boundaries_geojson")
    logger.info(f"📍 Loading PPS district boundaries from {region_path}")

    if not region_path.exists():
        logger.info(f"❌ Error: PPS district file not found: {region_path}")
        return None

    try:
        regions = gpd.read_file(region_path)
        logger.info(f"  ✓ Loaded {len(regions)} region features")
        return regions
    except Exception as e:
        logger.info(f"❌ Error loading region data: {e}")
        return None


def load_voter_data(config: Config):
    """Load and clean voter CSV data."""
    voter_path = config.get_input_path("voter_locations_csv")
    logger.info(f"👥 Loading voter data from {voter_path}")

    if not voter_path.exists():
        logger.info(f"❌ Error: Voters file not found: {voter_path}")
        return None

    try:
        df = pd.read_csv(voter_path, low_memory=False)
        logger.info(f"  ✓ Loaded {len(df):,} voter records")

        # Clean column names
        cols = df.columns.str.strip().str.lower().str.replace(r"[^0-9a-z]+", "_", regex=True)
        df.columns = cols

        # Get coordinate column names from config
        lat_col = config.get_column_name("latitude")
        lon_col = config.get_column_name("longitude")

        # Standardize coordinate column names
        coordinate_mapping = {}
        for col in cols:
            if col in ("lat", "latitude", lat_col.lower()):
                coordinate_mapping[col] = "latitude"
            elif col in ("lon", "lng", "longitude", lon_col.lower()):
                coordinate_mapping[col] = "longitude"

        if coordinate_mapping:
            df = df.rename(columns=coordinate_mapping)
            logger.info(f"  ✓ Standardized coordinate columns: {list(coordinate_mapping.values())}")

        # Validate required columns exist
        if "latitude" not in df.columns or "longitude" not in df.columns:
            logger.info("❌ Error: Could not find latitude/longitude columns in voter data")
            logger.info(f"   Available columns: {list(df.columns)}")
            return None

        # Remove invalid coordinates
        initial_count = len(df)
        df = df.dropna(subset=["latitude", "longitude"])

        # Remove obviously invalid coordinates
        df = df[(df["latitude"].between(-90, 90)) & (df["longitude"].between(-180, 180))]

        valid_count = len(df)
        removed_count = initial_count - valid_count

        if removed_count > 0:
            logger.info(f"  ⚠️ Removed {removed_count:,} records with invalid coordinates")

        logger.info(f"  ✓ Retained {valid_count:,} valid voter locations")
        return df

    except Exception as e:
        logger.info(f"❌ Error loading voter data: {e}")
        return None


def classify_voters(df, regions):
    """Classify voters as inside or outside PPS district."""
    logger.info("🗺️ Classifying voter locations...")

    try:
        # Create GeoDataFrame from voter coordinates
        gdf = gpd.GeoDataFrame(
            df,
            geometry=[Point(xy) for xy in zip(df.longitude, df.latitude)],
            crs="EPSG:4326",
        )

        # Ensure regions and points use the same CRS
        if regions.crs != gdf.crs:
            logger.info(f"  🔄 Reprojecting regions from {regions.crs} to {gdf.crs}")
            regions = regions.to_crs(gdf.crs)

        # Create union of all region geometries
        union = regions.geometry.union_all()

        # Classify points
        gdf["inside_pps"] = gdf.geometry.within(union)

        inside_count = gdf["inside_pps"].sum()
        outside_count = len(gdf) - inside_count

        logger.info("  ✓ Classification complete:")
        logger.info(f"    • Inside PPS: {inside_count:,} voters")
        logger.info(f"    • Outside PPS: {outside_count:,} voters")
        logger.info(f"    • PPS coverage: {inside_count / len(gdf):.1%}")

        return gdf

    except Exception as e:
        logger.info(f"❌ Error classifying voters: {e}")
        return None


def export_classification_data(gdf, config: Config):
    """Export inside/outside classification to CSV files."""
    logger.info("💾 Exporting classification data...")

    try:
        # Get output paths from config
        inside_path = config.get_voters_inside_csv_path()
        outside_path = config.get_voters_outside_csv_path()

        # Export voters inside PPS
        inside_voters = gdf[gdf["inside_pps"]].drop(columns="geometry")
        inside_voters.to_csv(inside_path, index=False)
        logger.info(f"  ✓ Inside PPS: {len(inside_voters):,} voters → {inside_path}")

        # Export voters outside PPS
        outside_voters = gdf[~gdf["inside_pps"]].drop(columns="geometry")
        outside_voters.to_csv(outside_path, index=False)
        logger.info(f"  ✓ Outside PPS: {len(outside_voters):,} voters → {outside_path}")

        return True

    except Exception as e:
        logger.info(f"❌ Error exporting data: {e}")
        return False


def create_heatmap(gdf, regions, config: Config):
    """Create interactive Folium heatmap."""
    logger.info("🗺️ Creating interactive heatmap...")

    try:
        # Get output path from config
        output_path = config.get_voter_heatmap_path()

        # Calculate map center
        center_lat = gdf.latitude.mean()
        center_lon = gdf.longitude.mean()
        center = [center_lat, center_lon]

        logger.info(f"  📍 Map center: {center[0]:.4f}, {center[1]:.4f}")

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
        logger.info(f"  ✓ Interactive heatmap saved: {output_path}")

        return True

    except Exception as e:
        logger.info(f"❌ Error creating heatmap: {e}")
        return False


def main():
    """Main execution function."""
    logger.info("👥 Voter Location Analysis")
    logger.info("=" * 50)

    # Load configuration
    try:
        config = Config()
        logger.info(f"📋 Project: {config.get('project_name')}")
        logger.info(f"📋 Description: {config.get('description')}")
    except Exception as e:
        logger.info(f"❌ Configuration error: {e}")
        logger.info("💡 Make sure config.yaml exists in the analysis directory")
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

    logger.info("✅ Voter location analysis completed successfully!")
    logger.info("📊 Outputs:")
    inside_path = config.get_voters_inside_csv_path()
    outside_path = config.get_voters_outside_csv_path()
    heatmap_path = config.get_voter_heatmap_path()
    logger.info(f"   • Inside PPS CSV: {inside_path}")
    logger.info(f"   • Outside PPS CSV: {outside_path}")
    logger.info(f"   • Interactive heatmap: {heatmap_path}")


if __name__ == "__main__":
    main()
