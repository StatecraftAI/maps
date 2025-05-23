#!/usr/bin/env python3
"""
Vector Tile Creation Script

This script processes the election results GeoJSON and creates vector tiles
for local use or manual upload to mapping services.

Dependencies:
- tippecanoe (for tile generation)

Usage:
    python create_vector_tiles.py
"""

import json
import sqlite3
import subprocess
import sys

import numpy as np
import pandas as pd
from config_loader import Config


def check_dependencies():
    """Check if required tools are installed."""
    print("🔧 Checking dependencies...")

    # Check tippecanoe
    try:
        result = subprocess.run(
            ["tippecanoe", "--version"], capture_output=True, text=True, check=True
        )
        print(f"  ✓ tippecanoe: {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("  ❌ tippecanoe not found. Install with:")
        print("     macOS: brew install tippecanoe")
        print("     Ubuntu: sudo apt install gdal-bin")
        return False


def process_geojson(config: Config):
    """Process the election GeoJSON to optimize it for vector tiles."""
    print("📊 Processing election GeoJSON...")

    # Get file paths from config
    input_geojson = config.get_web_geojson_path()
    processed_geojson = config.get_processed_geojson_path()

    if not input_geojson.exists():
        print(f"  ❌ Input file not found: {input_geojson}")
        return False

    # Load the GeoJSON (should already be in WGS84 from map_election_results.py)
    with open(input_geojson, "r") as f:
        data = json.load(f)

    print(f"  ✓ Loaded {len(data['features'])} features")

    # Verify CRS is WGS84
    crs = data.get("crs", {})
    if crs:
        crs_name = crs.get("properties", {}).get("name", "")
        print(f"  📍 Input CRS: {crs_name}")
        if "CRS84" in crs_name or "EPSG:4326" in str(crs):
            print("  ✓ Data is already in WGS84, no reprojection needed")
        else:
            print(f"  ⚠️ Unexpected CRS: {crs_name}")
    else:
        print("  📍 No CRS specified, assuming WGS84")

    # Validate coordinate sample
    if data["features"]:
        sample_feature = data["features"][0]
        if sample_feature.get("geometry"):
            coords = sample_feature["geometry"].get("coordinates", [])
            if coords and len(coords) > 0:
                # Get first coordinate pair
                first_coord = coords[0]
                while (
                    isinstance(first_coord, list)
                    and len(first_coord) > 0
                    and isinstance(first_coord[0], list)
                ):
                    first_coord = first_coord[0]

                if isinstance(first_coord, list) and len(first_coord) >= 2:
                    lon, lat = first_coord[0], first_coord[1]
                    if isinstance(lon, (int, float)) and isinstance(lat, (int, float)):
                        if -180 <= lon <= 180 and -90 <= lat <= 90:
                            print(f"  ✓ Sample coordinates valid: lon={lon:.6f}, lat={lat:.6f}")
                        else:
                            print(f"  ⚠️ Sample coordinates may be invalid: lon={lon}, lat={lat}")

    # Process features to optimize for web display (properties should already be optimized)
    processed_features = []

    # Get precision setting from config
    precision = config.get_system_setting("precision_decimals")

    for feature in data["features"]:
        props = feature["properties"]

        # Create clean properties dict (most cleaning done in map_election_results.py)
        # Just ensure data types are JSON-safe
        clean_props = {}
        for key, value in props.items():
            if value is None or (
                isinstance(value, float) and (pd.isna(value) or not np.isfinite(value))
            ):
                continue

            # Ensure JSON-safe types
            if isinstance(value, (str, int, bool)):
                clean_props[key] = value
            elif isinstance(value, float):
                clean_props[key] = round(value, precision)  # Use config precision
            else:
                clean_props[key] = str(value)

        # Create processed feature
        processed_feature = {
            "type": "Feature",
            "properties": clean_props,
            "geometry": feature["geometry"],
        }

        processed_features.append(processed_feature)

    # Create processed GeoJSON with WGS84 CRS
    processed_data = {
        "type": "FeatureCollection",
        "crs": {
            "type": "name",
            "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"},
        },
        "features": processed_features,
    }

    # Add metadata if it exists in original
    if "metadata" in data:
        processed_data["metadata"] = data["metadata"]

    # Save processed GeoJSON
    with open(processed_geojson, "w") as f:
        json.dump(processed_data, f, separators=(",", ":"))

    print(f"  ✓ Processed GeoJSON saved: {processed_geojson}")
    print(f"  📊 {len(processed_features)} features ready for tiling")

    return True


def create_vector_tiles(config: Config):
    """Generate vector tiles using tippecanoe."""
    print("🗂️ Creating vector tiles with tippecanoe...")

    # Get file paths and settings from config
    config.get_web_geojson_path()
    processed_geojson = config.get_processed_geojson_path()
    tiles_output = config.get_mbtiles_path()

    project_name = config.get("project_name")
    attribution = config.get_metadata("attribution")

    # Get visualization settings for tile generation
    min_zoom = config.get_visualization_setting("min_zoom")
    max_zoom = config.get_visualization_setting("max_zoom")
    base_zoom = config.get_visualization_setting("base_zoom")
    buffer_size = config.get_visualization_setting("buffer_size")
    simplification = config.get_visualization_setting("simplification")

    # Tippecanoe command with optimized settings for election data
    cmd = [
        "tippecanoe",
        "-o",
        str(tiles_output),
        "--name",
        project_name,
        "--attribution",
        attribution,
        "--minimum-zoom",
        str(min_zoom),
        "--maximum-zoom",
        str(max_zoom),
        "--base-zoom",
        str(base_zoom),
        "--drop-densest-as-needed",  # Simplify at lower zooms
        "--drop-fraction-as-needed",  # Drop features as needed
        "--buffer",
        str(buffer_size),
        "--simplification",
        str(simplification),
        "--layer",
        "election_results",  # Layer name for web mapping
        "--force",  # Overwrite existing tiles
        "--generate-ids",  # Generate feature IDs
        "--detect-shared-borders",  # Optimize shared borders
        str(processed_geojson),
    ]

    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"  ✓ Vector tiles created: {tiles_output}")

        # Show tile statistics
        file_size = tiles_output.stat().st_size / (1024 * 1024)  # MB
        print(f"  📦 Tile size: {file_size:.1f} MB")

        # Generate tilestats to improve performance (tippecanoe 1.21.0+)
        print("  📊 Generating tilestats for optimization...")
        optimized_path = tiles_output.with_suffix(".optimized.mbtiles")
        tilestats_cmd = [
            "tile-join",
            "-o",
            str(optimized_path),
            str(tiles_output),
        ]

        try:
            subprocess.run(tilestats_cmd, capture_output=True, text=True, check=True)
            # Replace original with optimized version
            tiles_output.unlink()
            optimized_path.rename(tiles_output)
            print("  ✓ Tilestats generated and tiles optimized")
        except subprocess.CalledProcessError as e:
            print(f"  ⚠️  Warning: Could not optimize tiles with tile-join: {e}")
            print("  📝 Original tiles are still usable")

        return True

    except subprocess.CalledProcessError as e:
        print(f"  ❌ Tippecanoe failed: {e}")
        print(f"  stderr: {e.stderr}")
        return False


def validate_mbtiles(config: Config):
    """Validate the created MBTiles file and show metadata."""
    print("🔍 Validating created tiles...")

    tiles_output = config.get_mbtiles_path()

    if not tiles_output.exists():
        print("  ❌ Tiles file not found")
        return False

    try:
        # Connect to the SQLite database inside the MBTiles file
        conn = sqlite3.connect(str(tiles_output))
        cursor = conn.cursor()

        # Get metadata
        cursor.execute("SELECT name, value FROM metadata")
        metadata = dict(cursor.fetchall())

        # Get tile count
        cursor.execute("SELECT COUNT(*) FROM tiles")
        tile_count = cursor.fetchone()[0]

        # Get zoom levels
        cursor.execute("SELECT MIN(zoom_level), MAX(zoom_level) FROM tiles")
        min_zoom, max_zoom = cursor.fetchone()

        conn.close()

        print("  ✓ Tiles file validated successfully")
        print("  📊 Metadata:")
        print(f"     • Name: {metadata.get('name', 'N/A')}")
        print(f"     • Format: {metadata.get('format', 'N/A')}")
        print(f"     • Min Zoom: {min_zoom}")
        print(f"     • Max Zoom: {max_zoom}")
        print(f"     • Total Tiles: {tile_count:,}")

        # Check for zoom level conflicts
        if min_zoom is not None and max_zoom is not None:
            if min_zoom > max_zoom:
                print(f"  ❌ ERROR: minzoom ({min_zoom}) > maxzoom ({max_zoom})")
                return False
            else:
                print(f"  ✓ Zoom levels valid: {min_zoom} ≤ {max_zoom}")

        # Show attribution if present
        if "attribution" in metadata:
            print(f"     • Attribution: {metadata['attribution']}")

        return True

    except Exception as e:
        print(f"  ❌ Error validating tiles: {e}")
        return False


def main():
    """Main execution function."""
    print("🗺️ Creating Vector Tiles")
    print("=" * 60)

    # Load configuration
    try:
        config = Config()
        print(f"📋 Project: {config.get('project_name')}")
        print(f"📋 Description: {config.get('description')}")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Make sure config.yaml exists in the analysis directory")
        sys.exit(1)

    # Check dependencies
    if not check_dependencies():
        print("\n❌ tippecanoe not installed. Please install it first:")
        print("   macOS: brew install tippecanoe")
        print("   Ubuntu: sudo apt install gdal-bin")
        sys.exit(1)

    # Process GeoJSON
    if not process_geojson(config):
        print("\n❌ Failed to process GeoJSON")
        sys.exit(1)

    # Create vector tiles
    if not create_vector_tiles(config):
        print("\n❌ Failed to create vector tiles")
        sys.exit(1)

    # Validate the created tiles
    if not validate_mbtiles(config):
        print("\n❌ Tile validation failed")
        sys.exit(1)

    tiles_output = config.get_mbtiles_path()
    print("\n✅ Vector tiles created successfully!")
    print(f"📦 Output file: {tiles_output}")
    print(f"📊 File size: {tiles_output.stat().st_size / (1024 * 1024):.1f} MB")

    print("\n🎯 Next steps:")
    print("1. Use the .mbtiles file with a local tile server for development")
    print("2. Manually upload to your preferred mapping service")
    print("3. Or serve directly with TileServer GL or similar")

    print("\n💡 Usage options:")
    print("• TileServer GL: tileserver-gl-light your-tiles.mbtiles")
    print("• Mapbox Studio: Upload via web interface")
    print("• Self-hosted: Use with any MBTiles-compatible server")


if __name__ == "__main__":
    main()
