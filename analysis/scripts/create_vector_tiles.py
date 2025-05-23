#!/usr/bin/env python3
"""
Vector Tile Creation Script for 2025 Zone 1 Election Data

This script processes the election results GeoJSON and creates vector tiles
for local use or manual upload to mapping services.

Dependencies:
- tippecanoe (for tile generation)

Usage:
    python create_vector_tiles.py
"""

import pathlib
import json
import subprocess
import os
import sys
import sqlite3
import time
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np

# Directories
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
ANALYSIS_DIR = SCRIPT_DIR.parent
GEOSPATIAL_DIR = ANALYSIS_DIR / 'geospatial'
TILES_DIR = ANALYSIS_DIR / 'tiles'

# Create tiles directory
TILES_DIR.mkdir(exist_ok=True)

# File paths
INPUT_GEOJSON = GEOSPATIAL_DIR / '2025_election_zone1_results.geojson'
PROCESSED_GEOJSON = GEOSPATIAL_DIR / '2025_election_zone1_results_processed.geojson'
TILES_OUTPUT = TILES_DIR / '2025_election_zone1_tiles.mbtiles'

# Tile configuration
TILESET_NAME = "2025 Zone 1 Election Results"
TILESET_DESCRIPTION = "Interactive map of 2025 Zone 1 election results with voter registration data"

def check_dependencies():
    """Check if required tools are installed."""
    print("🔧 Checking dependencies...")
    
    # Check tippecanoe
    try:
        result = subprocess.run(['tippecanoe', '--version'], 
                              capture_output=True, text=True, check=True)
        print(f"  ✓ tippecanoe: {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("  ❌ tippecanoe not found. Install with:")
        print("     macOS: brew install tippecanoe")
        print("     Ubuntu: sudo apt install gdal-bin")
        return False

def process_geojson():
    """Process the election GeoJSON to optimize it for vector tiles."""
    print("📊 Processing election GeoJSON...")
    
    if not INPUT_GEOJSON.exists():
        print(f"  ❌ Input file not found: {INPUT_GEOJSON}")
        return False
    
    # Load the GeoJSON (should already be in WGS84 from map_election_results.py)
    with open(INPUT_GEOJSON, 'r') as f:
        data = json.load(f)
    
    print(f"  ✓ Loaded {len(data['features'])} features")
    
    # Verify CRS is WGS84
    crs = data.get('crs', {})
    if crs:
        crs_name = crs.get('properties', {}).get('name', '')
        print(f"  📍 Input CRS: {crs_name}")
        if 'CRS84' in crs_name or 'EPSG:4326' in str(crs):
            print("  ✓ Data is already in WGS84, no reprojection needed")
        else:
            print(f"  ⚠️ Unexpected CRS: {crs_name}")
    else:
        print("  📍 No CRS specified, assuming WGS84")
    
    # Validate coordinate sample
    if data['features']:
        sample_feature = data['features'][0]
        if sample_feature.get('geometry'):
            coords = sample_feature['geometry'].get('coordinates', [])
            if coords and len(coords) > 0:
                # Get first coordinate pair
                first_coord = coords[0]
                while isinstance(first_coord, list) and len(first_coord) > 0 and isinstance(first_coord[0], list):
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
    
    for feature in data['features']:
        props = feature['properties']
        
        # Create clean properties dict (most cleaning done in map_election_results.py)
        # Just ensure data types are JSON-safe
        clean_props = {}
        for key, value in props.items():
            if value is None or (isinstance(value, float) and (pd.isna(value) or not np.isfinite(value))):
                continue
            
            # Ensure JSON-safe types
            if isinstance(value, (str, int, bool)):
                clean_props[key] = value
            elif isinstance(value, float):
                clean_props[key] = round(value, 6)  # Limit precision for file size
            else:
                clean_props[key] = str(value)
        
        # Create processed feature
        processed_feature = {
            'type': 'Feature',
            'properties': clean_props,
            'geometry': feature['geometry']
        }
        
        processed_features.append(processed_feature)
    
    # Create processed GeoJSON with WGS84 CRS
    processed_data = {
        'type': 'FeatureCollection',
        'crs': {
            'type': 'name',
            'properties': {
                'name': 'urn:ogc:def:crs:OGC:1.3:CRS84'
            }
        },
        'features': processed_features
    }
    
    # Add metadata if it exists in original
    if 'metadata' in data:
        processed_data['metadata'] = data['metadata']
    
    # Save processed GeoJSON
    with open(PROCESSED_GEOJSON, 'w') as f:
        json.dump(processed_data, f, separators=(',', ':'))
    
    print(f"  ✓ Processed GeoJSON saved: {PROCESSED_GEOJSON}")
    print(f"  📊 {len(processed_features)} features ready for tiling")
    
    return True

def create_vector_tiles():
    """Generate vector tiles using tippecanoe."""
    print("🗂️ Creating vector tiles with tippecanoe...")
    
    # Tippecanoe command with optimized settings for election data
    cmd = [
        'tippecanoe',
        '-o', str(TILES_OUTPUT),
        '--name', TILESET_NAME,
        '--attribution', 'Multnomah County Elections Division',
        '--minimum-zoom', '9',  # Start at a reasonable zoom level
        '--maximum-zoom', '13',  # Reduced max zoom to avoid conflicts
        '--base-zoom', '10',    # Set a base zoom level
        '--drop-densest-as-needed',  # Simplify at lower zooms
        '--drop-fraction-as-needed',  # Drop features as needed
        '--buffer', '64',  # Standard buffer size
        '--simplification', '10',  # More aggressive simplification
        '--layer', 'election_results',  # Layer name for web mapping
        '--force',  # Overwrite existing tiles
        '--generate-ids',  # Generate feature IDs
        '--detect-shared-borders',  # Optimize shared borders
        str(PROCESSED_GEOJSON)
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"  ✓ Vector tiles created: {TILES_OUTPUT}")
        
        # Show tile statistics
        file_size = TILES_OUTPUT.stat().st_size / (1024 * 1024)  # MB
        print(f"  📦 Tile size: {file_size:.1f} MB")
        
        # Generate tilestats to improve performance (tippecanoe 1.21.0+)
        print("  📊 Generating tilestats for optimization...")
        tilestats_cmd = [
            'tile-join',
            '-o', str(TILES_OUTPUT.with_suffix('.optimized.mbtiles')),
            str(TILES_OUTPUT)
        ]
        
        try:
            subprocess.run(tilestats_cmd, capture_output=True, text=True, check=True)
            # Replace original with optimized version
            TILES_OUTPUT.unlink()
            TILES_OUTPUT.with_suffix('.optimized.mbtiles').rename(TILES_OUTPUT)
            print("  ✓ Tilestats generated and tiles optimized")
        except subprocess.CalledProcessError as e:
            print(f"  ⚠️  Warning: Could not optimize tiles with tile-join: {e}")
            print("  📝 Original tiles are still usable")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"  ❌ Tippecanoe failed: {e}")
        print(f"  stderr: {e.stderr}")
        return False

def validate_mbtiles():
    """Validate the created MBTiles file and show metadata."""
    print("🔍 Validating created tiles...")
    
    if not TILES_OUTPUT.exists():
        print("  ❌ Tiles file not found")
        return False
    
    try:
        # Connect to the SQLite database inside the MBTiles file
        conn = sqlite3.connect(str(TILES_OUTPUT))
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
        
        print(f"  ✓ Tiles file validated successfully")
        print(f"  📊 Metadata:")
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
        if 'attribution' in metadata:
            print(f"     • Attribution: {metadata['attribution']}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error validating tiles: {e}")
        return False

def main():
    """Main execution function."""
    print("🗺️ Creating Vector Tiles for 2025 Zone 1 Election Data")
    print("=" * 60)
    
    # Check dependencies
    if not check_dependencies():
        print("\n❌ tippecanoe not installed. Please install it first:")
        print("   macOS: brew install tippecanoe")
        print("   Ubuntu: sudo apt install gdal-bin")
        sys.exit(1)
    
    # Process GeoJSON
    if not process_geojson():
        print("\n❌ Failed to process GeoJSON")
        sys.exit(1)
    
    # Create vector tiles
    if not create_vector_tiles():
        print("\n❌ Failed to create vector tiles")
        sys.exit(1)
    
    # Validate the created tiles
    if not validate_mbtiles():
        print("\n❌ Tile validation failed")
        sys.exit(1)
    
    print(f"\n✅ Vector tiles created successfully!")
    print(f"📦 Output file: {TILES_OUTPUT}")
    print(f"📊 File size: {TILES_OUTPUT.stat().st_size / (1024 * 1024):.1f} MB")
    
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