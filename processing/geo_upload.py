#!/usr/bin/env python3
"""
geo_upload.py - MVP Geospatial Upload Tool

The only file that matters for the MVP.

Usage: 
    python geo_upload.py data.geojson table_name
    python geo_upload.py data.shp election_results
    python geo_upload.py data.gpkg voter_precincts

Result: Data in your maps database. Done.
"""

import sys
import os
from pathlib import Path
import geopandas as gpd
from loguru import logger

# Add parent directory for config
sys.path.append(str(Path(__file__).parent.parent))
from ops.config_loader import Config

# Import existing Supabase upload (reuse what works)
try:
    from ops.supabase_integration import SupabaseUploader
    SUPABASE_AVAILABLE = True
except ImportError:
    logger.error("❌ Supabase integration not available")
    SUPABASE_AVAILABLE = False


def upload_geo_file(file_path: str, table_name: str) -> bool:
    """Upload any geospatial file to maps database. That's it."""
    
    file_path = Path(file_path)
    
    # Check file exists
    if not file_path.exists():
        logger.error(f"❌ File not found: {file_path}")
        return False
    
    logger.info(f"🗺️ Loading {file_path}")
    
    try:
        # Load any geo format (geopandas handles everything)
        gdf = gpd.read_file(file_path)
        logger.info(f"  ✅ Loaded {len(gdf):,} features")
        
        # Standardize for web (WGS84 is web standard)
        if gdf.crs is None:
            logger.warning("  ⚠️ No CRS found, assuming WGS84")
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_epsg() != 4326:
            logger.info(f"  🔄 Converting from {gdf.crs} to WGS84")
            gdf = gdf.to_crs("EPSG:4326")
        
        # Fix basic issues
        original_count = len(gdf)
        gdf = gdf[gdf.geometry.notna()]
        if len(gdf) < original_count:
            logger.info(f"  🧹 Removed {original_count - len(gdf)} features with invalid geometry")
        
        # Upload to Supabase
        if not SUPABASE_AVAILABLE:
            logger.error("❌ Supabase not available - install dependencies")
            return False
        
        config = Config()
        uploader = SupabaseUploader(config)
        
        logger.info(f"🚀 Uploading to table '{table_name}'...")
        success = uploader.upload_geodataframe(
            gdf, 
            table_name=table_name, 
            description=f"Uploaded from {file_path.name}"
        )
        
        if success:
            logger.success(f"✅ Success: {len(gdf):,} features → {table_name}")
            return True
        else:
            logger.error(f"❌ Upload failed")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to process {file_path}: {e}")
        return False


def main():
    """CLI interface"""
    if len(sys.argv) != 3:
        print("Usage: python geo_upload.py <file_path> <table_name>")
        print("")
        print("Examples:")
        print("  python geo_upload.py data.geojson election_results")
        print("  python geo_upload.py boundaries.shp voter_precincts")
        print("  python geo_upload.py census.gpkg demographics")
        sys.exit(1)
    
    file_path, table_name = sys.argv[1], sys.argv[2]
    
    logger.info("🗺️ MVP Geospatial Upload Tool")
    logger.info("=" * 40)
    
    if upload_geo_file(file_path, table_name):
        logger.success(f"🎉 Done! Data available in maps database as '{table_name}'")
        sys.exit(0)
    else:
        logger.error("💥 Upload failed")
        sys.exit(1)


if __name__ == "__main__":
    main() 