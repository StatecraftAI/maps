#!/usr/bin/env python3
"""
Example usage of the updated Supabase integration following platform patterns.

This demonstrates how to use both the SupabaseDatabase class for standard operations
and the SupabaseUploader for spatial data uploads, following the same patterns
used in the platform component.
"""

import sys
from pathlib import Path

# Add the parent directory to the path so we can import from ops
sys.path.append(str(Path(__file__).parent.parent))

import geopandas as gpd
import pandas as pd
from loguru import logger

from ops.supabase_integration import SupabaseDatabase, SupabaseUploader, get_supabase_database
from ops.repositories import SpatialRepository


def example_standard_database_operations():
    """Example of standard database operations using SupabaseDatabase."""
    logger.info("🔍 Example: Standard Database Operations")
    
    try:
        # Get database instance (following platform pattern)
        db = get_supabase_database()
        
        # Example: Select records from a table
        logger.info("   📊 Selecting records from voter_hexagons...")
        records = db.select(
            table="voter_hexagons",
            columns=["id", "state", "voter_density", "geometry"],
            filters={"state": "CA"},
            limit=10,
            order_by="voter_density desc"
        )
        logger.info(f"   ✅ Found {len(records)} records")
        
        # Example: Insert a new record
        logger.info("   📝 Inserting a test record...")
        new_record = {
            "state": "TX",
            "voter_density": 150.5,
            "metadata": {"source": "example", "created_by": "script"}
        }
        
        inserted = db.insert(
            table="test_features",
            data=new_record,
            returning="representation"
        )
        logger.info(f"   ✅ Inserted record: {inserted}")
        
        # Example: Update records
        if inserted:
            record_id = inserted[0].get("id")
            if record_id:
                logger.info(f"   🔄 Updating record {record_id}...")
                updated = db.update(
                    table="test_features",
                    data={"voter_density": 175.0},
                    filters={"id": record_id}
                )
                logger.info(f"   ✅ Updated record: {updated}")
        
    except Exception as e:
        logger.error(f"   ❌ Database operations failed: {e}")


def example_repository_pattern():
    """Example of using the repository pattern for spatial operations."""
    logger.info("🗺️ Example: Repository Pattern for Spatial Data")
    
    try:
        # Initialize database and repository
        db = get_supabase_database()
        spatial_repo = SpatialRepository(db)
        
        # Example: Get features by state
        logger.info("   🔍 Getting voter density hexagons for California...")
        ca_hexagons = spatial_repo.get_voter_density_hexagons(
            state="CA",
            min_density=100.0,
            limit=5
        )
        logger.info(f"   ✅ Found {len(ca_hexagons)} high-density hexagons in CA")
        
        # Example: Get features by bounds
        logger.info("   📍 Getting features within bounding box...")
        features = spatial_repo.get_features_by_bounds(
            table="voter_hexagons",
            bounds=[-124.0, 32.0, -114.0, 42.0],  # Rough CA bounds
            additional_filters={"state": "CA"}
        )
        logger.info(f"   ✅ Found {len(features)} features in bounds")
        
    except Exception as e:
        logger.error(f"   ❌ Repository operations failed: {e}")


def example_spatial_data_upload():
    """Example of uploading spatial data using SupabaseUploader."""
    logger.info("📤 Example: Spatial Data Upload")
    
    try:
        # Create sample GeoDataFrame
        logger.info("   🔧 Creating sample GeoDataFrame...")
        
        # Create some sample points
        import numpy as np
        from shapely.geometry import Point
        
        # Generate random points in California
        np.random.seed(42)
        lons = np.random.uniform(-124.0, -114.0, 10)
        lats = np.random.uniform(32.0, 42.0, 10)
        
        gdf = gpd.GeoDataFrame({
            'id': range(10),
            'state': 'CA',
            'voter_density': np.random.uniform(50, 200, 10),
            'population': np.random.randint(1000, 10000, 10),
            'geometry': [Point(lon, lat) for lon, lat in zip(lons, lats)]
        }, crs='EPSG:4326')
        
        logger.info(f"   ✅ Created GeoDataFrame with {len(gdf)} features")
        
        # Upload to Supabase
        logger.info("   📤 Uploading to Supabase...")
        uploader = SupabaseUploader()
        
        success = uploader.upload_geodataframe(
            gdf=gdf,
            table_name="example_voter_points",
            description="Example voter density points for testing",
            if_exists="replace"
        )
        
        if success:
            logger.success("   🎉 Upload completed successfully!")
        else:
            logger.error("   ❌ Upload failed")
            
    except Exception as e:
        logger.error(f"   ❌ Spatial upload failed: {e}")


def main():
    """Run all examples."""
    logger.info("🚀 Starting Supabase Integration Examples")
    logger.info("=" * 50)
    
    # Run examples
    example_standard_database_operations()
    logger.info("")
    
    example_repository_pattern()
    logger.info("")
    
    example_spatial_data_upload()
    logger.info("")
    
    logger.success("✅ All examples completed!")


if __name__ == "__main__":
    main() 