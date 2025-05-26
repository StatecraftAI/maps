#!/usr/bin/env python3
"""
Supabase PostGIS Integration Demo

This script demonstrates how to use the Supabase integration with our
voter and household analysis pipelines. It shows setup, configuration,
and usage examples.

Setup Instructions:
1. Create a Supabase project at https://supabase.com
2. Enable PostGIS extension in your database:
   - Go to SQL Editor in Supabase dashboard
   - Run: CREATE EXTENSION IF NOT EXISTS postgis;
3. Set environment variables:
   export SUPABASE_DB_HOST="db.your-project-id.supabase.co"
   export SUPABASE_DB_PASSWORD="your-database-password"
4. Run this demo to test the connection

Benefits:
- Spatial indexing and PostGIS queries
- Auto-generated REST/GraphQL APIs via Supabase
- Real-time subscriptions for live data updates
- Easy frontend integration with Supabase client libraries
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from loguru import logger


def check_dependencies():
    """Check if required dependencies are installed."""
    logger.info("🔍 Checking dependencies...")

    try:
        import geopandas
        import psycopg2
        import sqlalchemy

        logger.success("✅ All required dependencies available")
        logger.info(f"   SQLAlchemy: {sqlalchemy.__version__}")
        logger.info(f"   psycopg2: {psycopg2.__version__}")
        logger.info(f"   GeoPandas: {geopandas.__version__}")
        return True
    except ImportError as e:
        logger.error(f"❌ Missing dependencies: {e}")
        logger.error("   Install with: pip install sqlalchemy psycopg2-binary")
        return False


def check_credentials():
    """Check if Supabase credentials are configured."""
    logger.info("🔐 Checking Supabase credentials...")

    host = os.getenv("SUPABASE_DB_HOST")
    password = os.getenv("SUPABASE_DB_PASSWORD")

    if host and password:
        logger.success("✅ Supabase credentials found in environment")
        logger.info(f"   Host: {host}")
        logger.info("   Password: ***")
        return True
    else:
        logger.warning("⚠️ Supabase credentials not found")
        logger.info("💡 Set environment variables:")
        logger.info('   export SUPABASE_DB_HOST="db.your-project-id.supabase.co"')
        logger.info('   export SUPABASE_DB_PASSWORD="your-database-password"')
        logger.info("   (Find these in Supabase Project > Settings > Database)")
        return False


def test_connection():
    """Test the Supabase PostGIS connection."""
    logger.info("🧪 Testing Supabase PostGIS connection...")

    try:
        from supabase_integration import SupabaseUploader

        # Initialize uploader
        uploader = SupabaseUploader()

        # Test connection
        if uploader.validate_connection():
            logger.success("🎉 Supabase PostGIS connection successful!")

            # List existing tables
            tables = uploader.list_tables()
            if tables:
                logger.info(f"📊 Found {len(tables)} existing tables:")
                for table in tables[:10]:  # Show first 10
                    info = uploader.get_table_info(table)
                    if info:
                        logger.info(f"   - {table}: {info['row_count']:,} rows")
                    else:
                        logger.info(f"   - {table}")
                if len(tables) > 10:
                    logger.info(f"   ... and {len(tables) - 10} more")
            else:
                logger.info("📊 No tables found (fresh database)")

            return True
        else:
            logger.error("❌ Connection test failed")
            return False

    except Exception as e:
        logger.error(f"❌ Connection test error: {e}")
        return False


def demo_upload():
    """Demonstrate uploading sample data to Supabase."""
    logger.info("📤 Demonstrating data upload...")

    try:
        import geopandas as gpd
        from shapely.geometry import Point

        # Create sample GeoDataFrame
        logger.debug("   Creating sample dataset...")
        data = {
            "id": [1, 2, 3],
            "name": ["Point A", "Point B", "Point C"],
            "value": [10.5, 25.3, 18.7],
            "category": ["Type 1", "Type 2", "Type 1"],
        }

        # Create points around Portland area
        geometries = [
            Point(-122.6765, 45.5152),  # Portland
            Point(-122.6612, 45.5200),  # Pearl District
            Point(-122.6587, 45.5165),  # Downtown
        ]

        sample_gdf = gpd.GeoDataFrame(data, geometry=geometries, crs="EPSG:4326")

        # Upload to Supabase
        from supabase_integration import SupabaseUploader

        uploader = SupabaseUploader()

        success = uploader.upload_geodataframe(
            sample_gdf,
            table_name="demo_points",
            description="Sample points for Supabase integration demo",
        )

        if success:
            logger.success("✅ Demo upload successful!")
            logger.info("💡 You can now:")
            logger.info("   - Query this data via Supabase API")
            logger.info("   - Use PostGIS spatial functions")
            logger.info("   - Set up real-time subscriptions")
            return True
        else:
            logger.error("❌ Demo upload failed")
            return False

    except Exception as e:
        logger.error(f"❌ Demo upload error: {e}")
        return False


def show_usage_examples():
    """Show example SQL queries and usage patterns."""
    logger.info("📚 Usage Examples:")

    print("""
🔍 PostGIS Spatial Queries:
```sql
-- Find points within 1km of a location
SELECT name, value, ST_Distance(geometry::geography, ST_Point(-122.6765, 45.5152)::geography) as distance_m
FROM demo_points
WHERE ST_DWithin(geometry::geography, ST_Point(-122.6765, 45.5152)::geography, 1000);

-- Get bounding box of all features
SELECT ST_Extent(geometry) FROM voter_hexagons;

-- Count features by area
SELECT COUNT(*) as feature_count,
       SUM(total_voters) as total_voters
FROM voter_hexagons
WHERE ST_Intersects(geometry, ST_MakeEnvelope(-122.7, 45.4, -122.6, 45.6, 4326));
```

🌐 Supabase API Examples:
```javascript
// Fetch voter hexagons with spatial filter
const { data } = await supabase
  .from('voter_hexagons')
  .select('*')
  .gte('total_voters', 100);

// Real-time subscription to data changes
const subscription = supabase
  .from('household_demographics_pps')
  .on('*', (payload) => console.log('Data updated:', payload))
  .subscribe();
```

🗺️ Frontend Integration:
```javascript
// Fetch data for map visualization
const { data: hexagons } = await supabase
  .from('voter_hexagons')
  .select('hex_id, total_voters, pps_voters, geometry');

// Use with mapping libraries (Leaflet, Mapbox, etc.)
const geoJsonData = {
  type: 'FeatureCollection',
  features: hexagons.map(hex => ({
    type: 'Feature',
    geometry: hex.geometry,
    properties: { ...hex }
  }))
};
```
""")


def main():
    """Main demo function."""
    logger.info("🚀 Supabase PostGIS Integration Demo")
    logger.info("=" * 50)

    # Check setup
    if not check_dependencies():
        return

    if not check_credentials():
        logger.info("\n💡 Setup Instructions:")
        logger.info("1. Create Supabase project: https://supabase.com")
        logger.info("2. Enable PostGIS: CREATE EXTENSION IF NOT EXISTS postgis;")
        logger.info("3. Set environment variables (see above)")
        logger.info("4. Re-run this demo")
        return

    # Test connection
    if not test_connection():
        return

    # Demo upload
    logger.info("\n" + "=" * 50)
    demo_result = demo_upload()

    # Show usage examples
    logger.info("\n" + "=" * 50)
    show_usage_examples()

    # Summary
    logger.info("\n" + "=" * 50)
    if demo_result:
        logger.success("🎉 Demo completed successfully!")
        logger.info("✅ Your Supabase PostGIS integration is ready")
        logger.info("✅ Run voter analysis: python map_voters.py")
        logger.info("✅ Run household analysis: python map_households.py")
        logger.info("✅ Both will now automatically upload to Supabase")
    else:
        logger.warning("⚠️ Demo completed with issues")
        logger.info("💡 Check your Supabase configuration and try again")


if __name__ == "__main__":
    main()
