#!/usr/bin/env python3
"""
Supabase Round-trip Test

This script tests the full round-trip functionality by:
1. Loading spatial data from Supabase PostGIS database
2. Creating visualizations to verify data integrity
3. Confirming that the upload/download cycle works correctly

This validates that our Supabase integration is working properly.
"""

import sys
import time
from pathlib import Path
from typing import Optional

import folium
import geopandas as gpd
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))
from ops import Config

# Import Supabase integration
try:
    from ops.supabase_integration import SupabaseUploader
    SUPABASE_AVAILABLE = True
except ImportError as e:
    logger.error(f"❌ Supabase integration not available: {e}")
    SUPABASE_AVAILABLE = False
    sys.exit(1)


def load_data_from_supabase(uploader: SupabaseUploader, table_name: str) -> Optional[gpd.GeoDataFrame]:
    """
    Load spatial data from Supabase table.

    Args:
        uploader: SupabaseUploader instance
        table_name: Name of table to load

    Returns:
        GeoDataFrame with data from Supabase or None if failed
    """
    logger.info(f"📥 Loading data from Supabase table: {table_name}")

    try:
        # Validate connection
        if not uploader.validate_connection():
            logger.error("❌ Cannot connect to Supabase")
            return None

        # Check if table exists
        if not uploader.table_exists(table_name):
            logger.error(f"❌ Table '{table_name}' does not exist in Supabase")
            available_tables = uploader.list_tables()
            logger.info(f"   Available tables: {available_tables}")
            return None

        # Load data using geopandas
        logger.debug(f"   🔍 Reading table '{table_name}' from PostGIS...")

        gdf = gpd.read_postgis(
            sql=f"SELECT * FROM {table_name}",
            con=uploader.engine,
            geom_col='geometry'
        )

        logger.success(f"   ✅ Loaded {len(gdf):,} records from {table_name}")
        logger.info(f"      📊 Columns: {list(gdf.columns)}")
        logger.info(f"      🗺️ CRS: {gdf.crs}")

        # Validate geometry
        if 'geometry' in gdf.columns:
            valid_geom = gdf.geometry.is_valid.sum()
            logger.info(f"      ✓ Valid geometries: {valid_geom}/{len(gdf)}")

        return gdf

    except Exception as e:
        logger.error(f"❌ Failed to load data from {table_name}: {e}")
        logger.trace("Detailed error:")
        import traceback
        logger.trace(traceback.format_exc())
        return None


def create_test_visualization(gdf: gpd.GeoDataFrame, output_path: Path, title: str) -> bool:
    """
    Create a simple test visualization from Supabase data.

    Args:
        gdf: GeoDataFrame loaded from Supabase
        output_path: Output HTML file path
        title: Map title

    Returns:
        Success status
    """
    logger.info(f"🗺️ Creating test visualization: {title}")

    try:
        # Calculate map center
        bounds = gdf.total_bounds
        center_lat = (bounds[1] + bounds[3]) / 2
        center_lon = (bounds[0] + bounds[2]) / 2
        center = [center_lat, center_lon]

        logger.debug(f"   📍 Map center: {center[0]:.4f}, {center[1]:.4f}")

        # Create base map
        m = folium.Map(
            location=center,
            zoom_start=10,
            tiles="CartoDB Positron"
        )

        # Add title
        title_html = f"""
        <h3 align="center" style="font-size:20px; color: #333333; margin-top:10px;">
        <b>{title}</b><br>
        <span style="font-size:14px;">Data loaded from Supabase PostGIS</span><br>
        <span style="font-size:12px; color: #666;">Records: {len(gdf):,} | Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}</span>
        </h3>
        """
        m.get_root().html.add_child(folium.Element(title_html))

        # Determine visualization strategy based on data
        if 'total_voters' in gdf.columns:
            # Voter data - use choropleth
            logger.debug("   📊 Creating voter density choropleth...")

            folium.Choropleth(
                geo_data=gdf,
                name="Voter Density",
                data=gdf,
                columns=['hex_id' if 'hex_id' in gdf.columns else gdf.index, 'total_voters'],
                key_on="feature.properties.hex_id" if 'hex_id' in gdf.columns else "feature.id",
                fill_color="YlOrRd",
                fill_opacity=0.7,
                line_opacity=0.3,
                legend_name="Total Voters",
            ).add_to(m)

            # Add tooltips
            tooltip_fields = ['total_voters', 'pps_voters', 'pps_voter_pct', 'voter_density']
            tooltip_aliases = ['Total Voters:', 'PPS Voters:', 'PPS %:', 'Density (per km²):']

        elif 'total_households' in gdf.columns:
            # Household data - use choropleth
            logger.debug("   🏠 Creating household demographics choropleth...")

            folium.Choropleth(
                geo_data=gdf,
                name="Households without Minors",
                data=gdf,
                columns=[gdf.index, 'pct_households_no_minors'],
                key_on="feature.id",
                fill_color="YlGnBu",
                fill_opacity=0.7,
                line_opacity=0.3,
                legend_name="% Households without Minors",
            ).add_to(m)

            # Add tooltips
            tooltip_fields = ['total_households', 'households_no_minors', 'pct_households_no_minors', 'household_density']
            tooltip_aliases = ['Total Households:', 'HH without Minors:', '% No Minors:', 'Density (per km²):']

        else:
            # Generic data - simple display
            logger.debug("   📍 Creating generic feature display...")
            tooltip_fields = [col for col in gdf.columns if col != 'geometry'][:5]  # First 5 non-geometry columns
            tooltip_aliases = [f"{col}:" for col in tooltip_fields]

        # Add interactive tooltips
        folium.GeoJson(
            gdf.__geo_interface__,
            name="Feature Details",
            style_function=lambda feature: {
                "color": "#333333",
                "weight": 1,
                "fillOpacity": 0,
                "opacity": 0.7,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=tooltip_fields,
                aliases=tooltip_aliases,
                localize=True,
                sticky=False,
                labels=True,
                style="""
                    background-color: white;
                    border: 2px solid #333333;
                    border-radius: 5px;
                    box-shadow: 3px 3px 10px rgba(0,0,0,0.3);
                    padding: 10px;
                    font-family: Arial, sans-serif;
                    font-size: 12px;
                """,
            ),
        ).add_to(m)

        # Add layer control
        folium.LayerControl(collapsed=False).add_to(m)

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save map
        m.save(output_path)
        logger.success(f"   ✅ Test visualization saved: {output_path}")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to create visualization: {e}")
        logger.trace("Detailed visualization error:")
        import traceback
        logger.trace(traceback.format_exc())
        return False


def test_table_info(uploader: SupabaseUploader, table_name: str) -> None:
    """
    Display information about a Supabase table.

    Args:
        uploader: SupabaseUploader instance
        table_name: Name of table to inspect
    """
    logger.info(f"📊 Getting table information: {table_name}")

    try:
        info = uploader.get_table_info(table_name)
        if info:
            logger.info(f"   📍 Rows: {info['row_count']:,}")
            if info.get('bounds'):
                bounds = info['bounds']
                logger.info(f"   🗺️ Spatial extent: [{bounds[0]:.4f}, {bounds[1]:.4f}, {bounds[2]:.4f}, {bounds[3]:.4f}]")
            else:
                logger.info("   🗺️ No spatial extent available")
        else:
            logger.warning(f"   ⚠️ Could not get info for table {table_name}")

    except Exception as e:
        logger.error(f"❌ Failed to get table info: {e}")


def main() -> None:
    """Main test function."""
    logger.info("🧪 Supabase Round-trip Test")
    logger.info("=" * 50)

    # Load configuration
    try:
        config = Config()
        logger.info(f"📋 Project: {config.get('project_name')}")
    except Exception as e:
        logger.critical(f"❌ Configuration error: {e}")
        sys.exit(1)

    # Initialize Supabase uploader
    try:
        uploader = SupabaseUploader(config)
        logger.success("✅ Supabase uploader initialized")
    except Exception as e:
        logger.critical(f"❌ Failed to initialize Supabase uploader: {e}")
        sys.exit(1)

    # List available tables
    logger.info("📋 Available tables in Supabase:")
    tables = uploader.list_tables()
    for table in tables:
        logger.info(f"   📤 {table}")

    if not tables:
        logger.warning("⚠️ No tables found in Supabase database")
        return

    # Test each relevant table
    test_tables = [
        ("voter_hexagons", "Voter Density Hexagons"),
        ("voter_block_groups", "Voter Analysis by Block Groups"),
        ("household_demographics_pps", "Household Demographics"),
        ("pps_district_summary", "PPS District Summary")
    ]

    success_count = 0

    for table_name, display_name in test_tables:
        if table_name in tables:
            logger.info(f"\n🧪 Testing table: {table_name}")

            # Get table info
            test_table_info(uploader, table_name)

            # Load data
            gdf = load_data_from_supabase(uploader, table_name)
            if gdf is not None:
                # Create visualization
                output_path = config.get_output_dir("html") / f"test_{table_name}.html"
                if create_test_visualization(gdf, output_path, f"Test: {display_name}"):
                    success_count += 1
                    logger.success(f"   ✅ Round-trip test successful for {table_name}")
                else:
                    logger.error(f"   ❌ Visualization failed for {table_name}")
            else:
                logger.error(f"   ❌ Data loading failed for {table_name}")
        else:
            logger.debug(f"   ⏭️ Skipping {table_name} (not found in database)")

    # Summary
    logger.info(f"\n📊 Test Summary:")
    logger.info(f"   ✅ Successful tests: {success_count}")
    logger.info(f"   📤 Tables tested: {len([t for t, _ in test_tables if t in tables])}")

    if success_count > 0:
        logger.success("🎉 Supabase round-trip test completed successfully!")
        logger.info("   🗺️ Check the generated HTML files in the html/ directory")
    else:
        logger.error("❌ No successful round-trip tests")


if __name__ == "__main__":
    main()
