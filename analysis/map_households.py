#!/usr/bin/env python3
"""
Household Demographics Analysis

This script analyzes household demographics with a focus on households without minors
(empty nesters and senior households) within the Portland Public Schools district.

Key Analysis:
- Maps households without children by block group
- Calculates demographic concentrations within PPS boundaries
- Creates comparative visualizations
- Analyzes voting patterns among empty nester households

This data is relevant for school board elections as it helps understand
the geographic distribution of households that may have different
relationships to school district issues.

Methodology:
- Uses American Community Survey (ACS) data at block group level
- Spatially joins with PPS district boundaries
- Creates choropleth maps and summary statistics
- Generates detailed demographic reports
"""

import json
import sys

import folium
import geopandas as gpd
import pandas as pd
from loguru import logger

from ops import Config


def load_acs_data(config: Config):
    """Load and process ACS household data from JSON."""
    acs_path = config.get_input_path("acs_households_json")
    logger.info(f"📊 Loading ACS JSON from {acs_path}")

    if not acs_path.exists():
        logger.critical(f"❌ ACS JSON file not found: {acs_path}")
        return None

    try:
        with open(acs_path) as f:
            data_array = json.load(f)

        # First row is header, rest are data records
        header = data_array[0]
        records = data_array[1:]

        df = pd.DataFrame(records, columns=header)
        logger.success(f"✅ Loaded {len(df)} ACS records")

        # Process ACS fields
        df = df.rename(
            columns={
                "B11001_001E": "total_households",
                "B11001_002E": "households_no_minors",
            }
        )

        # Convert to numeric and handle errors
        df["total_households"] = (
            pd.to_numeric(df["total_households"], errors="coerce").fillna(0).astype(int)
        )
        df["households_no_minors"] = (
            pd.to_numeric(df["households_no_minors"], errors="coerce").fillna(0).astype(int)
        )

        # Create GEOID from component parts
        df["GEOID"] = df["state"] + df["county"] + df["tract"] + df["block group"]

        logger.success(f"✅ Processed household data for {len(df)} block groups")
        logger.info(f"📊 Total households: {df['total_households'].sum():,}")
        logger.info(f"📊 Households without minors: {df['households_no_minors'].sum():,}")

        return df

    except Exception as e:
        logger.error(f"❌ Error loading ACS data: {e}")
        logger.trace("Detailed ACS loading error:")
        import traceback

        logger.trace(traceback.format_exc())
        return None


def load_block_group_geometries(config: Config):
    """Load and filter block group geometries to Multnomah County."""
    bg_path = config.get_input_path("block_groups_shp")
    logger.info(f"🗺️ Loading block group geometries from {bg_path}")

    if not bg_path.exists():
        logger.critical(f"❌ Block groups shapefile not found: {bg_path}")
        return None

    try:
        gdf = gpd.read_file(bg_path)
        logger.success(f"✅ Loaded {len(gdf)} block groups from shapefile")

        # Filter to Multnomah County (Oregon=41, Multnomah=051)
        multnomah_gdf = gdf[(gdf["STATEFP"] == "41") & (gdf["COUNTYFP"] == "051")].copy()

        logger.success(f"✅ Filtered to {len(multnomah_gdf)} Multnomah County block groups")
        return multnomah_gdf

    except Exception as e:
        logger.error(f"❌ Error loading block group geometries: {e}")
        logger.trace("Detailed geometry loading error:")
        import traceback

        logger.trace(traceback.format_exc())
        return None


def merge_acs_with_geometries(acs_df, bg_gdf):
    """Merge ACS data with block group geometries."""
    logger.info("🔗 Merging ACS data with geometries...")

    try:
        # Merge ACS data with geometries
        gdf = bg_gdf.merge(
            acs_df[["GEOID", "total_households", "households_no_minors"]],
            on="GEOID",
            how="left",
        )

        # Fill missing values and ensure proper data types
        gdf["total_households"] = gdf["total_households"].fillna(0).astype(int)
        gdf["households_no_minors"] = gdf["households_no_minors"].fillna(0).astype(int)

        # Calculate percentage without minors
        gdf["percent_no_minors"] = gdf.apply(
            lambda row: round(100 * row["households_no_minors"] / row["total_households"], 1)
            if row["total_households"] > 0
            else 0,
            axis=1,
        )

        logger.success(f"✅ Merged data for {len(gdf)} block groups")
        logger.info(f"📊 Average percent without minors: {gdf['percent_no_minors'].mean():.1f}%")

        return gdf

    except Exception as e:
        logger.error(f"❌ Error merging data: {e}")
        logger.trace("Detailed merge error:")
        import traceback

        logger.trace(traceback.format_exc())
        return None


def filter_to_pps_district(gdf, config: Config):
    """Filter block groups to those within PPS district."""
    pps_path = config.get_input_path("district_boundaries_geojson")
    logger.info(f"🎯 Filtering to PPS district using {pps_path}")

    if not pps_path.exists():
        logger.critical(f"❌ PPS district file not found: {pps_path}")
        return None

    try:
        # Load PPS district boundaries
        pps_region = gpd.read_file(pps_path)
        logger.debug("✓ Loaded PPS district boundaries")

        # Project to consistent CRS for geometric operations
        target_crs = "EPSG:3857"  # Web Mercator for geometric operations
        pps_proj = pps_region.to_crs(target_crs)
        gdf_proj = gdf.to_crs(target_crs)

        # Create union of PPS boundaries
        pps_union = pps_proj.geometry.union_all()

        # Filter block groups whose centroids are within PPS district
        # Note: This centroid calculation is correctly done in projected coordinates
        centroids = gdf_proj.geometry.centroid
        mask = centroids.within(pps_union)

        # Filter and reproject back to WGS84
        pps_gdf = gdf_proj[mask].to_crs("EPSG:4326")

        logger.success(f"✅ Filtered to {len(pps_gdf)} block groups within PPS district")
        logger.info(f"📊 PPS coverage: {len(pps_gdf) / len(gdf):.1%} of Multnomah block groups")

        return pps_gdf

    except Exception as e:
        logger.error(f"❌ Error filtering to PPS district: {e}")
        logger.trace("Detailed filtering error:")
        import traceback

        logger.trace(traceback.format_exc())
        return None


def export_data_and_report(gdf, config: Config):
    """Export processed data and generate summary report."""
    logger.info("📄 Exporting data and generating report...")

    try:
        # Get output paths from config
        csv_path = config.get_households_analysis_csv_path()
        report_path = config.get_households_report_path()

        # Calculate overall statistics
        total_households = gdf["total_households"].sum()
        total_no_minors = gdf["households_no_minors"].sum()
        overall_percent = (total_no_minors / total_households * 100) if total_households > 0 else 0

        # Create report dataframe
        report_data = gdf[
            ["GEOID", "total_households", "households_no_minors", "percent_no_minors"]
        ].copy()
        report_data["percent_no_minors"] = report_data["percent_no_minors"].round(1)

        # Add overall summary row
        overall_row = pd.DataFrame(
            [
                {
                    "GEOID": "Overall PPS District",
                    "total_households": total_households,
                    "households_no_minors": total_no_minors,
                    "percent_no_minors": round(overall_percent, 1),
                }
            ]
        )

        report_data = pd.concat([report_data, overall_row], ignore_index=True)

        # Export CSV (without geometry)
        csv_data = gdf.drop(columns="geometry", errors="ignore")
        csv_data.to_csv(csv_path, index=False)
        logger.success(f"✅ CSV exported: {csv_path}")

        # Generate markdown report
        markdown_content = f"""# Household Demographics Report - PPS District

## Summary Statistics

- **Total Block Groups**: {len(gdf):,}
- **Total Households**: {total_households:,}
- **Households without Minors**: {total_no_minors:,}
- **Percentage without Minors**: {overall_percent:.1f}%

## Block Group Details

{report_data.to_markdown(index=False)}

---
*Generated by household demographics analysis pipeline*
"""

        with open(report_path, "w") as f:
            f.write(markdown_content)

        logger.success(f"✅ Report generated: {report_path}")
        logger.info(f"📊 Overall: {overall_percent:.1f}% of households have no minors")

        return True

    except Exception as e:
        logger.error(f"❌ Error exporting data: {e}")
        logger.trace("Detailed export error:")
        import traceback

        logger.trace(traceback.format_exc())
        return False


def create_choropleth_map(gdf, config: Config):
    """Create interactive Folium choropleth map."""
    logger.info("🗺️ Creating choropleth map...")

    try:
        # Get output path from config
        output_path = config.get_households_map_path()
        pps_path = config.get_input_path("district_boundaries_geojson")

        # Calculate map center using projected centroids to avoid GeoPandas warning
        logger.debug("📍 Calculating map center using projected coordinates...")

        # Reproject to projected CRS for accurate centroid calculation
        projected_crs = "EPSG:2913"  # NAD83(HARN) / Oregon North - appropriate for Oregon
        gdf_proj = gdf.to_crs(projected_crs)

        # Calculate centroids in projected coordinates
        centroids_proj = gdf_proj.geometry.centroid

        # Reproject centroids back to WGS84 for mapping
        centroids_gdf = gpd.GeoDataFrame(geometry=centroids_proj, crs=projected_crs).to_crs(
            "EPSG:4326"
        )

        # Extract lat/lon from reprojected centroids
        center_lon = centroids_gdf.geometry.x.mean()
        center_lat = centroids_gdf.geometry.y.mean()
        center = [center_lat, center_lon]

        logger.debug(f"   Map center: {center[0]:.4f}, {center[1]:.4f}")

        # Create base map
        m = folium.Map(location=center, zoom_start=12, tiles="CartoDB Dark_Matter")

        # Calculate quantile thresholds for better color distribution
        thresholds = list(gdf["percent_no_minors"].quantile([0, 0.2, 0.4, 0.6, 0.8, 1]).round(1))
        logger.debug(f"   📊 Color thresholds: {thresholds}")

        # Add choropleth layer
        folium.Choropleth(
            geo_data=gdf,
            name="Households without Minors",
            data=gdf,
            columns=["GEOID", "percent_no_minors"],
            key_on="feature.properties.GEOID",
            fill_color="YlOrRd",
            threshold_scale=thresholds,
            fill_opacity=0.6,
            line_opacity=0.3,
            legend_name="% Households without Minors",
        ).add_to(m)

        # Add PPS district boundary
        if pps_path.exists():
            pps_region = gpd.read_file(pps_path).to_crs("EPSG:4326")
            folium.GeoJson(
                data=pps_region.__geo_interface__,
                name="PPS District Boundary",
                style_function=lambda feature: {
                    "color": "#ff00ff",
                    "weight": 3,
                    "fillOpacity": 0,
                    "opacity": 0.8,
                },
            ).add_to(m)
            logger.debug("   ✓ Added PPS district boundary to map")

        # Add interactive tooltips for block groups
        folium.GeoJson(
            data=gdf.__geo_interface__,
            name="Block Group Details",
            style_function=lambda feature: {
                "color": "#888888",
                "weight": 0.5,
                "fillOpacity": 0,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=[
                    "GEOID",
                    "percent_no_minors",
                    "total_households",
                    "households_no_minors",
                ],
                aliases=[
                    "Block Group ID:",
                    "% No Minors:",
                    "Total Households:",
                    "HH without Minors:",
                ],
                localize=True,
                sticky=False,
                labels=True,
                style="""
                    background-color: white;
                    border: 2px solid black;
                    border-radius: 3px;
                    box-shadow: 3px;
                """,
            ),
        ).add_to(m)

        # Add layer control
        folium.LayerControl().add_to(m)

        # Save map
        m.save(output_path)
        logger.success(f"✅ Choropleth map saved: {output_path}")

        return True

    except Exception as e:
        logger.error(f"❌ Error creating choropleth map: {e}")
        logger.trace("Detailed choropleth map error:")
        import traceback

        logger.trace(traceback.format_exc())
        return False


def main():
    """Main execution function."""
    logger.info("🏠 Household Demographics Analysis")
    logger.info("=" * 50)

    # Load configuration
    try:
        config = Config()
        logger.info(f"📋 Project: {config.get('project_name')}")
        logger.info(f"📋 Description: {config.get('description')}")
    except Exception as e:
        logger.critical(f"❌ Configuration error: {e}")
        logger.info("💡 Make sure config.yaml exists in the analysis directory")
        sys.exit(1)

    # Load ACS data
    acs_df = load_acs_data(config)
    if acs_df is None:
        sys.exit(1)

    # Load block group geometries
    bg_gdf = load_block_group_geometries(config)
    if bg_gdf is None:
        sys.exit(1)

    # Merge ACS data with geometries
    merged_gdf = merge_acs_with_geometries(acs_df, bg_gdf)
    if merged_gdf is None:
        sys.exit(1)

    # Filter to PPS district
    pps_gdf = filter_to_pps_district(merged_gdf, config)
    if pps_gdf is None:
        sys.exit(1)

    # Export data and generate report
    if not export_data_and_report(pps_gdf, config):
        sys.exit(1)

    # Create choropleth map
    if not create_choropleth_map(pps_gdf, config):
        sys.exit(1)

    logger.success("✅ Household demographics analysis completed successfully!")
    logger.info("📊 Outputs:")
    csv_path = config.get_households_analysis_csv_path()
    report_path = config.get_households_report_path()
    map_path = config.get_households_map_path()
    logger.info(f"   • Processed data CSV: {csv_path}")
    logger.info(f"   • Summary report: {report_path}")
    logger.info(f"   • Interactive choropleth map: {map_path}")


if __name__ == "__main__":
    main()
