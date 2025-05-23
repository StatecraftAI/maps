import json
import pathlib
from typing import Optional, Union

import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from config_loader import Config


def detect_candidate_columns(gdf: gpd.GeoDataFrame) -> list:
    """Detect all candidate columns from the enriched dataset."""
    # Look for percentage columns (pct_candidatename) since these come from enrichment
    candidate_pct_cols = [
        col
        for col in gdf.columns
        if col.startswith("pct_")
        and col not in ["pct_dem", "pct_rep", "pct_nav"]
        and "candidate" not in col
    ]  # Avoid raw candidate_ columns
    print(f"  📊 Detected candidate percentage columns: {candidate_pct_cols}")
    return candidate_pct_cols


def detect_candidate_count_columns(gdf: gpd.GeoDataFrame) -> list:
    """Detect all candidate count columns from the enriched dataset."""
    # Look for count columns (cnt_candidatename) since these come from enrichment
    candidate_cnt_cols = [
        col
        for col in gdf.columns
        if col.startswith("cnt_") and col != "cnt_total_votes"
    ]
    print(f"  📊 Detected candidate count columns: {candidate_cnt_cols}")
    return candidate_cnt_cols


def clean_numeric(series: pd.Series, is_percent: bool = False) -> pd.Series:
    """
    Cleans a pandas Series to numeric type, handling commas and percent signs.

    Args:
        series: The pandas Series to clean.
        is_percent: If True, divides the numeric values by 100.0 (for data stored as percentages like "23%")

    Returns:
        A pandas Series with numeric data.
    """
    s = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    vals = pd.to_numeric(s, errors="coerce")
    if is_percent:
        vals = vals / 100.0
    return vals


def validate_and_reproject_to_wgs84(
    gdf: gpd.GeoDataFrame, config: Config, source_description: str = "GeoDataFrame"
) -> gpd.GeoDataFrame:
    """
    Validates and reprojects a GeoDataFrame to WGS84 (EPSG:4326) if needed.

    Args:
        gdf: Input GeoDataFrame
        config: Configuration instance
        source_description: Description for logging

    Returns:
        GeoDataFrame in WGS84 coordinate system
    """
    print(f"\n🗺️ Validating and reprojecting {source_description}:")

    # Check original CRS
    original_crs = gdf.crs
    print(f"  📍 Original CRS: {original_crs}")

    # Get CRS settings from config
    input_crs = config.get_system_setting("input_crs")
    output_crs = config.get_system_setting("output_crs")

    # Handle missing CRS
    if original_crs is None:
        print("  ⚠️ No CRS specified in data")

        # Try to detect coordinate system from sample coordinates
        if not gdf.empty and "geometry" in gdf.columns:
            sample_geom = (
                gdf.geometry.dropna().iloc[0]
                if len(gdf.geometry.dropna()) > 0
                else None
            )
            if sample_geom is not None:
                # Get first coordinate pair
                coords = None
                if hasattr(sample_geom, "exterior"):  # Polygon
                    coords = list(sample_geom.exterior.coords)[0]
                elif hasattr(sample_geom, "coords"):  # Point or LineString
                    coords = list(sample_geom.coords)[0]

                if coords:
                    x, y = coords[0], coords[1]
                    print(f"  🔍 Sample coordinates: x={x:.2f}, y={y:.2f}")

                    # Check if coordinates look like configured input CRS
                    if (
                        input_crs == "EPSG:2913"
                        and abs(x) > 1000000
                        and abs(y) > 1000000
                    ):
                        print(f"  🎯 Coordinates appear to be {input_crs}")
                        gdf = gdf.set_crs(input_crs, allow_override=True)
                    # Check if coordinates look like WGS84 (longitude/latitude)
                    elif -180 <= x <= 180 and -90 <= y <= 90:
                        print(f"  🎯 Coordinates appear to be {output_crs}")
                        gdf = gdf.set_crs(output_crs, allow_override=True)
                    else:
                        print(f"  ❓ Unknown coordinate system, assuming {input_crs}")
                        gdf = gdf.set_crs(input_crs, allow_override=True)
                else:
                    print(
                        f"  ❓ Could not extract sample coordinates, assuming {output_crs}"
                    )
                    gdf = gdf.set_crs(output_crs, allow_override=True)
            else:
                print(f"  ❓ No valid geometry found, assuming {output_crs}")
                gdf = gdf.set_crs(output_crs, allow_override=True)

    # Reproject to output CRS if needed
    current_crs = gdf.crs
    if current_crs is not None:
        try:
            current_epsg = current_crs.to_epsg()
            target_epsg = int(output_crs.split(":")[1])
            if current_epsg != target_epsg:
                print(f"  🔄 Reprojecting from EPSG:{current_epsg} to {output_crs}")
                gdf_reprojected = gdf.to_crs(output_crs)

                # Validate reprojection worked
                if not gdf_reprojected.empty and "geometry" in gdf_reprojected.columns:
                    sample_geom = (
                        gdf_reprojected.geometry.dropna().iloc[0]
                        if len(gdf_reprojected.geometry.dropna()) > 0
                        else None
                    )
                    if sample_geom is not None:
                        coords = None
                        if hasattr(sample_geom, "exterior"):  # Polygon
                            coords = list(sample_geom.exterior.coords)[0]
                        elif hasattr(sample_geom, "coords"):  # Point or LineString
                            coords = list(sample_geom.coords)[0]

                        if coords:
                            x, y = coords[0], coords[1]
                            print(
                                f"  ✓ Reprojected coordinates: lon={x:.6f}, lat={y:.6f}"
                            )

                            # Validate coordinates are in valid WGS84 range
                            if -180 <= x <= 180 and -90 <= y <= 90:
                                print("  ✓ Coordinates are valid WGS84")
                            else:
                                print(
                                    f"  ⚠️ Coordinates may be invalid: lon={x}, lat={y}"
                                )
                        else:
                            print("  ⚠️ Could not validate reprojected coordinates")

                gdf = gdf_reprojected
            else:
                print(f"  ✓ Already in {output_crs}")
        except Exception as e:
            print(f"  ❌ Error during reprojection: {e}")
            print(f"  🔧 Attempting to set CRS as {output_crs}")
            gdf = gdf.set_crs(output_crs, allow_override=True)

    # Final validation
    if gdf.crs is not None:
        try:
            final_epsg = gdf.crs.to_epsg()
            print(f"  ✅ Final CRS: EPSG:{final_epsg}")
        except:
            print(f"  ✅ Final CRS: {gdf.crs}")
    else:
        print("  ⚠️ Warning: Final CRS is None")

    # Validate geometry
    valid_geom_count = gdf.geometry.notna().sum()
    total_count = len(gdf)
    print(
        f"  📊 Geometry validation: {valid_geom_count}/{total_count} features have valid geometry"
    )

    return gdf


def optimize_geojson_properties(
    gdf: gpd.GeoDataFrame, config: Config
) -> gpd.GeoDataFrame:
    """
    Optimizes GeoDataFrame properties for web display and vector tile generation.

    Args:
        gdf: Input GeoDataFrame
        config: Configuration instance

    Returns:
        GeoDataFrame with optimized properties
    """
    print("\n🔧 Optimizing properties for web display:")

    # Create a copy to avoid modifying original
    gdf_optimized = gdf.copy()

    # Get precision settings from config
    config.get_system_setting("precision_decimals")
    prop_precision = config.get_system_setting("property_precision")

    # Clean up property names and values for web consumption
    columns_to_clean = gdf_optimized.columns.tolist()
    if "geometry" in columns_to_clean:
        columns_to_clean.remove("geometry")

    for col in columns_to_clean:
        if col in gdf_optimized.columns:
            # Handle different data types
            series = gdf_optimized[col]

            # Convert boolean columns stored as strings
            if col in [
                "participated_election",
                "has_election_data",
                "has_voter_data",
                "in_zone1",
                "complete_record",
            ]:
                gdf_optimized[col] = (
                    series.astype(str)
                    .str.lower()
                    .map({"true": True, "false": False, "1": True, "0": False})
                    .fillna(False)
                )

            # Clean numeric columns - detect count columns dynamically
            elif col.startswith("cnt_") or col in [
                "TOTAL",
                "DEM",
                "REP",
                "NAV",
                "vote_margin",
            ]:
                # Convert to int, handling NaN
                numeric_series = pd.to_numeric(series, errors="coerce").fillna(0)
                gdf_optimized[col] = numeric_series.astype(int)

            # Clean percentage/rate columns - detect percentage columns dynamically
            elif col.startswith("pct_") or col in [
                "turnout_rate",
                "engagement_score",
                "margin_pct",
                "dem_advantage",
                "major_party_pct",
                "dem_performance_vs_reg",
                "rep_performance_vs_reg",
            ]:
                numeric_series = pd.to_numeric(series, errors="coerce").fillna(0)
                # Round to configured precision for web optimization
                gdf_optimized[col] = numeric_series.round(prop_precision)

            # Handle string columns - ensure they're proper strings
            elif col in [
                "political_lean",
                "competitiveness",
                "leading_candidate",
                "record_type",
            ]:
                gdf_optimized[col] = (
                    series.astype(str).replace("nan", "").replace("None", "")
                )
                # Replace empty strings with appropriate defaults
                if col == "political_lean":
                    gdf_optimized[col] = gdf_optimized[col].replace("", "Unknown")
                elif col == "competitiveness":
                    gdf_optimized[col] = gdf_optimized[col].replace(
                        "", "No Election Data"
                    )
                elif col == "leading_candidate":
                    gdf_optimized[col] = gdf_optimized[col].replace("", "No Data")

            # Handle precinct identifiers
            elif col in ["precinct", "Precinct"]:
                gdf_optimized[col] = series.astype(str).str.strip()

    print(f"  ✓ Optimized {len(columns_to_clean)} property columns")

    # Add web-friendly geometry validation
    invalid_geom = gdf_optimized.geometry.isna() | (~gdf_optimized.geometry.is_valid)
    invalid_count = invalid_geom.sum()

    if invalid_count > 0:
        print(f"  ⚠️ Found {invalid_count} invalid geometries, attempting to fix...")
        # Try to fix invalid geometries
        gdf_optimized.geometry = gdf_optimized.geometry.buffer(0)

        # Check again
        still_invalid = gdf_optimized.geometry.isna() | (
            ~gdf_optimized.geometry.is_valid
        )
        still_invalid_count = still_invalid.sum()

        if still_invalid_count > 0:
            print(
                f"  ⚠️ {still_invalid_count} geometries still invalid after fix attempt"
            )
        else:
            print("  ✓ Fixed all invalid geometries")
    else:
        print("  ✓ All geometries are valid")

    return gdf_optimized


def tufte_map(
    gdf: gpd.GeoDataFrame,
    column: str,
    fname: Union[str, pathlib.Path],
    config: Config,
    cmap: str = None,
    title: str = "",
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    label: str = "",
    note: Optional[str] = None,
    diverging: bool = False,
    zoom_to_data: bool = False,
) -> None:
    """
    Generates and saves a minimalist Tufte-style map with optimized layout.

    Args:
        gdf: GeoDataFrame containing the data to plot.
        column: The name of the column in gdf to plot.
        fname: Filename (including path) to save the map.
        config: Configuration instance
        cmap: Colormap to use (uses config default if None).
        title: Title of the map.
        vmin: Minimum value for the color scale.
        vmax: Maximum value for the color scale.
        label: Label for the colorbar.
        note: Annotation note to display at the bottom of the map.
        diverging: Whether this is a diverging color scheme (centers on 0).
        zoom_to_data: If True, zoom to only areas with data in the specified column.
    """
    # Get visualization settings from config
    if cmap is None:
        cmap = config.get_visualization_setting(
            "colormap_diverging" if diverging else "colormap_default"
        )

    map_dpi = config.get_visualization_setting("map_dpi")
    figure_max_width = config.get_visualization_setting("figure_max_width")

    # Determine bounds - either full dataset or just areas with data
    if zoom_to_data:
        # Use only features that have valid data for this column
        data_features = gdf[gdf[column].notna() & (gdf[column] > 0)]
        if len(data_features) > 0:
            map_bounds = data_features.total_bounds
        else:
            map_bounds = gdf.total_bounds
    else:
        map_bounds = gdf.total_bounds

    # Calculate optimal figure size based on bounds aspect ratio
    data_width = map_bounds[2] - map_bounds[0]
    data_height = map_bounds[3] - map_bounds[1]
    aspect_ratio = data_width / data_height

    # Set figure size to match data aspect ratio (max from config)
    if aspect_ratio > 1:  # Wider than tall
        fig_width = min(figure_max_width, 10 * aspect_ratio)
        fig_height = fig_width / aspect_ratio
    else:  # Taller than wide
        fig_height = min(figure_max_width, 10 / aspect_ratio)
        fig_width = fig_height * aspect_ratio

    # Create figure with optimized size and DPI
    fig, ax = plt.subplots(figsize=(fig_width, fig_height), dpi=map_dpi)

    # Filter to only areas with data for better extent calculation
    data_gdf = gdf[gdf[column].notna()].copy()

    # Determine optimal vmin and vmax based on data distribution
    if vmin is None or vmax is None:
        data_values = data_gdf[column].dropna()
        if len(data_values) > 0:
            if diverging:
                # For diverging scales, center on 0 and use symmetric range
                abs_max = max(abs(data_values.min()), abs(data_values.max()))
                plot_vmin = -abs_max if vmin is None else vmin
                plot_vmax = abs_max if vmax is None else vmax
            else:
                # For sequential scales, use data range with slight padding
                data_range = data_values.max() - data_values.min()
                plot_vmin = (
                    data_values.min() - (data_range * 0.02) if vmin is None else vmin
                )
                plot_vmax = (
                    data_values.max() + (data_range * 0.02) if vmax is None else vmax
                )
        else:
            plot_vmin = 0
            plot_vmax = 1
    else:
        plot_vmin = vmin
        plot_vmax = vmax

    # Plot the map
    gdf.plot(
        column=column,
        cmap=cmap,
        linewidth=0.25,
        edgecolor="#444444",
        ax=ax,
        legend=False,
        vmin=plot_vmin,
        vmax=plot_vmax,
        missing_kwds={
            "color": "#f8f8f8",
            "edgecolor": "#cccccc",
            "hatch": "///",
            "linewidth": 0.25,
        },
    )

    # Set extent to optimal bounds (eliminate excessive white space)
    if zoom_to_data:
        data_features = gdf[gdf[column].notna() & (gdf[column] > 0)]
        if len(data_features) > 0:
            map_bounds = data_features.total_bounds
        else:
            map_bounds = gdf.total_bounds
    else:
        map_bounds = gdf.total_bounds

    # Be more aggressive about margin reduction - use minimal margins
    x_range = map_bounds[2] - map_bounds[0]
    y_range = map_bounds[3] - map_bounds[1]

    # Use tiny margins (1% instead of 5%) to maximize data area
    x_margin = x_range * 0.01
    y_margin = y_range * 0.01

    # Set tight bounds
    ax.set_xlim(map_bounds[0] - x_margin, map_bounds[2] + x_margin)
    ax.set_ylim(map_bounds[1] - y_margin, map_bounds[3] + y_margin)

    # Ensure equal aspect ratio to prevent distortion
    ax.set_aspect("equal")

    # Remove axes and spines for clean look
    ax.set_axis_off()

    # Add title with proper positioning and styling
    if title:
        fig.suptitle(
            title, fontsize=16, fontweight="bold", x=0.02, y=0.95, ha="left", va="top"
        )

    # Create and position colorbar (optimized for tight bounds)
    if plot_vmax > plot_vmin:  # Only add colorbar if there's a range
        sm = mpl.cm.ScalarMappable(
            norm=mpl.colors.Normalize(vmin=plot_vmin, vmax=plot_vmax), cmap=cmap
        )

        # Position colorbar more precisely to avoid affecting map bounds
        cbar_ax = fig.add_axes(
            [0.92, 0.15, 0.02, 0.7]
        )  # [left, bottom, width, height] - thinner, further right
        cbar = fig.colorbar(sm, cax=cbar_ax)

        # Style the colorbar
        cbar.ax.tick_params(labelsize=10, colors="#333333")
        cbar.outline.set_edgecolor("#666666")
        cbar.outline.set_linewidth(0.5)

        # Add colorbar label
        if label:
            cbar.set_label(
                label, rotation=90, labelpad=12, fontsize=11, color="#333333"
            )

    # Add note at bottom if provided
    if note:
        fig.text(
            0.02,
            0.02,
            note,
            ha="left",
            va="bottom",
            fontsize=9,
            color="#666666",
            style="italic",
            wrap=True,
        )

    # Save with optimized settings for maximum data area
    plt.savefig(
        fname,
        bbox_inches="tight",
        dpi=map_dpi,
        facecolor="white",
        edgecolor="none",
        pad_inches=0.02,
    )  # Minimal padding
    plt.close(fig)  # Close to free memory
    print(f"Map saved: {fname}")


# === Main Script Logic ===
def main() -> None:
    """
    Main function to load data, process it, and generate maps.
    """
    print("🗺️ Election Map Generation")
    print("=" * 60)

    # Load configuration
    try:
        config = Config()
        print(f"📋 Project: {config.get('project_name')}")
        print(f"📋 Description: {config.get('description')}")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Make sure config.yaml exists in the analysis directory")
        return

    # Get file paths from configuration
    enriched_csv_path = config.get_enriched_csv_path()
    boundaries_path = config.get_input_path("boundaries_geojson")
    output_geojson_path = config.get_web_geojson_path()
    maps_dir = config.get_output_dir("maps")

    print("\nFile paths:")
    print(f"  📄 Enriched CSV: {enriched_csv_path}")
    print(f"  🗺️ Boundaries: {boundaries_path}")
    print(f"  💾 Output GeoJSON: {output_geojson_path}")
    print(f"  🗂️ Maps directory: {maps_dir}")

    # === 1. Load Data ===
    print("\nLoading data files:")
    try:
        df_raw = pd.read_csv(enriched_csv_path, dtype=str)
        print(f"  ✓ Loaded CSV with {len(df_raw)} rows")

        gdf = gpd.read_file(boundaries_path)
        print(f"  ✓ Loaded GeoJSON with {len(gdf)} features")

    except FileNotFoundError as e:
        print(f"❌ Error: Input file not found. {e}")
        return
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    # === 2. Data Filtering and Preprocessing ===
    print("\nData preprocessing and filtering:")

    # Get column names from configuration
    precinct_csv_col = config.get_column_name("precinct_csv")
    precinct_geojson_col = config.get_column_name("precinct_geojson")

    # Filter out summary/aggregate rows from CSV (but keep county summaries)
    summary_precinct_ids = ["multnomah", "grand_total", ""]
    df = df_raw[~df_raw[precinct_csv_col].isin(summary_precinct_ids)].copy()
    print(
        f"  ✓ Filtered CSV: {len(df_raw)} → {len(df)} rows (removed {len(df_raw) - len(df)} summary rows)"
    )

    # Separate regular precincts from county summary rows
    county_summaries = df[df[precinct_csv_col].isin(["clackamas", "washington"])]
    regular_precincts = df[~df[precinct_csv_col].isin(["clackamas", "washington"])]

    print(f"  📊 Regular precincts: {len(regular_precincts)}")
    print(
        f"  📊 County summary rows: {len(county_summaries)} ({county_summaries[precinct_csv_col].tolist()})"
    )

    # Separate Zone 1 participants from non-participants (only for regular precincts)
    zone1_participants = (
        regular_precincts[
            regular_precincts["participated_election"].astype(str).str.lower() == "true"
        ]
        if "participated_election" in regular_precincts.columns
        else regular_precincts
    )
    non_participants = (
        regular_precincts[
            regular_precincts["participated_election"].astype(str).str.lower()
            == "false"
        ]
        if "participated_election" in regular_precincts.columns
        else pd.DataFrame()
    )

    print(f"  📊 Zone 1 participants: {len(zone1_participants)} precincts")
    print(f"  📊 Non-participants: {len(non_participants)} precincts")
    print(f"  📊 Total Multnomah precincts: {len(regular_precincts)} precincts")

    print(f"  CSV precinct column: {df[precinct_csv_col].dtype}")
    print(f"  GeoJSON precinct column: {gdf[precinct_geojson_col].dtype}")

    # Robust join (strip zeros, lower, strip spaces)
    df[precinct_csv_col] = (
        df[precinct_csv_col].astype(str).str.lstrip("0").str.strip().str.lower()
    )
    gdf[precinct_geojson_col] = (
        gdf[precinct_geojson_col].astype(str).str.lstrip("0").str.strip().str.lower()
    )

    print(f"  Sample CSV precincts: {df[precinct_csv_col].head().tolist()}")
    print(f"  Sample GeoJSON precincts: {gdf[precinct_geojson_col].head().tolist()}")

    # Analyze matching before merge
    csv_precincts = set(df[precinct_csv_col].unique())
    geo_precincts = set(gdf[precinct_geojson_col].unique())

    print(f"  Unique CSV precincts: {len(csv_precincts)}")
    print(f"  Unique GeoJSON precincts: {len(geo_precincts)}")
    print(f"  Intersection: {len(csv_precincts & geo_precincts)}")

    csv_only = csv_precincts - geo_precincts
    geo_only = geo_precincts - csv_precincts
    if csv_only:
        print(
            f"  ⚠️  CSV-only precincts: {sorted(csv_only)[:5]}{'...' if len(csv_only) > 5 else ''}"
        )
    if geo_only:
        print(
            f"  ⚠️  GeoJSON-only precincts: {sorted(geo_only)[:5]}{'...' if len(geo_only) > 5 else ''}"
        )

    gdf_merged = gdf.merge(
        df, left_on=precinct_geojson_col, right_on=precinct_csv_col, how="left"
    )
    print(f"  ✓ Merged data: {len(gdf_merged)} features")

    # COORDINATE VALIDATION AND REPROJECTION
    print("\n🗺️ Coordinate System Processing:")
    gdf_merged = validate_and_reproject_to_wgs84(
        gdf_merged, config, "merged election data"
    )

    # OPTIMIZE PROPERTIES FOR WEB
    gdf_merged = optimize_geojson_properties(gdf_merged, config)

    # Check for unmatched precincts
    matched = gdf_merged[~gdf_merged[precinct_csv_col].isna()]
    unmatched = gdf_merged[gdf_merged[precinct_csv_col].isna()]
    print(f"  ✓ Matched features: {len(matched)}")
    if len(unmatched) > 0:
        print(f"  ⚠️  Unmatched features: {len(unmatched)}")
        print(
            f"     Example unmatched GeoJSON precincts: {unmatched[precinct_geojson_col].head().tolist()}"
        )

    # Dynamically detect all columns to clean
    print("\n🧹 Cleaning data columns:")

    # Clean all count columns dynamically
    count_cols = [col for col in gdf_merged.columns if col.startswith("cnt_")]
    for col in count_cols:
        gdf_merged[col] = clean_numeric(gdf_merged[col], is_percent=False)
        valid_count = gdf_merged[col].notna().sum()
        if valid_count > 0:
            print(
                f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.0f} - {gdf_merged[col].max():.0f}"
            )

    # Clean all percentage columns dynamically
    pct_cols = [col for col in gdf_merged.columns if col.startswith("pct_")]
    for col in pct_cols:
        gdf_merged[col] = clean_numeric(gdf_merged[col], is_percent=False)
        valid_count = gdf_merged[col].notna().sum()
        if valid_count > 0:
            print(
                f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.3f} - {gdf_merged[col].max():.3f}"
            )

    # Clean other numeric columns
    other_numeric_cols = [
        "turnout_rate",
        "engagement_score",
        "dem_advantage",
        "margin_pct",
        "vote_margin",
        "major_party_pct",
    ]
    for col in other_numeric_cols:
        if col in gdf_merged.columns:
            gdf_merged[col] = clean_numeric(gdf_merged[col], is_percent=False)
            valid_count = gdf_merged[col].notna().sum()
            if valid_count > 0:
                print(
                    f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.3f} - {gdf_merged[col].max():.3f}"
                )

    # Handle categorical columns
    categorical_cols = ["participated_election", "political_lean", "competitiveness"]
    for col in categorical_cols:
        if col in gdf_merged.columns:
            # Special handling for boolean columns that may be stored as strings
            if col == "participated_election":
                gdf_merged[col] = (
                    gdf_merged[col]
                    .astype(str)
                    .str.lower()
                    .map({"true": True, "false": False})
                )

            value_counts = gdf_merged[col].value_counts()
            print(f"  ✓ {col} distribution: {dict(value_counts)}")

    # === Competition Metrics Analysis ===
    print("\nAnalyzing pre-calculated competition metrics:")

    # The enriched dataset already has margin_pct, competitiveness, leading_candidate calculated
    if "margin_pct" in gdf_merged.columns:
        margin_stats = gdf_merged[gdf_merged["margin_pct"].notna()]["margin_pct"]
        if len(margin_stats) > 0:
            print(
                f"  ✓ Vote margins available: median {margin_stats.median():.1%}, range {margin_stats.min():.1%} - {margin_stats.max():.1%}"
            )

    if "competitiveness" in gdf_merged.columns:
        comp_stats = gdf_merged["competitiveness"].value_counts()
        print(f"  📊 Competitiveness distribution: {dict(comp_stats)}")

    if "leading_candidate" in gdf_merged.columns:
        leader_stats = gdf_merged["leading_candidate"].value_counts()
        print(f"  📊 Leading candidate distribution: {dict(leader_stats)}")

    # Summary of Zone 1 vs Non-Zone 1
    if "participated_election" in gdf_merged.columns:
        participated_count = gdf_merged[
            gdf_merged["participated_election"] == True
        ].shape[0]
        not_participated_count = gdf_merged[
            gdf_merged["participated_election"] == False
        ].shape[0]
        print(
            f"  📊 Zone 1 participation: {participated_count} participated, {not_participated_count} did not participate"
        )

    # === 3. Save Merged GeoJSON ===
    try:
        print("\n💾 Saving optimized GeoJSON for web use:")

        # Ensure we have proper CRS before saving
        if gdf_merged.crs is None:
            print("  🔧 Setting WGS84 CRS for output")
            gdf_merged = gdf_merged.set_crs("EPSG:4326")

        # Calculate summary statistics for metadata
        zone1_features = (
            gdf_merged[gdf_merged.get("participated_election", False) == True]
            if "participated_election" in gdf_merged.columns
            else gdf_merged
        )
        total_votes_cast = (
            zone1_features["cnt_total_votes"].sum()
            if "cnt_total_votes" in zone1_features.columns
            else 0
        )

        # Save with proper driver options for web consumption
        gdf_merged.to_file(
            output_geojson_path,
            driver="GeoJSON",
        )

        # Add metadata to the saved GeoJSON file
        with open(output_geojson_path, "r") as f:
            geojson_data = json.load(f)

        # Add comprehensive metadata
        geojson_data["crs"] = {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"  # Standard web-friendly CRS identifier
            },
        }

        # Add metadata object
        geojson_data["metadata"] = {
            "title": config.get("project_name"),
            "description": config.get("description"),
            "source": config.get_metadata("data_source"),
            "created": "2025-01-22",
            "crs": "EPSG:4326",
            "coordinate_system": "WGS84 Geographic",
            "features_count": len(gdf_merged),
            "zone1_features": len(zone1_features) if len(zone1_features) > 0 else 0,
            "total_votes_cast": int(total_votes_cast)
            if not pd.isna(total_votes_cast)
            else 0,
            "data_sources": [
                config.get_metadata("attribution"),
                config.get_metadata("data_source"),
            ],
            "processing_notes": [
                f"Coordinates reprojected to {config.get_system_setting('output_crs')} for web compatibility",
                "Properties optimized for vector tile generation",
                "Geometry validated and fixed where necessary",
            ],
        }

        # Save the enhanced GeoJSON
        with open(output_geojson_path, "w") as f:
            json.dump(
                geojson_data, f, separators=(",", ":")
            )  # Compact formatting for web

        print(f"  ✓ Saved optimized GeoJSON: {output_geojson_path}")
        print(f"  📊 Features: {len(gdf_merged)}, CRS: EPSG:4326 (WGS84)")
        print(
            f"  🗳️ Zone 1 features: {len(zone1_features)}, Total votes: {int(total_votes_cast):,}"
        )

    except Exception as e:
        print(f"  ❌ Error saving GeoJSON: {e}")
        return

    # === 4. Generate Maps ===
    print("\nGenerating maps:")

    # 1. Zone 1 Participation Map
    if "participated_election" in gdf_merged.columns:
        # Create a numeric version for plotting
        gdf_merged["participated_numeric"] = gdf_merged["participated_election"].astype(
            int
        )

        tufte_map(
            gdf_merged,
            "participated_numeric",
            fname=maps_dir / "zone1_participation.png",
            config=config,
            cmap="RdYlGn",
            title="Zone 1 Election Participation by Geographic Feature",
            label="Participated in Election",
            vmin=0,
            vmax=1,
            note="Green areas participated in Zone 1 election, red areas did not",
        )

    # 2. Political Lean (All Multnomah Features)
    if "political_lean" in gdf_merged.columns:
        # Create numeric mapping for political lean
        lean_mapping = {
            "Strong Rep": 1,
            "Lean Rep": 2,
            "Competitive": 3,
            "Lean Dem": 4,
            "Strong Dem": 5,
        }
        gdf_merged["political_lean_numeric"] = gdf_merged["political_lean"].map(
            lean_mapping
        )

        tufte_map(
            gdf_merged,
            "political_lean_numeric",
            fname=maps_dir / "political_lean_all_precincts.png",
            config=config,
            cmap="RdBu",
            title="Political Lean by Voter Registration (All Multnomah)",
            label="Political Lean",
            vmin=1,
            vmax=5,
            note="Based on voter registration patterns. Red=Republican lean, Blue=Democratic lean",
        )

    # 3. Democratic Registration Advantage
    if "dem_advantage" in gdf_merged.columns:
        tufte_map(
            gdf_merged,
            "dem_advantage",
            fname=maps_dir / "democratic_advantage_registration.png",
            config=config,
            title="Democratic Registration Advantage (All Multnomah)",
            label="Democratic Advantage",
            diverging=True,
            note="Blue areas have more Democratic registrations, red areas more Republican",
        )

    # 4. Total votes (Zone 1 only)
    if (
        "cnt_total_votes" in gdf_merged.columns
        and not gdf_merged["cnt_total_votes"].isnull().all()
    ):
        has_votes = gdf_merged[gdf_merged["participated_election"] == True]
        print(f"  📊 Total votes: {len(has_votes)} features with election data")

        tufte_map(
            gdf_merged,
            "cnt_total_votes",
            fname=maps_dir / "total_votes_zone1.png",
            config=config,
            cmap="Oranges",
            title=f"Total Votes by Geographic Feature ({config.get('project_name')})",
            label="Number of Votes",
            vmin=0,
            zoom_to_data=True,
            note=f"Data available for {len(has_votes)} Zone 1 features. Zoomed to election area.",
        )

    # 5. Voter turnout (Zone 1 only)
    if (
        "turnout_rate" in gdf_merged.columns
        and not gdf_merged["turnout_rate"].isnull().all()
    ):
        has_turnout = gdf_merged[
            gdf_merged["turnout_rate"].notna() & (gdf_merged["turnout_rate"] > 0)
        ]
        print(f"  📊 Turnout: {len(has_turnout)} features with turnout data")

        tufte_map(
            gdf_merged,
            "turnout_rate",
            fname=maps_dir / "voter_turnout_zone1.png",
            config=config,
            cmap="Blues",
            title=f"Voter Turnout by Geographic Feature ({config.get('project_name')})",
            label="Turnout Rate",
            vmin=0,
            vmax=0.4,
            zoom_to_data=True,
            note=f"Source: {config.get_metadata('attribution')}. Zoomed to Zone 1 election area.",
        )

    # 6. Candidate Vote Share Maps (Zone 1 only) - DYNAMIC FOR ANY CANDIDATES
    candidate_pct_cols = detect_candidate_columns(gdf_merged)

    for pct_col in candidate_pct_cols:
        if not gdf_merged[pct_col].isnull().all():
            candidate_name = pct_col.replace("pct_", "").title()
            has_data = gdf_merged[gdf_merged[pct_col].notna()]
            print(
                f"  📊 {candidate_name} vote share: {len(has_data)} features with data"
            )

            tufte_map(
                gdf_merged,
                pct_col,
                fname=maps_dir / f"{candidate_name.lower()}_vote_share.png",
                config=config,
                cmap="Greens",
                title=f"{candidate_name} Vote Share by Geographic Feature",
                label="Vote Share",
                vmin=0.5,
                vmax=0.9,
                zoom_to_data=True,
                note=f"Shows {candidate_name}'s performance in Zone 1 features. Zoomed to election area.",
            )

    # 7. Engagement Score (Zone 1 participants only)
    if (
        "engagement_score" in gdf_merged.columns
        and not gdf_merged["engagement_score"].isnull().all()
    ):
        has_engagement = gdf_merged[gdf_merged["engagement_score"].notna()]
        print(f"  📊 Engagement score: {len(has_engagement)} features with data")

        # Use data-driven range for better contrast
        engagement_data = has_engagement["engagement_score"].dropna()
        q25, q75 = engagement_data.quantile([0.25, 0.75])

        tufte_map(
            gdf_merged,
            "engagement_score",
            fname=maps_dir / "engagement_score.png",
            config=config,
            cmap="viridis",
            title="Civic Engagement Score by Geographic Feature (Zone 1 Election)",
            label="Engagement Score",
            vmin=q25,
            vmax=q75,
            zoom_to_data=True,
            note="Higher scores = more diverse voter registration + higher turnout. Zoomed to election area.",
        )

    # 8. Vote margin/competition (Zone 1 only)
    if (
        "margin_pct" in gdf_merged.columns
        and not gdf_merged["margin_pct"].isnull().all()
    ):
        has_margin = gdf_merged[gdf_merged["margin_pct"].notna()]
        print(f"  📊 Vote Margin: {len(has_margin)} features with data")

        tufte_map(
            gdf_merged,
            "margin_pct",
            fname=maps_dir / "vote_margin_zone1.png",
            config=config,
            cmap="plasma",
            title=f"Vote Margin by Geographic Feature ({config.get('project_name')})",
            label="Vote Margin",
            vmin=0.3,
            vmax=0.8,
            zoom_to_data=True,
            note="Higher values = larger victory margins. Zoomed to Zone 1 election area.",
        )

    # 9. Political Lean vs Zone 1 Participation Analysis
    if (
        "political_lean" in gdf_merged.columns
        and "participated_election" in gdf_merged.columns
    ):
        # Create a combined metric for analytical insight
        gdf_merged["lean_participation"] = gdf_merged.apply(
            lambda row: f"{row['political_lean']} - {'Zone 1' if row['participated_election'] else 'No Zone 1'}",
            axis=1,
        )

        # Create numeric mapping for visualization
        combined_mapping = {
            "Strong Rep - No Zone 1": 1,
            "Strong Rep - Zone 1": 2,
            "Lean Rep - No Zone 1": 3,
            "Lean Rep - Zone 1": 4,
            "Competitive - No Zone 1": 5,
            "Competitive - Zone 1": 6,
            "Lean Dem - No Zone 1": 7,
            "Lean Dem - Zone 1": 8,
            "Strong Dem - No Zone 1": 9,
            "Strong Dem - Zone 1": 10,
        }
        gdf_merged["lean_participation_numeric"] = gdf_merged["lean_participation"].map(
            combined_mapping
        )

        tufte_map(
            gdf_merged,
            "lean_participation_numeric",
            fname=maps_dir / "political_lean_vs_zone1_participation.png",
            config=config,
            cmap="RdBu",
            title="Political Lean vs Zone 1 Election Participation",
            label="Lean + Participation",
            vmin=1,
            vmax=10,
            note="Analysis of voter registration patterns vs Zone 1 election participation. Red=Rep, Blue=Dem, Darker=participated",
        )

        print("  📊 Political Lean vs Participation: Combined analysis map created")

    print("\n✅ Script completed successfully!")
    print(f"   Maps saved to: {maps_dir}")
    print(f"   GeoJSON saved to: {output_geojson_path}")
    print(
        f"   Summary: {len(matched)} features with election data out of {len(gdf_merged)} total features"
    )

    # Summary of generated maps
    candidate_count = len(candidate_pct_cols)
    total_maps = 7 + candidate_count  # Base maps + candidate-specific maps

    print(f"\n🗺️ Generated {total_maps} maps:")
    print("   1. Zone 1 Participation Map")
    print("   2. Political Lean (All Multnomah)")
    print("   3. Democratic Registration Advantage")
    print("   4. Total votes (Zone 1 only)")
    print("   5. Voter turnout (Zone 1 only)")
    for i, pct_col in enumerate(candidate_pct_cols, 6):
        candidate_name = pct_col.replace("pct_", "").title()
        print(f"   {i}. {candidate_name} Vote Share (Zone 1 only)")
    print(f"   {6 + candidate_count}. Engagement Score (Zone 1 participants only)")
    print(f"   {7 + candidate_count}. Vote margin/competition (Zone 1 only)")
    print(f"   {8 + candidate_count}. Political Lean vs Zone 1 Participation Analysis")


if __name__ == "__main__":
    main()
