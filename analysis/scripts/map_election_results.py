import pathlib
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import json
from typing import Union, Optional, Any

# Directories and File Paths
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
ANALYSIS_DIR = SCRIPT_DIR.parent
DATA_DIR = ANALYSIS_DIR / 'data/elections'
GIS_DIR = ANALYSIS_DIR / 'geospatial'
MAPS_DIR = ANALYSIS_DIR / 'maps'

# Create output directories if they don't exist
MAPS_DIR.mkdir(parents=True, exist_ok=True)

# Input file names
CSV_FILENAME = "2025_election_zone1_total_votes_enriched.csv"
GEOJSON_FILENAME = "multnomah_elections_precinct_split_2024.geojson"

# Output file names
OUTPUT_GEOJSON = "2025_election_zone1_results.geojson"

# Column Names
COL_PRECINCT_DF = 'precinct'
COL_PRECINCT_GDF = 'Precinct'
COL_TOTAL_VOTES = 'cnt_total_votes'
COL_TURNOUT = 'turnout_rate'
COL_PPS_COVERAGE = 'rat_covered_pps'

# Candidate columns (correct names from enriched dataset)
COL_SPLITT = 'cnt_splitt'
COL_CAVAGNOLO = 'cnt_cavagnolo'
COL_SPLITT_PCT = 'pct_splitt'
COL_CAVAGNOLO_PCT = 'pct_cavagnolo'

# New enriched dataset columns
COL_PARTICIPATED = 'participated_election'
COL_POLITICAL_LEAN = 'political_lean'
COL_DEM_ADVANTAGE = 'dem_advantage'
COL_COMPETITIVENESS = 'competitiveness'
COL_ENGAGEMENT = 'engagement_score'
COL_DEM_REG = 'pct_dem'
COL_REP_REG = 'pct_rep'

# === Helper Functions ===
def clean_numeric(series: pd.Series, is_percent: bool = False) -> pd.Series:
    """
    Cleans a pandas Series to numeric type, handling commas and percent signs.

    Args:
        series: The pandas Series to clean.
        is_percent: If True, divides the numeric values by 100.0 (for data stored as percentages like "23%")

    Returns:
        A pandas Series with numeric data.
    """
    s = series.astype(str).str.replace(',', '', regex=False).str.replace('%', '', regex=False).str.strip()
    vals = pd.to_numeric(s, errors='coerce')
    if is_percent:
        vals = vals / 100.0
    return vals

def validate_and_reproject_to_wgs84(gdf: gpd.GeoDataFrame, source_description: str = "GeoDataFrame") -> gpd.GeoDataFrame:
    """
    Validates and reprojects a GeoDataFrame to WGS84 (EPSG:4326) if needed.
    
    Args:
        gdf: Input GeoDataFrame
        source_description: Description for logging
        
    Returns:
        GeoDataFrame in WGS84 coordinate system
    """
    print(f"\n🗺️ Validating and reprojecting {source_description}:")
    
    # Check original CRS
    original_crs = gdf.crs
    print(f"  📍 Original CRS: {original_crs}")
    
    # Handle missing CRS
    if original_crs is None:
        print("  ⚠️ No CRS specified in data")
        
        # Try to detect coordinate system from sample coordinates
        if not gdf.empty and 'geometry' in gdf.columns:
            sample_geom = gdf.geometry.dropna().iloc[0] if len(gdf.geometry.dropna()) > 0 else None
            if sample_geom is not None:
                # Get first coordinate pair
                coords = None
                if hasattr(sample_geom, 'exterior'):  # Polygon
                    coords = list(sample_geom.exterior.coords)[0]
                elif hasattr(sample_geom, 'coords'):  # Point or LineString
                    coords = list(sample_geom.coords)[0]
                
                if coords:
                    x, y = coords[0], coords[1]
                    print(f"  🔍 Sample coordinates: x={x:.2f}, y={y:.2f}")
                    
                    # Check if coordinates look like Oregon State Plane (large values in feet)
                    if abs(x) > 1000000 and abs(y) > 1000000:
                        print("  🎯 Coordinates appear to be Oregon State Plane North (EPSG:2913)")
                        gdf = gdf.set_crs('EPSG:2913', allow_override=True)
                    # Check if coordinates look like WGS84 (longitude/latitude)
                    elif -180 <= x <= 180 and -90 <= y <= 90:
                        print("  🎯 Coordinates appear to be WGS84 (EPSG:4326)")
                        gdf = gdf.set_crs('EPSG:4326', allow_override=True)
                    else:
                        print(f"  ❓ Unknown coordinate system, assuming WGS84")
                        gdf = gdf.set_crs('EPSG:4326', allow_override=True)
                else:
                    print("  ❓ Could not extract sample coordinates, assuming WGS84")
                    gdf = gdf.set_crs('EPSG:4326', allow_override=True)
            else:
                print("  ❓ No valid geometry found, assuming WGS84")
                gdf = gdf.set_crs('EPSG:4326', allow_override=True)
    
    # Reproject to WGS84 if needed
    current_crs = gdf.crs
    if current_crs is not None:
        try:
            current_epsg = current_crs.to_epsg()
            if current_epsg != 4326:
                print(f"  🔄 Reprojecting from EPSG:{current_epsg} to WGS84 (EPSG:4326)")
                gdf_reprojected = gdf.to_crs('EPSG:4326')
                
                # Validate reprojection worked
                if not gdf_reprojected.empty and 'geometry' in gdf_reprojected.columns:
                    sample_geom = gdf_reprojected.geometry.dropna().iloc[0] if len(gdf_reprojected.geometry.dropna()) > 0 else None
                    if sample_geom is not None:
                        coords = None
                        if hasattr(sample_geom, 'exterior'):  # Polygon
                            coords = list(sample_geom.exterior.coords)[0]
                        elif hasattr(sample_geom, 'coords'):  # Point or LineString
                            coords = list(sample_geom.coords)[0]
                        
                        if coords:
                            x, y = coords[0], coords[1]
                            print(f"  ✓ Reprojected coordinates: lon={x:.6f}, lat={y:.6f}")
                            
                            # Validate coordinates are in valid WGS84 range
                            if -180 <= x <= 180 and -90 <= y <= 90:
                                print("  ✓ Coordinates are valid WGS84")
                            else:
                                print(f"  ⚠️ Coordinates may be invalid: lon={x}, lat={y}")
                        else:
                            print("  ⚠️ Could not validate reprojected coordinates")
                
                gdf = gdf_reprojected
            else:
                print("  ✓ Already in WGS84 (EPSG:4326)")
        except Exception as e:
            print(f"  ❌ Error during reprojection: {e}")
            print("  🔧 Attempting to set CRS as WGS84")
            gdf = gdf.set_crs('EPSG:4326', allow_override=True)
    
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
    print(f"  📊 Geometry validation: {valid_geom_count}/{total_count} features have valid geometry")
    
    return gdf

def optimize_geojson_properties(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Optimizes GeoDataFrame properties for web display and vector tile generation.
    
    Args:
        gdf: Input GeoDataFrame
        
    Returns:
        GeoDataFrame with optimized properties
    """
    print(f"\n🔧 Optimizing properties for web display:")
    
    # Create a copy to avoid modifying original
    gdf_optimized = gdf.copy()
    
    # Clean up property names and values for web consumption
    columns_to_clean = gdf_optimized.columns.tolist()
    if 'geometry' in columns_to_clean:
        columns_to_clean.remove('geometry')
    
    for col in columns_to_clean:
        if col in gdf_optimized.columns:
            # Handle different data types
            series = gdf_optimized[col]
            
            # Convert boolean columns stored as strings
            if col in ['participated_election', 'has_election_data', 'has_voter_data', 'in_zone1', 'complete_record']:
                gdf_optimized[col] = series.astype(str).str.lower().map({
                    'true': True, 'false': False, '1': True, '0': False
                }).fillna(False)
            
            # Clean numeric columns
            elif col in ['cnt_total_votes', 'cnt_splitt', 'cnt_cavagnolo', 'cnt_leof', 'cnt_writein',
                        'TOTAL', 'DEM', 'REP', 'NAV', 'vote_margin']:
                # Convert to int, handling NaN
                numeric_series = pd.to_numeric(series, errors='coerce').fillna(0)
                gdf_optimized[col] = numeric_series.astype(int)
            
            # Clean percentage/rate columns (keep as float)
            elif col in ['turnout_rate', 'engagement_score', 'pct_splitt', 'pct_cavagnolo', 'pct_leof', 'pct_writein',
                        'margin_pct', 'pct_dem', 'pct_rep', 'pct_nav', 'dem_advantage', 'major_party_pct',
                        'dem_performance_vs_reg', 'rep_performance_vs_reg']:
                numeric_series = pd.to_numeric(series, errors='coerce').fillna(0)
                # Round to 3 decimal places for web optimization
                gdf_optimized[col] = numeric_series.round(3)
            
            # Handle string columns - ensure they're proper strings
            elif col in ['political_lean', 'competitiveness', 'leading_candidate', 'record_type']:
                gdf_optimized[col] = series.astype(str).replace('nan', '').replace('None', '')
                # Replace empty strings with appropriate defaults
                if col == 'political_lean':
                    gdf_optimized[col] = gdf_optimized[col].replace('', 'Unknown')
                elif col == 'competitiveness':
                    gdf_optimized[col] = gdf_optimized[col].replace('', 'No Election Data')
                elif col == 'leading_candidate':
                    gdf_optimized[col] = gdf_optimized[col].replace('', 'No Data')
            
            # Handle precinct identifiers
            elif col in ['precinct', 'Precinct']:
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
        still_invalid = gdf_optimized.geometry.isna() | (~gdf_optimized.geometry.is_valid)
        still_invalid_count = still_invalid.sum()
        
        if still_invalid_count > 0:
            print(f"  ⚠️ {still_invalid_count} geometries still invalid after fix attempt")
        else:
            print(f"  ✓ Fixed all invalid geometries")
    else:
        print(f"  ✓ All geometries are valid")
    
    return gdf_optimized

def tufte_map(
    gdf: gpd.GeoDataFrame,
    column: str,
    fname: Union[str, pathlib.Path],
    cmap: str = 'OrRd',
    title: str = '',
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    label: str = '',
    note: Optional[str] = None,
    diverging: bool = False,
    zoom_to_data: bool = False
) -> None:
    """
    Generates and saves a minimalist Tufte-style map with optimized layout.

    Args:
        gdf: GeoDataFrame containing the data to plot.
        column: The name of the column in gdf to plot.
        fname: Filename (including path) to save the map.
        cmap: Colormap to use.
        title: Title of the map.
        vmin: Minimum value for the color scale.
        vmax: Maximum value for the color scale.
        label: Label for the colorbar.
        note: Annotation note to display at the bottom of the map.
        diverging: Whether this is a diverging color scheme (centers on 0).
        zoom_to_data: If True, zoom to only areas with data in the specified column.
    """
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
    
    # Set figure size to match data aspect ratio (max 14 inches wide)
    if aspect_ratio > 1:  # Wider than tall
        fig_width = min(14, 10 * aspect_ratio)
        fig_height = fig_width / aspect_ratio
    else:  # Taller than wide
        fig_height = min(14, 10 / aspect_ratio) 
        fig_width = fig_height * aspect_ratio
    
    # Create figure with optimized size and DPI
    fig, ax = plt.subplots(figsize=(fig_width, fig_height), dpi=300)
    
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
                plot_vmin = data_values.min() - (data_range * 0.02) if vmin is None else vmin
                plot_vmax = data_values.max() + (data_range * 0.02) if vmax is None else vmax
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
        missing_kwds={"color": "#f8f8f8", "edgecolor": "#cccccc", "hatch": "///", "linewidth": 0.25}
    )
    
    # Set extent to optimal bounds (eliminate excessive white space)
    # Use the same bounds calculation as for figure sizing
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
    ax.set_aspect('equal')
    
    # Remove axes and spines for clean look
    ax.set_axis_off()
    
    # Add title with proper positioning and styling
    if title:
        fig.suptitle(title, fontsize=16, fontweight='bold', x=0.02, y=0.95, ha='left', va='top')
    
    # Create and position colorbar (optimized for tight bounds)
    if plot_vmax > plot_vmin:  # Only add colorbar if there's a range
        sm = mpl.cm.ScalarMappable(
            norm=mpl.colors.Normalize(vmin=plot_vmin, vmax=plot_vmax),
            cmap=cmap
        )
        
        # Position colorbar more precisely to avoid affecting map bounds
        cbar_ax = fig.add_axes([0.92, 0.15, 0.02, 0.7])  # [left, bottom, width, height] - thinner, further right
        cbar = fig.colorbar(sm, cax=cbar_ax)
        
        # Style the colorbar
        cbar.ax.tick_params(labelsize=10, colors='#333333')
        cbar.outline.set_edgecolor('#666666')
        cbar.outline.set_linewidth(0.5)
        
        # Add colorbar label
        if label:
            cbar.set_label(label, rotation=90, labelpad=12, fontsize=11, color='#333333')
    
    # Add note at bottom if provided
    if note:
        fig.text(0.02, 0.02, note, ha="left", va="bottom", fontsize=9, 
                color='#666666', style='italic', wrap=True)
    
    # Save with optimized settings for maximum data area
    plt.savefig(fname, bbox_inches="tight", dpi=300, facecolor='white', 
                edgecolor='none', pad_inches=0.02)  # Minimal padding
    plt.close(fig)  # Close to free memory
    print(f"Map saved: {fname}")

# === Main Script Logic ===
def main() -> None:
    """
    Main function to load data, process it, and generate maps.
    """
    print(f"Working directory structure:")
    print(f"  Script dir: {SCRIPT_DIR}")
    print(f"  Analysis dir: {ANALYSIS_DIR}")
    print(f"  Data dir: {DATA_DIR}")
    print(f"  GIS dir: {GIS_DIR}")
    print(f"  Output dir: {MAPS_DIR}")
    
    # === 1. Load Data ===
    csv_file_path = DATA_DIR / CSV_FILENAME
    geojson_file_path = GIS_DIR / GEOJSON_FILENAME

    print(f"\nLoading data files:")
    print(f"  CSV: {csv_file_path}")
    print(f"  GeoJSON: {geojson_file_path}")

    try:
        df_raw = pd.read_csv(csv_file_path, dtype=str)
        print(f"  ✓ Loaded CSV with {len(df_raw)} rows")
        
        gdf = gpd.read_file(geojson_file_path)
        print(f"  ✓ Loaded GeoJSON with {len(gdf)} features")
        
    except FileNotFoundError as e:
        print(f"❌ Error: Input file not found. {e}")
        return
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    # === 2. Data Filtering and Preprocessing ===
    print(f"\nData preprocessing and filtering:")
    
    # Filter out summary/aggregate rows from CSV (but keep county summaries for Zone 1)
    summary_precinct_ids = ['multnomah', 'grand_total', '']
    df = df_raw[~df_raw[COL_PRECINCT_DF].isin(summary_precinct_ids)].copy()
    print(f"  ✓ Filtered CSV: {len(df_raw)} → {len(df)} rows (removed {len(df_raw) - len(df)} summary rows)")
    
    # Separate regular precincts from county summary rows
    county_summaries = df[df[COL_PRECINCT_DF].isin(['clackamas', 'washington'])]
    regular_precincts = df[~df[COL_PRECINCT_DF].isin(['clackamas', 'washington'])]
    
    print(f"  📊 Regular precincts: {len(regular_precincts)}")
    print(f"  📊 County summary rows: {len(county_summaries)} ({county_summaries[COL_PRECINCT_DF].tolist()})")
    
    # Separate Zone 1 participants from non-participants (only for regular precincts)
    zone1_participants = regular_precincts[regular_precincts[COL_PARTICIPATED].astype(str).str.lower() == 'true'] if COL_PARTICIPATED in regular_precincts.columns else regular_precincts
    non_participants = regular_precincts[regular_precincts[COL_PARTICIPATED].astype(str).str.lower() == 'false'] if COL_PARTICIPATED in regular_precincts.columns else pd.DataFrame()
    
    print(f"  📊 Zone 1 participants: {len(zone1_participants)} precincts")
    print(f"  📊 Non-participants: {len(non_participants)} precincts")
    print(f"  📊 Total Multnomah precincts: {len(regular_precincts)} precincts")
    
    print(f"  CSV precinct column: {df[COL_PRECINCT_DF].dtype}")
    print(f"  GeoJSON precinct column: {gdf[COL_PRECINCT_GDF].dtype}")
    
    # Robust join (strip zeros, lower, strip spaces)
    df[COL_PRECINCT_DF] = df[COL_PRECINCT_DF].astype(str).str.lstrip('0').str.strip().str.lower()
    gdf[COL_PRECINCT_GDF] = gdf[COL_PRECINCT_GDF].astype(str).str.lstrip('0').str.strip().str.lower()
    
    print(f"  Sample CSV precincts: {df[COL_PRECINCT_DF].head().tolist()}")
    print(f"  Sample GeoJSON precincts: {gdf[COL_PRECINCT_GDF].head().tolist()}")
    
    # Analyze matching before merge
    csv_precincts = set(df[COL_PRECINCT_DF].unique())
    geo_precincts = set(gdf[COL_PRECINCT_GDF].unique())
    
    print(f"  Unique CSV precincts: {len(csv_precincts)}")
    print(f"  Unique GeoJSON precincts: {len(geo_precincts)}")
    print(f"  Intersection: {len(csv_precincts & geo_precincts)}")
    
    csv_only = csv_precincts - geo_precincts
    geo_only = geo_precincts - csv_precincts
    if csv_only:
        print(f"  ⚠️  CSV-only precincts: {sorted(csv_only)[:5]}{'...' if len(csv_only) > 5 else ''}")
    if geo_only:
        print(f"  ⚠️  GeoJSON-only precincts: {sorted(geo_only)[:5]}{'...' if len(geo_only) > 5 else ''}")
    
    gdf_merged = gdf.merge(df, left_on=COL_PRECINCT_GDF, right_on=COL_PRECINCT_DF, how='left')
    print(f"  ✓ Merged data: {len(gdf_merged)} features")

    # COORDINATE VALIDATION AND REPROJECTION
    print(f"\n🗺️ Coordinate System Processing:")
    gdf_merged = validate_and_reproject_to_wgs84(gdf_merged, "merged election data")
    
    # OPTIMIZE PROPERTIES FOR WEB
    gdf_merged = optimize_geojson_properties(gdf_merged)

    # Check for unmatched precincts
    matched = gdf_merged[~gdf_merged[COL_PRECINCT_DF].isna()]
    unmatched = gdf_merged[gdf_merged[COL_PRECINCT_DF].isna()]
    print(f"  ✓ Matched features: {len(matched)}")
    if len(unmatched) > 0:
        print(f"  ⚠️  Unmatched features: {len(unmatched)}")
        print(f"     Example unmatched GeoJSON precincts: {unmatched[COL_PRECINCT_GDF].head().tolist()}")

    # Clean numeric columns
    cols_to_clean = [COL_TOTAL_VOTES, COL_TURNOUT, COL_PPS_COVERAGE, COL_SPLITT, COL_CAVAGNOLO, 
                     COL_SPLITT_PCT, COL_CAVAGNOLO_PCT, COL_DEM_ADVANTAGE, COL_ENGAGEMENT, 
                     COL_DEM_REG, COL_REP_REG, 'margin_pct', 'vote_margin']
    for col in cols_to_clean:
        if col in gdf_merged.columns:
            # Percentage columns are already in decimal format in enriched dataset
            gdf_merged[col] = clean_numeric(gdf_merged[col], is_percent=False)
            valid_count = gdf_merged[col].notna().sum()
            if valid_count > 0:
                print(f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.3f} - {gdf_merged[col].max():.3f}")
            else:
                print(f"  ⚠️  Column {col}: No valid values found")
        else:
            print(f"  ⚠️  Warning: Column '{col}' not found in merged GeoDataFrame. Skipping cleaning.")
    
    # Handle categorical columns (don't need numeric cleaning)
    categorical_cols = [COL_PARTICIPATED, COL_POLITICAL_LEAN, COL_COMPETITIVENESS]
    for col in categorical_cols:
        if col in gdf_merged.columns:
            # Special handling for boolean columns that may be stored as strings
            if col == COL_PARTICIPATED:
                gdf_merged[col] = gdf_merged[col].astype(str).str.lower().map({'true': True, 'false': False})
            
            value_counts = gdf_merged[col].value_counts()
            print(f"  ✓ {col} distribution: {dict(value_counts)}")
        else:
            print(f"  ⚠️  Warning: Column '{col}' not found in merged GeoDataFrame.")

    # === Competition Metrics (Already Calculated in Enriched Dataset) ===
    print(f"\nAnalyzing pre-calculated competition metrics:")
    
    # The enriched dataset already has margin_pct, competitiveness, leading_candidate calculated
    # We just need to verify they exist and provide summary statistics
    
    if 'margin_pct' in gdf_merged.columns:
        margin_stats = gdf_merged[gdf_merged['margin_pct'].notna()]['margin_pct']
        if len(margin_stats) > 0:
            print(f"  ✓ Vote margins available: median {margin_stats.median():.1%}, range {margin_stats.min():.1%} - {margin_stats.max():.1%}")
    
    if COL_COMPETITIVENESS in gdf_merged.columns:
        comp_stats = gdf_merged[COL_COMPETITIVENESS].value_counts()
        print(f"  📊 Competitiveness distribution: {dict(comp_stats)}")
    
    if 'leading_candidate' in gdf_merged.columns:
        leader_stats = gdf_merged['leading_candidate'].value_counts()
        print(f"  📊 Leading candidate distribution: {dict(leader_stats)}")
    
    # Summary of Zone 1 vs Non-Zone 1
    if COL_PARTICIPATED in gdf_merged.columns:
        participated_count = gdf_merged[gdf_merged[COL_PARTICIPATED] == True].shape[0]
        not_participated_count = gdf_merged[gdf_merged[COL_PARTICIPATED] == False].shape[0]
        print(f"  📊 Zone 1 participation: {participated_count} participated, {not_participated_count} did not participate")

    # === 3. Save Merged GeoJSON ===
    output_geojson_path = GIS_DIR / OUTPUT_GEOJSON
    try:
        print(f"\n💾 Saving optimized GeoJSON for web use:")
        
        # Ensure we have proper CRS before saving
        if gdf_merged.crs is None:
            print("  🔧 Setting WGS84 CRS for output")
            gdf_merged = gdf_merged.set_crs('EPSG:4326')
        
        # Calculate summary statistics for metadata
        zone1_features = gdf_merged[gdf_merged.get('participated_election', False) == True] if 'participated_election' in gdf_merged.columns else gdf_merged
        total_votes_cast = zone1_features['cnt_total_votes'].sum() if 'cnt_total_votes' in zone1_features.columns else 0
        
        # Save with proper driver options for web consumption
        gdf_merged.to_file(
            output_geojson_path, 
            driver='GeoJSON'
            # crs='EPSG:4326'  # Don't pass CRS parameter with pyogrio engine
        )
        
        # Add metadata to the saved GeoJSON file
        with open(output_geojson_path, 'r') as f:
            geojson_data = json.load(f)
        
        # Add comprehensive metadata
        geojson_data['crs'] = {
            'type': 'name',
            'properties': {
                'name': 'urn:ogc:def:crs:OGC:1.3:CRS84'  # Standard web-friendly CRS identifier
            }
        }
        
        # Add metadata object
        geojson_data['metadata'] = {
            'title': '2025 Zone 1 Election Results',
            'description': 'Interactive map of 2025 Zone 1 election results with voter registration data',
            'source': '2025 Zone 1 Election Results with Voter Registration Data',
            'created': '2025-01-22',
            'crs': 'EPSG:4326',
            'coordinate_system': 'WGS84 Geographic',
            'features_count': len(gdf_merged),
            'zone1_features': len(zone1_features) if len(zone1_features) > 0 else 0,
            'total_votes_cast': int(total_votes_cast) if not pd.isna(total_votes_cast) else 0,
            'data_sources': [
                'Multnomah County Elections Division',
                'Oregon Secretary of State Voter Registration'
            ],
            'processing_notes': [
                'Coordinates reprojected to WGS84 for web compatibility',
                'Properties optimized for vector tile generation',
                'Geometry validated and fixed where necessary'
            ]
        }
        
        # Save the enhanced GeoJSON
        with open(output_geojson_path, 'w') as f:
            json.dump(geojson_data, f, separators=(',', ':'))  # Compact formatting for web
        
        print(f"  ✓ Saved optimized GeoJSON: {output_geojson_path}")
        print(f"  📊 Features: {len(gdf_merged)}, CRS: EPSG:4326 (WGS84)")
        print(f"  🗳️ Zone 1 features: {len(zone1_features)}, Total votes: {int(total_votes_cast):,}")
        
    except Exception as e:
        print(f"  ❌ Error saving GeoJSON: {e}")
        return

    # === 4. Generate Maps ===
    print(f"\nGenerating maps:")
    
    # 1. Zone 1 Participation Map
    if COL_PARTICIPATED in gdf_merged.columns:
        # Create a numeric version for plotting
        gdf_merged['participated_numeric'] = gdf_merged[COL_PARTICIPATED].astype(int)
        
        tufte_map(
            gdf_merged, 'participated_numeric',
            fname=MAPS_DIR / 'zone1_participation.png',
            cmap='RdYlGn',
            title='Zone 1 Election Participation by Geographic Feature',
            label='Participated in Election',
            vmin=0,
            vmax=1,
            note="Green areas participated in Zone 1 election, red areas did not"
        )
    
    # 2. Political Lean (All Multnomah Features)
    if COL_POLITICAL_LEAN in gdf_merged.columns:
        # Create numeric mapping for political lean
        lean_mapping = {'Strong Rep': 1, 'Lean Rep': 2, 'Competitive': 3, 'Lean Dem': 4, 'Strong Dem': 5}
        gdf_merged['political_lean_numeric'] = gdf_merged[COL_POLITICAL_LEAN].map(lean_mapping)
        
        tufte_map(
            gdf_merged, 'political_lean_numeric',
            fname=MAPS_DIR / 'political_lean_all_precincts.png',
            cmap='RdBu',
            title='Political Lean by Voter Registration (All Multnomah)',
            label='Political Lean',
            vmin=1,
            vmax=5,
            note="Based on voter registration patterns. Red=Republican lean, Blue=Democratic lean"
        )
    
    # 3. Democratic Registration Advantage
    if COL_DEM_ADVANTAGE in gdf_merged.columns:
        tufte_map(
            gdf_merged, COL_DEM_ADVANTAGE,
            fname=MAPS_DIR / 'democratic_advantage_registration.png',
            cmap='RdBu',
            title='Democratic Registration Advantage (All Multnomah)',
            label='Democratic Advantage',
            diverging=True,
            note="Blue areas have more Democratic registrations, red areas more Republican"
        )
    
    # 4. Total votes (Zone 1 only)
    if COL_TOTAL_VOTES in gdf_merged.columns and not gdf_merged[COL_TOTAL_VOTES].isnull().all():
        has_votes = gdf_merged[gdf_merged[COL_PARTICIPATED] == True]
        print(f"  📊 Total votes: {len(has_votes)} features with election data")
        
        tufte_map(
            gdf_merged, COL_TOTAL_VOTES,
            fname=MAPS_DIR / 'total_votes_zone1.png',
            cmap='Oranges',
            title='Total Votes by Geographic Feature (2025 Zone 1 Election)',
            label='Number of Votes',
            vmin=0,
            zoom_to_data=True,
            note=f"Data available for {len(has_votes)} Zone 1 features. Zoomed to election area."
        )
    
    # 5. Voter turnout (Zone 1 only)
    if COL_TURNOUT in gdf_merged.columns and not gdf_merged[COL_TURNOUT].isnull().all():
        has_turnout = gdf_merged[gdf_merged[COL_TURNOUT].notna() & (gdf_merged[COL_TURNOUT] > 0)]
        print(f"  📊 Turnout: {len(has_turnout)} features with turnout data")
        
        tufte_map(
            gdf_merged, COL_TURNOUT,
            fname=MAPS_DIR / 'voter_turnout_zone1.png',
            cmap='Blues',
            title='Voter Turnout by Geographic Feature (2025 Zone 1 Election)',
            label='Turnout Rate',
            vmin=0,
            vmax=0.4,
            zoom_to_data=True,
            note="Source: Multnomah County Elections Division. Zoomed to Zone 1 election area."
        )
    
    # 6. Splitt Vote Share (Zone 1 only)
    if COL_SPLITT_PCT in gdf_merged.columns and not gdf_merged[COL_SPLITT_PCT].isnull().all():
        has_data = gdf_merged[gdf_merged[COL_SPLITT_PCT].notna()]
        print(f"  📊 Splitt vote share: {len(has_data)} features with data")
        
        tufte_map(
            gdf_merged, COL_SPLITT_PCT,
            fname=MAPS_DIR / 'splitt_vote_share.png',
            cmap='Greens',
            title='Splitt Vote Share by Geographic Feature (2025 Zone 1 Election)',
            label='Vote Share',
            vmin=0.5,
            vmax=0.9,
            zoom_to_data=True,
            note="Shows Splitt's performance in Zone 1 features. Zoomed to election area."
        )
    
    # 7. Engagement Score (Zone 1 participants only)
    if COL_ENGAGEMENT in gdf_merged.columns and not gdf_merged[COL_ENGAGEMENT].isnull().all():
        has_engagement = gdf_merged[gdf_merged[COL_ENGAGEMENT].notna()]
        print(f"  📊 Engagement score: {len(has_engagement)} features with data")
        
        # Use data-driven range for better contrast
        engagement_data = has_engagement[COL_ENGAGEMENT].dropna()
        q25, q75 = engagement_data.quantile([0.25, 0.75])
        
        tufte_map(
            gdf_merged, COL_ENGAGEMENT,
            fname=MAPS_DIR / 'engagement_score.png',
            cmap='viridis',
            title='Civic Engagement Score by Geographic Feature (Zone 1 Election)',
            label='Engagement Score',
            vmin=q25,
            vmax=q75,
            zoom_to_data=True,
            note="Higher scores = more diverse voter registration + higher turnout. Zoomed to election area."
        )
    
    # 8. Vote margin/competition (Zone 1 only)
    if 'margin_pct' in gdf_merged.columns and not gdf_merged['margin_pct'].isnull().all():
        has_margin = gdf_merged[gdf_merged['margin_pct'].notna()]
        print(f"  📊 Vote Margin: {len(has_margin)} features with data")
        
        tufte_map(
            gdf_merged, 'margin_pct',
            fname=MAPS_DIR / 'vote_margin_zone1.png',
            cmap='plasma',
            title='Vote Margin by Geographic Feature (2025 Zone 1 Election)',
            label='Vote Margin',
            vmin=0.3,
            vmax=0.8,
            zoom_to_data=True,
            note="Higher values = larger victory margins. Zoomed to Zone 1 election area."
        )
    
    # 9. BONUS: Political Lean vs Zone 1 Participation Analysis
    if COL_POLITICAL_LEAN in gdf_merged.columns and COL_PARTICIPATED in gdf_merged.columns:
        # Create a combined metric for analytical insight
        gdf_merged['lean_participation'] = gdf_merged.apply(lambda row: 
            f"{row[COL_POLITICAL_LEAN]} - {'Zone 1' if row[COL_PARTICIPATED] else 'No Zone 1'}", axis=1)
        
        # Create numeric mapping for visualization
        combined_mapping = {
            'Strong Rep - No Zone 1': 1, 'Strong Rep - Zone 1': 2,
            'Lean Rep - No Zone 1': 3, 'Lean Rep - Zone 1': 4, 
            'Competitive - No Zone 1': 5, 'Competitive - Zone 1': 6,
            'Lean Dem - No Zone 1': 7, 'Lean Dem - Zone 1': 8,
            'Strong Dem - No Zone 1': 9, 'Strong Dem - Zone 1': 10
        }
        gdf_merged['lean_participation_numeric'] = gdf_merged['lean_participation'].map(combined_mapping)
        
        tufte_map(
            gdf_merged, 'lean_participation_numeric',
            fname=MAPS_DIR / 'political_lean_vs_zone1_participation.png',
            cmap='RdBu',
            title='Political Lean vs Zone 1 Election Participation',
            label='Lean + Participation',
            vmin=1,
            vmax=10,
            note="Analysis of voter registration patterns vs Zone 1 election participation. Red=Rep, Blue=Dem, Darker=participated"
        )
        
        print(f"  📊 Political Lean vs Participation: Combined analysis map created")

    print(f"\n✅ Script completed successfully!")
    print(f"   Maps saved to: {MAPS_DIR}")
    print(f"   GeoJSON saved to: {output_geojson_path}")
    print(f"   Summary: {len(matched)} features with election data out of {len(gdf_merged)} total features")
    
    # Comprehensive summary statistics using enriched dataset
    if len(matched) > 0:
        print(f"\n📊 Comprehensive Election Analysis:")
        
        # Zone 1 Election Results - FIXED: Sum by unique precinct + county summaries
        zone1_data = gdf_merged[gdf_merged[COL_PARTICIPATED] == True]
        if len(zone1_data) > 0:
            # Get unique precinct totals to avoid double-counting geographic splits
            unique_zone1_precincts = zone1_data.drop_duplicates(subset=[COL_PRECINCT_DF])
            
            # Add county summary votes (from Clackamas and Washington portions of Zone 1)
            if len(county_summaries) > 0:
                # Clean county summary data
                county_clean = county_summaries.copy()
                for col in [COL_TOTAL_VOTES, COL_SPLITT, COL_CAVAGNOLO]:
                    if col in county_clean.columns:
                        county_clean[col] = clean_numeric(county_clean[col], is_percent=False)
                county_votes = county_clean[[COL_TOTAL_VOTES, COL_SPLITT, COL_CAVAGNOLO]].sum()
            else:
                county_votes = pd.Series({COL_TOTAL_VOTES: 0, COL_SPLITT: 0, COL_CAVAGNOLO: 0})
            
            precinct_total_votes = unique_zone1_precincts[COL_TOTAL_VOTES].sum()
            precinct_splitt_total = unique_zone1_precincts[COL_SPLITT].sum() 
            precinct_cavagnolo_total = unique_zone1_precincts[COL_CAVAGNOLO].sum()
            
            # CORRECT ZONE 1 TOTALS including all counties
            total_votes_cast = precinct_total_votes + county_votes[COL_TOTAL_VOTES]
            splitt_total = precinct_splitt_total + county_votes[COL_SPLITT]
            cavagnolo_total = precinct_cavagnolo_total + county_votes[COL_CAVAGNOLO]
            
            avg_turnout = unique_zone1_precincts[unique_zone1_precincts[COL_TURNOUT] > 0][COL_TURNOUT].mean()
            
            print(f"   🗳️  Zone 1 Election Results (CORRECTED with all counties):")
            print(f"      • Total votes cast: {total_votes_cast:,.0f}")
            print(f"      • Splitt: {splitt_total:,.0f} votes ({splitt_total/total_votes_cast:.1%})")
            print(f"      • Cavagnolo: {cavagnolo_total:,.0f} votes ({cavagnolo_total/total_votes_cast:.1%})")
            print(f"      • Average turnout (Multnomah only): {avg_turnout:.1%}")
            print(f"      • Unique participating precincts: {len(unique_zone1_precincts)}")
            print(f"      • Geographic features in Zone 1: {len(zone1_data)}")
            if len(county_summaries) > 0:
                print(f"      • County summary additions: {county_summaries[COL_PRECINCT_DF].tolist()}")
        
        # Political Registration Analysis (All Multnomah) - FIXED: Use unique precincts
        all_features = gdf_merged[gdf_merged[COL_DEM_REG].notna()] if COL_DEM_REG in gdf_merged.columns else gdf_merged
        if len(all_features) > 0 and COL_POLITICAL_LEAN in gdf_merged.columns:
            # Get unique precincts for registration analysis
            unique_precincts = all_features.drop_duplicates(subset=[COL_PRECINCT_DF])
            
            lean_summary = unique_precincts[COL_POLITICAL_LEAN].value_counts()
            avg_dem_advantage = unique_precincts[COL_DEM_ADVANTAGE].mean() if COL_DEM_ADVANTAGE in unique_precincts.columns else 0
            
            print(f"\n   🏛️  Political Registration (All Multnomah):")
            print(f"      • Total unique precincts analyzed: {len(unique_precincts)}")
            print(f"      • Total geographic features: {len(all_features)}")
            print(f"      • Average Democratic advantage: {avg_dem_advantage:.1%}")
            print(f"      • Political lean distribution (by precinct):")
            for lean, count in lean_summary.items():
                print(f"        - {lean}: {count} precincts")
        
        # Competition Analysis - FIXED: Use unique precincts for summary stats
        if 'leading_candidate' in gdf_merged.columns:
            unique_zone1 = zone1_data.drop_duplicates(subset=[COL_PRECINCT_DF])
            leader_summary = unique_zone1['leading_candidate'].value_counts() if len(unique_zone1) > 0 else pd.Series()
            print(f"\n   ⚔️  Competition Analysis (Zone 1 by precinct):")
            for candidate, count in leader_summary.items():
                if candidate != 'No Data':
                    print(f"      • {candidate}: won {count} precincts")
        
        if COL_COMPETITIVENESS in gdf_merged.columns:
            unique_zone1 = zone1_data.drop_duplicates(subset=[COL_PRECINCT_DF])
            comp_summary = unique_zone1[COL_COMPETITIVENESS].value_counts() if len(unique_zone1) > 0 else pd.Series()
            print(f"      • Competitiveness (by precinct):")
            for comp_level, count in comp_summary.items():
                if comp_level != 'No Election Data':
                    print(f"        - {comp_level}: {count} precincts")
        
        # Engagement Analysis - FIXED: Use unique precincts
        if COL_ENGAGEMENT in gdf_merged.columns:
            unique_engagement = zone1_data.drop_duplicates(subset=[COL_PRECINCT_DF])
            engagement_data = unique_engagement[unique_engagement[COL_ENGAGEMENT].notna()]
            if len(engagement_data) > 0:
                avg_engagement = engagement_data[COL_ENGAGEMENT].mean()
                high_engagement = (engagement_data[COL_ENGAGEMENT] > avg_engagement).sum()
                print(f"\n   💪 Civic Engagement (Zone 1 Participants by precinct):")
                print(f"      • Average engagement score: {avg_engagement:.3f}")
                print(f"      • High-engagement precincts: {high_engagement}/{len(engagement_data)}")
        
        # Geographic Coverage
        unique_all = gdf_merged.drop_duplicates(subset=[COL_PRECINCT_DF])
        unique_zone1_total = zone1_data.drop_duplicates(subset=[COL_PRECINCT_DF]) if len(zone1_data) > 0 else []
        
        print(f"\n   🗺️  Geographic Coverage:")
        print(f"      • Total Multnomah precincts: {len(unique_all)}")
        print(f"      • Total geographic features: {len(gdf_merged)}")
        print(f"      • Zone 1 participating precincts: {len(unique_zone1_total)}")
        print(f"      • Zone 1 geographic features: {len(zone1_data)}")
        print(f"      • Features per precinct (avg): {len(gdf_merged)/len(unique_all):.1f}")
        print(f"      • Data completeness: {len(matched)}/{len(gdf_merged)} features matched to geography")

    print(f"\n🗺️  Generated maps:")
    print(f"   1. Zone 1 Participation Map")
    print(f"   2. Political Lean (All Multnomah)")
    print(f"   3. Democratic Registration Advantage")
    print(f"   4. Total votes (Zone 1 only)")
    print(f"   5. Voter turnout (Zone 1 only)")
    print(f"   6. Splitt Vote Share (Zone 1 only)")
    print(f"   7. Engagement Score (Zone 1 participants only)")
    print(f"   8. Vote margin/competition (Zone 1 only)")
    print(f"   9. Political Lean vs Zone 1 Participation Analysis")

if __name__ == "__main__":
    main()
