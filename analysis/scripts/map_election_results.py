import pathlib
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib as mpl
from typing import Union, Optional, Any

# Directories and File Paths
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
ANALYSIS_DIR = SCRIPT_DIR.parent
DATA_DIR = ANALYSIS_DIR / 'data'
GIS_DIR = ANALYSIS_DIR / 'gis'
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
COL_TURNOUT = 'rat_turnout'
COL_PPS_COVERAGE = 'rat_covered_pps'

# Candidate columns
COL_CHRISTY = 'cnt_christy_splitt'
COL_KEN = 'cnt_ken_cavagnolo'

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

def tufte_map(
    gdf: gpd.GeoDataFrame,
    column: str,
    fname: Union[str, pathlib.Path],
    cmap: str = 'OrRd',
    title: str = '',
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    label: str = '',
    note: Optional[str] = None
) -> None:
    """
    Generates and saves a minimalist Tufte-style map.

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
    """
    fig, ax = plt.subplots(figsize=(10, 8), dpi=300)
    
    # Determine vmin and vmax if not provided
    plot_vmin = vmin if vmin is not None else gdf[column].min()
    plot_vmax = vmax if vmax is not None else gdf[column].max()

    gdf.plot(
        column=column,
        cmap=cmap,
        linewidth=0.35,
        edgecolor="#666666",
        ax=ax,
        legend=False,
        vmin=plot_vmin,
        vmax=plot_vmax,
        missing_kwds={"color": "#f0f0f0", "edgecolor": "#cccccc", "hatch": "///"}
    )
    ax.set_axis_off()
    ax.set_title(title, fontsize=18, fontweight='bold', loc='left', pad=18)

    if note:
        plt.figtext(0.01, 0.01, note, ha="left", va="bottom", fontsize=10, color='#444')

    sm = mpl.cm.ScalarMappable(
        norm=mpl.colors.Normalize(vmin=plot_vmin, vmax=plot_vmax),
        cmap=cmap
    )
    cbar = fig.colorbar(sm, ax=ax, shrink=0.34, aspect=20, pad=0.015, anchor=(0, 0.5))
    cbar.set_label(label, rotation=90, labelpad=15, fontsize=14)
    cbar.ax.tick_params(labelsize=12)
    
    plt.savefig(fname, bbox_inches="tight", dpi=300, transparent=False)
    plt.close(fig) # Close the figure to free memory
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
    
    # Filter out summary/aggregate rows from CSV
    summary_precinct_ids = ['0', '', 'grand_total', 'clackamas', 'washington', 'multnomah']
    df = df_raw[~df_raw[COL_PRECINCT_DF].isin(summary_precinct_ids)].copy()
    print(f"  ✓ Filtered CSV: {len(df_raw)} → {len(df)} rows (removed {len(df_raw) - len(df)} summary rows)")
    
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

    # Check for unmatched precincts
    matched = gdf_merged[~gdf_merged[COL_PRECINCT_DF].isna()]
    unmatched = gdf_merged[gdf_merged[COL_PRECINCT_DF].isna()]
    print(f"  ✓ Matched precincts: {len(matched)}")
    if len(unmatched) > 0:
        print(f"  ⚠️  Unmatched precincts: {len(unmatched)}")
        print(f"     Example unmatched GeoJSON precincts: {unmatched[COL_PRECINCT_GDF].head().tolist()}")

    # Clean numeric columns
    cols_to_clean = [COL_TOTAL_VOTES, COL_TURNOUT, COL_PPS_COVERAGE, COL_CHRISTY, COL_KEN]
    for col in cols_to_clean:
        if col in gdf_merged.columns:
            # Only treat columns as percentages if they actually contain "%" symbols or are explicitly "pct_" columns
            is_percent = col.startswith('pct_')  # rat_ columns are already in decimal format
            gdf_merged[col] = clean_numeric(gdf_merged[col], is_percent=is_percent)
            valid_count = gdf_merged[col].notna().sum()
            print(f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.0f} - {gdf_merged[col].max():.0f}")
        else:
            print(f"  ⚠️  Warning: Column '{col}' not found in merged GeoDataFrame. Skipping cleaning.")
            # Optionally, create the column with NaNs if it's critical for plotting
            # gdf_merged[col] = pd.NA

    # === Calculate Competition Metrics ===
    print(f"\nCalculating competition metrics:")
    
    # Calculate vote margin (difference between top two candidates)
    gdf_merged['margin_votes'] = (gdf_merged[COL_CHRISTY] - gdf_merged[COL_KEN]).abs()
    gdf_merged['margin_pct'] = gdf_merged['margin_votes'] / gdf_merged[COL_TOTAL_VOTES]
    
    # Replace infinite values (division by zero) with NaN
    gdf_merged['margin_pct'] = gdf_merged['margin_pct'].replace([float('inf'), -float('inf')], pd.NA)
    
    # Calculate who's leading in each precinct (simplified)
    gdf_merged['leading_candidate'] = 'No Data'
    mask = gdf_merged[COL_TOTAL_VOTES] > 0
    gdf_merged.loc[mask & (gdf_merged[COL_CHRISTY] > gdf_merged[COL_KEN]), 'leading_candidate'] = 'Christy Splitt'
    gdf_merged.loc[mask & (gdf_merged[COL_KEN] > gdf_merged[COL_CHRISTY]), 'leading_candidate'] = 'Ken Cavagnolo'
    gdf_merged.loc[mask & (gdf_merged[COL_CHRISTY] == gdf_merged[COL_KEN]), 'leading_candidate'] = 'Tie'
    
    margin_stats = gdf_merged[gdf_merged['margin_pct'].notna()]['margin_pct']
    if len(margin_stats) > 0:
        print(f"  ✓ Vote margin calculated: median {margin_stats.median():.1%}, range {margin_stats.min():.1%} - {margin_stats.max():.1%}")
    
    competitive_precincts = gdf_merged[gdf_merged['margin_pct'] < 0.2]  # Less than 20% margin
    safe_precincts = gdf_merged[gdf_merged['margin_pct'] >= 0.2]  # 20%+ margin
    print(f"  📊 Competitive precincts (< 20% margin): {len(competitive_precincts)}")
    print(f"  📊 Safe precincts (≥ 20% margin): {len(safe_precincts)}")

    # === 3. Save Merged GeoJSON ===
    output_geojson_path = GIS_DIR / OUTPUT_GEOJSON
    try:
        # Save the merged geodataframe
        gdf_merged.to_file(output_geojson_path, driver='GeoJSON')
        print(f"  ✓ Saved merged results to: {output_geojson_path}")
    except Exception as e:
        print(f"  ❌ Error saving GeoJSON: {e}")

    # === 4. Generate Maps ===
    print(f"\nGenerating maps:")
    
    # Total votes
    if COL_TOTAL_VOTES in gdf_merged.columns and not gdf_merged[COL_TOTAL_VOTES].isnull().all():
        # Only include precincts with data for better color scaling
        has_data = gdf_merged[gdf_merged[COL_TOTAL_VOTES].notna() & (gdf_merged[COL_TOTAL_VOTES] > 0)]
        print(f"  📊 Total votes: {len(has_data)} precincts with data")
        
        tufte_map(
            gdf_merged, COL_TOTAL_VOTES,
            fname=MAPS_DIR / 'total_votes_by_precinct.png',
            cmap='Oranges',
            title='Total Votes by Precinct (2025 Zone 1 Election)',
            label='Number of Votes Cast',
            vmin=0,  # Start from 0 for better interpretation
            note=f"Data available for {len(has_data)} of {len(gdf_merged)} precincts"
        )
    else:
        print(f"  ⚠️  Skipping map for '{COL_TOTAL_VOTES}' due to missing data or all NaN values.")

    # Voter turnout
    if COL_TURNOUT in gdf_merged.columns and not gdf_merged[COL_TURNOUT].isnull().all():
        has_data = gdf_merged[gdf_merged[COL_TURNOUT].notna() & (gdf_merged[COL_TURNOUT] > 0)]
        print(f"  📊 Turnout: {len(has_data)} precincts with data")
        
        tufte_map(
            gdf_merged, COL_TURNOUT,
            fname=MAPS_DIR / 'voter_turnout_by_precinct.png',
            cmap='Blues',
            title='Voter Turnout by Precinct (2025 Zone 1 Election)',
            label='Turnout (Fraction of Registered Voters)',
            vmin=0,
            vmax=1,  # Cap at 100% turnout
            note="Source: Multnomah County Elections Division. Gray areas indicate no election data."
        )
    else:
        print(f"  ⚠️  Skipping map for '{COL_TURNOUT}' due to missing data or all NaN values.")

    # PPS coverage rate
    if COL_PPS_COVERAGE in gdf_merged.columns and not gdf_merged[COL_PPS_COVERAGE].isnull().all():
        has_data = gdf_merged[gdf_merged[COL_PPS_COVERAGE].notna()]
        print(f"  📊 PPS Coverage: {len(has_data)} precincts with data")
        
        tufte_map(
            gdf_merged, COL_PPS_COVERAGE,
            fname=MAPS_DIR / 'pps_coverage_rate_by_precinct.png',
            cmap='Greens',
            title='PPS Coverage Rate by Precinct',
            label='Fraction of Precinct Area Covered by PPS Boundary',
            vmin=0,
            vmax=1,  # 0-100% coverage
            note="Note: PPS coverage is based on spatial intersection of precinct and school district boundaries."
        )
    else:
        print(f"  ⚠️  Skipping map for '{COL_PPS_COVERAGE}' due to missing data or all NaN values.")

    # Vote margin/competition
    if 'margin_pct' in gdf_merged.columns and not gdf_merged['margin_pct'].isnull().all():
        has_data = gdf_merged[gdf_merged['margin_pct'].notna()]
        print(f"  📊 Vote Margin: {len(has_data)} precincts with data")
        
        tufte_map(
            gdf_merged, 'margin_pct',
            fname=MAPS_DIR / 'vote_margin_by_precinct.png',
            cmap='RdYlBu',  # Red = close race, Blue = safe
            title='Vote Margin by Precinct (2025 Zone 1 Election)',
            label='Vote Margin (Absolute Difference / Total Votes)',
            vmin=0,
            vmax=1,  # 0-100% margin
            note="Red areas indicate close races, blue areas indicate safe margins. Gray areas have no election data."
        )
    else:
        print(f"  ⚠️  Skipping map for vote margin due to missing data or all NaN values.")

    print(f"\n✅ Script completed successfully!")
    print(f"   Maps saved to: {MAPS_DIR}")
    print(f"   GeoJSON saved to: {output_geojson_path}")
    print(f"   Summary: {len(matched)} precincts with election data out of {len(gdf_merged)} total precincts")
    
    # Additional summary statistics
    if len(matched) > 0:
        print(f"\n📊 Election Summary Statistics:")
        total_votes_cast = gdf_merged[COL_TOTAL_VOTES].sum()
        christy_total = gdf_merged[COL_CHRISTY].sum() 
        ken_total = gdf_merged[COL_KEN].sum()
        print(f"   • Total votes cast: {total_votes_cast:,}")
        print(f"   • Christy Splitt: {christy_total:,} votes ({christy_total/total_votes_cast:.1%})")
        print(f"   • Ken Cavagnolo: {ken_total:,} votes ({ken_total/total_votes_cast:.1%})")
        print(f"   • Average turnout: {gdf_merged[gdf_merged[COL_TURNOUT] > 0][COL_TURNOUT].mean():.1%}")
        print(f"   • Precincts in PPS boundary: {(gdf_merged[COL_PPS_COVERAGE] > 0.5).sum()}")
        
        leading_summary = gdf_merged['leading_candidate'].value_counts()
        print(f"   • Precincts leading by candidate:")
        for candidate, count in leading_summary.items():
            if candidate != 'No Data':
                print(f"     - {candidate}: {count} precincts")
    
    print(f"\n🗺️  Generated maps:")
    print(f"   1. Total votes by precinct")
    print(f"   2. Voter turnout by precinct") 
    print(f"   3. PPS coverage rate by precinct")
    print(f"   4. Vote margin/competition by precinct")

if __name__ == "__main__":
    main()
