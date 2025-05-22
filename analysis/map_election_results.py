import os
import pathlib
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# === 1. Load Data ===
DATA_DIR = pathlib.Path(__file__).parent.resolve() / 'data'
CSV_PATH = DATA_DIR / 'total_votes_enriched.csv'
GEOJSON_PATH = DATA_DIR / 'multnomah_elections_precinct_split_2024.geojson'

# Read files
df = pd.read_csv(CSV_PATH, dtype=str)
gdf = gpd.read_file(GEOJSON_PATH)

# === 2. Prep for Join ===
df['precinct'] = df['precinct'].astype(str).str.lstrip('0').str.strip().str.lower()
gdf['Precinct'] = gdf['Precinct'].astype(str).str.lstrip('0').str.strip().str.lower()

# Merge attribute data into spatial data
gdf_merged = gdf.merge(df, left_on='Precinct', right_on='precinct', how='left')

# === 3. Clean/convert columns for mapping ===
def clean_numeric(series, is_percent=False):
    s = series.astype(str).str.replace(',', '').str.replace('%', '').str.strip()
    vals = pd.to_numeric(s, errors='coerce')
    if is_percent:
        vals = vals / 100.0
    return vals

for col in ['cnt_total_votes', 'rat_turnout', 'rat_covered_pps']:
    if col in gdf_merged.columns:
        is_percent = col.startswith('pct_') or col.startswith('rat_')
        gdf_merged[col] = clean_numeric(gdf_merged[col], is_percent=is_percent)

# === 4. Plot and save high-res maps ===
def save_choropleth(gdf, column, cmap, title, fname):
    if column not in gdf.columns:
        print(f"Column '{column}' not found. Skipping plot.")
        return
    ax = gdf.plot(
        column=column,
        cmap=cmap,
        legend=True,
        edgecolor='black',
        missing_kwds={"color": "lightgrey", "label": "No Data"},
        figsize=(10, 8)
    )
    ax.set_title(title)
    ax.axis('off')
    plt.tight_layout()
    plt.savefig(fname, dpi=300)
    plt.close()

save_choropleth(
    gdf_merged, 'cnt_total_votes', 'viridis',
    'Total Votes by Precinct', DATA_DIR / 'total_votes_by_precinct.png'
)
save_choropleth(
    gdf_merged, 'rat_turnout', 'plasma',
    'Voter Turnout by Precinct', DATA_DIR / 'turnout_by_precinct.png'
)
save_choropleth(
    gdf_merged, 'rat_covered_pps', 'YlGn',
    'PPS Coverage Rate by Precinct', DATA_DIR / 'pps_coverage_rate_by_precinct.png'
)

# (Optional) Save enriched GeoJSON for use in QGIS/web maps
gdf_merged.to_file(DATA_DIR / "enriched_precincts.geojson", driver="GeoJSON")
