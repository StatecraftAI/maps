#!/usr/bin/env python3
"""
Generate an interactive Leaflet-based heatmap of 600K+ points using Folium,
export CSVs for points inside/outside the region.

Usage:
    python interactive_heatmap.py

Dependencies:
    pip install pandas geopandas shapely folium loguru
"""
import os
import sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from loguru import logger
import folium
from folium.plugins import HeatMap

# Paths
DATA_DIR         = 'data'
GEOJSON_PATH     = os.path.join(DATA_DIR, 'pps_district_data.geojson')
CSV_PATH         = os.path.join(DATA_DIR, 'voters.csv')
INSIDE_CSV_PATH  = os.path.join(DATA_DIR, 'voters_inside_pps.csv')
OUTSIDE_CSV_PATH = os.path.join(DATA_DIR, 'voters_outside_pps.csv')
OUT_HTML_PATH    = 'interactive_heatmap.html'

# 1. Load region geometry
logger.info(f"Loading region from {GEOJSON_PATH}")
regions = gpd.read_file(GEOJSON_PATH)
logger.info(f"Loaded {len(regions)} region features.")

# 2. Read voters CSV
logger.info(f"Reading voters from {CSV_PATH}")
df = pd.read_csv(CSV_PATH, low_memory=False)
# standardize and rename
cols = df.columns.str.strip().str.lower().str.replace(r'[^0-9a-z]+','_',regex=True)
df.columns = cols
rename = {c:'latitude' for c in cols if c in ('lat','latitude')}
rename.update({c:'longitude' for c in cols if c in ('lon','lng','longitude')})
if rename:
    df = df.rename(columns=rename)
    logger.info(f"Renamed columns: {rename}")
df = df.dropna(subset=['latitude','longitude'])
logger.info(f"Retained {len(df):,} valid points.")

# 3. Classify inside/outside
logger.info("Classifying points inside vs outside region.")
gdf = gpd.GeoDataFrame(df, geometry=[Point(xy) for xy in zip(df.longitude, df.latitude)], crs=regions.crs)
union = regions.geometry.union_all()
gdf['inside'] = gdf.geometry.within(union)
inside_count = int(gdf.inside.sum())
outside_count = len(gdf) - inside_count
logger.info(f"Points inside: {inside_count:,}; outside: {outside_count:,}.")
# export
logger.info(f"Exporting inside to {INSIDE_CSV_PATH}")
gdf[gdf.inside].drop(columns='geometry').to_csv(INSIDE_CSV_PATH, index=False)
logger.info(f"Exporting outside to {OUTSIDE_CSV_PATH}")
gdf[~gdf.inside].drop(columns='geometry').to_csv(OUTSIDE_CSV_PATH, index=False)

# 4. Build Folium Map
center = [gdf.latitude.mean(), gdf.longitude.mean()]
logger.info(f"Centering map at {center}")
m = folium.Map(location=center, zoom_start=10, tiles='cartodbpositron')
# overlay boundary
folium.GeoJson(
    regions.__geo_interface__,
    name='Region',
    style_function=lambda f: {'color':'blue','weight':2,'fill':False}
).add_to(m)
# heatmap of all voters
heat_data = gdf[['latitude','longitude']].values.tolist()
logger.info("Adding HeatMap layer of all voters.")
HeatMap(heat_data, radius=10, blur=15, max_zoom=12).add_to(m)

# 5. Save
m.save(OUT_HTML_PATH)
logger.success(f"Interactive heatmap saved to {OUT_HTML_PATH}")

print(f"CSV outputs: {INSIDE_CSV_PATH}, {OUTSIDE_CSV_PATH}")
print(f"Interactive map: {OUT_HTML_PATH}")
