#!/usr/bin/env python3
"""
plot_pps_no_kids.py

Create an interactive choropleth map of the percentage of households
without children under 18 within the PPS boundary.

Requirements:
    pip install geopandas pandas folium

Place your files in the `data/` directory:
    - PPS_District_Data.geojson
    - OR_block_groups_2024.geojson
    - acs_b11001_blockgroup.csv  (columns: geoid,total_hh,no_kids_hh)
"""

import geopandas as gpd
import pandas as pd
import folium

# ---- CONFIG ----
DISTRICT_GEOJSON = "data/PPS_District_Data.geojson"
BG_GEOJSON       = "data/OR_block_groups_2024.geojson"
CENSUS_CSV       = "data/acs_b11001_blockgroup.csv"
OUTPUT_MAP       = "pps_no_kids_map.html"

# 1. Load data
district = gpd.read_file(DISTRICT_GEOJSON).to_crs(epsg=4326)
bg = gpd.read_file(BG_GEOJSON).to_crs(epsg=4326)
census = pd.read_csv(CENSUS_CSV, dtype={"geoid": str})

# 2. Compute percentage
census["pct_no_kids"] = 100 * census["no_kids_hh"] / census["total_hh"]

# 3. Merge census into BG geometries
bg["geoid"] = bg["GEOID"]  # adjust if field name differs
merged = bg.merge(census, on="geoid")

# 4. Spatial filter to district
merged = gpd.sjoin(merged, district, how="inner", predicate="intersects")
merged = merged.drop(columns=["index_right"])

# 5. Build Folium map
center = [
    district.geometry.centroid.y.mean(),
    district.geometry.centroid.x.mean()
]
m = folium.Map(location=center, zoom_start=12, tiles="CartoDB Positron")

folium.Choropleth(
    geo_data=merged.__geo_interface__,
    name="No Children %", 
    data=merged,
    columns=["geoid", "pct_no_kids"],
    key_on="feature.properties.geoid",
    fill_color="YlOrRd",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="% households without children <18"
).add_to(m)

folium.GeoJson(
    district.__geo_interface__,
    name="PPS Boundary",
    style_function=lambda f: {"color": "black", "weight": 2, "fill": False}
).add_to(m)

folium.LayerControl().add_to(m)

# 6. Save
m.save(OUTPUT_MAP)
print(f"Interactive map saved to {OUTPUT_MAP}")
