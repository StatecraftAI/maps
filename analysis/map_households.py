import os
import sys
import json
import pandas as pd
import geopandas as gpd
from loguru import logger
import folium
from branca.colormap import linear

# Paths
data_dir    = "data"
acs_json    = os.path.join(data_dir, "hh_no_minors_multnomah_bgs.json")
bg_shp      = os.path.join(data_dir, "tl_2022_41_bg.shp")
pps_geojson = os.path.join(data_dir, "pps_district_data.geojson")
merged_csv  = os.path.join(data_dir, "hh_no_minors_pps_bgs.csv")
report_md   = os.path.join(data_dir, "hh_no_minors_report.md")
output_html = "map_households.html"

# 1. Load Multnomah ACS JSON
logger.info(f"Loading ACS JSON from {acs_json}")
with open(acs_json) as f:
    arr = json.load(f)
header = arr[0]
records = arr[1:]
df = pd.DataFrame(records, columns=header)

# 2. Process ACS fields
df = df.rename(columns={'B11001_001E':'total_households','B11001_002E':'households_no_minors'})
df['total_households'] = df['total_households'].astype(int)
df['households_no_minors'] = df['households_no_minors'].astype(int)
df['GEOID'] = df['state'] + df['county'] + df['tract'] + df['block group']

# 3. Load BG shapefile and filter to Multnomah
gdf = gpd.read_file(bg_shp)
gdf = gdf[(gdf['STATEFP']=='41') & (gdf['COUNTYFP']=='051')].copy()

# 4. Merge ACS data
logger.info("Merging ACS data with geometries")
gdf = gdf.merge(df[['GEOID','total_households','households_no_minors']], on='GEOID', how='left')
gdf['total_households'] = gdf['total_households'].fillna(0).astype(int)
gdf['households_no_minors'] = gdf['households_no_minors'].fillna(0).astype(int)
gdf['percent_no_minors'] = gdf.apply(lambda r: round(100*r['households_no_minors']/r['total_households'],1) if r['total_households']>0 else 0, axis=1)

# 5. Filter to PPS district using projected geometry
logger.info("Filtering to PPS district")
region = gpd.read_file(pps_geojson).to_crs(epsg=3857)
gdf_proj = gdf.to_crs(epsg=3857)
union = region.geometry.union_all()
mask = gdf_proj.geometry.centroid.within(union)
gdf = gdf_proj[mask].to_crs(epsg=4326)

# 6. Export report and CSV
overall = pd.DataFrame([{
    'GEOID':'Overall',
    'total_households': gdf['total_households'].sum(),
    'households_no_minors': gdf['households_no_minors'].sum(),
    'percent_no_minors': (gdf['households_no_minors'].sum()/gdf['total_households'].sum())
}])
gdf_report = gdf[['GEOID','total_households','households_no_minors','percent_no_minors']].copy()
gdf_report['percent_no_minors'] = (gdf_report['percent_no_minors']).round(1)
gdf_report = pd.concat([gdf_report, overall], ignore_index=True)
md = gdf_report.to_markdown(index=False)
with open(report_md,'w') as f:
    f.write(md)
logger.info(f"Saved report to {report_md}")

# 7. Save CSV
gdf.drop(columns='geometry', errors='ignore').to_csv(merged_csv, index=False)
logger.info(f"Saved CSV to {merged_csv}")

# 8. Build Folium choropleth map
gdf['lat'] = gdf.geometry.centroid.y
gdf['lon'] = gdf.geometry.centroid.x
center = [gdf['lat'].mean(), gdf['lon'].mean()]
logger.info(f"Centering map at {center}")
m = folium.Map(location=center, zoom_start=12, tiles='CartoDB Dark_Matter')

# Quantile thresholds for choropleth
thresholds = list(gdf['percent_no_minors'].quantile([0,0.2,0.4,0.6,0.8,1]).round(3))
logger.info(f"Using thresholds: {thresholds}")

folium.Choropleth(
    geo_data=gdf,
    name='choropleth',
    data=gdf,
    columns=['GEOID','percent_no_minors'],
    key_on='feature.properties.GEOID',
    fill_color='YlOrRd',
    threshold_scale=thresholds,
    fill_opacity=0.33,
    line_opacity=0.2,
    legend_name='% Households without minors'
).add_to(m)

# Add region boundary in bright pink
tiles = folium.GeoJson(
    data=region.to_crs(epsg=4326).__geo_interface__,
    style_function=lambda feat: {'color':'#ff00ff','weight':3,'fillOpacity':0}
)
tiles.add_to(m)

# Add block-group boundaries in grey with tooltips
folium.GeoJson(
    data=gdf.__geo_interface__,
    style_function=lambda feat: {'color':'#888888','weight':0.5,'fillOpacity':0},
    tooltip=folium.GeoJsonTooltip(fields=['GEOID','percent_no_minors'], aliases=['GEOID:','Percent No Minors:'], localize=True)
).add_to(m)

# 10. Save
m.save(output_html)
logger.success(f"Saved choropleth map to {output_html}")
