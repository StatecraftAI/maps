# Election Data to Interactive Map Pipeline

This guide walks you through creating interactive maps from election data, from raw CSV files to web-ready vector tiles.

## 📋 Overview

This pipeline transforms election results and geographic data into beautiful, interactive web maps. Here's what it does:

1. **Merges** election data (CSV) with geographic boundaries (GeoJSON)
2. **Processes** coordinate systems and data types for web compatibility
3. **Generates** static analysis maps (PNG files)
4. **Creates** optimized GeoJSON for web display
5. **Builds** vector tiles for interactive maps
6. **Uploads** to Mapbox (optional) for hosting

## 🗂️ File Structure

```
your-project/
├── analysis/
│   ├── data/
│   │   └── elections/
│   │       └── your_election_data.csv
│   ├── geospatial/
│   │   └── your_boundaries.geojson
│   ├── maps/                    # Generated static maps
│   ├── tiles/                   # Generated vector tiles
│   └── scripts/
│       ├── map_election_results.py
│       └── create_vector_tiles.py
└── README_Election_Mapping_Pipeline.md
```

## 🔧 Prerequisites

### Required Software
```bash
# Python packages
pip install pandas geopandas matplotlib

# For vector tiles (macOS)
brew install tippecanoe

# For vector tiles (Ubuntu/Debian)
sudo apt install gdal-bin
```

### Required Data Files
1. **Election Results CSV** - with columns like:
   - `precinct` - precinct identifier
   - `cnt_total_votes` - total votes cast
   - `cnt_candidate1`, `cnt_candidate2` - vote counts by candidate
   - `turnout_rate` - voter turnout percentage

2. **Geographic Boundaries GeoJSON** - with:
   - `Precinct` - precinct identifier (must match CSV)
   - Polygon geometries for each precinct

### Optional: Mapping Service Account
- For hosting: Mapbox, MapTiler, or similar service
- Or self-host with TileServer GL

## 🚀 Step-by-Step Instructions

### Step 1: Prepare Your Data

1. **Place your election CSV** in `analysis/data/elections/`
2. **Place your boundaries GeoJSON** in `analysis/geospatial/`
3. **Update file names** in `map_election_results.py`:

   ```python
   CSV_FILENAME = "your_election_data.csv"
   GEOJSON_FILENAME = "your_boundaries.geojson"
   ```

### Step 2: Process Data and Generate Maps

```bash
cd analysis/scripts
python map_election_results.py
```

**What this does:**

- ✅ Loads and validates your data
- ✅ Fixes coordinate system issues (converts to WGS84)
- ✅ Merges election data with geographic boundaries
- ✅ Generates 9 different static maps (PNG files)
- ✅ Creates web-optimized GeoJSON file

**Outputs:**

- `analysis/maps/` - Static PNG maps for analysis
- `analysis/geospatial/[output].geojson` - Web-ready data

### Step 3: Create Interactive Vector Tiles

```bash
python create_vector_tiles.py
```

**What this does:**

- ✅ Processes GeoJSON for web display
- ✅ Generates compressed vector tiles (.mbtiles)
- ✅ Validates tile structure
- ✅ Ready for manual upload or self-hosting

**Outputs:**

- `analysis/tiles/[name].mbtiles` - Vector tiles for web maps

### Step 4: Deploy Your Vector Tiles (Choose One)

#### Option A: Upload to Mapbox Studio
1. Go to [Mapbox Studio](https://studio.mapbox.com)
2. Navigate to Tilesets
3. Click "New tileset" → "Upload file"
4. Upload your `.mbtiles` file
5. Configure settings and publish

#### Option B: Self-Host with TileServer GL
```bash
# Install TileServer GL
npm install -g tileserver-gl-light

# Serve your tiles locally
tileserver-gl-light analysis/tiles/2025_election_zone1_tiles.mbtiles

# Access at http://localhost:8080
```

#### Option C: Other Mapping Services
- **MapTiler Cloud**: Upload via web interface
- **ArcGIS Online**: Import MBTiles
- **Custom Server**: Use any MBTiles-compatible server

## 📊 Understanding the Output

### Static Maps Generated

1. **Zone 1 Participation** - Shows which areas had elections
2. **Political Lean** - Voter registration patterns
3. **Democratic Advantage** - Registration advantage maps
4. **Total Votes** - Vote counts by area
5. **Voter Turnout** - Turnout rates
6. **Candidate Performance** - Vote share maps
7. **Engagement Score** - Civic engagement metrics
8. **Vote Margins** - Competition analysis
9. **Combined Analysis** - Multi-factor visualization

### Data Files Created

- **Optimized GeoJSON** - Ready for web mapping libraries
- **Vector Tiles** - Compressed, scalable map data
- **Metadata** - Data source and processing information

## 🛠️ Troubleshooting

### Common Issues

**"Coordinate system problems"**

```
❌ Sample coordinates: x=7678756, y=703492
✅ The script automatically detects and fixes this
```

**"No features matched"**

- Check that precinct IDs match between CSV and GeoJSON
- Remove leading zeros, spaces, case differences are handled automatically

**"tippecanoe command not found"**
```bash
# macOS
brew install tippecanoe

# Ubuntu/Debian
sudo apt install gdal-bin

# Or install from source:
# git clone https://github.com/mapbox/tippecanoe.git
# cd tippecanoe && make && make install
```

**"Empty tiles generated"**
- Usually caused by coordinate system issues
- The pipeline now automatically fixes these problems
- Check that your GeoJSON has valid geometries

**"Tiles too large"**
- Reduce maximum zoom level in create_vector_tiles.py
- Increase simplification settings
- Filter out unnecessary properties

### Data Validation

The scripts include comprehensive validation:

- ✅ Coordinate system detection and conversion
- ✅ Data type optimization
- ✅ Geometry validation and repair
- ✅ Property filtering for web compatibility

## 🎯 Next Steps

### Create Interactive Web Map
Once you have vector tiles, create an interactive map using:

**Mapbox GL JS (with uploaded tileset):**
```html
<script src='https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js'></script>
<script>
mapboxgl.accessToken = 'your_token';
const map = new mapboxgl.Map({
    container: 'map',
    style: 'mapbox://styles/mapbox/light-v11',
    source: {
        'election-data': {
            'type': 'vector',
            'url': 'mapbox://your_username.your_tileset_id'
        }
    }
});
</script>
```

**Leaflet with local TileServer GL:**
```javascript
// Assuming TileServer GL running on localhost:8080
const map = L.map('map').setView([45.52, -122.68], 10);

L.vectorGrid.protobuf('http://localhost:8080/data/election_results/{z}/{x}/{y}.pbf', {
    vectorTileLayerStyles: {
        'election_results': {
            fillColor: 'blue',
            fillOpacity: 0.7,
            stroke: true,
            color: 'white',
            weight: 1
        }
    }
}).addTo(map);
```

**MapLibre GL JS (free alternative to Mapbox GL JS):**
```html
<script src='https://unpkg.com/maplibre-gl@latest/dist/maplibre-gl.js'></script>
<script>
const map = new maplibregl.Map({
    container: 'map',
    style: {
        'version': 8,
        'sources': {
            'election-tiles': {
                'type': 'vector',
                'tiles': ['http://localhost:8080/data/election_results/{z}/{x}/{y}.pbf']
            }
        },
        'layers': [
            {
                'id': 'election-results',
                'type': 'fill',
                'source': 'election-tiles',
                'source-layer': 'election_results'
            }
        ]
    }
});
</script>
```

### Advanced Features
- Add popup tooltips with election data
- Create choropleth styling based on vote margins
- Add filters for different election metrics
- Implement time series for multiple elections

## 💡 Tips for Success

1. **Start Small** - Test with a subset of your data first
2. **Check Coordinate Systems** - The pipeline handles this, but verify your source data
3. **Optimize for Web** - The scripts automatically optimize data types and file sizes
4. **Test Locally First** - Use TileServer GL for development before deploying
5. **Monitor File Sizes** - Vector tiles should be under 50MB for good performance
6. **Backup Your Data** - Keep original files safe before processing
7. **Version Control** - Track changes to your processing scripts

## 📚 Additional Resources

- [Mapbox GL JS Documentation](https://docs.mapbox.com/mapbox-gl-js/)
- [Vector Tiles Specification](https://github.com/mapbox/vector-tile-spec)
- [Tippecanoe Documentation](https://github.com/mapbox/tippecanoe)
- [GeoPandas User Guide](https://geopandas.org/en/stable/user_guide.html)

## 🆘 Getting Help

If you encounter issues:

1. Check the console output for specific error messages
2. Verify your input data format matches the expected structure
3. Ensure all dependencies are correctly installed
4. The scripts include detailed logging to help identify problems

---

*This pipeline was developed for the 2025 Zone 1 Election analysis and can be adapted for any election mapping project.*
