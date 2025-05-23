# Interactive Election Maps

This directory contains tools and templates for creating interactive maps of the 2025 Zone 1 election results using vector tiles and web mapping technologies.

## Overview

The interactive mapping system provides:

- **Vector tiles** for fast, scalable web maps
- **Multiple visualization layers** (participation, turnout, political lean, etc.)
- **Interactive popups** with detailed precinct data
- **Professional styling** with legends and controls
- **Mobile-responsive design**

## Files

### Scripts

- `create_vector_tiles.py` - Main script to process GeoJSON and create vector tiles
- `setup_mapbox_environment.sh` - Setup script for dependencies and environment

### Maps

- `interactive_election_map.html` - Production Mapbox GL JS map (requires Mapbox account)
- `local_development_map.html` - Development version using Leaflet (no account needed)

## Quick Start (Local Development)

1. **Open the local development map:**

   ```bash
   cd analysis/maps
   python -m http.server 8000
   # Open http://localhost:8000/local_development_map.html
   ```

2. **The map will automatically load the election GeoJSON data** and provide:
   - 8 different visualization layers
   - Interactive popups with election and voter registration data
   - Dynamic legends
   - Zoom and pan controls

## Production Setup (Mapbox Vector Tiles)

### Prerequisites

1. **Mapbox Account:**
   - Sign up at <https://account.mapbox.com/>
   - Create an access token with `uploads:write` scope
   - Note your username

2. **Install Dependencies:**

   ```bash
   # Run the setup script
   chmod +x scripts/setup_mapbox_environment.sh
   ./scripts/setup_mapbox_environment.sh
   ```

3. **Set Environment Variables:**

   ```bash
   export MAPBOX_ACCESS_TOKEN='your_token_here'
   export MAPBOX_USERNAME='your_username_here'
   
   # Add to ~/.bashrc for persistence
   echo 'export MAPBOX_ACCESS_TOKEN="your_token_here"' >> ~/.bashrc
   echo 'export MAPBOX_USERNAME="your_username_here"' >> ~/.bashrc
   ```

### Creating Vector Tiles

1. **Run the vector tile creation script:**

   ```bash
   cd analysis/scripts
   python create_vector_tiles.py
   ```

2. **The script will:**
   - Process the election GeoJSON data
   - Optimize properties for web display
   - Generate vector tiles using tippecanoe
   - Optionally upload to Mapbox

3. **Update the HTML map:**
   - Edit `interactive_election_map.html`
   - Replace `YOUR_MAPBOX_ACCESS_TOKEN_HERE` with your token
   - Replace `YOUR_USERNAME.2025-election-zone1` with your tileset ID

## Map Layers

The interactive map includes 8 visualization layers:

### 1. Zone 1 Participation

- **Purpose:** Shows which precincts participated in the Zone 1 election
- **Colors:** Red (no participation), Green (participated)

### 2. Total Votes Cast

- **Purpose:** Number of votes cast in each participating precinct
- **Scale:** 0 to 300+ votes
- **Colors:** Light orange to dark brown

### 3. Voter Turnout Rate

- **Purpose:** Percentage of registered voters who voted
- **Scale:** 0% to 35%+
- **Colors:** Light blue to dark blue

### 4. Splitt Vote Share

- **Purpose:** Percentage of votes received by Splitt
- **Scale:** 50% to 90%+
- **Colors:** Light green to dark green

### 5. Political Lean

- **Purpose:** Political lean based on voter registration patterns
- **Categories:** Strong Rep, Lean Rep, Competitive, Lean Dem, Strong Dem
- **Colors:** Red to blue spectrum

### 6. Democratic Advantage

- **Purpose:** Democratic registration advantage (Dem% - Rep%)
- **Scale:** -40% to +40%
- **Colors:** Red (Rep advantage) to blue (Dem advantage)

### 7. Civic Engagement Score

- **Purpose:** Composite score of registration diversity and turnout
- **Scale:** 0 to 0.5+
- **Colors:** Light yellow to dark orange

### 8. Competitiveness

- **Purpose:** Election competitiveness classification
- **Categories:** Safe, Likely, Lean, Toss-up, Competitive
- **Colors:** Green (safe) to red (competitive)

## Interactive Features

### Popups

Click any precinct to see:

- **Election Results:** Vote counts and percentages
- **Voter Registration:** Registration by party
- **Political Analysis:** Lean, advantage, engagement scores

### Controls

- **Layer Switcher:** Radio buttons to change visualization
- **Dynamic Legend:** Updates based on selected layer
- **Navigation:** Zoom, pan, fullscreen controls
- **Mobile Responsive:** Optimized for all screen sizes

## Technical Details

### Vector Tile Configuration

- **Zoom Levels:** 8 (county) to 14 (neighborhood detail)
- **Layer Name:** `election_results`
- **Simplification:** Optimized for web performance
- **Attribution:** Multnomah County Elections Division

### Data Processing

The script optimizes the GeoJSON by:

- Converting all numeric values to appropriate types
- Rounding percentages to 3 decimal places
- Creating boolean flags for easier styling
- Removing unnecessary properties to reduce file size

### Browser Compatibility

- **Modern Browsers:** Chrome, Firefox, Safari, Edge
- **Mobile:** iOS Safari, Chrome Mobile
- **Fallback:** Graceful degradation for older browsers

## Customization

### Adding New Layers

1. **Update `layerConfigs`** in the HTML file
2. **Add styling logic** in `getLayerStyle()`
3. **Update legend generation** in `updateLegend()`

### Styling Changes

- **Colors:** Modify the `colors` arrays in layer configs
- **Breakpoints:** Adjust `stops` for numeric layers
- **UI:** Edit CSS classes for overlays and controls

### Data Updates

- **Re-run** `map_election_results.py` to update the source GeoJSON
- **Re-run** `create_vector_tiles.py` to regenerate tiles
- **Upload** new tileset to Mapbox if using production version

## Troubleshooting

### Common Issues

1. **"tippecanoe not found"**
   - Run the setup script: `./setup_mapbox_environment.sh`
   - On macOS: `brew install tippecanoe`
   - On Linux: Build from source (script handles this)

2. **"MAPBOX_ACCESS_TOKEN not set"**
   - Create token at <https://account.mapbox.com/>
   - Set environment variable: `export MAPBOX_ACCESS_TOKEN='your_token'`

3. **"Error loading data"**
   - Check that `2025_election_zone1_results.geojson` exists
   - Verify file permissions and path
   - Check browser console for detailed errors

4. **Map not displaying**
   - Verify Mapbox token has correct permissions
   - Check tileset ID matches your upload
   - Ensure tileset processing is complete in Mapbox Studio

### Performance Tips

1. **Large Datasets:**
   - Use higher simplification values in tippecanoe
   - Reduce maximum zoom level
   - Consider data aggregation

2. **Slow Loading:**
   - Enable gzip compression on web server
   - Use CDN for tile delivery
   - Optimize GeoJSON before processing

## Support

For issues with:

- **Mapbox:** Check <https://docs.mapbox.com/>
- **Tippecanoe:** See <https://github.com/felt/tippecanoe>
- **Election Data:** Review the data processing scripts

## License

This mapping system is part of the populist consensus election analysis project. See the main repository LICENSE for details.
