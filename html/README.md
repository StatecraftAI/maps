# Interactive Election Maps

This directory contains interactive maps for the 2025 Portland Public Schools Zone 1 Director Election.

## Available Maps

### 1. Enhanced GeoJSON Map (⭐ **RECOMMENDED**)
**File:** `election_map_enhanced.html`
**Launch:** `./ops/launch_enhanced_map.sh`

The most feature-rich version with all issues fixed and school overlays added.

**🔥 Enhanced Features:**
- ✅ **Fixed popup chart sizing** - No more huge charts extending off screen
- ✅ **Improved color scales** - Data-driven ranges for better visual differentiation
- ✅ **Better hover info** - Shows precinct name/number and key stats
- ✅ **More layer options** - Candidate vote counts, registration data (16 total layers)
- ✅ **School overlays** - Portland Public Schools locations and boundaries
- ✅ **Independent toggles** - Turn each school layer on/off separately
- ✅ **Better legends** - Granular color legends with actual data ranges

**School Overlays Include:**
- 🏫 High School locations and boundaries
- 🏫 Middle School locations and boundaries
- 🏫 Elementary School locations and boundaries
- 🏫 District boundary

### 2. Simple GeoJSON Map
**File:** `election_map_geojson.html`
**Launch:** `./ops/launch_geojson_map.sh`

Clean, simple version without school overlays. Good for basic election analysis.

**Features:**
- 🗺️ Interactive precincts with hover effects
- 📊 9 data layers (Political Lean, Competitiveness, Leading Candidate, etc.)
- 📈 Click-to-view detailed charts for each precinct
- 🎛️ Opacity controls, layer switching, base map options
- 🏫 Zone 1 filtering toggle
- 📱 Mobile-responsive design

### 3. Vector Tiles Map (Legacy)
**File:** `election_map.html`
**Launch:** `./ops/launch_map.sh`

Vector tiles-based map that requires a tile server. More complex setup.

## Quick Start

**For most users** (recommended):
```bash
./ops/launch_enhanced_map.sh
```

**For basic election analysis only**:
```bash
./ops/launch_geojson_map.sh
```

## Data Layers

### Election Data (All Maps)
1. **Political Lean** - Democratic vs Republican tendency
2. **Competitiveness** - How close the race is
3. **Leading Candidate** - Who's ahead in each precinct
4. **Turnout Rate** - Voter participation percentage
5. **Victory Margin** - Winning margin in votes
6. **Total Votes** - Raw vote counts

### Enhanced Version Additional Layers
7. **Cavagnolo Vote Count** - Raw vote totals for Joe Cavagnolo
8. **Splitt Vote Count** - Raw vote totals for Tiffany Splitt
9. **Leof Vote Count** - Raw vote totals for candidate Leof
10. **Democratic Advantage** - Dem vs Rep registration advantage
11. **Cavagnolo %** - Joe Cavagnolo vote percentage
12. **Splitt %** - Tiffany Splitt vote percentage
13. **Leof %** - Leof vote percentage
14. **Democratic Registration %** - Dem party registration
15. **Republican Registration %** - Rep party registration
16. **Non-Affiliated %** - Independent voter registration

### School Overlays (Enhanced Version Only)
- **High Schools** - Portland Public high school locations (red markers)
- **Middle Schools** - Middle school locations (green markers)
- **Elementary Schools** - Elementary school locations (blue markers)
- **High School Boundaries** - Attendance area boundaries (red outlines)
- **Middle School Boundaries** - Attendance area boundaries (green outlines)
- **Elementary Boundaries** - Attendance area boundaries (blue outlines)
- **District Boundary** - Overall PPS district boundary (orange outline)

## Interactive Features

- **🖱️ Hover:** Enhanced precinct info with key statistics
- **🖱️ Click:** Detailed popup with candidate charts (properly sized!)
- **🎛️ Layer Controls:** Switch between 16 different data views
- **🎨 Opacity Slider:** Adjust layer transparency
- **🗺️ Base Maps:** Street, satellite, or topographic
- **🏫 Zone Filter:** Focus on Zone 1 only
- **📱 Mobile Responsive:** Works on all devices
- **🏫 School Toggles:** Turn school overlays on/off independently

## Performance Comparison

| Feature | Enhanced | Simple | Vector Tiles |
|---------|----------|--------|--------------|
| **Setup Time** | Instant | Instant | 5+ minutes |
| **Chart Issues** | ✅ Fixed | ❌ Huge charts | ❌ Complex |
| **Color Scales** | ✅ Data-driven | ❌ Basic | ❌ Basic |
| **School Overlays** | ✅ 7 layers | ❌ None | ❌ None |
| **Hover Info** | ✅ Enhanced | ⭐ Basic | ⭐ Basic |
| **Layer Count** | 16 layers | 9 layers | 9 layers |
| **File Size** | 4MB + schools | 4MB | Complex |

## Data Sources

- **Election Data:** `data//geospatial/2025_election_zone1_total_votes_processed.geojson` (4MB)
- **School Data:** Various `data//geospatial/pps_*.geojson` files (total ~2MB)
- **Vector Tiles:** `data//tiles/2025_election_zone1_total_votes_tiles.mbtiles` (legacy)

## Technical Details

### Enhanced Approach (Recommended)
- **Technology:** Pure Leaflet.js + Chart.js
- **Dependencies:** None (just web server)
- **Browser Support:** All modern browsers
- **Chart Sizing:** Fixed dimensions (280x180px)
- **Color Scaling:** Data-driven ranges based on actual Zone 1 values
- **School Integration:** Async loading of 7 school overlay files

### Simple GeoJSON Approach
- **Pros:** Simple, reliable
- **Cons:** Basic features, no school context
- **Best for:** Quick election analysis

### Vector Tiles Approach (Legacy)
- **Pros:** Efficient for huge datasets
- **Cons:** Complex setup, compatibility issues
- **Best for:** Production deployments with massive data

## Browser Compatibility

- Chrome/Edge: Full support ✅
- Firefox: Full support ✅
- Safari: Full support ✅
- Mobile browsers: Responsive design ✅

## Troubleshooting

### Enhanced Map Issues
1. **Schools not loading:** Check that `data/geospatial/pps_*.geojson` files exist
2. **Charts still big:** Clear browser cache and refresh
3. **Colors all same:** Try different layers to see data-driven scaling

### General Issues
1. **Map not loading:** Run from project root directory
2. **Data not displaying:** Check browser console for errors
3. **CORS errors:** Use HTTP server (scripts handle this)

## Development

### Quick Edits
```bash
# Edit the enhanced map
vim html/election_map_enhanced.html

# Test changes
./ops/launch_enhanced_map.sh
```

### Adding New School Overlays
1. Add GeoJSON file to `data/geospatial/`
2. Update `schoolFiles` object in JavaScript
3. Add checkbox to HTML controls
4. Add event listener for toggle

### Custom Color Schemes
Edit the `dataRanges` and `getFeatureColor()` function in the enhanced map for custom scaling.

## Summary

**Use the Enhanced version** (`./ops/launch_enhanced_map.sh`) for:
- ✅ All the latest fixes and improvements
- ✅ Rich school context and overlays
- ✅ Better visual differentiation
- ✅ Complete feature set

**Use Simple version** for quick election-only analysis without school context.

For questions or issues, check the project's main README.md.
