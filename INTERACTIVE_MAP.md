# Interactive Election Map Guide

Interactive web-based maps for exploring the 2025 Zone 1 Portland Public Schools Director Election results.

## 🚀 Quick Start (Recommended)

**New: GeoJSON-Based Map** - Simple, reliable, and fast!

```bash
# From project root
./ops/launch_geojson_map.sh
```

Then open: **http://localhost:8000/html/election_map_geojson.html**

That's it! No tile server setup required.

## 📁 Available Maps

### 1. GeoJSON Map (Primary) ⭐
- **File:** `html/election_map_geojson.html`
- **Data:** Direct GeoJSON loading (4MB)
- **Setup:** Zero configuration
- **Launch:** `./ops/launch_geojson_map.sh`
- **Best for:** Most users, development, demos

### 2. Vector Tiles Map (Legacy)
- **File:** `html/election_map.html`
- **Data:** Vector tiles via server
- **Setup:** Requires tile server
- **Launch:** `./ops/launch_map.sh`
- **Best for:** Very large datasets, production deployments

## 🗺️ Map Features

Both maps include identical functionality:

### Data Layers
1. **Political Lean** - Democratic vs Republican tendency
2. **Competitiveness** - How close the race is
3. **Leading Candidate** - Who's ahead in each precinct
4. **Turnout Rate** - Voter participation percentage
5. **Victory Margin** - Winning margin percentage
6. **Total Votes** - Raw vote counts
7. **Democratic Advantage** - Democratic party performance
8. **Cavagnolo %** - Joe Cavagnolo vote percentage
9. **Splitt %** - Tiffany Splitt vote percentage

### Interactive Features
- **🖱️ Hover:** Quick precinct info
- **🖱️ Click:** Detailed popup with candidate charts
- **🎛️ Layer Controls:** Switch between data views
- **🎨 Opacity Slider:** Adjust transparency
- **🗺️ Base Maps:** Street, satellite, topographic
- **🏫 Zone Filter:** Focus on Zone 1 only
- **📱 Mobile Responsive:** Works on all devices

## ⚡ Performance Comparison

| Feature | GeoJSON Map | Vector Tiles Map |
|---------|-------------|------------------|
| **Setup Time** | Instant | 5+ minutes |
| **Dependencies** | None | Tile server |
| **Initial Load** | 4MB (2-3 seconds) | ~500KB tiles |
| **Reliability** | Very High | Medium |
| **Debugging** | Easy | Complex |
| **Zoom Performance** | Consistent | Better at high zoom |

**Bottom Line:** Use GeoJSON unless you have specific performance requirements.

## 🛠️ Technical Details

### GeoJSON Approach
- **Technology:** Pure Leaflet.js + Chart.js
- **Data Source:** `data/geospatial/2025_election_zone1_total_votes_processed.geojson`
- **Server:** Simple Python HTTP server
- **Browser Support:** All modern browsers

### Vector Tiles Approach (Legacy)
- **Technology:** Leaflet + Vector Tiles plugin + TileServer GL
- **Data Source:** `data/tiles/2025_election_zone1_total_votes_tiles.mbtiles`
- **Server:** TileServer GL on port 8080
- **Browser Support:** Most modern browsers (plugin compatibility issues possible)

## 📊 Data Schema

The maps visualize precinct-level data including:

```javascript
{
  "precinct": "101",
  "political_lean": "Lean Dem",
  "competitiveness": "Competitive",
  "leading_candidate": "Cavagnolo",
  "cnt_cavagnolo": 245,
  "cnt_splitt": 198,
  "cnt_leof": 87,
  "cnt_total_votes": 530,
  "pct_cavagnolo": 46.2,
  "pct_splitt": 37.4,
  "pct_leof": 16.4,
  "turnout_rate": 78.5,
  "vote_margin": 8.8,
  "dem_advantage": 12.3,
  "in_zone1": true
}
```

## 🎨 Color Schemes

- **Political Lean:** Blue (Dem) → Purple (Competitive) → Red (Rep)
- **Competitiveness:** Green (Safe) → Orange (Competitive) → Red (Tossup)
- **Candidates:** Blue (Cavagnolo), Orange (Splitt), Green (Leof)
- **Numeric Data:** Blue gradient (low to high values)

## 🚨 Troubleshooting

### Map Won't Load
1. **Check location:** Run from project root directory
2. **Verify files:** Ensure GeoJSON data file exists
3. **Try GeoJSON version:** Simpler and more reliable
4. **Check console:** Browser dev tools for error messages

### Data Not Displaying
1. **File paths:** Check browser network tab
2. **CORS issues:** Use HTTP server, not file:// URLs
3. **Data validity:** Verify GeoJSON is valid JSON

### Performance Issues
1. **Use GeoJSON version:** Better for most use cases
2. **Check network:** 4MB download on slow connections
3. **Clear cache:** Browser may cache old versions

## 🔧 Development

### Quick Edits
```bash
# Edit the map directly
vim html/election_map_geojson.html

# Test changes
./ops/launch_geojson_map.sh
```

### Adding New Layers
1. Update the layer options in the `<select>` element
2. Add color scheme to `colorSchemes` object
3. Test with real data

### Custom Styling
All CSS is embedded in the HTML files for easy customization.

## 📝 Examples

### Basic Usage
```bash
# Start the map
./ops/launch_geojson_map.sh

# Open browser to
# http://localhost:8000/html/election_map_geojson.html
```

### Embedding in Website
```html
<iframe
  src="http://localhost:8000/html/election_map_geojson.html"
  width="100%"
  height="600px">
</iframe>
```

## 📞 Support

- **Primary:** Use `html/election_map_geojson.html` (recommended)
- **Fallback:** Use `html/election_map.html` (vector tiles)
- **Issues:** Check browser console, try different browsers
- **Data Problems:** Verify GeoJSON file exists and is valid

---

**Need help?** The GeoJSON version is much simpler and more reliable for most use cases!
