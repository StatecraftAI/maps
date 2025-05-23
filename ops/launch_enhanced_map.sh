#!/bin/bash

# Launch script for Enhanced GeoJSON-based election map
# Includes school overlays and improved features

echo "🗺️  Launching Enhanced Portland School Board Zone 1 Election Map"
echo "============================================================================"

# Check if the HTML file exists
if [ ! -f "html/election_map_enhanced.html" ]; then
    echo "❌ Error: html/election_map_enhanced.html not found"
    echo "Make sure you're running this from the project root directory"
    exit 1
fi

# Check if the data files exist
if [ ! -f "data/geospatial/2025_election_zone1_total_votes_processed.geojson" ]; then
    echo "❌ Error: Election GeoJSON data file not found"
    echo "Expected: data/geospatial/2025_election_zone1_total_votes_processed.geojson"
    exit 1
fi

# Check for school data files
school_files=(
    "data/geospatial/pps_high_school_locations.geojson"
    "data/geospatial/pps_middle_school_locations.geojson"
    "data/geospatial/pps_elementary_school_locations.geojson"
    "data/geospatial/pps_high_school_boundaries.geojson"
    "data/geospatial/pps_middle_school_boundaries.geojson"
    "data/geospatial/pps_elementary_school_boundaries.geojson"
    "data/geospatial/pps_district_boundary.geojson"
)

missing_files=()
for file in "${school_files[@]}"; do
    if [ ! -f "$file" ]; then
        missing_files+=("$file")
    fi
done

if [ ${#missing_files[@]} -gt 0 ]; then
    echo "⚠️  Warning: Some school overlay files are missing:"
    for file in "${missing_files[@]}"; do
        echo "   - $file"
    done
    echo "   The map will still work, but some school overlays may not be available"
    echo ""
fi

echo "✅ Found required election data file"
echo "📚 Found $((${#school_files[@]} - ${#missing_files[@]})) of ${#school_files[@]} school overlay files"
echo ""

# Start a simple HTTP server
echo "🌐 Starting local web server..."
echo "📁 Serving from: $(pwd)"
echo ""

# Check if Python 3 is available
if command -v python3 &> /dev/null; then
    echo "🐍 Using Python 3 HTTP server"
    echo "🔗 Open your browser to: http://localhost:8000/html/election_map_enhanced.html"
    echo ""
    echo "✨ Enhanced Features:"
    echo "   🎯 Fixed popup chart sizing"
    echo "   🎨 Improved color scales with better granularity"
    echo "   🖱️ Enhanced hover info with precinct details"
    echo "   📊 Additional layer options (candidate vote counts, registration data)"
    echo "   🏫 School overlays (locations and boundaries)"
    echo "   🎛️ Independent toggles for each school layer"
    echo ""
    echo "Press Ctrl+C to stop the server"
    echo "============================================================================"
    python3 -m http.server 8000
elif command -v python &> /dev/null; then
    echo "🐍 Using Python 2 HTTP server"
    echo "🔗 Open your browser to: http://localhost:8000/html/election_map_enhanced.html"
    echo ""
    echo "Press Ctrl+C to stop the server"
    echo "============================================================================"
    python -m SimpleHTTPServer 8000
else
    echo "❌ Error: Python not found"
    echo "Please install Python or use another web server to serve the HTML file"
    echo ""
    echo "Alternative: Open html/election_map_enhanced.html directly in your browser"
    echo "(Note: Some browsers may block file:// access to the GeoJSON data)"
    exit 1
fi 