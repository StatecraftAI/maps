#!/bin/bash

echo "🚀 Launching Portland School Board Election Map"
echo "============================================================================"

# Check if the HTML file exists
if [ ! -f "html/election_map.html" ]; then
    echo "❌ Error: html/election_map.html not found"
    echo "Make sure you're running this from the project root directory"
    exit 1
fi

# Check for election data files
echo "📊 Checking for election datasets..."

zone1_file="data/geospatial/2025_election_zone1_total_votes_processed.geojson"
zone5_file="data/geospatial/2025_election_zone5_total_votes_processed.geojson"
voter_reg_file="data/geospatial/multnomah_precinct_voter_totals_processed.geojson"

datasets_found=0
if [ -f "$zone1_file" ]; then
    echo "✅ Zone 1 election data found"
    datasets_found=$((datasets_found + 1))
else
    echo "⚠️  Zone 1 election data missing: $zone1_file"
fi

if [ -f "$zone5_file" ]; then
    echo "✅ Zone 5 election data found"
    datasets_found=$((datasets_found + 1))
else
    echo "⚠️  Zone 5 election data missing: $zone5_file"
fi

if [ -f "$voter_reg_file" ]; then
    echo "✅ Voter registration data found"
    datasets_found=$((datasets_found + 1))
else
    echo "⚠️  Voter registration data missing: $voter_reg_file"
fi

if [ $datasets_found -eq 0 ]; then
    echo "❌ Error: No election datasets found!"
    echo "   Please ensure at least one of the following files exists:"
    echo "   - $zone1_file"
    echo "   - $zone5_file"
    echo "   - $voter_reg_file"
    exit 1
fi

echo "📊 Found $datasets_found dataset(s) available"

# Check for school data files
echo ""
echo "🏫 Checking for school overlay files..."
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

echo "🏫 Found $((${#school_files[@]} - ${#missing_files[@]})) of ${#school_files[@]} school overlay files"
echo ""

# Find an available port (starting from 8000)
PORT=8000
while lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; do
    echo "Port $PORT is busy, trying $((PORT+1))..."
    PORT=$((PORT+1))
    if [ $PORT -gt 8010 ]; then
        echo "❌ Error: Could not find an available port between 8000-8010"
        echo "Please stop other web servers or specify a different port manually"
        exit 1
    fi
done

# Start a simple HTTP server
echo "🌐 Starting local web server on port $PORT..."
echo "📁 Serving from: $(pwd)"
echo ""

# Check if Python 3 is available
if command -v python3 &> /dev/null; then
    echo "🐍 Using Python 3 HTTP server"
    echo "🔗 Open your browser to: http://localhost:$PORT/html/election_map.html"
    echo ""
    echo "🆕 NEW FEATURES & FIXES:"
    echo "   🎯 MULTI-DATASET SUPPORT: Zone 1, Zone 5, and Voter Registration data"
    echo "   🎨 FIXED COLOR SCALES: Darker colors now properly indicate higher values"
    echo "   🔥 FIXED HEATMAP: Coordinate extraction error resolved"
    echo "   📊 DYNAMIC LAYERS: Different options based on selected dataset"
    echo "   📈 CANDIDATE-SPECIFIC CHARTS: Adaptive popups for different elections"
    echo ""
    echo "🚀 Core Features:"
    echo "   🎚️ RANGE SLIDERS for custom color scaling"
    echo "   🖱️ Enhanced hover info with precinct details"
    echo "   📍 CLUSTERING for school markers"
    echo "   ✏️ DRAWING TOOLS for custom analysis areas"
    echo "   📄 MAP EXPORT functionality"
    echo "   📱 FULLSCREEN mode"
    echo "   🏫 School overlays (locations and boundaries)"
    echo "   📈 LIVE STATISTICS panel"
    echo ""
    echo "💡 Usage Tips:"
    echo "   • Select different datasets from the 'Election Dataset' dropdown"
    echo "   • Use range sliders to enhance color contrast for tight data ranges"
    echo "   • Toggle 'Zone 1 Only' to see all precincts in the dataset"
    echo "   • Try the heatmap feature for vote density visualization"
    echo ""
    echo "Press Ctrl+C to stop the server"
    echo "============================================================================"
    python3 -m http.server $PORT
elif command -v python &> /dev/null; then
    echo "🐍 Using Python 2 HTTP server"
    echo "🔗 Open your browser to: http://localhost:$PORT/html/election_map.html"
    echo ""
    echo "Press Ctrl+C to stop the server"
    echo "============================================================================"
    python -m SimpleHTTPServer $PORT
else
    echo "❌ Error: Python not found"
    echo "Please install Python or use another web server to serve the HTML file"
    echo ""
    echo "Alternative: Open html/election_map.html directly in your browser"
    echo "(Note: Some browsers may block file:// access to the GeoJSON data)"
    exit 1
fi 