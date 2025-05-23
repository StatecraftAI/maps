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

zone1_file="data/geospatial/2025_election_zone1_total_votes_results.geojson"
zone5_file="data/geospatial/2025_election_zone5_total_votes_results.geojson"
voter_reg_file="data/geospatial/multnomah_precinct_voter_totals_processed.geojson"

datasets_found=0
if [ -f "$zone1_file" ]; then
    echo "✅ Zone 1 election data found"
    datasets_found=$((datasets_found + 1))
else
    echo "❌ Zone 1 data missing: $zone1_file"
    echo "   Run: cd analysis && python map_election_results.py"
fi

if [ -f "$zone5_file" ]; then
    echo "✅ Zone 5 election data found"
    datasets_found=$((datasets_found + 1))
else
    echo "⚠️  Zone 5 data missing: $zone5_file"
    echo "   Update config.yaml and run: cd analysis && python map_election_results.py"
fi

if [ -f "$voter_reg_file" ]; then
    echo "✅ Voter registration data found"
    datasets_found=$((datasets_found + 1))
else
    echo "⚠️  Voter registration data missing: $voter_reg_file"
fi

echo ""
echo "📈 Data Status: $datasets_found dataset(s) available"

# Check for school boundary files
echo "🏫 Checking for school boundary files..."
school_files_found=0
for file in data/geospatial/pps_*.geojson; do
    if [ -f "$file" ]; then
        school_files_found=$((school_files_found + 1))
    fi
done

if [ $school_files_found -gt 0 ]; then
    echo "✅ Found $school_files_found school boundary files"
else
    echo "⚠️  No school boundary files found (data/geospatial/pps_*.geojson)"
fi

echo ""
echo "🌐 Starting local web server..."

# Function to detect available port
find_available_port() {
    local port=8080
    while netstat -an | grep -q ":$port "; do
        port=$((port + 1))
    done
    echo $port
}

# Get available port
PORT=$(find_available_port)

echo "🔗 Port: $PORT"
echo "📍 URL: http://localhost:$PORT/html/election_map.html"
echo ""
echo "✨ Features Available:"
echo "   • Multi-dataset selection (Zone 1, Zone 5, Voter Registration)"
echo "   • Split precinct consolidation"
echo "   • Color-blind friendly palettes"
echo "   • Enhanced analytical fields (victory margins, competitiveness, etc.)"
echo "   • School boundary overlays"
echo "   • Vote heatmaps with fixed coordinate processing"
echo ""
echo "🔧 Controls:"
echo "   • Dataset Selector: Choose between available datasets"
echo "   • Layer Controls: Toggle choropleth layers"
echo "   • School Overlays: Toggle PPS boundary layers"
echo "   • Heatmap Toggle: Switch to vote density view"
echo ""
echo "Press Ctrl+C to stop the server"
echo "============================================================================"

# Start Python HTTP server
cd "$(dirname "$0")/.." || exit 1
python3 -m http.server $PORT 