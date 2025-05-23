#!/bin/bash

# Launch script for GeoJSON-based election map
# No tile server required - just serves the HTML file directly

echo "🗺️  Launching Portland School Board Zone 1 Election Map (GeoJSON version)"
echo "============================================================================"

# Check if the HTML file exists
if [ ! -f "html/election_map_geojson.html" ]; then
    echo "❌ Error: html/election_map_geojson.html not found"
    echo "Make sure you're running this from the project root directory"
    exit 1
fi

# Check if the data file exists
if [ ! -f "data/geospatial/2025_election_zone1_total_votes_processed.geojson" ]; then
    echo "❌ Error: GeoJSON data file not found"
    echo "Expected: data/geospatial/2025_election_zone1_total_votes_processed.geojson"
    exit 1
fi

echo "✅ Found all required files"
echo ""

# Start a simple HTTP server
echo "🌐 Starting local web server..."
echo "📁 Serving from: $(pwd)"
echo ""

# Check if Python 3 is available
if command -v python3 &> /dev/null; then
    echo "🐍 Using Python 3 HTTP server"
    echo "🔗 Open your browser to: http://localhost:8000/html/election_map_geojson.html"
    echo ""
    echo "Press Ctrl+C to stop the server"
    echo "============================================================================"
    python3 -m http.server 8000
elif command -v python &> /dev/null; then
    echo "🐍 Using Python 2 HTTP server"
    echo "🔗 Open your browser to: http://localhost:8000/html/election_map_geojson.html"
    echo ""
    echo "Press Ctrl+C to stop the server"
    echo "============================================================================"
    python -m SimpleHTTPServer 8000
else
    echo "❌ Error: Python not found"
    echo "Please install Python or use another web server to serve the HTML file"
    echo ""
    echo "Alternative: Open html/election_map_geojson.html directly in your browser"
    echo "(Note: Some browsers may block file:// access to the GeoJSON data)"
    exit 1
fi 