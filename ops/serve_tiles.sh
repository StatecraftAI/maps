#!/bin/bash

# Serve Vector Tiles Locally
# This script sets up and runs TileServer GL to serve your election vector tiles

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TILES_DIR="$SCRIPT_DIR/../tiles"
MBTILES_FILE="$TILES_DIR/2025_election_zone1_tiles.mbtiles"

echo "🗺️ Setting up local tile server for election map"
echo "=================================="

# Check if the MBTiles file exists
if [ ! -f "$MBTILES_FILE" ]; then
    echo "❌ Error: MBTiles file not found at $MBTILES_FILE"
    echo "   Run 'python create_vector_tiles.py' first to generate tiles"
    exit 1
fi

echo "✅ Found MBTiles file: $(basename "$MBTILES_FILE")"

# Check if TileServer GL is installed
if ! command -v tileserver-gl-light &> /dev/null; then
    echo "🔧 TileServer GL not found. Installing..."
    
    # Check if npm is available
    if ! command -v npm &> /dev/null; then
        echo "❌ Error: npm is required to install TileServer GL"
        echo "   Install Node.js and npm first:"
        echo "   - macOS: brew install node"
        echo "   - Ubuntu: sudo apt install nodejs npm"
        exit 1
    fi
    
    echo "📦 Installing tileserver-gl-light..."
    npm install -g tileserver-gl-light
    
    if [ $? -ne 0 ]; then
        echo "❌ Failed to install TileServer GL"
        exit 1
    fi
    
    echo "✅ TileServer GL installed successfully"
else
    echo "✅ TileServer GL is already installed"
fi

# Show file info
echo ""
echo "📊 Tile Information:"
echo "   File: $(basename "$MBTILES_FILE")"
echo "   Size: $(du -h "$MBTILES_FILE" | cut -f1)"
echo "   Location: $MBTILES_FILE"

echo ""
echo "🚀 Starting TileServer GL..."
echo "   URL: http://localhost:8080"
echo "   Tiles: http://localhost:8080/data/2025_election_zone1_tiles/{z}/{x}/{y}.pbf"
echo ""
echo "💡 Open analysis/maps/election_map.html in your browser"
echo "   The map will automatically detect the tile server and use vector tiles"
echo ""
echo "🛑 Press Ctrl+C to stop the server"
echo ""

# Start the tile server
cd "$TILES_DIR"
tileserver-gl-light "2025_election_zone1_tiles.mbtiles" --port 8080 