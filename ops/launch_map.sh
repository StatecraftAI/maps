#!/bin/bash

# Interactive Election Map Launcher
# This script starts the tile server and opens the interactive map

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MAP_FILE="$PROJECT_ROOT/html/election_map.html"
TILES_DIR="$PROJECT_ROOT/data/tiles"
MBTILES_FILE="$TILES_DIR/2025_election_zone1_total_votes_tiles.mbtiles"

echo "🗺️ Interactive Election Map Launcher"
echo "===================================="

# Check if the MBTiles file exists
if [ ! -f "$MBTILES_FILE" ]; then
    echo "❌ Error: Vector tiles not found at $MBTILES_FILE"
    echo ""
    echo "💡 To generate vector tiles, run:"
    echo "   cd $PROJECT_ROOT"
    echo "   python analysis/create_vector_tiles.py"
    echo ""
    exit 1
fi

echo "✅ Found vector tiles: $(basename "$MBTILES_FILE")"

# Check if map file exists
if [ ! -f "$MAP_FILE" ]; then
    echo "❌ Error: Map file not found at $MAP_FILE"
    exit 1
fi

echo "✅ Found map file: election_map.html"

# Function to check if port 8080 is in use
check_port() {
    if command -v lsof >/dev/null 2>&1; then
        lsof -ti:8080 >/dev/null 2>&1
    elif command -v netstat >/dev/null 2>&1; then
        netstat -ln | grep :8080 >/dev/null 2>&1
    else
        # Fallback - try to connect
        (echo >/dev/tcp/localhost/8080) >/dev/null 2>&1
    fi
}

# Check if tile server is already running
if check_port; then
    echo "✅ Tile server already running on port 8080"
    TILE_SERVER_RUNNING=true
else
    echo "🔧 Starting tile server..."
    TILE_SERVER_RUNNING=false
    
    # Check if TileServer GL is installed
    if ! command -v tileserver-gl-light &> /dev/null; then
        echo "❌ TileServer GL not found. Installing..."
        
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
    fi
    
    # Start tile server in background
    echo "🚀 Starting TileServer GL in background..."
    cd "$TILES_DIR"
    nohup tileserver-gl-light "2025_election_zone1_total_votes_tiles.mbtiles" --port 8080 > /dev/null 2>&1 &
    TILE_SERVER_PID=$!
    
    # Wait a moment for server to start
    echo "⏳ Waiting for tile server to initialize..."
    sleep 3
    
    # Check if server started successfully
    if check_port; then
        echo "✅ Tile server started successfully (PID: $TILE_SERVER_PID)"
    else
        echo "❌ Failed to start tile server"
        exit 1
    fi
fi

# Show connection info
echo ""
echo "📊 Tile Server Information:"
echo "   URL: http://localhost:8080"
echo "   Tiles: http://localhost:8080/data/2025_election_zone1_total_votes_tiles/{z}/{x}/{y}.pbf"
echo "   File: $(basename "$MBTILES_FILE")"
echo "   Size: $(du -h "$MBTILES_FILE" | cut -f1)"

echo ""
echo "🌐 Opening interactive map..."

# Determine the best way to open the map file
MAP_URL="file://$MAP_FILE"

# Try to open with the system's default browser
if command -v xdg-open >/dev/null 2>&1; then
    # Linux
    xdg-open "$MAP_URL"
elif command -v open >/dev/null 2>&1; then
    # macOS
    open "$MAP_URL"
elif command -v start >/dev/null 2>&1; then
    # Windows (Git Bash/WSL)
    start "$MAP_URL"
else
    echo "📝 Please manually open the following URL in your browser:"
    echo "   $MAP_URL"
fi

echo ""
echo "🎯 Map Features:"
echo "   • Multiple data layers (Political Lean, Competitiveness, Candidates)"
echo "   • Click precincts for detailed information"
echo "   • Layer controls and filters in the top-right panel"
echo "   • Base map options (Light, Satellite, Terrain, Dark)"
echo "   • Zone 1 filtering to focus on election precincts"

echo ""
if [ "$TILE_SERVER_RUNNING" = false ]; then
    echo "🛑 To stop the tile server later, run:"
    echo "   kill $TILE_SERVER_PID"
    echo "   (or use Ctrl+C if running in foreground)"
    
    # Store PID for cleanup script
    echo "$TILE_SERVER_PID" > "$PROJECT_ROOT/.tile_server_pid"
    echo ""
    echo "💡 Or run: ./ops/stop_tiles.sh"
else
    echo "💡 Tile server was already running - you may need to stop it manually"
fi

echo ""
echo "✨ Interactive map is ready! Click precincts to explore the election data." 