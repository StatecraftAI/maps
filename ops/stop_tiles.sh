#!/bin/bash

# Stop Tile Server Script
# This script stops the TileServer GL instance

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_FILE="$PROJECT_ROOT/.tile_server_pid"

echo "🛑 Stopping Tile Server"
echo "======================="

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

# Check if tile server is running
if ! check_port; then
    echo "✅ No tile server running on port 8080"
    # Clean up PID file if it exists
    if [ -f "$PID_FILE" ]; then
        rm "$PID_FILE"
        echo "🧹 Cleaned up PID file"
    fi
    exit 0
fi

# Try to use stored PID first
if [ -f "$PID_FILE" ]; then
    STORED_PID=$(cat "$PID_FILE")
    echo "📋 Found stored PID: $STORED_PID"
    
    # Check if process is still running
    if ps -p "$STORED_PID" > /dev/null 2>&1; then
        echo "🎯 Stopping tile server (PID: $STORED_PID)..."
        kill "$STORED_PID"
        
        # Wait a moment and check if it stopped
        sleep 2
        if ! ps -p "$STORED_PID" > /dev/null 2>&1; then
            echo "✅ Tile server stopped successfully"
            rm "$PID_FILE"
            exit 0
        else
            echo "⚠️ Process still running, trying force kill..."
            kill -9 "$STORED_PID"
            sleep 1
            if ! ps -p "$STORED_PID" > /dev/null 2>&1; then
                echo "✅ Tile server force stopped"
                rm "$PID_FILE"
                exit 0
            fi
        fi
    else
        echo "⚠️ Stored PID is stale"
        rm "$PID_FILE"
    fi
fi

# Fallback: find and kill any tileserver-gl-light processes
echo "🔍 Searching for tileserver-gl-light processes..."

# Different approaches for different systems
if command -v pgrep >/dev/null 2>&1; then
    # Use pgrep if available
    TILE_PIDS=$(pgrep -f "tileserver-gl-light")
elif command -v ps >/dev/null 2>&1; then
    # Fallback to ps
    TILE_PIDS=$(ps aux | grep "tileserver-gl-light" | grep -v grep | awk '{print $2}')
else
    echo "❌ Cannot find process management tools"
    exit 1
fi

if [ -n "$TILE_PIDS" ]; then
    echo "🎯 Found tileserver-gl-light processes: $TILE_PIDS"
    for pid in $TILE_PIDS; do
        echo "   Stopping PID: $pid"
        kill "$pid"
    done
    
    # Wait and check
    sleep 2
    if check_port; then
        echo "⚠️ Port still in use, trying force kill..."
        for pid in $TILE_PIDS; do
            kill -9 "$pid" 2>/dev/null
        done
        sleep 1
    fi
    
    if check_port; then
        echo "❌ Failed to stop tile server completely"
        echo "💡 You may need to manually kill the process or restart your terminal"
        exit 1
    else
        echo "✅ All tile server processes stopped"
    fi
else
    echo "🤔 No tileserver-gl-light processes found"
    
    # Check what's using port 8080
    if command -v lsof >/dev/null 2>&1; then
        echo "🔍 Checking what's using port 8080:"
        lsof -i :8080
        echo ""
        echo "💡 You may need to manually stop the process using port 8080"
    fi
fi

# Final check
if ! check_port; then
    echo "✅ Port 8080 is now free"
else
    echo "⚠️ Something is still using port 8080"
fi 