#!/bin/bash

# Setup script for Mapbox Vector Tiles environment
# This script installs tippecanoe and helps configure Mapbox credentials

echo "🗺️ Setting up Mapbox Vector Tiles Environment"
echo "=============================================="

# Check if running on macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "📱 Detected macOS - using Homebrew for installation"
    
    # Check if Homebrew is installed
    if ! command -v brew &> /dev/null; then
        echo "❌ Homebrew not found. Please install Homebrew first:"
        echo "   /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
        exit 1
    fi
    
    echo "🔧 Installing tippecanoe..."
    brew install tippecanoe
    
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "🐧 Detected Linux - building tippecanoe from source"
    
    # Install dependencies
    echo "📦 Installing dependencies..."
    sudo apt-get update
    sudo apt-get install -y build-essential libsqlite3-dev zlib1g-dev git
    
    # Clone and build tippecanoe
    echo "🔧 Building tippecanoe from source..."
    cd /tmp
    git clone https://github.com/felt/tippecanoe.git
    cd tippecanoe
    make -j
    sudo make install
    cd -
    
else
    echo "❓ Unsupported operating system: $OSTYPE"
    echo "Please manually install tippecanoe: https://github.com/felt/tippecanoe"
    exit 1
fi

# Verify tippecanoe installation
echo "✅ Verifying tippecanoe installation..."
if command -v tippecanoe &> /dev/null; then
    tippecanoe --version
    echo "✅ tippecanoe installed successfully!"
else
    echo "❌ tippecanoe installation failed"
    exit 1
fi

# Install Python dependencies
echo "🐍 Installing Python dependencies..."
pip install requests

echo ""
echo "🎯 Next Steps:"
echo "1. Get a Mapbox account at https://account.mapbox.com/"
echo "2. Create an access token with 'uploads:write' scope"
echo "3. Set environment variables:"
echo ""
echo "   export MAPBOX_ACCESS_TOKEN='your_token_here'"
echo "   export MAPBOX_USERNAME='your_username_here'"
echo ""
echo "4. Add these to your ~/.bashrc or ~/.zshrc for persistence:"
echo ""
echo "   echo 'export MAPBOX_ACCESS_TOKEN=\"your_token_here\"' >> ~/.bashrc"
echo "   echo 'export MAPBOX_USERNAME=\"your_username_here\"' >> ~/.bashrc"
echo ""
echo "5. Run the vector tile creation script:"
echo "   python create_vector_tiles.py"
echo ""
echo "✅ Setup complete! Ready to create vector tiles." 