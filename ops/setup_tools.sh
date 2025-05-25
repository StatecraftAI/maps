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
    sudo apt-get install -y build-essential libsqlite3-dev zlib1g-dev git gdal-bin libgdal-dev

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
pip install -r ../requirements.txt
pip install pre-commit
pre-commit clean
pre-commit install
pre-commit run --all-files

echo ""
echo "✅ Setup complete! Ready to create vector tiles."
