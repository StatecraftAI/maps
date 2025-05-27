#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_DIR="${PROJECT_ROOT}/data"
GEOSPATIAL_DIR="${DATA_DIR}/geospatial"

print_header() {
    echo "🚀 Launching StatecraftAI Maps Demo"
    echo "============================================================================"
}

check_python() {
    command -v python3 >/dev/null 2>&1 || {
        echo >&2 "❌ Error: Python 3 (python3) is required but not found."
        echo >&2 "Please install Python 3 and ensure 'python3' is in your PATH."
        exit 1
    }
    echo "✅ Python 3 found: $(python3 --version)"
}

check_main_html_file() {
    local html_file="${PROJECT_ROOT}/html/election_map.html"
    if [ ! -f "${html_file}" ]; then
        echo "❌ Error: Main map file not found at ${html_file}"
        echo "   Ensure this script is run from the project's 'ops' directory "
        echo "   or as 'ops/launch_map.sh' from the project root."
        exit 1
    fi
    echo "✅ Main HTML file found: ${html_file}"
}

check_election_data() {
    echo "📊 Checking for election datasets..."
    local zones=(1 4 5 6)
    local datasets_found=0
    local all_data_present=true

    for zone in "${zones[@]}"; do
        local zone_file="${GEOSPATIAL_DIR}/2025_election_zone${zone}_total_votes_results.geojson"
        if [ -f "${zone_file}" ]; then
            echo "  ✅ Zone ${zone} election data found."
            datasets_found=$((datasets_found + 1))
        else
            echo "  ⚠️ Zone ${zone} data missing: ${zone_file}"
            all_data_present=false
        fi
    done

    echo "📈 Election Data Status: ${datasets_found} of ${#zones[@]} primary dataset(s) available."
    if ! ${all_data_present}; then
        echo "   Consider running data processing scripts (e.g., 'analysis/map_election_results.py') if data is missing."
    fi
}

check_school_boundaries() {
    echo "🏫 Checking for school boundary files..."
    local school_files_pattern="${GEOSPATIAL_DIR}/pps_*.geojson"
    local school_files_found=0

    shopt -s nullglob
    for _file in ${school_files_pattern}; do
        school_files_found=$((school_files_found + 1))
    done
    shopt -u nullglob

    if [ "${school_files_found}" -gt 0 ]; then
        echo "✅ Found ${school_files_found} school boundary files (e.g., matching ${school_files_pattern})"
    else
        echo "⚠️ No school boundary files found matching: ${school_files_pattern}"
    fi
}

find_available_port() {
    local start_port=${1:-8080}
    local found_port

    found_port=$(python3 -c "
import socket, sys
p = int(sys.argv[1])
for _i in range(1000): # Try up to 1000 ports from start_port
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', p))
        s.close()
        print(p)
        sys.exit(0) # Success
    except OSError:
        p += 1
# If loop finishes, no port was found in range
sys.exit(1) # Indicate failure
" "${start_port}")

    if [ $? -eq 0 ] && [ -n "${found_port}" ]; then
        echo "${found_port}"
    else
        echo "⚠️ Could not find an available port starting from ${start_port} up to $((${start_port} + 999))." >&2
        local any_port
        any_port=$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()")
        if [ -n "${any_port}" ]; then
            echo "ℹ️ Using a randomly OS-assigned available port: ${any_port}" >&2
            echo "${any_port}"
        else
            echo "❌ Error: Truly unable to find any available port." >&2
            exit 1
        fi
    fi
}

main() {
    print_header
    check_python
    check_main_html_file
    echo
    check_election_data
    echo
    check_school_boundaries
    echo
    echo "🌐 Starting local web server..."

    local port
    port=$(find_available_port 8080)
    echo "🔗 Port: ${port}"
    local map_url="http://localhost:${port}/html/election_map.html"
    echo "📍 URL:  ${map_url}"
    echo
    echo "Press Ctrl+C to stop the server."
    echo "============================================================================"

    cd "${PROJECT_ROOT}" || {
        echo "❌ Error: Failed to change directory to project root: ${PROJECT_ROOT}"
        exit 1
    }

    python3 -m http.server "${port}"
}

main "$@"
