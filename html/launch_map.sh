#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_DIR="${PROJECT_ROOT}/data"
GEOSPATIAL_DIR="${DATA_DIR}/geospatial"

print_header() {
    echo "============================================================================"
    echo "🚀 Launching StatecraftAI Maps Demo"
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
    local port
    port=$(find_available_port 8080)
    local map_url="http://localhost:${port}/html/election_map.html"
    echo "🌐 Starting local web server..."
    echo "🔗 Port: ${port}"
    echo "📍 URL:  ${map_url}"
    echo "Press Ctrl+C to stop the server."
    echo "============================================================================"
    cd "${PROJECT_ROOT}" || {
        echo "❌ Error: Failed to change directory to project root: ${PROJECT_ROOT}"
        exit 1
    }
    python3 -m http.server "${port}"
}

main "$@"
