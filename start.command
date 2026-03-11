#!/bin/bash
echo "============================================"
echo "  BTV Airspace 3D Viewer"
echo "  Burlington VT - 50nm radius"
echo "============================================"
echo ""
echo "Starting local web server..."
echo "Open your browser to: http://localhost:8080"
echo "Press Ctrl+C to stop the server."
echo ""
cd "$(dirname "$0")"
open http://localhost:8080
python3 -m http.server 8080
