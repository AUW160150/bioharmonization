#!/usr/bin/env bash
# BioHarmonize — local demo server
# Usage: bash serve.sh
# Then open: http://localhost:8080/screen0_landing.html

PORT=${1:-8080}
DIR="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "  BioHarmonize Demo"
echo "  ─────────────────────────────────"
echo "  http://localhost:${PORT}/screen0_landing.html"
echo ""
echo "  Demo flow:"
echo "    Hospital:  Landing → 1A → Pipeline → Results → Earnings"
echo "    Pharma:    Landing → 1B → Browse → Spend Analytics"
echo ""
echo "  Press Ctrl+C to stop."
echo ""

cd "$DIR"

# Try Python 3 first, then Python 2, then npx http-server
if command -v python3 &>/dev/null; then
  python3 -m http.server $PORT
elif command -v python &>/dev/null; then
  python -m SimpleHTTPServer $PORT
elif command -v npx &>/dev/null; then
  npx http-server . -p $PORT --cors -o
else
  echo "Error: no suitable server found. Install Python 3 or Node.js."
  exit 1
fi
