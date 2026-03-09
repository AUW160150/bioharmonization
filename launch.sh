#!/usr/bin/env bash
# Terminal 1: bash launch.sh
# Terminal 2: bash launch.sh api

if [ "$1" = "api" ]; then
  cd "$(dirname "$0")"
  ~/Library/Python/3.9/bin/uvicorn api:app --host 0.0.0.0 --port 8000 --reload
else
  cd "$(dirname "$0")/frontend"
  python3 -m http.server 8080
fi
