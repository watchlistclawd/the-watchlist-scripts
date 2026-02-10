#!/bin/bash
# Wipe all data from the watchlist database
# Usage: ./wipe.sh

set -e
cd "$(dirname "$0")"

echo "⚠️  Wiping all data from watchlist database..."
psql watchlist -f wipe_data.sql

echo "✓ Done. Database is empty."
