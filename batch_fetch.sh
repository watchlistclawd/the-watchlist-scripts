#!/bin/bash
# Batch fetch all five test franchises
# Run with: bash scripts/batch_fetch.sh

set -e
cd "$(dirname "$0")/.."

echo "═══════════════════════════════════════════════════"
echo "  Batch Franchise Fetch — $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "═══════════════════════════════════════════════════"

franchises=(
    "Attack on Titan"
    "Demon Slayer"
    "Death Note"
    "Little Witch Academia"
    "Lupin III"
)

total=${#franchises[@]}
i=0

for title in "${franchises[@]}"; do
    i=$((i + 1))
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  [$i/$total] $title"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    python3 -u fetch_franchise_anime.py "$title" || echo "  ⚠ FAILED: $title"
    echo ""
done

echo "═══════════════════════════════════════════════════"
echo "  All done! $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "═══════════════════════════════════════════════════"
