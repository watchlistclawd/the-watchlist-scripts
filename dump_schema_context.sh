#!/bin/bash
# Dump schema context for Haiku prompts

TABLES="franchises entries companies creators characters entry_franchises entry_companies entry_creators entry_characters"

echo "-- Schema Context (auto-generated)"
echo "-- Use these EXACT column names in your SQL"
echo ""

for table in $TABLES; do
    echo "-- TABLE: $table"
    psql watchlist -c "\d $table" 2>/dev/null | grep -E "^\s+\w+" | head -20
    echo ""
done
