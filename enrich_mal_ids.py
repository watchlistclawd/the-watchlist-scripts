#!/usr/bin/env python3
"""
Enrich characters with MAL IDs by matching names.
"""

import json
import time
import urllib.request
from pathlib import Path

def normalize_name(name: str) -> str:
    """Normalize for matching - handles 'Last, First' format."""
    if not name:
        return ""
    if ", " in name:
        parts = name.split(", ", 1)
        if len(parts) == 2:
            name = f"{parts[1]} {parts[0]}"
    return name.lower().strip()

def fetch_mal_characters(mal_id: int) -> list:
    """Fetch characters from Jikan API."""
    url = f"https://api.jikan.moe/v4/anime/{mal_id}/characters"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "TheWatchlist/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            return data.get("data", [])
    except Exception as e:
        print(f"  Error fetching MAL {mal_id}: {e}")
        return []

def main():
    # Anime MAL IDs for our franchises
    anime_list = [
        ("sentenced-to-be-a-hero", 56009),
        ("attack-on-titan", 16498),
        ("jojos-bizarre-adventure", 14719),
    ]
    
    all_chars = {}  # normalized_name -> mal_id
    
    for franchise, mal_id in anime_list:
        print(f"\nFetching characters for {franchise} (MAL {mal_id})...")
        chars = fetch_mal_characters(mal_id)
        
        for c in chars:
            char = c.get("character", {})
            mal_char_id = char.get("mal_id")
            raw_name = char.get("name", "")
            norm = normalize_name(raw_name)
            
            if norm and mal_char_id:
                all_chars[norm] = {
                    "mal_id": mal_char_id,
                    "raw_name": raw_name,
                    "franchise": franchise
                }
        
        print(f"  Found {len(chars)} characters")
        time.sleep(1)  # Rate limit
    
    # Generate SQL
    print(f"\n\n-- Character MAL ID Updates")
    print(f"-- Total unique characters: {len(all_chars)}")
    print()
    
    for norm, data in sorted(all_chars.items()):
        mal_id = data["mal_id"]
        # Match by name pattern in our slug (slugified version)
        slug_pattern = norm.replace(" ", "-").replace("'", "")[:30]
        
        print(f"""
UPDATE characters 
SET details = COALESCE(details, '{{}}'::jsonb) || '{{"mal_id": "{mal_id}"}}'::jsonb
WHERE slug LIKE '%{slug_pattern}%' 
  AND (details->>'mal_id' IS NULL OR details->>'mal_id' = '');
-- {data['raw_name']}""")

if __name__ == "__main__":
    main()
