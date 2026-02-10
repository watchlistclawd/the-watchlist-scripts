#!/usr/bin/env python3
"""
Haiku Data Entry - Hand source files to Haiku for database population.

Usage:
    python3 data_entry.py sentenced-to-be-a-hero
    python3 data_entry.py attack-on-titan
"""
import argparse
import json
import os
import subprocess
import sys
from slim_sources import slim_anilist, slim_mal, slim_tvdb

DATA_ROOT = os.path.join(os.path.dirname(__file__), "..", "the-watchlist-data")
SOURCES_DIR = os.path.join(DATA_ROOT, "sources")

def load_sources(franchise_slug: str) -> dict:
    """Load all source files for a franchise (slimmed for prompt size)."""
    franchise_dir = os.path.join(SOURCES_DIR, franchise_slug)
    if not os.path.exists(franchise_dir):
        raise FileNotFoundError(f"No sources found for: {franchise_slug}")
    
    sources = {"anilist": [], "mal": [], "tvdb": []}
    
    for source_type in sources.keys():
        source_dir = os.path.join(franchise_dir, source_type)
        if os.path.exists(source_dir):
            for filename in os.listdir(source_dir):
                if filename.endswith(".json"):
                    filepath = os.path.join(source_dir, filename)
                    with open(filepath) as f:
                        data = json.load(f)
                    
                    # Slim the data to reduce prompt size
                    if source_type == "anilist":
                        data = slim_anilist(data)
                    elif source_type == "mal":
                        data = slim_mal(data)
                    elif source_type == "tvdb":
                        data = slim_tvdb(data)
                    
                    sources[source_type].append({
                        "id": filename.replace(".json", ""),
                        "data": data
                    })
    
    return sources

def load_db_context() -> dict:
    """Load current DB context."""
    result = subprocess.run(
        ["python3", "build_db_context.py"],
        capture_output=True, text=True, cwd=os.path.dirname(__file__)
    )
    return json.loads(result.stdout)

def build_prompt(franchise_slug: str, sources: dict, context: dict) -> str:
    """Build the Haiku prompt."""
    prompt = f"""You are a data entry specialist for The Watchlist database.

## CRITICAL: Entry vs Season Structure

**TVDB is the authoritative source for season structure.**

- ONE entry = ONE conceptual work (e.g., "Attack on Titan" the anime series)
- Multiple AniList/MAL entries often represent SEASONS of the same show — consolidate them!
- TVDB seasons map to our entry_seasons table
- Example: AniList has 10 separate entries for Attack on Titan (S1, S2, S3P1, S3P2, S4P1...) → these are ONE entry with multiple seasons

**Mapping logic:**
1. Use TVDB to determine how many seasons exist
2. Match each AniList/MAL entry to a TVDB season by air date and episode count
3. Create ONE entry row, with MULTIPLE entry_seasons rows

## Database Context

### Company Roles (use these IDs):
{json.dumps(context['roles']['company_roles'], indent=2)}

### Creator Roles (use these IDs):
{json.dumps(context['roles']['creator_roles'], indent=2)}

### Existing Companies:
{json.dumps(context['existing']['companies'], indent=2) if context['existing']['companies'] else "None yet"}

### Existing Creators:
{json.dumps(context['existing']['creators'], indent=2) if context['existing']['creators'] else "None yet"}

### Existing Franchises:
{json.dumps(context['existing']['franchises'], indent=2) if context['existing']['franchises'] else "None yet"}

## Source Data for "{franchise_slug}"

### TVDB Data (AUTHORITATIVE for seasons):
{json.dumps(sources['tvdb'], indent=2) if sources['tvdb'] else "None available - use AniList/MAL season info"}

### AniList Data:
{json.dumps(sources['anilist'], indent=2)}

### MyAnimeList Data:
{json.dumps(sources['mal'], indent=2)}

## Your Task

Generate SQL INSERT statements:
1. Franchise (if not exists)
2. Companies/studios (if not in existing list)
3. Creators/staff - ONLY people in BOTH AniList AND MAL
4. ONE entry for the series (not one per season!)
5. entry_seasons - one row per TVDB season
6. Characters - ONLY those in BOTH AniList AND MAL
7. Link tables (entry_companies, entry_creators, entry_characters)

## Schema Reference

### entries table
- id, media_type_id (anime: 5ea63465-e02f-4a08-8343-bcc7f9e8b52c)
- title, alternate_titles[], slug
- release_date (first air date), status
- details JSONB (store anilist_id, mal_id arrays for all seasons)
- locale_code ('ja')

### entry_seasons table
- id, entry_id (FK), season_number, title
- episode_count, air_date_start, air_date_end
- synopsis, primary_image

## Rules
- gen_random_uuid() for IDs
- Match creators by name (MAL: "Last, First" → AniList: "First Last")
- ON CONFLICT DO NOTHING for upserts
- Season 0 = specials (usually skip unless significant)

## Output Format

Return ONLY valid SQL. Start with:
-- Franchise: {franchise_slug}
-- Entry: [title] (TVDB seasons: X, AniList entries consolidated: Y)

Insert order:
1. franchises
2. companies
3. creators  
4. entries (ONE row)
5. entry_seasons (multiple rows)
6. characters
7. entry_franchises
8. entry_companies
9. entry_creators
10. entry_characters

"""
    return prompt

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("franchise", help="Franchise slug (folder name in sources/)")
    parser.add_argument("--dry-run", action="store_true", help="Print prompt without calling Haiku")
    args = parser.parse_args()
    
    print(f"Loading sources for: {args.franchise}")
    sources = load_sources(args.franchise)
    print(f"  AniList: {len(sources['anilist'])} files")
    print(f"  MAL: {len(sources['mal'])} files")
    print(f"  TVDB: {len(sources['tvdb'])} files")
    
    print("Loading DB context...")
    context = load_db_context()
    print(f"  Company roles: {len(context['roles']['company_roles'])}")
    print(f"  Creator roles: {len(context['roles']['creator_roles'])}")
    print(f"  Existing companies: {len(context['existing']['companies'])}")
    print(f"  Existing creators: {len(context['existing']['creators'])}")
    
    print("Building prompt...")
    prompt = build_prompt(args.franchise, sources, context)
    
    if args.dry_run:
        print("\n" + "="*60)
        print("DRY RUN - Prompt would be:")
        print("="*60)
        print(prompt[:3000] + "\n...[truncated]...")
        print(f"\nTotal prompt length: {len(prompt)} chars")
        return
    
    # Save prompt for reference
    prompt_path = os.path.join(SOURCES_DIR, args.franchise, "haiku_prompt.txt")
    with open(prompt_path, "w") as f:
        f.write(prompt)
    print(f"Prompt saved to: {prompt_path}")
    print(f"Prompt size: {len(prompt)} chars (~{len(prompt)//4} tokens)")
    
    print("\nReady to send to Haiku. Run this in a session that can call the LLM.")

if __name__ == "__main__":
    main()
