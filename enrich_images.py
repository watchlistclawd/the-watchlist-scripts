#!/usr/bin/env python3
"""
Generate SQL UPDATE statements to add images to database records.
Matches records by external IDs (anilist_id, mal_id, etc.) and applies
images from the manifest using the preference hierarchy.
"""
import json
import os
import sys
from pathlib import Path

# Preference order for selecting images
IMAGE_PREFERENCE = {
    "creators": ["anilist", "mal"],
    "entries": ["mal", "anilist", "tvdb"],
    "characters": ["anilist", "mal"],
}


def load_manifest(manifest_path: str) -> dict:
    """Load image manifest from JSON file."""
    with open(manifest_path) as f:
        return json.load(f)


def get_best_image(manifest: dict, entity_type: str, external_ids: dict) -> str | None:
    """
    Get the best available image path based on preference hierarchy.
    
    external_ids: {"anilist_id": "123", "mal_id": "456", ...}
    """
    prefs = IMAGE_PREFERENCE.get(entity_type, ["anilist", "mal"])
    entity_images = manifest.get(entity_type, {})
    
    for source in prefs:
        id_key = f"{source}_id"
        source_id = external_ids.get(id_key)
        if source_id:
            manifest_key = f"{source}:{source_id}"
            if manifest_key in entity_images:
                return entity_images[manifest_key]
    
    return None


def generate_enrichment_sql(manifest: dict, franchise_slug: str) -> list[str]:
    """
    Generate SQL UPDATE statements for all entities in the manifest.
    Returns list of SQL statements.
    """
    statements = []
    statements.append(f"-- Image enrichment for franchise: {franchise_slug}")
    statements.append(f"-- Generated from manifest")
    statements.append("")
    
    # Entries
    for key, path in manifest.get("entries", {}).items():
        source, source_id = key.split(":", 1)
        id_field = f"{source}_id"
        
        if source == "anilist":
            statements.append(f"""
UPDATE entries 
SET primary_image = '{path}', updated_at = NOW()
WHERE details->>'anilist_id' = '{source_id}'
  AND primary_image IS NULL;""")
        elif source == "mal":
            statements.append(f"""
UPDATE entries 
SET primary_image = '{path}', updated_at = NOW()
WHERE details->>'mal_id' = '{source_id}'
  AND primary_image IS NULL;""")
        elif source == "tvdb":
            statements.append(f"""
UPDATE entries 
SET primary_image = '{path}', updated_at = NOW()
WHERE details->>'tvdb_id' = '{source_id}'
  AND primary_image IS NULL;""")
    
    # Creators
    for key, path in manifest.get("creators", {}).items():
        source, source_id = key.split(":", 1)
        
        if source == "anilist":
            statements.append(f"""
UPDATE creators 
SET primary_image = '{path}', updated_at = NOW()
WHERE details->>'anilist_id' = '{source_id}'
  AND primary_image IS NULL;""")
        elif source == "mal":
            statements.append(f"""
UPDATE creators 
SET primary_image = '{path}', updated_at = NOW()
WHERE details->>'mal_id' = '{source_id}'
  AND primary_image IS NULL;""")
    
    # Characters
    for key, path in manifest.get("characters", {}).items():
        source, source_id = key.split(":", 1)
        
        if source == "anilist":
            statements.append(f"""
UPDATE characters 
SET primary_image = '{path}', updated_at = NOW()
WHERE details->>'anilist_id' = '{source_id}'
  AND primary_image IS NULL;""")
        elif source == "mal":
            statements.append(f"""
UPDATE characters 
SET primary_image = '{path}', updated_at = NOW()
WHERE details->>'mal_id' = '{source_id}'
  AND primary_image IS NULL;""")
    
    return statements


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 enrich_images.py <franchise-slug>")
        print("Example: python3 enrich_images.py attack-on-titan")
        sys.exit(1)
    
    franchise_slug = sys.argv[1]
    data_root = Path(__file__).parent.parent / "the-watchlist-data"
    manifest_path = data_root / "sources" / franchise_slug / "image_manifest.json"
    
    if not manifest_path.exists():
        print(f"Error: Manifest not found at {manifest_path}")
        print("Run image_downloader.py first to generate the manifest.")
        sys.exit(1)
    
    manifest = load_manifest(manifest_path)
    statements = generate_enrichment_sql(manifest, franchise_slug)
    
    # Output SQL
    output_path = data_root / "sources" / franchise_slug / "image_enrichment.sql"
    with open(output_path, "w") as f:
        f.write("\n".join(statements))
    
    print(f"Generated {len(statements)} SQL statements")
    print(f"Output: {output_path}")
    
    # Summary
    print(f"\nCounts:")
    print(f"  Entries: {len(manifest.get('entries', {}))}")
    print(f"  Creators: {len(manifest.get('creators', {}))}")
    print(f"  Characters: {len(manifest.get('characters', {}))}")


if __name__ == "__main__":
    main()
