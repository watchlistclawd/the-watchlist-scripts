#!/usr/bin/env python3
"""
Build DB context dump for Haiku data entry.
Outputs existing entities and roles for Haiku to reference.
"""
import json
import subprocess

def query(sql: str) -> str:
    """Run psql query and return output."""
    result = subprocess.run(
        ["psql", "watchlist", "-t", "-A", "-c", sql],
        capture_output=True, text=True
    )
    return result.stdout.strip()

def get_roles() -> dict:
    """Get all roles."""
    company_roles = []
    for line in query("SELECT id, name, display_name FROM company_roles").split("\n"):
        if line:
            parts = line.split("|")
            company_roles.append({"id": parts[0], "name": parts[1], "display_name": parts[2]})
    
    creator_roles = []
    for line in query("SELECT id, name, display_name, category FROM creator_roles").split("\n"):
        if line:
            parts = line.split("|")
            creator_roles.append({
                "id": parts[0], "name": parts[1], 
                "display_name": parts[2], "category": parts[3] if len(parts) > 3 else None
            })
    
    return {"company_roles": company_roles, "creator_roles": creator_roles}

def get_existing_entities() -> dict:
    """Get existing companies and creators."""
    companies = []
    for line in query("SELECT id, slug, name, wikidata_id FROM companies").split("\n"):
        if line:
            parts = line.split("|")
            companies.append({
                "id": parts[0], "slug": parts[1], 
                "name": parts[2], "wikidata_id": parts[3] if len(parts) > 3 else None
            })
    
    creators = []
    for line in query("SELECT id, slug, name, wikidata_id FROM creators").split("\n"):
        if line:
            parts = line.split("|")
            creators.append({
                "id": parts[0], "slug": parts[1],
                "name": parts[2], "wikidata_id": parts[3] if len(parts) > 3 else None
            })
    
    franchises = []
    for line in query("SELECT id, slug, name FROM franchises").split("\n"):
        if line:
            parts = line.split("|")
            franchises.append({"id": parts[0], "slug": parts[1], "name": parts[2]})
    
    return {
        "companies": companies,
        "creators": creators,
        "franchises": franchises
    }

def get_media_types() -> list:
    """Get media types."""
    types = []
    for line in query("SELECT id, name, display_name FROM media_types").split("\n"):
        if line:
            parts = line.split("|")
            types.append({"id": parts[0], "name": parts[1], "display_name": parts[2]})
    return types

def build_context() -> dict:
    """Build full context for Haiku."""
    return {
        "roles": get_roles(),
        "existing": get_existing_entities(),
        "media_types": get_media_types()
    }

if __name__ == "__main__":
    ctx = build_context()
    print(json.dumps(ctx, indent=2))
