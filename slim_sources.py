#!/usr/bin/env python3
"""
Create slim versions of source files for Haiku.
Extracts only the fields needed for data entry.
"""
import json
import sys

from config.role_blacklists import is_creator_role_blocked, is_company_role_blocked


def slim_anilist(data: dict) -> dict:
    """Extract essential fields from AniList data.
    
    Filters blacklisted roles, then limits staff to 15 and characters to 15.
    """
    # Priority roles for staff
    priority_roles = ['director', 'original creator', 'series composition', 'music', 'character design']
    
    staff_edges = data.get("staff", {}).get("edges") or []
    
    # Filter out blacklisted roles first
    staff_edges = [e for e in staff_edges if not is_creator_role_blocked(e.get("role", ""))]
    
    # Sort: priority roles first, then by order
    def staff_priority(e):
        role = (e.get("role") or "").lower()
        for i, pr in enumerate(priority_roles):
            if pr in role:
                return i
        return 99
    staff_edges = sorted(staff_edges, key=staff_priority)[:15]
    
    char_edges = data.get("characters", {}).get("edges") or []
    # MAIN characters first, then SUPPORTING
    char_edges = sorted(char_edges, key=lambda e: 0 if e.get("role") == "MAIN" else 1)[:15]
    
    return {
        "id": data.get("id"),
        "idMal": data.get("idMal"),
        "title": data.get("title"),
        "format": data.get("format"),
        "episodes": data.get("episodes"),
        "status": data.get("status"),
        "startDate": data.get("startDate"),
        "endDate": data.get("endDate"),
        "season": data.get("season"),
        "seasonYear": data.get("seasonYear"),
        "genres": data.get("genres"),
        "studios": [
            {"id": s.get("id"), "name": s.get("name"), "isAnimationStudio": s.get("isAnimationStudio")}
            for s in (data.get("studios", {}).get("nodes") or [])
            if s.get("isAnimationStudio") or not is_company_role_blocked("other")  # Keep animation studios, filter "other"
        ],
        "staff": [
            {
                "id": e.get("node", {}).get("id"),
                "name": e.get("node", {}).get("name", {}).get("full"),
                "nativeName": e.get("node", {}).get("name", {}).get("native"),
                "role": e.get("role")
            }
            for e in staff_edges
        ],
        "characters": [
            {
                "id": e.get("node", {}).get("id"),
                "name": e.get("node", {}).get("name", {}).get("full"),
                "nativeName": e.get("node", {}).get("name", {}).get("native"),
                "role": e.get("role"),
                "voiceActors": [
                    {
                        "id": va.get("id"),
                        "name": va.get("name", {}).get("full"),
                        "nativeName": va.get("name", {}).get("native")
                    }
                    for va in e.get("voiceActors", [])
                ]
            }
            for e in char_edges
        ]
    }

def slim_tvdb(data: dict) -> dict:
    """Extract essential fields from TVDB data.
    
    TVDB is the authoritative source for season structure.
    We extract: series info, seasons with episode counts, and key dates.
    """
    from collections import defaultdict
    
    # Count episodes per season from episode list
    season_eps = defaultdict(list)
    for ep in data.get("episodes", []):
        snum = ep.get("seasonNumber", 0)
        season_eps[snum].append({
            "number": ep.get("number"),
            "name": ep.get("name"),
            "aired": ep.get("aired")
        })
    
    # Build season summaries
    seasons = []
    for snum in sorted(season_eps.keys()):
        eps = season_eps[snum]
        aired_dates = [e["aired"] for e in eps if e.get("aired")]
        seasons.append({
            "seasonNumber": snum,
            "episodeCount": len(eps),
            "firstAired": min(aired_dates) if aired_dates else None,
            "lastAired": max(aired_dates) if aired_dates else None,
            "episodes": eps[:3]  # First 3 eps for title reference
        })
    
    # Get English name from aliases if available
    english_name = data.get("name")
    for alias in data.get("aliases", []):
        if alias.get("language") == "eng":
            english_name = alias.get("name")
            break
    
    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "englishName": english_name,
        "slug": data.get("slug"),
        "year": data.get("year"),
        "firstAired": data.get("firstAired"),
        "lastAired": data.get("lastAired"),
        "status": data.get("status", {}).get("name"),
        "overview": data.get("overview"),
        "seasons": seasons,
        "totalEpisodes": len(data.get("episodes", []))
    }


def slim_mal(data: dict) -> dict:
    """Extract essential fields from MAL/Jikan data.
    
    Filters blacklisted roles, then limits staff to 15 and characters to 15.
    """
    anime = data.get("anime", {})
    
    # Priority positions for staff
    priority_positions = ['director', 'original creator', 'series composition', 'music', 'character design', 'producer']
    
    staff_list = data.get("staff", [])
    
    # Filter out staff with only blacklisted positions
    def has_valid_position(s):
        positions = s.get("positions", [])
        return any(not is_creator_role_blocked(p) for p in positions)
    
    staff_list = [s for s in staff_list if has_valid_position(s)]
    
    # Also filter the positions themselves within each staff entry
    for s in staff_list:
        s["positions"] = [p for p in s.get("positions", []) if not is_creator_role_blocked(p)]
    
    def staff_priority(s):
        positions = [p.lower() for p in s.get("positions", [])]
        for i, pp in enumerate(priority_positions):
            if any(pp in pos for pos in positions):
                return i
        return 99
    staff_list = sorted(staff_list, key=staff_priority)[:15]
    
    char_list = data.get("characters", [])
    # Main first, then Supporting
    char_list = sorted(char_list, key=lambda c: 0 if c.get("role") == "Main" else 1)[:15]
    
    return {
        "mal_id": anime.get("mal_id"),
        "title": anime.get("title"),
        "title_english": anime.get("title_english"),
        "title_japanese": anime.get("title_japanese"),
        "type": anime.get("type"),
        "episodes": anime.get("episodes"),
        "status": anime.get("status"),
        "aired": anime.get("aired"),
        "season": anime.get("season"),
        "year": anime.get("year"),
        "studios": [
            {"mal_id": s.get("mal_id"), "name": s.get("name")}
            for s in anime.get("studios", [])
        ],
        "producers": [
            {"mal_id": p.get("mal_id"), "name": p.get("name")}
            for p in anime.get("producers", [])
        ],
        "staff": [
            {
                "mal_id": s.get("person", {}).get("mal_id"),
                "name": s.get("person", {}).get("name"),
                "positions": s.get("positions", [])
            }
            for s in staff_list
        ],
        "characters": [
            {
                "mal_id": c.get("character", {}).get("mal_id"),
                "name": c.get("character", {}).get("name"),
                "role": c.get("role"),
                "voiceActors": [
                    {
                        "mal_id": va.get("person", {}).get("mal_id"),
                        "name": va.get("person", {}).get("name"),
                        "language": va.get("language")
                    }
                    for va in c.get("voice_actors", [])
                    if va.get("language") == "Japanese"
                ]
            }
            for c in char_list
        ]
    }

if __name__ == "__main__":
    # Test
    import os
    sources_dir = os.path.join(os.path.dirname(__file__), "..", "the-watchlist-data", "sources")
    
    for franchise in ["sentenced-to-be-a-hero"]:
        print(f"=== {franchise} ===")
        
        # AniList
        al_dir = os.path.join(sources_dir, franchise, "anilist")
        if os.path.exists(al_dir):
            for f in os.listdir(al_dir):
                if f.endswith(".json"):
                    with open(os.path.join(al_dir, f)) as fh:
                        data = json.load(fh)
                    slim = slim_anilist(data)
                    print(f"AniList {f}: {len(json.dumps(data))} -> {len(json.dumps(slim))} chars")
        
        # MAL
        mal_dir = os.path.join(sources_dir, franchise, "mal")
        if os.path.exists(mal_dir):
            for f in os.listdir(mal_dir):
                if f.endswith(".json"):
                    with open(os.path.join(mal_dir, f)) as fh:
                        data = json.load(fh)
                    slim = slim_mal(data)
                    print(f"MAL {f}: {len(json.dumps(data))} -> {len(json.dumps(slim))} chars")
