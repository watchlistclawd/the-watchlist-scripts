#!/usr/bin/env python3
"""
Create slim versions of source files for LLM prompts.
Applies blacklists and filters to reduce token usage.
"""
import json
import sys
from collections import defaultdict

from config.role_blacklists import is_creator_role_blocked, is_company_role_blocked

# Language filters
KEEP_LANGUAGES = {'eng', 'jpn', 'kor', 'ja', 'en', 'ko', 'Japanese', 'English', 'Korean'}

# MAL relation types to exclude
RELATION_BLACKLIST = {'Summary', 'Character', 'Other'}

# Minimum tag rank to keep (top 30%)
MIN_TAG_RANK = 70


def slim_anilist(data: dict) -> dict:
    """Extract essential fields from AniList data."""
    
    # Priority roles for staff
    priority_roles = ['director', 'original creator', 'series composition', 'music', 'character design']
    
    staff_edges = data.get("staff", {}).get("edges") or []
    # Filter blacklisted roles
    staff_edges = [e for e in staff_edges if not is_creator_role_blocked(e.get("role", ""))]
    # Sort by priority
    def staff_priority(e):
        role = (e.get("role") or "").lower()
        for i, pr in enumerate(priority_roles):
            if pr in role:
                return i
        return 99
    staff_edges = sorted(staff_edges, key=staff_priority)[:15]
    
    char_edges = data.get("characters", {}).get("edges") or []
    char_edges = sorted(char_edges, key=lambda e: 0 if e.get("role") == "MAIN" else 1)[:15]
    
    # Filter tags by rank >= 70
    tags = [t for t in (data.get("tags") or []) if t.get("rank", 0) >= MIN_TAG_RANK]
    
    return {
        "id": data.get("id"),
        "idMal": data.get("idMal"),
        "title": data.get("title"),  # Already has romaji/english/native
        "format": data.get("format"),
        "episodes": data.get("episodes"),
        "status": data.get("status"),
        "startDate": data.get("startDate"),
        "endDate": data.get("endDate"),
        "season": data.get("season"),
        "seasonYear": data.get("seasonYear"),
        "genres": data.get("genres"),
        "tags": [{"name": t.get("name"), "rank": t.get("rank")} for t in tags],
        "studios": [
            {"id": s.get("id"), "name": s.get("name"), "isAnimationStudio": s.get("isAnimationStudio")}
            for s in (data.get("studios", {}).get("nodes") or [])
            if s.get("isAnimationStudio") or not is_company_role_blocked("other")
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
                    # AniList already filters to requested language in query
                ]
            }
            for e in char_edges
        ]
    }


def slim_tvdb(data: dict) -> dict:
    """Extract essential fields from TVDB data."""
    
    # Build episode list per season with full details
    season_eps = defaultdict(list)
    for ep in data.get("episodes", []):
        snum = ep.get("seasonNumber", 0)
        season_eps[snum].append({
            "number": ep.get("number"),
            "absoluteNumber": ep.get("absoluteNumber"),
            "title": ep.get("name"),
            "aired": ep.get("aired"),
            "runtime": ep.get("runtime")
        })
    
    # Build season summaries (episodes summarized for LLM, full list saved separately)
    seasons = []
    for snum in sorted(season_eps.keys()):
        eps = season_eps[snum]
        aired_dates = [e["aired"] for e in eps if e.get("aired")]
        seasons.append({
            "seasonNumber": snum,
            "episodeCount": len(eps),
            "firstAired": min(aired_dates) if aired_dates else None,
            "lastAired": max(aired_dates) if aired_dates else None,
            # First and last episode titles for context
            "firstEpisode": eps[0].get("title") if eps else None,
            "lastEpisode": eps[-1].get("title") if eps else None
        })
    
    # Filter aliases to eng/jpn/kor only
    aliases = [
        a for a in (data.get("aliases") or [])
        if a.get("language") in KEEP_LANGUAGES
    ]
    
    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "slug": data.get("slug"),
        "year": data.get("year"),
        "firstAired": data.get("firstAired"),
        "lastAired": data.get("lastAired"),
        "status": data.get("status", {}).get("name") if isinstance(data.get("status"), dict) else data.get("status"),
        "overview": data.get("overview"),
        "aliases": [{"language": a.get("language"), "name": a.get("name")} for a in aliases],
        "seasons": seasons,
        "totalEpisodes": len(data.get("episodes", []))
    }


def slim_mal(data: dict) -> dict:
    """Extract essential fields from MAL/Jikan data."""
    
    anime = data.get("anime", {})
    
    # Priority positions for staff
    priority_positions = ['director', 'original creator', 'series composition', 'music', 'character design', 'producer']
    
    staff_list = data.get("staff", [])
    # Filter blacklisted positions
    def has_valid_position(s):
        positions = s.get("positions", [])
        return any(not is_creator_role_blocked(p) for p in positions)
    staff_list = [s for s in staff_list if has_valid_position(s)]
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
    char_list = sorted(char_list, key=lambda c: 0 if c.get("role") == "Main" else 1)[:15]
    
    # Filter titles to eng/jpn/kor/romaji (Default, Japanese, English, Korean)
    titles = [
        t for t in (anime.get("titles") or [])
        if t.get("type") in {'Default', 'Japanese', 'English', 'Korean', 'Synonym'}
    ]
    
    # Filter relations - exclude Summary/Character/Other
    relations = [
        {
            "relation": r.get("relation"),
            "entry": [{"mal_id": e.get("mal_id"), "type": e.get("type"), "name": e.get("name")} for e in r.get("entry", [])]
        }
        for r in (anime.get("relations") or [])
        if r.get("relation") not in RELATION_BLACKLIST
    ]
    
    return {
        "mal_id": anime.get("mal_id"),
        "title": anime.get("title"),
        "title_english": anime.get("title_english"),
        "title_japanese": anime.get("title_japanese"),
        "titles": titles,
        "type": anime.get("type"),
        "source": anime.get("source"),
        "episodes": anime.get("episodes"),
        "status": anime.get("status"),
        "aired": anime.get("aired"),
        "season": anime.get("season"),
        "year": anime.get("year"),
        "genres": [{"mal_id": g.get("mal_id"), "name": g.get("name")} for g in (anime.get("genres") or [])],
        "themes": [{"mal_id": t.get("mal_id"), "name": t.get("name")} for t in (anime.get("themes") or [])],
        "demographics": [{"mal_id": d.get("mal_id"), "name": d.get("name")} for d in (anime.get("demographics") or [])],
        "studios": [
            {"mal_id": s.get("mal_id"), "name": s.get("name")}
            for s in (anime.get("studios") or [])
        ],
        "producers": [
            {"mal_id": p.get("mal_id"), "name": p.get("name")}
            for p in (anime.get("producers") or [])
        ],
        "relations": relations,
        "external": anime.get("external"),  # Keep external links
        "streaming": anime.get("streaming"),  # Keep streaming links
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
                    if va.get("language") in KEEP_LANGUAGES
                ]
            }
            for c in char_list
        ]
    }


if __name__ == "__main__":
    import os
    
    sources_dir = os.path.join(os.path.dirname(__file__), "..", "the-watchlist-data", "sources")
    
    for franchise in ["sentenced-to-be-a-hero", "attack-on-titan"]:
        fdir = os.path.join(sources_dir, franchise)
        if not os.path.exists(fdir):
            continue
            
        print(f"\n=== {franchise} ===")
        total_before = 0
        total_after = 0
        
        # AniList
        al_dir = os.path.join(fdir, "anilist")
        if os.path.exists(al_dir):
            for f in sorted(os.listdir(al_dir)):
                if f.endswith(".json"):
                    with open(os.path.join(al_dir, f)) as fh:
                        data = json.load(fh)
                    slim = slim_anilist(data)
                    before = len(json.dumps(data))
                    after = len(json.dumps(slim))
                    total_before += before
                    total_after += after
                    print(f"  AL {f}: {before:,} -> {after:,} ({100*after//before}%)")
        
        # MAL
        mal_dir = os.path.join(fdir, "mal")
        if os.path.exists(mal_dir):
            for f in sorted(os.listdir(mal_dir)):
                if f.endswith(".json"):
                    with open(os.path.join(mal_dir, f)) as fh:
                        data = json.load(fh)
                    slim = slim_mal(data)
                    before = len(json.dumps(data))
                    after = len(json.dumps(slim))
                    total_before += before
                    total_after += after
                    print(f"  MAL {f}: {before:,} -> {after:,} ({100*after//before}%)")
        
        # TVDB
        tvdb_dir = os.path.join(fdir, "tvdb")
        if os.path.exists(tvdb_dir):
            for f in sorted(os.listdir(tvdb_dir)):
                if f.endswith(".json"):
                    with open(os.path.join(tvdb_dir, f)) as fh:
                        data = json.load(fh)
                    slim = slim_tvdb(data)
                    before = len(json.dumps(data))
                    after = len(json.dumps(slim))
                    total_before += before
                    total_after += after
                    print(f"  TVDB {f}: {before:,} -> {after:,} ({100*after//before}%)")
        
        print(f"  TOTAL: {total_before:,} -> {total_after:,} chars ({100*total_after//total_before}%)")
        print(f"  Tokens: ~{total_before//4:,} -> ~{total_after//4:,}")
