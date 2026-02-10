#!/usr/bin/env python3
"""
Production Pipeline for The Watchlist Database
==============================================

Fetches anime/manga data from AniList, MAL, and TVDB, consolidates it,
and generates SQL for database population.

Usage:
    python3 pipeline.py fetch <franchise-slug> [--anilist-id ID] [--mal-id ID] [--tvdb-id ID]
    python3 pipeline.py process <franchise-slug>
    python3 pipeline.py generate <franchise-slug>
    python3 pipeline.py all <franchise-slug> [--anilist-id ID] [--mal-id ID] [--tvdb-id ID]

Pipeline stages:
    1. fetch    - Download data from APIs, save raw JSON, download images
    2. process  - Slim data, consolidate sources, normalize seasons
    3. generate - Generate SQL from consolidated data
    4. all      - Run all stages

Author: Omla 🦞
"""

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

# Add parent for imports
sys.path.insert(0, os.path.dirname(__file__))

from fetch_sources import (
    anilist_full as fetch_anilist_by_id,
    jikan_full as fetch_mal_full,
    tvdb_series_extended as fetch_tvdb_series,
    tvdb_login,
)

# Check if TVDB is available
try:
    TVDB_TOKEN = tvdb_login()
except:
    TVDB_TOKEN = None
from slim_sources import slim_anilist, slim_mal, slim_tvdb
from image_downloader import ImageManifest, extract_and_download_images

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_ROOT = SCRIPT_DIR.parent / "the-watchlist-data"
SOURCES_DIR = DATA_ROOT / "sources"

# Rate limiting
RATE_LIMIT_SECONDS = 1.0


def rate_limit():
    """Sleep to respect API rate limits."""
    time.sleep(RATE_LIMIT_SECONDS)


# =============================================================================
# STAGE 1: FETCH
# =============================================================================

def fetch_franchise(
    slug: str,
    anilist_id: Optional[int] = None,
    mal_id: Optional[int] = None,
    tvdb_id: Optional[int] = None,
) -> dict:
    """
    Fetch all data for a franchise from APIs.
    Returns summary of what was fetched.
    """
    franchise_dir = SOURCES_DIR / slug
    franchise_dir.mkdir(parents=True, exist_ok=True)
    
    results = {"anilist": [], "mal": [], "tvdb": [], "images": 0}
    manifest = ImageManifest(str(DATA_ROOT), slug)
    
    # --- AniList ---
    if anilist_id:
        print(f"\n=== Fetching AniList (starting from {anilist_id}) ===")
        anilist_dir = franchise_dir / "anilist"
        anilist_dir.mkdir(exist_ok=True)
        
        # Fetch main entry and all relations
        fetched_ids = set()
        to_fetch = [anilist_id]
        
        while to_fetch:
            current_id = to_fetch.pop(0)
            if current_id in fetched_ids:
                continue
            
            print(f"  Fetching AniList {current_id}...")
            try:
                data = fetch_anilist_by_id(current_id)
                if data:
                    # Save raw
                    with open(anilist_dir / f"{current_id}.json", "w") as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    
                    fetched_ids.add(current_id)
                    results["anilist"].append(current_id)
                    
                    # Download images
                    extract_and_download_images(manifest, "anilist", data)
                    
                    # Queue relations
                    for rel in data.get("relations", {}).get("edges", []):
                        rel_id = rel.get("node", {}).get("id")
                        rel_type = rel.get("relationType")
                        # Only follow certain relation types
                        if rel_id and rel_type in ["SEQUEL", "PREQUEL", "PARENT", "SIDE_STORY", "ALTERNATIVE"]:
                            if rel_id not in fetched_ids:
                                to_fetch.append(rel_id)
                    
                    rate_limit()
            except Exception as e:
                print(f"    Error: {e}")
    
    # --- MAL ---
    if mal_id or results["anilist"]:
        print(f"\n=== Fetching MAL ===")
        mal_dir = franchise_dir / "mal"
        mal_dir.mkdir(exist_ok=True)
        
        # Get MAL IDs from AniList data if not provided
        mal_ids_to_fetch = []
        if mal_id:
            mal_ids_to_fetch.append(mal_id)
        
        for al_id in results["anilist"]:
            al_file = anilist_dir / f"{al_id}.json"
            if al_file.exists():
                with open(al_file) as f:
                    al_data = json.load(f)
                    if al_data.get("idMal"):
                        mal_ids_to_fetch.append(al_data["idMal"])
        
        mal_ids_to_fetch = list(set(mal_ids_to_fetch))
        
        for mid in mal_ids_to_fetch:
            print(f"  Fetching MAL {mid}...")
            try:
                data = fetch_mal_full(mid)
                if data:
                    with open(mal_dir / f"{mid}.json", "w") as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    results["mal"].append(mid)
                    
                    # Download images
                    extract_and_download_images(manifest, "mal", data)
                    
                    rate_limit()
            except Exception as e:
                print(f"    Error: {e}")
    
    # --- TVDB ---
    if tvdb_id and TVDB_TOKEN:
        print(f"\n=== Fetching TVDB ===")
        tvdb_dir = franchise_dir / "tvdb"
        tvdb_dir.mkdir(exist_ok=True)
        
        print(f"  Fetching TVDB series {tvdb_id}...")
        try:
            data = fetch_tvdb_series(tvdb_id)
            if data:
                with open(tvdb_dir / f"{tvdb_id}.json", "w") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                results["tvdb"].append(tvdb_id)
                
                # Download images
                extract_and_download_images(manifest, "tvdb", data)
        except Exception as e:
            print(f"    Error: {e}")
    
    # Save manifest
    manifest.save()
    results["images"] = sum(len(v) for k, v in manifest.data.items() if isinstance(v, dict))
    
    print(f"\n=== Fetch Summary ===")
    print(f"  AniList entries: {len(results['anilist'])}")
    print(f"  MAL entries: {len(results['mal'])}")
    print(f"  TVDB entries: {len(results['tvdb'])}")
    print(f"  Images downloaded: {results['images']}")
    
    return results


# =============================================================================
# STAGE 2: PROCESS
# =============================================================================

@dataclass
class ConsolidatedCreator:
    """Creator consolidated from multiple sources."""
    name: str
    native_name: Optional[str] = None
    anilist_id: Optional[str] = None
    mal_id: Optional[str] = None
    roles: list = field(default_factory=list)
    
    @property
    def slug(self) -> str:
        return slugify(self.name)


@dataclass
class ConsolidatedCharacter:
    """Character consolidated from multiple sources."""
    name: str
    native_name: Optional[str] = None
    anilist_id: Optional[str] = None
    mal_id: Optional[str] = None
    role: str = "main"
    voice_actors: list = field(default_factory=list)  # List of (creator_slug, language)
    
    @property
    def slug(self) -> str:
        return slugify(self.name)


@dataclass  
class ConsolidatedSeason:
    """Season info consolidated from TVDB + AniList/MAL."""
    season_number: int
    title: Optional[str] = None
    episode_count: Optional[int] = None
    air_date_start: Optional[str] = None
    air_date_end: Optional[str] = None
    tvdb_id: Optional[int] = None
    anilist_ids: list = field(default_factory=list)
    mal_ids: list = field(default_factory=list)


@dataclass
class ConsolidatedEntry:
    """Entry consolidated from multiple sources."""
    title: str
    title_native: Optional[str] = None
    alternate_titles: list = field(default_factory=list)
    media_type: str = "anime"
    release_date: Optional[str] = None
    status: str = "released"
    genres: list = field(default_factory=list)
    tags: list = field(default_factory=list)
    anilist_ids: list = field(default_factory=list)
    mal_ids: list = field(default_factory=list)
    tvdb_id: Optional[int] = None
    seasons: list = field(default_factory=list)
    
    @property
    def slug(self) -> str:
        year = self.release_date[:4] if self.release_date else "unknown"
        return f"{slugify(self.title)}-{self.media_type}-{year}"


@dataclass
class ConsolidatedCompany:
    """Company consolidated from multiple sources."""
    name: str
    anilist_id: Optional[str] = None
    mal_id: Optional[str] = None
    role: str = "animation_studio"
    
    @property
    def slug(self) -> str:
        return slugify(self.name)


@dataclass
class ProcessedFranchise:
    """Fully processed franchise data."""
    slug: str
    name: str
    entries: list = field(default_factory=list)
    creators: dict = field(default_factory=dict)  # slug -> ConsolidatedCreator
    characters: dict = field(default_factory=dict)  # slug -> ConsolidatedCharacter
    companies: dict = field(default_factory=dict)  # slug -> ConsolidatedCompany


def slugify(text: str) -> str:
    """Generate URL-safe slug from text."""
    if not text:
        return "unknown"
    text = text.lower().strip()
    text = re.sub(r"[/\\]", "-", text)
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    return text.strip("-")[:80]


def normalize_name(name: str) -> str:
    """Normalize name for matching (handles MAL 'Last, First' format)."""
    if not name:
        return ""
    # MAL format: "Last, First" -> "First Last"
    if ", " in name:
        parts = name.split(", ", 1)
        if len(parts) == 2:
            name = f"{parts[1]} {parts[0]}"
    return name.lower().strip()


def process_franchise(slug: str) -> ProcessedFranchise:
    """
    Process raw source data into consolidated format.
    Uses TVDB as authority for season structure.
    """
    franchise_dir = SOURCES_DIR / slug
    if not franchise_dir.exists():
        raise FileNotFoundError(f"No sources for {slug}")
    
    result = ProcessedFranchise(slug=slug, name=slug.replace("-", " ").title())
    
    # Load all source data
    anilist_data = []
    mal_data = []
    tvdb_data = []
    
    anilist_dir = franchise_dir / "anilist"
    if anilist_dir.exists():
        for f in anilist_dir.glob("*.json"):
            with open(f) as fh:
                data = json.load(fh)
                anilist_data.append(slim_anilist(data))
        # Sort by release date (earliest first) to pick the main entry
        def get_start_date(d):
            s = d.get("startDate", {})
            if s.get("year"):
                return (s["year"], s.get("month", 1) or 1, s.get("day", 1) or 1)
            return (9999, 1, 1)
        anilist_data.sort(key=get_start_date)
    
    mal_dir = franchise_dir / "mal"
    if mal_dir.exists():
        for f in sorted(mal_dir.glob("*.json")):
            with open(f) as fh:
                data = json.load(fh)
                mal_data.append(slim_mal(data))
    
    tvdb_dir = franchise_dir / "tvdb"
    if tvdb_dir.exists():
        for f in sorted(tvdb_dir.glob("*.json")):
            with open(f) as fh:
                data = json.load(fh)
                tvdb_data.append(slim_tvdb(data))
    
    print(f"\n=== Processing {slug} ===")
    print(f"  AniList entries: {len(anilist_data)}")
    print(f"  MAL entries: {len(mal_data)}")
    print(f"  TVDB entries: {len(tvdb_data)}")
    
    # --- Determine franchise name ---
    if anilist_data:
        # Use first AniList entry's title
        first = anilist_data[0]
        result.name = first.get("title", {}).get("english") or first.get("title", {}).get("romaji") or slug
    
    # --- Process companies ---
    for al in anilist_data:
        for studio in al.get("studios", []):
            name = studio.get("name")
            if not name:
                continue
            s = slugify(name)
            if s not in result.companies:
                result.companies[s] = ConsolidatedCompany(
                    name=name,
                    anilist_id=str(studio.get("id")),
                    role="animation_studio" if studio.get("isAnimationStudio") else "producer"
                )
    
    for ml in mal_data:
        anime = ml.get("anime", {})
        for studio in anime.get("studios", []):
            name = studio.get("name")
            if not name:
                continue
            s = slugify(name)
            if s not in result.companies:
                result.companies[s] = ConsolidatedCompany(
                    name=name,
                    mal_id=str(studio.get("mal_id")),
                    role="animation_studio"
                )
            elif not result.companies[s].mal_id:
                result.companies[s].mal_id = str(studio.get("mal_id"))
        
        for prod in anime.get("producers", []):
            name = prod.get("name")
            if not name:
                continue
            s = slugify(name)
            if s not in result.companies:
                result.companies[s] = ConsolidatedCompany(
                    name=name,
                    mal_id=str(prod.get("mal_id")),
                    role="producer"
                )
    
    # --- Process creators ---
    # Build name->creator map for matching
    creator_by_normalized = {}
    
    for al in anilist_data:
        for staff in al.get("staff", []):
            name = staff.get("name")
            native = staff.get("nativeName")
            aid = str(staff.get("id"))
            role = staff.get("role", "")
            
            if not name:
                continue
            
            norm = normalize_name(name)
            if norm in creator_by_normalized:
                c = creator_by_normalized[norm]
                if not c.anilist_id:
                    c.anilist_id = aid
                if role and role not in c.roles:
                    c.roles.append(role)
            else:
                c = ConsolidatedCreator(
                    name=name,
                    native_name=native,
                    anilist_id=aid,
                    roles=[role] if role else []
                )
                creator_by_normalized[norm] = c
        
        # Voice actors from characters
        for char in al.get("characters", []):
            for va in char.get("voiceActors", []):
                name = va.get("name")
                native = va.get("nativeName")
                aid = str(va.get("id"))
                
                if not name:
                    continue
                
                norm = normalize_name(name)
                if norm in creator_by_normalized:
                    c = creator_by_normalized[norm]
                    if not c.anilist_id:
                        c.anilist_id = aid
                else:
                    c = ConsolidatedCreator(
                        name=name,
                        native_name=native,
                        anilist_id=aid,
                        roles=["Voice Actor"]
                    )
                    creator_by_normalized[norm] = c
    
    # Match with MAL
    for ml in mal_data:
        for staff in ml.get("staff", []):
            person = staff.get("person", {}) if isinstance(staff, dict) else staff
            name = person.get("name", "")
            mid = str(person.get("mal_id", ""))
            
            # MAL uses "Last, First" format
            norm = normalize_name(name)
            if norm in creator_by_normalized:
                c = creator_by_normalized[norm]
                if not c.mal_id:
                    c.mal_id = mid
        
        for char in ml.get("characters", []):
            for va in char.get("voice_actors", []):
                if va.get("language") not in ("Japanese", "English", "Korean"):
                    continue
                person = va.get("person", {})
                name = person.get("name", "")
                mid = str(person.get("mal_id", ""))
                
                norm = normalize_name(name)
                if norm in creator_by_normalized:
                    c = creator_by_normalized[norm]
                    if not c.mal_id:
                        c.mal_id = mid
    
    # Store by slug
    for c in creator_by_normalized.values():
        result.creators[c.slug] = c
    
    # --- Process characters ---
    char_by_normalized = {}
    
    for al in anilist_data:
        for char in al.get("characters", []):
            name = char.get("name")
            native = char.get("nativeName")
            aid = str(char.get("id"))
            role = char.get("role", "MAIN").lower()
            
            if not name:
                continue
            
            norm = normalize_name(name)
            if norm not in char_by_normalized:
                c = ConsolidatedCharacter(
                    name=name,
                    native_name=native,
                    anilist_id=aid,
                    role="main" if role == "main" else "supporting"
                )
                char_by_normalized[norm] = c
            
            # Add voice actors
            for va in char.get("voiceActors", []):
                va_name = va.get("name")
                if va_name:
                    va_slug = slugify(va_name)
                    # Assume Japanese unless specified
                    char_by_normalized[norm].voice_actors.append((va_slug, "ja"))
    
    # Match with MAL
    for ml in mal_data:
        for char in ml.get("characters", []):
            character = char.get("character", {}) if isinstance(char, dict) else char
            name = character.get("name", "")
            mid = str(character.get("mal_id", ""))
            
            # MAL uses "Last, First" format for characters too
            norm = normalize_name(name)
            if norm in char_by_normalized:
                c = char_by_normalized[norm]
                if not c.mal_id:
                    c.mal_id = mid
                
                # Add English VAs
                for va in char.get("voice_actors", []):
                    lang = va.get("language", "")
                    if lang == "English":
                        person = va.get("person", {})
                        va_name = person.get("name", "")
                        if va_name:
                            va_slug = slugify(normalize_name(va_name))
                            c.voice_actors.append((va_slug, "en"))
    
    for c in char_by_normalized.values():
        result.characters[c.slug] = c
    
    # --- Build consolidated entry ---
    # For now, create one entry per distinct work
    # Group AniList entries by whether they're seasons of the same show
    
    if anilist_data:
        # Use first entry as the main entry
        first = anilist_data[0]
        
        # Collect all anilist/mal IDs
        all_anilist_ids = [str(al.get("id")) for al in anilist_data]
        all_mal_ids = [str(ml.get("anime", {}).get("mal_id") or ml.get("mal_id")) for ml in mal_data]
        all_mal_ids = [m for m in all_mal_ids if m and m != "None"]
        
        # Build genres
        genres = []
        for g in first.get("genres", []):
            genres.append(g.lower().replace(" ", "_").replace("-", "_"))
        
        # Build tags (from AniList tags with rank >= 70)
        tags = []
        for t in first.get("tags", []):
            if t.get("rank", 0) >= 70:
                tags.append(t.get("name", "").lower().replace(" ", "_").replace("-", "_"))
        
        # Determine release date
        start = first.get("startDate", {})
        release_date = None
        if start.get("year"):
            y = start["year"]
            m = start.get("month", 1) or 1
            d = start.get("day", 1) or 1
            release_date = f"{y:04d}-{m:02d}-{d:02d}"
        
        # Determine status
        status_map = {
            "RELEASING": "releasing",
            "FINISHED": "released",
            "NOT_YET_RELEASED": "announced",
            "CANCELLED": "cancelled",
            "HIATUS": "hiatus"
        }
        status = status_map.get(first.get("status"), "released")
        
        entry = ConsolidatedEntry(
            title=first.get("title", {}).get("english") or first.get("title", {}).get("romaji") or slug,
            title_native=first.get("title", {}).get("native"),
            alternate_titles=[first.get("title", {}).get("romaji")] if first.get("title", {}).get("romaji") else [],
            media_type="anime",
            release_date=release_date,
            status=status,
            genres=genres,
            tags=tags,
            anilist_ids=all_anilist_ids,
            mal_ids=all_mal_ids,
        )
        
        # Build seasons
        # If we have TVDB, use that. Otherwise, create seasons from AniList entries.
        if tvdb_data:
            # Use TVDB entry with most episodes as the primary
            tvdb = max(tvdb_data, key=lambda t: t.get("totalEpisodes", 0))
            entry.tvdb_id = tvdb.get("id")
            
            # Use pre-processed seasons from slim_tvdb
            for season in tvdb.get("seasons", []):
                sn = season.get("seasonNumber", 0)
                if sn == 0:  # Skip specials
                    continue
                entry.seasons.append(ConsolidatedSeason(
                    season_number=sn,
                    episode_count=season.get("episodeCount"),
                    air_date_start=season.get("firstAired"),
                    air_date_end=season.get("lastAired"),
                    tvdb_id=entry.tvdb_id,
                ))
        else:
            # Create single season from first AniList entry
            episodes = first.get("episodes")
            end = first.get("endDate", {})
            end_date = None
            if end.get("year"):
                y = end["year"]
                m = end.get("month", 1) or 1
                d = end.get("day", 1) or 1
                end_date = f"{y:04d}-{m:02d}-{d:02d}"
            
            entry.seasons.append(ConsolidatedSeason(
                season_number=1,
                title="Season 1",
                episode_count=episodes,
                air_date_start=release_date,
                air_date_end=end_date,
                anilist_ids=all_anilist_ids,
                mal_ids=all_mal_ids,
            ))
        
        result.entries.append(entry)
    
    # Save processed data
    processed_path = franchise_dir / "processed.json"
    with open(processed_path, "w") as f:
        # Convert dataclasses to dicts
        output = {
            "slug": result.slug,
            "name": result.name,
            "entries": [],
            "creators": {},
            "characters": {},
            "companies": {},
        }
        for e in result.entries:
            output["entries"].append({
                "title": e.title,
                "title_native": e.title_native,
                "alternate_titles": e.alternate_titles,
                "media_type": e.media_type,
                "release_date": e.release_date,
                "status": e.status,
                "genres": e.genres,
                "tags": e.tags,
                "anilist_ids": e.anilist_ids,
                "mal_ids": e.mal_ids,
                "tvdb_id": e.tvdb_id,
                "slug": e.slug,
                "seasons": [{
                    "season_number": s.season_number,
                    "title": s.title,
                    "episode_count": s.episode_count,
                    "air_date_start": s.air_date_start,
                    "air_date_end": s.air_date_end,
                    "tvdb_id": s.tvdb_id,
                } for s in e.seasons]
            })
        for slug, c in result.creators.items():
            output["creators"][slug] = {
                "name": c.name,
                "native_name": c.native_name,
                "anilist_id": c.anilist_id,
                "mal_id": c.mal_id,
                "roles": c.roles,
            }
        for slug, c in result.characters.items():
            output["characters"][slug] = {
                "name": c.name,
                "native_name": c.native_name,
                "anilist_id": c.anilist_id,
                "mal_id": c.mal_id,
                "role": c.role,
                "voice_actors": c.voice_actors,
            }
        for slug, c in result.companies.items():
            output["companies"][slug] = {
                "name": c.name,
                "anilist_id": c.anilist_id,
                "mal_id": c.mal_id,
                "role": c.role,
            }
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n=== Process Summary ===")
    print(f"  Entries: {len(result.entries)}")
    print(f"  Creators: {len(result.creators)}")
    print(f"  Characters: {len(result.characters)}")
    print(f"  Companies: {len(result.companies)}")
    print(f"  Saved to: {processed_path}")
    
    return result


# =============================================================================
# STAGE 3: GENERATE SQL
# =============================================================================

def generate_sql(slug: str) -> str:
    """
    Generate SQL INSERT statements from processed data.
    Returns SQL as string and saves to file.
    """
    franchise_dir = SOURCES_DIR / slug
    processed_path = franchise_dir / "processed.json"
    
    if not processed_path.exists():
        raise FileNotFoundError(f"No processed data for {slug}. Run 'process' first.")
    
    with open(processed_path) as f:
        data = json.load(f)
    
    sql_lines = []
    sql_lines.append(f"-- ============================================================================")
    sql_lines.append(f"-- SQL for franchise: {slug}")
    sql_lines.append(f"-- Generated by pipeline.py")
    sql_lines.append(f"-- ============================================================================")
    sql_lines.append("")
    
    # Use a transaction
    sql_lines.append("BEGIN;")
    sql_lines.append("")
    
    # --- Franchise ---
    sql_lines.append("-- Franchise")
    sql_lines.append(f"""
INSERT INTO franchises (id, name, slug)
VALUES (gen_random_uuid(), '{escape_sql(data['name'])}', '{data['slug']}')
ON CONFLICT (slug) DO NOTHING;
""")
    
    # --- Companies ---
    if data.get("companies"):
        sql_lines.append("-- Companies")
        for slug_key, company in data["companies"].items():
            websites = {}
            if company.get("anilist_id"):
                websites["anilist_id"] = company["anilist_id"]
            if company.get("mal_id"):
                websites["mal_id"] = company["mal_id"]
            
            sql_lines.append(f"""
INSERT INTO companies (id, name, slug, websites)
VALUES (gen_random_uuid(), '{escape_sql(company['name'])}', '{slug_key}', '{json.dumps(websites)}'::jsonb)
ON CONFLICT (slug) DO NOTHING;
""")
    
    # --- Creators ---
    if data.get("creators"):
        sql_lines.append("-- Creators")
        for slug_key, creator in data["creators"].items():
            details = {}
            if creator.get("anilist_id"):
                details["anilist_id"] = creator["anilist_id"]
            if creator.get("mal_id"):
                details["mal_id"] = creator["mal_id"]
            
            native = f"'{escape_sql(creator['native_name'])}'" if creator.get("native_name") else "NULL"
            
            sql_lines.append(f"""
INSERT INTO creators (id, full_name, native_name, slug, details)
VALUES (gen_random_uuid(), '{escape_sql(creator['name'])}', {native}, '{slug_key}', '{json.dumps(details)}'::jsonb)
ON CONFLICT (slug) DO NOTHING;
""")
    
    # --- Entries ---
    for entry in data.get("entries", []):
        sql_lines.append("-- Entry: " + entry["title"])
        
        details = {
            "anilist_ids": entry.get("anilist_ids", []),
            "mal_ids": entry.get("mal_ids", []),
        }
        if entry.get("tvdb_id"):
            details["tvdb_id"] = str(entry["tvdb_id"])
        
        alt_titles = entry.get("alternate_titles", [])
        if entry.get("title_native"):
            alt_titles.append(entry["title_native"])
        alt_titles_sql = "ARRAY[" + ", ".join(f"'{escape_sql(t)}'" for t in alt_titles) + "]" if alt_titles else "NULL"
        
        release_date = f"'{entry['release_date']}'" if entry.get("release_date") else "NULL"
        
        sql_lines.append(f"""
INSERT INTO entries (id, media_type_id, title, alternate_titles, slug, release_date, status, locale_code, details)
SELECT gen_random_uuid(), mt.id, '{escape_sql(entry['title'])}', {alt_titles_sql}, '{entry['slug']}',
       {release_date}, '{entry.get('status', 'released')}', 'ja', '{json.dumps(details)}'::jsonb
FROM media_types mt WHERE mt.name = '{entry.get('media_type', 'anime')}'
ON CONFLICT (slug) DO NOTHING;
""")
        
        # --- Entry seasons ---
        for season in entry.get("seasons", []):
            title = f"'{escape_sql(season['title'])}'" if season.get("title") else "NULL"
            air_start = f"'{season['air_date_start']}'" if season.get("air_date_start") else "NULL"
            air_end = f"'{season['air_date_end']}'" if season.get("air_date_end") else "NULL"
            ep_count = season.get("episode_count") or "NULL"
            tvdb_id = season.get("tvdb_id") or "NULL"
            
            sql_lines.append(f"""
INSERT INTO entry_seasons (id, entry_id, season_number, title, episode_count, air_date_start, air_date_end, tvdb_id)
SELECT gen_random_uuid(), e.id, {season['season_number']}, {title}, {ep_count}, {air_start}, {air_end}, {tvdb_id}
FROM entries e WHERE e.slug = '{entry['slug']}'
ON CONFLICT (entry_id, season_number) DO NOTHING;
""")
        
        # --- Entry-Franchise link ---
        sql_lines.append(f"""
INSERT INTO entry_franchises (id, entry_id, franchise_id)
SELECT gen_random_uuid(), e.id, f.id
FROM entries e, franchises f
WHERE e.slug = '{entry['slug']}' AND f.slug = '{data['slug']}'
ON CONFLICT (entry_id, franchise_id) DO NOTHING;
""")
        
        # --- Entry-Company links ---
        for company_slug, company in data.get("companies", {}).items():
            sql_lines.append(f"""
INSERT INTO entry_companies (id, entry_id, company_id, role_id)
SELECT gen_random_uuid(), e.id, c.id, cr.id
FROM entries e, companies c, company_roles cr
WHERE e.slug = '{entry['slug']}' AND c.slug = '{company_slug}' AND cr.name = '{company['role']}'
ON CONFLICT (entry_id, company_id, role_id) DO NOTHING;
""")
        
        # --- Entry-Genre links ---
        for genre in entry.get("genres", []):
            sql_lines.append(f"""
INSERT INTO entry_genres (id, entry_id, genre_id)
SELECT gen_random_uuid(), e.id, g.id
FROM entries e, genres g
WHERE e.slug = '{entry['slug']}' AND g.name = '{genre}'
ON CONFLICT (entry_id, genre_id) DO NOTHING;
""")
        
        # --- Entry-Tag links ---
        for tag in entry.get("tags", []):
            sql_lines.append(f"""
INSERT INTO entry_tags (id, entry_id, tag_id)
SELECT gen_random_uuid(), e.id, t.id
FROM entries e, tags t
WHERE e.slug = '{entry['slug']}' AND t.name = '{tag}'
ON CONFLICT (entry_id, tag_id) DO NOTHING;
""")
    
    # --- Characters ---
    if data.get("characters"):
        sql_lines.append("-- Characters")
        for slug_key, char in data["characters"].items():
            details = {}
            if char.get("anilist_id"):
                details["anilist_id"] = char["anilist_id"]
            if char.get("mal_id"):
                details["mal_id"] = char["mal_id"]
            
            native = f"'{escape_sql(char['native_name'])}'" if char.get("native_name") else "NULL"
            char_slug = f"{slug_key}-{data['slug']}"
            
            sql_lines.append(f"""
INSERT INTO characters (id, name, native_name, slug, franchise_id, details)
SELECT gen_random_uuid(), '{escape_sql(char['name'])}', {native}, '{char_slug}', f.id, '{json.dumps(details)}'::jsonb
FROM franchises f WHERE f.slug = '{data['slug']}'
ON CONFLICT (slug) DO NOTHING;
""")
            
            # Entry-Character link
            for entry in data.get("entries", []):
                sql_lines.append(f"""
INSERT INTO entry_characters (id, entry_id, character_id, role)
SELECT gen_random_uuid(), e.id, c.id, '{char.get('role', 'supporting')}'
FROM entries e, characters c
WHERE e.slug = '{entry['slug']}' AND c.slug = '{char_slug}'
ON CONFLICT (entry_id, character_id) DO NOTHING;
""")
    
    # --- Entry-Creator links ---
    sql_lines.append("-- Entry-Creator links (Voice Actors)")
    for char_slug, char in data.get("characters", {}).items():
        char_full_slug = f"{char_slug}-{data['slug']}"
        for va_slug, lang in char.get("voice_actors", []):
            for entry in data.get("entries", []):
                sql_lines.append(f"""
INSERT INTO entry_creators (id, entry_id, creator_id, role_id, character_id, language)
SELECT gen_random_uuid(), e.id, cr.id, r.id, ch.id, '{lang}'
FROM entries e, creators cr, creator_roles r, characters ch
WHERE e.slug = '{entry['slug']}' AND cr.slug = '{va_slug}' AND r.name = 'voice_actor' AND ch.slug = '{char_full_slug}'
ON CONFLICT DO NOTHING;
""")
    
    sql_lines.append("")
    sql_lines.append("COMMIT;")
    sql_lines.append("")
    sql_lines.append("-- Summary")
    sql_lines.append("SELECT 'Loaded: ' || (SELECT name FROM franchises WHERE slug = '" + data['slug'] + "') as franchise;")
    
    sql = "\n".join(sql_lines)
    
    # Save
    sql_path = franchise_dir / "insert.sql"
    with open(sql_path, "w") as f:
        f.write(sql)
    
    print(f"\n=== Generated SQL ===")
    print(f"  Saved to: {sql_path}")
    print(f"  Size: {len(sql):,} bytes")
    
    return sql


def escape_sql(s: str) -> str:
    """Escape string for SQL."""
    if not s:
        return ""
    return s.replace("'", "''").replace("\\", "\\\\")


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="The Watchlist Data Pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Fetch
    fetch_parser = subparsers.add_parser("fetch", help="Fetch data from APIs")
    fetch_parser.add_argument("slug", help="Franchise slug")
    fetch_parser.add_argument("--anilist-id", type=int, help="Starting AniList ID")
    fetch_parser.add_argument("--mal-id", type=int, help="MAL ID (optional, derived from AniList)")
    fetch_parser.add_argument("--tvdb-id", type=int, help="TVDB series ID")
    
    # Process
    process_parser = subparsers.add_parser("process", help="Process raw data")
    process_parser.add_argument("slug", help="Franchise slug")
    
    # Generate
    gen_parser = subparsers.add_parser("generate", help="Generate SQL")
    gen_parser.add_argument("slug", help="Franchise slug")
    
    # All
    all_parser = subparsers.add_parser("all", help="Run full pipeline")
    all_parser.add_argument("slug", help="Franchise slug")
    all_parser.add_argument("--anilist-id", type=int, help="Starting AniList ID")
    all_parser.add_argument("--mal-id", type=int, help="MAL ID")
    all_parser.add_argument("--tvdb-id", type=int, help="TVDB series ID")
    
    args = parser.parse_args()
    
    if args.command == "fetch":
        fetch_franchise(args.slug, args.anilist_id, args.mal_id, args.tvdb_id)
    
    elif args.command == "process":
        process_franchise(args.slug)
    
    elif args.command == "generate":
        generate_sql(args.slug)
    
    elif args.command == "all":
        fetch_franchise(args.slug, args.anilist_id, args.mal_id, args.tvdb_id)
        process_franchise(args.slug)
        generate_sql(args.slug)


if __name__ == "__main__":
    main()
