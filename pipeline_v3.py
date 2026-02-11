#!/usr/bin/env python3
"""
Production Pipeline v3 for The Watchlist Database
==================================================

Key changes from v2:
- Properly consolidates TV anime seasons into ONE entry
- Follows ENTRIES.md rules: entire serialized show = one entry
- AniList/MAL season splits → entry_seasons
- Spin-offs stay separate entries

Usage:
    python3 pipeline_v3.py fetch <franchise-slug> --anilist-id ID [--include-manga]
    python3 pipeline_v3.py process <franchise-slug>
    python3 pipeline_v3.py generate <franchise-slug>
    python3 pipeline_v3.py all <franchise-slug> --anilist-id ID [--include-manga]

Author: Omla 🦞
"""

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Set
from pathlib import Path
from collections import defaultdict

# Add parent for imports
sys.path.insert(0, os.path.dirname(__file__))

from fetch_sources import (
    anilist_query,
    jikan_full,
    tvdb_series_extended,
    tvdb_search,
    tvdb_login,
)
from slim_sources import slim_anilist, slim_mal, slim_tvdb
from image_downloader import ImageManifest, extract_and_download_images

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_ROOT = SCRIPT_DIR.parent / "the-watchlist-data"
SOURCES_DIR = DATA_ROOT / "sources"

# Rate limiting
RATE_LIMIT_SECONDS = 1.0

# Check TVDB
try:
    TVDB_TOKEN = tvdb_login()
except:
    TVDB_TOKEN = None


def rate_limit():
    time.sleep(RATE_LIMIT_SECONDS)


def slugify(text: str) -> str:
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
    if ", " in name:
        parts = name.split(", ", 1)
        if len(parts) == 2:
            name = f"{parts[1]} {parts[0]}"
    return name.lower().strip()


def escape_sql(s: str) -> str:
    if not s:
        return ""
    return s.replace("'", "''").replace("\\", "\\\\")


def escape_json_for_sql(obj: dict) -> str:
    """Convert dict to JSON string escaped for SQL single-quote wrapper."""
    json_str = json.dumps(obj, ensure_ascii=False)
    return json_str.replace("'", "''")


# =============================================================================
# ANILIST QUERIES
# =============================================================================

ANILIST_FULL_QUERY = """
query ($id: Int) {
  Media(id: $id) {
    id
    idMal
    title {
      romaji
      english
      native
    }
    type
    format
    status
    description(asHtml: false)
    startDate { year month day }
    endDate { year month day }
    season
    seasonYear
    episodes
    duration
    chapters
    volumes
    source
    genres
    tags {
      name
      rank
      isMediaSpoiler
    }
    relations {
      edges {
        relationType
        node {
          id
          idMal
          type
          format
          title { romaji english native }
          startDate { year month day }
        }
      }
    }
    studios {
      nodes {
        id
        name
        isAnimationStudio
      }
    }
    staff {
      edges {
        role
        node {
          id
          name { full native }
          description
          primaryOccupations
          dateOfBirth { year month day }
          dateOfDeath { year month day }
          siteUrl
        }
      }
    }
    characters {
      edges {
        role
        node {
          id
          name { full native alternative }
          description
          siteUrl
        }
        voiceActors(language: JAPANESE) {
          id
          name { full native }
          languageV2
          siteUrl
        }
      }
    }
    externalLinks {
      site
      url
      type
    }
    siteUrl
  }
}
"""


def fetch_anilist_full(anilist_id: int) -> dict:
    """Fetch full AniList data with all fields."""
    try:
        result = anilist_query(ANILIST_FULL_QUERY, {"id": anilist_id})
        return result.get("data", {}).get("Media", {})
    except Exception as e:
        print(f"    Error fetching AniList {anilist_id}: {e}")
        return {}


# =============================================================================
# STAGE 1: FETCH (same as v2)
# =============================================================================

def fetch_franchise_v3(
    slug: str,
    anilist_id: int,
    include_manga: bool = True,
    max_entries: int = 50,
) -> dict:
    """Fetch all data for a franchise, including related works."""
    franchise_dir = SOURCES_DIR / slug
    franchise_dir.mkdir(parents=True, exist_ok=True)
    
    results = {"anilist": [], "mal": [], "tvdb": [], "images": 0}
    manifest = ImageManifest(str(DATA_ROOT), slug)
    
    anilist_dir = franchise_dir / "anilist"
    anilist_dir.mkdir(exist_ok=True)
    mal_dir = franchise_dir / "mal"
    mal_dir.mkdir(exist_ok=True)
    tvdb_dir = franchise_dir / "tvdb"
    tvdb_dir.mkdir(exist_ok=True)
    
    fetched_anilist = set()
    fetched_mal = set()
    to_fetch_anilist = [anilist_id]
    
    follow_relations = {
        "SEQUEL", "PREQUEL", "PARENT", "SIDE_STORY", 
        "ALTERNATIVE", "ADAPTATION", "SOURCE"
    }
    
    print(f"\n=== Fetching franchise: {slug} ===")
    print(f"  Starting from AniList ID: {anilist_id}")
    
    # --- Fetch AniList entries ---
    while to_fetch_anilist and len(fetched_anilist) < max_entries:
        current_id = to_fetch_anilist.pop(0)
        if current_id in fetched_anilist:
            continue
        
        print(f"\n  Fetching AniList {current_id}...")
        data = fetch_anilist_full(current_id)
        if not data:
            continue
        
        with open(anilist_dir / f"{current_id}.json", "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        fetched_anilist.add(current_id)
        results["anilist"].append(current_id)
        
        title = data.get("title", {}).get("english") or data.get("title", {}).get("romaji") or "Unknown"
        media_type = data.get("type", "ANIME")
        fmt = data.get("format", "")
        print(f"    → {title} ({media_type}/{fmt})")
        
        extract_and_download_images(manifest, "anilist", data)
        
        for edge in data.get("relations", {}).get("edges", []):
            rel_type = edge.get("relationType")
            rel_node = edge.get("node", {})
            rel_id = rel_node.get("id")
            rel_media_type = rel_node.get("type")
            
            if not rel_id or rel_id in fetched_anilist:
                continue
            
            if rel_media_type == "MANGA" and not include_manga:
                continue
            
            if rel_type in follow_relations:
                to_fetch_anilist.append(rel_id)
                rel_title = rel_node.get("title", {}).get("english") or rel_node.get("title", {}).get("romaji")
                print(f"      Queued: {rel_title} ({rel_type})")
        
        rate_limit()
    
    # --- Fetch MAL ---
    print(f"\n  Fetching MAL data...")
    for al_id in results["anilist"]:
        al_file = anilist_dir / f"{al_id}.json"
        if not al_file.exists():
            continue
        
        with open(al_file) as f:
            al_data = json.load(f)
        
        mal_id = al_data.get("idMal")
        if not mal_id or mal_id in fetched_mal:
            continue
        
        print(f"    Fetching MAL {mal_id}...")
        try:
            mal_data = jikan_full(mal_id)
            if mal_data:
                with open(mal_dir / f"{mal_id}.json", "w") as f:
                    json.dump(mal_data, f, indent=2, ensure_ascii=False)
                fetched_mal.add(mal_id)
                results["mal"].append(mal_id)
                extract_and_download_images(manifest, "mal", mal_data)
                rate_limit()
        except Exception as e:
            print(f"      Error: {e}")
    
    # --- Fetch TVDB ---
    if TVDB_TOKEN:
        print(f"\n  Fetching TVDB data...")
        # Only fetch for root TV anime (earliest in franchise)
        tv_entries = []
        for al_id in results["anilist"]:
            al_file = anilist_dir / f"{al_id}.json"
            if not al_file.exists():
                continue
            with open(al_file) as f:
                al_data = json.load(f)
            if al_data.get("type") == "ANIME" and al_data.get("format") == "TV":
                year = al_data.get("startDate", {}).get("year") or 9999
                title = al_data.get("title", {}).get("romaji") or al_data.get("title", {}).get("english")
                tv_entries.append((year, title, al_data))
        
        # Sort by year, search TVDB for earliest
        tv_entries.sort(key=lambda x: x[0])
        searched_tvdb = set()
        
        for year, title, al_data in tv_entries[:3]:  # Only first 3 to avoid duplicates
            if title in searched_tvdb:
                continue
            searched_tvdb.add(title)
            
            print(f"    Searching TVDB for: {title}")
            try:
                tvdb_results = tvdb_search(title)
                if tvdb_results:
                    best_match = None
                    for r in tvdb_results:
                        if r.get("year") == year:
                            best_match = r
                            break
                    if not best_match and tvdb_results:
                        best_match = tvdb_results[0]
                    
                    if best_match:
                        tvdb_id = best_match.get("tvdb_id")
                        if tvdb_id and not (tvdb_dir / f"{tvdb_id}.json").exists():
                            print(f"      Found: {best_match.get('name')} (TVDB {tvdb_id})")
                            tvdb_data = tvdb_series_extended(tvdb_id)
                            if tvdb_data:
                                with open(tvdb_dir / f"{tvdb_id}.json", "w") as f:
                                    json.dump(tvdb_data, f, indent=2, ensure_ascii=False)
                                results["tvdb"].append(tvdb_id)
                                extract_and_download_images(manifest, "tvdb", tvdb_data)
                rate_limit()
            except Exception as e:
                print(f"      Error: {e}")
    
    manifest.save()
    results["images"] = sum(len(v) for k, v in manifest.data.items() if isinstance(v, dict))
    
    print(f"\n=== Fetch Summary ===")
    print(f"  AniList entries: {len(results['anilist'])}")
    print(f"  MAL entries: {len(results['mal'])}")
    print(f"  TVDB entries: {len(results['tvdb'])}")
    
    return results


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class Entry:
    """A single conceptual work (may consolidate multiple AniList entries)."""
    # Source IDs (can have multiple for consolidated entries)
    anilist_ids: List[str] = field(default_factory=list)
    mal_ids: List[str] = field(default_factory=list)
    tvdb_id: Optional[str] = None
    wikidata_id: Optional[str] = None
    
    title: str = ""
    title_native: Optional[str] = None
    alternate_titles: List[str] = field(default_factory=list)
    
    media_type: str = "anime"
    format_detail: Optional[str] = None
    
    status: str = "released"
    release_date: Optional[str] = None
    end_date: Optional[str] = None
    
    description: Optional[str] = None
    episode_count: Optional[int] = None
    chapter_count: Optional[int] = None
    volume_count: Optional[int] = None
    
    genres: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    
    source_type: Optional[str] = None
    external_links: Dict[str, str] = field(default_factory=dict)
    
    # For consolidated TV anime
    is_consolidated: bool = False
    consolidated_from: List[int] = field(default_factory=list)  # AniList IDs
    
    @property
    def slug(self) -> str:
        year = self.release_date[:4] if self.release_date else "unknown"
        return f"{slugify(self.title)}-{self.media_type}-{year}"


@dataclass
class Season:
    """A season within an entry (maps to AniList entry for TV anime)."""
    season_number: int
    title: Optional[str] = None
    episode_count: Optional[int] = None
    air_date_start: Optional[str] = None
    air_date_end: Optional[str] = None
    
    # Source mapping
    anilist_id: Optional[int] = None
    mal_id: Optional[int] = None
    tvdb_season_number: Optional[int] = None


@dataclass
class Creator:
    anilist_id: Optional[str] = None
    mal_id: Optional[str] = None
    name: str = ""
    native_name: Optional[str] = None
    birth_date: Optional[str] = None
    death_date: Optional[str] = None
    description: Optional[str] = None
    primary_occupations: List[str] = field(default_factory=list)
    
    @property
    def slug(self) -> str:
        return slugify(self.name)


@dataclass
class Character:
    anilist_id: Optional[str] = None
    mal_id: Optional[str] = None
    name: str = ""
    native_name: Optional[str] = None
    alternate_names: List[str] = field(default_factory=list)
    description: Optional[str] = None
    role: str = "supporting"
    
    @property
    def slug(self) -> str:
        return slugify(self.name)


@dataclass
class Company:
    anilist_id: Optional[str] = None
    name: str = ""
    role: str = "animation_studio"
    
    @property
    def slug(self) -> str:
        return slugify(self.name)


@dataclass
class Relationship:
    source_slug: str
    target_slug: str
    relationship_type: str


@dataclass
class VoiceActorRole:
    entry_slug: str
    creator_slug: str
    character_slug: str
    language: str


@dataclass
class ProcessedFranchise:
    slug: str
    name: str
    description: Optional[str] = None
    
    entries: Dict[str, Entry] = field(default_factory=dict)
    seasons: Dict[str, List[Season]] = field(default_factory=dict)
    creators: Dict[str, Creator] = field(default_factory=dict)
    characters: Dict[str, Character] = field(default_factory=dict)
    companies: Dict[str, Company] = field(default_factory=dict)
    
    relationships: List[Relationship] = field(default_factory=list)
    va_roles: List[VoiceActorRole] = field(default_factory=list)
    
    entry_creators: Dict[str, List[Tuple[str, str]]] = field(default_factory=dict)
    entry_companies: Dict[str, List[Tuple[str, str]]] = field(default_factory=dict)
    entry_characters: Dict[str, List[Tuple[str, str]]] = field(default_factory=dict)


# =============================================================================
# STAGE 2: PROCESS
# =============================================================================

def map_format_to_media_type(al_format: str, al_type: str) -> str:
    if al_type == "MANGA":
        return "manga"
    if al_type == "NOVEL":
        return "light_novel"
    
    format_map = {
        "TV": "anime",
        "TV_SHORT": "anime",
        "MOVIE": "movie",
        "OVA": "ova",
        "ONA": "ona",
        "SPECIAL": "special",
        "MUSIC": "music",
        "MANGA": "manga",
        "NOVEL": "light_novel",
        "ONE_SHOT": "manga",
    }
    return format_map.get(al_format, "anime")


def map_status(al_status: str) -> str:
    status_map = {
        "RELEASING": "releasing",
        "FINISHED": "released",
        "NOT_YET_RELEASED": "announced",
        "CANCELLED": "cancelled",
        "HIATUS": "hiatus",
    }
    return status_map.get(al_status, "released")


def map_relation_type(al_relation: str) -> str:
    rel_map = {
        "SEQUEL": "sequel",
        "PREQUEL": "prequel",
        "PARENT": "parent",
        "SIDE_STORY": "side_story",
        "ALTERNATIVE": "alternative",
        "ADAPTATION": "adaptation",
        "SOURCE": "source",
        "CHARACTER": "other",
        "SUMMARY": "summary",
        "COMPILATION": "compilation",
        "CONTAINS": "other",
        "OTHER": "other",
    }
    return rel_map.get(al_relation, "other")


def format_date(date_obj: dict) -> Optional[str]:
    if not date_obj or not date_obj.get("year"):
        return None
    y = date_obj["year"]
    m = date_obj.get("month", 1) or 1
    d = date_obj.get("day", 1) or 1
    return f"{y:04d}-{m:02d}-{d:02d}"


def clean_description(desc: str) -> Optional[str]:
    if not desc:
        return None
    desc = re.sub(r'<[^>]+>', '', desc)
    desc = re.sub(r'\(Source:.*?\)', '', desc)
    desc = re.sub(r'\[Written by.*?\]', '', desc)
    desc = re.sub(r'\s+', ' ', desc).strip()
    if len(desc) > 2000:
        desc = desc[:1997] + "..."
    return desc if desc else None


def find_tv_sequel_chains(anilist_data: Dict[int, dict]) -> Dict[int, List[int]]:
    """
    Find chains of TV anime sequels that should be consolidated.
    Returns: {root_id: [id1, id2, id3, ...]} where ids are in chronological order.
    """
    # Build graph of TV sequels
    tv_entries = {}
    sequel_graph = {}  # id -> sequel_id
    prequel_graph = {}  # id -> prequel_id
    
    for al_id, data in anilist_data.items():
        # Check format is TV (type may be None in older data)
        if data.get("format") != "TV":
            continue
        # Skip if explicitly not anime (but allow None for older data)
        if data.get("type") and data.get("type") != "ANIME":
            continue
        
        tv_entries[al_id] = data
        sequel_graph[al_id] = None
        prequel_graph[al_id] = None
        
        for edge in data.get("relations", {}).get("edges", []):
            rel_type = edge.get("relationType")
            rel_node = edge.get("node", {})
            rel_id = rel_node.get("id")
            rel_format = rel_node.get("format")
            
            # Only follow TV sequels/prequels
            if rel_format != "TV":
                continue
            
            if rel_type == "SEQUEL" and rel_id in anilist_data:
                sequel_graph[al_id] = rel_id
            elif rel_type == "PREQUEL" and rel_id in anilist_data:
                prequel_graph[al_id] = rel_id
    
    # Find roots (entries with no prequel)
    roots = [al_id for al_id in tv_entries if prequel_graph.get(al_id) is None]
    
    # Build chains from each root
    chains = {}
    for root_id in roots:
        chain = [root_id]
        current = root_id
        while sequel_graph.get(current):
            next_id = sequel_graph[current]
            if next_id in chain:  # Prevent cycles
                break
            chain.append(next_id)
            current = next_id
        chains[root_id] = chain
    
    return chains


def process_franchise_v3(slug: str) -> ProcessedFranchise:
    """Process all source data, consolidating TV anime seasons."""
    franchise_dir = SOURCES_DIR / slug
    if not franchise_dir.exists():
        raise FileNotFoundError(f"No sources for {slug}")
    
    result = ProcessedFranchise(slug=slug, name=slug.replace("-", " ").title())
    
    # Load data
    anilist_dir = franchise_dir / "anilist"
    anilist_data = {}
    if anilist_dir.exists():
        for f in anilist_dir.glob("*.json"):
            with open(f) as fh:
                data = json.load(fh)
                anilist_data[data.get("id")] = data
    
    mal_dir = franchise_dir / "mal"
    mal_data = {}
    if mal_dir.exists():
        for f in mal_dir.glob("*.json"):
            with open(f) as fh:
                data = json.load(fh)
                mal_id = data.get("anime", {}).get("mal_id") or data.get("mal_id")
                if mal_id:
                    mal_data[mal_id] = data
    
    tvdb_dir = franchise_dir / "tvdb"
    tvdb_data = {}
    if tvdb_dir.exists():
        for f in tvdb_dir.glob("*.json"):
            with open(f) as fh:
                data = json.load(fh)
                slimmed = slim_tvdb(data)
                tvdb_data[data.get("id")] = slimmed
    
    print(f"\n=== Processing {slug} ===")
    print(f"  AniList entries: {len(anilist_data)}")
    print(f"  MAL entries: {len(mal_data)}")
    print(f"  TVDB entries: {len(tvdb_data)}")
    
    # Find TV sequel chains to consolidate
    tv_chains = find_tv_sequel_chains(anilist_data)
    consolidated_ids = set()
    for root_id, chain in tv_chains.items():
        if len(chain) > 1:
            print(f"\n  TV Chain found: {[anilist_data[i].get('title',{}).get('english') or anilist_data[i].get('title',{}).get('romaji') for i in chain]}")
            consolidated_ids.update(chain)
    
    # Track for deduplication
    creator_by_name = {}
    character_by_name = {}
    
    # --- Process consolidated TV anime first ---
    for root_id, chain in tv_chains.items():
        if len(chain) <= 1:
            continue  # Single entry, process normally
        
        # Use first entry's data as base
        root_data = anilist_data[root_id]
        title = root_data.get("title", {}).get("english") or root_data.get("title", {}).get("romaji")
        
        # Calculate total episodes
        total_episodes = sum(
            anilist_data[al_id].get("episodes") or 0 
            for al_id in chain
        )
        
        # Create consolidated entry
        entry = Entry(
            anilist_ids=[str(al_id) for al_id in chain],
            mal_ids=[str(anilist_data[al_id].get("idMal")) for al_id in chain if anilist_data[al_id].get("idMal")],
            title=title,
            title_native=root_data.get("title", {}).get("native"),
            media_type="anime",
            format_detail="TV",
            status=map_status(root_data.get("status")),
            release_date=format_date(root_data.get("startDate")),
            end_date=format_date(anilist_data[chain[-1]].get("endDate")),  # End date from last season
            description=clean_description(root_data.get("description")),
            episode_count=total_episodes if total_episodes > 0 else None,
            genres=[g.lower().replace(" ", "_").replace("-", "_") for g in (root_data.get("genres") or [])],
            tags=[t.get("name", "").lower().replace(" ", "_").replace("-", "_") 
                  for t in (root_data.get("tags") or []) 
                  if t.get("rank", 0) >= 70 and not t.get("isMediaSpoiler")],
            source_type=root_data.get("source"),
            is_consolidated=True,
            consolidated_from=chain,
        )
        
        # Merge external links from all seasons
        for al_id in chain:
            al = anilist_data[al_id]
            for link in al.get("externalLinks") or []:
                site = link.get("site", "").lower().replace(" ", "_")
                if site and link.get("url"):
                    entry.external_links[site] = link["url"]
            if al.get("siteUrl"):
                entry.external_links["anilist"] = al["siteUrl"]
        
        result.entries[entry.slug] = entry
        result.entry_creators[entry.slug] = []
        result.entry_companies[entry.slug] = []
        result.entry_characters[entry.slug] = []
        
        print(f"\n  Consolidated Entry: {title} ({len(chain)} AniList entries → 1 entry)")
        
        # Create seasons from each AniList entry
        result.seasons[entry.slug] = []
        for i, al_id in enumerate(chain):
            al = anilist_data[al_id]
            season_title = al.get("title", {}).get("english") or al.get("title", {}).get("romaji")
            
            season = Season(
                season_number=i + 1,
                title=season_title if season_title != title else None,  # Only set if different
                episode_count=al.get("episodes"),
                air_date_start=format_date(al.get("startDate")),
                air_date_end=format_date(al.get("endDate")),
                anilist_id=al_id,
                mal_id=al.get("idMal"),
            )
            result.seasons[entry.slug].append(season)
            print(f"    Season {i+1}: {season_title} ({season.episode_count} eps)")
        
        # Merge studios from all seasons
        seen_studios = set()
        for al_id in chain:
            al = anilist_data[al_id]
            for studio in al.get("studios", {}).get("nodes") or []:
                name = studio.get("name")
                if not name or name in seen_studios:
                    continue
                seen_studios.add(name)
                
                company_slug = slugify(name)
                if company_slug not in result.companies:
                    result.companies[company_slug] = Company(
                        anilist_id=str(studio.get("id")),
                        name=name,
                        role="animation_studio" if studio.get("isAnimationStudio") else "producer"
                    )
                
                role = "animation_studio" if studio.get("isAnimationStudio") else "producer"
                if (company_slug, role) not in result.entry_companies[entry.slug]:
                    result.entry_companies[entry.slug].append((company_slug, role))
        
        # Merge staff/characters from all seasons
        _process_staff_and_characters(
            result, entry, chain, anilist_data, mal_data,
            creator_by_name, character_by_name
        )
    
    # --- Process non-consolidated entries ---
    for al_id, al in anilist_data.items():
        if al_id in consolidated_ids:
            continue  # Already processed as part of a chain
        
        title = al.get("title", {}).get("english") or al.get("title", {}).get("romaji") or f"Unknown-{al_id}"
        
        entry = Entry(
            anilist_ids=[str(al_id)],
            mal_ids=[str(al.get("idMal"))] if al.get("idMal") else [],
            title=title,
            title_native=al.get("title", {}).get("native"),
            alternate_titles=[al.get("title", {}).get("romaji")] if al.get("title", {}).get("romaji") and al.get("title", {}).get("romaji") != title else [],
            media_type=map_format_to_media_type(al.get("format"), al.get("type")),
            format_detail=al.get("format"),
            status=map_status(al.get("status")),
            release_date=format_date(al.get("startDate")),
            end_date=format_date(al.get("endDate")),
            description=clean_description(al.get("description")),
            episode_count=al.get("episodes"),
            chapter_count=al.get("chapters"),
            volume_count=al.get("volumes"),
            genres=[g.lower().replace(" ", "_").replace("-", "_") for g in (al.get("genres") or [])],
            tags=[t.get("name", "").lower().replace(" ", "_").replace("-", "_") 
                  for t in (al.get("tags") or []) 
                  if t.get("rank", 0) >= 70 and not t.get("isMediaSpoiler")],
            source_type=al.get("source"),
        )
        
        for link in al.get("externalLinks") or []:
            site = link.get("site", "").lower().replace(" ", "_")
            if site and link.get("url"):
                entry.external_links[site] = link["url"]
        if al.get("siteUrl"):
            entry.external_links["anilist"] = al["siteUrl"]
        
        result.entries[entry.slug] = entry
        result.entry_creators[entry.slug] = []
        result.entry_companies[entry.slug] = []
        result.entry_characters[entry.slug] = []
        
        print(f"\n  Entry: {title} ({entry.media_type})")
        
        # Studios
        for studio in al.get("studios", {}).get("nodes") or []:
            name = studio.get("name")
            if not name:
                continue
            
            company_slug = slugify(name)
            if company_slug not in result.companies:
                result.companies[company_slug] = Company(
                    anilist_id=str(studio.get("id")),
                    name=name,
                    role="animation_studio" if studio.get("isAnimationStudio") else "producer"
                )
            
            role = "animation_studio" if studio.get("isAnimationStudio") else "producer"
            result.entry_companies[entry.slug].append((company_slug, role))
        
        # Staff & characters for single entry
        _process_staff_and_characters(
            result, entry, [al_id], anilist_data, mal_data,
            creator_by_name, character_by_name
        )
    
    # --- Build relationships ---
    _build_relationships(result, anilist_data, consolidated_ids, tv_chains)
    
    # --- Match TVDB seasons ---
    _match_tvdb_seasons(result, tvdb_data)
    
    # Set franchise name
    if result.entries:
        main_entries = [e for e in result.entries.values() if e.format_detail in ("TV", "MANGA", "NOVEL")]
        if main_entries:
            main_entries.sort(key=lambda e: e.release_date or "9999")
            result.name = main_entries[0].title
            result.description = main_entries[0].description
    
    # Save
    processed_path = franchise_dir / "processed_v3.json"
    with open(processed_path, "w") as f:
        output = {
            "slug": result.slug,
            "name": result.name,
            "description": result.description,
            "entries": {k: vars(v) for k, v in result.entries.items()},
            "seasons": {k: [vars(s) for s in v] for k, v in result.seasons.items()},
            "creators": {k: vars(v) for k, v in result.creators.items()},
            "characters": {k: vars(v) for k, v in result.characters.items()},
            "companies": {k: vars(v) for k, v in result.companies.items()},
            "relationships": [vars(r) for r in result.relationships],
            "va_roles": [vars(v) for v in result.va_roles],
            "entry_creators": result.entry_creators,
            "entry_companies": result.entry_companies,
            "entry_characters": result.entry_characters,
        }
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n=== Process Summary ===")
    print(f"  Entries: {len(result.entries)}")
    print(f"  Seasons: {sum(len(s) for s in result.seasons.values())}")
    print(f"  Creators: {len(result.creators)}")
    print(f"  Characters: {len(result.characters)}")
    print(f"  Companies: {len(result.companies)}")
    print(f"  Relationships: {len(result.relationships)}")
    print(f"  VA roles: {len(result.va_roles)}")
    
    return result


def _process_staff_and_characters(
    result: ProcessedFranchise,
    entry: Entry,
    al_ids: List[int],
    anilist_data: Dict[int, dict],
    mal_data: Dict[int, dict],
    creator_by_name: Dict[str, str],
    character_by_name: Dict[str, str],
):
    """Process staff and characters from AniList entries, merging across seasons."""
    
    seen_staff = set()
    seen_characters = set()
    
    for al_id in al_ids:
        al = anilist_data[al_id]
        
        # Staff
        for edge in al.get("staff", {}).get("edges") or []:
            node = edge.get("node", {})
            name = node.get("name", {}).get("full")
            if not name:
                continue
            
            norm_name = normalize_name(name)
            
            if norm_name not in creator_by_name:
                creator = Creator(
                    anilist_id=str(node.get("id")),
                    name=name,
                    native_name=node.get("name", {}).get("native"),
                    description=clean_description(node.get("description")),
                    birth_date=format_date(node.get("dateOfBirth")),
                    death_date=format_date(node.get("dateOfDeath")),
                    primary_occupations=node.get("primaryOccupations") or [],
                )
                creator_by_name[norm_name] = creator.slug
                result.creators[creator.slug] = creator
            
            creator_slug = creator_by_name[norm_name]
            role = edge.get("role", "").lower()
            
            role_mapping = {
                "director": "director",
                "chief director": "chief_director",
                "original creator": "original_creator",
                "original story": "original_story",
                "series composition": "series_composition",
                "screenplay": "screenplay",
                "script": "script",
                "storyboard": "storyboard",
                "character design": "character_design",
                "chief animation director": "chief_animation_director",
                "animation director": "animation_director",
                "key animation": "key_animation",
                "art director": "art_director",
                "music": "music",
                "sound director": "sound_director",
                "producer": "producer",
            }
            
            db_role = None
            for key, val in role_mapping.items():
                if key in role:
                    db_role = val
                    break
            
            if db_role and (creator_slug, db_role) not in seen_staff:
                seen_staff.add((creator_slug, db_role))
                result.entry_creators[entry.slug].append((creator_slug, db_role))
        
        # Characters
        for edge in al.get("characters", {}).get("edges") or []:
            node = edge.get("node", {})
            name = node.get("name", {}).get("full")
            if not name:
                continue
            
            char_role = "main" if edge.get("role") == "MAIN" else "supporting"
            norm_name = normalize_name(name)
            
            if norm_name not in character_by_name:
                character = Character(
                    anilist_id=str(node.get("id")),
                    name=name,
                    native_name=node.get("name", {}).get("native"),
                    alternate_names=node.get("name", {}).get("alternative") or [],
                    description=clean_description(node.get("description")),
                    role=char_role,
                )
                character_by_name[norm_name] = character.slug
                result.characters[character.slug] = character
            
            char_slug = character_by_name[norm_name]
            
            if (char_slug, char_role) not in seen_characters:
                seen_characters.add((char_slug, char_role))
                result.entry_characters[entry.slug].append((char_slug, char_role))
            
            # Voice actors
            for va in edge.get("voiceActors") or []:
                va_name = va.get("name", {}).get("full")
                if not va_name:
                    continue
                
                va_norm = normalize_name(va_name)
                if va_norm not in creator_by_name:
                    va_creator = Creator(
                        anilist_id=str(va.get("id")),
                        name=va_name,
                        native_name=va.get("name", {}).get("native"),
                    )
                    creator_by_name[va_norm] = va_creator.slug
                    result.creators[va_creator.slug] = va_creator
                
                va_slug = creator_by_name[va_norm]
                lang = "ja"
                if va.get("languageV2"):
                    lang_map = {"Japanese": "ja", "English": "en", "Korean": "ko"}
                    lang = lang_map.get(va["languageV2"], "ja")
                
                # Avoid duplicate VA roles
                va_role_key = (entry.slug, va_slug, char_slug, lang)
                existing = [r for r in result.va_roles 
                           if r.entry_slug == entry.slug and r.creator_slug == va_slug 
                           and r.character_slug == char_slug and r.language == lang]
                if not existing:
                    result.va_roles.append(VoiceActorRole(
                        entry_slug=entry.slug,
                        creator_slug=va_slug,
                        character_slug=char_slug,
                        language=lang,
                    ))
    
    # Add English VAs from MAL
    for al_id in al_ids:
        al = anilist_data[al_id]
        mal_id = al.get("idMal")
        if not mal_id or mal_id not in mal_data:
            continue
        
        ml = mal_data[mal_id]
        for char in ml.get("characters", []):
            character = char.get("character", {})
            char_name = character.get("name", "")
            char_norm = normalize_name(char_name)
            
            if char_norm not in character_by_name:
                continue
            
            char_slug = character_by_name[char_norm]
            
            # Update character with MAL ID
            if char_slug in result.characters:
                result.characters[char_slug].mal_id = str(character.get("mal_id"))
            
            for va in char.get("voice_actors", []):
                if va.get("language") != "English":
                    continue
                
                person = va.get("person", {})
                va_name = person.get("name", "")
                va_norm = normalize_name(va_name)
                
                if va_norm not in creator_by_name:
                    va_creator = Creator(
                        mal_id=str(person.get("mal_id")),
                        name=normalize_name(va_name).title(),
                    )
                    creator_by_name[va_norm] = va_creator.slug
                    result.creators[va_creator.slug] = va_creator
                else:
                    va_slug = creator_by_name[va_norm]
                    if va_slug in result.creators:
                        result.creators[va_slug].mal_id = str(person.get("mal_id"))
                
                va_slug = creator_by_name[va_norm]
                
                existing = [r for r in result.va_roles 
                           if r.entry_slug == entry.slug and r.creator_slug == va_slug 
                           and r.character_slug == char_slug and r.language == "en"]
                if not existing:
                    result.va_roles.append(VoiceActorRole(
                        entry_slug=entry.slug,
                        creator_slug=va_slug,
                        character_slug=char_slug,
                        language="en",
                    ))


def _build_relationships(
    result: ProcessedFranchise,
    anilist_data: Dict[int, dict],
    consolidated_ids: Set[int],
    tv_chains: Dict[int, List[int]],
):
    """Build relationships between entries."""
    
    # Map AniList ID -> entry slug
    al_to_slug = {}
    for entry in result.entries.values():
        for al_id in entry.anilist_ids:
            al_to_slug[int(al_id)] = entry.slug
    
    seen_rels = set()
    
    for al_id, al in anilist_data.items():
        source_slug = al_to_slug.get(al_id)
        if not source_slug:
            continue
        
        for edge in al.get("relations", {}).get("edges", []):
            rel_type = edge.get("relationType")
            rel_node = edge.get("node", {})
            rel_id = rel_node.get("id")
            
            if not rel_id or rel_id not in anilist_data:
                continue
            
            target_slug = al_to_slug.get(rel_id)
            if not target_slug or source_slug == target_slug:
                continue  # Same entry (e.g., seasons of same show)
            
            db_rel_type = map_relation_type(rel_type)
            if db_rel_type == "other":
                continue
            
            # Skip sequel/prequel between consolidated seasons
            if db_rel_type in ("sequel", "prequel"):
                if al_id in consolidated_ids and rel_id in consolidated_ids:
                    continue
            
            rel_key = (source_slug, target_slug, db_rel_type)
            if rel_key not in seen_rels:
                seen_rels.add(rel_key)
                result.relationships.append(Relationship(
                    source_slug=source_slug,
                    target_slug=target_slug,
                    relationship_type=db_rel_type,
                ))


def _match_tvdb_seasons(result: ProcessedFranchise, tvdb_data: Dict[int, dict]):
    """Match TVDB season data to our entry seasons."""
    
    for tvdb_id, tvdb in tvdb_data.items():
        tvdb_year = tvdb.get("year")
        if tvdb_year:
            tvdb_year = int(tvdb_year)
        
        # Find matching entry by year
        matching_entry = None
        for entry in result.entries.values():
            if entry.media_type == "anime" and entry.release_date:
                entry_year = int(entry.release_date[:4])
                if entry_year == tvdb_year:
                    matching_entry = entry
                    entry.tvdb_id = str(tvdb_id)
                    break
        
        if not matching_entry or matching_entry.slug not in result.seasons:
            continue
        
        # Try to match TVDB seasons to our seasons by year
        tvdb_seasons = {s.get("seasonNumber"): s for s in tvdb.get("seasons", []) if s.get("seasonNumber", 0) > 0}
        
        for season in result.seasons[matching_entry.slug]:
            if season.air_date_start:
                season_year = int(season.air_date_start[:4])
                
                # Find TVDB season with matching year
                for sn, tvdb_s in tvdb_seasons.items():
                    tvdb_first = tvdb_s.get("firstAired")
                    if tvdb_first and int(tvdb_first[:4]) == season_year:
                        season.tvdb_season_number = sn
                        # Only use TVDB episode count if we don't have one from AniList
                        if not season.episode_count and tvdb_s.get("episodeCount"):
                            season.episode_count = tvdb_s["episodeCount"]
                        break


# =============================================================================
# STAGE 3: GENERATE SQL
# =============================================================================

def generate_sql_v3(slug: str) -> str:
    """Generate SQL from processed v3 data."""
    franchise_dir = SOURCES_DIR / slug
    processed_path = franchise_dir / "processed_v3.json"
    
    if not processed_path.exists():
        raise FileNotFoundError(f"No processed_v3.json for {slug}")
    
    with open(processed_path) as f:
        data = json.load(f)
    
    lines = []
    lines.append(f"-- ============================================================================")
    lines.append(f"-- SQL for franchise: {slug}")
    lines.append(f"-- Generated by pipeline_v3.py")
    lines.append(f"-- Entries: {len(data['entries'])}")
    lines.append(f"-- ============================================================================")
    lines.append("")
    lines.append("BEGIN;")
    lines.append("")
    
    # Franchise
    desc = f"'{escape_sql(data['description'][:500])}'" if data.get('description') else "NULL"
    lines.append(f"""
INSERT INTO franchises (id, name, slug, description)
VALUES (gen_random_uuid(), '{escape_sql(data['name'])}', '{data['slug']}', {desc})
ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name;
""")
    
    # Companies
    if data.get("companies"):
        lines.append("-- Companies")
        for slug_key, company in data["companies"].items():
            websites = {}
            if company.get("anilist_id"):
                websites["anilist_id"] = company["anilist_id"]
            
            lines.append(f"""
INSERT INTO companies (id, name, slug, websites)
VALUES (gen_random_uuid(), '{escape_sql(company['name'])}', '{slug_key}', '{escape_json_for_sql(websites)}'::jsonb)
ON CONFLICT (slug) DO UPDATE SET websites = companies.websites || EXCLUDED.websites;
""")
    
    # Creators
    if data.get("creators"):
        lines.append("-- Creators")
        for slug_key, creator in data["creators"].items():
            details = {}
            if creator.get("anilist_id"):
                details["anilist_id"] = creator["anilist_id"]
            if creator.get("mal_id"):
                details["mal_id"] = creator["mal_id"]
            if creator.get("primary_occupations"):
                details["occupations"] = creator["primary_occupations"]
            
            native = f"'{escape_sql(creator['native_name'])}'" if creator.get("native_name") else "NULL"
            bio = f"'{escape_sql(creator['description'][:1000])}'" if creator.get("description") else "NULL"
            birth = f"'{creator['birth_date']}'" if creator.get("birth_date") else "NULL"
            death = f"'{creator['death_date']}'" if creator.get("death_date") else "NULL"
            
            lines.append(f"""
INSERT INTO creators (id, full_name, native_name, slug, biography, birth_date, death_date, details)
VALUES (gen_random_uuid(), '{escape_sql(creator['name'])}', {native}, '{slug_key}', {bio}, {birth}, {death}, '{escape_json_for_sql(details)}'::jsonb)
ON CONFLICT (slug) DO UPDATE SET details = creators.details || EXCLUDED.details;
""")
    
    # Entries
    for entry_slug, entry in data.get("entries", {}).items():
        lines.append(f"-- Entry: {entry['title']}")
        
        details = {
            "anilist_ids": entry.get("anilist_ids", []),
            "format": entry.get("format_detail"),
        }
        if entry.get("mal_ids"):
            details["mal_ids"] = entry["mal_ids"]
        if entry.get("tvdb_id"):
            details["tvdb_id"] = entry["tvdb_id"]
        if entry.get("episode_count"):
            details["episodes"] = entry["episode_count"]
        if entry.get("chapter_count"):
            details["chapters"] = entry["chapter_count"]
        if entry.get("volume_count"):
            details["volumes"] = entry["volume_count"]
        if entry.get("source_type"):
            details["source"] = entry["source_type"]
        if entry.get("external_links"):
            details["external_links"] = entry["external_links"]
        if entry.get("is_consolidated"):
            details["consolidated"] = True
        
        alt_titles = entry.get("alternate_titles", [])
        if entry.get("title_native"):
            alt_titles.append(entry["title_native"])
        alt_sql = "ARRAY[" + ", ".join(f"'{escape_sql(t)}'" for t in alt_titles) + "]" if alt_titles else "NULL"
        
        release = f"'{entry['release_date']}'" if entry.get("release_date") else "NULL"
        desc = f"'{escape_sql(entry['description'])}'" if entry.get("description") else "NULL"
        wikidata = f"'{entry['wikidata_id']}'" if entry.get("wikidata_id") else "NULL"
        
        lines.append(f"""
INSERT INTO entries (id, media_type_id, title, alternate_titles, slug, release_date, status, description, locale_code, wikidata_id, details)
SELECT gen_random_uuid(), mt.id, '{escape_sql(entry['title'])}', {alt_sql}, '{entry_slug}',
       {release}, '{entry.get('status', 'released')}', {desc}, 'ja', {wikidata}, '{escape_json_for_sql(details)}'::jsonb
FROM media_types mt WHERE mt.name = '{entry.get('media_type', 'anime')}'
ON CONFLICT (slug) DO UPDATE SET description = EXCLUDED.description, details = entries.details || EXCLUDED.details;
""")
        
        # Seasons
        for season in data.get("seasons", {}).get(entry_slug, []):
            title = f"'{escape_sql(season['title'])}'" if season.get("title") else "NULL"
            air_start = f"'{season['air_date_start']}'" if season.get("air_date_start") else "NULL"
            air_end = f"'{season['air_date_end']}'" if season.get("air_date_end") else "NULL"
            ep_count = season.get("episode_count") or "NULL"
            
            lines.append(f"""
INSERT INTO entry_seasons (id, entry_id, season_number, title, episode_count, air_date_start, air_date_end)
SELECT gen_random_uuid(), e.id, {season['season_number']}, {title}, {ep_count}, {air_start}, {air_end}
FROM entries e WHERE e.slug = '{entry_slug}'
ON CONFLICT (entry_id, season_number) DO NOTHING;
""")
        
        # Franchise link
        lines.append(f"""
INSERT INTO entry_franchises (id, entry_id, franchise_id)
SELECT gen_random_uuid(), e.id, f.id
FROM entries e, franchises f
WHERE e.slug = '{entry_slug}' AND f.slug = '{data['slug']}'
ON CONFLICT (entry_id, franchise_id) DO NOTHING;
""")
        
        # Company links
        for company_slug, role in data.get("entry_companies", {}).get(entry_slug, []):
            lines.append(f"""
INSERT INTO entry_companies (id, entry_id, company_id, role_id)
SELECT gen_random_uuid(), e.id, c.id, cr.id
FROM entries e, companies c, company_roles cr
WHERE e.slug = '{entry_slug}' AND c.slug = '{company_slug}' AND cr.name = '{role}'
ON CONFLICT (entry_id, company_id, role_id) DO NOTHING;
""")
        
        # Genre links
        for genre in entry.get("genres", []):
            lines.append(f"""
INSERT INTO entry_genres (id, entry_id, genre_id)
SELECT gen_random_uuid(), e.id, g.id
FROM entries e, genres g
WHERE e.slug = '{entry_slug}' AND g.name = '{genre}'
ON CONFLICT (entry_id, genre_id) DO NOTHING;
""")
        
        # Tag links
        for tag in entry.get("tags", []):
            lines.append(f"""
INSERT INTO entry_tags (id, entry_id, tag_id)
SELECT gen_random_uuid(), e.id, t.id
FROM entries e, tags t
WHERE e.slug = '{entry_slug}' AND t.name = '{tag}'
ON CONFLICT (entry_id, tag_id) DO NOTHING;
""")
        
        # Creator links
        for creator_slug, role in data.get("entry_creators", {}).get(entry_slug, []):
            lines.append(f"""
INSERT INTO entry_creators (id, entry_id, creator_id, role_id)
SELECT gen_random_uuid(), e.id, c.id, r.id
FROM entries e, creators c, creator_roles r
WHERE e.slug = '{entry_slug}' AND c.slug = '{creator_slug}' AND r.name = '{role}'
ON CONFLICT DO NOTHING;
""")
    
    # Characters
    if data.get("characters"):
        lines.append("-- Characters")
        for char_slug, char in data["characters"].items():
            details = {}
            if char.get("anilist_id"):
                details["anilist_id"] = char["anilist_id"]
            if char.get("mal_id"):
                details["mal_id"] = char["mal_id"]
            
            native = f"'{escape_sql(char['native_name'])}'" if char.get("native_name") else "NULL"
            desc = f"'{escape_sql(char['description'][:1000])}'" if char.get("description") else "NULL"
            alt_names = char.get("alternate_names", [])
            alt_sql = "ARRAY[" + ", ".join(f"'{escape_sql(n)}'" for n in alt_names) + "]" if alt_names else "NULL"
            char_full_slug = f"{char_slug}-{data['slug']}"
            
            lines.append(f"""
INSERT INTO characters (id, name, native_name, slug, description, alternate_names, franchise_id, details)
SELECT gen_random_uuid(), '{escape_sql(char['name'])}', {native}, '{char_full_slug}', {desc}, {alt_sql}, f.id, '{escape_json_for_sql(details)}'::jsonb
FROM franchises f WHERE f.slug = '{data['slug']}'
ON CONFLICT (slug) DO UPDATE SET details = characters.details || EXCLUDED.details;
""")
    
    # Entry-Character links
    for entry_slug, chars in data.get("entry_characters", {}).items():
        for char_slug, role in chars:
            char_full_slug = f"{char_slug}-{data['slug']}"
            lines.append(f"""
INSERT INTO entry_characters (id, entry_id, character_id, role)
SELECT gen_random_uuid(), e.id, c.id, '{role}'
FROM entries e, characters c
WHERE e.slug = '{entry_slug}' AND c.slug = '{char_full_slug}'
ON CONFLICT (entry_id, character_id) DO NOTHING;
""")
    
    # VA roles
    lines.append("-- Voice Actor roles")
    for va in data.get("va_roles", []):
        char_full_slug = f"{va['character_slug']}-{data['slug']}"
        lines.append(f"""
INSERT INTO entry_creators (id, entry_id, creator_id, role_id, character_id, language)
SELECT gen_random_uuid(), e.id, cr.id, r.id, ch.id, '{va['language']}'
FROM entries e, creators cr, creator_roles r, characters ch
WHERE e.slug = '{va['entry_slug']}' AND cr.slug = '{va['creator_slug']}' AND r.name = 'voice_actor' AND ch.slug = '{char_full_slug}'
ON CONFLICT DO NOTHING;
""")
    
    # Relationships
    if data.get("relationships"):
        lines.append("-- Entry relationships")
        for rel in data["relationships"]:
            lines.append(f"""
INSERT INTO entry_relationships (id, source_entry_id, target_entry_id, relationship_type_id)
SELECT gen_random_uuid(), s.id, t.id, rt.id
FROM entries s, entries t, relationship_types rt
WHERE s.slug = '{rel['source_slug']}' AND t.slug = '{rel['target_slug']}' AND rt.name = '{rel['relationship_type']}'
ON CONFLICT (source_entry_id, target_entry_id, relationship_type_id) DO NOTHING;
""")
    
    lines.append("")
    lines.append("COMMIT;")
    lines.append("")
    lines.append(f"SELECT 'Loaded franchise: {escape_sql(data['name'])}' as status;")
    lines.append(f"SELECT COUNT(*) as entries FROM entries e JOIN entry_franchises ef ON ef.entry_id = e.id JOIN franchises f ON ef.franchise_id = f.id WHERE f.slug = '{data['slug']}';")
    
    sql = "\n".join(lines)
    
    sql_path = franchise_dir / "insert_v3.sql"
    with open(sql_path, "w") as f:
        f.write(sql)
    
    print(f"\n=== Generated SQL ===")
    print(f"  Saved to: {sql_path}")
    print(f"  Size: {len(sql):,} bytes")
    
    return sql


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="The Watchlist Pipeline v3")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    fetch_p = subparsers.add_parser("fetch", help="Fetch from APIs")
    fetch_p.add_argument("slug", help="Franchise slug")
    fetch_p.add_argument("--anilist-id", type=int, required=True)
    fetch_p.add_argument("--include-manga", action="store_true", default=True)
    fetch_p.add_argument("--max-entries", type=int, default=50)
    
    proc_p = subparsers.add_parser("process", help="Process raw data")
    proc_p.add_argument("slug", help="Franchise slug")
    
    gen_p = subparsers.add_parser("generate", help="Generate SQL")
    gen_p.add_argument("slug", help="Franchise slug")
    
    all_p = subparsers.add_parser("all", help="Run full pipeline")
    all_p.add_argument("slug", help="Franchise slug")
    all_p.add_argument("--anilist-id", type=int, required=True)
    all_p.add_argument("--include-manga", action="store_true", default=True)
    all_p.add_argument("--max-entries", type=int, default=50)
    
    args = parser.parse_args()
    
    if args.command == "fetch":
        fetch_franchise_v3(args.slug, args.anilist_id, args.include_manga, args.max_entries)
    elif args.command == "process":
        process_franchise_v3(args.slug)
    elif args.command == "generate":
        generate_sql_v3(args.slug)
    elif args.command == "all":
        fetch_franchise_v3(args.slug, args.anilist_id, args.include_manga, args.max_entries)
        process_franchise_v3(args.slug)
        generate_sql_v3(args.slug)


if __name__ == "__main__":
    main()
