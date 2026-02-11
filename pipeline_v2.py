#!/usr/bin/env python3
"""
Production Pipeline v2 for The Watchlist Database
==================================================

Key changes from v1:
- Creates SEPARATE entries per work (not consolidated)
- Builds relationship graph (sequel/prequel/adaptation/source)
- Proper VA → character → language mapping
- Fetches manga/light novel sources
- Fills all columns (wikidata, descriptions, etc.)

Usage:
    python3 pipeline_v2.py fetch <franchise-slug> --anilist-id ID [--include-manga]
    python3 pipeline_v2.py process <franchise-slug>
    python3 pipeline_v2.py generate <franchise-slug>
    python3 pipeline_v2.py all <franchise-slug> --anilist-id ID [--include-manga]

Author: Omla 🦞
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from pathlib import Path
from collections import defaultdict

# Add parent for imports
sys.path.insert(0, os.path.dirname(__file__))

from fetch_sources import (
    anilist_full,
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
    # Replace single quotes with double single-quotes for SQL
    return json_str.replace("'", "''")


# =============================================================================
# ANILIST ENHANCED QUERIES
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


def fetch_wikidata_id(title: str, media_type: str = "anime") -> Optional[str]:
    """Try to find Wikidata ID for a title via Wikipedia API."""
    try:
        # Search Wikipedia
        search_url = f"https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch={urllib.parse.quote(title + ' ' + media_type)}&format=json"
        req = urllib.request.Request(search_url, headers={"User-Agent": "TheWatchlist/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            results = data.get("query", {}).get("search", [])
            if not results:
                return None
            
            # Get Wikidata ID from first result
            page_title = results[0].get("title", "")
            wd_url = f"https://en.wikipedia.org/w/api.php?action=query&titles={urllib.parse.quote(page_title)}&prop=pageprops&format=json"
            req2 = urllib.request.Request(wd_url, headers={"User-Agent": "TheWatchlist/1.0"})
            with urllib.request.urlopen(req2, timeout=10) as resp2:
                data2 = json.loads(resp2.read().decode())
                pages = data2.get("query", {}).get("pages", {})
                for page in pages.values():
                    wikibase = page.get("pageprops", {}).get("wikibase_item")
                    if wikibase:
                        return wikibase
    except Exception as e:
        pass
    return None


# =============================================================================
# STAGE 1: FETCH
# =============================================================================

def fetch_franchise_v2(
    slug: str,
    anilist_id: int,
    include_manga: bool = True,
    max_entries: int = 50,
) -> dict:
    """
    Fetch all data for a franchise, including related works.
    Creates separate entries for each work.
    """
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
    
    # Track what we've fetched
    fetched_anilist = set()
    fetched_mal = set()
    to_fetch_anilist = [anilist_id]
    
    # Relation types to follow
    follow_relations = {
        "SEQUEL", "PREQUEL", "PARENT", "SIDE_STORY", 
        "ALTERNATIVE", "ADAPTATION", "SOURCE"
    }
    if include_manga:
        follow_relations.add("ADAPTATION")
        follow_relations.add("SOURCE")
    
    print(f"\n=== Fetching franchise: {slug} ===")
    print(f"  Starting from AniList ID: {anilist_id}")
    print(f"  Include manga: {include_manga}")
    
    # --- Fetch AniList entries ---
    while to_fetch_anilist and len(fetched_anilist) < max_entries:
        current_id = to_fetch_anilist.pop(0)
        if current_id in fetched_anilist:
            continue
        
        print(f"\n  Fetching AniList {current_id}...")
        data = fetch_anilist_full(current_id)
        if not data:
            continue
        
        # Save raw
        with open(anilist_dir / f"{current_id}.json", "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        fetched_anilist.add(current_id)
        results["anilist"].append(current_id)
        
        title = data.get("title", {}).get("english") or data.get("title", {}).get("romaji") or "Unknown"
        media_type = data.get("type", "ANIME")
        print(f"    → {title} ({media_type})")
        
        # Download images
        extract_and_download_images(manifest, "anilist", data)
        
        # Queue relations
        for edge in data.get("relations", {}).get("edges", []):
            rel_type = edge.get("relationType")
            rel_node = edge.get("node", {})
            rel_id = rel_node.get("id")
            rel_media_type = rel_node.get("type")
            
            if not rel_id or rel_id in fetched_anilist:
                continue
            
            # Skip manga unless include_manga
            if rel_media_type == "MANGA" and not include_manga:
                continue
            
            # Follow allowed relation types
            if rel_type in follow_relations:
                to_fetch_anilist.append(rel_id)
                rel_title = rel_node.get("title", {}).get("english") or rel_node.get("title", {}).get("romaji")
                print(f"      Queued: {rel_title} ({rel_type})")
        
        rate_limit()
    
    # --- Fetch MAL for each AniList entry ---
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
    
    # --- Fetch TVDB for anime entries ---
    if TVDB_TOKEN:
        print(f"\n  Fetching TVDB data...")
        for al_id in results["anilist"]:
            al_file = anilist_dir / f"{al_id}.json"
            if not al_file.exists():
                continue
            
            with open(al_file) as f:
                al_data = json.load(f)
            
            # Only fetch TVDB for anime TV series
            if al_data.get("type") != "ANIME" or al_data.get("format") not in ("TV", "TV_SHORT"):
                continue
            
            title = al_data.get("title", {}).get("romaji") or al_data.get("title", {}).get("english")
            if not title:
                continue
            
            print(f"    Searching TVDB for: {title}")
            try:
                tvdb_results = tvdb_search(title)
                if tvdb_results:
                    # Pick best match by year
                    al_year = al_data.get("startDate", {}).get("year")
                    best_match = None
                    for r in tvdb_results:
                        if r.get("year") == al_year:
                            best_match = r
                            break
                    if not best_match and tvdb_results:
                        best_match = tvdb_results[0]
                    
                    if best_match:
                        tvdb_id = best_match.get("tvdb_id")
                        if tvdb_id:
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
    
    # Save manifest
    manifest.save()
    results["images"] = sum(len(v) for k, v in manifest.data.items() if isinstance(v, dict))
    
    print(f"\n=== Fetch Summary ===")
    print(f"  AniList entries: {len(results['anilist'])}")
    print(f"  MAL entries: {len(results['mal'])}")
    print(f"  TVDB entries: {len(results['tvdb'])}")
    print(f"  Images: {results['images']}")
    
    return results


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class Entry:
    """A single media entry (anime, manga, OVA, etc.)."""
    anilist_id: str
    mal_id: Optional[str] = None
    tvdb_id: Optional[str] = None
    wikidata_id: Optional[str] = None
    
    title: str = ""
    title_native: Optional[str] = None
    alternate_titles: List[str] = field(default_factory=list)
    
    media_type: str = "anime"  # anime, manga, light_novel, ova, ona, movie, special
    format_detail: Optional[str] = None  # TV, TV_SHORT, MOVIE, OVA, ONA, SPECIAL, MUSIC, MANGA, NOVEL, ONE_SHOT
    
    status: str = "released"
    release_date: Optional[str] = None
    end_date: Optional[str] = None
    
    description: Optional[str] = None
    episode_count: Optional[int] = None
    chapter_count: Optional[int] = None
    volume_count: Optional[int] = None
    
    genres: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    
    source_type: Optional[str] = None  # Original, Manga, Light Novel, etc.
    
    external_links: Dict[str, str] = field(default_factory=dict)
    
    @property
    def slug(self) -> str:
        year = self.release_date[:4] if self.release_date else "unknown"
        return f"{slugify(self.title)}-{self.media_type}-{year}"


@dataclass
class Season:
    """A season within an entry."""
    season_number: int
    title: Optional[str] = None
    episode_count: Optional[int] = None
    air_date_start: Optional[str] = None
    air_date_end: Optional[str] = None
    tvdb_id: Optional[int] = None


@dataclass
class Creator:
    """A person (staff, voice actor, author, etc.)."""
    anilist_id: Optional[str] = None
    mal_id: Optional[str] = None
    wikidata_id: Optional[str] = None
    
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
    """A fictional character."""
    anilist_id: Optional[str] = None
    mal_id: Optional[str] = None
    wikidata_id: Optional[str] = None
    
    name: str = ""
    native_name: Optional[str] = None
    alternate_names: List[str] = field(default_factory=list)
    
    description: Optional[str] = None
    role: str = "supporting"  # main, supporting
    
    @property
    def slug(self) -> str:
        return slugify(self.name)


@dataclass
class Company:
    """A company (studio, publisher, etc.)."""
    anilist_id: Optional[str] = None
    mal_id: Optional[str] = None
    wikidata_id: Optional[str] = None
    
    name: str = ""
    role: str = "animation_studio"
    
    @property
    def slug(self) -> str:
        return slugify(self.name)


@dataclass
class Relationship:
    """A relationship between two entries."""
    source_slug: str
    target_slug: str
    relationship_type: str  # sequel, prequel, adaptation, source, side_story, etc.


@dataclass
class VoiceActorRole:
    """A voice actor's role for a character in an entry."""
    entry_slug: str
    creator_slug: str
    character_slug: str
    language: str  # ja, en, ko


@dataclass
class ProcessedFranchise:
    """Fully processed franchise data."""
    slug: str
    name: str
    description: Optional[str] = None
    
    entries: Dict[str, Entry] = field(default_factory=dict)  # slug -> Entry
    seasons: Dict[str, List[Season]] = field(default_factory=dict)  # entry_slug -> [Season]
    creators: Dict[str, Creator] = field(default_factory=dict)  # slug -> Creator
    characters: Dict[str, Character] = field(default_factory=dict)  # slug -> Character  
    companies: Dict[str, Company] = field(default_factory=dict)  # slug -> Company
    
    relationships: List[Relationship] = field(default_factory=list)
    va_roles: List[VoiceActorRole] = field(default_factory=list)
    
    # Entry -> [creator_slug, role]
    entry_creators: Dict[str, List[Tuple[str, str]]] = field(default_factory=dict)
    # Entry -> [company_slug, role]
    entry_companies: Dict[str, List[Tuple[str, str]]] = field(default_factory=dict)
    # Entry -> [character_slug, role]
    entry_characters: Dict[str, List[Tuple[str, str]]] = field(default_factory=dict)


# =============================================================================
# STAGE 2: PROCESS
# =============================================================================

def map_format_to_media_type(al_format: str, al_type: str) -> str:
    """Map AniList format to our media_type."""
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
    """Map AniList status to our status."""
    status_map = {
        "RELEASING": "releasing",
        "FINISHED": "released",
        "NOT_YET_RELEASED": "announced",
        "CANCELLED": "cancelled",
        "HIATUS": "hiatus",
    }
    return status_map.get(al_status, "released")


def map_relation_type(al_relation: str) -> str:
    """Map AniList relation type to our relationship_type."""
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
    """Format AniList date object to YYYY-MM-DD."""
    if not date_obj or not date_obj.get("year"):
        return None
    y = date_obj["year"]
    m = date_obj.get("month", 1) or 1
    d = date_obj.get("day", 1) or 1
    return f"{y:04d}-{m:02d}-{d:02d}"


def clean_description(desc: str) -> Optional[str]:
    """Clean HTML and excessive formatting from description."""
    if not desc:
        return None
    # Remove HTML tags
    desc = re.sub(r'<[^>]+>', '', desc)
    # Remove source citations
    desc = re.sub(r'\(Source:.*?\)', '', desc)
    desc = re.sub(r'\[Written by.*?\]', '', desc)
    # Clean whitespace
    desc = re.sub(r'\s+', ' ', desc).strip()
    # Limit length
    if len(desc) > 2000:
        desc = desc[:1997] + "..."
    return desc if desc else None


def process_franchise_v2(slug: str) -> ProcessedFranchise:
    """Process all source data into structured format."""
    franchise_dir = SOURCES_DIR / slug
    if not franchise_dir.exists():
        raise FileNotFoundError(f"No sources for {slug}")
    
    result = ProcessedFranchise(slug=slug, name=slug.replace("-", " ").title())
    
    # Load AniList data
    anilist_dir = franchise_dir / "anilist"
    anilist_data = {}
    if anilist_dir.exists():
        for f in anilist_dir.glob("*.json"):
            with open(f) as fh:
                data = json.load(fh)
                anilist_data[data.get("id")] = data
    
    # Load MAL data
    mal_dir = franchise_dir / "mal"
    mal_data = {}
    if mal_dir.exists():
        for f in mal_dir.glob("*.json"):
            with open(f) as fh:
                data = json.load(fh)
                mal_id = data.get("anime", {}).get("mal_id") or data.get("mal_id")
                if mal_id:
                    mal_data[mal_id] = data
    
    # Load TVDB data
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
    
    # Track creator/character by normalized name for deduplication
    creator_by_name = {}
    character_by_name = {}
    
    # --- Process each AniList entry ---
    for al_id, al in anilist_data.items():
        title = al.get("title", {}).get("english") or al.get("title", {}).get("romaji") or f"Unknown-{al_id}"
        
        # Create Entry
        entry = Entry(
            anilist_id=str(al_id),
            mal_id=str(al.get("idMal")) if al.get("idMal") else None,
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
        
        # External links
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
        
        # --- Studios/Companies ---
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
        
        # --- Staff ---
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
            
            # Map role to our creator_roles
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
            
            if db_role:
                result.entry_creators[entry.slug].append((creator_slug, db_role))
        
        # --- Characters ---
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
                lang = "ja"  # Default to Japanese since we request JAPANESE VAs
                if va.get("languageV2"):
                    lang_map = {"Japanese": "ja", "English": "en", "Korean": "ko"}
                    lang = lang_map.get(va["languageV2"], "ja")
                
                result.va_roles.append(VoiceActorRole(
                    entry_slug=entry.slug,
                    creator_slug=va_slug,
                    character_slug=char_slug,
                    language=lang,
                ))
        
        # --- Relations ---
        for edge in al.get("relations", {}).get("edges") or []:
            rel_type = edge.get("relationType")
            rel_node = edge.get("node", {})
            rel_id = rel_node.get("id")
            
            if not rel_id or rel_id not in anilist_data:
                continue  # Only link to entries we have
            
            # Find target entry slug
            rel_al = anilist_data[rel_id]
            rel_title = rel_al.get("title", {}).get("english") or rel_al.get("title", {}).get("romaji")
            rel_media_type = map_format_to_media_type(rel_al.get("format"), rel_al.get("type"))
            rel_date = format_date(rel_al.get("startDate"))
            rel_year = rel_date[:4] if rel_date else "unknown"
            target_slug = f"{slugify(rel_title)}-{rel_media_type}-{rel_year}"
            
            db_rel_type = map_relation_type(rel_type)
            if db_rel_type != "other":
                result.relationships.append(Relationship(
                    source_slug=entry.slug,
                    target_slug=target_slug,
                    relationship_type=db_rel_type,
                ))
    
    # --- Merge MAL data ---
    for mal_id, ml in mal_data.items():
        anime = ml.get("anime", {})
        al_match = None
        
        # Find matching AniList entry
        for al_id, al in anilist_data.items():
            if al.get("idMal") == mal_id:
                al_match = al
                break
        
        if not al_match:
            continue
        
        # Find corresponding entry
        title = al_match.get("title", {}).get("english") or al_match.get("title", {}).get("romaji")
        media_type = map_format_to_media_type(al_match.get("format"), al_match.get("type"))
        date = format_date(al_match.get("startDate"))
        year = date[:4] if date else "unknown"
        entry_slug = f"{slugify(title)}-{media_type}-{year}"
        
        if entry_slug not in result.entries:
            continue
        
        entry = result.entries[entry_slug]
        entry.mal_id = str(mal_id)
        
        # Add MAL external links
        for link in anime.get("external", []):
            site = link.get("name", "").lower().replace(" ", "_")
            if site and link.get("url"):
                entry.external_links[site] = link["url"]
        
        for link in anime.get("streaming", []):
            site = link.get("name", "").lower().replace(" ", "_")
            if site and link.get("url"):
                entry.external_links[site] = link["url"]
        
        # Merge characters from MAL (for English VAs)
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
            
            # Add English VAs
            for va in char.get("voice_actors", []):
                if va.get("language") != "English":
                    continue
                
                person = va.get("person", {})
                va_name = person.get("name", "")
                va_norm = normalize_name(va_name)
                
                if va_norm not in creator_by_name:
                    va_creator = Creator(
                        mal_id=str(person.get("mal_id")),
                        name=normalize_name(va_name).title(),  # Convert from "Last, First"
                        native_name=None,
                    )
                    creator_by_name[va_norm] = va_creator.slug
                    result.creators[va_creator.slug] = va_creator
                else:
                    # Update MAL ID
                    va_slug = creator_by_name[va_norm]
                    if va_slug in result.creators:
                        result.creators[va_slug].mal_id = str(person.get("mal_id"))
                
                va_slug = creator_by_name[va_norm]
                result.va_roles.append(VoiceActorRole(
                    entry_slug=entry_slug,
                    creator_slug=va_slug,
                    character_slug=char_slug,
                    language="en",
                ))
        
        # Merge staff from MAL
        for staff in ml.get("staff", []):
            person = staff.get("person", {})
            person_name = person.get("name", "")
            person_norm = normalize_name(person_name)
            
            if person_norm in creator_by_name:
                creator_slug = creator_by_name[person_norm]
                if creator_slug in result.creators:
                    result.creators[creator_slug].mal_id = str(person.get("mal_id"))
    
    # --- Add TVDB seasons ---
    for tvdb_id, tvdb in tvdb_data.items():
        # Find matching entry by year
        tvdb_year = tvdb.get("year")
        if tvdb_year:
            tvdb_year = int(tvdb_year)
        matching_entry = None
        
        for entry in result.entries.values():
            if entry.media_type == "anime" and entry.release_date:
                entry_year = int(entry.release_date[:4])
                if entry_year == tvdb_year:
                    matching_entry = entry
                    entry.tvdb_id = str(tvdb_id)
                    break
        
        if not matching_entry:
            continue
        
        # Add seasons
        result.seasons[matching_entry.slug] = []
        for season in tvdb.get("seasons", []):
            sn = season.get("seasonNumber", 0)
            if sn == 0:
                continue
            result.seasons[matching_entry.slug].append(Season(
                season_number=sn,
                episode_count=season.get("episodeCount"),
                air_date_start=season.get("firstAired"),
                air_date_end=season.get("lastAired"),
                tvdb_id=tvdb_id,
            ))
    
    # Set franchise name from first/main entry
    if result.entries:
        # Pick entry with earliest date that's a main format (TV, MANGA)
        main_entries = [e for e in result.entries.values() if e.format_detail in ("TV", "MANGA", "NOVEL")]
        if main_entries:
            main_entries.sort(key=lambda e: e.release_date or "9999")
            result.name = main_entries[0].title
            result.description = main_entries[0].description
    
    # Save processed data
    processed_path = franchise_dir / "processed_v2.json"
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
    print(f"  Creators: {len(result.creators)}")
    print(f"  Characters: {len(result.characters)}")
    print(f"  Companies: {len(result.companies)}")
    print(f"  Relationships: {len(result.relationships)}")
    print(f"  VA roles: {len(result.va_roles)}")
    print(f"  Saved to: {processed_path}")
    
    return result


# =============================================================================
# STAGE 3: GENERATE SQL
# =============================================================================

def generate_sql_v2(slug: str) -> str:
    """Generate SQL from processed v2 data."""
    franchise_dir = SOURCES_DIR / slug
    processed_path = franchise_dir / "processed_v2.json"
    
    if not processed_path.exists():
        raise FileNotFoundError(f"No processed_v2.json for {slug}")
    
    with open(processed_path) as f:
        data = json.load(f)
    
    lines = []
    lines.append(f"-- ============================================================================")
    lines.append(f"-- SQL for franchise: {slug}")
    lines.append(f"-- Generated by pipeline_v2.py")
    lines.append(f"-- Entries: {len(data['entries'])}")
    lines.append(f"-- ============================================================================")
    lines.append("")
    lines.append("BEGIN;")
    lines.append("")
    
    # --- Franchise ---
    desc = f"'{escape_sql(data['description'][:500])}'" if data.get('description') else "NULL"
    lines.append(f"""
-- Franchise
INSERT INTO franchises (id, name, slug, description)
VALUES (gen_random_uuid(), '{escape_sql(data['name'])}', '{data['slug']}', {desc})
ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name;
""")
    
    # --- Companies ---
    if data.get("companies"):
        lines.append("-- Companies")
        for slug_key, company in data["companies"].items():
            websites = {}
            if company.get("anilist_id"):
                websites["anilist_id"] = company["anilist_id"]
            if company.get("mal_id"):
                websites["mal_id"] = company["mal_id"]
            
            lines.append(f"""
INSERT INTO companies (id, name, slug, websites)
VALUES (gen_random_uuid(), '{escape_sql(company['name'])}', '{slug_key}', '{escape_json_for_sql(websites)}'::jsonb)
ON CONFLICT (slug) DO UPDATE SET websites = companies.websites || EXCLUDED.websites;
""")
    
    # --- Creators ---
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
    
    # --- Entries ---
    for entry_slug, entry in data.get("entries", {}).items():
        lines.append(f"-- Entry: {entry['title']}")
        
        details = {
            "anilist_id": entry.get("anilist_id"),
            "format": entry.get("format_detail"),
        }
        if entry.get("mal_id"):
            details["mal_id"] = entry["mal_id"]
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
            tvdb_id = season.get("tvdb_id") or "NULL"
            
            lines.append(f"""
INSERT INTO entry_seasons (id, entry_id, season_number, title, episode_count, air_date_start, air_date_end, tvdb_id)
SELECT gen_random_uuid(), e.id, {season['season_number']}, {title}, {ep_count}, {air_start}, {air_end}, {tvdb_id}
FROM entries e WHERE e.slug = '{entry_slug}'
ON CONFLICT (entry_id, season_number) DO NOTHING;
""")
        
        # Entry-Franchise link
        lines.append(f"""
INSERT INTO entry_franchises (id, entry_id, franchise_id)
SELECT gen_random_uuid(), e.id, f.id
FROM entries e, franchises f
WHERE e.slug = '{entry_slug}' AND f.slug = '{data['slug']}'
ON CONFLICT (entry_id, franchise_id) DO NOTHING;
""")
        
        # Entry-Company links
        for company_slug, role in data.get("entry_companies", {}).get(entry_slug, []):
            lines.append(f"""
INSERT INTO entry_companies (id, entry_id, company_id, role_id)
SELECT gen_random_uuid(), e.id, c.id, cr.id
FROM entries e, companies c, company_roles cr
WHERE e.slug = '{entry_slug}' AND c.slug = '{company_slug}' AND cr.name = '{role}'
ON CONFLICT (entry_id, company_id, role_id) DO NOTHING;
""")
        
        # Entry-Genre links
        for genre in entry.get("genres", []):
            lines.append(f"""
INSERT INTO entry_genres (id, entry_id, genre_id)
SELECT gen_random_uuid(), e.id, g.id
FROM entries e, genres g
WHERE e.slug = '{entry_slug}' AND g.name = '{genre}'
ON CONFLICT (entry_id, genre_id) DO NOTHING;
""")
        
        # Entry-Tag links
        for tag in entry.get("tags", []):
            lines.append(f"""
INSERT INTO entry_tags (id, entry_id, tag_id)
SELECT gen_random_uuid(), e.id, t.id
FROM entries e, tags t
WHERE e.slug = '{entry_slug}' AND t.name = '{tag}'
ON CONFLICT (entry_id, tag_id) DO NOTHING;
""")
        
        # Entry-Creator links (non-VA)
        for creator_slug, role in data.get("entry_creators", {}).get(entry_slug, []):
            lines.append(f"""
INSERT INTO entry_creators (id, entry_id, creator_id, role_id)
SELECT gen_random_uuid(), e.id, c.id, r.id
FROM entries e, creators c, creator_roles r
WHERE e.slug = '{entry_slug}' AND c.slug = '{creator_slug}' AND r.name = '{role}'
ON CONFLICT DO NOTHING;
""")
    
    # --- Characters ---
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
    
    # --- Entry-Character links ---
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
    
    # --- VA roles ---
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
    
    # --- Relationships ---
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
    lines.append(f"SELECT COUNT(*) as entries FROM entries WHERE slug LIKE '%{data['slug']}%' OR details->>'anilist_id' IS NOT NULL;")
    
    sql = "\n".join(lines)
    
    sql_path = franchise_dir / "insert_v2.sql"
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
    parser = argparse.ArgumentParser(description="The Watchlist Pipeline v2")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Fetch
    fetch_p = subparsers.add_parser("fetch", help="Fetch from APIs")
    fetch_p.add_argument("slug", help="Franchise slug")
    fetch_p.add_argument("--anilist-id", type=int, required=True, help="Starting AniList ID")
    fetch_p.add_argument("--include-manga", action="store_true", default=True, help="Include manga/LN")
    fetch_p.add_argument("--max-entries", type=int, default=50, help="Max entries to fetch")
    
    # Process
    proc_p = subparsers.add_parser("process", help="Process raw data")
    proc_p.add_argument("slug", help="Franchise slug")
    
    # Generate
    gen_p = subparsers.add_parser("generate", help="Generate SQL")
    gen_p.add_argument("slug", help="Franchise slug")
    
    # All
    all_p = subparsers.add_parser("all", help="Run full pipeline")
    all_p.add_argument("slug", help="Franchise slug")
    all_p.add_argument("--anilist-id", type=int, required=True, help="Starting AniList ID")
    all_p.add_argument("--include-manga", action="store_true", default=True, help="Include manga/LN")
    all_p.add_argument("--max-entries", type=int, default=50, help="Max entries")
    
    args = parser.parse_args()
    
    if args.command == "fetch":
        fetch_franchise_v2(args.slug, args.anilist_id, args.include_manga, args.max_entries)
    elif args.command == "process":
        process_franchise_v2(args.slug)
    elif args.command == "generate":
        generate_sql_v2(args.slug)
    elif args.command == "all":
        fetch_franchise_v2(args.slug, args.anilist_id, args.include_manga, args.max_entries)
        process_franchise_v2(args.slug)
        generate_sql_v2(args.slug)


if __name__ == "__main__":
    main()
