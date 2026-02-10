#!/usr/bin/env python3
"""
Fetch raw source data for a franchise from TVDB, AniList, and Jikan/MAL.
Saves to sources/{franchise-slug}/

Usage:
    python3 fetch_sources.py "Attack on Titan"
    python3 fetch_sources.py "Sentenced to Be a Hero"
"""
import argparse
import json
import os
import re
import time
import urllib.request
import urllib.parse

DATA_ROOT = os.path.join(os.path.dirname(__file__), "..", "the-watchlist-data")
SOURCES_DIR = os.path.join(DATA_ROOT, "sources")

# TVDB
TVDB_BASE = "https://api4.thetvdb.com/v4"
TVDB_API_KEY = "4a1eb83b-bacd-4589-9401-7d524233d30b"
_tvdb_token = None

# Rate limiting
_last_request = {}

def rate_limit(api: str, min_gap: float = 1.0):
    """Simple rate limiter per API."""
    now = time.time()
    if api in _last_request:
        elapsed = now - _last_request[api]
        if elapsed < min_gap:
            time.sleep(min_gap - elapsed)
    _last_request[api] = time.time()

def slugify(text: str) -> str:
    """Generate URL-safe slug."""
    text = text.lower().strip()
    text = re.sub(r"[/\\]", "-", text)
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    return text.strip("-")

# ─── TVDB ─────────────────────────────────────────────────────────────────────

def tvdb_login():
    """Get TVDB JWT token."""
    global _tvdb_token
    if _tvdb_token:
        return _tvdb_token
    
    req = urllib.request.Request(
        f"{TVDB_BASE}/login",
        data=json.dumps({"apikey": TVDB_API_KEY}).encode(),
        headers={"Content-Type": "application/json"}
    )
    resp = json.loads(urllib.request.urlopen(req).read())
    _tvdb_token = resp["data"]["token"]
    return _tvdb_token

def tvdb_get(endpoint: str) -> dict:
    """GET from TVDB API."""
    rate_limit("tvdb", 0.5)
    token = tvdb_login()
    req = urllib.request.Request(
        f"{TVDB_BASE}{endpoint}",
        headers={"Authorization": f"Bearer {token}"}
    )
    return json.loads(urllib.request.urlopen(req).read())

def tvdb_search(query: str) -> list:
    """Search TVDB for anime series."""
    q = urllib.parse.quote(query)
    # Search with anime filter
    resp = tvdb_get(f"/search?query={q}")
    results = resp.get("data", [])
    # Filter to likely anime (Japanese origin or animation)
    return [r for r in results if r.get("primary_language") == "jpn" or "anime" in str(r).lower()]

def tvdb_series_extended(series_id: int) -> dict:
    """Get extended series info including seasons and episodes."""
    resp = tvdb_get(f"/series/{series_id}/extended?meta=episodes")
    return resp.get("data", {})

# ─── AniList ──────────────────────────────────────────────────────────────────

ANILIST_URL = "https://graphql.anilist.co"

def anilist_query(query: str, variables: dict, retries: int = 3) -> dict:
    """Execute AniList GraphQL query with retry on 429."""
    for attempt in range(retries):
        rate_limit("anilist", 2.0)  # Slower to avoid 429
        req = urllib.request.Request(
            ANILIST_URL,
            data=json.dumps({"query": query, "variables": variables}).encode(),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "TheWatchlist/1.0",
                "Accept": "application/json"
            }
        )
        try:
            return json.loads(urllib.request.urlopen(req).read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                wait = 30 * (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                raise

def anilist_search(title: str) -> list:
    """Search AniList for anime."""
    query = """
    query ($search: String) {
      Page(perPage: 10) {
        media(search: $search, type: ANIME, sort: POPULARITY_DESC) {
          id idMal title { romaji english native }
          format episodes startDate { year month day } endDate { year month day }
          status season seasonYear
        }
      }
    }
    """
    resp = anilist_query(query, {"search": title})
    return resp.get("data", {}).get("Page", {}).get("media", [])

def anilist_full(anilist_id: int) -> dict:
    """Get full AniList media data with characters and staff."""
    query = """
    query ($id: Int) {
      Media(id: $id) {
        id idMal title { romaji english native }
        format episodes duration status
        startDate { year month day } endDate { year month day }
        season seasonYear
        description(asHtml: false)
        genres tags { name rank }
        averageScore popularity
        studios { nodes { id name isAnimationStudio } }
        characters(sort: FAVOURITES_DESC, perPage: 25) {
          edges {
            node { id name { full native } image { large } }
            role
            voiceActors(language: JAPANESE) {
              id name { full native } image { large }
            }
          }
        }
        staff(perPage: 25) {
          edges {
            node { id name { full native } image { large } }
            role
          }
        }
        relations {
          edges {
            node { id idMal title { romaji english } format type }
            relationType
          }
        }
        externalLinks { site url }
      }
    }
    """
    resp = anilist_query(query, {"id": anilist_id})
    return resp.get("data", {}).get("Media", {})

# ─── Jikan/MAL ────────────────────────────────────────────────────────────────

JIKAN_BASE = "https://api.jikan.moe/v4"

def jikan_get(endpoint: str) -> dict:
    """GET from Jikan API."""
    rate_limit("jikan", 1.5)  # Jikan is stricter
    req = urllib.request.Request(
        f"{JIKAN_BASE}{endpoint}",
        headers={"User-Agent": "TheWatchlist/1.0"}
    )
    return json.loads(urllib.request.urlopen(req).read())

def jikan_full(mal_id: int) -> dict:
    """Get full MAL anime data."""
    return jikan_get(f"/anime/{mal_id}/full")

def jikan_characters(mal_id: int) -> list:
    """Get MAL anime characters."""
    resp = jikan_get(f"/anime/{mal_id}/characters")
    return resp.get("data", [])

def jikan_staff(mal_id: int) -> list:
    """Get MAL anime staff."""
    resp = jikan_get(f"/anime/{mal_id}/staff")
    return resp.get("data", [])

# ─── Main ─────────────────────────────────────────────────────────────────────

def fetch_franchise(title: str):
    """Fetch all sources for a franchise."""
    slug = slugify(title)
    franchise_dir = os.path.join(SOURCES_DIR, slug)
    os.makedirs(os.path.join(franchise_dir, "tvdb"), exist_ok=True)
    os.makedirs(os.path.join(franchise_dir, "anilist"), exist_ok=True)
    os.makedirs(os.path.join(franchise_dir, "mal"), exist_ok=True)
    
    print(f"Fetching sources for: {title}")
    print(f"Output: {franchise_dir}/\n")
    
    # 1. Search TVDB
    print("=== TVDB ===")
    tvdb_results = tvdb_search(title)
    print(f"Found {len(tvdb_results)} results")
    
    tvdb_ids = []
    for r in tvdb_results[:5]:  # Top 5 results
        tvdb_id = int(r.get("tvdb_id", r.get("id", 0)))
        name = r.get("name", "?")
        if tvdb_id and title.lower() in name.lower():
            print(f"  → Fetching: {name} (TVDB:{tvdb_id})")
            data = tvdb_series_extended(tvdb_id)
            with open(os.path.join(franchise_dir, "tvdb", f"{tvdb_id}.json"), "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            tvdb_ids.append(tvdb_id)
    
    # 2. Search AniList
    print("\n=== AniList ===")
    al_results = anilist_search(title)
    print(f"Found {len(al_results)} results")
    
    anilist_ids = []
    mal_ids = []
    for r in al_results[:10]:
        al_id = r.get("id")
        mal_id = r.get("idMal")
        name = r.get("title", {}).get("english") or r.get("title", {}).get("romaji")
        
        if al_id:
            print(f"  → Fetching: {name} (AL:{al_id}, MAL:{mal_id})")
            data = anilist_full(al_id)
            with open(os.path.join(franchise_dir, "anilist", f"{al_id}.json"), "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            anilist_ids.append(al_id)
            
            if mal_id:
                mal_ids.append(mal_id)
    
    # 3. Fetch Jikan/MAL for each MAL ID
    print("\n=== Jikan/MAL ===")
    for mal_id in mal_ids:
        print(f"  → Fetching: MAL:{mal_id}")
        try:
            anime = jikan_full(mal_id)
            chars = jikan_characters(mal_id)
            staff = jikan_staff(mal_id)
            
            data = {
                "anime": anime.get("data", {}),
                "characters": chars,
                "staff": staff
            }
            with open(os.path.join(franchise_dir, "mal", f"{mal_id}.json"), "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"    Error: {e}")
    
    print(f"\n✓ Done. Files saved to {franchise_dir}/")
    print(f"  TVDB: {len(tvdb_ids)} series")
    print(f"  AniList: {len(anilist_ids)} entries")
    print(f"  MAL: {len(mal_ids)} entries")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("title", help="Franchise title to search")
    args = parser.parse_args()
    fetch_franchise(args.title)
