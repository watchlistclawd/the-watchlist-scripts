#!/usr/bin/env python3
"""
Fetch anime series data from AniList GraphQL API.
Free, no auth required. Rate limit: 90 req/min.

Usage:
    python3 fetch_anilist_anime.py <franchise-slug> [--title "Search Title"]
    python3 fetch_anilist_anime.py demon-slayer --title "Kimetsu no Yaiba"
    
Saves: franchises/{slug}/entries/{entry-slug}/sources/anilist.toon
       franchises/{slug}/sources/anilist.toon (franchise-level)
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import toon
from utils import slugify, ensure_dir, franchise_dir, entry_dir, save_json
from rate_limiter import anilist_request

# --- GraphQL Queries ---

SEARCH_ANIME_QUERY = """
query ($search: String, $page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { total currentPage lastPage hasNextPage }
    media(search: $search, type: ANIME, sort: POPULARITY_DESC) {
      id
      idMal
      title { romaji english native userPreferred }
      synonyms
      format
      status
      description(asHtml: false)
      startDate { year month day }
      endDate { year month day }
      season
      seasonYear
      episodes
      duration
      source
      genres
      tags { name rank isMediaSpoiler }
      coverImage { extraLarge large medium }
      bannerImage
      averageScore
      meanScore
      popularity
      favourites
      studios(isMain: true) { nodes { id name isAnimationStudio } }
      staff(sort: RELEVANCE, perPage: 25) {
        edges {
          role
          node { id name { full native userPreferred } image { large } }
        }
      }
      characters(sort: RELEVANCE, perPage: 25) {
        edges {
          role
          voiceActors(language: JAPANESE) { id name { full native } image { large } }
          node {
            id
            name { full native userPreferred alternative }
            image { large }
            description(asHtml: false)
            favourites
          }
        }
      }
      relations {
        edges {
          relationType
          node {
            id
            idMal
            title { romaji english native }
            format
            type
          }
        }
      }
      externalLinks { url site type }
      siteUrl
    }
  }
}
"""

TOP_ANIME_QUERY = """
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { total currentPage lastPage hasNextPage }
    media(type: ANIME, format: TV, sort: POPULARITY_DESC, isAdult: false) {
      id
      idMal
      title { romaji english native userPreferred }
      popularity
      averageScore
      format
      episodes
      status
      season
      seasonYear
      coverImage { large }
    }
  }
}
"""


def _post(query: str, variables: dict) -> dict:
    """Make AniList GraphQL request via centralized rate limiter."""
    return anilist_request(query, variables)


def _format_date(date_obj: dict | None) -> str | None:
    """Convert AniList date {year, month, day} to 'YYYY-MM-DD'."""
    if not date_obj or not date_obj.get("year"):
        return None
    y = date_obj["year"]
    m = date_obj.get("month") or 1
    d = date_obj.get("day") or 1
    return f"{y:04d}-{m:02d}-{d:02d}"


def search_anime(title: str, per_page: int = 5) -> list[dict]:
    """Search AniList for anime by title. Returns raw media list."""
    resp = _post(SEARCH_ANIME_QUERY, {"search": title, "page": 1, "perPage": per_page})
    return resp.get("data", {}).get("Page", {}).get("media", [])


def get_top_anime(page: int = 1, per_page: int = 50) -> dict:
    """Get top anime by popularity. Returns page info + media list."""
    resp = _post(TOP_ANIME_QUERY, {"page": page, "perPage": per_page})
    page_data = resp.get("data", {}).get("Page", {})
    return {
        "page_info": page_data.get("pageInfo", {}),
        "media": page_data.get("media", []),
    }


def save_anime_source(media: dict, franchise_slug: str, entry_slug: str | None = None):
    """
    Save AniList anime data as TOON files.
    
    Saves:
    - Main anime data as TOON
    - Characters as separate TOON
    - Staff as separate TOON
    """
    if entry_slug:
        base = entry_dir(franchise_slug, entry_slug)
    else:
        base = franchise_dir(franchise_slug)
    
    sources = os.path.join(base, "sources")
    ensure_dir(sources)
    
    # Main anime entry data
    title = media.get("title", {})
    start = media.get("startDate", {})
    end = media.get("endDate", {})
    studios = media.get("studios", {}).get("nodes", [])
    
    entry_data = [{
        "anilist_id": media.get("id", ""),
        "mal_id": media.get("idMal", ""),
        "title_english": title.get("english", ""),
        "title_romaji": title.get("romaji", ""),
        "title_native": title.get("native", ""),
        "synonyms": json.dumps(media.get("synonyms", []), ensure_ascii=False),
        "format": media.get("format", ""),
        "status": media.get("status", ""),
        "description": (media.get("description") or "")[:500],
        "start_date": _format_date(start),
        "end_date": _format_date(end),
        "season": media.get("season", ""),
        "season_year": media.get("seasonYear", ""),
        "episodes": media.get("episodes", ""),
        "duration_min": media.get("duration", ""),
        "source": media.get("source", ""),
        "genres": json.dumps(media.get("genres", []), ensure_ascii=False),
        "avg_score": media.get("averageScore", ""),
        "popularity": media.get("popularity", ""),
        "cover_image": (media.get("coverImage") or {}).get("extraLarge", ""),
        "banner_image": media.get("bannerImage", ""),
        "studios": json.dumps([s["name"] for s in studios], ensure_ascii=False),
        "site_url": media.get("siteUrl", ""),
    }]
    
    toon.save(os.path.join(sources, "anilist.toon"), "anime", entry_data)
    
    # Characters
    char_edges = media.get("characters", {}).get("edges", [])
    if char_edges:
        chars = []
        for edge in char_edges:
            node = edge.get("node", {})
            name = node.get("name", {})
            va_list = edge.get("voiceActors", [])
            va = va_list[0] if va_list else {}
            va_name = va.get("name", {}) if va else {}
            chars.append({
                "anilist_id": node.get("id", ""),
                "name": name.get("full", ""),
                "native_name": name.get("native", ""),
                "alternatives": json.dumps(name.get("alternative", []), ensure_ascii=False),
                "role": edge.get("role", ""),
                "description": (node.get("description") or "")[:300],
                "image": (node.get("image") or {}).get("large", ""),
                "favourites": node.get("favourites", ""),
                "va_id": va.get("id", ""),
                "va_name": va_name.get("full", ""),
                "va_native": va_name.get("native", ""),
            })
        toon.save(os.path.join(sources, "anilist_characters.toon"), "characters", chars)
    
    # Staff
    staff_edges = media.get("staff", {}).get("edges", [])
    if staff_edges:
        staff = []
        for edge in staff_edges:
            node = edge.get("node", {})
            name = node.get("name", {})
            staff.append({
                "anilist_id": node.get("id", ""),
                "name": name.get("full", ""),
                "native_name": name.get("native", ""),
                "role": edge.get("role", ""),
                "image": (node.get("image") or {}).get("large", ""),
            })
        toon.save(os.path.join(sources, "anilist_staff.toon"), "staff", staff)
    
    # Tags (useful for our tags table)
    tags = media.get("tags", [])
    if tags:
        tag_data = [{"name": t["name"], "rank": t.get("rank", ""), "spoiler": t.get("isMediaSpoiler", "")} for t in tags]
        toon.save(os.path.join(sources, "anilist_tags.toon"), "tags", tag_data)
    
    # Relations (for entry_relationships)
    relations = media.get("relations", {}).get("edges", [])
    if relations:
        rel_data = []
        for edge in relations:
            node = edge.get("node", {})
            rel_title = node.get("title", {})
            rel_data.append({
                "relation_type": edge.get("relationType", ""),
                "anilist_id": node.get("id", ""),
                "mal_id": node.get("idMal", ""),
                "title_english": rel_title.get("english", ""),
                "title_romaji": rel_title.get("romaji", ""),
                "format": node.get("format", ""),
                "type": node.get("type", ""),
            })
        toon.save(os.path.join(sources, "anilist_relations.toon"), "relations", rel_data)

    # Also save raw JSON for reference
    save_json(os.path.join(sources, "anilist_raw.json"), media)
    
    print(f"  ✓ AniList saved to {sources}/")
    return media


def main():
    parser = argparse.ArgumentParser(description="Fetch anime data from AniList")
    parser.add_argument("franchise_slug", nargs="?", help="Franchise slug for folder organization")
    parser.add_argument("--title", "-t", help="Search title (defaults to slug with hyphens as spaces)")
    parser.add_argument("--entry-slug", "-e", help="Entry slug (if different from franchise)")
    parser.add_argument("--top", type=int, help="Fetch top N anime by popularity instead of searching")
    args = parser.parse_args()

    if args.top:
        print(f"Fetching top {args.top} anime from AniList...")
        all_media = []
        pages_needed = (args.top + 49) // 50
        for page in range(1, pages_needed + 1):
            result = get_top_anime(page=page, per_page=50)
            all_media.extend(result["media"])
            if not result["page_info"].get("hasNextPage"):
                break
            time.sleep(0.7)  # Rate limit
        
        all_media = all_media[:args.top]
        for m in all_media:
            title = m.get("title", {})
            print(f"  {m.get('id')} | {title.get('english') or title.get('romaji')} | eps:{m.get('episodes')} | pop:{m.get('popularity')}")
        
        save_json(os.path.join(franchise_dir("_rankings"), "anilist_top_anime.json"), all_media)
        print(f"\nSaved {len(all_media)} anime to _rankings/")
        return

    search_title = args.title or args.franchise_slug.replace("-", " ")
    print(f"Searching AniList for: {search_title}")
    
    results = search_anime(search_title, per_page=3)
    if not results:
        print("No results found.")
        return
    
    # Take the top result
    media = results[0]
    title = media.get("title", {})
    entry_slug = args.entry_slug or slugify(title.get("english") or title.get("romaji") or search_title)
    
    print(f"  Found: {title.get('english') or title.get('romaji')} (AniList ID: {media['id']})")
    save_anime_source(media, args.franchise_slug, entry_slug)


if __name__ == "__main__":
    main()
