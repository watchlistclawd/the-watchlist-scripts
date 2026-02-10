#!/usr/bin/env python3
"""
Fetch anime series data from Jikan (MyAnimeList API wrapper).
Free, no auth required. Rate limit: 3 req/sec, 60 req/min.

Usage:
    python3 fetch_jikan_anime.py <franchise-slug> [--title "Search Title"]
    python3 fetch_jikan_anime.py demon-slayer --title "Kimetsu no Yaiba"
    python3 fetch_jikan_anime.py --top 10

Saves: franchises/{slug}/entries/{entry-slug}/sources/jikan.toon
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.parse

sys.path.insert(0, os.path.dirname(__file__))
import toon
from utils import slugify, ensure_dir, franchise_dir, entry_dir, save_json
from rate_limiter import jikan_request


def _get(path: str, params: dict = None) -> dict:
    """Make Jikan API request via centralized rate limiter."""
    return jikan_request(path, params)


def search_anime(title: str, limit: int = 5) -> list[dict]:
    """Search Jikan for anime by title."""
    resp = _get("/anime", {"q": title, "limit": limit, "sfw": "true"})
    return resp.get("data", [])


def get_anime_full(mal_id: int) -> dict:
    """Get full anime details by MAL ID."""
    resp = _get(f"/anime/{mal_id}/full")
    return resp.get("data", {})


def get_anime_characters(mal_id: int) -> list[dict]:
    """Get anime characters by MAL ID."""
    resp = _get(f"/anime/{mal_id}/characters")
    return resp.get("data", [])


def get_anime_staff(mal_id: int) -> list[dict]:
    """Get anime staff by MAL ID."""
    resp = _get(f"/anime/{mal_id}/staff")
    return resp.get("data", [])


def get_top_anime(page: int = 1, limit: int = 25, filter_type: str = "bypopularity") -> dict:
    """Get top anime. filter: bypopularity, airing, upcoming, favorite."""
    resp = _get("/top/anime", {"page": page, "limit": limit, "filter": filter_type, "sfw": "true", "type": "tv"})
    return {
        "pagination": resp.get("pagination", {}),
        "data": resp.get("data", []),
    }


def save_anime_source(anime: dict, characters: list, staff: list, franchise_slug: str, entry_slug: str):
    """Save Jikan anime data as TOON files."""
    base = entry_dir(franchise_slug, entry_slug)
    sources = os.path.join(base, "sources")
    ensure_dir(sources)

    # Main anime data
    aired = anime.get("aired", {})
    studios = anime.get("studios", [])
    licensors = anime.get("licensors", [])
    producers = anime.get("producers", [])
    
    entry_data = [{
        "mal_id": anime.get("mal_id", ""),
        "title": anime.get("title", ""),
        "title_english": anime.get("title_english", ""),
        "title_japanese": anime.get("title_japanese", ""),
        "title_synonyms": json.dumps(anime.get("title_synonyms", []), ensure_ascii=False),
        "type": anime.get("type", ""),
        "source": anime.get("source", ""),
        "episodes": anime.get("episodes", ""),
        "status": anime.get("status", ""),
        "airing": anime.get("airing", ""),
        "aired_from": aired.get("from", ""),
        "aired_to": aired.get("to", ""),
        "duration": anime.get("duration", ""),
        "rating": anime.get("rating", ""),
        "score": anime.get("score", ""),
        "scored_by": anime.get("scored_by", ""),
        "rank": anime.get("rank", ""),
        "popularity": anime.get("popularity", ""),
        "members": anime.get("members", ""),
        "synopsis": (anime.get("synopsis") or "")[:500],
        "season": anime.get("season", ""),
        "year": anime.get("year", ""),
        "broadcast": json.dumps(anime.get("broadcast", {}), ensure_ascii=False),
        "genres": json.dumps([g["name"] for g in anime.get("genres", [])], ensure_ascii=False),
        "themes": json.dumps([t["name"] for t in anime.get("themes", [])], ensure_ascii=False),
        "demographics": json.dumps([d["name"] for d in anime.get("demographics", [])], ensure_ascii=False),
        "studios": json.dumps([s["name"] for s in studios], ensure_ascii=False),
        "licensors": json.dumps([l["name"] for l in licensors], ensure_ascii=False),
        "producers": json.dumps([p["name"] for p in producers], ensure_ascii=False),
        "image": (anime.get("images", {}).get("jpg", {}).get("large_image_url", "")),
        "url": anime.get("url", ""),
    }]
    
    toon.save(os.path.join(sources, "jikan.toon"), "anime", entry_data)

    # Characters
    if characters:
        char_data = []
        for c in characters:
            char = c.get("character", {})
            va_list = c.get("voice_actors", [])
            # Get Japanese VA
            ja_va = next((v for v in va_list if v.get("language") == "Japanese"), {})
            ja_person = ja_va.get("person", {}) if ja_va else {}
            char_data.append({
                "mal_id": char.get("mal_id", ""),
                "name": char.get("name", ""),
                "image": (char.get("images", {}).get("jpg", {}).get("image_url", "")),
                "role": c.get("role", ""),
                "favourites": c.get("favorites", ""),
                "va_mal_id": ja_person.get("mal_id", ""),
                "va_name": ja_person.get("name", ""),
                "va_image": (ja_person.get("images", {}).get("jpg", {}).get("image_url", "") if ja_person else ""),
            })
        toon.save(os.path.join(sources, "jikan_characters.toon"), "characters", char_data)

    # Staff
    if staff:
        staff_data = []
        for s in staff:
            person = s.get("person", {})
            positions = s.get("positions", [])
            staff_data.append({
                "mal_id": person.get("mal_id", ""),
                "name": person.get("name", ""),
                "image": (person.get("images", {}).get("jpg", {}).get("image_url", "")),
                "positions": json.dumps(positions, ensure_ascii=False),
            })
        toon.save(os.path.join(sources, "jikan_staff.toon"), "staff", staff_data)
    
    # Also save raw JSON
    save_json(os.path.join(sources, "jikan_raw.json"), {
        "anime": anime,
        "characters": characters,
        "staff": staff,
    })
    
    print(f"  ✓ Jikan saved to {sources}/")


def main():
    parser = argparse.ArgumentParser(description="Fetch anime data from Jikan/MAL")
    parser.add_argument("franchise_slug", nargs="?", help="Franchise slug")
    parser.add_argument("--title", "-t", help="Search title")
    parser.add_argument("--entry-slug", "-e", help="Entry slug if different from franchise")
    parser.add_argument("--mal-id", "-m", type=int, help="Fetch by MAL ID directly")
    parser.add_argument("--top", type=int, help="Fetch top N anime by popularity")
    args = parser.parse_args()

    if args.top:
        print(f"Fetching top {args.top} anime from MAL/Jikan...")
        all_anime = []
        pages_needed = (args.top + 24) // 25  # Jikan max 25 per page
        for page in range(1, pages_needed + 1):
            result = get_top_anime(page=page, limit=25)
            all_anime.extend(result["data"])
            has_next = result["pagination"].get("has_next_page", False)
            if not has_next:
                break
            time.sleep(1.0)  # Be nice to Jikan
        
        all_anime = all_anime[:args.top]
        for a in all_anime:
            print(f"  #{a.get('rank', '?'):>4} | {a.get('mal_id')} | {a.get('title')} | score:{a.get('score')} | pop:{a.get('popularity')}")
        
        save_json(os.path.join(franchise_dir("_rankings"), "jikan_top_anime.json"), all_anime)
        print(f"\nSaved {len(all_anime)} anime to _rankings/")
        return

    if not args.franchise_slug:
        parser.error("franchise_slug is required unless using --top")

    if args.mal_id:
        print(f"Fetching MAL ID {args.mal_id}...")
        anime = get_anime_full(args.mal_id)
        time.sleep(0.4)
        characters = get_anime_characters(args.mal_id)
        time.sleep(0.4)
        staff = get_anime_staff(args.mal_id)
    else:
        search_title = args.title or args.franchise_slug.replace("-", " ")
        print(f"Searching Jikan for: {search_title}")
        results = search_anime(search_title, limit=3)
        if not results:
            print("No results found.")
            return
        anime = results[0]
        mal_id = anime["mal_id"]
        print(f"  Found: {anime.get('title')} (MAL ID: {mal_id})")
        
        # Get full details + characters + staff
        time.sleep(0.4)
        anime = get_anime_full(mal_id)
        time.sleep(0.4)
        characters = get_anime_characters(mal_id)
        time.sleep(0.4)
        staff = get_anime_staff(mal_id)
    
    entry_slug = args.entry_slug or slugify(anime.get("title_english") or anime.get("title") or args.franchise_slug)
    save_anime_source(anime, characters, staff, args.franchise_slug, entry_slug)


if __name__ == "__main__":
    main()
