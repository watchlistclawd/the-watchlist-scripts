#!/usr/bin/env python3
"""
Fetch any media from AniList by ID — anime, manga, movie, etc.
Used by fetch_franchise.py to follow relation edges.

Usage:
    python3 fetch_anilist_media.py <franchise-slug> --anilist-id 87216 --entry-slug demon-slayer-manga
    python3 fetch_anilist_media.py demon-slayer --anilist-id 112151 --entry-slug demon-slayer-mugen-train-movie
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import toon
from utils import slugify, ensure_dir, franchise_dir, entry_dir, save_json

ANILIST_URL = "https://graphql.anilist.co"

MEDIA_BY_ID_QUERY = """
query ($id: Int) {
  Media(id: $id) {
    id
    idMal
    title { romaji english native userPreferred }
    synonyms
    type
    format
    status
    description(asHtml: false)
    startDate { year month day }
    endDate { year month day }
    season
    seasonYear
    episodes
    chapters
    volumes
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
"""


def _post(query: str, variables: dict) -> dict:
    import urllib.request
    req = urllib.request.Request(
        ANILIST_URL,
        data=json.dumps({"query": query, "variables": variables}).encode(),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "TheWatchlist/1.0 (data pipeline)",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def _format_date(date_obj: dict | None) -> str | None:
    if not date_obj or not date_obj.get("year"):
        return None
    y = date_obj["year"]
    m = date_obj.get("month") or 1
    d = date_obj.get("day") or 1
    return f"{y:04d}-{m:02d}-{d:02d}"


def get_media_by_id(anilist_id: int) -> dict:
    """Fetch any media by AniList ID."""
    resp = _post(MEDIA_BY_ID_QUERY, {"id": anilist_id})
    return resp.get("data", {}).get("Media", {})


def save_media_source(media: dict, franchise_slug: str, entry_slug: str):
    """
    Save AniList media data (anime, manga, movie, etc.) as TOON + raw JSON.
    """
    base = entry_dir(franchise_slug, entry_slug)
    sources = os.path.join(base, "sources")
    ensure_dir(sources)

    media_type = (media.get("type") or "UNKNOWN").lower()
    media_format = (media.get("format") or "UNKNOWN").lower()
    
    title = media.get("title", {})
    start = media.get("startDate", {})
    end = media.get("endDate", {})
    studios = media.get("studios", {}).get("nodes", [])

    # Main entry data — works for anime, manga, movies
    entry_data = [{
        "anilist_id": media.get("id", ""),
        "mal_id": media.get("idMal", ""),
        "type": media.get("type", ""),
        "format": media.get("format", ""),
        "title_english": title.get("english", ""),
        "title_romaji": title.get("romaji", ""),
        "title_native": title.get("native", ""),
        "synonyms": json.dumps(media.get("synonyms", []), ensure_ascii=False),
        "status": media.get("status", ""),
        "description": (media.get("description") or "")[:500],
        "start_date": _format_date(start),
        "end_date": _format_date(end),
        "season": media.get("season") or "",
        "season_year": media.get("seasonYear") or "",
        "episodes": media.get("episodes") or "",
        "chapters": media.get("chapters") or "",
        "volumes": media.get("volumes") or "",
        "duration_min": media.get("duration") or "",
        "source": media.get("source") or "",
        "genres": json.dumps(media.get("genres", []), ensure_ascii=False),
        "avg_score": media.get("averageScore", ""),
        "popularity": media.get("popularity", ""),
        "cover_image": (media.get("coverImage") or {}).get("extraLarge", ""),
        "banner_image": media.get("bannerImage") or "",
        "studios": json.dumps([s["name"] for s in studios], ensure_ascii=False),
        "site_url": media.get("siteUrl", ""),
    }]

    toon_name = f"anilist_{media_type}"
    toon.save(os.path.join(sources, f"anilist.toon"), toon_name, entry_data)

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
                "va_id": va.get("id", "") if va else "",
                "va_name": va_name.get("full", "") if va_name else "",
                "va_native": va_name.get("native", "") if va_name else "",
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

    # Tags
    tags = media.get("tags", [])
    if tags:
        tag_data = [{"name": t["name"], "rank": t.get("rank", ""), "spoiler": t.get("isMediaSpoiler", "")} for t in tags]
        toon.save(os.path.join(sources, "anilist_tags.toon"), "tags", tag_data)

    # Relations
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

    # Raw JSON
    save_json(os.path.join(sources, "anilist_raw.json"), media)

    print(f"  ✓ AniList {media_type}/{media_format} saved to {sources}/")
    return media


def main():
    parser = argparse.ArgumentParser(description="Fetch any AniList media by ID")
    parser.add_argument("franchise_slug", help="Franchise slug")
    parser.add_argument("--anilist-id", "-a", type=int, required=True, help="AniList media ID")
    parser.add_argument("--entry-slug", "-e", required=True, help="Entry slug")
    args = parser.parse_args()

    print(f"Fetching AniList ID {args.anilist_id}...")
    media = get_media_by_id(args.anilist_id)
    if not media:
        print("  ⚠ Not found")
        return

    title = media.get("title", {})
    print(f"  Found: {title.get('english') or title.get('romaji')} ({media.get('type')}/{media.get('format')})")
    save_media_source(media, args.franchise_slug, args.entry_slug)


if __name__ == "__main__":
    main()
