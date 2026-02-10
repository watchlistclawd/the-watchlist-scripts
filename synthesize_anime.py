#!/usr/bin/env python3
"""
Synthesize anime entry data from multiple TOON sources into schema-aligned JSON.

Usage:
    python3 synthesize_anime.py <franchise-slug> <entry-slug>
    python3 synthesize_anime.py demon-slayer demon-slayer-kimetsu-no-yaiba

Reads: franchises/{franchise}/entries/{entry}/sources/*.toon
Writes: franchises/{franchise}/entries/{entry}/entry.json
        franchises/{franchise}/franchise.json
        franchises/{franchise}/characters/{slug}/character.json
"""

import os
import sys
import json
import glob

sys.path.insert(0, os.path.dirname(__file__))
import toon
from utils import slugify, ensure_dir, franchise_dir, entry_dir, character_dir, save_json, DATA_ROOT


def load_sources(base_dir: str) -> dict:
    """Load all TOON and JSON source files from a sources/ directory."""
    sources_dir = os.path.join(base_dir, "sources")
    data = {}
    for f in sorted(glob.glob(os.path.join(sources_dir, "*.toon"))):
        name = os.path.splitext(os.path.basename(f))[0]
        try:
            collection_name, items = toon.load(f)
            data[name] = items
        except Exception as e:
            print(f"  Warning: Failed to load {f}: {e}")
    return data


def build_synthesis_prompt(sources: dict, franchise_slug: str, entry_slug: str) -> str:
    """Build the prompt for LLM synthesis."""
    
    # Read TOON files as raw text for the prompt
    sources_dir = os.path.join(entry_dir(franchise_slug, entry_slug), "sources")
    toon_texts = {}
    for f in sorted(glob.glob(os.path.join(sources_dir, "*.toon"))):
        name = os.path.splitext(os.path.basename(f))[0]
        with open(f, "r", encoding="utf-8") as fh:
            toon_texts[name] = fh.read()

    prompt = """You are a data synthesis agent for a media database called The Watchlist.

Your task: combine data from multiple API sources (AniList and Jikan/MyAnimeList) into a single, accurate, schema-aligned JSON output.

RULES:
1. When sources conflict, prefer the MORE SPECIFIC or MORE COMPLETE value.
2. For dates, use YYYY-MM-DD format. Extract from ISO timestamps if needed.
3. For descriptions, prefer the AniList version (usually better written). Remove HTML tags (<br>, etc).
4. For images, prefer AniList (higher resolution).
5. Include ALL external IDs from both sources.
6. Slugs must be lowercase, hyphenated, no special characters.
7. sort_title: move leading articles to end ("The Godfather" → "Godfather, The"). null if same as title.
8. Characters: include up to 15 most important (MAIN first, then SUPPORTING by favourites).
9. Staff/creators: include key roles only (Director, Series Composition, Character Design, Music, Original Creator).
10. status mapping: "Finished Airing"/"FINISHED" → "released", "Currently Airing"/"RELEASING" → "airing"

SOURCE DATA (TOON format - "name[count]{keys}: \\n values"):

"""
    for name, text in toon_texts.items():
        prompt += f"=== {name} ===\n{text}\n\n"

    prompt += """
OUTPUT FORMAT: Return ONLY valid JSON matching this exact structure. No markdown, no explanation.

{
  "franchise": {
    "name": "English franchise name",
    "native_name": "Japanese name or null",
    "alternate_names": ["other names"],
    "description": "1-2 sentence franchise description",
    "primary_image": "best available image URL",
    "slug": "franchise-slug",
    "websites": {}
  },
  "entry": {
    "media_type": "anime-series",
    "title": "English title",
    "sort_title": "Sort Title, The (or null if same as title)",
    "alternate_titles": ["romaji", "japanese", "other known titles"],
    "release_date": "YYYY-MM-DD (first episode air date)",
    "status": "released|airing|announced|cancelled",
    "description": "Clean synopsis, no HTML, max 500 chars, no spoilers",
    "nsfw": false,
    "locale_code": "ja",
    "primary_image": "cover image URL (prefer AniList)",
    "slug": "entry-slug",
    "details": {
      "external_ids": {
        "mal_id": 0,
        "anilist_id": 0,
        "tvdb_id": null,
        "imdb_id": null
      },
      "total_episodes": 0,
      "episode_runtime_minutes": 0,
      "broadcast_season": "spring|summer|fall|winter",
      "broadcast_year": 2024,
      "source_material": "manga|light_novel|original|visual_novel|game|web_novel",
      "aired": {
        "start": "YYYY-MM-DD",
        "end": "YYYY-MM-DD or null"
      },
      "broadcast": {
        "day": "Saturday",
        "time": "23:30",
        "timezone": "Asia/Tokyo"
      }
    },
    "seasons": [
      {
        "season_number": 1,
        "title": "Season title",
        "anilist_id": 0,
        "mal_id": 0,
        "episodes": 0,
        "start_date": "YYYY-MM-DD",
        "end_date": "YYYY-MM-DD"
      }
    ],
    "external_links": [
      {"site": "Twitter", "url": "https://...", "type": "SOCIAL"}
    ]
  },
  "translations": {
    "ja": { "translated_title": "Japanese title in native script" }
  },
  "genres": ["action", "adventure"],
  "tags": ["historical", "shounen"],
  "characters": [
    {
      "name": "English name (First Last)",
      "sort_name": "Last, First",
      "native_name": "Japanese name",
      "alternate_names": ["nicknames", "other spellings"],
      "description": "Brief description, max 200 chars",
      "primary_image": "image URL",
      "slug": "character-slug",
      "role": "main|supporting",
      "external_ids": {"anilist": 0, "mal": 0},
      "va_name": "Japanese VA name",
      "va_native_name": "VA name in Japanese",
      "va_external_ids": {"anilist": 0, "mal": 0}
    }
  ],
  "creators": [
    {
      "full_name": "Name",
      "sort_name": "Last, First",
      "native_name": "Japanese name or null",
      "primary_image": "image URL or null",
      "slug": "creator-slug",
      "role": "director|original-creator|series-composition|character-design|music",
      "external_ids": {"anilist": 0, "mal": 0},
      "credit_order": 1
    }
  ],
  "companies": [
    {
      "name": "Company Name",
      "native_name": "Japanese name or null",
      "slug": "company-slug",
      "role": "studio|licensor|producer",
      "external_ids": {"anilist": 0},
      "credit_order": 1
    }
  ]
}
"""
    return prompt


def main():
    if len(sys.argv) < 3:
        print("Usage: synthesize_anime.py <franchise-slug> <entry-slug>")
        print("  Prints the synthesis prompt to stdout. Pipe to an LLM.")
        sys.exit(1)

    f_slug = sys.argv[1]
    e_slug = sys.argv[2]

    base = entry_dir(f_slug, e_slug)
    if not os.path.exists(os.path.join(base, "sources")):
        print(f"Error: No sources found at {base}/sources/")
        sys.exit(1)

    sources = load_sources(base)
    prompt = build_synthesis_prompt(sources, f_slug, e_slug)
    
    # Output prompt to stdout (for piping to LLM or reviewing)
    print(prompt)


if __name__ == "__main__":
    main()
