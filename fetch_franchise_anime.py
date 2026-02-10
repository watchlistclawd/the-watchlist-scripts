#!/usr/bin/env python3
"""
Fetch an entire anime franchise from AniList + Jikan.

Crawls AniList relations to discover all entries in a franchise,
then consolidates TV sequels into single entries with seasons.

Usage:
    python3 fetch_franchise_anime.py "Demon Slayer" [--franchise-slug demon-slayer]
    python3 fetch_franchise_anime.py "Little Witch Academia"
    python3 fetch_franchise_anime.py --anilist-id 101922

Output structure:
    franchises/{slug}/
        franchise_sources/          # Franchise-level source data
        entries/{entry-slug}/
            sources/                # Per-entry TOON + raw JSON
        manifest.json               # What was fetched, entry groupings
"""

import argparse
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import toon
from utils import slugify, ensure_dir, save_json, DATA_ROOT
from fetch_anilist_anime import search_anime, save_anime_source, _post, _format_date, SEARCH_ANIME_QUERY
from fetch_jikan_anime import get_anime_full, get_anime_characters, get_anime_staff, save_anime_source as jikan_save
from rate_limiter import print_stats
from fuzzy_match import NameIndex, match_stats


# ── Title matching utilities ──────────────────────────────────────────────

def normalize_title(title: str) -> str:
    """Normalize a title for comparison: lowercase, strip punctuation/seasons/parts."""
    t = title.lower()
    # Remove common suffixes: Part X, Season X, 2nd Season, etc.
    t = re.sub(r'\b(part\s*[ivxlc\d]+|season\s*\d+|\d+(st|nd|rd|th)\s+season)\b', '', t)
    # Remove year in parens: (2015), (TV)
    t = re.sub(r'\([^)]*\)', '', t)
    # Remove punctuation
    t = re.sub(r'[^\w\s]', '', t)
    # Normalize number variants: "iii"/"iiird"/"third"/"3rd" → "3", etc.
    t = re.sub(r'\biiird\b', '3', t)
    t = re.sub(r'\biii\b', '3', t)
    t = re.sub(r'\bthird\b', '3', t)
    t = re.sub(r'\b3rd\b', '3', t)
    t = re.sub(r'\biird\b', '2', t)
    t = re.sub(r'\bii\b', '2', t)
    t = re.sub(r'\bsecond\b', '2', t)
    t = re.sub(r'\b2nd\b', '2', t)
    t = re.sub(r'\biv\b', '4', t)
    t = re.sub(r'\bfourth\b', '4', t)
    t = re.sub(r'\b4th\b', '4', t)
    t = re.sub(r'\b1st\b', '1', t)
    t = re.sub(r'\b5th\b', '5', t)
    # Remove common filler words for looser matching
    t = re.sub(r'\b(the|no|of|a|an)\b', '', t)
    # Collapse whitespace
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def titles_match_franchise(media_title: dict, franchise_keywords: list[str]) -> bool:
    """
    Check if a media title belongs to the same franchise.
    franchise_keywords: list of normalized keywords from the root franchise name.
    
    Returns True if the title contains the main franchise keyword(s).
    """
    # Replace slash with space before normalizing (Fate/Apocrypha → Fate Apocrypha)
    eng = normalize_title((media_title.get("english") or "").replace('/', ' '))
    romaji = normalize_title((media_title.get("romaji") or "").replace('/', ' '))
    native = media_title.get("native") or ""
    
    for kw in franchise_keywords:
        if kw in eng or kw in romaji or kw in native:
            return True
    return False


def extract_franchise_keywords(title: str) -> list[str]:
    """
    Extract the core franchise name keywords for matching.
    "Lupin the 3rd: Castle of Cagliostro" → ["lupin 3", "lupin"]
    "Little Witch Academia (TV)" → ["little witch academia"]
    "Demon Slayer: Kimetsu no Yaiba" → ["demon slayer", "kimetsu yaiba"]
    "Fate/stay night: Unlimited Blade Works" → ["fate stay night", "fate"]
    """
    # Replace slash with space for normalization (Fate/stay → Fate stay)
    clean_title = title.replace('/', ' ')
    
    # Remove subtitle after colon/dash
    base = re.split(r'[:\-–—]', clean_title)[0].strip()
    normalized = normalize_title(base)
    
    keywords = []
    if normalized:
        keywords.append(normalized)
        # Also extract just the first word(s) before any number as a broader match
        # "lupin 3" → also add "lupin" for matching "Lupin ZERO"
        core = re.sub(r'\s*\d+\s*$', '', normalized).strip()
        if core and core != normalized and len(core) >= 3:
            keywords.append(core)
    
    # For slash-brand franchises (Fate/, .hack//, etc.), add the brand prefix
    # "Fate/stay night" → also add "fate" so "Fate/Apocrypha" matches
    if '/' in title:
        brand = title.split('/')[0].strip().lower()
        if brand and len(brand) >= 3 and brand not in keywords:
            keywords.append(brand)
    
    # If original title has a colon, also add the subtitle part as keyword
    if ':' in title:
        parts = title.split(':', 1)
        sub = normalize_title(parts[1].strip())
        if sub and len(sub) > 3:
            keywords.append(sub)
    
    return keywords

# ── AniList relation crawling ──────────────────────────────────────────────

RELATION_QUERY = """
query ($id: Int) {
  Media(id: $id, type: ANIME) {
    id
    idMal
    title { romaji english native userPreferred }
    format
    status
    episodes
    season
    seasonYear
    startDate { year month day }
    endDate { year month day }
    coverImage { extraLarge large }
    relations {
      edges {
        relationType
        node {
          id
          idMal
          title { romaji english native }
          format
          type
          status
          episodes
          season
          seasonYear
          startDate { year month day }
        }
      }
    }
  }
}
"""

FULL_MEDIA_QUERY = """
query ($id: Int) {
  Media(id: $id, type: ANIME) {
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
          id idMal
          title { romaji english native }
          format type
        }
      }
    }
    externalLinks { url site type }
    siteUrl
  }
}
"""


def fetch_media_by_id(anilist_id: int) -> dict:
    """Fetch full media details by AniList ID."""
    resp = _post(FULL_MEDIA_QUERY, {"id": anilist_id})
    return resp.get("data", {}).get("Media", {})


def crawl_relations(root_id: int, franchise_keywords: list[str],
                    visited: set = None, depth: int = 0, max_depth: int = 12) -> list[dict]:
    """
    Crawl AniList relations starting from root_id.
    Returns list of all discovered anime (nodes with basic info).
    
    franchise_keywords: used to filter SIDE_STORY/ALTERNATIVE relations.
        SEQUEL/PREQUEL are always followed (same franchise by definition).
        SIDE_STORY/ALTERNATIVE/PARENT only followed if title matches franchise.
    """
    if visited is None:
        visited = set()

    if root_id in visited or depth > max_depth:
        return []

    visited.add(root_id)

    resp = _post(RELATION_QUERY, {"id": root_id})
    media = resp.get("data", {}).get("Media")
    if not media:
        return []

    results = [media]
    title = media.get("title", {})
    print(f"    [{depth:>2}] {title.get('english') or title.get('romaji')} ({media.get('format', '?')})")

    for edge in media.get("relations", {}).get("edges", []):
        node = edge["node"]
        rel_type = edge.get("relationType", "")

        # Only follow ANIME relations
        if node.get("type") != "ANIME":
            continue

        # SEQUEL/PREQUEL: always follow — same franchise by definition
        if rel_type in ("SEQUEL", "PREQUEL"):
            child_results = crawl_relations(node["id"], franchise_keywords, visited, depth + 1, max_depth)
            results.extend(child_results)
        
        # SIDE_STORY/ALTERNATIVE/PARENT: only follow if title matches franchise
        elif rel_type in ("SIDE_STORY", "PARENT", "ALTERNATIVE"):
            node_title = node.get("title", {})
            if titles_match_franchise(node_title, franchise_keywords):
                child_results = crawl_relations(node["id"], franchise_keywords, visited, depth + 1, max_depth)
                results.extend(child_results)
            else:
                skip_name = node_title.get("english") or node_title.get("romaji") or "?"
                print(f"    ⏭️  Skipping cross-franchise {rel_type}: {skip_name}")

    return results


# ── Entry grouping logic ──────────────────────────────────────────────────

def classify_entry(media: dict) -> str:
    """
    Classify an AniList media node into our entry type.
    Returns: 'tv-season', 'movie', 'ova', 'special', 'other'
    """
    fmt = (media.get("format") or "").upper()

    if fmt == "TV":
        return "tv-season"
    elif fmt == "TV_SHORT":
        return "tv-season"
    elif fmt == "MOVIE":
        return "movie"
    elif fmt in ("OVA", "ONA"):
        return "ova"
    elif fmt == "SPECIAL":
        return "special"
    else:
        return "other"


def _strip_arc_subtitle(title: str) -> str:
    """
    Strip arc/subtitle suffixes for TV grouping.
    
    "Demon Slayer: Kimetsu no Yaiba Entertainment District Arc" → "Demon Slayer: Kimetsu no Yaiba"
    "Attack on Titan Final Season" → "Attack on Titan"
    "Attack on Titan Final Season The Final Chapters Special 1" → "Attack on Titan"
    
    Works on already-normalized (lowercase, no punctuation) titles.
    """
    # Strip everything after known arc/season indicators
    # Order matters: longer patterns first
    arc_patterns = [
        r'\s+final\s+season.*$',
        r'\s+\d+(st|nd|rd|th)\s+season.*$',
        r'\s+season\s+\d+.*$',
        r'\s+part\s+[ivxlc\d]+.*$',
        r'\s+cour\s+\d+.*$',
    ]
    for pat in arc_patterns:
        title = re.sub(pat, '', title)

    # Strip trailing arc names: "entertainment district arc", "swordsmith village arc", etc.
    # Match up to 4 words before "arc" to avoid eating the whole title
    title = re.sub(r'\s+(?:\w+\s+){0,4}arc\s*$', '', title)

    return title.strip()


def _tv_group_key(media: dict) -> str:
    """
    Generate a grouping key for TV series based on normalized base title.
    TV series with the same base name get consolidated into one entry.
    
    "Lupin the 3rd" → "lupin 3"
    "Lupin the 3rd Part 2" → "lupin 3"
    "Demon Slayer: KnY Entertainment District Arc" → "demon slayer kimetsu yaiba"
    "Attack on Titan Final Season" → "attack on titan"
    """
    title = media.get("title", {})
    eng = title.get("english") or title.get("romaji") or ""
    # Normalize slashes to spaces (Fate/stay → Fate stay)
    eng_clean = eng.replace('/', ' ')
    
    normalized = normalize_title(eng_clean)
    stripped = _strip_arc_subtitle(normalized)
    
    # If the title has a colon, also compute the pre-colon key
    # "Demon Slayer: Kimetsu no Yaiba" → "demon slayer"
    # "Demon Slayer: KnY Entertainment District Arc" → "demon slayer"
    # Both get the same key this way.
    if ':' in eng_clean:
        pre_colon = normalize_title(eng_clean.split(':')[0].strip())
        pre_colon = _strip_arc_subtitle(pre_colon)
        post_colon = eng_clean.split(':', 1)[1].strip().lower()
        
        # Use pre-colon grouping ONLY if post-colon is purely an arc/season indicator
        # or a Japanese alt-title. NOT when it contains a distinct subtitle.
        #
        # "Demon Slayer: Kimetsu no Yaiba" → group by pre-colon (JP alt-title)
        # "Demon Slayer: KnY Entertainment District Arc" → group by pre-colon (arc)
        # "Fate/stay night: Unlimited Blade Works" → keep full (distinct adaptation)
        # "Fate/stay night: UBW 2nd Season" → keep full (season OF a distinct subtitle)
        
        # Strip season/part suffixes from post-colon to get the core subtitle
        post_core = re.sub(r'\b\d*(st|nd|rd|th)?\s*season\b.*$', '', post_colon, flags=re.IGNORECASE).strip()
        post_core = re.sub(r'\bseason\s*\d+\b.*$', '', post_core, flags=re.IGNORECASE).strip()
        post_core = re.sub(r'\bpart\s*[ivxlc\d]+\b.*$', '', post_core, flags=re.IGNORECASE).strip()
        
        # Check for known patterns first
        arc_indicators = ['arc', 'cour', 'chapter', 'hen']
        jp_alt_indicators = ['kimetsu', 'yaiba', 'shingeki', 'kyojin']
        
        is_arc = any(ind in post_colon for ind in arc_indicators)
        is_jp_alt = any(ind in post_colon for ind in jp_alt_indicators)
        is_season_only = not post_core  # post-colon was just "2nd Season" etc.
        
        # If it's clearly an arc, JP alt-title, or bare season marker → group by pre-colon
        if is_arc or is_jp_alt or is_season_only:
            return pre_colon
        
        # If after stripping season/part, the post-colon still has substantial content,
        # it's a distinct subtitle (like "Unlimited Blade Works") — use full title
        if post_core and len(post_core.split()) >= 2:
            return stripped
        
        # Otherwise it's a short/generic suffix — group by pre-colon
        return pre_colon
    
    return stripped


def group_entries(all_media: list[dict]) -> list[dict]:
    """
    Group discovered media into our entry model.

    Rules:
    - TV/TV_SHORT entries with matching base titles consolidate into ONE entry with seasons
    - TV series with different base titles become separate entries
    - Each MOVIE is a separate entry
    - Each OVA/ONA is a separate entry
    - SPECIALs are separate entries

    Returns list of entry groups.
    """
    tv_groups = {}  # group_key -> [media, ...]
    entries = []

    for media in all_media:
        entry_type = classify_entry(media)
        title = media.get("title", {})
        eng = title.get("english") or title.get("romaji") or title.get("userPreferred") or ""

        if entry_type == "tv-season":
            key = _tv_group_key(media)
            if key not in tv_groups:
                tv_groups[key] = []
            tv_groups[key].append(media)
        elif entry_type == "movie":
            entries.append({
                "type": "anime-movie",
                "media": media,
                "slug": slugify(eng),
                "title": eng,
            })
        elif entry_type in ("ova", "special", "other"):
            entries.append({
                "type": "anime-ova",
                "media": media,
                "slug": slugify(eng),
                "title": eng,
            })

    # Consolidate each TV group into an anime-series entry
    def sort_key(m):
        sd = m.get("startDate", {})
        y = sd.get("year") or 9999
        mo = sd.get("month") or 1
        d = sd.get("day") or 1
        return (y, mo, d)

    series_entries = []
    for key, seasons in tv_groups.items():
        seasons.sort(key=sort_key)
        first = seasons[0]
        first_title = first.get("title", {})
        eng = first_title.get("english") or first_title.get("romaji") or ""
        
        # Clean the slug — remove "(TV)" and similar artifacts
        slug = slugify(re.sub(r'\s*\((?:TV|tv)\)\s*', ' ', eng).strip())
        
        series_entries.append({
            "type": "anime-series",
            "seasons": seasons,
            "slug": slug,
            "title": eng,
        })
    
    # Sort series by earliest start date
    series_entries.sort(key=lambda e: sort_key(e["seasons"][0]))
    
    all_entries = series_entries + entries
    
    # Fix slug collisions by appending format suffix
    slug_counts = {}
    for e in all_entries:
        slug_counts[e["slug"]] = slug_counts.get(e["slug"], 0) + 1
    
    collisions = {s for s, c in slug_counts.items() if c > 1}
    if collisions:
        for e in all_entries:
            if e["slug"] in collisions:
                etype = e["type"]
                if etype == "anime-series":
                    e["slug"] = e["slug"] + "-tv"
                elif etype == "anime-movie":
                    e["slug"] = e["slug"] + "-movie"
                elif etype == "anime-ova":
                    # Check original format for OVA vs ONA vs SPECIAL
                    fmt = (e.get("media", {}).get("format") or "ova").lower()
                    e["slug"] = e["slug"] + "-" + fmt
    
    return all_entries


# ── Fetching & saving ─────────────────────────────────────────────────────

def fetch_and_save_entry(entry_group: dict, franchise_slug: str):
    """Fetch full data for an entry group and save to disk."""

    entry_slug = entry_group["slug"]
    entry_type = entry_group["type"]
    base_dir = os.path.join(DATA_ROOT, "franchises", franchise_slug, "entries", entry_slug)
    sources_dir = os.path.join(base_dir, "sources")
    ensure_dir(sources_dir)

    print(f"\n  Entry: {entry_group['title']} [{entry_type}]")

    if entry_type == "anime-series":
        # Fetch full data for each season from AniList
        all_season_data = []
        all_characters = {}  # Dedupe by anilist_id
        all_staff = {}  # Dedupe by anilist_id
        all_studios = {}  # Dedupe by anilist_id
        all_external_links = []  # Collect from all seasons
        jikan_char_index = NameIndex()   # Fuzzy: AniList name → MAL char ID
        jikan_va_index = NameIndex()     # Fuzzy: AniList VA name → MAL person ID
        jikan_staff_index = NameIndex()  # Fuzzy: AniList staff name → MAL person ID

        for i, season_media in enumerate(entry_group["seasons"]):
            anilist_id = season_media["id"]
            mal_id = season_media.get("idMal")
            season_title = season_media.get("title", {})
            s_eng = season_title.get("english") or season_title.get("romaji") or ""
            print(f"    Season {i+1}: {s_eng} (AniList:{anilist_id}, MAL:{mal_id})")

            # Fetch full AniList data
            full_media = fetch_media_by_id(anilist_id)

            # Save per-season AniList source
            save_anime_source(full_media, franchise_slug, f"{entry_slug}/_season_sources/anilist_s{i+1}")

            # Collect season info
            all_season_data.append({
                "season_number": i + 1,
                "title": s_eng,
                "anilist_id": anilist_id,
                "mal_id": mal_id,
                "episodes": full_media.get("episodes"),
                "start_date": _format_date(full_media.get("startDate")),
                "end_date": _format_date(full_media.get("endDate")),
                "cover_image": (full_media.get("coverImage") or {}).get("extraLarge", ""),
                "description": (full_media.get("description") or "")[:500],
            })

            # Merge characters (dedupe)
            for edge in full_media.get("characters", {}).get("edges", []):
                node = edge.get("node", {})
                cid = node.get("id")
                if cid and cid not in all_characters:
                    all_characters[cid] = {**edge, "from_season": i + 1}

            # Merge staff (dedupe)
            for edge in full_media.get("staff", {}).get("edges", []):
                node = edge.get("node", {})
                sid = node.get("id")
                if sid and sid not in all_staff:
                    all_staff[sid] = edge

            # Merge studios (dedupe)
            for studio in full_media.get("studios", {}).get("nodes", []):
                studio_id = studio.get("id")
                if studio_id and studio_id not in all_studios:
                    all_studios[studio_id] = studio

            # Collect external links
            for link in full_media.get("externalLinks", []) or []:
                if link not in all_external_links:
                    all_external_links.append(link)

            # Fetch Jikan data for this season
            if mal_id:
                try:
                    jikan_anime = get_anime_full(mal_id)
                    jikan_chars = get_anime_characters(mal_id)
                    jikan_staff_data = get_anime_staff(mal_id)
                    jikan_save(jikan_anime, jikan_chars, jikan_staff_data,
                              franchise_slug, f"{entry_slug}/_season_sources/jikan_s{i+1}")

                    # Build fuzzy name indexes for MAL ID cross-referencing
                    if jikan_chars:
                        for jc in jikan_chars:
                            char = jc.get("character", {})
                            jikan_char_index.add(char.get("name", ""), char.get("mal_id"))
                            for va in jc.get("voice_actors", []):
                                if va.get("language") == "Japanese":
                                    person = va.get("person", {})
                                    jikan_va_index.add(person.get("name", ""), person.get("mal_id"))

                    if jikan_staff_data:
                        for js in jikan_staff_data:
                            person = js.get("person", {})
                            jikan_staff_index.add(person.get("name", ""), person.get("mal_id"))

                except Exception as e:
                    print(f"    ⚠ Jikan fetch failed for MAL {mal_id}: {e}")

        # Save consolidated season manifest
        toon.save(
            os.path.join(sources_dir, "seasons.toon"),
            "seasons",
            all_season_data,
            keys=["season_number", "title", "anilist_id", "mal_id", "episodes",
                  "start_date", "end_date", "cover_image", "description"]
        )

        # Save first season's full AniList data as the "primary" source
        # (for franchise-level info like genres, description, etc.)
        # NOTE: This writes basic anilist_characters/staff TOONs — our consolidated
        # versions below will overwrite them with enhanced data (MAL IDs, etc.)
        first_full = fetch_media_by_id(entry_group["seasons"][0]["id"])
        save_anime_source(first_full, franchise_slug, entry_slug)

        # Save consolidated characters TOON (overwrites basic version from save_anime_source)
        if all_characters:
            chars_for_toon = []
            for cid, edge in all_characters.items():
                node = edge.get("node", {})
                name = node.get("name", {})
                va_list = edge.get("voiceActors", [])
                va = va_list[0] if va_list else {}
                va_name = va.get("name", {}) if va else {}
                # Cross-reference MAL IDs by fuzzy name matching
                full_name = name.get("full", "")
                mal_char_id = jikan_char_index.lookup(full_name, threshold=80) or ""
                va_full = va_name.get("full", "")
                mal_va_id = jikan_va_index.lookup(va_full, threshold=80) or ""
                chars_for_toon.append({
                    "anilist_id": node.get("id", ""),
                    "mal_id": mal_char_id,
                    "name": full_name,
                    "native_name": name.get("native", ""),
                    "alternatives": json.dumps(name.get("alternative", []), ensure_ascii=False),
                    "role": edge.get("role", ""),
                    "description": (node.get("description") or "")[:300],
                    "image": (node.get("image") or {}).get("large", ""),
                    "favourites": node.get("favourites", ""),
                    "va_anilist_id": va.get("id", ""),
                    "va_mal_id": mal_va_id,
                    "va_name": va_name.get("full", ""),
                    "va_native": va_name.get("native", ""),
                    "va_image": (va.get("image") or {}).get("large", ""),
                })
            toon.save(os.path.join(sources_dir, "anilist_characters.toon"), "characters", chars_for_toon)

        # Save consolidated staff TOON
        if all_staff:
            staff_for_toon = []
            for sid, edge in all_staff.items():
                node = edge.get("node", {})
                name = node.get("name", {})
                full_name = name.get("full", "")
                mal_staff_id = jikan_staff_index.lookup(full_name, threshold=80) or ""
                staff_for_toon.append({
                    "anilist_id": node.get("id", ""),
                    "mal_id": mal_staff_id,
                    "name": full_name,
                    "native_name": name.get("native", ""),
                    "role": edge.get("role", ""),
                    "image": (node.get("image") or {}).get("large", ""),
                })
            toon.save(os.path.join(sources_dir, "anilist_staff.toon"), "staff", staff_for_toon)

        # Save studios TOON
        if all_studios:
            studios_for_toon = []
            for studio_id, studio in all_studios.items():
                studios_for_toon.append({
                    "anilist_id": studio_id,
                    "name": studio.get("name", ""),
                    "is_animation_studio": studio.get("isAnimationStudio", ""),
                })
            toon.save(os.path.join(sources_dir, "studios.toon"), "studios", studios_for_toon)

        # Save external links TOON
        if all_external_links:
            links_for_toon = []
            for link in all_external_links:
                links_for_toon.append({
                    "site": link.get("site", ""),
                    "url": link.get("url", ""),
                    "type": link.get("type", ""),
                })
            toon.save(os.path.join(sources_dir, "external_links.toon"), "external_links", links_for_toon)

        # Report match rates
        char_matched = sum(1 for _, e in all_characters.items()
                          if jikan_char_index.lookup(e.get("node", {}).get("name", {}).get("full", ""), 80))
        staff_matched = sum(1 for _, e in all_staff.items()
                           if jikan_staff_index.lookup(e.get("node", {}).get("name", {}).get("full", ""), 80))
        print(f"    ✓ Consolidated {len(entry_group['seasons'])} seasons, "
              f"{len(all_characters)} chars (MAL IDs: {match_stats(len(all_characters), char_matched)}), "
              f"{len(all_staff)} staff (MAL IDs: {match_stats(len(all_staff), staff_matched)})")

    else:
        # Movie, OVA, Special — single media node
        media = entry_group["media"]
        anilist_id = media["id"]
        mal_id = media.get("idMal")

        # Full AniList fetch
        full_media = fetch_media_by_id(anilist_id)
        save_anime_source(full_media, franchise_slug, entry_slug)

        # Jikan fetch
        if mal_id:
            try:
                jikan_anime = get_anime_full(mal_id)
                jikan_chars = get_anime_characters(mal_id)
                jikan_staff_data = get_anime_staff(mal_id)
                jikan_save(jikan_anime, jikan_chars, jikan_staff_data, franchise_slug, entry_slug)
            except Exception as e:
                print(f"    ⚠ Jikan fetch failed for MAL {mal_id}: {e}")

        print(f"    ✓ Saved (AniList:{anilist_id}, MAL:{mal_id})")


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fetch entire anime franchise")
    parser.add_argument("title", nargs="?", help="Search title")
    parser.add_argument("--franchise-slug", "-f", help="Override franchise slug")
    parser.add_argument("--anilist-id", "-a", type=int, help="Start from AniList ID")
    args = parser.parse_args()

    if not args.title and not args.anilist_id:
        parser.error("Provide a title or --anilist-id")

    # Find root anime
    if args.anilist_id:
        root_id = args.anilist_id
        # Fetch title for keyword extraction
        resp = _post(RELATION_QUERY, {"id": root_id})
        root_media = resp.get("data", {}).get("Media", {})
        root_title = root_media.get("title", {})
        franchise_name = root_title.get("english") or root_title.get("romaji") or args.title or ""
        print(f"Starting from AniList ID: {root_id} ({franchise_name})")
    else:
        print(f"Searching AniList for: {args.title}")
        results = search_anime(args.title, per_page=3)
        if not results:
            print("No results found.")
            return
        root = results[0]
        root_id = root["id"]
        title = root.get("title", {})
        franchise_name = title.get("english") or title.get("romaji") or args.title
        print(f"  Found: {franchise_name} (AniList ID: {root_id})")

    # Extract franchise keywords for title filtering during crawl
    franchise_keywords = extract_franchise_keywords(franchise_name)
    # Also add keywords from the search title if different
    if args.title:
        franchise_keywords.extend(extract_franchise_keywords(args.title))
    franchise_keywords = list(set(franchise_keywords))  # dedupe
    print(f"  Franchise keywords: {franchise_keywords}")

    # Crawl relations
    print(f"\nCrawling relations...")
    all_media = crawl_relations(root_id, franchise_keywords)
    print(f"  Discovered {len(all_media)} anime entries")

    for m in all_media:
        t = m.get("title", {})
        fmt = m.get("format", "?")
        eps = m.get("episodes", "?")
        print(f"    [{fmt:>8}] {t.get('english') or t.get('romaji')} ({eps} eps)")

    # Group into our entry model
    entry_groups = group_entries(all_media)
    print(f"\nGrouped into {len(entry_groups)} entries:")
    for eg in entry_groups:
        if eg["type"] == "anime-series":
            print(f"  📺 {eg['title']} ({len(eg['seasons'])} seasons)")
        else:
            print(f"  🎬 {eg['title']} [{eg['type']}]")

    # Determine franchise slug — prefer search title over result title
    franchise_slug = args.franchise_slug
    if not franchise_slug:
        # Use the search title if provided (more likely to be the franchise name)
        # Fall back to the discovered franchise_name
        slug_source = args.title or franchise_name
        clean_name = re.sub(r'\s*\((?:TV|tv|OVA|ONA)\)\s*', ' ', slug_source).strip()
        # Strip subtitle after colon for cleaner slug
        clean_name = re.split(r'[:\-–—]', clean_name)[0].strip()
        franchise_slug = slugify(clean_name)

    # Save manifest
    manifest = {
        "franchise_slug": franchise_slug,
        "source": "anilist+jikan",
        "root_anilist_id": root_id,
        "entries_discovered": len(all_media),
        "entries_grouped": len(entry_groups),
        "entry_groups": [
            {
                "type": eg["type"],
                "slug": eg["slug"],
                "title": eg["title"],
                "seasons": len(eg.get("seasons", [])) if eg["type"] == "anime-series" else None,
                "anilist_ids": [s["id"] for s in eg["seasons"]] if eg["type"] == "anime-series"
                    else [eg["media"]["id"]],
                "mal_ids": [s.get("idMal") for s in eg["seasons"] if s.get("idMal")] if eg["type"] == "anime-series"
                    else ([eg["media"].get("idMal")] if eg["media"].get("idMal") else []),
            }
            for eg in entry_groups
        ],
    }
    manifest_path = os.path.join(DATA_ROOT, "franchises", franchise_slug, "manifest.json")
    save_json(manifest_path, manifest)
    print(f"\nManifest saved to {manifest_path}")

    # Fetch full data for each entry
    print(f"\nFetching full data...")
    for eg in entry_groups:
        fetch_and_save_entry(eg, franchise_slug)

    print_stats()
    print(f"\n✅ Done! Franchise data saved to franchises/{franchise_slug}/")


if __name__ == "__main__":
    main()
