#!/usr/bin/env python3
"""
TVDB-Backbone Franchise Fetcher.

TVDB provides the structural skeleton (what entries exist, how seasons are organized).
AniList/MAL provide the rich metadata body (characters, VAs, staff, scores, tags).

Flow:
  1. Search TVDB for all series + movies in a franchise
  2. Fetch TVDB extended data per entry (seasons, episodes, artwork)
  3. For each TVDB entry, fuzzy-match to AniList by title + release date
  4. Fetch AniList + Jikan detail for matched entries
  5. Save everything organized by TVDB structure

Usage:
    python3 fetch_franchise_tvdb.py "Demon Slayer"
    python3 fetch_franchise_tvdb.py "Fate/stay night"
    python3 fetch_franchise_tvdb.py "Attack on Titan" --include-movies
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import toon
from utils import slugify, ensure_dir, franchise_dir, entry_dir, save_json
from rate_limiter import anilist_request, jikan_request
from fuzzy_match import _ratio, dates_match, FUZZY_ENGINE
from fetch_tvdb_anime import (
    search_series as tvdb_search_series,
    get_series_extended as tvdb_get_series_extended,
    save_tvdb_source,
    _login as tvdb_login,
    _get as tvdb_get,
)
from fetch_anilist_anime import (
    search_anime as anilist_search,
    save_anime_source,
    _format_date,
)
from fetch_jikan_anime import save_anime_source as save_jikan_anime_source


# ── TVDB Discovery ────────────────────────────────────────────────────────

def discover_tvdb_entries(franchise_name: str, include_movies: bool = True,
                          extra_search_terms: list[str] | None = None,
                          anime_only: bool = True) -> list[dict]:
    """
    Search TVDB for all series and movies belonging to a franchise.
    
    Uses multiple search terms and validates results against franchise name
    using aliases + English translations from TVDB (since many anime have
    Japanese primary names on TVDB).
    
    anime_only: If True (default), filter to Japanese animation entries only.
    
    Returns list of {tvdb_id, name, english_name, type, year, aliases, ...} dicts.
    """
    entries = []
    seen_ids = set()

    # Build search terms: full name, components, user-provided extras
    search_terms = [franchise_name]
    if '/' in franchise_name:
        prefix = franchise_name.split('/')[0]
        if len(prefix) >= 3:
            search_terms.append(prefix)
    # Split on colon for subtitle searches
    if ':' in franchise_name:
        pre = franchise_name.split(':')[0].strip()
        if len(pre) >= 3:
            search_terms.append(pre)
    if extra_search_terms:
        search_terms.extend(extra_search_terms)

    # Dedupe search terms
    search_terms = list(dict.fromkeys(search_terms))

    def _process_results(results, entry_type, id_prefix="series-"):
        for r in results:
            tvdb_id = _extract_tvdb_id(r, prefix=id_prefix)
            if not tvdb_id or tvdb_id in seen_ids:
                continue

            # Collect all names: primary + aliases from search result
            primary_name = r.get("name", "")
            aliases = r.get("aliases", []) or []
            if isinstance(aliases, list) and aliases and isinstance(aliases[0], str):
                all_names = [primary_name] + aliases
            else:
                all_names = [primary_name]

            # Also add translations if available
            translations = r.get("translations", {})
            if isinstance(translations, dict):
                eng = translations.get("eng")
                if eng:
                    all_names.append(eng)

            # Also try slug → readable name
            slug = r.get("slug", "")
            if slug:
                all_names.append(slug.replace("-", " "))

            # Anime filter: check country=jpn or genres contains "Anime"/"Animation"
            if anime_only:
                country = (r.get("country") or "").lower()
                genres = r.get("genres") or []
                primary_lang = (r.get("primary_language") or "").lower()
                is_anime = (
                    country == "jpn" or
                    primary_lang == "jpn" or
                    any(g.lower() in ("anime", "animation") for g in genres)
                )
                if not is_anime:
                    continue

            # Check franchise membership against all names
            if _belongs_to_franchise(all_names, franchise_name):
                # Fetch English translation for display name
                eng_name = _get_english_name(tvdb_id, entry_type)
                display_name = eng_name or primary_name
                
                entries.append({
                    "tvdb_id": tvdb_id,
                    "name": display_name,
                    "original_name": primary_name,
                    "type": entry_type,
                    "year": r.get("year", ""),
                    "status": r.get("status", ""),
                    "aliases": all_names,
                })
                seen_ids.add(tvdb_id)

    # Search series
    for term in search_terms:
        results = tvdb_search_series(term, series_type="series")
        _process_results(results, "series")

    # Search movies
    if include_movies:
        for term in search_terms:
            try:
                results = tvdb_get("/search", {"query": term, "type": "movie"}).get("data", [])
            except Exception:
                results = []
            _process_results(results, "movie", id_prefix="movie-")

    return entries


def _get_english_name(tvdb_id: int, entry_type: str) -> str | None:
    """Fetch English translation name from TVDB."""
    try:
        endpoint = "series" if entry_type == "series" else "movies"
        resp = tvdb_get(f"/{endpoint}/{tvdb_id}/translations/eng")
        return resp.get("data", {}).get("name")
    except Exception:
        return None


def _extract_tvdb_id(result: dict, prefix: str = "series-") -> int | None:
    """Extract numeric TVDB ID from search result."""
    raw = result.get("tvdb_id", result.get("id", ""))
    if isinstance(raw, int):
        return raw
    raw = str(raw).replace(prefix, "").replace("series-", "").replace("movie-", "")
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None


def _belongs_to_franchise(all_names: list[str], franchise_name: str) -> bool:
    """
    Check if a TVDB entry (given all its name variants) belongs to the franchise.
    Uses rapidfuzz for flexible matching. Any name variant matching is enough.
    """
    f_lower = franchise_name.lower()
    f_norm = f_lower.replace('/', ' ').replace(':', ' ').replace('-', ' ')
    f_words = [w for w in f_norm.split() if len(w) > 2]
    if not f_words:
        return False

    # Slash-brand prefix (Fate/, Sword Art Online/)
    brand_prefix = None
    if '/' in franchise_name:
        brand_prefix = franchise_name.split('/')[0].lower()

    for name in all_names:
        if not name:
            continue
        t_norm = name.lower().replace('/', ' ').replace(':', ' ').replace('-', ' ')

        # Rapidfuzz token sort ratio against franchise name
        score = _ratio(franchise_name, name)
        if score >= 70:
            return True

        # Brand prefix match: require the slash pattern (Fate/ → matches "Fate/Zero")
        # Check against original name, not normalized, to preserve slash
        if brand_prefix:
            name_lower = name.lower()
            if f'{brand_prefix}/' in name_lower or f'{brand_prefix} /' in name_lower:
                return True

        # Keyword containment as a contiguous phrase
        # "demon slayer" must appear as a substring, not "slayer...demons"
        franchise_phrase = ' '.join(f_words)
        if franchise_phrase in t_norm:
            return True

        # For slash-brands that got normalized: "fate stay night" in name
        if '/' in franchise_name:
            slash_phrase = f_lower.replace('/', ' ')
            slash_words = ' '.join(w for w in slash_phrase.split() if len(w) > 2)
            if slash_words and slash_words in t_norm:
                return True

    return False


# ── AniList Matching ──────────────────────────────────────────────────────

ANILIST_SEARCH_QUERY = """
query ($search: String, $page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
      id
      idMal
      title { romaji english native }
      synonyms
      format
      status
      episodes
      startDate { year month day }
      endDate { year month day }
      season
      seasonYear
    }
  }
}
"""

ANILIST_DETAIL_QUERY = """
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


def match_tvdb_to_anilist(tvdb_entry: dict, tvdb_type: str) -> dict | None:
    """
    Find the best AniList match for a TVDB entry using rapidfuzz + release date.
    
    Strategy:
      1. Search AniList by TVDB title
      2. Score each result: rapidfuzz title similarity + date proximity bonus
      3. Accept if score >= 85 (high confidence) or >= 70 with matching year
      4. Return best match or None
    """
    tvdb_name = tvdb_entry.get("name", "")
    tvdb_year = str(tvdb_entry.get("year", ""))[:4]
    tvdb_first_aired = tvdb_entry.get("firstAired", "")

    if not tvdb_name:
        return None

    # Search AniList with a few title variants
    search_titles = [tvdb_name]
    # Strip JP chars if mixed — AniList English search is better
    import re
    ascii_only = re.sub(r'[^\x00-\x7F]+', '', tvdb_name).strip()
    if ascii_only and ascii_only != tvdb_name and len(ascii_only) > 5:
        search_titles.append(ascii_only)

    all_candidates = []
    for search_title in search_titles:
        try:
            resp = anilist_request(ANILIST_SEARCH_QUERY, {
                "search": search_title, "page": 1, "perPage": 10
            })
            media = resp.get("data", {}).get("Page", {}).get("media", [])
            all_candidates.extend(media)
        except Exception as e:
            print(f"    ⚠ AniList search failed for '{search_title}': {e}")

    if not all_candidates:
        return None

    # Deduplicate by AniList ID
    seen = set()
    candidates = []
    for c in all_candidates:
        if c["id"] not in seen:
            seen.add(c["id"])
            candidates.append(c)

    # Score each candidate
    best = None
    best_score = 0

    for cand in candidates:
        # Collect all title variants from AniList
        titles = []
        t = cand.get("title", {})
        for key in ("english", "romaji", "native"):
            if t.get(key):
                titles.append(t[key])
        titles.extend(cand.get("synonyms", []) or [])

        # Best title similarity against TVDB name
        title_score = max((_ratio(tvdb_name, t) for t in titles if t), default=0)

        # Year/date bonus
        cand_start = _format_date(cand.get("startDate"))
        cand_year = str(cand.get("seasonYear") or (cand.get("startDate", {}) or {}).get("year", ""))

        year_match = (tvdb_year and cand_year and tvdb_year == cand_year)
        date_close = dates_match(tvdb_first_aired, cand_start, tolerance_days=90) if tvdb_first_aired else False

        # Format compatibility check
        cand_format = cand.get("format", "")
        format_ok = True
        if tvdb_type == "series" and cand_format in ("MOVIE",):
            format_ok = False
        if tvdb_type == "movie" and cand_format in ("TV", "TV_SHORT"):
            format_ok = False

        # Compute effective score
        effective = title_score
        if year_match:
            effective += 8
        if date_close:
            effective += 5
        if not format_ok:
            effective -= 20

        if effective > best_score:
            best_score = effective
            best = cand

    if not best:
        return None

    # Acceptance thresholds
    # High confidence: title alone is strong enough
    if best_score >= 85:
        return best
    # Medium confidence: decent title + year match
    if best_score >= 70:
        return best
    # Low confidence: log and skip
    best_title = best.get("title", {}).get("english") or best.get("title", {}).get("romaji", "?")
    print(f"    ⚠ Low confidence match ({best_score:.0f}): '{tvdb_name}' → '{best_title}' — skipping")
    return None


def fetch_anilist_detail(anilist_id: int) -> dict | None:
    """Fetch full AniList detail for a matched anime."""
    try:
        resp = anilist_request(ANILIST_DETAIL_QUERY, {"id": anilist_id})
        return resp.get("data", {}).get("Media")
    except Exception as e:
        print(f"    ⚠ AniList detail fetch failed for {anilist_id}: {e}")
        return None


# ── Season-Level AniList Matching ─────────────────────────────────────────

def match_tvdb_seasons_to_anilist(tvdb_series: dict, franchise_name: str) -> list[dict]:
    """
    For a TVDB series with multiple seasons, find matching AniList entries per season.
    
    NEW MATCHING STRATEGY (v3 - Feb 2026 - Multi-Match):
    - Match by FRANCHISE name (not season name - TVDB season names are often empty!)
    - Calculate TVDB season date range from ALL episode air dates (earliest → latest)
    - Check if AniList entry date range OVERLAPS with TVDB season date range
    - ALLOW MULTIPLE AniList entries to match a single TVDB season (multi-cour support)
    - Example: AoT S4 "The Final Season" (2020-2023) matches 3 AniList entries (Part 1, Part 2, Final Chapters)
    
    Returns list of {season_number, tvdb_season, anilist_matches: [...], season_name_en, season_name_jp} dicts.
    """
    from datetime import datetime, timedelta
    
    seasons = tvdb_series.get("seasons", [])
    # Filter to aired-order seasons (type.id == 1), skip specials (S0)
    aired_seasons = [
        s for s in seasons
        if s.get("type", {}).get("id") == 1 and s.get("number", 0) > 0
    ]

    if not aired_seasons:
        return []

    # Build episode lookup by season number
    # Episodes are stored flat in tvdb_series["episodes"] with seasonNumber field
    all_episodes = tvdb_series.get("episodes", [])
    episodes_by_season = {}
    for ep in all_episodes:
        s_num = ep.get("seasonNumber", 0)
        if s_num not in episodes_by_season:
            episodes_by_season[s_num] = []
        episodes_by_season[s_num].append(ep)

    series_name = tvdb_series.get("name", franchise_name)
    results = []

    # Build a pool of AniList candidates by searching FRANCHISE NAME (not series name!)
    # This ensures we're matching within the same franchise
    try:
        resp = anilist_request(ANILIST_SEARCH_QUERY, {
            "search": franchise_name, "page": 1, "perPage": 30
        })
        al_pool = resp.get("data", {}).get("Page", {}).get("media", [])
    except Exception:
        al_pool = []

    # Also search by series name if different (to catch renamed series)
    if series_name.lower() != franchise_name.lower():
        try:
            resp2 = anilist_request(ANILIST_SEARCH_QUERY, {
                "search": series_name, "page": 1, "perPage": 20
            })
            al_pool.extend(resp2.get("data", {}).get("Page", {}).get("media", []))
        except Exception:
            pass

    # Dedupe and filter to TV/SPECIAL format (SPECIAL can include final chapters/compilation)
    seen = set()
    pool = []
    for c in al_pool:
        if c["id"] not in seen:
            seen.add(c["id"])
            # Consider TV, TV_SHORT, and SPECIAL (for final chapters, OVAs that are part of main story)
            # Exclude MOVIE, OVA, ONA formats as those are usually separate entries
            if c.get("format") in ("TV", "TV_SHORT", "SPECIAL", None):
                pool.append(c)

    # Sort pool by start date to help with chronological matching
    pool.sort(key=lambda x: (
        x.get("startDate", {}).get("year") or 9999,
        x.get("startDate", {}).get("month") or 99,
        x.get("startDate", {}).get("day") or 99,
    ))

    # Match each TVDB season to ALL matching AniList entries (multi-match!)
    # REMOVED: used_al_ids set — we now allow reuse across seasons

    for tvdb_season in aired_seasons:
        s_num = tvdb_season.get("number", 0)
        s_name_en = tvdb_season.get("name", "")  # English name (often empty)
        s_year = tvdb_season.get("year", "")
        
        # Get episodes for this season from the lookup
        s_episodes = episodes_by_season.get(s_num, [])
        s_episode_count = len(s_episodes)
        
        # Calculate TVDB season date range from ALL episode air dates
        s_start_date = None
        s_end_date = None
        if s_episodes:
            episode_dates = []
            for ep in s_episodes:
                aired = ep.get("aired", "")
                if aired:
                    try:
                        ep_date = datetime.fromisoformat(aired[:10])
                        episode_dates.append(ep_date)
                    except Exception:
                        pass
            
            if episode_dates:
                s_start_date = min(episode_dates)
                s_end_date = max(episode_dates)
        
        # Try to get Japanese name from TVDB translations
        s_name_jp = ""
        tvdb_season_id = tvdb_season.get("id", "")
        if tvdb_season_id:
            try:
                trans_resp = tvdb_get(f"/seasons/{tvdb_season_id}/translations/jpn")
                s_name_jp = trans_resp.get("data", {}).get("name", "")
            except Exception:
                pass

        # Find ALL matching AniList entries (multi-match!)
        anilist_matches = []

        for cand in pool:
            # Parse AniList dates
            cand_start = _format_date(cand.get("startDate"))
            cand_end = _format_date(cand.get("endDate"))
            
            if not cand_start:
                continue  # Can't match without start date
            
            try:
                al_start = datetime.fromisoformat(cand_start[:10])
                # If no end date, assume it's still airing or use start date
                al_end = datetime.fromisoformat(cand_end[:10]) if cand_end else al_start
            except Exception:
                continue
            
            # Check date range overlap with tolerance (±30 days on each end)
            tolerance = timedelta(days=30)
            
            # Skip if we don't have TVDB dates
            if not s_start_date or not s_end_date:
                continue
            
            # Date range overlap check:
            # AniList range: [al_start, al_end]
            # TVDB range: [s_start_date, s_end_date]
            # Overlap if: al_start <= s_end_date + tolerance AND al_end >= s_start_date - tolerance
            overlap = (al_start <= s_end_date + tolerance) and (al_end >= s_start_date - tolerance)
            
            if not overlap:
                continue
            
            # Franchise name check (verify we're in the same franchise)
            # For SPECIAL format, use more lenient matching (SPECIALs often have long descriptive titles)
            titles = []
            t = cand.get("title", {})
            for key in ("english", "romaji", "native"):
                if t.get(key):
                    titles.append(t[key])
            titles.extend(cand.get("synonyms", []) or [])
            
            # Check if franchise name appears as substring (for long special titles)
            franchise_lower = franchise_name.lower()
            substring_match = any(franchise_lower in (title or "").lower() for title in titles)
            
            # OR check fuzzy score (stricter for TV, more lenient for SPECIAL)
            fuzzy_threshold = 40 if cand.get("format") == "SPECIAL" else 60
            fuzzy_match = max((_ratio(franchise_name, title) for title in titles if title), default=0) >= fuzzy_threshold
            
            franchise_match = substring_match or fuzzy_match
            
            if not franchise_match:
                continue  # Must be in same franchise
            
            # Calculate match score based on date overlap quality and episode count
            cand_eps = cand.get("episodes") or 0
            ep_diff = abs(s_episode_count - cand_eps) if s_episode_count and cand_eps else 999
            
            # Overlap days
            overlap_start = max(al_start, s_start_date)
            overlap_end = min(al_end, s_end_date)
            overlap_days = (overlap_end - overlap_start).days
            
            # For single-day events (like movie/special compilations), treat as if they match if within range
            is_single_day = (al_start == al_end)
            within_range = (s_start_date <= al_start <= s_end_date)
            
            # Score: prioritize strong date overlap
            score = 50  # Base for passing overlap check
            
            # Bonus for longer overlap (but skip for single-day events within range)
            if is_single_day and within_range:
                score += 15  # Single-day special within season date range
            elif overlap_days > 30:
                score += 20
            elif overlap_days > 7:
                score += 10
            
            # Bonus for episode count closeness (but be lenient for SPECIAL format)
            if cand.get("format") == "SPECIAL" and cand_eps <= 2:
                # Short specials/movies are expected to have low episode counts
                score += 5
            elif ep_diff <= 2:
                score += 15
            elif ep_diff <= 5:
                score += 5
            
            # Accept if score is reasonable (lower threshold for specials)
            threshold = 60 if cand.get("format") != "SPECIAL" else 55
            if score >= threshold:
                anilist_matches.append({
                    "anilist_id": cand["id"],
                    "mal_id": cand.get("idMal"),
                    "title": cand.get("title", {}).get("english") or cand.get("title", {}).get("romaji", "?"),
                    "episodes": cand_eps,
                    "start_date": cand_start,
                    "end_date": cand_end,
                    "match_score": score,
                    "overlap_days": overlap_days,
                })
        
        # Sort matches by start date (chronological)
        anilist_matches.sort(key=lambda m: m["start_date"])
        
        # Log matches
        if anilist_matches:
            print(f"    S{s_num} → {len(anilist_matches)} AniList match(es):")
            for m in anilist_matches:
                print(f"      • {m['title']} (eps:{m['episodes']}, {m['start_date'][:10]}→{m['end_date'][:10] if m['end_date'] else '?'}, AL:{m['anilist_id']}, score:{m['match_score']:.0f})")
        else:
            print(f"    S{s_num} → No AniList matches (date range: {s_start_date.strftime('%Y-%m-%d') if s_start_date else '?'}→{s_end_date.strftime('%Y-%m-%d') if s_end_date else '?'})")
        
        results.append({
            "season_number": s_num,
            "tvdb_season": tvdb_season,
            "anilist_matches": anilist_matches,  # NOW A LIST!
            "season_name_en": s_name_en,  # Metadata only
            "season_name_jp": s_name_jp,  # Metadata only
            "tvdb_date_range": {
                "start": s_start_date.strftime("%Y-%m-%d") if s_start_date else None,
                "end": s_end_date.strftime("%Y-%m-%d") if s_end_date else None,
            }
        })

    return results


# ── Jikan/MAL Fetch ───────────────────────────────────────────────────────

def fetch_jikan_by_mal_id(mal_id: int) -> dict | None:
    """Fetch Jikan data by MAL ID."""
    if not mal_id:
        return None
    try:
        resp = jikan_request(f"/anime/{mal_id}/full")
        return resp.get("data")
    except Exception as e:
        print(f"    ⚠ Jikan fetch failed for MAL:{mal_id}: {e}")
        return None


def fetch_jikan_characters(mal_id: int) -> list | None:
    """Fetch character list from Jikan."""
    if not mal_id:
        return None
    try:
        resp = jikan_request(f"/anime/{mal_id}/characters")
        return resp.get("data", [])
    except Exception as e:
        print(f"    ⚠ Jikan characters failed for MAL:{mal_id}: {e}")
        return None


def fetch_jikan_staff(mal_id: int) -> list | None:
    """Fetch staff list from Jikan."""
    if not mal_id:
        return None
    try:
        resp = jikan_request(f"/anime/{mal_id}/staff")
        return resp.get("data", [])
    except Exception as e:
        print(f"    ⚠ Jikan staff failed for MAL:{mal_id}: {e}")
        return None


# ── Entry Save Logic ──────────────────────────────────────────────────────

def save_entry_data(franchise_slug: str, entry_slug: str, tvdb_data: dict | None,
                    anilist_data: dict | None, jikan_data: dict | None,
                    jikan_chars: list | None, jikan_staff: list | None,
                    entry_type: str = "anime-series",
                    season_matches: list | None = None):
    """Save all source data for an entry."""
    base = entry_dir(franchise_slug, entry_slug)
    sources = os.path.join(base, "sources")
    ensure_dir(sources)

    # TVDB
    if tvdb_data:
        save_tvdb_source(tvdb_data, franchise_slug, entry_slug)

    # AniList (entry-level — from the first/primary match)
    if anilist_data:
        save_anime_source(anilist_data, franchise_slug, entry_slug)

    # Jikan (entry-level)
    if jikan_data:
        save_jikan_anime_source(jikan_data, jikan_chars or [], jikan_staff or [],
                                franchise_slug, entry_slug)

    # Season-level AniList/Jikan data (v3: multi-match support!)
    if season_matches:
        seasons_manifest = []
        for sm in season_matches:
            s_num = sm["season_number"]
            al_matches = sm.get("anilist_matches", [])  # NOW A LIST!
            
            season_entry = {
                "season_number": s_num,
                "tvdb_season_id": sm["tvdb_season"].get("id", ""),
                "tvdb_season_name_en": sm.get("season_name_en", ""),
                "tvdb_season_name_jp": sm.get("season_name_jp", ""),
                "tvdb_date_range_start": sm.get("tvdb_date_range", {}).get("start", ""),
                "tvdb_date_range_end": sm.get("tvdb_date_range", {}).get("end", ""),
                "anilist_matches": [],  # List of all matches
            }
            
            if not al_matches:
                # No matches for this season
                seasons_manifest.append(season_entry)
                continue
            
            # Process each AniList match for this season
            for idx, al in enumerate(al_matches):
                # Save season-level AniList data
                # Use index suffix for multiple matches: anilist_s4_0, anilist_s4_1, etc.
                season_dir = os.path.join(base, "_season_sources", f"anilist_s{s_num}_{idx}", "sources")
                ensure_dir(season_dir)

                # Fetch full detail for this season's AniList entry
                al_id = al.get("anilist_id")
                al_detail = fetch_anilist_detail(al_id)
                if al_detail:
                    save_anime_source(al_detail, franchise_slug, entry_slug)
                    # Also save to season-specific dir
                    save_json(os.path.join(season_dir, "anilist_raw.json"), al_detail)

                # Fetch Jikan for this season
                mal_id = al.get("mal_id")
                if mal_id:
                    jk = fetch_jikan_by_mal_id(mal_id)
                    jk_chars = fetch_jikan_characters(mal_id)
                    jk_staff = fetch_jikan_staff(mal_id)
                    jk_dir = os.path.join(base, "_season_sources", f"jikan_s{s_num}_{idx}", "sources")
                    ensure_dir(jk_dir)
                    if jk:
                        save_json(os.path.join(jk_dir, "jikan_raw.json"), jk)
                
                # Add to season entry's match list
                season_entry["anilist_matches"].append({
                    "anilist_id": al.get("anilist_id"),
                    "mal_id": al.get("mal_id", ""),
                    "title": al.get("title", ""),
                    "episodes": al.get("episodes", ""),
                    "start_date": al.get("start_date", ""),
                    "end_date": al.get("end_date", ""),
                    "match_score": al.get("match_score", 0),
                    "overlap_days": al.get("overlap_days", 0),
                })
            
            seasons_manifest.append(season_entry)

        # Save seasons manifest
        if seasons_manifest:
            toon.save(os.path.join(sources, "seasons.toon"), "seasons", seasons_manifest)

    # Save entry type marker
    save_json(os.path.join(base, "entry_meta.json"), {
        "entry_type": entry_type,
        "tvdb_id": tvdb_data.get("id") if tvdb_data else None,
        "anilist_id": anilist_data.get("id") if anilist_data else None,
        "mal_id": anilist_data.get("idMal") if anilist_data else None,
    })


# ── Main Pipeline ─────────────────────────────────────────────────────────

def fetch_franchise(franchise_name: str, include_movies: bool = True,
                    franchise_slug: str | None = None,
                    extra_search_terms: list[str] | None = None,
                    anime_only: bool = True):
    """
    Full TVDB-backbone franchise fetch.
    
    1. Discover all TVDB entries (series + movies)
    2. For each entry: fetch TVDB extended → match AniList → fetch enrichment
    3. Save organized data
    """
    if not franchise_slug:
        franchise_slug = slugify(franchise_name)

    print(f"🔍 Discovering TVDB entries for: {franchise_name}")
    print(f"   Franchise slug: {franchise_slug}")
    print(f"   Fuzzy engine: {FUZZY_ENGINE}")
    print()

    # Step 1: TVDB Discovery
    tvdb_entries = discover_tvdb_entries(franchise_name, include_movies=include_movies,
                                         extra_search_terms=extra_search_terms,
                                         anime_only=anime_only)

    if not tvdb_entries:
        print("❌ No TVDB entries found!")
        return

    print(f"📺 Found {len(tvdb_entries)} TVDB entries:")
    for e in tvdb_entries:
        icon = "📺" if e["type"] == "series" else "🎬"
        print(f"  {icon} {e['name']} ({e['year']}) [TVDB:{e['tvdb_id']}]")
    print()

    # Step 2: Process each entry
    manifest_entries = []
    used_slugs = set()  # Track slugs to detect collisions

    for i, tvdb_entry in enumerate(tvdb_entries, 1):
        tvdb_id = tvdb_entry["tvdb_id"]
        tvdb_name = tvdb_entry["name"]
        tvdb_type = tvdb_entry["type"]
        entry_slug = slugify(tvdb_name)

        print(f"[{i}/{len(tvdb_entries)}] {tvdb_name}")
        print(f"  Slug: {entry_slug}")

        # Fetch TVDB extended data
        try:
            if tvdb_type == "series":
                tvdb_data = tvdb_get_series_extended(tvdb_id)
            else:
                resp = tvdb_get(f"/movies/{tvdb_id}/extended")
                tvdb_data = resp.get("data", {})
        except Exception as e:
            print(f"  ⚠ TVDB fetch failed: {e}")
            tvdb_data = None

        # Add firstAired to entry for matching
        if tvdb_data:
            tvdb_entry["firstAired"] = tvdb_data.get("firstAired", "")

        # Match to AniList
        print(f"  🔗 Matching to AniList...")
        al_match = match_tvdb_to_anilist(tvdb_entry, tvdb_type)

        anilist_data = None
        jikan_data = None
        jikan_chars = None
        jikan_staff = None
        season_matches = None

        if al_match:
            al_title = al_match.get("title", {}).get("english") or al_match.get("title", {}).get("romaji", "?")
            print(f"  ✓ AniList match: {al_title} (AL:{al_match['id']}, MAL:{al_match.get('idMal', '?')})")

            # Fetch full AniList detail
            anilist_data = fetch_anilist_detail(al_match["id"])

            # Fetch Jikan/MAL
            mal_id = al_match.get("idMal")
            if mal_id:
                jikan_data = fetch_jikan_by_mal_id(mal_id)
                jikan_chars = fetch_jikan_characters(mal_id)
                jikan_staff = fetch_jikan_staff(mal_id)
                print(f"  ✓ Jikan data fetched (MAL:{mal_id})")

            # For TV series: match seasons to AniList entries
            if tvdb_type == "series" and tvdb_data:
                print(f"  🔗 Matching seasons...")
                season_matches = match_tvdb_seasons_to_anilist(tvdb_data, franchise_name)
        else:
            print(f"  ⚠ No AniList match found")

        # Determine entry type
        if tvdb_type == "series":
            entry_type = "anime-series"
        else:
            # Check AniList format for more specificity
            al_format = (anilist_data or {}).get("format", "MOVIE")
            if al_format == "OVA":
                entry_type = "anime-ova"
            elif al_format == "ONA":
                entry_type = "anime-ona"
            elif al_format in ("SPECIAL",):
                entry_type = "anime-special"
            else:
                entry_type = "anime-movie"

        # Detect slug collision and append suffix
        if entry_slug in used_slugs:
            suffix_map = {
                "anime-series": "-tv",
                "anime-movie": "-movie",
                "anime-ova": "-ova",
                "anime-ona": "-ona",
                "anime-special": "-special",
            }
            suffix = suffix_map.get(entry_type, "-entry")
            original_slug = entry_slug
            entry_slug = f"{entry_slug}{suffix}"
            print(f"  ⚠ Slug collision detected! Renamed: {original_slug} → {entry_slug}")
        
        used_slugs.add(entry_slug)

        # Save
        save_entry_data(
            franchise_slug, entry_slug, tvdb_data, anilist_data,
            jikan_data, jikan_chars, jikan_staff,
            entry_type=entry_type, season_matches=season_matches,
        )

        # Count total AniList matches across all seasons (v3: multi-match)
        total_al_matches = sum(len(s.get("anilist_matches", [])) for s in (season_matches or []))
        seasons_with_matches = len([s for s in (season_matches or []) if s.get("anilist_matches")])
        
        manifest_entries.append({
            "slug": entry_slug,
            "name": tvdb_name,
            "type": entry_type,
            "tvdb_id": tvdb_id,
            "tvdb_type": tvdb_type,
            "anilist_id": al_match["id"] if al_match else None,
            "mal_id": al_match.get("idMal") if al_match else None,
            "year": tvdb_entry.get("year", ""),
            "seasons_with_matches": seasons_with_matches,
            "seasons_total": len(season_matches or []),
            "total_anilist_matches": total_al_matches,  # NEW: total AniList entries matched
        })

        print(f"  ✓ Saved to franchises/{franchise_slug}/entries/{entry_slug}/")
        print()

    # Save franchise manifest
    manifest = {
        "franchise": franchise_name,
        "slug": franchise_slug,
        "backbone": "tvdb",
        "entry_count": len(manifest_entries),
        "entries": manifest_entries,
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    manifest_path = os.path.join(franchise_dir(franchise_slug), "manifest.json")
    save_json(manifest_path, manifest)

    # Summary
    matched = sum(1 for e in manifest_entries if e.get("anilist_id"))
    print(f"✅ Done! {franchise_name}")
    print(f"   {len(manifest_entries)} entries ({matched} with AniList matches)")
    print(f"   Saved to franchises/{franchise_slug}/")


def main():
    parser = argparse.ArgumentParser(description="TVDB-backbone franchise fetcher")
    parser.add_argument("franchise", help="Franchise name (e.g., 'Demon Slayer')")
    parser.add_argument("--slug", help="Override franchise slug")
    parser.add_argument("--no-movies", action="store_true", help="Skip movie search")
    parser.add_argument("--extra-search", "-e", nargs="+", help="Additional TVDB search terms")
    parser.add_argument("--no-anime-filter", action="store_true", help="Don't filter to anime only")
    args = parser.parse_args()

    fetch_franchise(
        args.franchise,
        include_movies=not args.no_movies,
        franchise_slug=args.slug,
        extra_search_terms=args.extra_search,
        anime_only=not args.no_anime_filter,
    )


if __name__ == "__main__":
    main()
