#!/usr/bin/env python3
"""
Extract entities (characters, staff/creators, companies) from franchise raw data.
Cross-references between AniList and MAL - entities must appear in BOTH sources.
"""
import os
import sys
import json
import unicodedata
from pathlib import Path
from utils import slugify, load_json, save_json, franchise_dir, entry_dir


def normalize_name(name: str) -> str:
    """Normalize name for matching: lowercase, remove accents, strip whitespace."""
    # Handle None
    if not name:
        return ""
    
    # Unicode normalization to remove accents
    name = unicodedata.normalize('NFKD', name)
    name = name.encode('ASCII', 'ignore').decode('ASCII')
    
    # Lowercase and strip
    name = name.lower().strip()
    
    # Remove extra whitespace
    name = ' '.join(name.split())
    
    # Remove common punctuation variations
    name = name.replace(',', '').replace('.', '')
    
    return name


def match_names(name1: str, name2: str) -> bool:
    """Check if two names match after normalization."""
    return normalize_name(name1) == normalize_name(name2)


def extract_characters(anilist_data: dict, jikan_data: dict, franchise_slug: str, entry_slug: str) -> dict:
    """
    Extract characters that appear in BOTH AniList and MAL.
    Returns dict: {character_slug: character_data}
    """
    characters = {}
    
    # Get AniList characters
    anilist_chars = {}
    if 'characters' in anilist_data and 'edges' in anilist_data['characters']:
        for edge in anilist_data['characters']['edges']:
            node = edge.get('node', {})
            char_id = node.get('id')
            if char_id:
                anilist_chars[char_id] = {
                    'id': char_id,
                    'name': node.get('name', {}).get('full', ''),
                    'native': node.get('name', {}).get('native', ''),
                    'role': edge.get('role', 'SUPPORTING'),
                    'image': node.get('image', {}).get('large', ''),
                    'voice_actors': []
                }
                
                # Extract voice actors
                for va in edge.get('voiceActors', []):
                    anilist_chars[char_id]['voice_actors'].append({
                        'id': va.get('id'),
                        'name': va.get('name', {}).get('full', ''),
                        'native': va.get('name', {}).get('native', ''),
                        'image': va.get('image', {}).get('large', '')
                    })
    
    # Get MAL characters and cross-reference
    if 'characters' in jikan_data:
        for char_data in jikan_data['characters']:
            char = char_data.get('character', {})
            mal_id = char.get('mal_id')
            
            if not mal_id:
                continue
            
            # Check if this character exists in AniList data
            # Characters should have matching IDs between AniList and MAL
            if mal_id in anilist_chars:
                anilist_char = anilist_chars[mal_id]
                char_name = anilist_char['name'] or char.get('name', '')
                char_slug = slugify(char_name)
                
                if not char_slug:
                    continue
                
                # Extract MAL voice actors (Japanese only for cross-reference)
                mal_voice_actors = []
                for va in char_data.get('voice_actors', []):
                    if va.get('language') == 'Japanese':
                        person = va.get('person', {})
                        mal_voice_actors.append({
                            'mal_id': person.get('mal_id'),
                            'name': person.get('name', ''),
                            'image': person.get('images', {}).get('jpg', {}).get('image_url', '')
                        })
                
                # Cross-reference voice actors by name
                matched_vas = []
                for anilist_va in anilist_char['voice_actors']:
                    anilist_va_name = anilist_va['name']
                    for mal_va in mal_voice_actors:
                        mal_va_name = mal_va['name']
                        # MAL names are "Last, First" format - try both ways
                        mal_va_reversed = ' '.join(reversed(mal_va_name.split(', ')))
                        if match_names(anilist_va_name, mal_va_name) or match_names(anilist_va_name, mal_va_reversed):
                            matched_vas.append({
                                'anilist_id': anilist_va['id'],
                                'mal_id': mal_va['mal_id'],
                                'name': anilist_va['name'],
                                'native': anilist_va['native'],
                                'anilist_image': anilist_va['image'],
                                'mal_image': mal_va['image']
                            })
                            break
                
                characters[char_slug] = {
                    'name': char_name,
                    'slug': char_slug,
                    'anilist_id': mal_id,
                    'mal_id': mal_id,
                    'native_name': anilist_char['native'],
                    'role': anilist_char['role'],
                    'anilist_image': anilist_char['image'],
                    'mal_image': char.get('images', {}).get('jpg', {}).get('image_url', ''),
                    'voice_actors': matched_vas,
                    'franchises': [{
                        'franchise_slug': franchise_slug,
                        'entry_slug': entry_slug
                    }]
                }
    
    return characters


def extract_staff(anilist_data: dict, jikan_data: dict, franchise_slug: str, entry_slug: str) -> dict:
    """
    Extract staff/creators that appear in BOTH AniList and MAL.
    Returns dict: {creator_slug: creator_data}
    """
    creators = {}
    
    # Get AniList staff
    anilist_staff = {}
    if 'staff' in anilist_data and 'edges' in anilist_data['staff']:
        for edge in anilist_data['staff']['edges']:
            node = edge.get('node', {})
            staff_id = node.get('id')
            staff_name = node.get('name', {}).get('full', '')
            if staff_id and staff_name:
                normalized = normalize_name(staff_name)
                if normalized not in anilist_staff:
                    anilist_staff[normalized] = []
                anilist_staff[normalized].append({
                    'id': staff_id,
                    'name': staff_name,
                    'native': node.get('name', {}).get('native', ''),
                    'role': edge.get('role', ''),
                    'image': node.get('image', {}).get('large', '')
                })
    
    # Get MAL staff and cross-reference by name
    if 'staff' in jikan_data:
        for staff_data in jikan_data['staff']:
            person = staff_data.get('person', {})
            mal_id = person.get('mal_id')
            mal_name = person.get('name', '')
            
            if not mal_id or not mal_name:
                continue
            
            # Try to match with AniList staff
            normalized_mal = normalize_name(mal_name)
            # Also try reversed name (MAL uses "Last, First")
            mal_name_reversed = ' '.join(reversed(mal_name.split(', ')))
            normalized_mal_reversed = normalize_name(mal_name_reversed)
            
            matched_anilist = None
            if normalized_mal in anilist_staff:
                matched_anilist = anilist_staff[normalized_mal][0]
            elif normalized_mal_reversed in anilist_staff:
                matched_anilist = anilist_staff[normalized_mal_reversed][0]
            
            if matched_anilist:
                staff_name = matched_anilist['name']
                staff_slug = slugify(staff_name)
                
                if not staff_slug:
                    continue
                
                # Combine roles from both sources
                roles = []
                if matched_anilist['role']:
                    roles.append(matched_anilist['role'])
                for position in staff_data.get('positions', []):
                    if position and position not in roles:
                        roles.append(position)
                
                if staff_slug not in creators:
                    creators[staff_slug] = {
                        'name': staff_name,
                        'slug': staff_slug,
                        'anilist_id': matched_anilist['id'],
                        'mal_id': mal_id,
                        'native_name': matched_anilist['native'],
                        'anilist_image': matched_anilist['image'],
                        'mal_image': person.get('images', {}).get('jpg', {}).get('image_url', ''),
                        'roles': roles,
                        'franchises': []
                    }
                
                # Add franchise/entry reference
                franchise_ref = {
                    'franchise_slug': franchise_slug,
                    'entry_slug': entry_slug,
                    'roles': roles
                }
                if franchise_ref not in creators[staff_slug]['franchises']:
                    creators[staff_slug]['franchises'].append(franchise_ref)
    
    return creators


def extract_companies(anilist_data: dict, jikan_data: dict, franchise_slug: str, entry_slug: str) -> dict:
    """
    Extract companies (studios) that appear in BOTH AniList and MAL.
    Returns dict: {company_slug: company_data}
    """
    companies = {}
    
    # Get AniList studios
    anilist_studios = {}
    if 'studios' in anilist_data and 'nodes' in anilist_data['studios']:
        for node in anilist_data['studios']['nodes']:
            studio_id = node.get('id')
            studio_name = node.get('name', '')
            if studio_id and studio_name:
                normalized = normalize_name(studio_name)
                anilist_studios[normalized] = {
                    'id': studio_id,
                    'name': studio_name,
                    'is_animation_studio': node.get('isAnimationStudio', True)
                }
    
    # Get MAL studios and cross-reference by name
    mal_studios = []
    if 'anime' in jikan_data:
        anime = jikan_data['anime']
        if 'studios' in anime:
            for studio in anime['studios']:
                mal_studios.append({
                    'mal_id': studio.get('mal_id'),
                    'name': studio.get('name', ''),
                    'type': 'studio'
                })
        if 'producers' in anime:
            for producer in anime['producers']:
                mal_studios.append({
                    'mal_id': producer.get('mal_id'),
                    'name': producer.get('name', ''),
                    'type': 'producer'
                })
    
    # Cross-reference
    for mal_studio in mal_studios:
        mal_name = mal_studio['name']
        mal_id = mal_studio['mal_id']
        
        if not mal_name or not mal_id:
            continue
        
        normalized_mal = normalize_name(mal_name)
        
        if normalized_mal in anilist_studios:
            anilist_studio = anilist_studios[normalized_mal]
            company_name = anilist_studio['name']
            company_slug = slugify(company_name)
            
            if not company_slug:
                continue
            
            if company_slug not in companies:
                companies[company_slug] = {
                    'name': company_name,
                    'slug': company_slug,
                    'anilist_id': anilist_studio['id'],
                    'mal_id': mal_id,
                    'type': mal_studio['type'],
                    'is_animation_studio': anilist_studio['is_animation_studio'],
                    'franchises': []
                }
            
            # Add franchise reference
            franchise_ref = {
                'franchise_slug': franchise_slug,
                'entry_slug': entry_slug,
                'type': mal_studio['type']
            }
            if franchise_ref not in companies[company_slug]['franchises']:
                companies[company_slug]['franchises'].append(franchise_ref)
    
    return companies


def extract_franchise_entities(franchise_slug: str) -> dict:
    """
    Extract all entities from a franchise by scanning all its entries.
    Returns: {
        'characters': {...},
        'creators': {...},
        'companies': {...}
    }
    """
    all_characters = {}
    all_creators = {}
    all_companies = {}
    
    franchise_path = franchise_dir(franchise_slug)
    entries_path = os.path.join(franchise_path, 'entries')
    
    if not os.path.exists(entries_path):
        print(f"No entries found for franchise: {franchise_slug}")
        return {
            'characters': all_characters,
            'creators': all_creators,
            'companies': all_companies
        }
    
    # Process each entry
    for entry_name in os.listdir(entries_path):
        entry_path = entry_dir(franchise_slug, entry_name)
        sources_path = os.path.join(entry_path, 'sources')
        
        anilist_path = os.path.join(sources_path, 'anilist_raw.json')
        jikan_path = os.path.join(sources_path, 'jikan_raw.json')
        
        if not os.path.exists(anilist_path) or not os.path.exists(jikan_path):
            print(f"  Skipping {entry_name} - missing raw data files")
            continue
        
        print(f"  Processing entry: {entry_name}")
        
        try:
            anilist_data = load_json(anilist_path)
            jikan_data = load_json(jikan_path)
            
            # Extract entities
            characters = extract_characters(anilist_data, jikan_data, franchise_slug, entry_name)
            creators = extract_staff(anilist_data, jikan_data, franchise_slug, entry_name)
            companies = extract_companies(anilist_data, jikan_data, franchise_slug, entry_name)
            
            print(f"    Found: {len(characters)} characters, {len(creators)} creators, {len(companies)} companies")
            
            # Merge into all_* dicts, handling duplicates
            for slug, char in characters.items():
                if slug in all_characters:
                    # Character already exists - add franchise reference
                    all_characters[slug]['franchises'].extend(char['franchises'])
                else:
                    all_characters[slug] = char
            
            for slug, creator in creators.items():
                if slug in all_creators:
                    # Creator already exists - merge franchise references
                    for ref in creator['franchises']:
                        if ref not in all_creators[slug]['franchises']:
                            all_creators[slug]['franchises'].append(ref)
                    # Merge roles
                    for role in creator['roles']:
                        if role not in all_creators[slug]['roles']:
                            all_creators[slug]['roles'].append(role)
                else:
                    all_creators[slug] = creator
            
            for slug, company in companies.items():
                if slug in all_companies:
                    # Company already exists - merge franchise references
                    for ref in company['franchises']:
                        if ref not in all_companies[slug]['franchises']:
                            all_companies[slug]['franchises'].append(ref)
                else:
                    all_companies[slug] = company
        
        except Exception as e:
            print(f"    Error processing {entry_name}: {e}")
            import traceback
            traceback.print_exc()
    
    return {
        'characters': all_characters,
        'creators': all_creators,
        'companies': all_companies
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python extract_entities.py <franchise-slug>")
        print("Example: python extract_entities.py attack-on-titan")
        sys.exit(1)
    
    franchise_slug = sys.argv[1]
    
    print(f"Extracting entities from franchise: {franchise_slug}")
    print(f"Critical rule: Entities must appear in BOTH AniList AND MAL\n")
    
    entities = extract_franchise_entities(franchise_slug)
    
    # Save extracted entities for this franchise
    output_dir = os.path.join(franchise_dir(franchise_slug), 'extracted_entities')
    os.makedirs(output_dir, exist_ok=True)
    
    save_json(os.path.join(output_dir, 'characters.json'), entities['characters'])
    save_json(os.path.join(output_dir, 'creators.json'), entities['creators'])
    save_json(os.path.join(output_dir, 'companies.json'), entities['companies'])
    
    print(f"\n✓ Extraction complete!")
    print(f"  Characters: {len(entities['characters'])}")
    print(f"  Creators: {len(entities['creators'])}")
    print(f"  Companies: {len(entities['companies'])}")
    print(f"\nSaved to: {output_dir}/")


if __name__ == '__main__':
    main()
