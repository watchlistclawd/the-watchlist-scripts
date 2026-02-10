#!/usr/bin/env python3
"""
Populate top-level entity folders (characters/, creators/, companies/)
by deduplicating extracted entities from all franchises.
"""
import os
import sys
from pathlib import Path
from utils import (
    slugify, load_json, save_json, ensure_dir, 
    franchise_dir, character_dir, creator_dir, company_dir, DATA_ROOT
)


def merge_character_data(existing: dict, new: dict) -> dict:
    """Merge two character data dicts, combining franchises."""
    merged = existing.copy()
    
    # Merge franchises
    for franchise in new.get('franchises', []):
        if franchise not in merged.get('franchises', []):
            merged.setdefault('franchises', []).append(franchise)
    
    # Use data with more voice actors if available
    if len(new.get('voice_actors', [])) > len(merged.get('voice_actors', [])):
        merged['voice_actors'] = new['voice_actors']
        merged['anilist_image'] = new.get('anilist_image', merged.get('anilist_image', ''))
        merged['mal_image'] = new.get('mal_image', merged.get('mal_image', ''))
    
    return merged


def merge_creator_data(existing: dict, new: dict) -> dict:
    """Merge two creator data dicts, combining franchises and roles."""
    merged = existing.copy()
    
    # Merge franchises
    for franchise in new.get('franchises', []):
        if franchise not in merged.get('franchises', []):
            merged.setdefault('franchises', []).append(franchise)
    
    # Merge roles
    for role in new.get('roles', []):
        if role not in merged.get('roles', []):
            merged.setdefault('roles', []).append(role)
    
    return merged


def merge_company_data(existing: dict, new: dict) -> dict:
    """Merge two company data dicts, combining franchises."""
    merged = existing.copy()
    
    # Merge franchises
    for franchise in new.get('franchises', []):
        if franchise not in merged.get('franchises', []):
            merged.setdefault('franchises', []).append(franchise)
    
    return merged


def collect_all_entities() -> dict:
    """
    Scan all franchises and collect their extracted entities.
    Returns: {
        'characters': {slug: data},
        'creators': {slug: data},
        'companies': {slug: data}
    }
    """
    all_characters = {}
    all_creators = {}
    all_companies = {}
    
    franchises_dir = os.path.join(DATA_ROOT, 'franchises')
    
    if not os.path.exists(franchises_dir):
        print(f"Franchises directory not found: {franchises_dir}")
        return {
            'characters': all_characters,
            'creators': all_creators,
            'companies': all_companies
        }
    
    # Process each franchise
    for franchise_slug in sorted(os.listdir(franchises_dir)):
        franchise_path = os.path.join(franchises_dir, franchise_slug)
        if not os.path.isdir(franchise_path):
            continue
        
        extracted_dir = os.path.join(franchise_path, 'extracted_entities')
        if not os.path.exists(extracted_dir):
            print(f"  Skipping {franchise_slug} - no extracted entities")
            continue
        
        print(f"  Loading entities from: {franchise_slug}")
        
        # Load characters
        chars_path = os.path.join(extracted_dir, 'characters.json')
        if os.path.exists(chars_path):
            chars = load_json(chars_path)
            for slug, data in chars.items():
                if slug in all_characters:
                    all_characters[slug] = merge_character_data(all_characters[slug], data)
                else:
                    all_characters[slug] = data
        
        # Load creators
        creators_path = os.path.join(extracted_dir, 'creators.json')
        if os.path.exists(creators_path):
            creators = load_json(creators_path)
            for slug, data in creators.items():
                if slug in all_creators:
                    all_creators[slug] = merge_creator_data(all_creators[slug], data)
                else:
                    all_creators[slug] = data
        
        # Load companies
        companies_path = os.path.join(extracted_dir, 'companies.json')
        if os.path.exists(companies_path):
            companies = load_json(companies_path)
            for slug, data in companies.items():
                if slug in all_companies:
                    all_companies[slug] = merge_company_data(all_companies[slug], data)
                else:
                    all_companies[slug] = data
    
    return {
        'characters': all_characters,
        'creators': all_creators,
        'companies': all_companies
    }


def populate_characters(characters: dict):
    """Write character data to top-level characters/ folder."""
    print(f"\nPopulating characters/ folder...")
    
    for slug, data in characters.items():
        char_path = character_dir(data['franchises'][0]['franchise_slug'], slug)
        # Wait, character_dir takes franchise_slug as first arg, but we want top-level
        # Let me fix this - we want characters at the top level
        char_path = os.path.join(DATA_ROOT, 'characters', slug)
        sources_path = os.path.join(char_path, 'sources')
        
        ensure_dir(sources_path)
        
        # Create main character.json
        character_json = {
            'name': data['name'],
            'slug': slug,
            'native_name': data.get('native_name', ''),
            'role': data.get('role', ''),
            'anilist_id': data['anilist_id'],
            'mal_id': data['mal_id'],
            'images': {
                'anilist': data.get('anilist_image', ''),
                'mal': data.get('mal_image', '')
            },
            'voice_actors': data.get('voice_actors', []),
            'franchises': data['franchises']
        }
        save_json(os.path.join(char_path, 'character.json'), character_json)
        
        # Create source files
        anilist_source = {
            'id': data['anilist_id'],
            'name': data['name'],
            'native_name': data.get('native_name', ''),
            'image': data.get('anilist_image', ''),
            'voice_actors': [
                {
                    'id': va['anilist_id'],
                    'name': va['name'],
                    'native': va['native'],
                    'image': va['anilist_image']
                }
                for va in data.get('voice_actors', [])
            ]
        }
        save_json(os.path.join(sources_path, 'anilist.json'), anilist_source)
        
        mal_source = {
            'mal_id': data['mal_id'],
            'name': data['name'],
            'image': data.get('mal_image', ''),
            'voice_actors': [
                {
                    'mal_id': va['mal_id'],
                    'name': va['name'],
                    'image': va['mal_image']
                }
                for va in data.get('voice_actors', [])
            ]
        }
        save_json(os.path.join(sources_path, 'mal.json'), mal_source)
        
        print(f"  ✓ {data['name']}")


def populate_creators(creators: dict):
    """Write creator data to top-level creators/ folder."""
    print(f"\nPopulating creators/ folder...")
    
    for slug, data in creators.items():
        creator_path = creator_dir(slug)
        sources_path = os.path.join(creator_path, 'sources')
        
        ensure_dir(sources_path)
        
        # Create main creator.json
        creator_json = {
            'name': data['name'],
            'slug': slug,
            'native_name': data.get('native_name', ''),
            'anilist_id': data['anilist_id'],
            'mal_id': data['mal_id'],
            'images': {
                'anilist': data.get('anilist_image', ''),
                'mal': data.get('mal_image', '')
            },
            'roles': data.get('roles', []),
            'franchises': data['franchises']
        }
        save_json(os.path.join(creator_path, 'creator.json'), creator_json)
        
        # Create source files
        anilist_source = {
            'id': data['anilist_id'],
            'name': data['name'],
            'native_name': data.get('native_name', ''),
            'image': data.get('anilist_image', '')
        }
        save_json(os.path.join(sources_path, 'anilist.json'), anilist_source)
        
        mal_source = {
            'mal_id': data['mal_id'],
            'name': data['name'],
            'image': data.get('mal_image', '')
        }
        save_json(os.path.join(sources_path, 'mal.json'), mal_source)
        
        print(f"  ✓ {data['name']} ({', '.join(data['roles'][:2])})")


def populate_companies(companies: dict):
    """Write company data to top-level companies/ folder."""
    print(f"\nPopulating companies/ folder...")
    
    for slug, data in companies.items():
        company_path = company_dir(slug)
        sources_path = os.path.join(company_path, 'sources')
        
        ensure_dir(sources_path)
        
        # Create main company.json
        company_json = {
            'name': data['name'],
            'slug': slug,
            'anilist_id': data['anilist_id'],
            'mal_id': data['mal_id'],
            'type': data.get('type', ''),
            'is_animation_studio': data.get('is_animation_studio', False),
            'franchises': data['franchises']
        }
        save_json(os.path.join(company_path, 'company.json'), company_json)
        
        # Create source files
        anilist_source = {
            'id': data['anilist_id'],
            'name': data['name'],
            'is_animation_studio': data.get('is_animation_studio', False)
        }
        save_json(os.path.join(sources_path, 'anilist.json'), anilist_source)
        
        mal_source = {
            'mal_id': data['mal_id'],
            'name': data['name'],
            'type': data.get('type', '')
        }
        save_json(os.path.join(sources_path, 'mal.json'), mal_source)
        
        print(f"  ✓ {data['name']} ({data.get('type', 'N/A')})")


def main():
    print("Populating top-level entity folders from all franchises...")
    print("Deduplicating entities across franchises\n")
    
    # Collect all entities
    entities = collect_all_entities()
    
    print(f"\nTotal unique entities:")
    print(f"  Characters: {len(entities['characters'])}")
    print(f"  Creators: {len(entities['creators'])}")
    print(f"  Companies: {len(entities['companies'])}")
    
    # Populate entity folders
    populate_characters(entities['characters'])
    populate_creators(entities['creators'])
    populate_companies(entities['companies'])
    
    print(f"\n✓ Population complete!")
    print(f"  Characters written to: {DATA_ROOT}/characters/")
    print(f"  Creators written to: {DATA_ROOT}/creators/")
    print(f"  Companies written to: {DATA_ROOT}/companies/")


if __name__ == '__main__':
    main()
