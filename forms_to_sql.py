#!/usr/bin/env python3
"""Convert filled JSON forms to SQL INSERT statements."""

import json
import sys
from pathlib import Path

def slugify(s):
    return s.lower().replace(' ', '-').replace('_', '-')

def sql_value(v):
    """Convert Python value to SQL literal."""
    if v is None or v == "NULL_CONFIRMED":
        return "NULL"
    elif isinstance(v, bool):
        return "true" if v else "false"
    elif isinstance(v, (int, float)):
        return str(v)
    elif isinstance(v, list):
        items = ", ".join(f"'{x}'" if isinstance(x, str) else str(x) for x in v)
        return f"ARRAY[{items}]"
    elif isinstance(v, dict):
        return f"'{json.dumps(v)}'::jsonb"
    else:
        # Escape single quotes
        escaped = str(v).replace("'", "''")
        return f"'{escaped}'"

def generate_sql(data):
    sql = []
    
    # 1. Franchise
    f = data['franchise']
    sql.append(f"""-- FRANCHISE
INSERT INTO franchises (id, name, native_name, slug, created_at, updated_at)
VALUES (gen_random_uuid(), {sql_value(f['name'])}, {sql_value(f['native_name'])}, {sql_value(f['slug'])}, NOW(), NOW());
""")

    # 2. Genres (from entry_genres, dedupe)
    genres_seen = set()
    for g in data.get('entry_genres', []):
        if g['genre_name'] not in genres_seen:
            genres_seen.add(g['genre_name'])
            sql.append(f"""INSERT INTO genres (id, name, display_name, media_type_id, created_at, updated_at)
VALUES (gen_random_uuid(), {sql_value(g['genre_name'])}, {sql_value(g['genre_display_name'])}, '5ea63465-e02f-4a08-8343-bcc7f9e8b52c', NOW(), NOW())
ON CONFLICT (name, media_type_id) DO NOTHING;""")
    sql.append("")

    # 3. Tags (from entry_tags, dedupe)
    tags_seen = set()
    for t in data.get('entry_tags', []):
        if t['tag_name'] not in tags_seen:
            tags_seen.add(t['tag_name'])
            sql.append(f"""INSERT INTO tags (id, name, display_name, category, created_at, updated_at)
VALUES (gen_random_uuid(), {sql_value(t['tag_name'])}, {sql_value(t['tag_display_name'])}, {sql_value(t.get('tag_category'))}, NOW(), NOW())
ON CONFLICT (name) DO NOTHING;""")
    sql.append("")

    # 4. Companies (from entry_companies, dedupe)
    companies_seen = set()
    for c in data.get('entry_companies', []):
        if c['company_slug'] not in companies_seen:
            companies_seen.add(c['company_slug'])
            sql.append(f"""INSERT INTO companies (id, name, slug, created_at, updated_at)
VALUES (gen_random_uuid(), {sql_value(c['company_name'])}, {sql_value(c['company_slug'])}, NOW(), NOW())
ON CONFLICT (slug) DO NOTHING;""")
    sql.append("")

    # 5. Entry
    e = data['entry']
    sql.append(f"""-- ENTRY
INSERT INTO entries (id, media_type_id, title, alternate_titles, release_date, status, description, locale_code, slug, primary_image, details, created_at, updated_at)
VALUES (
  gen_random_uuid(),
  {sql_value(e['media_type_id'])}::uuid,
  {sql_value(e['title'])},
  {sql_value(e.get('alternate_titles', []))},
  {sql_value(e.get('release_date'))}::date,
  {sql_value(e.get('status', 'released'))},
  {sql_value(e.get('description'))},
  {sql_value(e['locale_code'])},
  {sql_value(e['slug'])},
  {sql_value(e.get('primary_image'))},
  {sql_value(e.get('details', {}))},
  NOW(), NOW()
);
""")

    # 6. Entry Seasons
    for s in data.get('entry_seasons', []):
        sql.append(f"""INSERT INTO entry_seasons (id, entry_id, season_number, title, episode_count, air_date_start, air_date_end, created_at, updated_at)
VALUES (
  gen_random_uuid(),
  (SELECT id FROM entries WHERE slug = {sql_value(s['entry_slug'])}),
  {sql_value(s['season_number'])},
  {sql_value(s.get('title'))},
  {sql_value(s.get('episode_count'))},
  {sql_value(s.get('air_date_start'))}::date,
  {sql_value(s.get('air_date_end'))}::date,
  NOW(), NOW()
);""")
    sql.append("")

    # 7. Entry Franchises
    sql.append(f"""-- ENTRY_FRANCHISES
INSERT INTO entry_franchises (id, entry_id, franchise_id, created_at)
VALUES (
  gen_random_uuid(),
  (SELECT id FROM entries WHERE slug = {sql_value(e['slug'])}),
  (SELECT id FROM franchises WHERE slug = {sql_value(f['slug'])}),
  NOW()
);
""")

    # 8. Entry Genres
    for g in data.get('entry_genres', []):
        sql.append(f"""INSERT INTO entry_genres (id, entry_id, genre_id, is_primary, created_at)
VALUES (
  gen_random_uuid(),
  (SELECT id FROM entries WHERE slug = {sql_value(g['entry_slug'])}),
  (SELECT id FROM genres WHERE name = {sql_value(g['genre_name'])}),
  {sql_value(g.get('is_primary', False))},
  NOW()
);""")
    sql.append("")

    # 9. Entry Tags
    for t in data.get('entry_tags', []):
        sql.append(f"""INSERT INTO entry_tags (id, entry_id, tag_id, created_at)
VALUES (
  gen_random_uuid(),
  (SELECT id FROM entries WHERE slug = {sql_value(t['entry_slug'])}),
  (SELECT id FROM tags WHERE name = {sql_value(t['tag_name'])}),
  NOW()
);""")
    sql.append("")

    # 10. Entry Companies
    role_map = {
        'studio': '01a492b4-279b-4b0f-ad54-dd43c2567374',
        'publisher': 'ee5ff71d-938e-41c0-8950-2cf171c90564',
        'producer': 'e0b91b72-bd9e-415b-804d-dcf10d83b851',
        'broadcaster': 'd0720c02-64c0-4ba4-9776-4f5dd8895f99',
    }
    for c in data.get('entry_companies', []):
        role_id = role_map.get(c.get('role', 'producer'), role_map['producer'])
        sql.append(f"""INSERT INTO entry_companies (id, entry_id, company_id, role_id, created_at)
VALUES (
  gen_random_uuid(),
  (SELECT id FROM entries WHERE slug = {sql_value(c['entry_slug'])}),
  (SELECT id FROM companies WHERE slug = {sql_value(c['company_slug'])}),
  '{role_id}'::uuid,
  NOW()
);""")

    return "\n".join(sql)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: forms_to_sql.py <filled_forms.json> [output.sql]")
        sys.exit(1)
    
    input_file = Path(sys.argv[1])
    output_file = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    
    with open(input_file) as f:
        data = json.load(f)
    
    sql = generate_sql(data)
    
    if output_file:
        with open(output_file, 'w') as f:
            f.write(sql)
        print(f"SQL written to {output_file}")
    else:
        print(sql)
