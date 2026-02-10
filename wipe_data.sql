-- WIPE ALL DATA
-- Truncates all tables, preserves schema
-- Run with: psql watchlist -f wipe_data.sql

-- Truncate all tables (CASCADE handles FK dependencies)
TRUNCATE TABLE
    characters,
    companies,
    company_roles,
    countries,
    creator_roles,
    creators,
    entries,
    entry_characters,
    entry_companies,
    entry_creators,
    entry_franchises,
    entry_genres,
    entry_relationships,
    entry_seasons,
    entry_tags,
    entry_translations,
    franchises,
    genres,
    locales,
    media_types,
    music_tracks,
    product_categories,
    product_characters,
    product_companies,
    product_entries,
    product_images,
    product_listings,
    product_subcategories,
    product_tracks,
    product_translations,
    products,
    relationship_types,
    retailers,
    season_episodes,
    tags,
    track_creators,
    user_profiles
CASCADE;

-- Confirm
SELECT 'All data wiped. Tables are empty.' AS status;
