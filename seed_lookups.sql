-- Seed lookup tables for The Watchlist
-- Run after wipe or on fresh DB

-- Locales
INSERT INTO locales (code, name, native_name) VALUES
  ('en', 'English', 'English'),
  ('ja', 'Japanese', '日本語'),
  ('ko', 'Korean', '한국어'),
  ('zh', 'Chinese', '中文'),
  ('es', 'Spanish', 'Español'),
  ('fr', 'French', 'Français'),
  ('de', 'German', 'Deutsch'),
  ('pt', 'Portuguese', 'Português'),
  ('it', 'Italian', 'Italiano')
ON CONFLICT (code) DO NOTHING;

-- Media Types
INSERT INTO media_types (name, display_name, description) VALUES
  ('anime', 'Anime', 'Japanese animated series and films'),
  ('manga', 'Manga', 'Japanese comics'),
  ('movie', 'Movie', 'Feature films'),
  ('tv', 'TV Series', 'Live-action television series'),
  ('music', 'Music', 'Albums, singles, soundtracks'),
  ('game', 'Game', 'Video games'),
  ('book', 'Book', 'Novels, light novels')
ON CONFLICT (name) DO NOTHING;

-- Company Roles
INSERT INTO company_roles (name, display_name, description) VALUES
  ('animation_studio', 'Animation Studio', 'Primary animation production'),
  ('studio', 'Studio', 'Production studio'),
  ('producer', 'Producer', 'Production company'),
  ('distributor', 'Distributor', 'Distribution company'),
  ('publisher', 'Publisher', 'Publishing company'),
  ('licensor', 'Licensor', 'Licensing rights holder'),
  ('broadcaster', 'Broadcaster', 'TV network or streaming platform'),
  ('developer', 'Developer', 'Game/software development'),
  ('record_label', 'Record Label', 'Music label'),
  ('manufacturer', 'Manufacturer', 'Product manufacturer')
ON CONFLICT (name) DO NOTHING;

-- Creator Roles (Anime/Animation)
INSERT INTO creator_roles (name, display_name, category, description) VALUES
  ('director', 'Director', 'direction', 'Series or episode director'),
  ('chief_director', 'Chief Director', 'direction', 'Overall series director'),
  ('episode_director', 'Episode Director', 'direction', 'Individual episode director'),
  ('assistant_director', 'Assistant Director', 'direction', 'Assistant to director'),
  ('original_creator', 'Original Creator', 'story', 'Creator of source material'),
  ('original_story', 'Original Story', 'story', 'Original story creator'),
  ('series_composition', 'Series Composition', 'writing', 'Overall story structure'),
  ('screenplay', 'Screenplay', 'writing', 'Episode screenplay writer'),
  ('script', 'Script', 'writing', 'Script writer'),
  ('storyboard', 'Storyboard', 'visual', 'Storyboard artist'),
  ('character_design', 'Character Design', 'visual', 'Character designer'),
  ('chief_animation_director', 'Chief Animation Director', 'animation', 'Overall animation supervision'),
  ('animation_director', 'Animation Director', 'animation', 'Animation supervision'),
  ('key_animation', 'Key Animation', 'animation', 'Key animator'),
  ('in_between', 'In-Between Animation', 'animation', 'In-between animator'),
  ('art_director', 'Art Director', 'visual', 'Background and setting design'),
  ('color_design', 'Color Design', 'visual', 'Color palette design'),
  ('compositing', 'Compositing', 'visual', 'Digital compositing'),
  ('cgi_director', 'CGI Director', 'visual', '3D/CG direction'),
  ('music', 'Music', 'audio', 'Music composer'),
  ('sound_director', 'Sound Director', 'audio', 'Sound design and direction'),
  ('sound_effects', 'Sound Effects', 'audio', 'Sound effects creation'),
  ('producer', 'Producer', 'production', 'Production management'),
  ('executive_producer', 'Executive Producer', 'production', 'Executive production'),
  ('voice_actor', 'Voice Actor', 'cast', 'Voice acting'),
  ('narrator', 'Narrator', 'cast', 'Narration'),
  ('theme_song_performance', 'Theme Song Performance', 'music', 'OP/ED performance'),
  ('theme_song_composition', 'Theme Song Composition', 'music', 'OP/ED composition'),
  ('theme_song_lyrics', 'Theme Song Lyrics', 'music', 'OP/ED lyrics'),
  ('author', 'Author', 'story', 'Book/manga author'),
  ('artist', 'Artist', 'visual', 'Manga/comic artist'),
  ('illustrator', 'Illustrator', 'visual', 'Light novel illustrator'),
  ('editor', 'Editor', 'production', 'Editor')
ON CONFLICT (name) DO NOTHING;

-- Relationship Types (for entry_relationships)
INSERT INTO relationship_types (name, display_name, inverse_name, description) VALUES
  ('sequel', 'Sequel', 'prequel', 'Chronological sequel'),
  ('prequel', 'Prequel', 'sequel', 'Chronological prequel'),
  ('spinoff', 'Spin-off', 'parent', 'Derived work focusing on side characters/stories'),
  ('parent', 'Parent Work', 'spinoff', 'Original work that a spinoff derives from'),
  ('adaptation', 'Adaptation', 'source', 'Adapted from another medium'),
  ('source', 'Source Material', 'adaptation', 'Original source material'),
  ('alternative', 'Alternative Version', 'alternative', 'Alternative retelling'),
  ('side_story', 'Side Story', 'main_story', 'Related side story'),
  ('main_story', 'Main Story', 'side_story', 'Main story that side stories relate to'),
  ('summary', 'Summary', 'full', 'Condensed version'),
  ('full', 'Full Version', 'summary', 'Full version that summary condenses'),
  ('compilation', 'Compilation', 'compiled', 'Compilation of multiple works'),
  ('compiled', 'Compiled Work', 'compilation', 'Work included in compilation')
ON CONFLICT (name) DO NOTHING;

SELECT 'Lookup tables seeded.' AS status;
