"""
Tamil2Lyrics → WordPress Importer
==================================
Imports scraped JSONL data directly into a WordPress MySQL database.

Edit the CONFIG section below before running.

Usage:
    pip install pymysql tqdm
    python importer.py

Import order:
  1. Music directors  → wp_terms + wp_term_taxonomy
  2. Movies           → wp_posts (CPT) + wp_postmeta
  3. Lyricists        → wp_posts (CPT) + wp_postmeta
  4. Songs            → wp_posts (CPT) + wp_postmeta (with linked IDs)
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import pymysql
from tqdm import tqdm

# ===========================================================================
# CONFIG — edit everything in this section
# ===========================================================================

DB = {
    "host":     "localhost",       # MySQL host
    "port":     3306,
    "user":     "root",            # MySQL username
    "password": "your_password",   # MySQL password
    "database": "your_wp_db",      # WordPress database name
    "prefix":   "wp_",             # WordPress table prefix (usually wp_)
    "charset":  "utf8mb4",
}

# WordPress post type slugs registered by your theme
POST_TYPES = {
    "song":       "lyrics",        # slug for song posts        e.g. "lyrics", "song"
    "movie":      "album",         # slug for movie/album posts e.g. "album", "movie"
    "lyricist":   "artist",        # slug for lyricist posts    e.g. "artist", "lyricist"
}

# Taxonomy slug for music directors (set "" to store as postmeta string only)
MUSIC_DIRECTOR_TAXONOMY = "music_director"  # e.g. "music_director", "composer", ""

# WordPress user ID to assign as post author (usually 1 for admin)
AUTHOR_ID = 1

# Post status for all imported posts: "publish", "draft", "private"
POST_STATUS = "publish"

# Postmeta keys — how your theme reads data from each song post
SONG_META_KEYS = {
    "singer":           "singers",          # singer name(s) string
    "lyrics_en":        "lyrics_english",   # English lyrics (also stored in post_content)
    "lyrics_ta":        "lyrics_tamil",     # Tamil lyrics
    "movie_post_id":    "movie_id",         # wp post ID of the related movie post
    "lyricist_post_id": "lyricist_id",      # wp post ID of the related lyricist post
    "music_director":   "music_director",   # music director name string (always stored)
    "year":             "year",             # release year
}

MOVIE_META_KEYS = {
    "year":             "year",
}

# What goes in post_content for songs? "lyrics_en", "lyrics_ta", or "" for empty
SONG_POST_CONTENT = "lyrics_en"

# Input files (output from scraper.py)
OUTPUT_DIR = Path("output")
SONGS_FILE      = OUTPUT_DIR / "songs_final.jsonl"   # fully joined records
MOVIES_FILE     = OUTPUT_DIR / "movies.jsonl"
LYRICISTS_FILE  = OUTPUT_DIR / "lyricists.jsonl"

# Batch size for bulk inserts
BATCH = 200

# ===========================================================================
# Helpers
# ===========================================================================

NOW = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        print(f"  WARNING: {path} not found, skipping")
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def table(name: str) -> str:
    return f"{DB['prefix']}{name}"


def get_conn() -> pymysql.Connection:
    return pymysql.connect(
        host=DB["host"],
        port=DB["port"],
        user=DB["user"],
        password=DB["password"],
        database=DB["database"],
        charset=DB["charset"],
        autocommit=False,
    )


def slug_exists(cur, post_type: str, slug: str) -> int | None:
    """Return existing post ID if slug already imported, else None."""
    cur.execute(
        f"SELECT ID FROM {table('posts')} WHERE post_type=%s AND post_name=%s LIMIT 1",
        (post_type, slug),
    )
    row = cur.fetchone()
    return row[0] if row else None


def insert_post(cur, title: str, slug: str, post_type: str, content: str = "") -> int:
    """Insert a wp_posts row and return its ID."""
    cur.execute(
        f"""INSERT INTO {table('posts')}
            (post_author, post_date, post_date_gmt, post_content, post_title,
             post_status, post_name, post_type, post_modified, post_modified_gmt,
             to_ping, pinged, post_content_filtered, guid, comment_status, ping_status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'','','','','closed','closed')""",
        (AUTHOR_ID, NOW, NOW, content, title,
         POST_STATUS, slug, post_type, NOW, NOW),
    )
    post_id = cur.lastrowid
    # Update guid with placeholder (theme can update later)
    cur.execute(
        f"UPDATE {table('posts')} SET guid=%s WHERE ID=%s",
        (f"?p={post_id}", post_id),
    )
    return post_id


def insert_postmeta_bulk(cur, rows: list[tuple]):
    """Bulk insert postmeta rows: [(post_id, meta_key, meta_value), ...]"""
    if not rows:
        return
    cur.executemany(
        f"INSERT INTO {table('postmeta')} (post_id, meta_key, meta_value) VALUES (%s,%s,%s)",
        rows,
    )


# ===========================================================================
# Phase 1 — Music directors as taxonomy terms
# ===========================================================================

def import_music_directors(songs: list[dict]) -> dict[str, int]:
    """
    Create taxonomy terms for each unique music director name.
    Returns { name_lower: term_taxonomy_id }.
    """
    if not MUSIC_DIRECTOR_TAXONOMY:
        return {}

    # Collect unique director names from songs
    directors = {}
    for song in songs:
        md = song.get("music_director", {})
        name = md.get("name", "").strip() if isinstance(md, dict) else str(md).strip()
        slug = md.get("slug", "").strip() if isinstance(md, dict) else ""
        if name and name not in directors:
            directors[name] = slug or name.lower().replace(" ", "-")

    print(f"Music directors: {len(directors)} unique names")
    if not directors:
        return {}

    conn = get_conn()
    cur = conn.cursor()
    name_to_tt_id: dict[str, int] = {}

    try:
        # Check which already exist
        cur.execute(
            f"SELECT t.name, tt.term_taxonomy_id FROM {table('terms')} t "
            f"JOIN {table('term_taxonomy')} tt ON t.term_id=tt.term_id "
            f"WHERE tt.taxonomy=%s",
            (MUSIC_DIRECTOR_TAXONOMY,),
        )
        for row in cur.fetchall():
            name_to_tt_id[row[0].lower()] = row[1]

        new_count = 0
        for name, slug in tqdm(directors.items(), desc="Music directors", unit="term"):
            if name.lower() in name_to_tt_id:
                continue

            # Ensure unique slug
            base_slug = slug
            suffix = 1
            while True:
                cur.execute(
                    f"SELECT term_id FROM {table('terms')} WHERE slug=%s LIMIT 1",
                    (slug,)
                )
                if not cur.fetchone():
                    break
                slug = f"{base_slug}-{suffix}"
                suffix += 1

            cur.execute(
                f"INSERT INTO {table('terms')} (name, slug, term_group) VALUES (%s,%s,0)",
                (name, slug),
            )
            term_id = cur.lastrowid
            cur.execute(
                f"INSERT INTO {table('term_taxonomy')} (term_id, taxonomy, description, parent, count) "
                f"VALUES (%s,%s,'',0,0)",
                (term_id, MUSIC_DIRECTOR_TAXONOMY),
            )
            tt_id = cur.lastrowid
            name_to_tt_id[name.lower()] = tt_id
            new_count += 1

        conn.commit()
        print(f"  → {new_count} new terms created, {len(name_to_tt_id)} total")
    except Exception as e:
        conn.rollback()
        print(f"  ERROR: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return name_to_tt_id


# ===========================================================================
# Phase 2 — Movies
# ===========================================================================

def import_movies(movies: list[dict]) -> dict[str, int]:
    """Import movie posts. Returns { movie_slug: post_id }."""
    print(f"Movies: {len(movies)} records")
    if not movies:
        return {}

    conn = get_conn()
    cur = conn.cursor()
    slug_to_id: dict[str, int] = {}
    skipped = 0
    new_count = 0
    meta_rows: list[tuple] = []

    try:
        pbar = tqdm(movies, desc="Movies", unit="post")
        for m in pbar:
            slug = m.get("movie_slug", "").strip()
            name = m.get("name", "").strip() or slug
            year = m.get("year", "")
            if not slug:
                continue

            existing = slug_exists(cur, POST_TYPES["movie"], slug)
            if existing:
                slug_to_id[slug] = existing
                skipped += 1
                continue

            post_id = insert_post(cur, name, slug, POST_TYPES["movie"])
            slug_to_id[slug] = post_id
            new_count += 1

            if year:
                meta_rows.append((post_id, MOVIE_META_KEYS["year"], year))

            if len(meta_rows) >= BATCH:
                insert_postmeta_bulk(cur, meta_rows)
                meta_rows.clear()
                conn.commit()

        if meta_rows:
            insert_postmeta_bulk(cur, meta_rows)
        conn.commit()
        print(f"  → {new_count} new, {skipped} skipped (already exist)")
    except Exception as e:
        conn.rollback()
        print(f"  ERROR: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return slug_to_id


# ===========================================================================
# Phase 3 — Lyricists
# ===========================================================================

def import_lyricists(lyricists: list[dict]) -> dict[str, int]:
    """Import lyricist posts. Returns { lyricist_slug: post_id }."""
    print(f"Lyricists: {len(lyricists)} records")
    if not lyricists:
        return {}

    conn = get_conn()
    cur = conn.cursor()
    slug_to_id: dict[str, int] = {}
    skipped = 0
    new_count = 0

    try:
        pbar = tqdm(lyricists, desc="Lyricists", unit="post")
        for lyr in pbar:
            slug = lyr.get("slug", "").strip()
            name = lyr.get("name", "").strip() or slug
            if not slug:
                continue

            existing = slug_exists(cur, POST_TYPES["lyricist"], slug)
            if existing:
                slug_to_id[slug] = existing
                skipped += 1
                continue

            post_id = insert_post(cur, name, slug, POST_TYPES["lyricist"])
            slug_to_id[slug] = post_id
            new_count += 1

            if new_count % BATCH == 0:
                conn.commit()

        conn.commit()
        print(f"  → {new_count} new, {skipped} skipped (already exist)")
    except Exception as e:
        conn.rollback()
        print(f"  ERROR: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return slug_to_id


# ===========================================================================
# Phase 4 — Songs
# ===========================================================================

def import_songs(
    songs: list[dict],
    movie_slug_to_id: dict[str, int],
    lyricist_slug_to_id: dict[str, int],
    md_name_to_tt_id: dict[str, int],
):
    """Import song posts with all linked metadata."""
    print(f"Songs: {len(songs)} records")
    if not songs:
        return

    conn = get_conn()
    cur = conn.cursor()
    skipped = 0
    new_count = 0
    meta_rows: list[tuple] = []
    term_rel_rows: list[tuple] = []

    try:
        pbar = tqdm(songs, desc="Songs", unit="post")
        for song in pbar:
            slug = song.get("slug", "").strip()
            title = song.get("title_en", "").strip() or slug
            if not slug:
                continue

            existing = slug_exists(cur, POST_TYPES["song"], slug)
            if existing:
                skipped += 1
                continue

            # Post content
            content = ""
            if SONG_POST_CONTENT == "lyrics_en":
                content = song.get("lyrics_en", "")
            elif SONG_POST_CONTENT == "lyrics_ta":
                content = song.get("lyrics_ta", "")

            post_id = insert_post(cur, title, slug, POST_TYPES["song"], content)
            new_count += 1

            # Resolve related IDs
            movie_obj = song.get("movie", {})
            movie_slug = movie_obj.get("slug", "") if isinstance(movie_obj, dict) else ""
            movie_post_id = movie_slug_to_id.get(movie_slug, "")

            lyr_obj = song.get("lyricist", {})
            lyr_slug = lyr_obj.get("slug", "") if isinstance(lyr_obj, dict) else ""
            lyr_name = lyr_obj.get("name", "") if isinstance(lyr_obj, dict) else ""
            lyr_post_id = lyricist_slug_to_id.get(lyr_slug, "")

            md_obj = song.get("music_director", {})
            md_name = md_obj.get("name", "") if isinstance(md_obj, dict) else str(md_obj)

            year = movie_obj.get("year", "") if isinstance(movie_obj, dict) else ""

            # Postmeta
            meta_rows += [
                (post_id, SONG_META_KEYS["singer"],           song.get("singer", "")),
                (post_id, SONG_META_KEYS["lyrics_en"],        song.get("lyrics_en", "")),
                (post_id, SONG_META_KEYS["lyrics_ta"],        song.get("lyrics_ta", "")),
                (post_id, SONG_META_KEYS["music_director"],   md_name),
                (post_id, SONG_META_KEYS["year"],             year),
            ]
            if movie_post_id:
                meta_rows.append((post_id, SONG_META_KEYS["movie_post_id"], str(movie_post_id)))
            if lyr_post_id:
                meta_rows.append((post_id, SONG_META_KEYS["lyricist_post_id"], str(lyr_post_id)))

            # Music director taxonomy relationship
            if MUSIC_DIRECTOR_TAXONOMY and md_name:
                tt_id = md_name_to_tt_id.get(md_name.lower())
                if tt_id:
                    term_rel_rows.append((post_id, tt_id, 0))

            if len(meta_rows) >= BATCH * 10:
                insert_postmeta_bulk(cur, meta_rows)
                meta_rows.clear()
                if term_rel_rows:
                    cur.executemany(
                        f"INSERT IGNORE INTO {table('term_relationships')} "
                        f"(object_id, term_taxonomy_id, term_order) VALUES (%s,%s,%s)",
                        term_rel_rows,
                    )
                    term_rel_rows.clear()
                conn.commit()

        # Flush remainder
        if meta_rows:
            insert_postmeta_bulk(cur, meta_rows)
        if term_rel_rows:
            cur.executemany(
                f"INSERT IGNORE INTO {table('term_relationships')} "
                f"(object_id, term_taxonomy_id, term_order) VALUES (%s,%s,%s)",
                term_rel_rows,
            )
        conn.commit()

        # Update term counts
        if MUSIC_DIRECTOR_TAXONOMY:
            cur.execute(
                f"""UPDATE {table('term_taxonomy')} tt
                    SET count = (
                        SELECT COUNT(*) FROM {table('term_relationships')} tr
                        WHERE tr.term_taxonomy_id = tt.term_taxonomy_id
                    )
                    WHERE tt.taxonomy = %s""",
                (MUSIC_DIRECTOR_TAXONOMY,),
            )
            conn.commit()

        print(f"  → {new_count} new, {skipped} skipped (already exist)")
    except Exception as e:
        conn.rollback()
        print(f"  ERROR: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ===========================================================================
# Main
# ===========================================================================

def main():
    print("=" * 60)
    print("Tamil2Lyrics → WordPress Importer")
    print("=" * 60)

    # Validate config
    if DB["password"] == "your_password" or DB["database"] == "your_wp_db":
        print("ERROR: Edit the DB config at the top of this file before running.")
        sys.exit(1)

    # Load data
    print("\nLoading scraped data...")
    songs     = load_jsonl(SONGS_FILE)
    movies    = load_jsonl(MOVIES_FILE)
    lyricists = load_jsonl(LYRICISTS_FILE)
    print(f"  {len(songs)} songs, {len(movies)} movies, {len(lyricists)} lyricists")

    if not songs:
        print("ERROR: No songs found. Run scraper.py first.")
        sys.exit(1)

    # Test DB connection
    print("\nTesting database connection...")
    try:
        conn = get_conn()
        conn.ping()
        conn.close()
        print("  Connected OK")
    except Exception as e:
        print(f"  FAILED: {e}")
        sys.exit(1)

    # Phase 1: Music director terms
    print("\n" + "=" * 60)
    print("Phase 1: Music directors (taxonomy terms)")
    print("=" * 60)
    md_name_to_tt_id = import_music_directors(songs)

    # Phase 2: Movies
    print("\n" + "=" * 60)
    print("Phase 2: Movies")
    print("=" * 60)
    movie_slug_to_id = import_movies(movies)

    # Phase 3: Lyricists
    print("\n" + "=" * 60)
    print("Phase 3: Lyricists")
    print("=" * 60)
    lyricist_slug_to_id = import_lyricists(lyricists)

    # Phase 4: Songs
    print("\n" + "=" * 60)
    print("Phase 4: Songs")
    print("=" * 60)
    import_songs(songs, movie_slug_to_id, lyricist_slug_to_id, md_name_to_tt_id)

    print("\n" + "=" * 60)
    print("DONE! Check your WordPress admin to verify the import.")
    print("=" * 60)


if __name__ == "__main__":
    main()
