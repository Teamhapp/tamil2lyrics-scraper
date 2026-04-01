"""
Tamil2Lyrics → WordPress Importer
==================================
Imports scraped JSONL data into a WordPress site using the
tamil2lyrics custom theme (t2l_song, t2l_movie_post, t2l_person_post CPTs
and t2l_movie, t2l_director, t2l_singer, t2l_lyricist, t2l_decade taxonomies).

Edit the DB section below, then run:
    pip install pymysql tqdm
    python importer.py

Import order:
  1. Taxonomy terms  → t2l_movie, t2l_director, t2l_singer, t2l_lyricist, t2l_decade
  2. Movie posts     → t2l_movie_post  (linked to t2l_movie term via t2l_linked_tax_slug)
  3. Person posts    → t2l_person_post (linked to their taxonomy terms)
  4. Song posts      → t2l_song with all taxonomy relationships + postmeta
"""

import json
import re
import sys
import threading
from datetime import datetime
from pathlib import Path

import pymysql
from tqdm import tqdm

# ===========================================================================
# Progress tracking — writes output/progress.json every update
# Open progress.html in a browser while the import runs
# ===========================================================================

PROGRESS_FILE = Path("output/progress.json")
_progress_lock = threading.Lock()

_progress: dict = {
    "status":     "starting",
    "current_phase": "",
    "started_at": "",
    "updated_at": "",
    "phases": {
        "Terms":   {"label": "Taxonomy Terms",  "done": 0, "total": 0, "status": "pending"},
        "Movies":  {"label": "Movie Posts",     "done": 0, "total": 0, "status": "pending"},
        "People":  {"label": "Person Posts",    "done": 0, "total": 0, "status": "pending"},
        "Songs":   {"label": "Song Posts",      "done": 0, "total": 0, "status": "pending"},
    },
}


def _save_progress():
    _progress["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    PROGRESS_FILE.parent.mkdir(exist_ok=True)
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(_progress, f, ensure_ascii=False, indent=2)


def progress_start(phase: str, total: int):
    with _progress_lock:
        _progress["status"] = "running"
        _progress["current_phase"] = phase
        if not _progress["started_at"]:
            _progress["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _progress["phases"][phase]["total"] = total
        _progress["phases"][phase]["done"] = 0
        _progress["phases"][phase]["status"] = "running"
        _save_progress()


def progress_update(phase: str, done: int):
    with _progress_lock:
        _progress["phases"][phase]["done"] = done
        _save_progress()


def progress_done(phase: str):
    with _progress_lock:
        p = _progress["phases"][phase]
        p["done"] = p["total"]
        p["status"] = "done"
        _save_progress()


def progress_finish():
    with _progress_lock:
        _progress["status"] = "done"
        _progress["current_phase"] = ""
        _save_progress()

# ===========================================================================
# CONFIG — loaded from output/config.json (set via dashboard) or defaults below
# ===========================================================================

# Load config.json written by the dashboard
_CONFIG_FILE = Path("output/config.json")
_cfg: dict = {}
if _CONFIG_FILE.exists():
    try:
        _cfg = json.loads(_CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass

DB = {
    "host":     _cfg.get("host",     "localhost"),
    "port":     int(_cfg.get("port", 3306)),
    "user":     _cfg.get("user",     "root"),
    "password": _cfg.get("password", ""),
    "database": _cfg.get("database", ""),
    "prefix":   _cfg.get("prefix",   "wp_"),
    "charset":  "utf8mb4",
}

AUTHOR_ID   = int(_cfg.get("author_id", 1))
POST_STATUS = "publish"

# Input files
OUTPUT_DIR      = Path("output")
SONGS_FILE      = OUTPUT_DIR / "songs_final.jsonl"
MOVIES_FILE     = OUTPUT_DIR / "movies.jsonl"
LYRICISTS_FILE  = OUTPUT_DIR / "lyricists.jsonl"
MUSIC_DIRS_FILE = OUTPUT_DIR / "music_directors.jsonl"

BATCH = 500   # flush to DB every N rows

# ===========================================================================
# DB helpers
# ===========================================================================

NOW = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        print(f"  WARNING: {path} not found")
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def get_conn() -> pymysql.Connection:
    return pymysql.connect(
        host=DB["host"], port=DB["port"],
        user=DB["user"], password=DB["password"],
        database=DB["database"], charset=DB["charset"],
        autocommit=False,
    )


def T(name: str) -> str:
    return f"{DB['prefix']}{name}"


def make_unique_slug(cur, base_slug: str) -> str:
    """Ensure slug is unique in wp_posts."""
    slug = base_slug
    n = 1
    while True:
        cur.execute(f"SELECT ID FROM {T('posts')} WHERE post_name=%s LIMIT 1", (slug,))
        if not cur.fetchone():
            return slug
        slug = f"{base_slug}-{n}"
        n += 1


def make_unique_term_slug(cur, base_slug: str) -> str:
    """Ensure slug is unique in wp_terms."""
    slug = base_slug
    n = 1
    while True:
        cur.execute(f"SELECT term_id FROM {T('terms')} WHERE slug=%s LIMIT 1", (slug,))
        if not cur.fetchone():
            return slug
        slug = f"{base_slug}-{n}"
        n += 1


def insert_post(cur, title: str, slug: str, post_type: str, content: str = "") -> int:
    slug = make_unique_slug(cur, slug)
    cur.execute(
        f"""INSERT INTO {T('posts')}
            (post_author, post_date, post_date_gmt, post_content, post_title,
             post_status, post_name, post_type, post_modified, post_modified_gmt,
             to_ping, pinged, post_content_filtered, guid, comment_status, ping_status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'','','','','closed','closed')""",
        (AUTHOR_ID, NOW, NOW, content, title, POST_STATUS, slug, post_type, NOW, NOW),
    )
    post_id = cur.lastrowid
    cur.execute(f"UPDATE {T('posts')} SET guid=%s WHERE ID=%s", (f"?p={post_id}", post_id))
    return post_id


def post_exists(cur, post_type: str, slug: str) -> int | None:
    cur.execute(
        f"SELECT ID FROM {T('posts')} WHERE post_type=%s AND post_name=%s LIMIT 1",
        (post_type, slug),
    )
    row = cur.fetchone()
    return row[0] if row else None


# ===========================================================================
# Phase 1 — Taxonomy terms
# ===========================================================================

def get_or_create_term(cur, name: str, slug: str, taxonomy: str) -> int:
    """
    Return term_taxonomy_id for an existing or newly created term.
    Checks by slug within taxonomy.
    """
    cur.execute(
        f"""SELECT tt.term_taxonomy_id FROM {T('terms')} t
            JOIN {T('term_taxonomy')} tt ON t.term_id = tt.term_id
            WHERE tt.taxonomy=%s AND t.slug=%s LIMIT 1""",
        (taxonomy, slug),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    slug = make_unique_term_slug(cur, slug)
    cur.execute(f"INSERT INTO {T('terms')} (name, slug, term_group) VALUES (%s,%s,0)", (name, slug))
    term_id = cur.lastrowid
    cur.execute(
        f"INSERT INTO {T('term_taxonomy')} (term_id, taxonomy, description, parent, count) VALUES (%s,%s,'',0,0)",
        (term_id, taxonomy),
    )
    return cur.lastrowid


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text


def parse_singers(singer_str: str) -> list[str]:
    """Split 'A & B, C' into individual singer names."""
    if not singer_str:
        return []
    parts = re.split(r"[,&]", singer_str)
    return [p.strip() for p in parts if p.strip()]


def import_all_terms(songs: list[dict], movies: list[dict], music_dirs: list[dict]) -> dict:
    """
    Build all taxonomy terms upfront.
    Returns:
      {
        't2l_movie':    { movie_slug: tt_id },
        't2l_director': { director_slug: tt_id },
        't2l_singer':   { singer_slug: tt_id },
        't2l_lyricist': { lyricist_slug: tt_id },
        't2l_decade':   { '1990s': tt_id, ... },
      }
    """
    print("Building taxonomy term maps...")

    # Collect unique entities
    movie_terms    = {}   # slug → name
    director_terms = {}
    singer_terms   = {}
    lyricist_terms = {}
    decade_terms   = set()

    for song in songs:
        movie = song.get("movie", {})
        m_slug = movie.get("slug", "")
        m_name = movie.get("name", "")
        if m_slug and m_name:
            movie_terms[m_slug] = m_name

        year = movie.get("year", "")
        if year and year.isdigit():
            decade_terms.add(f"{(int(year)//10)*10}s")

        md = song.get("music_director", {})
        d_slug = md.get("slug", "") if isinstance(md, dict) else ""
        d_name = md.get("name", "") if isinstance(md, dict) else str(md)
        if d_slug and d_name:
            director_terms[d_slug] = d_name
        elif d_name:
            director_terms[slugify(d_name)] = d_name

        lyr = song.get("lyricist", {})
        l_slug = lyr.get("slug", "") if isinstance(lyr, dict) else ""
        l_name = lyr.get("name", "") if isinstance(lyr, dict) else str(lyr)
        if l_slug and l_name:
            lyricist_terms[l_slug] = l_name
        elif l_name:
            lyricist_terms[slugify(l_name)] = l_name

        for singer_name in parse_singers(song.get("singer", "")):
            s_slug = slugify(singer_name)
            if s_slug:
                singer_terms[s_slug] = singer_name

    # Supplement movie terms from movies.jsonl
    for m in movies:
        slug = m.get("movie_slug", "")
        name = m.get("name", "")
        if slug and name:
            movie_terms.setdefault(slug, name)

    print(f"  movies: {len(movie_terms)}, directors: {len(director_terms)}, "
          f"singers: {len(singer_terms)}, lyricists: {len(lyricist_terms)}, "
          f"decades: {len(decade_terms)}")

    conn = get_conn()
    cur = conn.cursor()
    result = {t: {} for t in ["t2l_movie", "t2l_director", "t2l_singer", "t2l_lyricist", "t2l_decade"]}

    all_terms_total = sum([len(movie_terms), len(director_terms), len(singer_terms), len(lyricist_terms), len(decade_terms)])
    progress_start("Terms", all_terms_total)
    done_so_far = 0

    try:
        for taxonomy, terms in [
            ("t2l_movie",    movie_terms),
            ("t2l_director", director_terms),
            ("t2l_singer",   singer_terms),
            ("t2l_lyricist", lyricist_terms),
        ]:
            for slug, name in tqdm(terms.items(), desc=taxonomy, unit="term"):
                tt_id = get_or_create_term(cur, name, slug, taxonomy)
                result[taxonomy][slug] = tt_id
                done_so_far += 1
                if done_so_far % 50 == 0:
                    progress_update("Terms", done_so_far)

        for decade in tqdm(decade_terms, desc="t2l_decade", unit="term"):
            tt_id = get_or_create_term(cur, decade, slugify(decade), "t2l_decade")
            result["t2l_decade"][decade] = tt_id
            done_so_far += 1

        conn.commit()
        progress_done("Terms")
    except Exception as e:
        conn.rollback()
        print(f"  ERROR: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    print("  Taxonomy terms done.")
    return result


# ===========================================================================
# Phase 2 — Movie posts (t2l_movie_post)
# Linked to t2l_movie taxonomy via t2l_linked_tax_slug postmeta
# ===========================================================================

def import_movie_posts(movies: list[dict], movie_tt_ids: dict) -> dict[str, int]:
    """Returns { movie_slug: post_id }"""
    print(f"\nMovies: {len(movies)}")
    progress_start("Movies", len(movies))
    conn = get_conn()
    cur = conn.cursor()
    slug_to_id: dict[str, int] = {}
    new_count = skipped = 0
    meta_rows: list[tuple] = []

    try:
        for idx, m in enumerate(tqdm(movies, desc="Movie posts", unit="post")):
            slug       = m.get("movie_slug", "").strip()
            name       = m.get("name", "").strip() or slug
            year       = m.get("year", "")
            if not slug:
                continue

            existing = post_exists(cur, "t2l_movie_post", slug)
            if existing:
                slug_to_id[slug] = existing
                skipped += 1
                continue

            post_id = insert_post(cur, name, slug, "t2l_movie_post")
            slug_to_id[slug] = post_id
            new_count += 1

            meta_rows += [
                (post_id, "t2l_linked_tax_slug", slug),     # link to taxonomy term
                (post_id, "t2l_film_year",        year or ""),
                (post_id, "t2l_song_count",       str(len(m.get("songs", [])))),
            ]

            if len(meta_rows) >= BATCH:
                cur.executemany(
                    f"INSERT INTO {T('postmeta')} (post_id,meta_key,meta_value) VALUES (%s,%s,%s)",
                    meta_rows,
                )
                meta_rows.clear()
                conn.commit()
                progress_update("Movies", idx + 1)

        if meta_rows:
            cur.executemany(
                f"INSERT INTO {T('postmeta')} (post_id,meta_key,meta_value) VALUES (%s,%s,%s)",
                meta_rows,
            )
        conn.commit()
        progress_done("Movies")
        print(f"  → {new_count} new, {skipped} skipped")
    except Exception as e:
        conn.rollback()
        print(f"  ERROR: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return slug_to_id


# ===========================================================================
# Phase 3 — Person posts (t2l_person_post)
# One post per unique person (can be director + lyricist + singer combined)
# ===========================================================================

def import_person_posts(
    lyricists: list[dict],
    music_dirs: list[dict],
    songs: list[dict],
    term_maps: dict,
) -> dict[str, int]:
    """
    Create one t2l_person_post per unique person (merged by slug).
    Returns { person_slug: post_id }
    """
    # Build a unified person registry
    # slug → { name, roles: set, dir_slug, singer_slug, lyricist_slug }
    people: dict[str, dict] = {}

    for lyr in lyricists:
        slug = lyr.get("slug", "").strip()
        name = lyr.get("name", "").strip()
        if not slug:
            continue
        p = people.setdefault(slug, {"name": name, "roles": set()})
        p["roles"].add("lyricist")
        p["lyricist_slug"] = slug

    for md in music_dirs:
        slug = md.get("slug", "").strip()
        name = md.get("name", "").strip()
        if not slug:
            continue
        p = people.setdefault(slug, {"name": name, "roles": set()})
        p["roles"].add("director")
        p["dir_slug"] = slug

    # Also add singers found in songs
    for song in songs:
        for sname in parse_singers(song.get("singer", "")):
            s_slug = slugify(sname)
            if not s_slug:
                continue
            p = people.setdefault(s_slug, {"name": sname, "roles": set()})
            p["roles"].add("singer")
            p["singer_slug"] = s_slug

    print(f"\nPeople: {len(people)} unique persons")
    progress_start("People", len(people))

    conn = get_conn()
    cur = conn.cursor()
    slug_to_id: dict[str, int] = {}
    new_count = skipped = 0
    meta_rows: list[tuple] = []

    try:
        for idx, (slug, info) in enumerate(tqdm(people.items(), desc="Person posts", unit="post")):
            name  = info["name"]
            roles = info["roles"]

            existing = post_exists(cur, "t2l_person_post", slug)
            if existing:
                slug_to_id[slug] = existing
                skipped += 1
                continue

            post_id = insert_post(cur, name, slug, "t2l_person_post")
            slug_to_id[slug] = post_id
            new_count += 1

            meta_rows += [
                (post_id, "t2l_person_type", ",".join(sorted(roles))),
            ]
            if "dir_slug" in info:
                meta_rows.append((post_id, "t2l_linked_dir_slug", info["dir_slug"]))
            if "lyricist_slug" in info:
                meta_rows.append((post_id, "t2l_linked_lyricist_slug", info["lyricist_slug"]))
            if "singer_slug" in info:
                meta_rows.append((post_id, "t2l_linked_singer_slug", info["singer_slug"]))

            # Assign taxonomy terms to this person post
            term_rel_rows = []
            if "director" in roles and slug in term_maps["t2l_director"]:
                term_rel_rows.append((post_id, term_maps["t2l_director"][slug], 0))
            if "lyricist" in roles and slug in term_maps["t2l_lyricist"]:
                term_rel_rows.append((post_id, term_maps["t2l_lyricist"][slug], 0))
            if "singer" in roles and slug in term_maps["t2l_singer"]:
                term_rel_rows.append((post_id, term_maps["t2l_singer"][slug], 0))
            if term_rel_rows:
                cur.executemany(
                    f"INSERT IGNORE INTO {T('term_relationships')} (object_id,term_taxonomy_id,term_order) VALUES (%s,%s,%s)",
                    term_rel_rows,
                )

            if len(meta_rows) >= BATCH:
                cur.executemany(
                    f"INSERT INTO {T('postmeta')} (post_id,meta_key,meta_value) VALUES (%s,%s,%s)",
                    meta_rows,
                )
                meta_rows.clear()
                conn.commit()
                progress_update("People", idx + 1)

        if meta_rows:
            cur.executemany(
                f"INSERT INTO {T('postmeta')} (post_id,meta_key,meta_value) VALUES (%s,%s,%s)",
                meta_rows,
            )
        conn.commit()
        progress_done("People")
        print(f"  → {new_count} new, {skipped} skipped")
    except Exception as e:
        conn.rollback()
        print(f"  ERROR: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return slug_to_id


# ===========================================================================
# Phase 4 — Song posts (t2l_song)
# ===========================================================================

def import_songs(songs: list[dict], term_maps: dict):
    print(f"\nSongs: {len(songs)}")
    progress_start("Songs", len(songs))
    conn = get_conn()
    cur = conn.cursor()
    new_count = skipped = 0
    meta_rows: list[tuple] = []
    term_rel_rows: list[tuple] = []

    try:
        for idx, song in enumerate(tqdm(songs, desc="Songs", unit="post")):
            slug      = song.get("slug", "").strip()
            title     = song.get("title_en", "").strip() or slug
            lyrics_en = song.get("lyrics_en", "")
            lyrics_ta = song.get("lyrics_ta", "")
            singer    = song.get("singer", "")

            movie     = song.get("movie", {})
            m_slug    = movie.get("slug", "") if isinstance(movie, dict) else ""
            year      = movie.get("year", "") if isinstance(movie, dict) else ""

            lyr       = song.get("lyricist", {})
            l_slug    = lyr.get("slug", "") if isinstance(lyr, dict) else ""

            md        = song.get("music_director", {})
            d_slug    = md.get("slug", "") if isinstance(md, dict) else slugify(str(md))

            if not slug:
                continue

            existing = post_exists(cur, "t2l_song", slug)
            if existing:
                skipped += 1
                continue

            # post_content = English lyrics (theme reads this via the_content())
            post_id = insert_post(cur, title, slug, "t2l_song", lyrics_en)
            new_count += 1

            # Postmeta
            decade = f"{(int(year)//10)*10}s" if year and year.isdigit() else ""
            meta_rows += [
                (post_id, "t2l_lyrics_english", lyrics_en),
                (post_id, "t2l_lyrics_tamil",   lyrics_ta),
                (post_id, "t2l_movie_year",     year or ""),
            ]
            if song.get("title_original"):
                meta_rows.append((post_id, "t2l_title_tamil", song["title_original"]))

            # Taxonomy relationships
            for taxonomy, term_slug in [
                ("t2l_movie",    m_slug),
                ("t2l_director", d_slug),
                ("t2l_lyricist", l_slug),
            ]:
                if term_slug and term_slug in term_maps[taxonomy]:
                    term_rel_rows.append((post_id, term_maps[taxonomy][term_slug], 0))

            # Singer — can be multiple
            for sname in parse_singers(singer):
                s_slug = slugify(sname)
                if s_slug and s_slug in term_maps["t2l_singer"]:
                    term_rel_rows.append((post_id, term_maps["t2l_singer"][s_slug], 0))

            # Decade
            if decade and decade in term_maps["t2l_decade"]:
                term_rel_rows.append((post_id, term_maps["t2l_decade"][decade], 0))

            # Flush in batches
            if len(meta_rows) >= BATCH * 5:
                cur.executemany(
                    f"INSERT INTO {T('postmeta')} (post_id,meta_key,meta_value) VALUES (%s,%s,%s)",
                    meta_rows,
                )
                meta_rows.clear()
                if term_rel_rows:
                    cur.executemany(
                        f"INSERT IGNORE INTO {T('term_relationships')} (object_id,term_taxonomy_id,term_order) VALUES (%s,%s,%s)",
                        term_rel_rows,
                    )
                    term_rel_rows.clear()
                conn.commit()
                progress_update("Songs", idx + 1)

        # Final flush
        if meta_rows:
            cur.executemany(
                f"INSERT INTO {T('postmeta')} (post_id,meta_key,meta_value) VALUES (%s,%s,%s)",
                meta_rows,
            )
        if term_rel_rows:
            cur.executemany(
                f"INSERT IGNORE INTO {T('term_relationships')} (object_id,term_taxonomy_id,term_order) VALUES (%s,%s,%s)",
                term_rel_rows,
            )
        conn.commit()

        # Update term counts for all taxonomies
        for taxonomy in ["t2l_movie", "t2l_director", "t2l_singer", "t2l_lyricist", "t2l_decade"]:
            cur.execute(
                f"""UPDATE {T('term_taxonomy')} tt
                    SET count = (
                        SELECT COUNT(*) FROM {T('term_relationships')} tr
                        WHERE tr.term_taxonomy_id = tt.term_taxonomy_id
                    )
                    WHERE tt.taxonomy = %s""",
                (taxonomy,),
            )
        conn.commit()

        progress_done("Songs")
        print(f"  → {new_count} new, {skipped} skipped")
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
    print("Tamil2Lyrics Theme Importer")
    print("=" * 60)

    if not DB.get("password") or not DB.get("database"):
        print("ERROR: Set DB credentials via the dashboard (http://localhost:8080) or output/config.json")
        sys.exit(1)

    print("\nLoading scraped data...")
    songs     = load_jsonl(SONGS_FILE)
    movies    = load_jsonl(MOVIES_FILE)
    lyricists = load_jsonl(LYRICISTS_FILE)
    music_dirs = load_jsonl(MUSIC_DIRS_FILE)
    print(f"  {len(songs)} songs, {len(movies)} movies, "
          f"{len(lyricists)} lyricists, {len(music_dirs)} music directors")

    if not songs:
        print("ERROR: No songs found. Run scraper.py first.")
        sys.exit(1)

    print("\nTesting DB connection...")
    try:
        conn = get_conn()
        conn.ping()
        conn.close()
        print("  Connected OK")
    except Exception as e:
        print(f"  FAILED: {e}")
        sys.exit(1)

    # Phase 1 — All taxonomy terms
    print("\n" + "=" * 60)
    print("Phase 1: Taxonomy terms")
    print("=" * 60)
    term_maps = import_all_terms(songs, movies, music_dirs)

    # Phase 2 — Movie posts
    print("\n" + "=" * 60)
    print("Phase 2: Movie posts (t2l_movie_post)")
    print("=" * 60)
    import_movie_posts(movies, term_maps["t2l_movie"])

    # Phase 3 — Person posts
    print("\n" + "=" * 60)
    print("Phase 3: Person posts (t2l_person_post)")
    print("=" * 60)
    import_person_posts(lyricists, music_dirs, songs, term_maps)

    # Phase 4 — Songs
    print("\n" + "=" * 60)
    print("Phase 4: Song posts (t2l_song)")
    print("=" * 60)
    import_songs(songs, term_maps)

    print("\n" + "=" * 60)
    print("DONE! Go to wp-admin to verify the import.")
    print("  Songs:   /wp-admin/edit.php?post_type=t2l_song")
    print("  Movies:  /wp-admin/edit.php?post_type=t2l_movie_post")
    print("  People:  /wp-admin/edit.php?post_type=t2l_person_post")
    print("=" * 60)
    progress_finish()


if __name__ == "__main__":
    main()
