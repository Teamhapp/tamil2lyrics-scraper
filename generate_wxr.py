"""
Tamil2Lyrics → WordPress WXR Generator
========================================
Generates WordPress eXtended RSS (WXR 1.2) XML files that can be imported
via wp-admin → Tools → Import → WordPress.

Output files (in output/wxr/):
  01_terms.xml          ← All taxonomy terms (import FIRST)
  02_movies_NNN.xml     ← t2l_movie_post CPT, chunked
  03_people_NNN.xml     ← t2l_person_post CPT, chunked
  04_songs_NNN.xml      ← t2l_song CPT, chunked (largest set)

Import order matters:
  1. Import 01_terms.xml first
  2. Import 02_movies_*.xml
  3. Import 03_people_*.xml
  4. Import 04_songs_*.xml (songs reference movie/lyricist taxonomy terms)

Usage:
    python generate_wxr.py

Edit the CONFIG section below to set your site URL.
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from xml.sax.saxutils import escape

# ===========================================================================
# CONFIG
# ===========================================================================

SITE_URL   = "https://your-site.com"   # ← change to your WordPress URL
AUTHOR     = "admin"                   # WordPress username
CHUNK_SIZE = 1000                      # posts per XML file

OUTPUT_DIR = Path("output")
WXR_DIR    = OUTPUT_DIR / "wxr"

SONGS_FILE       = OUTPUT_DIR / "songs_final.jsonl"
MOVIES_FILE      = OUTPUT_DIR / "movies.jsonl"
LYRICISTS_FILE   = OUTPUT_DIR / "lyricists.jsonl"
MUSIC_DIRS_FILE  = OUTPUT_DIR / "music_directors.jsonl"

# ===========================================================================
# Helpers
# ===========================================================================

NOW     = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
NOW_WP  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

_id_counter = 10000   # start post IDs well above typical WP post IDs


def next_id() -> int:
    global _id_counter
    _id_counter += 1
    return _id_counter


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def cdata(text: str) -> str:
    """Wrap text in CDATA, escaping any ]]> sequences."""
    if text is None:
        text = ""
    text = str(text).replace("]]>", "]]]]><![CDATA[>")
    return f"<![CDATA[{text}]]>"


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        print(f"  WARNING: {path} not found")
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def parse_singers(singer_str: str) -> list[str]:
    if not singer_str:
        return []
    return [s.strip() for s in re.split(r"[,&]", singer_str) if s.strip()]


# ===========================================================================
# WXR document builder
# ===========================================================================

WXR_HEADER = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
  xmlns:excerpt="http://wordpress.org/export/1.2/excerpt/"
  xmlns:content="http://purl.org/rss/1.0/modules/content/"
  xmlns:wfw="http://wellformedweb.org/CommentAPI/"
  xmlns:dc="http://purl.org/dc/elements/1.1/"
  xmlns:wp="http://wordpress.org/export/1.2/">
<channel>
  <title>Tamil2Lyrics Import</title>
  <link>{site_url}</link>
  <description></description>
  <pubDate>{now}</pubDate>
  <language>en-US</language>
  <wp:wxr_version>1.2</wp:wxr_version>
  <wp:base_site_url>{site_url}</wp:base_site_url>
  <wp:base_blog_url>{site_url}</wp:base_blog_url>
  <wp:author>
    <wp:author_id>1</wp:author_id>
    <wp:author_login>{cdata_author}</wp:author_login>
    <wp:author_email>{cdata_email}</wp:author_email>
    <wp:author_display_name>{cdata_author}</wp:author_display_name>
    <wp:author_first_name><![CDATA[]]></wp:author_first_name>
    <wp:author_last_name><![CDATA[]]></wp:author_last_name>
  </wp:author>
"""

WXR_FOOTER = """</channel>
</rss>"""


def wxr_header(include_terms: list[tuple] = None) -> str:
    """Return WXR header XML, optionally including <wp:term> entries."""
    h = WXR_HEADER.format(
        site_url=SITE_URL,
        now=NOW,
        cdata_author=cdata(AUTHOR),
        cdata_email=cdata(f"{AUTHOR}@example.com"),
    )
    if include_terms:
        for taxonomy, slug, name in include_terms:
            h += wxr_term(taxonomy, slug, name)
    return h


def wxr_term(taxonomy: str, slug: str, name: str) -> str:
    return (
        f"  <wp:term>"
        f"<wp:term_taxonomy>{cdata(taxonomy)}</wp:term_taxonomy>"
        f"<wp:term_slug>{cdata(slug)}</wp:term_slug>"
        f"<wp:term_name>{cdata(name)}</wp:term_name>"
        f"</wp:term>\n"
    )


def wxr_item(
    post_id: int,
    title: str,
    slug: str,
    post_type: str,
    content: str = "",
    excerpt: str = "",
    meta: dict = None,
    tax_terms: list[tuple] = None,   # [(domain, nicename, display_name), ...]
) -> str:
    link = f"{SITE_URL}/{slug}/"
    lines = [
        "  <item>",
        f"    <title>{escape(title)}</title>",
        f"    <link>{link}</link>",
        f"    <pubDate>{NOW}</pubDate>",
        f"    <dc:creator>{cdata(AUTHOR)}</dc:creator>",
        f"    <guid isPermaLink=\"false\">{SITE_URL}/?p={post_id}</guid>",
        "    <description></description>",
        f"    <content:encoded>{cdata(content)}</content:encoded>",
        f"    <excerpt:encoded>{cdata(excerpt)}</excerpt:encoded>",
        f"    <wp:post_id>{post_id}</wp:post_id>",
        f"    <wp:post_date>{cdata(NOW_WP)}</wp:post_date>",
        f"    <wp:post_date_gmt>{cdata(NOW_WP)}</wp:post_date_gmt>",
        "    <wp:comment_status><![CDATA[closed]]></wp:comment_status>",
        "    <wp:ping_status><![CDATA[closed]]></wp:ping_status>",
        f"    <wp:post_name>{cdata(slug)}</wp:post_name>",
        "    <wp:status><![CDATA[publish]]></wp:status>",
        "    <wp:post_parent>0</wp:post_parent>",
        "    <wp:menu_order>0</wp:menu_order>",
        f"    <wp:post_type>{cdata(post_type)}</wp:post_type>",
        "    <wp:post_password><![CDATA[]]></wp:post_password>",
        "    <wp:is_sticky>0</wp:is_sticky>",
    ]

    # Taxonomy category elements
    for domain, nicename, display in (tax_terms or []):
        lines.append(
            f'    <category domain="{escape(domain)}" nicename="{escape(nicename)}">'
            f"{cdata(display)}</category>"
        )

    # Post meta
    for key, val in (meta or {}).items():
        if val is None or val == "":
            continue
        lines += [
            "    <wp:postmeta>",
            f"      <wp:meta_key>{cdata(key)}</wp:meta_key>",
            f"      <wp:meta_value>{cdata(val)}</wp:meta_value>",
            "    </wp:postmeta>",
        ]

    lines.append("  </item>")
    return "\n".join(lines) + "\n"


# ===========================================================================
# Chunk writer
# ===========================================================================

def write_chunks(prefix: str, items_xml: list[str], all_terms: list[tuple]):
    """Write items_xml into chunked WXR files, each including all_terms."""
    total   = len(items_xml)
    n_files = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
    for i in range(n_files):
        chunk     = items_xml[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE]
        file_name = WXR_DIR / f"{prefix}_{i+1:03d}.xml"
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(wxr_header(all_terms))
            for item in chunk:
                f.write(item)
            f.write(WXR_FOOTER)
        print(f"  Wrote {file_name.name}  ({len(chunk)} posts)")


# ===========================================================================
# Phase 1 — Collect all taxonomy terms
# ===========================================================================

def collect_terms(songs: list[dict], movies: list[dict]) -> dict[str, dict[str, str]]:
    """
    Returns { taxonomy: { slug: name } } for all 5 taxonomies.
    """
    terms: dict[str, dict[str, str]] = {
        "t2l_movie":    {},
        "t2l_director": {},
        "t2l_singer":   {},
        "t2l_lyricist": {},
        "t2l_decade":   {},
    }

    # Movies from movies.jsonl (most complete)
    for m in movies:
        slug = m.get("movie_slug", "").strip()
        name = m.get("name", "").strip()
        if slug and name:
            terms["t2l_movie"][slug] = name

    # Movies from songs (catch any extras)
    for song in songs:
        mv = song.get("movie", {})
        slug = mv.get("slug", "") if isinstance(mv, dict) else ""
        name = mv.get("name", "") if isinstance(mv, dict) else ""
        year = mv.get("year", "") if isinstance(mv, dict) else ""
        if slug and name:
            terms["t2l_movie"].setdefault(slug, name)
        if year and year.isdigit():
            decade = f"{(int(year)//10)*10}s"
            terms["t2l_decade"][decade] = decade

    for song in songs:
        # Director
        md = song.get("music_director", {})
        d_slug = md.get("slug", "") if isinstance(md, dict) else slugify(str(md))
        d_name = md.get("name", "") if isinstance(md, dict) else str(md)
        if d_slug and d_name:
            terms["t2l_director"][d_slug] = d_name

        # Lyricist
        lyr = song.get("lyricist", {})
        l_slug = lyr.get("slug", "") if isinstance(lyr, dict) else ""
        l_name = lyr.get("name", "") if isinstance(lyr, dict) else str(lyr)
        if l_slug and l_name:
            terms["t2l_lyricist"][l_slug] = l_name

        # Singers
        for sname in parse_singers(song.get("singer", "")):
            s_slug = slugify(sname)
            if s_slug:
                terms["t2l_singer"][s_slug] = sname

    for tx, d in terms.items():
        print(f"  {tx}: {len(d)} terms")

    return terms


# ===========================================================================
# Phase 2 — terms-only XML (import this FIRST)
# ===========================================================================

def generate_terms_file(terms: dict[str, dict[str, str]]):
    out = WXR_DIR / "01_terms.xml"
    all_term_tuples = [
        (tx, slug, name)
        for tx, d in terms.items()
        for slug, name in d.items()
    ]
    with open(out, "w", encoding="utf-8") as f:
        f.write(wxr_header(all_term_tuples))
        f.write(WXR_FOOTER)
    print(f"  Wrote {out.name}  ({len(all_term_tuples)} terms)")


# ===========================================================================
# Phase 3 — Movie posts
# ===========================================================================

def generate_movie_posts(movies: list[dict], terms: dict) -> list[str]:
    items = []
    for m in movies:
        slug = m.get("movie_slug", "").strip()
        name = m.get("name", "").strip() or slug
        year = m.get("year", "")
        if not slug:
            continue

        pid = next_id()
        meta = {
            "t2l_film_year":       year,
            "t2l_linked_tax_slug": slug,
            "t2l_song_count":      str(len(m.get("songs", []))),
        }
        items.append(wxr_item(pid, name, slug, "t2l_movie_post", meta=meta))
    return items


# ===========================================================================
# Phase 4 — Person posts (directors + lyricists + singers merged)
# ===========================================================================

def generate_person_posts(
    lyricists: list[dict],
    music_dirs: list[dict],
    songs: list[dict],
    terms: dict,
) -> list[str]:
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

    for song in songs:
        for sname in parse_singers(song.get("singer", "")):
            s_slug = slugify(sname)
            if not s_slug:
                continue
            p = people.setdefault(s_slug, {"name": sname, "roles": set()})
            p["roles"].add("singer")
            p["singer_slug"] = s_slug

    items = []
    for slug, info in people.items():
        name  = info["name"]
        roles = info["roles"]
        pid   = next_id()

        meta = {"t2l_person_type": ",".join(sorted(roles))}
        if "dir_slug" in info:
            meta["t2l_linked_dir_slug"] = info["dir_slug"]
        if "lyricist_slug" in info:
            meta["t2l_linked_lyricist_slug"] = info["lyricist_slug"]
        if "singer_slug" in info:
            meta["t2l_linked_singer_slug"] = info["singer_slug"]

        # Taxonomy terms for this person
        tax_terms = []
        if "director" in roles and slug in terms["t2l_director"]:
            tax_terms.append(("t2l_director", slug, terms["t2l_director"][slug]))
        if "lyricist" in roles and slug in terms["t2l_lyricist"]:
            tax_terms.append(("t2l_lyricist", slug, terms["t2l_lyricist"][slug]))
        if "singer" in roles and slug in terms["t2l_singer"]:
            tax_terms.append(("t2l_singer", slug, terms["t2l_singer"][slug]))

        items.append(wxr_item(pid, name, slug, "t2l_person_post", meta=meta, tax_terms=tax_terms))

    return items


# ===========================================================================
# Phase 5 — Song posts
# ===========================================================================

def generate_song_posts(songs: list[dict], terms: dict) -> list[str]:
    items = []
    for song in songs:
        slug     = song.get("slug", "").strip()
        title    = song.get("title_en", "").strip() or slug
        lyrics_en = song.get("lyrics_en", "")
        lyrics_ta = song.get("lyrics_ta", "")
        singer    = song.get("singer", "")

        movie  = song.get("movie", {})
        m_slug = movie.get("slug", "") if isinstance(movie, dict) else ""
        year   = movie.get("year", "") if isinstance(movie, dict) else ""

        lyr    = song.get("lyricist", {})
        l_slug = lyr.get("slug", "") if isinstance(lyr, dict) else ""

        md     = song.get("music_director", {})
        d_slug = md.get("slug", "") if isinstance(md, dict) else slugify(str(md))

        if not slug:
            continue

        pid = next_id()

        decade = f"{(int(year)//10)*10}s" if year and year.isdigit() else ""

        meta = {
            "t2l_lyrics_english": lyrics_en,
            "t2l_lyrics_tamil":   lyrics_ta,
            "t2l_movie_year":     year,
        }
        if song.get("title_original"):
            meta["t2l_title_tamil"] = song["title_original"]

        # Taxonomy terms
        tax_terms = []
        if m_slug and m_slug in terms["t2l_movie"]:
            tax_terms.append(("t2l_movie", m_slug, terms["t2l_movie"][m_slug]))
        if d_slug and d_slug in terms["t2l_director"]:
            tax_terms.append(("t2l_director", d_slug, terms["t2l_director"][d_slug]))
        if l_slug and l_slug in terms["t2l_lyricist"]:
            tax_terms.append(("t2l_lyricist", l_slug, terms["t2l_lyricist"][l_slug]))
        if decade and decade in terms["t2l_decade"]:
            tax_terms.append(("t2l_decade", slugify(decade), decade))

        for sname in parse_singers(singer):
            s_slug = slugify(sname)
            if s_slug and s_slug in terms["t2l_singer"]:
                tax_terms.append(("t2l_singer", s_slug, terms["t2l_singer"][s_slug]))

        items.append(wxr_item(
            pid, title, slug, "t2l_song",
            content=lyrics_en,
            meta=meta,
            tax_terms=tax_terms,
        ))
    return items


# ===========================================================================
# Main
# ===========================================================================

def main():
    if SITE_URL == "https://your-site.com":
        print("WARNING: Edit SITE_URL in generate_wxr.py before importing to WordPress.")
        print("         Continuing anyway — you can import and update URLs later.\n")

    WXR_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading data...")
    songs     = load_jsonl(SONGS_FILE)
    movies    = load_jsonl(MOVIES_FILE)
    lyricists = load_jsonl(LYRICISTS_FILE)
    music_dirs = load_jsonl(MUSIC_DIRS_FILE)
    print(f"  {len(songs):,} songs, {len(movies):,} movies, "
          f"{len(lyricists):,} lyricists, {len(music_dirs):,} music directors")

    if not songs:
        print("ERROR: No songs found. Run scraper.py first.")
        sys.exit(1)

    # Collect all taxonomy terms
    print("\nCollecting taxonomy terms...")
    terms = collect_terms(songs, movies)

    # Flatten term list for inclusion in every file
    all_terms = [
        (tx, slug, name)
        for tx, d in terms.items()
        for slug, name in d.items()
    ]

    # 01 — Terms file
    print("\nGenerating 01_terms.xml...")
    generate_terms_file(terms)

    # 02 — Movie posts
    print("\nGenerating movie posts...")
    movie_items = generate_movie_posts(movies, terms)
    print(f"  {len(movie_items):,} movie posts")
    write_chunks("02_movies", movie_items, all_terms)

    # 03 — Person posts
    print("\nGenerating person posts...")
    person_items = generate_person_posts(lyricists, music_dirs, songs, terms)
    print(f"  {len(person_items):,} person posts")
    write_chunks("03_people", person_items, all_terms)

    # 04 — Song posts
    print("\nGenerating song posts...")
    song_items = generate_song_posts(songs, terms)
    print(f"  {len(song_items):,} song posts")
    write_chunks("04_songs", song_items, all_terms)

    # Summary
    all_files = sorted(WXR_DIR.glob("*.xml"))
    total_size = sum(f.stat().st_size for f in all_files)
    print(f"\n{'='*55}")
    print(f"Done! {len(all_files)} WXR files in output/wxr/")
    print(f"Total size: {total_size/1024/1024:.1f} MB")
    print(f"\nImport order in wp-admin > Tools > Import > WordPress:")
    for i, f in enumerate(all_files, 1):
        sz = f.stat().st_size / 1024
        print(f"  {i}. {f.name}  ({sz:.0f} KB)")
    print(f"\nNote: Increase PHP upload_max_filesize and memory_limit")
    print(f"      if large files fail. Or use WP-CLI:")
    print(f"      wp import output/wxr/04_songs_001.xml --authors=skip")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
