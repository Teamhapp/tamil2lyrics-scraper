"""
Tamil2Lyrics.com Scraper
========================
Scrapes all ~20,364 songs and ~4,506 movies from tamil2lyrics.com
using sitemap-seeded concurrent workers with checkpoint/resume support.

Uses curl for HTTP (works reliably on this Windows environment)
and ThreadPoolExecutor for concurrency.

Usage:
    pip install -r requirements.txt
    python scraper.py
"""

import json
import re
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://www.tamil2lyrics.com"
OUTPUT_DIR = Path("output")
CHECKPOINT_DIR = OUTPUT_DIR / "checkpoints"

LYRICS_SITEMAPS = [
    f"{BASE_URL}/lyrics-sitemap.xml",
    f"{BASE_URL}/lyrics-sitemap2.xml",
    f"{BASE_URL}/lyrics-sitemap3.xml",
    f"{BASE_URL}/lyrics-sitemap4.xml",
    f"{BASE_URL}/lyrics-sitemap5.xml",
]
ALBUM_SITEMAP = f"{BASE_URL}/album-sitemap.xml"
ARTIST_SITEMAP = f"{BASE_URL}/artist-sitemap.xml"
MUSIC_DIR_SITEMAP = f"{BASE_URL}/music_director-sitemap.xml"

WORKERS = 5
DELAY_PER_WORKER = 1.5          # seconds between requests per worker
MAX_RETRIES = 3
CHECKPOINT_EVERY = 500          # save progress every N items

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Thread-safe write lock for JSONL output
_write_lock = threading.Lock()

# ---------------------------------------------------------------------------
# HTTP via curl
# ---------------------------------------------------------------------------

def curl_fetch(url: str, timeout: int = 30) -> str | None:
    """Fetch a URL using curl subprocess. Returns HTML string or None."""
    for attempt in range(MAX_RETRIES):
        try:
            result = subprocess.run(
                [
                    "curl", "-s", "-L",
                    "--max-time", str(timeout),
                    "-H", f"User-Agent: {USER_AGENT}",
                    "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "-w", "\n%{http_code}",
                    url,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout + 10,
            )
            # Last line is the status code
            lines = result.stdout.rsplit("\n", 1)
            if len(lines) == 2:
                body, status_str = lines
                status = int(status_str.strip())
            else:
                body = result.stdout
                status = 0

            if status == 200:
                return body
            elif status in (429, 503):
                wait = min(2 ** (attempt + 2), 60)
                time.sleep(wait)
            else:
                return None
        except (subprocess.TimeoutExpired, Exception):
            time.sleep(2 ** (attempt + 1))
    return None

# ---------------------------------------------------------------------------
# Sitemap parsing
# ---------------------------------------------------------------------------

def extract_urls_from_sitemap_xml(xml_text: str) -> list[str]:
    """Parse a sitemap XML and return all <loc> URLs."""
    root = ET.fromstring(xml_text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    return [loc.text.strip() for loc in root.findall(".//sm:loc", ns) if loc.text]


def fetch_sitemap_urls(sitemap_url: str) -> list[str]:
    """Download a single sitemap and extract URLs."""
    print(f"  Fetching {sitemap_url}...")
    text = curl_fetch(sitemap_url, timeout=60)
    if not text:
        print(f"  FAILED: {sitemap_url}")
        return []
    return extract_urls_from_sitemap_xml(text)


def collect_all_seed_urls() -> tuple[list[str], list[str], list[str], list[str]]:
    """Fetch all sitemaps and return (song_urls, movie_urls, lyricist_urls, music_dir_urls)."""
    song_urls = []
    for sm_url in LYRICS_SITEMAPS:
        urls = fetch_sitemap_urls(sm_url)
        song_urls.extend(urls)
        print(f"    → {len(urls)} URLs")

    movie_urls = fetch_sitemap_urls(ALBUM_SITEMAP)
    print(f"    → {len(movie_urls)} movie URLs")

    lyricist_urls = fetch_sitemap_urls(ARTIST_SITEMAP)
    print(f"    → {len(lyricist_urls)} lyricist URLs")

    music_dir_urls = fetch_sitemap_urls(MUSIC_DIR_SITEMAP)
    print(f"    → {len(music_dir_urls)} music director URLs")

    return song_urls, movie_urls, lyricist_urls, music_dir_urls

# ---------------------------------------------------------------------------
# Song page parser
# ---------------------------------------------------------------------------

def parse_song_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    # Post ID from body class
    body = soup.find("body")
    body_classes = body.get("class", []) if body else []
    post_id = next(
        (c.replace("postid-", "") for c in body_classes if c.startswith("postid-")),
        None,
    )

    # Title
    h1 = soup.find("h1")
    title_raw = h1.get_text(strip=True) if h1 else ""
    title_clean = re.sub(r"\s*Song Lyrics\s*$", "", title_raw, flags=re.I).strip()

    # Slug from URL
    slug = url.rstrip("/").split("/")[-1]

    # Lyricist from <h3>
    h3 = soup.find("h3")
    lyricist = h3.get_text(strip=True) if h3 else ""

    # Movie link
    movie_link = soup.find("a", href=re.compile(r"/movies/"))
    movie_name = movie_link.get_text(strip=True) if movie_link else ""
    movie_slug = (
        movie_link["href"].split("/movies/")[-1].strip("/") if movie_link else ""
    )

    # Lyricist link
    lyr_link = soup.find("a", href=re.compile(r"/Lyricist/"))
    lyricist_slug = (
        lyr_link["href"].split("/Lyricist/")[-1].strip("/") if lyr_link else ""
    )

    # Tab content — English and Tamil lyrics
    tabs = soup.select(".tabcontent")
    lyrics_en_raw = tabs[0].get_text("\n") if len(tabs) > 0 else ""
    lyrics_ta_raw = tabs[1].get_text("\n") if len(tabs) > 1 else ""

    meta_en, lyrics_en = _parse_meta_and_lyrics(lyrics_en_raw)
    _, lyrics_ta = _parse_meta_and_lyrics(lyrics_ta_raw)

    singer = meta_en.get("singers", meta_en.get("singer", "")).strip()
    music_director = meta_en.get("music by", meta_en.get("music director", "")).strip()

    # Related songs
    related = [a["href"] for a in soup.select("ul.related-list a") if a.get("href")]

    return {
        "post_id": post_id,
        "url": url,
        "slug": slug,
        "title_en": title_clean,
        "title_original": title_raw,
        "lyricist": lyricist,
        "lyricist_slug": lyricist_slug,
        "movie": movie_name,
        "movie_slug": movie_slug,
        "singer": singer,
        "music_director": music_director,
        "lyrics_en": lyrics_en,
        "lyrics_ta": lyrics_ta,
        "related_songs": related,
    }


def _parse_meta_and_lyrics(raw: str) -> tuple[dict, str]:
    """Split the raw tab text into metadata dict and lyrics body.

    Handles two formats:
      - "Singers : Dhanush & Dhee"  (key:value on same line)
      - "Singers :"  /  "Dhanush & Dhee"  (key on one line, value on next)
    """
    lines = [line.strip() for line in raw.strip().splitlines() if line.strip()]
    meta = {}
    i = 0
    while i < len(lines) and i < 8:
        line = lines[i]
        if ":" in line:
            key, _, val = line.partition(":")
            key = key.strip().lower()
            val = val.strip()
            if key in ("singers", "singer", "music by", "music director"):
                if val:
                    # key: value on same line
                    meta[key] = val
                    i += 1
                elif i + 1 < len(lines):
                    # key: on one line, value on next
                    meta[key] = lines[i + 1].strip()
                    i += 2
                else:
                    i += 1
            else:
                # Not a known meta key — this is lyrics
                break
        else:
            break
    return meta, "\n".join(lines[i:])

# ---------------------------------------------------------------------------
# Movie page parser
# ---------------------------------------------------------------------------

def parse_movie_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    h1 = soup.find("h1")
    raw_title = h1.get_text(strip=True) if h1 else ""

    year_match = re.search(r"\((\d{4})\)", raw_title)
    year = year_match.group(1) if year_match else ""
    name = re.sub(r"\s*\(\d{4}\)\s*", "", raw_title).strip()

    movie_slug = url.rstrip("/").split("/movies/")[-1].strip("/")

    songs = []
    seen_slugs = set()
    for a in soup.select('a[href*="/lyrics/"]'):
        text = a.get_text(strip=True)
        if not text or "Click Here" in text:
            continue
        s_slug = a["href"].split("/lyrics/")[-1].strip("/")
        if s_slug and s_slug not in seen_slugs:
            seen_slugs.add(s_slug)
            songs.append({"title": text, "slug": s_slug})

    lyricists = {}
    for a in soup.select('a[href*="/Lyricist/"]'):
        l_slug = a["href"].split("/Lyricist/")[-1].strip("/")
        if l_slug:
            lyricists[l_slug] = a.get_text(strip=True)

    return {
        "movie_slug": movie_slug,
        "name": name,
        "year": year,
        "url": url,
        "songs": songs,
        "lyricists": [{"slug": s, "name": n} for s, n in lyricists.items()],
    }

# ---------------------------------------------------------------------------
# Lyricist page parser
# ---------------------------------------------------------------------------

def parse_lyricist_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    slug = url.rstrip("/").split("/Lyricist/")[-1].strip("/")

    # Name from <h1>
    h1 = soup.find("h1")
    name = h1.get_text(strip=True) if h1 else ""

    # All song links on this page
    songs = []
    seen = set()
    for a in soup.select('a[href*="/lyrics/"]'):
        text = a.get_text(strip=True)
        href = a.get("href", "")
        s_slug = href.rstrip("/").split("/lyrics/")[-1].strip("/")
        if s_slug and s_slug not in seen and text:
            seen.add(s_slug)
            songs.append({
                "slug": s_slug,
                "title": re.sub(r"\s*Song Lyrics\s*$", "", text, flags=re.I).strip(),
            })

    return {
        "slug": slug,
        "name": name,
        "url": url,
        "songs": songs,
        "song_count": len(songs),
    }


# ---------------------------------------------------------------------------
# Music director page parser
# ---------------------------------------------------------------------------

def parse_music_director_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    slug = url.rstrip("/").split("/music_director/")[-1].strip("/")

    # Name from <title>: "Name | Tamil Song Lyrics - ..."
    title_tag = soup.find("title")
    name = title_tag.text.split("|")[0].strip() if title_tag else ""

    # Movies listed as h3 > a[href*="/movies/"]
    movies = []
    seen = set()
    for h3 in soup.find_all("h3"):
        a = h3.find("a", href=re.compile(r"/movies/"))
        if a:
            m_slug = a["href"].rstrip("/").split("/movies/")[-1].strip("/")
            m_name = a.get_text(strip=True)
            if m_slug and m_slug not in seen:
                seen.add(m_slug)
                movies.append({"slug": m_slug, "name": m_name})

    return {
        "slug": slug,
        "name": name,
        "url": url,
        "movies": movies,
        "movie_count": len(movies),
    }


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_checkpoint(name: str) -> set[str]:
    """Load set of already-scraped URLs from checkpoint file."""
    path = CHECKPOINT_DIR / f"{name}_done.txt"
    if path.exists():
        return set(path.read_text(encoding="utf-8").strip().splitlines())
    return set()


def save_checkpoint(name: str, done_urls: set[str]):
    path = CHECKPOINT_DIR / f"{name}_done.txt"
    path.write_text("\n".join(done_urls), encoding="utf-8")


def append_jsonl(filepath: Path, record: dict):
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with _write_lock:
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(line)

# ---------------------------------------------------------------------------
# Worker functions
# ---------------------------------------------------------------------------

def process_song(url: str, output_file: Path) -> tuple[str, bool]:
    """Fetch and parse a single song page. Returns (url, success)."""
    time.sleep(DELAY_PER_WORKER)
    html = curl_fetch(url)
    if not html:
        return url, False
    try:
        record = parse_song_page(html, url)
        append_jsonl(output_file, record)
        return url, True
    except Exception as e:
        tqdm.write(f"Parse error {url}: {e}")
        return url, False


def process_movie(url: str, output_file: Path) -> tuple[str, bool]:
    """Fetch and parse a single movie page. Returns (url, success)."""
    time.sleep(DELAY_PER_WORKER)
    html = curl_fetch(url)
    if not html:
        return url, False
    try:
        record = parse_movie_page(html, url)
        append_jsonl(output_file, record)
        return url, True
    except Exception as e:
        tqdm.write(f"Parse error {url}: {e}")
        return url, False


def process_lyricist(url: str, output_file: Path) -> tuple[str, bool]:
    """Fetch and parse a single lyricist page. Returns (url, success)."""
    time.sleep(DELAY_PER_WORKER)
    html = curl_fetch(url)
    if not html:
        return url, False
    try:
        record = parse_lyricist_page(html, url)
        append_jsonl(output_file, record)
        return url, True
    except Exception as e:
        tqdm.write(f"Parse error {url}: {e}")
        return url, False


def process_music_director(url: str, output_file: Path) -> tuple[str, bool]:
    """Fetch and parse a single music director page. Returns (url, success)."""
    time.sleep(DELAY_PER_WORKER)
    html = curl_fetch(url)
    if not html:
        return url, False
    try:
        record = parse_music_director_page(html, url)
        append_jsonl(output_file, record)
        return url, True
    except Exception as e:
        tqdm.write(f"Parse error {url}: {e}")
        return url, False

# ---------------------------------------------------------------------------
# Main scraping orchestration
# ---------------------------------------------------------------------------

def scrape_collection(
    name: str,
    urls: list[str],
    worker_fn,
    output_file: Path,
):
    """Generic scraper for a collection of URLs with checkpoint/resume."""
    done = load_checkpoint(name)
    remaining = [u for u in urls if u not in done]

    print(f"\n{name.title()}: {len(done)} already done, {len(remaining)} remaining")

    if not remaining:
        print(f"  Nothing to do!")
        return

    pbar = tqdm(total=len(urls), initial=len(done), desc=name.title(), unit="page")
    checkpoint_counter = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(worker_fn, url, output_file): url
            for url in remaining
        }

        for future in as_completed(futures):
            url = futures[future]
            try:
                _, success = future.result()
            except Exception as e:
                tqdm.write(f"Worker error {url}: {e}")

            done.add(url)
            checkpoint_counter += 1
            pbar.update(1)

            if checkpoint_counter % CHECKPOINT_EVERY == 0:
                save_checkpoint(name, done)
                tqdm.write(f"  [checkpoint] {len(done)} {name} saved")

    save_checkpoint(name, done)
    pbar.close()
    print(f"{name.title()} complete: {len(done)} total")


def enrich_songs_with_year():
    """Post-process: add movie year to each song record."""
    movies_file = OUTPUT_DIR / "movies.jsonl"
    songs_file = OUTPUT_DIR / "songs.jsonl"

    if not movies_file.exists() or not songs_file.exists():
        print("Skipping enrichment — missing output files")
        return

    # Build movie_slug → year map
    movie_years = {}
    with open(movies_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                m = json.loads(line)
                if m.get("year"):
                    movie_years[m["movie_slug"]] = m["year"]

    # Rewrite songs with year field
    enriched_file = OUTPUT_DIR / "songs_enriched.jsonl"
    enriched_count = 0
    with open(songs_file, "r", encoding="utf-8") as fin, \
         open(enriched_file, "w", encoding="utf-8") as fout:
        for line in fin:
            if line.strip():
                song = json.loads(line)
                slug = song.get("movie_slug", "")
                song["year"] = movie_years.get(slug, "")
                if song["year"]:
                    enriched_count += 1
                fout.write(json.dumps(song, ensure_ascii=False) + "\n")

    # Replace original with enriched
    songs_file.unlink()
    enriched_file.rename(songs_file)
    print(f"Enrichment: {enriched_count} songs got year from movie data")


def main():
    # Ensure output dirs exist
    OUTPUT_DIR.mkdir(exist_ok=True)
    CHECKPOINT_DIR.mkdir(exist_ok=True)

    # Phase 1: Collect seed URLs from sitemaps
    print("=" * 60)
    print("Phase 1: Fetching sitemaps")
    print("=" * 60)
    song_urls, movie_urls, lyricist_urls, music_dir_urls = collect_all_seed_urls()
    print(
        f"\nTotal: {len(song_urls)} songs, {len(movie_urls)} movies, "
        f"{len(lyricist_urls)} lyricists, {len(music_dir_urls)} music directors"
    )

    if not song_urls:
        print("ERROR: No song URLs found. Check network connectivity.")
        sys.exit(1)

    # Phase 2: Scrape songs
    print("\n" + "=" * 60)
    print("Phase 2: Scraping song pages")
    print("=" * 60)
    scrape_collection("songs", song_urls, process_song, OUTPUT_DIR / "songs.jsonl")

    # Phase 3: Scrape movies
    print("\n" + "=" * 60)
    print("Phase 3: Scraping movie pages")
    print("=" * 60)
    scrape_collection("movies", movie_urls, process_movie, OUTPUT_DIR / "movies.jsonl")

    # Phase 4: Scrape lyricists
    print("\n" + "=" * 60)
    print("Phase 4: Scraping lyricist pages")
    print("=" * 60)
    scrape_collection("lyricists", lyricist_urls, process_lyricist, OUTPUT_DIR / "lyricists.jsonl")

    # Phase 5: Scrape music directors
    print("\n" + "=" * 60)
    print("Phase 5: Scraping music director pages")
    print("=" * 60)
    scrape_collection("music_directors", music_dir_urls, process_music_director, OUTPUT_DIR / "music_directors.jsonl")

    # Phase 6: Enrich songs with movie year
    print("\n" + "=" * 60)
    print("Phase 6: Enriching songs with movie year")
    print("=" * 60)
    enrich_songs_with_year()

    # Summary
    counts = {}
    for name, fname in [
        ("songs", "songs.jsonl"),
        ("movies", "movies.jsonl"),
        ("lyricists", "lyricists.jsonl"),
        ("music_directors", "music_directors.jsonl"),
    ]:
        f = OUTPUT_DIR / fname
        counts[name] = sum(1 for _ in open(f, encoding="utf-8")) if f.exists() else 0

    print("\n" + "=" * 60)
    print(f"DONE!")
    for name, count in counts.items():
        print(f"  {name}: {count}")
    print(f"Output: {OUTPUT_DIR.resolve()}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
