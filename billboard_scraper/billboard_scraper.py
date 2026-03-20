"""
Billboard Chart Scraper
Pulls chart data into a PostgreSQL 'billboard' database using a star schema.
"""

import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
import billboard
import psycopg2
import psycopg2.extras
import requests

# Load .env from the same directory as this script
load_dotenv(Path(__file__).parent / ".env")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME", "billboard"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.environ["DB_PASSWORD"],  # required — no default
}

WEEKS_BACK = 4
SLEEP_BB = 2.0   # seconds between Billboard requests
SLEEP_MB = 1.1   # seconds between MusicBrainz requests

CHARTS = [
    ("hot-100",                      "Hot 100",                    "Pop/General",      "Singles"),
    ("billboard-200",                "Billboard 200",              "General",          "Albums"),
    ("hot-country-songs",            "Hot Country Songs",          "Country",          "Singles"),
    ("hot-r-and-b-hip-hop-songs",    "Hot R&B/Hip-Hop Songs",      "R&B/Hip-Hop",      "Singles"),
    ("hot-rock-and-alternative-songs","Hot Rock & Alternative Songs","Rock/Alternative","Singles"),
    ("hot-latin-songs",              "Hot Latin Songs",            "Latin",            "Singles"),
    ("pop-songs",                    "Pop Songs",                  "Pop",              "Airplay"),
    ("rap-song",                     "Rap Songs",                  "Hip-Hop/Rap",      "Singles"),
    ("alternative-songs",            "Alternative Songs",          "Alternative",      "Airplay"),
    ("dance-club-songs",             "Dance Club Songs",           "Dance/Electronic", "Singles"),
]

MB_SEARCH_URL = "https://musicbrainz.org/ws/2/artist/"
MB_HEADERS = {"User-Agent": "BillboardScraper/1.0 (educational project)"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_id      SERIAL PRIMARY KEY,
    full_date    DATE    NOT NULL UNIQUE,
    week_number  INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    month_name   VARCHAR(20) NOT NULL,
    quarter      INTEGER NOT NULL,
    year         INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_chart (
    chart_id        SERIAL PRIMARY KEY,
    chart_slug      VARCHAR(100) NOT NULL UNIQUE,
    chart_name      VARCHAR(200) NOT NULL,
    chart_genre     VARCHAR(100),
    chart_category  VARCHAR(50),
    chart_frequency VARCHAR(20) DEFAULT 'Weekly'
);

CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id   SERIAL PRIMARY KEY,
    artist_name VARCHAR(500) NOT NULL UNIQUE,
    genre       VARCHAR(200),
    mb_artist_id VARCHAR(50),
    country     VARCHAR(100),
    begin_date  DATE
);

CREATE TABLE IF NOT EXISTS dim_song (
    song_id     SERIAL PRIMARY KEY,
    song_title  VARCHAR(500) NOT NULL,
    artist_id   INTEGER NOT NULL REFERENCES dim_artist(artist_id),
    UNIQUE (song_title, artist_id)
);

CREATE TABLE IF NOT EXISTS fact_chart_position (
    position_id    SERIAL PRIMARY KEY,
    song_id        INTEGER NOT NULL REFERENCES dim_song(song_id),
    artist_id      INTEGER NOT NULL REFERENCES dim_artist(artist_id),
    chart_id       INTEGER NOT NULL REFERENCES dim_chart(chart_id),
    date_id        INTEGER NOT NULL REFERENCES dim_date(date_id),
    rank           INTEGER NOT NULL,
    last_week_rank INTEGER,
    peak_position  INTEGER,
    weeks_on_chart INTEGER,
    is_new_entry   BOOLEAN,
    UNIQUE (chart_id, date_id, rank)
);

CREATE INDEX IF NOT EXISTS idx_fact_chart_id  ON fact_chart_position(chart_id);
CREATE INDEX IF NOT EXISTS idx_fact_date_id   ON fact_chart_position(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_song_id   ON fact_chart_position(song_id);
CREATE INDEX IF NOT EXISTS idx_fact_artist_id ON fact_chart_position(artist_id);
"""

# ---------------------------------------------------------------------------
# Helpers: upserts
# ---------------------------------------------------------------------------

def upsert_date(cur, d: date) -> int:
    cur.execute(
        """
        INSERT INTO dim_date (full_date, week_number, month, month_name, quarter, year)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (full_date) DO NOTHING
        RETURNING date_id
        """,
        (
            d,
            d.isocalendar()[1],
            d.month,
            d.strftime("%B"),
            (d.month - 1) // 3 + 1,
            d.year,
        ),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT date_id FROM dim_date WHERE full_date = %s", (d,))
    return cur.fetchone()[0]


def upsert_chart(cur, slug: str, name: str, genre: str, category: str) -> int:
    cur.execute(
        """
        INSERT INTO dim_chart (chart_slug, chart_name, chart_genre, chart_category, chart_frequency)
        VALUES (%s, %s, %s, %s, 'Weekly')
        ON CONFLICT (chart_slug) DO UPDATE
            SET chart_name     = EXCLUDED.chart_name,
                chart_genre    = EXCLUDED.chart_genre,
                chart_category = EXCLUDED.chart_category
        RETURNING chart_id
        """,
        (slug, name, genre, category),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT chart_id FROM dim_chart WHERE chart_slug = %s", (slug,))
    return cur.fetchone()[0]


def upsert_artist(cur, name: str, mb_info: dict | None) -> int:
    genre = mb_info.get("genre") if mb_info else None
    mb_id = mb_info.get("mb_artist_id") if mb_info else None
    country = mb_info.get("country") if mb_info else None
    begin_date = mb_info.get("begin_date") if mb_info else None

    cur.execute(
        """
        INSERT INTO dim_artist (artist_name, genre, mb_artist_id, country, begin_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (artist_name) DO UPDATE
            SET genre        = COALESCE(EXCLUDED.genre,        dim_artist.genre),
                mb_artist_id = COALESCE(EXCLUDED.mb_artist_id, dim_artist.mb_artist_id),
                country      = COALESCE(EXCLUDED.country,      dim_artist.country),
                begin_date   = COALESCE(EXCLUDED.begin_date,   dim_artist.begin_date)
        RETURNING artist_id
        """,
        (name, genre, mb_id, country, begin_date),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT artist_id FROM dim_artist WHERE artist_name = %s", (name,))
    return cur.fetchone()[0]


def upsert_song(cur, title: str, artist_id: int) -> int:
    cur.execute(
        """
        INSERT INTO dim_song (song_title, artist_id)
        VALUES (%s, %s)
        ON CONFLICT (song_title, artist_id) DO NOTHING
        RETURNING song_id
        """,
        (title, artist_id),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "SELECT song_id FROM dim_song WHERE song_title = %s AND artist_id = %s",
        (title, artist_id),
    )
    return cur.fetchone()[0]


def upsert_fact(cur, song_id, artist_id, chart_id, date_id, entry):
    last_week = entry.lastPos if entry.lastPos != 0 else None
    cur.execute(
        """
        INSERT INTO fact_chart_position
            (song_id, artist_id, chart_id, date_id, rank, last_week_rank,
             peak_position, weeks_on_chart, is_new_entry)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (chart_id, date_id, rank) DO UPDATE
            SET song_id        = EXCLUDED.song_id,
                artist_id      = EXCLUDED.artist_id,
                last_week_rank = EXCLUDED.last_week_rank,
                peak_position  = EXCLUDED.peak_position,
                weeks_on_chart = EXCLUDED.weeks_on_chart,
                is_new_entry   = EXCLUDED.is_new_entry
        """,
        (
            song_id,
            artist_id,
            chart_id,
            date_id,
            entry.rank,
            last_week,
            entry.peakPos,
            entry.weeks,
            entry.isNew,
        ),
    )

# ---------------------------------------------------------------------------
# MusicBrainz enrichment
# ---------------------------------------------------------------------------

_mb_cache: dict[str, dict | None] = {}


def lookup_musicbrainz(artist_name: str) -> dict | None:
    if artist_name in _mb_cache:
        return _mb_cache[artist_name]

    try:
        resp = requests.get(
            MB_SEARCH_URL,
            params={"query": f'artist:"{artist_name}"', "limit": 1, "fmt": "json"},
            headers=MB_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        artists = data.get("artists", [])
        if not artists:
            _mb_cache[artist_name] = None
            return None

        a = artists[0]
        tags = sorted(a.get("tags", []), key=lambda t: t.get("count", 0), reverse=True)
        genre_str = ", ".join(t["name"] for t in tags[:3]) or None

        begin_raw = a.get("life-span", {}).get("begin")
        begin_date = None
        if begin_raw:
            # Handle year-only, year-month, or full date
            parts = begin_raw.split("-")
            try:
                if len(parts) == 3:
                    begin_date = date(int(parts[0]), int(parts[1]), int(parts[2]))
                elif len(parts) == 2:
                    begin_date = date(int(parts[0]), int(parts[1]), 1)
                elif len(parts) == 1:
                    begin_date = date(int(parts[0]), 1, 1)
            except (ValueError, IndexError):
                begin_date = None

        result = {
            "mb_artist_id": a.get("id"),
            "country": a.get("country"),
            "begin_date": begin_date,
            "genre": genre_str,
        }
        _mb_cache[artist_name] = result
        time.sleep(SLEEP_MB)
        return result

    except Exception as exc:
        log.warning("MusicBrainz lookup failed for %r: %s", artist_name, exc)
        _mb_cache[artist_name] = None
        return None

# ---------------------------------------------------------------------------
# Week date helpers
# ---------------------------------------------------------------------------

def last_n_saturdays(n: int) -> list[date]:
    """Return the last n Saturdays (most recent first)."""
    today = date.today()
    # weekday(): Monday=0 ... Saturday=5 ... Sunday=6
    days_since_saturday = (today.weekday() - 5) % 7
    last_saturday = today - timedelta(days=days_since_saturday)
    return [last_saturday - timedelta(weeks=i) for i in range(n)]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("Connecting to database %r on %s:%s", DB_CONFIG["dbname"], DB_CONFIG["host"], DB_CONFIG["port"])
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    with conn.cursor() as cur:
        log.info("Creating schema (if not exists)...")
        cur.execute(DDL)
    conn.commit()

    weeks = last_n_saturdays(WEEKS_BACK)
    log.info("Scraping %d charts x %d weeks", len(CHARTS), len(weeks))

    for slug, chart_name, genre, category in CHARTS:
        with conn.cursor() as cur:
            chart_id = upsert_chart(cur, slug, chart_name, genre, category)
        conn.commit()

        for week_date in weeks:
            date_str = week_date.strftime("%Y-%m-%d")
            log.info("Fetching %s  week=%s", slug, date_str)

            try:
                chart_data = billboard.ChartData(slug, date=date_str, timeout=30)
            except Exception as exc:
                log.error("Failed to fetch %s / %s: %s — skipping", slug, date_str, exc)
                time.sleep(SLEEP_BB)
                continue

            # Use the actual chart date from the response
            actual_date = week_date
            if chart_data.date:
                try:
                    actual_date = date.fromisoformat(chart_data.date)
                except ValueError:
                    pass

            with conn.cursor() as cur:
                date_id = upsert_date(cur, actual_date)

                inserted = 0
                for entry in chart_data:
                    mb_info = lookup_musicbrainz(entry.artist)
                    artist_id = upsert_artist(cur, entry.artist, mb_info)
                    song_id = upsert_song(cur, entry.title, artist_id)
                    upsert_fact(cur, song_id, artist_id, chart_id, date_id, entry)
                    inserted += 1

            conn.commit()
            log.info("  -> committed %d entries for %s / %s", inserted, slug, actual_date)
            time.sleep(SLEEP_BB)

    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    main()
