# Power BI MCP

A Billboard chart data pipeline that scrapes chart data, loads it into a PostgreSQL star schema, and connects it to a Power BI semantic model for reporting.

---

## Project Structure

```
Power BI MCP/
├── billboard_scraper/
│   ├── billboard_scraper.py   # Scraper: schema DDL, scraping, MusicBrainz enrichment, DB load
│   ├── requirements.txt       # Python dependencies
│   ├── .env                   # DB credentials (gitignored — see .env.example)
│   └── .env.example           # Credential template
├── Power BI MCP Test.SemanticModel/   # Power BI semantic model (TMDL)
├── Power BI MCP Test.Report/          # Power BI report definition
└── Power BI MCP Test.pbip             # Power BI project file
```

---

## Billboard Scraper

Scrapes 10 major Billboard charts × 4 weeks of history and loads data into a PostgreSQL `billboard` database.

### Charts
| Slug | Chart | Entries |
|---|---|---|
| hot-100 | Hot 100 | 100 |
| billboard-200 | Billboard 200 | 200 |
| hot-country-songs | Hot Country Songs | 100 |
| hot-r-and-b-hip-hop-songs | Hot R&B/Hip-Hop Songs | 50 |
| hot-rock-songs | Hot Rock Songs | 25 |
| hot-latin-songs | Hot Latin Songs | 100 |
| pop-songs | Pop Songs | 40 |
| rap-song | Rap Songs | 25 |
| alternative-songs | Alternative Songs | 40 |
| dance-electronic-songs | Dance/Electronic Songs | 25 |

### Setup

1. Install dependencies:
   ```bash
   pip install -r billboard_scraper/requirements.txt
   ```

2. Copy `.env.example` to `.env` and fill in your PostgreSQL credentials.

3. Create the `billboard` database in PostgreSQL (the scraper creates the tables automatically).

4. Run the scraper:
   ```bash
   python billboard_scraper/billboard_scraper.py
   ```

### Artist Enrichment

Artists are enriched via the [MusicBrainz REST API](https://musicbrainz.org/doc/MusicBrainz_API) — adds genre tags, country, and career start date to `dim_artist`.

---

## Star Schema

```
dim_date ────────────────────────────┐
dim_chart ───────────────────────────┤
dim_song ────────────────────────────┼──► fact_chart_position
dim_artist ──────────────────────────┘
     ▲
     └── dim_song (inactive relationship)
```

| Table | Description |
|---|---|
| `dim_date` | Calendar attributes for each chart week |
| `dim_chart` | Chart metadata (name, genre, category) |
| `dim_artist` | Artist info enriched from MusicBrainz |
| `dim_song` | Song title and primary artist |
| `fact_chart_position` | Chart positions with rank, peak, weeks on chart |

---

## Power BI Semantic Model

The PBIP semantic model connects directly to the PostgreSQL `billboard` database.

**Requirements:**
- [Npgsql PostgreSQL driver](https://github.com/npgsql/npgsql/releases) installed on the machine running Power BI Desktop
- PostgreSQL credentials entered in Power BI Desktop on first open

**MCP Measures table — 10 DAX measures across 3 folders:**

| Folder | Measure |
|---|---|
| Volume | Total Entries, # New Entries, % New Entries, # Distinct Artists, # Distinct Songs, # Charts |
| Rank | Avg Rank, Best Peak Position, Avg Rank Change |
| Longevity | Max Weeks on Chart |
