# OpenInsider Intelligence Platform

Automated SEC Form 4 insider trading analytics. Scrapes [openinsider.com](http://openinsider.com) for insider buy/sell filings, enriches them with market data, computes a deterministic signal score, optionally runs GPT analysis, detects multi-insider cluster buying, and sends email alerts -- all viewable through a Flask dashboard.

---

## How It Works

### The Pipeline

Running `python -m openinsider.pipeline` executes six steps in order:

1. **Ingest** -- Scrapes three OpenInsider pages (latest screener filings, cluster buys, top officer purchases). Each filing is deduplicated by its SEC filing URL and stored in SQLite. Pages are fetched with a 2-second delay between requests.

2. **Enrich** -- For each new filing, fetches the ticker's market data via yfinance: closing price, 52-week high/low, drawdown from high, price vs 200-day moving average, volume ratio, and market cap. Results are cached in the `market_data` table so the same ticker/date pair is never fetched twice. Tickers are batched (5 at a time, 1s between batches) to avoid rate limits.

3. **Score** -- Computes a deterministic score from 0-100 for each filing based on 9 weighted factors (see Scoring below). This is fully deterministic -- no LLM involved. Every filing gets a score.

4. **Cluster Detection** -- Scans all tickers with recent purchases using a 14-day sliding window. When 2+ distinct insiders buy the same stock within 14 days, that's flagged as a cluster. Clusters are scored based on participant count, total value, average seniority, and CEO/CFO participation.

5. **LLM Analysis** (optional) -- Filings scoring >= 40 get sent to GPT-4o with an evidence packet containing the filing data, market context, insider history, and cluster info. The LLM returns a structured JSON assessment: priority (high/medium/low), investment thesis, bull/bear cases, confidence score, and evidence citations. Skipped entirely if `OPENAI_API_KEY` is not set.

6. **Notify** (optional) -- Checks scored filings against five trigger conditions and sends HTML email alerts via SMTP. Skipped if `SMTP_HOST` is not set.

### Scoring System

Each filing receives a composite score from 0 to 100, built from 9 independent factors:

| Factor | Max Points | How It Works |
|--------|-----------|--------------|
| Trade type | 15 | Purchase = 15, everything else = 0 |
| Insider seniority | 15 | CEO = 15, CFO = 13, COO/President = 12, Director = 9, VP = 7, Other = 3 |
| Trade value | 15 | >$1M = 15, >$500K = 12, >$100K = 8, >$10K = 4 |
| Market drawdown | 15 | >30% below 52-week high = 15, >20% = 10, >10% = 5 |
| Ownership delta | 10 | >20% increase = 10, >5% = 7, >1% = 4 |
| First buy timing | 10 | First purchase in 12+ months = 10, in 6+ months = 7 |
| Cluster bonus | 10 | 3+ insiders buying = 10, 2 insiders = 6 |
| Non-10b5-1 plan | 5 | Discretionary (not pre-planned) trade = 5 |
| Small cap | 5 | Market cap <$2B = 5, <$10B = 3 |

A CEO buying $2M of a small-cap stock at a 35% drawdown with no prior purchases in a year, alongside two other insiders, would score near the maximum. A routine VP selling shares scores 0.

### Notification Triggers

An email alert fires when **any** of these conditions are met:

1. Deterministic score >= 75
2. Score >= 50 AND LLM priority = "high" AND LLM confidence >= 0.7
3. Cluster with 3+ insiders AND total cluster value >= $500K
4. CEO/CFO purchase >= $200K at a watchlisted ticker
5. First insider purchase at a company in 12+ months, value >= $100K

Anti-spam: max 10 emails/day, no duplicate alerts per filing, 24-hour cooldown per ticker.

### Database Schema

SQLite with WAL mode. Seven tables:

- **filings** -- Core table. One row per SEC Form 4 filing. Contains insider name, title, ticker, trade type, price, quantity, value, ownership delta, price changes, deterministic score, LLM analysis JSON, and the raw HTML for reprocessing.
- **market_data** -- Cached yfinance data. One row per ticker/date pair (close, 52w high/low, drawdown, 200 DMA, volume ratio, market cap).
- **insiders** -- Aggregated insider profiles (name, company, title, buy/sell counts).
- **clusters** -- Detected cluster events (ticker, date range, participant list, total value, cluster score).
- **watchlist** -- User-managed tickers to monitor with optional notes.
- **notifications** -- Log of all sent alerts (filing ID, channel, message, timestamp).
- **scrape_runs** -- Audit log of every scrape (timestamp, page URL, filings found/new, status, errors).

---

## Quick Start

```bash
# From the repo root:

# Run the full pipeline (scrape + enrich + score + cluster + LLM + notify)
python -m openinsider.pipeline

# Launch the dashboard at http://localhost:5002
python -m openinsider.web.app

# Or run individual steps:
python -c "import asyncio; from openinsider.ingestion.ingest import run_ingestion; asyncio.run(run_ingestion())"
python -c "from openinsider.analysis.enrichment import enrich_pending_filings; enrich_pending_filings()"
python -c "from openinsider.analysis.scoring import score_all_unscored; score_all_unscored()"
python -c "from openinsider.analysis.clusters import detect_all_clusters; detect_all_clusters()"
```

## Project Structure

```
openinsider/
├── config.py                  # All constants, env vars, thresholds
├── db.py                      # SQLite schema, connection pool (thread-local)
├── pipeline.py                # 6-step orchestrator (the main entry point)
├── __main__.py                # python -m openinsider entry point
│
├── ingestion/                 # Data acquisition
│   ├── scraper.py             #   HTML parser for openinsider.com tables
│   └── ingest.py              #   Async scrape loop, dedup via INSERT OR IGNORE
│
├── analysis/                  # Signal processing
│   ├── enrichment.py          #   yfinance market data fetcher + cache
│   ├── signals.py             #   9-factor scoring functions (pure logic, no DB)
│   ├── clusters.py            #   14-day sliding window cluster detection
│   └── scoring.py             #   Wires signals.py + DB data together
│
├── integrations/              # External services (both optional)
│   ├── llm_analyst.py         #   OpenAI GPT-4o evidence-based analysis
│   └── notifications.py       #   SMTP email alerts with 5 trigger conditions
│
├── research/                  # Research tools
│   ├── backtest.py            #   Forward return calculation, score calibration
│   └── watchlist.py           #   Ticker watchlist CRUD
│
├── web/                       # Flask dashboard (port 5002)
│   ├── app.py                 #   17 API endpoints + 3 page routes
│   └── templates/
│       ├── dashboard.html     #   Main SPA (6 tabs: feed, opportunities, clusters, heatmap, research, watchlist)
│       ├── company.html       #   Company detail with price chart + trade overlay
│       └── insider.html       #   Insider profile with trade history timeline
│
├── scripts/
│   ├── run_pipeline.sh        #   Cron wrapper (activates venv, runs pipeline, logs output)
│   └── start_dashboard.sh     #   Dashboard launcher
│
└── data/                      # SQLite database (gitignored)
    └── openinsider.db
```

## Environment Variables

All optional. The system works without any of them -- you just won't get LLM analysis or email alerts.

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | _(none)_ | Enables GPT-4o analysis on filings scoring >= 40 |
| `OPENAI_MODEL` | `gpt-4o` | Which OpenAI model to use |
| `SMTP_HOST` | _(none)_ | SMTP server for email alerts (e.g. `smtp.gmail.com`) |
| `SMTP_PORT` | `587` | SMTP port |
| `SMTP_USER` | _(none)_ | SMTP username |
| `SMTP_PASSWORD` | _(none)_ | SMTP password ([Gmail App Password](https://myaccount.google.com/apppasswords)) |
| `ALERT_EMAIL_TO` | _(none)_ | Comma-separated recipient email(s) |

### Gmail Setup Example

```bash
export SMTP_HOST=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USER=you@gmail.com
export SMTP_PASSWORD=abcd-efgh-ijkl-mnop   # App Password, not your real password
export ALERT_EMAIL_TO=you@gmail.com
```

## Cron Setup

```bash
# Run pipeline every 30 min during US market hours (Mon-Fri 8am-6pm ET)
*/30 8-18 * * 1-5 /absolute/path/to/openinsider/scripts/run_pipeline.sh

# Edit with:
crontab -e
```

The script changes to the repo root, activates the virtualenv, runs the pipeline, and appends output to `data/pipeline.log`.

## Dashboard

The web dashboard at `http://localhost:5002` has six tabs:

- **Live Feed** -- Filterable table of all filings. Filter by trade type, minimum value, minimum score, ticker, and date range.
- **Opportunities** -- Top-ranked purchase filings sorted by deterministic score, with LLM analysis when available.
- **Clusters** -- Detected multi-insider buying events with participant lists and total values.
- **Heatmap** -- Plotly heatmap of net insider buying volume by ticker.
- **Research** -- Score calibration chart (win rate by score threshold) and top insider leaderboard.
- **Watchlist** -- Add/remove tickers to monitor. Watchlisted tickers get priority notifications.

Company pages (`/company/AAPL`) show a price chart with buy/sell markers overlaid, plus all filings for that ticker. Insider pages (`/insider/John%20Smith`) show the person's full trade history across all companies.

## API Endpoints

All return JSON. The dashboard consumes these.

| Endpoint | Description |
|----------|-------------|
| `GET /api/stats` | Summary: filings today, clusters this week, highest score, active insiders |
| `GET /api/filings` | Paginated filings. Params: `trade_type`, `min_value`, `min_score`, `ticker`, `days`, `page`, `per_page` |
| `GET /api/filings/<id>` | Single filing detail including LLM analysis |
| `GET /api/opportunities` | Top purchases by score. Params: `min_score` (default 60), `limit` (default 20) |
| `GET /api/clusters` | Recent clusters. Params: `days` (default 30), `min_insiders` (default 2) |
| `GET /api/company/<ticker>/summary` | Aggregated stats: total buys/sells, unique insiders, avg score |
| `GET /api/company/<ticker>/timeline` | Filing list + price history for charting |
| `GET /api/insider/<name>/history` | All filings by this insider |
| `GET /api/insider/<name>/profile` | Quality metrics: win rate, buy/sell ratio, trade count |
| `GET /api/search?q=AAPL` | Search tickers, company names, and insider names |
| `GET /api/heatmap` | Insider buying volume by ticker. Params: `metric` (buy_value/net), `period` (e.g. 30d) |
| `GET /api/scrape-status` | Last pipeline run status and timing |
| `GET/POST/DELETE /api/watchlist` | Watchlist CRUD. POST body: `{"ticker": "AAPL", "notes": "..."}` |
| `GET /api/backtest/calibration` | Win rate by score threshold (0-100 in steps of 10) |
| `GET /api/research/top-insiders` | Insider leaderboard. Params: `min_trades` (default 5), `sort` (accuracy/total_trades) |

## Data Sources

- **Filings**: [openinsider.com](http://openinsider.com) -- scraped HTML tables (SEC Form 4 data)
- **Market data**: [yfinance](https://github.com/ranaroussi/yfinance) -- Yahoo Finance price/volume/market cap
- **LLM analysis**: [OpenAI API](https://platform.openai.com/) -- GPT-4o (optional)
