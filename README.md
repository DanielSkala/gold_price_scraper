# Personal Finance & Market Intelligence Toolkit

A collection of personal finance and market analysis tools.

## Setup

```bash
pip install poetry
poetry install --no-root
```

---

## Projects

| Project | Description | Port | Docs |
|---------|-------------|------|------|
| [`gold/`](gold/) | Gold bar premium tracker (zlataky.sk) | — | [README](gold/README.md) |
| [`expenses/`](expenses/) | Credit card expense analytics + dashboard | 5001 | [README](expenses/README.md) |
| [`investing/`](investing/) | S&P 500 investment growth simulator | — | [README](investing/README.md) |
| [`tatra_banka_interest_rates/`](tatra_banka_interest_rates/) | Mortgage rate scraper (Selenium) | — | [README](tatra_banka_interest_rates/README.md) |
| [`openinsider/`](openinsider/) | SEC insider trading analytics + dashboard | 5002 | [README](openinsider/README.md) |

## Repository Structure

```
.
├── gold/                          # Gold bar premium scraper
│   ├── main.py                    #   Scraper + Plotly chart
│   └── gold_premiums.csv          #   Historical data
│
├── expenses/                      # Expense analytics
│   ├── app.py                     #   Flask dashboard (port 5001)
│   ├── credit_card_expenses.py    #   CSV parser + categorizer
│   ├── templates/dashboard.html   #   Interactive dashboard
│   └── expense_reports/           #   TatraBanka CSV exports
│
├── investing/                     # Investment projections
│   └── investing.py               #   S&P 500 growth simulator
│
├── tatra_banka_interest_rates/    # Mortgage rates
│   ├── scrape_tatrabanka_mortgage.py  # Selenium scraper
│   └── run_scraper.sh             #   Cron wrapper
│
├── openinsider/                   # Insider trading intelligence
│   ├── config.py                  #   Configuration + env vars
│   ├── db.py                      #   SQLite schema (7 tables)
│   ├── pipeline.py                #   End-to-end orchestrator
│   ├── ingestion/                 #   Data acquisition
│   │   ├── scraper.py             #     OpenInsider HTML scraper
│   │   └── ingest.py              #     Scrape orchestrator
│   ├── analysis/                  #   Signal processing
│   │   ├── enrichment.py          #     yfinance market data
│   │   ├── signals.py             #     9-factor scoring (0-100)
│   │   ├── clusters.py            #     Cluster detection
│   │   └── scoring.py             #     Batch scoring
│   ├── integrations/              #   External services
│   │   ├── llm_analyst.py         #     OpenAI GPT analysis
│   │   └── notifications.py       #     Email alerts
│   ├── research/                  #   Backtesting
│   │   ├── backtest.py            #     Score calibration + returns
│   │   └── watchlist.py           #     Ticker watchlist
│   ├── web/                       #   Flask dashboard (port 5002)
│   │   ├── app.py                 #     Routes + API
│   │   └── templates/             #     HTML pages
│   ├── scripts/                   #   Shell wrappers
│   └── data/                      #   SQLite database (gitignored)
│
├── pyproject.toml                 # Poetry dependencies
└── .gitignore
```

## Quick Start per Project

```bash
# Gold premiums
python gold/main.py

# Expense dashboard
python expenses/app.py                    # http://localhost:5001

# Investment projections
python investing/investing.py

# Mortgage rates
python tatra_banka_interest_rates/scrape_tatrabanka_mortgage.py

# OpenInsider — scrape + score + dashboard
python -m openinsider.ingest              # Scrape filings
python -m openinsider.pipeline            # Full pipeline (ingest+enrich+score+cluster)
python -m openinsider.web.app              # http://localhost:5002
```

## Environment Variables (OpenInsider only)

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | No | GPT-4o analysis on high-scoring filings |
| `SMTP_HOST` | No | Email alerts (e.g., `smtp.gmail.com`) |
| `SMTP_PORT` | No | Default: `587` |
| `SMTP_USER` | No | SMTP username |
| `SMTP_PASSWORD` | No | SMTP password ([Gmail App Password](https://myaccount.google.com/apppasswords)) |
| `ALERT_EMAIL_TO` | No | Alert recipient email |
