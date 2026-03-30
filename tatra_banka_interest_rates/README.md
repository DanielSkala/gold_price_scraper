# Tatra Banka Mortgage Rate Monitor

Scrapes TatraBanka mortgage interest rates using Selenium (headless Chrome). Stores historical data in CSV for trend tracking.

## Requirements

Requires Chrome/Chromium installed (Selenium uses it headlessly).

## Usage

```bash
python tatra_banka_interest_rates/scrape_tatrabanka_mortgage.py

# Or via the wrapper script:
./tatra_banka_interest_rates/run_scraper.sh
```

## Files

- `scrape_tatrabanka_mortgage.py` — Selenium scraper
- `run_scraper.sh` — Shell wrapper for cron
- `tatrabanka_mortgage_rates.csv` — Historical rate data (auto-appended)
