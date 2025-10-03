#!/bin/bash
# Script to run Tatrabanka mortgage scraper
# This script is called by cron

cd /Users/daskala/Desktop/gold_price_scraper/tatra_banka_interest_rates
/Users/daskala/Desktop/gold_price_scraper/.venv/bin/python scrape_tatrabanka_mortgage.py >> scraper.log 2>&1
