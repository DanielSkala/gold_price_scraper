import os
from pathlib import Path

OPENINSIDER_BASE_URL = "http://openinsider.com"

SCRAPE_PAGES = [
    # 7-day screener: all purchases filed in the last 7 days
    "/screener?s=&o=&pl=&ph=&ll=&lh=&fd=7&fdr=&td=&tdr=&feession=&cession=&sig=&sidTicker=&tiession=&tc=1&tst=0&iession=&ic=1",
    # 7-day screener: all sales filed in the last 7 days
    "/screener?s=&o=&pl=&ph=&ll=&lh=&fd=7&fdr=&td=&tdr=&feession=&cession=&sig=&sidTicker=&tiession=&tc=7&tst=0&iession=&ic=1",
    # Big purchases ($100K+) filed in the last 7 days
    "/screener?s=&o=&pl=100&ph=&ll=&lh=&fd=7&fdr=&td=&tdr=&feession=&cession=&sig=&sidTicker=&tiession=&tc=1&tst=0&iession=&ic=1",
    # Latest insider trading (all types, 100 most recent)
    "/latest-insider-trading",
    # Cluster buys (multiple insiders buying same stock)
    "/latest-cluster-buys",
    # Top officer purchases this month
    "/top-officer-purchases-of-the-month",
    # Top officer purchases this week
    "/top-officer-purchases-of-the-week",
]

SCRAPE_DELAY_SECONDS = 2.0

DATA_DIR = Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "openinsider.db"

YFINANCE_BATCH_SIZE = 5
YFINANCE_DELAY_SECONDS = 1.0

FLASK_PORT = 5002

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")

SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "")

SCORE_THRESHOLD_LLM = 40
SCORE_THRESHOLD_ALERT = 75
MAX_EMAILS_PER_DAY = 10
TICKER_COOLDOWN_HOURS = 24
