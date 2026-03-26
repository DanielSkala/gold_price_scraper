"""
Historical backfill script for OpenInsider data.

Scrapes years of insider trading filings using the screener's custom date range
with pagination support. Uses synchronous requests for reliability.

Usage:
    python -m openinsider.scripts.backfill
    python -m openinsider.scripts.backfill --start 2015-01-01 --end 2026-03-24
    python -m openinsider.scripts.backfill --no-resume
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Tuple

import requests

from openinsider.config import DATA_DIR, OPENINSIDER_BASE_URL
from openinsider.db import get_connection, init_db
from openinsider.ingestion.scraper import OpenInsiderScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(DATA_DIR / "backfill.log"),
    ],
)
logger = logging.getLogger(__name__)

PROGRESS_FILE = DATA_DIR / "backfill_progress.json"
DELAY_SECONDS = 5.0
MAX_RETRIES = 3
MAX_ROWS = 500

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


class BackfillRunner:
    def __init__(self, start_date: str = "2020-01-01", end_date: str = None,
                 resume: bool = True):
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
        self.scraper = OpenInsiderScraper()
        self.conn = None
        self.stats = {"total_found": 0, "total_new": 0, "requests": 0, "errors": 0}
        self.completed = set()
        if resume:
            self._load_progress()

    def _load_progress(self):
        if PROGRESS_FILE.exists():
            try:
                data = json.loads(PROGRESS_FILE.read_text())
                self.completed = set(data.get("completed", []))
                saved_stats = data.get("stats", {})
                for k in self.stats:
                    if k in saved_stats:
                        self.stats[k] = saved_stats[k]
                logger.info("Resumed: %d months already done, %d new filings so far",
                            len(self.completed), self.stats["total_new"])
            except Exception:
                logger.warning("Could not load progress file, starting fresh")

    def _save_progress(self):
        data = {
            "completed": list(self.completed),
            "stats": self.stats,
            "saved_at": datetime.now().isoformat(),
        }
        PROGRESS_FILE.write_text(json.dumps(data))

    def generate_months(self) -> List[Tuple[datetime, datetime]]:
        months = []
        current = self.start_date.replace(day=1)
        while current <= self.end_date:
            if current.month == 12:
                month_end = current.replace(year=current.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                month_end = current.replace(month=current.month + 1, day=1) - timedelta(days=1)
            month_end = min(month_end, self.end_date)
            months.append((current, month_end))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        return months

    def build_url(self, start: datetime, end: datetime, page: int = 1) -> str:
        start_str = start.strftime("%m/%d/%Y")
        end_str = end.strftime("%m/%d/%Y")
        fdr = f"{start_str} - {end_str}"
        return (
            f"{OPENINSIDER_BASE_URL}/screener?s=&o=&pl=&ph=&ll=&lh="
            f"&fd=-1&fdr={fdr}&td=0&tdr="
            f"&fdlyl=&fdlyh=&daysago="
            f"&xs=1"
            f"&vl=&vh=&ocl=&och="
            f"&sic1=&sic2=&sic3=&sicl=&sich="
            f"&isofficer=1&iscob=1&isceo=1&ispres=1&iscoo=1&iscfo=1"
            f"&isgc=1&isvp=1&isdirector=1&istenpercent=1&isother=1"
            f"&grp=&nfl=&nfh=&nil=&nih=&nol=&noh="
            f"&v2l=&v2h=&oc2l=&oc2h="
            f"&sortcol=&cnt={MAX_ROWS}&page={page}"
        )

    def month_key(self, start: datetime) -> str:
        return start.strftime("%Y-%m")

    def fetch_page(self, url: str) -> str:
        """Fetch a single URL with retries using requests library."""
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=120)
                resp.raise_for_status()
                self.stats["requests"] += 1
                return resp.text
            except Exception as e:
                wait = 10 * (2 ** attempt)
                logger.warning("Attempt %d failed: %s (retry in %ds)",
                               attempt + 1, type(e).__name__ + ": " + str(e), wait)
                self.stats["errors"] += 1
                time.sleep(wait)
        return None

    def store_filings(self, filings) -> int:
        new_count = 0
        for f in filings:
            if not f.filing_url:
                continue
            try:
                cursor = self.conn.execute(
                    """INSERT OR IGNORE INTO filings
                       (filing_url, filing_date, trade_date, ticker, company_name,
                        insider_name, title, trade_type, price, qty, owned, delta_own,
                        value, price_change_1d, price_change_1w, price_change_1m,
                        price_change_6m, raw_html)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        f.filing_url, f.filing_date, f.trade_date, f.ticker,
                        f.company_name, f.insider_name, f.title, f.trade_type,
                        f.price, f.qty, f.owned, f.delta_own, f.value,
                        f.price_change_1d, f.price_change_1w, f.price_change_1m,
                        f.price_change_6m, f.raw_html,
                    ),
                )
                if cursor.rowcount > 0:
                    new_count += 1
            except Exception as e:
                logger.debug("Insert error: %s", e)
        self.conn.commit()
        return new_count

    def fetch_month(self, start: datetime, end: datetime) -> Tuple[int, int]:
        """Fetch all pages for a month window. Returns (found, new)."""
        total_found = 0
        total_new = 0
        page = 1
        key = self.month_key(start)
        max_pages = 20  # OpenInsider pagination loops after ~20 pages
        consecutive_zero_new = 0

        while page <= max_pages:
            logger.info("  Fetching %s page %d...", key, page)
            url = self.build_url(start, end, page)
            html = self.fetch_page(url)
            if html is None:
                logger.error("  Failed to fetch %s page %d", key, page)
                break

            filings = self.scraper._parse_table(html)
            found = len(filings)
            total_found += found

            if found > 0:
                new = self.store_filings(filings)
                total_new += new
                logger.info("  %s page %d: %d found, %d new", key, page, found, new)

                # Detect pagination loop: if we keep getting 0 new, pagination is recycling
                if new == 0 and found == MAX_ROWS:
                    consecutive_zero_new += 1
                    if consecutive_zero_new >= 3:
                        logger.info("  %s: pagination likely looping (3 consecutive pages with 0 new), stopping", key)
                        break
                else:
                    consecutive_zero_new = 0
            else:
                logger.info("  %s page %d: empty (end of data)", key, page)

            if found < MAX_ROWS:
                break

            page += 1
            time.sleep(DELAY_SECONDS)

        return total_found, total_new

    def run(self):
        init_db()
        self.conn = get_connection()
        months = self.generate_months()
        total_months = len(months)
        start_time = time.time()

        logger.info("Backfill: %d monthly windows from %s to %s, %d already done",
                     total_months, self.start_date.strftime("%Y-%m"),
                     self.end_date.strftime("%Y-%m"), len(self.completed))

        for win_start, win_end in months:
            key = self.month_key(win_start)

            if key in self.completed:
                continue

            found, new = self.fetch_month(win_start, win_end)
            self.stats["total_found"] += found
            self.stats["total_new"] += new
            self.completed.add(key)

            elapsed = time.time() - start_time
            completed_count = len(self.completed)
            remaining_months = total_months - completed_count
            avg_time = elapsed / completed_count if completed_count > 0 else 0
            eta_min = (remaining_months * avg_time) / 60

            logger.info(
                "%s: found %d, new %d | Overall: %d/%d months (%.0f%%) | "
                "Total new: %d | Requests: %d | ETA: %.0fm",
                key, found, new, completed_count, total_months,
                100 * completed_count / total_months,
                self.stats["total_new"], self.stats["requests"], eta_min,
            )
            self._save_progress()
            time.sleep(10)

        self._save_progress()
        elapsed = time.time() - start_time
        row = self.conn.execute("SELECT COUNT(*) as cnt FROM filings").fetchone()
        logger.info(
            "Backfill complete in %.1f minutes. New filings: %d, Total in DB: %d, "
            "Requests: %d, Errors: %d",
            elapsed / 60, self.stats["total_new"], row["cnt"],
            self.stats["requests"], self.stats["errors"],
        )


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Backfill historical OpenInsider data")
    parser.add_argument("--start", default="2020-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--no-resume", action="store_true", help="Start fresh, ignore progress")
    args = parser.parse_args()

    runner = BackfillRunner(
        start_date=args.start,
        end_date=args.end,
        resume=not args.no_resume,
    )
    runner.run()


if __name__ == "__main__":
    main()
