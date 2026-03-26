"""
Enrich filings with Rule 10b5-1 plan status from SEC Form 4 XML.

Fetches actual SEC EDGAR XML filings and extracts the aff10b5One field
(mandatory since 2023, schema X0407+). For pre-2023 filings, falls back
to scanning footnotes for "10b5-1" mentions.

Usage:
    python -m openinsider.scripts.enrich_10b5_1
    python -m openinsider.scripts.enrich_10b5_1 --limit 100
    python -m openinsider.scripts.enrich_10b5_1 --workers 8
    python -m openinsider.scripts.enrich_10b5_1 --all  # include pre-2023
    python -m openinsider.scripts.enrich_10b5_1 --no-resume
"""

import json
import logging
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests
from requests.adapters import HTTPAdapter

from openinsider.config import DATA_DIR
from openinsider.db import get_connection, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(DATA_DIR / "enrich_10b5_1.log"),
    ],
)
logger = logging.getLogger(__name__)

PROGRESS_FILE = DATA_DIR / "10b5_1_progress.json"
MAX_RETRIES = 3
BATCH_SIZE = 100

HEADERS = {
    "User-Agent": "OpenInsider-Research/1.0 (openinsider-research@proton.me)",
    "Accept-Encoding": "gzip, deflate",
}


class RateLimiter:
    """Token bucket rate limiter for SEC's 10 req/s limit."""

    def __init__(self, rate: float = 10.0):
        self.min_interval = 1.0 / rate
        self.lock = threading.Lock()
        self.last = time.monotonic()

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.monotonic()


def sec_url_to_raw_xml(filing_url: str) -> Optional[str]:
    """Convert XSLT-rendered SEC URL to raw XML URL."""
    if not filing_url or "sec.gov" not in filing_url:
        return None
    url = filing_url.replace("/xslF345X03/", "/")
    url = url.replace("http://", "https://")
    return url


def extract_10b5_1(xml_text: str) -> Optional[int]:
    """Parse Form 4 XML for aff10b5One field.
    Returns 1 (is 10b5-1), 0 (not 10b5-1), or None (field absent)."""
    try:
        root = ET.fromstring(xml_text)
        elem = root.find("aff10b5One")
        if elem is not None and elem.text is not None:
            return 1 if elem.text.strip() == "1" else 0
        return None
    except ET.ParseError:
        return None


def check_footnotes_for_10b5_1(xml_text: str) -> Optional[int]:
    """Scan text for 10b5-1 mentions (fallback for pre-2023 filings)."""
    text_lower = xml_text.lower()
    if "10b5-1" in text_lower or "10b5 1" in text_lower or "10b-5-1" in text_lower:
        return 1
    return None  # No mention = unknown


class SEC10b51Enricher:
    def __init__(self, limit: int = 0, resume: bool = True,
                 workers: int = 5, since: str = "2023-02-01",
                 process_all: bool = False):
        self.limit = limit
        self.workers = min(workers, 10)  # Cap at 10 (SEC limit)
        self.since = since if not process_all else None
        self.conn = None
        self.stats = {
            "checked": 0, "is_10b5_1": 0, "not_10b5_1": 0,
            "unknown": 0, "errors": 0, "skipped": 0,
        }
        self.rate_limiter = RateLimiter(rate=10.0)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        # Increase connection pool to match worker count
        adapter = HTTPAdapter(
            pool_connections=self.workers + 2,
            pool_maxsize=self.workers + 2,
        )
        self.session.mount("https://", adapter)
        if resume:
            self._load_progress()

    def _load_progress(self):
        if PROGRESS_FILE.exists():
            try:
                data = json.loads(PROGRESS_FILE.read_text())
                saved = data.get("stats", {})
                for k in self.stats:
                    if k in saved:
                        self.stats[k] = saved[k]
                logger.info("Resumed: %d already checked, %d 10b5-1, %d discretionary",
                            self.stats["checked"], self.stats["is_10b5_1"], self.stats["not_10b5_1"])
            except Exception:
                logger.warning("Could not load progress, starting fresh")

    def _save_progress(self):
        data = {
            "stats": self.stats,
            "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        PROGRESS_FILE.write_text(json.dumps(data))

    def fetch_and_parse(self, filing_id: int, filing_url: str) -> tuple:
        """Fetch XML and determine 10b5-1 status. Thread-safe."""
        raw_url = sec_url_to_raw_xml(filing_url)
        if not raw_url:
            return (filing_id, None)

        self.rate_limiter.acquire()

        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(raw_url, timeout=30)
                if resp.status_code == 429:
                    wait = 30 * (2 ** attempt)
                    logger.warning("Rate limited, waiting %ds", wait)
                    time.sleep(wait)
                    continue
                if resp.status_code == 404:
                    return (filing_id, -1)
                resp.raise_for_status()
                xml_text = resp.text

                # Try authoritative field first
                result = extract_10b5_1(xml_text)
                if result is not None:
                    return (filing_id, result)

                # Fallback: footnote scanning for pre-2023
                footnote_result = check_footnotes_for_10b5_1(xml_text)
                if footnote_result is not None:
                    return (filing_id, footnote_result)

                return (filing_id, -1)  # No data available

            except requests.RequestException as e:
                wait = 5 * (2 ** attempt)
                logger.debug("Fetch error %s: %s (retry in %ds)", raw_url[-40:], e, wait)
                time.sleep(wait)

        return (filing_id, -1)  # All retries failed

    def _mark_old_filings(self):
        """Bulk-mark pre-cutoff filings as -1 (unavailable) without fetching."""
        if not self.since:
            return
        cursor = self.conn.execute(
            "UPDATE filings SET is_10b5_1 = -1 WHERE is_10b5_1 IS NULL AND filing_date < ?",
            (self.since,),
        )
        self.conn.commit()
        marked = cursor.rowcount
        if marked > 0:
            logger.info("Marked %d pre-%s filings as unavailable (no aff10b5One field before 2023)",
                        marked, self.since)

    def run(self):
        init_db()
        self.conn = get_connection()
        start_time = time.time()

        # Bulk-mark old filings first
        self._mark_old_filings()

        # Query only remaining NULL filings
        query = """
            SELECT id, filing_url FROM filings
            WHERE is_10b5_1 IS NULL AND filing_url LIKE '%sec.gov%'
        """
        params = []
        if self.since:
            query += " AND filing_date >= ?"
            params.append(self.since)
        query += " ORDER BY filing_date DESC"
        if self.limit > 0:
            query += f" LIMIT {self.limit}"

        rows = self.conn.execute(query, params).fetchall()
        total = len(rows)
        logger.info("Found %d filings to enrich (workers=%d)", total, self.workers)

        if total == 0:
            logger.info("Nothing to do")
            return

        batch_updates = []
        processed = 0

        # Process in chunks using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            # Submit work in chunks to avoid overwhelming memory
            chunk_size = self.workers * 20
            for chunk_start in range(0, total, chunk_size):
                chunk = rows[chunk_start:chunk_start + chunk_size]
                futures = {
                    pool.submit(self.fetch_and_parse, row["id"], row["filing_url"]): row["id"]
                    for row in chunk
                }

                for future in as_completed(futures):
                    filing_id, result = future.result()

                    if result is None:
                        self.stats["skipped"] += 1
                        processed += 1
                        continue

                    batch_updates.append((result, filing_id))
                    self.stats["checked"] += 1
                    processed += 1

                    if result == 1:
                        self.stats["is_10b5_1"] += 1
                    elif result == 0:
                        self.stats["not_10b5_1"] += 1
                    else:
                        self.stats["unknown"] += 1

                    # Batch commit
                    if len(batch_updates) >= BATCH_SIZE:
                        self.conn.executemany(
                            "UPDATE filings SET is_10b5_1 = ? WHERE id = ?",
                            batch_updates,
                        )
                        self.conn.commit()
                        batch_updates = []

                    # Progress logging every 500
                    if processed % 500 == 0:
                        elapsed = time.time() - start_time
                        rate = self.stats["checked"] / elapsed if elapsed > 0 else 0
                        remaining = (total - processed) / rate / 60 if rate > 0 else 0
                        pct_10b5 = (self.stats["is_10b5_1"] / self.stats["checked"] * 100
                                    if self.stats["checked"] > 0 else 0)
                        logger.info(
                            "%d/%d (%.1f%%) | 10b5-1: %d (%.0f%%) | discretionary: %d | "
                            "unknown: %d | errors: %d | %.1f req/s | ETA: %.0fm",
                            processed, total, 100 * processed / total,
                            self.stats["is_10b5_1"], pct_10b5,
                            self.stats["not_10b5_1"], self.stats["unknown"],
                            self.stats["errors"], rate, remaining,
                        )
                        self._save_progress()

        # Final batch
        if batch_updates:
            self.conn.executemany(
                "UPDATE filings SET is_10b5_1 = ? WHERE id = ?",
                batch_updates,
            )
            self.conn.commit()

        self._save_progress()
        elapsed = time.time() - start_time
        logger.info(
            "Done in %.1f minutes. Checked: %d, 10b5-1: %d, discretionary: %d, "
            "unknown: %d, errors: %d",
            elapsed / 60, self.stats["checked"], self.stats["is_10b5_1"],
            self.stats["not_10b5_1"], self.stats["unknown"], self.stats["errors"],
        )


def enrich_recent_filings(limit: int = 50) -> int:
    """Lightweight enrichment for pipeline use — only recent NULL filings."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT id, filing_url FROM filings
        WHERE is_10b5_1 IS NULL AND filing_url LIKE '%sec.gov%'
          AND filing_date >= date('now', '-7 days')
        ORDER BY filing_date DESC LIMIT ?
    """, (limit,)).fetchall()

    if not rows:
        return 0

    enricher = SEC10b51Enricher(limit=0, resume=False, workers=3)
    enricher.conn = conn
    count = 0
    batch_updates = []

    for row in rows:
        _, result = enricher.fetch_and_parse(row["id"], row["filing_url"])
        if result is not None:
            batch_updates.append((result, row["id"]))
            count += 1

    if batch_updates:
        conn.executemany("UPDATE filings SET is_10b5_1 = ? WHERE id = ?", batch_updates)
        conn.commit()

    return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Enrich filings with 10b5-1 plan status from SEC XML")
    parser.add_argument("--limit", type=int, default=0, help="Max filings to process (0=unlimited)")
    parser.add_argument("--workers", type=int, default=5, help="Concurrent workers (default: 5, max: 10)")
    parser.add_argument("--since", default="2023-02-01",
                        help="Only fetch XML for filings on/after this date (default: 2023-02-01)")
    parser.add_argument("--all", action="store_true", dest="process_all",
                        help="Process ALL filings including pre-2023 (slow)")
    parser.add_argument("--no-resume", action="store_true", help="Start fresh")
    args = parser.parse_args()

    enricher = SEC10b51Enricher(
        limit=args.limit,
        resume=not args.no_resume,
        workers=args.workers,
        since=args.since,
        process_all=args.process_all,
    )
    enricher.run()


if __name__ == "__main__":
    main()
