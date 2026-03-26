import asyncio
import logging
from datetime import datetime, timezone

from openinsider.db import get_connection, init_db
from openinsider.ingestion.scraper import OpenInsiderScraper

logger = logging.getLogger(__name__)


async def run_ingestion():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    init_db()
    conn = get_connection()
    scraper = OpenInsiderScraper()

    for page_url in scraper.pages:
        started_at = datetime.now(timezone.utc).isoformat()
        filings_found = 0
        filings_new = 0
        status = "success"
        error_message = None

        try:
            scraper_single = OpenInsiderScraper(pages=[page_url])
            filings = await scraper_single.fetch_filings()
            filings_found = len(filings)

            for f in filings:
                if not f.filing_url:
                    continue
                cursor = conn.execute(
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
                    filings_new += 1

            conn.commit()
            logger.info("Page %s: found=%d, new=%d", page_url, filings_found, filings_new)

        except Exception as e:
            status = "error"
            error_message = str(e)
            logger.exception("Ingestion error for %s", page_url)

        finished_at = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT INTO scrape_runs
               (started_at, finished_at, source, url, filings_found, filings_new, status, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (started_at, finished_at, "openinsider", page_url, filings_found, filings_new, status, error_message),
        )
        conn.commit()

    logger.info("Ingestion complete")


if __name__ == "__main__":
    asyncio.run(run_ingestion())
