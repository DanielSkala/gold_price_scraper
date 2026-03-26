import asyncio
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional

import aiohttp
from bs4 import BeautifulSoup

from openinsider.config import OPENINSIDER_BASE_URL, SCRAPE_DELAY_SECONDS, SCRAPE_PAGES

logger = logging.getLogger(__name__)


@dataclass
class RawFiling:
    filing_url: str = ""
    filing_date: str = ""
    trade_date: str = ""
    ticker: str = ""
    company_name: str = ""
    insider_name: str = ""
    title: str = ""
    trade_type: str = ""
    price: Optional[float] = None
    qty: Optional[float] = None
    owned: Optional[float] = None
    delta_own: Optional[float] = None
    value: Optional[float] = None
    price_change_1d: Optional[float] = None
    price_change_1w: Optional[float] = None
    price_change_1m: Optional[float] = None
    price_change_6m: Optional[float] = None
    raw_html: str = ""


class FilingSource(ABC):
    @abstractmethod
    async def fetch_filings(self) -> List[RawFiling]:
        pass


class OpenInsiderScraper(FilingSource):
    def __init__(self, base_url: str = OPENINSIDER_BASE_URL, pages: list = None,
                 delay: float = SCRAPE_DELAY_SECONDS):
        self.base_url = base_url
        self.pages = pages or SCRAPE_PAGES
        self.delay = delay

    @staticmethod
    def _clean_number(text: str) -> Optional[float]:
        if not text:
            return None
        cleaned = re.sub(r"[,$\s+]", "", text.strip())
        if not cleaned or cleaned == "-":
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    @staticmethod
    def _clean_percentage(text: str) -> Optional[float]:
        if not text:
            return None
        cleaned = re.sub(r"[%\s+]", "", text.strip())
        if not cleaned or cleaned == "-":
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    def _detect_columns(self, table) -> dict:
        """Detect column mapping from header row."""
        header_map = {}
        thead = table.find("thead") or table
        header_row = thead.find("tr")
        if not header_row:
            return header_map
        headers = header_row.find_all("th")
        for i, h in enumerate(headers):
            text = h.get_text(strip=True).replace("\xa0", " ").lower()
            header_map[text] = i
        return header_map

    def _parse_table(self, html: str) -> List[RawFiling]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", class_="tinytable")
        if not table:
            logger.warning("No tinytable found in page")
            return []

        headers = self._detect_columns(table)
        has_insider_name = "insider name" in headers

        filings = []
        tbody = table.find("tbody")
        rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 13:
                continue

            # Filing URL is in Cell 1 (Filing Date column contains SEC link)
            filing_url = ""
            filing_cell = cells[1]
            link = filing_cell.find("a")
            if link and link.get("href"):
                href = link["href"]
                if "sec.gov" in href:
                    filing_url = href if href.startswith("http") else "https:" + href
                else:
                    filing_url = href if href.startswith("http") else self.base_url + href

            # If no SEC link found, construct a dedup key from the data
            if not filing_url:
                ticker = cells[3].get_text(strip=True)
                trade_date = cells[2].get_text(strip=True)
                value = cells[12].get_text(strip=True)
                filing_url = f"openinsider://{ticker}/{trade_date}/{value}"

            insider_name = ""
            title = ""
            if has_insider_name:
                insider_name = cells[5].get_text(strip=True)
                title = cells[6].get_text(strip=True)

            filing = RawFiling(
                filing_url=filing_url,
                filing_date=cells[1].get_text(strip=True),
                trade_date=cells[2].get_text(strip=True),
                ticker=cells[3].get_text(strip=True),
                company_name=cells[4].get_text(strip=True),
                insider_name=insider_name,
                title=title,
                trade_type=cells[7].get_text(strip=True),
                price=self._clean_number(cells[8].get_text(strip=True)),
                qty=self._clean_number(cells[9].get_text(strip=True)),
                owned=self._clean_number(cells[10].get_text(strip=True)),
                delta_own=self._clean_percentage(cells[11].get_text(strip=True)),
                value=self._clean_number(cells[12].get_text(strip=True)),
                price_change_1d=self._clean_percentage(cells[13].get_text(strip=True)) if len(cells) > 13 else None,
                price_change_1w=self._clean_percentage(cells[14].get_text(strip=True)) if len(cells) > 14 else None,
                price_change_1m=self._clean_percentage(cells[15].get_text(strip=True)) if len(cells) > 15 else None,
                price_change_6m=self._clean_percentage(cells[16].get_text(strip=True)) if len(cells) > 16 else None,
                raw_html=str(row),
            )
            filings.append(filing)

        return filings

    async def fetch_filings(self) -> List[RawFiling]:
        all_filings: List[RawFiling] = []
        async with aiohttp.ClientSession() as session:
            for i, page in enumerate(self.pages):
                url = self.base_url + page
                logger.info("Fetching %s", url)
                try:
                    async with session.get(url) as response:
                        html = await response.text()
                    filings = self._parse_table(html)
                    logger.info("Found %d filings from %s", len(filings), page)
                    all_filings.extend(filings)
                except Exception:
                    logger.exception("Error fetching %s", url)

                if i < len(self.pages) - 1:
                    await asyncio.sleep(self.delay)

        return all_filings
