"""Simple CRUD for ticker watchlist."""

import logging

from openinsider.db import get_connection

logger = logging.getLogger(__name__)


def add_to_watchlist(ticker, notes="", alert_on_any=False):
    """Add a ticker to the watchlist. Returns True if added, False if already exists."""
    ticker = ticker.upper().strip()
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO watchlist (ticker, notes) VALUES (?, ?)",
            (ticker, notes),
        )
        conn.commit()
        logger.info("Added %s to watchlist", ticker)
        return True
    except Exception:
        logger.debug("Ticker %s already on watchlist", ticker)
        return False


def remove_from_watchlist(ticker):
    """Remove a ticker from the watchlist. Returns True if removed."""
    ticker = ticker.upper().strip()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM watchlist WHERE ticker = ?", (ticker,))
    removed = cursor.rowcount > 0
    conn.commit()
    if removed:
        logger.info("Removed %s from watchlist", ticker)
    return removed


def get_watchlist():
    """Get all watchlist entries. Returns list of dicts."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM watchlist ORDER BY added_at DESC")
    return [dict(r) for r in cursor.fetchall()]


def is_watchlisted(ticker):
    """Check if a ticker is on the watchlist."""
    ticker = ticker.upper().strip()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM watchlist WHERE ticker = ?", (ticker,))
    return cursor.fetchone()[0] > 0
