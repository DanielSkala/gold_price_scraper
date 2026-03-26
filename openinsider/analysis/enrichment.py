import asyncio
import logging
from datetime import datetime, timedelta

import yfinance as yf

from openinsider.config import YFINANCE_BATCH_SIZE, YFINANCE_DELAY_SECONDS
from openinsider.db import get_connection

logger = logging.getLogger(__name__)


def get_market_context(ticker: str, trade_date: str) -> dict:
    conn = get_connection()
    cached = conn.execute(
        "SELECT * FROM market_data WHERE ticker = ? AND date = ?",
        (ticker, trade_date),
    ).fetchone()
    if cached:
        return dict(cached)

    result = {
        "ticker": ticker,
        "date": trade_date,
        "close": 0.0,
        "high_52w": 0.0,
        "low_52w": 0.0,
        "drawdown_from_52w_high": 0.0,
        "price_vs_200dma": 0.0,
        "volume_ratio_20d": 0.0,
        "market_cap": 0.0,
    }

    try:
        ticker_data = yf.Ticker(ticker)
        hist = ticker_data.history(period="1y")

        if hist.empty:
            logger.warning("No history for %s", ticker)
            _store_market_data(result)
            return result

        trade_dt = datetime.strptime(trade_date, "%Y-%m-%d")
        mask = hist.index <= trade_dt.strftime("%Y-%m-%d")
        relevant = hist[mask] if mask.any() else hist

        if relevant.empty:
            relevant = hist

        close = float(relevant["Close"].iloc[-1])
        high_52w = float(hist["High"].max())
        low_52w = float(hist["Low"].min())
        drawdown = ((close - high_52w) / high_52w * 100) if high_52w > 0 else 0.0

        dma_200 = float(hist["Close"].tail(200).mean()) if len(hist) >= 200 else float(hist["Close"].mean())
        price_vs_200dma = ((close - dma_200) / dma_200 * 100) if dma_200 > 0 else 0.0

        vol_20 = float(hist["Volume"].tail(20).mean()) if len(hist) >= 20 else float(hist["Volume"].mean())
        last_vol = float(relevant["Volume"].iloc[-1])
        volume_ratio = (last_vol / vol_20) if vol_20 > 0 else 0.0

        info = ticker_data.info
        market_cap = float(info.get("marketCap", 0) or 0)

        result.update({
            "close": close,
            "high_52w": high_52w,
            "low_52w": low_52w,
            "drawdown_from_52w_high": round(drawdown, 2),
            "price_vs_200dma": round(price_vs_200dma, 2),
            "volume_ratio_20d": round(volume_ratio, 2),
            "market_cap": market_cap,
        })

    except Exception:
        logger.exception("yfinance error for %s on %s", ticker, trade_date)

    _store_market_data(result)
    return result


def _store_market_data(data: dict):
    conn = get_connection()
    conn.execute(
        """INSERT OR IGNORE INTO market_data
           (ticker, date, close, high_52w, low_52w, drawdown_from_52w_high,
            price_vs_200dma, volume_ratio_20d, market_cap)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            data["ticker"], data["date"], data["close"], data.get("high_52w", 0),
            data.get("low_52w", 0), data["drawdown_from_52w_high"],
            data["price_vs_200dma"], data["volume_ratio_20d"], data["market_cap"],
        ),
    )
    conn.commit()


def enrich_pending_filings(limit: int = 50):
    conn = get_connection()
    rows = conn.execute(
        """SELECT f.id, f.ticker, f.trade_date FROM filings f
           LEFT JOIN market_data m ON f.ticker = m.ticker AND f.trade_date = m.date
           WHERE m.id IS NULL AND f.ticker IS NOT NULL AND f.trade_date IS NOT NULL
           LIMIT ?""",
        (limit,),
    ).fetchall()

    if not rows:
        logger.info("No filings pending enrichment")
        return

    tickers_dates = [(r["ticker"], r["trade_date"]) for r in rows]
    unique_pairs = list(set(tickers_dates))
    logger.info("Enriching %d unique ticker-date pairs", len(unique_pairs))

    for i in range(0, len(unique_pairs), YFINANCE_BATCH_SIZE):
        batch = unique_pairs[i:i + YFINANCE_BATCH_SIZE]
        for ticker, trade_date in batch:
            get_market_context(ticker, trade_date)

        if i + YFINANCE_BATCH_SIZE < len(unique_pairs):
            import time
            time.sleep(YFINANCE_DELAY_SECONDS)

    logger.info("Enrichment complete")
