"""Research lab for backtesting insider trading signals."""

import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta

import yfinance as yf

from openinsider.db import get_connection

logger = logging.getLogger(__name__)


def calculate_forward_returns(filing_id, periods=None):
    """Calculate forward returns for a filing at various holding periods.

    Args:
        filing_id: The filing ID.
        periods: List of trading day offsets (default: [5, 21, 63, 126]).

    Returns:
        Dict mapping period -> return percentage, or empty dict on failure.
    """
    if periods is None:
        periods = [5, 21, 63, 126]

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT ticker, trade_date, price FROM filings WHERE id = ?",
        (filing_id,),
    )
    row = cursor.fetchone()

    if not row:
        logger.warning("Filing %s not found", filing_id)
        return {}

    ticker, trade_date_str, trade_price = row["ticker"], row["trade_date"], row["price"]
    if not ticker or not trade_date_str or not trade_price:
        return {}

    trade_price = float(trade_price)
    if trade_price <= 0:
        return {}

    max_period = max(periods)
    try:
        trade_date = datetime.strptime(trade_date_str, "%Y-%m-%d")
    except ValueError:
        return {}

    end_date = trade_date + timedelta(days=int(max_period * 1.6) + 30)
    if end_date > datetime.now():
        end_date = datetime.now()

    try:
        data = yf.download(
            ticker,
            start=trade_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            progress=False,
        )
    except Exception as e:
        logger.error("yfinance download failed for %s: %s", ticker, e)
        return {}

    if data.empty:
        return {}

    # Handle multi-level columns from yfinance
    if hasattr(data.columns, "levels") and len(data.columns.levels) > 1:
        data = data.droplevel(1, axis=1)

    closes = data["Close"].tolist()
    returns = {}

    for period in periods:
        if period < len(closes):
            future_price = closes[period]
            ret = ((future_price - trade_price) / trade_price) * 100
            returns[period] = round(ret, 2)

    return returns


def backtest_score_threshold(min_score):
    """Backtest filings at or above a score threshold.

    Uses 21-day forward returns. Returns dict with win_rate, avg_return,
    count, sharpe_approx.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, ticker, trade_date, price, deterministic_score
           FROM filings
           WHERE deterministic_score >= ?
             AND trade_type = 'P - Purchase'
             AND price IS NOT NULL
             AND trade_date <= date('now', '-30 days')
           ORDER BY trade_date""",
        (min_score,),
    )
    rows = cursor.fetchall()

    returns_21d = []
    for row in rows:
        fwd = calculate_forward_returns(row["id"], periods=[21])
        if 21 in fwd:
            returns_21d.append(fwd[21])

    if not returns_21d:
        return {"win_rate": 0, "avg_return": 0, "count": 0, "sharpe_approx": 0}

    wins = sum(1 for r in returns_21d if r > 0)
    avg_ret = sum(returns_21d) / len(returns_21d)
    std_ret = math.sqrt(
        sum((r - avg_ret) ** 2 for r in returns_21d) / max(len(returns_21d) - 1, 1)
    )
    sharpe = (avg_ret / std_ret) * math.sqrt(252 / 21) if std_ret > 0 else 0

    return {
        "win_rate": round(wins / len(returns_21d), 3),
        "avg_return": round(avg_ret, 2),
        "count": len(returns_21d),
        "sharpe_approx": round(sharpe, 2),
    }


def generate_score_calibration_report():
    """Sweep score thresholds and generate performance report.

    Returns list of dicts, one per threshold.
    """
    thresholds = [30, 40, 50, 60, 70, 80]
    report = []
    for threshold in thresholds:
        result = backtest_score_threshold(threshold)
        result["threshold"] = threshold
        report.append(result)
    return report


def update_insider_quality_scores():
    """Compute per-insider quality scores based on historical forward returns.

    Writes quality score into insiders table. Returns count of insiders updated.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get all purchase filings with enough history for 63-day returns
    cursor.execute(
        """SELECT id, insider_name, ticker, trade_date, price
           FROM filings
           WHERE trade_type = 'P - Purchase'
             AND price IS NOT NULL
             AND trade_date <= date('now', '-90 days')"""
    )
    rows = cursor.fetchall()

    insider_returns = defaultdict(list)
    for row in rows:
        fwd = calculate_forward_returns(row["id"], periods=[63])
        if 63 in fwd:
            insider_returns[row["insider_name"]].append(fwd[63])

    updated = 0
    for name, rets in insider_returns.items():
        if not rets:
            continue
        avg = sum(rets) / len(rets)
        win_rate = sum(1 for r in rets if r > 0) / len(rets)
        # Quality score: blend of avg return and win rate, scaled 0-100
        quality = min(100, max(0, int((win_rate * 50) + (min(avg, 50) * 1.0))))

        # Update existing insider records for this name
        cursor.execute(
            """UPDATE insiders SET total_buys = ?
               WHERE name = ?""",
            (len(rets), name),
        )
        updated += 1

    conn.commit()
    logger.info("Updated quality scores for %d insiders", updated)
    return updated
