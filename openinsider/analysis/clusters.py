import json
import logging
from datetime import datetime, timedelta

from openinsider.db import get_connection
from openinsider.analysis.signals import parse_insider_seniority

logger = logging.getLogger(__name__)

CLUSTER_WINDOW_DAYS = 14
MIN_INSIDERS = 2


def detect_clusters(ticker: str, lookback_days: int = 90) -> list:
    conn = get_connection()
    cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        """SELECT id, trade_date, insider_name, title, value, trade_type
           FROM filings
           WHERE ticker = ? AND trade_date >= ? AND trade_type LIKE 'P%'
           ORDER BY trade_date""",
        (ticker, cutoff),
    ).fetchall()

    if len(rows) < MIN_INSIDERS:
        return []

    filings = [dict(r) for r in rows]
    clusters = []

    dates = sorted(set(f["trade_date"] for f in filings if f["trade_date"]))
    if not dates:
        return []

    start_dt = datetime.strptime(dates[0], "%Y-%m-%d")
    end_dt = datetime.strptime(dates[-1], "%Y-%m-%d")
    current = start_dt

    while current <= end_dt:
        window_end = current + timedelta(days=CLUSTER_WINDOW_DAYS)
        window_filings = [
            f for f in filings
            if f["trade_date"] and current <= datetime.strptime(f["trade_date"], "%Y-%m-%d") <= window_end
        ]

        unique_insiders = set(f["insider_name"] for f in window_filings if f["insider_name"])
        if len(unique_insiders) >= MIN_INSIDERS:
            cluster = {
                "ticker": ticker,
                "start_date": current.strftime("%Y-%m-%d"),
                "end_date": window_end.strftime("%Y-%m-%d"),
                "insider_names": json.dumps(list(unique_insiders)),
                "filing_ids": json.dumps([f["id"] for f in window_filings]),
                "participant_count": len(unique_insiders),
                "total_value": sum(f["value"] or 0 for f in window_filings),
            }
            cluster.update(score_cluster(window_filings))
            clusters.append(cluster)

        current += timedelta(days=1)

    merged = _merge_overlapping(clusters)

    for c in merged:
        _store_cluster(c)

    return merged


def _merge_overlapping(clusters: list) -> list:
    if not clusters:
        return []

    clusters.sort(key=lambda c: c["start_date"])
    merged = [clusters[0]]
    for c in clusters[1:]:
        prev = merged[-1]
        if c["start_date"] <= prev["end_date"]:
            if c["participant_count"] > prev["participant_count"] or c["total_value"] > prev["total_value"]:
                merged[-1] = c
        else:
            merged.append(c)
    return merged


def score_cluster(filings: list) -> dict:
    if not filings:
        return {"avg_seniority": 0, "has_ceo": 0, "has_cfo": 0, "cluster_score": 0}

    seniorities = [parse_insider_seniority(f.get("title", "")) for f in filings]
    avg_seniority = sum(seniorities) / len(seniorities) if seniorities else 0

    titles_lower = [((f.get("title") or "")).lower() for f in filings]
    has_ceo = int(any("ceo" in t or "chief executive" in t for t in titles_lower))
    has_cfo = int(any("cfo" in t or "chief financial" in t for t in titles_lower))

    unique_insiders = set(f.get("insider_name") for f in filings if f.get("insider_name"))
    total_value = sum(f.get("value") or 0 for f in filings)

    score = 0.0
    score += min(len(unique_insiders) * 10, 40)
    score += min(total_value / 100_000, 20)
    score += avg_seniority
    score += has_ceo * 10
    score += has_cfo * 8

    return {
        "avg_seniority": round(avg_seniority, 2),
        "has_ceo": has_ceo,
        "has_cfo": has_cfo,
        "cluster_score": round(min(score, 100), 2),
    }


def _store_cluster(cluster: dict):
    conn = get_connection()
    conn.execute(
        """INSERT INTO clusters
           (ticker, start_date, end_date, insider_names, filing_ids,
            participant_count, total_value, avg_seniority, has_ceo, has_cfo, cluster_score)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            cluster["ticker"], cluster["start_date"], cluster["end_date"],
            cluster["insider_names"], cluster["filing_ids"],
            cluster["participant_count"], cluster["total_value"],
            cluster["avg_seniority"], cluster["has_ceo"], cluster["has_cfo"],
            cluster["cluster_score"],
        ),
    )
    conn.commit()


def detect_all_clusters(lookback_days: int = 90) -> list:
    conn = get_connection()
    cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    tickers = conn.execute(
        """SELECT DISTINCT ticker FROM filings
           WHERE trade_date >= ? AND trade_type LIKE 'P%' AND ticker IS NOT NULL""",
        (cutoff,),
    ).fetchall()

    all_clusters = []
    for row in tickers:
        ticker = row["ticker"]
        clusters = detect_clusters(ticker, lookback_days)
        all_clusters.extend(clusters)
        logger.info("Ticker %s: %d clusters", ticker, len(clusters))

    logger.info("Total clusters detected: %d", len(all_clusters))
    return all_clusters
