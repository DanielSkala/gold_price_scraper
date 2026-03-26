import json
import logging

from openinsider.db import get_connection
from openinsider.analysis.enrichment import get_market_context
from openinsider.analysis.signals import compute_score

logger = logging.getLogger(__name__)


def build_features(filing_row: dict) -> dict:
    conn = get_connection()

    market_context = {"drawdown_from_52w_high": 0, "market_cap": 0}
    if filing_row.get("ticker") and filing_row.get("trade_date"):
        md = conn.execute(
            "SELECT * FROM market_data WHERE ticker = ? AND date = ?",
            (filing_row["ticker"], filing_row["trade_date"]),
        ).fetchone()
        if md:
            market_context = dict(md)

    insider_history = []
    if filing_row.get("insider_name") and filing_row.get("ticker"):
        rows = conn.execute(
            """SELECT trade_date, trade_type FROM filings
               WHERE insider_name = ? AND ticker = ? AND id != ?
               ORDER BY trade_date DESC LIMIT 20""",
            (filing_row["insider_name"], filing_row["ticker"], filing_row.get("id", 0)),
        ).fetchall()
        insider_history = [dict(r) for r in rows]

    cluster_info = {"participant_count": 0}
    if filing_row.get("ticker"):
        cluster = conn.execute(
            """SELECT * FROM clusters
               WHERE ticker = ? AND start_date <= ? AND end_date >= ?
               ORDER BY cluster_score DESC LIMIT 1""",
            (filing_row["ticker"], filing_row.get("trade_date", ""), filing_row.get("trade_date", "")),
        ).fetchone()
        if cluster:
            cluster_info = dict(cluster)

    return {
        "filing": filing_row,
        "market_context": market_context,
        "insider_history": insider_history,
        "cluster_info": cluster_info,
    }


def score_all_unscored(limit: int = 200):
    conn = get_connection()
    rows = conn.execute(
        """SELECT * FROM filings
           WHERE deterministic_score IS NULL
           LIMIT ?""",
        (limit,),
    ).fetchall()

    if not rows:
        logger.info("No unscored filings")
        return

    logger.info("Scoring %d filings", len(rows))
    scored = 0

    for row in rows:
        filing = dict(row)
        features = build_features(filing)
        score, breakdown = compute_score(
            features["filing"],
            features["market_context"],
            features["insider_history"],
            features["cluster_info"],
        )

        conn.execute(
            "UPDATE filings SET deterministic_score = ? WHERE id = ?",
            (score, filing["id"]),
        )
        scored += 1

    conn.commit()
    logger.info("Scored %d filings", scored)
