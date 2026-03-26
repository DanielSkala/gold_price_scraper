"""
Rescore all filings with the current scoring logic.

Recomputes deterministic_score for all filings using:
- Filing data (trade_type, title, value, delta_own, is_10b5_1)
- Cluster data (from clusters table, matched via filing_ids JSON)
- Market data (from market_data table, matched via ticker)
- Insider history (lightweight: only checks if insider has prior buys)

Usage:
    python -m openinsider.scripts.rescore
    python -m openinsider.scripts.rescore --dry-run
    python -m openinsider.scripts.rescore --limit 1000
"""

import json
import logging
import time
from collections import defaultdict

from openinsider.analysis.signals import compute_score
from openinsider.db import get_connection, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 1000


def build_cluster_map(conn) -> dict:
    """Build filing_id -> cluster_info lookup from clusters table."""
    cluster_map = {}
    rows = conn.execute("SELECT filing_ids, participant_count FROM clusters").fetchall()
    for row in rows:
        try:
            filing_ids = json.loads(row["filing_ids"])
        except (json.JSONDecodeError, TypeError):
            continue
        info = {"participant_count": row["participant_count"]}
        for fid in filing_ids:
            cluster_map[fid] = info
    logger.info("Loaded %d clusters covering %d filings", len(rows), len(cluster_map))
    return cluster_map


def build_market_map(conn) -> dict:
    """Build ticker -> market_context lookup from market_data table."""
    market_map = {}
    rows = conn.execute("""
        SELECT ticker, drawdown_from_52w_high, market_cap
        FROM market_data
    """).fetchall()
    for row in rows:
        market_map[row["ticker"]] = {
            "drawdown_from_52w_high": row["drawdown_from_52w_high"] or 0,
            "market_cap": row["market_cap"] or 0,
        }
    logger.info("Loaded market data for %d tickers", len(market_map))
    return market_map


def build_insider_history_map(conn) -> dict:
    """Build insider_name -> lightweight history for first_buy scoring.

    For each insider, we only need to know:
    - Whether they have any prior buys
    - The date of their most recent buy
    This avoids loading full history for 100K+ insiders.
    """
    history_map = defaultdict(list)
    rows = conn.execute("""
        SELECT insider_name, trade_type, DATE(filing_date) as trade_date
        FROM filings
        WHERE insider_name IS NOT NULL
        ORDER BY insider_name, filing_date
    """).fetchall()
    for row in rows:
        history_map[row["insider_name"]].append({
            "trade_type": row["trade_type"],
            "trade_date": row["trade_date"],
        })
    logger.info("Loaded history for %d insiders", len(history_map))
    return history_map


def rescore_all(limit: int = 0, dry_run: bool = False):
    init_db()
    conn = get_connection()
    start = time.time()

    # Pre-load lookup tables
    logger.info("Building lookup tables...")
    cluster_map = build_cluster_map(conn)
    market_map = build_market_map(conn)
    insider_history_map = build_insider_history_map(conn)

    load_time = time.time() - start
    logger.info("Lookup tables loaded in %.1fs", load_time)

    # Fetch all filings
    query = """
        SELECT id, filing_url, filing_date, trade_date, ticker, company_name,
               insider_name, title, trade_type, price, qty, owned, delta_own,
               value, deterministic_score, is_10b5_1
        FROM filings ORDER BY id
    """
    if limit > 0:
        query += f" LIMIT {limit}"

    rows = conn.execute(query).fetchall()
    total = len(rows)
    logger.info("Rescoring %d filings...", total)

    batch_updates = []
    stats = {"unchanged": 0, "changed": 0, "total_delta": 0.0}
    score_dist = defaultdict(int)  # score bucket -> count

    for i, row in enumerate(rows):
        filing_row = dict(row)

        # Get context data
        ticker = filing_row.get("ticker", "")
        insider = filing_row.get("insider_name", "")
        filing_id = filing_row["id"]

        market_context = market_map.get(ticker, {})
        cluster_info = cluster_map.get(filing_id, {})

        # Build history EXCLUDING this filing (to avoid self-reference)
        full_history = insider_history_map.get(insider, [])
        # For first_buy, we need history of OTHER filings by this insider
        # Filter out entries with the same date as this filing to approximate
        this_date = (filing_row.get("filing_date") or "")[:10]
        insider_history = [
            h for h in full_history
            if h["trade_date"] != this_date
        ]

        new_score, _ = compute_score(filing_row, market_context, insider_history, cluster_info)
        old_score = filing_row.get("deterministic_score") or 0

        # Track score distribution
        bucket = (int(new_score) // 10) * 10
        score_dist[bucket] += 1

        if abs(new_score - old_score) > 0.01:
            stats["changed"] += 1
            stats["total_delta"] += new_score - old_score
            batch_updates.append((new_score, filing_id))
        else:
            stats["unchanged"] += 1

        # Batch commit
        if not dry_run and len(batch_updates) >= BATCH_SIZE:
            conn.executemany(
                "UPDATE filings SET deterministic_score = ? WHERE id = ?",
                batch_updates,
            )
            conn.commit()
            batch_updates = []

        # Progress
        if (i + 1) % 50000 == 0:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed
            eta = (total - i - 1) / rate / 60
            logger.info(
                "%d/%d (%.1f%%) | changed: %d | unchanged: %d | %.0f/s | ETA: %.1fm",
                i + 1, total, 100 * (i + 1) / total,
                stats["changed"], stats["unchanged"], rate, eta,
            )

    # Final batch
    if not dry_run and batch_updates:
        conn.executemany(
            "UPDATE filings SET deterministic_score = ? WHERE id = ?",
            batch_updates,
        )
        conn.commit()

    elapsed = time.time() - start
    avg_delta = stats["total_delta"] / stats["changed"] if stats["changed"] > 0 else 0

    logger.info("=" * 60)
    logger.info("Rescore complete in %.1f minutes", elapsed / 60)
    logger.info("Changed: %d | Unchanged: %d | Avg delta: %+.1f",
                stats["changed"], stats["unchanged"], avg_delta)
    if dry_run:
        logger.info("DRY RUN — no changes written to DB")

    logger.info("Score distribution:")
    for bucket in sorted(score_dist.keys()):
        count = score_dist[bucket]
        pct = 100 * count / total
        bar = "#" * int(pct)
        logger.info("  %3d-%3d: %6d (%5.1f%%) %s", bucket, bucket + 9, count, pct, bar)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Rescore all filings with current scoring logic")
    parser.add_argument("--limit", type=int, default=0, help="Max filings to rescore (0=all)")
    parser.add_argument("--dry-run", action="store_true", help="Compute scores without writing to DB")
    args = parser.parse_args()

    rescore_all(limit=args.limit, dry_run=args.dry_run)

    if not args.dry_run:
        from openinsider.analysis.research_stats import generate_research_json
        logger.info("Regenerating research stats JSON...")
        generate_research_json()


if __name__ == "__main__":
    main()
