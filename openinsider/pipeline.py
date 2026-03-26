"""Full pipeline orchestrator. Runs all steps in sequence."""

import asyncio
import logging
from datetime import datetime

from openinsider.db import get_connection

logger = logging.getLogger(__name__)


def run_pipeline():
    """Execute the full ingestion-to-notification pipeline. Returns stats dict."""
    from openinsider.ingestion.ingest import run_ingestion
    from openinsider.analysis.enrichment import enrich_pending_filings
    from openinsider.analysis.scoring import score_all_unscored
    from openinsider.analysis.clusters import detect_all_clusters
    from openinsider.integrations.llm_analyst import analyze_batch
    from openinsider.integrations.notifications import check_and_notify

    start = datetime.now()
    stats = {}
    logger.info("Pipeline started at %s", start.isoformat())

    try:
        logger.info("Step 1/6: Ingestion")
        stats["ingested"] = asyncio.run(run_ingestion())

        logger.info("Step 2/7: Enrichment")
        stats["enriched"] = enrich_pending_filings()

        logger.info("Step 3/7: 10b5-1 plan detection (SEC XML)")
        try:
            from openinsider.scripts.enrich_10b5_1 import enrich_recent_filings
            stats["10b5_1_enriched"] = enrich_recent_filings(limit=50)
        except Exception as e:
            logger.warning("10b5-1 enrichment failed: %s", e)
            stats["10b5_1_enriched"] = 0

        logger.info("Step 4/7: Scoring")
        stats["scored"] = score_all_unscored()

        logger.info("Step 5/7: Cluster detection")
        stats["clusters"] = detect_all_clusters()

        logger.info("Step 6/7: LLM analysis")
        stats["analyzed"] = analyze_batch()

        logger.info("Step 7/8: Notifications")
        stats["notified"] = check_and_notify()

        logger.info("Step 8/8: Research stats regeneration")
        try:
            from openinsider.analysis.research_stats import generate_research_json
            generate_research_json()
        except Exception as e:
            logger.warning("Research stats generation failed: %s", e)

        stats["status"] = "success"
    except Exception as e:
        logger.error("Pipeline failed: %s", e, exc_info=True)
        stats["status"] = "error"
        stats["error"] = str(e)

    elapsed = (datetime.now() - start).total_seconds()
    stats["elapsed_seconds"] = round(elapsed, 1)
    logger.info("Pipeline finished in %.1fs: %s", elapsed, stats)

    # Log to scrape_runs
    try:
        conn = get_connection()
        cursor = conn.cursor()
        error_msg = stats.get("error", "") if stats.get("status") == "error" else None
        cursor.execute(
            """INSERT INTO scrape_runs
               (started_at, finished_at, source, filings_found, status, error_message)
               VALUES (?, ?, 'pipeline', ?, ?, ?)""",
            (
                start.isoformat(),
                datetime.now().isoformat(),
                stats.get("ingested", 0),
                stats.get("status", "unknown"),
                error_msg,
            ),
        )
        conn.commit()
    except Exception as e:
        logger.error("Failed to log pipeline run: %s", e)

    return stats


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    result = run_pipeline()
    print(f"Pipeline complete: {result}")
