"""LLM-powered filing analysis using OpenAI GPT."""

import json
import logging
from datetime import datetime

from openinsider.config import OPENAI_API_KEY, OPENAI_MODEL
from openinsider.db import get_connection

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are an expert insider-trading analyst. You will receive an evidence packet containing SEC Form 4 filing data, market context, insider profile history, and cluster information.

Your job is to assess the significance of this insider transaction and provide a structured analysis.

Rules:
- Only reason from evidence provided in the packet. Never fabricate data points.
- Be skeptical by default. Most insider purchases are routine.
- "high" priority requires deterministic_score > 60 AND at least 2 strong signals.
- Strong signals include: large purchase relative to salary, CEO/CFO buying, cluster buying, buying during drawdown, first purchase in 12+ months.
- Cite specific data points from the evidence packet.

You MUST respond with valid JSON in this exact format:
{
  "priority": "high|medium|low",
  "thesis": "1-3 sentence investment thesis",
  "bull_case": "why this signal is meaningful",
  "bear_case": "counterpoints and risks",
  "context_notes": "what makes this unusual or routine",
  "next_steps": ["2-4 research actions"],
  "confidence": 0.0-1.0,
  "evidence_citations": ["specific data points that drove your assessment"]
}"""


def _build_evidence_packet(filing_row):
    """Assemble evidence packet for a single filing."""
    conn = get_connection()
    cursor = conn.cursor()
    filing = dict(filing_row)
    filing_id = filing["id"]
    ticker = filing.get("ticker", "")
    trade_date = filing.get("trade_date", "")

    # Market context from market_data table
    cursor.execute(
        """SELECT market_cap, volume_ratio_20d, price_vs_200dma,
                  drawdown_from_52w_high
           FROM market_data
           WHERE ticker = ?
           ORDER BY date DESC LIMIT 1""",
        (ticker,),
    )
    mkt = cursor.fetchone()
    market_context = {}
    if mkt:
        mkt = dict(mkt)
        market_context = {
            "market_cap": mkt.get("market_cap"),
            "volume_ratio_20d": mkt.get("volume_ratio_20d"),
            "price_vs_200dma": mkt.get("price_vs_200dma"),
            "drawdown_from_52w_high": mkt.get("drawdown_from_52w_high"),
        }

    # Insider profile: past trades
    insider_name = filing.get("insider_name", "")
    cursor.execute(
        """SELECT ticker, trade_date, trade_type, value, price,
                  deterministic_score, llm_priority
           FROM filings
           WHERE insider_name = ? AND id != ?
           ORDER BY trade_date DESC LIMIT 20""",
        (insider_name, filing_id),
    )
    past_trades = [dict(r) for r in cursor.fetchall()]
    wins = sum(1 for t in past_trades if (t.get("deterministic_score") or 0) >= 50)
    insider_profile = {
        "name": insider_name,
        "title": filing.get("title", ""),
        "past_trade_count": len(past_trades),
        "historical_accuracy": round(wins / max(len(past_trades), 1), 2),
        "recent_trades": past_trades[:5],
    }

    # Cluster context
    cursor.execute(
        """SELECT insider_name, title, trade_date, value, qty
           FROM filings
           WHERE ticker = ?
             AND trade_type = 'P - Purchase'
             AND trade_date >= date(?, '-90 days')
             AND id != ?
           ORDER BY trade_date DESC""",
        (ticker, trade_date, filing_id),
    )
    cluster_trades = [dict(r) for r in cursor.fetchall()]
    cluster_context = {
        "other_insiders_buying": len(cluster_trades),
        "total_cluster_value": sum(t.get("value") or 0 for t in cluster_trades),
        "trades": cluster_trades[:10],
    }

    # Recent company filings
    cursor.execute(
        """SELECT insider_name, title, trade_type, trade_date, value, qty,
                  deterministic_score
           FROM filings
           WHERE ticker = ? AND id != ?
           ORDER BY trade_date DESC LIMIT 10""",
        (ticker, filing_id),
    )
    recent_company = [dict(r) for r in cursor.fetchall()]

    return {
        "filing": {k: v for k, v in filing.items()
                   if k not in ("llm_analysis", "raw_html")},
        "market_context": market_context,
        "insider_profile": insider_profile,
        "cluster_context": cluster_context,
        "deterministic_score": filing.get("deterministic_score", 0),
        "recent_company_filings": recent_company,
    }


def _call_openai(evidence_packet):
    """Call OpenAI API with evidence packet and return parsed analysis."""
    from openai import OpenAI

    client = OpenAI(api_key=OPENAI_API_KEY)
    user_content = json.dumps(evidence_packet, default=str)

    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        response_format={"type": "json_object"},
        temperature=0.3,
    )

    result = json.loads(response.choices[0].message.content)

    # Validate required fields
    required = ("priority", "thesis", "bull_case", "bear_case",
                "confidence", "evidence_citations")
    for field in required:
        if field not in result:
            result[field] = None

    if result.get("priority") not in ("high", "medium", "low"):
        result["priority"] = "low"
    if not isinstance(result.get("confidence"), (int, float)):
        result["confidence"] = 0.0
    result["confidence"] = max(0.0, min(1.0, float(result["confidence"])))

    return result


def analyze_filing(filing_id):
    """Analyze a single filing with LLM. Returns analysis dict."""
    if not OPENAI_API_KEY:
        logger.info("No OPENAI_API_KEY configured, skipping LLM analysis")
        return {}

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM filings WHERE id = ?", (filing_id,))
    row = cursor.fetchone()

    if not row:
        logger.warning("Filing %s not found", filing_id)
        return {}

    filing = dict(row)
    score = filing.get("deterministic_score") or 0
    if score < 40:
        logger.debug("Filing %s score %d below threshold, skipping", filing_id, score)
        return {}

    packet = _build_evidence_packet(row)

    try:
        analysis = _call_openai(packet)
    except Exception as e:
        logger.error("OpenAI call failed for filing %s: %s", filing_id, e)
        return {}

    # Store results
    cursor.execute(
        """UPDATE filings
           SET llm_analysis = ?, llm_priority = ?
           WHERE id = ?""",
        (json.dumps(analysis), analysis.get("priority"), filing_id),
    )
    conn.commit()

    logger.info("Analyzed filing %s: priority=%s confidence=%.2f",
                filing_id, analysis.get("priority"), analysis.get("confidence", 0))
    return analysis


def analyze_batch(limit=20):
    """Analyze unprocessed filings with score >= 40. Returns count analyzed."""
    if not OPENAI_API_KEY:
        logger.info("No OPENAI_API_KEY configured, skipping batch LLM analysis")
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id FROM filings
           WHERE deterministic_score >= 40
             AND llm_analysis IS NULL
           ORDER BY deterministic_score DESC
           LIMIT ?""",
        (limit,),
    )
    ids = [row["id"] for row in cursor.fetchall()]

    count = 0
    for fid in ids:
        result = analyze_filing(fid)
        if result:
            count += 1

    logger.info("Batch analysis complete: %d/%d filings analyzed", count, len(ids))
    return count
