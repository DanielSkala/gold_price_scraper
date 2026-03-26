"""Email notification system for significant insider trading signals."""

import json
import logging
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from openinsider.config import (
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD,
    ALERT_EMAIL_TO, MAX_EMAILS_PER_DAY, TICKER_COOLDOWN_HOURS,
)
from openinsider.db import get_connection

logger = logging.getLogger(__name__)

ALERT_FROM = SMTP_USER or "openinsider@localhost"


def _get_sent_today():
    """Count notifications sent today."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM notifications WHERE sent_at >= date('now')",
    )
    return cursor.fetchone()[0]


def _was_recently_notified(ticker):
    """Check if ticker had a notification in the last cooldown period."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT COUNT(*) FROM notifications
           WHERE message LIKE ?
             AND sent_at >= datetime('now', ?)""",
        (f"%{ticker}%", f"-{TICKER_COOLDOWN_HOURS} hours"),
    )
    return cursor.fetchone()[0] > 0


def _was_filing_notified(filing_id):
    """Check if this filing already triggered a notification."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM notifications WHERE filing_id = ?",
        (filing_id,),
    )
    return cursor.fetchone()[0] > 0


def _should_notify(filing_row, cluster_row=None):
    """Determine if a filing should trigger a notification."""
    score = filing_row.get("deterministic_score") or 0
    llm_priority = filing_row.get("llm_priority") or ""
    llm_analysis = filing_row.get("llm_analysis")
    llm_confidence = 0.0
    if llm_analysis:
        try:
            analysis = json.loads(llm_analysis) if isinstance(llm_analysis, str) else llm_analysis
            llm_confidence = float(analysis.get("confidence", 0))
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    value = filing_row.get("value") or 0
    title = (filing_row.get("title") or "").upper()
    ticker = filing_row.get("ticker") or ""

    # Condition 1: High deterministic score
    if score >= 75:
        return True

    # Condition 2: Medium score + high LLM priority + high confidence
    if score >= 50 and llm_priority == "high" and llm_confidence >= 0.7:
        return True

    # Condition 3: Cluster with 3+ insiders and total value >= 500k
    if cluster_row:
        cluster_count = cluster_row.get("participant_count", 0)
        cluster_value = cluster_row.get("total_value", 0)
        if cluster_count >= 3 and cluster_value >= 500000:
            return True

    # Condition 4: CEO/CFO purchase >= 200k at watchlisted ticker
    if any(t in title for t in ("CEO", "CFO")) and value >= 200000:
        from openinsider.research.watchlist import is_watchlisted
        if is_watchlisted(ticker):
            return True

    # Condition 5: First purchase in 12+ months, value >= 100k
    if value >= 100000 and filing_row.get("trade_type") == "P - Purchase":
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """SELECT COUNT(*) FROM filings
               WHERE ticker = ?
                 AND trade_type = 'P - Purchase'
                 AND trade_date >= date(?, '-365 days')
                 AND trade_date < ?
                 AND id != ?""",
            (ticker, filing_row.get("trade_date", ""),
             filing_row.get("trade_date", ""), filing_row.get("id")),
        )
        prior_count = cursor.fetchone()[0]
        if prior_count == 0:
            return True

    return False


def _build_email_html(filing_row, analysis=None):
    """Build HTML email body for a filing notification."""
    ticker = filing_row.get("ticker", "N/A")
    insider = filing_row.get("insider_name", "Unknown")
    title = filing_row.get("title", "")
    trade_type = filing_row.get("trade_type", "")
    value = filing_row.get("value") or 0
    score = filing_row.get("deterministic_score") or 0
    trade_date = filing_row.get("trade_date", "")
    price = filing_row.get("price", "")
    qty = filing_row.get("qty", "")
    filing_url = filing_row.get("filing_url", "")

    thesis = ""
    bull_case = ""
    bear_case = ""
    llm_priority = filing_row.get("llm_priority", "")
    if analysis:
        thesis = analysis.get("thesis", "")
        bull_case = analysis.get("bull_case", "")
        bear_case = analysis.get("bear_case", "")

    sec_link = ""
    if filing_url:
        sec_link = f"<a href='https://www.sec.gov{filing_url}'>View SEC Filing</a> | "

    llm_section = ""
    if thesis:
        llm_section = (
            "<hr style='margin: 16px 0;'>"
            "<h3 style='margin: 0 0 8px;'>LLM Analysis</h3>"
            f"<p><strong>Thesis:</strong> {thesis}</p>"
            f"<p><strong>Bull Case:</strong> {bull_case}</p>"
            f"<p><strong>Bear Case:</strong> {bear_case}</p>"
        )

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: #1a1a2e; color: white; padding: 20px; border-radius: 8px 8px 0 0;">
            <h2 style="margin: 0;">Insider Alert: {ticker}</h2>
            <p style="margin: 5px 0 0; opacity: 0.8;">Score: {score}/100 | Priority: {llm_priority or 'N/A'}</p>
        </div>
        <div style="padding: 20px; border: 1px solid #ddd; border-top: none;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr><td style="padding: 6px 0; font-weight: bold;">Insider</td><td>{insider}</td></tr>
                <tr><td style="padding: 6px 0; font-weight: bold;">Title</td><td>{title}</td></tr>
                <tr><td style="padding: 6px 0; font-weight: bold;">Type</td><td>{trade_type}</td></tr>
                <tr><td style="padding: 6px 0; font-weight: bold;">Value</td><td>${value:,.0f}</td></tr>
                <tr><td style="padding: 6px 0; font-weight: bold;">Price</td><td>{price}</td></tr>
                <tr><td style="padding: 6px 0; font-weight: bold;">Qty</td><td>{qty}</td></tr>
                <tr><td style="padding: 6px 0; font-weight: bold;">Trade Date</td><td>{trade_date}</td></tr>
            </table>
            {llm_section}
            <hr style="margin: 16px 0;">
            <p>{sec_link}<a href="http://localhost:5000/">Open Dashboard</a></p>
        </div>
        <div style="background: #f5f5f5; padding: 10px 20px; border-radius: 0 0 8px 8px; font-size: 12px; color: #666;">
            OpenInsider Intelligence Platform | {datetime.now().strftime('%Y-%m-%d %H:%M')}
        </div>
    </body>
    </html>
    """
    return html


def _send_email(subject, html_body):
    """Send an HTML email via SMTP. Returns True on success."""
    if not SMTP_HOST:
        logger.info("No SMTP_HOST configured, skipping email send")
        return False

    if not ALERT_EMAIL_TO:
        logger.warning("No ALERT_EMAIL_TO configured")
        return False

    recipients = [e.strip() for e in ALERT_EMAIL_TO.split(",") if e.strip()]
    if not recipients:
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = ALERT_FROM
        msg["To"] = ", ".join(recipients)
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            if SMTP_PORT != 25:
                server.starttls()
            if SMTP_USER and SMTP_PASSWORD:
                server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(ALERT_FROM, recipients, msg.as_string())

        logger.info("Email sent: %s", subject)
        return True
    except Exception as e:
        logger.error("Failed to send email: %s", e)
        return False


def _log_notification(filing_id, ticker, subject):
    """Record notification in the database."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO notifications (filing_id, channel, message, sent_at)
           VALUES (?, 'email', ?, datetime('now'))""",
        (filing_id, f"{ticker}: {subject}"),
    )
    conn.commit()


def check_and_notify():
    """Check all recent filings and send notifications as needed. Returns count sent."""
    if not SMTP_HOST:
        logger.info("No SMTP_HOST configured, skipping notifications")
        return 0

    sent_today = _get_sent_today()
    if sent_today >= MAX_EMAILS_PER_DAY:
        logger.info("Daily email limit (%d) reached", MAX_EMAILS_PER_DAY)
        return 0

    conn = get_connection()
    cursor = conn.cursor()

    # Get recent scored filings not yet notified
    cursor.execute(
        """SELECT f.*, c.participant_count AS cluster_participant_count,
                  c.total_value AS cluster_total_value
           FROM filings f
           LEFT JOIN clusters c ON f.ticker = c.ticker
             AND f.trade_date >= c.start_date
             AND f.trade_date <= c.end_date
           WHERE f.deterministic_score IS NOT NULL
             AND f.created_at >= datetime('now', '-24 hours')
           ORDER BY f.deterministic_score DESC""",
    )
    filings = cursor.fetchall()

    count = 0
    for row in filings:
        if sent_today + count >= MAX_EMAILS_PER_DAY:
            break

        filing = dict(row)
        filing_id = filing["id"]
        ticker = filing.get("ticker", "")

        if _was_filing_notified(filing_id):
            continue
        if _was_recently_notified(ticker):
            continue

        cluster_row = None
        if filing.get("cluster_participant_count"):
            cluster_row = {
                "participant_count": filing["cluster_participant_count"],
                "total_value": filing.get("cluster_total_value", 0),
            }

        if not _should_notify(filing, cluster_row):
            continue

        # Build and send email
        analysis = None
        if filing.get("llm_analysis"):
            try:
                analysis = json.loads(filing["llm_analysis"]) if isinstance(filing["llm_analysis"], str) else filing["llm_analysis"]
            except (json.JSONDecodeError, TypeError):
                pass

        score = filing.get("deterministic_score", 0)
        subject = f"{ticker} - Score {score} - {filing.get('insider_name', 'Unknown')}"
        html = _build_email_html(filing, analysis)

        if _send_email(f"[OpenInsider] {subject}", html):
            _log_notification(filing_id, ticker, subject)
            count += 1

    logger.info("Notifications sent: %d", count)
    return count
