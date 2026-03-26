import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

SENIORITY_MAP = {
    "ceo": 12, "chief executive": 12,
    "cfo": 13, "chief financial": 13,
    "coo": 10, "chief operating": 10, "president": 10,
    "director": 8,
    "vp": 9, "vice president": 9, "evp": 9, "svp": 9,
}
DEFAULT_SENIORITY = 5  # "Other" insiders perform ok, 10% owners are worst


def parse_insider_seniority(title: str) -> int:
    if not title:
        return DEFAULT_SENIORITY
    title_lower = title.lower()
    # Penalize 10% owners (empirically weakest group)
    if "10%" in title_lower:
        return 3
    for keyword, score in SENIORITY_MAP.items():
        if keyword in title_lower:
            return score
    return DEFAULT_SENIORITY


def _is_purchase(trade_type: str) -> bool:
    """Check if trade type is a purchase."""
    if not trade_type:
        return False
    return trade_type.strip().upper().startswith("P")


def _score_trade_type(trade_type: str) -> int:
    """15 points for purchases. This is the fundamental signal."""
    if _is_purchase(trade_type):
        return 15
    return 0


def _score_seniority(title: str) -> int:
    """Up to 13 points. CFOs empirically have best returns, then CEO, VP, Directors."""
    return parse_insider_seniority(title)


def _score_trade_value(value: Optional[float]) -> int:
    """Up to 15 points. Larger trades = more skin in the game = stronger signal.
    Data shows clear monotonic relationship: >1M gets 4.4% avg return vs <10K gets 1.8%."""
    if not value or value <= 0:
        return 0
    if value > 1_000_000:
        return 15
    if value > 500_000:
        return 12
    if value > 100_000:
        return 8
    if value > 10_000:
        return 4
    return 1


def _score_drawdown(drawdown: float) -> int:
    """Up to 12 points. Buying into weakness is contrarian and often profitable.
    Reduced from 15 since most filings lack this data."""
    if not drawdown:
        return 0
    dd = abs(drawdown)
    if dd > 30:
        return 12
    if dd > 20:
        return 8
    if dd > 10:
        return 4
    return 0


def _score_ownership_delta(delta_own: Optional[float], is_purchase: bool) -> int:
    """Up to 15 points for purchases only. This is the STRONGEST predictor.
    Data shows: >50% delta = 3.91% avg return, New positions (>90%) = 4.40%,
    while <1% delta = only 1.67%. The bigger the relative bet, the stronger the signal.
    Sales get 0 — a large sell-off is NOT a bullish signal."""
    if not delta_own or not is_purchase:
        return 0
    d = delta_own  # Positive for purchases (from SEC Form 4 data)
    if d < 0 or d > 500:
        return 0  # Ignore impossible values (e.g., -32768 sentinel)
    if d > 90:
        return 15  # New position - extremely bullish
    if d > 50:
        return 13
    if d > 20:
        return 10
    if d > 5:
        return 7
    if d > 1:
        return 4
    return 0


def _score_conviction(value: Optional[float], delta_own: Optional[float], is_purchase: bool) -> int:
    """Up to 10 points for purchases only. Combined conviction signal.
    When someone spends >$500K AND increases their stake by >10%, that's a strong conviction bet.
    Data shows: big bet + big delta = 60.7% win rate, 5.85% avg return.
    Sales get 0 — selling a lot is not "conviction" in the bullish sense."""
    if not value or not delta_own or not is_purchase:
        return 0
    if delta_own < 0 or delta_own > 500:
        return 0  # Ignore impossible values
    if value > 500_000 and delta_own > 10:
        return 10
    if value > 100_000 and delta_own > 20:
        return 8
    if value > 100_000 and delta_own > 5:
        return 5
    return 0


def _score_first_buy(insider_history: list, is_purchase: bool) -> int:
    """Up to 8 points for purchases only. First buy after long silence is notable.
    If we have NO history at all, we give 0 (unknown, not assumed first buy).
    Only score if we have actual history proving no recent buys."""
    if not is_purchase:
        return 0
    if not insider_history:
        return 0  # No data = unknown, not "first buy"
    from datetime import datetime, timedelta
    now = datetime.utcnow()
    buys = [h for h in insider_history if h.get("trade_type", "").upper().startswith("P")]
    if not buys:
        # We have sell/other history but no buys — this IS a first buy signal
        return 8
    latest = max(datetime.strptime(b["trade_date"], "%Y-%m-%d") for b in buys if b.get("trade_date"))
    months_ago = (now - latest).days / 30
    if months_ago > 12:
        return 8
    if months_ago > 6:
        return 5
    return 0


def _score_cluster(cluster_info: dict) -> int:
    """Up to 10 points. Multiple insiders buying together is a strong signal."""
    count = cluster_info.get("participant_count", 0)
    if count >= 3:
        return 10
    if count >= 2:
        return 6
    return 0


def _score_10b5_1(filing_row: dict) -> int:
    """Range: -20 to +5. 10b5-1 plan trades are pre-scheduled and uninformative.
    This is derived from the actual SEC Form 4 XML aff10b5One field.
    Returns:
      +5  if confirmed NOT a 10b5-1 plan (discretionary trade)
      0   if unknown (not yet checked or data unavailable)
      -20 if confirmed IS a 10b5-1 plan (automatic, uninformative)
    """
    is_10b5_1 = filing_row.get("is_10b5_1")
    if is_10b5_1 == 1:
        return -20
    if is_10b5_1 == 0:
        return 5
    return 0


def _score_small_cap(market_cap: float) -> int:
    """Up to 5 points. Insider trades in small caps are more informative
    because there's less analyst coverage. Reduced weight since data is rarely available."""
    if market_cap <= 0:
        return 0
    if market_cap < 2_000_000_000:
        return 5
    if market_cap < 10_000_000_000:
        return 3
    return 0


def compute_score(
    filing_row: dict,
    market_context: dict,
    insider_history: list,
    cluster_info: dict,
) -> tuple:
    """Compute deterministic score (0-100) for a filing.

    Score breakdown (max 100):
      - Trade type: 15 (purchase vs sale)
      - Seniority: 13 (CFO/CEO/VP/Director/Other/10%Owner)
      - Trade value: 15 (absolute dollar amount)
      - Ownership delta: 15 (relative size of bet - strongest predictor)
      - Conviction combo: 10 (big value + big delta combined)
      - First buy signal: 8 (buying after long silence)
      - Cluster buying: 10 (multiple insiders in same period)
      - 10b5-1 plan: -20 to +5 (from SEC Form 4 XML aff10b5One field)
      - Drawdown: 12 (buying into stock weakness, needs market data)
      - Small cap: 5 (less analyst coverage, needs market data)
      Total possible: 108, capped at 100, floored at 0
    """
    breakdown = {}
    is_purchase = _is_purchase(filing_row.get("trade_type", ""))

    breakdown["trade_type"] = _score_trade_type(filing_row.get("trade_type", ""))
    breakdown["seniority"] = _score_seniority(filing_row.get("title", ""))
    breakdown["trade_value"] = _score_trade_value(filing_row.get("value"))
    breakdown["drawdown"] = _score_drawdown(market_context.get("drawdown_from_52w_high", 0))
    breakdown["ownership_delta"] = _score_ownership_delta(filing_row.get("delta_own"), is_purchase)
    breakdown["conviction"] = _score_conviction(filing_row.get("value"), filing_row.get("delta_own"), is_purchase)
    breakdown["first_buy"] = _score_first_buy(insider_history, is_purchase)
    breakdown["cluster"] = _score_cluster(cluster_info)
    breakdown["plan_10b5_1"] = _score_10b5_1(filing_row)
    breakdown["small_cap"] = _score_small_cap(market_context.get("market_cap", 0))

    total = sum(breakdown.values())
    total = max(0, min(total, 100))

    return total, breakdown
