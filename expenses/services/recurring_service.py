"""Recurring-charge detection — a true periodicity engine.

Given the full transaction history, this clusters transactions by brand-level
merchant (see `overrides_service.normalize_merchant`) and infers, from the ACTUAL
intervals between charges, whether a merchant is billed on a regular cadence
(weekly / monthly / quarterly / yearly). For each recurring merchant it:

  - classifies the cadence from the median inter-arrival gap,
  - predicts the next charge date (last seen + one cadence),
  - detects a price change across billing cycles (early vs recent amount),
  - flags "zombie" charges (recurring but not seen in a while), and
  - splits spend into committed (recurring) vs discretionary.

Everything is deterministic string/number logic — no model, no API, and it uses
only date + merchant + amount, which the parser already provides.
"""
from collections import defaultdict
from datetime import datetime, timedelta
from statistics import median

from services import overrides_service

# Cadence buckets: (label, ideal gap in days, tolerance ± days). A merchant's median
# inter-arrival gap must fall inside one of these windows to count as recurring.
_CADENCES = [
    ("weekly", 7, 3),
    ("biweekly", 14, 4),
    ("monthly", 30, 8),
    ("quarterly", 91, 20),
    ("yearly", 365, 45),
]

# A cadence-per-day figure so different cadences can be compared as €/month.
_CADENCE_PER_MONTH = {
    "weekly": 52 / 12,
    "biweekly": 26 / 12,
    "monthly": 1.0,
    "quarterly": 1 / 3,
    "yearly": 1 / 12,
}

_MIN_OCCURRENCES = 3          # need at least 3 charges to infer a rhythm
_MAX_GAP_CV = 0.45            # gaps must be reasonably regular (coeff. of variation)
_MAX_AMOUNT_CV = 0.35         # amounts must be reasonably stable (bills/subs don't swing)
_MIN_AMOUNT_CONSISTENCY = 0.7 # ≥70% of charges within 15% of the median amount
_PRICE_CHANGE_MIN_PCT = 0.05  # ≥5% amount move to call it a price change
_ZOMBIE_MISSED_CYCLES = 2.5   # missed this many cycles → likely cancelled / zombie


def _classify_cadence(gap_days):
    """Return (label, ideal_gap) for the cadence bucket the median gap falls in, or None."""
    for label, ideal, tol in _CADENCES:
        if abs(gap_days - ideal) <= tol:
            return label, ideal
    return None


def _amount_trend(amounts):
    """Compare the earliest third of charges to the latest third; return a signed
    fraction (e.g. +0.15 = 15% pricier now) or 0.0 when stable / too few points."""
    n = len(amounts)
    if n < 4:
        # With few points, compare first vs last directly.
        first, last = amounts[0], amounts[-1]
    else:
        k = max(1, n // 3)
        first = sum(amounts[:k]) / k
        last = sum(amounts[-k:]) / k
    if first <= 0:
        return 0.0
    change = (last - first) / first
    return change if abs(change) >= _PRICE_CHANGE_MIN_PCT else 0.0


def detect_recurring(transactions, as_of=None):
    """Detect recurring merchants.

    transactions: list of {date: datetime, merchant: str, amount: float, category: str}.
    as_of: reference "today" (datetime) for next-charge & zombie logic. Defaults to the
           real current date (clamped to be no earlier than the latest transaction).
           "Is this charge still active?" is a present-tense question, so a subscription
           that stopped months ago is correctly a zombie and drops out of committed spend
           — matching the UI, which times predictions against the real today.

    Returns {
        recurring: [ {merchant, display, category, cadence, occurrences, avg_amount,
                      last_amount, monthly_cost, first_seen, last_seen, predicted_next,
                      price_change_pct, is_zombie} ],  # sorted by monthly_cost desc
        committed_monthly: float,   # sum of recurring monthly-equivalent cost (active only)
        count: int,
    }
    """
    if not transactions:
        return {"recurring": [], "committed_monthly": 0.0, "count": 0}

    if as_of is None:
        latest_tx = max(tx["date"] for tx in transactions)
        now = datetime.now()
        # Clamp to the latest transaction so future-dated data can't push predictions
        # past the data we actually have.
        as_of = max(now, latest_tx)

    # Group by brand-level merchant.
    groups = defaultdict(list)
    for tx in transactions:
        groups[overrides_service.normalize_merchant(tx["merchant"])].append(tx)

    recurring = []
    for norm, txs in groups.items():
        if len(txs) < _MIN_OCCURRENCES:
            continue
        txs = sorted(txs, key=lambda t: t["date"])

        # Inter-arrival gaps in days between consecutive charges.
        gaps = [(txs[i]["date"] - txs[i - 1]["date"]).days for i in range(1, len(txs))]
        gaps = [g for g in gaps if g > 0]  # ignore same-day duplicates
        if len(gaps) < 2:
            continue

        med_gap = median(gaps)
        if med_gap <= 0:
            continue
        # Regularity check: gaps must cluster (low spread relative to the median).
        mean_gap = sum(gaps) / len(gaps)
        variance = sum((g - mean_gap) ** 2 for g in gaps) / len(gaps)
        cv = (variance ** 0.5) / med_gap if med_gap else 99
        if cv > _MAX_GAP_CV:
            continue

        cadence = _classify_cadence(med_gap)
        if not cadence:
            continue
        cadence_label, ideal_gap = cadence

        amounts = [t["amount"] for t in txs]
        avg_amount = sum(amounts) / len(amounts)

        # Amount-stability gate: real bills & subscriptions charge a consistent amount.
        # This rejects same-cadence-but-variable spend (gas stations, restaurants, a
        # mall visited every fortnight) that a pure interval test would misfire on.
        med_amount = median(amounts)
        if med_amount <= 0:
            continue
        amt_mean = avg_amount
        amt_cv = (sum((a - amt_mean) ** 2 for a in amounts) / len(amounts)) ** 0.5 / amt_mean
        consistency = sum(1 for a in amounts if abs(a - med_amount) <= 0.15 * med_amount) / len(amounts)
        if amt_cv > _MAX_AMOUNT_CV or consistency < _MIN_AMOUNT_CONSISTENCY:
            continue

        last_seen = txs[-1]["date"]
        predicted_next = last_seen + timedelta(days=ideal_gap)

        # Zombie: hasn't recurred in well over one cycle → probably cancelled.
        days_since = (as_of - last_seen).days
        is_zombie = days_since > ideal_gap * _ZOMBIE_MISSED_CYCLES

        monthly_cost = avg_amount * _CADENCE_PER_MONTH[cadence_label]

        # Most common raw display name in the group (nicer than the normalized key).
        display = max(set(t["merchant"] for t in txs), key=lambda m: sum(1 for t in txs if t["merchant"] == m))
        # Category: whatever most of the group is currently classified as.
        cat = max(set(t["category"] for t in txs), key=lambda c: sum(1 for t in txs if t["category"] == c))

        recurring.append({
            "merchant": norm,
            "display": display,
            "category": cat,
            "cadence": cadence_label,
            "occurrences": len(txs),
            "avg_amount": round(avg_amount, 2),
            "last_amount": round(amounts[-1], 2),
            "monthly_cost": round(monthly_cost, 2),
            "first_seen": txs[0]["date"].strftime("%Y-%m-%d"),
            "last_seen": last_seen.strftime("%Y-%m-%d"),
            "predicted_next": predicted_next.strftime("%Y-%m-%d"),
            "price_change_pct": round(_amount_trend(amounts), 3),
            "is_zombie": is_zombie,
        })

    recurring.sort(key=lambda r: -r["monthly_cost"])
    committed = sum(r["monthly_cost"] for r in recurring if not r["is_zombie"])

    return {
        "recurring": recurring,
        "committed_monthly": round(committed, 2),
        "count": len(recurring),
    }
