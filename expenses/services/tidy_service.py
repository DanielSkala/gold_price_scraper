"""Tidy-up queue for the 'other' bucket.

Powers the playful "let's categorize these" flow: it takes the transactions still
sitting in 'other', groups them by brand-level merchant, and ranks the groups by
IMPORTANCE so the user reviews the ones that matter first. Each queue item carries
enough context (counts, total, date range, sample photo days) for the UI to render
a rich card the user can file into a category or skip.

Payment gateways are excluded (their names don't say WHAT was bought), matching the
discovery service. Deterministic; no model.
"""
from collections import defaultdict

from services import discovery_service, overrides_service


def _importance(count, total):
    """Rank score: frequency and money both matter. A thing bought many times OR one
    that cost a lot is worth filing. sqrt-dampened total keeps one huge purchase from
    burying everything, while repeat purchases get real weight."""
    return count * 2.0 + (total ** 0.5)


def build_tidy_queue(other_txs, limit=40, order='importance'):
    """Group 'other' transactions by brand merchant and rank them for review.

    other_txs: [{date 'YYYY-MM-DD', merchant, amount}]. Returns
        {items: [ {merchant, display, count, total, avg, first_date, last_date,
                    dates:[...], sample_days:[dates], importance} ],
         other_total, other_count, group_count}
    """
    if not other_txs:
        return {"items": [], "other_total": 0.0, "other_count": 0, "group_count": 0}

    other_total = sum(t["amount"] for t in other_txs)

    # Skip payment-gateway rows (opaque names) — they stay in 'other' but aren't worth
    # surfacing as a card. Everything else is grouped by brand-level merchant.
    groups = defaultdict(list)
    for t in other_txs:
        ml = discovery_service._normalize(t["merchant"])
        if discovery_service._is_payment_gateway(ml):
            continue
        groups[overrides_service.normalize_merchant(t["merchant"])].append(t)

    items = []
    for norm, txs in groups.items():
        txs_sorted = sorted(txs, key=lambda t: t["date"])
        count = len(txs_sorted)
        total = sum(t["amount"] for t in txs_sorted)
        dates = [t["date"] for t in txs_sorted]
        # Prefer the most common raw spelling as the display name.
        display = max(set(t["merchant"] for t in txs_sorted),
                      key=lambda m: sum(1 for t in txs_sorted if t["merchant"] == m))
        # Up to 3 distinct days to look for photos (spread across the range).
        distinct_days = sorted(set(dates))
        if len(distinct_days) <= 3:
            sample_days = distinct_days
        else:
            sample_days = [distinct_days[0],
                           distinct_days[len(distinct_days) // 2],
                           distinct_days[-1]]
        items.append({
            "merchant": norm,
            "display": display,
            "count": count,
            "total": round(total, 2),
            "avg": round(total / count, 2),
            "first_date": dates[0],
            "last_date": dates[-1],
            "dates": dates,
            "sample_days": sample_days,
            "importance": round(_importance(count, total), 2),
        })

    if order == 'count':
        items.sort(key=lambda x: (-x["count"], -x["total"]))
    elif order == 'total':
        items.sort(key=lambda x: -x["total"])
    else:  # importance (default)
        items.sort(key=lambda x: -x["importance"])

    return {
        "items": items[:limit],
        "other_total": round(other_total, 2),
        "other_count": len(other_txs),
        "group_count": len(items),
    }
