"""Tidy-up queue for the fallback ("uncategorized") bucket.

Powers the playful "let's categorize these" flow: it takes the transactions nothing has
classified yet, groups them by brand-level merchant, and scores the groups by
IMPORTANCE so the ones that matter surface. Each queue item carries enough context
(counts, total, date range, sample photo days) for the UI to render a rich card the
user can file into a category or skip.

Only `limit` of (often hundreds of) groups fit in one queue, so the DEFAULT ordering
is a stochastic MIX rather than a straight importance sort: a strict ranking meant
every visit opened on the same card, and a merchant the user kept skipping came back at
position 1 forever while the long tail was never reachable at all. See
`_mixed_selection`. The deterministic orders are still available for callers that
want a stable ranking.

The caller decides WHICH transactions to pass in — today only still-uncategorized ones,
so a merchant deliberately filed into the real 'other' category never reappears here.

Payment gateways are excluded (their names don't say WHAT was bought), matching the
discovery service. No model.
"""
import random
from collections import defaultdict

from services import discovery_service, overrides_service

# Share of the queue drawn from the high-importance head (the rest comes from the
# long tail), so a visit feels familiar without being a repeat of the last one.
_HEAD_SHARE = 0.5
# How many of the top-ranked groups count as the "head" we sample those slots from,
# as a multiple of the head slots. >1 so which meaningful merchants appear varies.
_HEAD_WINDOW = 4


def _importance(count, total):
    """Rank score: frequency and money both matter. A thing bought many times OR one
    that cost a lot is worth filing. sqrt-dampened total keeps one huge purchase from
    burying everything, while repeat purchases get real weight."""
    return count * 2.0 + (total ** 0.5)


def _weighted_sample(candidates, k, weight_of, rng):
    """Sample up to `k` distinct items without replacement, weight ∝ `weight_of(item)`.

    Same approach as the mosaic-photo picker: heavier items are likelier, but nothing is
    guaranteed and nothing is excluded."""
    pool = list(candidates)
    picked = []
    while pool and len(picked) < k:
        total = sum(max(weight_of(c), 1e-9) for c in pool)
        r = rng.uniform(0, total)
        upto = 0.0
        for idx, c in enumerate(pool):
            upto += max(weight_of(c), 1e-9)
            if upto >= r:
                picked.append(pool.pop(idx))
                break
        else:                       # float drift left `r` past the end — take the last
            picked.append(pool.pop())
    return picked


def _mixed_selection(items, limit, rng):
    """Choose and order `limit` cards so no two visits look the same.

    Half the slots are drawn from the high-importance head, weighted by importance, so
    the merchants that actually matter keep surfacing — but WHICH of them varies. The
    rest are drawn uniformly from everything else, giving the long tail (which a strict
    ranking buries forever) a real chance. The final order is shuffled, so the queue
    never opens on the same card twice running.
    """
    ranked = sorted(items, key=lambda x: -x["importance"])
    if len(ranked) <= limit:
        rng.shuffle(ranked)
        return ranked

    head_slots = max(1, int(round(limit * _HEAD_SHARE)))
    head_size = min(len(ranked), max(limit, head_slots * _HEAD_WINDOW))
    head, tail = ranked[:head_size], ranked[head_size:]

    # sqrt-dampened weight: raw importance is so top-heavy that the single biggest
    # merchant would ride along in ~85% of visits. Dampening keeps the preference for
    # what matters while spreading the draw across the head (~50% for the top group).
    picked = _weighted_sample(head, head_slots, lambda it: it["importance"] ** 0.5, rng)
    # Identity, not equality: two groups can carry equal stats but are distinct cards.
    taken = {id(it) for it in picked}
    leftovers = [it for it in head if id(it) not in taken] + tail
    slots = limit - len(picked)
    if slots > 0 and leftovers:
        picked += rng.sample(leftovers, min(slots, len(leftovers)))
    rng.shuffle(picked)
    return picked


def build_tidy_queue(other_txs, limit=40, order='mix', seed=None):
    """Group uncategorized transactions by brand merchant and pick a queue to review.

    other_txs: [{date 'YYYY-MM-DD', merchant, amount}].
    order: 'mix' (default) = importance-weighted but varies every call, see
        `_mixed_selection`; 'importance' / 'count' / 'total' = stable ranking.
    seed: only used by 'mix' — pass one to reproduce a particular shuffle.

    Returns
        {items: [ {merchant, display, count, total, avg, first_date, last_date,
                    dates:[...], sample_days:[dates], importance} ],
         other_total, other_count, group_count}
    """
    if not other_txs:
        return {"items": [], "other_total": 0.0, "other_count": 0, "group_count": 0}

    other_total = sum(t["amount"] for t in other_txs)

    # Skip payment-gateway rows (opaque names) — they stay in the bucket but aren't worth
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
        queue = items[:limit]
    elif order == 'total':
        items.sort(key=lambda x: -x["total"])
        queue = items[:limit]
    elif order == 'importance':
        items.sort(key=lambda x: -x["importance"])
        queue = items[:limit]
    else:  # 'mix' (default) — a different, still importance-led queue every visit
        rng = random.Random(seed) if seed is not None else random.Random()
        queue = _mixed_selection(items, limit, rng)

    return {
        "items": queue,
        "other_total": round(other_total, 2),
        "other_count": len(other_txs),
        "group_count": len(items),
    }
