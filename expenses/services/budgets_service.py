"""Per-category budget pacing.

Turns the user's optional per-category monthly limits (settings_service) plus a
month's actual spend into a pacing view the UI can render directly. Pacing only
makes sense for a month that is still IN PROGRESS — for a past (complete) month we
report the final spend vs limit with no projection.

Pure logic; no Flask, no persistence here.
"""
from calendar import monthrange
from datetime import date


def _elapsed_fraction(month, today):
    """(elapsed_days, total_days, is_current) for `month` ('YYYY-MM') relative to `today`.

    - Past month → elapsed == total (complete), is_current False.
    - Future month → elapsed 0.
    - Current month → days up to and including today.
    """
    y, m = (int(x) for x in month.split('-'))
    total_days = monthrange(y, m)[1]
    if (y, m) < (today.year, today.month):
        return total_days, total_days, False   # fully elapsed, complete month
    if (y, m) > (today.year, today.month):
        return 0, total_days, False             # hasn't started
    return today.day, total_days, True          # in progress


def build_budget_pacing(month, category_totals, budgets, today=None):
    """Build per-category pacing for a month.

    month: 'YYYY-MM'. category_totals: {category: spent_this_month}.
    budgets: {category: monthly_limit}. today: date (defaults to date.today()).

    Returns {
        month, is_current, elapsed_days, total_days,
        items: { category: {
            limit, spent, pct,               # pct of limit spent (0..∞, as %)
            ideal_pct,                       # where you 'should' be today (current month only)
            projected, projected_pct,        # end-of-month projection (current month only)
            remaining, over,                 # euros left / euros over the limit
            status,                          # 'under' | 'on_track' | 'at_risk' | 'over'
        } },
        summary: { budgeted, at_risk, over } # counts for a roll-up
    }
    """
    today = today or date.today()
    elapsed, total, is_current = _elapsed_fraction(month, today)

    items = {}
    at_risk = over = 0
    for cat, limit in (budgets or {}).items():
        if not limit or limit <= 0:
            continue
        spent = round(float(category_totals.get(cat, 0.0)), 2)
        pct = (spent / limit * 100) if limit else 0.0

        entry = {
            'limit': round(float(limit), 2),
            'spent': spent,
            'pct': round(pct, 1),
            'remaining': round(limit - spent, 2),
            'over': round(max(0.0, spent - limit), 2),
        }

        if is_current and elapsed > 0:
            frac = elapsed / total
            ideal_pct = frac * 100
            projected = spent / frac
            projected_pct = projected / limit * 100 if limit else 0.0
            entry['ideal_pct'] = round(ideal_pct, 1)
            entry['projected'] = round(projected, 2)
            entry['projected_pct'] = round(projected_pct, 1)
            # Status from the PROJECTION (where you'll land), not just today's spend.
            if spent > limit:
                status = 'over'
            elif projected_pct > 110:
                status = 'over'
            elif projected_pct >= 100:
                status = 'at_risk'
            else:
                status = 'on_track'
        else:
            # Complete (or not-started) month: judge on final spend only.
            entry['ideal_pct'] = None
            entry['projected'] = None
            entry['projected_pct'] = None
            status = 'over' if spent > limit else ('at_risk' if pct >= 90 else 'under')

        entry['status'] = status
        if status == 'over':
            over += 1
        elif status == 'at_risk':
            at_risk += 1
        items[cat] = entry

    return {
        'month': month,
        'is_current': is_current,
        'elapsed_days': elapsed,
        'total_days': total,
        'items': items,
        'summary': {'budgeted': len(items), 'at_risk': at_risk, 'over': over},
    }
