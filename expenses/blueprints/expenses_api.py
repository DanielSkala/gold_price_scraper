"""Expense analytics API routes."""
import random
from collections import defaultdict

from flask import Blueprint, jsonify, request

from credit_card_expenses import CATEGORIES, CATEGORY_ORDER
from services import (
    discovery_service,
    overrides_service,
    photos_lib,
    recommendations_service,
    reports_service,
    settings_service,
)
from services.expenses_service import (
    compute_outlier_indices,
    get_expenses,
    get_expenses_data,
    get_monthly_data,
)

bp = Blueprint('expenses_api', __name__)


def _should_exclude_outliers():
    return request.args.get('exclude_outliers') == '1'


def _category_order():
    """Built-in category order plus any custom categories (so their spend shows
    up in the overview aggregations, not just the monthly detail view)."""
    customs = [c['name'] for c in overrides_service.get_custom_categories()]
    # keep 'other' last: insert customs before it
    order = [c for c in CATEGORY_ORDER if c != 'other'] + customs + ['other']
    return order


@bp.route('/api/monthly-data')
def monthly_data():
    monthly_sums, _ = get_monthly_data(exclude_outliers=_should_exclude_outliers())

    # Convert to format suitable for charts
    months = sorted(monthly_sums.keys())
    data = {
        'months': months,
        'categories': {},
        'totals': []
    }

    for category in _category_order():
        data['categories'][category] = [
            monthly_sums[month].get(category, 0.0) for month in months
        ]

    data['totals'] = [
        sum(monthly_sums[month].get(cat, 0.0) for cat in _category_order())
        for month in months
    ]

    return jsonify(data)


@bp.route('/api/category-totals')
def category_totals():
    monthly_sums, _ = get_monthly_data(exclude_outliers=_should_exclude_outliers())

    totals = {}
    for category in _category_order():
        total = sum(
            monthly_sums[month].get(category, 0.0)
            for month in monthly_sums.keys()
        )
        totals[category] = total

    return jsonify(totals)


@bp.route('/api/category-averages')
def category_averages():
    monthly_sums, _ = get_monthly_data(exclude_outliers=_should_exclude_outliers())

    if not monthly_sums:
        return jsonify({})

    all_months = sorted(monthly_sums.keys())
    num_months = len(all_months)

    # Calculate overall averages
    overall_averages = {}
    for category in _category_order():
        total = sum(
            monthly_sums[month].get(category, 0.0)
            for month in all_months
        )
        overall_averages[category] = total / num_months if num_months > 0 else 0

    # Calculate last 3 months averages
    last_3_months = all_months[-3:] if len(all_months) >= 3 else all_months
    last_3_averages = {}
    for category in _category_order():
        total = sum(
            monthly_sums[month].get(category, 0.0)
            for month in last_3_months
        )
        last_3_averages[category] = total / len(last_3_months) if last_3_months else 0

    return jsonify({
        'overall': overall_averages,
        'last_3_months': last_3_averages,
        'num_months': num_months,
        'last_3_count': len(last_3_months)
    })


@bp.route('/api/transactions')
def transactions():
    all_expenses = get_expenses(exclude_outliers=_should_exclude_outliers())

    # Convert datetime objects to strings for JSON serialization
    transactions_json = []
    for tx in all_expenses:
        tx_copy = tx.copy()
        tx_copy['date'] = tx['date'].strftime('%Y-%m-%d')
        transactions_json.append(tx_copy)

    # Sort by date descending
    transactions_json.sort(key=lambda x: x['date'], reverse=True)

    return jsonify(transactions_json)


@bp.route('/api/trends')
def trends():
    monthly_sums, _ = get_monthly_data(exclude_outliers=_should_exclude_outliers())
    months = sorted(monthly_sums.keys())

    if len(months) < 2:
        return jsonify({})

    trends = {}
    for category in _category_order():
        values = [monthly_sums[month].get(category, 0.0) for month in months]
        if len(values) >= 2:
            # Calculate simple trend (last month vs previous month)
            current = values[-1]
            previous = values[-2] if len(values) > 1 else 0
            trend = ((current - previous) / previous * 100) if previous > 0 else 0
            trends[category] = {
                'current': current,
                'previous': previous,
                'trend_percent': trend
            }

    return jsonify(trends)


@bp.route('/api/categories')
def categories():
    return jsonify({
        'categories': CATEGORIES,
        'category_order': CATEGORY_ORDER,
        'custom_categories': overrides_service.get_custom_categories(),
    })


def _valid_categories():
    """Built-in categories plus any user-added custom ones."""
    return set(CATEGORY_ORDER) | {c['name'] for c in overrides_service.get_custom_categories()}


@bp.route('/api/reports', methods=['GET'])
def reports():
    """List expense-report files with derived period/totals (dates read from the
    transactions inside each file, since filenames don't encode them)."""
    data = reports_service.list_reports()
    data['has_data'] = reports_service.has_data()
    return jsonify(data)


@bp.route('/api/reports/load-samples', methods=['POST'])
def load_sample_reports():
    """Copy the bundled synthetic sample exports into the live data dir (onboarding)."""
    ok, result = reports_service.load_samples()
    if not ok:
        return jsonify({'error': result}), 400
    return jsonify({'ok': True, 'copied': result})


@bp.route('/api/reports', methods=['POST'])
def upload_report():
    """Upload a new CSV expense report. Validates it parses before keeping it."""
    f = request.files.get('file')
    if not f:
        return jsonify({'error': 'no file provided'}), 400
    ok, result = reports_service.save_report(f)
    if not ok:
        return jsonify({'error': result}), 400
    return jsonify({'ok': True, 'report': result})


@bp.route('/api/reports/<path:filename>', methods=['DELETE'])
def delete_report(filename):
    ok, result = reports_service.delete_report(filename)
    if not ok:
        return jsonify({'error': result}), 404
    return jsonify({'ok': True, 'deleted': result})


@bp.route('/api/settings')
def settings():
    return jsonify(settings_service.get_all())


@bp.route('/api/settings/budget-goal', methods=['POST'])
def set_budget_goal():
    body = request.get_json(silent=True) or {}
    try:
        goal = round(float(body.get('budget_goal')))
    except (TypeError, ValueError):
        return jsonify({'error': 'budget_goal must be a number'}), 400
    goal = max(0, min(goal, 1000000))
    settings_service.set_value('budget_goal', goal)
    return jsonify({'ok': True, 'budget_goal': goal})


@bp.route('/api/custom-categories', methods=['GET', 'POST'])
def custom_categories():
    if request.method == 'GET':
        return jsonify({'custom_categories': overrides_service.get_custom_categories()})

    body = request.get_json(silent=True) or {}
    name = (body.get('name') or '').strip()
    color = (body.get('color') or '').strip()
    icon = (body.get('icon') or 'fa-tag').strip()
    if not name:
        return jsonify({'error': 'name is required'}), 400
    if name in CATEGORY_ORDER:
        return jsonify({'error': 'that category already exists'}), 400
    if not color:
        color = '#6366f1'
    entry = overrides_service.add_custom_category(name, color, icon)
    return jsonify({'ok': True, 'category': entry})


@bp.route('/api/create-category-from-suggestion', methods=['POST'])
def create_category_from_suggestion():
    """Create a custom category AND move the chosen merchants into it (via merchant
    rules), in one step. Body: {name, color, icon, merchants: [names]}.
    Used by the 'suggested categories' + button in the overview."""
    body = request.get_json(silent=True) or {}
    name = (body.get('name') or '').strip()
    color = (body.get('color') or '#6366f1').strip()
    icon = (body.get('icon') or 'fa-tag').strip()
    merchants = body.get('merchants') or []
    if not name:
        return jsonify({'error': 'name is required'}), 400
    if name in CATEGORY_ORDER:
        return jsonify({'error': 'that category already exists'}), 400

    entry = overrides_service.add_custom_category(name, color, icon)
    # Point each selected merchant's rule at the new category so all its txns move.
    for m in merchants:
        if m:
            overrides_service.set_merchant_rule(m, name)

    # Count how many transactions now fall under the new category.
    mkeys = {overrides_service.merchant_key(m) for m in merchants if m}
    affected = sum(
        1 for tx in get_expenses_data()
        if overrides_service.merchant_key(tx['merchant']) in mkeys
    )
    return jsonify({'ok': True, 'category': entry, 'affected': affected})


@bp.route('/api/current-month-details')
@bp.route('/api/current-month-details/<month>')
def current_month_details(month=None):
    monthly_sums, all_expenses = get_monthly_data(exclude_outliers=_should_exclude_outliers())

    if not monthly_sums:
        return jsonify({})

    # Get the specified month or most recent month
    if month and month in monthly_sums:
        selected_month = month
    else:
        selected_month = max(monthly_sums.keys())

    current_month_data = monthly_sums[selected_month]

    # Get transactions for selected month
    current_month_transactions = [
        tx for tx in all_expenses
        if tx["date"].strftime("%Y-%m") == selected_month
    ]

    # Group transactions by category
    category_transactions = defaultdict(list)
    for tx in current_month_transactions:
        tx_copy = tx.copy()
        tx_copy['date'] = tx['date'].strftime('%Y-%m-%d')
        category_transactions[tx['category']].append(tx_copy)

    # Daily spending for selected month
    daily_spending = defaultdict(float)
    for tx in current_month_transactions:
        day = tx["date"].strftime("%Y-%m-%d")
        daily_spending[day] += tx["amount"]

    return jsonify({
        'month': selected_month,
        'category_totals': dict(current_month_data),
        'category_transactions': dict(category_transactions),
        'daily_spending': dict(daily_spending),
        'total_transactions': len(current_month_transactions),
        'available_months': sorted(monthly_sums.keys(), reverse=True)
    })


@bp.route('/api/top-spend-days')
def top_spend_days():
    """The all-time highest-spending days, each bundled with curated photo(s) +
    location for the overview's photo mosaic / bubbles.
    Query: ?limit=N (days, default 7), ?photos_per_day=K (default 1)."""
    limit = max(1, min(int(request.args.get('limit', '7') or 7), 16))
    per_day = max(1, min(int(request.args.get('photos_per_day', '1') or 1), 6))
    all_expenses = get_expenses(exclude_outliers=_should_exclude_outliers())

    daily = defaultdict(float)
    for tx in all_expenses:
        daily[tx["date"].strftime("%Y-%m-%d")] += tx["amount"]

    ranked = sorted(daily.items(), key=lambda kv: -kv[1])[:limit]

    days = []
    for date, amount in ranked:
        photos = photos_lib.photos_for_day(date)
        curated = photos_lib.curate(photos, target=per_day)
        locations = photos_lib.top_locations(photos, limit=1)
        days.append({
            "date": date,
            "amount": round(amount, 2),
            "photo": curated[0] if curated else None,   # primary (back-compat)
            "photos": curated,                            # up to `per_day` curated photos
            "location": locations[0]["name"] if locations else (curated[0]["location"] if curated else None),
        })

    return jsonify({"days": days})


@bp.route('/api/mosaic-photos')
def mosaic_photos():
    """Photos for the overview mosaic backdrop — drawn from a RANDOM sample of days
    that have spend + photos, so the collage looks different on each load instead of
    always showing the same top-spend days. Higher-spend days are weighted a little
    more likely, but selection is genuinely varied. Query: ?days=N, ?photos_per_day=K,
    ?seed=S (optional, for a reproducible shuffle)."""
    n_days = max(1, min(int(request.args.get('days', '10') or 10), 24))
    per_day = max(1, min(int(request.args.get('photos_per_day', '4') or 4), 6))
    seed = request.args.get('seed')

    all_expenses = get_expenses(exclude_outliers=_should_exclude_outliers())
    daily = defaultdict(float)
    for tx in all_expenses:
        daily[tx["date"].strftime("%Y-%m-%d")] += tx["amount"]
    if not daily:
        return jsonify({"days": []})

    rng = random.Random(seed) if seed is not None else random.Random()
    # Weighted-random sample without replacement: weight ∝ spend, so bigger days
    # appear a bit more often, but any day can surface — variety every load.
    pool = list(daily.items())
    picked = []
    weights = {d: max(a, 1.0) for d, a in pool}
    candidates = pool[:]
    while candidates and len(picked) < n_days:
        total = sum(weights[d] for d, _ in candidates)
        r = rng.uniform(0, total)
        upto = 0
        for idx, (d, a) in enumerate(candidates):
            upto += weights[d]
            if upto >= r:
                picked.append((d, a))
                candidates.pop(idx)
                break

    days = []
    for date, amount in picked:
        photos = photos_lib.photos_for_day(date)
        curated = photos_lib.curate(photos, target=per_day)
        if not curated:
            continue
        days.append({
            "date": date,
            "amount": round(amount, 2),
            "photos": curated,
        })
    return jsonify({"days": days})


@bp.route('/api/category-transactions/<category>')
def category_transactions(category):
    all_expenses = get_expenses(exclude_outliers=_should_exclude_outliers())

    # Filter transactions by category
    category_txs = [
        tx for tx in all_expenses
        if tx['category'] == category
    ]

    # Convert datetime objects to strings and sort by date descending
    transactions_json = []
    for tx in category_txs:
        tx_copy = tx.copy()
        tx_copy['date'] = tx['date'].strftime('%Y-%m-%d')
        transactions_json.append(tx_copy)

    transactions_json.sort(key=lambda x: x['date'], reverse=True)

    return jsonify({
        'category': category,
        'transactions': transactions_json,
        'total_amount': sum(tx['amount'] for tx in category_txs),
        'transaction_count': len(transactions_json)
    })


@bp.route('/api/current-month-category-transactions/<category>')
@bp.route('/api/current-month-category-transactions/<category>/<month>')
def current_month_category_transactions(category, month=None):
    monthly_sums, all_expenses = get_monthly_data(exclude_outliers=_should_exclude_outliers())

    if not monthly_sums:
        return jsonify({'category': category, 'transactions': [], 'total_amount': 0, 'transaction_count': 0})

    # Get the specified month or most recent month
    if month and month in monthly_sums:
        selected_month = month
    else:
        selected_month = max(monthly_sums.keys())

    # Filter transactions by category and month
    category_txs = [
        tx for tx in all_expenses
        if tx['category'] == category and tx['date'].strftime('%Y-%m') == selected_month
    ]

    # Convert datetime objects to strings and sort by date descending
    transactions_json = []
    for tx in category_txs:
        tx_copy = tx.copy()
        tx_copy['date'] = tx['date'].strftime('%Y-%m-%d')
        transactions_json.append(tx_copy)

    transactions_json.sort(key=lambda x: x['date'], reverse=True)

    return jsonify({
        'category': category,
        'month': selected_month,
        'transactions': transactions_json,
        'total_amount': sum(tx['amount'] for tx in category_txs),
        'transaction_count': len(transactions_json)
    })


@bp.route('/api/outliers')
def outliers():
    all_expenses = get_expenses_data()
    outlier_indices = compute_outlier_indices(all_expenses)

    outlier_txs = []
    for i in sorted(outlier_indices):
        tx = all_expenses[i].copy()
        tx['date'] = tx['date'].strftime('%Y-%m-%d')
        outlier_txs.append(tx)

    outlier_txs.sort(key=lambda x: x['date'], reverse=True)
    total = sum(tx['amount'] for tx in outlier_txs)

    return jsonify({
        'outliers': outlier_txs,
        'total_amount': total,
        'count': len(outlier_txs)
    })


@bp.route('/api/override', methods=['POST'])
def set_category_override():
    """Manually reclassify a transaction. Body: {date, merchant, amount, category}.
    Pass category="" (or null) to clear the override and revert to auto-categorization."""
    body = request.get_json(silent=True) or {}
    date = body.get('date')
    merchant = body.get('merchant')
    amount = body.get('amount')
    category = body.get('category')

    if not date or merchant is None or amount is None:
        return jsonify({'error': 'date, merchant and amount are required'}), 400
    if category and category not in _valid_categories():
        return jsonify({'error': f'unknown category: {category}'}), 400

    # Learn a merchant rule too, so future transactions from this merchant
    # auto-categorize (unless the client opts out with apply_to_merchant=false).
    learn = body.get('apply_to_merchant', True)
    key = overrides_service.tx_key(date, merchant, amount)
    result = overrides_service.set_override(key, category, merchant=merchant if learn else None)

    # How many transactions (across ALL months) this change affects — i.e. how many
    # share this merchant. Lets the UI tell the user the size of the change.
    mkey = overrides_service.merchant_key(merchant)
    affected = sum(
        1 for tx in get_expenses_data()
        if overrides_service.merchant_key(tx['merchant']) == mkey
    )

    return jsonify({'ok': True, 'key': key, 'category': result, 'affected': affected, 'merchant': merchant})


@bp.route('/api/discover-categories')
def discover_categories():
    """Suggest new categories hidden inside the 'other' bucket. Deterministic;
    analyses only transactions currently categorised as 'other'."""
    all_expenses = get_expenses(exclude_outliers=_should_exclude_outliers())
    other_txs = []
    for tx in all_expenses:
        if tx['category'] == 'other':
            other_txs.append({
                'date': tx['date'].strftime('%Y-%m-%d'),
                'merchant': tx['merchant'],
                'amount': tx['amount'],
                'category': tx['category'],
            })
    return jsonify(discovery_service.discover_categories(other_txs))


@bp.route('/api/transactions-for-merchants', methods=['POST'])
def transactions_for_merchants():
    """Return all transactions belonging to a set of merchant names (normalised
    match). Used to preview which items fall under a *suggested* category before
    it's created. Body: {merchants: [names]}."""
    body = request.get_json(silent=True) or {}
    names = body.get('merchants') or []
    wanted = {overrides_service.merchant_key(m) for m in names if m}
    all_expenses = get_expenses(exclude_outliers=_should_exclude_outliers())
    txs = []
    for tx in all_expenses:
        if overrides_service.merchant_key(tx['merchant']) in wanted:
            c = tx.copy()
            c['date'] = tx['date'].strftime('%Y-%m-%d')
            txs.append(c)
    txs.sort(key=lambda x: x['date'], reverse=True)
    return jsonify({
        'transactions': txs,
        'total_amount': sum(t['amount'] for t in txs),
        'transaction_count': len(txs),
    })


@bp.route('/api/recommendations')
def recommendations():
    """Deterministic, local savings recommendations vs the knowledge base + goal.
    No LLM / API key — pure algorithm over the user's spending."""
    monthly_sums, all_expenses = get_monthly_data(exclude_outliers=_should_exclude_outliers())

    # Serialise txns (dates → strings) for the engine.
    def _ser(tx):
        return {
            'date': tx['date'].strftime('%Y-%m-%d'),
            'merchant': tx['merchant'],
            'amount': tx['amount'],
            'category': tx['category'],
        }

    recent_cutoff_months = sorted(monthly_sums.keys())[-3:] if monthly_sums else []
    recent_txs = [_ser(tx) for tx in all_expenses
                  if tx['date'].strftime('%Y-%m') in recent_cutoff_months]

    result = recommendations_service.build_recommendations({
        'monthly_sums': monthly_sums,
        'recent_txs': recent_txs,
        'all_txs': [_ser(tx) for tx in all_expenses],
        'goal': settings_service.get('budget_goal'),
    })
    return jsonify(result)
