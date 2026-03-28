from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import json
from datetime import datetime
from credit_card_expenses import parse_expenses_from_csv, CATEGORIES, CATEGORY_ORDER
import glob
import os
from collections import defaultdict

app = Flask(__name__)
CORS(app)

def get_expenses_data():
    csv_files = glob.glob(os.path.join("./expense_reports", "*.csv"))
    all_expenses = []

    for csv_file in csv_files:
        file_expenses = parse_expenses_from_csv(csv_file)
        all_expenses.extend(file_expenses)

    return all_expenses


def compute_outlier_indices(expenses):
    """Detect outlier transactions using IQR method per category.

    A transaction is an outlier if:
    - Its amount exceeds Q3 + 1.5*IQR AND is at least €100, OR
    - Its amount is >= 4x the category median AND at least €100
      (fallback for small categories where IQR is too wide)
    """
    category_groups = defaultdict(list)
    for i, tx in enumerate(expenses):
        category_groups[tx['category']].append((i, tx['amount']))

    outlier_indices = set()

    for category, items in category_groups.items():
        amounts = sorted(amount for _, amount in items)
        n = len(amounts)
        if n < 3:
            continue

        median = amounts[n // 2]
        q1 = amounts[n // 4]
        q3 = amounts[(3 * n) // 4]
        iqr = q3 - q1
        iqr_bound = q3 + 1.5 * iqr
        median_bound = median * 4

        for idx, amount in items:
            if amount >= 100 and (amount > iqr_bound or amount > median_bound):
                outlier_indices.add(idx)

    return outlier_indices


def _should_exclude_outliers():
    return request.args.get('exclude_outliers') == '1'


def get_monthly_data(exclude_outliers=False):
    all_expenses = get_expenses_data()
    if exclude_outliers:
        outlier_indices = compute_outlier_indices(all_expenses)
        all_expenses = [tx for i, tx in enumerate(all_expenses) if i not in outlier_indices]

    monthly_sums = defaultdict(lambda: defaultdict(float))

    for tx in all_expenses:
        month = tx["date"].strftime("%Y-%m")
        monthly_sums[month][tx["category"]] += tx["amount"]

    return dict(monthly_sums), all_expenses

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/monthly-data')
def monthly_data():
    monthly_sums, _ = get_monthly_data(exclude_outliers=_should_exclude_outliers())

    # Convert to format suitable for charts
    months = sorted(monthly_sums.keys())
    data = {
        'months': months,
        'categories': {},
        'totals': []
    }

    for category in CATEGORY_ORDER:
        data['categories'][category] = [
            monthly_sums[month].get(category, 0.0) for month in months
        ]

    data['totals'] = [
        sum(monthly_sums[month].get(cat, 0.0) for cat in CATEGORY_ORDER)
        for month in months
    ]

    return jsonify(data)

@app.route('/api/category-totals')
def category_totals():
    monthly_sums, _ = get_monthly_data(exclude_outliers=_should_exclude_outliers())

    totals = {}
    for category in CATEGORY_ORDER:
        total = sum(
            monthly_sums[month].get(category, 0.0)
            for month in monthly_sums.keys()
        )
        totals[category] = total

    return jsonify(totals)

@app.route('/api/category-averages')
def category_averages():
    monthly_sums, _ = get_monthly_data(exclude_outliers=_should_exclude_outliers())

    if not monthly_sums:
        return jsonify({})

    all_months = sorted(monthly_sums.keys())
    num_months = len(all_months)

    # Calculate overall averages
    overall_averages = {}
    for category in CATEGORY_ORDER:
        total = sum(
            monthly_sums[month].get(category, 0.0)
            for month in all_months
        )
        overall_averages[category] = total / num_months if num_months > 0 else 0

    # Calculate last 3 months averages
    last_3_months = all_months[-3:] if len(all_months) >= 3 else all_months
    last_3_averages = {}
    for category in CATEGORY_ORDER:
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

@app.route('/api/transactions')
def transactions():
    all_expenses = get_expenses_data()
    if _should_exclude_outliers():
        outlier_indices = compute_outlier_indices(all_expenses)
        all_expenses = [tx for i, tx in enumerate(all_expenses) if i not in outlier_indices]

    # Convert datetime objects to strings for JSON serialization
    transactions_json = []
    for tx in all_expenses:
        tx_copy = tx.copy()
        tx_copy['date'] = tx['date'].strftime('%Y-%m-%d')
        transactions_json.append(tx_copy)

    # Sort by date descending
    transactions_json.sort(key=lambda x: x['date'], reverse=True)

    return jsonify(transactions_json)

@app.route('/api/trends')
def trends():
    monthly_sums, _ = get_monthly_data(exclude_outliers=_should_exclude_outliers())
    months = sorted(monthly_sums.keys())

    if len(months) < 2:
        return jsonify({})

    trends = {}
    for category in CATEGORY_ORDER:
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

@app.route('/api/categories')
def categories():
    return jsonify({
        'categories': CATEGORIES,
        'category_order': CATEGORY_ORDER
    })

@app.route('/api/current-month-details')
@app.route('/api/current-month-details/<month>')
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

@app.route('/api/category-transactions/<category>')
def category_transactions(category):
    all_expenses = get_expenses_data()
    if _should_exclude_outliers():
        outlier_indices = compute_outlier_indices(all_expenses)
        all_expenses = [tx for i, tx in enumerate(all_expenses) if i not in outlier_indices]

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

@app.route('/api/current-month-category-transactions/<category>')
@app.route('/api/current-month-category-transactions/<category>/<month>')
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

@app.route('/api/outliers')
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


if __name__ == '__main__':
    app.run(debug=True, port=5001)