"""Expense data loading and aggregation.

Pure data logic — no Flask request handling here. Blueprints pass in the
`exclude_outliers` flag they read from the request.
"""
import glob
import os
from collections import defaultdict

from credit_card_expenses import parse_expenses_from_csv
from services import categories_service, overrides_service

EXPENSE_REPORTS_DIR = "./expense_reports"


def get_expenses_data():
    csv_files = glob.glob(os.path.join(EXPENSE_REPORTS_DIR, "*.csv"))
    all_expenses = []

    for csv_file in csv_files:
        file_expenses = parse_expenses_from_csv(csv_file)
        all_expenses.extend(file_expenses)

    # Re-run auto-categorisation through the live category catalog, so runtime edits
    # to keywords / categories take effect without touching the CSV parser. (The
    # parser's own category is just a first pass; the catalog is the source of truth.)
    for tx in all_expenses:
        tx["category"] = categories_service.categorize(tx["merchant"])

    # Apply manual reclassifications, in precedence order:
    #   1. per-transaction override (exact date|merchant|amount)  — highest
    #   2. merchant rule (learned "remember this merchant forever")
    # Both flag the transaction as `overridden` so the UI can mark it.
    overrides = overrides_service.get_all()
    rules = overrides_service.get_rules()
    if overrides or rules:
        for tx in all_expenses:
            key = overrides_service.tx_key(tx["date"].strftime("%Y-%m-%d"), tx["merchant"], tx["amount"])
            new_cat = overrides.get(key)
            if not new_cat and rules:
                new_cat = rules.get(overrides_service.merchant_key(tx["merchant"]))
            if new_cat and new_cat != tx["category"]:
                tx["category"] = new_cat
                tx["overridden"] = True

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


def get_expenses(exclude_outliers=False):
    """All expenses, optionally with outliers removed."""
    all_expenses = get_expenses_data()
    if exclude_outliers:
        outlier_indices = compute_outlier_indices(all_expenses)
        all_expenses = [tx for i, tx in enumerate(all_expenses) if i not in outlier_indices]
    return all_expenses


def get_monthly_data(exclude_outliers=False):
    all_expenses = get_expenses(exclude_outliers=exclude_outliers)

    monthly_sums = defaultdict(lambda: defaultdict(float))

    for tx in all_expenses:
        month = tx["date"].strftime("%Y-%m")
        monthly_sums[month][tx["category"]] += tx["amount"]

    return dict(monthly_sums), all_expenses
