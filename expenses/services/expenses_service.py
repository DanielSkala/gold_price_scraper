"""Expense data loading and aggregation.

Pure data logic — no Flask request handling here. Blueprints pass in the
`exclude_outliers` flag they read from the request.
"""
import glob
import os
import threading
from collections import defaultdict

from credit_card_expenses import parse_expenses_from_csv
from services import categories_service, overrides_service

EXPENSE_REPORTS_DIR = "./expense_reports"

# ── Processed-snapshot cache ─────────────────────────────────────────────────
# Parsing + categorising + applying overrides for every CSV on every request is the
# app's biggest repeated cost: a single dashboard load hits this pipeline 7-13 times.
# We cache the fully-processed transaction list and only rebuild it when something it
# depends on actually changes: the set of report files (path/mtime/size), the category
# catalog, or the override/rule store (both expose a monotonic generation()). Callers
# get fresh per-transaction dict copies so they can never corrupt the cached snapshot.
_cache_lock = threading.Lock()
_cache_key = None
_cache_txs = None


def _reports_signature():
    """A cheap fingerprint of the report files: (path, mtime_ns, size) per CSV. Changes
    whenever a file is added, removed, or modified in place."""
    sig = []
    for path in sorted(glob.glob(os.path.join(EXPENSE_REPORTS_DIR, "*.csv"))):
        try:
            st = os.stat(path)
        except OSError:
            continue
        sig.append((path, st.st_mtime_ns, st.st_size))
    return tuple(sig)


def _build_expenses_data():
    csv_files = glob.glob(os.path.join(EXPENSE_REPORTS_DIR, "*.csv"))
    all_expenses = []

    for csv_file in csv_files:
        # Isolate per-file parse failures: a single malformed CSV dropped into the
        # reports dir (a documented workflow) must not 500 every data endpoint. A
        # broken file contributes no rows instead — same tolerance reports_service
        # uses when deriving report metadata.
        try:
            file_expenses = parse_expenses_from_csv(csv_file)
        except Exception:
            continue
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


def invalidate_cache():
    """Force the next get_expenses_data() to rebuild. Called on report add/delete so a
    same-second file change (which mtime_ns already catches, but this is belt-and-braces)
    can't serve a stale snapshot."""
    global _cache_key, _cache_txs
    with _cache_lock:
        _cache_key = None
        _cache_txs = None


def get_expenses_data():
    """Fully-processed transactions (parsed + categorised + overrides applied).

    Returns fresh per-transaction copies backed by a snapshot cache keyed on the report
    files plus the category/override generations, so repeated calls within a request (or
    across a burst of dashboard requests) don't re-parse every CSV."""
    global _cache_key, _cache_txs
    key = (
        _reports_signature(),
        categories_service.generation(),
        overrides_service.generation(),
    )
    with _cache_lock:
        if key != _cache_key or _cache_txs is None:
            _cache_txs = _build_expenses_data()
            _cache_key = key
        snapshot = _cache_txs
    # Hand back shallow copies: transactions are flat dicts (date, merchant, amount,
    # category, overridden) and callers reassign scalar keys — copying the dict prevents
    # those writes from mutating the shared cached snapshot. The datetime is immutable.
    return [dict(tx) for tx in snapshot]


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

        median = _quantile(amounts, 0.5)
        q1 = _quantile(amounts, 0.25)
        q3 = _quantile(amounts, 0.75)
        iqr = q3 - q1
        iqr_bound = q3 + 1.5 * iqr
        median_bound = median * 4

        for idx, amount in items:
            if amount >= 100 and (amount > iqr_bound or amount >= median_bound):
                outlier_indices.add(idx)

    return outlier_indices


def _quantile(sorted_values, q):
    """Linear-interpolation quantile (same method as numpy/pandas default), so Q1/Q3
    are real statistical quartiles rather than raw array indices. `sorted_values` must
    be sorted ascending and non-empty."""
    n = len(sorted_values)
    if n == 1:
        return sorted_values[0]
    pos = q * (n - 1)
    lo = int(pos)
    frac = pos - lo
    if lo + 1 < n:
        return sorted_values[lo] + frac * (sorted_values[lo + 1] - sorted_values[lo])
    return sorted_values[lo]


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
