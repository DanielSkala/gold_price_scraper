"""Expense-report file management.

Lists the CSV exports that feed the app, deriving each report's period (year/month
range) from the transactions inside it — the filenames themselves don't encode
dates. Also handles uploading new reports. Kept dependency-light and generic so it
works for any bank export the parser understands.
"""
import glob
import os
import re
import shutil
from datetime import datetime

from werkzeug.utils import secure_filename

from credit_card_expenses import parse_expenses_from_csv

EXPENSE_REPORTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'expense_reports')
SAMPLE_REPORTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'sample_reports')


def _month_label(ym):
    """'2025-03' → 'Mar 2025'."""
    try:
        return datetime.strptime(ym, '%Y-%m').strftime('%b %Y')
    except ValueError:
        return ym


def _report_meta(path):
    """Derive period + totals for one CSV by reading its transactions."""
    try:
        txs = parse_expenses_from_csv(path)
    except Exception:
        txs = []
    months = sorted({t['date'].strftime('%Y-%m') for t in txs})
    total = sum(t['amount'] for t in txs)
    stat = os.stat(path)
    return {
        'filename': os.path.basename(path),
        'transactions': len(txs),
        'total': round(total, 2),
        'from': months[0] if months else None,
        'to': months[-1] if months else None,
        'from_label': _month_label(months[0]) if months else None,
        'to_label': _month_label(months[-1]) if months else None,
        'months': len(months),
        'size_kb': round(stat.st_size / 1024, 1),
        'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d'),
    }


def list_reports():
    """All report files with derived metadata, newest period first."""
    files = sorted(glob.glob(os.path.join(EXPENSE_REPORTS_DIR, '*.csv')))
    reports = [_report_meta(f) for f in files]
    # Sort by the report's latest month (most recent first); undated last.
    reports.sort(key=lambda r: (r['to'] or ''), reverse=True)
    total_txns = sum(r['transactions'] for r in reports)
    all_months = set()
    for r in reports:
        if r['from'] and r['to']:
            all_months.add(r['from'])
            all_months.add(r['to'])
    return {
        'reports': reports,
        'count': len(reports),
        'total_transactions': total_txns,
    }


def save_report(file_storage):
    """Persist an uploaded CSV. Returns (ok, message_or_meta). Validates that it
    parses to at least one transaction so we don't accept junk files."""
    name = secure_filename(file_storage.filename or '')
    if not name.lower().endswith('.csv'):
        return False, 'Please upload a .csv file.'
    os.makedirs(EXPENSE_REPORTS_DIR, exist_ok=True)

    # Avoid clobbering an existing file: suffix with a counter if needed.
    base, ext = os.path.splitext(name)
    dest = os.path.join(EXPENSE_REPORTS_DIR, name)
    n = 1
    while os.path.exists(dest):
        dest = os.path.join(EXPENSE_REPORTS_DIR, f'{base} ({n}){ext}')
        n += 1

    file_storage.save(dest)

    # Validate it parses; if not, remove it and report the problem.
    try:
        txs = parse_expenses_from_csv(dest)
    except Exception:
        txs = []
    if not txs:
        try:
            os.remove(dest)
        except OSError:
            pass
        return False, 'That file has no transactions we could read.'
    return True, _report_meta(dest)


def delete_report(filename):
    """Delete a report by filename (basename only — no path traversal)."""
    safe = os.path.basename(filename)
    path = os.path.join(EXPENSE_REPORTS_DIR, safe)
    if not os.path.exists(path):
        return False, 'Report not found.'
    os.remove(path)
    return True, safe


def has_data():
    """True if there's at least one usable expense-report CSV."""
    return bool(glob.glob(os.path.join(EXPENSE_REPORTS_DIR, '*.csv')))


def load_samples():
    """Copy the bundled synthetic sample exports into the live reports dir so a fresh
    clone can explore the app instantly. Returns (ok, count_or_message)."""
    samples = sorted(glob.glob(os.path.join(SAMPLE_REPORTS_DIR, '*.csv')))
    if not samples:
        return False, 'No sample data is bundled with this build.'
    os.makedirs(EXPENSE_REPORTS_DIR, exist_ok=True)
    copied = 0
    for src in samples:
        dest = os.path.join(EXPENSE_REPORTS_DIR, os.path.basename(src))
        if not os.path.exists(dest):
            shutil.copy2(src, dest)
            copied += 1
    return True, copied
