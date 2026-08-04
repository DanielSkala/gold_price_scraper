"""Expense-report file management.

Lists the CSV exports that feed the app, deriving each report's period (year/month
range) from the transactions inside it — the filenames themselves don't encode
dates. Also handles uploading new reports. Kept dependency-light and generic so it
works for any bank export the parser understands.
"""
import glob
import hashlib
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


def _file_sha256(path):
    """SHA-256 of a file's bytes, streamed so large files don't load into memory."""
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def save_report(file_storage):
    """Persist an uploaded CSV. Returns (ok, message_or_meta). Validates that it
    parses to at least one transaction so we don't accept junk files, and rejects an
    exact byte-for-byte duplicate of a report we already have (uploading the same
    export twice would otherwise double every total)."""
    name = secure_filename(file_storage.filename or '')
    if not name.lower().endswith('.csv'):
        return False, 'Please upload a .csv file.'
    os.makedirs(EXPENSE_REPORTS_DIR, exist_ok=True)

    # Write to a temp file first so we can hash/validate before committing to a
    # permanent name. This also lets us reject exact-content duplicates cleanly.
    base, ext = os.path.splitext(name)
    tmp = os.path.join(EXPENSE_REPORTS_DIR, f'.upload-{base}{ext}.tmp')
    file_storage.save(tmp)

    def _cleanup_tmp():
        try:
            os.remove(tmp)
        except OSError:
            pass

    # Reject an exact-content duplicate of any existing report.
    try:
        new_hash = _file_sha256(tmp)
        for existing in glob.glob(os.path.join(EXPENSE_REPORTS_DIR, '*.csv')):
            if os.path.getsize(existing) == os.path.getsize(tmp) and _file_sha256(existing) == new_hash:
                _cleanup_tmp()
                return False, f'That file is identical to “{os.path.basename(existing)}”, which is already loaded.'
    except OSError:
        _cleanup_tmp()
        return False, 'Could not read the uploaded file.'

    # Validate it parses; if not, drop it and report the problem.
    try:
        txs = parse_expenses_from_csv(tmp)
    except Exception:
        txs = []
    if not txs:
        _cleanup_tmp()
        return False, 'That file has no transactions we could read.'

    # Commit to a permanent, non-clobbering name.
    dest = os.path.join(EXPENSE_REPORTS_DIR, name)
    n = 1
    while os.path.exists(dest):
        dest = os.path.join(EXPENSE_REPORTS_DIR, f'{base} ({n}){ext}')
        n += 1
    os.replace(tmp, dest)
    _invalidate_expenses()
    return True, _report_meta(dest)


def delete_report(filename):
    """Delete a report by filename (basename only — no path traversal)."""
    safe = os.path.basename(filename)
    path = os.path.join(EXPENSE_REPORTS_DIR, safe)
    if not os.path.exists(path):
        return False, 'Report not found.'
    os.remove(path)
    _invalidate_expenses()
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
    if copied:
        _invalidate_expenses()
    return True, copied


def _invalidate_expenses():
    """Drop the expense snapshot cache after a report file changes. Imported lazily to
    avoid a circular import (expenses_service imports credit_card_expenses, not us)."""
    try:
        from services import expenses_service
        expenses_service.invalidate_cache()
    except Exception:
        pass
