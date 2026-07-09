"""Manual category overrides + learned merchant rules.

Two layers, both persisted to a JSON file so they survive restarts:

  overrides   {tx_key -> category}   — a single transaction reclassified
              (keyed by date|merchant|amount). Highest precedence.
  rules       {merchant_norm -> category}  — "remember this merchant forever":
              once you recategorize an item, every current AND future
              transaction from the same merchant auto-moves to that category.

File format: {"overrides": {...}, "rules": {...}}. Legacy files that were a
bare {tx_key: category} map are migrated on load.
"""
import json
import os
import threading

OVERRIDES_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'category_overrides.json')

_lock = threading.Lock()
_cache = None  # dict with keys "overrides" and "rules"


def tx_key(date_str, merchant, amount):
    """Stable identity for a transaction. `date_str` is YYYY-MM-DD, amount normalized to 2dp."""
    return f"{date_str}|{merchant}|{float(amount):.2f}"


def merchant_key(merchant):
    """Normalized merchant identity for a rule (case/space-insensitive)."""
    return " ".join(str(merchant).lower().split())


def _load():
    global _cache
    if _cache is not None:
        return _cache
    try:
        with open(OVERRIDES_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}
    # Migrate legacy shape (a flat {tx_key: category} map) into the new structure.
    if 'overrides' not in data and 'rules' not in data:
        data = {'overrides': data if isinstance(data, dict) else {}, 'rules': {}}
    data.setdefault('overrides', {})
    data.setdefault('rules', {})
    data.setdefault('custom_categories', [])  # [{name, color, icon}]
    _cache = data
    return _cache


def _save():
    os.makedirs(os.path.dirname(OVERRIDES_FILE), exist_ok=True)
    tmp = OVERRIDES_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(_cache, f, indent=2, ensure_ascii=False)
    os.replace(tmp, OVERRIDES_FILE)


def get_all():
    """Return the {tx_key: category} per-transaction overrides (read-only copy)."""
    return dict(_load()['overrides'])


def get_rules():
    """Return the {merchant_norm: category} merchant rules (read-only copy)."""
    return dict(_load()['rules'])


def get_custom_categories():
    """Return the list of user-added categories: [{name, color, icon}]."""
    return list(_load()['custom_categories'])


def add_custom_category(name, color, icon):
    """Add a custom category (idempotent by name). Returns the stored dict."""
    entry = {'name': name, 'color': color, 'icon': icon}
    with _lock:
        data = _load()
        existing = [c for c in data['custom_categories'] if c['name'].lower() == name.lower()]
        if existing:
            existing[0].update(entry)
        else:
            data['custom_categories'].append(entry)
        _save()
    return entry


def set_merchant_rule(merchant, category):
    """Directly set (or clear) the merchant→category rule for a merchant. Used when
    bulk-assigning merchants to a newly created category. Pass falsy category to clear."""
    with _lock:
        data = _load()
        mk = merchant_key(merchant)
        if category:
            data['rules'][mk] = category
        else:
            data['rules'].pop(mk, None)
        _save()
    return category or None


def get_override(key):
    return _load()['overrides'].get(key)


def set_override(key, category, merchant=None):
    """Reclassify one transaction. If `merchant` is given, ALSO learn a merchant
    rule so future transactions from that merchant auto-categorize.
    Pass a falsy `category` to clear both the override and the merchant rule."""
    with _lock:
        data = _load()
        if category:
            data['overrides'][key] = category
            if merchant is not None:
                data['rules'][merchant_key(merchant)] = category
        else:
            data['overrides'].pop(key, None)
            if merchant is not None:
                data['rules'].pop(merchant_key(merchant), None)
        _save()
    return category or None
