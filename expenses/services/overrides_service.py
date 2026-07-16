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
import re
import threading

OVERRIDES_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'category_overrides.json')

# Legal-entity suffix tokens that carry no brand meaning ("Billa spol s r o" → "billa").
_LEGAL_SUFFIX_TOKENS = {
    "sro", "sr", "ro", "s", "r", "o", "as", "spol", "ltd", "inc", "llc", "gmbh",
    "kg", "co", "ltda", "plc", "kft", "ag", "bv", "oy", "ab",
}

# Web/TLD noise tokens so 'Netflix.com' and 'NETFLIX 8801' both group as 'netflix'.
_WEB_TOKENS = {"com", "www", "net", "org", "io", "app", "sk", "cz", "eu"}

_lock = threading.Lock()
_cache = None  # dict with keys "overrides" and "rules"


def tx_key(date_str, merchant, amount):
    """Stable identity for a transaction. `date_str` is YYYY-MM-DD, amount normalized to 2dp."""
    return f"{date_str}|{merchant}|{float(amount):.2f}"


def merchant_key(merchant):
    """Normalized merchant IDENTITY for a rule (case/space-insensitive).

    Kept intentionally exact (per-branch): this is the persisted key for merchant
    rules & overrides, so it must stay stable and precise. For BRAND-LEVEL grouping
    (recurring detection, clustering) use `normalize_merchant` instead."""
    return " ".join(str(merchant).lower().split())


def normalize_merchant(merchant):
    """Brand-level grouping key: collapses store branches to one merchant.

    Strips trailing store/branch numbers and location codes and drops legal-entity
    suffixes, so 'LIDL 164', 'LIDL 042 BA' and 'Lidl' all map to 'lidl', and
    'BILLA SPOL S RO 113' maps to 'billa'. Used for clustering transactions from the
    same brand together (recurring-charge detection, discovery) — NOT for rule
    identity (see `merchant_key`)."""
    s = str(merchant).lower()
    # Keep letters/digits/spaces; turn everything else into a separator.
    s = re.sub(r"[^a-z0-9]+", " ", s)
    tokens = s.split()
    # Drop trailing pure-digit tokens (store/branch numbers) and 2-letter city/branch
    # codes that follow them (e.g. 'lidl 042 ba' → 'lidl').
    while tokens and (tokens[-1].isdigit() or (len(tokens[-1]) <= 2 and tokens[-1].isalpha() and len(tokens) > 1 and any(t.isdigit() for t in tokens[:-1]))):
        tokens.pop()
    # Drop legal-entity suffix tokens anywhere they appear.
    tokens = [t for t in tokens if t not in _LEGAL_SUFFIX_TOKENS]
    # Drop any remaining standalone digit tokens (embedded reference numbers).
    tokens = [t for t in tokens if not t.isdigit()]
    # Drop web/TLD noise tokens, but only if a real brand token survives (so a
    # merchant literally named just "app"/"io" isn't erased).
    if any(t not in _WEB_TOKENS for t in tokens):
        tokens = [t for t in tokens if t not in _WEB_TOKENS]
    result = " ".join(tokens).strip()
    # Never collapse to nothing — fall back to the exact key if stripping ate it all.
    return result or merchant_key(merchant)


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


def repoint_category(old_category, new_category):
    """Repoint every merchant rule and per-transaction override from one category
    name to another. Used when a category is renamed (→ new name) or deleted
    (→ 'other'), so the transactions the user manually placed there follow along
    instead of silently reverting to keyword auto-categorisation."""
    with _lock:
        data = _load()
        for store in ('rules', 'overrides'):
            for k, v in list(data[store].items()):
                if v == old_category:
                    data[store][k] = new_category
        _save()


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
