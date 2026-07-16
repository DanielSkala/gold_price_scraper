"""Unified category catalog — the single source of truth for ALL categories.

Every category (built-in or user-created) is data, not code: a name plus a color,
an icon, and a list of merchant keywords used to auto-classify transactions. The
catalog is persisted to `data/categories.json` and fully editable at runtime, so
users can rename, restyle, re-key, add, and delete any category.

On first run the file is seeded from the historical hardcoded definitions (the
`_SEED` table below, mirroring the old `CATEGORIES` / colors / icons), and any
custom categories that previously lived in `data/category_overrides.json` are
migrated in — so nothing is lost and existing installs look identical.

Shape:
  {
    "order": ["groceries", "eating out", ..., "other"],
    "categories": {
      "groceries": {"color": "#0f766e", "icon": "fa-basket-shopping",
                    "keywords": ["lidl", ...], "builtin": true},
      "Subscriptions": {"color": "#7c3aed", "icon": "fa-cloud",
                        "keywords": [], "builtin": false},
      ...
    }
  }

`other` is special: it's the fallback bucket, always present and never deletable.
"""
import json
import os
import threading

from credit_card_expenses import CATEGORIES as _SEED_KEYWORDS, CATEGORY_ORDER as _SEED_ORDER

CATEGORIES_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'categories.json')
# Legacy custom-category store — migrated in on first seed, then ignored.
_LEGACY_OVERRIDES_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'category_overrides.json')

FALLBACK_CATEGORY = 'other'

# Color + icon seed for the built-in categories (kept in sync with what the UI used
# to hardcode). Keywords come from credit_card_expenses.CATEGORIES.
_SEED_STYLE = {
    "groceries":    {"color": "#0f766e", "icon": "fa-basket-shopping"},
    "gas stations": {"color": "#1d4ed8", "icon": "fa-gas-pump"},
    "parking":      {"color": "#0369a1", "icon": "fa-square-parking"},
    "car wash":     {"color": "#0891b2", "icon": "fa-car"},
    "oil partner":  {"color": "#4d7c0f", "icon": "fa-oil-can"},
    "eating out":   {"color": "#c2410c", "icon": "fa-utensils"},
    "travel":       {"color": "#b45309", "icon": "fa-plane"},
    "bolt":         {"color": "#be123c", "icon": "fa-taxi"},
    "nabytok":      {"color": "#7c3aed", "icon": "fa-couch"},
    "other":        {"color": "#57534e", "icon": "fa-receipt"},
}
_DEFAULT_STYLE = {"color": "#57534e", "icon": "fa-tag"}

_lock = threading.Lock()
_cache = None


def _norm(name):
    """Case-insensitive identity for a category name."""
    return str(name).strip().lower()


def _build_seed():
    """Construct the initial catalog from the historical hardcoded definitions,
    then migrate any pre-existing custom categories from the legacy overrides file."""
    categories = {}
    for name in _SEED_ORDER:
        style = _SEED_STYLE.get(name, _DEFAULT_STYLE)
        categories[name] = {
            "color": style["color"],
            "icon": style["icon"],
            "keywords": list(_SEED_KEYWORDS.get(name, [])),
            "builtin": True,
        }
    order = list(_SEED_ORDER)

    # Migrate custom categories that used to live in category_overrides.json.
    try:
        with open(_LEGACY_OVERRIDES_FILE, 'r', encoding='utf-8') as f:
            legacy = json.load(f)
        for c in (legacy.get('custom_categories') or []):
            nm = c.get('name')
            if nm and _norm(nm) not in {_norm(k) for k in categories}:
                categories[nm] = {
                    "color": c.get('color', _DEFAULT_STYLE["color"]),
                    "icon": c.get('icon', _DEFAULT_STYLE["icon"]),
                    "keywords": list(c.get('keywords', [])),
                    "builtin": False,
                }
                order.insert(len(order) - 1, nm)  # before 'other'
    except (FileNotFoundError, json.JSONDecodeError, AttributeError):
        pass

    return {"order": order, "categories": categories}


def _load():
    global _cache
    if _cache is not None:
        return _cache
    try:
        with open(CATEGORIES_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict) or 'categories' not in data:
            raise ValueError('bad shape')
        data.setdefault('order', list(data['categories'].keys()))
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        data = _build_seed()
        _cache = data
        _save()
        return _cache
    # Always guarantee the fallback bucket exists.
    if FALLBACK_CATEGORY not in data['categories']:
        data['categories'][FALLBACK_CATEGORY] = {**_DEFAULT_STYLE, "keywords": [], "builtin": True}
        if FALLBACK_CATEGORY not in data['order']:
            data['order'].append(FALLBACK_CATEGORY)
    _cache = data
    return _cache


def _save():
    os.makedirs(os.path.dirname(CATEGORIES_FILE), exist_ok=True)
    tmp = CATEGORIES_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(_cache, f, indent=2, ensure_ascii=False)
    os.replace(tmp, CATEGORIES_FILE)


def _find(name):
    """Return the real (stored-case) category name matching `name`, or None."""
    target = _norm(name)
    for real in _load()['categories']:
        if _norm(real) == target:
            return real
    return None


# ── Public read API ─────────────────────────────────────────────────────────
def get_order():
    """Category names in display order (fallback bucket always last)."""
    data = _load()
    order = [c for c in data['order'] if c in data['categories']]
    # Append any categories missing from `order`, keep `other` last.
    for c in data['categories']:
        if c not in order:
            order.append(c)
    order = [c for c in order if c != FALLBACK_CATEGORY] + [FALLBACK_CATEGORY]
    return order


def get_catalog():
    """Full catalog for the API/UI: [{name, color, icon, keywords, builtin}] in order."""
    cats = _load()['categories']
    return [{"name": n, **cats[n]} for n in get_order()]


def get_entry(name):
    """Return {name, color, icon, keywords, builtin} for a category, or None if unknown."""
    real = _find(name)
    if not real:
        return None
    return {"name": real, **_load()['categories'][real]}


def is_known(name):
    return _find(name) is not None


def is_deletable(name):
    real = _find(name)
    return bool(real) and real != FALLBACK_CATEGORY


def categorize(merchant):
    """Classify a merchant string to a category by keyword substring match, else
    the fallback bucket. First match in display order wins (deterministic)."""
    m = str(merchant).lower()
    cats = _load()['categories']
    for name in get_order():
        if name == FALLBACK_CATEGORY:
            continue
        for kw in cats[name].get('keywords', []):
            if kw and kw.lower() in m:
                return name
    return FALLBACK_CATEGORY


# ── Public write API ────────────────────────────────────────────────────────
def add(name, color, icon, keywords=None):
    """Create a category (idempotent by name). Returns the stored entry."""
    with _lock:
        data = _load()
        real = _find(name)
        entry = {
            "color": color or _DEFAULT_STYLE["color"],
            "icon": icon or _DEFAULT_STYLE["icon"],
            "keywords": list(keywords or []),
            "builtin": False,
        }
        if real:
            # Preserve builtin flag / merge into existing.
            entry["builtin"] = data['categories'][real].get('builtin', False)
            data['categories'][real].update(entry)
            name = real
        else:
            data['categories'][name] = entry
            data['order'].insert(max(0, len(data['order']) - 1), name)  # before 'other'
        _save()
    return {"name": name, **_load()['categories'][name]}


def update(old_name, new_name, color, icon, keywords=None):
    """Edit a category: rename and/or restyle and/or re-key. Returns (entry, renamed
    bool) or (None, False) if `old_name` doesn't exist. The `other` bucket can be
    restyled/re-keyed but not renamed."""
    with _lock:
        data = _load()
        real = _find(old_name)
        if real is None:
            return None, False
        cur = data['categories'][real]
        renamed = False
        # 'other' keeps its name; everything else may be renamed.
        if real != FALLBACK_CATEGORY and _norm(new_name) != _norm(real):
            data['categories'].pop(real)
            data['categories'][new_name] = cur
            data['order'] = [new_name if c == real else c for c in data['order']]
            real = new_name
            renamed = True
        cur['color'] = color or cur.get('color')
        cur['icon'] = icon or cur.get('icon')
        if keywords is not None:
            cur['keywords'] = list(keywords)
        _save()
    return {"name": real, **_load()['categories'][real]}, renamed


def delete(name):
    """Remove a category. Returns the real name removed, or None. The fallback
    bucket cannot be deleted. (Moving its transactions to `other` is the caller's
    job — see overrides_service repointing.)"""
    with _lock:
        data = _load()
        real = _find(name)
        if real is None or real == FALLBACK_CATEGORY:
            return None
        data['categories'].pop(real, None)
        data['order'] = [c for c in data['order'] if c != real]
        _save()
    return real
