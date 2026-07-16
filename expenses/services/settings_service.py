"""Small persisted app settings (e.g. the monthly budget goal)."""
import json
import os
import threading

SETTINGS_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'settings.json')

_lock = threading.Lock()
_cache = None

DEFAULTS = {
    'budget_goal': 1250,
    # Optional per-category monthly spending limits: {category_name: euros}.
    # Absent / not-set categories simply have no budget (feature is opt-in).
    'category_budgets': {},
}


def _load():
    global _cache
    if _cache is not None:
        return _cache
    try:
        with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}
    merged = dict(DEFAULTS)
    merged.update(data if isinstance(data, dict) else {})
    _cache = merged
    return _cache


def _save():
    os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
    tmp = SETTINGS_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(_cache, f, indent=2, ensure_ascii=False)
    os.replace(tmp, SETTINGS_FILE)


def get_all():
    return dict(_load())


def get(key):
    return _load().get(key)


def set_value(key, value):
    with _lock:
        data = _load()
        data[key] = value
        _save()
    return value


def get_category_budgets():
    """The {category: monthly_limit} map (read-only copy)."""
    return dict(_load().get('category_budgets') or {})


def set_category_budget(category, limit):
    """Set or clear one category's monthly limit. Pass a falsy/0 limit to remove it
    (a category with no entry simply isn't budgeted). Returns the stored value or None."""
    category = (category or '').strip()
    if not category:
        return None
    with _lock:
        data = _load()
        budgets = dict(data.get('category_budgets') or {})
        if limit and limit > 0:
            budgets[category] = round(float(limit), 2)
            stored = budgets[category]
        else:
            budgets.pop(category, None)
            stored = None
        data['category_budgets'] = budgets
        _save()
    return stored
