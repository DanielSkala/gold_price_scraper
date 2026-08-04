"""Small persisted app settings (e.g. the monthly budget goal)."""
import json
import os
import threading

from services import json_store

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
    json_store.write_atomic(SETTINGS_FILE, _cache)


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


def rename_category_budget(old_name, new_name):
    """Move a category's budget from old_name to new_name when a category is renamed,
    so the limit follows the category instead of being stranded under the old key."""
    old_name = (old_name or '').strip()
    new_name = (new_name or '').strip()
    if not old_name or not new_name or old_name == new_name:
        return
    with _lock:
        data = _load()
        budgets = dict(data.get('category_budgets') or {})
        if old_name in budgets:
            # Don't clobber an existing budget already set under the new name.
            budgets.setdefault(new_name, budgets[old_name])
            del budgets[old_name]
            data['category_budgets'] = budgets
            _save()


def remove_category_budget(category):
    """Drop a category's budget when the category is deleted, so no orphaned limit
    lingers (and can't unexpectedly resurrect if the name is reused)."""
    category = (category or '').strip()
    if not category:
        return
    with _lock:
        data = _load()
        budgets = dict(data.get('category_budgets') or {})
        if category in budgets:
            del budgets[category]
            data['category_budgets'] = budgets
            _save()
