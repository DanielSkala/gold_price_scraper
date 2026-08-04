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

`uncategorized` is special: it's the fallback bucket, always present and never
deletable. It means "nothing has classified this yet" — NOT "I decided this is
miscellaneous". That second meaning belongs to `other`, which is an ordinary category
you file into deliberately (the tidy flow's "Keep in Other"). Keeping them apart is
what lets the tidy queue offer only genuinely unsorted merchants: parking something in
`other` is a real categorisation, so it drops out of the queue for good, stays visible
in the breakdown, and can be changed back from the badge picker like anything else.

Installs created before the split are migrated on first load — see
`_migrate_split_fallback`.
"""
import json
import os
import threading

from credit_card_expenses import CATEGORIES as _SEED_KEYWORDS, CATEGORY_ORDER as _SEED_ORDER
from services import json_store

CATEGORIES_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'categories.json')
# Legacy custom-category store — migrated in on first seed, then ignored.
_LEGACY_OVERRIDES_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'category_overrides.json')

FALLBACK_CATEGORY = 'uncategorized'
# The bucket the fallback used to be called, now an ordinary "I decided it's
# miscellaneous" category. Only referenced by the one-time migration below.
_LEGACY_FALLBACK = 'other'

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
    # A real category now ("I decided this is miscellaneous"), so it gets a real colour
    # and icon of its own instead of the grey it wore as the fallback bucket — plum is
    # unused elsewhere in the palette, and assorted shapes read as "a bit of everything".
    "other":        {"color": "#86198f", "icon": "fa-shapes"},
    # The fallback bucket. Deliberately drab — it's a to-do pile, not a category.
    "uncategorized": {"color": "#78716c", "icon": "fa-circle-question"},
}
_DEFAULT_STYLE = {"color": "#57534e", "icon": "fa-tag"}
# How the single combined 'other' bucket looked before the split. Used ONLY to
# recognise an untouched pre-split entry, so a style the user picked is never clobbered.
_LEGACY_FALLBACK_STYLE = {"color": "#57534e", "icon": "fa-receipt"}

_lock = threading.Lock()
_cache = None
# Bumped on every persisted change so the expense snapshot cache knows to recategorize.
_generation = 0


def generation():
    """Monotonic counter of catalog changes — part of the expense-cache key."""
    return _generation


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
                order.insert(len(order) - 1, nm)  # before the fallback bucket
    except (FileNotFoundError, json.JSONDecodeError, AttributeError):
        pass

    # `_SEED_ORDER` is historical: it ends at 'other', which WAS the fallback bucket back
    # then. Add today's fallback explicitly (last), so a brand-new install is seeded in
    # the post-split shape directly instead of leaning on the migration to rename it.
    # Without this the fresh-seed path handed 'uncategorized' the plum 'other' styling and
    # both buckets came out looking identical.
    if not any(_norm(n) == _norm(FALLBACK_CATEGORY) for n in categories):
        style = _SEED_STYLE.get(FALLBACK_CATEGORY, _DEFAULT_STYLE)
        categories[FALLBACK_CATEGORY] = {
            "color": style["color"],
            "icon": style["icon"],
            "keywords": [],
            "builtin": True,
        }
        order.append(FALLBACK_CATEGORY)

    return {"order": order, "categories": categories}


def _load_locked():
    """Populate and return `_cache`. Callers MUST hold `_lock`."""
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
        _migrate_split_fallback(data)      # the seed still names the bucket 'other'
        _cache = data
        _save()
        return _cache
    # Split the old combined 'other' bucket into fallback + real category (one-time),
    # then make sure the real 'other' looks like a category rather than a second bucket.
    migrated = _migrate_split_fallback(data)
    migrated = _restyle_migrated_other(data) or migrated
    # Always guarantee the fallback bucket exists. Case-insensitive, like every other
    # name lookup here — a catalog holding 'Uncategorized' must not gain a second,
    # lowercase twin.
    if not any(_norm(n) == _norm(FALLBACK_CATEGORY) for n in data['categories']):
        style = _SEED_STYLE.get(FALLBACK_CATEGORY, _DEFAULT_STYLE)
        data['categories'][FALLBACK_CATEGORY] = {**style, "keywords": [], "builtin": True}
        if FALLBACK_CATEGORY not in data['order']:
            data['order'].append(FALLBACK_CATEGORY)
        migrated = True
    # Backfill any built-in categories added after this catalog was first seeded
    # (e.g. 'travel'). Without this, a pre-existing data/categories.json never gains
    # the new category and its transactions silently stay in the fallback bucket.
    if _backfill_missing_builtins(data) or migrated:
        _cache = data
        _save()
        return _cache
    _cache = data
    return _cache


def _looks_untouched(entry):
    """True if this entry still wears the pre-split bucket's grey receipt look, i.e. the
    user never restyled it. Guards both style migrations from overwriting a real choice."""
    return (entry.get('color'), entry.get('icon')) == \
           (_LEGACY_FALLBACK_STYLE['color'], _LEGACY_FALLBACK_STYLE['icon'])


def _restyle_migrated_other(data):
    """Give `other` its own colour and icon on installs that split BEFORE it had them.

    The first version of the split handed the new real 'other' the fallback's old grey
    receipt, which left it looking like a second to-do pile rather than an ordinary
    category. Only touches an entry still wearing exactly that look, so a colour/icon the
    user has since picked is left alone. Returns True if anything changed."""
    real = next((n for n in data['categories'] if _norm(n) == _norm(_LEGACY_FALLBACK)), None)
    if real is None:
        return False
    entry = data['categories'][real]
    if not _looks_untouched(entry):
        return False
    entry.update(_SEED_STYLE[_LEGACY_FALLBACK])
    return True


def _migrate_split_fallback(data):
    """Split the historical single 'other' bucket into `uncategorized` + `other`.

    Before the split, 'other' meant both "not classified yet" and "I decided this is
    miscellaneous". Everything currently in it is, by definition, the FIRST of those —
    the user never got to express the second — so the existing entry is renamed to
    `uncategorized` (keeping its style and keywords, and its place at the end of the
    order), and a fresh, empty `other` is added as an ordinary category alongside the
    real ones.

    Nothing is reclassified: transactions have no stored category, they're derived from
    the catalog on every load, so renaming the bucket moves them all with it. Merchant
    rules / per-transaction overrides pointing at 'other' are deliberately LEFT alone —
    under the new model those mean "the user chose miscellaneous", which is exactly the
    new real category. Runs once: after it, `uncategorized` exists.

    Returns True if anything changed (so the caller persists)."""
    cats = data['categories']
    if any(_norm(name) == _norm(FALLBACK_CATEGORY) for name in cats):
        return False                       # already split
    legacy = next((name for name in cats if _norm(name) == _norm(_LEGACY_FALLBACK)), None)
    if legacy is None:
        return False                       # no old bucket to split; the guard above adds one

    entry = cats.pop(legacy)
    # Restyle the fallback to its own look, so the two buckets aren't visually identical
    # in the breakdown — unless the user had picked a custom colour/icon, which we keep.
    if _looks_untouched(entry):
        entry = {**entry, **_SEED_STYLE[FALLBACK_CATEGORY]}
    cats[FALLBACK_CATEGORY] = {**entry, "builtin": True}
    data['order'] = [FALLBACK_CATEGORY if c == legacy else c for c in data['order']]
    if FALLBACK_CATEGORY not in data['order']:
        data['order'].append(FALLBACK_CATEGORY)

    # The new 'other': a normal category, no keywords, so nothing auto-lands in it.
    style = _SEED_STYLE.get(_LEGACY_FALLBACK, _DEFAULT_STYLE)
    cats[_LEGACY_FALLBACK] = {**style, "keywords": [], "builtin": True}
    if _LEGACY_FALLBACK not in data['order']:
        data['order'].insert(max(0, len(data['order']) - 1), _LEGACY_FALLBACK)

    # A budget set on the old bucket was tracking unsorted spend — follow it there.
    try:
        from services import settings_service
        settings_service.rename_category_budget(legacy, FALLBACK_CATEGORY)
    except Exception:
        pass                               # a stranded budget must never block startup
    return True


def _backfill_missing_builtins(data):
    """Insert any seed built-in category missing from `data` (in place).

    Matches on case-insensitive name so a user-renamed built-in isn't duplicated.
    Returns True if anything was added (so the caller can persist)."""
    known = {_norm(name) for name in data['categories']}
    changed = False
    for name in _SEED_ORDER:
        if _norm(name) in known:
            continue
        style = _SEED_STYLE.get(name, _DEFAULT_STYLE)
        data['categories'][name] = {
            "color": style["color"],
            "icon": style["icon"],
            "keywords": list(_SEED_KEYWORDS.get(name, [])),
            "builtin": True,
        }
        # Keep the fallback bucket last if it's present in the order.
        if FALLBACK_CATEGORY in data['order']:
            data['order'].insert(data['order'].index(FALLBACK_CATEGORY), name)
        else:
            data['order'].append(name)
        changed = True
    return changed


def _load():
    # Double-checked locking: fast path avoids the lock once initialized; the
    # initial load + `_cache` assignment happen under `_lock` so a concurrent
    # write can't be shadowed by a stale disk read.
    if _cache is not None:
        return _cache
    with _lock:
        return _load_locked()


def _save():
    global _generation
    json_store.write_atomic(CATEGORIES_FILE, _cache)
    _generation += 1


def _find_locked(name):
    """Like `_find`, but assumes `_lock` is held (safe to call from writers)."""
    target = _norm(name)
    for real in _load_locked()['categories']:
        if _norm(real) == target:
            return real
    return None


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
        data = _load_locked()
        real = _find_locked(name)
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
            data['order'].insert(max(0, len(data['order']) - 1), name)  # before the fallback
        _save()
    return {"name": name, **_load()['categories'][name]}


def update(old_name, new_name, color, icon, keywords=None):
    """Edit a category: rename and/or restyle and/or re-key. Returns (entry, renamed
    bool) or (None, False) if `old_name` doesn't exist. The `other` bucket can be
    restyled/re-keyed but not renamed."""
    with _lock:
        data = _load_locked()
        real = _find_locked(old_name)
        if real is None:
            return None, False
        cur = data['categories'][real]
        renamed = False
        # The fallback bucket keeps its name; everything else (now including the real
        # 'other') may be renamed.
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
        data = _load_locked()
        real = _find_locked(name)
        if real is None or real == FALLBACK_CATEGORY:
            return None
        data['categories'].pop(real, None)
        data['order'] = [c for c in data['order'] if c != real]
        _save()
    return real
