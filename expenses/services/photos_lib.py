"""Read-only access to the macOS Photos library.

Uses SQLite in `immutable=1` mode so we can safely read while Photos.app
is running. Assumes the default library at ~/Pictures/Photos Library.photoslibrary/.
"""
import os
import sqlite3
import time
from collections import defaultdict
from pathlib import Path

from PIL import Image

PHOTOS_LIBRARY = Path(os.path.expanduser("~/Pictures/Photos Library.photoslibrary"))
PHOTOS_DB = PHOTOS_LIBRARY / "database" / "Photos.sqlite"
DERIVATIVES_DIR = PHOTOS_LIBRARY / "resources" / "derivatives"

# Core Data epoch (2001-01-01 UTC) offset from unix epoch
CORE_DATA_EPOCH = 978307200

# In-process cache for dHash values (photos never change once taken)
_HASH_CACHE = {}
# TTL cache for expensive queries
_QUERY_CACHE = {}
_QUERY_TTL = 60


def _connect():
    if not PHOTOS_DB.exists():
        return None
    uri = f"file:{PHOTOS_DB}?mode=ro&immutable=1"
    return sqlite3.connect(uri, uri=True)


def _row_to_photo(row):
    uuid, taken_at, lat, lng, kind, moment_title, width, height, orientation = row
    has_gps = lat is not None and lng is not None and lat != -180.0 and lng != -180.0
    # Photos ≥ 5 are rotated 90°, so swap width/height to get the DISPLAYED aspect.
    w, h = (width or 0), (height or 0)
    if orientation and orientation >= 5:
        w, h = h, w
    return {
        "uuid": uuid,
        "taken_at": taken_at,
        "lat": lat if has_gps else None,
        "lng": lng if has_gps else None,
        "is_video": kind == 1,
        "location": (moment_title or "").strip() or None,
        "width": w or None,
        "height": h or None,
        # Aspect ratio (w/h) of the displayed image, for justified collage layout.
        "aspect": round(w / h, 4) if (w and h) else None,
    }


def _fetch_photos(where_clause, params):
    """`where_clause` may reference ? placeholders; caller supplies matching `params`."""
    cache_key = (where_clause, params)
    cached = _QUERY_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < _QUERY_TTL:
        return cached[1]

    conn = _connect()
    if conn is None:
        return []
    try:
        sql = f"""
            SELECT a.ZUUID,
                   datetime(a.ZDATECREATED + ?, 'unixepoch', 'localtime') AS taken_at,
                   a.ZLATITUDE,
                   a.ZLONGITUDE,
                   a.ZKIND,
                   m.ZTITLE,
                   a.ZWIDTH,
                   a.ZHEIGHT,
                   a.ZORIENTATION
            FROM ZASSET a
            LEFT JOIN ZMOMENT m ON a.ZMOMENT = m.Z_PK
            WHERE a.ZTRASHEDDATE IS NULL
              AND a.ZHIDDEN = 0
              AND {where_clause}
            ORDER BY a.ZDATECREATED
        """
        rows = conn.execute(sql, (CORE_DATA_EPOCH, *params)).fetchall()
    except sqlite3.DatabaseError:
        # The Photos library DB can momentarily report "database disk image is
        # malformed" while Photos.app writes to it (even in immutable mode).
        # Treat as "no photos right now" rather than 500-ing the whole page.
        return []
    finally:
        conn.close()

    photos = [_row_to_photo(r) for r in rows]
    _QUERY_CACHE[cache_key] = (time.time(), photos)
    return photos


def photos_for_day(date_str):
    return _fetch_photos(
        "date(a.ZDATECREATED + ?, 'unixepoch', 'localtime') = ?",
        (CORE_DATA_EPOCH, date_str),
    )


def photos_for_month(month_str):
    """`month_str` in YYYY-MM format."""
    return _fetch_photos(
        "strftime('%Y-%m', a.ZDATECREATED + ?, 'unixepoch', 'localtime') = ?",
        (CORE_DATA_EPOCH, month_str),
    )


def top_locations(photos, limit=3):
    counts = defaultdict(int)
    for p in photos:
        if p["location"]:
            counts[p["location"]] += 1
    ranked = sorted(counts.items(), key=lambda x: -x[1])[:limit]
    return [{"name": name, "count": n} for name, n in ranked]


def thumbnail_path(uuid, size="thumb"):
    """Return filesystem path to a photo's derivative, or None if missing.

    size='thumb' → medium (~500px), size='large' → high-res (~1500px).
    """
    if not uuid:
        return None
    first_char = uuid[0].upper()
    subdir = DERIVATIVES_DIR / first_char
    if not subdir.is_dir():
        return None
    if size == "large":
        suffixes = ("_1_102_o.jpeg", "_1_105_c.jpeg")
    else:
        suffixes = ("_1_105_c.jpeg", "_1_102_o.jpeg")
    for suffix in suffixes:
        candidate = subdir / f"{uuid}{suffix}"
        if candidate.exists():
            return candidate
    for candidate in subdir.glob(f"{uuid}*"):
        if candidate.suffix.lower() in (".jpeg", ".jpg", ".png"):
            return candidate
    return None


# ── Perceptual hashing (dHash) for near-duplicate detection ─────────
def _dhash(path, size=8):
    """Return a 64-bit dHash for a JPEG file, or None on failure."""
    try:
        img = Image.open(path).convert("L").resize((size + 1, size))
        pixels = list(img.getdata())
        h = 0
        for row in range(size):
            for col in range(size):
                left = pixels[row * (size + 1) + col]
                right = pixels[row * (size + 1) + col + 1]
                h = (h << 1) | (1 if left > right else 0)
        return h
    except Exception:
        return None


def _hamming(a, b):
    return bin(a ^ b).count("1")


def _photo_hash(uuid):
    if uuid in _HASH_CACHE:
        return _HASH_CACHE[uuid]
    path = thumbnail_path(uuid, size="thumb")
    h = _dhash(path) if path else None
    _HASH_CACHE[uuid] = h
    return h


def curate(photos, target=8, similarity_threshold=10):
    """Pick a diversified subset of `photos`.

    Strategy:
      1. Filter to photos that have an on-disk derivative.
      2. Compute a dHash for each; drop near-duplicates (Hamming < threshold).
      3. Round-robin across location groups to spread the selection.
      4. Cap at `target` items, preserving chronological order.

    Runs O(N) hash computations (~5-15ms each). Cached per-uuid across calls.
    """
    if not photos:
        return []

    # Step 1: reachable-only
    reachable = [p for p in photos if thumbnail_path(p["uuid"]) is not None]
    if not reachable:
        return []

    # Step 2: dedup via dHash
    kept = []
    kept_hashes = []
    for p in reachable:
        h = _photo_hash(p["uuid"])
        if h is None:
            kept.append(p)
            kept_hashes.append(None)
            continue
        # Skip if within similarity threshold of any already-kept photo
        is_dup = False
        for kh in kept_hashes:
            if kh is not None and _hamming(h, kh) < similarity_threshold:
                is_dup = True
                break
        if not is_dup:
            kept.append(p)
            kept_hashes.append(h)

    if len(kept) <= target:
        return kept

    # Step 3: diversify across locations. Round-robin over location groups.
    by_loc = defaultdict(list)
    for p in kept:
        by_loc[p["location"] or "__unknown__"].append(p)

    selected = []
    # Keep queue of (location, index) round-robin
    keys = sorted(by_loc.keys(), key=lambda k: -len(by_loc[k]))
    cursors = {k: 0 for k in keys}
    while len(selected) < target:
        made_progress = False
        for k in keys:
            if cursors[k] < len(by_loc[k]):
                selected.append(by_loc[k][cursors[k]])
                cursors[k] += 1
                made_progress = True
                if len(selected) >= target:
                    break
        if not made_progress:
            break

    # Preserve chronological order in the returned list
    selected.sort(key=lambda p: p["taken_at"])
    return selected


def summary_for_range(photos, curated_target=8):
    """Bundle top locations + curated selection + counts for a photo set."""
    return {
        "total_photos": len(photos),
        "top_locations": top_locations(photos, limit=3),
        "curated": curate(photos, target=curated_target),
    }


def photos_by_day(month_str, per_day=4):
    """Group a month's photos by day for a timeline view.

    Returns a list of day dicts (chronological), each with a curated subset of
    photos so that days with many bursts don't dominate. Only days that
    actually have photos are included.
    """
    photos = photos_for_month(month_str)
    if not photos:
        return []

    by_day = defaultdict(list)
    for p in photos:
        day = p["taken_at"].split(" ")[0]
        by_day[day].append(p)

    days = []
    for day in sorted(by_day.keys()):
        day_photos = by_day[day]
        locations = top_locations(day_photos, limit=2)
        days.append({
            "date": day,
            "total": len(day_photos),
            "locations": locations,
            "primary_location": locations[0]["name"] if locations else None,
            "photos": curate(day_photos, target=per_day),
        })
    return days
