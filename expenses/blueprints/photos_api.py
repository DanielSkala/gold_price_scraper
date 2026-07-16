"""macOS Photos integration API routes."""
import re

from flask import Blueprint, abort, jsonify, request, send_file

from services import photos_lib

bp = Blueprint('photos_api', __name__)

_UUID_RE = re.compile(r'^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$')
_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_MONTH_RE = re.compile(r'^\d{4}-\d{2}$')


@bp.route('/api/photos-for-day/<date>')
def photos_for_day_route(date):
    if not _DATE_RE.match(date):
        abort(400)
    photos = photos_lib.photos_for_day(date)
    try:
        curated_target = int(request.args.get('curated', '0') or 0)
    except (ValueError, TypeError):
        curated_target = 0
    payload = {
        "date": date,
        "photos": photos,
        "top_locations": photos_lib.top_locations(photos, limit=3),
    }
    if curated_target > 0:
        payload["curated"] = photos_lib.curate(photos, target=curated_target)
    return jsonify(payload)


@bp.route('/api/photos-for-month/<month>')
def photos_for_month_route(month):
    if not _MONTH_RE.match(month):
        abort(400)
    photos = photos_lib.photos_for_month(month)
    try:
        curated_target = int(request.args.get('target', '9'))
    except (ValueError, TypeError):
        curated_target = 9
    return jsonify({
        "month": month,
        "total_photos": len(photos),
        "top_locations": photos_lib.top_locations(photos, limit=3),
        "curated": photos_lib.curate(photos, target=curated_target),
    })


@bp.route('/api/photos-timeline/<month>')
def photos_timeline_route(month):
    if not _MONTH_RE.match(month):
        abort(400)
    try:
        per_day = int(request.args.get('per_day', '4'))
    except (ValueError, TypeError):
        per_day = 4
    days = photos_lib.photos_by_day(month, per_day=per_day)
    total = sum(d["total"] for d in days)
    # Top locations reflect ALL photos in the month, not just the curated subset.
    return jsonify({
        "month": month,
        "total_photos": total,
        "active_days": len(days),
        "top_locations": photos_lib.top_locations(photos_lib.photos_for_month(month), limit=3),
        "days": days,
    })


@bp.route('/photo/<uuid>')
def photo_thumb(uuid):
    if not _UUID_RE.match(uuid):
        abort(400)
    size = request.args.get('size', 'thumb')
    path = photos_lib.thumbnail_path(uuid, size=size)
    if path is None:
        abort(404)
    try:
        return send_file(str(path), mimetype='image/jpeg', max_age=3600)
    except (FileNotFoundError, PermissionError, IOError):
        abort(404)
