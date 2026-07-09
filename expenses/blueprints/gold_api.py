"""Gold price / premium / portfolio API routes."""
from flask import Blueprint, jsonify, request

from services import gold_service

bp = Blueprint('gold_api', __name__)


@bp.route('/api/gold/spot-history')
def gold_spot_history():
    period = request.args.get('period', '2y')
    payload = gold_service.spot_history_payload(period)
    if payload is None:
        return jsonify({"error": "Failed to fetch gold data"}), 500
    return jsonify(payload)


@bp.route('/api/gold/premiums')
def gold_premiums():
    payload = gold_service.premiums_payload()
    if payload is None:
        return jsonify({"error": "Premium data not found"}), 404
    return jsonify(payload)


@bp.route('/api/gold/portfolio')
def gold_portfolio():
    payload = gold_service.portfolio_payload()
    if payload is None:
        return jsonify({"error": "Failed to fetch gold data"}), 500
    return jsonify(payload)
