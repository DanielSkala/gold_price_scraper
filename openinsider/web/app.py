import logging
import os
import time
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import sqlite3
import json
from datetime import datetime, timedelta

from openinsider.analysis.research_stats import load_research_json, generate_research_json

logger = logging.getLogger(__name__)

app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
)
CORS(app)

# Simple in-memory cache with TTL
_cache = {}
_CACHE_TTL = 60  # seconds
_CACHE_TTL_LONG = 600  # 10 min for expensive, slow-changing endpoints
_LONG_CACHE_KEYS = {'research_all', 'calibration'}


def cache_get(key):
    entry = _cache.get(key)
    if not entry:
        return None
    ttl = _CACHE_TTL_LONG if key in _LONG_CACHE_KEYS else _CACHE_TTL
    if time.time() - entry[1] < ttl:
        return entry[0]
    return None


def cache_set(key, value):
    _cache[key] = (value, time.time())


def cache_clear():
    _cache.clear()


def get_connection():
    """Get database connection. Tries openinsider.db package first, falls back to local."""
    try:
        from openinsider.db import get_connection as db_get_connection
        return db_get_connection()
    except Exception:
        db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'openinsider.db')
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn


def safe_query(query, params=(), fetchone=False):
    """Execute a query safely, returning empty results if table doesn't exist."""
    try:
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(query, params)
        if fetchone:
            row = cursor.fetchone()
            return dict(row) if row else None
        rows = cursor.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning("safe_query error: %s | query: %s", e, query[:100])
        return None if fetchone else []


# --- Page routes ---

@app.route('/')
def dashboard():
    return render_template('dashboard.html')


@app.route('/company/<ticker>')
def company_page(ticker):
    return render_template('company.html', ticker=ticker)


@app.route('/insider/<name>')
def insider_page(name):
    return render_template('insider.html', insider_name=name)


# --- API endpoints ---

@app.route('/api/filings')
def api_filings():
    trade_type = request.args.get('trade_type', '')
    min_value = max(0, request.args.get('min_value', 0, type=float))
    min_score = max(0, min(100, request.args.get('min_score', 0, type=float)))
    ticker = request.args.get('ticker', '')
    days = request.args.get('days', 30, type=int)
    sort = request.args.get('sort', 'date_desc')
    page = max(1, request.args.get('page', 1, type=int))
    per_page = max(1, min(500, request.args.get('per_page', 50, type=int)))

    conditions = []
    params = []

    if trade_type:
        conditions.append("trade_type LIKE ?")
        params.append(f"{trade_type}%")
    if min_value > 0:
        conditions.append("value >= ?")
        params.append(min_value)
    if min_score > 0:
        conditions.append("deterministic_score >= ?")
        params.append(min_score)
    if ticker:
        conditions.append("ticker LIKE ?")
        params.append(f"%{ticker}%")
    if days > 0:
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        conditions.append("filing_date >= ?")
        params.append(cutoff)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    offset = (page - 1) * per_page

    # Sort options
    sort_map = {
        'date_desc': 'filing_date DESC',
        'date_asc': 'filing_date ASC',
        'score_desc': 'deterministic_score DESC',
        'value_desc': 'ABS(value) DESC',
        'value_asc': 'ABS(value) ASC',
    }
    order_by = sort_map.get(sort, 'filing_date DESC')

    count_rows = safe_query(f"SELECT COUNT(*) as cnt FROM filings{where}", params)
    total = count_rows[0]['cnt'] if count_rows else 0

    rows = safe_query(
        f"SELECT id, filing_date, trade_date, ticker, company_name, insider_name, "
        f"title, trade_type, price, qty, value, delta_own, deterministic_score, "
        f"price_change_1d, price_change_1w, price_change_1m, is_10b5_1 "
        f"FROM filings{where} ORDER BY {order_by} LIMIT ? OFFSET ?",
        params + [per_page, offset]
    )

    return jsonify({
        'filings': rows,
        'total': total,
        'page': page,
        'per_page': per_page,
        'pages': (total + per_page - 1) // per_page if per_page > 0 else 0
    })


@app.route('/api/filings/<int:filing_id>')
def api_filing_detail(filing_id):
    row = safe_query("SELECT * FROM filings WHERE id = ?", (filing_id,), fetchone=True)
    if not row:
        return jsonify({'error': 'Filing not found'}), 404

    # Compute score breakdown on the fly
    try:
        from openinsider.analysis.scoring import build_features
        from openinsider.analysis.signals import compute_score
        features = build_features(row)
        score, breakdown = compute_score(
            features["filing"],
            features["market_context"],
            features["insider_history"],
            features["cluster_info"],
        )
        row["score_breakdown"] = breakdown
        mc = features["market_context"]
        row["score_inputs"] = {
            "market_context": mc,
            "has_market_data": mc.get("id") is not None,  # True only if real DB row exists
            "insider_history_count": len(features["insider_history"]),
            "cluster_participant_count": features["cluster_info"].get("participant_count", 0),
        }
    except Exception:
        row["score_breakdown"] = None
        row["score_inputs"] = None

    return jsonify(row)


@app.route('/api/opportunities')
def api_opportunities():
    min_score = max(0, min(100, request.args.get('min_score', 60, type=float)))
    limit = max(1, min(500, request.args.get('limit', 20, type=int)))
    trade_type = request.args.get('trade_type', '')
    sort = request.args.get('sort', 'score_desc')
    days = request.args.get('days', 0, type=int)
    hide_10b5_1 = request.args.get('hide_10b5_1', 1, type=int)

    conditions = ["deterministic_score >= ?"]
    params = [min_score]

    if trade_type:
        conditions.append("trade_type LIKE ?")
        params.append(f"{trade_type}%")

    if days > 0:
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        conditions.append("filing_date >= ?")
        params.append(cutoff)

    if hide_10b5_1:
        conditions.append("(is_10b5_1 IS NULL OR is_10b5_1 != 1)")

    where = " AND ".join(conditions)

    sort_map = {
        'score_desc': 'deterministic_score DESC',
        'date_desc': 'filing_date DESC',
        'value_desc': 'ABS(value) DESC',
    }
    order_by = sort_map.get(sort, 'deterministic_score DESC')

    rows = safe_query(
        f"SELECT id, filing_date, ticker, company_name, insider_name, title, "
        f"trade_type, price, qty, value, delta_own, deterministic_score, llm_analysis, llm_priority, is_10b5_1 "
        f"FROM filings WHERE {where} "
        f"ORDER BY {order_by} LIMIT ?",
        params + [limit]
    )
    return jsonify({'opportunities': rows})


@app.route('/api/clusters')
def api_clusters():
    days = request.args.get('days', 30, type=int)
    min_insiders = request.args.get('min_insiders', 2, type=int)
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    rows = safe_query(
        "SELECT * FROM clusters WHERE start_date >= ? AND participant_count >= ? "
        "ORDER BY cluster_score DESC",
        (cutoff, min_insiders)
    )
    return jsonify({'clusters': rows})


@app.route('/api/company/<ticker>/timeline')
def api_company_timeline(ticker):
    filings = safe_query(
        "SELECT id, filing_date, trade_date, insider_name, title, trade_type, price, qty, value, "
        "delta_own, deterministic_score FROM filings WHERE ticker = ? ORDER BY filing_date DESC",
        (ticker,)
    )
    prices = safe_query(
        "SELECT date, close, volume FROM market_data WHERE ticker = ? ORDER BY date",
        (ticker,)
    )
    return jsonify({'filings': filings, 'prices': prices})


@app.route('/api/company/<ticker>/price-history')
def api_price_history(ticker):
    """Fetch 1-year daily price history from yfinance on demand."""
    period = request.args.get('period', '1y')
    allowed_periods = {'1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'}
    if period not in allowed_periods:
        period = '1y'
    try:
        import yfinance as yf
        tk = yf.Ticker(ticker)
        hist = tk.history(period=period)
        if hist.empty:
            return jsonify({'prices': [], 'ticker': ticker})
        prices = []
        for date, row in hist.iterrows():
            prices.append({
                'date': date.strftime('%Y-%m-%d'),
                'open': round(row['Open'], 2),
                'high': round(row['High'], 2),
                'low': round(row['Low'], 2),
                'close': round(row['Close'], 2),
                'volume': int(row['Volume'])
            })
        return jsonify({'prices': prices, 'ticker': ticker})
    except Exception as e:
        return jsonify({'prices': [], 'ticker': ticker, 'error': str(e)})


@app.route('/api/company/<ticker>/summary')
def api_company_summary(ticker):
    stats = safe_query(
        "SELECT COUNT(*) as total_filings, "
        "SUM(CASE WHEN trade_type LIKE 'P%' THEN 1 ELSE 0 END) as total_buys, "
        "SUM(CASE WHEN trade_type LIKE 'S%' THEN 1 ELSE 0 END) as total_sells, "
        "SUM(CASE WHEN trade_type LIKE 'P%' THEN value ELSE 0 END) as total_buy_value, "
        "SUM(CASE WHEN trade_type LIKE 'S%' THEN value ELSE 0 END) as total_sell_value, "
        "COUNT(DISTINCT insider_name) as unique_insiders, "
        "AVG(deterministic_score) as avg_score, "
        "MAX(deterministic_score) as max_score, "
        "MIN(filing_date) as first_filing, MAX(filing_date) as last_filing, "
        "company_name "
        "FROM filings WHERE ticker = ?",
        (ticker,),
        fetchone=True
    )
    if not stats:
        stats = {}
    return jsonify(stats)


@app.route('/api/insider/<name>/history')
def api_insider_history(name):
    rows = safe_query(
        "SELECT id, filing_date, trade_date, ticker, company_name, trade_type, "
        "value, price, qty, deterministic_score "
        "FROM filings WHERE insider_name = ? ORDER BY filing_date DESC",
        (name,)
    )
    return jsonify({'filings': rows})


@app.route('/api/insider/<name>/profile')
def api_insider_profile(name):
    profile = safe_query(
        "SELECT * FROM insiders WHERE name = ?", (name,), fetchone=True
    )
    if not profile:
        # Build from filings if insiders table doesn't have them
        profile = safe_query(
            "SELECT insider_name as name, "
            "COUNT(*) as total_trades, "
            "SUM(CASE WHEN trade_type LIKE 'P%' THEN 1 ELSE 0 END) as total_buys, "
            "SUM(CASE WHEN trade_type LIKE 'S%' THEN 1 ELSE 0 END) as total_sells, "
            "SUM(CASE WHEN trade_type LIKE 'P%' THEN value ELSE 0 END) as total_buy_value, "
            "SUM(CASE WHEN trade_type LIKE 'S%' THEN value ELSE 0 END) as total_sell_value, "
            "AVG(deterministic_score) as quality_score, "
            "MIN(filing_date) as first_seen, MAX(filing_date) as last_seen "
            "FROM filings WHERE insider_name = ?",
            (name,),
            fetchone=True
        )
    return jsonify(profile or {})


@app.route('/api/stats')
def api_stats():
    cached = cache_get('stats')
    if cached:
        return jsonify(cached)

    today = datetime.now().strftime('%Y-%m-%d')
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    total_filings = safe_query("SELECT COUNT(*) as cnt FROM filings", fetchone=True)
    filings_today = safe_query(
        "SELECT COUNT(*) as cnt FROM filings WHERE filing_date >= ?", (today,), fetchone=True
    )
    highest_score = safe_query(
        "SELECT MAX(deterministic_score) as max_score FROM filings", fetchone=True
    )
    active_insiders = safe_query(
        "SELECT COUNT(DISTINCT insider_name) as cnt FROM filings WHERE filing_date >= ?",
        (week_ago,), fetchone=True
    )
    clusters_week = safe_query(
        "SELECT COUNT(*) as cnt FROM clusters WHERE start_date >= ?", (week_ago,), fetchone=True
    )

    result = {
        'filings_today': filings_today['cnt'] if filings_today else 0,
        'clusters_this_week': clusters_week['cnt'] if clusters_week else 0,
        'highest_score': highest_score['max_score'] if highest_score and highest_score['max_score'] else 0,
        'active_insiders': active_insiders['cnt'] if active_insiders else 0,
        'total_filings': total_filings['cnt'] if total_filings else 0
    }
    cache_set('stats', result)
    return jsonify(result)


@app.route('/api/heatmap')
def api_heatmap():
    metric = request.args.get('metric', 'buy_value')
    period = request.args.get('period', '30d')
    cache_key = f'heatmap_{metric}_{period}'
    cached = cache_get(cache_key)
    if cached:
        return jsonify(cached)

    days = int(period.replace('d', '')) if period.endswith('d') else 30
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    if metric == 'buy_value':
        rows = safe_query(
            "SELECT ticker, SUM(CASE WHEN trade_type LIKE 'P%' THEN value ELSE 0 END) as buy_value, "
            "SUM(CASE WHEN trade_type LIKE 'S%' THEN value ELSE 0 END) as sell_value, "
            "COUNT(*) as num_filings "
            "FROM filings WHERE filing_date >= ? GROUP BY ticker "
            "HAVING buy_value > 0 ORDER BY buy_value DESC LIMIT 50",
            (cutoff,)
        )
    else:
        rows = safe_query(
            "SELECT ticker, "
            "SUM(CASE WHEN trade_type LIKE 'P%' THEN value ELSE 0 END) - "
            "SUM(CASE WHEN trade_type LIKE 'S%' THEN value ELSE 0 END) as net_value, "
            "COUNT(*) as num_filings "
            "FROM filings WHERE filing_date >= ? GROUP BY ticker "
            "ORDER BY net_value DESC LIMIT 50",
            (cutoff,)
        )
    result = {'heatmap': rows}
    cache_set(cache_key, result)
    return jsonify(result)


@app.route('/api/search')
def api_search():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify({'results': []})

    # Try FTS5 first (much faster), fall back to LIKE
    try:
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        # FTS5 prefix search
        fts_q = q.replace('"', '') + '*'
        ticker_rows = conn.execute(
            "SELECT DISTINCT f.ticker, f.company_name "
            "FROM filings_fts fts JOIN filings f ON f.id = fts.rowid "
            "WHERE filings_fts MATCH ? LIMIT 10",
            (f'ticker : {fts_q} OR company_name : {fts_q}',)
        ).fetchall()
        tickers = [dict(r) for r in ticker_rows]

        insider_rows = conn.execute(
            "SELECT DISTINCT f.insider_name "
            "FROM filings_fts fts JOIN filings f ON f.id = fts.rowid "
            "WHERE filings_fts MATCH ? LIMIT 10",
            (f'insider_name : {fts_q}',)
        ).fetchall()
        insiders = [r['insider_name'] for r in insider_rows]
    except Exception:
        # Fallback to LIKE queries
        tickers = safe_query(
            "SELECT DISTINCT ticker, company_name FROM filings "
            "WHERE ticker LIKE ? OR company_name LIKE ? LIMIT 10",
            (f"%{q}%", f"%{q}%")
        )
        insiders_rows = safe_query(
            "SELECT DISTINCT insider_name FROM filings WHERE insider_name LIKE ? LIMIT 10",
            (f"%{q}%",)
        )
        insiders = [r['insider_name'] for r in insiders_rows]

    return jsonify({
        'companies': tickers,
        'insiders': insiders
    })


@app.route('/api/scrape-status')
def api_scrape_status():
    last_run = safe_query(
        "SELECT * FROM scrape_runs ORDER BY started_at DESC LIMIT 1",
        fetchone=True
    )
    return jsonify(last_run or {'status': 'no_runs'})


@app.route('/api/data-health')
def api_data_health():
    """Comprehensive data freshness and pipeline health info."""
    cached = cache_get('data_health')
    if cached:
        return jsonify(cached)

    total_filings = safe_query("SELECT COUNT(*) as cnt FROM filings")
    total_clusters = safe_query("SELECT COUNT(*) as cnt FROM clusters")
    total_tickers = safe_query("SELECT COUNT(DISTINCT ticker) as cnt FROM filings")
    total_buys = safe_query("SELECT COUNT(*) as cnt FROM filings WHERE trade_type LIKE 'P%'")
    total_sells = safe_query("SELECT COUNT(*) as cnt FROM filings WHERE trade_type LIKE 'S%'")
    date_range = safe_query("SELECT MIN(filing_date) as earliest, MAX(filing_date) as latest FROM filings", fetchone=True)

    # Recent scrape runs (last 10)
    recent_runs = safe_query(
        "SELECT started_at, finished_at, url, filings_found, filings_new, status, error_message "
        "FROM scrape_runs ORDER BY started_at DESC LIMIT 10"
    )

    # Last successful run with new data
    last_new_data = safe_query(
        "SELECT started_at, filings_new FROM scrape_runs "
        "WHERE filings_new > 0 ORDER BY started_at DESC LIMIT 1",
        fetchone=True
    )

    # Filings added in last 24h
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M')
    recent_filings = safe_query(
        "SELECT COUNT(*) as cnt FROM filings WHERE created_at >= ?", (yesterday,)
    )

    # Pipeline run count today
    today = datetime.now().strftime('%Y-%m-%d')
    runs_today = safe_query(
        "SELECT COUNT(DISTINCT started_at) as cnt FROM scrape_runs WHERE started_at >= ?",
        (today,)
    )

    # Errors in last 24h
    recent_errors = safe_query(
        "SELECT COUNT(*) as cnt FROM scrape_runs WHERE status = 'error' AND started_at >= ?",
        (yesterday,)
    )

    result = {
        'total_filings': total_filings[0]['cnt'] if total_filings else 0,
        'total_clusters': total_clusters[0]['cnt'] if total_clusters else 0,
        'total_tickers': total_tickers[0]['cnt'] if total_tickers else 0,
        'total_buys': total_buys[0]['cnt'] if total_buys else 0,
        'total_sells': total_sells[0]['cnt'] if total_sells else 0,
        'earliest_filing': date_range['earliest'][:10] if date_range and date_range['earliest'] else None,
        'latest_filing': date_range['latest'][:19] if date_range and date_range['latest'] else None,
        'filings_last_24h': recent_filings[0]['cnt'] if recent_filings else 0,
        'pipeline_runs_today': runs_today[0]['cnt'] if runs_today else 0,
        'errors_last_24h': recent_errors[0]['cnt'] if recent_errors else 0,
        'last_new_data': last_new_data if last_new_data else None,
        'recent_runs': recent_runs or [],
    }
    cache_set('data_health', result)
    return jsonify(result)


@app.route('/api/watchlist', methods=['GET'])
def api_watchlist_get():
    rows = safe_query("SELECT * FROM watchlist ORDER BY id DESC")
    return jsonify({'watchlist': rows})


@app.route('/api/watchlist', methods=['POST'])
def api_watchlist_add():
    data = request.get_json()
    if not data or not data.get('ticker'):
        return jsonify({'error': 'ticker required'}), 400
    try:
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        conn.execute(
            "INSERT OR IGNORE INTO watchlist (ticker, notes) VALUES (?, ?)",
            (data['ticker'].upper(), data.get('notes', ''))
        )
        conn.commit()
        return jsonify({'status': 'added'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/watchlist', methods=['DELETE'])
def api_watchlist_delete():
    data = request.get_json()
    if not data or not data.get('id'):
        return jsonify({'error': 'id required'}), 400
    try:
        conn = get_connection()
        conn.execute("DELETE FROM watchlist WHERE id = ?", (data['id'],))
        conn.commit()
        return jsonify({'status': 'deleted'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/backtest/calibration')
def api_backtest_calibration():
    """Score calibration: for each threshold, what % of buys had positive returns."""
    cached = cache_get('calibration')
    if cached:
        return jsonify(cached)

    thresholds = list(range(0, 101, 10))
    results = []
    for threshold in thresholds:
        rows = safe_query(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN price_change_1m > 0 THEN 1 ELSE 0 END) as wins "
            "FROM filings WHERE trade_type LIKE 'P%' AND deterministic_score >= ? "
            "AND price_change_1m IS NOT NULL",
            (threshold,)
        )
        if rows and rows[0]['total'] > 0:
            results.append({
                'threshold': threshold,
                'total': rows[0]['total'],
                'wins': rows[0]['wins'] or 0,
                'win_rate': round((rows[0]['wins'] or 0) / rows[0]['total'] * 100, 1)
            })
        else:
            results.append({'threshold': threshold, 'total': 0, 'wins': 0, 'win_rate': 0})
    result = {'calibration': results}
    cache_set('calibration', result)
    return jsonify(result)


@app.route('/api/research/all')
def api_research_all():
    """Serve pre-computed research stats from JSON file (instant, ~0ms)."""
    cached = cache_get('research_all')
    if cached:
        return jsonify(cached)

    result = load_research_json()
    if result is None:
        logger.warning("research_stats.json not found, generating now...")
        result = generate_research_json()

    cache_set('research_all', result)
    return jsonify(result)


@app.route('/api/research/trajectory-sample')
def api_trajectory_sample():
    """Sample post-purchase price trajectories for the spaghetti chart."""
    min_score = request.args.get('min_score', 40, type=int)
    cache_key = f'trajectory_{min_score}'
    cached = cache_get(cache_key)
    if cached:
        return jsonify(cached)

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT price_change_1d, price_change_1w, price_change_1m, price_change_6m, "
        "deterministic_score, ticker "
        "FROM filings WHERE trade_type LIKE 'P%' AND deterministic_score >= ? "
        "AND price_change_1d IS NOT NULL AND price_change_1w IS NOT NULL "
        "AND price_change_1m IS NOT NULL AND price_change_6m IS NOT NULL "
        "ORDER BY RANDOM() LIMIT 200",
        (min_score,)
    ).fetchall()
    rows = [dict(r) for r in rows]

    avg = conn.execute(
        "SELECT AVG(price_change_1d) as d1, AVG(price_change_1w) as w1, "
        "AVG(price_change_1m) as m1, AVG(price_change_6m) as m6 "
        "FROM filings WHERE trade_type LIKE 'P%' AND deterministic_score >= ? "
        "AND price_change_1d IS NOT NULL AND price_change_1w IS NOT NULL "
        "AND price_change_1m IS NOT NULL AND price_change_6m IS NOT NULL",
        (min_score,)
    ).fetchone()

    median = conn.execute(
        "SELECT price_change_1d, price_change_1w, price_change_1m, price_change_6m "
        "FROM filings WHERE trade_type LIKE 'P%' AND deterministic_score >= ? "
        "AND price_change_1d IS NOT NULL AND price_change_1w IS NOT NULL "
        "AND price_change_1m IS NOT NULL AND price_change_6m IS NOT NULL "
        "ORDER BY price_change_1m LIMIT 1 OFFSET ("
        "SELECT COUNT(*)/2 FROM filings WHERE trade_type LIKE 'P%' AND deterministic_score >= ? "
        "AND price_change_1d IS NOT NULL AND price_change_1w IS NOT NULL "
        "AND price_change_1m IS NOT NULL AND price_change_6m IS NOT NULL)",
        (min_score, min_score)
    ).fetchone()

    result = {
        'trajectories': rows,
        'average': dict(avg) if avg else {},
        'median': dict(median) if median else {},
        'count': len(rows),
    }
    cache_set(cache_key, result)
    return jsonify(result)


@app.route('/api/research/top-insiders')
def api_top_insiders():
    min_trades = request.args.get('min_trades', 5, type=int)
    sort = request.args.get('sort', 'accuracy')

    cache_key = f'top_insiders_{min_trades}_{sort}'
    cached = cache_get(cache_key)
    if cached:
        return jsonify(cached)

    order_col = 'win_rate' if sort == 'accuracy' else 'total_trades'

    rows = safe_query(
        f"SELECT insider_name, COUNT(*) as total_trades, "
        f"SUM(CASE WHEN trade_type LIKE 'P%' THEN 1 ELSE 0 END) as buys, "
        f"SUM(CASE WHEN trade_type LIKE 'S%' THEN 1 ELSE 0 END) as sells, "
        f"SUM(value) as total_value, "
        f"AVG(deterministic_score) as avg_score, "
        f"AVG(CASE WHEN trade_type LIKE 'P%' AND price_change_1m IS NOT NULL THEN price_change_1m END) as avg_return, "
        f"ROUND(100.0 * SUM(CASE WHEN trade_type LIKE 'P%' AND price_change_1m > 0 THEN 1 ELSE 0 END) / "
        f"NULLIF(SUM(CASE WHEN trade_type LIKE 'P%' AND price_change_1m IS NOT NULL THEN 1 ELSE 0 END), 0), 1) as win_rate "
        f"FROM filings GROUP BY insider_name "
        f"HAVING total_trades >= ? ORDER BY {order_col} DESC LIMIT 50",
        (min_trades,)
    )
    result = {'insiders': rows}
    cache_set(cache_key, result)
    return jsonify(result)


def warm_cache():
    """Pre-warm expensive caches in a background thread on startup."""
    import threading
    def _warm():
        time.sleep(1)
        with app.test_request_context():
            for fn in [api_stats, api_research_all, api_trajectory_sample]:
                try:
                    fn()
                except Exception:
                    pass
    threading.Thread(target=_warm, daemon=True).start()


# --- Notable insiders (curated list) ---
# Maps search patterns to display info. These are real SEC Form 4 filers.
NOTABLE_INSIDERS = [
    # Tech titans — ticker-constrained to avoid false positives
    {"pattern": "Musk Elon", "tickers": ["TSLA"], "display_name": "Elon Musk", "category": "Tech CEO", "note": "CEO of Tesla, SpaceX, owner of X"},
    {"pattern": "Musk Kimbal", "tickers": ["TSLA"], "display_name": "Kimbal Musk", "category": "Tech/Family", "note": "Elon's brother, Tesla board member"},
    {"pattern": "Bezos Jeffrey", "tickers": ["AMZN"], "display_name": "Jeff Bezos", "category": "Tech CEO", "note": "Founder & Exec Chair of Amazon"},
    {"pattern": "Zuckerberg Mark", "tickers": ["META", "FB"], "display_name": "Mark Zuckerberg", "category": "Tech CEO", "note": "CEO of Meta/Facebook"},
    {"pattern": "Nadella Satya", "tickers": ["MSFT"], "display_name": "Satya Nadella", "category": "Tech CEO", "note": "CEO of Microsoft"},
    {"pattern": "Pichai Sundar", "tickers": ["GOOGL", "GOOG"], "display_name": "Sundar Pichai", "category": "Tech CEO", "note": "CEO of Alphabet/Google"},
    {"pattern": "Jassy Andrew", "tickers": ["AMZN"], "display_name": "Andy Jassy", "category": "Tech CEO", "note": "CEO of Amazon"},
    {"pattern": "Cook Timothy D", "tickers": ["AAPL"], "display_name": "Tim Cook", "category": "Tech CEO", "note": "CEO of Apple"},
    {"pattern": "Ellison Lawrence", "tickers": ["ORCL", "TSLA"], "display_name": "Larry Ellison", "category": "Tech CEO", "note": "Co-founder of Oracle, Tesla board"},
    {"pattern": "Hastings Reed", "tickers": ["NFLX"], "display_name": "Reed Hastings", "category": "Tech CEO", "note": "Co-founder of Netflix"},
    {"pattern": "Chesky Brian", "tickers": ["ABNB"], "display_name": "Brian Chesky", "category": "Tech CEO", "note": "CEO of Airbnb"},
    {"pattern": "Thiel Peter", "tickers": ["PLTR", "FB"], "display_name": "Peter Thiel", "category": "Investor", "note": "Co-founder of PayPal, Palantir"},
    # Finance / Investors
    {"pattern": "Dimon James", "tickers": ["JPM"], "display_name": "Jamie Dimon", "category": "Finance CEO", "note": "CEO of JPMorgan Chase"},
    {"pattern": "Icahn Carl", "tickers": None, "display_name": "Carl Icahn", "category": "Activist Investor", "note": "Billionaire activist investor"},
    {"pattern": "Buffett Howard", "tickers": None, "display_name": "Howard Buffett", "category": "Finance/Family", "note": "Warren Buffett's son, board member"},
    {"pattern": "Schwarzman Stephen", "tickers": ["BX"], "display_name": "Stephen Schwarzman", "category": "Finance CEO", "note": "CEO of Blackstone"},
    {"pattern": "Walton Family Holdings", "tickers": ["WMT"], "display_name": "Walton Family Trust", "category": "Family Office", "note": "Walmart founding family"},
    # Political / Government connected
    {"pattern": "Mnuchin Steven", "tickers": None, "display_name": "Steven Mnuchin", "category": "Political", "note": "Former US Treasury Secretary"},
    {"pattern": "Kushner Joshua", "tickers": ["OSCR"], "display_name": "Joshua Kushner", "category": "Political/VC", "note": "Jared Kushner's brother, VC investor"},
    {"pattern": "Nunes Devin", "tickers": ["DJT"], "display_name": "Devin Nunes", "category": "Political", "note": "Former congressman, CEO of Trump Media (DJT)"},
    # Mega-company leaders
    {"pattern": "Schultz Howard", "tickers": ["SBUX"], "display_name": "Howard Schultz", "category": "Corporate CEO", "note": "Former CEO of Starbucks"},
]


@app.route('/api/notable-insiders')
def api_notable_insiders():
    days = request.args.get('days', 0, type=int)
    trade_type = request.args.get('trade_type', '')

    cache_key = f"notable_insiders_{days}_{trade_type}"
    cached = cache_get(cache_key)
    if cached:
        return jsonify(cached)

    conn = get_connection()
    results = []

    for person in NOTABLE_INSIDERS:
        params = ['%' + person["pattern"] + '%']
        extra_clauses = ""
        if person.get("tickers"):
            placeholders = ",".join("?" * len(person["tickers"]))
            extra_clauses += f" AND ticker IN ({placeholders})"
            params.extend(person["tickers"])
        if days > 0:
            cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            extra_clauses += " AND filing_date >= ?"
            params.append(cutoff)
        if trade_type:
            extra_clauses += " AND trade_type LIKE ?"
            params.append(f"{trade_type}%")

        rows = conn.execute(
            f"""SELECT id, filing_date, trade_date, ticker, company_name, insider_name,
                      title, trade_type, price, qty, value, delta_own, deterministic_score, is_10b5_1
               FROM filings WHERE insider_name LIKE ?{extra_clauses}
               ORDER BY filing_date DESC LIMIT 50""",
            params
        ).fetchall()

        if not rows:
            continue

        filings = [dict(r) for r in rows]
        purchases = [f for f in filings if f.get("trade_type", "").startswith("P")]
        sales = [f for f in filings if "Sale" in (f.get("trade_type") or "")]
        total_bought = sum(abs(f["value"] or 0) for f in purchases)
        total_sold = sum(abs(f["value"] or 0) for f in sales)

        results.append({
            "display_name": person["display_name"],
            "category": person["category"],
            "note": person["note"],
            "total_filings": len(filings),
            "purchases": len(purchases),
            "sales": len(sales),
            "total_bought": total_bought,
            "total_sold": total_sold,
            "latest_filing": filings[0] if filings else None,
            "recent_filings": filings[:10],
        })

    results.sort(key=lambda x: x["latest_filing"]["filing_date"] if x.get("latest_filing") else "", reverse=True)
    cache_set(cache_key, results)
    return jsonify(results)


warm_cache()


if __name__ == '__main__':
    import sys
    port = int(sys.argv[sys.argv.index('--port') + 1]) if '--port' in sys.argv else 5002
    app.run(debug=True, port=port, threaded=True)
