"""
Pre-compute research statistics and save as JSON.

Called after rescoring to generate instant-loading research data.
The web endpoint just reads the JSON file — no queries needed.
"""

import json
import logging
import sqlite3
import time

from openinsider.config import DATA_DIR
from openinsider.db import get_connection, init_db

logger = logging.getLogger(__name__)

RESEARCH_JSON = DATA_DIR / "research_stats.json"


def compute_research_stats() -> dict:
    """Compute all research statistics from the database."""
    init_db()
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    result = {'periods': ['1 Day', '1 Week', '1 Month', '6 Months'], 'overall': {}, 'by_bin': {}}
    bin_labels = ['0-19', '20-39', '40-59', '60+']

    # --- ONE query for ALL buy stats ---
    buy_rows = conn.execute("""
        SELECT
            CASE
                WHEN deterministic_score < 20 THEN 0
                WHEN deterministic_score < 40 THEN 1
                WHEN deterministic_score < 60 THEN 2
                ELSE 3
            END as bin_idx,
            COUNT(*) as total,
            SUM(CASE WHEN price_change_1d IS NOT NULL THEN 1 ELSE 0 END) as n_1d,
            SUM(CASE WHEN price_change_1d > 0 THEN 1 ELSE 0 END) as wins_1d,
            SUM(price_change_1d) as sum_1d,
            SUM(CASE WHEN price_change_1d > 0 THEN price_change_1d ELSE 0 END) as sum_win_1d,
            SUM(CASE WHEN price_change_1d IS NOT NULL AND price_change_1d <= 0 THEN price_change_1d ELSE 0 END) as sum_loss_1d,
            SUM(CASE WHEN price_change_1w IS NOT NULL THEN 1 ELSE 0 END) as n_1w,
            SUM(CASE WHEN price_change_1w > 0 THEN 1 ELSE 0 END) as wins_1w,
            SUM(price_change_1w) as sum_1w,
            SUM(CASE WHEN price_change_1w > 0 THEN price_change_1w ELSE 0 END) as sum_win_1w,
            SUM(CASE WHEN price_change_1w IS NOT NULL AND price_change_1w <= 0 THEN price_change_1w ELSE 0 END) as sum_loss_1w,
            SUM(CASE WHEN price_change_1m IS NOT NULL THEN 1 ELSE 0 END) as n_1m,
            SUM(CASE WHEN price_change_1m > 0 THEN 1 ELSE 0 END) as wins_1m,
            SUM(price_change_1m) as sum_1m,
            SUM(CASE WHEN price_change_1m > 0 THEN price_change_1m ELSE 0 END) as sum_win_1m,
            SUM(CASE WHEN price_change_1m IS NOT NULL AND price_change_1m <= 0 THEN price_change_1m ELSE 0 END) as sum_loss_1m,
            SUM(CASE WHEN price_change_6m IS NOT NULL THEN 1 ELSE 0 END) as n_6m,
            SUM(CASE WHEN price_change_6m > 0 THEN 1 ELSE 0 END) as wins_6m,
            SUM(price_change_6m) as sum_6m,
            SUM(CASE WHEN price_change_6m > 0 THEN price_change_6m ELSE 0 END) as sum_win_6m,
            SUM(CASE WHEN price_change_6m IS NOT NULL AND price_change_6m <= 0 THEN price_change_6m ELSE 0 END) as sum_loss_6m
        FROM filings
        WHERE trade_type LIKE 'P%' AND deterministic_score IS NOT NULL
        GROUP BY bin_idx ORDER BY bin_idx
    """).fetchall()

    col_map = {
        '1 Day': '1d', '1 Week': '1w', '1 Month': '1m', '6 Months': '6m',
    }

    for label, sfx in col_map.items():
        bin_data = [{'bin': bl, 'total': 0, 'wins': 0, 'win_rate': 0, 'avg_return': 0} for bl in bin_labels]
        total_all, wins_all, sum_ret, sum_win, sum_loss, n_win, n_loss = 0, 0, 0.0, 0.0, 0.0, 0, 0
        for r in buy_rows:
            idx = r['bin_idx']
            n = r[f'n_{sfx}'] or 0
            w = r[f'wins_{sfx}'] or 0
            s = r[f'sum_{sfx}'] or 0
            if n > 0:
                bin_data[idx] = {
                    'bin': bin_labels[idx], 'total': n, 'wins': w,
                    'win_rate': round(100 * w / n, 1),
                    'avg_return': round(s / n, 2),
                }
            total_all += n
            wins_all += w
            sum_ret += s
            sum_win += r[f'sum_win_{sfx}'] or 0
            n_win += w
            sum_loss += r[f'sum_loss_{sfx}'] or 0
            n_loss += (n - w)

        result['by_bin'][label] = bin_data
        if total_all:
            result['overall'][label] = {
                'total': total_all, 'wins': wins_all,
                'win_rate': round(100 * wins_all / total_all, 1),
                'avg_return': round(sum_ret / total_all, 2),
                'avg_win': round(sum_win / n_win, 2) if n_win else 0,
                'avg_loss': round(sum_loss / n_loss, 2) if n_loss else 0,
            }

    # --- Sell stats ---
    sell_row = conn.execute("""
        SELECT
            SUM(CASE WHEN price_change_1d IS NOT NULL THEN 1 ELSE 0 END) as n_1d,
            SUM(CASE WHEN price_change_1d > 0 THEN 1 ELSE 0 END) as w_1d,
            SUM(price_change_1d) as s_1d,
            SUM(CASE WHEN price_change_1w IS NOT NULL THEN 1 ELSE 0 END) as n_1w,
            SUM(CASE WHEN price_change_1w > 0 THEN 1 ELSE 0 END) as w_1w,
            SUM(price_change_1w) as s_1w,
            SUM(CASE WHEN price_change_1m IS NOT NULL THEN 1 ELSE 0 END) as n_1m,
            SUM(CASE WHEN price_change_1m > 0 THEN 1 ELSE 0 END) as w_1m,
            SUM(price_change_1m) as s_1m,
            SUM(CASE WHEN price_change_6m IS NOT NULL THEN 1 ELSE 0 END) as n_6m,
            SUM(CASE WHEN price_change_6m > 0 THEN 1 ELSE 0 END) as w_6m,
            SUM(price_change_6m) as s_6m
        FROM filings WHERE trade_type LIKE 'S%'
    """).fetchone()
    if sell_row:
        for label, sfx in col_map.items():
            n = sell_row[f'n_{sfx}'] or 0
            w = sell_row[f'w_{sfx}'] or 0
            s = sell_row[f's_{sfx}'] or 0
            if n > 0:
                result['overall'][f'{label} (Sales)'] = {
                    'total': n, 'wins': w,
                    'win_rate': round(100 * w / n, 1),
                    'avg_return': round(s / n, 2),
                }

    # --- Score distribution + calibration ---
    dist_rows = conn.execute("""
        SELECT CAST(deterministic_score / 10 AS INTEGER) * 10 as bin,
        COUNT(*) as total,
        AVG(price_change_1m) as avg_return_1m,
        AVG(price_change_1w) as avg_return_1w,
        SUM(CASE WHEN price_change_1m IS NOT NULL THEN 1 ELSE 0 END) as n_1m,
        SUM(CASE WHEN price_change_1m > 0 THEN 1 ELSE 0 END) as wins_1m
        FROM filings WHERE trade_type LIKE 'P%' AND deterministic_score IS NOT NULL
        GROUP BY bin ORDER BY bin
    """).fetchall()
    result['distribution'] = [dict(r) for r in dist_rows]

    calibration = []
    for threshold in range(0, 101, 10):
        total = sum(r['n_1m'] or 0 for r in dist_rows if r['bin'] >= threshold)
        wins = sum(r['wins_1m'] or 0 for r in dist_rows if r['bin'] >= threshold)
        calibration.append({
            'threshold': threshold, 'total': total, 'wins': wins,
            'win_rate': round(100 * wins / total, 1) if total else 0,
        })
    result['calibration'] = calibration

    # --- Factor analysis ---
    factors = []
    for query, factor_name, val_col in [
        ("""SELECT
                CASE
                    WHEN title LIKE '%CEO%' OR title LIKE '%Chief Executive%' THEN 'CEO/CFO'
                    WHEN title LIKE '%CFO%' OR title LIKE '%Chief Financial%' THEN 'CEO/CFO'
                    WHEN title LIKE '%Director%' THEN 'Director'
                    WHEN title LIKE '%VP%' OR title LIKE '%Vice President%' THEN 'VP'
                    ELSE 'Other'
                END as role,
                COUNT(*) as total, AVG(price_change_1m) as avg_ret,
                ROUND(100.0 * SUM(CASE WHEN price_change_1m > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate
            FROM filings WHERE trade_type LIKE 'P%' AND price_change_1m IS NOT NULL
            GROUP BY role""", 'Seniority', 'role'),
        ("""SELECT
                CASE
                    WHEN ABS(value) < 10000 THEN '<$10K'
                    WHEN ABS(value) < 100000 THEN '$10K-100K'
                    WHEN ABS(value) < 500000 THEN '$100K-500K'
                    WHEN ABS(value) < 1000000 THEN '$500K-1M'
                    ELSE '>$1M'
                END as bracket,
                COUNT(*) as total, AVG(price_change_1m) as avg_ret,
                ROUND(100.0 * SUM(CASE WHEN price_change_1m > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate
            FROM filings WHERE trade_type LIKE 'P%' AND price_change_1m IS NOT NULL
            GROUP BY bracket ORDER BY MIN(ABS(value))""", 'Trade Value', 'bracket'),
        ("""SELECT
                CASE
                    WHEN ABS(delta_own) < 1 THEN '<1%'
                    WHEN ABS(delta_own) < 5 THEN '1-5%'
                    WHEN ABS(delta_own) < 20 THEN '5-20%'
                    ELSE '>20%'
                END as bracket,
                COUNT(*) as total, AVG(price_change_1m) as avg_ret,
                ROUND(100.0 * SUM(CASE WHEN price_change_1m > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate
            FROM filings WHERE trade_type LIKE 'P%' AND price_change_1m IS NOT NULL
            AND delta_own IS NOT NULL
            GROUP BY bracket ORDER BY MIN(ABS(delta_own))""", 'Ownership Delta', 'bracket'),
    ]:
        for r in conn.execute(query).fetchall():
            factors.append({
                'factor': factor_name, 'value': r[val_col], 'total': r['total'],
                'avg_return': round(r['avg_ret'] or 0, 2), 'win_rate': r['win_rate'] or 0,
            })
    result['factors'] = factors

    # --- Top insiders ---
    ins_rows = conn.execute("""
        SELECT insider_name, COUNT(*) as total_trades,
        SUM(CASE WHEN trade_type LIKE 'P%' THEN 1 ELSE 0 END) as buys,
        SUM(CASE WHEN trade_type LIKE 'S%' THEN 1 ELSE 0 END) as sells,
        SUM(value) as total_value,
        AVG(deterministic_score) as avg_score,
        AVG(CASE WHEN trade_type LIKE 'P%' AND price_change_1m IS NOT NULL THEN price_change_1m END) as avg_return,
        ROUND(100.0 * SUM(CASE WHEN trade_type LIKE 'P%' AND price_change_1m > 0 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN trade_type LIKE 'P%' AND price_change_1m IS NOT NULL THEN 1 ELSE 0 END), 0), 1) as win_rate
        FROM filings GROUP BY insider_name
        HAVING total_trades >= 3 ORDER BY win_rate DESC LIMIT 50
    """).fetchall()
    result['insiders'] = [dict(r) for r in ins_rows]

    result['generated_at'] = time.strftime('%Y-%m-%dT%H:%M:%S')
    return result


def generate_research_json():
    """Compute research stats and write to JSON file."""
    start = time.time()
    result = compute_research_stats()
    RESEARCH_JSON.write_text(json.dumps(result))
    elapsed = time.time() - start
    logger.info("Research stats generated in %.1fs -> %s", elapsed, RESEARCH_JSON)
    return result


def load_research_json() -> dict:
    """Load pre-computed research stats from JSON file."""
    if RESEARCH_JSON.exists():
        return json.loads(RESEARCH_JSON.read_text())
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    generate_research_json()
