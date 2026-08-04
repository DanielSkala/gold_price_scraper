"""Gold spot price, premium and portfolio logic.

The Gold tab is OPTIONAL and specific to the original author's setup. It relies on
three things that a generic clone may not have, so each degrades gracefully:
  * the sibling `../../gold/gold_premiums.csv` (only present in the full monorepo) —
    a missing CSV just makes /api/gold/premiums return 404 (the tab shows no premiums);
  * a network connection for live spot prices via yfinance — offline fetches return
    None so the routes answer with a clean JSON error instead of a 500 stack trace;
  * the author's own purchase history in GOLD_PURCHASES — override it for your own
    holdings via a `data/gold_purchases.json` file or the GOLD_PURCHASES_JSON env var.
None of these ever block app startup or affect the other tabs.
"""
import csv
import json
import math
import os
import time

import pandas as pd
import yfinance as yf

# ── Gold constants ──────────────────────────────────────────────
TROY_OUNCE_GRAMS = 31.1034768
GOLD_CSV = os.path.join(os.path.dirname(__file__), '..', '..', 'gold', 'gold_premiums.csv')
GOLD_WEIGHTS = [1, 2, 5, 10, 20, 31.1, 50, 100, 250, 500, 1000]
GOLD_WEIGHT_LABELS = ['1g', '2g', '5g', '10g', '20g', '31.1g', '50g', '100g', '250g', '500g', '1000g']

# The author's own bar purchases — kept as the default so their Gold tab is unchanged.
# Override with your own holdings via data/gold_purchases.json or $GOLD_PURCHASES_JSON
# (a JSON list of {date, weight_g, price_eur, label} objects).
_DEFAULT_GOLD_PURCHASES = [
    {"date": "2025-02-03", "weight_g": 20, "price_eur": 1785.00, "label": "Argor Heraeus 20g"},
    {"date": "2026-01-29", "weight_g": 20, "price_eur": 3063.00, "label": "Argor Heraeus 20g"},
]
_GOLD_PURCHASES_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'gold_purchases.json')


def _load_gold_purchases():
    """Purchases from $GOLD_PURCHASES_JSON or data/gold_purchases.json, else the
    author's defaults. Any parse error falls back to the defaults."""
    raw = os.environ.get('GOLD_PURCHASES_JSON')
    if not raw and os.path.exists(_GOLD_PURCHASES_FILE):
        try:
            with open(_GOLD_PURCHASES_FILE, encoding='utf-8') as f:
                raw = f.read()
        except OSError:
            raw = None
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
        except (ValueError, TypeError):
            pass
    return _DEFAULT_GOLD_PURCHASES


GOLD_PURCHASES = _load_gold_purchases()

# Simple in-memory cache for gold spot data (fetching is slow)
_gold_cache = {}  # keyed by period
GOLD_CACHE_TTL = 300  # 5 minutes
GOLD_VALID_PERIODS = ['1mo', '3mo', '6mo', '1y', '2y', '5y', 'max']


def fetch_gold_spot_history(period='2y'):
    """Fetch gold spot price history in EUR for the given period, with caching."""
    if period not in GOLD_VALID_PERIODS:
        period = '2y'

    now = time.time()
    cached = _gold_cache.get(period)
    if cached is not None and (now - cached["ts"]) < GOLD_CACHE_TTL:
        return cached["data"]

    try:
        gold = yf.Ticker("GC=F")
        fx = yf.Ticker("EURUSD=X")
        gold_hist = gold.history(period=period)
        fx_hist = fx.history(period=period)
    except Exception:
        # No network / yfinance rate-limit / API change — treat as "unavailable"
        # so callers return a clean error rather than 500-ing on a stack trace.
        return None

    if gold_hist.empty or fx_hist.empty:
        return None

    gold_close = gold_hist["Close"].copy()
    gold_close.index = gold_close.index.tz_localize(None).normalize()
    gold_close = gold_close[~gold_close.index.duplicated(keep="last")]

    fx_close = fx_hist["Close"].copy()
    fx_close.index = fx_close.index.tz_localize(None).normalize()
    fx_close = fx_close[~fx_close.index.duplicated(keep="last")]

    df = pd.merge(gold_close.rename("gold_usd"), fx_close.rename("eurusd"),
                   left_index=True, right_index=True, how="inner")
    df["gold_eur_per_oz"] = df["gold_usd"] / df["eurusd"]
    df["gold_eur_per_g"] = df["gold_eur_per_oz"] / TROY_OUNCE_GRAMS

    _gold_cache[period] = {"data": df, "ts": now}
    return df


def get_spot_on_date(df, date_str):
    try:
        target = pd.Timestamp(date_str)
    except (ValueError, TypeError):
        return None
    if target is None or pd.isna(target):
        return None
    mask = df.index <= target
    if mask.any():
        return float(df.loc[mask, "gold_eur_per_oz"].iloc[-1])
    return None


def spot_history_payload(period='2y'):
    """Return the /api/gold/spot-history payload, or None if data unavailable."""
    df = fetch_gold_spot_history(period)
    if df is None:
        return None

    dates = [d.strftime('%Y-%m-%d') for d in df.index]
    prices_oz = [round(v, 2) for v in df["gold_eur_per_oz"].tolist()]
    prices_g = [round(v, 2) for v in df["gold_eur_per_g"].tolist()]

    return {
        "period": period,
        "dates": dates,
        "prices_eur_oz": prices_oz,
        "prices_eur_g": prices_g,
        "current_eur_oz": prices_oz[-1] if prices_oz else None,
        "current_eur_g": prices_g[-1] if prices_g else None,
    }


def premiums_payload():
    """Return the /api/gold/premiums payload, or None if the CSV is missing."""
    if not os.path.exists(GOLD_CSV):
        return None

    rows = []
    dates = []
    with open(GOLD_CSV, newline='') as f:
        for row in csv.reader(f):
            premiums = []
            for cell in row[:-1]:
                try:
                    premiums.append(round(float(cell.strip()), 2))
                except ValueError:
                    premiums.append(None)
            rows.append(premiums)
            dates.append(row[-1].strip())

    # Compute averages per weight
    avg = []
    for col_idx in range(len(GOLD_WEIGHTS)):
        vals = [r[col_idx] for r in rows if col_idx < len(r) and r[col_idx] is not None]
        avg.append(round(sum(vals) / len(vals), 2) if vals else None)

    # Latest row
    latest = rows[-1] if rows else []

    return {
        "weights": GOLD_WEIGHTS,
        "weight_labels": GOLD_WEIGHT_LABELS,
        "dates": dates,
        "rows": rows,
        "averages": avg,
        "latest": latest,
        "latest_date": dates[-1] if dates else None,
    }


def portfolio_payload():
    """Return the /api/gold/portfolio payload, or None if data unavailable."""
    df = fetch_gold_spot_history('max')
    if df is None:
        return None

    current_spot_oz = float(df["gold_eur_per_oz"].iloc[-1])
    current_spot_g = current_spot_oz / TROY_OUNCE_GRAMS

    items = []
    total_cost = 0
    total_weight = 0

    for p in GOLD_PURCHASES:
        # Guard against a malformed self-edited gold_purchases.json entry: missing keys
        # or a zero/negative weight or cost would otherwise KeyError or ZeroDivisionError
        # and 500 the whole gold tab. Skip such entries instead.
        try:
            weight = float(p["weight_g"])
            cost = float(p["price_eur"])
        except (KeyError, TypeError, ValueError):
            continue
        if weight <= 0 or cost <= 0 or not (math.isfinite(weight) and math.isfinite(cost)):
            continue
        label = p.get("label", f"{weight:g}g bar")
        value_now = weight * current_spot_g
        gl = value_now - cost
        gl_pct = (gl / cost) * 100

        spot_at_purchase_oz = get_spot_on_date(df, p.get("date"))
        price_per_oz = (cost / weight) * TROY_OUNCE_GRAMS
        premium_pct = ((price_per_oz / spot_at_purchase_oz) - 1) * 100 if spot_at_purchase_oz else None

        items.append({
            "label": label,
            "date": p["date"],
            "weight_g": weight,
            "cost": round(cost, 2),
            "cost_per_oz": round(price_per_oz, 2),
            "spot_at_purchase_oz": round(spot_at_purchase_oz, 2) if spot_at_purchase_oz else None,
            "premium_pct": round(premium_pct, 1) if premium_pct is not None else None,
            "value_now": round(value_now, 2),
            "gain_loss": round(gl, 2),
            "gain_loss_pct": round(gl_pct, 1),
        })
        total_cost += cost
        total_weight += weight

    total_value = total_weight * current_spot_g
    total_gl = total_value - total_cost
    total_gl_pct = (total_gl / total_cost) * 100 if total_cost else 0

    return {
        "total_weight_g": total_weight,
        "total_cost": round(total_cost, 2),
        "current_value": round(total_value, 2),
        "gain_loss": round(total_gl, 2),
        "gain_loss_pct": round(total_gl_pct, 1),
        "current_spot_per_oz": round(current_spot_oz, 2),
        "current_spot_per_g": round(current_spot_g, 2),
        "items": items,
    }
