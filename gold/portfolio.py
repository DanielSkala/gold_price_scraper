"""
Gold Portfolio Tracker
======================
Tracks gold bar purchases against the EUR spot price.
Shows spot price history, purchase points, premiums, and total P&L.

HOW TO ADD PURCHASES:
    Edit the PURCHASES list below. Each entry is a dict with:
        - date:       "YYYY-MM-DD" purchase date
        - weight_g:   weight in grams
        - price_eur:  total price you actually paid in EUR
        - label:      (optional) short description, e.g. "Argor 10g"
"""

from __future__ import annotations

import datetime
from typing import Dict, List, Optional

import pandas as pd
import plotly.graph_objects as go
import yfinance as yf

TROY_OUNCE_GRAMS = 31.1034768

# ──────────────────────────────────────────────────────────────────────
#  YOUR PURCHASES — edit this list
# ──────────────────────────────────────────────────────────────────────
PURCHASES = [
    {"date": "2025-02-03", "weight_g": 20, "price_eur": 1785.00, "label": "Argor Heraeus 20g"},
    {"date": "2026-01-29", "weight_g": 20, "price_eur": 3063.00, "label": "Argor Heraeus 20g"},
]

TIMEFRAMES = ["1d", "5d", "1mo", "6mo", "1y", "2y", "5y", "max"]
DEFAULT_TIMEFRAME = "2y"


def fetch_gold_eur_history(period: str) -> pd.DataFrame:
    """Fetch gold price in EUR/gram for the given yfinance period."""
    gold = yf.Ticker("GC=F")
    fx = yf.Ticker("EURUSD=X")

    gold_hist = gold.history(period=period)
    fx_hist = fx.history(period=period)

    if gold_hist.empty or fx_hist.empty:
        raise RuntimeError("Failed to fetch price data from Yahoo Finance")

    # Normalize both to tz-naive date index for reliable joining
    gold_close = gold_hist["Close"].copy()
    gold_close.index = gold_close.index.tz_localize(None).normalize()
    gold_close = gold_close.rename("gold_usd")

    fx_close = fx_hist["Close"].copy()
    fx_close.index = fx_close.index.tz_localize(None).normalize()
    fx_close = fx_close.rename("eurusd")

    # Drop duplicate dates (keep last), then inner join
    gold_close = gold_close[~gold_close.index.duplicated(keep="last")]
    fx_close = fx_close[~fx_close.index.duplicated(keep="last")]

    df = pd.merge(gold_close, fx_close, left_index=True, right_index=True, how="inner")
    df["gold_eur_per_oz"] = df["gold_usd"] / df["eurusd"]
    df["gold_eur_per_g"] = df["gold_eur_per_oz"] / TROY_OUNCE_GRAMS
    return df


def get_spot_price_on_date(df: pd.DataFrame, date_str: str) -> float | None:
    """Get the spot price (EUR/oz) on or closest before a given date."""
    target = pd.Timestamp(date_str)
    mask = df.index <= target
    if mask.any():
        return float(df.loc[mask, "gold_eur_per_oz"].iloc[-1])
    return None


def build_chart(df_by_tf: dict, purchases: list[dict], default_tf: str) -> go.Figure:
    """Build the interactive Plotly chart with timeframe selector buttons.

    Args:
        df_by_tf: dict mapping timeframe string -> DataFrame
        purchases: list of purchase dicts
        default_tf: which timeframe to show initially
    """
    fig = go.Figure()

    # We'll add one spot-price trace per timeframe, only the default visible
    trace_groups = {}  # tf -> list of trace indices
    trace_idx = 0

    for tf in TIMEFRAMES:
        df = df_by_tf.get(tf)
        if df is None or df.empty:
            trace_groups[tf] = []
            continue

        group_start = trace_idx
        visible = (tf == default_tf)

        # --- Spot price line ---
        fig.add_trace(go.Scatter(
            x=df.index,
            y=df["gold_eur_per_oz"],
            mode="lines",
            name="Gold Spot (EUR/oz)",
            line=dict(color="#D4AF37", width=2),
            hovertemplate="%{x|%d %b %Y}<br>Spot: %{y:,.2f} EUR/oz<extra></extra>",
            visible=visible,
            showlegend=(tf == default_tf),
        ))
        trace_idx += 1

        # --- Purchase markers and vertical lines ---
        for p in purchases:
            pdate = pd.Timestamp(p["date"])
            weight = p["weight_g"]
            price_total = p["price_eur"]
            price_per_oz = (price_total / weight) * TROY_OUNCE_GRAMS
            label = p.get("label", f"{weight}g bar")

            spot_at_purchase = get_spot_price_on_date(df, p["date"])
            premium_pct = ((price_per_oz / spot_at_purchase) - 1) * 100 if spot_at_purchase else None

            # Marker at the price you actually paid (per oz)
            hover = (
                f"<b>{label}</b><br>"
                f"Date: {p['date']}<br>"
                f"Weight: {weight}g<br>"
                f"Paid: {price_total:,.2f} EUR ({price_per_oz:,.2f} EUR/oz)<br>"
            )
            if premium_pct is not None:
                hover += f"Spot: {spot_at_purchase:,.2f} EUR/oz<br>"
                hover += f"Premium: {premium_pct:.1f}%"
            hover += "<extra></extra>"

            fig.add_trace(go.Scatter(
                x=[pdate],
                y=[price_per_oz],
                mode="markers",
                name=f"Buy: {label}",
                marker=dict(
                    symbol="triangle-up",
                    size=14,
                    color="red",
                    line=dict(width=1, color="darkred"),
                ),
                hovertemplate=hover,
                visible=visible,
                showlegend=(tf == default_tf),
            ))
            trace_idx += 1

            # Spot price at purchase as a small dot
            if spot_at_purchase is not None:
                fig.add_trace(go.Scatter(
                    x=[pdate],
                    y=[spot_at_purchase],
                    mode="markers",
                    marker=dict(symbol="circle", size=7, color="#D4AF37",
                                line=dict(width=1, color="black")),
                    showlegend=False,
                    hovertemplate=f"Spot at purchase: {spot_at_purchase:,.2f} EUR/oz<extra></extra>",
                    visible=visible,
                ))
                trace_idx += 1

        trace_groups[tf] = list(range(group_start, trace_idx))

    total_traces = trace_idx

    # --- Timeframe selector buttons ---
    buttons = []
    for tf in TIMEFRAMES:
        visibility = [False] * total_traces
        for idx in trace_groups.get(tf, []):
            visibility[idx] = True
        buttons.append(dict(
            label=tf.upper(),
            method="update",
            args=[
                {"visible": visibility},
                {"title": f"Gold Spot Price (EUR/oz) — {tf} view"},
            ],
        ))

    # --- Vertical lines for purchases (these persist across all views) ---
    shapes = []
    for p in purchases:
        shapes.append(dict(
            type="line",
            x0=p["date"], x1=p["date"],
            y0=0, y1=1,
            yref="paper",
            line=dict(color="rgba(220, 50, 50, 0.6)", width=1.5, dash="dash"),
        ))

    fig.update_layout(
        title=f"Gold Spot Price (EUR/oz) — {default_tf} view",
        xaxis_title="Date",
        yaxis_title="EUR per troy ounce",
        template="plotly_white",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        shapes=shapes,
        updatemenus=[dict(
            type="buttons",
            direction="right",
            x=0.5,
            xanchor="center",
            y=-0.12,
            yanchor="top",
            buttons=buttons,
            font=dict(size=12),
            bgcolor="white",
            bordercolor="#D4AF37",
            borderwidth=1,
        )],
        margin=dict(b=100),
    )

    return fig


def compute_portfolio(df: pd.DataFrame, purchases: list[dict]) -> dict:
    """Compute portfolio value, gain/loss, and per-purchase breakdown."""
    current_spot_per_oz = float(df["gold_eur_per_oz"].iloc[-1])
    current_spot_per_g = current_spot_per_oz / TROY_OUNCE_GRAMS

    if not purchases:
        return {"total_weight_g": 0, "total_cost": 0, "current_value": 0,
                "gain_loss": 0, "gain_loss_pct": 0,
                "current_spot_per_oz": current_spot_per_oz,
                "items": []}

    items = []
    total_cost = 0
    total_weight = 0

    for p in purchases:
        weight = p["weight_g"]
        cost = p["price_eur"]
        label = p.get("label", f"{weight}g bar")
        value_now = weight * current_spot_per_g
        gl = value_now - cost
        gl_pct = (gl / cost) * 100

        spot_at_purchase_oz = get_spot_price_on_date(df, p["date"])
        price_per_oz = (cost / weight) * TROY_OUNCE_GRAMS
        premium_pct = ((price_per_oz / spot_at_purchase_oz) - 1) * 100 if spot_at_purchase_oz else None

        items.append({
            "label": label,
            "date": p["date"],
            "weight_g": weight,
            "cost": cost,
            "cost_per_oz": price_per_oz,
            "spot_at_purchase_oz": spot_at_purchase_oz,
            "premium_pct": premium_pct,
            "value_now": value_now,
            "gain_loss": gl,
            "gain_loss_pct": gl_pct,
        })
        total_cost += cost
        total_weight += weight

    total_value = total_weight * current_spot_per_g
    total_gl = total_value - total_cost
    total_gl_pct = (total_gl / total_cost) * 100 if total_cost else 0

    return {
        "total_weight_g": total_weight,
        "total_cost": total_cost,
        "current_value": total_value,
        "gain_loss": total_gl,
        "gain_loss_pct": total_gl_pct,
        "current_spot_per_oz": current_spot_per_oz,
        "items": items,
    }


def print_portfolio(portfolio: dict):
    """Print a formatted portfolio summary to the terminal."""
    spot_oz = portfolio["current_spot_per_oz"]
    print(f"\n{'='*65}")
    print(f"  GOLD PORTFOLIO — {datetime.date.today()}")
    print(f"  Current spot: {spot_oz:,.2f} EUR/oz  ({spot_oz / TROY_OUNCE_GRAMS:.2f} EUR/g)")
    print(f"{'='*65}")

    if not portfolio["items"]:
        print("  No purchases recorded. Edit PURCHASES in portfolio.py.\n")
        return

    for item in portfolio["items"]:
        sign = "+" if item["gain_loss"] >= 0 else ""
        prem = f"  premium {item['premium_pct']:.1f}%" if item["premium_pct"] is not None else ""
        print(
            f"  {item['label']:20s}  {item['date']}  "
            f"{item['weight_g']:>6.1f}g  "
            f"cost {item['cost']:>8.2f} EUR  "
            f"now {item['value_now']:>8.2f} EUR  "
            f"{sign}{item['gain_loss']:>7.2f} ({sign}{item['gain_loss_pct']:.1f}%)"
            f"{prem}"
        )

    print(f"  {'-'*56}")
    gl = portfolio["gain_loss"]
    sign = "+" if gl >= 0 else ""
    print(
        f"  {'TOTAL':20s}  {'':10s}  "
        f"{portfolio['total_weight_g']:>6.1f}g  "
        f"cost {portfolio['total_cost']:>8.2f} EUR  "
        f"now {portfolio['current_value']:>8.2f} EUR  "
        f"{sign}{gl:>7.2f} ({sign}{portfolio['gain_loss_pct']:.1f}%)"
    )
    print()


def main():
    # Fetch all timeframes for the interactive chart
    df_by_tf = {}
    for tf in TIMEFRAMES:
        print(f"Fetching {tf}...", end=" ", flush=True)
        try:
            df_by_tf[tf] = fetch_gold_eur_history(tf)
            print(f"{len(df_by_tf[tf])} pts")
        except Exception as e:
            print(f"failed ({e})")
            df_by_tf[tf] = None

    # Use the longest available timeframe for portfolio computation
    for fallback in ("max", "5y", DEFAULT_TIMEFRAME):
        df_full = df_by_tf.get(fallback)
        if df_full is not None and not df_full.empty:
            break
    print(f"\nUsing {len(df_full)} data points for portfolio ({df_full.index[0].date()} to {df_full.index[-1].date()})")

    portfolio = compute_portfolio(df_full, PURCHASES)
    print_portfolio(portfolio)

    fig = build_chart(df_by_tf, PURCHASES, DEFAULT_TIMEFRAME)
    fig.show()


if __name__ == "__main__":
    main()
