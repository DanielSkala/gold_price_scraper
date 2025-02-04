import asyncio
import logging
import re
import time

import aiohttp
import plotly.graph_objects as go
import yfinance as yf
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TROY_OUNCE = 31.1034768


def get_gold_spot_price():
    try:
        gold_data = yf.Ticker("GC=F")
        gold_price_usd_per_ounce = gold_data.history(period="1d")["Close"].iloc[-1]
        eurusd_data = yf.Ticker("EURUSD=X")
        eurusd_rate = eurusd_data.history(period="1d")["Close"].iloc[-1]
        gold_price_eur_per_ounce = gold_price_usd_per_ounce / eurusd_rate
        return gold_price_eur_per_ounce / TROY_OUNCE  # 1 troy ounce = 31.1 grams
    except Exception as e:
        logger.error(f"Failed to fetch gold spot price: {e}")
        return 2730 / TROY_OUNCE  # Fallback price as of 4.3.2025


async def fetch_gold_price(url, session):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            html = await response.text()
        soup = BeautifulSoup(html, "html.parser")
        price_span = soup.find("span", id="hlavni_cena")
        if not price_span:
            logger.error(f"Price not found or possibly sold out for {url}")
            return None
        price_text = price_span.get_text(strip=True).replace("EUR", "").strip()
        price_str = price_text.replace(" ", "").replace(",", ".")
        return float(price_str)
    except Exception as e:
        logger.error(f"Error fetching price from {url}: {e}")
        return None


async def extract_all_prices(gold_bars):
    async with aiohttp.ClientSession() as session:
        tasks = {
            weight: asyncio.create_task(fetch_gold_price(url, session))
            for weight, url in gold_bars.items()
        }
        return {weight: await task for weight, task in tasks.items()}


def plot_graph(weights, premiums, sorted_indices):
    # Extract the top three lowest premiums.
    weights_top = [weights[i] for i in sorted_indices]
    premiums_top = [premiums[i] for i in sorted_indices]

    # Create the main trace (line + markers).
    trace_line = go.Scatter(
        x=weights,
        y=premiums,
        mode="lines+markers",
        name="Premium (%)",
        line=dict(color="blue"),
        hovertemplate="Weight: %{x}g<br>Premium: %{y:.2f}%<extra></extra>",
    )

    # Create a trace for text annotations.
    trace_text = go.Scatter(
        x=weights,
        y=[p + 0.5 for p in premiums],
        mode="text",
        text=[f"{p:.2f}%" for p in premiums],
        textposition="top center",
        showlegend=False,
    )

    # Create a trace to highlight the top 3 lowest premiums.
    trace_low = go.Scatter(
        x=weights_top,
        y=premiums_top,
        mode="markers",
        marker=dict(color="red", size=12),
        name="Lowest Premiums",
        hovertemplate="Weight: %{x}g<br>Premium: %{y:.2f}%<extra></extra>",
    )

    # Add today's date to the title.
    layout = go.Layout(
        title=f"Gold Bar Premiums from zlataky.sk ({time.strftime('%d.%m.%Y')})",
        xaxis=dict(
            title="Gold Bar Weight (g)",
            type="log",
            tickvals=weights,
            ticktext=[f"{w}g" for w in weights],
        ),
        yaxis=dict(title="Premium (%)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    fig = go.Figure(data=[trace_line, trace_text, trace_low], layout=layout)
    fig.show()


def main():
    # The urls are hardcoded because each gold bar has a slightly different url.
    gold_bars = {
        "1g": "https://zlataky.sk/1-g-argor-heraeus-sa-svajciarsko-investicna-zlata-tehlicka",
        "2g": "https://zlataky.sk/2-g-argor-heraeus-sa-svycarsko-investicni-zlaty-slitek",
        "5g": "https://zlataky.sk/5-g-argor-heraeus-sa-svajciarsko-investicna-zlata-tehlicka",
        "10g": "https://zlataky.sk/10-g-argor-heraeus-sa-svajciarsko-investicna-zlata-tehlicka",
        "20g": "https://zlataky.sk/20-g-argor-heraeus-sa-svycarsko-investicna-zlata-tehlicka",
        "31.1g": "https://zlataky.sk/31-1g-argor-heraeus-sa-svycarsko-investicni-zlaty-slitek",
        "50g": "https://zlataky.sk/50-g-argor-heraeus-sa-svycarsko-investicni-zlaty-slitek",
        "100g": "https://zlataky.sk/100-g-argor-heraeus-sa-svajciarsko-investicna-zlata-tehlicka",
        "250g": "https://zlataky.sk/250-g-argor-heraeus-sa-svajciarsko-investicna-zlata-tehlicka",
        "500g": "https://zlataky.sk/500-g-argor-heraeus-sa-svajciarsko-investicna-zlata-tehlicka",
        "1000g": "https://zlataky.sk/1000-g-argor-heraeus-sa-svajciarsko-investicna-zlata-tehlicka",
    }

    gold_spot_price = get_gold_spot_price()
    logger.info(f"Gold spot price: {gold_spot_price:.2f} EUR/g or {gold_spot_price * TROY_OUNCE:.2f} EUR/oz")
    prices_dict = asyncio.run(extract_all_prices(gold_bars))

    weights, premiums = [], []
    for weight, price in prices_dict.items():
        if price is None:
            continue
        gram_weight = float(re.search(r"[0-9]+\.?[0-9]*", weight).group())
        premium = ((price / gram_weight) / gold_spot_price - 1) * 100
        weights.append(gram_weight)
        premiums.append(premium)
        logger.info(f"{weight}: price = {price:.2f} EUR, premium = {premium:.2f}%")

    if not weights:
        logger.error("No valid prices extracted.")
        return

    sorted_indices = sorted(range(len(premiums)), key=lambda i: premiums[i])[:3]
    plot_graph(weights, premiums, sorted_indices)


if __name__ == "__main__":
    main()
