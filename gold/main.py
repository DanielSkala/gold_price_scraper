import asyncio
import csv
import datetime
import logging
import re
import time

import aiohttp
import plotly.graph_objects as go
import yfinance as yf
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TROY_OUNCE = 31.1034768  # To grams


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
        return None


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


def calculate_average_premiums(csv_file) -> list:
    def to_float(v):
        try:
            return float(v.strip())
        except ValueError:
            return None

    with open(csv_file, newline="") as f:
        # Read each row, and drop the last column (date)
        rows = [[to_float(cell) for cell in row[:-1]] for row in csv.reader(f)]

    # Transpose rows to columns and compute averages ignoring None values.
    return [
        sum(vals) / len(vals) if (vals := [x for x in col if x is not None]) else None
        for col in zip(*rows)
    ]


def main():
    # Define the weights based on the gold_bars dictionary in main()
    weights = [1, 2, 5, 10, 20, 31.1, 50, 100, 250, 500, 1000]

    # Get current prices
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
    current_premiums = []

    if gold_spot_price is not None:
        prices_dict = asyncio.run(extract_all_prices(gold_bars))

        for weight in sorted(
            gold_bars.keys(), key=lambda w: float(re.search(r"[0-9]+\.?[0-9]*", w).group())
        ):
            price = prices_dict[weight]
            gram_weight = float(re.search(r"[0-9]+\.?[0-9]*", weight).group())
            if price is None:
                premium = None
            else:
                premium = ((price / gram_weight) / gold_spot_price - 1) * 100
            current_premiums.append(premium)
    else:
        current_premiums = [None] * len(weights)

    # Read the CSV file
    with open("gold_premiums.csv", newline="") as f:
        reader = csv.reader(f)
        rows = []
        dates = []
        for row in reader:
            # Convert premium values to float, handling "N/A" values
            premiums = []
            for cell in row[:-1]:  # Exclude the date column
                try:
                    premiums.append(float(cell.strip()))
                except ValueError:
                    premiums.append(None)  # Use None for "N/A" values
            rows.append(premiums)
            dates.append(row[-1])  # Store the date

    # Calculate average premiums
    avg_premiums = calculate_average_premiums("gold_premiums.csv")

    # Create a figure
    fig = go.Figure()

    # Add a trace for each row (date) with 50% transparency
    for i, (premiums, date) in enumerate(zip(rows, dates)):
        fig.add_trace(
            go.Scatter(
                x=weights,
                y=premiums,
                mode="lines+markers",
                name=date,
                line=dict(width=1),
                opacity=0.15,
                connectgaps=True,  # Connect gaps where data is missing
                hovertemplate="Weight: %{x}<br>Premium: %{y:.2f}%<br>Date: "
                + date
                + "<extra></extra>",
                showlegend=False,  # Hide from legend
            )
        )

    # Add the average premium line with full visibility and make it stand out
    fig.add_trace(
        go.Scatter(
            x=weights,
            y=avg_premiums,
            mode="lines+markers",
            name="Average Premium (%)",
            line=dict(color="red", width=2),
            marker=dict(size=8),
            connectgaps=True,
            hovertemplate="Weight: %{x}<br>Average Premium: %{y:.2f}%<extra></extra>",
            showlegend=True,  # Show in legend
        )
    )

    # Add the current premium line
    fig.add_trace(
        go.Scatter(
            x=weights,
            y=current_premiums,
            mode="lines+markers",
            name="Current Premium (%)",
            line=dict(color="blue", width=3),
            marker=dict(size=8),
            connectgaps=True,
            hovertemplate="Weight: %{x}<br>Current Premium: %{y:.2f}%<extra></extra>",
            showlegend=True,  # Show in legend
        )
    )

    # Add the top three lowest premiums on current premium line
    lowest_premiums = sorted(
        [
            (premium, weight)
            for premium, weight in zip(current_premiums, weights)
            if premium is not None
        ]
    )[:3]

    for premium, weight in lowest_premiums:
        fig.add_trace(
            go.Scatter(
                x=[weight],
                y=[premium],
                mode="markers",
                name=f"Lowest Premium ({weight}g)",
                marker=dict(color="#EFBF04", size=10),
                hovertemplate=f"Weight: {weight}g<br>Premium: {premium:.2f}%<extra></extra>",
            )
        )

    # Set up the layout
    fig.update_layout(
        title=f"All Gold Bar Premiums from zlataky.sk (as of {time.strftime('%d.%m.%Y')})",
        xaxis=dict(
            title="Gold Bar Weight (g)",
            type="log",
            tickvals=weights,
            ticktext=[f"{w}g" for w in weights],
        ),
        yaxis=dict(
            title="Premium (%)",
            dtick=2.5,  # Set tick step to 2.5 for more dense y-axis
        ),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
        ),  # Enable legend
    )

    # Show the figure
    fig.show()

    # Open a csv file and append there the premiums (eg. 3.5) for each weight. If no premium is available, write "N/A".
    with open("gold_premiums.csv", "a") as f:
        # Use a conditional expression to write "N/A" if premium is None
        line = ", ".join(
            [f"{premium:.2f}" if premium is not None else "N/A" for premium in current_premiums]
        )
        # Append today's date as a string
        line += ", " + str(datetime.date.today()) + "\n"
        f.write(line)


if __name__ == "__main__":
    main()
