#!/usr/bin/env python3
"""
Scraper for Tatrabanka mortgage interest rates
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from tabulate import tabulate
import time
import csv
from datetime import date
import os
import plotly.graph_objects as go
import pandas as pd


def calculate_monthly_payment(principal, annual_rate, years):
    """
    Calculate monthly mortgage payment

    Args:
        principal: Loan amount in EUR
        annual_rate: Annual interest rate as percentage (e.g., 3.59 for 3.59%)
        years: Loan term in years

    Returns:
        Monthly payment in EUR
    """
    # Convert annual rate to monthly rate
    monthly_rate = (annual_rate / 100) / 12

    # Total number of payments
    n_payments = years * 12

    # Monthly payment formula: M = P * [r(1+r)^n] / [(1+r)^n - 1]
    if monthly_rate == 0:
        return principal / n_payments

    monthly_payment = principal * (monthly_rate * (1 + monthly_rate) ** n_payments) / \
                     ((1 + monthly_rate) ** n_payments - 1)

    return monthly_payment


def create_chart(csv_filename='tatrabanka_mortgage_rates.csv'):
    """
    Create interactive chart from mortgage rates CSV data

    Args:
        csv_filename: Path to CSV file with mortgage rates

    Returns:
        None (displays chart in browser)
    """
    if not os.path.exists(csv_filename):
        print(f"CSV file {csv_filename} not found!")
        return

    # Read CSV data
    df = pd.read_csv(csv_filename)

    # Convert date column to datetime and format as date only
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Create figure
    fig = go.Figure()

    # Add trace for each fixation period
    fixation_periods = sorted(df['fixation_period'].unique())

    for period in fixation_periods:
        period_data = df[df['fixation_period'] == period].sort_values('date')

        fig.add_trace(go.Scatter(
            x=period_data['date'],
            y=period_data['interest_rate'],
            name=f'{period} years',
            mode='lines+markers',
            line=dict(width=2),
            marker=dict(size=8)
        ))

    # Get today's rates for annotation (drop duplicates to show only unique fixation periods)
    today = date.today()
    today_data = df[df['date'] == today].drop_duplicates(subset=['fixation_period'])

    # Build annotation text with today's rates
    if not today_data.empty:
        annotation_text = f"<b>Today's Rates ({today})</b><br>"
        for _, row in today_data.sort_values('fixation_period').iterrows():
            annotation_text += f"{int(row['fixation_period'])}y: {row['interest_rate']:.2f}%<br>"
    else:
        annotation_text = "No data for today"

    # Update layout
    fig.update_layout(
        title='Tatrabanka Mortgage Interest Rates Over Time',
        xaxis_title='Date',
        yaxis_title='Interest Rate (%)',
        yaxis=dict(
            range=[0, 10],
            tickformat='.1f'
        ),
        hovermode='x unified',
        template='plotly_white',
        height=600,
        width=1000,
        legend=dict(
            title='Fixation Period',
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ),
        annotations=[
            dict(
                text=annotation_text,
                xref="paper",
                yref="paper",
                x=0.98,
                y=0.98,
                xanchor="right",
                yanchor="top",
                showarrow=False,
                bgcolor="rgba(255, 255, 255, 0.9)",
                bordercolor="black",
                borderwidth=1,
                borderpad=8,
                font=dict(size=11, family="monospace")
            )
        ]
    )

    # Show in browser only (no HTML file)
    print("\nOpening chart in browser...")
    fig.show()


def scrape_mortgage_rates(mortgage_amount=220750, loan_term_years=30):
    """Scrape mortgage interest rates from Tatrabanka website"""
    url = "https://www.tatrabanka.sk/sk/personal/uvery/hypoteka/"

    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get(url)

        # Wait for the page to load
        time.sleep(3)

        # Try to enable the "Program odmeňovania" checkbox if visible
        try:
            checkbox = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "inputHTBbenefit_program"))
            )
            if not checkbox.is_selected():
                driver.execute_script("arguments[0].click();", checkbox)
                time.sleep(1)
        except Exception:
            pass  # Checkbox might already be checked or not found

        table_data = []
        csv_data = []
        headers = [
            "Doba fixacie",
            "Urokova sadzba",
            f"Mesacna splatka ({mortgage_amount} EUR)"
        ]

        # All rates are already loaded on the page in the radio button labels
        # Find all radio button containers
        try:
            radio_labels = driver.find_elements(By.CSS_SELECTOR, 'label.radio-inline')

            for label in radio_labels:
                try:
                    # Extract the period and clean non-breaking spaces
                    period_elem = label.find_element(By.CSS_SELECTOR, 'span.year')
                    period_text = period_elem.text.strip().replace('\xa0', ' ').replace('\u00a0', ' ')

                    # Extract the rate and clean
                    rate_elem = label.find_element(By.CSS_SELECTOR, 'span.info.sadzba')
                    rate_text = rate_elem.text.strip().replace('\xa0', ' ').replace('\u00a0', ' ')

                    # Extract rate value for calculation
                    rate_value = float(rate_text.split()[0].replace(',', '.'))

                    # Calculate monthly payment for user's mortgage amount
                    monthly_payment = calculate_monthly_payment(
                        mortgage_amount,
                        rate_value,
                        loan_term_years
                    )
                    payment_text = f"{monthly_payment:.2f} EUR"

                    # Extract fixation period number
                    period_num = int(period_text.split()[0])

                    table_data.append([period_text, rate_text, payment_text])
                    csv_data.append({
                        'date': date.today().isoformat(),
                        'fixation_period': period_num,
                        'interest_rate': rate_value,
                        'monthly_payment': round(monthly_payment, 2)
                    })

                except Exception:
                    continue

        except Exception as e:
            print(f"Error extracting rates: {e}")

        # Sort by period
        if table_data:
            table_data.sort(key=lambda x: int(x[0].split()[0]))
            csv_data.sort(key=lambda x: x['fixation_period'])

        # If no rates found, show error
        if not table_data:
            print("Unable to extract mortgage rates from the website.")
            print("The page structure may have changed.")
            return

        # Save to CSV file
        csv_filename = 'tatrabanka_mortgage_rates.csv'
        file_exists = os.path.isfile(csv_filename)

        with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['date', 'fixation_period', 'interest_rate', 'monthly_payment']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header if file doesn't exist
            if not file_exists:
                writer.writeheader()

            # Write all rows
            for row in csv_data:
                writer.writerow(row)

        print(f"\nData saved to {csv_filename}")

        # Print the table using tabulate
        print("\nTatrabanka Mortgage Interest Rates")
        print("=" * 80)
        print(tabulate(table_data, headers=headers, tablefmt='grid'))
        print(f"\n* Rates shown with 'Program odmenovania' discount")
        print(f"* Mortgage amount: {mortgage_amount} EUR")
        print(f"* Loan term: {loan_term_years} years")
        print(f"* Date: {date.today().isoformat()}")
        print(f"Source: {url}")

        # Create chart from historical data
        create_chart(csv_filename)

    finally:
        driver.quit()


if __name__ == "__main__":
    scrape_mortgage_rates()
