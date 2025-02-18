import csv
from typing import List, Tuple

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np


def simulate_investment_growth(
    monthly_investment: float, annual_interest_rate: float, total_years: int
) -> List[Tuple[int, float]]:
    """
    Simulate the growth of an investment with constant monthly contributions.

    Args:
        monthly_investment (float): The monthly investment amount in EUR.
        annual_interest_rate (float): The annual interest rate (e.g., 0.1 for 10%).
        total_years (int): The total investment duration in years.

    Returns:
        List[Tuple[int, float]]: A list of tuples where each tuple contains the year and the balance at
                                 the end of that year.
    """
    monthly_interest_rate = (1 + annual_interest_rate) ** (1 / 12) - 1
    balance = 0.0
    results = []
    total_months = total_years * 12

    for month in range(1, total_months + 1):
        balance = balance * (1 + monthly_interest_rate) + monthly_investment

        if month % 12 == 0:
            year = month // 12
            results.append((year, balance))

    return results


def simulate_investment_growth_with_raise(
    base_monthly_investment: float,
    annual_interest_rate: float,
    total_years: int,
    inflation_rate: float = 0.02,
    additional_raise: float = 0.05,
) -> List[Tuple[int, float]]:
    """
    Simulate the growth of an investment with monthly contributions that increase annually.

    The monthly contribution increases at the beginning of each new year (starting from year 2)
    by the combined rate of inflation and an additional raise. For example, with a base monthly
    investment of €3,000, an inflation rate of 2% and an additional raise of 5%, the monthly
    contribution will increase to €3,000 * 1.07 in the second year.

    Args:
        base_monthly_investment (float): The starting monthly investment amount in EUR.
        annual_interest_rate (float): The annual interest rate (e.g., 0.1 for 10%).
        total_years (int): The total investment duration in years.
        inflation_rate (float): The inflation rate per year (default is 0.02 for 2%).
        additional_raise (float): The additional raise percentage per year on top of inflation
                                  (default is 0.05 for 5%).

    Returns:
        List[Tuple[int, float]]: A list of tuples where each tuple contains the year and the balance
                                 at the end of that year.
    """
    monthly_interest_rate = (1 + annual_interest_rate) ** (1 / 12) - 1
    balance = 0.0
    results = []
    total_months = total_years * 12

    # Start with the base monthly investment
    current_monthly_investment = base_monthly_investment

    for month in range(1, total_months + 1):
        # Increase the monthly investment at the beginning of each new year (starting in month 13)
        if month > 1 and month % 12 == 1:
            current_monthly_investment *= 1 + inflation_rate + additional_raise

        balance = balance * (1 + monthly_interest_rate) + current_monthly_investment

        if month % 12 == 0:
            year = month // 12
            results.append((year, balance))

    return results


def write_results_to_csv(filename: str, results: List[Tuple[int, float]]) -> None:
    """
    Write the simulation results to a CSV file.

    Args:
        filename (str): The name of the output CSV file.
        results (List[Tuple[int, float]]): The simulation results as a list of (year, balance) tuples.
    """
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Year", "Balance (EUR)"])
        for year, balance in results:
            writer.writerow([year, round(balance, 2)])
    print(f"CSV file '{filename}' created successfully.")


def plot_investment_growth(
    results: List[Tuple[int, float]], initial_investment: float, total_years: int
) -> None:
    """
    Plot the investment growth over time.

    Args:
        results (List[Tuple[int, float]]): The simulation results as a list of (year, balance) tuples.
        initial_investment (float): The starting monthly investment amount, used for the legend label.
        total_years (int): The total number of years for setting the X-axis ticks.
    """
    years = [year for year, _ in results]
    balances = [balance for _, balance in results]

    plt.figure(figsize=(12, 6))
    plt.plot(
        years,
        balances,
        marker="o",
        linestyle="-",
        color="b",
        label=f"Investment Growth (Starting at €{initial_investment:,.0f}/month)",
    )

    plt.xlabel("Years")
    plt.ylabel("Total Balance (€)")
    plt.title("Projected Investment Growth in S&P 500")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.7)

    max_balance = max(balances)
    # Set Y-axis ticks every 100,000 EUR
    y_ticks = np.arange(0, max_balance + 100_000, 100_000)
    plt.yticks(y_ticks)

    plt.xticks(range(0, total_years + 1, 5))
    plt.gca().yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"€{x:,.0f}"))

    plt.show()


def main() -> None:
    # Base parameters
    base_monthly_investment = 3000
    annual_interest_rate = 0.1  # Historical average return of S&P 500 is 11.2%, let's use 10%
    total_years = 15

    # Parameters for annual raise:
    inflation_rate = 0.02  # Default inflation rate of 2%
    additional_raise = 0.05  # Additional raise of 5% on top of inflation (i.e. 7% total per year)

    # Simulate investment growth with increasing monthly contributions.
    results = simulate_investment_growth_with_raise(
        base_monthly_investment,
        annual_interest_rate,
        total_years,
        inflation_rate,
        additional_raise,
    )

    output_filename = "./sp500_investment_growth_with_raise.csv"
    write_results_to_csv(output_filename, results)
    plot_investment_growth(results, base_monthly_investment, total_years)


if __name__ == "__main__":
    main()
