import csv
import glob
import os
from collections import defaultdict
from datetime import datetime

from tabulate import tabulate

# Define keywords for each category in a case-insensitive manner
CATEGORIES = {
    "groceries": [
        "lidl",
        "billa",
        "malina",
        "kraj",
        "terno",
        "stary otec",
        "ah",
        "albert hein",
        "jumbo",
        "spar",
        "tesco",
        "pekaren",
        "albert heijn",
    ],
    "eating out": [
        "roxor",
        "dunkin donuts",
        "chokiki",
        "poseidon",
        "mcdonald",
        "kfc",
        "restauracia" "kaviaren" "ramen",
        "bowlicheck",
        "soho",
        "starbucks",
        "subway",
        "koliba",
        "pho",
        "dominos",
        "kavoaren",
        "kantina",
        "bbq",
        "burger",
        "cafe",
        "wolt",
        "noodle",
        "fresh market",
        "noodle",
        "costa",
        "fruitisimo",
    ],
    "bolt": ["bolt", "taxi"],
    "car wash": ["mobydick", "pasadur"],
    "gas stations": ["orlen", "slovnaft", "omv", "shell"],
    "nabytok": ["ikea", "hornbach", "jysk", "mobelix", "decathlon", "bauhaus"],
    "travel": [
        "flixbus",
        "ryanair",
        "airbnb",
        "booking",
        "hotels.com",
    ],
}

# We may want a fixed order for categories in the final table
CATEGORY_ORDER = [
    "groceries",
    "eating out",
    "bolt",
    "car wash",
    "gas stations",
    "other",  # We'll add "other" explicitly
]


def categorize_merchant(merchant: str) -> str:
    """
    Categorize a single transaction based on the merchant description.
    Return one of the keys from CATEGORIES or 'other' if no match.
    """
    merchant_lower = merchant.lower()
    for category, keywords in CATEGORIES.items():
        for keyword in keywords:
            if keyword in merchant_lower:
                return category
    return "other"


def parse_expenses_from_csv(csv_file: str):
    col_transaction_type = 0
    col_amount_eur = 2
    col_date = 6
    col_merchant = 10

    expenses = []

    with open(csv_file, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        # Skip header row
        _ = next(reader, None)

        for row in reader:
            if len(row) < 11:
                continue

            # Skip "Kredit" (refund) rows
            transaction_type = row[col_transaction_type].strip()
            if transaction_type.lower() == "kredit":
                continue

            # Parse date (DD.MM.YYYY)
            date_str = row[col_date].strip()
            try:
                tx_date = datetime.strptime(date_str, "%d.%m.%Y")
            except ValueError:
                continue

            # Parse amount (Suma EUR)
            amount_str = row[col_amount_eur].strip()
            try:
                tx_amount = float(amount_str)
            except ValueError:
                continue

            merchant_str = row[col_merchant].strip()
            category = categorize_merchant(merchant_str)

            expenses.append(
                {
                    "date": tx_date,
                    "merchant": merchant_str,
                    "amount": tx_amount,
                    "category": category,
                }
            )

    return expenses


def main(input_directory="./expense_reports"):
    """
    1. Finds all CSV files in the directory.
    2. Aggregates expenses from all of them.
    3. Groups them by month and category.
    4. Prints out ONE large table where each row is a (month)
       and each column is a (category).
    """
    csv_files = glob.glob(os.path.join(input_directory, "*.csv"))
    all_expenses = []

    for csv_file in csv_files:
        file_expenses = parse_expenses_from_csv(csv_file)
        all_expenses.extend(file_expenses)

    monthly_sums = defaultdict(lambda: defaultdict(float))

    for tx in all_expenses:
        month = tx["date"].strftime("%Y-%m")  # e.g. '2024-05'
        monthly_sums[month][tx["category"]] += tx["amount"]

    all_months = sorted(monthly_sums.keys())

    headers = ["Month"] + CATEGORY_ORDER + ["Monthly Total"]

    table_data = []
    for month in all_months:
        row = [month]
        monthly_total = 0.0
        for cat in CATEGORY_ORDER:
            cat_total = monthly_sums[month].get(cat, 0.0)
            row.append(f"{cat_total:.2f}")
            monthly_total += cat_total
        row.append(f"{monthly_total:.2f}")
        table_data.append(row)

    averages = {}
    for cat in CATEGORY_ORDER:
        cat_total = sum(monthly_sums[month].get(cat, 0.0) for month in all_months)
        averages[cat] = cat_total / len(all_months)

    averages["Monthly Total"] = sum(float(row[-1]) for row in table_data) / len(all_months)

    table_data.append(
        ["Average"]
        + [f"{averages[cat]:.2f}" for cat in CATEGORY_ORDER]
        + [f"{averages['Monthly Total']:.2f}"]
    )

    print("\n=== Detailed Transactions ===\n")
    all_expenses_sorted = sorted(all_expenses, key=lambda x: x["date"])
    print(tabulate(all_expenses_sorted, headers="keys", tablefmt="pretty"))

    print("\n=== Monthly Expenses by Category ===\n")
    print(tabulate(table_data, headers=headers, tablefmt="pretty"))


if __name__ == "__main__":
    main()
