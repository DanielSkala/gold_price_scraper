"""Generate synthetic TatraBanka-format expense exports for the demo / onboarding.

Produces a few CSVs under `sample_reports/` (committed to the repo) that mimic the
real TatraBanka "Export pohybov" schema, so a fresh clone can show the app working
without the user's private data. Deterministic (seeded) so the sample is stable.

Run:  python scripts/generate_sample_reports.py
"""
import csv
import os
import random
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, '..', 'sample_reports')

# TatraBanka export header (matches the real files exactly).
HEADER = [
    "Typ transakcie", "Originálna suma", "Suma EUR", "Suma v USD", "Kurz",
    "Stav transakcie", "Dátum transakcie", "Dátum spracovania", "Číslo karty",
    "Popis", "Názov obchodníka",
]

# Merchant pools per spending theme → these hit the app's auto-categoriser and the
# category-discovery keywords, so the demo shows groceries, eating out, fuel, plus
# "other" merchants that discovery can surface (pharmacy, clothing, electronics…).
MERCHANTS = {
    "groceries":    [("LIDL 042 BRATISLAVA", 8, 55), ("Kaufland SK 1230", 10, 70),
                     ("BILLA 4021", 5, 40), ("TESCO HYPERMARKET", 12, 65),
                     ("Potraviny Malina", 3, 25)],
    "eating out":   [("MCDONALDS SK", 6, 15), ("KFC Eurovea", 7, 18),
                     ("Restauracia Koliba", 14, 45), ("STARBUCKS", 3, 8),
                     ("Wolt", 9, 28), ("Pizzeria Bella", 10, 30)],
    "gas stations": [("SLOVNAFT AS 55", 30, 75), ("OMV BRATISLAVA", 35, 80),
                     ("SHELL 8257", 28, 70)],
    "bolt":         [("BOLT.EU/O/2504", 4, 14), ("TAXI SLOVAKIA", 6, 18)],
    "travel":       [("RYANAIR", 40, 120), ("BOOKING.COM", 60, 180), ("FLIXBUS", 12, 45)],
    "parking":      [("PARKDOTS EUR", 1, 4), ("EASYPARK", 2, 6)],
    # "other" merchants — deliberately un-categorised so discovery can suggest them.
    "pharmacy":     [("Dr.Max Lekaren", 5, 30), ("BENU LEKAREN 21", 4, 25)],
    "clothing":     [("ZARA AUPARK", 20, 90), ("H&M BRATISLAVA", 15, 60), ("CCC OBUV", 25, 70)],
    "electronics":  [("ALZA.SK", 15, 200), ("NAY ELEKTRODOM", 20, 150)],
    "subscriptions":[("GOOGLE*YOUTUBEPREMIUM", 12, 14), ("NETFLIX.COM", 11, 13),
                     ("SPOTIFY", 10, 11)],
}

# Rough monthly frequency (how many txns/month) per theme — shapes a believable mix.
FREQ = {
    "groceries": 12, "eating out": 9, "gas stations": 4, "bolt": 5, "travel": 1,
    "parking": 4, "pharmacy": 1, "clothing": 1, "electronics": 1, "subscriptions": 3,
}


def _fmt_amount(v):
    return f"{v:.2f}"


def _row(d, merchant, amount):
    ds = d.strftime("%d.%m.%Y")
    return [
        "Debet", f"{amount:.2f} EUR", _fmt_amount(amount), "", "",
        "Zúčtovaná", ds, ds, "433703******0000",
        "EUR AP NÁKUP POS", merchant,
    ]


def _month_rows(rng, y, m):
    rows = []
    days_in_month = (date(y + (m == 12), (m % 12) + 1, 1) - timedelta(days=1)).day
    for theme, n in FREQ.items():
        # vary count a little month to month
        count = max(0, int(rng.gauss(n, n * 0.3)))
        pool = MERCHANTS[theme]
        for _ in range(count):
            merchant, lo, hi = rng.choice(pool)
            amount = round(rng.uniform(lo, hi), 2)
            day = rng.randint(1, days_in_month)
            rows.append((date(y, m, day), merchant, amount))
    rows.sort(key=lambda r: r[0])
    return rows


def generate(months_per_file=2, num_files=3, start=(2025, 1)):
    os.makedirs(OUT_DIR, exist_ok=True)
    rng = random.Random(42)   # deterministic sample
    y, m = start
    for i in range(num_files):
        path = os.path.join(OUT_DIR, f"sample_export_{i + 1}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(HEADER)
            for _ in range(months_per_file):
                for d, merchant, amount in _month_rows(rng, y, m):
                    w.writerow(_row(d, merchant, amount))
                m += 1
                if m > 12:
                    m = 1; y += 1
        print(f"wrote {path}")


if __name__ == "__main__":
    generate()
