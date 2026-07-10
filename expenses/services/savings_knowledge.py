"""Local knowledge base of savings & cost statistics.

This is a curated, static database of ~100 real-world cost facts (Slovakia / EU,
2025-2026, EUR) used by the recommendations engine. It has NO runtime dependency
on any LLM or external API — it's plain data that a deterministic algorithm reads.

Sources (researched 2026-07): Numbeo Slovakia cost-of-living & food-prices,
Eurostat household consumption 2024, public transit tariffs, typical published
subscription prices. Figures are approximate, single-person / typical-case, and
meant for ballpark guidance — not accounting.

Structure:
  FACTS: list of dicts, each a single comparable statistic:
    key        unique id
    label      human label
    category   which app category it informs (or None for general)
    value      euro amount
    unit       what `value` measures (e.g. 'per_month', 'per_item', 'per_liter')
    tags       keywords for matching merchants / items
    source     short provenance

  SWAP_RULES: high-level "spend less on X by doing Y" money-moves the engine can
    propose, each with the math needed to quantify the saving.

  SUBSCRIPTION_PRICES: typical monthly prices to spot & sanity-check subscriptions.

  BENCHMARKS_MONTHLY: typical single-person monthly spend per category (Slovakia),
    used to compare the user's spending against a norm.
"""

# ── Per-category typical monthly spend for one person in Slovakia (EUR) ──────
# Used to say "you spend X vs a typical Y". Deliberately conservative.
BENCHMARKS_MONTHLY = {
    "groceries":    {"value": 260, "label": "an average Slovak", "source": "Numbeo food basket"},
    "eating out":   {"value": 90,  "label": "an average Slovak", "source": "Eurostat restaurants share"},
    "gas stations": {"value": 120, "label": "a typical Slovak driver", "source": "Numbeo fuel + usage"},
    "bolt":         {"value": 30,  "label": "an average city commuter", "source": "Numbeo taxi"},
    "travel":       {"value": 110, "label": "an average European", "source": "Eurostat recreation/travel"},
    "parking":      {"value": 25,  "label": "a typical Slovak driver", "source": "Numbeo parking"},
    "car wash":     {"value": 12,  "label": "an average driver", "source": "typical wash cadence"},
}

# ── Atomic cost facts (the "hundred statistics") ─────────────────────────────
# value + unit let the engine do arithmetic (e.g. cooked meal vs restaurant meal).
FACTS = [
    # Groceries / cooking
    {"key": "restaurant_meal_cheap",  "label": "Meal at an inexpensive restaurant", "category": "eating out", "value": 8.0,  "unit": "per_meal", "tags": ["restaurant", "meal"], "source": "Numbeo SK"},
    {"key": "restaurant_meal_mid",    "label": "Mid-range 3-course meal for two",   "category": "eating out", "value": 44.0, "unit": "per_meal_2", "tags": ["restaurant"], "source": "Numbeo SK"},
    {"key": "fastfood_combo",         "label": "Fast-food combo meal",              "category": "eating out", "value": 8.0,  "unit": "per_meal", "tags": ["mcdonald", "kfc", "burger", "combo"], "source": "Numbeo SK"},
    {"key": "home_cooked_meal",       "label": "Home-cooked meal (ingredients)",    "category": "groceries",  "value": 2.5,  "unit": "per_meal", "tags": ["cook", "home"], "source": "derived from grocery basket"},
    {"key": "coffee_cafe",            "label": "Cappuccino in a cafe",              "category": "eating out", "value": 2.56, "unit": "per_item", "tags": ["coffee", "cappuccino", "cafe", "starbucks", "costa"], "source": "Numbeo SK"},
    {"key": "coffee_home",            "label": "Coffee made at home",               "category": "groceries",  "value": 0.30, "unit": "per_item", "tags": ["coffee", "home"], "source": "derived"},
    {"key": "coffee_machine",         "label": "Home espresso machine (one-off)",   "category": "groceries",  "value": 120.0,"unit": "one_off",  "tags": ["coffee", "machine"], "source": "typical entry model"},
    {"key": "lunch_out",              "label": "Bought lunch (workday)",            "category": "eating out", "value": 8.0,  "unit": "per_meal", "tags": ["lunch", "obed"], "source": "Numbeo SK"},
    {"key": "lunch_packed",           "label": "Packed lunch from home",            "category": "groceries",  "value": 2.8,  "unit": "per_meal", "tags": ["lunch", "packed"], "source": "derived"},
    {"key": "grocery_milk",           "label": "Milk (1L)",                         "category": "groceries",  "value": 0.96, "unit": "per_liter", "tags": ["milk"], "source": "Numbeo SK"},
    {"key": "grocery_bread",          "label": "Fresh bread (500g)",                "category": "groceries",  "value": 1.38, "unit": "per_item", "tags": ["bread", "pekaren"], "source": "Numbeo SK"},
    {"key": "grocery_eggs",           "label": "Eggs (12)",                         "category": "groceries",  "value": 3.45, "unit": "per_item", "tags": ["eggs"], "source": "Numbeo SK"},
    {"key": "grocery_chicken",        "label": "Chicken fillet (1kg)",              "category": "groceries",  "value": 6.75, "unit": "per_kg",   "tags": ["chicken"], "source": "Numbeo SK"},
    {"key": "grocery_cheese",         "label": "Local cheese (1kg)",                "category": "groceries",  "value": 12.37,"unit": "per_kg",   "tags": ["cheese"], "source": "Numbeo SK"},
    {"key": "grocery_water",          "label": "Bottled water (1.5L)",              "category": "groceries",  "value": 0.79, "unit": "per_item", "tags": ["water"], "source": "Numbeo SK"},
    {"key": "grocery_beer_shop",      "label": "Domestic beer at shop (0.5L)",      "category": "groceries",  "value": 1.0,  "unit": "per_item", "tags": ["beer", "pivo"], "source": "Numbeo SK"},
    {"key": "discount_grocer_saving", "label": "Discount grocer vs premium basket", "category": "groceries",  "value": 0.20, "unit": "fraction", "tags": ["lidl", "kaufland", "discount"], "source": "typical 15-25% basket gap"},

    # Eating out / drinks
    {"key": "beer_restaurant",        "label": "Beer in a restaurant (0.5L)",       "category": "eating out", "value": 2.0,  "unit": "per_item", "tags": ["beer", "pub", "bar"], "source": "Numbeo SK"},
    {"key": "delivery_fee",           "label": "Food-delivery fee + markup",        "category": "eating out", "value": 4.5,  "unit": "per_order","tags": ["wolt", "bolt food", "delivery"], "source": "typical delivery surcharge"},
    {"key": "soft_drink_out",         "label": "Soft drink eating out",             "category": "eating out", "value": 2.2,  "unit": "per_item", "tags": ["cola", "drink"], "source": "Numbeo SK"},

    # Transport
    {"key": "transit_single",         "label": "Single transit ticket",             "category": "bolt",       "value": 1.0,  "unit": "per_ride", "tags": ["ticket", "mhd", "bus", "tram"], "source": "SK transit tariff"},
    {"key": "transit_pass",           "label": "Monthly public-transport pass",     "category": "bolt",       "value": 30.0, "unit": "per_month","tags": ["pass", "predplatny"], "source": "Numbeo SK"},
    {"key": "taxi_per_km",            "label": "Taxi / Bolt per km",                "category": "bolt",       "value": 1.0,  "unit": "per_km",   "tags": ["taxi", "bolt"], "source": "Numbeo SK"},
    {"key": "fuel_liter",             "label": "Petrol (1L)",                       "category": "gas stations","value": 1.60, "unit": "per_liter","tags": ["fuel", "petrol", "benzin"], "source": "Numbeo SK"},
    {"key": "car_monthly_tco",        "label": "Car total cost of ownership /mo",   "category": "gas stations","value": 300.0,"unit": "per_month","tags": ["car"], "source": "fuel+insurance+depreciation est."},
    {"key": "bike_commute_saving",    "label": "Cycling vs driving (per km)",       "category": "gas stations","value": 0.20, "unit": "per_km",   "tags": ["bike", "cycle"], "source": "avoided fuel+wear"},
    {"key": "carpool_saving",         "label": "Carpool split (2 people)",          "category": "gas stations","value": 0.50, "unit": "fraction", "tags": ["carpool"], "source": "shared fuel"},

    # Subscriptions (also in SUBSCRIPTION_PRICES for the detector)
    {"key": "sub_gym",                "label": "Gym membership /mo",                "category": None,         "value": 39.83,"unit": "per_month","tags": ["gym", "fitness", "orangetheory"], "source": "Numbeo SK"},
    {"key": "sub_gym_budget",         "label": "Budget gym /mo",                    "category": None,         "value": 20.0, "unit": "per_month","tags": ["gym"], "source": "budget chains"},
    {"key": "cinema_ticket",          "label": "Cinema ticket",                     "category": None,         "value": 8.0,  "unit": "per_item", "tags": ["cinema", "kino"], "source": "Numbeo SK"},
    {"key": "mobile_plan",            "label": "Mobile plan /mo",                   "category": None,         "value": 22.43,"unit": "per_month","tags": ["mobile", "o2", "telekom", "orange"], "source": "Numbeo SK"},
    {"key": "internet_home",          "label": "Home internet /mo",                 "category": None,         "value": 17.54,"unit": "per_month","tags": ["internet", "wifi"], "source": "Numbeo SK"},

    # Household / utilities
    {"key": "utilities_flat",         "label": "Utilities, 85m² flat /mo",          "category": None,         "value": 231.0,"unit": "per_month","tags": ["utilities", "energy"], "source": "Numbeo SK"},
    {"key": "water_saving_tap",       "label": "Tap vs bottled water /mo",          "category": "groceries",  "value": 15.0, "unit": "per_month","tags": ["water"], "source": "derived"},

    # Personal / misc
    {"key": "haircut",                "label": "Haircut",                           "category": None,         "value": 12.0, "unit": "per_item", "tags": ["haircut", "barber", "kadernictvo"], "source": "typical SK"},
    {"key": "lottery_avg",            "label": "Lottery ticket",                    "category": None,         "value": 2.0,  "unit": "per_item", "tags": ["lottery", "tipos", "loto"], "source": "typical"},
    {"key": "atm_fee_foreign",        "label": "Foreign ATM/withdrawal fee",        "category": None,         "value": 3.0,  "unit": "per_item", "tags": ["atm", "vyber", "fee", "poplatok"], "source": "typical bank fee"},

    # Savings-behaviour facts (used in generic tips)
    {"key": "eating_out_target",      "label": "Recommended eating-out share of food budget", "category": "eating out", "value": 0.25, "unit": "fraction", "tags": [], "source": "common budgeting advice"},
    {"key": "savings_rate_target",    "label": "Recommended savings rate of income",          "category": None,         "value": 0.20, "unit": "fraction", "tags": [], "source": "50/30/20 rule"},
    {"key": "gold_hedge_note",        "label": "Gold as inflation hedge, not spending", "category": None,     "value": 0.0, "unit": "note",     "tags": ["gold"], "source": "asset, excluded from spend"},
]

# ── Typical monthly subscription prices (EUR) for the detector/sanity-check ──
SUBSCRIPTION_PRICES = {
    "netflix":        {"label": "Netflix", "typical": 13.0, "tags": ["netflix"]},
    "spotify":        {"label": "Spotify", "typical": 10.99, "tags": ["spotify"]},
    "youtube":        {"label": "YouTube Premium", "typical": 11.99, "tags": ["youtube"]},
    "disney":         {"label": "Disney+", "typical": 8.99, "tags": ["disney"]},
    "hbo":            {"label": "HBO Max", "typical": 8.99, "tags": ["hbo", "max"]},
    "amazon_prime":   {"label": "Amazon Prime", "typical": 8.99, "tags": ["prime", "amazon"]},
    "apple":          {"label": "Apple (iCloud/One/Music)", "typical": 9.99, "tags": ["apple", "icloud", "itunes"]},
    "google":         {"label": "Google One/Storage", "typical": 1.99, "tags": ["google"]},
    "chatgpt":        {"label": "ChatGPT Plus", "typical": 20.0, "tags": ["openai", "chatgpt"]},
    "microsoft":      {"label": "Microsoft 365", "typical": 7.0, "tags": ["microsoft", "office365"]},
    "gym":            {"label": "Gym membership", "typical": 39.83, "tags": ["gym", "fitness"]},
    "adobe":          {"label": "Adobe Creative Cloud", "typical": 24.19, "tags": ["adobe"]},
    "dropbox":        {"label": "Dropbox", "typical": 11.99, "tags": ["dropbox"]},
    "audible":        {"label": "Audible", "typical": 9.95, "tags": ["audible"]},
    "linkedin":       {"label": "LinkedIn Premium", "typical": 29.99, "tags": ["linkedin"]},
    "patreon":        {"label": "Patreon", "typical": 5.0, "tags": ["patreon"]},
    "nyt":            {"label": "News subscription", "typical": 8.0, "tags": ["times", "sme", "dennik"]},
}

# ── High-level money-moves the engine can quantify and propose ───────────────
# Each rule the engine evaluates against the user's actual spending. `math` names
# the calculation the engine implements; `template` is filled with computed values.
SWAP_RULES = [
    {
        "key": "cook_vs_eatout",
        "when_category": "eating out",
        "title": "Cook more meals at home",
        "math": "eatout_meals_to_home",   # meals implied by eating-out spend * (out - home)
        "template": "You spent €{spent:.0f} eating out (~{meals:.0f} meals). Cooking half of those at home could save about €{saving:.0f}/mo.",
        "assumption": "Restaurant meal ~€{meal_cost:.0f} vs ~€{home_cost:.1f} cooked at home.",
    },
    {
        "key": "coffee_home",
        "when_category": "eating out",
        "title": "Brew coffee at home",
        "math": "coffee_swap",
        "template": "If ~{cups:.0f} of your cafe coffees were made at home, you'd save about €{saving:.0f}/mo.",
        "assumption": "Cafe cappuccino ~€{cafe:.2f} vs ~€{home:.2f} at home.",
    },
    {
        "key": "discount_grocer",
        "when_category": "groceries",
        "title": "Shop more at discount grocers",
        "math": "grocery_discount",
        "template": "Moving ~30% of groceries to a discount grocer (Lidl/Kaufland) could trim about €{saving:.0f}/mo.",
        "assumption": "Discount baskets run ~15-25% cheaper than premium stores.",
    },
    {
        "key": "delivery_pickup",
        "when_category": "eating out",
        "title": "Skip delivery, pick up instead",
        "math": "delivery_fees",
        "template": "Avoiding delivery fees/markups on your ~{orders:.0f} likely delivery orders could save about €{saving:.0f}/mo.",
        "assumption": "Delivery adds ~€{fee:.1f} per order in fees + markup.",
    },
    {
        "key": "transit_vs_taxi",
        "when_category": "bolt",
        "title": "Swap some taxis for transit",
        "math": "taxi_to_transit",
        "template": "Replacing half your ~{rides:.0f} taxi rides with transit could save about €{saving:.0f}/mo — a monthly pass is only €{pass_cost:.0f}.",
        "assumption": "Taxi rides average several € vs €{ticket:.0f} a transit ticket.",
    },
]

# ── Actionable lifestyle tips per category ──────────────────────────────────
# When the engine flags a category (over benchmark, trending up, or budget-heavy),
# it pairs the observation with ONE concrete thing the user can DO. Each tip has an
# estimated `save_frac` (fraction of that category's spend it realistically trims)
# and a `source` for the claim. `titles` are short, catchy hooks. Keyed by the
# app's category names; `_default` covers custom/unknown categories generically.
CATEGORY_TIPS = {
    "gas stations": [
        {"title": "Fill up at cheaper stations", "action": "Fuel at supermarket/discount stations (e.g. by price apps) instead of motorway pumps — often 8–12c/L cheaper.", "save_frac": 0.08, "source": "SK fuel price spread, Numbeo"},
        {"title": "Drive in eco mode", "action": "Smoother acceleration + eco mode cuts fuel use roughly 10–15%.", "save_frac": 0.12, "source": "US DOE fueleconomy.gov driving-behaviour study"},
        {"title": "Keep tyres inflated", "action": "Correct tyre pressure improves mileage ~3% — check monthly.", "save_frac": 0.03, "source": "US DOE fueleconomy.gov"},
    ],
    "eating out": [
        {"title": "Cook a few dinners in", "action": "Swapping 2–3 restaurant meals a week for home cooking is the biggest lever here.", "save_frac": 0.30, "source": "meal-cost delta, Numbeo SK"},
        {"title": "Set a weekly dining budget", "action": "Cap eating-out to a set number of meals per week and track against it.", "save_frac": 0.15, "source": "common budgeting practice"},
    ],
    "groceries": [
        {"title": "Shop at discount grocers", "action": "Move ~30% of your basket to Lidl/Kaufland — discount baskets run 15–25% cheaper.", "save_frac": 0.08, "source": "SK basket price gap"},
        {"title": "Plan meals, cut waste", "action": "A weekly meal plan + list avoids impulse buys and the ~20% of food households throw away.", "save_frac": 0.10, "source": "FAO/EU food-waste estimates"},
    ],
    "bolt": [
        {"title": "Take transit off-peak", "action": "Use a monthly pass for routine trips and save taxis for when you truly need them.", "save_frac": 0.4, "source": "transit vs taxi fare, Numbeo SK"},
        {"title": "Walk or bike short hops", "action": "Rides under ~2 km are often faster on foot/bike and free.", "save_frac": 0.15, "source": "urban trip-distance data"},
    ],
    "parking": [
        {"title": "Use a monthly parking plan", "action": "If you park in the same zone daily, a monthly/resident permit beats per-hour rates.", "save_frac": 0.3, "source": "SK parking tariff structure"},
        {"title": "Park a little further out", "action": "Zones one ring out are often far cheaper for a short walk.", "save_frac": 0.2, "source": "city zonal pricing"},
    ],
    "car wash": [
        {"title": "Wash less often / DIY", "action": "Stretch to every 2–3 weeks or use a self-service bay instead of a full valet.", "save_frac": 0.4, "source": "self-serve vs full-wash pricing"},
    ],
    "travel": [
        {"title": "Book earlier, travel light", "action": "Booking flights/stays 4–6 weeks ahead and skipping checked bags trims a big chunk.", "save_frac": 0.2, "source": "airline fare-timing studies"},
        {"title": "Travel in shoulder season", "action": "Shifting trips off peak dates cuts flight + accommodation cost materially.", "save_frac": 0.2, "source": "seasonal pricing data"},
    ],
    "_default": [
        {"title": "Set a monthly cap", "action": "Give this category a target and check in on it each month — awareness alone curbs overspend.", "save_frac": 0.12, "source": "budgeting research (mental accounting)"},
        {"title": "Review recent purchases", "action": "Open this category and cancel or return anything you didn't really need.", "save_frac": 0.1, "source": "general spending review"},
    ],
}


def tips_for_category(cat):
    """Return the list of actionable tips for a category (falls back to generic)."""
    key = str(cat).strip().lower()
    return CATEGORY_TIPS.get(key) or CATEGORY_TIPS["_default"]
