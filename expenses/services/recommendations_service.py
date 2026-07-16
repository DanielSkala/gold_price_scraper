"""Deterministic savings recommendations — model-agnostic, no LLM, no API key.

Given the user's recent spending and their monthly budget goal, this produces
concrete, quantified recommendations by running a fixed set of analysers over the
data and the local knowledge base (`savings_knowledge.py`). Every euro figure is
computed from the user's real numbers; the knowledge base supplies the reference
costs (restaurant meal price, discount-grocer gap, transit fares, etc.).

Design goals:
  - Fully local & deterministic. Same input → same output. No network at runtime.
  - Actionable & quantified: each rec says what to do and roughly how much it saves.
  - Goal-aware: highlights the smallest set of changes that would land within goal.

Public entry point: `build_recommendations(analysis_input)`.
"""
from collections import defaultdict

from services import savings_knowledge as kb


def _fact(key):
    for f in kb.FACTS:
        if f["key"] == key:
            return f
    return None


def _fact_value(key, default=0.0):
    f = _fact(key)
    return f["value"] if f else default


def _recent_months(monthly_sums, n=3):
    months = sorted(monthly_sums.keys())
    return months[-n:] if months else []


def _avg_by_category(monthly_sums, months):
    """Average monthly spend per category over the given months."""
    if not months:
        return {}
    sums = defaultdict(float)
    for m in months:
        for cat, val in monthly_sums.get(m, {}).items():
            sums[cat] += val
    return {cat: total / len(months) for cat, total in sums.items()}


# ── Individual analysers. Each returns a list of recommendation dicts. ───────
# A recommendation:
#   {id, title, detail, saving (€/mo, may be 0), category, severity, kind, assumption?}

def _rec(id, title, detail, saving, category, kind, severity="tip", assumption=None):
    r = {
        "id": id, "title": title, "detail": detail,
        "saving": round(saving, 2), "category": category,
        "kind": kind, "severity": severity,
    }
    if assumption:
        r["assumption"] = assumption
    return r


def _analyse_eating_out(avg_cat, txs_recent, n_months=3):
    recs = []
    spent = avg_cat.get("eating out", 0.0)
    if spent <= 0:
        return recs
    n_months = max(n_months, 1)

    meal_out = _fact_value("restaurant_meal_cheap", 8.0)
    meal_home = _fact_value("home_cooked_meal", 2.5)
    # Estimate meals from spend (rough — some are coffees/drinks, so this is a ceiling).
    meals = spent / meal_out
    # Propose cooking HALF of them at home.
    saving = (meals * 0.5) * (meal_out - meal_home)
    recs.append(_rec(
        "cook_vs_eatout",
        "Cook more meals at home",
        f"You spend about €{spent:.0f}/mo eating out (~{meals:.0f} meals). "
        f"Cooking half of them at home could save roughly €{saving:.0f}/mo.",
        saving, "eating out", "swap", "opportunity",
        assumption=f"Restaurant meal ~€{meal_out:.0f} vs ~€{meal_home:.1f} cooked at home.",
    ))

    # Coffee sub-analysis: count likely coffee purchases in recent eating-out txs.
    cafe = _fact_value("coffee_cafe", 2.56)
    home = _fact_value("coffee_home", 0.30)
    coffee_words = ("coffee", "cappuccino", "cafe", "kava", "starbucks", "costa", "espresso")
    coffee_txs = [t for t in txs_recent
                  if t["category"] == "eating out"
                  and any(w in t["merchant"].lower() for w in coffee_words)]
    if coffee_txs:
        # per-month count over the analysed window
        cups = len(coffee_txs) / n_months
        saving_c = cups * (cafe - home)
        if saving_c >= 3:
            recs.append(_rec(
                "coffee_home",
                "Brew coffee at home",
                f"~{cups:.0f} cafe coffees a month showed up recently. Making them at home could save "
                f"about €{saving_c:.0f}/mo — a home machine (~€120) pays for itself in a few months.",
                saving_c, "eating out", "swap",
                assumption=f"Cafe cappuccino ~€{cafe:.2f} vs ~€{home:.2f} at home.",
            ))

    # Delivery fees.
    fee = _fact_value("delivery_fee", 4.5)
    delivery_words = ("wolt", "bolt food", "delivery", "donaska", "foodora")
    del_txs = [t for t in txs_recent
               if any(w in t["merchant"].lower() for w in delivery_words)]
    if del_txs:
        orders = len(del_txs) / n_months
        saving_d = orders * fee
        recs.append(_rec(
            "delivery_pickup",
            "Skip delivery, pick up instead",
            f"~{orders:.0f} likely delivery orders a month recently. Picking up instead could save "
            f"about €{saving_d:.0f}/mo in fees and markups.",
            saving_d, "eating out", "swap",
            assumption=f"Delivery adds ~€{fee:.1f} per order.",
        ))
    return recs


def _analyse_groceries(avg_cat):
    recs = []
    spent = avg_cat.get("groceries", 0.0)
    if spent <= 0:
        return recs
    frac = _fact_value("discount_grocer_saving", 0.20)
    # Assume 30% of the basket could move to a discounter at `frac` less.
    saving = spent * 0.30 * frac
    if saving >= 3:
        recs.append(_rec(
            "discount_grocer",
            "Shop more at discount grocers",
            f"Groceries run about €{spent:.0f}/mo. Moving ~30% of it to a discount grocer "
            f"(Lidl/Kaufland) could trim roughly €{saving:.0f}/mo.",
            saving, "groceries", "swap",
            assumption="Discount baskets run ~15-25% cheaper than premium stores.",
        ))
    return recs


def _analyse_transport(avg_cat, txs_recent):
    recs = []
    taxi = avg_cat.get("bolt", 0.0)
    if taxi > 0:
        pass_cost = _fact_value("transit_pass", 30.0)
        # Replace half of taxi spend with transit; saving is the difference vs a pass share.
        saving = max(0.0, taxi * 0.5 - pass_cost * 0.5)
        rides = len([t for t in txs_recent if t["category"] == "bolt"])
        if taxi * 0.5 > pass_cost * 0.5 and saving >= 3:
            recs.append(_rec(
                "transit_vs_taxi",
                "Swap some taxis for transit",
                f"You spend about €{taxi:.0f}/mo on taxis (~{rides} rides). Replacing half with "
                f"public transport could save roughly €{saving:.0f}/mo — a monthly pass is only €{pass_cost:.0f}.",
                saving, "bolt", "swap",
                assumption=f"A transit ticket is ~€{_fact_value('transit_single', 1.0):.0f}.",
            ))
    return recs


def _pretty(cat):
    """Human-readable category label from any raw key/name."""
    return str(cat).replace('_', ' ').replace('-', ' ').strip()


def _analyse_category_trends(monthly_sums, recent_months, avg_cat):
    """GENERIC, data-driven analyser — works for ANY category (built-in, custom, or
    ones that don't exist yet) with NO hardcoded names and NO external benchmark.

    For each category it compares the recent window's average to the user's OWN
    prior baseline (all earlier months). Categories spending materially more than
    their historical norm are flagged, with the excess as the saving lever. This is
    what keeps recommendations meaningful as spending changes over years and as new
    categories appear — the baseline rolls forward automatically.
    """
    recs = []
    all_months = sorted(monthly_sums.keys())
    if len(all_months) < 4:
        return recs  # need enough history for a stable baseline

    recent_set = set(recent_months)
    baseline_months = [m for m in all_months if m not in recent_set]
    if not baseline_months:
        return recs

    # Every category the user actually has (union across all months) — fully dynamic.
    categories = set()
    for m in all_months:
        categories.update(monthly_sums[m].keys())

    for cat in categories:
        if cat == 'other':
            continue  # 'other' isn't a single actionable thing (use category discovery instead)
        recent_avg = avg_cat.get(cat, 0.0)
        base_avg = sum(monthly_sums[m].get(cat, 0.0) for m in baseline_months) / len(baseline_months)
        if recent_avg < 25 or base_avg <= 0:
            continue  # ignore tiny or brand-new categories (no baseline to compare)
        growth = (recent_avg - base_avg) / base_avg
        excess = recent_avg - base_avg
        # Flag a meaningful, sustained increase over the user's own norm.
        if growth >= 0.25 and excess >= 15:
            label = _pretty(cat)
            tip, tip_saving = _best_tip(cat, recent_avg)
            # Saving = the reachable slice of the increase, but at least the tip's estimate.
            saving = max(excess * 0.5, tip_saving * 0.6)
            recs.append(_rec(
                f"trend_{cat}",
                tip["title"],
                f"{tip['action']} Your {label} has climbed to ~€{recent_avg:.0f}/mo — "
                f"{growth * 100:.0f}% above your usual €{base_avg:.0f}/mo.",
                saving,
                cat, "trend",
                severity="watch" if growth < 0.6 else "alert",
                assumption=f"Est. ~{round(tip['save_frac']*100)}% of spend · {tip['source']}. Trend vs your own {len(baseline_months)}-month average.",
            ))
    return recs


def _analyse_top_categories(avg_cat, goal, existing_ids):
    """Also generic: if over goal and no category-specific advice fired for the
    biggest categories, suggest trimming whichever categories dominate the budget.
    Guarantees useful output for ANY spending profile."""
    recs = []
    if goal <= 0:
        return recs
    total = sum(avg_cat.values())
    if total <= goal:
        return recs
    # Biggest categories first; skip 'other' (not directly actionable) and any that
    # already have a specific rec.
    ranked = sorted(
        ((c, v) for c, v in avg_cat.items() if v > 0 and c != 'other'),
        key=lambda kv: -kv[1],
    )
    for cat, spent in ranked[:2]:
        share = spent / total if total else 0
        if share < 0.15:
            continue
        if any(cid.endswith(cat) or cid == f"trend_{cat}" for cid in existing_ids):
            continue
        label = _pretty(cat)
        tip, saving = _best_tip(cat, spent)
        recs.append(_rec(
            f"top_{cat}",
            tip["title"],
            f"{tip['action']} {label.capitalize()} is one of your biggest categories at "
            f"~€{spent:.0f}/mo ({share * 100:.0f}% of spending).",
            saving,
            cat, "top", severity="tip",
            assumption=f"Est. saving ~{round(tip['save_frac']*100)}% of spend · {tip['source']}.",
        ))
    return recs


def _best_tip(cat, spent):
    """Pick the highest-impact actionable tip for a category, and quantify its
    saving from the tip's save_frac applied to the user's actual spend."""
    tips = kb.tips_for_category(cat)
    best = max(tips, key=lambda t: t.get("save_frac", 0))
    saving = spent * best.get("save_frac", 0.1)
    return best, saving


def _analyse_vs_benchmarks(avg_cat):
    """Flag categories above a typical single-person norm — and, crucially, tell the
    user WHAT TO DO about it (a concrete lifestyle change), quantified from their own
    spend. Only fires for categories that HAVE a benchmark; the generic self-baseline
    analyser covers everything else, so new categories are never ignored."""
    recs = []
    for cat, bench in kb.BENCHMARKS_MONTHLY.items():
        spent = avg_cat.get(cat, 0.0)
        ref = bench["value"]
        if ref <= 0 or spent <= 0:
            continue
        ratio = spent / ref
        if ratio >= 1.5:
            over = spent - ref
            tip, saving = _best_tip(cat, spent)
            recs.append(_rec(
                f"benchmark_{cat.replace(' ', '_')}",
                tip["title"],   # the ACTION is the headline, not the observation
                f"{tip['action']} Your {cat} runs ~€{spent:.0f}/mo — {ratio:.1f}× what "
                f"{bench['label']} spends (~€{ref:.0f}/mo), so there's room to trim.",
                saving,
                cat, "benchmark",
                severity="watch" if ratio < 2.2 else "alert",
                assumption=f"Est. saving ~{round(tip['save_frac']*100)}% of spend · {tip['source']}. Benchmark: {bench['source']}.",
            ))
    return recs


def _analyse_subscriptions(recurring):
    """Turn detected recurring charges into savings recommendations.

    `recurring` is the output of recurring_service.detect_recurring(): real cadence
    detection over the actual charge history, not name matching. Two flavours:
      - A price-hike alert when a subscription's amount has crept up materially.
      - A zombie/audit tip listing active recurring charges worth reviewing.
    """
    recs = []
    items = (recurring or {}).get("recurring", [])
    if not items:
        return recs

    active = [r for r in items if not r["is_zombie"]]
    zombies = [r for r in items if r["is_zombie"]]

    # 1) Price-hike alert — the single biggest recent increase, if any is real.
    hiked = [r for r in active if r["price_change_pct"] >= 0.10]
    if hiked:
        top = max(hiked, key=lambda r: r["price_change_pct"] * r["monthly_cost"])
        pct = round(top["price_change_pct"] * 100)
        recs.append(_rec(
            "subscription_price_hike",
            f"{_clean_name(top['display'])} costs more than it used to",
            f"{_clean_name(top['display'])} has risen about {pct}% to ~€{top['last_amount']:.2f} "
            f"per charge. Check whether the plan still earns its keep or can be downgraded.",
            top["monthly_cost"] * 0.5,   # downgrading/dropping recovers a good chunk
            None, "subscription", "watch",
            assumption="Detected from the change between your earliest and latest charges.",
        ))

    # 2) Zombie alert — recurring charges that stopped appearing may still be billing
    #    on a card you don't watch, or are ones you meant to cancel.
    if zombies:
        z = max(zombies, key=lambda r: r["monthly_cost"])
        recs.append(_rec(
            "subscription_zombie",
            f"Is {_clean_name(z['display'])} still active?",
            f"{_clean_name(z['display'])} billed ~€{z['avg_amount']:.2f} on a {z['cadence']} rhythm "
            f"but hasn't appeared since {z['last_seen']}. Make sure it's actually cancelled — "
            f"forgotten subscriptions quietly keep charging.",
            z["monthly_cost"],
            None, "subscription", "watch",
        ))

    # 3) General audit — the committed monthly base, so it's always visible.
    committed = (recurring or {}).get("committed_monthly", 0.0)
    if active and committed > 0:
        names = ", ".join(_clean_name(r["display"]) for r in sorted(active, key=lambda r: -r["monthly_cost"])[:4])
        recs.append(_rec(
            "subscription_audit",
            "Review your recurring charges",
            f"You have {len(active)} recurring charge(s) — {names}"
            f"{'…' if len(active) > 4 else ''} — totalling about €{committed:.0f}/mo. "
            f"Cancelling any you no longer use saves its full price every month.",
            committed * 0.25,   # assume ~a quarter is trimmable
            None, "subscription", "tip",
        ))
    return recs


def _clean_name(raw):
    """Tidy a raw merchant string for display in a sentence (drop URLs/ref codes)."""
    s = str(raw).split("*")[-1].strip()
    s = s.replace("WWW.", "").replace("www.", "")
    # Keep it short and readable.
    return s.title() if s.isupper() or s.islower() else s


def _analyse_category_budgets(avg_cat, budgets):
    """Flag categories whose recent (3-month avg) spend runs over a user-set budget.

    Uses the same timeframe as the other analysers (avg over the recent window) so
    it's a stable "you consistently exceed this budget" signal, complementary to the
    live in-month pacing shown on the monthly cards."""
    recs = []
    for cat, limit in (budgets or {}).items():
        if not limit or limit <= 0:
            continue
        spent = avg_cat.get(cat, 0.0)
        if spent <= 0 or spent <= limit:
            continue
        over = spent - limit
        if over < 5:   # ignore trivial overruns
            continue
        label = _pretty(cat)
        pct = over / limit * 100
        recs.append(_rec(
            f"budget_{cat.replace(' ', '_')}",
            f"You're over budget on {label}",
            f"{label.capitalize()} averages ~€{spent:.0f}/mo — about €{over:.0f} over your "
            f"€{limit:.0f} budget ({pct:.0f}% above). Trimming it back to plan frees €{over:.0f}/mo.",
            over,
            cat, "budget",
            severity="alert" if pct >= 40 else "watch",
            assumption="Compared to the monthly budget you set for this category.",
        ))
    return recs


def _goal_analysis(avg_cat, monthly_total, goal, recs):
    """Produce the headline goal recommendation: which changes close the gap to goal."""
    gap = monthly_total - goal
    total_savings = sum(r["saving"] for r in recs)
    if gap <= 0:
        return _rec(
            "goal_status",
            "You're within your goal 🎉",
            f"Your recent average (€{monthly_total:.0f}/mo) is already under your €{goal:.0f} goal "
            f"by €{-gap:.0f}. Keep it up — the tips below could build a small buffer.",
            0.0, None, "goal", "good",
        )
    # Greedily pick the biggest recommendations until the gap is covered.
    ordered = sorted([r for r in recs if r["saving"] > 0], key=lambda r: -r["saving"])
    picked, running = [], 0.0
    for r in ordered:
        if running >= gap:
            break
        picked.append(r)
        running += r["saving"]
    if picked and running >= gap:
        names = "; ".join(p["title"].lower() for p in picked)
        detail = (f"You're €{gap:.0f}/mo over your €{goal:.0f} goal. Applying "
                  f"{len(picked)} change(s) — {names} — could save about €{running:.0f}/mo "
                  f"and bring you within goal.")
        sev = "opportunity"
    elif total_savings > 0:
        detail = (f"You're €{gap:.0f}/mo over your €{goal:.0f} goal. The suggestions below "
                  f"could recover about €{total_savings:.0f}/mo — a solid step toward it.")
        sev = "watch"
    else:
        detail = (f"You're €{gap:.0f}/mo over your €{goal:.0f} goal. Your spending is fairly lean "
                  f"across categories — trimming the biggest ones is the main lever.")
        sev = "watch"
    return _rec("goal_status", "Closing the gap to your goal", detail, min(running if picked else 0.0, gap), None, "goal", sev)


def build_recommendations(analysis_input):
    """Main entry point.

    analysis_input = {
        "monthly_sums": {"YYYY-MM": {cat: amount}},   # for averaging
        "recent_txs":   [ {date, merchant, amount, category} ],  # last-N-month txns
        "recurring":    { recurring: [...], committed_monthly },  # from recurring_service
        "goal":         1250,
    }
    Returns {recommendations, headline, potential_savings, months_analysed, avg_total}.
    """
    monthly_sums = analysis_input.get("monthly_sums", {})
    recent_txs = analysis_input.get("recent_txs", [])
    recurring = analysis_input.get("recurring", {})
    category_budgets = analysis_input.get("category_budgets", {})
    goal = float(analysis_input.get("goal", 0) or 0)

    months = _recent_months(monthly_sums, n=3)
    avg_cat = _avg_by_category(monthly_sums, months)
    avg_total = sum(avg_cat.values())

    recs = []
    # Category-specific swaps (nice extras where we have domain knowledge).
    recs += _analyse_eating_out(avg_cat, recent_txs, n_months=len(months) or 3)
    recs += _analyse_groceries(avg_cat)
    recs += _analyse_transport(avg_cat, recent_txs)
    recs += _analyse_vs_benchmarks(avg_cat)
    recs += _analyse_subscriptions(recurring)
    # User-set per-category budgets (highest-signal: the user told us the target).
    recs += _analyse_category_budgets(avg_cat, category_budgets)
    # Generic, data-driven analysers (work for ANY category, now and in 30 years).
    recs += _analyse_category_trends(monthly_sums, months, avg_cat)

    # Avoid piling multiple recs on the same category — keep the most actionable one.
    # Priority by kind, then by saving.
    kind_pri = {"budget": -1, "swap": 0, "benchmark": 1, "trend": 2, "subscription": 0, "top": 3, "goal": 0}
    per_cat_best = {}
    passthrough = []
    for r in recs:
        cat = r.get("category")
        if not cat:
            passthrough.append(r)
            continue
        cur = per_cat_best.get(cat)
        if cur is None or (kind_pri.get(r["kind"], 9), -r["saving"]) < (kind_pri.get(cur["kind"], 9), -cur["saving"]):
            per_cat_best[cat] = r
    recs = passthrough + list(per_cat_best.values())

    # Fallback: guarantee useful output for any profile if over goal (uses top cats).
    existing_ids = {r["id"] for r in recs}
    recs += _analyse_top_categories(avg_cat, goal, existing_ids)

    # De-dup by id (keep largest saving) and drop trivial ones.
    seen = {}
    for r in recs:
        if r["id"] not in seen or r["saving"] > seen[r["id"]]["saving"]:
            seen[r["id"]] = r
    recs = [r for r in seen.values() if r["saving"] >= 2 or r["kind"] in ("benchmark", "subscription")]

    # Rank: alerts first, then by saving.
    sev_rank = {"alert": 0, "watch": 1, "opportunity": 2, "tip": 3, "good": 4}
    recs.sort(key=lambda r: (sev_rank.get(r["severity"], 5), -r["saving"]))

    headline = None
    if goal > 0 and avg_total > 0:
        headline = _goal_analysis(avg_cat, avg_total, goal, recs)

    potential = round(sum(r["saving"] for r in recs), 2)

    return {
        "headline": headline,
        "recommendations": recs,
        "potential_savings": potential,
        "months_analysed": len(months),
        "avg_total": round(avg_total, 2),
        "goal": goal,
    }
