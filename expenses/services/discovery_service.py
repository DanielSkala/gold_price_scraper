"""Category discovery — surface hidden categories inside the big 'other' bucket.

The 'other' category tends to accumulate many unrelated merchants. This module
looks ONLY at transactions currently categorised as 'other' and:

  1. Normalises + tokenises each merchant name.
  2. Matches merchants against a dictionary of candidate categories (health,
     pharmacy, clothing, entertainment, subscriptions, ...). Merchants that hit a
     candidate are grouped under that suggested category.
  3. For merchants that match nothing, it clusters by their most distinctive
     shared token (so e.g. three "XYZ s.r.o." rows surface as one suggestion).

Everything is deterministic string logic — no model, no API.
"""
import re
from collections import defaultdict

# Candidate categories the 'other' bucket often hides. Keyword → suggested category.
# Keywords are matched case-insensitively as substrings of the merchant name.
CANDIDATE_CATEGORIES = {
    "pharmacy & health": {
        "icon": "fa-pills", "color": "#0e7490",
        "keywords": ["lekaren", "pharmacy", "apotheke", "benu", "dr.max", "drmax", "pilulka",
                     "clinic", "klinika", "poliklinika", "hospital", "nemocnica", "zubn", "dental",
                     "optika", "optic", "medical"],
    },
    "clothing & shoes": {
        "icon": "fa-shirt", "color": "#be185d",
        "keywords": ["h&m", "hm.com", "zara", "reserved", "c&a", "cropp", "house", "sinsay",
                     "nike", "adidas", "ccc", "deichmann", "footshop", "aboutyou", "zalando",
                     "primark", "decathlon", "intersport", "clothing", "obuv", "shoes"],
    },
    "entertainment": {
        "icon": "fa-film", "color": "#7c3aed",
        "keywords": ["cinema", "kino", "cinema city", "cinemax", "divadlo", "theatre", "theater",
                     "koncert", "concert", "ticketportal", "ticketmaster", "predpredaj",
                     "steam", "playstation", "xbox", "nintendo", "gog", "epic games"],
    },
    "subscriptions & digital": {
        "icon": "fa-cloud", "color": "#2563eb",
        "keywords": ["netflix", "spotify", "youtube", "disney", "hbo", "apple.com", "itunes",
                     "icloud", "google", "microsoft", "openai", "chatgpt", "adobe", "dropbox",
                     "audible", "patreon", "linkedin", "amazon prime", "prime video"],
    },
    "health & fitness": {
        "icon": "fa-dumbbell", "color": "#15803d",
        "keywords": ["gym", "fitness", "fitko", "orangetheory", "multisport", "wellness",
                     "spa", "yoga", "crossfit", "bazen", "pool"],
    },
    "electronics & tech": {
        "icon": "fa-laptop", "color": "#0369a1",
        "keywords": ["alza", "datart", "nay", "mall.sk", "notino", "mediamarkt", "iStyle",
                     "istyle", "electro", "mobil", "hej.sk"],
    },
    "beauty & personal care": {
        "icon": "fa-scissors", "color": "#db2777",
        "keywords": ["kadernictvo", "barber", "holic", "salon", "kozmetika", "cosmetic",
                     "nechty", "nails", "notino", "sephora", "dm drogerie", "teta", "manikura"],
    },
    "home & garden": {
        "icon": "fa-house", "color": "#65a30d",
        "keywords": ["obi", "hornbach", "bauhaus", "zahrada", "garden", "kvety", "flowers",
                     "kvetinarstvo", "dedoles"],
    },
    "books & education": {
        "icon": "fa-book", "color": "#b45309",
        "keywords": ["martinus", "panta rhei", "kniha", "book", "gorila", "udemy", "coursera",
                     "duolingo", "skillshare", "school", "skola", "kurz", "course"],
    },
    "fees & finance": {
        "icon": "fa-building-columns", "color": "#57534e",
        "keywords": ["poplatok", "fee", "urok", "interest", "poistenie", "insurance", "poistovna",
                     "dan", "tax", "uver", "splatka", "revolut", "paypal", "wise"],
    },
    "gifts & donations": {
        "icon": "fa-gift", "color": "#c2410c",
        "keywords": ["darc", "gift", "charity", "donation", "dar", "unicef", "cervený kriz",
                     "nadacia", "zbierka"],
    },
    "pets": {
        "icon": "fa-paw", "color": "#a16207",
        "keywords": ["pet", "zoo", "veterin", "vet", "krmivo", "pesk", "maxizoo", "superzoo"],
    },
    "alcohol & tobacco": {
        "icon": "fa-wine-glass", "color": "#9f1239",
        "keywords": ["vinoteka", "wine", "pivoteka", "tabak", "tobacco", "trafika", "liehoviny"],
    },
}

# Tokens too generic to cluster on (legal-entity suffixes, filler, digits).
_STOPWORDS = {
    "sro", "s", "r", "o", "as", "a", "spol", "ltd", "inc", "llc", "gmbh", "kg", "co",
    "the", "and", "of", "sk", "cz", "eu", "com", "www", "obchod", "shop", "store",
    "bratislava", "kosice", "praha", "brno", "sk1", "pos", "nfc", "card", "platba",
}

# Payment gateways / processors: the merchant name only says "paid online via X",
# not WHAT was bought — so they must never become a suggested category. They still
# count toward the 'other' total (the money was spent), we just don't cluster on them.
_PAYMENT_GATEWAYS = [
    "gopay", "comgate", "besteron", "trustpay", "global payments", "gpwebpay",
    "gp webpay", "24-pay", "24 pay", "paypal", "pay pal", "stripe", "adyen",
    "revolut", "wise", "transferwise", "sumup", "square", "klarna", "skrill",
    "neteller", "payu", "paysera", "viva", "shoptet", "shopify", "checkout.com",
    "mollie", "braintree", "worldpay", "barion", "csob platobna", "tatrapay",
    "cardpay", "viamo", "apple pay", "google pay", "g pay", "paybysquare",
]


def _is_payment_gateway(merchant_lower):
    return any(g in merchant_lower for g in _PAYMENT_GATEWAYS)


def _normalize(merchant):
    """Lowercase, strip accents-ish noise, keep alphanumerics as space-separated tokens.
    Collapses runs of whitespace so multi-word matching is reliable."""
    s = str(merchant).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _tokens(merchant):
    out = []
    for tok in _normalize(merchant).split():
        if tok in _STOPWORDS:
            continue
        if tok.isdigit():
            continue
        if len(tok) < 3:
            continue
        out.append(tok)
    return out


def _match_candidate(merchant_norm):
    """Return (category_name, meta) if the merchant hits a candidate dictionary, else None.

    `merchant_norm` must already be _normalize()d. Keywords are normalized the same way
    (so punctuated keywords like 'h&m' and 'dr.max' become 'h m'/'dr max' and can match),
    and matched at WORD-START boundaries — this keeps intended prefix matches ('zubn' →
    'zubna', 'veterin' → 'veterinar') while rejecting mid-word false hits ('pet' inside
    'carpet', 'house' inside 'warehouse')."""
    padded = f" {merchant_norm} "
    for name, meta in CANDIDATE_CATEGORIES.items():
        for kw in meta["keywords"]:
            kw_norm = _normalize(kw)
            if kw_norm and f" {kw_norm}" in padded:
                return name, meta
    return None


def discover_categories(other_txs, min_txns=2, min_total=15.0, max_suggestions=8):
    """Given the list of 'other' transactions, propose new categories.

    other_txs: list of {date, merchant, amount, ...}. Only these are analysed.
    Returns a list of suggestions, each:
        {
          name, icon, color, kind ('known'|'cluster'),
          total, count, share_of_other,
          merchants: [{merchant, count, total}],
          sample_items: [merchant strings],
        }
    """
    if not other_txs:
        return {"suggestions": [], "other_total": 0.0, "other_count": 0}

    # `other_total` includes everything (gateway spend was still real spend).
    other_total = sum(t["amount"] for t in other_txs)

    # Drop payment-gateway rows from the analysis: their names don't reveal WHAT was
    # bought, so they'd form meaningless "categories". They stay in `other_total`.
    analysable = [t for t in other_txs if not _is_payment_gateway(_normalize(t["merchant"]))]

    # Group by suggested candidate category first.
    known_groups = defaultdict(list)      # category_name -> [tx]
    unmatched = []
    for tx in analysable:
        m = _match_candidate(_normalize(tx["merchant"]))
        if m:
            known_groups[m[0]].append(tx)
        else:
            unmatched.append(tx)

    suggestions = []

    # 1) Known candidate categories.
    for name, txs in known_groups.items():
        meta = CANDIDATE_CATEGORIES[name]
        total = sum(t["amount"] for t in txs)
        if len(txs) < min_txns and total < min_total:
            continue
        suggestions.append(_build_suggestion(name, meta["icon"], meta["color"], "known", txs, other_total))

    # 2) Cluster the unmatched by their most distinctive shared token.
    #    Build token -> txs, keep tokens shared by >=2 DIFFERENT merchants.
    token_to_txs = defaultdict(list)
    for tx in unmatched:
        seen = set()
        for tok in _tokens(tx["merchant"]):
            if tok in seen:
                continue
            seen.add(tok)
            token_to_txs[tok].append(tx)

    used = set()  # tx identity (id()) already placed into a cluster
    # Rank candidate tokens by total spend they'd capture (bigger = more useful).
    ranked_tokens = sorted(
        token_to_txs.items(),
        key=lambda kv: -sum(t["amount"] for t in kv[1]),
    )
    for tok, txs in ranked_tokens:
        fresh = [t for t in txs if id(t) not in used]
        distinct_merchants = {t["merchant"].strip().lower() for t in fresh}
        total = sum(t["amount"] for t in fresh)
        # Require a real cluster: multiple txns (or a meaningful sum) and >1 occurrence.
        if len(fresh) < min_txns and total < min_total:
            continue
        if len(fresh) < 2:
            continue
        name = tok.capitalize()
        suggestions.append(_build_suggestion(name, "fa-tag", "#78716c", "cluster", fresh, other_total))
        for t in fresh:
            used.add(id(t))

    # Sort by captured spend, biggest opportunity first, and cap.
    suggestions.sort(key=lambda s: -s["total"])
    suggestions = suggestions[:max_suggestions]

    return {
        "suggestions": suggestions,
        "other_total": round(other_total, 2),
        "other_count": len(other_txs),
    }


def _build_suggestion(name, icon, color, kind, txs, other_total):
    by_merchant = defaultdict(lambda: {"count": 0, "total": 0.0})
    for t in txs:
        m = t["merchant"].strip()
        by_merchant[m]["count"] += 1
        by_merchant[m]["total"] += t["amount"]
    merchants = sorted(
        ({"merchant": m, "count": v["count"], "total": round(v["total"], 2)} for m, v in by_merchant.items()),
        key=lambda x: -x["total"],
    )
    total = sum(t["amount"] for t in txs)
    return {
        "name": name,
        "icon": icon,
        "color": color,
        "kind": kind,
        "total": round(total, 2),
        "count": len(txs),
        "share_of_other": round(total / other_total, 3) if other_total else 0.0,
        "merchants": merchants,
        "sample_items": [m["merchant"] for m in merchants[:6]],
    }
