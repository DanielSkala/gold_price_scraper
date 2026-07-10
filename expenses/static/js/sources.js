// ── Source → URL resolver ───────────────────────────────────────────────────
// Reference/source strings across the app (fun-fact tiles, savings assumptions)
// are free text like "Numbeo Slovakia food basket, 2025". This maps a keyword in
// that text to a real, clickable URL so every reference links out to its source.

// Ordered: first keyword found in the (lowercased) source text wins.
const SOURCE_URLS = [
    ['numbeo slovakia food',   'https://www.numbeo.com/food-prices/country_result.jsp?country=Slovakia'],
    ['numbeo',                 'https://www.numbeo.com/cost-of-living/country_result.jsp?country=Slovakia'],
    ['eurostat',               'https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Household_consumption_by_purpose'],
    ['fueleconomy',            'https://www.fueleconomy.gov/feg/driveHabits.jsp'],
    ['us doe',                 'https://www.fueleconomy.gov/feg/driveHabits.jsp'],
    ['fao',                    'https://www.fao.org/platform-food-loss-waste/en'],
    ['food-waste',             'https://environment.ec.europa.eu/topics/waste-and-recycling/food-waste_en'],
    ['food waste',            'https://environment.ec.europa.eu/topics/waste-and-recycling/food-waste_en'],
    ['airline fare',           'https://www.google.com/travel/flights'],
    ['fare-timing',            'https://www.google.com/travel/flights'],
    ['seasonal pricing',       'https://www.google.com/travel/flights'],
    ['transit',                'https://dpb.sk/en/'],
    ['budgeting',              'https://www.investopedia.com/terms/b/budget.asp'],
    ['mental accounting',      'https://en.wikipedia.org/wiki/Mental_accounting'],
];

const esc = (s) => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// Resolve a source string to a URL (or null if nothing matches).
export function sourceUrl(text) {
    const t = String(text || '').toLowerCase();
    for (const [kw, url] of SOURCE_URLS) {
        if (t.includes(kw)) return url;
    }
    return null;
}

// Render a source string as a clickable link when we can resolve a URL, else plain.
export function sourceLink(text, cls = 'src-link') {
    const url = sourceUrl(text);
    const label = esc(text);
    if (!url) return label;
    return `<a class="${cls}" href="${url}" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation()">${label}</a>`;
}

// Human-readable source phrases that may appear inside a longer assumption string;
// each becomes a clickable link in place. Longest phrases first so we don't wrap a
// substring of a longer match.
const INLINE_PHRASES = [
    'Numbeo Slovakia food basket',
    'Numbeo Slovakia taxi tariff',
    'Numbeo Slovakia parking',
    'Numbeo Slovakia reference prices',
    'Numbeo fuel + usage',
    'Numbeo fuel price × avg usage',
    'Eurostat 2024 recreation/travel',
    'Eurostat 2024 restaurants share',
    'US DOE fueleconomy.gov driving-behaviour study',
    'US DOE fueleconomy.gov',
    'FAO/EU food-waste estimates',
    'airline fare-timing studies',
    'budgeting research (mental accounting)',
    'Numbeo',
    'Eurostat',
];

// Turn any known source phrases inside a longer text into clickable links, leaving
// the rest escaped/plain. Used for the "Ways to save" assumption line.
export function linkifySources(text) {
    let out = esc(text);
    for (const phrase of INLINE_PHRASES) {
        const url = sourceUrl(phrase);
        if (!url) continue;
        const escPhrase = esc(phrase);
        if (out.includes(escPhrase) && !out.includes(`>${escPhrase}<`)) {
            out = out.replace(escPhrase,
                `<a class="src-link" href="${url}" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation()">${escPhrase}</a>`);
        }
    }
    return out;
}
