// ── Category Color Map (single source of truth) ──
// One intentional color per category, used everywhere (donut slices, trend lines,
// breakdown card borders, transaction badges). Hues encode meaning:
//   NEEDS (essential)      → calm cool hues that recede
//   WANTS (discretionary)  → warm attention-drawing hues (invite cutting)
//   NEUTRAL (mixed)        → muted, neither encourage nor flag
// fg = saturated (text / lines / slices), bg = light tint (badge background).
export const CATEGORY_COLORS = {
    // NEEDS — calm cool
    groceries:    { fg: '#0f766e', bg: '#eefaf8' },  // deep teal
    gas_stations: { fg: '#1d4ed8', bg: '#eff6ff' },  // blue (utilitarian)
    parking:      { fg: '#0369a1', bg: '#eff8fd' },  // steel blue
    car_wash:     { fg: '#0891b2', bg: '#ecfeff' },  // cyan
    oil_partner:  { fg: '#4d7c0f', bg: '#f7fee7' },  // olive
    // WANTS — warm, eye-catching
    eating_out:   { fg: '#c2410c', bg: '#fff7ed' },  // burnt orange
    travel:       { fg: '#b45309', bg: '#fffbeb' },  // amber
    bolt:         { fg: '#be123c', bg: '#fff1f2' },  // coral/rose (taxi)
    // NEUTRAL — muted
    nabytok:      { fg: '#7c3aed', bg: '#f5f3ff' },  // violet (furniture)
    other:        { fg: '#57534e', bg: '#f5f5f4' },  // warm gray
};

const CATEGORY_FALLBACK = { fg: '#57534e', bg: '#f5f5f4' };

// Subtle FontAwesome icon per category. Obvious real-world mapping.
export const CATEGORY_ICONS = {
    groceries:    'fa-basket-shopping',
    gas_stations: 'fa-gas-pump',
    parking:      'fa-square-parking',
    car_wash:     'fa-car',
    oil_partner:  'fa-oil-can',
    eating_out:   'fa-utensils',
    travel:       'fa-plane',
    bolt:         'fa-taxi',        // bolt = taxi
    nabytok:      'fa-couch',       // furniture
    other:        'fa-receipt',
};

// Custom categories added at runtime via the UI (loaded from /api/custom-categories).
// Each: {name, color, icon}. Merged into the lookups below.
export const CUSTOM_CATEGORIES = {};  // key -> {fg, bg, icon}

export function registerCustomCategories(list) {
    for (const c of (list || [])) {
        // Keep the original display name so pickers/badges show it with its real casing.
        CUSTOM_CATEGORIES[catKey(c.name)] = { name: c.name, fg: c.color, bg: tintOf(c.color), icon: c.icon || 'fa-tag' };
    }
}

// Display names of all custom categories, in their original casing.
export function customCategoryNames() {
    return Object.values(CUSTOM_CATEGORIES).map(c => c.name);
}

// Derive a light background tint from a foreground hex (mix ~12% toward white bg).
function tintOf(hex) {
    const h = hex.replace('#', '');
    const r = parseInt(h.slice(0, 2), 16), g = parseInt(h.slice(2, 4), 16), b = parseInt(h.slice(4, 6), 16);
    const mix = (c) => Math.round(c + (247 - c) * 0.88);  // toward #f7f6f3-ish
    return `rgb(${mix(r)},${mix(g)},${mix(b)})`;
}

// Category strings come in mixed forms ("gas stations", "gas-stations"); normalize to the key.
function catKey(name) {
    return String(name).toLowerCase().replace(/[\s-]+/g, '_');
}

function catEntry(name) {
    const k = catKey(name);
    return CATEGORY_COLORS[k] || CUSTOM_CATEGORIES[k] || CATEGORY_FALLBACK;
}

export function getCategoryColor(name) {
    return catEntry(name).fg;
}
export function getCategoryBg(name) {
    return catEntry(name).bg;
}
export function getCategoryIcon(name) {
    const k = catKey(name);
    return CATEGORY_ICONS[k] || (CUSTOM_CATEGORIES[k] && CUSTOM_CATEGORIES[k].icon) || 'fa-tag';
}

// Stat-card accent bars are structural, not category-bound: primary, teal, amber, gray.
export const STAT_ACCENTS = ['#2563eb', '#0f766e', '#b45309', '#57534e'];

// Read a CSS custom property off :root so charts follow the active theme.
function cssVar(name, fallback) {
    try {
        const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
        return v || fallback;
    } catch (e) { return fallback; }
}
export function isDark() {
    return document.documentElement.getAttribute('data-theme') === 'dark';
}

// Theme-aware Plotly tokens. These are GETTERS (functions) so each chart render
// picks up the current theme's colors. Call them at render time.
export function plotlyFont() { return { family: '-apple-system, BlinkMacSystemFont, "SF Pro Text", system-ui, sans-serif', color: cssVar('--chart-ink', '#57534e') }; }
export function plotlyGrid() { return cssVar('--chart-grid', '#f0eeea'); }
export function chartBg() { return cssVar('--chart-bg', '#ffffff'); }

// Shared hover-label — a solid card (dark-on-light in light mode, inverts in dark)
// so text stays legible over any chart lines/bars.
export function hoverLabel() {
    const dark = isDark();
    const bg = dark ? '#f5f1ea' : '#1c1917';
    const fg = dark ? '#17150f' : '#ffffff';
    return { bgcolor: bg, bordercolor: bg, font: { family: '-apple-system, BlinkMacSystemFont, "SF Pro Text", system-ui, sans-serif', size: 12.5, color: fg }, align: 'left' };
}

// Back-compat exports (light-mode values). Prefer the getters above in new code.
export const PLOTLY_FONT = { family: '-apple-system, BlinkMacSystemFont, "SF Pro Text", system-ui, sans-serif', color: '#57534e' };
export const PLOTLY_GRID = '#f0eeea';
export const HOVER_LABEL = {
    bgcolor: '#1c1917',
    bordercolor: '#1c1917',
    font: { family: '-apple-system, BlinkMacSystemFont, "SF Pro Text", system-ui, sans-serif', size: 12.5, color: '#ffffff' },
    align: 'left',
};

// Modebar / config shared across charts (hide the toolbar entirely).
export const PLOTLY_CONFIG = { responsive: true, displayModeBar: false };
