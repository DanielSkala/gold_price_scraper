// ── Shared HTML escaping ────────────────────────────────────────────
// Single source of truth for escaping untrusted text (merchant names from the
// bank CSV, user-created category names, macOS Photos album/location titles)
// before it goes into an innerHTML template literal. Prefer this over the many
// ad-hoc per-file copies that used to drift and leave gaps.
export function escapeHtml(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

// ── Category catalog (single source of truth, loaded from the backend) ──
// Every category — built-in or user-created — is data: {name, fg (color), bg (tint),
// icon, keywords, builtin}. The catalog is fetched from /api/categories at startup
// (see registerCatalog); until then a small seed keeps the very first paint sensible.
// Colors are used everywhere (donut slices, trend lines, card borders, badges).
export const CATEGORY_CATALOG = {};   // catKey → {name, fg, bg, icon, keywords, builtin}

// The fallback bucket: "nothing has classified this yet". Distinct from the real
// 'other' category, which means "I decided this is miscellaneous" (see FALLBACK_NAME
// users of isEditableCategory/isDeletableCategory below).
export const FALLBACK_NAME = 'uncategorized';
export const isFallbackCategory = (name) => catKey(name) === catKey(FALLBACK_NAME);

const CATEGORY_FALLBACK = { name: 'other', fg: '#57534e', bg: '#f5f5f4', icon: 'fa-tag', builtin: true };

// Minimal seed so a render before the fetch resolves isn't colorless. Replaced wholesale
// by the backend catalog on load — NOT the source of truth.
const _SEED = {
    groceries:    ['#0f766e', 'fa-basket-shopping'], gas_stations: ['#1d4ed8', 'fa-gas-pump'],
    parking:      ['#0369a1', 'fa-square-parking'],  car_wash:     ['#0891b2', 'fa-car'],
    oil_partner:  ['#4d7c0f', 'fa-oil-can'],         eating_out:   ['#c2410c', 'fa-utensils'],
    travel:       ['#b45309', 'fa-plane'],           bolt:         ['#be123c', 'fa-taxi'],
    nabytok:      ['#7c3aed', 'fa-couch'],           other:        ['#86198f', 'fa-shapes'],
    uncategorized: ['#78716c', 'fa-circle-question'],
};
for (const [k, [color, icon]] of Object.entries(_SEED)) {
    CATEGORY_CATALOG[k] = { name: k.replace(/_/g, ' '), fg: color, bg: tintOf(color), icon, keywords: [], builtin: true };
}

// Replace the whole catalog from the backend payload ([{name, color, icon, keywords, builtin}]).
export function registerCatalog(list) {
    for (const k of Object.keys(CATEGORY_CATALOG)) delete CATEGORY_CATALOG[k];
    for (const c of (list || [])) {
        CATEGORY_CATALOG[catKey(c.name)] = {
            name: c.name, fg: c.color, bg: tintOf(c.color),
            icon: c.icon || 'fa-tag', keywords: c.keywords || [], builtin: !!c.builtin,
        };
    }
}

// All category display names, in catalog order-ish (Object insertion order).
export function allCategoryNames() {
    return Object.values(CATEGORY_CATALOG).map(c => c.name);
}

// The stored entry for a category (for edit-dialog prefill etc.), or null.
export function getCategoryEntry(name) {
    return CATEGORY_CATALOG[catKey(name)] || null;
}

// Every category is editable except the fallback bucket, which is structural: the app
// always needs somewhere for unclassified transactions to land. The real 'other' IS
// editable now — it's an ordinary category.
export function isEditableCategory(name) {
    return !isFallbackCategory(name);
}
export function isDeletableCategory(name) {
    return !isFallbackCategory(name) && !!CATEGORY_CATALOG[catKey(name)];
}

// Drop a category from the client cache (after it's deleted server-side).
export function unregisterCategory(name) {
    delete CATEGORY_CATALOG[catKey(name)];
}

// Derive a light background tint from a foreground hex (mix ~12% toward white bg).
function tintOf(hex) {
    const h = String(hex || '#57534e').replace('#', '');
    const r = parseInt(h.slice(0, 2), 16), g = parseInt(h.slice(2, 4), 16), b = parseInt(h.slice(4, 6), 16);
    const mix = (c) => Math.round(c + (247 - c) * 0.88);  // toward #f7f6f3-ish
    return `rgb(${mix(r)},${mix(g)},${mix(b)})`;
}

// Category strings come in mixed forms ("gas stations", "gas-stations"); normalize to the key.
function catKey(name) {
    return String(name).toLowerCase().replace(/[\s-]+/g, '_');
}

function catEntry(name) {
    return CATEGORY_CATALOG[catKey(name)] || CATEGORY_FALLBACK;
}

export function getCategoryColor(name) { return catEntry(name).fg; }
export function getCategoryBg(name) { return catEntry(name).bg; }
export function getCategoryIcon(name) { return catEntry(name).icon || 'fa-tag'; }

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
