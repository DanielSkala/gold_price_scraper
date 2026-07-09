import { state } from './state.js';
import { renderOverviewCharts } from './charts_overview.js';
import { renderMonthlyDetailCharts } from './charts_monthly.js';
import { renderGoldCharts } from './gold.js';

// ── Dark / light theme ─────────────────────────────────────────────────────
// The whole app is token-driven, so flipping [data-theme] on <html> restyles
// every component via CSS. Charts (Plotly) bake colors in at render time, so we
// re-render the active view's charts after switching.

function applyIcon(theme) {
    const btn = document.getElementById('themeToggle');
    if (!btn) return;
    const icon = btn.querySelector('i');
    if (icon) icon.className = theme === 'dark' ? 'fas fa-sun' : 'fas fa-moon';
    btn.title = theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode';
}

// Re-render whichever view's charts are on screen so Plotly picks up new colors.
// Plotly.react doesn't reliably repaint the background rects on a pure color change,
// so purge the chart divs first to force a clean re-render in the new theme.
const CHART_IDS = ['monthlyChart', 'categoryChart', 'currentMonthChart', 'dailySpendingChart',
                   'goldSpotChart', 'goldPremiumChart'];
function rerenderCharts() {
    requestAnimationFrame(() => {
        try {
            CHART_IDS.forEach(id => {
                const el = document.getElementById(id);
                if (el && el.offsetParent !== null && window.Plotly) Plotly.purge(el);
            });
            if (state.currentView === 'overview') renderOverviewCharts();
            else if (state.currentView === 'monthly') renderMonthlyDetailCharts();
            else if (state.currentView === 'gold' && state.goldLoaded) renderGoldCharts();
        } catch (e) { /* charts may not be ready yet on first paint */ }
    });
}

export function toggleTheme() {
    const cur = document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
    const next = cur === 'dark' ? 'light' : 'dark';
    if (next === 'dark') document.documentElement.setAttribute('data-theme', 'dark');
    else document.documentElement.removeAttribute('data-theme');
    try { localStorage.setItem('theme', next); } catch (e) {}
    applyIcon(next);
    rerenderCharts();
    // Let other UI (e.g. the sidebar switch) sync without a hard import dependency.
    document.dispatchEvent(new CustomEvent('themechange', { detail: next }));
}

// Sync the toggle icon with whatever the pre-paint head script already applied.
export function initTheme() {
    const theme = document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
    applyIcon(theme);
}
