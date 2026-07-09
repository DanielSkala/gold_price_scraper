import { state } from './state.js';
import { renderMonthlyDetailCharts } from './charts_monthly.js';
import { renderOverviewCharts } from './charts_overview.js';
import { loadGoldData, renderGoldCharts } from './gold.js';
import { loadMonthDetails } from './loading.js';
import { renderStats } from './stats.js';

// Position the segmented-switch slider under the active view button.
export function positionViewSlider() {
    const slider = document.getElementById('viewSwitchSlider');
    const active = document.querySelector('.view-btn.active');
    if (!slider || !active) return;
    slider.style.width = `${active.offsetWidth}px`;
    slider.style.transform = `translateX(${active.offsetLeft - 3}px)`;
}

// ── View Switching ─────────────────────────────
export function switchView(view, evt) {
    document.querySelectorAll('.view-btn').forEach(btn => btn.classList.remove('active'));
    const btn = (evt && evt.currentTarget) || document.querySelector(`.view-btn[data-view="${view}"]`);
    if (btn) btn.classList.add('active');
    positionViewSlider();

    const monthSelector = document.getElementById('monthSelector');
    monthSelector.style.display = view === 'monthly' ? 'block' : 'none';

    // Hide outlier toggle on gold view (not relevant)
    document.getElementById('outlierToggle').style.display = view === 'gold' ? 'none' : '';

    document.querySelectorAll('.view-container').forEach(c => c.classList.remove('active'));
    document.getElementById(view + 'View').classList.add('active');
    state.currentView = view;
    state.transactionsDisplayed = 50;

    // The shared stats grid is retired: Overview has its own header (stats + spend
    // bubbles), Monthly has its month hero, Gold has its own grid. Keep it hidden.
    document.getElementById('statsGrid').style.display = 'none';
    // Overview still needs its header (stats + bubbles) refreshed when switched to.
    if (view === 'overview') renderStats();

    // Re-render charts after view becomes visible so Plotly
    // can measure the container width correctly.
    setTimeout(() => {
        if (view === 'overview') {
            renderOverviewCharts();
        } else if (view === 'monthly') {
            renderMonthlyDetailCharts();
        } else if (view === 'gold') {
            if (!state.goldLoaded) {
                loadGoldData();
            } else {
                renderGoldCharts();
            }
        }
    }, 50);
}

export function handleMonthChange() {
    state.selectedMonth = document.getElementById('monthSelector').value;
    loadMonthDetails(state.selectedMonth);
}

// Step to the previous (-1) or next (+1) month. available_months is sorted
// newest-first, so "next month" (later date) means a LOWER index.
export function stepMonth(delta) {
    const months = state.currentMonthDetails && state.currentMonthDetails.available_months;
    if (!months || !months.length) return;
    const idx = months.indexOf(state.selectedMonth);
    if (idx === -1) return;
    const target = idx - delta;   // +1 = next/newer = lower index
    if (target < 0 || target >= months.length) return;
    const month = months[target];
    // Keep the dropdown in sync too.
    const sel = document.getElementById('monthSelector');
    if (sel) sel.value = month;
    state.selectedMonth = month;
    loadMonthDetails(month);
}
