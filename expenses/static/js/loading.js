import { state } from './state.js';
import { renderMonthlyDetailCharts } from './charts_monthly.js';
import { renderOverviewCharts } from './charts_overview.js';
import { renderOutlierPanel } from './outliers.js';
import { renderStats } from './stats.js';
import { renderTransactionsTable } from './transactions.js';
import { loadCustomCategories } from './categories.js';
import { renderOverviewInsights } from './insights.js';

// ── Data Loading ───────────────────────────────
// ES modules are deferred, so DOMContentLoaded may have ALREADY fired by the time
// this runs. Guard with readyState so the initial load never gets silently skipped.
function onReady(fn) {
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', fn);
    } else {
        fn();
    }
}

onReady(function () {
    // Load custom-category colors/icons + persisted settings first so they resolve
    // on the first render.
    Promise.all([
        loadCustomCategories(),
        fetch('/api/settings').then(r => r.json())
            .then(s => { if (s && typeof s.budget_goal === 'number') state.budgetGoal = s.budget_goal; })
            .catch(() => {}),
    ]).then(loadAllData);
});

export function loadAllData() {
    const qs = state.excludeOutliers ? '?exclude_outliers=1' : '';
    const monthPath = state.selectedMonth ? '/' + state.selectedMonth : '';
    const monthQs = state.selectedMonth ? monthPath + qs : qs;

    Promise.all([
        fetch('/api/monthly-data' + qs).then(r => r.json()),
        fetch('/api/category-totals' + qs).then(r => r.json()),
        fetch('/api/category-averages' + qs).then(r => r.json()),
        fetch('/api/transactions' + qs).then(r => r.json()),
        fetch('/api/trends' + qs).then(r => r.json()),
        fetch('/api/current-month-details' + monthQs).then(r => r.json()),
        fetch('/api/outliers').then(r => r.json()),
        // Refresh custom category colors/icons so newly-added ones always resolve.
        loadCustomCategories(),
    ]).then(([monthly, totals, averages, trans, trendData, monthlyDetails, outliers]) => {
        state.monthlyData = monthly;
        state.categoryTotals = totals;
        state.categoryAverages = averages;
        state.transactions = trans;
        state.trends = trendData;
        state.currentMonthDetails = monthlyDetails;
        state.selectedMonth = monthlyDetails.month;
        state.outlierData = outliers;

        renderStats();
        renderOverviewCharts();
        renderOverviewInsights();
        renderMonthlyDetailCharts();
        setupMonthSelector();
        renderTransactionsTable();
        renderOutlierPanel();
    }).catch(error => {
        console.error('Error loading data:', error);
    });
}

export function setupMonthSelector() {
    if (!state.currentMonthDetails.available_months) return;
    const selector = document.getElementById('monthSelector');
    selector.innerHTML = state.currentMonthDetails.available_months.map(month => `
        <option value="${month}" ${month === state.selectedMonth ? 'selected' : ''}>
            ${new Date(month + '-01').toLocaleDateString('en-US', { month: 'long', year: 'numeric' })}
        </option>
    `).join('');
}

export function loadMonthDetails(month) {
    const qs = state.excludeOutliers ? '?exclude_outliers=1' : '';
    fetch(`/api/current-month-details/${month}${qs}`)
        .then(r => r.json())
        .then(data => {
            state.currentMonthDetails = data;
            state.selectedMonth = month;
            renderStats();
            renderMonthlyDetailCharts();
        })
        .catch(error => console.error('Error loading month details:', error));
}
