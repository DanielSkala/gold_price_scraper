// ── Shared mutable dashboard state ─────────────
// Single source of truth imported by every module. Fields mirror the original
// top-level `let` variables; access as state.currentView, state.monthlyData, etc.
export const state = {
    currentView: 'overview',
    currentChartType: 'line',
    currentCategoryView: 'total',
    monthlyData: null,
    categoryTotals: null,
    categoryAverages: null,
    transactions: null,
    trends: null,
    currentMonthDetails: null,
    selectedMonth: null,
    sortColumn: 'date',
    sortDirection: 'desc',
    transactionsDisplayed: 50,
    currentFilteredTransactions: null,
    excludeOutliers: false,
    outlierData: null,
    goldSpotData: null,
    goldPremiumData: null,
    goldPortfolioData: null,
    goldLoaded: false,
    goldTimeframe: '2y',
    dailyMode: 'amount',   // 'amount' (blue/red shades) or 'stacked' (by category)
    budgetGoal: 1250,      // monthly spending goal (editable + persisted)
};

// `panelState` was a separate top-level object in the original; keep it here too
// so the panel + memories modules share it.
export const panelState = {
    title: '',
    showDate: true,      // show date under merchant (false when panel is a single-day view)
    category: null,      // the category this panel is filtered to (null for day / all views)
    calendarCtx: null,   // {category, txs} — drives the calendar; persists across day drill-downs
    selectedDay: null,   // YYYY-MM-DD of the day being viewed (ringed in the calendar)
    dayGroupCategory: null, // in a day view opened from a category, list this category first
    backCategory: null,  // category to return to via the "back" link
    allTxs: [],          // unfiltered
    filter: { q: '' },
    sort: 'date-desc',
    selected: new Set(), // keys "date|merchant|amount|category"
};

// ── Outlier-aware amount coloring ──────────────
// A transaction amount is only shown in red when the transaction is a flagged
// outlier (abnormal spend) — so red keeps its "this one is unusual" meaning.
// Cached key set is rebuilt when outlierData changes identity.
let _outlierKeys = null;
let _outlierRef = null;

function outlierKeySet() {
    if (state.outlierData === _outlierRef) return _outlierKeys;
    _outlierRef = state.outlierData;
    _outlierKeys = new Set(
        (state.outlierData?.outliers || []).map(o => `${o.date}|${o.merchant}|${o.amount}`)
    );
    return _outlierKeys;
}

export function isOutlierTx(tx) {
    return outlierKeySet().has(`${tx.date}|${tx.merchant}|${tx.amount}`);
}

// CSS color for a transaction amount: red if outlier, neutral dark otherwise.
export function amountColor(tx) {
    return isOutlierTx(tx) ? 'var(--neg)' : 'var(--text)';
}
