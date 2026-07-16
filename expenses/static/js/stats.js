import { state } from './state.js';
import { STAT_ACCENTS } from './constants.js';
import { renderSpendBubbles } from './bubbles.js';
import { renderFunFacts } from './funfacts.js';

// ── Stats ──────────────────────────────────────
// The Overview view renders a bespoke header (compact stats on the left, floating
// spend-day photo bubbles on the right). The Monthly view uses the shared 4-card grid.
export function renderStats() {
    if (state.currentView === 'overview') {
        renderOverviewHeader();
        return;
    }
    const grid = document.getElementById('statsGrid');
    if (!state.currentMonthDetails) return;
    const monthTotal = Object.values(state.currentMonthDetails.category_totals).reduce((a, b) => a + b, 0);
    const topMonthCat = Object.entries(state.currentMonthDetails.category_totals)
        .filter(([_, v]) => v > 0).reduce((a, b) => a[1] > b[1] ? a : b, ['other', 0]);
    const avgDaily = monthTotal / (Object.keys(state.currentMonthDetails.daily_spending || {}).length || 1);

    grid.innerHTML = [
        createStatCard('Month Total', `€${monthTotal.toFixed(0)}`, 0),
        createStatCard('Avg Per Day', `€${avgDaily.toFixed(0)}`, 1),
        createStatCard('Top Category', topMonthCat[0].replace('_', ' ').replace('-', ' '), 2, `€${topMonthCat[1].toFixed(0)}`),
        createStatCard('Transactions', state.currentMonthDetails.total_transactions, 3)
    ].join('');
}

// Overview header: four compact stat blocks clumped on the left.
export function renderOverviewHeader() {
    const host = document.getElementById('overviewStats');
    if (!host) return;
    if (!state.categoryTotals || !state.monthlyData || !state.monthlyData.totals) return;

    const totalSpending = Object.values(state.categoryTotals).reduce((a, b) => a + b, 0);
    const topCategory = Object.entries(state.categoryTotals).reduce((a, b) => a[1] > b[1] ? a : b);
    const totals = state.monthlyData.totals;
    const avgMonthly = totals.reduce((a, b) => a + b, 0) / totals.length;
    const recentTrend = totals.length > 1
        ? ((totals[totals.length - 1] - totals[totals.length - 2]) / totals[totals.length - 2] * 100)
        : 0;

    host.innerHTML = [
        miniStat('Total spending', `€${totalSpending.toFixed(0)}`, STAT_ACCENTS[0]),
        miniStat('Avg monthly', `€${avgMonthly.toFixed(0)}`, STAT_ACCENTS[1], null, recentTrend),
        miniStat('Top category', topCategory[0].replace('_', ' ').replace('-', ' '), STAT_ACCENTS[2], `€${topCategory[1].toFixed(0)}`),
        miniStat('Months tracked', state.monthlyData.months.length, STAT_ACCENTS[3]),
    ].join('');

    // Floating photo bubbles for the top-spend days (wide band under the header).
    renderSpendBubbles();
    // Fun facts: how the user's spending compares to national/EU averages.
    renderFunFacts();
}

// A small, dense stat block for the overview header.
function miniStat(label, value, accent, subValue, trend) {
    const trendHtml = (trend !== undefined && trend !== null)
        ? `<span class="mini-stat-trend ${trend >= 0 ? 'trend-up' : 'trend-down'}">
               ${trend >= 0 ? '&#9650;' : '&#9660;'} ${Math.abs(trend).toFixed(1)}%
           </span>`
        : '';
    return `
        <div class="mini-stat" style="--stat-accent:${accent}">
            <div class="mini-stat-label">${label}</div>
            <div class="mini-stat-value">${value}${subValue ? `<span class="mini-stat-sub">${subValue}</span>` : ''}</div>
            ${trendHtml}
        </div>
    `;
}

export function createStatCard(label, value, colorIdx, subValue, trend) {
    return `
        <div class="stat-card" style="--stat-accent: ${STAT_ACCENTS[colorIdx % STAT_ACCENTS.length]}">
            <div class="stat-label">${label}</div>
            <div class="stat-value">${value}</div>
            ${subValue ? `<div class="stat-sub">${subValue}</div>` : ''}
            ${trend !== undefined && trend !== null ? `
                <div class="stat-trend ${trend >= 0 ? 'trend-up' : 'trend-down'}">
                    ${trend >= 0 ? '&#9650;' : '&#9660;'} ${Math.abs(trend).toFixed(1)}%
                </div>
            ` : ''}
        </div>
    `;
}
