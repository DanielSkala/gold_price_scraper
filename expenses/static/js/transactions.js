import { state, amountColor } from './state.js';
import { renderMonthlyChart } from './charts_overview.js';
import { showCategoryDetails } from './panel.js';
import { categoryBadgeHtml } from './overrides.js';

// HTML-escape for safe embedding in table cells (defense-in-depth for merchant data).
const escapeHtml = (s) => String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/'/g, '&#39;');

// ── Transactions Table ─────────────────────────
export function sortTransactions(data, column, direction) {
    if (!data || !Array.isArray(data)) return [];
    return [...data].sort((a, b) => {
        let vA, vB;
        switch (column) {
            case 'date': vA = new Date(a.date); vB = new Date(b.date); break;
            case 'amount': vA = a.amount; vB = b.amount; break;
            case 'merchant': vA = a.merchant.toLowerCase(); vB = b.merchant.toLowerCase(); break;
            case 'category': vA = a.category.toLowerCase(); vB = b.category.toLowerCase(); break;
            default: return 0;
        }
        if (vA < vB) return direction === 'asc' ? -1 : 1;
        if (vA > vB) return direction === 'asc' ? 1 : -1;
        return 0;
    });
}

export function handleSort(column) {
    if (state.sortColumn === column) {
        state.sortDirection = state.sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        state.sortColumn = column;
        state.sortDirection = 'desc';
    }
    state.transactionsDisplayed = 50;
    renderTransactionsTable();
    if (state.currentView === 'monthly') renderMonthlyTransactionsTable();
}

export function renderTransactionsTable(filteredTransactions = null) {
    if (!state.transactions || !Array.isArray(state.transactions)) {
        document.getElementById('transactionsTable').innerHTML = '<div class="loading"><i class="fas fa-spinner"></i><p>Loading...</p></div>';
        return;
    }

    try {
        // Apply the active search term (kept in state so it survives sorts/refreshes)
        // unless an explicit list was passed in (e.g. the "load more" path).
        let baseData = filteredTransactions;
        if (baseData == null) {
            const term = state.searchTerm;
            baseData = term
                ? state.transactions.filter(tx =>
                    tx.merchant.toLowerCase().includes(term) || tx.category.toLowerCase().includes(term))
                : state.transactions;
        }
        const sortedData = sortTransactions(baseData, state.sortColumn, state.sortDirection);
        state.currentFilteredTransactions = sortedData;

        if (!sortedData || sortedData.length === 0) {
            document.getElementById('transactionsTable').innerHTML = '<div style="text-align:center;padding:40px;color:var(--text-3);">No transactions</div>';
            return;
        }

        const showing = sortedData.slice(0, state.transactionsDisplayed);
        const hasMore = sortedData.length > state.transactionsDisplayed;
        const arrow = (col) => state.sortColumn === col ? (state.sortDirection === 'asc' ? '&#9650;' : '&#9660;') : '';

        document.getElementById('transactionsTable').innerHTML = `
            <div style="font-size:0.78rem;color:var(--text-3);margin-bottom:10px;">
                ${showing.length} of ${sortedData.length} transactions
            </div>
            <table class="transaction-table">
                <thead><tr>
                    <th onclick="App.handleSort('date')">Date <span class="sort-arrow">${arrow('date')}</span></th>
                    <th onclick="App.handleSort('merchant')">Merchant <span class="sort-arrow">${arrow('merchant')}</span></th>
                    <th onclick="App.handleSort('category')">Category <span class="sort-arrow">${arrow('category')}</span></th>
                    <th onclick="App.handleSort('amount')" style="text-align:right;">Amount <span class="sort-arrow">${arrow('amount')}</span></th>
                </tr></thead>
                <tbody>
                    ${showing.map(tx => `
                        <tr>
                            <td style="color:var(--text-2);font-size:0.82rem;">${new Date(tx.date).toLocaleDateString()}</td>
                            <td style="font-weight:500;">${escapeHtml(tx.merchant)}</td>
                            <td>${categoryBadgeHtml(tx)}</td>
                            <td style="text-align:right;font-family:var(--font-serif);color:${amountColor(tx)};font-size:0.9rem;">€${tx.amount.toFixed(2)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
            ${hasMore ? `
                <div style="text-align:center;padding:16px;">
                    <button onclick="App.loadMoreTransactions()" style="
                        padding:8px 20px;border:1px solid var(--border);border-radius:6px;
                        background:transparent;color:var(--text-2);cursor:pointer;
                        font-weight:500;font-size:0.82rem;font-family:var(--font-sans);
                        transition:all 0.12s ease;
                    " onmouseover="this.style.background='var(--text)';this.style.color='white';this.style.borderColor='var(--text)'"
                       onmouseout="this.style.background='transparent';this.style.color='var(--text-2)';this.style.borderColor='var(--border)'">
                        Load more
                    </button>
                </div>
            ` : ''}
        `;
    } catch (error) {
        console.error('Error rendering transactions:', error);
        document.getElementById('transactionsTable').innerHTML = '<div style="text-align:center;padding:40px;color:var(--red);">Error loading transactions</div>';
    }
}

export function loadMoreTransactions() {
    state.transactionsDisplayed += 50;
    // Re-render with no explicit list so the active search term (state.searchTerm)
    // is reapplied consistently — showing more of the SAME filtered set.
    renderTransactionsTable();
}

export function toggleChart(type, evt) {
    state.currentChartType = type;
    document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.remove('active'));
    if (evt && evt.target) evt.target.classList.add('active');
    renderMonthlyChart();
}

export function renderMonthlyTransactionsTable(filteredTransactions = null) {
    if (!state.currentMonthDetails) return;

    let monthlyTxs = Object.values(state.currentMonthDetails.category_transactions || {}).flat();
    let showing = filteredTransactions;
    if (showing == null) {
        const term = state.monthlySearchTerm;
        showing = term
            ? monthlyTxs.filter(tx =>
                tx.merchant.toLowerCase().includes(term) || tx.category.toLowerCase().includes(term))
            : monthlyTxs;
    }
    showing = sortTransactions(showing, state.sortColumn, state.sortDirection);

    const arrow = (col) => state.sortColumn === col ? (state.sortDirection === 'asc' ? '&#9650;' : '&#9660;') : '';

    document.getElementById('monthlyTransactionsTable').innerHTML = `
        <table class="transaction-table">
            <thead><tr>
                <th onclick="App.handleSort('date')">Date <span class="sort-arrow">${arrow('date')}</span></th>
                <th onclick="App.handleSort('merchant')">Merchant <span class="sort-arrow">${arrow('merchant')}</span></th>
                <th onclick="App.handleSort('category')">Category <span class="sort-arrow">${arrow('category')}</span></th>
                <th onclick="App.handleSort('amount')" style="text-align:right;">Amount <span class="sort-arrow">${arrow('amount')}</span></th>
            </tr></thead>
            <tbody>
                ${showing.map(tx => `
                    <tr>
                        <td style="color:var(--text-2);font-size:0.82rem;">${new Date(tx.date).toLocaleDateString()}</td>
                        <td style="font-weight:500;">${escapeHtml(tx.merchant)}</td>
                        <td>${categoryBadgeHtml(tx)}</td>
                        <td style="text-align:right;font-family:var(--font-serif);color:${amountColor(tx)};font-size:0.9rem;">€${tx.amount.toFixed(2)}</td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
        <div style="text-align:center;padding:14px;font-size:0.78rem;color:var(--text-3);">
            ${showing.length} transactions &middot; ${new Date(state.currentMonthDetails.month + '-01').toLocaleDateString('en-US', { month: 'long', year: 'numeric' })}
        </div>
    `;
}
