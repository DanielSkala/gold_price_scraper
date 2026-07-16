import { state } from './state.js';
import { renderMonthlyTransactionsTable, renderTransactionsTable } from './transactions.js';

// ── Search ─────────────────────────────────────
// Both search boxes exist in the initial HTML, so (with deferred modules) the DOM
// is already parsed here and we can attach listeners directly.
const searchBox = document.getElementById('searchBox');
if (searchBox) {
    searchBox.addEventListener('input', function(e) {
        const term = e.target.value.toLowerCase();
        state.transactionsDisplayed = 50;
        if (!term) { renderTransactionsTable(); return; }
        renderTransactionsTable(state.transactions.filter(tx =>
            tx.merchant.toLowerCase().includes(term) || tx.category.toLowerCase().includes(term)
        ));
    });
}

const monthlySearchBox = document.getElementById('monthlySearchBox');
if (monthlySearchBox) {
    monthlySearchBox.addEventListener('input', function(e) {
        if (!state.currentMonthDetails) return;
        const term = e.target.value.toLowerCase();
        const all = Object.values(state.currentMonthDetails.category_transactions || {}).flat();
        if (!term) { renderMonthlyTransactionsTable(); return; }
        renderMonthlyTransactionsTable(all.filter(tx =>
            tx.merchant.toLowerCase().includes(term) || tx.category.toLowerCase().includes(term)
        ));
    });
}
