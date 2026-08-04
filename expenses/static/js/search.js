import { state } from './state.js';
import { renderMonthlyTransactionsTable, renderTransactionsTable } from './transactions.js';

// ── Search ─────────────────────────────────────
// Both search boxes exist in the initial HTML, so (with deferred modules) the DOM
// is already parsed here and we can attach listeners directly.
// The active term is stored in state so it survives a re-sort or a data refresh; the
// table renderers apply it themselves (see renderTransactionsTable). Here we just
// record the term and trigger a re-render.
const searchBox = document.getElementById('searchBox');
if (searchBox) {
    searchBox.addEventListener('input', function(e) {
        state.searchTerm = e.target.value.toLowerCase();
        state.transactionsDisplayed = 50;
        renderTransactionsTable();
    });
}

const monthlySearchBox = document.getElementById('monthlySearchBox');
if (monthlySearchBox) {
    monthlySearchBox.addEventListener('input', function(e) {
        state.monthlySearchTerm = e.target.value.toLowerCase();
        renderMonthlyTransactionsTable();
    });
}
