import { state, panelState } from './state.js';
import { loadAllData } from './loading.js';

// ── Outliers ───────────────────────────────────
export function toggleOutliers() {
    state.excludeOutliers = !state.excludeOutliers;
    const btn = document.getElementById('outlierToggle');
    const text = document.getElementById('outlierToggleText');

    if (state.excludeOutliers) {
        btn.classList.add('active');
        text.textContent = 'Routine Only';
    } else {
        btn.classList.remove('active');
        text.textContent = 'Hide Outliers';
    }

    state.transactionsDisplayed = 50;
    loadAllData();
}

export function renderOutlierPanel() {
    const panel = document.getElementById('outlierPanel');
    if (!state.outlierData || state.outlierData.count === 0 || !state.excludeOutliers) {
        panel.classList.remove('visible');
        return;
    }

    panel.classList.add('visible');

    document.getElementById('outlierSummary').innerHTML = `
        <span><strong>${state.outlierData.count}</strong> items</span>
        <span>&euro;<strong>${state.outlierData.total_amount.toFixed(0)}</strong> total</span>
        <span style="font-size: 0.75rem; color: var(--text-3);"><i class="fas fa-chevron-down"></i></span>
    `;

    document.getElementById('outlierList').innerHTML = state.outlierData.outliers.map(tx => `
        <div class="outlier-item">
            <div class="outlier-item-left">
                <span class="outlier-merchant">${tx.merchant}</span>
                <span class="outlier-meta">${new Date(tx.date).toLocaleDateString()} &middot; ${tx.category.replace('_', ' ')}</span>
            </div>
            <span class="outlier-amount">&euro;${tx.amount.toFixed(2)}</span>
        </div>
    `).join('');
}

export function closeSidePanel() {
    document.getElementById('sidePanel').classList.remove('open');
    const scrim = document.getElementById('sidePanelScrim');
    if (scrim) scrim.classList.remove('open');
    document.getElementById('selectionBar').classList.remove('visible');
    const pf = document.getElementById('panelFilters');
    if (pf) pf.style.display = 'none';
    panelState.selected = new Set();
}
