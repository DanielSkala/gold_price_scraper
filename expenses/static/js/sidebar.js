import { state } from './state.js';
import { loadAllData } from './loading.js';

// ── Collapsible sidebar: settings + expense-report management ───────────────

const euro = (n) => `€${Math.round(n).toLocaleString()}`;
function esc(s) {
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

let _open = false;

export function toggleSidebar() {
    _open = !_open;
    document.getElementById('sidebar').classList.toggle('open', _open);
    document.getElementById('sidebarBackdrop').classList.toggle('open', _open);
    document.getElementById('sidebar').setAttribute('aria-hidden', String(!_open));
    if (_open) {
        syncSettings();
        loadReports();
    }
}

// Reflect current settings into the sidebar controls.
function syncSettings() {
    const goal = document.getElementById('sidebarGoal');
    if (goal && state.budgetGoal != null) goal.value = state.budgetGoal;
    const sw = document.getElementById('sidebarThemeSwitch');
    if (sw) sw.classList.toggle('on', document.documentElement.getAttribute('data-theme') === 'dark');
}

// Keep the theme switch visual in sync when theme flips elsewhere.
export function refreshSidebarTheme() {
    const sw = document.getElementById('sidebarThemeSwitch');
    if (sw) sw.classList.toggle('on', document.documentElement.getAttribute('data-theme') === 'dark');
}

function loadReports() {
    const host = document.getElementById('reportList');
    const count = document.getElementById('reportsCount');
    if (!host) return;
    host.innerHTML = `<div class="report-loading"><i class="fas fa-spinner fa-spin"></i></div>`;
    fetch('/api/reports')
        .then(r => r.json())
        .then(data => {
            const reports = data.reports || [];
            if (count) count.textContent = `${data.count} files · ${data.total_transactions.toLocaleString()} txns`;
            if (!reports.length) { host.innerHTML = `<div class="report-empty">No reports yet. Add one above.</div>`; return; }
            host.innerHTML = reports.map(renderReport).join('');
            host.querySelectorAll('.report-del').forEach(btn => {
                btn.addEventListener('click', () => deleteReport(btn.dataset.file));
            });
        })
        .catch(() => { host.innerHTML = `<div class="report-empty">Could not load reports.</div>`; });
}

function renderReport(r) {
    const period = (r.from_label && r.to_label)
        ? (r.from_label === r.to_label ? r.from_label : `${r.from_label} → ${r.to_label}`)
        : 'Unknown period';
    return `
        <div class="report-item">
            <div class="report-item-main">
                <div class="report-period">${esc(period)}</div>
                <div class="report-meta">${r.transactions} txns · ${euro(r.total)} · ${esc(r.filename)}</div>
            </div>
            <button class="report-del" data-file="${esc(r.filename)}" title="Delete report" aria-label="Delete report"><i class="fas fa-trash"></i></button>
        </div>
    `;
}

function deleteReport(filename) {
    if (!window.confirm(`Delete “${filename}”? This removes it from your data.`)) return;
    fetch('/api/reports/' + encodeURIComponent(filename), { method: 'DELETE' })
        .then(r => r.json())
        .then(d => {
            if (d.error) { showMsg(d.error, true); return; }
            loadReports();
            loadAllData();   // refresh the whole dashboard with the new dataset
        })
        .catch(() => showMsg('Could not delete the report.', true));
}

function showMsg(text, isError) {
    const el = document.getElementById('reportUploadMsg');
    if (!el) return;
    el.textContent = text;
    el.className = 'report-upload-msg' + (isError ? ' error' : ' ok');
    if (text) setTimeout(() => { if (el.textContent === text) el.textContent = ''; }, 5000);
}

function initUpload() {
    const input = document.getElementById('reportFile');
    if (!input) return;
    input.addEventListener('change', () => {
        const file = input.files && input.files[0];
        if (!file) return;
        showMsg('Uploading…', false);
        const fd = new FormData();
        fd.append('file', file);
        fetch('/api/reports', { method: 'POST', body: fd })
            .then(r => r.json())
            .then(d => {
                input.value = '';
                if (d.error) { showMsg(d.error, true); return; }
                const p = d.report;
                showMsg(`Added ${p.transactions} transactions (${p.from_label || '?'} → ${p.to_label || '?'}).`, false);
                loadReports();
                loadAllData();   // recompute everything with the newly-added data
            })
            .catch(() => { input.value = ''; showMsg('Upload failed.', true); });
    });
}

function initGoal() {
    const goal = document.getElementById('sidebarGoal');
    if (!goal) return;
    const save = () => {
        const v = Math.max(0, Math.round(parseFloat(goal.value) || 0));
        if (v === state.budgetGoal) return;
        state.budgetGoal = v;
        fetch('/api/settings/budget-goal', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ budget_goal: v }),
        }).then(() => loadAllData()).catch(() => {});
    };
    goal.addEventListener('change', save);
    goal.addEventListener('blur', save);
}

export function initSidebar() {
    initUpload();
    initGoal();
    // Esc closes the sidebar.
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && _open) toggleSidebar(); });
    // Keep the theme switch in sync when theme changes anywhere.
    document.addEventListener('themechange', refreshSidebarTheme);
}
