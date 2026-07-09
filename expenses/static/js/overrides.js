// ── Manual category overrides ──────────────────
// Clicking a transaction's category badge opens a picker of all categories.
// Selecting one reclassifies that transaction (persisted server-side), then the
// whole dashboard reloads so the item moves out of its old category everywhere.
import { CATEGORY_COLORS, CUSTOM_CATEGORIES, customCategoryNames, getCategoryColor, getCategoryBg, getCategoryIcon } from './constants.js';
import { loadAllData } from './loading.js';
import { applyOverrideToPanel, refreshPanelBody } from './panel.js';
import { openNewCategoryDialog } from './categories.js';

// Built-in categories (display form). Custom categories are appended at runtime.
const BUILTIN_CATEGORIES = [
    'groceries', 'eating out', 'bolt', 'car wash', 'gas stations',
    'parking', 'nabytok', 'oil partner', 'other',
];
// Full list including any user-added custom categories (with their real casing).
function categoryList() {
    const extra = customCategoryNames();
    return [...BUILTIN_CATEGORIES, ...extra.filter(c => !BUILTIN_CATEGORIES.includes(c))];
}

let activePopover = null;

function closePicker() {
    if (activePopover) {
        activePopover.remove();
        activePopover = null;
        document.removeEventListener('click', onDocClick, true);
        document.removeEventListener('keydown', onKey, true);
        window.removeEventListener('scroll', closePicker, true);
    }
}

function onDocClick(e) {
    if (activePopover && !activePopover.contains(e.target)) closePicker();
}
function onKey(e) {
    if (e.key === 'Escape') closePicker();
}

// Open the picker anchored to the clicked badge. `tx` needs {date, merchant, amount, category}.
export function openCategoryPicker(tx, anchorEl) {
    closePicker();

    const pop = document.createElement('div');
    pop.className = 'cat-picker';
    pop.innerHTML = `
        <div class="cat-picker-head">Move to…</div>
        ${categoryList().map(cat => {
            const isCurrent = cat.toLowerCase() === String(tx.category).toLowerCase();
            return `
                <button class="cat-picker-item${isCurrent ? ' current' : ''}" data-cat="${cat}" ${isCurrent ? 'disabled' : ''}>
                    <span class="cat-dot" style="background:${getCategoryColor(cat)}"><i class="fas ${getCategoryIcon(cat)}"></i></span>
                    <span class="cat-name">${cat}</span>
                    ${isCurrent ? '<span class="cat-check">current</span>' : ''}
                </button>
            `;
        }).join('')}
        ${tx.overridden ? `<button class="cat-picker-item cat-picker-reset" data-cat="__reset__">
            <span class="cat-dot" style="background:var(--text-3)"><i class="fas fa-rotate-left"></i></span>
            <span class="cat-name">Reset to automatic</span>
        </button>` : ''}
        <button class="cat-picker-item cat-picker-add" data-cat="__add__">
            <span class="cat-dot" style="background:var(--primary)"><i class="fas fa-plus"></i></span>
            <span class="cat-name">New category…</span>
        </button>
        <div class="cat-picker-note"><i class="fas fa-infinity"></i> Applies to all "${esc(tx.merchant)}" transactions</div>
    `;
    document.body.appendChild(pop);

    // Position below the badge, flipping above if it would overflow the viewport.
    const r = anchorEl.getBoundingClientRect();
    const popH = pop.offsetHeight;
    let top = r.bottom + 6;
    if (top + popH > window.innerHeight - 8) top = Math.max(8, r.top - popH - 6);
    let left = r.left;
    const popW = pop.offsetWidth;
    if (left + popW > window.innerWidth - 8) left = window.innerWidth - popW - 8;
    pop.style.top = `${top}px`;
    pop.style.left = `${left}px`;

    pop.querySelectorAll('.cat-picker-item').forEach(btn => {
        if (btn.hasAttribute('disabled')) return;
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const cat = btn.getAttribute('data-cat');
            closePicker();
            if (cat === '__add__') {
                // Open the "new category" dialog; on create, move this tx into it.
                openNewCategoryDialog((newCat) => reclassify(tx, newCat, anchorEl));
            } else {
                reclassify(tx, cat === '__reset__' ? '' : cat, anchorEl);
            }
        });
    });

    activePopover = pop;
    // Defer listener attach so this opening click doesn't immediately close it.
    setTimeout(() => {
        document.addEventListener('click', onDocClick, true);
        document.addEventListener('keydown', onKey, true);
        window.addEventListener('scroll', closePicker, true);
    }, 0);
}

function reclassify(tx, category, anchorEl) {
    // Optimistic: reflect the change in the open side panel immediately + animate.
    const leaves = applyOverrideToPanel(tx, category);
    const row = anchorEl && anchorEl.closest('.transaction-item');

    if (leaves && row) {
        // Animate the row sliding/fading out, then re-render the panel list.
        animateRowOut(row, () => refreshPanelBody());
    } else {
        // Item stays visible (day view, or moved within same category filter):
        // just re-render so the badge + arrow update in place.
        refreshPanelBody();
    }

    // Persist + refresh the rest of the dashboard (tables, charts, totals) in the background.
    fetch('/api/override', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ date: tx.date, merchant: tx.merchant, amount: tx.amount, category }),
    })
        .then(r => r.json())
        .then(data => {
            const n = data.affected || 1;
            if (category) {
                const catName = category.replace('_', ' ').replace('-', ' ');
                showToast(`Moved ${n} ${n === 1 ? 'transaction' : 'transactions'} to <b>${catName}</b>`);
            } else {
                showToast(`Reset ${n} ${n === 1 ? 'transaction' : 'transactions'} to automatic`);
            }
            loadAllData();
        })
        .catch(err => console.error('Override failed:', err));
}

// Brief toast notification (auto-dismisses). Reused for override feedback.
let toastTimer = null;
export function showToast(html) {
    let toast = document.getElementById('appToast');
    if (!toast) {
        toast = document.createElement('div');
        toast.id = 'appToast';
        toast.className = 'app-toast';
        document.body.appendChild(toast);
    }
    toast.innerHTML = `<i class="fas fa-check-circle"></i> <span>${html}</span>`;
    // restart the enter animation
    toast.classList.remove('show');
    void toast.offsetWidth;
    toast.classList.add('show');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => toast.classList.remove('show'), 3200);
}

// Collapse a transaction row out of the list with a quick slide+fade.
function animateRowOut(row, done) {
    row.style.overflow = 'hidden';
    row.style.transition = 'none';
    const h = row.offsetHeight;
    row.style.height = h + 'px';
    // Next frame: animate to zero height + translated/faded.
    requestAnimationFrame(() => {
        row.style.transition = 'height 0.28s ease, opacity 0.28s ease, transform 0.28s ease, margin 0.28s ease, padding 0.28s ease';
        row.style.height = '0px';
        row.style.opacity = '0';
        row.style.transform = 'translateX(24px)';
        row.style.marginTop = '0px';
        row.style.marginBottom = '0px';
        row.style.paddingTop = '0px';
        row.style.paddingBottom = '0px';
    });
    setTimeout(() => { if (done) done(); }, 300);
}

// ── Editable category badge ────────────────────
// HTML-escape for safe embedding in attributes/text.
function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;')
        .replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/'/g, '&#39;');
}

// Build an editable category badge. Identity is carried in data-* attributes so
// merchants with quotes/apostrophes are safe. A ✎ hint + (if overridden) an
// override arrow are shown. Click is handled by the delegated listener below.
export function categoryBadgeHtml(tx) {
    const cls = tx.category.replace(' ', '-').replace('_', '-');
    const marker = tx.overridden
        ? '<i class="fas fa-arrow-turn-up cat-override-mark" title="Manually recategorized"></i>'
        : '';
    const icon = `<i class="fas ${getCategoryIcon(tx.category)} cat-badge-icon"></i>`;
    // Inline color/bg so CUSTOM categories (which have no hardcoded CSS class) are
    // styled correctly too, not just the built-in ones.
    const style = `background:${getCategoryBg(tx.category)};color:${getCategoryColor(tx.category)};`;
    return `<span class="category-badge editable ${cls}" style="${style}"
        data-ovr="1"
        data-date="${esc(tx.date)}"
        data-merchant="${esc(tx.merchant)}"
        data-amount="${tx.amount}"
        data-category="${esc(tx.category)}"
        data-overridden="${tx.overridden ? '1' : ''}"
        title="Click to recategorize">${marker}${icon}${esc(tx.category.replace('_', ' '))}<i class="fas fa-pen cat-edit-hint"></i></span>`;
}

// One delegated listener for every editable badge on the page.
export function initOverrideDelegation() {
    if (document.body.dataset.ovrBound === '1') return;
    document.body.dataset.ovrBound = '1';
    document.addEventListener('click', (e) => {
        const badge = e.target.closest('.category-badge.editable[data-ovr="1"]');
        if (!badge) return;
        e.stopPropagation();
        openCategoryPicker({
            date: badge.dataset.date,
            merchant: badge.dataset.merchant,
            amount: Number(badge.dataset.amount),
            category: badge.dataset.category,
            overridden: badge.dataset.overridden === '1',
        }, badge);
    }, true);
}
