import { state, panelState, amountColor } from './state.js';
import { renderPhotoStrip } from './memories.js';
import { categoryBadgeHtml } from './overrides.js';
import { getCategoryColor, escapeHtml } from './constants.js';

// ── Unified Side Panel ─────────────────────────
// Panel holds an unfiltered list of transactions plus filter/selection state.

export function txKey(tx) {
    // Escape pipes in name/category so distinct transactions can't collide into the
    // same key (e.g. merchant "a" + category "b" vs merchant "a|b" + category "c").
    const esc = (s) => String(s).replace(/\|/g, '\\|');
    return `${tx.date}|${esc(tx.merchant)}|${tx.amount}|${esc(tx.category)}`;
}

export function openPanel({ title, subtitle = null, transactions, showDate = true, dateForPhotos = null, category = null, calendarCtx = undefined, selectedDay = null, dayGroupCategory = null, dayGroupKeys = null, backCategory = null }) {
    panelState.title = title;
    panelState.allTxs = [...transactions];
    panelState.showDate = showDate;
    panelState.category = category;
    // Calendar context persists across day drill-downs. `undefined` = keep whatever
    // is set (used when we don't want to touch it); pass `null` to explicitly clear.
    // A brand-new context resets which month the calendar shows (→ its busiest month).
    if (calendarCtx !== undefined) {
        panelState.calendarCtx = calendarCtx;
        panelState.calMonth = null;
    }
    panelState.selectedDay = selectedDay;
    panelState.dayGroupCategory = dayGroupCategory;  // group this category first in a day view
    panelState.dayGroupKeys = dayGroupKeys;          // virtual-category membership by txKey (suggested categories)
    panelState.backCategory = backCategory;          // show a "back to category" link
    panelState.filter = { q: '' };
    panelState.sort = 'amount-desc';
    panelState.selected = new Set();

    // Header: title (category / day) + optional subtitle (the specific day when
    // drilling in from a category calendar).
    document.getElementById('sidePanelTitle').textContent = title;
    const subEl = document.getElementById('sidePanelSubtitle');
    if (subtitle) {
        subEl.textContent = subtitle;
        subEl.style.display = 'block';
    } else {
        subEl.textContent = '';
        subEl.style.display = 'none';
    }
    document.getElementById('panelFilters').style.display = 'flex';
    document.getElementById('panelSearchInput').value = '';
    document.querySelectorAll('.panel-filters .sort-btn').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-sort') === 'amount-desc');
    });

    const strip = document.getElementById('panelPhotoStrip');
    if (dateForPhotos) {
        strip.style.display = 'block';
        renderPhotoStrip(dateForPhotos);
    } else {
        strip.style.display = 'none';
        strip.innerHTML = '';
    }

    renderPanelBody();
    updateSelectionBar();
    document.getElementById('sidePanel').classList.add('open');
    const scrim = document.getElementById('sidePanelScrim');
    if (scrim) scrim.classList.add('open');
}

export function applyPanelFilters() {
    const { q } = panelState.filter;
    const ql = q.trim().toLowerCase();
    const filtered = panelState.allTxs.filter(tx => {
        if (!ql) return true;
        return `${tx.merchant} ${tx.category}`.toLowerCase().includes(ql);
    });
    const cmp = panelState.sort === 'amount-asc'
        ? (a, b) => a.amount - b.amount
        : (a, b) => b.amount - a.amount;
    return filtered.sort(cmp);
}

export function setPanelSort(mode) {
    panelState.sort = mode;
    document.querySelectorAll('.panel-filters .sort-btn').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-sort') === mode);
    });
    renderPanelBody();
}

// Month calendar showing which days had a transaction in this category.
// Driven by panelState.calendarCtx = {category, txs} so it PERSISTS when the
// user drills into a specific day (which swaps the transaction list but keeps
// the calendar context). Weekends are subtly tinted; the open day is ringed.
function renderCategoryCalendar() {
    const ctx = panelState.calendarCtx;
    if (!ctx || !ctx.txs || !ctx.txs.length) return '';

    // A category can span MANY months, but a calendar shows one — so it's navigable.
    // Months that actually have transactions, sorted ascending.
    const monthsPresent = [...new Set(ctx.txs.map(t => t.date.slice(0, 7)))].sort();
    // Per-month totals, to default to the busiest month and show a per-month count.
    const perMonthCount = {};
    for (const t of ctx.txs) {
        const mk = t.date.slice(0, 7);
        perMonthCount[mk] = (perMonthCount[mk] || 0) + 1;
    }
    const busiest = monthsPresent.reduce((a, b) => (perMonthCount[b] > (perMonthCount[a] || 0) ? b : a), monthsPresent[0]);

    // Which month to display: the explicitly-navigated one (if it has data), else the
    // day being viewed, else the currently-selected app month, else the busiest month.
    let month = panelState.calMonth;
    if (!month || !monthsPresent.includes(month)) {
        if (panelState.selectedDay && monthsPresent.includes(panelState.selectedDay.slice(0, 7))) {
            month = panelState.selectedDay.slice(0, 7);
        } else if (state.selectedMonth && monthsPresent.includes(state.selectedMonth)) {
            month = state.selectedMonth;
        } else {
            month = busiest;
        }
    }
    panelState.calMonth = month;   // remember so prev/next have a base

    const monthIdx = monthsPresent.indexOf(month);
    const hasPrev = monthIdx > 0;
    const hasNext = monthIdx < monthsPresent.length - 1;

    // Sum per day for this month.
    const perDay = {};   // day-of-month -> total amount
    for (const t of ctx.txs) {
        if (t.date.slice(0, 7) !== month) continue;
        const day = parseInt(t.date.slice(8, 10), 10);
        perDay[day] = (perDay[day] || 0) + t.amount;
    }

    const [y, m] = month.split('-').map(Number);
    const daysInMonth = new Date(y, m, 0).getDate();
    // JS getDay: 0=Sun..6=Sat. Convert to Monday-first offset (0=Mon..6=Sun).
    const firstDow = (new Date(y, m - 1, 1).getDay() + 6) % 7;
    // Use an explicit color if the ctx supplies one (e.g. a suggested category that
    // isn't in the catalog yet), else fall back to the category's catalog color.
    const color = ctx.color || getCategoryColor(ctx.category);
    const monthLabel = new Date(y, m - 1, 1).toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
    const activeDays = Object.keys(perDay).length;
    const selDay = (panelState.selectedDay && panelState.selectedDay.slice(0, 7) === month)
        ? parseInt(panelState.selectedDay.slice(8, 10), 10) : null;

    const dow = ['M', 'T', 'W', 'T', 'F', 'S', 'S'];
    let cells = dow.map((d, i) => `<div class="cal-dow${i >= 5 ? ' cal-weekend' : ''}">${d}</div>`).join('');
    for (let i = 0; i < firstDow; i++) cells += `<div class="cal-cell cal-empty"></div>`;
    for (let day = 1; day <= daysInMonth; day++) {
        const amt = perDay[day];
        const iso = `${month}-${String(day).padStart(2, '0')}`;
        // Monday-first column index: (firstDow + day - 1) % 7 → 5,6 are Sat,Sun.
        const col = (firstDow + day - 1) % 7;
        const weekendCls = col >= 5 ? ' cal-weekend' : '';
        const selCls = day === selDay ? ' cal-selected' : '';
        if (amt) {
            // Clickable: opens the full day view (all purchases that day + photos).
            // stopPropagation so the re-render doesn't let this bubble to the
            // outside-click handler (the clicked cell gets detached mid-click).
            cells += `<div class="cal-cell cal-on${weekendCls}${selCls}" style="--cal-color:${color};"
                title="${monthLabel.split(' ')[0]} ${day}: €${amt.toFixed(2)} — click for the day"
                onclick="event.stopPropagation(); App.showDayDetails('${iso}', true)">${day}</div>`;
        } else {
            cells += `<div class="cal-cell${weekendCls}${selCls}">${day}</div>`;
        }
    }

    // Total active days across ALL months (so the header isn't mistaken for the whole).
    const totalDays = new Set(ctx.txs.map(t => t.date)).size;
    const spanHint = monthsPresent.length > 1
        ? `<span class="cal-legend"><span class="cal-swatch" style="background:${color}"></span>${totalDays} days · ${monthsPresent.length} mo</span>`
        : `<span class="cal-legend"><span class="cal-swatch" style="background:${color}"></span>${activeDays} ${activeDays === 1 ? 'day' : 'days'}</span>`;

    return `
        <div class="cat-calendar">
            <div class="cal-head">
                <span class="cal-nav">
                    <button class="cal-arrow" ${hasPrev ? '' : 'disabled'} title="Previous month"
                        onclick="event.stopPropagation(); App.calStepMonth(-1)"><i class="fas fa-chevron-left"></i></button>
                    <span class="cal-title">${monthLabel}</span>
                    <button class="cal-arrow" ${hasNext ? '' : 'disabled'} title="Next month"
                        onclick="event.stopPropagation(); App.calStepMonth(1)"><i class="fas fa-chevron-right"></i></button>
                </span>
                ${spanHint}
            </div>
            <div class="cal-grid">${cells}</div>
        </div>
    `;
}

// Step the calendar to the previous (-1) / next (+1) month that has transactions.
export function calStepMonth(delta) {
    const ctx = panelState.calendarCtx;
    if (!ctx || !ctx.txs) return;
    const monthsPresent = [...new Set(ctx.txs.map(t => t.date.slice(0, 7)))].sort();
    const cur = panelState.calMonth || monthsPresent[monthsPresent.length - 1];
    const idx = monthsPresent.indexOf(cur);
    const next = monthsPresent[idx + delta];
    if (!next) return;   // at an end — arrow is disabled anyway
    panelState.calMonth = next;
    renderPanelCalendar();
}

// Render the calendar into its OWN fixed container above the photos, so it never
// shifts up/down when a day's photo strip / transaction list changes height.
function renderPanelCalendar() {
    const host = document.getElementById('panelCalendar');
    const html = renderCategoryCalendar();
    if (html) {
        host.innerHTML = html;
        host.style.display = 'block';
    } else {
        host.innerHTML = '';
        host.style.display = 'none';
    }
}

export function renderPanelBody() {
    const content = document.getElementById('sidePanelContent');
    const filtered = applyPanelFilters();
    const total = filtered.reduce((s, tx) => s + tx.amount, 0);
    const totalUnfiltered = panelState.allTxs.reduce((s, tx) => s + tx.amount, 0);
    const isFiltered = filtered.length !== panelState.allTxs.length;

    // "Back to category" link when we've drilled from a category calendar into a day.
    const backHtml = panelState.backCategory
        ? (() => {
              // Escape for BOTH the JS-string literal and the HTML attribute it sits in,
              // so a category name with quotes/brackets can't break out of the handler.
              const safeCat = escapeHtml(panelState.backCategory.replace(/\\/g, '\\\\').replace(/'/g, "\\'"));
              const backLabel = escapeHtml(panelState.backCategory.replace('_', ' ').replace('-', ' '));
              return `<div class="panel-back" onclick="event.stopPropagation(); App.showCategoryDetails('${safeCat}')">
               <i class="fas fa-chevron-left"></i> Back to ${backLabel}
           </div>`;
          })()
        : '';

    const summaryHtml = `
        ${backHtml}
        <div class="category-summary">
            <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                <span style="font-size: 0.82rem; color: var(--text-2);">${isFiltered ? 'Filtered total' : 'Total'}</span>
                <span style="font-family: var(--font-serif); font-size: 1.1rem; font-weight: 500;">€${total.toFixed(2)}</span>
            </div>
            <div style="display: flex; justify-content: space-between;">
                <span style="font-size: 0.82rem; color: var(--text-2);">Transactions</span>
                <span style="font-size: 0.88rem; font-weight: 500;">
                    ${filtered.length}${isFiltered ? ` <span style="color:var(--text-3);">of ${panelState.allTxs.length}</span>` : ''}
                </span>
            </div>
            ${isFiltered ? `
                <div style="display: flex; justify-content: space-between; margin-top: 8px; padding-top: 8px; border-top: 1px dashed var(--border-light);">
                    <span style="font-size: 0.75rem; color: var(--text-3);">All items total</span>
                    <span style="font-size: 0.82rem; color: var(--text-3);">€${totalUnfiltered.toFixed(2)}</span>
                </div>
            ` : ''}
        </div>
        ${renderPanelInsights(filtered)}
    `;

    renderPanelCalendar();

    if (filtered.length === 0) {
        content.innerHTML = summaryHtml + `
            <div style="text-align: center; padding: 40px 12px; color: var(--text-3); font-size: 0.85rem;">
                No transactions match the current filters.
            </div>
        `;
        return;
    }

    const rowHtml = (tx) => {
        const k = txKey(tx);
        const isSel = panelState.selected.has(k);
        const dateStr = panelState.showDate
            ? `<div style="font-size: 0.75rem; color: var(--text-3); margin-top: 2px;">${new Date(tx.date).toLocaleDateString()}</div>`
            : '';
        return `
            <div class="transaction-item ${isSel ? 'selected' : ''}" data-key="${k}" onclick="App.toggleTxSelection(this)">
                <div style="display: flex; justify-content: space-between; align-items: start; gap: 12px;">
                    <div style="min-width: 0; flex: 1;">
                        <div style="font-weight: 500; font-size: 0.88rem; margin-bottom: 4px;">${escapeHtml(tx.merchant)}</div>
                        ${categoryBadgeHtml(tx)}
                        ${dateStr}
                    </div>
                    <div style="font-family: var(--font-serif); font-weight: 400; color: ${amountColor(tx)}; font-size: 0.95rem; white-space: nowrap;">€${tx.amount.toFixed(2)}</div>
                </div>
            </div>
        `;
    };

    const sectionLabel = (text, extra = '') =>
        `<div class="panel-section-label">${text}${extra}</div>`;

    let listHtml;
    const grp = panelState.dayGroupCategory;
    if (grp) {
        // Day view opened from a category: show that category's items first,
        // then a divider, then everything else that day. For a VIRTUAL (suggested)
        // category, membership is defined by the suggestion's own transactions
        // (matched by txKey) — the items still live in `other`, so tx.category
        // wouldn't match the suggested name.
        const keys = panelState.dayGroupKeys;
        const inGroup = keys ? (t => keys.has(txKey(t))) : (t => t.category === grp);
        const inCat = filtered.filter(inGroup);
        const others = filtered.filter(t => !inGroup(t));
        const catName = grp.replace('_', ' ').replace('-', ' ');
        listHtml =
            sectionLabel(`${catName} (${inCat.length})`) +
            (inCat.length ? inCat.map(rowHtml).join('')
                          : `<div class="panel-empty-note">Nothing in ${catName} this day.</div>`) +
            (others.length ? sectionLabel(`Everything else that day (${others.length})`) + others.map(rowHtml).join('') : '');
    } else {
        listHtml =
            sectionLabel(`Transactions (${filtered.length})`,
                `<span style="float: right; font-weight: 400; text-transform: none; letter-spacing: 0; color: var(--text-3);">Click to select</span>`) +
            filtered.map(rowHtml).join('');
    }

    content.innerHTML = summaryHtml + listHtml;
}

// Small, subtle insight chips derived from the panel's transaction set. These are
// meant to be interesting-at-a-glance (biggest buy, average, busiest merchant/day,
// weekend share) without cluttering — 3–4 compact chips.
function renderPanelInsights(txs) {
    if (!txs || txs.length < 2) return '';   // need a couple of items to be interesting
    const total = txs.reduce((s, t) => s + t.amount, 0);
    const avg = total / txs.length;

    // Biggest single purchase.
    const biggest = txs.reduce((a, b) => (b.amount > a.amount ? b : a));

    // Most frequent merchant (by count).
    const byMerchant = {};
    for (const t of txs) byMerchant[t.merchant] = (byMerchant[t.merchant] || 0) + 1;
    const topMerchant = Object.entries(byMerchant).sort((a, b) => b[1] - a[1])[0];

    // Weekend share (Sat/Sun) of spend — a fun behavioural nugget.
    let weekendSpend = 0;
    for (const t of txs) {
        const dow = new Date(t.date + 'T00:00:00').getDay();   // 0=Sun,6=Sat
        if (dow === 0 || dow === 6) weekendSpend += t.amount;
    }
    const weekendPct = total > 0 ? Math.round((weekendSpend / total) * 100) : 0;

    const chips = [];
    chips.push({ icon: 'fa-arrow-up', label: 'Biggest', value: `€${biggest.amount.toFixed(0)}`, title: `${biggest.merchant} — €${biggest.amount.toFixed(2)}` });
    chips.push({ icon: 'fa-divide', label: 'Average', value: `€${avg.toFixed(0)}` });
    if (topMerchant && topMerchant[1] > 1) {
        chips.push({ icon: 'fa-repeat', label: topMerchant[0], value: `${topMerchant[1]}×`, title: `${topMerchant[1]} visits to ${topMerchant[0]}` });
    }
    if (weekendSpend > 0) {
        chips.push({ icon: 'fa-umbrella-beach', label: 'Weekends', value: `${weekendPct}%` });
    }

    return `
        <div class="panel-insights">
            ${chips.map(c => `
                <div class="panel-insight" ${c.title ? `title="${escapeHtml(c.title)}"` : ''}>
                    <i class="fas ${c.icon}"></i>
                    <div class="pi-body">
                        <span class="pi-value">${c.value}</span>
                        <span class="pi-label">${escapeHtml(c.label)}</span>
                    </div>
                </div>
            `).join('')}
        </div>
    `;
}

export function toggleTxSelection(el) {
    const key = el.getAttribute('data-key');
    if (panelState.selected.has(key)) {
        panelState.selected.delete(key);
        el.classList.remove('selected');
    } else {
        panelState.selected.add(key);
        el.classList.add('selected');
    }
    updateSelectionBar();
}

export function updateSelectionBar() {
    const bar = document.getElementById('selectionBar');
    const count = panelState.selected.size;
    if (count === 0) {
        bar.classList.remove('visible');
        return;
    }
    let sum = 0;
    for (const tx of panelState.allTxs) {
        if (panelState.selected.has(txKey(tx))) sum += tx.amount;
    }
    document.getElementById('selCount').textContent = `${count} selected`;
    document.getElementById('selTotal').textContent = `€${sum.toFixed(2)}`;
    bar.classList.add('visible');
}

export function clearSelection() {
    panelState.selected = new Set();
    document.querySelectorAll('.transaction-item.selected').forEach(el => el.classList.remove('selected'));
    updateSelectionBar();
}

export function onPanelFilterChange() {
    panelState.filter.q = document.getElementById('panelSearchInput').value;
    renderPanelBody();
}

// Reflect a category override inside the open panel without a full re-fetch.
// Because an override also learns a merchant rule, ALL transactions from the same
// merchant move together — so in a category-filtered panel we drop every matching
// merchant row (not just the exact one). Returns true if the clicked row leaves.
export function applyOverrideToPanel(tx, newCategory) {
    if (!document.getElementById('sidePanel').classList.contains('open')) return false;
    const sameMerchant = (t) => t.merchant === tx.merchant;
    const leaves = panelState.category && newCategory && newCategory !== panelState.category;
    if (leaves) {
        panelState.allTxs = panelState.allTxs.filter(t => !sameMerchant(t));
    } else {
        panelState.allTxs.forEach(t => {
            if (sameMerchant(t)) { t.category = newCategory || t.category; t.overridden = !!newCategory; }
        });
    }
    return leaves;
}

// Re-render the panel body after an optimistic change.
export function refreshPanelBody() {
    if (document.getElementById('sidePanel').classList.contains('open')) renderPanelBody();
}

export function showCategoryDetails(category) {
    const qs = state.excludeOutliers ? '?exclude_outliers=1' : '';
    let apiEndpoint;
    if (state.currentView === 'monthly') {
        apiEndpoint = state.selectedMonth
            ? `/api/current-month-category-transactions/${category}/${state.selectedMonth}${qs}`
            : `/api/current-month-category-transactions/${category}${qs}`;
    } else {
        apiEndpoint = `/api/category-transactions/${category}${qs}`;
    }

    fetch(apiEndpoint)
        .then(r => r.json())
        .then(data => {
            openPanel({
                title: data.category.replace('_', ' ').replace('-', ' '),
                transactions: data.transactions,
                showDate: true,
                category: data.category,
                // Seed the persistent calendar with this category's transactions.
                calendarCtx: { category: data.category, txs: data.transactions },
                selectedDay: null,
            });
        })
        .catch(error => console.error('Error loading category details:', error));
}

// `keepCalendar` = true when drilling in from the category calendar (preserve it);
// false/omitted when opening a day from the chart/timeline (clear any calendar).
export function showDayDetails(date, keepCalendar = false) {
    // The category we came from (if any) — used to group the day's list and to
    // offer a "back to category" link.
    const ctx = keepCalendar ? panelState.calendarCtx : null;
    const fromCategory = ctx ? ctx.category : null;
    // A virtual (suggested) category groups by its own transactions, not tx.category.
    const groupKeys = ctx && ctx.virtual && ctx.txs
        ? new Set(ctx.txs.map(txKey))
        : null;

    const openWith = (monthDetails) => {
        const allMonthTxs = Object.values(monthDetails.category_transactions || {}).flat();
        const dayTxs = allMonthTxs.filter(tx => tx.date === date);
        const d = new Date(date);
        const dayLabel = d.toLocaleDateString(undefined, { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' });
        const catLabel = fromCategory ? fromCategory.replace('_', ' ').replace('-', ' ') : null;
        openPanel({
            title: catLabel || dayLabel,
            subtitle: catLabel ? dayLabel : null,
            transactions: dayTxs, showDate: false, dateForPhotos: date,
            calendarCtx: keepCalendar ? undefined : null,  // undefined = keep, null = clear
            selectedDay: keepCalendar ? date : null,
            dayGroupCategory: fromCategory,   // group this category first, then a divider
            dayGroupKeys: groupKeys,          // virtual category: membership by txKey
            // A suggested category has no real page to go "back" to — only real ones do.
            backCategory: groupKeys ? null : fromCategory,
        });
    };

    const month = date.slice(0, 7);
    // Use the already-loaded month if it matches; otherwise fetch that month's details
    // (e.g. clicking a calendar day for a month other than the current selection).
    if (state.currentMonthDetails && state.currentMonthDetails.month === month) {
        openWith(state.currentMonthDetails);
    } else {
        const qs = state.excludeOutliers ? '?exclude_outliers=1' : '';
        fetch(`/api/current-month-details/${month}${qs}`)
            .then(r => r.json())
            .then(openWith)
            .catch(error => console.error('Error loading day details:', error));
    }
}
