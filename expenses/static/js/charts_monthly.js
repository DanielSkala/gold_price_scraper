import { state } from './state.js';
import { getCategoryColor, getCategoryIcon, isEditableCategory, isDeletableCategory, plotlyFont, plotlyGrid, chartBg, hoverLabel, isDark, PLOTLY_CONFIG } from './constants.js';
import { renderMonthMemories } from './memories.js';
import { showCategoryDetails, showDayDetails } from './panel.js';
import { renderMonthlyTransactionsTable } from './transactions.js';

// ── Monthly Detail Charts ──────────────────────
export function renderMonthlyDetailCharts() {
    if (!state.currentMonthDetails || !state.currentMonthDetails.month) return;

    const monthLabel = new Date(state.currentMonthDetails.month + '-01').toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
    document.getElementById('monthlyTransactionsTitle').textContent = `${monthLabel} Transactions`;

    renderMonthHero(monthLabel);
    renderMonthMemories(state.currentMonthDetails.month, monthLabel, state.currentMonthDetails.daily_spending || {});

    // Pie chart
    const cats = Object.entries(state.currentMonthDetails.category_totals).filter(([_, v]) => v > 0).sort((a, b) => b[1] - a[1]);
    if (cats.length > 0) {
        const monthPieTotal = cats.reduce((s, [, v]) => s + v, 0);
        Plotly.newPlot('currentMonthChart', [{
            labels: cats.map(([c]) => c.replace('_', ' ').replace('-', ' ')),
            values: cats.map(([_, v]) => v),
            type: 'pie',
            hole: 0.52,
            sort: false,
            marker: { colors: cats.map(([c]) => getCategoryColor(c)), line: { color: chartBg(), width: 2 } },
            // Show category name + percent on each slice's leader line, so you can read which is which.
            texttemplate: '%{label}<br>%{percent}',
            textposition: 'outside',
            textfont: { size: 10, color: plotlyFont().color },
            automargin: true,
            hovertemplate: '<b>%{label}</b><br>€%{value:,.0f} · %{percent}<extra></extra>'
        }], {
            margin: { t: 20, r: 20, b: 20, l: 20 },
            showlegend: false,
            plot_bgcolor: chartBg(), paper_bgcolor: chartBg(),
            font: plotlyFont(),
            hoverlabel: hoverLabel(),
            annotations: [{
                text: `<span style="font-size:0.8rem;color:#a8a29e">Total</span><br><b>€${monthPieTotal.toLocaleString('en', { maximumFractionDigits: 0 })}</b>`,
                showarrow: false, font: { size: 16, color: (isDark() ? '#f5f1ea' : '#1c1917'), family: '-apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", system-ui, sans-serif' }, x: 0.5, y: 0.5
            }]
        }, PLOTLY_CONFIG);

        document.getElementById('currentMonthChart').on('plotly_click', function(data) {
            showCategoryDetails(cats[data.points[0].pointNumber][0]);
        });
    }

    renderDailyChart();
    if (state.currentMonthDetails.daily_spending && Object.keys(state.currentMonthDetails.daily_spending).length) {
        renderDailyPhotoRow(state.currentMonthDetails.month);
    }

    renderCategoryBreakdown();
    renderMonthlyTransactionsTable();
}

// Switch the daily chart between 'amount' (blue/red shades) and 'stacked' (by category).
export function setDailyMode(mode) {
    state.dailyMode = mode;
    document.getElementById('dailyModeAmount').classList.toggle('active', mode === 'amount');
    document.getElementById('dailyModeStacked').classList.toggle('active', mode === 'stacked');
    renderDailyChart();
}

// The Daily spending chart. Two modes share the same day columns:
//  - amount:  one bar per day, blue tint by amount, red if overspend (>1σ)
//  - stacked: each day's bar stacked by category (same total height, category colors)
export function renderDailyChart() {
    const md = state.currentMonthDetails;
    if (!md || !md.daily_spending) return;
    const daily = Object.entries(md.daily_spending).sort((a, b) => a[0].localeCompare(b[0]));
    if (!daily.length) return;

    const dayDates = daily.map(([d]) => d);
    const dayLabels = daily.map(([d]) => String(parseInt(d.slice(8, 10), 10)));
    const dailyChart = document.getElementById('dailySpendingChart');
    Plotly.purge(dailyChart);

    // Rounded bar corners + tidy axes for a modern look.
    const commonLayout = {
        margin: { t: 16, r: 8, b: 28, l: 34 },
        xaxis: { tickfont: { size: 9 }, type: 'category', tickangle: 0, fixedrange: true, showgrid: false },
        yaxis: { tickfont: { size: 9 }, gridcolor: plotlyGrid(), tickprefix: '€', tickformat: ',.0f', fixedrange: true, zeroline: false },
        plot_bgcolor: chartBg(), paper_bgcolor: chartBg(),
        font: plotlyFont(),
        hoverlabel: hoverLabel(),
        bargap: 0.2,
        autosize: true,
    };

    let traces, layout;
    if (state.dailyMode === 'stacked') {
        // Build per-day totals per category from the month's grouped transactions.
        const cats = Object.keys(md.category_transactions || {});
        const byCat = {};
        cats.forEach(c => { byCat[c] = dayDates.map(() => 0); });
        for (const [cat, txs] of Object.entries(md.category_transactions || {})) {
            for (const tx of txs) {
                const di = dayDates.indexOf(tx.date);
                if (di !== -1) byCat[cat][di] += tx.amount;
            }
        }
        const orderedCats = cats
            .filter(c => byCat[c].some(v => v > 0))
            .sort((a, b) => byCat[b].reduce((s, v) => s + v, 0) - byCat[a].reduce((s, v) => s + v, 0));
        traces = orderedCats.map(cat => ({
            x: dayLabels,
            // 0 → null so empty categories neither stack a sliver nor show a "€0.00"
            // box in the unified hover — only categories with real spend appear.
            y: byCat[cat].map(v => v > 0 ? v : null),
            type: 'bar',
            name: cat.replace('_', ' ').replace('-', ' '),
            marker: { color: getCategoryColor(cat), line: { width: 0 } },
            customdata: dayDates,
            hovertemplate: `${cat.replace('_', ' ')}: <b>€%{y:,.2f}</b><extra></extra>`,
        }));
        // x-unified: one consolidated card per day (header = day), only nonzero rows.
        layout = { ...commonLayout, barmode: 'stack', showlegend: false, hovermode: 'x unified' };
    } else {
        const amounts = daily.map(([_, a]) => a);
        const mean = amounts.reduce((s, a) => s + a, 0) / amounts.length;
        const std = Math.sqrt(amounts.reduce((s, a) => s + (a - mean) ** 2, 0) / amounts.length);
        const overspend = mean + std;
        const maxAmount = Math.max(...amounts, 1);
        const lerp = (a, b, t) => Math.round(a + (b - a) * t);
        const tintFor = (a) => {
            const t = Math.min(a / maxAmount, 1);
            return `rgb(${lerp(0xdb, 0x25, t)},${lerp(0xea, 0x63, t)},${lerp(0xfe, 0xeb, t)})`;
        };
        traces = [{
            x: dayLabels,
            y: amounts,
            type: 'bar',
            marker: { color: amounts.map(a => a > overspend ? '#b91c1c' : tintFor(a)), line: { width: 0 } },
            text: amounts.map(a => `€${Math.round(a)}`),
            textposition: 'outside',
            textfont: { size: 8, color: '#78716c' },
            cliponaxis: false,
            customdata: dayDates,
            hovertemplate: '<b>%{customdata}</b><br>Spent  <b>€%{y:,.2f}</b><extra></extra>',
        }];
        layout = commonLayout;
    }

    Plotly.newPlot(dailyChart, traces, layout, PLOTLY_CONFIG).then(() => {
        // Photos hang under their bars; re-anchor them every time the plot lays out.
        _dailyDayDates = dayDates;
        layoutDailyPhotos();
    });

    dailyChart.on('plotly_click', function(data) {
        if (!data || !data.points || !data.points.length) return;
        const pt = data.points[0];
        const idx = pt.pointIndex ?? pt.pointNumber;
        const date = pt.customdata || (idx != null ? dayDates[idx] : null);
        if (date) showDayDetails(date);
    });
    dailyChart.on('plotly_hover', function() { dailyChart.style.cursor = 'pointer'; });
    dailyChart.on('plotly_unhover', function() { dailyChart.style.cursor = ''; });
    // Keep photos glued under their bars when the chart resizes (responsive relayout).
    dailyChart.on('plotly_afterplot', layoutDailyPhotos);
}

// Photos for the TOP-5 highest-spending days of the month, hung DIRECTLY UNDER the
// corresponding bar in the Daily spending chart (aligned to each bar's x-pixel).
// Only the days that mattered most get a photo — no per-day clutter, no scrolling.
// Uses the timeline endpoint (curated per-day photos). Cached per month.
const _dailyPhotoCache = {};
let _dailyDayDates = [];   // current chart's day dates, in x-axis order (index → date)

function renderDailyPhotoRow(month) {
    const host = document.getElementById('dailyPhotoRow');
    if (!host) return;

    // The 5 days with the most spend this month (only days that have spend at all).
    const spend = state.currentMonthDetails.daily_spending || {};
    const topDays = Object.entries(spend)
        .filter(([, amt]) => amt > 0)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5);
    const spendByDate = Object.fromEntries(topDays);
    const topSet = new Set(topDays.map(([d]) => d));

    const render = (days) => {
        const byDate = Object.fromEntries((days || []).map(dd => [dd.date, dd]));
        // Keep top-days order (highest spend first); only those with a photo.
        const cards = topDays
            .map(([date]) => byDate[date])
            .filter(dd => dd && dd.photos && dd.photos.length);
        if (!cards.length) { host.innerHTML = ''; host.style.display = 'none'; return; }
        host.style.display = 'block';
        host.innerHTML = cards.map(dd => {
            const p = dd.photos[0];
            const amt = spendByDate[dd.date] || 0;
            // data-date lets layoutDailyPhotos() find this card's bar to align under.
            return `
                <div class="daily-photo" data-date="${dd.date}" title="${dd.date}" onclick="App.showDayDetails('${dd.date}')">
                    <div class="daily-photo-img"><img src="/photo/${p.uuid}" loading="lazy" alt=""></div>
                    <span class="daily-photo-amt">€${Math.round(amt).toLocaleString()}</span>
                </div>
            `;
        }).join('');
        layoutDailyPhotos();
    };

    if (_dailyPhotoCache[month]) { render(_dailyPhotoCache[month]); return; }
    host.innerHTML = '';
    // Fetch only the top-5 days' photos (endpoint returns all curated days; we filter).
    fetch(`/api/photos-timeline/${month}?per_day=1`)
        .then(r => r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status)))
        .then(data => {
            const days = (data.days || []).filter(dd => topSet.has(dd.date));
            _dailyPhotoCache[month] = days;
            render(days);
        })
        .catch(() => { host.style.display = 'none'; });
}

// Position each top-day photo card exactly under its bar. Each card is sized to
// ONE COLUMN wide (the category-slot width), so its center lines up with the bar's
// center and adjacent cards tile edge-to-edge without overlapping — no nudging.
function layoutDailyPhotos() {
    const host = document.getElementById('dailyPhotoRow');
    const chart = document.getElementById('dailySpendingChart');
    if (!host || !chart || !chart._fullLayout) return;
    const xa = chart._fullLayout.xaxis;
    if (!xa || typeof xa.d2p !== 'function') return;
    const cards = [...host.querySelectorAll('.daily-photo')];
    if (!cards.length) return;

    // Width of a single category column, in pixels. On a category axis the spacing
    // between adjacent categories is constant, so d2p(1)-d2p(0) is one column.
    const n = _dailyDayDates.length;
    let colW = n > 1 ? Math.abs(xa.d2p(1) - xa.d2p(0)) : (xa._length || host.clientWidth);
    if (!isFinite(colW) || colW <= 0) colW = 48;

    // Hide the €amount labels when a column is too narrow to show them without
    // overlapping a neighbour — the aligned photo alone stays clean.
    const showAmt = colW >= 46;

    let maxBottom = 0;
    cards.forEach(card => {
        const idx = _dailyDayDates.indexOf(card.getAttribute('data-date'));
        if (idx === -1) { card.style.display = 'none'; return; }
        card.style.display = 'flex';
        // One column wide, centered on the bar's x-pixel.
        const center = xa._offset + xa.d2p(idx);
        card.style.width = `${colW}px`;
        card.style.left = `${Math.round(center - colW / 2)}px`;
        const amt = card.querySelector('.daily-photo-amt');
        if (amt) amt.style.display = showAmt ? '' : 'none';
        maxBottom = Math.max(maxBottom, card.offsetHeight);
    });
    // Give the (absolutely-positioned) cards room so the section reserves height.
    host.style.height = `${maxBottom || 84}px`;
}

// Prominent month header: month name + the key numbers for the month at a glance.
export function renderMonthHero(monthLabel) {
    const d = state.currentMonthDetails;
    const totals = d.category_totals || {};
    const monthTotal = Object.values(totals).reduce((s, v) => s + v, 0);
    const days = Object.keys(d.daily_spending || {}).length || 1;
    const avgDay = monthTotal / days;
    const top = Object.entries(totals).filter(([_, v]) => v > 0).sort((a, b) => b[1] - a[1])[0];
    const topName = top ? top[0].replace('_', ' ').replace('-', ' ') : '—';
    const txCount = d.total_transactions || 0;

    document.getElementById('monthHeroTitle').textContent = monthLabel;

    // Enable/disable the prev/next arrows at the ends of the available range.
    // available_months is newest-first: index 0 = newest, last = oldest.
    const months = d.available_months || [];
    const idx = months.indexOf(d.month);
    const prevBtn = document.getElementById('monthPrevBtn');
    const nextBtn = document.getElementById('monthNextBtn');
    if (prevBtn) prevBtn.disabled = idx === -1 || idx >= months.length - 1;  // no older month
    if (nextBtn) nextBtn.disabled = idx <= 0;                                 // no newer month

    document.getElementById('monthHeroStats').innerHTML = `
        <div class="month-hero-stat">
            <span class="mh-label">Total spent</span>
            <span class="mh-value">€${monthTotal.toLocaleString('en', { maximumFractionDigits: 0 })}</span>
        </div>
        <div class="month-hero-stat">
            <span class="mh-label">Avg / day</span>
            <span class="mh-value">€${avgDay.toLocaleString('en', { maximumFractionDigits: 0 })}</span>
        </div>
        <div class="month-hero-stat">
            <span class="mh-label">Top category</span>
            <span class="mh-value mh-value-sm" style="text-transform:capitalize;">${topName}</span>
        </div>
        <div class="month-hero-stat">
            <span class="mh-label">Transactions</span>
            <span class="mh-value">${txCount}</span>
        </div>
    `;

    renderMonthHighlights(d, monthTotal);
}

// A subtle strip of interesting month facts under the hero stats: busiest day,
// biggest single purchase, most-frequented merchant, and a quiet-days count.
function renderMonthHighlights(d, monthTotal) {
    const host = document.getElementById('monthHeroHighlights');
    if (!host) return;
    const allTxs = Object.values(d.category_transactions || {}).flat();
    if (allTxs.length < 2) { host.innerHTML = ''; host.style.display = 'none'; return; }

    // Busiest day (highest single-day spend).
    const daily = d.daily_spending || {};
    const busiest = Object.entries(daily).sort((a, b) => b[1] - a[1])[0];
    const busiestLabel = busiest
        ? new Date(busiest[0] + 'T00:00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
        : '—';

    // Biggest single purchase.
    const biggest = allTxs.reduce((a, b) => (b.amount > a.amount ? b : a));

    // Most-frequented merchant.
    const byMerchant = {};
    for (const t of allTxs) byMerchant[t.merchant] = (byMerchant[t.merchant] || 0) + 1;
    const topMerchant = Object.entries(byMerchant).sort((a, b) => b[1] - a[1])[0];

    // Days with zero spend in the month (a calm-vs-busy nugget).
    const [yy, mm] = d.month.split('-').map(Number);
    const daysInMonth = new Date(yy, mm, 0).getDate();
    const activeDays = Object.keys(daily).length;
    const zeroDays = Math.max(0, daysInMonth - activeDays);

    const facts = [
        { icon: 'fa-fire', text: `Busiest day <strong>${busiestLabel}</strong> · €${Math.round(busiest ? busiest[1] : 0)}` },
        { icon: 'fa-arrow-up', text: `Biggest buy <strong>${escapeHtmlM(biggest.merchant)}</strong> · €${Math.round(biggest.amount)}` },
    ];
    if (topMerchant && topMerchant[1] > 1) {
        facts.push({ icon: 'fa-repeat', text: `Most visited <strong>${escapeHtmlM(topMerchant[0])}</strong> · ${topMerchant[1]}×` });
    }
    facts.push({ icon: 'fa-mug-hot', text: `<strong>${zeroDays}</strong> no-spend ${zeroDays === 1 ? 'day' : 'days'}` });

    host.style.display = 'flex';
    host.innerHTML = facts.map(f => `
        <span class="mh-highlight"><i class="fas ${f.icon}"></i>${f.text}</span>
    `).join('');
}

function escapeHtmlM(s) {
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

export function renderCategoryBreakdown() {
    if (!state.currentMonthDetails) return;
    const container = document.getElementById('categoryBreakdown');
    const data = Object.entries(state.currentMonthDetails.category_totals).filter(([_, a]) => a > 0).sort((a, b) => b[1] - a[1]);
    const total = data.reduce((s, [_, a]) => s + a, 0);

    container.innerHTML = `
        <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 10px; margin-bottom: 16px;">
            ${data.map(([cat, amount]) => {
                const pct = ((amount / total) * 100).toFixed(1);
                const txCount = state.currentMonthDetails.category_transactions[cat]?.length || 0;
                const color = getCategoryColor(cat);
                const safe = cat.replace(/'/g, "\\'");
                // Every category can be edited; only 'other' can't be deleted or renamed.
                const editBtn = isEditableCategory(cat) ? `
                        <button class="cat-card-btn" title="Edit category"
                            onclick="event.stopPropagation(); App.editCategory('${safe}')"><i class="fas fa-pen"></i></button>` : '';
                const delBtn = isDeletableCategory(cat) ? `
                        <button class="cat-card-btn cat-card-del" title="Delete category"
                            onclick="event.stopPropagation(); App.deleteCategory('${safe}')"><i class="fas fa-trash"></i></button>` : '';
                const actions = (editBtn || delBtn) ? `<div class="cat-card-actions">${editBtn}${delBtn}</div>` : '';
                return `
                    <div class="cat-breakdown-card" onclick="App.showCategoryDetails('${safe}')"
                        style="border-left-color:${color};">
                        ${actions}
                        <div style="font-weight: 600; font-size: 0.85rem; text-transform: capitalize; margin-bottom: 4px; display:flex; align-items:center; gap:7px;">
                            <i class="fas ${getCategoryIcon(cat)}" style="color:${color}; font-size:0.8rem;"></i>
                            ${cat.replace('_', ' ').replace('-', ' ')}
                        </div>
                        <div style="font-family: var(--font-serif); font-size: 1.35rem; color: var(--text); margin-bottom: 4px;">
                            €${amount.toFixed(2)}
                        </div>
                        <div style="display: flex; justify-content: space-between; font-size: 0.75rem; color: var(--text-3);">
                            <span>${pct}%</span>
                            <span>${txCount} txns</span>
                        </div>
                    </div>
                `;
            }).join('')}
            <div class="add-category-tile" onclick="App.openNewCategoryDialog()">
                <i class="fas fa-plus"></i>
                <span>New category</span>
            </div>
        </div>
        <div style="text-align: center; padding: 12px; border-top: 1px solid var(--border-light); font-size: 0.85rem; color: var(--text-2);">
            Monthly Total: <strong style="font-family: var(--font-serif); font-size: 1.05rem;">€${total.toFixed(2)}</strong>
        </div>
    `;
}
