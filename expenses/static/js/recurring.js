import { state } from './state.js';
import { openPanel } from './panel.js';

// ── Upcoming & Recurring ────────────────────────────────────────────────────
// Renders detected recurring charges (subscriptions & bills) as an upcoming-bills
// list: what's billed, how often, the next expected charge, and price-hike / zombie
// flags. Backed by the deterministic /api/recurring endpoint (no LLM). Clicking a
// row opens the side panel with that merchant's full charge history + calendar.

const euro = (n) => `€${Math.round(n).toLocaleString()}`;
const euro2 = (n) => `€${Number(n).toFixed(2)}`;

// A distinct hue PER MERCHANT for the meter/chips. Recurring charges are usually all
// in one category (e.g. "Subscriptions"), so the category color would make every slice
// identical — these give each charge its own readable colour instead.
const REC_PALETTE = [
    '#2563eb', '#0f766e', '#b45309', '#7c3aed', '#be123c', '#0891b2',
    '#65a30d', '#c2410c', '#4f46e5', '#db2777', '#0369a1', '#a16207',
];
const recColor = (i) => REC_PALETTE[i % REC_PALETTE.length];

function esc(s) {
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// Tidy a raw merchant string for display (drop URL/ref noise, title-case shouty names).
function cleanName(raw) {
    let s = String(raw).split('*').pop().trim();
    s = s.replace(/^WWW\./i, '');
    if (s === s.toUpperCase() || s === s.toLowerCase()) {
        s = s.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
    }
    return s;
}

const CADENCE_LABEL = {
    weekly: 'Weekly', biweekly: 'Every 2 weeks', monthly: 'Monthly',
    quarterly: 'Quarterly', yearly: 'Yearly',
};

// "in 3 days" / "in 2 weeks" / "today" / "5 days ago" from an ISO date, relative to
// the latest data date (state.selectedMonth is month-only, so use today as a proxy
// but fall back gracefully — the server predicts from the last charge).
function relativeWhen(iso) {
    const target = new Date(iso + 'T00:00:00');
    const now = new Date();
    const days = Math.round((target - now) / 86400000);
    if (days === 0) return 'due today';
    if (days > 0) {
        if (days === 1) return 'in 1 day';
        if (days < 14) return `in ${days} days`;
        if (days < 60) return `in ${Math.round(days / 7)} weeks`;
        return `in ${Math.round(days / 30)} months`;
    }
    const a = -days;
    if (a === 1) return 'yesterday';
    if (a < 14) return `${a} days ago`;
    if (a < 60) return `${Math.round(a / 7)} weeks ago`;
    return `${Math.round(a / 30)} months ago`;
}

export function renderRecurring() {
    const section = document.getElementById('recurringSection');
    const body = document.getElementById('recurringBody');
    const sub = document.getElementById('recurringSub');
    if (!section || !body) return;

    const qs = state.excludeOutliers ? '?exclude_outliers=1' : '';
    fetch('/api/recurring' + qs)
        .then(r => r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status)))
        .then(data => {
            const items = data.recurring || [];
            if (!items.length) { section.style.display = 'none'; return; }
            section.style.display = '';
            sub.textContent = `${items.length} charge${items.length === 1 ? '' : 's'} tracked`;
            renderBand(body, items, data.committed_monthly || 0);
        })
        .catch(() => { section.style.display = 'none'; });
}

// A single compact "committed spend" band: a hero total + a segmented proportional
// meter (each recurring charge a category-colored slice) + a live readout that
// defaults to the next upcoming charge and follows whichever slice/chip you hover.
function renderBand(body, items, committed) {
    // Largest → smallest by monthly cost, so the meter and chips read biggest-first.
    // (Zombies still sink to the end since they aren't committed spend.)
    const active = items.filter(i => !i.is_zombie)
        .sort((a, b) => b.monthly_cost - a.monthly_cost);
    const zombies = items.filter(i => i.is_zombie)
        .sort((a, b) => b.monthly_cost - a.monthly_cost);
    const ordered = [...active, ...zombies];
    // Give each item a distinct colour by its position in the sorted list.
    ordered.forEach((it, i) => { it._color = recColor(i); });

    // Meter proportions from monthly cost across ACTIVE items (zombies aren't committed).
    const meterTotal = active.reduce((s, i) => s + i.monthly_cost, 0) || 1;
    const segs = active.map((it) => {
        const pct = (it.monthly_cost / meterTotal) * 100;
        return `<span class="rec-seg" data-idx="${ordered.indexOf(it)}" style="flex:${pct.toFixed(3)} 1 0;background:${it._color}"
                     title="${esc(cleanName(it.display))} · ${euro(it.monthly_cost)}/mo"></span>`;
    }).join('');

    const chips = ordered.map((it) => {
        const idx = ordered.indexOf(it);
        const flagDot = it.price_change_pct >= 0.10 ? '<span class="rec-chip-up" title="price rose">▲</span>'
            : it.is_zombie ? '<span class="rec-chip-ghost" title="possibly inactive">•</span>' : '';
        return `<button class="rec-chip${it.is_zombie ? ' is-zombie' : ''}" data-idx="${idx}" type="button">
                    <span class="rec-chip-dot" style="background:${it._color}"></span>
                    <span class="rec-chip-name">${esc(cleanName(it.display))}</span>
                    <span class="rec-chip-amt">${euro(it.monthly_cost)}</span>${flagDot}
                </button>`;
    }).join('');

    // Default readout: the largest active charge (first in the sorted list).
    const defaultIdx = active.length ? ordered.indexOf(active[0]) : 0;

    body.innerHTML = `
        <div class="rec-band">
            <div class="rec-band-left">
                <div class="rec-total"><span class="rec-total-cur">€</span>${Math.round(committed).toLocaleString()}</div>
                <div class="rec-total-label">per month committed</div>
            </div>
            <div class="rec-band-right">
                <div class="rec-readout" id="recReadout"></div>
                <div class="rec-meter" id="recMeter">${segs}</div>
                <div class="rec-chips" id="recChips">${chips}</div>
            </div>
        </div>
    `;

    const readout = body.querySelector('#recReadout');
    const setReadout = (idx, pinned) => readout.innerHTML = readoutHtml(ordered[idx], pinned);
    setReadout(defaultIdx, false);

    // Hover a slice or chip → highlight + update readout; mouse-out → back to default.
    const segEls = [...body.querySelectorAll('.rec-seg')];
    const chipEls = [...body.querySelectorAll('.rec-chip')];
    const highlight = (idx) => {
        segEls.forEach(s => s.classList.toggle('is-active', +s.dataset.idx === idx));
        chipEls.forEach(c => c.classList.toggle('is-active', +c.dataset.idx === idx));
    };
    const clearHighlight = () => { highlight(-1); setReadout(defaultIdx, false); };

    [...segEls, ...chipEls].forEach(el => {
        const idx = +el.dataset.idx;
        el.addEventListener('mouseenter', () => { highlight(idx); setReadout(idx, false); });
    });
    body.querySelector('.rec-band-right').addEventListener('mouseleave', clearHighlight);

    // Click a chip (or slice) → open that merchant's full charge history.
    [...segEls, ...chipEls].forEach(el => {
        el.addEventListener('click', () => {
            const it = ordered[+el.dataset.idx];
            openChargeHistory(it.display, it._color || '#57534e');
        });
    });
}

// The live detail line shown above the meter for a given item.
function readoutHtml(it, pinned) {
    if (!it) return '';
    const name = cleanName(it.display);
    const cadence = CADENCE_LABEL[it.cadence] || it.cadence;

    let lead, when;
    if (it.is_zombie) {
        lead = `<span class="rec-ro-tag rec-ro-ghost"><i class="fas fa-ghost"></i> possibly inactive</span>`;
        when = `last charged ${it.last_seen}`;
    } else {
        // "next in 3 days" reads well for future dates; for a prediction that's
        // already elapsed, "expected …" avoids the nonsensical "next X ago".
        const future = new Date(it.predicted_next + 'T00:00:00') >= new Date();
        const verb = future ? 'next' : 'expected';
        lead = `<span class="rec-ro-tag"><i class="fas fa-calendar-day"></i> ${verb} ${relativeWhen(it.predicted_next)}</span>`;
        when = `expected ~${it.predicted_next}`;
    }

    let hike = '';
    if (it.price_change_pct >= 0.10) {
        hike = `<span class="rec-ro-hike rec-ro-up"><i class="fas fa-arrow-trend-up"></i> up ${Math.round(it.price_change_pct * 100)}%</span>`;
    } else if (it.price_change_pct <= -0.10) {
        hike = `<span class="rec-ro-hike rec-ro-down"><i class="fas fa-arrow-trend-down"></i> down ${Math.round(-it.price_change_pct * 100)}%</span>`;
    }

    return `
        <span class="rec-ro-dot" style="background:${it._color || it.color || '#57534e'}"><i class="fas ${it.icon || 'fa-rotate'}"></i></span>
        <span class="rec-ro-body">
            <span class="rec-ro-title">
                <b>${esc(name)}</b> ${lead} ${hike}
            </span>
            <span class="rec-ro-sub">${cadence} · ${euro2(it.avg_amount)}/charge · ${euro(it.monthly_cost)}/mo · ${when}</span>
        </span>
    `;
}

// Open the side panel showing every charge from this merchant, with the calendar.
function openChargeHistory(display, color) {
    fetch('/api/transactions-for-merchants' + (state.excludeOutliers ? '?exclude_outliers=1' : ''), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ merchants: [display] }),
    })
        .then(r => r.json())
        .then(data => {
            const txs = data.transactions || [];
            const name = cleanName(display);
            openPanel({
                title: name,
                subtitle: 'Recurring charge · all transactions',
                transactions: txs,
                showDate: true,
                calendarCtx: { category: name, txs, color, virtual: true },
            });
        })
        .catch(() => {});
}
