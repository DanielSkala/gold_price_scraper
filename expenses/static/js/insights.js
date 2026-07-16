import { state } from './state.js';
import { openNewCategoryDialog } from './categories.js';
import { loadAllData } from './loading.js';
import { openPanel } from './panel.js';
import { linkifySources } from './sources.js';
import { renderRecurring } from './recurring.js';

// ── Overview insights: savings recommendations + category discovery ─────────
// Both are backed by deterministic, local endpoints (no LLM). This module just
// fetches and renders them into the overview.

const euro = (n) => `€${Math.round(n).toLocaleString()}`;

function esc(s) {
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// Severity → tone class + a soft icon for the recommendation cards.
const SEV = {
    alert:       { tone: 'tone-neg',     icon: 'fa-exclamation' },
    watch:       { tone: 'tone-warn',    icon: 'fa-exclamation' },
    opportunity: { tone: 'tone-pos',     icon: 'fa-arrow-down' },
    tip:         { tone: 'tone-primary', icon: 'fa-lightbulb' },
    good:        { tone: 'tone-pos',     icon: 'fa-check' },
};

// A short, human title per recommendation kind (the backend detail is the tooltip).
function recIcon(r) {
    const byId = {
        cook_vs_eatout: 'fa-utensils', coffee_home: 'fa-mug-hot', delivery_pickup: 'fa-bag-shopping',
        discount_grocer: 'fa-basket-shopping', transit_vs_taxi: 'fa-bus', subscription_audit: 'fa-rotate',
    };
    if (byId[r.id]) return byId[r.id];
    if (r.id && r.id.indexOf('benchmark_') === 0) return 'fa-chart-bar';
    if (r.id && r.id.indexOf('trend_') === 0) return 'fa-arrow-up';
    if (r.id && r.id.indexOf('top_') === 0) return 'fa-chart-pie';
    return (SEV[r.severity] || SEV.tip).icon;
}

// The backend title is now always a concrete ACTION ("Fill up at cheaper stations",
// "Cook a few dinners in", …). A few swap-rule ids get an even punchier hook; the
// rest use the action title as-is.
function recHeadline(r) {
    const byId = {
        cook_vs_eatout:   'Cook a few dinners in',
        coffee_home:      'Make your coffee at home',
        delivery_pickup:  'Ditch the delivery fees',
        transit_vs_taxi:  'Take transit, skip the taxi',
    };
    return byId[r.id] || r.title;
}

export function renderRecommendations() {
    const section = document.getElementById('recommendationsSection');
    const body = document.getElementById('recommendationsBody');
    const sub = document.getElementById('recommendationsSub');
    if (!section || !body) return;

    const qs = state.excludeOutliers ? '?exclude_outliers=1' : '';
    fetch('/api/recommendations' + qs)
        .then(r => r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status)))
        .then(data => {
            // Sort by saving so the top-3 are the biggest wins (largest first).
            const recs = (data.recommendations || []).slice().sort((a, b) => (b.saving || 0) - (a.saving || 0));
            if (!recs.length && !data.headline) { section.style.display = 'none'; return; }
            section.style.display = '';

            if (data.potential_savings > 0) {
                sub.textContent = `up to ${euro(data.potential_savings)}/mo`;
            } else {
                sub.textContent = `${data.months_analysed}-mo average`;
            }

            const headlineHtml = data.headline ? renderHeadline(data.headline) : '';
            const TOP = 3;
            const top = recs.slice(0, TOP);
            const rest = recs.slice(TOP);
            const moreBtn = rest.length
                ? `<button class="insight-more" id="recMoreBtn">Show ${rest.length} more</button>`
                : '';
            body.innerHTML = headlineHtml
                + `<div class="rec-cards">${top.map((r, i) => renderRecCard(r, i)).join('')}</div>`
                + (rest.length ? `<div class="rec-cards" id="recMore" style="display:none;margin-top:12px;">${rest.map((r, i) => renderRecCard(r, TOP + i)).join('')}</div>` : '')
                + moreBtn;

            const btn = document.getElementById('recMoreBtn');
            if (btn) {
                btn.addEventListener('click', () => {
                    const more = document.getElementById('recMore');
                    const open = more.style.display !== 'none';
                    more.style.display = open ? 'none' : 'grid';
                    btn.textContent = open ? `Show ${rest.length} more` : 'Show fewer';
                });
            }

            // "Learn more" expands a card's details in place (no hover resize).
            body.querySelectorAll('.rec-learn').forEach(link => {
                link.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const card = link.closest('.rec-card');
                    const open = card.classList.toggle('expanded');
                    link.innerHTML = open
                        ? 'Show less<i class="fas fa-chevron-up"></i>'
                        : 'Learn more<i class="fas fa-chevron-down"></i>';
                });
            });
        })
        .catch(() => { section.style.display = 'none'; });
}

// A quiet lead sentence summarising goal progress (no card / accent bar).
function renderHeadline(h) {
    const savePill = h.saving > 0 ? `<span class="lead-save">−${euro(h.saving)}/mo</span>` : '';
    return `<p class="insight-lead">${esc(h.detail)} ${savePill}</p>`;
}

// Recommendation card: at rest, only a punchy headline + savings pill + a "Learn
// more" nudge. The full explanation (detail + the assumption) stays hidden until
// tapped — so the section scans instantly and invites a click.
function renderRecCard(r, idx) {
    const sev = SEV[r.severity] || SEV.tip;
    const save = r.saving > 0 ? `<span class="rec-save">Save ${euro(r.saving)}</span>` : '';
    const detail = esc(r.detail) + (r.assumption ? ` <span class="rec-assume">${linkifySources(r.assumption)}</span>` : '');
    return `
        <div class="rec-card ${sev.tone}">
            <div class="rec-card-head">
                <span class="rec-ico"><i class="fas ${recIcon(r)}"></i></span>
                <span class="rec-title">${esc(recHeadline(r))}</span>
                ${save}
            </div>
            <div class="rec-expand"><div class="rec-detail">${detail}</div></div>
            <button class="rec-learn" type="button">Learn more<i class="fas fa-chevron-down"></i></button>
        </div>
    `;
}

export function renderDiscovery() {
    const section = document.getElementById('discoverySection');
    const body = document.getElementById('discoveryBody');
    const sub = document.getElementById('discoverySub');
    if (!section || !body) return;

    const qs = state.excludeOutliers ? '?exclude_outliers=1' : '';
    fetch('/api/discover-categories' + qs)
        .then(r => r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status)))
        .then(data => {
            // A "recurring merchant" cluster is only worth surfacing when it has ≥3
            // transactions; otherwise it's noise. Named-candidate suggestions stay.
            const sugg = (data.suggestions || []).filter(s =>
                s.kind !== 'cluster' || s.count >= 3);
            if (!sugg.length) { section.style.display = 'none'; return; }
            section.style.display = '';
            sub.textContent = `inside your ${euro(data.other_total)} “other”`;

            // Three spacious feature cards + the remainder as a compact "More" list.
            const feature = sugg.slice(0, 3);
            const rest = sugg.slice(3);
            const featureHtml = feature.map((s, i) => renderDiscoveryFeature(s, i)).join('');
            const moreHtml = rest.length ? renderDiscoveryMore(rest, 3) : '';
            body.innerHTML = `<div class="disc-cards">${featureHtml}${moreHtml}</div>`;

            // Add buttons create the category (stop the click from also opening items).
            body.querySelectorAll('.disc-add, .disc-more-add').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const s = sugg[parseInt(btn.dataset.idx, 10)];
                    openAddSuggestedCategory(s);
                });
            });
            // Clicking a card / row opens a panel listing exactly which items it holds.
            body.querySelectorAll('[data-sugg-idx]').forEach(el => {
                el.addEventListener('click', () => {
                    const s = sugg[parseInt(el.dataset.suggIdx, 10)];
                    openSuggestionItems(s);
                });
            });
        })
        .catch(() => { section.style.display = 'none'; });
}

// Open the side panel showing the actual transactions behind a suggested category.
// A "not a category yet" note + an Add action make its suggested status clear.
function openSuggestionItems(s) {
    const merchants = (s.merchants || []).map(m => m.merchant);
    fetch('/api/transactions-for-merchants' + (state.excludeOutliers ? '?exclude_outliers=1' : ''), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ merchants }),
    })
        .then(r => r.json())
        .then(data => {
            const txs = data.transactions || [];
            openPanel({
                title: s.name,
                subtitle: 'Suggested category · not added yet',
                transactions: txs,
                showDate: true,
                // Seed the calendar from THESE transactions (and clear any stale one from
                // a previously-opened category) so the highlighted days match the items.
                // `virtual: true` marks it as a not-yet-real category: day drill-downs must
                // match membership by these transactions, not by a (nonexistent) tx.category.
                calendarCtx: { category: s.name, txs, color: s.color, virtual: true },
            });
        })
        .catch(() => {});
}

function openAddSuggestedCategory(s) {
    const icon = s.icon || 'fa-tag';
    openNewCategoryDialog(
        () => { loadAllData(); },   // refresh everything so the new category shows up
        {
            prefill: { name: s.name, color: s.color, icon },
            items: (s.merchants || []).map(m => ({ merchant: m.merchant, total: m.total, count: m.count })),
            onSubmit: ({ name, color, icon, merchants }) =>
                fetch('/api/create-category-from-suggestion', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, color, icon, merchants }),
                }).then(r => r.json()).then(d => { if (d.error) throw new Error(d.error); return d; }),
        }
    );
}

// Spacious feature card — visually marked as a SUGGESTION (dashed border + tag), so
// it doesn't look like a real category. Whole card opens its items; Add creates it.
function renderDiscoveryFeature(s, idx) {
    const merchants = (s.merchants || []).length;
    return `
        <div class="disc-card is-suggested" style="--disc-color:${s.color}" data-sugg-idx="${idx}" title="See items in “${esc(s.name)}”">
            <div class="disc-card-top">
                <span class="disc-ico"><i class="fas ${s.icon}"></i></span>
                <button class="disc-add" data-idx="${idx}">Add</button>
            </div>
            <div class="disc-card-name">${esc(s.name)} <span class="disc-tag">Suggested</span></div>
            <div class="disc-card-total">${euro(s.total)}</div>
            <div class="disc-card-meta">${s.count} txns · ${merchants} merchant${merchants === 1 ? '' : 's'} · <span class="disc-see">see items</span></div>
        </div>
    `;
}

// Compact "More categories" card holding the remaining suggestions as small rows.
function renderDiscoveryMore(rest, offset) {
    const rows = rest.map((s, j) => {
        const idx = offset + j;
        return `
            <div class="disc-more-row" data-sugg-idx="${idx}" title="See items in “${esc(s.name)}”">
                <span class="disc-more-ico" style="color:${s.color}"><i class="fas ${s.icon}"></i></span>
                <span class="disc-more-name">${esc(s.name)}</span>
                <span class="disc-more-total">${euro(s.total)}</span>
                <button class="disc-more-add" data-idx="${idx}" title="Add ${esc(s.name)}"><i class="fas fa-plus"></i></button>
            </div>
        `;
    }).join('');
    return `
        <div class="disc-card disc-card-more is-suggested">
            <div class="disc-more-title">More categories <span class="disc-tag">Suggested</span></div>
            <div class="disc-more-list">${rows}</div>
        </div>
    `;
}

export function renderOverviewInsights() {
    renderRecurring();
    renderRecommendations();
    renderDiscovery();
}
