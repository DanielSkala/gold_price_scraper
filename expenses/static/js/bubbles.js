import { state } from './state.js';
import { showDayDetails } from './panel.js';
import { escapeHtml } from './constants.js';

// ── Overview: biggest-spend days as a horizontal filmstrip of "memory cards" ──
// Each top-spend day is a framed card: a photo from that day with the amount + place
// on a scrim caption at the BOTTOM of the image (never over the busy middle), and the
// date on a footer. Days without a photo fall back to a clean typographic card so the
// strip never breaks. Clicking a card opens that day's side panel.

let _daysCache = null;   // ranked biggest-spend days (cached by outlier key)
let _daysKey = null;

export function renderSpendBubbles() {
    const host = document.getElementById('spendBubbles');
    if (!host) return;

    const exq = state.excludeOutliers ? 'exclude_outliers=1&' : '';
    const key = state.excludeOutliers ? 'excl' : 'incl';
    if (_daysCache && _daysKey === key) {
        paintFilmstrip(host, _daysCache);
        return;
    }
    host.innerHTML = `<div class="filmstrip-loading"><i class="fas fa-spinner fa-spin"></i></div>`;
    // One curated photo per day is all a card needs (fast: target=1 short-circuits).
    fetch(`/api/top-spend-days?${exq}limit=8&photos_per_day=1`)
        .then(r => r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status)))
        .then(data => {
            _daysCache = (data.days || []);
            _daysKey = key;
            paintFilmstrip(host, _daysCache);
        })
        .catch(() => { host.innerHTML = ''; });
}

// Invalidate the cache (e.g. when the outlier filter flips).
export function resetSpendBubbles() {
    _daysCache = null;
    _daysKey = null;
}

function paintFilmstrip(host, days) {
    const ranked = [...days].sort((a, b) => b.amount - a.amount).slice(0, 8);
    if (!ranked.length) { host.innerHTML = ''; return; }

    const sub = document.getElementById('spendDaysSub');
    if (sub) sub.textContent = `top ${ranked.length} by spend`;

    host.innerHTML = ranked.map((d, i) => {
        const dt = new Date(d.date + 'T00:00:00');
        const dateLabel = dt.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' });
        const uuid = (d.photo && d.photo.uuid) || (d.photos && d.photos[0] && d.photos[0].uuid);
        const amt = `€${Math.round(d.amount).toLocaleString()}`;
        const loc = d.location
            ? `<span class="film-loc"><i class="fas fa-map-marker-alt"></i>${escapeHtml(d.location)}</span>`
            : '';
        if (uuid) {
            return `
                <div class="film-card" onclick="App.showDayDetails('${d.date}')" title="Open ${dateLabel}">
                    <div class="film-photo">
                        <span class="film-rank">${i + 1}</span>
                        <img src="/photo/${escapeHtml(uuid)}" loading="lazy" alt="">
                        <div class="film-caption">
                            <div class="film-amt">${amt}</div>
                            ${loc}
                        </div>
                    </div>
                    <div class="film-date">${dateLabel}</div>
                </div>`;
        }
        // Photoless day → typographic card.
        return `
            <div class="film-card no-photo" onclick="App.showDayDetails('${d.date}')" title="Open ${dateLabel}">
                <div class="film-photo">
                    <span class="film-rank">${i + 1}</span>
                    <span class="film-amt-big">${amt}</span>
                    ${loc}
                </div>
                <div class="film-date">${dateLabel}</div>
            </div>`;
    }).join('');
}
