import { state } from './state.js';
import { showDayDetails } from './panel.js';

// ── Overview: full photo mosaic + liquid-glass panels ─────────────────────
// The overview header is covered corner-to-corner by a rectangular mosaic of the
// user's biggest-spend-day photos. On top float semi-transparent "liquid glass"
// panels holding the title, key stats, and the ranked biggest-spend days. Each
// mosaic tile and each day row opens that day's side panel.

let _rankedDaysCache = null;   // ranked biggest-spend days for the glass list (cached by outlier key)
let _rankedKey = null;

export function renderSpendBubbles() {
    const host = document.getElementById('spendBubbles');
    const grid = document.getElementById('mosaicGrid');
    if (!host || !grid) return;

    const exq = state.excludeOutliers ? 'exclude_outliers=1&' : '';

    // MOSAIC: random days each load, so the collage is never the same twice.
    // A fresh seed per render guarantees variety even on repeat visits.
    grid.innerHTML = `<div class="mosaic-loading"><i class="fas fa-spinner fa-spin"></i></div>`;
    const seed = Math.floor(Math.random() * 1e9);
    fetch(`/api/mosaic-photos?${exq}days=12&photos_per_day=4&seed=${seed}`)
        .then(r => r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status)))
        .then(data => {
            const mosaicDays = (data.days || []).filter(d => d.photos && d.photos.length);
            _painted = { grid, days: mosaicDays };
            paintMosaic(grid, mosaicDays);
            attachMosaicResize();
        })
        .catch(() => { grid.innerHTML = ''; });

    // GLASS LIST: the actual ranked biggest-spend days (stable, meaningful).
    const key = state.excludeOutliers ? 'excl' : 'incl';
    if (_rankedDaysCache && _rankedKey === key) {
        paintDaysList(host, _rankedDaysCache);
        return;
    }
    fetch(`/api/top-spend-days?${exq}limit=7&photos_per_day=1`)
        .then(r => r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status)))
        .then(data => {
            _rankedDaysCache = (data.days || []);
            _rankedKey = key;
            paintDaysList(host, _rankedDaysCache);
        })
        .catch(() => { host.innerHTML = ''; });
}

// Invalidate the cache (e.g. when the outlier filter flips).
export function resetSpendBubbles() {
    _rankedDaysCache = null;
    _rankedKey = null;
}

let _painted = null;   // {grid, days} for responsive re-tiling of the mosaic

let _resizeBound = false;
function attachMosaicResize() {
    if (_resizeBound) return;
    _resizeBound = true;
    let raf = null;
    window.addEventListener('resize', () => {
        if (raf) return;
        raf = requestAnimationFrame(() => {
            raf = null;
            if (_painted && _painted.grid.offsetParent !== null) paintMosaic(_painted.grid, _painted.days);
        });
    });
}

// Build a flat, deduped list of photo tiles across the top days, interleaved by
// day so the collage mixes locations rather than clustering one day. Each tile
// carries its display aspect ratio so we can size it without distortion.
function tilesFromDays(days) {
    const perDay = days.map(d => {
        const photos = (d.photos && d.photos.length) ? d.photos : (d.photo ? [d.photo] : []);
        return photos.filter(p => p && p.uuid).map(p => ({ uuid: p.uuid, date: d.date, aspect: p.aspect || 1 }));
    });
    const tiles = [];
    const seen = new Set();
    let added = true, i = 0;
    while (added) {
        added = false;
        for (const list of perDay) {
            if (i < list.length) {
                const t = list[i];
                if (!seen.has(t.uuid)) { seen.add(t.uuid); tiles.push(t); }
                added = true;
            }
        }
        i++;
    }
    return tiles;
}

// Templates are described as horizontal ROWS, each with a height (in row units) and
// a list of tile widths (in column units) that sum to 12. This always tiles the grid
// exactly — no gaps, no overflow — while giving varied widths (per tile) and varied
// heights (per row), e.g. a 5×3 next to a 3×3 next to a 2×4. `buildTemplate` turns a
// row spec into absolute grid-line slots.
function buildTemplate(rows) {
    const slots = [];
    let r = 1;
    for (const row of rows) {
        let c = 1;
        for (const w of row.w) {
            slots.push({ c1: c, r1: r, c2: c + w, r2: r + row.h });
            c += w;
        }
        r += row.h;
    }
    return { slots, cols: MOSAIC_COLS, rows: r - 1 };
}

// A fine 24-column grid gives width granularity so tiles aren't forced wide. Rows
// are kept tall (few bands) so each tile stays roughly square-to-portrait — never a
// stretched-wide strip. Each row's widths must sum to 24. `n` = the layout's exact
// slot count, so we only pick a layout we can fully fill.
const MOSAIC_COLS = 24;
const MOSAIC_LAYOUTS = [
    // fallback: 7 tiles
    { n: 7,  rows: [ { h: 5, w: [6, 6, 6, 6] }, { h: 4, w: [8, 8, 8] } ] },
    // default: 9 tiles — square-ish top row, gentler-landscape bottom row
    { n: 9,  rows: [ { h: 5, w: [6, 6, 6, 6] }, { h: 4, w: [5, 5, 4, 5, 5] } ] },
];

// Pixel aspect of a slot given the grid cell aspect (cellW/cellH).
function slotAspect(s, cellAspect) {
    return ((s.c2 - s.c1) * cellAspect) / (s.r2 - s.r1);
}

// A varied-size collage: pick the densest layout we have enough photos for, then
// assign each photo to the slot whose aspect best matches — so wide shots land in
// wide slots, tall in tall — minimising the crop from object-fit: cover.
function paintMosaic(grid, days) {
    const tiles = tilesFromDays(days);
    if (!tiles.length) { grid.innerHTML = ''; return; }

    // Densest layout whose tile count we can fill; fall back to the smallest.
    let layout = MOSAIC_LAYOUTS[0];
    for (const l of MOSAIC_LAYOUTS) if (tiles.length >= l.n) layout = l;
    const { slots, cols: COLS, rows: ROWS } = buildTemplate(layout.rows);

    grid.style.setProperty('--mosaic-cols', COLS);
    grid.style.setProperty('--mosaic-rows', ROWS);

    const w = grid.clientWidth || 1000;
    const h = grid.clientHeight || 300;
    const cellAspect = (w / COLS) / (h / ROWS);

    const count = slots.length;
    const chosen = tiles.slice(0, count);

    // Greedy least-cropping assignment: match sorted-by-aspect photos to
    // sorted-by-aspect slots (both ascending → wide-to-wide, tall-to-tall).
    const slotOrder = slots.map((s, i) => ({ i, a: slotAspect(s, cellAspect) }))
        .sort((x, y) => x.a - y.a);
    const photoOrder = chosen.map((t, i) => ({ i, a: t.aspect || 1 }))
        .sort((x, y) => x.a - y.a);
    const slotForPhoto = new Array(count);
    photoOrder.forEach((p, rank) => { slotForPhoto[p.i] = slots[slotOrder[rank].i]; });

    grid.innerHTML = chosen.map((t, idx) => {
        const s = slotForPhoto[idx];
        return `
            <div class="mosaic-tile"
                 style="grid-column:${s.c1} / ${s.c2}; grid-row:${s.r1} / ${s.r2}; animation-delay:${idx * 60}ms"
                 title="Open ${t.date}" onclick="App.showDayDetails('${t.date}')">
                <img src="/photo/${t.uuid}" loading="lazy" alt="">
            </div>
        `;
    }).join('');
}

// The ranked biggest-spend days, as compact rows inside the glass panel.
function paintDaysList(host, days) {
    const ranked = [...days].sort((a, b) => b.amount - a.amount).slice(0, 5);
    host.innerHTML = ranked.map((d, i) => {
        const dt = new Date(d.date + 'T00:00:00');
        const dateLabel = dt.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
        const thumb = (d.photo && d.photo.uuid) || (d.photos && d.photos[0] && d.photos[0].uuid);
        const loc = d.location ? `<span class="gd-loc"><i class="fas fa-map-marker-alt"></i>${escapeHtml(d.location)}</span>` : '';
        return `
            <div class="glass-day" onclick="App.showDayDetails('${d.date}')" title="Open ${dateLabel}">
                <span class="gd-rank">${i + 1}</span>
                ${thumb ? `<span class="gd-thumb"><img src="/photo/${thumb}" loading="lazy" alt=""></span>` : ''}
                <span class="gd-meta">
                    <span class="gd-date">${dateLabel}</span>
                    ${loc}
                </span>
                <span class="gd-amt">€${Math.round(d.amount).toLocaleString()}</span>
            </div>
        `;
    }).join('');
}

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
