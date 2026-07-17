import { state } from './state.js';
import { allCategoryNames, getCategoryColor, getCategoryBg, getCategoryIcon } from './constants.js';
import { loadAllData } from './loading.js';
import { showToast } from './overrides.js';
import { openNewCategoryDialog } from './categories.js';

// ── Tidy up "Other" ─────────────────────────────────────────────────────────
// A playful, one-card-at-a-time flow to file the biggest/most-frequent merchants
// still sitting in the 'other' bucket. Each card shows rich context (count, total,
// date range, photos from purchase days); the user files it into a category or skips.
// Moving reuses /api/override (which also learns a merchant rule, so every txn from
// that merchant moves together). A running counter + progress bar make it feel like
// a game, ending in a summary of how much was tidied.

const euro = (n) => `€${Math.round(n).toLocaleString()}`;
const euro2 = (n) => `€${Number(n).toFixed(2)}`;

function esc(s) {
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// Session state for the current run.
let queue = [];
let idx = 0;
let movedItems = 0;      // number of merchant-groups filed
let movedTxns = 0;       // number of transactions moved out of 'other'
let dirty = false;       // did we change anything? (drives a single reload on close)
let keyHandler = null;

function fmtDate(iso) {
    return new Date(iso + 'T00:00:00').toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: 'numeric' });
}

// Categories to offer, 'other' excluded (you're moving OUT of it).
function targetCategories() {
    return allCategoryNames().filter(c => c.toLowerCase() !== 'other');
}

export function startTidy() {
    const overlay = document.getElementById('tidyOverlay');
    const body = document.getElementById('tidyBody');
    if (!overlay || !body) return;

    idx = 0; movedItems = 0; movedTxns = 0; dirty = false; queue = [];
    overlay.classList.add('open');
    document.body.style.overflow = 'hidden';
    body.innerHTML = `<div class="tidy-loading"><i class="fas fa-spinner fa-spin"></i><p>Finding items worth filing…</p></div>`;
    updateHead();

    keyHandler = (e) => {
        if (e.key === 'Escape') closeTidy();
        else if (e.key === 'ArrowRight') skipCurrent();
        else if (e.key === 'ArrowLeft') goBack();
    };
    document.addEventListener('keydown', keyHandler);

    const qs = state.excludeOutliers ? '?exclude_outliers=1' : '';
    fetch('/api/tidy-other' + qs)
        .then(r => r.json())
        .then(data => {
            queue = (data.items || []).filter(it => it.merchant);
            if (!queue.length) { renderEmpty(); return; }
            renderCard();
            updateHead();
        })
        .catch(() => { body.innerHTML = `<div class="tidy-loading"><p>Couldn't load items. Try again later.</p></div>`; });
}

export function closeTidy(evt) {
    // When called from the overlay backdrop, only close on a direct backdrop click.
    if (evt && evt.target && evt.target.id && evt.target.id !== 'tidyOverlay') return;
    const overlay = document.getElementById('tidyOverlay');
    if (overlay) overlay.classList.remove('open');
    document.body.style.overflow = '';
    if (keyHandler) { document.removeEventListener('keydown', keyHandler); keyHandler = null; }
    // A single dashboard refresh if anything changed (cards updated optimistically).
    if (dirty) { loadAllData(); dirty = false; }
}

function updateHead() {
    const step = document.getElementById('tidyStep');
    const moved = document.getElementById('tidyMoved');
    const fill = document.getElementById('tidyProgressFill');
    const done = Math.min(idx, queue.length);
    if (step) step.textContent = queue.length ? `${Math.min(idx + 1, queue.length)} of ${queue.length}` : '';
    if (moved) moved.innerHTML = movedItems
        ? `<i class="fas fa-check-circle"></i> ${movedItems} filed · ${movedTxns} txns out of Other`
        : '';
    if (fill) fill.style.width = queue.length ? `${(done / queue.length) * 100}%` : '0%';
}

// Advance to the next card (or the summary when the queue is exhausted).
function next() {
    idx += 1;
    updateHead();
    if (idx >= queue.length) renderSummary();
    else renderCard();
}

function skipCurrent() {
    if (idx < queue.length) next();
}

// Go back to the previous item. If that item was already filed, un-file it first
// (clear its rule so it returns to 'other') and roll back the counters — so the
// user can genuinely reconsider it, not just look at it.
function goBack() {
    if (idx <= 0) return;
    idx -= 1;
    const it = queue[idx];
    if (it && it.filedCategory) {
        const undoneCat = it.filedCategory;
        const undoneCount = it.filedCount || it.count;
        it.filedCategory = null;
        movedItems = Math.max(0, movedItems - 1);
        movedTxns = Math.max(0, movedTxns - undoneCount);
        dirty = true;
        fetch('/api/tidy-unfile', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ merchant: it.merchant }),
        }).catch(() => {});
        showToast(`Put ${_clean(it.display)} back — reconsider it`);
    }
    updateHead();
    renderCard();
}

// Tidy a raw merchant string for a short toast.
function _clean(raw) {
    let s = String(raw).split('*').pop().trim().replace(/^WWW\./i, '');
    return s.length > 28 ? s.slice(0, 27) + '…' : s;
}

function renderCard() {
    const body = document.getElementById('tidyBody');
    const it = queue[idx];
    if (!it) { renderSummary(); return; }

    const span = it.first_date === it.last_date
        ? fmtDate(it.first_date)
        : `${fmtDate(it.first_date)} → ${fmtDate(it.last_date)}`;

    const chips = targetCategories().map(cat => `
        <button class="tidy-chip" data-cat="${esc(cat)}">
            <span class="tidy-chip-dot" style="background:${getCategoryColor(cat)}"><i class="fas ${getCategoryIcon(cat)}"></i></span>
            ${esc(cat.replace(/_/g, ' ').replace(/-/g, ' '))}
        </button>`).join('');

    const photos = it.photos || [];
    // Photos are OPT-IN: they show the day's whole camera roll, which is often unrelated
    // to the purchase, so we hide them behind a toggle rather than leading with them.
    const photoToggle = photos.length
        ? `<button class="tidy-photo-toggle" id="tidyPhotoToggle">
               <i class="fas fa-camera"></i> View photos from these days
           </button>
           <div class="tidy-photos" id="tidyPhotos" hidden>${photos.map(p => `
              <div class="tidy-photo" title="${esc(fmtDate(p.date))}">
                  <img src="/photo/${p.uuid}" loading="lazy" alt="">
                  <span class="tidy-photo-date">${esc(new Date(p.date + 'T00:00:00').toLocaleDateString(undefined, { day: 'numeric', month: 'short' }))}</span>
              </div>`).join('')}
              <div class="tidy-photos-note">Your camera roll from these days — may not show the actual purchase.</div>
           </div>`
        : '';

    body.innerHTML = `
        <div class="tidy-card">
            <div class="tidy-hero">
                <span class="tidy-hero-icon"><i class="fas fa-receipt"></i></span>
                <div class="tidy-hero-text">
                    <div class="tidy-eyebrow">Which category fits?</div>
                    <div class="tidy-merchant" title="${esc(it.display)}">${esc(it.display)}</div>
                </div>
            </div>
            <div class="tidy-stats">
                <span class="tidy-stat"><b>${it.count}</b> ${it.count === 1 ? 'purchase' : 'purchases'}</span>
                <span class="tidy-stat"><b>${euro(it.total)}</b> total</span>
                <span class="tidy-stat">avg ${euro2(it.avg)}</span>
                <span class="tidy-stat"><i class="fas fa-calendar"></i> ${span}</span>
            </div>
            <div class="tidy-chips">
                ${chips}
                <button class="tidy-chip tidy-chip-new" data-cat="__add__">
                    <span class="tidy-chip-dot" style="background:var(--primary)"><i class="fas fa-plus"></i></span>
                    New category…
                </button>
            </div>
            ${photoToggle}
            <div class="tidy-actions">
                <button class="tidy-back" id="tidyBackBtn" ${idx === 0 ? 'disabled' : ''}><i class="fas fa-arrow-left"></i> Back</button>
                <button class="tidy-skip" id="tidySkipBtn">Skip for now <i class="fas fa-forward"></i></button>
            </div>
        </div>
    `;

    body.querySelectorAll('.tidy-chip').forEach(btn => {
        btn.addEventListener('click', () => {
            const cat = btn.dataset.cat;
            if (cat === '__add__') {
                openNewCategoryDialog((newCat) => { if (newCat) fileCurrent(newCat); });
            } else {
                fileCurrent(cat);
            }
        });
    });
    const backBtn = body.querySelector('#tidyBackBtn');
    if (backBtn) backBtn.addEventListener('click', goBack);
    body.querySelector('#tidySkipBtn').addEventListener('click', skipCurrent);
    const photoBtn = body.querySelector('#tidyPhotoToggle');
    if (photoBtn) {
        photoBtn.addEventListener('click', () => {
            const strip = body.querySelector('#tidyPhotos');
            const showing = !strip.hasAttribute('hidden');
            if (showing) {
                strip.setAttribute('hidden', '');
                photoBtn.innerHTML = `<i class="fas fa-camera"></i> View photos from these days`;
            } else {
                strip.removeAttribute('hidden');
                photoBtn.innerHTML = `<i class="fas fa-eye-slash"></i> Hide photos`;
            }
        });
    }
}

// Move the current merchant's transactions into `category` (persist + learn a rule),
// then advance. Optimistic: the counter bumps immediately; the dashboard reloads once
// on close so the flow stays snappy.
function fileCurrent(category) {
    const it = queue[idx];
    if (!it) return;
    const card = document.querySelector('.tidy-card');
    if (card) card.classList.add('tidy-card-leaving');

    // File the whole brand group (may span several raw merchant spellings) via the
    // normalized key, so e.g. "LIDL 164" + "LIDL 042" all move together.
    fetch('/api/tidy-file', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ merchant: it.merchant, category }),
    })
        .then(r => r.json())
        .then(data => {
            const n = data.affected || it.count;
            // Remember what we did so Back can undo it.
            it.filedCategory = category;
            it.filedCount = n;
            movedItems += 1;
            movedTxns += n;
            dirty = true;
            const catName = category.replace(/_/g, ' ').replace(/-/g, ' ');
            showToast(`Moved ${n} ${n === 1 ? 'transaction' : 'transactions'} to <b>${esc(catName)}</b>`);
            setTimeout(next, 180);
        })
        .catch(() => {
            if (card) card.classList.remove('tidy-card-leaving');
        });
}

function renderEmpty() {
    const body = document.getElementById('tidyBody');
    body.innerHTML = `
        <div class="tidy-done">
            <div class="tidy-done-emoji">✨</div>
            <h3>Nothing to tidy!</h3>
            <p>Your “Other” bucket has no recurring or significant merchants worth filing right now.</p>
            <button class="tidy-done-btn" onclick="App.closeTidy()">Great</button>
        </div>`;
    updateHead();
}

function renderSummary() {
    const body = document.getElementById('tidyBody');
    const fill = document.getElementById('tidyProgressFill');
    if (fill) fill.style.width = '100%';
    const headline = movedItems
        ? `You filed ${movedItems} ${movedItems === 1 ? 'merchant' : 'merchants'} — ${movedTxns} ${movedTxns === 1 ? 'transaction' : 'transactions'} out of Other!`
        : `No changes this round — that's fine!`;
    body.innerHTML = `
        <div class="tidy-done">
            <div class="tidy-done-emoji">${movedItems ? '🎉' : '👍'}</div>
            <h3>${esc(headline)}</h3>
            <p>${movedItems ? 'Nice work tidying up. Your categories are cleaner now.' : 'You can come back and tidy up anytime.'}</p>
            <button class="tidy-done-btn" onclick="App.closeTidy()">Done</button>
        </div>`;
    updateHead();
}
