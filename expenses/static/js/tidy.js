import { state } from './state.js';
import { allCategoryNames, getCategoryColor, getCategoryIcon, isFallbackCategory } from './constants.js';
import { loadAllData } from './loading.js';
import { showToast } from './overrides.js';
import { openNewCategoryDialog } from './categories.js';

// ── Tidy up the uncategorized bucket ───────────────────────────────────────
// A playful, one-card-at-a-time flow to file the biggest/most-frequent merchants
// nothing has classified yet. Each card shows rich context (count, total,
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
let movedTxns = 0;       // number of transactions given a real category
let keptItems = 0;       // groups filed into the real 'other' ("Keep in Other")
let dirty = false;       // did we change anything? (drives a single reload on close)
let keyHandler = null;
// File/un-file requests still in flight. The user can close the overlay the instant
// after picking a category, so `closeTidy` waits on these before deciding whether to
// refresh — otherwise `dirty` is still false there and the dashboard would keep
// showing the item under its OLD category until the next manual reload.
let pending = [];

function track(promise) {
    pending.push(promise);
    const done = () => { pending = pending.filter(p => p !== promise); };
    promise.then(done, done);
    return promise;
}

// Every write goes through this. `fetch` RESOLVES on 4xx/5xx, so a plain `.catch()` sees
// nothing when a request fails — which let the UI announce "we won't ask again" while the
// server (an older build with no such route) had answered 404. Rejecting on !ok means a
// failed write always reaches the error path and gets rolled back and reported.
function postJson(url, body) {
    return fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    }).then(r => {
        if (!r.ok) throw new Error(`${url} → HTTP ${r.status}`);
        return r.json();
    });
}

// Shared failure note: says plainly that nothing was saved, and hints at the usual cause.
function warnWriteFailed(what) {
    showToast(`Couldn't save — ${what} is unchanged. Is the server up to date?`, 'error');
}

function fmtDate(iso) {
    return new Date(iso + 'T00:00:00').toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: 'numeric' });
}

// Chips to offer: everything except the bucket you're emptying, and except the real
// 'other' — that one has its own dedicated "Keep in Other" button, so offering it twice
// would just be two controls doing the same thing.
function targetCategories() {
    return allCategoryNames().filter(c => !isFallbackCategory(c) && catName(c) !== 'other');
}

const catName = (c) => String(c).trim().toLowerCase();

export function startTidy() {
    const overlay = document.getElementById('tidyOverlay');
    const body = document.getElementById('tidyBody');
    if (!overlay || !body) return;

    idx = 0; movedItems = 0; movedTxns = 0; keptItems = 0;
    dirty = false; queue = []; pending = [];
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
    // Any still-unfinished write has to land first, or we'd read `dirty` too early and
    // skip the refresh entirely, leaving the moved item showing its old category.
    const refresh = () => { if (dirty) { loadAllData(); dirty = false; } };
    const inFlight = pending.slice();
    if (inFlight.length) Promise.allSettled(inFlight).then(refresh);
    else refresh();
}

function updateHead() {
    const step = document.getElementById('tidyStep');
    const moved = document.getElementById('tidyMoved');
    const fill = document.getElementById('tidyProgressFill');
    const done = Math.min(idx, queue.length);
    if (step) step.textContent = queue.length ? `${Math.min(idx + 1, queue.length)} of ${queue.length}` : '';
    if (moved) {
        const bits = [];
        if (movedItems) bits.push(`<i class="fas fa-check-circle"></i> ${movedItems} filed · ${movedTxns} txns sorted`);
        if (keptItems) bits.push(`<i class="fas fa-thumbtack"></i> ${keptItems} kept in Other`);
        moved.innerHTML = bits.join(' <span class="tidy-moved-sep">·</span> ');
    }
    if (fill) fill.style.width = queue.length ? `${(done / queue.length) * 100}%` : '0%';
}

// Advance to the next card (or the summary when the queue is exhausted).
function next() {
    idx += 1;
    updateHead();
    if (idx >= queue.length) renderSummary();
    else renderCard();
}

// Skip = "not now": temporary, so the card can come back on a later visit.
function skipCurrent() {
    if (idx < queue.length) next();
}

// Keep in Other = "this isn't uncategorized, it IS miscellaneous". That's a real
// decision, so it's a real categorisation: the brand is filed into the ordinary 'other'
// category. The queue only draws from the uncategorized bucket, so it never comes back —
// and unlike a hidden mute list, the choice stays visible in the breakdown and can be
// changed from the badge picker like any other category.
function keepCurrent() {
    const it = queue[idx];
    if (!it) return;
    fileGroup(it, KEEP_CATEGORY, {
        onDone: (n) => {
            it.keptOther = true;
            keptItems += 1;
            showToast(n
                ? `Kept <b>${esc(_clean(it.display))}</b> in Other — we won't ask again`
                : `Nothing to move — these are pinned to another category`);
        },
        onFail: () => { it.keptOther = false; },
    });
}

// Go back to the previous item. If that item was already filed, un-file it first
// (clear its rule so it returns to 'other') and roll back the counters — so the
// user can genuinely reconsider it, not just look at it. A "Keep in Other" is undone
// the same way, so a mis-click isn't permanent.
function goBack() {
    if (idx <= 0) return;
    idx -= 1;
    const it = queue[idx];
    if (it && it.filedCategory) {
        const undoneCat = it.filedCategory;
        const undoneCount = it.filedCount || 0;
        const undoPayload = it.filedUndo;
        const wasKept = !!it.keptOther;
        it.filedCategory = null;
        it.filedUndo = null;
        it.keptOther = false;
        if (wasKept) keptItems = Math.max(0, keptItems - 1);
        else { movedItems = Math.max(0, movedItems - 1); movedTxns = Math.max(0, movedTxns - undoneCount); }
        dirty = true;
        // Send back the server's own undo payload so ONLY what this filing changed is
        // reverted (a plain "clear this brand's rules" would also wipe rules the user
        // had set up before the tidy session).
        track(postJson('/api/tidy-unfile', { merchant: it.merchant, undo: undoPayload })
            .then(() => showToast(wasKept
                ? `We'll ask about ${esc(_clean(it.display))} again`
                : `Put ${esc(_clean(it.display))} back — reconsider it`))
            .catch(() => {
                it.filedCategory = undoneCat;   // the filing is still in place server-side
                it.filedUndo = undoPayload;
                it.keptOther = wasKept;
                if (wasKept) keptItems += 1;
                else { movedItems += 1; movedTxns += undoneCount; }
                updateHead();
                warnWriteFailed(`the change to ${esc(_clean(it.display))}`);
            }));
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

    // Photos are OPT-IN and LAZY: they show the day's whole camera roll (often unrelated
    // to the purchase), so we hide them behind a toggle AND only fetch them when the user
    // opens it — enriching every card up front was dozens of Photos-DB queries per request.
    const hasSampleDays = (it.sample_days || []).length > 0;
    const photoToggle = hasSampleDays
        ? `<button class="tidy-photo-toggle" id="tidyPhotoToggle">
               <i class="fas fa-camera"></i> View photos from these days
           </button>
           <div class="tidy-photos" id="tidyPhotos" hidden></div>`
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
                <div class="tidy-actions-right">
                    ${keepAvailable() ? `
                    <button class="tidy-keep" id="tidyKeepBtn" title="File it under Other — it's miscellaneous, not unsorted. We won't ask again.">
                        <i class="fas fa-thumbtack"></i> Keep in Other
                    </button>` : ''}
                    <button class="tidy-skip" id="tidySkipBtn" title="Ask me again another time">Skip for now <i class="fas fa-forward"></i></button>
                </div>
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
    const keepBtn = body.querySelector('#tidyKeepBtn');
    if (keepBtn) keepBtn.addEventListener('click', keepCurrent);
    const photoBtn = body.querySelector('#tidyPhotoToggle');
    if (photoBtn) {
        photoBtn.addEventListener('click', () => {
            const strip = body.querySelector('#tidyPhotos');
            const showing = !strip.hasAttribute('hidden');
            if (showing) {
                strip.setAttribute('hidden', '');
                photoBtn.innerHTML = `<i class="fas fa-camera"></i> View photos from these days`;
                return;
            }
            strip.removeAttribute('hidden');
            photoBtn.innerHTML = `<i class="fas fa-eye-slash"></i> Hide photos`;
            loadCardPhotos(it, strip);
        });
    }
}

// Lazily fetch a card's photos the first time its toggle is opened. Results are cached
// on the item so re-toggling (or navigating back to the card) is instant, and a stale
// response can't land in the wrong card because we re-check the visible index.
function loadCardPhotos(item, strip) {
    if (item._photos) { strip.innerHTML = renderPhotoStripHtml(item._photos); return; }
    if (item._photosLoading) return;
    item._photosLoading = true;
    strip.innerHTML = `<div class="tidy-photos-note">Loading photos…</div>`;
    const days = (item.sample_days || []).slice(0, 3).join(',');
    fetch('/api/photos-for-days?days=' + encodeURIComponent(days))
        .then(r => r.json())
        .then(data => {
            item._photos = data.photos || [];
            item._photosLoading = false;
            // Only paint if this card is still the one on screen.
            if (queue[idx] === item) {
                const s = document.getElementById('tidyPhotos');
                if (s && !s.hasAttribute('hidden')) s.innerHTML = renderPhotoStripHtml(item._photos);
            }
        })
        .catch(() => {
            item._photosLoading = false;
            strip.innerHTML = `<div class="tidy-photos-note">Could not load photos.</div>`;
        });
}

function renderPhotoStripHtml(photos) {
    if (!photos.length) {
        return `<div class="tidy-photos-note">No photos from these days.</div>`;
    }
    return photos.map(p => `
        <div class="tidy-photo" title="${esc(fmtDate(p.date))}">
            <img src="/photo/${esc(p.uuid)}" loading="lazy" alt="">
            <span class="tidy-photo-date">${esc(new Date(p.date + 'T00:00:00').toLocaleDateString(undefined, { day: 'numeric', month: 'short' }))}</span>
        </div>`).join('')
        + `<div class="tidy-photos-note">Your camera roll from these days — may not show the actual purchase.</div>`;
}

// The real 'other' category — where "Keep in Other" files a decided merchant.
const KEEP_CATEGORY = 'other';

// The user can delete the 'other' category like any other — don't offer a button that
// would just fail. (Chips still cover every category that does exist.)
const keepAvailable = () => allCategoryNames().some(c => catName(c) === KEEP_CATEGORY);

// File the whole brand group (may span several raw merchant spellings) into `category`
// via the normalized key, so e.g. "LIDL 164" + "LIDL 042" all move together. Then
// advance. Optimistic: counters bump immediately and the dashboard reloads once on
// close, so the flow stays snappy. `onDone(n)` owns the counters + message for the
// specific action (an ordinary filing vs. "Keep in Other"), since only the caller knows
// which one this is.
function fileGroup(it, category, { onDone, onFail } = {}) {
    const card = document.querySelector('.tidy-card');
    if (card) card.classList.add('tidy-card-leaving');

    track(postJson('/api/tidy-file', { merchant: it.merchant, category })
        .then(data => {
            // Report what the server ACTUALLY moved — never the card's own count. A
            // transaction pinned to another category by hand doesn't follow the filing,
            // and claiming otherwise is how "I moved it but it's still there" happens.
            const n = Number.isFinite(data.affected) ? data.affected : it.count;
            // Remember what we did so Back can undo exactly this.
            it.filedCategory = category;
            it.filedCount = n;
            it.filedUndo = data.undo;
            dirty = true;
            if (onDone) onDone(n);
            updateHead();
            setTimeout(next, 180);
        })
        .catch(() => {
            if (card) card.classList.remove('tidy-card-leaving');
            if (onFail) onFail();
            warnWriteFailed(esc(_clean(it.display)));
        }));
}

function fileCurrent(category) {
    const it = queue[idx];
    if (!it) return;
    fileGroup(it, category, {
        onDone: (n) => {
            if (n > 0) { movedItems += 1; movedTxns += n; }
            const label = category.replace(/_/g, ' ').replace(/-/g, ' ');
            showToast(n
                ? `Moved ${n} ${n === 1 ? 'transaction' : 'transactions'} to <b>${esc(label)}</b>`
                : `Nothing to move — these are pinned to another category`);
        },
    });
}

function renderEmpty() {
    const body = document.getElementById('tidyBody');
    body.innerHTML = `
        <div class="tidy-done">
            <div class="tidy-done-emoji">✨</div>
            <h3>Nothing to tidy!</h3>
            <p>Nothing uncategorized is worth filing right now — no recurring or significant merchants left.</p>
            <button class="tidy-done-btn" onclick="App.closeTidy()">Great</button>
        </div>`;
    updateHead();
}

function renderSummary() {
    const body = document.getElementById('tidyBody');
    const fill = document.getElementById('tidyProgressFill');
    if (fill) fill.style.width = '100%';
    const headline = movedItems
        ? `You filed ${movedItems} ${movedItems === 1 ? 'merchant' : 'merchants'} — ${movedTxns} ${movedTxns === 1 ? 'transaction' : 'transactions'} sorted!`
        : keptItems
            ? `${keptItems} ${keptItems === 1 ? 'merchant' : 'merchants'} filed under Other.`
            : `No changes this round — that's fine!`;
    const sub = movedItems
        ? 'Nice work tidying up. Your categories are cleaner now.'
        : keptItems
            ? "They're miscellaneous, not unsorted — we won't bring them up again."
            : 'You can come back and tidy up anytime.';
    body.innerHTML = `
        <div class="tidy-done">
            <div class="tidy-done-emoji">${movedItems ? '🎉' : keptItems ? '📌' : '👍'}</div>
            <h3>${esc(headline)}</h3>
            <p>${esc(sub)}</p>
            <button class="tidy-done-btn" onclick="App.closeTidy()">Done</button>
        </div>`;
    updateHead();
}
