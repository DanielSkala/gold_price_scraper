// ── Custom categories ──────────────────────────
// Lets the user create their own category (name + color + icon) from the UI.
import { registerCatalog, getCategoryEntry, unregisterCategory } from './constants.js';

function escapeHtmlCat(s) {
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// Icons already used by the built-in categories — excluded so custom ones differ.
const USED_ICONS = new Set([
    'fa-basket-shopping', 'fa-gas-pump', 'fa-square-parking', 'fa-car',
    'fa-oil-can', 'fa-utensils', 'fa-plane', 'fa-taxi', 'fa-couch', 'fa-receipt',
]);

// Curated icons that ALL exist in Font Awesome 6.0.0 free-solid (the version we
// load) — verified so none render blank. Each has keywords for search + name-suggest.
const ICONS = [
    { i: 'fa-tag',                kw: ['tag', 'label', 'other', 'misc'] },
    { i: 'fa-cart-shopping',      kw: ['shopping', 'shop', 'store', 'retail', 'buy'] },
    { i: 'fa-house',              kw: ['home', 'house', 'rent', 'mortgage', 'housing'] },
    { i: 'fa-heart',              kw: ['health', 'love', 'charity', 'donation', 'care'] },
    { i: 'fa-gift',               kw: ['gift', 'present', 'birthday', 'donation'] },
    { i: 'fa-shirt',              kw: ['clothes', 'clothing', 'fashion', 'apparel', 'shirt'] },
    { i: 'fa-dumbbell',           kw: ['gym', 'fitness', 'workout', 'sport', 'exercise'] },
    { i: 'fa-graduation-cap',     kw: ['education', 'school', 'university', 'tuition', 'course', 'study'] },
    { i: 'fa-briefcase',          kw: ['work', 'business', 'office', 'job'] },
    { i: 'fa-mug-hot',            kw: ['coffee', 'cafe', 'tea', 'drink'] },
    { i: 'fa-film',               kw: ['movie', 'cinema', 'film', 'entertainment', 'streaming'] },
    { i: 'fa-gamepad',            kw: ['games', 'gaming', 'game', 'play'] },
    { i: 'fa-paw',                kw: ['pet', 'pets', 'dog', 'cat', 'animal', 'vet'] },
    { i: 'fa-plug',               kw: ['utilities', 'electricity', 'power', 'energy', 'subscription', 'subscriptions'] },
    { i: 'fa-pills',              kw: ['pharmacy', 'medicine', 'health', 'drugs', 'meds'] },
    { i: 'fa-baby',               kw: ['baby', 'kids', 'child', 'children', 'family'] },
    { i: 'fa-wine-glass',         kw: ['alcohol', 'wine', 'bar', 'drinks', 'party'] },
    { i: 'fa-book',               kw: ['books', 'reading', 'education', 'library'] },
    { i: 'fa-music',              kw: ['music', 'concert', 'spotify', 'audio'] },
    { i: 'fa-headphones',         kw: ['music', 'audio', 'podcast', 'headphones'] },
    { i: 'fa-camera',             kw: ['photo', 'camera', 'photography'] },
    { i: 'fa-bicycle',            kw: ['bike', 'bicycle', 'cycling', 'transport'] },
    { i: 'fa-train',              kw: ['train', 'transport', 'commute', 'rail'] },
    { i: 'fa-bus',                kw: ['bus', 'transport', 'commute', 'transit'] },
    { i: 'fa-ship',               kw: ['boat', 'ship', 'ferry', 'cruise'] },
    { i: 'fa-bed',                kw: ['hotel', 'sleep', 'accommodation', 'bed'] },
    { i: 'fa-bath',               kw: ['bath', 'bathroom', 'spa', 'hygiene'] },
    { i: 'fa-scissors',           kw: ['haircut', 'barber', 'salon', 'grooming'] },
    { i: 'fa-heart-pulse',        kw: ['health', 'medical', 'doctor', 'hospital', 'fitness'] },
    { i: 'fa-stethoscope',        kw: ['doctor', 'medical', 'health', 'clinic'] },
    { i: 'fa-tooth',              kw: ['dentist', 'teeth', 'dental', 'health'] },
    { i: 'fa-futbol',             kw: ['football', 'soccer', 'sport', 'ball'] },
    { i: 'fa-palette',            kw: ['art', 'hobby', 'painting', 'creative'] },
    { i: 'fa-guitar',             kw: ['music', 'instrument', 'guitar', 'hobby'] },
    { i: 'fa-dog',                kw: ['dog', 'pet', 'animal'] },
    { i: 'fa-cat',                kw: ['cat', 'pet', 'animal'] },
    { i: 'fa-seedling',           kw: ['plants', 'garden', 'gardening', 'nature'] },
    { i: 'fa-leaf',               kw: ['plants', 'nature', 'eco', 'green'] },
    { i: 'fa-wrench',             kw: ['repair', 'tools', 'maintenance', 'fix'] },
    { i: 'fa-hammer',             kw: ['diy', 'tools', 'repair', 'build', 'home improvement'] },
    { i: 'fa-lightbulb',          kw: ['electricity', 'utilities', 'ideas', 'energy'] },
    { i: 'fa-wifi',               kw: ['internet', 'wifi', 'broadband', 'subscription'] },
    { i: 'fa-laptop',             kw: ['tech', 'computer', 'laptop', 'electronics'] },
    { i: 'fa-desktop',            kw: ['tech', 'computer', 'pc', 'electronics'] },
    { i: 'fa-cloud',              kw: ['cloud', 'storage', 'subscription', 'saas'] },
    { i: 'fa-piggy-bank',         kw: ['savings', 'save', 'bank', 'money'] },
    { i: 'fa-credit-card',        kw: ['card', 'payment', 'fees', 'bank'] },
    { i: 'fa-building-columns',   kw: ['bank', 'tax', 'government', 'finance', 'loan'] },
    { i: 'fa-pizza-slice',        kw: ['pizza', 'food', 'takeaway', 'fast food'] },
    { i: 'fa-ice-cream',          kw: ['icecream', 'dessert', 'sweets', 'treat'] },
    { i: 'fa-mug-saucer',         kw: ['coffee', 'cafe', 'tea', 'breakfast'] },
    { i: 'fa-carrot',             kw: ['vegetables', 'food', 'healthy', 'groceries'] },
    { i: 'fa-fish',               kw: ['fish', 'seafood', 'food'] },
    { i: 'fa-egg',                kw: ['egg', 'breakfast', 'food', 'groceries'] },
    { i: 'fa-shield-halved',      kw: ['insurance', 'security', 'protection', 'safety'] },
    { i: 'fa-key',                kw: ['rent', 'keys', 'housing', 'access'] },
    { i: 'fa-fire',               kw: ['gas', 'heating', 'utilities', 'energy'] },
    { i: 'fa-bolt',               kw: ['electricity', 'power', 'energy', 'utilities'] },
    { i: 'fa-droplet',            kw: ['water', 'utilities', 'bills'] },
    { i: 'fa-glasses',            kw: ['glasses', 'optician', 'eyewear', 'health'] },
    { i: 'fa-ring',               kw: ['jewelry', 'wedding', 'ring', 'gift'] },
    { i: 'fa-gem',                kw: ['jewelry', 'luxury', 'gem', 'gift'] },
    { i: 'fa-crown',              kw: ['premium', 'luxury', 'subscription', 'vip'] },
    { i: 'fa-star',               kw: ['favorite', 'premium', 'special', 'star'] },
    { i: 'fa-plane',              kw: ['flight', 'travel', 'vacation', 'trip'] },
    { i: 'fa-suitcase',           kw: ['travel', 'trip', 'luggage', 'vacation'] },
    { i: 'fa-ticket',             kw: ['tickets', 'events', 'entertainment', 'concert'] },
    { i: 'fa-phone',              kw: ['phone', 'mobile', 'call', 'bills'] },
    { i: 'fa-envelope',           kw: ['mail', 'post', 'letter', 'shipping'] },
    { i: 'fa-box',                kw: ['delivery', 'package', 'shipping', 'parcel'] },
    { i: 'fa-truck',              kw: ['delivery', 'moving', 'freight', 'shipping'] },
    { i: 'fa-toolbox',            kw: ['tools', 'repair', 'maintenance', 'diy'] },
];
// Exclude those used by built-in categories.
const ICON_CHOICES = ICONS.filter(x => !USED_ICONS.has(x.i));

const COLOR_CHOICES = [
    '#0f766e', '#1d4ed8', '#0369a1', '#0891b2', '#4d7c0f',
    '#c2410c', '#b45309', '#be123c', '#7c3aed', '#db2777',
    '#0d9488', '#65a30d', '#9333ea', '#e11d48', '#57534e',
];

// Load the full category catalog at startup so colors/icons/keywords resolve everywhere.
export function loadCustomCategories() {
    return fetch('/api/categories')
        .then(r => r.json())
        .then(d => { registerCatalog(d.catalog || []); })
        .catch(() => {});
}

// Suggest an icon from the typed name using keyword matching. Returns fa-* or null.
function suggestIcon(name) {
    const n = name.trim().toLowerCase();
    if (!n) return null;
    for (const { i, kw } of ICON_CHOICES) {
        if (kw.some(k => n.includes(k) || k.includes(n))) return i;
    }
    return null;
}

let selColor, selIcon;

// Open the dialog. `onCreated(categoryName)` is called after the category is saved.
// `opts` (optional) tailors it for the "add suggested category" flow:
//   { prefill: {name, color, icon},
//     items: [{merchant, total, count}],   // renders a checklist of merchants to include
//     onSubmit: ({name, color, icon, merchants}) => Promise }  // custom save
export function openNewCategoryDialog(onCreated, opts = {}) {
    const prefill = opts.prefill || {};
    const items = opts.items || null;
    const editing = opts.edit || null;   // {name, color, icon} → PUT instead of POST
    selColor = prefill.color || COLOR_CHOICES[8];
    selIcon = prefill.icon || ICON_CHOICES[0].i;
    let expanded = false;
    let query = '';

    const itemsHtml = items ? `
            <label class="newcat-label">Items to include <span class="newcat-count" id="newcatItemCount"></span></label>
            <div class="newcat-items" id="newcatItems">
                ${items.map((it, i) => `
                    <label class="newcat-item">
                        <input type="checkbox" class="newcat-item-cb" data-idx="${i}" checked>
                        <span class="newcat-item-name">${escapeHtmlCat(it.merchant)}</span>
                        <span class="newcat-item-amt">€${Math.round(it.total).toLocaleString()}${it.count > 1 ? ` · ${it.count}×` : ''}</span>
                    </label>
                `).join('')}
            </div>
    ` : '';

    // Keyword field — shown for normal create/edit (not the merchant-checklist flow).
    // Any transaction whose merchant contains one of these words auto-lands here.
    const keywordsVal = (prefill.keywords || []).join(', ');
    const keywordsHtml = items ? '' : `
            <label class="newcat-label">Keywords <span class="newcat-count">merchant words that map here, comma-separated</span></label>
            <input type="text" class="newcat-input" id="newcatKeywords" placeholder="e.g. lidl, tesco, billa" autocomplete="off" value="${escapeHtmlCat(keywordsVal)}">
    `;

    const overlay = document.createElement('div');
    overlay.className = 'newcat-overlay';
    overlay.innerHTML = `
        <div class="newcat-dialog" role="dialog" aria-modal="true">
            <div class="newcat-head">
                <h3>${editing ? 'Edit category' : (items ? 'Add category' : 'New category')}</h3>
                <button class="newcat-close" aria-label="Close"><i class="fas fa-times"></i></button>
            </div>
            <label class="newcat-label">Name</label>
            <input type="text" class="newcat-input" id="newcatName" placeholder="e.g. Subscriptions" maxlength="24" autocomplete="off" value="${escapeHtmlCat(prefill.name || '')}">

            <label class="newcat-label">Color</label>
            <div class="newcat-swatches" id="newcatColors">
                ${COLOR_CHOICES.map(c => `<button class="newcat-swatch" data-color="${c}" style="background:${c}"></button>`).join('')}
            </div>

            <div class="newcat-icon-head">
                <label class="newcat-label" style="margin:0;">Icon</label>
                <div class="newcat-icon-search">
                    <i class="fas fa-magnifying-glass"></i>
                    <input type="text" id="newcatIconSearch" placeholder="Search icons…" autocomplete="off">
                </div>
            </div>
            <div class="newcat-icons" id="newcatIcons"></div>
            <button class="newcat-more" id="newcatMore" type="button"></button>

            ${keywordsHtml}
            ${itemsHtml}

            <div class="newcat-preview">
                Preview: <span class="category-badge newcat-preview-badge" id="newcatPreview"></span>
            </div>

            <div class="newcat-actions">
                <button class="newcat-cancel">Cancel</button>
                <button class="newcat-create">${editing ? 'Save changes' : (items ? 'Add category' : 'Create category')}</button>
            </div>
            <div class="newcat-error" id="newcatError"></div>
        </div>
    `;
    document.body.appendChild(overlay);

    const nameInput = overlay.querySelector('#newcatName');
    const iconSearch = overlay.querySelector('#newcatIconSearch');
    const iconGrid = overlay.querySelector('#newcatIcons');
    const moreBtn = overlay.querySelector('#newcatMore');
    const preview = overlay.querySelector('#newcatPreview');
    const errEl = overlay.querySelector('#newcatError');

    const COLLAPSED_COUNT = 24;   // ~3 rows of 8

    const filtered = () => {
        if (!query) return ICON_CHOICES;
        const q = query.toLowerCase();
        return ICON_CHOICES.filter(x => x.i.includes(q) || x.kw.some(k => k.includes(q)));
    };

    const renderIcons = () => {
        const list = filtered();
        const shown = (expanded || query) ? list : list.slice(0, COLLAPSED_COUNT);
        iconGrid.innerHTML = shown.map(x =>
            `<button class="newcat-icon${x.i === selIcon ? ' sel' : ''}" data-icon="${x.i}" title="${x.i.replace('fa-', '')}"><i class="fas ${x.i}"></i></button>`
        ).join('') || `<div class="newcat-noicons">No icons match “${query}”.</div>`;
        // Show/hide the "more" toggle (only when collapsed & not searching & there's more).
        if (!query && list.length > COLLAPSED_COUNT) {
            moreBtn.style.display = 'block';
            moreBtn.innerHTML = expanded
                ? '<i class="fas fa-chevron-up"></i> Show fewer'
                : `<i class="fas fa-chevron-down"></i> Show all ${list.length} icons`;
        } else {
            moreBtn.style.display = 'none';
        }
    };

    const renderPreview = () => {
        overlay.querySelectorAll('.newcat-swatch').forEach(b =>
            b.classList.toggle('sel', b.dataset.color === selColor));
        overlay.querySelectorAll('.newcat-icon').forEach(b =>
            b.classList.toggle('sel', b.dataset.icon === selIcon));
        const nm = nameInput.value.trim() || 'category';
        preview.style.background = tint(selColor);
        preview.style.color = selColor;
        preview.innerHTML = `<i class="fas ${selIcon} cat-badge-icon"></i>${nm}`;
    };

    // A name change suggests an icon (only until the user manually picks one).
    // If the dialog was prefilled (suggested category), keep the given icon.
    let userPickedIcon = !!(prefill.icon);
    nameInput.addEventListener('input', () => {
        if (!userPickedIcon) {
            const s = suggestIcon(nameInput.value);
            if (s) { selIcon = s; renderIcons(); }
        }
        renderPreview();
    });

    // Items checklist (suggested-category flow): live count of selected items.
    const itemsWrap = overlay.querySelector('#newcatItems');
    const updateItemCount = () => {
        const countEl = overlay.querySelector('#newcatItemCount');
        if (!countEl || !itemsWrap) return;
        const n = itemsWrap.querySelectorAll('.newcat-item-cb:checked').length;
        countEl.textContent = `(${n} selected)`;
    };
    if (itemsWrap) {
        itemsWrap.addEventListener('change', updateItemCount);
        updateItemCount();
    }

    iconSearch.addEventListener('input', () => { query = iconSearch.value.trim(); renderIcons(); });

    overlay.querySelector('#newcatColors').addEventListener('click', (e) => {
        const b = e.target.closest('.newcat-swatch'); if (!b) return;
        selColor = b.dataset.color; renderPreview();
    });
    iconGrid.addEventListener('click', (e) => {
        const b = e.target.closest('.newcat-icon'); if (!b) return;
        selIcon = b.dataset.icon; userPickedIcon = true; renderPreview();
    });
    moreBtn.addEventListener('click', () => { expanded = !expanded; renderIcons(); });

    const close = () => overlay.remove();
    overlay.querySelector('.newcat-close').addEventListener('click', close);
    overlay.querySelector('.newcat-cancel').addEventListener('click', close);
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });
    overlay.addEventListener('keydown', (e) => { if (e.key === 'Escape') close(); });

    const kwInput = overlay.querySelector('#newcatKeywords');
    const keywords = () => (kwInput ? kwInput.value : '');

    overlay.querySelector('.newcat-create').addEventListener('click', () => {
        const name = nameInput.value.trim();
        if (!name) { errEl.textContent = 'Please enter a name.'; nameInput.focus(); return; }
        // After any create/edit the catalog can change wholesale (order, rename) — so
        // re-fetch it before the caller re-renders, keeping the client cache truthful.
        const refreshThen = () => loadCustomCategories().then(() => { close(); if (onCreated) onCreated(name); });

        // Edit flow: PUT the updated name/color/icon/keywords for an existing category.
        if (editing) {
            fetch('/api/custom-categories/' + encodeURIComponent(editing.name), {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name, color: selColor, icon: selIcon, keywords: keywords() }),
            })
                .then(r => r.json())
                .then(d => { if (d.error) { errEl.textContent = d.error; return; } refreshThen(); })
                .catch(() => { errEl.textContent = 'Could not save the category.'; });
            return;
        }

        // Suggested-category flow: custom submit with the selected merchant items.
        if (items && opts.onSubmit) {
            const merchants = [...(itemsWrap ? itemsWrap.querySelectorAll('.newcat-item-cb:checked') : [])]
                .map(cb => items[parseInt(cb.dataset.idx, 10)].merchant);
            if (!merchants.length) { errEl.textContent = 'Select at least one item.'; return; }
            Promise.resolve(opts.onSubmit({ name, color: selColor, icon: selIcon, merchants }))
                .then(() => refreshThen())
                .catch(() => { errEl.textContent = 'Could not create the category.'; });
            return;
        }

        fetch('/api/custom-categories', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, color: selColor, icon: selIcon, keywords: keywords() }),
        })
            .then(r => r.json())
            .then(d => { if (d.error) { errEl.textContent = d.error; return; } refreshThen(); })
            .catch(() => { errEl.textContent = 'Could not create the category.'; });
    });

    renderIcons();
    renderPreview();
    setTimeout(() => nameInput.focus(), 30);
}

// Open the dialog pre-filled to EDIT a category (name / color / icon / keywords).
// Works for any category — built-in or custom. `onSaved(newName)` runs after save.
export function openEditCategoryDialog(name, onSaved) {
    const c = getCategoryEntry(name);
    if (!c) return;
    const p = { name: c.name, color: c.fg, icon: c.icon, keywords: c.keywords || [] };
    openNewCategoryDialog(onSaved, { edit: p, prefill: p });
}

// Delete a category (its transactions move to "other"). `onDeleted()` runs after a
// successful delete. Confirms first — it's destructive.
export function deleteCategory(name, onDeleted) {
    if (!window.confirm(`Delete “${name}”? Its transactions will move to “other”.`)) return;
    fetch('/api/custom-categories/' + encodeURIComponent(name), { method: 'DELETE' })
        .then(r => r.json())
        .then(d => {
            if (d.error) { window.alert(d.error); return; }
            unregisterCategory(name);
            loadCustomCategories().then(() => { if (onDeleted) onDeleted(name); });
        })
        .catch(() => window.alert('Could not delete the category.'));
}

// Same light-tint helper as constants (kept local to avoid an export cycle).
function tint(hex) {
    const h = hex.replace('#', '');
    const r = parseInt(h.slice(0, 2), 16), g = parseInt(h.slice(2, 4), 16), b = parseInt(h.slice(4, 6), 16);
    const mix = (c) => Math.round(c + (247 - c) * 0.88);
    return `rgb(${mix(r)},${mix(g)},${mix(b)})`;
}
