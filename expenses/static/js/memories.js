import { showDayDetails } from './panel.js';

// ── Memories: month photo timeline (Apple-style bubbles) ──
export function renderLocationChips(container, locations) {
    if (!locations || locations.length === 0) {
        container.innerHTML = '';
        return;
    }
    container.innerHTML = locations.map(loc => `
        <span class="location-chip">
            <i class="fas fa-map-marker-alt"></i>
            ${loc.name}
            <span class="chip-count">${loc.count}</span>
        </span>
    `).join('');
}

// One dominant hero bubble + smaller satellites around it, within a ~138px cluster box.
// Hero photos are enlarged ~15% and satellites shrunk ~15% for stronger contrast.
export function bubbleLayout(n) {
    const layouts = {
        1: [{ s: 118, x: 10, y: 10, hero: true }],
        2: [{ s: 97, x: 4, y: 20, hero: true }, { s: 43, x: 90, y: 4 }],
        3: [{ s: 92, x: 2, y: 16, hero: true }, { s: 41, x: 92, y: 2 }, { s: 39, x: 94, y: 66 }],
        4: [{ s: 90, x: 0, y: 12, hero: true }, { s: 40, x: 96, y: 0 }, { s: 37, x: 98, y: 56 }, { s: 34, x: 46, y: 96 }],
    };
    return layouts[Math.min(n, 4)] || layouts[4];
}

export function renderDayCluster(day, spend) {
    const photos = day.photos || [];
    if (photos.length === 0) return '';
    const layout = bubbleLayout(photos.length);
    const bubbles = photos.slice(0, layout.length).map((p, i) => {
        const pos = layout[i];
        const time = (p.taken_at || '').split(' ')[1]?.slice(0, 5) || '';
        const geo = (p.lat != null && p.lng != null)
            ? `https://www.google.com/maps?q=${p.lat},${p.lng}`
            : null;
        // Per-bubble float params so bubbles drift out of sync (seeded by index).
        // Slow + subtle: long durations, small travel — a barely-there drift.
        const dur = (11 + (i * 1.7) % 5).toFixed(2);
        const delay = (-(i * 2.1) % 6).toFixed(2);
        const fx = (i % 2 === 0 ? 1.5 : -1.5);
        const fy = -(2 + (i % 3));
        const heroCls = pos.hero ? ' is-hero' : '';
        return `
            <div class="tl-bubble${heroCls}"
                 style="width:${pos.s}px;height:${pos.s}px;left:${pos.x}px;top:${pos.y}px;z-index:${pos.hero ? 6 : i + 7};
                        --dur:${dur}s;--delay:${delay}s;--fx:${fx}px;--fy:${fy}px;">
                <div class="tl-bubble-fx"
                     onclick="event.stopPropagation(); App.openPhotoLightbox('${p.uuid}', '${day.date} ${time}', ${geo ? `'${geo}'` : 'null'})">
                    <img class="glass-base" src="/photo/${p.uuid}" loading="lazy" alt="">
                </div>
            </div>
        `;
    }).join('');

    const remaining = day.total - Math.min(photos.length, layout.length);
    const moreBadge = remaining > 0
        ? `<div class="tl-more" onclick="event.stopPropagation(); App.showDayDetails('${day.date}')">+${remaining}</div>`
        : '';

    const d = new Date(day.date + 'T00:00:00');
    const dow = d.toLocaleDateString(undefined, { weekday: 'short' });
    const dayNum = d.getDate();
    const locLabel = day.primary_location
        ? `<div class="tl-day-loc"><i class="fas fa-map-marker-alt"></i>${day.primary_location}</div>`
        : '';

    // Spend for this day (may be undefined/0 if nothing was spent).
    const amount = spend && spend[day.date] ? spend[day.date] : 0;
    const spendLabel = amount > 0
        ? `<div class="tl-day-spend">€${Math.round(amount)}</div>`
        : '';

    return `
        <div class="tl-day" onclick="App.showDayDetails('${day.date}')" style="cursor:pointer;">
            <div class="tl-cluster">
                ${bubbles}
                ${moreBadge}
            </div>
            <div class="tl-node-dot"></div>
            <div class="tl-day-dow">${dow}</div>
            <div class="tl-day-label">${dayNum}</div>
            ${spendLabel}
            ${locLabel}
        </div>
    `;
}

// Subtle cursor parallax: bubbles lean slightly toward the pointer.
export function attachBubbleParallax(timeline) {
    if (timeline.dataset.parallaxBound === '1') return; // bind once per element
    timeline.dataset.parallaxBound = '1';
    let raf = null;
    let lastEvt = null;
    const onMove = (e) => {
        lastEvt = e;
        if (raf) return;
        raf = requestAnimationFrame(() => {
            raf = null;
            const bubbles = timeline.querySelectorAll('.tl-bubble-fx');
            bubbles.forEach(fx => {
                const r = fx.getBoundingClientRect();
                const cx = r.left + r.width / 2;
                const cy = r.top + r.height / 2;
                const dx = lastEvt.clientX - cx;
                const dy = lastEvt.clientY - cy;
                const dist = Math.hypot(dx, dy);
                // Only bubbles near the cursor respond; effect fades with distance.
                const radius = 150;
                if (dist < radius) {
                    const strength = (1 - dist / radius) * 3; // max 3px lean — very subtle
                    fx.style.setProperty('--px', (dx / dist * strength).toFixed(2) + 'px');
                    fx.style.setProperty('--py', (dy / dist * strength).toFixed(2) + 'px');
                } else {
                    fx.style.setProperty('--px', '0px');
                    fx.style.setProperty('--py', '0px');
                }
            });
        });
    };
    const onLeave = () => {
        timeline.querySelectorAll('.tl-bubble-fx').forEach(fx => {
            fx.style.setProperty('--px', '0px');
            fx.style.setProperty('--py', '0px');
        });
    };
    timeline.addEventListener('mousemove', onMove);
    timeline.addEventListener('mouseleave', onLeave);
}

export function renderMonthMemories(monthStr, monthLabel, dailySpending = {}) {
    const container = document.getElementById('monthMemoriesContainer');
    const timeline = document.getElementById('monthPhotoTimeline');
    const chips = document.getElementById('monthLocationChips');
    const title = document.getElementById('monthMemoriesTitle');

    container.style.display = 'block';
    title.textContent = `${monthLabel} · Timeline`;
    chips.innerHTML = '';
    timeline.innerHTML = `<div class="tl-loading"><i class="fas fa-spinner fa-spin"></i> Loading memories…</div>`;

    fetch(`/api/photos-timeline/${monthStr}?per_day=4`)
        .then(r => r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status)))
        .then(data => {
            if (!data || !data.days || data.days.length === 0) {
                container.style.display = 'none';
                return;
            }
            renderLocationChips(chips, data.top_locations);
            timeline.innerHTML = data.days.map(d => renderDayCluster(d, dailySpending)).join('');
            attachBubbleParallax(timeline);
        })
        .catch(err => {
            console.error('Failed to load month timeline:', err);
            container.style.display = 'none';
        });
}

// Holds the current day panel's photo data so "Show all" can re-render without refetching.
let panelPhotoData = { photos: [], curated: [], top_locations: [], expanded: false };

export function photoTileHtml(p) {
    const timeStr = p.taken_at.split(' ')[1].slice(0, 5);
    const geo = (p.lat != null && p.lng != null)
        ? `https://www.google.com/maps?q=${p.lat},${p.lng}`
        : null;
    return `
        <div class="photo-tile" onclick="App.openPhotoLightbox('${p.uuid}', '${timeStr}', ${geo ? `'${geo}'` : 'null'})">
            <img src="/photo/${p.uuid}" loading="lazy" alt="${timeStr}">
            <div class="photo-tile-meta">
                <span>${timeStr}</span>
                ${geo ? `<i class="fas fa-map-marker-alt" title="Has location"></i>` : ''}
            </div>
        </div>
    `;
}

export function renderPanelPhotoBody() {
    const host = document.getElementById('panelPhotoStrip');
    const { photos, curated, top_locations, expanded } = panelPhotoData;

    const locChipsHtml = (top_locations && top_locations.length)
        ? `<div class="location-chips" style="margin-bottom:8px;">${top_locations.map(loc => `
            <span class="location-chip">
                <i class="fas fa-map-marker-alt"></i> ${loc.name}
                <span class="chip-count">${loc.count}</span>
            </span>`).join('')}</div>`
        : '';

    const shown = expanded ? photos : curated;
    const hasMore = curated.length < photos.length;

    // When collapsed on a busy day, note that we're showing highlights.
    const label = expanded
        ? `<i class="fas fa-images"></i> All photos <span style="color:var(--text-3);font-weight:400;">— ${photos.length}</span>`
        : (hasMore
            ? `<i class="fas fa-images"></i> Highlights <span style="color:var(--text-3);font-weight:400;">— ${curated.length} of ${photos.length}</span>`
            : `<i class="fas fa-images"></i> Photos from this day`);

    // Expanded view uses a wrapping grid; collapsed uses the horizontal strip.
    const galleryClass = expanded ? 'photo-strip photo-strip-grid' : 'photo-strip';

    const toggleBtn = hasMore
        ? `<button class="photo-show-all" onclick="event.stopPropagation(); App.togglePanelPhotos()">
               ${expanded ? `<i class="fas fa-chevron-up"></i> Show highlights` : `<i class="fas fa-images"></i> Show all ${photos.length}`}
           </button>`
        : '';

    host.innerHTML = `
        <div class="photo-strip-label"><span>${label}</span></div>
        ${locChipsHtml}
        <div class="${galleryClass}">
            ${shown.map(photoTileHtml).join('')}
        </div>
        ${toggleBtn}
    `;
}

export function togglePanelPhotos() {
    panelPhotoData.expanded = !panelPhotoData.expanded;
    renderPanelPhotoBody();
}

export function renderPhotoStrip(date) {
    const host = document.getElementById('panelPhotoStrip');
    if (!host) return;
    host.innerHTML = `<div class="photo-strip-loading">Loading photos…</div>`;

    fetch(`/api/photos-for-day/${date}?curated=9`)
        .then(r => r.json())
        .then(data => {
            const photos = data.photos || [];
            if (photos.length === 0) {
                host.innerHTML = `
                    <div class="photo-strip-empty">
                        <i class="fas fa-camera"></i> No photos from this day
                    </div>
                `;
                return;
            }
            panelPhotoData = {
                photos,
                curated: data.curated || photos,
                top_locations: data.top_locations || [],
                expanded: false,
            };
            renderPanelPhotoBody();
        })
        .catch(err => {
            console.error('Failed to load photos:', err);
            host.innerHTML = `<div class="photo-strip-empty">Could not load photos.</div>`;
        });
}

export function openPhotoLightbox(uuid, timeStr, mapUrl) {
    const lb = document.getElementById('photoLightbox');
    document.getElementById('photoLightboxImg').src = `/photo/${uuid}?size=large`;
    document.getElementById('photoLightboxTime').textContent = timeStr;
    const mapLink = document.getElementById('photoLightboxMap');
    if (mapUrl) {
        mapLink.href = mapUrl;
        mapLink.style.display = 'inline-flex';
    } else {
        mapLink.style.display = 'none';
    }
    lb.classList.add('open');
}

export function closePhotoLightbox() {
    document.getElementById('photoLightbox').classList.remove('open');
    document.getElementById('photoLightboxImg').src = '';
}
