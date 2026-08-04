import { closePhotoLightbox } from './memories.js';
import { closeSidePanel } from './outliers.js';

// ── Panel Close Handlers ───────────────────────
// Elements that OPEN the panel — don't treat their clicks as "outside" clicks.
const PANEL_OPENER_SELECTOR = '[data-category-click], #dailySpendingChart, #currentMonthChart, #categoryChart, #categoryBreakdown, #monthPhotoTimeline, #dailyPhotoRow, #spendBubbles, .film-card, .daily-photo, [data-sugg-idx]';
document.addEventListener('click', function(e) {
    const panel = document.getElementById('sidePanel');
    if (panel.classList.contains('open') && !panel.contains(e.target) && !e.target.closest(PANEL_OPENER_SELECTOR)) {
        closeSidePanel();
    }
});

document.addEventListener('keydown', function(e) {
    if (e.key !== 'Escape') return;
    const lb = document.getElementById('photoLightbox');
    if (lb && lb.classList.contains('open')) {
        closePhotoLightbox();
    } else {
        closeSidePanel();
    }
});
