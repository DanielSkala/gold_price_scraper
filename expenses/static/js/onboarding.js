import { loadAllData } from './loading.js';

// ── Onboarding / empty state ────────────────────────────────────────────────
// When a fresh clone has no expense reports, show a full-screen prompt with the
// TatraBanka export steps, an upload button, and a "try sample data" shortcut.

function overlay() { return document.getElementById('onboarding'); }

function show() {
    const el = overlay();
    if (el) el.style.display = 'flex';
    // Hide the dashboard chrome behind it so it reads as a clean first-run screen.
    document.body.classList.add('onboarding-active');
}

function hide() {
    const el = overlay();
    if (el) el.style.display = 'none';
    document.body.classList.remove('onboarding-active');
}

function setMsg(text, kind) {
    const el = document.getElementById('onboardMsg');
    if (!el) return;
    el.textContent = text || '';
    el.className = 'onboarding-msg' + (kind ? ' ' + kind : '');
}

// Check once at startup whether any data exists; show/hide the overlay accordingly.
export function checkOnboarding() {
    return fetch('/api/reports')
        .then(r => r.json())
        .then(d => {
            if (d && d.has_data) { hide(); return true; }
            show();
            return false;
        })
        .catch(() => true);   // on error, don't block the app
}

function afterDataAdded() {
    hide();
    loadAllData();
}

export function initOnboarding() {
    const fileInput = document.getElementById('onboardFile');
    if (fileInput) {
        fileInput.addEventListener('change', () => {
            const file = fileInput.files && fileInput.files[0];
            if (!file) return;
            setMsg('Uploading…', '');
            const fd = new FormData();
            fd.append('file', file);
            fetch('/api/reports', { method: 'POST', body: fd })
                .then(r => r.json())
                .then(d => {
                    fileInput.value = '';
                    if (d.error) { setMsg(d.error, 'error'); return; }
                    setMsg('', '');
                    afterDataAdded();
                })
                .catch(() => { fileInput.value = ''; setMsg('Upload failed.', 'error'); });
        });
    }

    const sampleBtn = document.getElementById('onboardSample');
    if (sampleBtn) {
        sampleBtn.addEventListener('click', () => {
            setMsg('Loading sample data…', '');
            fetch('/api/reports/load-samples', { method: 'POST' })
                .then(r => r.json())
                .then(d => {
                    if (d.error) { setMsg(d.error, 'error'); return; }
                    setMsg('', '');
                    afterDataAdded();
                })
                .catch(() => setMsg('Could not load sample data.', 'error'));
        });
    }
}
