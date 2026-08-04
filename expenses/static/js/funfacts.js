import { state } from './state.js';

// ── Fun facts: your spending vs. national / EU averages ───────────────────
// Compares the user's average MONTHLY spend per category against rough benchmark
// figures for a single person in Slovakia / the EU, then phrases each comparison
// as a light positive / negative / neutral message.
//
// Benchmarks are approximate monthly euro figures for one person (see the
// `spending-benchmarks` memory for sources: Numbeo Slovakia + Eurostat 2024).
// They're deliberately ballpark — this is a fun feature, not accounting.

const BENCHMARKS = {
    // category key (MUST match the category names in category-averages) →
    // { avg: €/month benchmark, label, icon, ctx, src }
    groceries:      { avg: 260,   label: 'groceries', icon: 'fa-basket-shopping', ctx: 'an average Slovak', src: 'Numbeo Slovakia food basket, 2025' },
    'gas stations': { avg: 120,   label: 'fuel',      icon: 'fa-gas-pump',        ctx: 'a typical Slovak driver', src: 'Numbeo fuel price × avg usage' },
    'eating out':   { avg: 90,    label: 'eating out', icon: 'fa-utensils',       ctx: 'an average Slovak', src: 'Eurostat 2024 restaurants share' },
    bolt:           { avg: 30,    label: 'taxis',      icon: 'fa-taxi',           ctx: 'an average city commuter', src: 'Numbeo Slovakia taxi tariff' },
    travel:         { avg: 110,   label: 'travel',     icon: 'fa-plane',          ctx: 'an average European', src: 'Eurostat 2024 recreation/travel' },
    parking:        { avg: 25,    label: 'parking',    icon: 'fa-square-parking', ctx: 'a typical Slovak driver', src: 'Numbeo Slovakia parking' },
    'car wash':     { avg: 12,    label: 'car care',   icon: 'fa-car',            ctx: 'an average driver', src: 'typical wash cadence' },
    // Gym vs a Slovak fitness-club membership (Numbeo Slovakia 2025: €39.83/mo for one adult).
    Gym:            { avg: 40,    label: 'the gym',   icon: 'fa-dumbbell',        ctx: 'a fitness-club membership', src: 'Numbeo Slovakia fitness club, monthly fee, 2025 (€39.83)' },
    // Subscriptions vs typical digital bills — Slovak broadband (€17.59) + mobile plan
    // (€22.43) ≈ €40/mo (Numbeo Slovakia 2025).
    Subscriptions:  { avg: 40,    label: 'subscriptions', icon: 'fa-cloud',       ctx: 'a typical internet + mobile bill', src: 'Numbeo Slovakia broadband €17.59 + mobile €22.43, 2025' },
};

const fmt = (n) => `€${Math.round(n).toLocaleString()}`;

// Build one comparison message + tone for a category vs its benchmark.
function compare(userAvg, bench) {
    const ratio = userAvg / bench.avg;
    let tone, text;
    if (ratio <= 0.55) {
        tone = 'pos';
        const frac = ratio <= 0.35 ? 'a third' : 'about half';
        text = `Your <strong>${bench.label}</strong> spend is ${frac} of what ${bench.ctx} spends — <strong>${fmt(userAvg)}</strong> vs ${fmt(bench.avg)}/mo.`;
    } else if (ratio < 0.9) {
        tone = 'pos';
        text = `You spend a bit less on <strong>${bench.label}</strong> than ${bench.ctx}: <strong>${fmt(userAvg)}</strong> vs ${fmt(bench.avg)}/mo.`;
    } else if (ratio <= 1.15) {
        tone = 'neutral';
        text = `Your <strong>${bench.label}</strong> spend is right around the average — <strong>${fmt(userAvg)}</strong> vs ${fmt(bench.avg)}/mo for ${bench.ctx}.`;
    } else if (ratio <= 1.8) {
        tone = 'warn';
        text = `You spend <strong>${(ratio).toFixed(1)}×</strong> the average on <strong>${bench.label}</strong> — <strong>${fmt(userAvg)}</strong> vs ${fmt(bench.avg)}/mo.`;
    } else {
        tone = 'neg';
        text = `Your <strong>${bench.label}</strong> spend is <strong>${Math.round(ratio)}×</strong> what ${bench.ctx} spends: <strong>${fmt(userAvg)}</strong> vs ${fmt(bench.avg)}/mo.`;
    }
    return { tone, text, icon: bench.icon, src: bench.src };
}

// Tone → the color used for the user's bar (semantic, token-driven).
const TONE_COLOR = {
    pos: 'var(--pos)', neutral: 'var(--text-3)', warn: 'var(--caution)', neg: 'var(--neg)',
};

export function renderFunFacts() {
    const host = document.getElementById('funFacts');
    if (!host) return;
    const averages = state.categoryAverages && state.categoryAverages.overall;
    if (!averages) { host.innerHTML = ''; return; }

    // Category-vs-benchmark comparisons, richest categories first.
    const ranked = Object.keys(BENCHMARKS)
        .map(k => ({ k, avg: averages[k] || 0 }))
        .filter(x => x.avg > 3)                    // ignore near-zero categories
        .sort((a, b) => b.avg - a.avg)
        .slice(0, 6);

    const section = document.getElementById('funFactsSection');
    if (!ranked.length) {
        host.innerHTML = '';
        if (section) section.style.display = 'none';
        return;
    }
    if (section) section.style.display = '';

    // Horizontal "you vs typical" bars: a shared axis (0 → max), a baseline tick at the
    // "typical" value, and the user's bar coloured by tone. The multiplier is the callout.
    // Distinct shape from the recommendation LIST and the recurring TIMELINE.
    const rows = ranked.map(({ k, avg }) => {
        const b = BENCHMARKS[k];
        const ratio = avg / b.avg;
        const c = compare(avg, b);
        // Scale: fit both the user value and the baseline; cap the axis so one huge
        // outlier doesn't flatten the rest, but always leave the baseline visible.
        const axisMax = Math.max(avg, b.avg) * 1.15;
        const userPct = Math.min(100, (avg / axisMax) * 100);
        const basePct = Math.min(100, (b.avg / axisMax) * 100);
        const mult = ratio >= 0.95 && ratio <= 1.05 ? '≈ average'
            : ratio >= 1 ? `${ratio.toFixed(1)}×` : `${Math.round(ratio * 100)}%`;
        return `
            <div class="cmp-row">
                <div class="cmp-head">
                    <span class="cmp-label"><i class="fas ${b.icon}"></i> ${esc(b.label)}</span>
                    <span class="cmp-mult tone-${c.tone}">${mult} <span class="cmp-mult-sub">of ${esc(b.ctx)}</span></span>
                </div>
                <div class="cmp-track" title="You €${Math.round(avg)}/mo · typical €${b.avg}/mo">
                    <span class="cmp-fill" style="width:${userPct.toFixed(1)}%;background:${TONE_COLOR[c.tone] || 'var(--text-3)'}"></span>
                    <span class="cmp-base" style="left:${basePct.toFixed(1)}%" title="Typical: €${b.avg}/mo"></span>
                </div>
                <div class="cmp-vals">
                    <span class="cmp-you">You €${Math.round(avg).toLocaleString()}/mo</span>
                    <span class="cmp-typ">typical €${b.avg}/mo</span>
                </div>
            </div>`;
    }).join('');

    // Choose a column count that leaves no ragged last row: 4 → 2×2, others → up to 3.
    const n = ranked.length;
    const cols = n === 4 ? 2 : Math.min(3, n);
    host.innerHTML = `<div class="cmp-list" data-cols="${cols}">${rows}</div>`;
}

function esc(s) {
    return String(s).replace(/[&<>"]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;' }[c]));
}
