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
    // category key → { avg: €/month for an average Slovak, label, icon, ctx }
    groceries:    { avg: 260, label: 'groceries', icon: 'fa-basket-shopping', ctx: 'an average Slovak' },
    gas_stations: { avg: 120, label: 'fuel',      icon: 'fa-gas-pump',        ctx: 'a typical Slovak driver' },
    eating_out:   { avg: 90,  label: 'eating out', icon: 'fa-utensils',       ctx: 'an average Slovak' },
    bolt:         { avg: 30,  label: 'taxis',      icon: 'fa-taxi',           ctx: 'an average city commuter' },
    travel:       { avg: 110, label: 'travel',     icon: 'fa-plane',          ctx: 'an average European' },
    parking:      { avg: 25,  label: 'parking',    icon: 'fa-square-parking', ctx: 'a typical Slovak driver' },
    car_wash:     { avg: 12,  label: 'car care',   icon: 'fa-car',            ctx: 'an average driver' },
};

// A few cross-category "you spend on X what an average family spends on Y" lines.
// Each references a relatable Slovak monthly figure for flavour.
const RELATABLE = [
    { amount: 30,  text: 'a monthly public-transport pass' },
    { amount: 17,  text: 'a month of home internet' },
    { amount: 8,   text: 'a restaurant lunch' },
    { amount: 231, text: 'the monthly utilities of an 85m² flat' },
];

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
    return { tone, text, icon: bench.icon };
}

// Pick a relatable cross-reference for a category's monthly spend.
function relatableLine(userAvg, label) {
    // Find the relatable item whose multiple lands closest to a clean small integer.
    let best = null;
    for (const r of RELATABLE) {
        const mult = userAvg / r.amount;
        if (mult < 1.3) continue;
        const rounded = Math.round(mult);
        const err = Math.abs(mult - rounded);
        if (!best || err < best.err) best = { ...r, mult: rounded, err };
    }
    if (!best) return null;
    return {
        tone: 'neutral',
        icon: 'fa-lightbulb',
        text: `Your monthly <strong>${label}</strong> spend equals <strong>${best.mult}×</strong> ${best.text}.`,
        meta: 'Fun comparison',
    };
}

export function renderFunFacts() {
    const host = document.getElementById('funFacts');
    if (!host) return;
    const averages = state.categoryAverages && state.categoryAverages.overall;
    if (!averages) { host.innerHTML = ''; return; }

    const facts = [];
    // Category-vs-benchmark comparisons, richest categories first.
    const ranked = Object.keys(BENCHMARKS)
        .map(k => ({ k, avg: averages[k] || 0 }))
        .filter(x => x.avg > 3)                    // ignore near-zero categories
        .sort((a, b) => b.avg - a.avg);

    for (const { k, avg } of ranked) {
        const c = compare(avg, BENCHMARKS[k]);
        facts.push({ ...c, meta: 'vs. national average' });
        if (facts.length >= 4) break;
    }

    // Fill a 4th tile with a relatable cross-reference if we have room.
    if (facts.length < 4 && ranked.length) {
        const top = ranked[0];
        const rel = relatableLine(top.avg, BENCHMARKS[top.k].label);
        if (rel) facts.push(rel);
    }

    const section = document.getElementById('funFactsSection');
    if (!facts.length) {
        host.innerHTML = '';
        if (section) section.style.display = 'none';
        return;
    }
    if (section) section.style.display = '';
    // Small tiles: a muted pastel icon + concise comparison text.
    host.innerHTML = facts.slice(0, 4).map(f => `
        <div class="ov-tile tone-${f.tone}">
            <span class="ov-tile-ico"><i class="fas ${f.icon}"></i></span>
            <span class="ov-tile-text">${f.text}</span>
        </div>
    `).join('');
}
