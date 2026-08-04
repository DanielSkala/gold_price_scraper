import { state } from './state.js';
import { STAT_ACCENTS, escapeHtml } from './constants.js';
import { renderSpendBubbles } from './bubbles.js';
import { renderFunFacts } from './funfacts.js';

// ── Stats ──────────────────────────────────────
// The Overview view renders a bespoke header (compact stats on the left, floating
// spend-day photo bubbles on the right). The Monthly view uses the shared 4-card grid.
export function renderStats() {
    if (state.currentView === 'overview') {
        renderOverviewHeader();
        return;
    }
    const grid = document.getElementById('statsGrid');
    if (!state.currentMonthDetails) return;
    const monthTotal = Object.values(state.currentMonthDetails.category_totals).reduce((a, b) => a + b, 0);
    const topMonthCat = Object.entries(state.currentMonthDetails.category_totals)
        .filter(([_, v]) => v > 0).reduce((a, b) => a[1] > b[1] ? a : b, ['uncategorized', 0]);
    const avgDaily = monthTotal / (Object.keys(state.currentMonthDetails.daily_spending || {}).length || 1);

    grid.innerHTML = [
        createStatCard('Month Total', `€${monthTotal.toFixed(0)}`, 0),
        createStatCard('Avg Per Day', `€${avgDaily.toFixed(0)}`, 1),
        createStatCard('Top Category', escapeHtml(topMonthCat[0].replace('_', ' ').replace('-', ' ')), 2, `€${topMonthCat[1].toFixed(0)}`),
        createStatCard('Transactions', state.currentMonthDetails.total_transactions, 3)
    ].join('');
}

// Overview hero — "The Wave": a warm greeting + one rotating positive highlight +
// the all-time stats, over a soft wave drawn from the monthly-spend series. No
// judgmental budget ring; a subtle over-budget chip shows ONLY for the live month.
export function renderOverviewHeader() {
    const host = document.getElementById('overviewStats');
    if (!host) return;
    if (!state.categoryTotals || !state.monthlyData || !state.monthlyData.totals) return;

    const totalSpending = Object.values(state.categoryTotals).reduce((a, b) => a + b, 0);
    const catEntries = Object.entries(state.categoryTotals);
    if (!catEntries.length) return;   // no data yet — nothing meaningful to show
    const topCategory = catEntries.reduce((a, b) => a[1] > b[1] ? a : b);
    const totals = state.monthlyData.totals;
    const months = state.monthlyData.months || [];
    const avgMonthly = totals.length ? totals.reduce((a, b) => a + b, 0) / totals.length : 0;
    const txnCount = Array.isArray(state.transactions) ? state.transactions.length : null;
    const catCount = catEntries.filter(([_, v]) => v > 0).length;

    // Greeting eyebrow (time-of-day). Purely welcoming, never judgmental.
    const eyebrow = document.getElementById('heroEyebrow');
    if (eyebrow) eyebrow.textContent = greeting();

    // Rotating positive/interesting highlight as the headline.
    const headline = document.getElementById('heroHeadline');
    if (headline) headline.innerHTML = pickHighlight({
        totalSpending, avgMonthly, months, totals, topCategory, catCount, txnCount,
    });

    // Soft decorative wave from the monthly-spend series (the "overview of the overview").
    renderHeroWave(totals);

    // Subtle over-budget chip — ONLY when the CURRENT (live) month is over goal.
    renderBudgetChip();

    // All-time stats grid.
    const topName = topCategory[0].replace('_', ' ').replace('-', ' ');
    host.innerHTML = [
        strI('Total tracked', `€${Math.round(totalSpending).toLocaleString()}`),
        strI('Avg / month', `€${Math.round(avgMonthly).toLocaleString()}`),
        strI('Months', months.length),
        strI(txnCount != null ? 'Purchases' : 'Categories',
             txnCount != null ? txnCount.toLocaleString() : catCount),
    ].join('');

    // Photo filmstrip of biggest-spend days + the "how you compare" section.
    renderSpendBubbles();
    renderFunFacts();
}

// One item in the all-time stat grid.
function strI(label, value, sub) {
    return `<span class="stat-strip-item">
        <span class="ss-label">${label}</span>
        <span class="ss-value">${value}${sub ? `<span class="ss-sub">${sub}</span>` : ''}</span>
    </span>`;
}

// Time-of-day greeting.
function greeting() {
    const h = new Date().getHours();
    if (h < 5) return 'Good night';
    if (h < 12) return 'Good morning';
    if (h < 18) return 'Good afternoon';
    return 'Good evening';
}

const MONTH_NAMES = ['January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'];
function prettyMonth(ym) {
    if (!ym) return '';
    const [y, m] = ym.split('-');
    return `${MONTH_NAMES[(+m) - 1] || ''} ${y}`;
}

// Build a set of warm, positive/interesting one-liners from the data and pick one.
// Rotates each visit (varies by day) so the hero feels fresh, like the photos did.
function pickHighlight({ totalSpending, avgMonthly, months, totals, topCategory, catCount, txnCount }) {
    const facts = [];
    const eur = (n) => `€${Math.round(n).toLocaleString()}`;
    const em = (s) => `<span class="hero-accent">${escapeHtml(s)}</span>`;

    if (months.length) {
        facts.push(`You've tracked ${em(eur(totalSpending))} across ${em(months.length + ' months')}.`);
    }
    if (avgMonthly > 0) {
        facts.push(`You spend about ${em(eur(avgMonthly) + '/mo')} on average — steady as she goes.`);
    }
    // Calmest (lowest-spend) month — a genuinely positive nugget.
    if (totals.length >= 3) {
        let minI = 0;
        for (let i = 1; i < totals.length; i++) if (totals[i] < totals[minI]) minI = i;
        if (months[minI]) facts.push(`Your calmest month was ${em(prettyMonth(months[minI]))} — just ${em(eur(totals[minI]))}.`);
    }
    // Recent trend, phrased gently (down = nice, up = neutral-curious, flat = calm).
    if (totals.length >= 6) {
        const recent = totals.slice(-3).reduce((a, b) => a + b, 0) / 3;
        const prior = totals.slice(-6, -3).reduce((a, b) => a + b, 0) / 3;
        if (prior > 0) {
            const chg = (recent - prior) / prior;
            if (chg <= -0.05) facts.push(`Nice — your spending is ${em(Math.round(-chg * 100) + '% lower')} than a few months ago.`);
            else if (chg >= 0.05) facts.push(`Your recent months run a little higher — worth a glance below.`);
            else facts.push(`Your last few months have been remarkably ${em('steady')}.`);
        }
    }
    if (catCount) {
        facts.push(`Your spending spreads across ${em(catCount + ' categories')} — a full picture below.`);
    }
    if (txnCount) {
        facts.push(`That's ${em(txnCount.toLocaleString() + ' purchases')} — all sorted and ready to explore.`);
    }
    if (!facts.length) return 'Your spending at a glance';

    // Rotate by day-of-year so it changes between visits but is stable within a day.
    const now = new Date();
    const doy = Math.floor((now - new Date(now.getFullYear(), 0, 0)) / 86400000);
    return facts[doy % facts.length];
}

// Draw a soft, smooth area wave from the monthly-spend series — decorative, calming,
// and literally a picture of the whole history. Normalised to the SVG viewBox.
function renderHeroWave(totals) {
    const svg = document.getElementById('heroWave');
    if (!svg) return;
    const W = 1200, H = 160;
    const vals = (totals || []).filter(v => typeof v === 'number');
    if (vals.length < 2) { svg.innerHTML = ''; return; }

    const max = Math.max(...vals) || 1;
    const min = Math.min(...vals);
    const range = (max - min) || 1;
    // The wave lives in its own bottom band (no text over it), so it can use most of the
    // vertical space; a little top padding keeps the peaks from touching the top edge.
    const topPad = 22, botPad = 6;
    const usable = H - topPad - botPad;
    const pts = vals.map((v, i) => {
        const x = (i / (vals.length - 1)) * W;
        const y = topPad + (1 - (v - min) / range) * usable;
        return [x, y];
    });
    // Smooth path via Catmull-Rom → cubic Bézier.
    let d = `M ${pts[0][0].toFixed(1)} ${pts[0][1].toFixed(1)}`;
    for (let i = 0; i < pts.length - 1; i++) {
        const p0 = pts[i - 1] || pts[i];
        const p1 = pts[i];
        const p2 = pts[i + 1];
        const p3 = pts[i + 2] || p2;
        const c1x = p1[0] + (p2[0] - p0[0]) / 6;
        const c1y = p1[1] + (p2[1] - p0[1]) / 6;
        const c2x = p2[0] - (p3[0] - p1[0]) / 6;
        const c2y = p2[1] - (p3[1] - p1[1]) / 6;
        d += ` C ${c1x.toFixed(1)} ${c1y.toFixed(1)}, ${c2x.toFixed(1)} ${c2y.toFixed(1)}, ${p2[0].toFixed(1)} ${p2[1].toFixed(1)}`;
    }
    const area = `${d} L ${W} ${H} L 0 ${H} Z`;
    svg.innerHTML = `
        <defs>
            <linearGradient id="heroWaveFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="var(--primary)" stop-opacity="0.20"></stop>
                <stop offset="100%" stop-color="var(--primary)" stop-opacity="0.02"></stop>
            </linearGradient>
        </defs>
        <path d="${area}" fill="url(#heroWaveFill)"></path>
        <path d="${d}" fill="none" stroke="var(--primary)" stroke-opacity="0.45"
              stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"></path>
    `;
}

// Subtle budget chip — appears ONLY when the live (current) month is over goal. It is
// informative, not alarming: a small amber pill, never a big red ring, and never for a
// finished past month (which would be a meaningless "you're over" on old data).
function renderBudgetChip() {
    const chip = document.getElementById('heroBudgetChip');
    if (!chip) return;
    const cmd = state.currentMonthDetails || {};
    const bud = cmd.budgets || {};
    const goal = Number(state.budgetGoal) || 0;
    const spent = cmd.category_totals
        ? Object.values(cmd.category_totals).reduce((a, b) => a + b, 0)
        : 0;
    if (bud.is_current && goal > 0 && spent > goal) {
        const over = Math.round(spent - goal);
        chip.innerHTML = `<i class="fas fa-circle-info"></i> €${over.toLocaleString()} over this month's goal`;
        chip.style.display = '';
    } else {
        chip.innerHTML = '';
        chip.style.display = 'none';
    }
}

// A small, dense stat block for the overview header.
function miniStat(label, value, accent, subValue, trend) {
    const trendHtml = (trend !== undefined && trend !== null)
        ? `<span class="mini-stat-trend ${trend >= 0 ? 'trend-up' : 'trend-down'}">
               ${trend >= 0 ? '&#9650;' : '&#9660;'} ${Math.abs(trend).toFixed(1)}%
           </span>`
        : '';
    return `
        <div class="mini-stat" style="--stat-accent:${accent}">
            <div class="mini-stat-label">${label}</div>
            <div class="mini-stat-value">${value}${subValue ? `<span class="mini-stat-sub">${subValue}</span>` : ''}</div>
            ${trendHtml}
        </div>
    `;
}

export function createStatCard(label, value, colorIdx, subValue, trend) {
    return `
        <div class="stat-card" style="--stat-accent: ${STAT_ACCENTS[colorIdx % STAT_ACCENTS.length]}">
            <div class="stat-label">${label}</div>
            <div class="stat-value">${value}</div>
            ${subValue ? `<div class="stat-sub">${subValue}</div>` : ''}
            ${trend !== undefined && trend !== null ? `
                <div class="stat-trend ${trend >= 0 ? 'trend-up' : 'trend-down'}">
                    ${trend >= 0 ? '&#9650;' : '&#9660;'} ${Math.abs(trend).toFixed(1)}%
                </div>
            ` : ''}
        </div>
    `;
}
