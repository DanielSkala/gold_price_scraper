import { state } from './state.js';
import { plotlyFont, plotlyGrid, chartBg, hoverLabel, PLOTLY_CONFIG } from './constants.js';

const GOLD = '#D4AF37';
const GOLD_SOFT = 'rgba(212, 175, 55, 0.12)';

// ── Gold ───────────────────────────────────────
export function loadGoldData() {
    const fetches = [
        fetch(`/api/gold/spot-history?period=${state.goldTimeframe}`).then(r => r.json()),
    ];
    // Only fetch premiums and portfolio on first load (they don't depend on timeframe)
    if (!state.goldLoaded) {
        fetches.push(fetch('/api/gold/premiums').then(r => r.json()));
        fetches.push(fetch('/api/gold/portfolio').then(r => r.json()));
    }

    Promise.all(fetches).then(results => {
        state.goldSpotData = results[0];
        if (results.length > 1) {
            state.goldPremiumData = results[1];
            state.goldPortfolioData = results[2];
        }
        state.goldLoaded = true;
        renderGoldCharts();
    }).catch(err => {
        console.error('Error loading gold data:', err);
        document.getElementById('goldHeroStats').innerHTML =
            '<div style="color:var(--neg);">Failed to load gold data. Is yfinance working?</div>';
    });
}

export function changeGoldTimeframe(tf, btn) {
    state.goldTimeframe = tf;
    document.querySelectorAll('#goldTimeframeFilters .filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    loadGoldData();
}

export function renderGoldCharts() {
    renderGoldHero();
    renderGoldSpotChart();
    renderGoldPremiumChart();
    renderGoldPortfolioTable();
}

// Prominent hero: portfolio value as the headline, key numbers alongside.
export function renderGoldHero() {
    if (!state.goldPortfolioData) return;
    const p = state.goldPortfolioData;
    const up = p.gain_loss >= 0;
    const glColor = up ? 'var(--pos)' : 'var(--neg)';
    const sign = up ? '+' : '';
    const fmt0 = (n) => '€' + Number(n).toLocaleString('en', { maximumFractionDigits: 0 });

    document.getElementById('goldHeroTitle').textContent =
        fmt0(p.current_value);
    document.getElementById('goldHeroSub').innerHTML =
        `<span style="color:${glColor};font-weight:600;">${sign}${fmt0(Math.abs(p.gain_loss))} (${sign}${p.gain_loss_pct}%)</span>
         <span style="color:var(--text-3);"> · ${p.total_weight_g}g across ${p.items.length} purchases</span>`;

    document.getElementById('goldHeroStats').innerHTML = `
        <div class="month-hero-stat">
            <span class="mh-label">Total cost</span>
            <span class="mh-value">${fmt0(p.total_cost)}</span>
        </div>
        <div class="month-hero-stat">
            <span class="mh-label">Spot / oz</span>
            <span class="mh-value">${fmt0(p.current_spot_per_oz)}</span>
        </div>
        <div class="month-hero-stat">
            <span class="mh-label">Spot / g</span>
            <span class="mh-value">€${p.current_spot_per_g.toFixed(2)}</span>
        </div>
    `;
}

export function renderGoldSpotChart() {
    if (!state.goldSpotData || !state.goldSpotData.dates) return;

    const traces = [];

    // Spot price line
    traces.push({
        x: state.goldSpotData.dates,
        y: state.goldSpotData.prices_eur_oz,
        type: 'scatter',
        mode: 'lines',
        name: 'Gold spot',
        line: { color: GOLD, width: 2.5, shape: 'spline' },
        hovertemplate: '<b>%{x}</b><br>€%{y:,.2f}/oz<extra></extra>',
        fill: 'tozeroy',
        fillcolor: GOLD_SOFT,
    });

    // Purchase markers (price paid) + spot-at-purchase
    if (state.goldPortfolioData && state.goldPortfolioData.items) {
        const buyDates = [], buyPrices = [], buyTexts = [];
        const spotDates = [], spotPrices = [];

        state.goldPortfolioData.items.forEach(item => {
            buyDates.push(item.date);
            buyPrices.push(item.cost_per_oz);
            const prem = item.premium_pct !== null ? `Premium: ${item.premium_pct}%` : '';
            buyTexts.push(`<b>${item.label}</b><br>${item.date}<br>Paid: €${item.cost.toLocaleString()}<br>€${item.cost_per_oz.toLocaleString()}/oz<br>${prem}<extra></extra>`);
            if (item.spot_at_purchase_oz) {
                spotDates.push(item.date);
                spotPrices.push(item.spot_at_purchase_oz);
            }
        });

        traces.push({
            x: buyDates, y: buyPrices,
            type: 'scatter', mode: 'markers',
            name: 'You bought',
            marker: { symbol: 'triangle-up', size: 13, color: '#b91c1c', line: { width: 1.5, color: '#fff' } },
            hovertemplate: buyTexts,
        });

        if (spotDates.length) {
            traces.push({
                x: spotDates, y: spotPrices,
                type: 'scatter', mode: 'markers',
                name: 'Spot then',
                marker: { symbol: 'circle', size: 8, color: GOLD, line: { width: 1.5, color: '#fff' } },
                hovertemplate: '<b>Spot at purchase</b><br>%{x}<br>€%{y:,.2f}/oz<extra></extra>',
            });
        }
    }

    const shapes = (state.goldPortfolioData?.items || []).map(item => ({
        type: 'line',
        x0: item.date, x1: item.date,
        y0: 0, y1: 1, yref: 'paper',
        line: { color: 'rgba(185, 28, 28, 0.25)', width: 1, dash: 'dot' },
    }));

    Plotly.newPlot('goldSpotChart', traces, {
        xaxis: { showgrid: false, tickfont: { size: 11 } },
        yaxis: { gridcolor: plotlyGrid(), tickfont: { size: 11 }, tickprefix: '€', tickformat: ',.0f' },
        plot_bgcolor: chartBg(), paper_bgcolor: chartBg(),
        font: plotlyFont(),
        margin: { t: 10, r: 16, b: 44, l: 60 },
        hovermode: 'x unified',
        hoverlabel: hoverLabel(),
        legend: { orientation: 'h', y: -0.16, x: 0.5, xanchor: 'center', font: { size: 11 } },
        shapes: shapes,
    }, PLOTLY_CONFIG);
}

export function renderGoldPremiumChart() {
    if (!state.goldPremiumData || !state.goldPremiumData.rows) return;

    const weights = state.goldPremiumData.weights;
    const rows = state.goldPremiumData.rows;
    const n = weights.length;

    // Instead of drawing ~290 faded traces (slow!), collapse history into a min–max band
    // per weight: two lines + fill. This is the same "historical range" story, far faster.
    const mins = [], maxs = [];
    for (let i = 0; i < n; i++) {
        let lo = Infinity, hi = -Infinity;
        for (const row of rows) {
            const v = row[i];
            if (v !== null && v !== undefined) { if (v < lo) lo = v; if (v > hi) hi = v; }
        }
        mins.push(lo === Infinity ? null : lo);
        maxs.push(hi === -Infinity ? null : hi);
    }

    const traces = [
        // Upper bound (invisible line, anchors the fill)
        {
            x: weights, y: maxs, type: 'scatter', mode: 'lines',
            line: { width: 0 }, connectgaps: true, showlegend: false, hoverinfo: 'skip',
        },
        // Lower bound + fill up to the upper bound = shaded historical range
        {
            x: weights, y: mins, type: 'scatter', mode: 'lines',
            line: { width: 0 }, fill: 'tonexty', fillcolor: 'rgba(120,113,108,0.12)',
            connectgaps: true, name: 'Historical range', hoverinfo: 'skip',
        },
        // Average
        {
            x: weights, y: state.goldPremiumData.averages,
            type: 'scatter', mode: 'lines+markers', name: 'Average',
            line: { color: '#57534e', width: 2, dash: 'dot' }, marker: { size: 5 },
            connectgaps: true,
            hovertemplate: '<b>%{x}g</b><br>Avg premium: %{y:.1f}%<extra></extra>',
        },
        // Latest
        {
            x: weights, y: state.goldPremiumData.latest,
            type: 'scatter', mode: 'lines+markers',
            name: `Latest (${state.goldPremiumData.latest_date})`,
            line: { color: '#2563eb', width: 2.5 }, marker: { size: 7 },
            connectgaps: true,
            hovertemplate: '<b>%{x}g</b><br>Current premium: %{y:.1f}%<extra></extra>',
        },
    ];

    // Best 3 current deals (lowest premium) highlighted in gold
    const validLatest = state.goldPremiumData.latest
        .map((v, i) => v !== null ? { v, w: weights[i] } : null)
        .filter(Boolean)
        .sort((a, b) => a.v - b.v)
        .slice(0, 3);

    traces.push({
        x: validLatest.map(d => d.w),
        y: validLatest.map(d => d.v),
        type: 'scatter', mode: 'markers', name: 'Best value now',
        marker: { color: GOLD, size: 13, symbol: 'star', line: { width: 1, color: '#92711c' } },
        hovertemplate: '<b>%{x}g</b><br>Premium: %{y:.1f}% ★<extra></extra>',
    });

    Plotly.newPlot('goldPremiumChart', traces, {
        xaxis: {
            title: 'Bar weight',
            type: 'log', tickvals: weights, ticktext: state.goldPremiumData.weight_labels,
            tickfont: { size: 11 },
        },
        yaxis: { title: 'Premium %', gridcolor: plotlyGrid(), tickfont: { size: 11 }, ticksuffix: '%', dtick: 2.5 },
        plot_bgcolor: chartBg(), paper_bgcolor: chartBg(),
        font: plotlyFont(),
        margin: { t: 10, r: 16, b: 50, l: 52 },
        hoverlabel: hoverLabel(),
        legend: { orientation: 'h', y: -0.2, x: 0.5, xanchor: 'center', font: { size: 11 } },
    }, PLOTLY_CONFIG);
}

export function renderGoldPortfolioTable() {
    if (!state.goldPortfolioData || !state.goldPortfolioData.items) return;
    const container = document.getElementById('goldPortfolioTable');
    const p = state.goldPortfolioData;

    container.innerHTML = `
        <div class="gold-holding gold-holding-header">
            <div>Holding</div>
            <div style="text-align:right;">Cost</div>
            <div style="text-align:right;">Value now</div>
            <div style="text-align:right;">Premium</div>
            <div style="text-align:right;">P&L</div>
        </div>
        ${p.items.map(item => {
            const up = item.gain_loss >= 0;
            const glColor = up ? 'var(--pos)' : 'var(--neg)';
            const sign = up ? '+' : '';
            return `
                <div class="gold-holding">
                    <div>
                        <div style="font-weight:500;">${item.label}</div>
                        <div style="font-size:0.78rem;color:var(--text-3);">${new Date(item.date).toLocaleDateString()} &middot; ${item.weight_g}g</div>
                    </div>
                    <div style="text-align:right;font-family:var(--font-serif);">€${item.cost.toLocaleString()}</div>
                    <div style="text-align:right;font-family:var(--font-serif);">€${item.value_now.toLocaleString('en', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</div>
                    <div style="text-align:right;font-size:0.82rem;color:var(--text-2);">${item.premium_pct !== null ? item.premium_pct + '%' : '—'}</div>
                    <div style="text-align:right;font-family:var(--font-serif);color:${glColor};font-weight:500;">
                        ${sign}€${Math.abs(item.gain_loss).toLocaleString('en', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                        <div style="font-size:0.75rem;font-family:var(--font-sans);font-weight:400;">${sign}${item.gain_loss_pct}%</div>
                    </div>
                </div>
            `;
        }).join('')}
        <div class="gold-holding gold-total">
            <div>
                <div style="font-weight:600;">Total</div>
                <div style="font-size:0.78rem;color:var(--text-3);">${p.total_weight_g}g</div>
            </div>
            <div style="text-align:right;font-family:var(--font-serif);">€${p.total_cost.toLocaleString()}</div>
            <div style="text-align:right;font-family:var(--font-serif);">€${p.current_value.toLocaleString('en', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</div>
            <div></div>
            <div style="text-align:right;font-family:var(--font-serif);color:${p.gain_loss >= 0 ? 'var(--pos)' : 'var(--neg)'};font-weight:600;">
                ${p.gain_loss >= 0 ? '+' : ''}€${Math.abs(p.gain_loss).toLocaleString('en', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                <div style="font-size:0.75rem;font-family:var(--font-sans);font-weight:500;">${p.gain_loss >= 0 ? '+' : ''}${p.gain_loss_pct}%</div>
            </div>
        </div>
    `;
}
