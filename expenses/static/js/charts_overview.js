import { state } from './state.js';
import { getCategoryColor, plotlyFont, plotlyGrid, chartBg, hoverLabel, isDark, PLOTLY_CONFIG } from './constants.js';
import { showCategoryDetails } from './panel.js';

// ── Overview Charts ────────────────────────────
export function renderOverviewCharts() {
    renderMonthlyChart();
    renderCategoryChart();
}

export function renderMonthlyChart() {
    const traces = [];
    const months = state.monthlyData.months;
    const goal = state.budgetGoal;
    const chartType = state.currentChartType;   // 'line' | 'bar' | 'area'
    const isLine = chartType === 'line';
    const isArea = chartType === 'area';
    const isBar = chartType === 'bar';

    // Hue-neutral ink for the two hero series so the colorful category context reads
    // as a soft band behind them. In dark mode the "ink" flips to near-white.
    const GOAL_COLOR = '#16a34a';   // confident green (reads on both themes)
    const dark = isDark();
    const TOTAL_COLOR = dark ? '#f5f1ea' : '#1c1917';
    const MARKER_RING = dark ? '#211e18' : '#ffffff';

    // ── Trace 0: budget goal ────────────────────────────────────────────────
    // A flat reference line across all months. Trace index 0 is contractual —
    // the drag/edit machinery below relies on it. No hover (it's a reference).
    traces.push({
        x: months,
        y: months.map(() => goal),
        type: 'scatter',
        mode: 'lines',
        name: `Goal · €${goal.toLocaleString()}/mo`,
        line: { color: GOAL_COLOR, width: 2, dash: 'dot', shape: 'spline' },
        opacity: 0.9,
        hoverinfo: 'skip',
    });

    // ── Category context lines (drawn first so hero lines sit on top) ────────
    // De-emphasized on purpose: thin, translucent, smoothed splines, no markers.
    // They form a soft "context band" — present, but never competing for attention.
    // A subtle per-series tooltip stays available on hover so the data is still
    // interrogable without cluttering the resting state.
    const allCategories = Object.entries(state.monthlyData.categories)
        .sort((a, b) => b[1].reduce((s, v) => s + v, 0) - a[1].reduce((s, v) => s + v, 0));

    allCategories.forEach(([category, values]) => {
        const color = getCategoryColor(category);
        const label = category.replace('_', ' ').replace('-', ' ');
        const t = {
            x: months,
            y: values,
            type: isBar ? 'bar' : 'scatter',
            name: label,
            // Category lines are a "context band" — keep them off the legend so it
            // stays a single compact row (Goal + Total). Their colors are shown in
            // the Categories donut / Spending-by-category cards instead.
            showlegend: false,
            opacity: isBar ? 0.65 : 0.75,
            hovertemplate: `<b>${label}</b>  €%{y:,.0f}<extra></extra>`,
        };
        if (isBar) {
            t.marker = { color: color };
        } else {
            t.mode = 'lines';
            t.line = { color: color, width: 2, shape: 'spline', smoothing: 0.6 };
            if (isArea) {
                t.fill = 'tozeroy';
                t.fillcolor = hexToRgba(color, 0.06);
            }
        }
        traces.push(t);
    });

    // ── Total — the hero series & primary hover target ──────────────────────
    // Thick ink line, clear markers that grow on hover, a clean single dark card.
    const total = {
        x: months,
        y: state.monthlyData.totals,
        type: isBar ? 'bar' : 'scatter',
        name: 'Total',
        hovertemplate: '<b>%{x}</b><br>Total spent   <b>€%{y:,.0f}</b><extra></extra>',
    };
    if (isBar) {
        total.marker = { color: TOTAL_COLOR };
    } else {
        total.mode = isLine ? 'lines+markers' : 'lines';
        total.line = { color: TOTAL_COLOR, width: 3, shape: 'spline', smoothing: 0.4 };
        // Per-point size array so hover can enlarge ONLY the hovered marker.
        total.marker = { size: months.map(() => 6), color: TOTAL_COLOR, line: { color: MARKER_RING, width: 1.5 } };
        if (isArea) {
            total.fill = 'tozeroy';
            total.fillcolor = dark ? 'rgba(245,241,234,0.06)' : 'rgba(28,25,23,0.05)';
        }
    }
    traces.push(total);

    // Plotly.react (not newPlot) so switching line/bar/area morphs smoothly.
    Plotly.react('monthlyChart', traces, {
        xaxis: {
            showgrid: false,
            tickfont: { size: 11 },
            showspikes: isLine || isArea,
            spikethickness: 1,
            spikecolor: 'rgba(28,25,23,0.18)',
            spikemode: 'across',
            spikedash: 'dot',
            spikesnap: 'cursor',
        },
        yaxis: {
            // € now lives on the tick values, not on an axis title. Thousands grouping.
            tickprefix: '€',
            tickformat: ',.0f',
            separatethousands: true,
            showgrid: true,
            gridcolor: plotlyGrid(),
            gridwidth: 1,
            tickfont: { size: 11 },
            zeroline: false,
            rangemode: 'tozero',
        },
        plot_bgcolor: chartBg(),
        paper_bgcolor: chartBg(),
        font: plotlyFont(),
        margin: { t: 16, r: 20, b: 40, l: 64 },
        hovermode: 'closest',
        hoverlabel: hoverLabel(),
        legend: {
            orientation: 'h',
            y: -0.14,
            x: 0.5,
            xanchor: 'center',
            font: { size: 11, color: '#78716c' },
            itemsizing: 'constant',
            bgcolor: 'rgba(0,0,0,0)',
        },
        barmode: 'group',
        bargap: 0.28,
        bargroupgap: 0.08,
        transition: { duration: 450, easing: 'cubic-in-out' },
        annotations: [{
            x: 1, xref: 'paper', y: goal, yref: 'y',
            xanchor: 'right', yanchor: 'bottom',
            text: `Monthly goal · €${goal.toLocaleString()}`,
            showarrow: false,
            font: { size: 10, color: GOAL_COLOR, family: '-apple-system, BlinkMacSystemFont, "SF Pro Text", system-ui, sans-serif' },
            bgcolor: 'rgba(240,253,244,0.92)',
            bordercolor: 'rgba(22,163,74,0.25)',
            borderwidth: 1,
            borderpad: 3,
            yshift: 3,
        }],
    }, PLOTLY_CONFIG);

    // Re-arm the drag interaction if we're in edit mode (chart was re-plotted).
    if (goalEditMode) attachGoalDrag();

    // Lively hover: grow the Total marker under the cursor, then settle it back.
    // Attached once per element; the closure re-reads the current trace count so it
    // always targets the Total (last) trace regardless of category count.
    attachHoverGrow();
}

let hoverGrowAttached = false;
function attachHoverGrow() {
    const chart = document.getElementById('monthlyChart');
    if (!chart || hoverGrowAttached) return;
    hoverGrowAttached = true;
    const totalIdx = () => (chart.data ? chart.data.length - 1 : -1);
    // Base size array for the Total markers; only the hovered index is enlarged.
    const baseSizes = () => (chart.data[totalIdx()].x || []).map(() => 6);
    chart.on('plotly_hover', (ev) => {
        if (goalEditMode) return;
        const i = totalIdx();
        const pt = ev.points && ev.points[0];
        if (!pt || pt.curveNumber !== i) return;
        if (chart.data[i].type !== 'scatter') return;   // markers only in line/area
        const sizes = baseSizes();
        sizes[pt.pointNumber] = 12;                      // grow ONLY the hovered dot
        Plotly.restyle(chart, { 'marker.size': [sizes] }, [i]);
    });
    chart.on('plotly_unhover', () => {
        const i = totalIdx();
        if (i < 0 || !chart.data[i] || chart.data[i].type !== 'scatter') return;
        Plotly.restyle(chart, { 'marker.size': [baseSizes()] }, [i]);
    });
}

// hex → rgba string with a given alpha (for soft area fills).
function hexToRgba(hex, alpha) {
    const h = hex.replace('#', '');
    const r = parseInt(h.slice(0, 2), 16), g = parseInt(h.slice(2, 4), 16), b = parseInt(h.slice(4, 6), 16);
    return `rgba(${r},${g},${b},${alpha})`;
}

// ── Editable budget goal (hover → Edit → drag) ──
let goalEditMode = false;

export function toggleGoalEdit() {
    goalEditMode = !goalEditMode;
    const btn = document.getElementById('goalEditBtn');
    const chart = document.getElementById('monthlyChart');
    if (btn) {
        btn.classList.toggle('active', goalEditMode);
        // The button doubles as the exit affordance: "Edit goal" ⇄ "Done".
        btn.innerHTML = goalEditMode
            ? '<i class="fas fa-check" style="margin-right:5px;"></i>Done'
            : '<i class="fas fa-pen" style="margin-right:5px;"></i>Edit goal';
    }
    chart.classList.toggle('goal-editing', goalEditMode);
    if (goalEditMode) {
        // Kill ALL Plotly interactivity: no zoom/pan/select, no hover tooltips.
        Plotly.relayout(chart, { dragmode: false, hovermode: false });
        attachGoalDrag();
    } else {
        detachGoalDrag();
        Plotly.relayout(chart, { dragmode: 'zoom', hovermode: 'x unified' });
        // Persist the final value.
        fetch('/api/settings/budget-goal', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ budget_goal: state.budgetGoal }),
        }).catch(() => {});
    }
}

let goalDragHandlers = null;
function attachGoalDrag() {
    detachGoalDrag();
    const chart = document.getElementById('monthlyChart');

    // Light-touch overlay: a nearly-transparent surface that captures drags and
    // blocks Plotly's own interactions, but does NOT dim the chart. The goal line is
    // a clean solid rule with a single centered pill you grab and slide up/down.
    const overlay = document.createElement('div');
    overlay.className = 'goal-edit-overlay';
    overlay.innerHTML = `
        <div class="goal-hint"><i class="fas fa-arrows-up-down"></i> Drag the line to set your monthly goal</div>
        <div class="goal-drag-line" id="goalDragLine">
            <span class="goal-drag-pill" id="goalDragLabel"></span>
        </div>`;
    chart.appendChild(overlay);

    // Hide the underlying Plotly goal line + its annotation while editing, so only
    // the bright overlay line is visible (no confusing double line).
    Plotly.restyle(chart, { visible: [false] }, [0]);
    Plotly.relayout(chart, { 'annotations[0].visible': false });

    const yAxis = () => chart._fullLayout && chart._fullLayout.yaxis;

    // clientY → snapped € value.
    const valueAt = (clientY) => {
        const ya = yAxis();
        if (!ya) return state.budgetGoal;
        const rect = chart.getBoundingClientRect();
        const offset = (ya._offset != null) ? ya._offset : chart._fullLayout.margin.t;
        const val = ya.p2l((clientY - rect.top) - offset);
        return Math.max(0, Math.round(val / 50) * 50);   // snap to €50
    };

    // Position the overlay goal line at the current value's pixel Y.
    const positionLine = () => {
        const ya = yAxis();
        if (!ya) return;
        const offset = (ya._offset != null) ? ya._offset : chart._fullLayout.margin.t;
        const y = offset + ya.l2p(state.budgetGoal);
        const line = document.getElementById('goalDragLine');
        const label = document.getElementById('goalDragLabel');
        if (line) line.style.top = `${y}px`;
        if (label) label.innerHTML = `<i class="fas fa-grip-lines"></i> €${state.budgetGoal.toLocaleString()}<span class="goal-pill-unit">/mo</span>`;
    };

    const setGoal = (clientY) => {
        const val = valueAt(clientY);
        if (!isFinite(val)) return;
        state.budgetGoal = val;
        positionLine();
        // Keep the (hidden) underlying goal trace + annotation in sync so they're
        // correct the moment we restore them on exit. Keep visible:false here.
        const months = state.monthlyData.months;
        Plotly.restyle(chart, { y: [months.map(() => val)], name: [`Goal · €${val}/mo`] }, [0]);
        Plotly.relayout(chart, { 'annotations[0].y': val, 'annotations[0].text': `Monthly goal €${val}` });
    };

    let dragging = false;
    const onDown = (e) => { dragging = true; setGoal(e.clientY); e.preventDefault(); };
    const onMove = (e) => { if (dragging) { setGoal(e.clientY); e.preventDefault(); } };
    const onUp = () => { dragging = false; };
    // Clicking anywhere on the dimmed chart jumps the line there, then you can drag.
    overlay.addEventListener('mousedown', onDown);
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);

    positionLine();
    goalDragHandlers = { overlay, onMove, onUp };
}

function detachGoalDrag() {
    if (!goalDragHandlers) return;
    const { overlay, onMove, onUp } = goalDragHandlers;
    window.removeEventListener('mousemove', onMove);
    window.removeEventListener('mouseup', onUp);
    if (overlay && overlay.parentNode) overlay.parentNode.removeChild(overlay);
    // Restore the underlying Plotly goal line + annotation.
    const chart = document.getElementById('monthlyChart');
    if (chart && chart.data) {
        Plotly.restyle(chart, { visible: [true] }, [0]);
        Plotly.relayout(chart, { 'annotations[0].visible': true });
    }
    goalDragHandlers = null;
}

export function renderCategoryChart() {
    let dataToUse, subtitleText, centerText;

    if (state.currentCategoryView === 'total') {
        dataToUse = state.categoryTotals;
        subtitleText = 'Total across all time';
        centerText = `€${Object.values(state.categoryTotals).reduce((a, b) => a + b, 0).toFixed(0)}<br><span style="font-size:12px;color:#a8a29e">Total</span>`;
    } else if (state.currentCategoryView === 'average') {
        dataToUse = state.categoryAverages.overall;
        subtitleText = `Average per month (${state.categoryAverages.num_months} months)`;
        centerText = `€${Object.values(state.categoryAverages.overall).reduce((a, b) => a + b, 0).toFixed(0)}<br><span style="font-size:12px;color:#a8a29e">Avg/mo</span>`;
    } else {
        dataToUse = state.categoryAverages.last_3_months;
        subtitleText = `Avg of last ${state.categoryAverages.last_3_count} months`;
        centerText = `€${Object.values(state.categoryAverages.last_3_months).reduce((a, b) => a + b, 0).toFixed(0)}<br><span style="font-size:12px;color:#a8a29e">Avg/mo</span>`;
    }

    document.getElementById('categorySubtitle').textContent = subtitleText;

    const sorted = Object.entries(dataToUse).filter(([_, v]) => v > 0).sort((a, b) => b[1] - a[1]);
    const inkColor = isDark() ? '#f5f1ea' : '#1c1917';

    Plotly.react('categoryChart', [{
        labels: sorted.map(([c]) => c.replace('_', ' ').replace('-', ' ')),
        values: sorted.map(([_, v]) => v),
        type: 'pie',
        hole: 0.55,
        marker: {
            colors: sorted.map(([c]) => getCategoryColor(c)),
            line: { color: chartBg(), width: 2 }
        },
        // Percentages INSIDE the ring (cleaner than outside leader-lines); Plotly hides
        // labels that don't fit, so tiny slices stay uncluttered. Names live in the
        // legend beside the ring, and hover shows the full figure.
        textinfo: 'percent',
        textposition: 'inside',
        insidetextorientation: 'horizontal',
        texttemplate: '%{percent:.0%}',
        textfont: { size: 11, color: '#fff' },
        automargin: true,
        hovertemplate: '<b>%{label}</b><br>€%{value:,.0f} · %{percent}<extra></extra>',
    }], {
        font: plotlyFont(),
        margin: { t: 20, r: 16, b: 20, l: 16 },
        showlegend: true,
        legend: {
            orientation: 'v', x: 1.02, y: 0.5, xanchor: 'left', yanchor: 'middle',
            font: { size: 11, color: plotlyFont().color }, bgcolor: 'rgba(0,0,0,0)',
            itemsizing: 'constant',
        },
        plot_bgcolor: chartBg(),
        paper_bgcolor: chartBg(),
        hoverlabel: hoverLabel(),
        annotations: [{ text: centerText, x: 0.5, y: 0.5, font: { size: 18, family: '-apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", system-ui, sans-serif', color: inkColor }, showarrow: false }]
    }, PLOTLY_CONFIG);

    document.getElementById('categoryChart').on('plotly_click', function(data) {
        const category = sorted[data.points[0].pointNumber][0];
        showCategoryDetails(category);
    });
}

export function toggleCategoryView(view, evt) {
    state.currentCategoryView = view;
    const buttons = document.querySelectorAll('.chart-container .chart-filters .filter-btn');
    buttons.forEach(btn => btn.classList.remove('active'));
    if (evt && evt.target) evt.target.classList.add('active');
    renderCategoryChart();
}
