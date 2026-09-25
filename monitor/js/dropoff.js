/* ============================================================================
   Dropoff -- ported from the old tab. Every outcome row comes from the one
   ladder, DROPOFF_STAGE_SQL, so the rows always add up to the period total,
   and the total row is on screen so anyone can check that without a query.

   The presets are the old tab's dpPresetRange, unchanged in behaviour: they
   work from TODAY IN ET at noon UTC, so neither the viewer's laptop zone nor a
   DST change can move a day, and a week preset opens on a Monday (PR 121).
   ============================================================================ */
GW.TABS.dropoff = (function (G) {
  var U = G.ui, esc = G.esc, fmt = G.fmt;
  var st = { preset: '12w', grain: 'week', mode: 'leads', source: '__all', unit: 'n', from: '', to: '' };
  var data = null, err = null, root = null, seq = 0, scrollAt = null, periodsSig = '';

  function monday(d) { d.setUTCDate(d.getUTCDate() - (d.getUTCDay() + 6) % 7); return d; }
  /* The window a preset stands for, from TODAY IN ET as YYYY-MM-DD. Pure. */
  function presetRange(p, today) {
    var a = String(today).split('-'), t = new Date(Date.UTC(+a[0], +a[1] - 1, +a[2], 12)), from = new Date(t), g = 'week';
    if (p === '12w') { from.setUTCDate(from.getUTCDate() - 7 * 11); monday(from); }
    else if (p === '26w') { from.setUTCDate(from.getUTCDate() - 7 * 25); monday(from); }
    /* DAY FIRST, THEN MONTH: the other order overflows on the 29th-31st. */
    else if (p === '12m') { from.setUTCDate(1); from.setUTCMonth(from.getUTCMonth() - 11); g = 'month'; }
    else if (p === 'ytd') { from = new Date(Date.UTC(t.getUTCFullYear(), 0, 1, 12)); g = 'month'; }
    else return null;
    return { from: from.toISOString().slice(0, 10), to: t.toISOString().slice(0, 10), grain: g };
  }
  function applyPreset() {
    var r = presetRange(st.preset, G.etDay(new Date())); if (!r) return;
    st.from = r.from; st.to = r.to; st.grain = r.grain;
  }
  /* THE LATEST REQUEST WINS, not the last to arrive. Two filter changes in a
     row sent two requests; when the first answered last, the table showed
     the old window under the new controls. */
  function load() {
    var my = ++seq;
    return G.api('/monitor/dropoff', { grain: st.grain, mode: st.mode, source: st.source, from: st.from, to: st.to })
      .then(function (d) { if (my === seq) { data = d; err = null; } }).catch(function (e) { if (my === seq) err = e.message; });
  }
  function activate(el) { root = el; if (!st.from) applyPreset(); render(); G.every('dropoff', 300000, function () { load().then(render); }); return load().then(render); }
  function deactivate() { G.stop('dropoff'); root = null; }
  function tone(t) { return t === 'good' ? 'b-good' : t === 'bad' ? 'b-bad' : t === 'warn' ? 'b-warn' : 'b-neu'; }
  function pctOr(p) { return p === null || p === undefined ? '—' : p + '%'; }

  function controls() {
    var opt = function (v, l, cur) { return '<option value="' + esc(v) + '"' + (v === cur ? ' selected' : '') + '>' + esc(l) + '</option>'; };
    var srcs = (data && data.sources) || [], total = srcs.reduce(function (a, b) { return a + b.n; }, 0);
    /* The picker always shows the filter that is APPLIED. A source with no
       leads in a newly chosen window drops out of the list, the browser then
       showed "All sources" -- while the data stayed filtered to it. */
    if (st.source !== '__all' && !srcs.some(function (x) { return x.name === st.source; })) srcs = srcs.concat([{ name: st.source, n: 0 }]);
    return '<div class="fieldset" role="group" aria-label="Dropoff filters">' +
      '<select class="field" data-dp="preset" aria-label="Period">' + opt('12w', 'Last 12 weeks', st.preset) + opt('26w', 'Last 26 weeks', st.preset) + opt('12m', 'Last 12 months', st.preset) + opt('ytd', 'This year', st.preset) + opt('custom', 'Custom', st.preset) + '</select>' +
      '<select class="field" data-dp="grain" aria-label="Grouping">' + opt('week', 'By week', st.grain) + opt('month', 'By month', st.grain) + '</select>' +
      '<select class="field" data-dp="mode" aria-label="Count" title="Leads counts form sessions, so one person trying twice counts twice. People counts each address once, placed in the period they first arrived and carrying the best outcome any attempt reached.">' + opt('leads', 'Count leads', st.mode) + opt('people', 'Count people', st.mode) + '</select>' +
      '<select class="field" data-dp="source" aria-label="Source" title="Read from the ad click, then from the referrer where the click lost its tags.">' + opt('__all', 'All sources — ' + fmt(total), st.source) +
        srcs.map(function (s) { return opt(s.name, s.name + ' — ' + fmt(s.n), st.source); }).join('') + '</select>' +
      '<select class="field" data-dp="unit" aria-label="Show">' + opt('n', 'Show numbers', st.unit) + opt('pct', 'Show % of period', st.unit) + '</select>' +
      /* the range moves as ONE unit: "to [date]" was wrapping onto a line of its own */
      '<span class="dates"><label>From <input class="field" type="date" data-dp="from" value="' + esc(st.from) + '"></label>' +
      '<label>to <input class="field" type="date" data-dp="to" value="' + esc(st.to) + '"></label></span></div>';
  }
  function summary(d) {
    var U2 = d.unit, rates = d.periods.map(function (p) { return d.booked_rate[p.key]; }).filter(function (x) { return x !== null && x !== undefined; });
    var lo = rates.length ? Math.min.apply(null, rates) : null, hi = rates.length ? Math.max.apply(null, rates) : null, leak = null;
    /* The BIGGEST leak, found, never assumed: it named "Left on step 2"
       whatever the numbers said, and with a source filter or in People mode
       that is often not the biggest -- and can be zero. */
    d.rows.forEach(function (r) { if (r.key !== '1_booked' && (!leak || r.total > leak.total)) leak = r; });
    if (leak && !leak.total) leak = null;
    return '<div class="sumgrid">' +
      U.metricCard({ id: 'dp-grand', label: 'In this window', value: d.grand, sub: U2 + ' who got through step 1' }) +
      U.metricCard({ id: 'dp-booked', label: 'Booked', value: d.booked, sub: pctOr(d.grand_booked_rate) + ' of ' + U2 }) +
      U.metricCard({ id: 'dp-notbooked', label: 'Did not book', value: d.not_booked, sub: 'everything below the top row' }) +
      '<div class="card mc" data-card="dp-range"><div class="klabel">' + (st.grain === 'month' ? 'Monthly' : 'Weekly') + ' range</div><div class="row"><span class="num">' + (lo === null ? '—' : Math.round(lo) + '–' + Math.round(hi) + '%') + '</span></div><div class="ksub">booked, best to worst period</div></div>' +
      U.metricCard({ id: 'dp-leak', label: 'Biggest leak', value: leak ? leak.total : null, sub: leak ? leak.label.toLowerCase() : 'nobody lost in this window' }) + '</div>';
  }
  function tableHtml(d) {
    var pct = st.unit === 'pct';
    var hh = '<tr><th scope="col">Outcome</th>' + d.periods.map(function (p) { return '<th scope="col"' + (p.partial ? ' title="Not fully covered by the window, so lower than a full period"' : '') + '>' + esc(p.label) + (p.partial ? '<span class="part">part</span>' : '') + '</th>'; }).join('') + '<th scope="col">Total</th></tr>';
    var b = '';
    d.rows.forEach(function (row) {
      if (row.total === 0 && row.key === '4_dq_other') return;
      /* a ROW HEADER, so moving across a row reads its outcome, not "Sep 14, 48" */
      b += '<tr><th scope="row" class="rl"><span class="badge ' + tone(row.tone) + '">' + esc(row.label) + '</span><div class="desc">' + esc(row.desc) + '</div></th>';
      d.periods.forEach(function (p) { var v = row.counts[p.key], t = d.totals[p.key];
        b += '<td' + (v === 0 ? ' class="zero"' : '') + '>' + (pct ? (t ? (100 * v / t).toFixed(1) + '%' : '—') : fmt(v)) + '</td>'; });
      b += '<td><b>' + (pct ? (d.grand ? (100 * row.total / d.grand).toFixed(1) + '%' : '—') : fmt(row.total)) + '</b></td></tr>';
    });
    b += '<tr class="total"><th scope="row" class="rl">All ' + esc(d.unit) + '</th>' + d.periods.map(function (p) { return '<td>' + (pct ? '100%' : fmt(d.totals[p.key])) + '</td>'; }).join('') + '<td>' + (pct ? '100%' : fmt(d.grand)) + '</td></tr>';
    b += '<tr class="rate"><th scope="row" class="rl">Booked</th>' + d.periods.map(function (p) { return '<td>' + pctOr(d.booked_rate[p.key]) + '</td>'; }).join('') + '<td>' + pctOr(d.grand_booked_rate) + '</td></tr>';
    return '<div class="xscroll" tabindex="0" role="region" aria-label="Dropoff by period"><table class="dp-table">' + hh + b + '</table></div>';
  }
  function range(d) { var y = d.from.slice(0, 4) !== d.to.slice(0, 4); return G.dayD(d.from, { noWeekday: true, year: y }) + ' – ' + G.dayD(d.to, { noWeekday: true, year: true }); }
  function render() {
    if (!root || (G.current && G.current() !== 'dropoff')) return;
    var d = data;
    var head = '<section class="ph"><div class="ph-top"><h1 class="title" tabindex="-1">Dropoff</h1><span class="readat">' + (d ? 'Read ' + esc(G.etTime(d.generated_at || Date.now())) + ' ET · ' + esc(range(d)) : '') + '</span></div>' +
      '<p class="lede">What happened to everyone who got through step 1 of the demo form, period by period. Rows are mutually exclusive, so they always add up to the period total.</p>' +
      '<div class="controls">' + controls() + '</div></section>';
    var body;
    if (!d && err) body = '<section class="card panel">' + U.unavailable('The dropoff report', err) + '</section>';
    else if (!d) body = U.loading(4);
    else body = summary(d) + U.panel({ title: 'By outcome', qual: 'counted as ' + d.unit, body: tableHtml(d),
      foot: '<span>A period marked <b>part</b> is not fully covered by the window — usually the current one, still filling. Source is read from the ad click first, then from the referrer where the click lost its tags: that recovers <b>' + fmt(d.recovered) + '</b> ' + esc(d.unit) + ' that would otherwise read as direct. Our own test submissions are included (the Lead magnet tab is the one place they are left out) — <b>' + fmt(d.internal) + '</b> of these.' +
        /* ON SCREEN, not only in the select's hover title: in People mode a
           week here can read lower than the Overview's, and both are right. */
        (d.unit === 'people' ? ' Counting people: each person sits in the period they <b>first</b> arrived within this window, carrying the best outcome any attempt reached — so a week here can read lower than on the Overview, which counts everyone who came that week.' : '') + '</span>' });
    if (d && err) body = '<div class="readat"><span class="badge b-warn">Last refresh failed (' + esc(String(err).slice(0, 40)) + ') — showing the previous read</span></div>' + body;
    /* THE NEWEST PERIODS ARE WHAT PEOPLE CHECK, and below ~1300px they were
       off the right edge with the table opened at July. It opens scrolled to
       the right end; a repaint keeps the reader's own position; a new set of
       periods (preset, grain, dates) snaps right again. */
    var x0 = root.querySelector ? root.querySelector('.xscroll') : null;
    var sig = d ? d.periods.map(function (p) { return p.key; }).join(',') : '';
    if (x0 && sig === periodsSig) scrollAt = x0.scrollLeft; else scrollAt = null;
    periodsSig = sig;
    G.paint(root, head + body);
    var x1 = root.querySelector ? root.querySelector('.xscroll') : null;
    if (x1) x1.scrollLeft = scrollAt === null ? x1.scrollWidth : scrollAt;
  }
  if (typeof document !== 'undefined' && document.addEventListener) document.addEventListener('change', function (e) {
    var el = e.target; if (!el || !el.getAttribute || !el.getAttribute('data-dp')) return;
    var k = el.getAttribute('data-dp'), v = el.value;
    if (k === 'preset') { st.preset = v; if (v !== 'custom') applyPreset(); }
    else if (k === 'from' || k === 'to') { st[k] = v; st.preset = 'custom'; }
    else st[k] = v;
    if (k === 'unit') { render(); return; }
    load().then(render);
  });
  return { title: 'Dropoff', activate: activate, deactivate: deactivate, render: render, presetRange: presetRange, _st: st, _set: function (d) { data = d; } };
})(GW);
