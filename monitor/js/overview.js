/* ============================================================================
   Overview -- Today (live), This week, All time. People or Leads.

   Every number comes from /monitor/overview, which applies ONE definition set
   to every window (see overviewReport in index.js). The periods, the slots
   and the notes are all built from the server's windows, never from the
   viewer's clock. Today refreshes every minute; the others every five; all of
   it pauses while the tab is hidden.

   A FAILED REFRESH KEEPS THE LAST GOOD NUMBERS AND SAYS SO. Blanking them
   would read as "nothing happened"; zeroing them would read as a collapse.
   ============================================================================ */
GW.TABS.overview = (function (G) {
  var U = G.ui, fmt = G.fmt, esc = G.esc, pct = G.pct;
  var data = {}, err = {}, health = null, healthErr = null, root = null, attnOpen = null;
  var MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  var DOW = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
  function word() { return G.S.unit === 'people' ? 'people' : 'leads'; }
  function hourLabel(h) { return h === 0 ? '12a' : h < 12 ? h + 'a' : h === 12 ? '12p' : (h - 12) + 'p'; }
  function hourHead(h) { return h === 0 ? '12 AM' : h < 12 ? h + ' AM' : h === 12 ? '12 PM' : (h - 12) + ' PM'; }
  function dayParts(iso) { var p = iso.split('-'); return { d: +p[2], m: MON[+p[1] - 1] }; }
  /* the ET hour and ET date of an instant */
  function etHour(ts) { return +new Intl.DateTimeFormat('en-US', { timeZone: G.TZ, hour: 'numeric', hourCycle: 'h23' }).format(new Date(ts)); }

  function load(view) {
    return G.api('/monitor/overview', { view: view }).then(function (d) { data[view] = d; err[view] = null; })
      .catch(function (e) { err[view] = e.message || String(e); });
  }
  /* The strip reads EVERY check System health shows, not the nine
     /monitor/health returns: API uptime and ELV come from their own routes,
     and without them "Every check is green" could be false while System
     health showed a red row, and the nav badge undercounted. The two are
     the SAME functions the Health tab runs (GW.healthExtra), mapped from
     its badge classes onto the server's four states. */
  var CLS_STATE = { 'b-good': 'green', 'b-warn': 'amber', 'b-bad': 'red', 'b-neu': 'insufficient_data' };
  function loadHealth() {
    var extra = GW.healthExtra ? GW.healthExtra() : Promise.resolve({});
    return Promise.all([G.api('/monitor/health', { _: Date.now() }, { timeout: 25000 }), extra]).then(function (r) {
      var d = r[0] || {}, checks = Object.assign({}, d.checks || {});
      Object.keys(r[1] || {}).forEach(function (k) { var x = r[1][k]; checks[k] = { state: CLS_STATE[x.cls] || 'red', text: x.text, detail: x.detail || '' }; });
      health = Object.assign({}, d, { checks: checks, readAt: Date.now() }); healthErr = null;
    }).catch(function (e) { healthErr = e.message || String(e); });
  }
  function refresh() { return load(G.S.view).then(render); }
  function interval(v) { return v === 'today' ? 60000 : 300000; }

  function schedule() {
    G.every('overview', interval(G.S.view), refresh);
    G.every('overview-health', 300000, function () { loadHealth().then(render); });
  }
  /* COMING BACK REFRESHES WHAT HAS GONE STALE. It used to refetch only what
     had never loaded, so returning to Today showed the numbers from when you
     left under a green "Live" badge, and a Re-check that turned a row green
     on System health left this strip red for up to five minutes. */
  function stale(d, ms) { return !d || Date.now() - Date.parse(d.generated_at) >= ms; }
  function activate(el) {
    root = el; render();
    schedule();
    var v = G.S.view;
    return Promise.all([stale(data[v], interval(v)) ? load(v) : null, !health || Date.now() - health.readAt >= 60000 ? loadHealth() : null]).then(render);
  }
  /* root = null: a response that lands after the reader has moved to another
     tab must not repaint this one over it (render returns at once). */
  function deactivate() { G.stop('overview'); G.stop('overview-health'); root = null; }
  function setView(v) { G.S.view = v; G.writeHash(); render(); schedule(); if (!data[v]) load(v).then(render); else refresh(); }

  /* ── The attention strip. Fed by the same checks System health shows. Red
     is what needs someone; the rest is summarised, and every count is a
     button that names the checks behind it, so "2 not judged" is never a
     number you have to take on trust. ── */
  function attention() {
    if (healthErr && !health) return '<section class="card attn bad" aria-label="Needs attention"><div class="attn-ic">' + G.ic('warning-circle') + '</div><div class="attn-body"><div class="attn-t">System health could not be read</div><div class="attn-s">' + esc(healthErr) + '. That is not the same as healthy — open System health to re-check.</div></div><div class="attn-side"><a class="btn sm" href="#tab=health" data-tab="health">System health' + G.ic('arrow-right') + '</a></div></section>';
    if (!health) return '<section class="card attn" aria-busy="true"><div class="attn-body"><span class="skel"></span></div></section>';
    var checks = health.checks || {}, names = Object.assign({}, GW.HEALTH_EXTRA_NAMES || {}, GW.HEALTH_NAMES || {});
    var by = { red: [], amber: [], green: [], insufficient_data: [] };
    Object.keys(checks).forEach(function (k) { var c = checks[k]; if (c && by[c.state]) by[c.state].push({ k: k, c: c }); });
    var nm = function (x) { return names[x.k] || x.k; };
    if (G.navBadge) G.navBadge('health', by.red.length);
    var list = function (xs) { return xs.map(nm).join(', '); };
    /* The counts are BUTTONS that open an inline explanation: a hover title
       does not exist on a touchscreen, and "4 too quiet to judge" must never
       be a number you take on trust. */
    var EXPLAIN = {
      red: ['Red right now', by.red, 'Something is failing and needs a person.'],
      amber: ['Amber', by.amber, 'Working, but worth a look.'],
      green: ['Green', by.green, 'Checked and working just now.'],
      insufficient_data: ['Not judged', by.insufficient_data, 'We could not call these healthy or broken — usually too little traffic in the window, sometimes a layer that is switched off. The row on System health says which.'],
    };
    var chip = function (key, cls, label) {
      var n = by[key].length; if (!n && key !== 'green') return '';
      return '<button class="badge ' + cls + ' attn-chip" data-attn="' + key + '" aria-expanded="' + (attnOpen === key) + '" aria-controls="attn-explain">' + n + ' ' + label + '</button>';
    };
    var badges = chip('red', 'b-bad', G.plural(by.red.length, 'needs', 'need') + ' attention') + chip('amber', 'b-warn', 'to watch') +
      chip('green', 'b-good', 'healthy') + chip('insufficient_data', 'b-neu', 'not judged');
    var ex = attnOpen && EXPLAIN[attnOpen] ? '<div class="attn-explain" id="attn-explain" role="region"><b>' + esc(EXPLAIN[attnOpen][0]) + ':</b> ' + (EXPLAIN[attnOpen][1].length ? esc(list(EXPLAIN[attnOpen][1])) : 'none') + '. ' + esc(EXPLAIN[attnOpen][2]) + '</div>' : '<div id="attn-explain" hidden></div>';
    var side = '<div class="attn-side">' + badges + '<a class="btn sm" href="#tab=health" data-tab="health">System health' + G.ic('arrow-right') + '</a></div>' + ex;
    if (!by.red.length) {
      return '<section class="card attn ok" aria-label="Needs attention"><div class="attn-ic">' + G.ic('check-circle') + '</div><div class="attn-body"><div class="attn-t">Nothing needs attention</div><div class="attn-s">Every check System health runs is green, amber or not judged. Checked ' + esc(G.etTime(health.checkedAt || health.generatedAt || health.readAt || Date.now())) + ' ET.</div></div>' + side + '</section>';
    }
    var top = by.red[0], more = by.red.slice(1);
    return '<section class="card attn bad" aria-label="Needs attention"><div class="attn-ic">' + G.ic('warning-circle') + '</div><div class="attn-body">' +
      '<div class="attn-t">' + esc(nm(top)) + ': ' + esc(top.c.text) + '</div>' +
      /* WHAT IT COSTS FIRST, in the SDR's words, then the check's own detail
         -- which opens with the vendor's raw error ("You have insufficient
         credits! Upgrade your plan...") and was being run straight into the
         impact sentence with no stop between them. */
      (GW.HEALTH_IMPACT && GW.HEALTH_IMPACT[top.k] ? '<div class="attn-s">' + esc(GW.HEALTH_IMPACT[top.k]) + '</div>' : '') +
      (top.c.detail ? '<div class="attn-d">' + esc(top.c.detail) + '</div>' : '') +
      (more.length ? '<div class="attn-more">Also red: ' + more.map(function (x) { return esc(nm(x)) + ' (' + esc(x.c.text) + ')'; }).join('; ') + '</div>' : '') +
      '</div>' + side + '</section>';
  }

  /* ── Slots, from the server's windows ── */
  function slotsFor(d) {
    var sr = d.series, out = [];
    if (sr.grain === 'hour') {
      sr.slots.forEach(function (k) { var h = +k; out.push({ key: k, label: hourLabel(h), labelNext: hourLabel((h + 1) % 24), head: hourHead(h) + ' – ' + hourHead((h + 1) % 24), headStart: hourHead(h), headEnd: hourHead((h + 1) % 24) }); });
    } else if (sr.grain === 'day') {
      /* LAST WEEK IS LINED UP BY POSITION, not by date. The server keys last
         week's buckets by last week's dates (the 14th-20th); looking them up
         with this week's (the 21st-27th) never matched, so every grey bar
         and every "Last week" table cell was a confident 0 under a legend
         that said "Last week". Found by the 26 Sept review. prevKey is the
         i-th day of the comparison window at noon ET, so DST cannot shift it. */
      var prevStart = d.windows.cmp ? d.windows.cmp.from : null;
      sr.slots.forEach(function (k, i) {
        var p = dayParts(k), pd = prevStart ? new Date(new Date(prevStart).getTime() + i * 864e5 + 43200e3) : null;
        out.push({ key: k, label: DOW[i], sub: String(p.d), head: DOW[i] + ' ' + p.d + ' ' + p.m, prevKey: pd ? G.etDay(pd) : null, prevHead: pd ? G.etD(pd) : '' });
      });
    } else {
      var years = {}; sr.slots.forEach(function (k) { years[k.slice(0, 4)] = 1; });
      var multi = Object.keys(years).length > 1;
      sr.slots.forEach(function (k) { var y = k.slice(0, 4), m = MON[+k.slice(5, 7) - 1]; out.push({ key: k, label: m, sub: multi ? y : '', head: m + ' ' + y }); });
    }
    return out;
  }
  function seriesFor(d, slots) {
    var u = G.S.unit, sr = d.series, now = d.asof, cur = [], prev = sr.prev ? [] : null, partialIdx = -1;
    var nowKey = sr.grain === 'hour' ? String(etHour(now)).padStart(2, '0') : sr.grain === 'day' ? G.etDay(new Date(now)) : G.etDay(new Date(now)).slice(0, 7);
    slots.forEach(function (sl, i) {
      var future = sl.key > nowKey;
      cur.push(future ? null : (sr.cur[u][sl.key] || 0));
      if (prev) prev.push(sr.prev[u][sl.prevKey || sl.key] || 0);
      if (sl.key === nowKey) partialIdx = i;
    });
    return { cur: cur, prev: prev, partialIdx: partialIdx };
  }
  function sumNote(d, series) {
    var total = d.kpi.people[G.S.unit][0], sum = 0;
    series.cur.forEach(function (v) { sum += v || 0; });
    var span = d.series.grain === 'hour' ? 'in a later hour' : d.series.grain === 'day' ? 'on a second day' : 'in a later month';
    if (G.S.unit === 'leads') return 'Every attempt is in one bar, so the bars add up to the headline exactly.';
    if (sum === total) return 'Nobody came back ' + span + ', so the bars add up to the headline.';
    var rp = d.series.repeats || {};
    var who = d.series.grain === 'month' ? 'A person counts once per month they came: ' + fmt(rp.repeaters) + ' came back later' : fmt(rp.repeaters) + ' ' + G.plural(rp.repeaters, 'person', 'people') + ' came back ' + span;
    return who + ', so the bars add to ' + fmt(sum) + ', not ' + fmt(total) + '.';
  }
  /* Month on month, honestly: a partial first month is never a base, and
     the running month is compared at the SAME POINT of the last one. */
  function momFn(d, slots, series) {
    return function (i) {
      if (i === 0) return null;
      var fl = d.first_lead ? G.etDay(new Date(d.first_lead)) : null;
      if (i === 1 && fl && fl.slice(8) !== '01') return '— (' + MON[+fl.slice(5, 7) - 1] + ' started on the ' + (+fl.slice(8)) + 'th)';
      if (i === series.partialIdx && d.month) { var m = d.month[G.S.unit]; return m[1] ? ((m[0] >= m[1] ? '+' : '') + Math.round((m[0] - m[1]) / m[1] * 100) + '% vs ' + slots[i - 1].label + ' at the same point') : null; }
      var a = series.cur[i], b = series.cur[i - 1];
      return b ? ((a >= b ? '+' : '') + Math.round((a - b) / b * 100) + '% vs ' + slots[i - 1].label) : null;
    };
  }
  function meta(d) {
    var w = d.windows;
    if (d.view === 'today') return G.etD(d.asof) + ' · so far · against yesterday at this time';
    /* the start keeps its month when the week crosses one: "Mon 28 Sep – Thu 1 Oct", never "Mon 28 – Thu 1 Oct" */
    if (d.view === 'week') return G.etD(w.cur.from, { noMonth: G.etDay(new Date(w.cur.from)).slice(0, 7) === G.etDay(new Date(d.asof)).slice(0, 7) }) + ' – ' + G.etD(d.asof) + ' so far · against last week at the same point';
    return 'Since the form went live · ' + G.etD(d.first_lead, { noWeekday: true, year: true }) + ' – ' + G.etD(d.asof, { noWeekday: true, year: true });
  }

  function kpis(d) {
    var u = G.S.unit, W = word(), k = d.kpi, h = '';
    var peopleLbl = u === 'people' ? 'People' : 'Leads';
    /* The parts ADD UP to the number above them: the ladder's "disqualified,
       other" row is named when it is not zero. Each person resolves to one
       stage, so other = dq - b2c - waitlist exactly. */
    var dqSub = function (i) {
      var other = k.dq[u][i] - k.b2c[u][i] - k.waitlist[u][i];
      return fmt(k.b2c[u][i]) + ' sell to consumers · ' + fmt(k.waitlist[u][i]) + ' asked for the waitlist' + (other > 0 ? ' · ' + fmt(other) + ' no reason recorded' : '');
    };
    var BLOCKED_T = 'The Dropoff ladder’s blocked row: blocked, and not booked or disqualified first. The Blocked tab lists every blocked lead.';
    /* "MODEL" IN THE NAME, on every view. On the classic dashboard "Meta
       withheld" is a filter over five reasons -- blocked, model, website not
       verified, disqualified, ours -- and this card counts one of them, a
       population thirty times smaller, under the same two words. */
    var WITHHELD_L = 'Meta withheld — model';
    var WITHHELD_T = 'Leads the model placed in an industry we do not advertise to. Not blocked: they can book, they reach Salesforce and are dialled. Only the Meta conversion events are held back.';
    if (d.view === 'all') {
      var tot = k.people[u][0], bk = k.booked[u][0], mo = d.month, mc = mo[u], mb = mo.booked[u];
      h += U.leadCard({ id: 'people', label: peopleLbl, big: tot, sub: 'Got through step 1, since ' + G.etD(d.first_lead, { noWeekday: true }),
        cmp: U.cmpBlock('This month so far', 'Last month, same point', mc[0], mc[1], false, true, 'Month on month') });
      h += U.leadCard({ id: 'rate', label: 'Booking rate', big: pct(bk, tot), isRate: true, sub: '<b>' + fmt(bk) + '</b> of ' + fmt(tot) + ' ' + W + ' booked',
        cmp: U.cmpBlock('This month so far', 'Last month, same point', pct(mb[0], mc[0]), pct(mb[1], mc[1]), true, true, 'Month on month') });
      h += U.metricCard({ id: 'completed', label: 'Completed step 2', value: k.completed[u][0], sub: pct(k.completed[u][0], tot) + '% of ' + W, title: 'Filled in and sent step 2 of the form.' });
      h += U.metricCard({ id: 'booked', label: 'Booked', value: bk, sub: pct(bk, k.completed[u][0]) + '% of completed' });
      /* ALWAYS PEOPLE, and it says so ON the card in Leads mode: every other
         card switches unit, and a hover title does not exist on a phone. */
      h += U.metricCard({ id: 'recovered', label: 'Recovered bookings', value: d.recovered, sub: 'completed, left, then booked on a later visit' + (u === 'leads' ? ' · counted in people' : '') });
      h += U.metricCard({ id: 'dq', label: 'Disqualified', value: k.dq[u][0], sub: dqSub(0) });
      h += U.metricCard({ id: 'blocked', label: 'Blocked — not our market', value: k.blocked[u][0], sub: 'real estate or insurance', title: BLOCKED_T });
      h += U.metricCard({ id: 'withheld', label: WITHHELD_L, value: k.withheld[u][0], sub: 'not blocked — only the ad signal withheld', title: WITHHELD_T });
      return h;
    }
    var c = k.people[u][0], p = k.people[u][1], b = k.booked[u], cm = k.completed[u];
    var cmpWord = d.view === 'today' ? 'at this time yesterday' : 'at this point last week';
    var cl = d.view === 'today' ? 'Today' : 'This week', pl = d.view === 'today' ? 'Yesterday' : 'Last week';
    var r0 = pct(b[0], c), r1 = pct(b[1], p);
    h += U.leadCard({ id: 'people', label: peopleLbl, big: c, chip: U.delta(c, p, true), sub: '<b>' + fmt(p) + '</b> ' + cmpWord, cmp: U.cmpBlock(cl, pl, c, p, false, true) });
    h += U.leadCard({ id: 'rate', label: 'Booking rate', big: r0, isRate: true, chip: U.delta(r0, r1, true, true),
      sub: '<b>' + fmt(b[0]) + '</b> of ' + fmt(c) + ' ' + W + ' booked' + (r1 !== null ? ' · ' + r1 + '% ' + cmpWord : ''), cmp: U.cmpBlock(cl, pl, r0, r1, true, true) });
    h += U.metricCard({ id: 'completed', label: 'Completed step 2', value: cm[0], chip: U.delta(cm[0], cm[1], true), sub: fmt(cm[1]) + ' ' + cmpWord, title: 'Filled in and sent step 2 of the form.' });
    h += U.metricCard({ id: 'booked', label: 'Booked', value: b[0], chip: U.delta(b[0], b[1], true), sub: fmt(b[1]) + ' ' + cmpWord });
    /* SESSIONS, and the rate beside them is in the SELECTED unit -- it
       divided people whatever the toggle said, so in Leads mode this card
       and the funnel beside it printed two different "% got through step 1". */
    var ss0 = d.sessions[0], f = d.funnel;
    h += U.metricCard({ id: 'sessions', label: 'Sessions', value: ss0, chip: U.delta(ss0, d.sessions[1], true),
      sub: f && f.sessions ? G.pct1(f[u].step1, f.sessions) + '% got through step 1' : '', title: 'Visits to a page carrying the form, one per browser tab, bots excluded.' });
    h += U.metricCard({ id: 'dq', label: 'Disqualified', value: k.dq[u][0], chip: U.delta(k.dq[u][0], k.dq[u][1], null), sub: dqSub(0) });
    h += U.metricCard({ id: 'blocked', label: 'Blocked — not our market', value: k.blocked[u][0], chip: U.delta(k.blocked[u][0], k.blocked[u][1], null), sub: fmt(k.blocked[u][1]) + ' ' + cmpWord, title: BLOCKED_T });
    h += U.metricCard({ id: 'withheld', label: WITHHELD_L, value: k.withheld[u][0], chip: U.delta(k.withheld[u][0], k.withheld[u][1], null), sub: fmt(k.withheld[u][1]) + ' ' + cmpWord, title: WITHHELD_T });
    return h;
  }

  function panels(d) {
    var u = G.S.unit, slots = slotsFor(d), series = seriesFor(d, slots), view = d.view;
    var title = (u === 'people' ? 'People' : 'Leads') + (view === 'today' ? ' by hour' : view === 'week' ? ' per day' : ' per month');
    var qual = view === 'today' ? 'today so far, against yesterday' : view === 'week' ? 'this week, against last week' : 'since the form went live';
    var curLabel = view === 'today' ? 'Today' : view === 'week' ? 'This week' : (u === 'people' ? 'People' : 'Leads');
    var prevLabel = view === 'today' ? 'Yesterday' : 'Last week';
    var legend = series.prev ? '<div class="legend"><span><i class="sw"></i>' + curLabel + '</span><span><i class="sw-prev"></i>' + prevLabel + '</span></div>' : '';
    /* ONE fixed name with aria-pressed carrying the state: a name that also
       flipped read "Show the chart, toggle button, pressed" while the table
       was showing, which sounds like the chart is on. */
    var tbtn = '<button class="ibtn" data-table-toggle aria-pressed="' + G.S.table + '" aria-label="Show as a table" title="' + (G.S.table ? 'Back to the chart' : 'Show as a table') + '">' + G.ic(G.S.table ? 'chart-bar' : 'table') + '</button>';
    var h = U.panel({ id: 'ov-chart', title: title, qual: qual, right: legend + tbtn, body: '<div class="chart" id="ov-chart-el"></div>', foot: '<span id="ov-sumnote">' + esc(sumNote(d, series)) + '</span>' });
    var f = d.funnel;
    if (f) {
      var fs = f[u];
      h += U.panel({ id: 'ov-funnel', title: 'Funnel', qual: view === 'today' ? 'today' : view === 'week' ? 'this week' : 'since ' + G.etD(f.since, { noWeekday: true }),
        body: U.funnel(f.sessions, [['Got through step 1', fs.step1], ['Completed step 2', fs.completed], ['Booked', fs.booked]]),
        foot: '<span>A session is one visit to a page carrying the form, one per browser tab, bots excluded. ' + (view === 'all' ? 'Sessions are tracked from ' + esc(G.etD(f.since, { noWeekday: true })) + ', so the funnel starts there. ' : '') + 'Bookings that arrived by webhook alone are left out: they never loaded a form page.</span>' });
    }
    if (d.channels) {
      var list = d.channels[u] || [], total = list.reduce(function (a, x) { return a + x.n; }, 0);
      var top3 = list.slice(0, 3), rest = list.slice(3), restN = rest.reduce(function (a, x) { return a + x.n; }, 0);
      /* "Everything else" SAYS what is in it, on screen: it was a hover title,
         which a phone never shows. */
      var rows = top3.concat(restN ? [{ name: 'Everything else', n: restN, names: rest.map(function (x) { return x.name + ' ' + fmt(x.n); }).join(' · ') }] : []);
      var mx = rows.length ? rows[0].n : 1;
      h += U.panel({ id: 'ov-chan', cls: 'span-all', title: 'Where they came from', qual: (view === 'today' ? 'today' : 'this week') + ', ' + (u === 'people' ? 'first visit per person' : 'every attempt'),
        body: rows.length ? '<div class="chan">' + rows.map(function (x) { return '<div class="ch"><span>' + esc(x.name) + '</span><div class="tr"><div class="fi" style="width:' + (x.n / mx * 100) + '%"></div></div><span class="v">' + fmt(x.n) + '<span>' + pct(x.n, total) + '%</span></span>' + (x.names ? '<small class="chn">' + esc(x.names) + '</small>' : '') + '</div>'; }).join('') + '</div>' : U.empty('No leads yet in this window'),
        foot: '<span>Read from the ad click first, then from the referrer where the click lost its tags — the same rule as the Dropoff tab.</span>' });
    }
    return { html: h, draw: function () {
      var el = document.getElementById('ov-chart-el'); if (!el) return;
      var r = GW.chart.draw(el, { title: title, grain: d.series.grain, slots: slots, cur: series.cur, prev: series.prev, partialIdx: series.partialIdx,
        curLabel: curLabel, prevLabel: prevLabel, table: G.S.table, caps: view === 'today' ? 'key' : 'all', partialWord: view === 'today' ? 'now' : 'so far',
        slotName: view === 'today' ? 'Hour' : view === 'week' ? 'Day' : 'Month', momText: view === 'all' ? momFn(d, slots, series) : null });
      var note = document.getElementById('ov-sumnote');
      if (note && r && r.grouped) note.textContent = sumNote(d, series) + ' On a narrow screen the hours are grouped in threes — switch to the table for every hour.';
    } };
  }

  function render() {
    if (!root || (G.current && G.current() !== 'overview')) return;
    var v = G.S.view, d = data[v], e = err[v];
    var live = v === 'today' ? '<span class="badge b-good" id="ov-live"><span class="live-dot"></span>Live</span>' : '';
    var readat = d ? (v === 'today' ? live + '<span class="wide-only">Updates every minute · paused while this tab is hidden</span>' + (d.last_lead_at ? '<span>Last lead ' + esc(G.etTime(d.last_lead_at)) + ' (' + esc(G.ago(d.last_lead_at)) + ')</span>' : '')
                                     : 'Read ' + esc(G.etTime(d.generated_at)) + ' ET · refreshes every 5 minutes') : '';
    /* the reason is ON SCREEN, not in a hover title */
    if (d && e) readat += ' <span class="badge b-warn">Last refresh failed (' + esc(String(e).slice(0, 40)) + ') — showing ' + esc(G.etTime(d.generated_at)) + '</span>';
    /* PHONE: the period and the unit share the controls block without a
       separate line for the unit note -- the headline number used to start
       below the first screen, under two full-width toggles and three lines
       of small print. The note rides in the meta line instead. */
    var unitNote = G.S.unit === 'people' ? 'each address counted once' : 'every form attempt counted';
    var head = '<section class="ph"><div class="ph-top"><h1 class="title" tabindex="-1">Overview</h1><span class="readat">' + readat + '</span></div>' +
      '<div class="controls"><div class="filters">' + U.tg([['today', 'Today', '<span class="live-dot"></span>'], ['week', 'This week'], ['all', 'All time']], v, 'data-view', 'Period') +
      U.tg([['people', 'People'], ['leads', 'Leads']], G.S.unit, 'data-unit', 'Count') + '</div>' +
      '<span class="meta">' + (d ? esc(meta(d)) + ' · ' : '') + esc(unitNote) + '</span></div></section>';
    var body, p = null;
    if (!d && e) body = '<section class="card panel">' + U.unavailable('The overview', e) + '</section>';
    else if (!d) body = '<section class="kpis">' + U.loading(2) + U.loading(2) + '</section>';
    else { p = panels(d); body = '<section class="kpis" id="ov-kpis">' + kpis(d) + '</section><div class="grid2">' + p.html + '</div>'; }
    G.paint(root, head + attention() + body);
    if (p) p.draw();
  }
  if (typeof document !== 'undefined' && document.addEventListener) document.addEventListener('click', function (e) {
    var b = e.target && e.target.closest ? e.target.closest('[data-attn]') : null; if (!b) return;
    var k = b.getAttribute('data-attn'); attnOpen = attnOpen === k ? null : k; render();
  });
  G.onVisibility = function (isHidden) { var b = document.getElementById('ov-live'); if (b) { b.className = 'badge ' + (isHidden ? 'b-neu' : 'b-good'); b.innerHTML = '<span class="live-dot' + (isHidden ? ' paused' : '') + '"></span>' + (isHidden ? 'Paused' : 'Live'); } };
  return { title: 'Overview', activate: activate, deactivate: deactivate, render: render, setView: setView, refresh: refresh,
           _data: function () { return data; }, _slotsFor: slotsFor, _seriesFor: seriesFor, _sumNote: sumNote, _momFn: momFn, _meta: meta };
})(GW);
