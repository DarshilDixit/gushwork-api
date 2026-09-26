/* ============================================================================
   Lead magnet -- the LP funnel, ported. Two things carried over exactly:

   - TRUE TOTALS FROM THE SERVER. /monitor/lm-leads is LIMIT 500; counting the
     pills client-side over whatever it returned understated every pill past
     500 rows while reading as a total. The pills read statusTotals, and the
     table says when it shows a page and not the population.
   - THE TWO WRITE ACTIONS, Mark sent / Undo and Push to Loops, are POSTs to
     the same routes the old tab uses. tools/preview-monitor.js refuses every
     non-GET, so a preview can never trigger one.

   The drop-off bars are no longer red and amber: status colours are reserved
   for status (dataviz non-negotiable). Severity is in the numbers.
   ============================================================================ */
GW.TABS.lm = (function (G) {
  var U = G.ui, esc = G.esc, fmt = G.fmt;
  var days = '30', m = null, mErr = null, leads = [], totals = null, shownOf = null, lErr = null, filter = 'all', q = '', root = null, seq = 0;
  /* "ALL" SAID ALL AND MEANT "all but ours": it read "All 8" beside "Internal
     tests 9", and on the Duplicates tab next door the same kind of pill
     includes our tests. The label now says what it counts, in the words
     Duplicates uses. */
  var PILLS = [['all', 'All real leads'], ['awaiting', 'Awaiting send'], ['sent', 'Sent'], ['abandoned', 'Abandoned'], ['internal', 'Our own tests']];
  /* the latest window wins, not the last response to arrive */
  function load() {
    var my = ++seq, m1 = m, e1 = mErr, l1 = leads, s1 = shownOf, le1 = lErr;
    return Promise.all([
      G.api('/monitor/lm-metrics', { days: days }).then(function (d) { m1 = d; e1 = null; }).catch(function (e) { e1 = e.message; }),
      G.api('/monitor/lm-leads', { days: days }).then(function (d) { l1 = d.leads || []; s1 = typeof d.total === 'number' ? d.total : null; le1 = null; }).catch(function (e) { le1 = e.message; }),
    ]).then(function () { if (my !== seq) return; m = m1; mErr = e1; leads = l1; shownOf = s1; lErr = le1; totals = m && m.statusTotals || null; });
  }
  function activate(el) { root = el; render(); G.every('lm', 300000, function () { load().then(render); }); return load().then(render); }
  function deactivate() { G.stop('lm'); root = null; }
  function pc(a, b) { return b > 0 ? Math.round(a / b * 100) + '%' : '—'; }
  function bars(rows, total, customFlag) {
    if (!rows.length) return U.empty('Nothing yet', 'No lead-magnet submissions in this window.');
    var mx = Math.max.apply(null, rows.map(function (r) { return r.n; })) || 1;
    return rows.map(function (r) {
      return '<div class="lbar"><div class="t"><span>' + esc(r.label) + (customFlag && r.is_custom ? ' <span class="badge b-neu">custom</span>' : '') + '</span></div>' +
        '<div class="tr"><div class="fi" style="width:' + (r.n / mx * 100) + '%"></div></div><span class="v">' + fmt(r.n) + (total ? '<span>' + pc(r.n, total) + '</span>' : '') + '</span></div>'; }).join('');
  }
  function drop(label, lost, base, hint) {
    return '<div class="lbar"><div class="t"><span>' + esc(label) + '</span><small>' + esc(hint) + '</small></div><div class="tr"><div class="fi" style="width:' + (base > 0 ? lost / base * 100 : 0) + '%"></div></div><span class="v">' + fmt(lost) + '<span>' + pc(lost, base) + '</span></span></div>';
  }
  function match(l) { if (filter === 'internal') return l.is_internal; if (l.is_internal) return false; if (filter === 'all') return true; return l.status === filter; }
  function searched() {
    var s = q.toLowerCase().trim(), base = leads.filter(match); if (!s) return base;
    return base.filter(function (l) { return [l.email, l.industry_category, l.product_or_service, l.website, l.utm_campaign, l.utm_source].join(' ').toLowerCase().indexOf(s) >= 0; });
  }
  function counts() {
    if (totals) return { all: totals.all, awaiting: totals.awaiting, sent: totals.sent, abandoned: totals.abandoned, internal: totals.internal };
    var c = { all: 0, awaiting: 0, sent: 0, abandoned: 0, internal: 0 };
    leads.forEach(function (l) { if (l.is_internal) { c.internal++; return; } c.all++; if (c[l.status] !== undefined) c[l.status]++; });
    return c;
  }
  function statusBadge(l) {
    if (l.is_internal) return '<span class="badge b-neu">ours</span>';
    return l.status === 'sent' ? '<span class="badge b-good">Sent</span>' : l.status === 'awaiting' ? '<span class="badge b-warn">Awaiting</span>' : '<span class="badge b-bad">Abandoned</span>';
  }
  function detail(l) {
    var t = function (x) { return x ? G.et(x) : null; };
    var loops = l.loops_sent ? 'Sent ' + G.et(l.loops_sent_at) : (l.loops_error ? 'Failed: ' + l.loops_error : 'Not sent');
    var act = (l.status === 'awaiting' && !l.is_internal) ? '<button class="btn sm" data-lm-mark="' + esc(l.id) + '" data-undo="0">Mark sent</button>' :
      (l.status === 'sent' ? '<button class="btn sm" data-lm-mark="' + esc(l.id) + '" data-undo="1">Undo sent</button>' : '');
    var retry = (l.loops_error || (!l.loops_sent && l.completed)) ? '<button class="btn sm" data-lm-retry="' + esc(l.id) + '">Push to Loops</button>' : '';
    return (act || retry ? '<div class="fieldset" style="margin-bottom:var(--gw-space-12)">' + act + retry + '</div>' : '') + U.kv([
      ['Attempts', l.attempts > 1 ? l.attempts + ' sessions from this email' : 'First attempt'], ['Entered from', l.entry_point], ['ELV status', l.elv_status],
      ['Reached step', l.step_reached !== null && l.step_reached !== undefined ? l.step_reached + ' of 4' : null], ['UTM source', l.utm_source], ['UTM medium', l.utm_medium],
      ['UTM campaign', l.utm_campaign], ['UTM content', l.utm_content], ['UTM term', l.utm_term], ['Referrer', l.referrer],
      ['Landing page', l.landing_page], ['Previous page', l.previous_page], ['Form page', l.page_url], ['Meta fbc', l.fbc], ['Meta fbp', l.fbp],
      ['Meta Contact sent', l.capi_contact_sent ? 'Yes' : 'No'], ['Loops', loops], ['Submitted', t(l.submitted_at)], ['First seen', t(l.created_at)],
      ['Delivered at', t(l.delivered_at)], ['Session ID', l.session_id]]);
  }
  function render() {
    if (!root || (G.current && G.current() !== 'lm')) return;
    var f = (m && m.funnel) || {};
    var v = +f.views || 0, o = +f.modal_opens || 0, e = +f.emails || 0, sb = +f.submitted || 0;
    var opt = function (x, l) { return '<option value="' + x + '"' + (x === days ? ' selected' : '') + '>' + l + '</option>'; };
    var head = '<section class="ph"><div class="ph-top"><h1 class="title" tabindex="-1">Lead magnet</h1><span class="readat">' + (m ? 'Sessions are visits · people are distinct emails. ' + fmt(+f.people || 0) + ' people entered an email — ' + fmt(+f.people_submitted || 0) + ' completed, ' + fmt(+f.people_abandoned || 0) + ' did not.' : '') + '</span></div>' +
      '<div class="controls"><div class="fieldset"><select class="field" data-lm-days aria-label="Window">' + opt('7', 'Last 7 days') + opt('30', 'Last 30 days') + opt('90', 'Last 90 days') + opt('365', 'Last year') + '</select></div></div></section>';
    var body = '';
    if (!m && mErr) body += '<section class="card panel">' + U.unavailable('The lead-magnet funnel', mErr) + '</section>';
    else if (!m) body += U.loading(3);
    else {
      /* A CLASS, not an inline style: the inline four columns beat the phone
         rule, and four 73px cards broke every label one word per line. And
         "visits", never "people": these are sessions, and a visitor who only
         loaded the page has no email to be a person by. */
      body += '<section class="sumgrid four">' +
        U.metricCard({ id: 'lm-views', label: 'Page views', value: v, sub: 'visits to the page' }) +
        /* a rate with nothing under it says so in words, not "— of opens" */
        U.metricCard({ id: 'lm-opens', label: 'Form opened', value: o, sub: v ? pc(o, v) + ' of views' : 'no views to count from' }) +
        U.metricCard({ id: 'lm-emails', label: 'Email entered', value: e, sub: o ? pc(e, o) + ' of opens' : 'no form opens yet' }) +
        U.metricCard({ id: 'lm-submitted', label: 'Submitted', value: sb, sub: e ? pc(sb, e) + ' of emails' : 'no emails entered yet' }) + '</section>';
      body += '<div class="grid2">' +
        U.panel({ title: 'Where people drop off', body: drop('Left without opening the form', +f.bounced_before_open || 0, v, 'Saw the page, never clicked a call to action') +
          drop('Opened the form, no email', +f.opened_no_email || 0, o, 'The form opened, no valid email was entered') +
          drop('Entered email, never submitted', +f.abandoned || 0, e, 'Verified email captured — these are recoverable'),
          foot: '<span><b>' + fmt(sb) + '</b> completed — ' + pc(sb, v) + ' of views.</span>' }) +
        U.panel({ title: 'Email type', body: bars([{ label: 'Business email', n: +f.business_email || 0 }, { label: 'Free mailbox', n: +f.free_email || 0 }], (+f.business_email || 0) + (+f.free_email || 0)) }) +
        U.panel({ title: 'Industries', qual: 'custom where they typed their own', body: bars(m.industries || [], sb, true) }) +
        U.panel({ title: 'Where they entered from', qual: 'which call to action opened the form', body: (m.entry_points || []).length ? (m.entry_points || []).map(function (x) {
            return '<div class="lbar"><div class="t"><span>' + esc(x.label) + '</span><small>' + fmt(x.n) + ' opens · ' + pc(x.completed, x.n) + ' submitted</small></div><div class="tr"><div class="fi" style="width:' + (m.entry_points[0].n ? x.n / m.entry_points[0].n * 100 : 0) + '%"></div></div><span class="v">' + fmt(x.n) + '</span></div>'; }).join('')
          : U.empty('No opens recorded yet', 'The page needs the v4.4 embed to record which call to action opened the form.') }) +
        U.panel({ cls: 'span-all', title: 'Custom categories entered', qual: 'what people typed when the list did not fit', body: (m.custom_categories || []).length ? bars(m.custom_categories, null) : U.empty('None yet', 'The dropdown is covering everyone so far.') }) +
        U.panel({ cls: 'span-all', title: 'Daily volume', qual: 'views, emails and submissions', body: (m.daily || []).length ? U.grid([
            { label: 'Day', cls: 'day', get: function (x) { return G.dayD(x.day); } },
            { label: 'Views', get: function (x) { return fmt(x.views); } },
            { label: 'Email entered', get: function (x) { return fmt(x.emails); } },
            { label: 'Submitted', get: function (x) { return fmt(x.submitted); } },
          ], m.daily.slice().reverse(), { region: 'Daily volume' }) : U.empty('No days yet') }) +
        '</div>';
    }
    /* Leads */
    var rows = searched(), c = counts();
    var cap = (shownOf !== null && shownOf > leads.length) ? ' · the table shows the most recent ' + fmt(leads.length) + ' of ' + fmt(shownOf) + '; the counts above are full totals' : '';
    body += U.panel({ title: 'Leads', qual: rows.length + ' shown' + (!totals ? ' · counts are for the loaded rows only' : '') + cap,
      right: '<button class="btn sm" data-lm-csv>' + G.ic('download-simple') + 'Export CSV</button>',
      body: '<div class="fieldset"><input class="field search" type="search" data-lm-q placeholder="Search email, industry, website…" aria-label="Search lead-magnet leads by email, industry, product, website or campaign" value="' + esc(q) + '"></div>' +
        U.pills(PILLS, filter, c, 'data-lm-pill', 'Which leads') +
        (lErr && !leads.length ? U.unavailable('Lead-magnet leads', lErr) : U.rtable({ ns: 'lm', rows: rows, key: function (l) { return l.id; }, rowName: function (l) { return l.email || 'a lead'; }, emptyTitle: 'Nothing matches', emptyBody: 'Try another filter or clear the search.',
          cols: [
            { label: 'Email', html: function (l) { return esc(l.email) + (l.is_free_email ? ' <span class="badge b-neu" title="Free mailbox">free</span>' : '') + (l.attempts > 1 ? ' <span class="meta">×' + l.attempts + '</span>' : ''); } },
            { label: 'Industry', html: function (l) { return esc(l.industry_category || '—') + (l.industry_is_custom ? ' <span class="meta">(custom)</span>' : ''); } },
            { label: 'Product / service', cls: 'w', get: function (l) { return l.product_or_service || '—'; } },
            { label: 'Sells to', get: function (l) { return l.sell_to || '—'; } },
            { label: 'Website', cls: 'w', html: function (l) { return esc(l.website || '—') + (l.website_source === 'derived_from_email' ? ' <span class="meta">(from email)</span>' : ''); } },
            { label: 'Source', get: function (l) { return l.utm_source || l.referrer || 'direct'; } },
            { label: 'Status', html: statusBadge },
            { label: 'When (ET)', cls: 'm', get: function (l) { return G.et(l.submitted_at || l.created_at); } },
          ], detail: detail })) });
    G.paint(root, head + body);
  }
  function csv() {
    var rows0 = searched(); if (!rows0.length) return;
    var cols = ['email', 'status', 'industry_category', 'industry_is_custom', 'product_or_service', 'sell_to', 'website', 'website_source', 'is_free_email', 'elv_status', 'entry_point', 'attempts', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'referrer', 'landing_page', 'previous_page', 'page_url', 'submitted_at', 'delivered', 'delivered_at', 'session_id'];
    /* A cell a VISITOR typed that starts with = + - @ tab or CR would run as
       a formula when an SDR opens this in Excel or Sheets: prefixed with an
       apostrophe, which both read as text. The classic export does the same. */
    var Q = '"', qq = function (v) { var t = String(v === null || v === undefined ? '' : v); if (t.length && '=+-@\t\r'.indexOf(t.charAt(0)) >= 0) t = "'" + t; return Q + t.split(Q).join(Q + Q) + Q; };
    var out = [cols.join(',')].concat(rows0.map(function (l) { return cols.map(function (k) { return qq(l[k]); }).join(','); }));
    var a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([out.join('\n')], { type: 'text/csv' }));
    a.download = 'lead-magnet-' + G.etDay(new Date()) + '.csv'; a.click();
  }
  function mark(id, undo) {
    return G.api('/monitor/lm-delivered/' + encodeURIComponent(id), { undo: undo ? '1' : '0' }, { method: 'POST' }).then(function () {
      leads.forEach(function (l) { if (String(l.id) === String(id)) { l.status = undo ? 'awaiting' : 'sent'; l.delivered_at = undo ? null : new Date().toISOString(); } });
      render(); return load().then(render);
    }).catch(function (e) { window.alert('Could not update: ' + e.message); });
  }
  function retry(id) {
    return G.api('/monitor/lm-loops-retry/' + encodeURIComponent(id), {}, { method: 'POST' }).then(function (d) { window.alert(d.ok ? 'Pushed to Loops' : 'Failed: ' + (d.error || 'unknown')); return load().then(render); })
      .catch(function (e) { window.alert('Retry failed: ' + e.message); });
  }
  if (typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('click', function (e) {
      var t = e.target && e.target.closest ? e.target : null; if (!t) return;
      var p = t.closest('[data-lm-pill]'); if (p) { filter = p.getAttribute('data-lm-pill'); render(); return; }
      if (t.closest('[data-lm-csv]')) { csv(); return; }
      var mk = t.closest('[data-lm-mark]'); if (mk) { mark(mk.getAttribute('data-lm-mark'), mk.getAttribute('data-undo') === '1'); return; }
      var rt = t.closest('[data-lm-retry]'); if (rt) { retry(rt.getAttribute('data-lm-retry')); return; }
    });
    document.addEventListener('change', function (e) { if (e.target && e.target.hasAttribute && e.target.hasAttribute('data-lm-days')) { days = e.target.value; m = null; load().then(render); } });
    /* focus and the caret survive the repaint through G.paint */
    document.addEventListener('input', function (e) { if (e.target && e.target.hasAttribute && e.target.hasAttribute('data-lm-q')) { q = e.target.value; render(); } });
  }
  return { title: 'Lead magnet', activate: activate, deactivate: deactivate, render: render, _set: function (a, b) { m = a; leads = b || []; totals = a && a.statusTotals || null; } };
})(GW);
