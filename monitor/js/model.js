/* ============================================================================
   Model -- what the website classifier (the non-ICP "AI check") decided, and
   what it could not read. One payload, /monitor/non-icp, unchanged; the
   Monday near-miss digest reads the same payload, which is one more reason
   this tab RENDERS it and never recomputes it:

   - the ladder, the three industry groups, the labels, the percentages, the
     confidence and floor figures all arrive resolved from the server;
   - the business-type list lives in NON_ICP_BUSINESS_TYPES on the server and
     is never copied here -- a client copy is exactly the drift the rule
     forbids (CLAUDE.md, "THE MODEL LAYER'S OWN TRAPS");
   - the standing cache is TOP-LEVEL (d.cache). Reading d.scrape.cache is
     the 15 Sept "0 companies classified all time" incident.

   UNITS, in words on screen: the ladder, the groups and the decisions count
   LEADS; the scrape panel, the cache and the near misses count COMPANIES
   (registrable domains). Page text and the model's quote come from other
   people's websites: everything is escaped.
   ============================================================================ */
GW.TABS.model = (function (G) {
  var U = G.ui, L = G.L, esc = G.esc, fmt = G.fmt;
  var DAYS = [['1', 'Last 24 hours'], ['7', 'Last 7 days'], ['30', 'Last 30 days'], ['90', 'Last 90 days']];
  var PRODUCTS = [['all', 'All products'], ['aeo', 'AEO'], ['crm', 'CRM'], ['__none', 'Untagged']];
  var PAGE = 25;
  var data = null, err = null, root = null, seq = 0, busy = false, decShown = PAGE, unrShown = PAGE;
  function days() { var d = G.S.q.days; return DAYS.some(function (x) { return x[0] === d; }) ? d : '7'; }
  function product() { var p = G.S.q.product; return PRODUCTS.some(function (x) { return x[0] === p; }) ? p : 'all'; }
  function load() {
    var my = ++seq, p = product(); busy = true;
    return G.api('/monitor/non-icp', { days: days(), product: p === 'all' ? undefined : p }, { timeout: 30000 })
      .then(function (d) { if (my === seq) { data = d; err = null; } }, function (e) { if (my === seq) err = e.message; })
      .then(function () { if (my === seq) busy = false; });
  }
  function activate(el) { root = el; var p = load(); render(); G.every('model', 300000, function () { load().then(render); }); return p.then(render); }
  function deactivate() { G.stop('model'); root = null; }
  function pctText(p) { return p === null || p === undefined ? '—' : p + '%'; }            /* one decimal from the server; never re-rounded */
  function conf(c) { return c === null || c === undefined ? '—' : Math.round(c * 100) + '%'; }
  function yesno(b, yes, no) { return b ? yes : no; }
  function N(v) { var n = Number(v); return isNaN(n) ? 0 : n; }
  function bar(n, total) { return '<div class="tr"><div class="fi" style="width:' + (total ? Math.round(n / total * 100) : 0) + '%"></div></div>'; }
  function sel(attr, list, cur, label) {
    return '<label class="lfl inline"><span>' + label + '</span><select class="field" data-mdl="' + attr + '">' + list.map(function (o) { return '<option value="' + o[0] + '"' + (o[0] === cur ? ' selected' : '') + '>' + o[1] + '</option>'; }).join('') + '</select></label>';
  }
  /* "the model", as the server's own labels on this tab say it */
  var ACT = { blocked_list: ['Blocked — brand list', 'b-bad'], blocked_model: ['Blocked — model', 'b-bad'], meta_only: ['Meta withheld', 'b-warn'] };
  /* the SAME relabel as the ladder: in flag-only mode (NON_ICP_LLM_META off)
     a flagged lead still fires Meta, so its chip must not say "withheld" */
  function act(x, d) { if (x.action === 'meta_only' && !(d.flags && d.flags.llm_meta)) return ['Flagged — Meta still sent', 'b-neu']; return ACT[x.action] || [x.action, 'b-neu']; }
  function more(n, shown, key) { return n > shown ? '<div class="more"><span>Showing ' + fmt(shown) + ' of ' + fmt(n) + '</span><button class="btn" data-more="' + key + '">Show ' + fmt(Math.min(PAGE, n - shown)) + ' more</button></div>' : ''; }
  var THIS_YEAR = G.etDay(new Date()).slice(0, 4);
  function when(ts) {
    var d = G.okDate(ts); if (!d) return ts ? String(ts) : '—';
    return new Intl.DateTimeFormat('en-US', { timeZone: G.TZ, month: 'short', day: 'numeric', year: G.etDay(d).slice(0, 4) === THIS_YEAR ? undefined : 'numeric', hour: 'numeric', minute: '2-digit' }).format(d);
  }

  function flags(d) {
    var f = d.flags || {};
    return '<div class="flags">' + [
      'Brand list: <b>' + yesno(f.list_block, 'blocking', 'off') + '</b>', 'Model: <b>' + yesno(f.llm_enabled, 'on', 'off') + '</b>',
      'Model blocks: <b>' + yesno(f.llm_block, 'yes', 'no') + '</b>', 'Meta withheld for flagged leads: <b>' + yesno(f.llm_meta, 'yes', 'no') + '</b>',
      'model <b>' + esc(f.model || '—') + '</b>', 'confidence needed <b>' + conf(f.confidence_floor) + '</b>', 'prompt <b>' + esc(f.prompt_version || '—') + '</b>',
    ].join(' · ') + (d.truncated ? ' · <span class="bad-t">window capped at 5,000 leads — the counts are a floor, not a total</span>' : '') + '</div>';
  }
  function ladder(d) {
    var l = d.ladder || { rows: [], total: 0 }, llmMeta = d.flags && d.flags.llm_meta;
    var suffix = d.product === 'aeo' ? ' — AEO only' : d.product === 'crm' ? ' — CRM only' : d.product === '__none' ? ' — untagged product only' : '';
    var rows = (l.rows || []).map(function (r) {
      /* "Meta withheld" is only TRUE while Meta is withheld for flagged
         leads; in flag-only mode the same row is a flag, and Meta still
         fires (All leads' own reason logic says the same). */
      var label = r.key === 'meta_only' && !llmMeta ? 'Flagged by the model (Meta still sent)' : r.label;
      return '<div class="lbar wide" data-row="' + esc(r.key) + '"><div class="t"><span>' + esc(label) + '</span></div>' + bar(r.n, l.total) +
        '<span class="v"><b data-v="' + esc(r.n) + '">' + fmt(r.n) + '</b><span>' + pctText(r.pct) + '</span>' + (r.ours ? '<span> · ' + fmt(r.ours) + ' ours</span>' : '') + '</span></div>';
    }).join('');
    return U.panel({ title: 'What the layer did', qual: 'leads in the window', body: rows || U.empty('No leads in this window'),
      foot: '<span><b data-v="' + esc(l.total) + '">' + fmt(l.total) + '</b> ' + G.plural(l.total, 'lead') + ' in the window' + esc(suffix) + '. The five rows are mutually exclusive and add up to that total.' +
        (l.ours ? ' <b>' + fmt(l.ours) + '</b> of them are our own test submissions — counted here like everything else, and marked on each row below.' : '') + '</span>' });
  }
  function groups(d) {
    var gs = d.industries || [];
    if (!gs.length) return U.panel({ title: 'What we acted on', qual: 'by what decided', body: U.empty('Nothing acted on in this window') });
    return '<div class="mgroups">' + gs.map(function (g) {
      var body = !g.rows || !g.rows.length ? '<p class="lnote">None in this window.</p>' :
        U.grid([
          { label: 'Industry', get: function (r) { return r.label; } },
          { label: 'Leads', attr: function (r) { return 'data-v="' + esc(r.leads) + '"'; }, get: function (r) { return fmt(r.leads); } },
          { label: 'Companies', get: function (r) { return r.uncategorised && !r.domains ? '—' : fmt(r.domains); } },
          { label: 'Median confidence', html: function (r) { return r.uncategorised ? '—' : conf(r.median_confidence); } },
        ], g.rows, { rowCls: function (r) { return r.uncategorised ? 'uncat' : ''; } });
      /* The note is a server constant, escaped anyway -- except the Meta
         group's, which said these leads "booked and are dialled as normal";
         a flagged lead can also be disqualified or never have submitted.
         In flag-only mode the group is a flag, not a withholding, as the
         ladder above already says. */
      var title = g.label, note = g.note || '';
      if (g.key === 'meta_only') {
        if (d.flags && d.flags.llm_meta === false) { title = 'Flagged by the model — Meta still sent'; note = 'Flag-only mode: nothing was withheld. These are the leads the model would act on if Meta withholding were switched on.'; }
        else note = 'Not blocked. Only the conversion events were withheld; nothing else about these leads changed.';
      }
      return U.panel({ cls: 'mgroup', title: title, qual: fmt(g.leads) + ' ' + G.plural(g.leads, 'lead'), body: '<p class="lnote">' + esc(note) + '</p>' + body });
    }).join('') + '</div>';
  }
  function decisions(d) {
    var all = d.decisions || [], rows = all.slice(0, decShown);
    return U.panel({ id: 'mdl-dec', title: 'Every decision, with the evidence', qual: fmt(all.length) + ' ' + G.plural(all.length, 'lead'),
      body: '<p class="lnote">The quote is what the model read on their own site and decided on. If a row looks like a real prospect, the quote is the fastest way to tell a wrong scope from a wrong call. ' +
        /* the one place a reader learns which switch stops which action -- all
           three take effect on Railway without a deploy */
        'To stop a kind of action: a brand-list block stops with <code>NON_ICP_BLOCK=false</code>, a model block with <code>NON_ICP_LLM_BLOCK=false</code>, and holding back Meta alone with <code>NON_ICP_LLM_META=false</code>.</p>' +
        U.rtable({ ns: 'mdl', rows: rows, key: function (x) { return x.session_id; }, rowName: function (x) { return x.email || 'this lead'; },
          emptyTitle: 'Nothing was blocked or held back in this window',
          cols: [
            { label: 'When (ET)', cls: 'm', get: function (x) { return when(x.created_at); } },
            { label: 'Email', cls: 'ecell', html: function (x) { return '<span class="em">' + esc(x.email || '—') + '</span>' + (x.is_internal ? '<span class="marks"><span class="badge b-neu">ours</span></span>' : ''); } },
            { label: 'Industry', html: function (x) { return esc(x.business_type_label || x.business_type || '—') + (x.confidence !== null && x.confidence !== undefined ? ' <span class="na">' + conf(x.confidence) + '</span>' : ''); } },
            { label: 'Action', html: function (x) { var a = act(x, d); return '<span class="badge ' + a[1] + '">' + esc(a[0]) + '</span>'; } },
            { label: 'Booked', opt: 1, html: function (x) { return x.booked ? '<span class="badge b-good">Yes</span>' : '<span class="badge b-neu">No</span>'; } },
            { label: 'What the page said', cls: 'quote', get: function (x) { return x.evidence_quote || '—'; } },
          ],
          detail: function (x) {
            /* Booked is dropped from the row at mid widths, so it is here too */
            return U.kv([['Booked', x.booked ? 'Yes' : 'No'], ['Domain judged', x.domain_judged ? '<code>' + esc(x.domain_judged) + '</code>' : null, 'html'], ['Why', x.reason], ['Quote', x.evidence_quote ? '“' + x.evidence_quote + '”' : null], ['Company', x.company], ['Their website', x.website], ['Product', x.product],
              ['Decided by', x.source ? L.sourceShort(x.source) : null], ['Model', x.model_id], ['Prompt', x.prompt_version],
              ['Page read', x.page_url_used ? x.page_url_used + (x.page_text_chars ? ' (' + fmt(x.page_text_chars) + ' characters)' : '') : null], ['Decided at', x.checked_at ? G.et(x.checked_at) : null]]);
          } }) + more(all.length, decShown, 'mdl-dec') });
  }
  function scrape(d) {
    var s = d.scrape || {}, w = s.window || {}, c = d.cache || {}, ip = s.inProcess || {};
    var bad = (w.unreachable || 0) + (w.thin || 0) + (w.other || 0);
    var allU = s.unreadable || [], rows = allU.slice(0, unrShown);
    var list = U.rtable({ ns: 'mdlu', rows: rows, key: function (u) { return u.domain + '|' + (u.email || ''); }, rowName: function (u) { return u.domain; },
      emptyTitle: 'Every domain we have an answer for was readable', emptyBody: 'Domains never tried are counted above, not here.',
      cols: [
        { label: 'Domain', cls: 'kcell', html: function (u) { return '<code>' + esc(u.domain) + '</code>'; } },
        /* a row here whose page READ FINE is a model call that failed (timed
           out, refused, unparseable) -- the page was not the problem */
        { label: 'Why', html: function (u) { return '<span title="' + esc(u.scrape_status || '') + '">' + esc(u.scrape_status === 'ok' ? 'page read; the model call failed' : L.scrape(u.scrape_status)) + '</span>' + (u.error ? '<div class="na">' + esc(String(u.error).slice(0, 120)) + '</div>' : ''); } },
        /* a break offered after the @, so a narrow column splits an address there, not mid-word */
        { label: 'Lead', cls: 'wrap', html: function (u) { return u.email ? esc(u.email).replace('@', '@<wbr>') : '—'; } },
        { label: 'Their website', cls: 'wrap', opt: 1, get: function (u) { return u.website || '—'; } },
        { label: 'Blocked anyway?', html: function (u) { return u.blocked ? '<span class="badge b-bad">Yes — ' + esc(L.sourceShort(u.blocked_by)) + '</span>' : '<span class="badge b-neu">No</span>'; } },
        { label: 'Last tried (ET)', cls: 'm', get: function (u) { return when(u.checked_at); } },
      ],
      /* 'Their website' is dropped from the row at mid widths; here it stays */
      detail: function (u) { return U.kv([['Their website', u.website], ['What went wrong', u.error ? String(u.error) : null], ['Status', u.scrape_status]]) || '<p class="lnote">Nothing more is recorded for this company.</p>'; } }) + more(allU.length, unrShown, 'mdl-unr');
    return U.panel({ id: 'mdl-scrape', title: 'What we could not read', qual: 'companies behind this window’s leads',
      body: '<div class="sumgrid four">' +
        U.metricCard({ id: 'mdl-unread', label: 'Could not be read', value: w.unreadable_pct, unit: w.unreadable_pct === null || w.unreadable_pct === undefined ? '' : '%', sub: fmt(bad) + ' of ' + fmt(w.answered) + ' companies we have an answer for' }) +
        U.metricCard({ id: 'mdl-never', label: 'Never tried', value: w.no_verdict, sub: 'of ' + fmt(w.total) + ' companies — not a failure, so not in the rate' }) +
        /* TOP-LEVEL cache -- the incident */
        U.metricCard({ id: 'mdl-cache', label: 'Classified, all time', value: c.domains, sub: fmt(c.judged) + ' judged from their site · ' + fmt(c.unreadable) + ' currently unreadable' }) +
        /* writes failed and blocks failed open each turn System health red on
           their own; the classic printed them here, and so does this */
        U.metricCard({ id: 'mdl-deploy', label: 'Classified since this deploy', value: ip.ok, chip: (N(ip.writeFailed) || N(ip.bypassFailed)) ? '<span class="badge b-bad">failures</span>' : '',
          sub: fmt(ip.errored) + ' API errors · ' + fmt(ip.unreachable) + ' unreadable · cache hits ' + (ip.cacheHitPct === null || ip.cacheHitPct === undefined ? '—' : ip.cacheHitPct + '%') +
            '<br><span class="' + (N(ip.writeFailed) ? 'bad-t' : '') + '">' + fmt(N(ip.writeFailed)) + ' ' + G.plural(N(ip.writeFailed), 'write') + ' failed</span> · <span class="' + (N(ip.bypassFailed) ? 'bad-t' : '') + '">' + fmt(N(ip.bypassFailed)) + ' ' + G.plural(N(ip.bypassFailed), 'block') + ' failed open</span>' +
            (ip.avgWarmMs !== null && ip.avgWarmMs !== undefined ? ' · warm ' + fmt(ip.avgWarmMs) + ' ms average, ' + fmt(ip.maxWarmMs) + ' ms worst' : '') }) + '</div>' +
        '<p class="lnote">No rate is shown for the all-time figure: most of it came from a backfill that only loaded companies it could read, so a percentage over it would not mean anything. ' + (s.notes || []).map(esc).join(' ') + '</p>' +
        '<h3 class="pgh">Companies we could not read, this window</h3>' + list });
  }
  function near(d) {
    var n = d.nearMisses || { rows: [], floors: {} };
    return U.panel({ id: 'mdl-near', title: 'Near misses', qual: 'companies, all time — not narrowed by the window',
      body: '<p class="lnote">These looked like real estate or insurance, but the AI check was not sure enough to turn them away, so they came through as normal leads. It needs <b>' + conf(n.floors && n.floors.page) +
        '</b> certainty when it can read their website and <b>' + conf(n.floors && n.floors.name) + '</b> when it can only judge the domain name. <b data-v="' + esc(n.total) + '">' + fmt(n.total) + '</b> came close without crossing it; <b>' +
        fmt(n.withLeads) + '</b> had somebody fill in the form in this window (marked below).</p>' +
        U.rtable({ ns: 'mdln', rows: n.rows || [], key: function (r) { return r.domain; }, rowName: function (r) { return r.domain; }, emptyTitle: 'Nothing came close to the line without crossing it',
          cols: [
            { label: 'Domain', html: function (r) { return '<code>' + esc(r.domain) + '</code>' + (r.leads > 0 ? '<span class="marks"><span class="badge b-warn">has a lead</span></span>' : ''); } },
            { label: 'Looked like', get: function (r) { return r.label || '—'; } },
            { label: 'How sure', r: 1, get: function (r) { return r.confidence_pct + '%'; } },
            { label: 'Needed', r: 1, get: function (r) { return r.floor_pct + '%'; } },
            { label: 'Judged from', get: function (r) { return r.judged_from || '—'; } },
            { label: 'Leads this window', r: 1, html: function (r) { return r.leads > 0 ? '<b>' + fmt(r.leads) + '</b>' : '0'; } },
          ] }) +
        /* the server lists the first 50; a cap nobody can see reads as the whole set */
        (N(n.total) > (n.rows || []).length ? '<p class="lnote">Showing the first ' + fmt((n.rows || []).length) + ' of ' + fmt(n.total) + ', the ones with leads first.</p>' : '') });
  }
  function cache(d) {
    var c = d.cache || {}, rows = c.byType || [];
    return U.panel({ id: 'mdl-cachet', title: 'What the cache knows', qual: 'companies, all time',
      body: '<p class="lnote">A standing inventory of every company ever classified, mostly the historical backfill. <b>Not this window, and not leads</b>: how many companies in our history the AI check would act on if they came back today.</p>' +
        (rows.length ? U.grid([
          { label: 'Industry', get: function (r) { return r.label; } },
          { label: 'What it does', html: function (r) { return r.action === 'block' ? '<span class="badge b-bad">Blocks</span>' : r.action === 'meta' ? '<span class="badge b-warn">Withholds Meta</span>' : '<span class="badge b-neu">No action</span>'; } },
          { label: 'Companies', attr: function (r) { return 'data-v="' + esc(r.domains) + '"'; }, get: function (r) { return fmt(r.domains); } },
        ], rows) : U.empty('Nothing cached yet')) });
  }
  function render() {
    if (!root || (G.current && G.current() !== 'model')) return;
    var d = data;
    /* the window is read back from the PAYLOAD (the server echoes it): the
       select moves first, and old numbers under a new window are a lie */
    var wd = d && d.windowDays !== null && d.windowDays !== undefined && !isNaN(Number(d.windowDays)) ? Number(d.windowDays) : null, stale = d && (busy || (wd !== null && String(wd) !== days()));
    var head = '<section class="ph"><div class="ph-top"><h1 class="title" tabindex="-1">Model</h1><span class="readat">' + (d ? 'Read ' + esc(G.etTime(d.generatedAt)) + ' ET' + (wd !== null ? ' · the last ' + (wd === 1 ? '24 hours' : fmt(wd) + ' days') : '') : '') + (stale ? ' · updating…' : '') + '</span></div>' +
      /* TRUE FOR EVERY ROW: a late verdict marks a lead after it booked, and a
         name-only verdict never read the site */
      '<p class="lede">What the website classifier — the model — decided, and what it could not read. A <b>blocked</b> lead was stopped as real estate or insurance: before the calendar, or marked after booking when the answer came late. ' +
        (d && d.flags && d.flags.llm_meta === false ? 'A <b>flagged</b> lead was not blocked, and its Meta events were still sent: the model is only observing.'
          : 'A <b>Meta withheld</b> lead was not blocked: only its conversion events were held back, and nothing else about it changed.') + '</p>' +
      '<div class="controls">' + sel('days', DAYS, days(), 'Window') + sel('product', PRODUCTS, product(), 'Product') + '</div></section>';
    var body;
    if (!d && err) body = '<section class="card panel">' + U.unavailable('The Model report', err) + '</section>';
    else if (!d) body = U.loading(5);
    else body = U.drawn('The Model report', function () { return (err ? '<div class="readat"><span class="badge b-warn">Last refresh failed (' + esc(String(err).slice(0, 40)) + ') — showing the previous read</span></div>' : '') +
      flags(d) + ladder(d) + groups(d) + decisions(d) + scrape(d) + near(d) + cache(d); });
    G.paint(root, head + body);
  }
  /* the last "Show more" removes itself: focus the first row it revealed */
  function afterMore(ns, from) { var xs = root && root.querySelectorAll ? root.querySelectorAll('[data-x^="' + ns + '-"]') : []; if (xs[from] && !root.querySelector('[data-more="' + (ns === 'mdl' ? 'mdl-dec' : 'mdl-unr') + '"]') && xs[from].focus) xs[from].focus(); }
  if (typeof document !== 'undefined' && document.addEventListener) document.addEventListener('click', function (e) {
    var t = e.target && e.target.closest ? e.target.closest('[data-more]') : null; if (!t || G.current() !== 'model') return;
    if (t.getAttribute('data-more') === 'mdl-dec') { var a = decShown; decShown += PAGE; render(); afterMore('mdl', a); }
    if (t.getAttribute('data-more') === 'mdl-unr') { var b = unrShown; unrShown += PAGE; render(); afterMore('mdlu', b); }
  });
  if (typeof document !== 'undefined' && document.addEventListener) document.addEventListener('change', function (e) {
    var el = e.target; if (!el || !el.getAttribute || G.current() !== 'model') return;
    var k = el.getAttribute('data-mdl'); if (!k) return;
    if (k === 'days') { if (el.value === '7') delete G.S.q.days; else G.S.q.days = el.value; }
    if (k === 'product') { if (el.value === 'all') delete G.S.q.product; else G.S.q.product = el.value; }
    decShown = unrShown = PAGE; G.writeHash(); var p = load(); render();
    p.then(function () { if (G.current() !== 'model') return; render(); var d = data; if (d && d.ladder) G.announce(fmt(d.ladder.total) + ' ' + G.plural(d.ladder.total, 'lead') + ' in the window'); });
  });
  return { title: 'Model', activate: activate, deactivate: deactivate, render: render, load: load, _set: function (d) { data = d; } };
})(GW);
