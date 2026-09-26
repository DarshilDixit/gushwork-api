/* ============================================================================
   All leads and Blocked -- ONE row renderer and ONE lead panel, two tabs, two
   namespaces ('ld' and 'blk'). The classic renders both through
   leadRowsHtml(leads, ns) for the same reason: a second copy drifts, and the
   12 Sept scope break is what a second copy looks like when it breaks.

   Both tabs read /monitor/leads, exactly as the classic does -- no new route,
   no server change. Blocked is the same route with nonicp=only.

   UNITS, every one printed in words (CLAUDE.md, "Blocked counts: three
   different units"): /monitor/leads counts LEAD ROWS, one per form session;
   the Blocked tab's people figure comes from /monitor/metrics and is only
   shown where it counts the same population.
   ============================================================================ */
(function (G) {
  var U = G.ui, L = G.L, esc = G.esc, fmt = G.fmt;
  var YES = '<span class="badge b-good">Yes</span>', NO = '<span class="badge b-neu">No</span>';

  /* ── The lead panel ─────────────────────────────────────────────── */

  /* One change log per SESSION, fetched once on first open and kept across
     repaints and refreshes: a repaint re-opens a row without a click (G.paint),
     so the log must come from here rather than from a one-shot fetch that the
     next repaint throws away. Three states, never two: "loading", "ok" with
     rows (possibly none), and "error" -- an unreadable history is not a clean
     one, and it is retried on the next open.
     NO PROTOTYPE: the key is a session_id, which is whatever the browser sent
     /partial. On a plain {} a lead with session_id "constructor" found
     Object's own constructor here, read it as a cached log, and threw -- and
     All leads never painted again while that row was on page 1. */
  var changes = Object.create(null);
  function link(v) { var h = G.href(v); return h ? '<a href="' + esc(h) + '" target="_blank" rel="noopener noreferrer">' + esc(v) + '</a>' : esc(v); }
  function group(title, fields) {
    var f = fields.filter(function (x) { return x && x[1] !== null && x[1] !== undefined && x[1] !== ''; });
    if (!f.length) return '';
    return '<div class="pgrp"><h3 class="pgh">' + title + '</h3>' + U.kv(f) + '</div>';
  }
  function mono(v) { return v ? ['', '<code>' + esc(v) + '</code>', 'html'] : null; }
  function kvm(label, v) { var m = mono(v); if (m) m[0] = label; return m; }
  function kvl(label, v) { return v ? [label, link(v), 'html'] : null; }
  function when(v) { return v ? G.et(v) : null; }

  /* THE PARTNER BOX: name -> email -> raw key, the one display chain Slack,
     the dashboard and hear_about_us share (CLAUDE.md). Reasons are words; the
     stored slug rides in a tooltip for whoever debugs it. */
  function reasonHtml(r, cls) { return '<span class="' + cls + '" title="' + esc(r) + '">' + esc(L.psReason(r)) + '</span>'; }
  function partnerBox(l) {
    if (!l.ps_partner_key) return '';
    var who = l.ps_partner_name ? esc(l.ps_partner_name)
      : (l.ps_partner_email ? esc(l.ps_partner_email) + ' <span class="na">(name not resolved)</span>' : '<code>' + esc(l.ps_partner_key) + '</code> <span class="na">(partner not resolved)</span>');
    /* what the visitor CAME IN SAYING: hear_about_us is overwritten with
       "Partner - X", so the raw answer is the only record of their own words */
    var said = l.hear_about_us_raw ? esc(l.hear_about_us_raw)
      : (l.hear_about_us && String(l.hear_about_us).indexOf('Partner - ') !== 0 ? esc(l.hear_about_us) : '<span class="na">nothing recorded</span>');
    var sent = l.ps_signup_sent_at ? esc(G.et(l.ps_signup_sent_at))
      : '<span class="na">not sent</span>' + (l.ps_signup_fail_reason ? ' ' + reasonHtml(l.ps_signup_fail_reason, 'bad-t') : (l.ps_signup_skipped_reason ? ' — ' + reasonHtml(l.ps_signup_skipped_reason, 'na') : ''));
    var rows = [['Partner', who, 'html'], ['Partner email', l.ps_partner_email], ['Came in saying', said, 'html'],
      l.ps_partner_name ? kvm('Partner key', l.ps_partner_key) : null, ['Clicked', when(l.ps_click_at)], kvm('Customer key', l.ps_customer_key),
      /* SENT and VERIFIED are different facts: sent only means PartnerStack
         answered 200 to an empty-bodied endpoint */
      ['Conversion sent', sent, 'html'],
      ['Conversion verified', l.ps_signup_verified_at ? esc(G.et(l.ps_signup_verified_at)) : (l.ps_signup_sent_at ? '<span class="na">awaiting read-back</span>' : null), 'html'],
      ['Qualified sent', l.ps_qualified_sent_at ? esc(G.et(l.ps_qualified_sent_at)) : (l.ps_qualify_fail_reason ? '<span class="bad-t">failed: </span>' + reasonHtml(l.ps_qualify_fail_reason, 'bad-t') : null), 'html']];
    var hist = l.ps_click_history; if (typeof hist === 'string') { try { hist = JSON.parse(hist); } catch (e) { hist = null; } }
    var h = '<div class="pbox"><h3 class="pgh">Partner</h3>' + U.kv(rows.filter(function (r) { return r && r[1]; }));
    if (hist && hist.length) {
      /* the LAST click wins attribution; the history answers "why A and not B" */
      h += '<div class="pgh sub">Click history (' + hist.length + ', oldest first — the last click wins)</div>' + U.grid([
        { label: 'Click', html: function (c) { return c._won ? '<span class="badge b-good">won</span>' : '<span class="na">earlier</span>'; } },
        { label: 'When (ET)', cls: 'day', get: function (c) { return c.at ? G.et(c.at) : '—'; } },
        { label: 'Partner key', html: function (c) { return '<code>' + esc(c.pk || '—') + '</code>'; } },
        { label: 'Click id', html: function (c) { return '<code>' + esc(c.xid || '—') + '</code>'; } },
      ], hist.map(function (c, i) { return Object.assign({ _won: i === hist.length - 1 }, c); }));
    }
    return h + '</div>';
  }

  function webCheck(l) {
    var r = l.website_check_reason;
    if (l.website_check_failed) return ['Website check', '<span class="bad-t">Failed:</span> ' + esc(L.website(r) || 'no reason recorded'), 'html'];
    if (r === 'social_profile_url') return ['Website check', 'Gave a social profile instead of a company website'];
    /* the four verdicts that mean "nothing to say" -- as the classic panel */
    if (r && ['content_clean', 'resolved', 'ok', 'test_email_skipped'].indexOf(r) === -1) return ['Website check', L.website(r)];
    return null;
  }
  function attempts(l) {
    var p = Number(l.prior_attempts) || 0, d = Number(l.prior_disqualified) || 0;   /* bigint COUNTs arrive as strings */
    return p > 0 ? 'Attempt ' + (p + 1) + ' — ' + p + ' earlier' + (d > 0 ? ', ' + d + ' of them disqualified' : '') : null;
  }
  function leadPanel(l, key) {
    /* WHY THIS ROW IS MARKED, in words, before anything else. The classic
       kept this in hover titles only, which a phone never shows. */
    var why = [];
    if (l.non_icp_blocked) { why.push(['Blocked by', L.sourceShort(l.non_icp_source) + ' — ' + L.sourceWhy(l.non_icp_source)]); why.push(kvm('Matched domain', l.non_icp_reason)); }
    if (l.meta_withheld_reason) why.push(['Meta conversion', L.metaWhy(l.meta_withheld_reason)]);
    if (l.is_internal) why.push(['Ours', 'One of our own test submissions, or sent from the staging site. Counted in every total like everything else.']);
    var loc = [l.enriched_city, l.enriched_state, l.enriched_country].filter(Boolean).join(', ');
    return group('Why this lead is marked', why) + partnerBox(l) +
      /* THE VISITOR'S location, from their IP -- a different fact from
         Apollo's Person location below, so a separate group, rendered first */
      group('Visitor — from their IP address', [['Location', [l.ip_city, l.ip_region, l.ip_country].filter(Boolean).join(', ')], ['Timezone', l.ip_timezone],
        ['Network', l.ip_isp ? l.ip_isp + (l.ip_org_domain ? ' (' + l.ip_org_domain + ')' : '') : ''], kvm('IP address', l.ip_address)]) +
      group('Form &amp; enrichment', [['Title', l.enriched_title], ['Seniority', l.enriched_seniority], ['Department', l.enriched_departments], ['Email status (Apollo)', l.enriched_email_status],
        ['Company', l.company || l.e_company], ['Sells to (as stored)', /^B2B \(clarified from /.test(l.sell_to || '') ? l.sell_to : null], ['Company size', l.enriched_company_size], ['Industry', l.enriched_industry], ['Founded', l.enriched_founded_year],
        ['Annual revenue', l.enriched_annual_revenue], ['Total funding', l.enriched_total_funding], ['Funding stage', l.enriched_funding_stage], ['Funding events', l.enriched_funding_events],
        ['Alexa rank', l.enriched_alexa_ranking], ['Keywords', l.enriched_keywords],
        /* Apollo's PERSON record (apolloEnrichmentFields reads person.city);
           the company's own address is Company HQ */
        ['Person location (Apollo)', loc], ['Company HQ (Apollo)', l.enriched_org_hq],
        kvl('LinkedIn', l.enriched_linkedin), ['Phone', l.e_phone || l.phone], kvl('Website', l.website), webCheck(l),
        ['Nothing verified', l.unverifiable_pair ? 'Catch-all email and an unreachable website: nothing confirmed this lead.' : null],
        ['Email check', l.elv_status], ['Attempts', attempts(l)], ['Heard about us', l.hear_about_us],
        /* product is the calendar they got; product_interest is what they
           ticked -- two columns because they answer two questions */
        ['Product (their calendar)', l.product], ['Asked for (on /demo)', l.product_interest], ['About their business', l.about_business]]) +
      group('Journey &amp; attribution', [['UTM source', l.utm_source], ['UTM medium', l.utm_medium], ['UTM campaign', l.utm_campaign], ['UTM term', l.utm_term],
        ['Referrer', l.referrer], ['Prefill', l.prefill_source], kvl('Landing page', l.landing_page), kvl('Previous page', l.previous_page), kvl('Form page', l.page_url),
        ['Submitted', when(l.submitted_at)], ['Booked at', when(l.booked_at)], ['Meeting', when(l.start_time)], ['Email sent', l.loops_sent ? 'Yes' : 'No']]) +
      group('Technical', [['Meta fbc', l.fbc], ['Meta fbp', l.fbp], kvm('Session ID', l.session_id), ['Enriched at', when(l.enriched_at)]]) +
      '<div class="pgrp lchg" id="lc-' + esc(key) + '">' + changesHtml(l.session_id) + '</div>';
  }

  function changesHtml(sid) {
    var c = changes[sid], h = '<h3 class="pgh">What changed on this lead</h3>';
    if (!c || c.state === 'loading') return h + '<div class="lnote">Reading the change history…</div>';
    if (c.state === 'error') return h + '<div class="lnote warn">Change log unavailable — this is not the same as no changes. Close and reopen the row to try again.</div>';
    if (!c.rows.length) return h + '<div class="lnote">No identity fields changed on this lead.</div>';
    return h + U.grid([
      { label: 'When (ET)', cls: 'day', get: function (x) { return G.et(x.changed_at); } },
      { label: 'Field', html: function (x) { return '<b>' + esc(x.field) + '</b>'; } },
      { label: 'Change', html: function (x) { return esc(x.old_value || '—') + ' → ' + esc(x.new_value || '—') + (x.booking_uid_present ? ' <span class="badge b-neu">after booking</span>' : ''); } },
      { label: 'Who', html: function (x) { var w = L.changeWho(x.attribution); return w.badge ? '<span class="badge b-neu">' + esc(w.t) + '</span>' : '<span class="na">' + esc(w.t) + '</span>'; } },
      { label: 'Where', html: function (x) { var wh = L.changeWhere(x); return esc(x.source_route || '') + (wh ? '<div class="na">' + esc(wh) + '</div>' : ''); } },
    ], c.rows, { region: 'What changed on this lead' }) + (c.rows.length >= 200 ? '<div class="lnote">Showing the first 200 changes; there may be more.</div>' : '');
  }
  /* The RAW session_id addresses the lead; the row key addresses the DOM.
     Never swap them (the classic incident). */
  function ensureChanges(sid, key) {
    if (!sid) return;
    var c = changes[sid];
    if (c && (c.state === 'ok' || c.state === 'loading')) return;
    changes[sid] = { state: 'loading' }; paintChanges(sid, key);
    G.api('/monitor/lead-changes', { session_id: sid }, { timeout: 15000 })
      .then(function (d) { changes[sid] = d && d.ok ? { state: 'ok', rows: d.changes || [] } : { state: 'error' }; }, function () { changes[sid] = { state: 'error' }; })
      .then(function () { paintChanges(sid, key); });
  }
  function paintChanges(sid, key) { var el = document.getElementById('lc-' + key); if (el) el.innerHTML = changesHtml(sid); }
  function afterPaint(root) {
    var xs = root && root.querySelectorAll ? root.querySelectorAll('[data-x][aria-expanded="true"][data-sid]') : [];
    for (var i = 0; i < xs.length; i++) ensureChanges(xs[i].getAttribute('data-sid'), xs[i].getAttribute('data-x'));
  }

  /* ── Rows ──────────────────────────────────────────────────────── */
  function host(u) { if (!u) return ''; try { return new URL(u).hostname.replace(/^www\./, ''); } catch (e) { return ''; } }
  /* Source: the ad click, else WHERE they came from. The classic printed
     "referral" for any referrer, google.com included. A referrer that is not
     a URL (the form stores the word "direct") is shown as it is. */
  function source(l) {
    if (l.utm_source) return l.utm_source + (l.utm_medium ? ' / ' + l.utm_medium : '');
    var h = host(l.referrer); return h ? 'from ' + h : (l.referrer ? String(l.referrer).slice(0, 40) : '—');
  }
  function marks(l, ns) {
    var b = [];
    if (l.is_internal) b.push('<span class="badge b-neu" title="One of our own test submissions, or sent from the staging site. Counted in every total — the Our own tests filter takes them out of a number you are about to quote.">ours</span>');
    if (l.non_icp_blocked) {
      /* On Blocked every row is blocked, so the chip says WHICH CHECK did it */
      b.push(ns === 'blk' ? '<span class="badge b-bad" title="' + esc(L.sourceWhy(l.non_icp_source)) + '">' + esc(L.sourceShort(l.non_icp_source)) + '</span>'
        : '<span class="badge b-bad" title="Blocked — not our market (' + esc(l.non_icp_reason || '') + '). Blocked by: ' + esc(L.sourceShort(l.non_icp_source)) + '. Still counted in every total.">blocked</span>');
    }
    if (l.website_check_failed) b.push('<span class="badge b-bad" title="' + esc(L.website(l.website_check_reason) || 'Website check failed') + '">website failed</span>');
    else if (l.website_check_reason === 'social_profile_url') b.push('<span class="badge b-neu" title="Gave a social profile instead of a company website">social profile</span>');
    /* THE META CHIP, visible text, not a glyph. Not repeated on Blocked when
       the reason IS blocked, on either tab: the "blocked" chip beside it
       already says so, and the panel spells out what that withheld. */
    if (l.meta_withheld_reason && !(l.non_icp_blocked && l.meta_withheld_reason === 'blocked')) b.push('<span class="badge b-warn" title="' + esc(L.metaWhy(l.meta_withheld_reason)) + '">Meta: ' + esc(L.metaShort(l.meta_withheld_reason)) + '</span>');
    return b.length ? '<span class="marks">' + b.join('') + '</span>' : '';
  }
  /* "B2B (clarified from B2C)" is ONE fact in four wrapped lines. Shown as
     B2B with a "clarified" chip whose tooltip keeps the stored text. */
  function sellTo(l) {
    var v = l.sell_to; if (!v) return '—';
    return /^B2B \(clarified from /.test(v) ? 'B2B <span class="badge b-neu" title="' + esc(v) + '">clarified</span>' : esc(v);
  }
  /* A compact ET time for a table cell: the year only when it is not this year */
  var THIS_YEAR = G.etDay(new Date()).slice(0, 4);
  function created(ts) {
    var d = G.okDate(ts); if (!d) return ts ? String(ts) : '—';
    var y = G.etDay(d).slice(0, 4);
    return new Intl.DateTimeFormat('en-US', { timeZone: G.TZ, month: 'short', day: 'numeric', year: y === THIS_YEAR ? undefined : 'numeric', hour: 'numeric', minute: '2-digit' }).format(d);
  }
  function cols(ns, sortable) {
    var s = function (k) { return sortable ? k : null; };
    return [
      { label: 'Email', sort: s('email'), cls: 'ecell', html: function (l) { return '<span class="em">' + esc(l.email || '—') + '</span>' + marks(l, ns); } },
      { label: 'Name', sort: s('name'), get: function (l) { return [l.first_name, l.last_name].filter(Boolean).join(' ') || '—'; } },
      { label: 'Company', sort: s('company'), get: function (l) { return l.company || '—'; } },
      { label: 'Sells to', sort: s('sell_to'), html: sellTo },
      { label: 'Product', opt: 1, get: function (l) { return l.product || '—'; } },
      { label: 'Stage', html: L.stageBadge },
      { label: 'Enriched', opt: 1, html: function (l) { return (l.enriched_title || l.enriched_company_size || l.e_company) ? YES : NO; } },
      { label: 'Created (ET)', sort: s('created_at'), cls: 'm', get: function (l) { return created(l.created_at); } },
      { label: 'Source', cls: 'mw', opt: 1, get: source },
    ];
  }
  function table(rows, ns, sort, emptyTitle, emptyBody) {
    var o = { ns: ns, rows: rows, key: function (l) { return l.session_id; }, rowName: function (l) { return l.email || 'this lead'; },
      xattr: function (l) { return 'data-sid="' + esc(l.session_id) + '"'; },
      emptyTitle: emptyTitle, emptyBody: emptyBody, cols: cols(ns, !!sort), sort: sort };
    o.detail = function (l) { return leadPanel(l, U.keyOf(o, l)); };
    return U.rtable(o);
  }
  G.leadRows = { table: table, leadPanel: leadPanel, marks: marks, source: source, _changes: changes };

  /* ── All leads ─────────────────────────────────────────────────── */

  /* Every filter, its server parameter (the SAME name in the link), and the
     value that means "not filtering". A value equal to its default is not
     sent: for sellTo, utmSource and partner the server reads ANY present
     value as a match, so sending "all" would return nothing. */
  var DEF = { search: '', stage: 'all', nonicp: '', internal: '', meta: 'all', sellTo: 'all', product: 'all', interest: 'all', utmSource: 'all',
              enrichment: 'all', partner: 'all', websiteCheck: 'all', repeatAttempts: 'all', hearAbout: '', dateFrom: '', dateTo: '' };
  var STAGES = [['all', 'All stages'], ['booked', 'Booked'], ['completed', 'Completed, not booked'], ['step1', 'Left on step 2'], ['disqualified', 'Disqualified, not booked']];
  var SELECTS = [
    ['nonicp', 'Blocked leads', [['', 'Included, and marked'], ['only', 'Only blocked'], ['exclude', 'Hide blocked']]],
    ['internal', 'Our own tests', [['', 'Included, and marked'], ['exclude', 'Hide our own tests'], ['only', 'Only our own tests']]],
    ['meta', 'Meta conversion', [['all', 'Any'], ['withheld', 'Withheld, any reason'], ['blocked', 'Withheld: blocked'], ['model', 'Withheld: model flagged the industry'],
      ['internal', 'Withheld: ours'], ['website', 'Withheld: website not verified'], ['disqualified', 'Withheld: disqualified'], ['sent', 'Sent']]],
    ['sellTo', 'Sells to', [['all', 'Any'], ['B2B', 'B2B'], ['B2B (clarified from B2C)', 'B2B (clarified from B2C)'], ['B2B (clarified from Mixed)', 'B2B (clarified from Mixed)'],
      ['B2C', 'B2C'], ['Mixed', 'Mixed'], ['__clarified', 'Clarified, any']]],
    ['product', 'Product (their calendar)', [['all', 'Any'], ['aeo', 'AEO'], ['crm', 'CRM'], ['__none', 'Untagged']]],
    ['interest', 'Asked for (on /demo)', [['all', 'Any'], ['aeo', 'Lead Gen only'], ['crm', 'AI-CRM only'], ['aeo,crm', 'Both'], ['__none', 'Never asked']]],
    ['utmSource', 'Ad source', null],
    ['enrichment', 'Enriched by Apollo', [['all', 'Any'], ['yes', 'Enriched'], ['no', 'Not enriched']]],
    ['partner', 'Partner', null],
    ['websiteCheck', 'Website check', [['all', 'Any'], ['failed', 'Failed'], ['passed', 'Not failed'], ['social', 'Social profile'], ['unverified', 'Not verified, any reason']]],
    ['repeatAttempts', 'Attempts', [['all', 'Any'], ['yes', 'Repeat attempts only'], ['no', 'First attempts only']]],
  ];
  function val(k) { var v = G.S.q[k]; return v === undefined || v === null ? DEF[k] : v; }
  function setQ(k, v) { if (v === DEF[k] || v === '' || v === undefined) delete G.S.q[k]; else G.S.q[k] = v; }
  function today() { return G.etDay(new Date()); }
  function minusDays(iso, n) { var a = iso.split('-'); return new Date(Date.UTC(+a[0], +a[1] - 1, +a[2], 12) - n * 864e5).toISOString().slice(0, 10); }
  /* The preset is DERIVED from the dates, never stored beside them: the
     classic counted one preset as three active filters, and "Any date" did
     not clear the dates it had set. */
  function preset() {
    var f = val('dateFrom'), t = val('dateTo'), d = today();
    if (!f && !t) return '';
    /* "Custom" is the one preset the dates cannot say: seeded with the last
       seven days it read back as "Last 7 days", so choosing it snapped the
       select back and the From/To boxes never appeared. It is remembered. */
    if (G.S.q.dp === 'custom') return 'custom';
    if (t === d && f === d) return 'today';
    if (t === d && f === minusDays(d, 6)) return '7d';
    if (t === d && f === minusDays(d, 29)) return '30d';
    return 'custom';
  }
  function active() {
    var n = 0; Object.keys(DEF).forEach(function (k) { var v = val(k); if (k === 'search' || k === 'hearAbout') v = String(v).trim(); if (k !== 'dateFrom' && k !== 'dateTo' && v !== DEF[k]) n++; });
    return n + (val('dateFrom') || val('dateTo') ? 1 : 0);
  }
  /* The sort comes from the link, so it is held to the columns the table
     offers: the server looks it up in a plain object, and sort=constructor
     put Object's own source text into ORDER BY and answered 500. */
  var SORTS = 'email name company sell_to created_at'.split(' ');   /* a string, not an array: the icon check reads a three-string array as a nav entry */
  function sortKey() { return SORTS.indexOf(G.S.q.sort) >= 0 ? G.S.q.sort : 'created_at'; }
  function sortDir() { return G.S.q.dir === 'asc' ? 'asc' : 'desc'; }
  function params(extra) {
    var p = { stage: val('stage'), sort: sortKey(), dir: sortDir() };
    /* typed text is kept as typed (so a space between two words survives the
       repaint) and trimmed only on the way to the server, as the server does */
    Object.keys(DEF).forEach(function (k) { if (k === 'stage') return; var v = val(k); if (k === 'search' || k === 'hearAbout') v = String(v).trim(); if (v !== DEF[k]) p[k] = v; });
    return Object.assign(p, extra || {});
  }

  GW.TABS.leads = (function () {
    var data = null, err = null, root = null, seq = 0, busy = false, open = false, opts = null, optsErr = null;
    function page() { return Math.max(1, +G.S.q.page || 1); }
    function load() {
      var my = ++seq; busy = true;
      return G.api('/monitor/leads', params({ page: page() }), { timeout: 20000 })
        .then(function (d) { if (my !== seq) return; if (pastEnd(d)) return load(); data = d; err = null; }, function (e) { if (my === seq) err = e.message; })
        .then(function () { if (my === seq) busy = false; });
    }
    function loadOpts() {
      return G.api('/monitor/filter-options', {}, { timeout: 15000 }).then(function (d) { opts = d; optsErr = null; }, function (e) { optsErr = e.message; });
    }
    /* load BEFORE the first paint, so "updating…" shows over the old rows
       instead of the old count sitting under new filters */
    function activate(el) {
      root = el; var p = Promise.all([load(), opts ? null : loadOpts()]); render();
      G.every('leads', 300000, function () { load().then(render); });
      return p.then(render);
    }
    function deactivate() { G.stop('leads'); root = null; clearTimeout(tmr); }
    function sel(k, label, list) {
      var v = val(k);
      if (list.every(function (o) { return o[0] !== v; })) list = list.concat([[v, v]]);   /* a value from a link that is not in the list still shows as chosen */
      return '<label class="lfl"><span>' + esc(label) + '</span><select class="field" data-lf="' + k + '">' + list.map(function (o) {
        return '<option value="' + esc(o[0]) + '"' + (o[0] === v ? ' selected' : '') + '>' + esc(o[1]) + '</option>'; }).join('') + '</select></label>';
    }
    function lists(k) {
      if (k === 'utmSource') return [['all', 'Any']].concat(((opts && opts.utmSource) || []).map(function (s) { return [s, s]; }));
      if (k === 'partner') return [['all', 'Any'], ['__any', 'Any partner'], ['__none', 'No partner']].concat(((opts && opts.partners) || []).map(function (p) { return [p.key, p.name || p.email || p.key]; }));
      return null;
    }
    /* a direction the header set that the list above does not name */
    function sortExtra() { var v = sortKey() + ':' + sortDir(), known = ['created_at:desc', 'created_at:asc', 'email:asc', 'name:asc', 'company:asc', 'sell_to:asc'];
      return known.indexOf(v) >= 0 ? [] : [[v, ({ email: 'Email', name: 'Name', company: 'Company', sell_to: 'Sells to' })[sortKey()] + (sortDir() === 'asc' ? ' A–Z' : ' Z–A')]]; }
    function controls() {
      var n = active(), pr = preset();
      var top = '<div class="lf-top"><input class="field search" type="search" data-lf="search" placeholder="Search email, name, company, website, phone" aria-label="Search leads by email, first or last name, company, website, or phone number" value="' + esc(val('search')) + '">' +
        '<select class="field" data-lf="stage" aria-label="Stage">' + STAGES.map(function (o) { return '<option value="' + o[0] + '"' + (o[0] === val('stage') ? ' selected' : '') + '>' + o[1] + '</option>'; }).join('') + '</select>' +
        '<button class="btn" data-lmore aria-expanded="' + open + '" aria-controls="lf-panel">' + G.ic('sliders-horizontal') + 'Filters' + (n ? ' <span class="badge b-neu">' + n + '</span>' : '') + '</button>' +
        /* the column headers ARE the sort controls, and a card view has no
           header row -- so below 900 the same choice is a select */
        '<select class="field cardsort" data-lsortsel aria-label="Sort by">' + [['created_at:desc', 'Newest first'], ['created_at:asc', 'Oldest first'], ['email:asc', 'Email A–Z'], ['name:asc', 'Name A–Z'],
          ['company:asc', 'Company A–Z'], ['sell_to:asc', 'Sells to A–Z']].concat(sortExtra()).map(function (o) { return '<option value="' + o[0] + '"' + (o[0] === sortKey() + ':' + sortDir() ? ' selected' : '') + '>' + o[1] + '</option>'; }).join('') + '</select>' +
        (n ? '<button class="btn" data-lclear>' + G.ic('x') + 'Clear</button>' : '') +
        '<button class="btn" data-lcsv>' + G.ic('download-simple') + 'Export CSV</button></div>';
      var panel = '<div class="lf-panel" id="lf-panel"' + (open ? '' : ' hidden') + '>' +
        SELECTS.map(function (s) { return sel(s[0], s[1], s[2] || lists(s[0])); }).join('') +
        '<label class="lfl"><span>Heard about us</span><input class="field" type="search" data-lf="hearAbout" list="lf-hear" value="' + esc(val('hearAbout')) + '" placeholder="Any"></label>' +
        '<datalist id="lf-hear">' + ((opts && opts.hearAbout) || []).map(function (h) { return '<option value="' + esc(h) + '">'; }).join('') + '</datalist>' +
        '<label class="lfl"><span>Created (ET days)</span><select class="field" data-lf="preset">' + [['', 'Any date'], ['today', 'Today'], ['7d', 'Last 7 days'], ['30d', 'Last 30 days'], ['custom', 'Custom']].map(function (o) {
          return '<option value="' + o[0] + '"' + (o[0] === pr ? ' selected' : '') + '>' + o[1] + '</option>'; }).join('') + '</select></label>' +
        (pr === 'custom' ? '<span class="lfl dates"><label><span>From</span><input class="field" type="date" data-lf="dateFrom" value="' + esc(val('dateFrom')) + '"></label>' +
          '<label><span>To</span><input class="field" type="date" data-lf="dateTo" value="' + esc(val('dateTo')) + '"></label></span>' : '') +
        (optsErr ? '<p class="lnote warn">The ad-source and partner lists could not be read, so only the fixed choices are offered.</p>' : '') + '</div>';
      return top + panel;
    }
    function render() {
      if (!root || (G.current && G.current() !== 'leads')) return;
      var d = data;
      var count = d ? '<b>' + fmt(d.total) + '</b> ' + G.plural(d.total, 'lead') + ' found' + (d.pages > 1 ? ' · page ' + fmt(d.page) + ' of ' + fmt(d.pages) : '') : '';
      var sk = sortKey(), sd = sortDir();
      var head = '<section class="ph"><div class="ph-top"><h1 class="title" tabindex="-1">All leads</h1><span class="readat">' + count + (busy && d ? ' · updating…' : '') + '</span></div>' +
        '<p class="lede">One row per form session that got through step 1 — a person who tried twice is two rows. Blocked leads and our own tests are counted and marked, never hidden.</p>' +
        '<div class="controls lf">' + controls() + '</div></section>';
      var body;
      if (!d && err) body = '<section class="card panel">' + U.unavailable('All leads', err) + '</section>';
      else if (!d) body = U.loading(5);
      else body = U.drawn('All leads', function () {
        var n = active();
        return (err ? '<div class="readat"><span class="badge b-warn">Last refresh failed (' + esc(String(err).slice(0, 40)) + ') — showing the previous read</span></div>' : '') +
          '<section class="card">' + table(d.leads || [], 'ld', { key: sk, dir: sd, attr: 'data-lsort' },
            n ? 'No leads match ' + n + ' active ' + G.plural(n, 'filter') : 'No leads yet', n ? 'Clear a filter, or clear them all.' : '') +
          U.pager(d.page, d.pages, 'data-pg') + '</section>';
      });
      G.paint(root, head + body);
      afterPaint(root);
    }
    /* Partners' drill-down: "show me this partner's leads" */
    function openFiltered(q) { G.show('leads', true, q); }
    function said() { var d = data; return d ? fmt(d.total) + ' ' + G.plural(d.total, 'lead') + ' found' + (d.pages > 1 ? ', page ' + fmt(d.page) + ' of ' + fmt(d.pages) : '') : ''; }
    return { title: 'All leads', activate: activate, deactivate: deactivate, render: render, load: load, openFiltered: openFiltered, said: said,
             toggle: function () { open = !open; render(); }, _set: function (d, o) { data = d; if (o) opts = o; }, _params: params };
  })();

  /* ── Blocked ───────────────────────────────────────────────────── */
  GW.TABS.blocked = (function () {
    var data = null, err = null, tot = null, people = null, root = null, seq = 0, busy = false, dm = '';
    function mode() { var m = G.S.q.internal || ''; return m === 'only' || m === 'exclude' ? m : ''; }
    function page() { return Math.max(1, +G.S.q.page || 1); }
    function load() {
      var my = ++seq, m = mode(), base = { nonicp: 'only', stage: 'all', sort: 'created_at', dir: 'desc' };
      var a = G.api('/monitor/leads', Object.assign({ page: page(), internal: m || undefined }, base), { timeout: 20000 });
      /* "excluding our own tests" -- a LEADS figure, like the one beside it */
      var b = G.api('/monitor/leads', Object.assign({ page: 1, internal: 'exclude' }, base), { timeout: 20000 }).then(function (d) { return d.total; }, function () { return null; });
      /* PEOPLE, from the all-time counter that counts this same population
         (non_icp_blocked, deduped by address). Only asked for with no filter,
         because that is the only population it describes. */
      var c = m ? Promise.resolve(null) : G.api('/monitor/metrics', {}, { timeout: 20000 }).then(function (d) { return d.peopleNonIcp; }, function () { return null; });
      busy = true;
      /* dm is the mode THIS payload was fetched for: the header words its
         number from the payload, never from the select, which moves first */
      return Promise.all([a, b, c]).then(function (r) { if (my !== seq) return; if (pastEnd(r[0])) return load(); data = r[0]; dm = m; err = null; tot = r[1]; people = r[2]; }, function (e) { if (my === seq) err = e.message; })
        .then(function () { if (my === seq) busy = false; });
    }
    function activate(el) { root = el; var p = load(); render(); G.every('blocked', 300000, function () { load().then(render); }); return p.then(render); }
    function deactivate() { G.stop('blocked'); root = null; }
    function counts(d) {
      var m = dm, a = d.total;
      if (m === 'only') return '<b>' + fmt(a) + '</b> of our own test ' + G.plural(a, 'lead') + ' blocked';
      if (m === 'exclude') return '<b>' + fmt(a) + '</b> ' + G.plural(a, 'lead') + ' blocked, not counting our own tests';
      return '<b>' + fmt(a) + '</b> ' + G.plural(a, 'lead') + ' blocked' + (people !== null && people !== undefined ? ' · <b>' + fmt(people) + '</b> ' + G.plural(people, 'person', 'people') : '') +
        ' · ' + (tot === null ? 'could not separate our own tests' : '<b>' + fmt(tot) + '</b> ' + G.plural(tot, 'lead') + ' excluding our own tests');
    }
    function render() {
      if (!root || (G.current && G.current() !== 'blocked')) return;
      var d = data, m = mode();
      var head = '<section class="ph"><div class="ph-top"><h1 class="title" tabindex="-1">Blocked</h1><span class="readat">' + (d ? counts(d) : '') + (busy && d ? ' · updating…' : '') + '</span></div>' +
        /* TRUE FOR EVERY ROW: a late model verdict marks a lead AFTER it has
           booked and never cancels the meeting, so "turned away before the
           calendar" was false for those. */
        '<p class="lede">Leads we stopped as real estate or insurance — by the brand-domain list or by the AI check. Most were stopped before the calendar; a few were marked only after they had booked, when the AI check answered late, and those kept their meeting. Every one is still in All leads and in every total: marked, never removed.</p>' +
        '<div class="controls"><label class="lfl inline"><span>Our own tests</span><select class="field" data-blk aria-label="Our own tests">' +
        [['', 'Included, and marked'], ['exclude', 'Hide our own tests'], ['only', 'Only our own tests']].map(function (o) { return '<option value="' + o[0] + '"' + (o[0] === m ? ' selected' : '') + '>' + o[1] + '</option>'; }).join('') +
        '</select></label></div></section>';
      var body;
      if (!d && err) body = '<section class="card panel">' + U.unavailable('Blocked leads', err) + '</section>';
      else if (!d) body = U.loading(4);
      else body = U.drawn('Blocked leads', function () { return (err ? '<div class="readat"><span class="badge b-warn">Last refresh failed (' + esc(String(err).slice(0, 40)) + ') — showing the previous read</span></div>' : '') +
        '<section class="card">' + table(d.leads || [], 'blk', null,
          dm === 'only' ? 'None of our own tests are blocked' : dm === 'exclude' ? 'Nothing blocked apart from our own tests' : 'Nothing blocked',
          dm ? '' : 'Either the block is switched off or nobody has matched yet.') + U.pager(d.page, d.pages, 'data-pg') + '</section>'; }) +
        '<p class="foot"><span>The chip on each row says which check blocked it. A brand-list block comes from NON_ICP_DOMAINS and stops with NON_ICP_BLOCK=false; an AI-check block stops with NON_ICP_LLM_BLOCK=false. Either takes effect on Railway without a deploy.</span></p>';
      G.paint(root, head + body);
      afterPaint(root);
    }
    function said() { var d = data; return d ? counts(d).replace(/<[^>]*>/g, '') : ''; }
    return { title: 'Blocked', activate: activate, deactivate: deactivate, render: render, load: load, said: said, _set: function (d, t, p) { data = d; tot = t; people = p; dm = mode(); } };
  })();

  /* ── Events: one delegated set for both tabs ────────────────────── */
  var tmr = null;
  function tab() { return G.current && G.current(); }
  /* A PAGE PAST THE END (a link, or a filtered set that shrank under the
     five-minute refresh) came back empty beside a non-zero total and read
     "No leads yet". It is moved to the last page and read again. */
  function pastEnd(d) {
    if (!d || (d.leads || []).length || !(d.total > 0) || !(d.pages >= 1) || !(+G.S.q.page > d.pages)) return false;
    if (d.pages > 1) G.S.q.page = String(d.pages); else delete G.S.q.page;
    G.writeHash(); return true;
  }
  /* load() FIRST, so the paint that follows shows "updating…"; then say what
     the reader asked for came back (the live region, never the timer) */
  function reload(keepPage) {
    var cur = tab(); if (cur !== 'leads' && cur !== 'blocked') return;   /* the search debounce can fire after a tab change */
    if (!keepPage) delete G.S.q.page;
    G.writeHash(); var T = G.TABS[cur], p = T.load(); T.render();
    return p.then(function () { if (tab() !== cur) return; T.render(); if (T.said) G.announce(T.said()); });
  }
  function setSort(k, d) {
    G.S.q.sort = SORTS.indexOf(k) >= 0 ? k : 'created_at'; G.S.q.dir = d === 'asc' ? 'asc' : 'desc';
    if (G.S.q.sort === 'created_at' && G.S.q.dir === 'desc') { delete G.S.q.sort; delete G.S.q.dir; }
  }
  if (typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('click', function (e) {
      var t = e.target && e.target.closest ? e.target : null; if (!t) return;
      var cur = tab(); if (cur !== 'leads' && cur !== 'blocked') return;
      /* the row opened (ui.js toggled it first): fetch its change log */
      var x = t.closest('[data-sid]'); if (x) { if (x.getAttribute('aria-expanded') === 'true') ensureChanges(x.getAttribute('data-sid'), x.getAttribute('data-x')); return; }
      var pg = t.closest('[data-pg]') || t.closest('[data-pg-step]'); if (pg) { G.S.q.page = pg.getAttribute('data-pg') || pg.getAttribute('data-pg-step'); if (G.S.q.page === '1') delete G.S.q.page; reload(true); return; }
      if (cur !== 'leads') return;
      var s = t.closest('[data-lsort]'); if (s) { var k = s.getAttribute('data-lsort'), was = G.S.q.sort || 'created_at';
        G.S.q.dir = k === was ? ((G.S.q.dir || 'desc') === 'desc' ? 'asc' : 'desc') : (k === 'created_at' ? 'desc' : 'asc');
        setSort(k, G.S.q.dir); reload(); return; }
      if (t.closest('[data-lmore]')) { G.TABS.leads.toggle(); return; }
      /* Clear removes itself, so focus goes to the search box, not the body */
      if (t.closest('[data-lclear]')) { G.S.q = {}; reload(); var f = document.querySelector('#view [data-lf="search"]'); if (f && f.focus) f.focus(); return; }
      if (t.closest('[data-lcsv]')) { var p = params({ format: 'csv' }); window.location.href = G.url('/monitor/leads', p); return; }
    });
    document.addEventListener('change', function (e) {
      var el = e.target; if (!el || !el.getAttribute) return;
      if (el.hasAttribute('data-blk') && tab() === 'blocked') { setQ('internal', el.value); reload(); return; }
      if (el.hasAttribute('data-lsortsel') && tab() === 'leads') { var sv = String(el.value).split(':'); setSort(sv[0], sv[1]); reload(); return; }
      var k = el.getAttribute('data-lf'); if (!k || tab() !== 'leads' || k === 'search' || k === 'hearAbout') return;
      if (k === 'preset') {
        var v = el.value, d = today();
        if (v === 'custom') G.S.q.dp = 'custom'; else delete G.S.q.dp;
        if (v === '') { setQ('dateFrom', ''); setQ('dateTo', ''); }
        else if (v === 'today') { setQ('dateFrom', d); setQ('dateTo', d); }
        else if (v === '7d') { setQ('dateFrom', minusDays(d, 6)); setQ('dateTo', d); }
        else if (v === '30d') { setQ('dateFrom', minusDays(d, 29)); setQ('dateTo', d); }
        else { if (!val('dateFrom')) setQ('dateFrom', minusDays(d, 6)); if (!val('dateTo')) setQ('dateTo', d); }
      } else setQ(k, el.value);
      reload();
    });
    /* typed filters wait for a pause, then ask the server (the list pages in SQL) */
    document.addEventListener('input', function (e) {
      var el = e.target; if (!el || !el.getAttribute || tab() !== 'leads') return;
      var k = el.getAttribute('data-lf'); if (k !== 'search' && k !== 'hearAbout') return;
      setQ(k, el.value); clearTimeout(tmr); tmr = setTimeout(reload, 350);
    });
  }
})(GW);
