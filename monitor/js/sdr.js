/* ============================================================================
   SDR list -- everyone an SDR could still call, one row per PERSON: the
   newest qualifying attempt of each address that has no booking on ANY of
   its sessions (CLAUDE.md, "Bookings: two different questions", question 1
   -- no time comparison). B2B leads, plus CRM leads whatever they sell to;
   never a blocked or disqualified lead. All of that is decided by
   /monitor/sdr on the server, unchanged; this tab only shows it.

   THE SEARCH MATCHES WHAT THE EXPORT MATCHES. The table filters in the
   browser; the CSV export filters on the server. SDR_SEARCH_FIELDS below
   must equal the server's SDR_SEARCH_COLUMNS, and the term is trimmed and
   lower-cased exactly as the server does it -- the classic did not trim on
   screen but did on export, so " acme" showed nothing and exported every
   acme row. tests/test-monitor-next.js holds the lists together.
   ============================================================================ */
GW.TABS.sdr = (function (G) {
  var U = G.ui, L = G.L, esc = G.esc, fmt = G.fmt;
  var SDR_SEARCH_FIELDS = ['email', 'company', 'first_name', 'enriched_industry'];
  var PAGE = 50;
  var data = null, err = null, root = null, q = '', shown = PAGE, seq = 0;
  function load() {
    var my = ++seq;
    return G.api('/monitor/sdr', { _: Date.now() }, { timeout: 20000 })
      .then(function (d) { if (my === seq) { data = d; err = null; } }, function (e) { if (my === seq) err = e.message; });
  }
  function activate(el) { root = el; render(); G.every('sdr', 300000, function () { load().then(render); }); return load().then(render); }
  function deactivate() { G.stop('sdr'); root = null; }
  function term() { return String(q).trim().toLowerCase(); }
  function matches(l) { var t = term(); return !t || SDR_SEARCH_FIELDS.some(function (f) { return String(l[f] || '').toLowerCase().indexOf(t) >= 0; }); }
  function link(v, text) { var h = G.href(v); return h ? '<a href="' + esc(h) + '" target="_blank" rel="noopener noreferrer">' + esc(text || v) + '</a>' : (v ? esc(v) : '—'); }
  /* Two facts, one cell: the partner who referred them and what they came in
     saying. Only this field records the visitor's own words on a partner lead. */
  function source(l) {
    var b = [];
    if (l.ps_partner_name) b.push('<span class="badge b-neu">partner</span> ' + esc(l.ps_partner_name));
    if (l.hear_about_us_raw) b.push(esc(l.hear_about_us_raw));
    return b.length ? b.join('<br>') : '—';
  }
  function detail(l) {
    var loc = [l.enriched_city, l.enriched_country].filter(Boolean).join(', ');
    var ad = [l.utm_source, l.utm_medium].filter(Boolean).join(' / ');
    return U.kv([['Phone', l.phone], ['Sells to', l.sell_to], ['Title', l.enriched_title], ['Industry', l.enriched_industry], ['Company size', l.enriched_company_size],
      ['LinkedIn', l.enriched_linkedin ? link(l.enriched_linkedin) : null, 'html'], ['Heard about us (as Salesforce has it)', l.hear_about_us],
      ['Website', l.website ? link(l.website) : null, 'html'], ['Ad source', ad], ['Campaign', l.utm_campaign], ['Referrer', l.referrer],
      ['Landing page', l.landing_page ? link(l.landing_page) : null, 'html'], ['Seniority', l.enriched_seniority], ['Department', l.enriched_departments],
      ['Person location (Apollo)', loc], ['Annual revenue', l.enriched_annual_revenue], ['Total funding', l.enriched_total_funding], ['Funding stage', l.enriched_funding_stage],
      ['Submitted', l.submitted_at ? G.et(l.submitted_at) : null]]) || '<p class="lnote">Nothing more is recorded for this person.</p>';
  }
  function render() {
    if (!root || (G.current && G.current() !== 'sdr')) return;
    var d = data, rows = d ? (d.leads || []).filter(matches) : [], page = rows.slice(0, shown), more = rows.length - page.length, t = term();
    var count = d ? '<b>' + fmt(d.total) + '</b> ' + G.plural(d.total, 'person', 'people') + ' to call' + (t ? ' · <b>' + fmt(rows.length) + '</b> ' + G.plural(rows.length, 'matches', 'match') + ' the search' : '') : '';
    var head = '<section class="ph"><div class="ph-top"><h1 class="title" tabindex="-1">SDR list</h1><span class="readat">' + count + '</span></div>' +
      '<p class="lede">Everyone we could still call: B2B leads — and CRM leads, whatever they sell to — with no booking on any of their sessions. One row per person, showing their newest qualifying attempt. Blocked and disqualified leads are not on it.</p>' +
      (d ? '<div class="controls"><input class="field search" type="search" data-sdr-q placeholder="Search email, company, first name, industry" aria-label="Search the SDR list by email, company, first name or industry" value="' + esc(q) + '">' +
        '<button class="btn" data-sdr-csv>' + G.ic('download-simple') + 'Export CSV' + (t ? ' (these ' + fmt(rows.length) + ')' : '') + '</button></div>' : '') + '</section>';
    var body;
    if (!d && err) body = '<section class="card panel">' + U.unavailable('The SDR list', err) + '</section>';
    else if (!d) body = U.loading(5);
    else body = U.drawn('The SDR list', function () { return (err ? '<div class="readat"><span class="badge b-warn">Last refresh failed (' + esc(String(err).slice(0, 40)) + ') — showing the previous read</span></div>' : '') +
      '<section class="card">' + U.rtable({ ns: 'sdr', rows: page, key: function (l) { return String(l.email).toLowerCase(); }, rowName: function (l) { return l.email; },
        emptyTitle: t ? 'No one matches' : 'Nobody to call right now', emptyBody: t ? 'The search looks at email, company, first name and industry.' : 'Everyone qualifying has a booking.',
        cols: [
          { label: 'Email', cls: 'ecell', html: function (l) { return '<span class="em">' + esc(l.email || '—') + '</span>'; } },
          { label: 'Name', get: function (l) { return [l.first_name, l.last_name].filter(Boolean).join(' ') || '—'; } },
          { label: 'Company', get: function (l) { return l.company || '—'; } },
          { label: 'Source', html: source },
          { label: 'Title', opt: 1, get: function (l) { return l.enriched_title || '—'; } },
          { label: 'Industry', opt: 1, get: function (l) { return l.enriched_industry || '—'; } },
          /* booked and disqualified never reach this list, so the ladder is two rungs here */
          { label: 'Stage', html: L.stageBadge },
          /* the newest attempt that QUALIFIED -- a later B2C or blocked visit is not it */
          { label: 'Newest qualifying attempt (ET)', cls: 'm', get: function (l) { return G.et(l.created_at); } },
        ], detail: detail }) +
      (more > 0 ? '<div class="more"><span>Showing ' + fmt(page.length) + ' of ' + fmt(rows.length) + '</span><button class="btn" data-more="sdr">Show ' + fmt(Math.min(PAGE, more)) + ' more</button></div>' : '') + '</section>'; });
    G.paint(root, head + body);
  }
  var said = null;
  if (typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('input', function (e) {
      if (!(e.target && e.target.hasAttribute && e.target.hasAttribute('data-sdr-q'))) return;
      q = e.target.value; shown = PAGE; render();
      /* said once the typing pauses, not on every key */
      clearTimeout(said); said = setTimeout(function () { if (G.current() !== 'sdr' || !data) return; var n = (data.leads || []).filter(matches).length; G.announce(term() ? fmt(n) + ' ' + G.plural(n, 'person matches', 'people match') + ' the search' : fmt(data.total) + ' ' + G.plural(data.total, 'person', 'people') + ' to call'); }, 700);
    });
    document.addEventListener('click', function (e) {
      var t = e.target && e.target.closest ? e.target : null; if (!t || (G.current && G.current() !== 'sdr')) return;
      /* the last "Show more" removes itself: focus the first person it revealed */
      if (t.closest('[data-more="sdr"]')) { var from = shown; shown += PAGE; render();
        if (!root.querySelector('[data-more="sdr"]')) { var xs = root.querySelectorAll('[data-x^="sdr-"]'); if (xs[from] && xs[from].focus) xs[from].focus(); } return; }
      /* the export carries the SAME trimmed term the table is filtered by */
      if (t.closest('[data-sdr-csv]')) { window.location.href = G.url('/monitor/sdr', { format: 'csv', search: String(q).trim() }); }
    });
  }
  return { title: 'SDR list', activate: activate, deactivate: deactivate, render: render, load: load, _set: function (d) { data = d; }, _fields: SDR_SEARCH_FIELDS };
})(GW);
