/* ============================================================================
   Duplicates -- addresses that appear in more than one session. Ported, with
   one addition: OUR OWN test addresses are marked on the row (is_internal,
   decided server-side by the one clause every outbound guard uses). They are
   the top of this list; the old tab showed them with no marker at all.
   Marked, never excluded by default -- the filter is opt-in.
   ============================================================================ */
GW.TABS.dupes = (function (G) {
  var U = G.ui, esc = G.esc, fmt = G.fmt;
  /* PAGED AND SEARCHABLE. All 338 addresses rendered at once, and on a phone
     each is a seven-line card: a page about 151,000px tall, with everything
     past our own tests at the top effectively out of reach. */
  var PAGE = 50;
  var data = null, err = null, root = null, filter = 'all', q = '', shown = PAGE;
  function load() { return G.api('/monitor/duplicates', { _: Date.now() }, { timeout: 20000 }).then(function (d) { data = d; err = null; }).catch(function (e) { err = e.message; }); }
  function activate(el) { root = el; render(); G.every('dupes', 300000, function () { load().then(render); }); return load().then(render); }
  function deactivate() { G.stop('dupes'); root = null; }
  function stage(s) {
    if (s.booking_uid) return '<span class="badge b-good">Booked</span>';
    if (s.disqualified) return '<span class="badge b-warn">Disqualified</span>';
    if (s.completed) return '<span class="badge b-neu">Completed</span>';
    return '<span class="badge b-neu">Left on step 2</span>';
  }
  function render() {
    if (!root || (G.current && G.current() !== 'dupes')) return;
    var s0 = q.toLowerCase().trim();
    var rows = data ? data.leads.filter(function (l) { return (filter === 'all' ? true : filter === 'ours' ? l.is_internal : !l.is_internal) && (!s0 || String(l.email).toLowerCase().indexOf(s0) >= 0); }) : [];
    var counts = data ? { all: data.leads.length, ours: data.leads.filter(function (l) { return l.is_internal; }).length } : {};
    if (data) counts.real = counts.all - counts.ours;
    var page = rows.slice(0, shown), more = rows.length - page.length;
    /* WHAT THE LIST IS: addresses with more than one LEAD ROW -- most never
       submitted twice. "Filled the form in more than once" said otherwise. */
    var head = '<section class="ph"><div class="ph-top"><h1 class="title" tabindex="-1">Duplicates</h1><span class="readat">' + (data ? fmt(data.total) + ' ' + G.plural(data.total, 'address', 'addresses') + ' with more than one attempt' : '') + '</span></div>' +
      '<p class="lede">Addresses that got through step 1 in more than one session, most attempts first. Our own test addresses are marked on the row and counted like everything else.</p>' +
      (data ? '<div class="controls">' + U.pills([['all', 'Everything'], ['real', 'Hide our own tests'], ['ours', 'Only our own tests']], filter, counts, 'data-dupes', 'Which addresses') +
        '<input class="field search" type="search" data-dupes-q placeholder="Search an address" aria-label="Search duplicate addresses" value="' + esc(q) + '"></div>' : '') + '</section>';
    var body;
    if (!data && err) body = '<section class="card panel">' + U.unavailable('Duplicates', err) + '</section>';
    else if (!data) body = U.loading(5);
    else body = '<section class="card">' + U.rtable({ ns: 'dupe', rows: page, key: function (l) { return String(l.email).toLowerCase(); }, rowName: function (l) { return l.email; },
      emptyTitle: s0 ? 'No address matches' : 'No duplicates in this view', emptyBody: s0 ? 'Try part of the address, or clear the search.' : '',
      cols: [
        { label: 'Email', html: function (l) { return esc(l.email) + (l.is_internal ? ' <span class="badge b-neu" title="One of our own test addresses, or submitted from the staging site">ours</span>' : ''); } },
        { label: 'Attempts', r: 1, html: function (l) { return '<b>' + fmt(l.session_count) + '</b>'; } },
        { label: 'Booked', html: function (l) { return l.has_booking ? '<span class="badge b-good">Yes</span>' : '<span class="badge b-neu">No</span>'; } },
        { label: 'Completed', html: function (l) { return l.has_completed ? '<span class="badge b-neu">Yes</span>' : '<span class="badge b-neu">No</span>'; } },
        { label: 'First seen (ET)', cls: 'm', get: function (l) { return G.et(l.first_seen); } },
        { label: 'Last seen (ET)', cls: 'm', get: function (l) { return G.et(l.last_seen); } },
      ],
      detail: function (l) {
        return '<div class="hlist">' + (l.sessions || []).map(function (s) {
          return '<div class="hrow"><div><div class="n">' + stage(s) + ' <span class="meta">' + esc(G.et(s.created_at)) + '</span></div><div class="d">' + esc(s.page_url || '') + '</div></div><div class="s"><small class="num-t">' + esc(String(s.session_id).slice(0, 13)) + '…</small></div></div>';
        }).join('') + '</div>';
      } }) + (more > 0 ? '<div class="more"><span>Showing ' + fmt(page.length) + ' of ' + fmt(rows.length) + '</span><button class="btn" data-more="dupes">Show ' + fmt(Math.min(PAGE, more)) + ' more</button></div>' : '') + '</section>';
    G.paint(root, head + body);
  }
  if (typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('click', function (e) {
      var t = e.target && e.target.closest ? e.target : null; if (!t) return;
      var b = t.closest('[data-dupes]'); if (b) { filter = b.getAttribute('data-dupes'); shown = PAGE; render(); return; }
      if (t.closest('[data-more="dupes"]')) { shown += PAGE; render(); }
    });
    document.addEventListener('input', function (e) { if (e.target && e.target.hasAttribute && e.target.hasAttribute('data-dupes-q')) { q = e.target.value; shown = PAGE; render(); } });
  }
  return { title: 'Duplicates', activate: activate, deactivate: deactivate, render: render, _set: function (d) { data = d; } };
})(GW);
