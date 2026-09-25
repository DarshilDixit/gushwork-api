/* ============================================================================
   Duplicates -- addresses that appear in more than one session. Ported, with
   one addition: OUR OWN test addresses are marked on the row (is_internal,
   decided server-side by the one clause every outbound guard uses). They are
   the top of this list; the old tab showed them with no marker at all.
   Marked, never excluded by default -- the filter is opt-in.
   ============================================================================ */
GW.TABS.dupes = (function (G) {
  var U = G.ui, esc = G.esc, fmt = G.fmt;
  var data = null, err = null, root = null, filter = 'all';
  function load() { return G.api('/monitor/duplicates', { _: Date.now() }, { timeout: 20000 }).then(function (d) { data = d; err = null; }).catch(function (e) { err = e.message; }); }
  function activate(el) { root = el; render(); G.every('dupes', 300000, function () { load().then(render); }); return load().then(render); }
  function deactivate() { G.stop('dupes'); }
  function stage(s) {
    if (s.booking_uid) return '<span class="badge b-good">Booked</span>';
    if (s.disqualified) return '<span class="badge b-warn">Disqualified</span>';
    if (s.completed) return '<span class="badge b-neu">Completed</span>';
    return '<span class="badge b-neu">Left on step 2</span>';
  }
  function render() {
    if (!root) return;
    var rows = data ? data.leads.filter(function (l) { return filter === 'all' ? true : filter === 'ours' ? l.is_internal : !l.is_internal; }) : [];
    var counts = data ? { all: data.leads.length, ours: data.leads.filter(function (l) { return l.is_internal; }).length } : {};
    if (data) counts.real = counts.all - counts.ours;
    var head = '<section class="ph"><div class="ph-top"><h1 class="title">Duplicates</h1><span class="readat">' + (data ? fmt(data.total) + ' ' + G.plural(data.total, 'address', 'addresses') + ' with more than one session' : '') + '</span></div>' +
      '<p class="lede">Addresses that filled the form in more than once, most sessions first. Our own test addresses are marked on the row and counted like everything else.</p>' +
      (data ? '<div class="controls">' + U.pills([['all', 'Everything'], ['real', 'Hide our own tests'], ['ours', 'Only our own tests']], filter, counts, 'data-dupes') + '</div>' : '') + '</section>';
    var body;
    if (!data && err) body = '<section class="card panel">' + U.unavailable('Duplicates', err) + '</section>';
    else if (!data) body = U.loading(5);
    else body = '<section class="card">' + U.rtable({ ns: 'dupe', rows: rows, emptyTitle: 'No duplicates in this view',
      cols: [
        { label: 'Email', html: function (l) { return esc(l.email) + (l.is_internal ? ' <span class="badge b-neu" title="One of our own test addresses, or submitted from the staging site">ours</span>' : ''); } },
        { label: 'Sessions', r: 1, html: function (l) { return '<b>' + fmt(l.session_count) + '</b>'; } },
        { label: 'Booked', html: function (l) { return l.has_booking ? '<span class="badge b-good">Yes</span>' : '<span class="badge b-neu">No</span>'; } },
        { label: 'Completed', html: function (l) { return l.has_completed ? '<span class="badge b-neu">Yes</span>' : '<span class="badge b-neu">No</span>'; } },
        { label: 'First seen (ET)', cls: 'm', get: function (l) { return G.et(l.first_seen); } },
        { label: 'Last seen (ET)', cls: 'm', get: function (l) { return G.et(l.last_seen); } },
      ],
      detail: function (l) {
        return '<div class="hlist">' + (l.sessions || []).map(function (s) {
          return '<div class="hrow"><div><div class="n">' + stage(s) + ' <span class="meta">' + esc(G.et(s.created_at)) + '</span></div><div class="d">' + esc(s.page_url || '') + '</div></div><div class="s"><small class="num-t">' + esc(String(s.session_id).slice(0, 13)) + '…</small></div></div>';
        }).join('') + '</div>';
      } }) + '</section>';
    root.innerHTML = head + body;
  }
  if (typeof document !== 'undefined' && document.addEventListener) document.addEventListener('click', function (e) { var b = e.target && e.target.closest ? e.target.closest('[data-dupes]') : null; if (b) { filter = b.getAttribute('data-dupes'); render(); } });
  return { title: 'Duplicates', activate: activate, deactivate: deactivate, render: render, _set: function (d) { data = d; } };
})(GW);
