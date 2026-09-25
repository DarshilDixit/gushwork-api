/* ============================================================================
   The shell: navigation, tab switching, the drawer, theme, refresh.

   Tabs not yet rebuilt are real links to the classic dashboard at /monitor,
   opened at that tab (#tab=...), and marked as leaving -- a nav row that
   looks like it stays here and does not is worse than one that says so.
   ============================================================================ */
(function (G) {
  var NAV = [
    { items: [['overview', 'Overview', 'house']] },
    { label: 'Leads', items: [['leads', 'All leads', 'users', 1], ['sdr', 'SDR list', 'phone-call', 1], ['dropoff', 'Dropoff', 'funnel'], ['dupes', 'Duplicates', 'copy'], ['visitors', 'Visitors', 'globe-hemisphere-west', 1]] },
    { label: 'Lead quality', items: [['blocked', 'Blocked', 'prohibit', 1], ['model', 'Model', 'robot', 1]] },
    { label: 'Revenue', items: [['partners', 'Partners', 'handshake', 1], ['lm', 'Lead magnet', 'magnet']] },
    { label: 'System', items: [['health', 'System health', 'heartbeat']] },
  ];
  var current = null, badges = {}, lastW = 0;
  function navHtml() {
    return NAV.map(function (g) {
      return '<div class="group">' + (g.label ? '<div class="glabel">' + G.esc(g.label) + '</div>' : '') + g.items.map(function (it) {
        var b = badges[it[0]] ? '<span class="badge b-bad">' + badges[it[0]] + '</span>' : '';
        if (it[3]) return '<a class="nav" href="' + G.esc(G.classic(it[0])) + '" title="Opens this tab in the classic dashboard">' + G.ic(it[2]) + '<span class="l">' + G.esc(it[1]) + '</span>' + G.ic('arrow-up-right', 'ext') + '<span class="sr-only"> (classic dashboard)</span></a>';
        return '<a class="nav" href="#tab=' + it[0] + '" data-tab="' + it[0] + '"' + (it[0] === current ? ' aria-current="page"' : '') + '>' + G.ic(it[2]) + '<span class="l">' + G.esc(it[1]) + '</span>' + b + '</a>';
      }).join('') + '</div>';
    }).join('');
  }
  function renderNav() { ['nav-side', 'nav-drawer'].forEach(function (id) { var el = G.$(id); if (el) el.innerHTML = navHtml(); }); }
  G.navBadge = function (id, n) { var v = n > 0 ? n : 0; if ((badges[id] || 0) === v) return; badges[id] = v; renderNav(); };

  function drawer(open) {
    var d = G.$('drawer'), c = G.$('drawer-catch'), t = document.querySelector('.menu-trigger');
    if (!d) return;
    d.classList.toggle('open', open); if (c) c.classList.toggle('open', open);
    if (t) t.setAttribute('aria-expanded', String(open));
    if (open) { var f = d.querySelector('a, button'); if (f && f.focus) f.focus(); } else if (t && document.activeElement && d.contains(document.activeElement) && t.focus) t.focus();
  }
  function show(tab) {
    if (!G.TABS[tab]) tab = 'overview';
    if (current && current !== tab && G.TABS[current] && G.TABS[current].deactivate) G.TABS[current].deactivate();
    current = tab; G.S.tab = tab; G.writeHash();
    document.title = G.TABS[tab].title + ' · Gushwork Monitor';
    renderNav(); drawer(false);
    var el = G.$('view'); el.innerHTML = '';
    try { window.scrollTo(0, 0); } catch (e) {}
    G.TABS[tab].activate(el);
  }
  G.show = show;
  G.current = function () { return current; };

  document.addEventListener('click', function (e) {
    var t = e.target && e.target.closest ? e.target : null; if (!t) return;
    var a = t.closest('[data-tab]'); if (a) { e.preventDefault(); show(a.getAttribute('data-tab')); return; }
    if (t.closest('.menu-trigger')) { var d = G.$('drawer'); drawer(!(d && d.classList.contains('open'))); return; }
    if (t.closest('#drawer-catch')) { drawer(false); return; }
    var v = t.closest('[data-view]'); if (v && current === 'overview') { G.TABS.overview.setView(v.getAttribute('data-view')); return; }
    var u = t.closest('[data-unit]'); if (u) { G.S.unit = u.getAttribute('data-unit'); G.writeHash(); if (G.TABS[current].render) G.TABS[current].render(); return; }
    if (t.closest('[data-table-toggle]')) { G.S.table = !G.S.table; G.writeHash(); if (G.TABS[current].render) G.TABS[current].render(); return; }
    var th = t.closest('[data-theme-set]'); if (th) { G.setTheme(th.getAttribute('data-theme-set')); return; }
    if (t.closest('[data-refresh]')) { var T = G.TABS[current]; if (T.refresh) T.refresh(); else if (T.run) T.run(); else T.activate(G.$('view')); return; }
  });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape') { var d = G.$('drawer'); if (d && d.classList.contains('open')) drawer(false); } });
  /* Redraw on a real width change only: a phone's URL bar showing and hiding
     fires resize with the same width, and redrawing then would thrash. */
  var rt = null;
  window.addEventListener('resize', function () { clearTimeout(rt); rt = setTimeout(function () { var w = window.innerWidth; if (w === lastW) return; lastW = w; if (current && G.TABS[current].render) G.TABS[current].render(); }, 150); });
  window.addEventListener('hashchange', function () { var was = G.S.tab; G.readHash(); if (G.S.tab !== was) show(G.S.tab); else if (current && G.TABS[current].render) G.TABS[current].render(); });

  G.readHash(); G.applyTheme(); lastW = window.innerWidth; show(G.S.tab);
})(GW);
