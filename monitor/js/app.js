/* ============================================================================
   The shell: navigation, tab switching, the drawer, theme, refresh.

   Tabs not yet rebuilt are real links to the classic dashboard at /monitor,
   opened at that tab (#tab=...), and marked as leaving -- a nav row that
   looks like it stays here and does not is worse than one that says so.
   ============================================================================ */
(function (G) {
  var NAV = [
    { items: [['overview', 'Overview', 'house']] },
    { label: 'Leads', items: [['leads', 'All leads', 'users'], ['sdr', 'SDR list', 'phone-call'], ['dropoff', 'Dropoff', 'funnel'], ['dupes', 'Duplicates', 'copy'], ['visitors', 'Visitors', 'globe-hemisphere-west']] },
    { label: 'Lead quality', items: [['blocked', 'Blocked', 'prohibit'], ['model', 'Model', 'robot']] },
    { label: 'Revenue', items: [['partners', 'Partners', 'handshake'], ['lm', 'Lead magnet', 'magnet']] },
    { label: 'System', items: [['health', 'System health', 'heartbeat']] },
  ];
  var current = null, badges = {}, lastW = 0;
  function navHtml() {
    return NAV.map(function (g) {
      return '<div class="group">' + (g.label ? '<div class="glabel">' + G.esc(g.label) + '</div>' : '') + g.items.map(function (it) {
        /* the count SAYS what it counts to a screen reader: "System health 1" meant nothing */
        var b = badges[it[0]] ? '<span class="badge b-bad">' + badges[it[0]] + '<span class="sr-only"> ' + G.plural(badges[it[0]], 'check') + ' red</span></span>' : '';
        if (it[3]) return '<a class="nav" href="' + G.esc(G.classic(it[0])) + '" title="Opens this tab in the classic dashboard">' + G.ic(it[2]) + '<span class="l">' + G.esc(it[1]) + '</span>' + G.ic('arrow-up-right', 'ext') + '<span class="sr-only"> (classic dashboard)</span></a>';
        return '<a class="nav" href="#tab=' + it[0] + '" data-tab="' + it[0] + '"' + (it[0] === current ? ' aria-current="page"' : '') + '>' + G.ic(it[2]) + '<span class="l">' + G.esc(it[1]) + '</span>' + b + '</a>';
      }).join('') + '</div>';
    }).join('');
  }
  function renderNav() { ['nav-side', 'nav-drawer'].forEach(function (id) { var el = G.$(id); if (el) el.innerHTML = navHtml(); }); }
  G.navBadge = function (id, n) { var v = n > 0 ? n : 0; if ((badges[id] || 0) === v) return; badges[id] = v; renderNav(); };

  /* The drawer is NON-MODAL, as designed (no scrim), so it closes the moment
     focus leaves it: Tab past its ends used to walk focus onto controls
     hidden underneath it. Closing hands focus back to the menu button
     whenever focus was in the drawer or had fallen to the page body. */
  function isOpen() { var d = G.$('drawer'); return !!(d && d.classList.contains('open')); }
  function drawer(open) {
    var d = G.$('drawer'), c = G.$('drawer-catch'), t = document.querySelector('.menu-trigger');
    if (!d) return;
    var was = isOpen();
    d.classList.toggle('open', open); if (c) c.classList.toggle('open', open);
    if (t) t.setAttribute('aria-expanded', String(open));
    if (open) { var f = d.querySelector('a, button'); if (f && f.focus) f.focus(); }
    else if (was && t && t.focus) { var a = document.activeElement; if (!a || a === document.body || d.contains(a)) t.focus(); }
  }
  /* A tab change the READER asked for moves focus to the new tab's heading,
     so a keyboard or screen-reader user lands on what they opened instead of
     on the page body. Boot does not move focus. */
  /* q: the new tab's own filters. A tab you MOVE to starts clean unless the
     caller hands it some (Partners opening "All leads, partner = X"); at boot
     the filters came from the link and are kept. */
  function show(tab, asked, q) {
    if (!G.TABS[tab]) tab = 'overview';
    if (current && current !== tab && G.TABS[current] && G.TABS[current].deactivate) G.TABS[current].deactivate();
    if (q) G.S.q = q; else if (current && current !== tab) G.S.q = {};
    current = tab; G.S.tab = tab; G.writeHash();
    document.title = G.TABS[tab].title + ' · Gushwork Monitor';
    var fromDrawer = isOpen();
    renderNav(); drawer(false);
    var el = G.$('view'); el.innerHTML = '';
    try { window.scrollTo(0, 0); } catch (e) {}
    G.TABS[tab].activate(el);
    if (asked || fromDrawer) { var h = el.querySelector && el.querySelector('h1'); if (h && h.focus) { try { h.focus({ preventScroll: true }); } catch (e) { h.focus(); } } }
  }
  G.show = show;
  G.current = function () { return current; };

  document.addEventListener('click', function (e) {
    var t = e.target && e.target.closest ? e.target : null; if (!t) return;
    var a = t.closest('[data-tab]'); if (a) { e.preventDefault(); show(a.getAttribute('data-tab'), true); return; }
    /* Skip to content: focus the view, and leave the hash (and the state it
       carries) alone -- "#view" used to reset the period and the unit. */
    if (t.closest('.skip')) { e.preventDefault(); var v0 = G.$('view'); if (v0 && v0.focus) v0.focus(); return; }
    if (t.closest('.menu-trigger')) { var d = G.$('drawer'); drawer(!(d && d.classList.contains('open'))); return; }
    if (t.closest('#drawer-catch')) { drawer(false); return; }
    var v = t.closest('[data-view]'); if (v && current === 'overview') { G.TABS.overview.setView(v.getAttribute('data-view')); return; }
    var u = t.closest('[data-unit]'); if (u) { G.S.unit = u.getAttribute('data-unit'); G.writeHash(); if (G.TABS[current].render) G.TABS[current].render(); return; }
    if (t.closest('[data-table-toggle]')) { G.S.table = !G.S.table; G.writeHash(); if (G.TABS[current].render) G.TABS[current].render(); return; }
    var th = t.closest('[data-theme-set]'); if (th) { G.setTheme(th.getAttribute('data-theme-set')); return; }
    if (t.closest('[data-refresh]')) {
      var T = G.TABS[current], p = T.refresh ? T.refresh() : T.run ? T.run(true) : T.activate(G.$('view'));
      if (!T.run && p && p.then) p.then(function () { G.announce(T.title + ' refreshed'); });
      return;
    }
  });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && isOpen()) drawer(false); });
  document.addEventListener('focusout', function (e) {
    var d = G.$('drawer'), t = document.querySelector('.menu-trigger'), to = e.relatedTarget;
    if (!isOpen() || !d || !d.contains(e.target) || !to) return;
    if (!d.contains(to) && !(t && t.contains(to))) drawer(false);
  });
  /* Redraw on a real width change only: a phone's URL bar showing and hiding
     fires resize with the same width, and redrawing then would thrash. */
  var rt = null;
  window.addEventListener('resize', function () { clearTimeout(rt); rt = setTimeout(function () { var w = window.innerWidth; if (w === lastW) return; lastW = w; if (current && G.TABS[current].render) G.TABS[current].render(); }, 150); });
  window.addEventListener('hashchange', function () { var was = G.S.tab; if (!G.readHash()) return; if (G.S.tab !== was) show(G.S.tab, true, G.S.q); else if (current && G.TABS[current].render) G.TABS[current].render(); });

  G.readHash(); G.applyTheme(); lastW = window.innerWidth; show(G.S.tab);
})(GW);
