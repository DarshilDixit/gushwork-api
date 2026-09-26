/* ============================================================================
   Visitors -- where people actually were when they filled the form, from
   their IP address. /monitor/visitors, unchanged. Different from Apollo's
   Person location on a lead (CLAUDE.md, "leads.ip_address and the ip_*
   columns"): that is Apollo's record of the PERSON, this is the connection.

   REPEAT ADDRESSES is what earns the tab: "which address sent more than one
   lead" is a question about the whole set, not about one row. It is all
   time, on purpose; everything else follows the window.

   UNITS, in words: coverage counts LEADS and ADDRESSES; places and networks
   count leads and people; every "Booked" is leads. No percentage anywhere
   in coverage -- leads from before capture have no address and never will.

   THE MAP (CLAUDE.md, "THE VISITORS TAB, AND THE THREE TILE PROVIDERS"):
   - tiles from Esri's Canvas service, {z}/{y}/{x} -- the only one of three
     that works embedded (OpenStreetMap 403s by Referer; Carto prints "API
     KEY REQUIRED" across a 200);
   - circle AREA scales with leads (radius 6 + 16*sqrt(leads/max)), drawn
     largest first so small ones land on top, with a light ring;
   - Leaflet loads only when the map is first opened, pinned and checked by
     SRI; if it cannot load, the tables are the fallback and say so;
   - ONE map container for the life of the page, re-attached after every
     repaint, because GW.paint replaces the HTML and a Leaflet map on a
     detached node is a grey box.
   ============================================================================ */
GW.TABS.visitors = (function (G) {
  var U = G.ui, esc = G.esc, fmt = G.fmt;
  var DAYS = [['7', 'Last 7 days'], ['30', 'Last 30 days'], ['90', 'Last 90 days'], ['365', 'Last year']];
  var LEAFLET = { js: 'https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.js', css: 'https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.css',
                  jsSri: 'sha384-cxOPjt7s7Iz04uaHJceBmS+qpjv2JkIHNVcuOrM+YHwZOmJGBXI00mdUXEq65HTH', cssSri: 'sha384-sHL9NAb7lN7rfvG5lfHpm643Xkcjzp4jFvuavGOndn6pjVqS6ny56CAt3nsEVT4H' };
  /* ONE BASEMAP PER THEME, both from the same keyless Esri Canvas service: the
     light-grey map sat as a bright block on the dark page. The dark one was
     checked the way CLAUDE.md asks of any tile source -- tiles downloaded at
     three zoom levels and looked at -- before it was added. */
  var TILES = { light: 'https://services.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}',
                dark: 'https://services.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}' };
  var data = null, err = null, root = null, seq = 0, busy = false, libState = null, mapEl = null, map = null, layer = null, tiles = null, tilesFor = null, drawnFor = null, fitFor = null;
  function days() { var d = G.S.q.days; return DAYS.some(function (x) { return x[0] === d; }) ? d : '30'; }
  function mapOn() { return G.S.q.map === '1'; }
  function load() {
    var my = ++seq; busy = true;
    return G.api('/monitor/visitors', { days: days() }, { timeout: 20000 })
      .then(function (d) { if (my === seq) { data = d; err = null; } }, function (e) { if (my === seq) err = e.message; })
      .then(function () { if (my === seq) busy = false; });
  }
  function activate(el) { root = el; var p = load(); render(); G.every('visitors', 300000, function () { load().then(render); }); return p.then(render); }
  function deactivate() { G.stop('visitors'); root = null; }
  function where(r) { return [r.city, r.region, r.country].filter(Boolean).join(', ') || '—'; }
  /* The visitor's OWN zone -- the one deliberate non-ET time on the
     dashboard. An unknown zone prints nothing rather than throwing. */
  function localNow(tz) { try { return new Intl.DateTimeFormat('en-US', { timeZone: tz, hour: 'numeric', minute: '2-digit', hour12: true }).format(new Date()); } catch (e) { return ''; } }
  function bar(n, max) { return '<div class="vbar"><b data-v="' + esc(n) + '">' + fmt(n) + '</b><div class="tr"><div class="fi" style="width:' + Math.round(n / Math.max(max, 1) * 100) + '%"></div></div></div>'; }

  /* ONE provider under several domains is ONE network: "Verizon Business"
     arrived twice, on verizonbusiness.com and frontiernet.net. Merged here
     so the API keeps reporting what the provider said. People is a SUM of
     per-domain distinct counts, so it can count one person twice -- said on
     screen. Stable sort, so ties keep the API's order. */
  function mergeNetworks(rows) {
    var by = {}, order = [];
    (rows || []).forEach(function (n) {
      var k = n.isp || '—';
      if (!by[k]) { by[k] = { isp: k, domains: [], leads: 0, people: 0, booked: 0, first: order.length }; order.push(k); }
      var m = by[k]; m.leads += n.leads || 0; m.people += n.people || 0; m.booked += n.booked || 0;
      if (n.org_domain && m.domains.indexOf(n.org_domain) === -1) m.domains.push(n.org_domain);
    });
    return order.map(function (k) { return by[k]; }).sort(function (a, b) { return b.leads - a.leads || a.first - b.first; });
  }
  function countries(places) {
    var by = {};
    (places || []).forEach(function (p) { var k = p.country || '?'; by[k] = (by[k] || 0) + (p.leads || 0); });
    return Object.keys(by).map(function (k) { return [k, by[k]]; }).sort(function (a, b) { return b[1] - a[1]; });
  }

  function coverage(c) {
    return '<div class="sumgrid four">' +
      U.metricCard({ id: 'vis-leads', label: 'All leads in the window', value: c.leads, sub: 'leads — including leads from before we recorded addresses' }) +
      U.metricCard({ id: 'vis-addr', label: 'With an address', value: c.with_address, sub: 'leads' }) +
      U.metricCard({ id: 'vis-place', label: 'Resolved to a place', value: c.with_place, sub: 'leads — the set the places, networks and time zones below are drawn from' }) +
      U.metricCard({ id: 'vis-distinct', label: 'Distinct addresses', value: c.distinct_addresses, sub: 'addresses' }) + '</div>' +
      '<p class="lnote">Leads from before this was switched on have no address and never will, so these are counted rather than shown as a rate.</p>';
  }
  function lists(d) {
    var rp = d.repeats || { rows: [] }, pl = (d.places && d.places.rows) || [], nets = mergeNetworks(d.networks), tz = d.timezones || [];
    var pmax = pl.reduce(function (a, r) { return Math.max(a, r.leads || 0); }, 0), nmax = nets.reduce(function (a, r) { return Math.max(a, r.leads); }, 0), tmax = tz.reduce(function (a, r) { return Math.max(a, r.leads || 0); }, 0);
    var h = U.panel({ id: 'vis-repeats', title: 'Repeat addresses', qual: 'all time, not just this window',
      body: '<p class="lnote">More than one lead from the same address. Two submissions months apart are exactly what this is for. An office or a household can legitimately send several, so this is a place to look, not a verdict.</p>' +
        U.rtable({ ns: 'visr', rows: rp.rows || [], key: function (r) { return r.ip_address; }, rowName: function (r) { return r.ip_address; }, emptyTitle: 'No address has sent more than one lead yet',
          cols: [
            { label: 'Address', html: function (r) { return '<code>' + esc(r.ip_address) + '</code>'; } },
            { label: 'Leads', r: 1, html: function (r) { return '<b data-v="' + esc(r.leads) + '">' + fmt(r.leads) + '</b>'; } },
            { label: 'People', r: 1, get: function (r) { return fmt(r.people); } },
            { label: 'Where', get: where },
            { label: 'Network', opt: 1, get: function (r) { return r.isp || '—'; } },
            { label: 'First seen (ET)', cls: 'm', opt: 1, get: function (r) { return G.et(r.first_seen); } },
            { label: 'Last seen (ET)', cls: 'm', get: function (r) { return G.et(r.last_seen); } },
          ],
          detail: function (r) { return U.kv([['Emails', (r.emails || []).join(', ') || 'none recorded'], ['Network', r.isp], ['First seen (ET)', G.et(r.first_seen)]]); } }) +
        (rp.total > rp.shown ? '<p class="lnote">Showing ' + fmt(rp.shown) + ' of ' + fmt(rp.total) + ' repeat addresses — the ones with the most leads.</p>' : '') });
    h += U.panel({ id: 'vis-places', title: 'Where they were', qual: 'leads in the window, by place',
      body: '<div class="chips">' + countries(pl).map(function (c) { return '<span class="badge b-neu">' + esc(c[0]) + ' · ' + fmt(c[1]) + ' ' + G.plural(c[1], 'lead') + '</span>'; }).join('') + '</div>' +
        (pl.length ? U.grid([
          { label: 'City', get: function (p) { return p.city || '—'; } },
          { label: 'Region', get: function (p) { return p.region || '—'; } },
          { label: 'Country', get: function (p) { return p.country || '—'; } },
          { label: 'Leads', html: function (p) { return bar(p.leads, pmax); } },
          { label: 'People', get: function (p) { return fmt(p.people); } },
          { label: 'Booked', get: function (p) { return fmt(p.booked); } },
        ], pl, { region: 'Where they were' }) : U.empty('Nothing resolved in this window')) +
        (pl.length >= 500 ? '<p class="lnote">The 500 places with the most leads; the country totals above add up those 500.</p>' : '') });
    h += U.panel({ id: 'vis-networks', title: 'Networks', qual: 'who provides their connection',
      body: '<p class="lnote">A business provider is a different signal from home broadband. One provider seen under several domains is one row, with every domain listed.</p>' +
        (nets.length ? U.grid([
          { label: 'Network', get: function (n) { return n.isp; } },
          { label: 'Domains', cls: 'na', get: function (n) { return n.domains.join(', ') || '—'; } },
          { label: 'Leads', html: function (n) { return bar(n.leads, nmax); } },
          { label: 'People', get: function (n) { return fmt(n.people); } },
          { label: 'Booked', get: function (n) { return fmt(n.booked); } },
        ], nets, { region: 'Networks' }) : U.empty('Nothing resolved in this window')) +
        (nets.length ? '<p class="lnote">People is added up across a provider’s domains, so one person seen on two of them counts twice.' + ((d.networks || []).length >= 100 ? ' These are the 100 busiest.' : '') + '</p>' : '') });
    h += U.panel({ id: 'vis-zones', title: 'Time zones', qual: 'what time it is where they are, for whoever is calling',
      body: tz.length ? U.grid([
        { label: 'Time zone', get: function (z) { return z.timezone; } },
        { label: 'Local time now', get: function (z) { return localNow(z.timezone) || '—'; } },
        { label: 'Leads', html: function (z) { return bar(z.leads, tmax); } },
        { label: 'Booked', get: function (z) { return fmt(z.booked); } },
      ], tz, { region: 'Time zones' }) +
        (tz.length >= 60 ? '<p class="lnote">The 60 busiest time zones.</p>' : '') : U.empty('Nothing resolved in this window') });
    return h;
  }

  /* ── The map ─────────────────────────────────────────────────────── */
  function loadLeaflet() {
    if (typeof window !== 'undefined' && window.L) return Promise.resolve(true);
    if (libState) return libState;
    libState = new Promise(function (res) {
      var css = document.createElement('link'); css.rel = 'stylesheet'; css.href = LEAFLET.css; css.integrity = LEAFLET.cssSri; css.crossOrigin = 'anonymous';
      var js = document.createElement('script'); js.src = LEAFLET.js; js.integrity = LEAFLET.jsSri; js.crossOrigin = 'anonymous';
      js.onload = function () { res(!!window.L); }; js.onerror = function () { res(false); };
      document.head.appendChild(css); document.head.appendChild(js);
    });
    return libState;
  }
  function points(d) {
    return ((d.places && d.places.rows) || []).filter(function (p) { return typeof p.lat === 'number' && typeof p.lon === 'number'; })
      .slice().sort(function (a, b) { return (b.leads || 0) - (a.leads || 0); });   /* largest first, HERE, never trusted from the API order */
  }
  function radius(leads, max) { return 6 + 16 * Math.sqrt((leads || 0) / Math.max(max, 1)); }
  function setTiles(t) {
    var Lf = window.L; if (!map || !Lf) return;
    t = t || (document.documentElement && document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light');
    if (tilesFor === t) return;
    if (tiles) map.removeLayer(tiles);
    tiles = Lf.tileLayer(TILES[t] || TILES.light, { maxZoom: 12, attribution: 'Tiles &copy; Esri' }).addTo(map); tilesFor = t;
  }
  var prevOnTheme = G.onTheme;
  G.onTheme = function (t) { if (prevOnTheme) prevOnTheme(t); setTiles(t); };
  function drawMap(d) {
    var Lf = window.L, slot = document.getElementById('vis-map-slot'), note = document.getElementById('vis-map-note');
    if (!slot) return;
    if (!Lf) { if (note) note.textContent = 'The map library did not load — the tables have the same data.'; return; }
    if (!mapEl) {
      mapEl = document.createElement('div'); mapEl.className = 'vmap';
      slot.appendChild(mapEl);
      /* held inside the world north to south: zoomed out, the view ran past
         the top of the tiles and drew a bare band above the Arctic */
      map = Lf.map(mapEl, { worldCopyJump: true, scrollWheelZoom: false, dragging: !(Lf.Browser && Lf.Browser.mobile), tap: false, attributionControl: true,
        maxBounds: [[-85, -720], [85, 720]], maxBoundsViscosity: 1 }).setView([25, 0], 2);
      map.attributionControl.setPrefix(false);
    } else if (mapEl.parentNode !== slot) slot.appendChild(mapEl);
    setTiles();
    setTimeout(function () { if (map) map.invalidateSize(); }, 0);
    var pts = points(d), max = pts.reduce(function (a, p) { return Math.max(a, p.leads || 0); }, 0);
    /* THE READER'S PLACE SURVIVES A REFRESH. A repaint of the same read
       (a width change, a theme switch) keeps the circles and any open popup;
       new data swaps the circles and keeps the view; only a new window, or
       the first draw, re-fits the map to its points. */
    if (d !== drawnFor) {
      if (layer) { map.removeLayer(layer); layer = null; }
      layer = Lf.layerGroup(pts.map(function (p) {
        var name = [p.city, p.region, p.country].filter(Boolean).join(', ') || 'Unknown place';
        return Lf.circleMarker([p.lat, p.lon], { radius: radius(p.leads, max), weight: 2, className: 'vdot' })
          .bindPopup('<b>' + esc(name) + '</b><br>' + fmt(p.leads) + ' ' + G.plural(p.leads, 'lead') + '<br>' + fmt(p.people) + ' ' + G.plural(p.people, 'person', 'people') + '<br>' + fmt(p.booked) + ' booked');
      })).addTo(map);
      drawnFor = d;
    }
    var wk = String(d.window_days);
    if (pts.length && fitFor !== wk) { map.fitBounds(Lf.latLngBounds(pts.map(function (p) { return [p.lat, p.lon]; })).pad(0.2)); fitFor = wk; }
    var miss = ((d.places && d.places.rows) || []).length - pts.length;
    if (note) note.innerHTML = fmt(pts.length) + ' ' + G.plural(pts.length, 'place') + ' drawn' +
      (miss > 0 ? '; <b>' + fmt(miss) + '</b> more resolved to a city but have no coordinates, so they cannot be placed — they are all in the tables.' : '.') + ' A circle’s area grows with its lead count, from a minimum size so a single lead stays visible.';
    return { drawn: pts.length, miss: miss };
  }

  function render() {
    if (!root || (G.current && G.current() !== 'visitors')) return;
    var d = data, on = mapOn();
    var stale = d && (busy || String(d.window_days) !== days());
    var head = '<section class="ph"><div class="ph-top"><h1 class="title" tabindex="-1">Visitors</h1><span class="readat">' + (d ? 'Window ' + esc(String(d.window_days)) + ' days' : '') + (stale ? ' · updating…' : '') + '</span></div>' +
      '<p class="lede">Where people actually were when they filled the form, from their IP address. Different from <b>Person location</b> on a lead, which is Apollo’s record of where the person is based; the company’s own address is Company HQ.</p>' +
      '<div class="controls"><label class="lfl inline"><span>Window</span><select class="field" data-vis="days">' + DAYS.map(function (o) { return '<option value="' + o[0] + '"' + (o[0] === days() ? ' selected' : '') + '>' + o[1] + '</option>'; }).join('') + '</select></label>' +
      /* the label says what the button DOES; with aria-pressed as well a
         screen reader heard "Show tables, pressed" while the map was open */
      '<button class="btn" data-vis-map>' + G.ic(on ? 'table' : 'map-trifold') + (on ? 'Show tables' : 'Show map') + '</button></div></section>';
    var body;
    if (!d && err) body = '<section class="card panel">' + U.unavailable('Visitors', err) + '</section>';
    else if (!d) body = U.loading(4);
    else body = U.drawn('Visitors', function () { return (err ? '<div class="readat"><span class="badge b-warn">Last refresh failed (' + esc(String(err).slice(0, 40)) + ') — showing the previous read</span></div>' : '') +
      U.panel({ id: 'vis-cov', title: 'Coverage', qual: 'this window', body: coverage(d.coverage || {}) }) +
      (on ? U.panel({ id: 'vis-map', title: 'Map', qual: 'places in the window', body: '<div id="vis-map-slot" class="vmap-slot"></div><p class="lnote" id="vis-map-note">Loading the map…</p>' }) : lists(d)); });
    if (mapEl && mapEl.parentNode) mapEl.parentNode.removeChild(mapEl);   /* keep the container alive across the repaint */
    G.paint(root, head + body);
    if (d && on) loadLeaflet().then(function () { if (G.current() === 'visitors' && mapOn()) drawMap(data); });
  }
  if (typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('change', function (e) {
      var el = e.target; if (!el || !el.getAttribute || G.current() !== 'visitors' || el.getAttribute('data-vis') !== 'days') return;
      if (el.value === '30') delete G.S.q.days; else G.S.q.days = el.value;
      G.writeHash(); var p = load(); render();
      p.then(function () { if (G.current() !== 'visitors') return; render(); var c = data && data.coverage; if (c) G.announce(fmt(c.leads) + ' ' + G.plural(c.leads, 'lead') + ' in the last ' + esc(String(data.window_days)) + ' days'); });
    });
    document.addEventListener('click', function (e) {
      var t = e.target && e.target.closest ? e.target.closest('[data-vis-map]') : null; if (!t || G.current() !== 'visitors') return;
      if (mapOn()) delete G.S.q.map; else G.S.q.map = '1';
      G.writeHash(); render();
    });
  }
  return { title: 'Visitors', activate: activate, deactivate: deactivate, render: render, load: load, _set: function (d) { data = d; },
           _merge: mergeNetworks, _points: points, _radius: radius, _draw: drawMap, _localNow: localNow, _tiles: TILES };
})(GW);
