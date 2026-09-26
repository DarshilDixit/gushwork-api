/* ============================================================================
   GW -- the shared core of /monitor/next. A classic script, one namespace.

   Everything here is TOP LEVEL of the GW object and nothing is declared inside
   a loader: the old dashboard's leadRowsHtml was declared inside loadLeads, so
   All Leads could see it and Blocked could not, and the failure arrived as a
   tidy "Could not load" line rather than a crash.
   ============================================================================ */
var GW = (function () {
  var CFG = window.__GW__ || {};           // { token, tz, classic } -- written by monitor-next.js
  var TZ = CFG.tz || 'America/New_York';

  /* ── State, and the hash that mirrors it, so a link says what it shows ── */
  /* q holds the CURRENT tab's own filters (All leads, Model, Visitors...), so a
     copied link says exactly what it shows and Partners can open "All leads,
     partner = X". Every key that is not one of the four shared ones lands
     here; a tab reads and writes its own keys and nothing else. */
  var S = { tab: 'overview', view: 'week', unit: 'people', table: false, q: {} };
  var SHARED = { tab: 1, view: 1, unit: 1, table: 1 };
  var TABS = {};
  /* Returns false, and changes nothing, for a hash that is not ours: the
     "Skip to content" link is #view, and reading that as a state would reset
     the period, the unit and the table toggle. A stray "%" (a pasted link)
     makes decodeURIComponent THROW -- at boot that left a page with no nav
     and no content -- so each pair is decoded on its own and a bad one is
     skipped, never fatal. */
  function readHash() {
    var h = String((window.location && window.location.hash) || '').replace(/^#/, '');
    var p = {};
    h.split('&').forEach(function (kv) { var i = kv.indexOf('='); if (i > 0) { try { p[decodeURIComponent(kv.slice(0, i))] = decodeURIComponent(kv.slice(i + 1)); } catch (e) {} } });
    if (!p.tab) return false;
    S.tab = p.tab;
    S.q = {}; Object.keys(p).forEach(function (k) { if (!SHARED[k]) S.q[k] = p[k]; });
    if (p.view === 'today' || p.view === 'week' || p.view === 'all') S.view = p.view;
    if (p.unit === 'people' || p.unit === 'leads') S.unit = p.unit;
    S.table = p.table === '1';
    return true;
  }
  function writeHash() {
    var h = 'tab=' + S.tab + (S.tab === 'overview' ? '&view=' + S.view : '') + '&unit=' + S.unit + (S.table ? '&table=1' : '');
    Object.keys(S.q || {}).forEach(function (k) { var v = S.q[k]; if (v !== undefined && v !== null && v !== '') h += '&' + encodeURIComponent(k) + '=' + encodeURIComponent(v); });
    try { if (window.history && window.history.replaceState) window.history.replaceState(null, '', '#' + h); else window.location.hash = h; } catch (e) {}
  }

  /* ── API. The token rides in the query like every other /monitor call. ── */
  function url(path, params) {
    var q = [];
    if (CFG.token) q.push('token=' + encodeURIComponent(CFG.token));
    Object.keys(params || {}).forEach(function (k) { if (params[k] !== undefined && params[k] !== null && params[k] !== '') q.push(encodeURIComponent(k) + '=' + encodeURIComponent(params[k])); });
    return path + (q.length ? '?' + q.join('&') : '');
  }
  /* opts.body is sent as JSON. The one write that needs it is the Partners
     acknowledgement, whose route reads req.body only -- and whose un-ack
     needs the JSON boolean false, which a query string cannot carry. The
     token still rides in the query, like every other /monitor call. */
  function api(path, params, opts) {
    opts = opts || {};
    var init = { cache: 'no-store', method: opts.method || 'GET' };
    if (opts.body !== undefined) { init.headers = { 'Content-Type': 'application/json' }; init.body = JSON.stringify(opts.body); }
    try { if (typeof AbortSignal !== 'undefined' && AbortSignal.timeout) init.signal = AbortSignal.timeout(opts.timeout || 20000); } catch (e) {}
    return fetch(url(path, params), init).then(function (r) {
      if (!r.ok) return r.text().then(function (t) { var e = new Error('HTTP ' + r.status + (t ? ' ' + String(t).slice(0, 120) : '')); e.status = r.status; throw e; });
      return r.json();
    });
  }

  /* ── Formatting. Every date and time in EASTERN, never the viewer's laptop:
     CLAUDE.md -- "never derive a calendar date in browser code from
     getFullYear/getMonth/getDate". ── */
  function fmt(n) { return n === null || n === undefined || isNaN(n) ? '—' : Number(n).toLocaleString('en-US'); }
  function pct(a, b) { return b ? Math.round(a / b * 100) : null; }
  function pct1(a, b) { return b ? Math.round(a / b * 1000) / 10 : null; }
  function esc(s) { if (s === null || s === undefined) return ''; return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;'); }
  function dtf(opts) { return new Intl.DateTimeFormat('en-US', Object.assign({ timeZone: TZ }, opts)); }
  /* A VALUE THAT IS NOT A DATE PRINTS AS ITSELF, it never throws. Intl's
     format() throws RangeError on an invalid Date, and leads.start_time is
     TEXT that can be malformed -- one bad row would have blanked a whole
     table from inside its detail panel. */
  function okDate(ts) { var d = new Date(ts); return isNaN(d.getTime()) ? null : d; }
  function et(ts) { if (!ts) return '—'; var d = okDate(ts); return d ? dtf({ dateStyle: 'medium', timeStyle: 'short' }).format(d) : String(ts); }
  function etTime(ts) { if (!ts) return '—'; var d = okDate(ts); return d ? dtf({ hour: 'numeric', minute: '2-digit' }).format(d) : String(ts); }
  function etDate(ts, opts) { if (!ts) return '—'; var d = okDate(ts); return d ? dtf(opts || { weekday: 'short', day: 'numeric', month: 'short' }).format(d) : String(ts); }
  /* A link from DATA is only ever http(s). A LinkedIn field holding
     "javascript:..." ran on click in the classic tab. A bare domain gets
     https:// in front, as the classic did; anything else is not a link. */
  function href(v) {
    var s = String(v || '').trim(); if (!s) return null;
    if (/^https?:\/\//i.test(s)) return s;
    if (/^[a-z][a-z0-9+.-]*:/i.test(s)) return null;
    return 'https://' + s.replace(/^\/+/, '');
  }
  /* "Mon 21 Sep" / "21 Sep 2026" -- built from parts, because Intl's en-US
     ordering gives "21 Mon" or "Fri, Sep 25", neither of which reads right. */
  function etD(ts, o) {
    if (!ts) return '\u2014'; o = o || {};
    var dd = okDate(ts); if (!dd) return String(ts);
    var parts = {}; dtf({ weekday: 'short', day: 'numeric', month: 'short', year: 'numeric' }).formatToParts(dd).forEach(function (x) { parts[x.type] = x.value; });
    return [o.noWeekday ? null : parts.weekday, parts.day, o.noMonth ? null : parts.month, o.year ? parts.year : null].filter(Boolean).join(' ');
  }
  /* A calendar date the SERVER already resolved in ET ("2026-09-25"), shown
     like every other date here. Read at 16:00 UTC -- noon or 11am in New
     York, whichever side of DST -- so no zone can move it a day. */
  function dayD(iso, o) { return iso ? etD(String(iso).slice(0, 10) + 'T16:00:00Z', o) : '\u2014'; }
  function etDay(d) { return new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit' }).format(d || new Date()); }
  function ago(ts) {
    if (!ts) return '';
    var m = Math.round((Date.now() - new Date(ts).getTime()) / 60000);
    if (m < 1) return 'just now'; if (m < 60) return m + ' min ago';
    var h = Math.round(m / 60); if (h < 48) return h + 'h ago';
    return Math.round(h / 24) + 'd ago';
  }
  function plural(n, one, many) { return n === 1 ? one : (many || one + 's'); }

  /* ── Refresh, paused while the tab is hidden. One scheduler for every
     timer on the page: a hidden tab asks nothing of the server, and coming
     back refreshes at once if the data went stale meanwhile. ── */
  var jobs = {};
  function hidden() { return typeof document !== 'undefined' && document.visibilityState === 'hidden'; }
  function every(key, ms, fn) {
    stop(key);
    var j = { ms: ms, fn: fn, last: Date.now(), t: null };
    jobs[key] = j;
    j.t = setInterval(function () { if (hidden()) return; j.last = Date.now(); fn(); }, ms);
  }
  function stop(key) { if (jobs[key]) { clearInterval(jobs[key].t); delete jobs[key]; } }
  function onVisible() {
    if (hidden()) return;
    Object.keys(jobs).forEach(function (k) { var j = jobs[k]; if (Date.now() - j.last >= j.ms) { j.last = Date.now(); j.fn(); } });
    if (GW.onVisibility) GW.onVisibility(false);
  }
  if (typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('visibilitychange', function () { if (hidden()) { if (GW.onVisibility) GW.onVisibility(true); } else onVisible(); });
  }

  /* ── Theme: light, dark, or follow the computer. Remembered per viewer in
     localStorage, which is a convenience and may be unavailable -- every
     read and write is guarded. ── */
  var themeChoice = 'system';
  try { themeChoice = window.localStorage.getItem('gw-theme') || 'system'; } catch (e) {}
  function systemDark() { try { return !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches); } catch (e) { return false; } }
  function applyTheme() {
    var t = themeChoice === 'system' ? (systemDark() ? 'dark' : 'light') : themeChoice;
    if (document.documentElement) document.documentElement.setAttribute('data-theme', t);
    var bs = document.querySelectorAll ? document.querySelectorAll('[data-theme-set]') : [];
    for (var i = 0; i < bs.length; i++) bs[i].setAttribute('aria-pressed', String(bs[i].getAttribute('data-theme-set') === themeChoice));
    if (GW.onTheme) GW.onTheme(t);
  }
  function setTheme(c) { themeChoice = c; try { window.localStorage.setItem('gw-theme', c); } catch (e) {} applyTheme(); }
  try { if (window.matchMedia) { var mq = window.matchMedia('(prefers-color-scheme: dark)'); if (mq.addEventListener) mq.addEventListener('change', function () { if (themeChoice === 'system') applyTheme(); }); } } catch (e) {}

  /* ONE WAY TO REPAINT A TAB, and it keeps the reader's place. Every tab
     re-renders by replacing its HTML -- on a click, and on a timer every 60s
     on Today -- which destroyed whatever had keyboard focus and dropped it to
     the page body, and collapsed every expanded row. So: note the focused
     control by its data- attribute (and a text field's caret), note which
     rows are open, swap, then put both back. A control that no longer
     exists simply is not refocused. */
  var FOCUS_KEYS = ['data-view', 'data-unit', 'data-table-toggle', 'data-attn', 'data-tab', 'data-dp', 'data-dupes', 'data-dupes-q', 'data-more',
                    'data-lm-pill', 'data-lm-q', 'data-lm-days', 'data-lm-csv', 'data-lm-mark', 'data-lm-retry', 'data-recheck', 'data-x', 'data-refresh',
                    /* PR C: All leads, Blocked, SDR, Partners, Model, Visitors */
                    'data-lf', 'data-lsort', 'data-pg', 'data-lmore', 'data-lcsv', 'data-lclear', 'data-blk', 'data-sdr-q', 'data-sdr-csv',
                    'data-psort', 'data-pdrill', 'data-ack', 'data-ack-go', 'data-ack-cancel', 'data-ack-note', 'data-mdl', 'data-vis', 'data-vis-map'];
  function focusSel(el) {
    if (!el || !el.getAttribute) return null;
    for (var i = 0; i < FOCUS_KEYS.length; i++) {
      var k = FOCUS_KEYS[i];
      if (el.hasAttribute && el.hasAttribute(k)) { var v = el.getAttribute(k); return '[' + k + (v ? '="' + String(v).replace(/["\\]/g, '\\$&') + '"' : '') + ']'; }
    }
    return el.tagName === 'H1' ? 'h1' : null;
  }
  function paint(root, html) {
    if (!root) return;
    var doc = typeof document !== 'undefined' ? document : null, a = doc && doc.activeElement, sel = null, caret = null, open = [];
    if (a && root.contains && root.contains(a)) {
      sel = focusSel(a);
      try { if (typeof a.selectionStart === 'number') caret = [a.selectionStart, a.selectionEnd]; } catch (e) {}
    }
    var xs = root.querySelectorAll ? root.querySelectorAll('[data-x][aria-expanded="true"]') : [];
    for (var i = 0; i < xs.length; i++) open.push(xs[i].getAttribute('data-x'));
    root.innerHTML = html;
    open.forEach(function (k) {
      var b = root.querySelector && root.querySelector('[data-x="' + k + '"]'), d = doc && doc.getElementById(k + '-d');
      if (b && d) { b.setAttribute('aria-expanded', 'true'); d.removeAttribute('hidden'); }
    });
    if (sel && root.querySelector) {
      var n = root.querySelector(sel);
      if (n && n.focus) { try { n.focus({ preventScroll: true }); } catch (e) { n.focus(); } if (caret && n.setSelectionRange) { try { n.setSelectionRange(caret[0], caret[1]); } catch (e) {} } }
    }
  }
  /* One polite live region, in the page shell rather than in a tab, because
     a region that is itself replaced announces nothing. Used for what a
     person ASKED for (Refresh, Re-check, a filter) -- never the one-minute
     timer, which would talk over a screen-reader user every minute. */
  function announce(text) { var el = typeof document !== 'undefined' && document.getElementById('gw-live'); if (el) el.textContent = String(text || ''); }
  function ic(n, cls) { return '<svg class="ic' + (cls ? ' ' + cls : '') + '" aria-hidden="true"><use href="#i-' + n + '"/></svg>'; }
  function $(id) { return document.getElementById(id); }
  function classic(tab) { return (CFG.classic || '/monitor') + (CFG.token ? '?token=' + encodeURIComponent(CFG.token) : '') + '#tab=' + encodeURIComponent(tab); }

  return { CFG: CFG, TZ: TZ, S: S, TABS: TABS, readHash: readHash, writeHash: writeHash, url: url, api: api,
           fmt: fmt, pct: pct, pct1: pct1, esc: esc, et: et, etTime: etTime, etDate: etDate, etD: etD, dayD: dayD, etDay: etDay, okDate: okDate, href: href, ago: ago, plural: plural,
           every: every, stop: stop, hidden: hidden, jobs: jobs, applyTheme: applyTheme, setTheme: setTheme,
           theme: function () { return themeChoice; }, ic: ic, $: $, classic: classic, paint: paint, announce: announce, focusSel: focusSel };
})();
