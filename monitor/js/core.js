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
  var S = { tab: 'overview', view: 'week', unit: 'people', table: false };
  var TABS = {};
  function readHash() {
    var h = String((window.location && window.location.hash) || '').replace(/^#/, '');
    var p = {};
    h.split('&').forEach(function (kv) { var i = kv.indexOf('='); if (i > 0) p[decodeURIComponent(kv.slice(0, i))] = decodeURIComponent(kv.slice(i + 1)); });
    if (p.tab) S.tab = p.tab;
    if (p.view === 'today' || p.view === 'week' || p.view === 'all') S.view = p.view;
    if (p.unit === 'people' || p.unit === 'leads') S.unit = p.unit;
    S.table = p.table === '1';
  }
  function writeHash() {
    var h = 'tab=' + S.tab + (S.tab === 'overview' ? '&view=' + S.view : '') + '&unit=' + S.unit + (S.table ? '&table=1' : '');
    try { if (window.history && window.history.replaceState) window.history.replaceState(null, '', '#' + h); else window.location.hash = h; } catch (e) {}
  }

  /* ── API. The token rides in the query like every other /monitor call. ── */
  function url(path, params) {
    var q = [];
    if (CFG.token) q.push('token=' + encodeURIComponent(CFG.token));
    Object.keys(params || {}).forEach(function (k) { if (params[k] !== undefined && params[k] !== null && params[k] !== '') q.push(encodeURIComponent(k) + '=' + encodeURIComponent(params[k])); });
    return path + (q.length ? '?' + q.join('&') : '');
  }
  function api(path, params, opts) {
    opts = opts || {};
    var init = { cache: 'no-store', method: opts.method || 'GET' };
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
  function et(ts) { if (!ts) return '—'; return dtf({ dateStyle: 'medium', timeStyle: 'short' }).format(new Date(ts)); }
  function etTime(ts) { if (!ts) return '—'; return dtf({ hour: 'numeric', minute: '2-digit' }).format(new Date(ts)); }
  function etDate(ts, opts) { if (!ts) return '—'; return dtf(opts || { weekday: 'short', day: 'numeric', month: 'short' }).format(new Date(ts)); }
  /* "Mon 21 Sep" / "21 Sep 2026" -- built from parts, because Intl's en-US
     ordering gives "21 Mon" or "Fri, Sep 25", neither of which reads right. */
  function etD(ts, o) {
    if (!ts) return '\u2014'; o = o || {};
    var parts = {}; dtf({ weekday: 'short', day: 'numeric', month: 'short', year: 'numeric' }).formatToParts(new Date(ts)).forEach(function (x) { parts[x.type] = x.value; });
    return [o.noWeekday ? null : parts.weekday, parts.day, o.noMonth ? null : parts.month, o.year ? parts.year : null].filter(Boolean).join(' ');
  }
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

  function ic(n, cls) { return '<svg class="ic' + (cls ? ' ' + cls : '') + '" aria-hidden="true"><use href="#i-' + n + '"/></svg>'; }
  function $(id) { return document.getElementById(id); }
  function classic(tab) { return (CFG.classic || '/monitor') + (CFG.token ? '?token=' + encodeURIComponent(CFG.token) : '') + '#tab=' + encodeURIComponent(tab); }

  return { CFG: CFG, TZ: TZ, S: S, TABS: TABS, readHash: readHash, writeHash: writeHash, url: url, api: api,
           fmt: fmt, pct: pct, pct1: pct1, esc: esc, et: et, etTime: etTime, etDate: etDate, etD: etD, etDay: etDay, ago: ago, plural: plural,
           every: every, stop: stop, hidden: hidden, jobs: jobs, applyTheme: applyTheme, setTheme: setTheme,
           theme: function () { return themeChoice; }, ic: ic, $: $, classic: classic };
})();
