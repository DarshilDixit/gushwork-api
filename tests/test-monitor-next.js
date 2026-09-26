/* ============================================================================
   /monitor/next -- the new dashboard, EXECUTED.

   Boots the real app with pg and fetch stubbed, and checks the three things a
   source assertion cannot:

   1. THE ROUTES: auth, the asset allowlist (including a path traversal), and
      that a hostile token cannot break out of the page's config script.
   2. /monitor/overview: runs the REAL overviewReport against stub rows and
      checks what it assembles -- windows bound as parameters, slots generated
      in full, both units, month on month, a bad asof refused.
   3. THE PAGE: evaluates the page's OWN served JavaScript in a stubbed DOM,
      drives every rebuilt tab, and reads the numbers back out of what was
      painted -- compared to the payload that was handed in, with fixture
      values odd enough that they cannot match by accident. "It rendered" is
      one level short of "it rendered the right number", and the gap between
      the two is where the Model tab's "0 companies classified" lived.

   Plus the structural guards: every theme alias declared in both themes, every
   icon the code asks for present in the sprite, refresh paused while the tab
   is hidden, the Dropoff presets identical to the old tab's on every day of
   three years, and data escaped before it reaches innerHTML.

   What it CANNOT see is layout -- that is tools/check-monitor-layout.mjs.

   Dependency-free: no database, no network. Run: node tests/test-monitor-next.js
   ============================================================================ */
require('./crash-reporter')('test-monitor-next');

const Module = require('module');
const path = require('path');
const fs = require('fs');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) { if (cond) pass++; else { fail++; failures.push(name + (extra ? ' — ' + String(extra).slice(0, 300) : '')); } }
const eq = (n, a, b) => ok(n, a === b, `got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`);

const ROOT = path.join(__dirname, '..');
const PORT = 41247;
const BASE = `http://127.0.0.1:${PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
/* A token with characters that would break out of a <script> if it were ever
   pasted in raw. It has to equal MONITOR_TOKEN to be accepted at all. */
const TOKEN = 'tok"</script><b>x';

/* ── The fixture: one fixed instant, 14:52 ET on Fri 25 Sept 2026 ──────── */
const ASOF = '2026-09-25T18:52:00.000Z';
const B = {
  asof: ASOF, d0: '2026-09-25T04:00:00.000Z', d1: '2026-09-24T04:00:00.000Z', d1_same: '2026-09-24T18:52:00.000Z',
  w0: '2026-09-21T04:00:00.000Z', w1: '2026-09-14T04:00:00.000Z', w1_same: '2026-09-18T18:52:00.000Z',
  m0: '2026-09-01T04:00:00.000Z', m1: '2026-08-01T04:00:00.000Z', m1_same: '2026-08-25T18:52:00.000Z',
  go_live: '2026-08-21T10:32:37.000Z', first_lead: '2026-03-20T14:00:00.000Z',
};
/* Odd, distinct numbers, so a painted value cannot coincide with another. */
const KPI = {
  cur: { people: 263, leads: 281, people_done: 211, leads_done: 219, people_booked: 173, leads_booked: 179, people_dq: 23, leads_dq: 27, people_b2c: 13, leads_b2c: 15, people_waitlist: 9, leads_waitlist: 11, people_blocked: 17, leads_blocked: 19, people_withheld: 7, leads_withheld: 8, f_people: 259, f_leads: 277, f_people_done: 209, f_leads_done: 217, f_people_booked: 171, f_leads_booked: 177 },
  cmp: { people: 191, leads: 199, people_done: 149, leads_done: 153, people_booked: 121, leads_booked: 125, people_dq: 12, leads_dq: 14, people_b2c: 5, leads_b2c: 6, people_waitlist: 6, leads_waitlist: 7, people_blocked: 14, leads_blocked: 15, people_withheld: 4, leads_withheld: 5, f_people: 189, f_leads: 197, f_people_done: 147, f_leads_done: 151, f_people_booked: 119, f_leads_booked: 123 },
  month: { people: 1043, leads: 1097, people_booked: 703, leads_booked: 711 },
};
const S = { queries: [], fetches: [], dbDead: false };
function rowsFor(sql, params) {
  const flat = sql.replace(/\s+/g, ' ');
  if (/AS d0,/.test(flat) && /AS first_lead/.test(flat)) return [B];
  if (/best AS \(SELECT k, person, MIN\(stage\)/.test(flat)) {
    /* windows arrive as bound VALUES triples: k, s, e */
    const ks = []; for (let i = 0; i + 2 < params.length; i += 3) ks.push(params[i]);
    return ks.map((k) => Object.assign({ k }, KPI[k] || KPI.cur));
  }
  if (/FROM w LEFT JOIN form_sessions/.test(flat)) {
    /* params: BOT_RE, then k, s, e triples. The funnel window of the Today
       and Week views IS the current window, so it gets the same count. */
    const w = {}; for (let i = 1; i + 2 < params.length; i += 3) w[params[i]] = params[i + 1] + '|' + params[i + 2];
    return Object.keys(w).map((k) => ({ k, sessions: k === 'cur' || (k === 'fun' && w.fun === w.cur) ? 5903 : k === 'cmp' ? 4697 : 31577 }));
  }
  if (/'bucket' AS kind/.test(flat)) {
    const grain = /date_trunc\('hour'/.test(flat) ? 'hour' : /date_trunc\('day'/.test(flat) ? 'day' : 'month';
    const out = [];
    if (grain === 'day') {
      [['2026-09-21', 41], ['2026-09-22', 47], ['2026-09-23', 44], ['2026-09-24', 81], ['2026-09-25', 53]].forEach(([b, n]) => out.push({ kind: 'bucket', k: 'cur', bucket: b, people: n, leads: n + 2, people_booked: n - 10, leads_booked: n - 9 }));
      [['2026-09-14', 42], ['2026-09-15', 49], ['2026-09-16', 44], ['2026-09-17', 42], ['2026-09-18', 42], ['2026-09-19', 31], ['2026-09-20', 40]].forEach(([b, n]) => out.push({ kind: 'bucket', k: 'prev', bucket: b, people: n, leads: n, people_booked: n - 12, leads_booked: n - 12 }));
      out.push({ kind: 'repeats', k: 'cur', bucket: null, people: 3, leads: 3 });
    } else if (grain === 'hour') {
      for (let h = 0; h <= 14; h++) out.push({ kind: 'bucket', k: 'cur', bucket: String(h).padStart(2, '0'), people: (h % 5) + 1, leads: (h % 5) + 1, people_booked: 1, leads_booked: 1 });
      for (let h = 0; h < 24; h++) out.push({ kind: 'bucket', k: 'prev', bucket: String(h).padStart(2, '0'), people: (h % 4) + 2, leads: (h % 4) + 2, people_booked: 1, leads_booked: 1 });
      out.push({ kind: 'repeats', k: 'cur', bucket: null, people: 1, leads: 1 });
    } else {
      [['2026-03', 31], ['2026-04', 337], ['2026-05', 761], ['2026-06', 979], ['2026-07', 1297], ['2026-08', 1023], ['2026-09', 1043]].forEach(([b, n]) => out.push({ kind: 'bucket', k: 'cur', bucket: b, people: n, leads: n + 50, people_booked: Math.round(n * 0.67), leads_booked: Math.round(n * 0.67) }));
      out.push({ kind: 'repeats', k: 'cur', bucket: null, people: 139, leads: 167 });
    }
    return out;
  }
  if (/first AS \(/.test(flat)) return [{ unit: 'people', source: 'Google', n: 29 }, { unit: 'people', source: 'Meta', n: 197 }, { unit: 'people', source: 'Direct / organic', n: 31 }, { unit: 'people', source: 'LinkedIn', n: 3 }, { unit: 'people', source: 'Partner / referral', n: 3 }, { unit: 'leads', source: 'Meta', n: 211 }, { unit: 'leads', source: 'Google', n: 33 }];
  if (/MAX\(created_at\) AS last_lead_at/.test(flat)) return [{ last_lead_at: '2026-09-25T18:16:42.000Z' }];
  if (/AS recovered FROM \(/.test(flat)) return [{ recovered: '53' }];
  return [];
}
function stubQuery(q, p) {
  const sql = typeof q === 'string' ? q : (q && q.text) || '';
  const params = p || (q && q.values) || [];
  S.queries.push({ sql, params });
  if (S.dbDead) throw new Error('connection refused');
  return { rows: rowsFor(sql, params), rowCount: 1 };
}
class StubClient { async query(q, p) { return stubQuery(q, p); } release() {} }
class StubPool { async connect() { return new StubClient(); } async query(q, p) { return stubQuery(q, p); } on() {} async end() {} }
const origLoad = Module._load;
Module._load = function (request) {
  if (request === 'pg') return { Pool: StubPool, Client: StubClient, types: { setTypeParser() {} } };
  return origLoad.apply(this, arguments);
};
const realFetch = global.fetch.bind(global);
global.fetch = async function (url, opts) {
  const u = String(url);
  if (u.startsWith(BASE)) return realFetch(url, opts);
  S.fetches.push(u);
  return { ok: true, status: 200, json: async () => ({}), text: async () => '{}' };
};
Object.assign(process.env, { PORT: String(PORT), DATABASE_URL: 'postgres://stub/stub', MONITOR_TOKEN: TOKEN, ALLOWED_ORIGIN: 'https://www.gushwork.ai' });
const realLog = console.log, realWarn = console.warn, realErr = console.error;
const quiet = () => { console.log = console.warn = console.error = () => {}; };
const loud = () => { console.log = realLog; console.warn = realWarn; console.error = realErr; };
quiet();
require(path.join(ROOT, 'index.js'));
const tq = '?token=' + encodeURIComponent(TOKEN);

/* ── A stubbed browser: enough DOM for the page's own scripts to run ───── */
function browser(payloads, cfg) {
  const els = {}, listeners = {}, wlisteners = {}, intervals = [], calls = [];
  const mk = (id) => {
    const attrs = {}, cls = new Set();
    const e = {
      id, _html: '', style: {}, clientWidth: 960, hidden: false,
      get innerHTML() { return this._html; }, set innerHTML(v) { this._html = String(v); },
      get textContent() { return this._html.replace(/<[^>]*>/g, ''); }, set textContent(v) { this._html = String(v); },
      classList: { add: (c) => cls.add(c), remove: (c) => cls.delete(c), contains: (c) => cls.has(c), toggle: (c, on) => { if (on === undefined ? !cls.has(c) : on) cls.add(c); else cls.delete(c); } },
      setAttribute: (k, v) => { attrs[k] = String(v); }, getAttribute: (k) => (k in attrs ? attrs[k] : null), removeAttribute: (k) => { delete attrs[k]; }, hasAttribute: (k) => k in attrs,
      querySelector: () => null, querySelectorAll: () => [], contains: () => false, focus() {}, addEventListener() {}, closest: () => null,
      appendChild(c) { c.parentNode = this; return c; }, removeChild(c) { c.parentNode = null; return c; }, parentNode: null,
    };
    return e;
  };
  const document = {
    getElementById: (id) => els[id] || (els[id] = mk(id)),
    querySelector: () => null, querySelectorAll: () => [],
    documentElement: mk('__html'), body: mk('__body'), head: mk('__head'), activeElement: null, title: '', visibilityState: 'visible',
    addEventListener: (t, f) => { (listeners[t] = listeners[t] || []).push(f); },
    createElement: () => mk('__created'), fonts: { ready: Promise.resolve(), check: () => true },
  };
  const window = {
    location: { hash: '' }, history: { replaceState: (a, b, h) => { window.location.hash = h; } },
    localStorage: { getItem: () => null, setItem() {} }, matchMedia: () => ({ matches: false, addEventListener() {} }),
    addEventListener: (t, f) => { (wlisteners[t] = wlisteners[t] || []).push(f); }, innerWidth: 1440, scrollTo() {}, alert() {},
    /* THE SERVED PAGE'S OWN CONFIG when given -- it carries the server's
       label maps, so the page is tested with what a real visitor gets */
    __GW__: cfg || { token: TOKEN, tz: 'America/New_York', classic: '/monitor' },
  };
  const fetchStub = async (url, init) => {
    const u = new URL(String(url), 'http://x');
    calls.push({ path: u.pathname, q: Object.fromEntries(u.searchParams), method: (init && init.method) || 'GET', body: init && init.body, headers: init && init.headers });
    const body = payloads(u.pathname, Object.fromEntries(u.searchParams));
    if (body instanceof Error) return { ok: false, status: 503, json: async () => ({}), text: async () => body.message };
    return { ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) };
  };
  const setIntervalStub = (fn, ms) => { intervals.push({ fn, ms }); return intervals.length; };
  return { els, listeners, wlisteners, intervals, calls, document, window, fetch: fetchStub, setInterval: setIntervalStub };
}
function run(js, b) {
  const GWo = new Function('window', 'document', 'fetch', 'setInterval', 'clearInterval', 'setTimeout', 'clearTimeout', 'AbortSignal', 'URL', 'Blob', 'Intl',
    js + '\n;return GW;')(b.window, b.document, b.fetch, b.setInterval, () => {}, (f) => { f(); return 0; }, () => {}, { timeout: () => undefined }, URL, function () {}, Intl);
  return GWo;
}
const ticks = async (n = 12) => { for (let i = 0; i < n; i++) await new Promise((r) => setImmediate(r)); };
const nums = (html) => [...html.matchAll(/data-v="([^"]*)"/g)].map((m) => m[1]);

(async () => {
  await sleep(900);
  loud();

  /* ═══ 1. THE ROUTES ═══════════════════════════════════════════════════ */
  const noTok = await realFetch(BASE + '/monitor/next');
  eq('route: no token is 401', noTok.status, 401);
  const r = await realFetch(BASE + '/monitor/next' + tq);
  const html = await r.text();
  eq('route: the page is 200', r.status, 200);
  ok('route: served as HTML, never cached', /text\/html/.test(r.headers.get('content-type')) && /no-store/.test(r.headers.get('cache-control') || ''));
  const mn = require(path.join(ROOT, 'monitor-next.js'));
  for (const f of mn.JS_ORDER) ok(`route: ${f} is in the page`, html.includes('/* ---- ' + f + ' ---- */'));
  ok('route: the scripts are in the documented order', mn.JS_ORDER.every((f, i, a) => i === 0 || html.indexOf('/* ---- ' + f) > html.indexOf('/* ---- ' + a[i - 1])));
  /* The config script: the token rides in JSON with "<" escaped, so a value
     can never close the script tag it sits in. */
  const cfgTag = (html.match(/<script>window\.__GW__=[\s\S]*?<\/script>/) || [''])[0];
  ok('route: a hostile token cannot close the config script', cfgTag && !/<\/script><b>/.test(cfgTag.slice(0, -9)) && /\\u003c\/script>/.test(cfgTag), cfgTag.slice(0, 160));
  ok('route: the config carries the timezone as ET', /"tz":"America\/New_York"/.test(cfgTag));
  ok('route: fonts load through the token-gated asset route', /\/monitor\/next\/asset\/Inter-VariableFont_opsz_wght\.ttf\?token=/.test(html));
  ok('route: the page is noindex', /name="robots" content="noindex,nofollow"/.test(html));
  const font = await realFetch(BASE + '/monitor/next/asset/Vert_Grotesk_Display_VF.ttf' + tq);
  eq('asset: an allowlisted font is 200', font.status, 200);
  ok('asset: served as a font', /font\/ttf/.test(font.headers.get('content-type') || ''));
  eq('asset: no token is 401', (await realFetch(BASE + '/monitor/next/asset/Vert_Grotesk_Display_VF.ttf')).status, 401);
  eq('asset: a name not on the list is 404', (await realFetch(BASE + '/monitor/next/asset/app.css' + tq)).status, 404);
  eq('asset: a path traversal is 404', (await realFetch(BASE + '/monitor/next/asset/..%2F..%2Findex.js' + tq)).status, 404);
  eq('asset: an inherited object key is 404, not a crash', (await realFetch(BASE + '/monitor/next/asset/constructor' + tq)).status, 404);
  /* The old dashboard, both directions */
  const old = await (await realFetch(BASE + '/monitor' + tq)).text();
  ok('classic: links to the new dashboard', /href="\/monitor\/next\?token=/.test(old));
  ok('classic: opens at #tab= when sent from the new one', /match\(\/tab=\(\[a-z\]\+\)\/\)/.test(old) && /showTab\(m\[1\]\)/.test(old));
  ok('classic: the disqualified card no longer says only "B2C / Mixed"', !/"B2C \/ Mixed \\u00B7 "/.test(old) && /B2C, mixed or waitlist/.test(old));

  /* ═══ 2. /monitor/overview ═════════════════════════════════════════════ */
  eq('overview: no token is 401', (await realFetch(BASE + '/monitor/overview?view=week')).status, 401);
  const bad = await realFetch(BASE + '/monitor/overview' + tq + '&view=week&asof=not-a-date');
  eq('overview: a bad asof is refused with 400', bad.status, 400);
  const P = {};
  for (const v of ['today', 'week', 'all']) {
    S.queries = [];
    const res = await realFetch(BASE + '/monitor/overview' + tq + '&view=' + v + '&asof=' + encodeURIComponent(ASOF));
    eq(`overview/${v}: 200`, res.status, 200);
    P[v] = await res.json();
    ok(`overview/${v}: the asof is bound, never pasted into SQL`, S.queries.every((q) => !q.sql.includes('2026-09-25T18:52')), '');
    eq(`overview/${v}: the view comes back`, P[v].view, v);
  }
  /* A hostile view never reaches SQL: it is whitelisted to a grain first. */
  S.queries = [];
  const inj = await (await realFetch(BASE + '/monitor/overview' + tq + '&view=' + encodeURIComponent("today'); DROP TABLE leads; --"))).json();
  eq('overview: an unknown view falls back to the week', inj.view, 'week');
  ok('overview: a hostile view string never reaches SQL', S.queries.length > 0 && S.queries.every((q) => !/DROP TABLE/.test(q.sql) && !(q.params || []).some((x) => /DROP TABLE/.test(String(x)))));
  S.queries = [];
  const fut = await (await realFetch(BASE + '/monitor/overview' + tq + '&view=today&asof=2099-01-01T00:00:00Z')).json();
  ok('overview: a future asof is clamped to now', new Date(fut.asof).getTime() <= Date.now() + 1000, fut.asof);
  const W = P.week;
  eq('overview/week: people this week, from the cur window', W.kpi.people.people[0], KPI.cur.people);
  eq('overview/week: people at the same point last week, from cmp', W.kpi.people.people[1], KPI.cmp.people);
  eq('overview/week: leads', W.kpi.people.leads[0], KPI.cur.leads);
  eq('overview/week: completed = submitted, people', W.kpi.completed.people[0], KPI.cur.people_done);
  eq('overview/week: booked, leads unit', W.kpi.booked.leads[1], KPI.cmp.leads_booked);
  eq('overview/week: disqualified from the ladder', W.kpi.dq.people[0], KPI.cur.people_dq);
  eq('overview/week: the waitlist split', W.kpi.waitlist.people[0], KPI.cur.people_waitlist);
  eq('overview/week: blocked from the ladder', W.kpi.blocked.leads[0], KPI.cur.leads_blocked);
  eq('overview/week: meta withheld', W.kpi.withheld.people[0], KPI.cur.people_withheld);
  eq('overview/week: sessions, cur', W.sessions[0], 5903);
  ok('overview: the payload calls them sessions, never page loads', !('page_loads' in W) && W.funnel.sessions === 5903);
  eq('overview/week: the funnel reads the form-only columns', W.funnel.people.step1, KPI.cur.f_people);
  eq('overview/week: seven day slots, generated', W.series.slots.length, 7);
  eq('overview/week: the slots start on the Monday', W.series.slots[0], '2026-09-21');
  eq('overview/week: last week’s series is there', W.series.prev.people['2026-09-19'], 31);
  eq('overview/week: repeat people for the sum note', W.series.repeats.repeaters, 3);
  eq('overview/week: channels sorted largest first', W.channels.people[0].name, 'Meta');
  eq('overview/week: last lead time', W.last_lead_at, '2026-09-25T18:16:42.000Z');
  eq('overview/today: 24 hour slots, generated', P.today.series.slots.length, 24);
  eq('overview/today: the slots are 00..23', P.today.series.slots.join(','), Array.from({ length: 24 }, (_, i) => String(i).padStart(2, '0')).join(','));
  eq('overview/all: months from the first lead to now', P.all.series.slots.join(','), '2026-03,2026-04,2026-05,2026-06,2026-07,2026-08,2026-09');
  eq('overview/all: month on month, this month', P.all.month.people[0], KPI.month.people);
  eq('overview/all: month on month, last month at the same point', P.all.month.people[1], KPI.cmp.people);
  eq('overview/all: recovered bookings from the shared definition', P.all.recovered, 53);
  eq('overview/all: no all-time sessions (tracking began at go_live)', P.all.sessions[0], null);
  eq('overview/all: the funnel starts at go_live', P.all.funnel.since, B.go_live);
  /* /monitor/duplicates, through the booted route: the SQL that reaches the
     database groups by lower(email) ALONE -- the dedup key, always -- and
     carries the one internal-lead clause every outbound guard asks. */
  S.queries = [];
  const dup = await realFetch(BASE + '/monitor/duplicates' + tq);
  const dq = S.queries.map((q) => q.sql.replace(/\s+/g, ' ')).find((q) => /HAVING COUNT\(\*\) > 1/.test(q)) || '';
  eq('duplicates: the route answers', dup.status, 200);
  ok('duplicates: grouped by lower(email) alone, never the raw address too', /GROUP BY LOWER\(l\.email\) HAVING/.test(dq) && /MIN\(l\.email\) AS email/.test(dq), dq.slice(0, 200));
  ok('duplicates: is_internal is the shared clause, with its params bound', /bool_or\(\(LOWER\(l\.email\) = ANY\(\$1::text\[\]\)/.test(dq));
  S.dbDead = true;
  const dead = await realFetch(BASE + '/monitor/overview' + tq + '&view=week');
  S.dbDead = false;
  eq('overview: a dead database is a 500, never a page of zeros', dead.status, 500);

  /* ═══ 3. THE PAGE, EXECUTED ════════════════════════════════════════════ */
  const js = (html.match(/<script>(\/\* ---- core\.js[\s\S]*?)<\/script><\/body>/) || [])[1] || '';
  ok('page: the inline script was found', js.length > 20000, js.length);
  ok('page: the rail\'s BACKGROUND runs the page, its CONTENTS stick', /<aside class="sidebar"><div class="side-in">/.test(html) &&
     /\.side-in \{ position: sticky;/.test(fs.readFileSync(path.join(ROOT, 'monitor', 'app.css'), 'utf8')) && !/\.sidebar \{[^}]*position: sticky/.test(fs.readFileSync(path.join(ROOT, 'monitor', 'app.css'), 'utf8')));
  ok('page: the skip link is its own class, visible when focused', /<a class="skip" href="#view">/.test(html) && /\.skip:focus \{[^}]*position: fixed/.test(fs.readFileSync(path.join(ROOT, 'monitor', 'app.css'), 'utf8')));
  ok('page: ONE live region, in the shell rather than in a tab', (html.match(/id="gw-live"/g) || []).length === 1 && /id="gw-live" class="sr-only" role="status" aria-live="polite"/.test(html));
  const HEALTH = { checks: {
    apollo: { state: 'red', text: 'Out of credits for 2d', detail: 'You have insufficient credits! · 86 refused in the last 24h · Last enrichment 2d ago', summary: '86 refused in the last 24h · Last enrichment 2d ago' },
    partial: { state: 'green', text: '10 leads saved in the last 2h' }, submit: { state: 'green', text: '78 completions' },
    booking: { state: 'green', text: '85% booked' }, cron: { state: 'insufficient_data', text: 'No run yet' }, aws: { state: 'green', text: 'In sync' },
    recovery: { state: 'green', text: '78 follow-ups' }, partnerstack: { state: 'insufficient_data', text: 'No partner activity' }, nonicpllm: { state: 'amber', text: '80 verdicts' },
  } };
  const DROP = { from: '2026-07-06', to: '2026-09-25', grain: 'week', mode: 'leads', unit: 'leads', generated_at: ASOF,
    periods: [{ key: '2026-09-14', label: 'Sep 14', partial: false }, { key: '2026-09-21', label: 'Sep 21', partial: true }],
    rows: [{ key: '1_booked', label: 'Booked', desc: 'Picked a time', tone: 'good', counts: { '2026-09-14': 191, '2026-09-21': 173 }, total: 364 },
           { key: '7_drop_step1', label: 'Left on step 2', desc: 'Finished step 1', tone: 'neu', counts: { '2026-09-14': 47, '2026-09-21': 39 }, total: 86 }],
    totals: { '2026-09-14': 238, '2026-09-21': 212 }, grand: 450, booked: 364, not_booked: 86,
    booked_rate: { '2026-09-14': 80.3, '2026-09-21': 81.6 }, grand_booked_rate: 80.9, sources: [{ name: 'Meta', n: 331 }], internal: 7, recovered: 29 };
  const DUPES = { total: 2, leads: [
    { email: 'darshil.dixit@gushwork.ai', session_count: 27, has_booking: 1, has_completed: 1, first_seen: '2026-03-20T13:36:00Z', last_seen: '2026-09-15T19:04:00Z', is_internal: true, sessions: [] },
    { email: 'peter<img src=x onerror=alert(1)>@flighted.co', session_count: 7, has_booking: 1, has_completed: 1, first_seen: '2026-03-23T15:56:00Z', last_seen: '2026-09-14T13:10:00Z', is_internal: false, sessions: [{ session_id: 'abcdef0123456789', booking_uid: 'b', created_at: '2026-09-14T13:10:00Z', page_url: '/demo' }] }] };
  const LM_M = { funnel: { views: 23, modal_opens: 11, emails: 7, submitted: 5, people: 7, people_submitted: 5, people_abandoned: 2, bounced_before_open: 12, opened_no_email: 4, abandoned: 2, business_email: 4, free_email: 3 },
    industries: [{ label: 'SaaS', n: 3 }], entry_points: [], custom_categories: [], daily: [{ day: '2026-09-24', views: 9, emails: 3, submitted: 2 }],
    statusTotals: { all: 613, awaiting: 17, sent: 571, abandoned: 25, internal: 4 } };
  const LM_L = { total: 900, leads: [{ id: 1, email: 'a@b.co', status: 'awaiting', industry_category: 'SaaS', created_at: ASOF }] };
  const COV = { ok: false, reason: 'salesforce_timeout', held: 431, submitted: 947, days: 30 };
  const METRICS = { enriched: 4537, enrichTitlePct: 55, enrichFundingPct: 8, enrichLocationPct: 63, peopleNonIcp: 42 };
  /* ── PR C fixtures: odd, distinct numbers so a painted value cannot match by accident ── */
  const SID_A = '3f2a9c1e-7b4d-4e21-9a0c-5d6e7f8a9b0c', SID_B = '8c1d2e3f-4a5b-4c6d-8e9f-0a1b2c3d4e5f', SID_C = '1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d';
  const LEAD_A = { session_id: SID_A, email: 'ann@acmerealty.com', first_name: 'Ann', last_name: 'Acme', company: 'Acme Realty', sell_to: 'B2B (clarified from B2C)', product: 'aeo',
    completed: true, booking_uid: null, disqualified: false, non_icp_blocked: true, non_icp_source: 'llm_name_only', non_icp_reason: 'acmerealty.com', meta_withheld_reason: 'blocked', is_internal: false,
    website_check_failed: true, website_check_reason: 'parked_confirmed', created_at: '2026-09-24T14:05:00Z', start_time: 'not a date', enriched_linkedin: 'javascript:alert(1)',
    ip_city: 'Boston', ip_region: 'Massachusetts', ip_country: 'US', enriched_city: 'Woburn', enriched_state: 'Massachusetts', enriched_country: 'United States',
    prior_attempts: '2', prior_disqualified: '1', utm_source: 'facebook', utm_medium: 'paid',
    ps_partner_key: 'pk_77', ps_partner_name: null, ps_partner_email: 'p@partner.co', ps_signup_fail_reason: 'http_400',
    ps_click_history: JSON.stringify([{ at: '2026-09-20T10:00:00Z', pk: 'pk_11', xid: 'x1' }, { at: '2026-09-21T10:00:00Z', pk: 'pk_77', xid: 'x2' }]) };
  const LEAD_B = { session_id: SID_B, email: 'darshil@gushwork.ai', first_name: 'D', company: 'Gushwork', sell_to: 'B2B', product: 'crm', completed: true, booking_uid: 'bk_1',
    is_internal: true, meta_withheld_reason: 'internal', created_at: '2026-03-02T15:00:00Z', enriched_title: 'Founder' };
  const LEAD_C = { session_id: SID_C, email: 'cy@shop.example', sell_to: 'B2C', product: 'aeo', completed: false, booking_uid: null, meta_withheld_reason: 'model',
    referrer: 'https://www.google.com/search?q=x', created_at: '2026-09-25T11:00:00Z' };
  const LEADS = { total: 4471, page: 2, pages: 179, leads: [LEAD_A, LEAD_B, LEAD_C] };
  const BLOCKED = { total: 51, page: 1, pages: 3, leads: [LEAD_A] };
  /* THE MODEL PAYLOAD: cache is TOP-LEVEL and scrape.cache does not exist,
     so reading the wrong one can only paint nothing, never the right number */
  const MODEL = { windowDays: 30, product: 'crm', generatedAt: ASOF, truncated: true,
    flags: { list_block: true, llm_enabled: true, llm_block: true, llm_meta: true, model: 'claude-x', confidence_floor: 0.75, prompt_version: 'v9' },
    ladder: { total: 981, ours: 17, rows: [
      { key: 'blocked_list', label: 'Blocked — brand list', n: 137, ours: 3, pct: 14 }, { key: 'blocked_model', label: 'Blocked — model', n: 29, ours: 0, pct: 3 },
      { key: 'meta_only', label: 'Meta withheld only', n: 41, ours: 0, pct: 4.2 }, { key: 'checked_clear', label: 'Checked, no action', n: 563, ours: 14, pct: 57.4 },
      { key: 'not_decided', label: 'Not decided', n: 211, ours: 0, pct: 21.5 }] },
    industries: [
      { key: 'blocked_list', label: 'Blocked by the brand-domain list', note: 'A string comparison.', leads: 137, rows: [{ business_type: 'insurance', label: 'Insurance', leads: 101, domains: 67, median_confidence: 0.99, uncategorised: false }, { business_type: '_uncategorised', label: 'Not categorised — no verdict for the domain that blocked them', leads: 36, domains: 0, median_confidence: null, uncategorised: true }] },
      { key: 'blocked_model', label: 'Blocked by the model', note: 'The model read the site.', leads: 0, rows: [] },
      { key: 'meta_only', label: 'Meta withheld by the model — not blocked', note: 'They booked.', leads: 41, rows: [{ business_type: 'home_services', label: 'Home services / trades', leads: 41, domains: 38, median_confidence: 0.84, uncategorised: false }] }],
    cache: { domains: 2975, judged: 2965, unreadable: 10, byType: [{ business_type: 'real_estate', label: 'Real estate', action: 'block', domains: 141 }, { business_type: 'home_services', label: 'Home services / trades', action: 'meta', domains: 224 }] },
    decisions: [
      { session_id: 'md-1', created_at: '2026-09-24T14:00:00Z', email: 'agent@allstate.com', is_internal: true, action: 'blocked_list', booked: false, business_type_label: 'Insurance', confidence: 0.99, evidence_quote: '<img src=x onerror=alert(3)> We sell insurance', domain_judged: 'allstate.com', source: 'domain_list', reason: 'insurer' },
      { session_id: 'md-2', created_at: '2026-09-23T14:00:00Z', email: 'jo@plumbco.com', is_internal: false, action: 'meta_only', booked: true, business_type_label: 'Home services / trades', confidence: 0.84, evidence_quote: 'We fix pipes', domain_judged: 'plumbco.com', source: 'llm' }],
    scrape: { window: { ok: 9, unreachable: 1, thin: 2, other: 0, no_verdict: 7, total: 19, answered: 12, unreadable_pct: 25 },
      unreadable: [{ domain: 'nosite.test', scrape_status: 'thin', error: null, checked_at: '2026-09-24T10:00:00Z', email: 'e@nosite.test', website: 'nosite.test', blocked: true, blocked_by: 'llm_name_only' }],
      inProcess: { ok: 23, errored: 1, unreachable: 2, cacheHits: 8, cacheMisses: 2, cacheHitPct: 80 }, notes: ['Latest outcome per domain, not a historical rate.'] },
    nearMisses: { band: 0.1, floors: { page: 0.75, name: 0.9 }, total: 13, withLeads: 2, rows: [{ domain: 'nearly.io', label: 'Real estate', confidence_pct: 71, floor_pct: 75, judged_from: 'their website', leads: 2 }] } };
  /* Visitors: the classic suite's own odd numbers, places deliberately NOT in lead order */
  const VIS = { window_days: 90, coverage: { leads: 4471, with_address: 3312, with_place: 3301, address_only: 11, distinct_addresses: 2287 },
    repeats: { total: 9, shown: 2, rows: [
      { ip_address: '203.0.113.45', leads: 7, people: 3, city: 'Boston', region: 'Massachusetts', country: 'US', isp: 'Verizon Business', first_seen: '2026-06-01T12:00:00Z', last_seen: '2026-09-20T12:00:00Z', emails: ['a@x.com', 'b@x.com'] },
      { ip_address: '2001:db8::1', leads: 2, people: 2, city: null, region: null, country: null, isp: null, first_seen: '2026-07-01T12:00:00Z', last_seen: '2026-08-01T12:00:00Z', emails: null }] },
    places: { mappable: 2, rows: [
      { city: 'Denver', region: 'Colorado', country: 'US', lat: 39.74, lon: -104.99, leads: 153, people: 150, booked: 90 },
      { city: 'Boston', region: 'Massachusetts', country: 'US', lat: 42.36, lon: -71.06, leads: 612, people: 590, booked: 402 },
      { city: 'Austin', region: 'Texas', country: 'US', lat: null, lon: null, leads: 77, people: 71, booked: 40 }] },
    networks: [{ isp: 'Verizon Business', org_domain: 'verizonbusiness.com', leads: 829, people: 800, booked: 511 }, { isp: 'Verizon Business', org_domain: 'frontiernet.net', leads: 11, people: 10, booked: 4 },
      { isp: 'AT&T Enterprises, LLC', org_domain: 'att.com', leads: 37, people: 36, booked: 20 }],
    timezones: [{ timezone: 'America/New_York', leads: 937, booked: 604 }, { timezone: 'Not/AZone', leads: 3, booked: 1 }] };
  /* Partners: an internally consistent lifecycle -- the eight states add up
     to the total, the Salesforce splits add up to the state they split */
  const ago90 = new Date(Date.now() - 90 * 60000).toISOString();
  const PD = [
    { customer_key: 'fail.co', state: 'conversion_failed', partner_display: 'Alpha Partners', partner_key_count: 2, partner_others: ['Beta Co'], signup_fail_reason: 'http_400', acknowledged: false, sf_state: 'create_errored', last_seen: '2026-09-24T10:00:00Z' },
    { customer_key: 'acked.co', state: 'conversion_failed', partner_display: 'Alpha Partners', partner_key_count: 1, signup_fail_reason: 'phantom_200', acknowledged: true, ack_note: 'known test', sf_state: null, last_seen: '2026-09-23T10:00:00Z' },
    { customer_key: 'paid.co', state: 'qualified', partner_display: 'Alpha Partners', sf_state: 'ticked', qualified_sent: true, signup_sent: true, signup_verified: true, last_seen: '2026-09-22T10:00:00Z' },
    { customer_key: 'wait.co', state: 'converted', partner_display: 'Alpha Partners', sf_state: 'ticked', qualified_sent: false, signup_sent: true, signup_verified: false, last_seen: '2026-09-21T10:00:00Z' },
    { customer_key: 'ae.co', state: 'awaiting_demo', partner_display: 'b@beta.test', sf_state: 'exists_unticked', signup_sent: true, qualified_sent: false, last_seen: '2026-09-20T10:00:00Z' },
    { customer_key: 'nosent.co', state: 'awaiting_demo', partner_display: 'b@beta.test', sf_state: 'exists_unticked', signup_sent: false, qualified_sent: false, last_seen: '2026-09-19T10:00:00Z' },
    { customer_key: 'afterpaid.co', state: 'qualified', partner_display: 'b@beta.test', sf_state: 'exists_unticked', signup_sent: true, qualified_sent: true, last_seen: '2026-09-18T10:00:00Z' }];
  const PARTNERS = { totals: { leads: 12, leads24h: 3, conversions: 5, qualified: 2 },
    lifecycle: { needsAttention: 5, acknowledged: 1, needsAttentionComplete: true, totalDomains: 7, domains: PD, byState: { conversion_failed: 2, qualified: 2, converted: 1, awaiting_demo: 2 },
      noCustomerKeyLeads: 3, bySfState: { ticked: 2, exists_unticked: 3, create_errored: 1 }, sfActionable: 1, sfUnactionable: 1, sfUntickedAfterPaid: 1,
      sfNewestCheckedAt: ago90, sfStaleAfterMin: 45, sfLastRead: { ok: false, reason: 'incomplete' }, domainsCapped: false, domainsLimit: 500 },
    funnel: { rateMin: 10, stages: [{ key: 'clicks', label: 'Clicks', unit: true }, { key: 'step1', label: 'Reached step 1' }, { key: 'completed', label: 'Completed the form' },
      { key: 'conversions', label: 'Conversion sent', abs: 'abs_conversions' }, { key: 'verified', label: 'Conversion verified', abs: 'abs_verified' }, { key: 'booked', label: 'Booked', abs: 'abs_booked' },
      { key: 'opportunity', label: 'Opportunity created', abs: 'abs_opportunity' }, { key: 'ticked', label: 'Qualified Demo ticked', abs: 'abs_ticked' }, { key: 'qualified', label: 'The $50 fired', abs: 'abs_qualified' }],
      losses: { conversions: [{ key: 'lost_conversion', label: 'conversion failed', bad: true }, { key: 'lost_skipped', label: 'skipped' }] },
      programme: { clicks: '14', step1: '40', completed: '32', conversions: '20', abs_conversions: '20', verified: '16', abs_verified: '16', booked: '9', abs_booked: '9',
        opportunity: '0', abs_opportunity: '1', ticked: '0', abs_ticked: '1', qualified: '0', abs_qualified: '1', lost_conversion: '2', lost_skipped: '0' } },
    partners: [{ partner_key: 'pk_a', partner_name: 'Alpha', step1: '7', completed: '6', conversions: '4', abs_conversions: '4', verified: '4', abs_verified: '4', booked: '0', abs_booked: '0', opportunity: '0', abs_opportunity: '0', ticked: '0', abs_ticked: '0', qualified: '0', abs_qualified: '2', clicks: 6 },
      { partner_key: 'pk_b', partner_name: null, partner_email: 'b@beta.test', step1: '9', completed: '1', conversions: '1', abs_conversions: '1', verified: '1', abs_verified: '1', booked: '1', abs_booked: '1', opportunity: '0', abs_opportunity: '0', ticked: '0', abs_ticked: '0', qualified: '0', abs_qualified: '0', clicks: null }] };
  const GAPS = { missedConversions: [{ customer_key: 'miss.co', partner_display: 'Alpha Partners', email: 'x@miss.co', first_seen: '2026-09-20T10:00:00Z' }], missingOpportunity: [],
    opportunityCheck: { ok: false, reason: 'sf_down' }, skipped: [{ customer_key: 'test.com', reason: 'test_email' }], awaitingQualification: 5, graceDays: 3 };
  let gapsDown = false;
  const SDR = { total: 37, leads: [
    { email: 'Ann@Acme.co', first_name: 'Ann', last_name: 'Lee', company: 'Acme Widgets', enriched_industry: 'Manufacturing', completed: true, created_at: '2026-09-24T12:00:00Z', ps_partner_name: 'Alpha Partners', hear_about_us_raw: 'a podcast', enriched_linkedin: 'javascript:alert(2)', phone: '+14155550134' },
    { email: 'bo@zeta.io', first_name: 'Bo', company: 'Zeta', completed: false, created_at: '2026-09-23T12:00:00Z' },
    { email: 'cy@omega.com', first_name: 'Cy', company: 'Omega', enriched_industry: 'Software', completed: false, created_at: '2026-09-22T12:00:00Z' }] };
  const CHANGES = { [SID_A]: { ok: true, changes: [{ field: 'sell_to', old_value: 'B2C', new_value: 'B2B (clarified from B2C)', attribution: 'ours_sell_to_clarified', source_route: '/partial', arrived_step: 1, booking_uid_present: false, changed_at: '2026-09-24T14:06:00Z' }] },
    [SID_B]: { ok: false, unavailable: true }, [SID_C]: { ok: true, changes: [] } };
  let healthDown = false, leadsHook = null, ackDown = false;
  const payloads = (p, q) => {
    if (leadsHook && p === '/monitor/leads') { const r = leadsHook(q); if (r) return r; }
    if (p === '/monitor/overview') return P[q.view] || P.week;
    if (p === '/monitor/health') return healthDown ? new Error('upstream down') : HEALTH;
    if (p === '/monitor/elv-health') return { state: 'insufficient_data', rate: 0, checks: 0 };
    if (p === '/monitor/dropoff') return DROP;
    if (p === '/monitor/duplicates') return DUPES;
    if (p === '/monitor/lm-metrics') return LM_M;
    if (p === '/monitor/lm-leads') return LM_L;
    if (p === '/monitor/enrichment-coverage') return COV;
    if (p === '/monitor/metrics') return METRICS;
    if (p === '/monitor/sdr') return SDR;
    if (p === '/monitor/non-icp') return MODEL;
    if (p === '/monitor/visitors') return VIS;
    if (p === '/monitor/partners') return PARTNERS;
    if (p === '/monitor/partner-gaps') return gapsDown ? new Error('upstream down') : GAPS;
    if (p === '/monitor/partner-ack') return ackDown ? new Error('refused') : { ok: true };
    if (p === '/monitor/leads') return q.nonicp === 'only' ? (q.internal === 'exclude' ? { total: 45, page: 1, pages: 2, leads: [] } : BLOCKED) : LEADS;
    if (p === '/monitor/filter-options') return { hearAbout: ['Podcast'], utmSource: ['facebook', 'google'], partners: [{ key: 'pk_77', name: null, email: 'p@partner.co' }] };
    if (p === '/monitor/lead-changes') return CHANGES[q.session_id] || { ok: true, changes: [] };
    if (p === '/health') return { ok: true };
    return {};
  };
  const servedCfg = JSON.parse((cfgTag.match(/window\.__GW__=(.*);<\/script>$/) || [])[1] || 'null');
  ok('page: the config carries the server\'s label maps', servedCfg && servedCfg.labels && servedCfg.labels.website && servedCfg.labels.website.parked_confirmed === 'Domain registered but no website on it' && servedCfg.labels.meta.blocked === 'Blocked — non-ICP');
  const b = browser(payloads, servedCfg);
  b.window.location.hash = '#tab=overview&view=week&unit=people';
  let GW = null, evalErr = null;
  try { GW = run(js, b); } catch (e) { evalErr = e; }
  ok('page: the served script evaluates without throwing', !evalErr, evalErr && evalErr.stack);
  if (!GW) { finish(); return; }
  await ticks();
  const view = () => b.els.view ? b.els.view.innerHTML : '';
  /* A fake element for the page's own delegated handlers: closest() answers
     for an attribute selector it carries, so a click is routed exactly as a
     real one would be. */
  const el = (attrs, extra) => Object.assign({
    getAttribute: (k) => (k in attrs ? attrs[k] : null), hasAttribute: (k) => k in attrs,
    setAttribute: (k, v) => { attrs[k] = String(v); }, removeAttribute: (k) => { delete attrs[k]; },
    closest(sel) { const m = /^\[([a-z-]+)(?:="([^"]*)")?\]$/.exec(sel); return m && m[1] in attrs && (m[2] === undefined || attrs[m[1]] === m[2]) ? this : null; },
  }, extra || {});
  const fire = (type, target) => (b.listeners[type] || []).forEach((f) => f({ target, preventDefault() {}, key: target.key }));

  /* Overview, week, people */
  let v = view(), n = nums(v);
  ok('overview: rendered', /<h1 class="title" tabindex="-1">Overview<\/h1>/.test(v), v.slice(0, 200));
  eq('overview: the people lead card is the payload’s', n[0], String(KPI.cur.people));
  eq('overview: the booking rate is booked / people', n[1], String(Math.round(KPI.cur.people_booked / KPI.cur.people * 100)));
  ok('overview: the rate change is in points', /\+2 pts|\+[0-9]+ pts/.test(v));
  ok('overview: completed card', n.includes(String(KPI.cur.people_done)));
  ok('overview: disqualified card, with its split', n.includes(String(KPI.cur.people_dq)) && v.includes(KPI.cur.people_b2c + ' sell to consumers') && v.includes(KPI.cur.people_waitlist + ' asked for the waitlist'));
  /* THE PARTS ADD UP: 23 disqualified = 13 + 9 + the one with no reason */
  const dqOther = KPI.cur.people_dq - KPI.cur.people_b2c - KPI.cur.people_waitlist;
  ok('overview: the disqualified split names the remainder, so it adds up', dqOther === 1 && v.includes(' · ' + dqOther + ' no reason recorded'));
  ok('overview: the withheld card says it is the MODEL, not every reason', v.includes('Meta withheld — model') && !/>Meta withheld<\/div>/.test(v));
  ok('overview: blocked and withheld cards', n.includes(String(KPI.cur.people_blocked)) && n.includes(String(KPI.cur.people_withheld)));
  ok('overview: Disqualified carries the comparison like every card, THEN its breakdown',
     /data-card="dq"[\s\S]*?class="ksub">12 at this point last week<span class="kbreak">13 sell to consumers · 9 asked for the waitlist · 1 no reason recorded<\/span>/.test(v), (v.match(/data-card="dq"[^]*?<\/div><\/div>/) || [''])[0].slice(-220));
  ok('overview: SESSIONS, with the step-1 rate', n.includes('5903') && />Sessions</.test(v) && !/Page loads/.test(v) && v.includes((Math.round(KPI.cur.f_people / 5903 * 1000) / 10) + '% got through step 1'));
  ok('overview: the funnel starts from sessions and says what one is', v.includes('5,903</b> sessions') && v.includes('A session is one visit to a page carrying the form'));
  ok('overview: the comparison names the same point', v.includes('at this point last week'));
  ok('overview: the funnel is the form-only one', n.includes(String(KPI.cur.f_people)) && n.includes(String(KPI.cur.f_people_booked)));
  /* THE SUM NOTE is computed, and it is the thing the owner asked to be true. */
  const barSum = [41, 47, 44, 81, 53].reduce((a, x) => a + x, 0);
  ok('overview: the note says why the bars add to more than the headline', v.includes('3 people came back on a second day, so the bars add to ' + barSum + ', not ' + KPI.cur.people + '.'), (v.match(/id="ov-sumnote">[^<]*/) || [''])[0]);
  ok('overview: channels show first touch, largest first', v.indexOf('>Meta<') > 0 && v.indexOf('>Meta<') < v.indexOf('>Direct / organic<'));
  ok('overview: small channels fold into "Everything else"', v.includes('Everything else'));
  ok('overview: "Everything else" names what is in it ON SCREEN', /<small class="chn">LinkedIn 3 · Partner \/ referral 3<\/small>/.test(v), (v.match(/class="chn">[^<]*/) || [''])[0]);
  /* The attention strip reads /monitor/health */
  ok('attention: the red check leads, in words', v.includes('Apollo enrichment: Out of credits for 2d'));
  /* the plain summary, never Apollo's own error text -- that stays on System health */
  ok('attention: the plain summary, and NOT the vendor text', /<div class="attn-d">86 refused in the last 24h · Last enrichment 2d ago<\/div>/.test(v) && !v.includes('You have insufficient credits!'));
  ok('attention: what it COSTS comes first, the facts after, each its own line',
     v.indexOf('they arrive without title') > 0 && v.indexOf('they arrive without title') < v.indexOf('86 refused in the last 24h'));
  /* API uptime and ELV come from their own routes and are in the count: the
     ELV fixture is quiet (grey), so three are not judged, not two. */
  ok('attention: the counts are buttons, not hover-only badges', /<button class="badge b-neu attn-chip" data-attn="insufficient_data"[^>]*>3 not judged<\/button>/.test(v), (v.match(/attn-chip[^<]*/g) || []).join(' | '));
  ok('attention: amber is its own count', /data-attn="amber"[^>]*>1 to watch</.test(v));
  ok('attention: API uptime is counted with the rest', /data-attn="green"[^>]*>6 healthy</.test(v), (v.match(/data-attn="green"[^<]*/) || [''])[0]);
  ok('nav: the System health badge counts the red checks, and SAYS what it counts', /data-tab="health"[\s\S]*?<span class="badge b-bad">1<span class="sr-only"> check red<\/span><\/span>/.test(b.els['nav-side'] ? b.els['nav-side'].innerHTML : ''));
  fire('click', el({ 'data-attn': 'insufficient_data' })); v = view();
  ok('attention: a chip opens the names behind it, ELV among them', /<b>Not judged:<\/b> [^<]*Email verification \(ELV\)/.test(v), (v.match(/attn-explain[^]*?<\/div>/) || [''])[0].slice(0, 300));
  fire('click', el({ 'data-attn': 'insufficient_data' })); v = view();
  const chart = b.els['ov-chart-el'] ? b.els['ov-chart-el'].innerHTML : '';
  ok('chart: drawn as SVG at the container width', /<svg width="960"/.test(chart), chart.slice(0, 80));
  /* NOT "a --prev path exists": a zero-height bar is <path d=""> and matched
     that regex while every grey bar was missing. Count bars with a shape. */
  eq('chart: last week is a grey bar behind EVERY day', [...chart.matchAll(/<path d="M[^"]+" fill="var\(--prev\)"/g)].length, 7);
  const wk = GW.TABS.overview._seriesFor(P.week, GW.TABS.overview._slotsFor(P.week));
  eq('chart: last week lines up by POSITION, Monday with Monday', wk.prev.join(','), '42,49,44,42,42,31,40');
  ok('chart: this week is blue with a surface ring', /stroke="var\(--card\)" stroke-width="4"/.test(chart) && /fill="var\(--data\)"/.test(chart));
  ok('chart: the running day is faded and says so', /opacity="0\.5"/.test(chart) && />so far</.test(chart));
  /* Units: switching to Leads changes every card */
  GW.S.unit = 'leads'; GW.TABS.overview.render(); v = view(); n = nums(v);
  eq('overview/leads: the lead card switches unit', n[0], String(KPI.cur.leads));
  ok('overview/leads: the Sessions rate is in the SAME unit as the funnel beside it', v.includes((Math.round(KPI.cur.f_leads / 5903 * 1000) / 10) + '% got through step 1') && !v.includes((Math.round(KPI.cur.f_people / 5903 * 1000) / 10) + '% got through step 1'));
  ok('overview/leads: the note says the bars add up exactly', v.includes('add up to the headline exactly'));
  ok('overview/leads: the chart title follows the unit', v.includes('Leads per day'));
  GW.S.unit = 'people';
  /* Table view */
  GW.S.table = true; GW.TABS.overview.render();
  const tbl = b.els['ov-chart-el'].innerHTML;
  ok('chart: the Table switch paints a table with both weeks', /<table>/.test(tbl) && tbl.includes('<th>Last week</th>'));
  ok('chart: the table reads last week BACK, named by its own date', /<td>Mon 21 Sep<\/td><td>41<\/td><td>42 <span class="pd">Mon 14 Sep<\/span><\/td>/.test(tbl), (tbl.match(/<td>Mon 21[^]*?<\/tr>/) || [''])[0]);
  ok('chart: the table toggle keeps ONE name, aria-pressed carries it', /data-table-toggle aria-pressed="true" aria-label="Show as a table"/.test(view()));
  GW.S.table = false;
  /* 3-hour blocks on a phone: each block ends where its last hour ends */
  const hs = GW.TABS.overview._slotsFor(P.today), gp = GW.chart.group({ slots: hs, cur: hs.map(() => 1), prev: hs.map(() => 2), partialIdx: 14 }, 3);
  eq('chart: a 3-hour block reads 12a to 3a, not 12a to 5a', gp.slots[0].label + ' ' + gp.slots[0].sub, '12a –3a');
  eq('chart: its tooltip names one range', gp.slots[0].head, '12 AM – 3 AM');
  eq('chart: the last block ends at midnight', gp.slots[7].sub, '–12a');
  eq('chart: grouped values SUM', gp.cur[0] + '/' + gp.prev[0], '3/6');
  const zc = { innerHTML: '', clientWidth: 600, setAttribute() {}, querySelector: () => ({ style: {}, offsetHeight: 0 }), querySelectorAll: () => [] };
  GW.chart.draw(zc, { title: 't', grain: 'day', slots: hs.slice(0, 7), cur: [0, 0, 0, 0, 0, 0, 0], prev: [0, 0, 0, 0, 0, 0, 0], partialIdx: 0, curLabel: 'a', prevLabel: 'b' });
  ok('chart: an ALL-ZERO window draws, with no NaN anywhere', zc.innerHTML.length > 200 && !/NaN|Infinity/.test(zc.innerHTML));
  /* A week across a month keeps the start's month */
  eq('overview: a week across a month names both months', GW.TABS.overview._meta({ view: 'week', asof: '2026-10-01T18:00:00Z', windows: { cur: { from: '2026-09-28T04:00:00Z' } } }).split(' so far')[0], 'Mon 28 Sep – Thu 1 Oct');
  eq('overview: a week inside one month drops the repeat', GW.TABS.overview._meta({ view: 'week', asof: ASOF, windows: { cur: { from: B.w0 } } }).split(' so far')[0], 'Mon 21 – Fri 25 Sep');
  /* Today and All time */
  GW.TABS.overview.setView('today'); await ticks(); v = view(); n = nums(v);
  ok('overview/today: compares with yesterday at this time', v.includes('at this time yesterday'));
  ok('overview/today: says it is live and pauses when hidden', v.includes('Updates every minute') && v.includes('paused while this tab is hidden'));
  /* the running hour's label says "so far"; hour 14 in the fixture holds (14 % 5) + 1 = 5 */
  ok('chart/today: the current hour reads "5 so far", not a bare number', /<text class="cap"[^>]*>5 so far<\/text>/.test(b.els['ov-chart-el'].innerHTML), (b.els['ov-chart-el'].innerHTML.match(/<text class="cap"[^>]*>[^<]*/g) || []).join(' | '));
  const zh = { innerHTML: '', clientWidth: 900, setAttribute() {}, querySelector: () => ({ style: {}, offsetHeight: 0 }), querySelectorAll: () => [] };
  const hsl = GW.TABS.overview._slotsFor(P.today), zcur = hsl.map((_, i) => (i < 14 ? 3 : i === 14 ? 0 : null));
  GW.chart.draw(zh, { title: 't', grain: 'hour', slots: hsl, cur: zcur, prev: null, partialIdx: 14, curLabel: 'a', prevLabel: 'b', caps: 'key', partialCap: ' so far' });
  ok('chart/today: a current hour still at 0 gets NO label at all', !/<text class="cap"[^>]*>0( so far)?<\/text>/.test(zh.innerHTML), (zh.innerHTML.match(/<text class="cap"[^>]*>[^<]*/g) || []).join(' | '));
  /* FILL: given 300px of room the chart is 300 tall; with none it keeps its 208 floor; never past 420 */
  const fh = (ch) => { const e = { innerHTML: '', clientWidth: 900, clientHeight: ch, setAttribute() {}, querySelector: () => ({ style: {}, offsetHeight: 0 }), querySelectorAll: () => [] };
    GW.chart.draw(e, { title: 't', grain: 'day', slots: hsl.slice(0, 7), cur: [1, 2, 3, 4, 5, 6, 7], prev: null, partialIdx: 6, curLabel: 'a', fill: true }); return (e.innerHTML.match(/<svg width="\d+" height="(\d+)"/) || [])[1]; };
  eq('chart: fill takes the room its row gives it', [fh(300), fh(0), fh(900)].join(','), '300,208,420');
  GW.TABS.overview.setView('all'); await ticks(); v = view(); n = nums(v);
  ok('overview/all: month on month on the lead card', v.includes('Month on month') && v.includes('This month so far') && v.includes('Last month, same point'));
  ok('overview/all: recovered bookings card', n.includes('53'));
  GW.S.unit = 'leads'; GW.TABS.overview.render();
  ok('overview/all: Recovered says ON the card that it stays in people', /Recovered bookings[\s\S]*?counted in people/.test(view()));
  GW.S.unit = 'people'; GW.TABS.overview.render();
  const all = GW.TABS.overview;
  const aslots = all._slotsFor(P.all), aser = all._seriesFor(P.all, aslots), mom = all._momFn(P.all, aslots, aser);
  ok('overview/all: April is never compared with a partial March', /March started on the 20th|Mar started on the 20th/.test(mom(1)), mom(1));
  ok('overview/all: the running month is compared at the same point', /at the same point/.test(mom(6)), mom(6));
  ok('overview/all: a full month is compared with the one before', /^[+-]\d+% vs Jul$/.test(mom(5)), mom(5));
  /* Refresh cadence, and the pause */
  const ivs = b.intervals.map((x) => x.ms);
  ok('refresh: Today polls every minute', ivs.includes(60000), ivs.join(','));
  ok('refresh: health every five minutes', ivs.includes(300000));
  const before = b.calls.length;
  b.document.visibilityState = 'hidden';
  b.intervals.forEach((x) => x.fn());
  await ticks();
  eq('refresh: a hidden tab asks the server for NOTHING', b.calls.length, before);
  b.document.visibilityState = 'visible';
  b.intervals.filter((x) => x.ms === 60000).forEach((x) => x.fn()); await ticks();
  ok('refresh: a visible tab refreshes', b.calls.length > before);

  /* A late answer for a tab you have left must not paint over the one you are on */
  GW.show('dropoff'); await ticks();
  GW.TABS.overview.render();
  ok('tabs: a late Overview render does NOT paint over Dropoff', /<h1 class="title" tabindex="-1">Dropoff</.test(view()) && !/<h1 class="title" tabindex="-1">Overview</.test(view()));

  /* Health */
  GW.show('health'); await ticks(20); v = view();
  ok('health: every server check has a row', Object.keys(GW.HEALTH_NAMES).every((k) => v.includes('data-check="' + k + '"')));
  ok('health: the red check is red', /data-check="apollo"[\s\S]*?b-bad[^>]*>Out of credits for 2d</.test(v));
  ok('health: quiet is grey, not green', /data-check="cron"[\s\S]*?b-neu[^>]*>No run yet</.test(v));
  ok('health: Salesforce unreadable is UNKNOWN, never zero', v.includes('UNKNOWN, not zero') && !/Arrived in Salesforce<\/div><div class="row"><span class="num" data-v="0"/.test(v));
  ok('health: the coverage cards are the payload’s', nums(v).includes('4537'));
  ok('health: never "reached step 2" for people who SENT it', !/reached step 2/.test(v) && v.includes('completed step 2'));
  ok('health: four-card panels are a four-column grid', (v.match(/class="sumgrid four"/g) || []).length === 2);
  ok('health: the API check has a timeout, like every other call', /AbortSignal\.timeout\(8000\)/.test(fs.readFileSync(path.join(ROOT, 'monitor', 'js', 'health.js'), 'utf8')));
  ok('health: System health keeps Apollo\'s own words in full', v.includes('You have insufficient credits!'));
  ok('health: API uptime and ELV are rows, named', v.includes('data-check="api"') && v.includes('data-check="elv"') && v.includes('Email verification (ELV)'));
  /* every server health id is known to the new tab -- a check with no row renders nowhere */
  const serverIds = [...new Set([...fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8').matchAll(/^\s{2}(\w+):\s+\{ source: '/gm)].map((m) => m[1]))];
  ok('health: every HEALTH_ALERT_META id has a name here', serverIds.length >= 8 && serverIds.every((k) => GW.HEALTH_NAMES[k]), serverIds.filter((k) => !GW.HEALTH_NAMES[k]).join(','));
  healthDown = true; await GW.TABS.health.run(); v = view(); healthDown = false;
  ok('health: an unreachable health check paints every row RED', Object.keys(GW.HEALTH_NAMES).every((k) => new RegExp('data-check="' + k + '"[\\s\\S]*?b-bad[^>]*>Could not check<').test(v)));

  /* Dropoff */
  GW.show('dropoff'); await ticks(); v = view();
  ok('dropoff: every payload cell is painted, in order', ['191', '173', '364', '47', '39', '86', '238', '212', '450'].every((x) => v.includes('>' + x + '<')), '');
  ok('dropoff: the partial period is marked', v.includes('<span class="part">part</span>'));
  ok('dropoff: the summary reads the payload', nums(v).includes('450') && nums(v).includes('364'));
  ok('dropoff: the booked rate row', v.includes('80.9%'));
  ok('dropoff: the window reads as dates, not ISO', v.includes('6 Jul – 25 Sep 2026') && !v.includes('2026-07-06 to'));
  ok('dropoff: outcomes are ROW headers', /<th scope="row" class="rl"><span class="badge b-good">Booked<\/span>/.test(v));
  eq('dropoff: the biggest leak is the biggest non-booked row', (v.match(/data-card="dp-leak"[\s\S]*?class="ksub">([^<]*)/) || [])[1], 'left on step 2');
  const DROP2 = JSON.parse(JSON.stringify(DROP));
  DROP2.rows.push({ key: '6_drop_calendar', label: 'Left at the calendar', desc: 'x', tone: 'neu', counts: { '2026-09-14': 60, '2026-09-21': 51 }, total: 111 });
  GW.TABS.dropoff._set(DROP2); GW.TABS.dropoff.render(); v = view();
  ok('dropoff: ...and it FOLLOWS the data, not the label', /data-card="dp-leak"[\s\S]*?data-v="111"[\s\S]*?class="ksub">left at the calendar</.test(v));
  GW.TABS.dropoff._st.source = 'LinkedIn'; GW.TABS.dropoff.render(); v = view();
  ok('dropoff: an applied source missing from the new window still shows as chosen', /<option value="LinkedIn" selected>LinkedIn — 0<\/option>/.test(v));
  GW.TABS.dropoff._st.source = '__all'; GW.TABS.dropoff._set(DROP); GW.TABS.dropoff.render();
  /* THE PRESETS ARE THE OLD TAB'S, day for day: lift the old one from the classic page and run both over three years. */
  const oldFn = (old.match(/function dpPresetRange\(p,today\)\{[\s\S]*?grain:g\};\}/) || [''])[0];
  ok('dropoff: the classic preset function was found', oldFn.length > 100);
  const oldR = new Function('function dpMonday(d){d.setUTCDate(d.getUTCDate()-(d.getUTCDay()+6)%7);return d;}' + oldFn + ';return dpPresetRange;')();
  let diffs = [];
  for (let i = 0; i < 365 * 3; i++) {
    const today = new Date(Date.UTC(2026, 0, 1, 12) + i * 864e5).toISOString().slice(0, 10);
    for (const pr of ['12w', '26w', '12m', 'ytd', 'custom']) {
      const a = JSON.stringify(oldR(pr, today)), c = JSON.stringify(GW.TABS.dropoff.presetRange(pr, today));
      if (a !== c) diffs.push(pr + ' ' + today + ' ' + a + ' vs ' + c);
    }
  }
  ok('dropoff: the presets match the classic tab on every day of three years', diffs.length === 0, diffs.slice(0, 3).join(' | '));

  /* Duplicates */
  GW.show('dupes'); await ticks(); v = view();
  ok('dupes: our own address is marked', /darshil\.dixit@gushwork\.ai <span class="badge b-neu"[^>]*>ours</.test(v));
  ok('dupes: a real address is not marked', !/peter[^<]*<span class="badge b-neu"[^>]*>ours/.test(v));
  ok('dupes: DATA IS ESCAPED before innerHTML', v.includes('peter&lt;img src=x onerror=alert(1)&gt;@flighted.co') && !v.includes('<img src=x'));
  ok('dupes: the filter counts', /data-dupes="ours"[^>]*>Only our own tests<span>1</.test(v));
  ok('dupes: each expander is NAMED for its row', v.includes('aria-label="Details for darshil.dixit@gushwork.ai"') && !v.includes('aria-label="Show details"'));
  ok('dupes: row keys are stable, never the index', /data-x="dupe-darshil_2edixit_40gushwork_2eai"/.test(v));
  ok('dupes: the pill group has a name', /class="pills" role="group" aria-label="Which addresses"/.test(v));
  fire('input', el({ 'data-dupes-q': '' }, { value: 'peter' })); v = view();
  ok('dupes: search narrows the list', !v.includes('darshil.dixit@gushwork.ai</span>') && v.includes('peter&lt;img'));
  fire('input', el({ 'data-dupes-q': '' }, { value: '' }));
  const many = { total: 60, leads: Array.from({ length: 60 }, (_, i) => ({ email: 'p' + i + '@x.co', session_count: 2, is_internal: false, sessions: [] })) };
  GW.TABS.dupes._set(many); GW.TABS.dupes.render(); v = view();
  ok('dupes: PAGED -- 50 rows, and it says how many more', (v.match(/class="xb"/g) || []).length === 50 && v.includes('Showing 50 of 60') && v.includes('Show 10 more'));
  fire('click', el({ 'data-more': 'dupes' })); v = view();
  eq('dupes: Show more shows the rest', (v.match(/class="xb"/g) || []).length, 60);
  GW.TABS.dupes._set(DUPES);

  /* Lead magnet */
  GW.show('lm'); await ticks(); v = view();
  ok('lm: the funnel cards are the payload’s', ['23', '11', '7', '5'].every((x) => nums(v).includes(x)));
  ok('lm: the pills read the SERVER totals, not the loaded rows', /data-lm-pill="sent"[^>]*>Sent<span>571</.test(v) && /data-lm-pill="all"[^>]*>All real leads<span>613</.test(v));
  ok('lm: page views are visits, never people', v.includes('visits to the page') && !v.includes('people who loaded the page'));
  ok('lm: the summary is a four-card grid by CLASS, so the phone rule applies', /<section class="sumgrid four">/.test(v) && !/grid-template-columns:repeat\(4/.test(v));
  ok('lm: daily volume reads as dates', /<td class="day" data-l="Day"><span class="cv">Thu 24 Sep<\/span><\/td>/.test(v));
  /* THE CSV, executed: a visitor-typed formula must arrive as text */
  const lmSrc = fs.readFileSync(path.join(ROOT, 'monitor', 'js', 'lm.js'), 'utf8');
  const qqSrc = (lmSrc.match(/qq = (function \(v\) \{[\s\S]*?\});/) || [])[1];
  const qq = qqSrc && new Function('Q', 'return ' + qqSrc)('"');
  ok('lm: the CSV escaper was found', typeof qq === 'function');
  if (qq) {
    ok('lm: CSV -- a leading = + - @ tab or CR becomes text', ['=HYPERLINK("x")', '+1', '-2', '@SUM(A1)', '\tx', '\rx'].every((x) => qq(x).startsWith('"\'')), ['=1', '+1'].map(qq).join(' '));
    eq('lm: CSV -- ordinary values untouched, quotes doubled', [qq('ok'), qq(''), qq(null), qq('a"b')].join('|'), '"ok"|""|""|"a""b"');
  }
  const classicQ = (() => { const src = fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8'); const i = src.indexOf("var q=function(v){var s=String"); if (i < 0) return null;
    const line = src.slice(src.lastIndexOf('\n', i) + 1, src.indexOf('\n', i)).trim().replace(/\s*\+\s*$/, ''); const browser = eval(line);
    return new Function('Q', 'AP', 'return (' + browser.replace(/^var q=/, '').replace(/;$/, '') + ')')('"', "'"); })();
  ok('classic: its CSV export escapes formulas the same way', classicQ && classicQ('=1') === qq('=1') && classicQ('ok') === qq('ok') && classicQ('a"b') === qq('a"b'));
  ok('lm: it says when the table is a page, not the population', v.includes('the table shows the most recent 1 of 900'));
  ok('lm: the drop-off bars are not status-coloured', !/b91c1c|f59e0b/.test(v));
  ok('lm: Mark sent is a button on an awaiting row', /data-lm-mark="1" data-undo="0"/.test(v));

  /* THE HASH: "#view" (the skip link) is not ours and changes nothing; a
     stray "%" is skipped, never fatal -- at boot it used to blank the page. */
  const was = JSON.stringify(GW.S);
  b.window.location.hash = '#view';
  ok('hash: the skip link\'s #view is ignored, state untouched', GW.readHash() === false && JSON.stringify(GW.S) === was);
  b.window.location.hash = '#tab=dupes&view=%E0%A4%A&unit=leads';
  let hashErr = null; try { GW.readHash(); } catch (e) { hashErr = e; }
  ok('hash: a malformed escape is skipped, the good keys still apply', !hashErr && GW.S.tab === 'dupes' && GW.S.unit === 'leads', hashErr && hashErr.message);
  GW.S.unit = 'people'; GW.S.tab = 'lm'; GW.writeHash();   /* put back what this test changed */
  /* GW.PAINT puts focus and open rows back after the HTML is replaced */
  const focused = { hasAttribute: (k) => k === 'data-view', getAttribute: (k) => (k === 'data-view' ? 'week' : null), tagName: 'BUTTON' };
  let refocused = null, reopened = null, htmlSet = null;
  const again = { focus() { refocused = 'week'; } }, xbtn = { getAttribute: () => 'dupe-a', setAttribute: (k, v) => { if (k === 'aria-expanded') reopened = v; } };
  const proot = { contains: (x) => x === focused, querySelectorAll: () => [xbtn], querySelector: (sel) => (sel === '[data-view="week"]' ? again : sel === '[data-x="dupe-a"]' ? xbtn : null), set innerHTML(v) { htmlSet = v; } };
  b.document.activeElement = focused;
  GW.paint(proot, '<p>new</p>');
  b.document.activeElement = null;
  ok('paint: the HTML is replaced', htmlSet === '<p>new</p>');
  ok('paint: the focused control is focused again after the swap', refocused === 'week');
  ok('paint: an open row is opened again after the swap', reopened === 'true' && !b.els['dupe-a-d'].hasAttribute('hidden'));

  /* ═══ PR C: All leads ═══ */
  GW.show('leads'); await ticks(20); v = view();
  ok('leads: rendered', /<h1 class="title" tabindex="-1">All leads<\/h1>/.test(v));
  ok('leads: the count is the payload\'s, in LEADS, with the page', /<b>4,471<\/b> leads found · page 2 of 179/.test(v), (v.match(/class="readat">[^]*?<\/span>/) || [''])[0].slice(0, 200));
  ok('leads: the pager says where you are, and marks the current page', v.includes('Page 2 of 179') && /class="pgb on" data-pg="2"[^>]*aria-current="page"/.test(v) && v.includes('data-pg="179"'));
  eq('leads: one expander per lead', (v.match(/class="xb"/g) || []).length, 3);
  ok('leads: each expander carries the RAW session_id, apart from the sanitised key', v.includes('data-sid="' + SID_A + '"') && /data-x="ld-3f2a9c1e_2d7b4d/.test(v));
  ok('leads: the stage ladder, with "Left on step 2" -- never "Step 1"', v.includes('>Completed<') && v.includes('>Booked<') && v.includes('>Left on step 2<') && !/>Step 1</.test(v));
  ok('leads: markers are words -- blocked, website failed, ours', />blocked<\/span>/.test(v) && />website failed<\/span>/.test(v) && />ours<\/span>/.test(v));
  ok('leads: the Meta chip is not repeated beside "blocked"', !/>Meta: blocked</.test(v) && />Meta: ours</.test(v) && />Meta: model</.test(v));
  ok('leads: a clarified B2B reads as B2B, the stored text kept', /B2B <span class="badge b-neu" title="B2B \(clarified from B2C\)">clarified<\/span>/.test(v));
  ok('leads: source is the ad click, else where they came from', v.includes('facebook / paid') && v.includes('from google.com') && !/>referral</.test(v));
  ok('leads: last year\'s row keeps its year, this year\'s drops it', /Mar 2, 2026|Mar 2, 2026,/.test(v) || /Mar 2,/.test(v));
  /* the panel of row A, painted in the detail row */
  ok('leads: the panel says WHY the row is marked, in words', v.includes('Why this lead is marked') && v.includes('AI check (name only) — We could not load their website'));
  ok('leads: the Meta line uses the SERVER\'s label, from the page config', v.includes('No Meta conversion was sent for this lead: Blocked — non-ICP.'));
  ok('leads: the visitor\'s IP location is its own group, BEFORE Apollo\'s', v.indexOf('Visitor — from their IP address') > 0 && v.indexOf('Visitor — from their IP address') < v.indexOf('Form &amp; enrichment'));
  ok('leads: Apollo\'s location is named as the PERSON\'s', v.includes('Person location (Apollo)') && v.includes('Woburn, Massachusetts, United States'));
  ok('leads: attempts from bigint strings', v.includes('Attempt 3 — 2 earlier, 1 of them disqualified'));
  ok('leads: a malformed start_time prints as itself, it never throws', v.includes('>not a date<'));
  ok('leads: a javascript: link is never a link', !/href="javascript/i.test(v) && v.includes('javascript:alert(1)'));
  ok('leads: the partner box -- email when no name, the reason in words', v.includes('p@partner.co <span class="na">(name not resolved)</span>') && v.includes('PartnerStack answered HTTP 400') && v.includes('>won<'));
  ok('leads: the website verdict uses the server\'s own label', v.includes('Domain registered but no website on it'));
  /* the change log: opened through the real handler, fetched by the RAW session_id */
  const callsBefore = b.calls.length;
  const keyA = 'ld-' + SID_A.replace(/[^A-Za-z0-9]/g, (c) => '_' + c.charCodeAt(0).toString(16));
  /* a real row starts CLOSED: the shared handler opens it, then this one fetches */
  const rowA = el({ 'data-x': keyA, 'data-sid': SID_A, 'aria-expanded': 'false' });
  fire('click', rowA); await ticks();
  const lc = b.calls.slice(callsBefore).filter((c) => c.path === '/monitor/lead-changes');
  ok('leads: opening a row fetches its change log by the RAW session_id', lc.length === 1 && lc[0].q.session_id === SID_A, JSON.stringify(lc));
  ok('leads: the change log is painted, with who and where in words', /sell_to<\/b><\/span><\/td><td data-l="Change"><span class="cv">B2C → B2B \(clarified from B2C\)/.test(b.els['lc-' + keyA].innerHTML) && b.els['lc-' + keyA].innerHTML.includes('they said “actually B2B”'));
  fire('click', rowA); fire('click', rowA); await ticks();   /* closed, then opened again */
  eq('leads: a loaded log is not fetched twice', b.calls.slice(callsBefore).filter((c) => c.path === '/monitor/lead-changes').length, 1);
  const keyB = keyA.replace(SID_A.replace(/[^A-Za-z0-9]/g, (c) => '_' + c.charCodeAt(0).toString(16)), SID_B.replace(/[^A-Za-z0-9]/g, (c) => '_' + c.charCodeAt(0).toString(16)));
  const keyC = 'ld-' + SID_C.replace(/[^A-Za-z0-9]/g, (c) => '_' + c.charCodeAt(0).toString(16));
  fire('click', el({ 'data-x': keyB, 'data-sid': SID_B, 'aria-expanded': 'false' })); fire('click', el({ 'data-x': keyC, 'data-sid': SID_C, 'aria-expanded': 'false' })); await ticks();
  ok('leads: an unreadable log says UNAVAILABLE, never "no changes"', b.els['lc-' + keyB].innerHTML.includes('Change log unavailable — this is not the same as no changes'));
  ok('leads: an empty log says so', b.els['lc-' + keyC].innerHTML.includes('No identity fields changed on this lead.'));
  /* filters: from the link, to the request; defaults never sent */
  GW.S.q = { stage: 'booked', partner: 'pk_77', search: '  jo hn ', sellTo: 'all' };
  const lp = GW.TABS.leads._params({ page: 1 });
  ok('leads: filters reach the request, typed text trimmed there and only there', lp.stage === 'booked' && lp.partner === 'pk_77' && lp.search === 'jo hn' && !('sellTo' in lp) && !('utmSource' in lp) && lp.sort === 'created_at');
  GW.S.q = {};
  ok('leads: with no filters, only stage, sort and dir are sent', JSON.stringify(Object.keys(GW.TABS.leads._params()).sort()) === JSON.stringify(['dir', 'sort', 'stage']));
  fire('change', el({ 'data-lf': 'preset' }, { value: '7d' })); await ticks();
  ok('leads: a date preset sets BOTH dates, and counts as ONE filter', GW.S.q.dateFrom && GW.S.q.dateTo && /data-lmore[^>]*>[\s\S]*?Filters <span class="badge b-neu">1<\/span>/.test(view()), JSON.stringify(GW.S.q));
  fire('change', el({ 'data-lf': 'preset' }, { value: '' })); await ticks();
  ok('leads: "Any date" CLEARS both dates', !GW.S.q.dateFrom && !GW.S.q.dateTo);
  fire('click', el({ 'data-lcsv': '' })); 
  ok('leads: Export CSV is the same query, format=csv, token in the query', /\/monitor\/leads\?token=[^&]+&.*format=csv/.test(String(b.window.location.href || '')), String(b.window.location.href || '').replace(/token=[^&]+/, 'token=***'));
  /* ═══ PR C: Blocked ═══ */
  const bc0 = b.calls.length;
  GW.show('blocked'); await ticks(20); v = view();
  ok('blocked: three figures, each with its unit', /<b>51<\/b> leads blocked · <b>42<\/b> people · <b>45<\/b> leads excluding our own tests/.test(v), (v.match(/class="readat">[^]*?<\/span><\/div>/) || [''])[0].slice(0, 220));
  const blkCalls = b.calls.slice(bc0).filter((c) => c.path === '/monitor/leads');
  ok('blocked: it reads /monitor/leads with nonicp=only, newest first', blkCalls.length === 2 && blkCalls.every((c) => c.q.nonicp === 'only' && c.q.stage === 'all' && c.q.sort === 'created_at' && c.q.dir === 'desc') && blkCalls.some((c) => c.q.internal === 'exclude'));
  ok('blocked: the chip says WHICH CHECK, and no "Meta: blocked"', />AI check \(name only\)<\/span>/.test(v) && !/>Meta: blocked</.test(v));
  ok('blocked: the same lead has a DIFFERENT key here than on All leads', v.includes('data-x="blk-3f2a9c1e') && !v.includes('data-x="ld-3f2a9c1e'));
  ok('blocked: the lede is true for late verdicts too', v.includes('a few were marked only after they had booked') && !v.includes('turned away before the calendar'));
  const bc1 = b.calls.length;
  fire('change', el({ 'data-blk': '' }, { value: 'only' })); await ticks(20); v = view();
  ok('blocked: "only ours" says so, and asks for no people figure', /<b>51<\/b> of our own test leads blocked/.test(v) && !b.calls.slice(bc1).some((c) => c.path === '/monitor/metrics'));
  fire('change', el({ 'data-blk': '' }, { value: '' })); await ticks(20);

  /* ═══ PR C: SDR list ═══ */
  GW.show('sdr'); await ticks(20); v = view();
  ok('sdr: the count is PEOPLE, from the payload total', /<b>37<\/b> people to call/.test(v));
  eq('sdr: one row per person in the payload', (v.match(/class="xb"/g) || []).length, 3);
  ok('sdr: keyed by the lower-cased address, never the row index', v.includes('data-x="sdr-ann_40acme_2eco"'));
  ok('sdr: the stage is Completed or Left on step 2', v.includes('>Completed<') && v.includes('>Left on step 2<') && !/>Step 1</.test(v));
  ok('sdr: the source cell carries the partner AND what they came in saying', /<span class="badge b-neu">partner<\/span> Alpha Partners<br>a podcast/.test(v));
  ok('sdr: a javascript: LinkedIn is never a link', !/href="javascript/i.test(v));
  fire('input', el({ 'data-sdr-q': '' }, { value: '  ACME ' })); v = view();
  ok('sdr: search is trimmed and case-blind, as the server does it', /<b>1<\/b> matches the search/.test(v) && (v.match(/class="xb"/g) || []).length === 1);
  fire('click', el({ 'data-sdr-csv': '' }));
  ok('sdr: the export carries the SAME trimmed term', /\/monitor\/sdr\?token=[^&]+&format=csv&search=ACME$/.test(String(b.window.location.href || '')), String(b.window.location.href || '').replace(/token=[^&]+/, 'token=***'));
  fire('input', el({ 'data-sdr-q': '' }, { value: 'software' })); v = view();
  ok('sdr: industry is searched too', /<b>1<\/b> matches the search/.test(v) && v.includes('cy@omega.com'));
  fire('input', el({ 'data-sdr-q': '' }, { value: '' }));
  /* THE PAIR IS NOW A TRIPLE: server, classic, new -- all four fields, all three copies */
  const srv = (fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8').match(/const SDR_SEARCH_COLUMNS = (\[[^\]]+\]);/) || [])[1];
  const cls = (fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8').match(/'var SDR_SEARCH_FIELDS=(\[[^\]]+\]);'/) || [])[1];
  const nw = (fs.readFileSync(path.join(ROOT, 'monitor', 'js', 'sdr.js'), 'utf8').match(/var SDR_SEARCH_FIELDS = (\[[^\]]+\]);/) || [])[1];
  const norm = (x) => x && JSON.stringify(eval(x).slice().sort());
  ok('sdr: the search fields are the SAME in the server, the classic and the new tab', srv && cls && nw && norm(srv) === norm(cls) && norm(cls) === norm(nw) && JSON.stringify(GW.TABS.sdr._fields.slice().sort()) === norm(nw), [srv, cls, nw].join(' | '));

  /* ═══ PR C: Model ═══ */
  const mc0 = b.calls.length;
  GW.show('model', true, { days: '30', product: 'crm' }); await ticks(20); v = view();
  const mcall = b.calls.slice(mc0).find((c) => c.path === '/monitor/non-icp');
  ok('model: the window and product from the link reach the request', mcall && mcall.q.days === '30' && mcall.q.product === 'crm');
  ok('model: the ladder paints every row\'s count from the payload', ['137', '29', '41', '563', '211'].every((x) => new RegExp('data-v="' + x + '"').test(v)) && /data-v="981"/.test(v));
  ok('model: the server\'s one-decimal share, never re-rounded', v.includes('57.4%') && v.includes('4.2%'));
  ok('model: ours is counted beside, never subtracted', v.includes('· 3 ours') && v.includes('<b>17</b> of them are our own test submissions'));
  ok('model: the caption names the product that was APPLIED', v.includes('leads in the window — CRM only'));
  ok('model: a capped window says the counts are a floor', v.includes('the counts are a floor, not a total'));
  ok('model: all THREE groups render, the empty one included', v.includes('Blocked by the brand-domain list') && v.includes('Blocked by the model') && v.includes('Meta withheld by the model — not blocked') && /Blocked by the model[\s\S]*?None in this window\./.test(v));
  ok('model: an uncategorised row says so, with no borrowed confidence', /class="uncat"><td data-l="Industry"><span class="cv">Not categorised[^<]*<\/span><\/td><td data-v="36" data-l="Leads"><span class="cv">36<\/span><\/td><td data-l="Companies"><span class="cv">—<\/span><\/td><td data-l="Median confidence"><span class="cv">—<\/span><\/td>/.test(v));
  /* PLAIN TABLES STACK ON A PHONE only because every cell names its column --
     a cell with no label turns into an unlabelled number in the stacked card. */
  const gh = GW.ui.grid([{ label: 'City', get: (r) => r.c }, { label: 'Leads', attr: (r) => 'data-v="' + r.n + '"', get: (r) => String(r.n) }, { label: 'Note', get: () => '' }],
    [{ c: 'Oslo <b>', n: 83 }, { c: 'Lima', n: 29 }], { rowCls: (r) => (r.n === 29 ? 'uncat' : ''), region: 'Places' });
  const ghCells = [...gh.matchAll(/<td[^>]*>/g)].map((m) => m[0]);
  ok('grid: every cell carries its column name, in column order', ghCells.length === 6 && ghCells.every((c, i) => c.includes('data-l="' + ['City', 'Leads', 'Note'][i % 3] + '"')), ghCells.join(' '));
  ok('grid: values are escaped, an empty one reads as a dash, attributes and row classes arrive', gh.includes('Oslo &lt;b&gt;') && />—<\/span>/.test(gh) && gh.includes('data-v="83"') && gh.includes('<tr class="uncat">'));
  ok('grid: it is a stacking box, and a scroll region only when asked', /^<div class="tbl stack" tabindex="0" role="region" aria-label="Places">/.test(gh) && /^<div class="tbl stack"><table>/.test(GW.ui.grid([{ label: 'A', get: () => 'x' }], [{}])));
  ok('model: the ALL-TIME figures come from the TOP-LEVEL cache', /data-card="mdl-cache"[\s\S]*?data-v="2975"/.test(v) && v.includes('2,965 judged from their site · 10 currently unreadable') && !/data-card="mdl-cache"[\s\S]*?data-v="0"/.test(v));
  ok('model: never-tried is its own number, outside the rate', /data-card="mdl-never"[\s\S]*?data-v="7"[\s\S]*?of 19 companies — not a failure/.test(v) && /data-card="mdl-unread"[\s\S]*?data-v="25"[\s\S]*?3 of 12 companies/.test(v));
  ok('model: the "latest outcome, not a rate" note is ON SCREEN', v.includes('Latest outcome per domain, not a historical rate.'));
  ok('model: the cache has no percentage anywhere', !/id="mdl-cachet"[\s\S]*?%<\/td>/.test(v) && /data-v="141"/.test(v) && v.includes('>Withholds Meta<'));
  ok('model: every decision row is painted, ours marked', (v.match(/data-x="mdl-/g) || []).length === 2 && /agent@allstate\.com<\/span><span class="marks"><span class="badge b-neu">ours<\/span>/.test(v));
  ok('model: the quote from a stranger\'s page is ESCAPED', v.includes('&lt;img src=x onerror=alert(3)&gt; We sell insurance') && !v.includes('<img src=x'));
  ok('model: a name-only block reads as words on the unreadable list', v.includes('Yes — AI check (name only)') && v.includes('too little text to judge'));
  ok('model: near misses -- both floors, the total, and who has a lead', v.includes('<b>75%</b> certainty') && v.includes('<b>90%</b> when') && /data-v="13"/.test(v) && v.includes('>has a lead<'));
  /* flag-only mode: the same row is a flag, and Meta still fires */
  GW.TABS.model._set(Object.assign({}, MODEL, { flags: Object.assign({}, MODEL.flags, { llm_meta: false }) })); GW.TABS.model.render(); v = view();
  ok('model: with Meta NOT withheld, the row says Meta was still sent', v.includes('Flagged by the model (Meta still sent)') && !v.includes('>Meta withheld only<'));
  GW.TABS.model._set(null); GW.TABS.model.render();

  /* ═══ PR C: Visitors ═══ */
  const vc0 = b.calls.length;
  GW.show('visitors', true, { days: '90' }); await ticks(20); v = view();
  ok('visitors: the window from the link reaches the request', b.calls.slice(vc0).some((c) => c.path === '/monitor/visitors' && c.q.days === '90'));
  ok('visitors: coverage is the payload\'s, each with its unit in words', ['4471', '3312', '3301', '2287'].every((x) => new RegExp('data-v="' + x + '"').test(v)) && /id="vis-cov"[\s\S]*?addresses/.test(v));
  ok('visitors: coverage is never a percentage', !/id="vis-cov"[\s\S]*?%[\s\S]*?id="vis-repeats"/.test(v));
  ok('visitors: a repeat address is painted with its count, IPv6 included', /data-v="7"/.test(v) && v.includes('<code>2001:db8::1</code>'));
  ok('visitors: it says how many of how many repeat addresses', v.includes('Showing 2 of 9 repeat addresses'));
  const nets = GW.TABS.visitors._merge(VIS.networks);
  ok('visitors: ONE provider under two domains is ONE row, both domains kept', nets.length === 2 && nets[0].isp === 'Verizon Business' && nets[0].leads === 840 && nets[0].domains.join(',') === 'verizonbusiness.com,frontiernet.net');
  ok('visitors: a different provider stays its own row, escaped on screen', nets[1].isp === 'AT&T Enterprises, LLC' && v.includes('AT&amp;T Enterprises, LLC') && /data-v="840"/.test(v) && !/data-v="829"/.test(v));
  ok('visitors: the country chips add up the places', v.includes('US · 842 leads'));
  ok('visitors: local time is the VISITOR\'s zone, and an unknown zone is blank', GW.TABS.visitors._localNow('Asia/Kolkata') !== GW.TABS.visitors._localNow('America/Los_Angeles') && GW.TABS.visitors._localNow('Not/AZone') === '');
  const visSrc = fs.readFileSync(path.join(ROOT, 'monitor', 'js', 'visitors.js'), 'utf8');
  ok('visitors: tiles from Esri Canvas, {z}/{y}/{x}, never a rejected provider', /services\.arcgisonline\.com\/ArcGIS\/rest\/services\/Canvas\/World_Light_Gray_Base\/MapServer\/tile\/\{z\}\/\{y\}\/\{x\}/.test(GW.TABS.visitors._tiles.light) && /services\.arcgisonline\.com\/ArcGIS\/rest\/services\/Canvas\/World_Dark_Gray_Base\/MapServer\/tile\/\{z\}\/\{y\}\/\{x\}/.test(GW.TABS.visitors._tiles.dark) && !/tile\.openstreetmap\.org|basemaps\.cartocdn\.com/.test(visSrc));
  ok('visitors: Leaflet is pinned and checked by SRI', /leaflet@1\.9\.4\/dist\/leaflet\.js/.test(visSrc) && /sha384-[A-Za-z0-9+/=]{64}/.test(visSrc) && /crossOrigin = 'anonymous'/.test(visSrc));
  /* the map: absent Leaflet says so; a stand-in Leaflet runs the drawing code */
  delete b.window.L; GW.TABS.visitors._draw(VIS);
  ok('visitors: with no Leaflet the map says so, and nothing throws', /did not load/.test(b.els['vis-map-note'].textContent));
  const drawn = [], tileLog = [];
  let fits = 0, layersAdded = 0;
  b.window.L = { Browser: { mobile: false }, map() { return { setView() { return this; }, attributionControl: { setPrefix() {} }, invalidateSize() {}, removeLayer(x) { if (x && x.u) tileLog.push('-' + x.u); }, fitBounds() { fits++; } }; },
    tileLayer: (u, o) => ({ u, o, addTo() { tileLog.push('+' + u); return this; } }), latLngBounds: () => ({ pad() { return this; } }),
    circleMarker: (ll, opts) => { const m = { ll, opts, bindPopup(c) { m.popup = c; return m; } }; drawn.push(m); return m; }, layerGroup: (a) => ({ a, addTo() { layersAdded++; return this; } }) };
  const res = GW.TABS.visitors._draw(VIS);
  ok('visitors: it draws exactly the mappable places, and counts the rest', res && res.drawn === 2 && res.miss === 1 && /2 places drawn; <b>1<\/b> more resolved to a city but have no coordinates/.test(b.els['vis-map-note'].innerHTML));
  ok('visitors: circles are drawn LARGEST FIRST, whatever order the API sent', drawn[0].ll[0] === 42.36 && drawn[1].ll[0] === 39.74);
  ok('visitors: circle AREA scales with leads -- a quarter the leads, half the extra radius', Math.abs((drawn[1].opts.radius - 6) - (drawn[0].opts.radius - 6) / 2) < 0.5);
  ok('visitors: circles are styled by class, from tokens, never raw hex', drawn.every((m) => m.opts.className === 'vdot' && !m.opts.color && !m.opts.fillColor));
  const tl = GW.TABS.visitors._tiles;
  ok('visitors: the map starts on the basemap for the theme it opened in', tileLog.length === 1 && tileLog[0] === '+' + (b.document.documentElement.getAttribute('data-theme') === 'dark' ? tl.dark : tl.light), tileLog.join(' '));
  tileLog.length = 0; GW.onTheme('dark'); GW.onTheme('dark');
  ok('visitors: switching to dark swaps the basemap once, and a repeat does nothing', tileLog.length === 2 && tileLog[1] === '+' + tl.dark && tileLog[0].startsWith('-'), tileLog.join(' '));
  tileLog.length = 0; GW.onTheme('light');
  ok('visitors: and back to light', tileLog.length === 2 && tileLog[1] === '+' + tl.light, tileLog.join(' '));
  /* THE READER'S PLACE: a repaint of the same read neither redraws nor
     re-fits; new data redraws and keeps the view; a new window re-fits */
  const f0 = fits, l0 = layersAdded;
  GW.TABS.visitors._draw(VIS);
  ok('review: repainting the SAME read keeps the circles and the view', fits === f0 && layersAdded === l0, [fits, f0, layersAdded, l0].join(','));
  GW.TABS.visitors._draw(Object.assign({}, VIS));
  ok('review: new data swaps the circles and keeps the reader\'s zoom', fits === f0 && layersAdded === l0 + 1, [fits, f0, layersAdded, l0].join(','));
  GW.TABS.visitors._draw(Object.assign({}, VIS, { window_days: 7 }));
  ok('review: a new window re-fits the map', fits === f0 + 1, [fits, f0].join(','));
  ok('visitors: the popup names the place and its counts', /<b>Boston, Massachusetts, US<\/b><br>612 leads<br>590 people<br>402 booked/.test(drawn[0].popup));
  delete b.window.L;

  /* ═══ PR C: Partners ═══ */
  GW.show('partners'); await ticks(20); v = view();
  ok('partners: the six cards are the payload\'s', [['p-attn', 5], ['p-domains', 7], ['p-sfwait', 1], ['p-leads', 12], ['p-conv', 5], ['p-qual', 2]].every(([id, n]) => new RegExp('data-card="' + id + '"[\\s\\S]*?data-v="' + n + '"').test(v)));
  ok('partners: Needs attention says act today', /data-card="p-attn"[\s\S]*?act on these today/.test(v));
  ok('partners: the states are in LADDER order, the failed ones red', v.indexOf('2 qualified') > 0 && v.indexOf('2 qualified') < v.indexOf('2 conversion failed') && /b-bad">2 conversion failed</.test(v));
  ok('partners: leads with no company domain are counted as LEADS, and say so', v.includes('3 with no company domain (leads, not companies)'));
  ok('partners: the ticked split -- fired, and CANNOT fire when the conversion is not verified', v.includes('1 ticked, $50 fired') && v.includes('1 ticked, cannot fire yet') && !v.includes('1 ticked, will fire next poll'));
  ok('partners: the unticked split adds up to the state', v.includes('1 waiting on an AE') && v.includes('1 unticked, no conversion sent (not actionable)') && v.includes('1 unticked after the $50 fired (not actionable)'));
  ok('partners: never created is red; unchecked is its own number', /b-bad">1 Opportunity never created</.test(v) && v.includes('1 not checked yet'));
  ok('partners: a failed Salesforce read and a STALE check are both red and named', v.includes('Salesforce read FAILED (incomplete)') && /b-bad"[^>]*>STALE — last checked 90 min ago</.test(v));
  ok('partners: the funnel leads with what happened, and rates run stage to stage', /data-stage="completed"[\s\S]*?80% of the stage before/.test(v) && /data-stage="conversions"[\s\S]*?62\.5% of the stage before/.test(v) && /data-stage="opportunity"[\s\S]*?too few to rate \(n=9\)/.test(v));
  ok('partners: a stage reached by a skip says so, and the $50 reads 1, never 0', v.includes('0 on the funnel path · 1 skipped an earlier stage') && /data-stage="qualified"[\s\S]*?data-v="1"/.test(v));
  ok('partners: clicks are not companies; a zero loss is not shown; a real one is red', v.includes('clicks, not companies') && /bad-t">2 conversion failed</.test(v) && !/0 skipped</.test(v));
  ok('partners: no rate above 100%', !/(1[0-9]{2}|[2-9][0-9]{2})(\.\d)?% of the stage before/.test(v));
  ok('partners: revenue gaps with Salesforce down read N+? and are NOT clean', /data-v="1\+\?"/.test(v) && v.includes('NOT a clean result'));
  ok('partners: a correct skip is reported in words, never counted as a gap', v.includes('<code>test.com</code> (our own test address, skipped)') && v.includes('5 partner demos past the 3-day mark'));
  ok('partners: the reason is words, the slug kept for whoever debugs it', v.includes('<span title="http_400">PartnerStack answered HTTP 400</span>'));
  ok('partners: a company claimed by two partners says so', v.includes('1 other partner'));
  ok('partners: Salesforce per company follows the same rule', v.includes('ticked, cannot fire yet — conversion not verified') && v.includes('Opportunity never created'));
  ok('partners: acknowledge is offered ONLY on failed rows; un-acknowledge on an acked one', (v.match(/data-ack="/g) || []).length === 2 && v.includes('data-ack="fail.co" data-on="1"') && v.includes('data-ack="acked.co" data-on="0"') && v.includes('>acknowledged<'));
  const per = v.slice(v.indexOf('id="p-per"'));
  ok('partners: per partner, sorted by step 1 (the one with 9 first)', per.indexOf('>b@beta.test<') > 0 && per.indexOf('>b@beta.test<') < per.indexOf('>Alpha<'), [per.indexOf('>b@beta.test<'), per.indexOf('>Alpha<')].join(','));
  ok('partners: the dagger where a stage was skipped, and "—" for unknown clicks', /title="0 on the funnel path, 2 skipped an earlier stage[^"]*">2 †</.test(per) && per.includes('>—<'), (per.match(/†[^<]*/g) || []).join(' | '));
  /* THE WRITE, driven: Cancel sends NOTHING; Acknowledge sends one JSON body */
  const pc0 = b.calls.length;
  fire('click', el({ 'data-ack': 'fail.co', 'data-on': '1' })); v = view();
  ok('partners: Acknowledge opens a note, with a Cancel', v.includes('data-ack-note="fail.co"') && v.includes('data-ack-go="fail.co"') && v.includes('data-ack-cancel="fail.co"'));
  fire('click', el({ 'data-ack-cancel': 'fail.co' })); await ticks();
  ok('partners: CANCEL SENDS NOTHING -- the classic acknowledged anyway', !b.calls.slice(pc0).some((c) => c.path === '/monitor/partner-ack') && !view().includes('data-ack-note="fail.co"'));
  fire('click', el({ 'data-ack': 'fail.co', 'data-on': '1' })); fire('click', el({ 'data-ack-go': 'fail.co' })); await ticks(20);
  const ackc = b.calls.slice(pc0).filter((c) => c.path === '/monitor/partner-ack');
  ok('partners: one POST, a JSON body with the BOOLEAN, the token in the query', ackc.length === 1 && ackc[0].method === 'POST' && ackc[0].q.token === TOKEN && ackc[0].headers && ackc[0].headers['Content-Type'] === 'application/json' &&
     JSON.stringify(JSON.parse(ackc[0].body)) === JSON.stringify({ customer_key: 'fail.co', note: '', acknowledged: true }), ackc[0] && ackc[0].body);
  ok('partners: after the write, the programme is read again', b.calls.slice(pc0).filter((c) => c.path === '/monitor/partners').length >= 1);
  const pc1 = b.calls.length;
  fire('click', el({ 'data-ack': 'acked.co', 'data-on': '0' })); await ticks(20);
  const unack = b.calls.slice(pc1).find((c) => c.path === '/monitor/partner-ack');
  ok('partners: un-acknowledge sends the JSON FALSE -- a string would acknowledge', unack && JSON.parse(unack.body).acknowledged === false && typeof JSON.parse(unack.body).acknowledged === 'boolean');
  /* gaps unreadable: unavailable, never zero */
  gapsDown = true; GW.TABS.partners._set(PARTNERS, null); await new Promise((r) => { b.intervals.filter((x) => x.ms === 600000).forEach((x) => x.fn()); setImmediate(r); }); await ticks(20); v = view(); gapsDown = false;
  ok('partners: revenue gaps that cannot be read say UNAVAILABLE, never zero', /id="p-gaps"[\s\S]*?Partner revenue gaps could not be read/.test(v) && !/id="p-gaps"[\s\S]*?data-v="0"/.test(v));
  /* the drill-down: a partner's leads, on the new All leads */
  const pc2 = b.calls.length;
  fire('click', el({ 'data-pdrill': 'pk_a' })); await ticks(20);
  ok('partners: "See this partner\'s leads" opens All leads filtered on that partner', GW.current() === 'leads' && GW.S.q.partner === 'pk_a' && b.calls.slice(pc2).some((c) => c.path === '/monitor/leads' && c.q.partner === 'pk_a'));
  GW.show('partners'); await ticks(20);

  /* ═══ PR C: the review's fixes, each one DRIVEN ═══ */
  const live = () => (b.els['gw-live'] ? b.els['gw-live'].textContent : '');
  /* 1. a session_id that is an Object.prototype name: the page must still paint */
  leadsHook = (q) => q.nonicp ? null : { total: 1, page: 1, pages: 1, leads: [Object.assign({}, LEAD_C, { session_id: 'constructor', email: 'proto@x.test' })] };
  GW.show('leads'); await ticks(20); v = view();
  ok('review: a lead whose session_id is "constructor" still paints All leads', v.includes('proto@x.test') && v.includes('Reading the change history') && !v.includes('could not be drawn'), v.slice(0, 200));
  leadsHook = null;
  /* 2. the draw guard: a render that throws paints an error, never an endless skeleton */
  const ce = console.error; console.error = () => {};
  GW.TABS.leads._set({ total: 2, page: 1, pages: 1, leads: [null] }); try { GW.TABS.leads.render(); } catch (e) {} v = view(); console.error = ce;
  ok('review: a row that cannot be drawn paints an error in place of the table', v.includes('All leads could not be read') && v.includes('this view could not be drawn') && v.includes('All leads</h1>'));
  /* 3. the sort from a link is held to the table's own columns */
  GW.S.q = { sort: 'constructor', dir: 'sideways' }; const sp = GW.TABS.leads._params();
  GW.S.q = { sort: 'email', dir: 'asc' }; const sp2 = GW.TABS.leads._params(); GW.S.q = {};
  ok('review: a sort from the link that is not a column is never sent', sp.sort === 'created_at' && sp.dir === 'desc' && sp2.sort === 'email' && sp2.dir === 'asc', JSON.stringify([sp, sp2]));
  /* 4. a tab name that is an Object method */
  try { GW.show('toString'); } catch (e) {} ok('review: a tab name like "toString" falls back to Overview', GW.current() === 'overview');
  /* 5. Custom dates can be chosen from the screen */
  GW.show('leads'); await ticks(20);
  fire('change', el({ 'data-lf': 'preset' }, { value: 'custom' })); await ticks(20); v = view();
  ok('review: choosing Custom shows From and To and STAYS Custom', /<option value="custom" selected>/.test(v) && v.includes('data-lf="dateFrom"') && v.includes('data-lf="dateTo"'));
  fire('change', el({ 'data-lf': 'dateFrom' }, { value: '2026-09-01' })); await ticks(20); v = view();
  ok('review: a typed From date keeps the Custom boxes', v.includes('value="2026-09-01"') && /<option value="custom" selected>/.test(v));
  fire('change', el({ 'data-lf': 'preset' }, { value: '' })); await ticks(20);
  ok('review: "Any date" leaves Custom and clears both dates', !GW.S.q.dp && !GW.S.q.dateFrom && !GW.S.q.dateTo);
  /* 6. load BEFORE paint: old rows under new filters say "updating…" */
  fire('change', el({ 'data-lf': 'stage' }, { value: 'booked' })); v = view();
  ok('review: a filter change paints "updating…" until the new rows arrive', v.includes('updating…'));
  await ticks(20);
  ok('review: ...then clears it, and says what came back aloud', !view().includes('updating…') && live() === '4,471 leads found, page 2 of 179', live());
  fire('change', el({ 'data-lf': 'stage' }, { value: 'all' })); await ticks(20);
  /* the card-view sort select reaches the request */
  const ss0 = b.calls.length; fire('change', el({ 'data-lsortsel': '' }, { value: 'email:asc' })); await ticks(20);
  ok('review: the card-view sort select sorts', b.calls.slice(ss0).some((c) => c.path === '/monitor/leads' && c.q.sort === 'email' && c.q.dir === 'asc') && /data-lsortsel[\s\S]*?<option value="email:asc" selected>/.test(view()));
  GW.S.q = {};
  /* 7. a page past the end is read again as the last page */
  leadsHook = (q) => q.nonicp ? null : q.page === '9' ? { total: 30, page: 9, pages: 2, leads: [] } : q.page === '2' ? { total: 30, page: 2, pages: 2, leads: [LEAD_C] } : null;
  const pe0 = b.calls.length; GW.show('leads', false, { page: '9' }); await ticks(30); v = view();
  const pages = b.calls.slice(pe0).filter((c) => c.path === '/monitor/leads').map((c) => c.q.page);
  ok('review: a page past the end is read again as the last page, never "No leads yet"', pages.includes('9') && pages.includes('2') && GW.S.q.page === '2' && !v.includes('No leads yet') && v.includes('cy@shop.example'), JSON.stringify(pages));
  leadsHook = null; GW.S.q = {};
  /* 8. the pager: Previous and Next never share a selector with a page button */
  GW.show('leads'); await ticks(20); v = view();
  ok('review: Previous and Next carry their target apart from the page buttons', /data-pg-step="1"[^>]*aria-label="Previous page"/.test(v) && /data-pg-step="3"[^>]*aria-label="Next page"/.test(v) && !/data-pg="1"[^>]*aria-label/.test(v));
  const pg0 = b.calls.length; fire('click', el({ 'data-pg-step': '3' })); await ticks(20);
  ok('review: Next still pages, and the page is announced', b.calls.slice(pg0).some((c) => c.path === '/monitor/leads' && c.q.page === '3') && /page 2 of 179/.test(live()));
  GW.S.q = {};
  /* 9. a new hash on the SAME tab fetches, not only repaints */
  GW.show('leads'); await ticks(20);
  const hc0 = b.calls.length; b.window.location.hash = '#tab=leads&stage=booked'; (b.wlisteners.hashchange || []).forEach((f) => f()); await ticks(20);
  ok('review: a new hash on the same tab reads the server again', b.calls.slice(hc0).some((c) => c.path === '/monitor/leads' && c.q.stage === 'booked'));
  GW.S.q = {};
  /* 10. Blocked words its count from the payload it has, not from the select */
  GW.show('blocked'); await ticks(20);
  fire('change', el({ 'data-blk': '' }, { value: 'only' })); v = view();
  ok('review: Blocked keeps the old wording beside the old count until the new one lands', /<b>51<\/b> leads blocked · /.test(v) && v.includes('updating…') && !/of our own test leads blocked/.test(v));
  await ticks(20); v = view();
  ok('review: ...then words the new count for its own population', /<b>51<\/b> of our own test leads blocked/.test(v) && !v.includes('updating…'));
  fire('change', el({ 'data-blk': '' }, { value: '' })); await ticks(20);
  /* 11. the lead panel says how B2B was clarified, as text */
  GW.show('leads'); await ticks(20); v = view();
  ok('review: the lead panel shows the stored "clarified from" text', v.includes('Sells to (as stored)') && v.includes('B2B (clarified from B2C)</div>'));
  /* 12. Partners: the note survives a repaint, a failed save keeps the form */
  GW.show('partners'); await ticks(20);
  fire('click', el({ 'data-ack': 'fail.co', 'data-on': '1' }));
  fire('input', el({ 'data-ack-note': 'fail.co' }, { value: 'deleted by hand' }));
  GW.TABS.partners.render(); v = view();
  ok('review: a background repaint keeps the note being typed', v.includes('data-ack-note="fail.co" value="deleted by hand"'));
  ok('review: every ack control names its company', v.includes('aria-label="Why the failure for fail.co is not a real loss"') && v.includes('aria-label="Acknowledge the failure for fail.co"') && v.includes('aria-label="Cancel acknowledging fail.co"'));
  const ak0 = b.calls.length; ackDown = true; fire('click', el({ 'data-ack-go': 'fail.co' })); await ticks(20); v = view(); ackDown = false;
  const akb = b.calls.slice(ak0).find((c) => c.path === '/monitor/partner-ack');
  ok('review: the note sent is the one typed', akb && JSON.parse(akb.body).note === 'deleted by hand', akb && akb.body);
  ok('review: a failed save keeps the form open, the note intact, the error inside it', v.includes('data-ack-note="fail.co" value="deleted by hand"') && /class="ackf"><div class="bad-t" role="alert">Could not update/.test(v));
  ok('review: ...and says so aloud', /^Could not update/.test(live()), live());
  fire('click', el({ 'data-ack-cancel': 'fail.co' })); v = view();
  ok('review: Cancel clears the error and the draft', !v.includes('Could not update') && !v.includes('data-ack-note="fail.co"'));
  fire('click', el({ 'data-ack': 'fail.co', 'data-on': '1' })); v = view();
  ok('review: reopened after Cancel, the box is empty', v.includes('data-ack-note="fail.co" value=""'));
  fire('click', el({ 'data-ack-go': 'fail.co' })); await ticks(20);
  ok('review: a save that lands is announced', live() === 'Acknowledged fail.co', live());
  /* 13. every claimant, and the facts that lived only in hover text */
  v = view();
  ok('review: the claimed-by tooltip names EVERY partner, the shown one first', v.includes('Claimed by 2 partners: Alpha Partners, Beta Co.'));
  ok('review: a company row opens to Salesforce, the reason and every claimant, as text', /id="pd-fail_2eco-d"[\s\S]*?Salesforce[\s\S]*?Opportunity never created[\s\S]*?Claimed by[\s\S]*?Alpha Partners, Beta Co/.test(v));
  ok('review: the acknowledgement note is visible text in the row', /id="pd-acked_2eco-d"[\s\S]*?Yes — known test/.test(v));
  ok('review: a partner row opens to the columns a mid-width screen drops', /id="pp-pk_5fa-d"[\s\S]*?>Clicks<[\s\S]*?>Verified<[\s\S]*?>Opportunity</.test(v));
  ok('review: what a dagger means is said in words under the table', v.includes('A number marked † counts companies that skipped an earlier stage'));
  ok('review: the 24-hour figure says what it counts', v.includes('3 with a lead in the last 24 hours') && !v.includes('arrived in the last 24 hours'));
  fire('change', el({ 'data-psortsel': '' }, { value: 'name:asc' })); v = view(); const per2 = v.slice(v.indexOf('id="p-per"'));
  ok('review: card view sorts partners through a select', per2.indexOf('>Alpha<') > 0 && per2.indexOf('>Alpha<') < per2.indexOf('>b@beta.test<'));
  fire('change', el({ 'data-psortsel': '' }, { value: 'step1:desc' }));
  /* 14. a failed re-check of the gaps keeps the old result, and says how old */
  await new Promise((r) => { b.intervals.filter((x) => x.ms === 600000).forEach((x) => x.fn()); setImmediate(r); }); await ticks(20);
  gapsDown = true; await new Promise((r) => { b.intervals.filter((x) => x.ms === 600000).forEach((x) => x.fn()); setImmediate(r); }); await ticks(20); v = view(); gapsDown = false;
  ok('review: a failed gaps re-check keeps the old result AND says how old it is', /id="p-gaps"[\s\S]*?The last check failed[\s\S]*?showing the result from [0-9]/.test(v) && /id="p-gaps"[\s\S]*?data-v="1\+\?"/.test(v));
  /* 15. Model: flag-only mode, the deploy counters, the window, the caps */
  const MF = JSON.parse(JSON.stringify(MODEL)); MF.flags.llm_meta = false; MF.scrape.inProcess.writeFailed = 3; MF.scrape.inProcess.bypassFailed = 0;
  MF.scrape.unreadable.push({ domain: 'apifail.test', scrape_status: 'ok', error: 'timeout', checked_at: '2026-09-24T10:00:00Z', email: 'z@apifail.test', website: 'apifail.test', blocked: false, blocked_by: null });
  GW.show('model'); await ticks(20); GW.S.q.days = '30'; GW.TABS.model._set(MF); GW.TABS.model.render(); v = view();
  ok('review: flag-only mode -- the chip, the group and the lede all say Meta was still sent', v.includes('Flagged — Meta still sent') && v.includes('Flagged by the model — Meta still sent') && v.includes('its Meta events were still sent') && !/>Meta withheld</.test(v));
  ok('review: writes failed and blocks failed open are on the deploy card, red when there are any', /data-card="mdl-deploy"[\s\S]*?>failures<[\s\S]*?class="bad-t">3 writes failed<\/span> · <span class="">0 blocks failed open/.test(v));
  ok('review: the window is read back from the payload', v.includes('the last 30 days') && !v.includes('updating…'));
  GW.S.q.days = '7'; GW.TABS.model.render();
  ok('review: a window the payload was not read for says "updating…"', view().includes('updating…'));
  GW.S.q.days = '30'; GW.TABS.model.render(); v = view();
  ok('review: a model call that failed is not "read fine"', v.includes('page read; the model call failed') && !v.includes('>read fine<'));
  ok('review: the near-miss cap is said', v.includes('Showing the first 1 of 13'));
  ok('review: all three switches are named', v.includes('NON_ICP_BLOCK=false') && v.includes('NON_ICP_LLM_BLOCK=false') && v.includes('NON_ICP_LLM_META=false'));
  ok('review: Booked and Their website are in the row details, since the rows drop them at mid widths', /id="mdl-md_2d1-d"[\s\S]*?>Booked</.test(v) && /id="mdlu-[^"]*-d"[\s\S]*?>Their website</.test(v));
  GW.TABS.model._set(MODEL); GW.TABS.model.render(); v = view();
  ok('review: with Meta withholding on, the Meta group says only what was held back', v.includes('Only the conversion events were withheld; nothing else about these leads changed.') && !v.includes('They booked.'));
  GW.S.q = {};
  /* 16. Visitors and SDR */
  GW.show('visitors'); await ticks(20); v = view();
  ok('review: the long place, network and zone lists are named scroll regions', v.includes('role="region" aria-label="Where they were"') && v.includes('role="region" aria-label="Networks"') && v.includes('role="region" aria-label="Time zones"'));
  ok('review: the map button has no pressed state beside its changing label', v.includes('data-vis-map>') && !/data-vis-map aria-pressed/.test(v));
  GW.show('sdr'); await ticks(20); v = view();
  ok('review: the SDR date column says it is the newest QUALIFYING attempt', v.includes('Newest qualifying attempt (ET)') && !v.includes('Last attempt (ET)'));
  fire('input', el({ 'data-sdr-q': '' }, { value: 'acme' })); await ticks();
  ok('review: a search result is announced once the typing pauses', live() === '1 person matches the search', live());
  fire('input', el({ 'data-sdr-q': '' }, { value: '' }));
  /* 17. the words: website verdicts as the SERVER words them, and no false claims */
  const wrlSrc = (fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8').match(/function websiteReasonLabel\(reason\) \{[\s\S]*?\n\}/) || [])[0];
  const wrl = wrlSrc && new Function('WEBSITE_REASON_LABELS', wrlSrc + '\nreturn websiteReasonLabel;')(servedCfg.labels.website);
  const WIN = ['http_403', 'http_999', 'http_401', 'http_429', 'http_500', 'http_404', 'parked_confirmed', 'timeout', 'some_new_code', 'resolved'];
  ok('review: every website verdict reads exactly as the server words it', wrl && WIN.every((r) => GW.L.website(r) === wrl(r)), WIN.map((r) => r + '=' + GW.L.website(r) + '|' + (wrl && wrl(r))).join('; '));
  ok('review: the Meta chip for an unverified site never says "no website"', GW.L.metaShort('website') === 'site not verified');
  ok('review: the model and website reasons claim nothing about booking or dialling', !/books|dialled/.test(GW.L.metaWhy('model')) && !/books|dialled/.test(GW.L.metaWhy('website')));
  /* links: http(s) only, a bare domain gets https, any other scheme is NOT a link */
  ok('review: a link is http(s) or nothing -- javascript:, data: and mailto: are refused outright', GW.href('javascript:alert(1)') === null && GW.href('data:text/html,x') === null && GW.href('mailto:a@b.co') === null &&
     GW.href('acme.com') === 'https://acme.com' && GW.href('http://x.io/a') === 'http://x.io/a' && GW.href('') === null);
  /* a floor is never shown as a total (CLAUDE.md, "Needs attention") */
  GW.show('partners'); await ticks(20);
  GW.TABS.partners._set(Object.assign({}, PARTNERS, { lifecycle: Object.assign({}, PARTNERS.lifecycle, { needsAttentionComplete: false }) })); GW.TABS.partners.render(); v = view();
  ok('review: when the unbounded count could not run, Needs attention says AT LEAST', /data-card="p-attn"[\s\S]*?AT LEAST this many — the full count could not be read/.test(v));
  GW.TABS.partners._set(PARTNERS); GW.TABS.partners.render();
  ok('review: and says nothing of the sort when it could', !/AT LEAST/.test(view()));
  /* 18. structural: the guards a stubbed DOM cannot drive */
  const leadsSrc = fs.readFileSync(path.join(ROOT, 'monitor', 'js', 'leads.js'), 'utf8'), coreSrc = fs.readFileSync(path.join(ROOT, 'monitor', 'js', 'core.js'), 'utf8');
  ok('review: the change-log cache has no prototype', /var changes = Object\.create\(null\);/.test(leadsSrc));
  ok('review: the search debounce is cancelled on leaving, and reload refuses any other tab', /function deactivate\(\) \{ G\.stop\('leads'\); root = null; clearTimeout\(tmr\); \}/.test(leadsSrc) && /function reload\(keepPage\) \{\s*var cur = tab\(\); if \(cur !== 'leads' && cur !== 'blocked'\) return;/.test(leadsSrc));
  ok('review: a repaint never parks focus on a DISABLED control', /if \(n && n\.disabled && n\.parentNode/.test(coreSrc));
  const layoutSrc = fs.readFileSync(path.join(ROOT, 'tools', 'check-monitor-layout.mjs'), 'utf8'), xcSrc = fs.readFileSync(path.join(ROOT, 'tools', 'crosscheck-monitor.mjs'), 'utf8');
  ok('review: layout screenshots go OUTSIDE the repo by default, and the profile is thrown away', /const OUT = process\.env\.OUT \|\| join\(tmpdir\(\), 'gw-layout-shots'\);/.test(layoutSrc) && /mkdtempSync\(join\(tmpdir\(\)/.test(layoutSrc) && /process\.on\('exit', cleanup\)/.test(layoutSrc));
  ok('review: the crosscheck kills Chrome and removes its profile on ANY exit', /mkdtempSync\(join\(tmpdir\(\)/.test(xcSrc) && /process\.on\('exit', cleanup\)/.test(xcSrc) && /process\.on\('unhandledRejection'/.test(xcSrc));
  ok('review: .gitignore keeps a stray screenshot folder out of the repo', /^layout-shots\/$/m.test(fs.readFileSync(path.join(ROOT, '.gitignore'), 'utf8')));
  ok('review: the layout check measures the mid-width band (1280)', /'360,390,414,768,1024,1280,1440'/.test(layoutSrc));
  const cssR = fs.readFileSync(path.join(ROOT, 'monitor', 'app.css'), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');
  ok('review: the Custom dates row spans only the columns that exist', /\.lfl\.dates \{[^}]*grid-column: 1 \/ -1;/.test(cssR) && !/\.lfl\.dates \{[^}]*span 2/.test(cssR));
  ok('review: the card-view sort shows only where the header row is gone', /\.cardsort \{ display: none; \}\s*@container view \(max-width: 899px\) \{\s*\.cardsort \{ display: block; \}/.test(cssR));
  ok('review: header sort buttons are 44px wide on a coarse pointer too', /\.sortb \{ min-height: 44px; min-width: 44px;/.test(cssR));
  /* the nav checks below read the page as Partners left it, after a health
     run that could not reach /monitor/health (the "toString" check above
     opened Overview, which read health again) */
  GW.show('partners'); await ticks(20); healthDown = true; await GW.TABS.health.run(); healthDown = false; await ticks(20);

  /* Hash, tabs, nav */
  ok('nav: every rebuilt tab is registered with activate and deactivate', ['overview', 'health', 'dropoff', 'dupes', 'lm', 'leads', 'blocked', 'sdr', 'model', 'visitors', 'partners'].every((t) => GW.TABS[t] && GW.TABS[t].activate && GW.TABS[t].deactivate && GW.TABS[t].title));
  ok('nav: switching tab writes the hash', /tab=partners/.test(b.window.location.hash), b.window.location.hash);
  const navHtml = b.els['nav-side'] ? b.els['nav-side'].innerHTML : '';
  /* EVERY tab is rebuilt: no nav row leaves for the classic page, and the
     sidebar footer is the one way back to it until the switch (PR D) */
  ok('nav: no nav row links to the classic dashboard any more', !/class="nav" href="\/monitor\?token=/.test(navHtml) && !/\(classic dashboard\)/.test(navHtml));
  ok('nav: the classic dashboard is still one click away, in the footer', /<a href="\/monitor\?token=[^"]*">Open the classic dashboard<\/a>/.test(html));
  ok('nav: All leads and Blocked are rebuilt, no longer classic links', /data-tab="leads"/.test(navHtml) && /data-tab="blocked"/.test(navHtml) && !/href="\/monitor\?token=[^"]*#tab=leads"/.test(navHtml) && !/href="\/monitor\?token=[^"]*#tab=blocked"/.test(navHtml));
  /* the last health run could not reach /monitor/health: all nine server
     checks are red, and the badge -- now set by System health too -- says 9 */
  ok('nav: the badge follows the LATEST run, from either tab', /data-tab="health"[\s\S]*?<span class="badge b-bad">9<span class="sr-only"> checks red<\/span>/.test(navHtml), (navHtml.match(/data-tab="health"[^]*?<\/a>/) || [''])[0].slice(0, 300));

  /* ═══ 4. STRUCTURE ═════════════════════════════════════════════════════ */
  const css = fs.readFileSync(path.join(ROOT, 'monitor', 'app.css'), 'utf8');
  const block = (sel) => { const i = css.indexOf(sel + ' {'); return i < 0 ? '' : css.slice(i, css.indexOf('\n}', i)); };
  const names = (s) => new Set([...s.matchAll(/(--[a-z0-9-]+)\s*:/g)].map((m) => m[1]));
  const L = names(block('[data-theme="light"]')), D = names(block('[data-theme="dark"]'));
  ok('theme: the light block declares aliases', L.size >= 30, L.size);
  ok('theme: EVERY light alias is declared in dark', [...L].every((x) => D.has(x)), [...L].filter((x) => !D.has(x)).join(','));
  ok('theme: every dark alias is declared in light', [...D].every((x) => L.has(x)), [...D].filter((x) => !L.has(x)).join(','));
  const used = new Set();
  for (const f of mn.JS_ORDER) {
    const s = fs.readFileSync(path.join(ROOT, 'monitor', 'js', f), 'utf8');
    /* the FIRST argument only -- ic(name, cls): 'ext' is a class, not an icon */
    for (const m of s.matchAll(/\bic\(([^)]*)\)/g)) for (const q of m[1].split(',')[0].matchAll(/'([a-z][a-z-]*)'/g)) used.add(q[1]);
    for (const m of s.matchAll(/\['[a-z]+', '[^']+', '([a-z-]+)'/g)) used.add(m[1]);
  }
  for (const m of fs.readFileSync(path.join(ROOT, 'monitor-next.js'), 'utf8').matchAll(/#i-([a-z-]+)/g)) used.add(m[1]);
  const have = new Set(fs.readdirSync(path.join(ROOT, 'monitor', 'icons')).map((f) => f.replace(/\.svg$/, '')));
  ok('icons: the code asks for icons', used.size >= 20, used.size);
  ok('icons: EVERY icon the code asks for is in the sprite', [...used].every((x) => have.has(x)), [...used].filter((x) => !have.has(x)).join(','));
  ok('tokens: the design tokens are the plugin’s, copied verbatim', /v1\.49\.0/.test(fs.readFileSync(path.join(ROOT, 'monitor', 'tokens.css'), 'utf8').slice(0, 400)));
  const allJs = mn.JS_ORDER.map((f) => fs.readFileSync(path.join(ROOT, 'monitor', 'js', f), 'utf8')).join('\n');
  ok('dates: no calendar date from the viewer’s laptop clock', !/\.(getFullYear|getMonth|getDate|getHours)\(\)/.test(allJs));
  ok('css: every colour is a token or an alias', !/#[0-9a-fA-F]{3,8}\b/.test(css.replace(/\/\*[\s\S]*?\*\//g, '')), (css.replace(/\/\*[\s\S]*?\*\//g, '').match(/#[0-9a-fA-F]{3,8}\b/g) || []).slice(0, 5).join(','));
  /* THE CARD RULES REACH THE OUTER TABLE ONLY. As descendant selectors they
     also restyled every table inside an expanded row: on a phone the lead's
     change log kept its header row above cells stacked with no labels. */
  const cssNc = css.replace(/\/\*[\s\S]*?\*\//g, '');
  ok('css: no rtable rule reaches a table nested in its detail row', !/\.rt (tbody|thead|tr|td|th)\b/.test(cssNc) && /\.rt > tbody > tr\.detail > td \{/.test(cssNc), (cssNc.match(/\.rt (tbody|thead|tr|td|th)\b[^{]*/g) || []).slice(0, 4).join(' | '));
  ok('css: a plain table stacks, labelled, below 560', /@container view \(max-width: 559px\) \{[^@]*\.tbl\.stack td::before \{ content: attr\(data-l\)/.test(cssNc) && /\.tbl\.stack thead \{ display: none; \}/.test(cssNc));
  ok('css: a link that is a detail value is thumb-sized on touch', /@media \(max-width: 1023px\), \(pointer: coarse\) \{[^@]*\.kv \.v a \{[^}]*min-height: 44px/.test(cssNc));
  ok('tables: every plain table outside the chart goes through U.grid', mn.JS_ORDER.filter((f) => f !== 'chart.js').every((f) => !/class="tbl"/.test(fs.readFileSync(path.join(ROOT, 'monitor', 'js', f), 'utf8'))),
    mn.JS_ORDER.filter((f) => f !== 'chart.js' && /class="tbl"/.test(fs.readFileSync(path.join(ROOT, 'monitor', 'js', f), 'utf8'))).join(','));
  const pv = fs.readFileSync(path.join(ROOT, 'tools', 'preview-monitor.js'), 'utf8');
  ok('preview: every connection is read-only at the database', /default_transaction_read_only=on/.test(pv));
  ok('preview: every non-GET is refused', /req\.method !== 'GET' && req\.method !== 'HEAD'/.test(pv) && /status\(405\)/.test(pv));
  ok('preview: it never boots index.js', !/require\([^)]*index(\.js)?['"]\)/.test(pv));

  finish();
})();

function finish() {
  loud();
  console.log('');
  if (failures.length) { console.log('  FAILURES:'); for (const f of failures) console.log('   ✗ ' + f); }
  console.log(`  passed: ${pass}`);
  console.log(`  failed: ${fail}`);
  console.log('');
  process.exit(fail ? 1 : 0);
}
