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
function browser(payloads) {
  const els = {}, listeners = {}, intervals = [], calls = [];
  const mk = (id) => {
    const attrs = {}, cls = new Set();
    const e = {
      id, _html: '', style: {}, clientWidth: 960, hidden: false,
      get innerHTML() { return this._html; }, set innerHTML(v) { this._html = String(v); },
      get textContent() { return this._html.replace(/<[^>]*>/g, ''); }, set textContent(v) { this._html = String(v); },
      classList: { add: (c) => cls.add(c), remove: (c) => cls.delete(c), contains: (c) => cls.has(c), toggle: (c, on) => { if (on === undefined ? !cls.has(c) : on) cls.add(c); else cls.delete(c); } },
      setAttribute: (k, v) => { attrs[k] = String(v); }, getAttribute: (k) => (k in attrs ? attrs[k] : null), removeAttribute: (k) => { delete attrs[k]; }, hasAttribute: (k) => k in attrs,
      querySelector: () => null, querySelectorAll: () => [], contains: () => false, focus() {}, addEventListener() {}, closest: () => null,
    };
    return e;
  };
  const document = {
    getElementById: (id) => els[id] || (els[id] = mk(id)),
    querySelector: () => null, querySelectorAll: () => [],
    documentElement: mk('__html'), body: mk('__body'), activeElement: null, title: '', visibilityState: 'visible',
    addEventListener: (t, f) => { (listeners[t] = listeners[t] || []).push(f); },
    createElement: () => mk('__created'), fonts: { ready: Promise.resolve(), check: () => true },
  };
  const window = {
    location: { hash: '' }, history: { replaceState: (a, b, h) => { window.location.hash = h; } },
    localStorage: { getItem: () => null, setItem() {} }, matchMedia: () => ({ matches: false, addEventListener() {} }),
    addEventListener() {}, innerWidth: 1440, scrollTo() {}, alert() {},
    __GW__: { token: TOKEN, tz: 'America/New_York', classic: '/monitor' },
  };
  const fetchStub = async (url, init) => {
    const u = new URL(String(url), 'http://x');
    calls.push({ path: u.pathname, q: Object.fromEntries(u.searchParams), method: (init && init.method) || 'GET' });
    const body = payloads(u.pathname, Object.fromEntries(u.searchParams));
    if (body instanceof Error) return { ok: false, status: 503, json: async () => ({}), text: async () => body.message };
    return { ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) };
  };
  const setIntervalStub = (fn, ms) => { intervals.push({ fn, ms }); return intervals.length; };
  return { els, listeners, intervals, calls, document, window, fetch: fetchStub, setInterval: setIntervalStub };
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
  ok('page: the skip link is its own class, visible when focused', /<a class="skip" href="#view">/.test(html) && /\.skip:focus \{[^}]*position: fixed/.test(fs.readFileSync(path.join(ROOT, 'monitor', 'app.css'), 'utf8')));
  ok('page: ONE live region, in the shell rather than in a tab', (html.match(/id="gw-live"/g) || []).length === 1 && /id="gw-live" class="sr-only" role="status" aria-live="polite"/.test(html));
  const HEALTH = { checks: {
    apollo: { state: 'red', text: 'Out of credits for 2d', detail: 'You have insufficient credits! · 86 refused in the last 24h' },
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
  const METRICS = { enriched: 4537, enrichTitlePct: 55, enrichFundingPct: 8, enrichLocationPct: 63 };
  let healthDown = false;
  const payloads = (p, q) => {
    if (p === '/monitor/overview') return P[q.view] || P.week;
    if (p === '/monitor/health') return healthDown ? new Error('upstream down') : HEALTH;
    if (p === '/monitor/elv-health') return { state: 'insufficient_data', rate: 0, checks: 0 };
    if (p === '/monitor/dropoff') return DROP;
    if (p === '/monitor/duplicates') return DUPES;
    if (p === '/monitor/lm-metrics') return LM_M;
    if (p === '/monitor/lm-leads') return LM_L;
    if (p === '/monitor/enrichment-coverage') return COV;
    if (p === '/monitor/metrics') return METRICS;
    if (p === '/health') return { ok: true };
    return {};
  };
  const b = browser(payloads);
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
  ok('attention: its detail is shown, escaped', v.includes('You have insufficient credits!'));
  ok('attention: what it COSTS comes first, the vendor detail after, each its own line',
     v.indexOf('they arrive without title') > 0 && v.indexOf('they arrive without title') < v.indexOf('You have insufficient credits!') && /<div class="attn-d">You have insufficient credits!/.test(v));
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
  ok('lm: daily volume reads as dates', /<td class="day">Thu 24 Sep<\/td>/.test(v));
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

  /* Hash, tabs, nav */
  ok('nav: every rebuilt tab is registered with activate and deactivate', ['overview', 'health', 'dropoff', 'dupes', 'lm'].every((t) => GW.TABS[t] && GW.TABS[t].activate && GW.TABS[t].deactivate && GW.TABS[t].title));
  ok('nav: switching tab writes the hash', /tab=lm/.test(b.window.location.hash), b.window.location.hash);
  const navHtml = b.els['nav-side'] ? b.els['nav-side'].innerHTML : '';
  ok('nav: the tabs not rebuilt link to the classic dashboard', /href="\/monitor\?token=[^"]*#tab=leads"/.test(navHtml) && /href="\/monitor\?token=[^"]*#tab=partners"/.test(navHtml));
  ok('nav: a classic link says it leaves', /\(classic dashboard\)/.test(navHtml));
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
