/* ============================================================
   form_page_views, and what happens when writing it FAILS — EXECUTED.

   form_sessions.hits is a counter. Reconstructing what a visitor actually
   did meant reading Railway deploy logs, which roll off with the
   deployment -- that is the whole reason this table exists.

   The load-bearing property is NOT that the row gets written. It is that
   nothing downstream notices when it does not. This table is a reporting
   convenience sitting inside the handler that records every page load;
   if it can take /session down it is worse than not existing. So the
   suite drives the route three ways: the write succeeding, the write
   throwing, and the form_sessions upsert itself throwing.

   A source assertion could only show a try/catch is present. This boots
   the real index.js with pg stubbed, drives POST /session over real HTTP,
   and compares what reached the database and what came back.

   Dependency-free. Run:  node tests/test-session-page-views.js
   ============================================================ */

require('./crash-reporter')('test-session-page-views');

const Module = require('module');
const path   = require('path');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
function eq(name, a, b) { ok(name, a === b, `got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`); }

const PORT = 41238;
const BASE = `http://127.0.0.1:${PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* failOn: which statement the database should reject this run. */
const S = { queries: [], failOn: null };
let unhandled = 0;
process.on('unhandledRejection', () => { unhandled++; });

function stubQuery(q, params) {
  const flat = (typeof q === 'string' ? q : (q && q.text) || '').replace(/\s+/g, ' ').trim();
  const isPageView = /INSERT INTO form_page_views/i.test(flat);
  const isSession  = /INSERT INTO form_sessions/i.test(flat);
  if (isPageView || isSession) S.queries.push({ flat, params, kind: isPageView ? 'page_view' : 'session' });
  if (S.failOn === 'page_view' && isPageView) throw new Error('simulated: relation form_page_views is unavailable');
  if (S.failOn === 'session'   && isSession)  throw new Error('simulated: relation form_sessions is unavailable');
  if (isSession) return { rows: [{ hits: 3 }], rowCount: 1 };
  return { rows: [], rowCount: 0 };
}
class StubClient { async query(q, p) { return stubQuery(q, p); } release() {} }
class StubPool {
  async connect() { return new StubClient(); }
  async query(q, p) { return stubQuery(q, p); }
  on() {} async end() {}
}
const origLoad = Module._load;
Module._load = function (request) {
  if (request === 'pg') return { Pool: StubPool, Client: StubClient, types: { setTypeParser() {} } };
  return origLoad.apply(this, arguments);
};

const realFetch = global.fetch.bind(global);
global.fetch = async (url, opts) =>
  String(url).startsWith(BASE) ? realFetch(url, opts) : { ok: true, status: 200, text: async () => 'ok', json: async () => ({}) };

Object.assign(process.env, {
  PORT: String(PORT), DATABASE_URL: 'postgres://stub/stub',
  ALLOWED_ORIGIN: 'https://www.gushwork.ai', MONITOR_TOKEN: 'stub',
});

const realLog = console.log, realWarn = console.warn, realErr = console.error;
/* Captured rather than discarded: which line the route logs is the only
   externally visible difference between "the page view failed" and "the
   whole visit failed", and a log that claims the second when the first
   happened is the same class of untruth this repo bans in verdicts. */
let logged = [];
const cap = (...a) => { logged.push(a.map(String).join(' ')); };
const quiet = () => { console.log = console.warn = console.error = cap; };
const loud  = () => { console.log = realLog; console.warn = realWarn; console.error = realErr; };

quiet();
require(path.join(__dirname, '..', 'index.js'));

const PAGE = 'https://www.gushwork.ai/start?utm_source=facebook&utm_term=T&fbadid=12345';

async function drive(failOn) {
  S.queries = []; S.failOn = failOn; logged = [];
  const res = await realFetch(BASE + '/session', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Origin: 'https://www.gushwork.ai', 'User-Agent': 'suite/1.0' },
    body: JSON.stringify({
      session_id: 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',
      page_url: PAGE, referrer: 'https://l.facebook.com/', page_referrer: 'https://www.gushwork.ai/pricing',
      utm_source: 'facebook', utm_term: 'T',
    }),
  });
  let body = null;
  try { body = await res.json(); } catch { /* non-JSON */ }
  await sleep(400);
  return { status: res.status, body, queries: S.queries.slice(), logged: logged.slice() };
}

(async () => {
  await sleep(900);

  /* ── A. Happy path ─────────────────────────────────────────── */
  quiet(); const a = await drive(null); loud();
  eq('A: /session responds 200', a.status, 200);
  eq('A: body is ok', JSON.stringify(a.body), JSON.stringify({ ok: true }));
  ok('A: form_sessions upsert ran', a.queries.some((q) => q.kind === 'session'));
  const pv = a.queries.find((q) => q.kind === 'page_view');
  ok('A: form_page_views insert ran', !!pv);
  if (pv) {
    eq('A: session_id recorded', pv.params[0], 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee');
    eq('A: page_url kept WHOLE, query string included', pv.params[1], PAGE);
    eq('A: hit_no comes from the upsert RETURNING, not a second read', pv.params[2], 3);
    ok("A: source is the literal 'session_route'", /'session_route'/.test(pv.flat), pv.flat);
    ok('A: no referrer column is written in v1', !/referrer/i.test(pv.flat), pv.flat);
  }

  /* ── B. The page-view write FAILS ──────────────────────────── */
  const before = unhandled;
  quiet(); const b = await drive('page_view'); loud();
  eq('B: /session STILL responds 200 when the page view write fails', b.status, 200);
  eq('B: body unchanged', JSON.stringify(b.body), JSON.stringify({ ok: true }));
  ok('B: the form_sessions upsert still ran', b.queries.some((q) => q.kind === 'session'));
  ok('B: the page view was attempted and rejected', b.queries.some((q) => q.kind === 'page_view'));
  eq('B: no unhandled rejection escaped', unhandled, before);

  /* The inner try/catch earns its place HERE. Without it the page-view
     failure falls through to the outer catch, which reports the whole
     visit as unrecorded -- when form_sessions was in fact written. The
     response and the upsert are identical either way, so this is the only
     assertion that can tell the two apart. */
  ok('B: reports the PAGE VIEW as unrecorded',
     b.logged.some((l) => /page view not recorded/.test(l)), b.logged.join(' | ') || '(nothing logged)');
  ok('B: does NOT claim the whole visit went unrecorded',
     !b.logged.some((l) => /visit not recorded/.test(l)), b.logged.join(' | '));

  /* Identical to the happy path in everything a caller can observe. */
  eq('B: status identical to the happy path', b.status, a.status);
  eq('B: body identical to the happy path', JSON.stringify(b.body), JSON.stringify(a.body));
  const aSess = a.queries.find((q) => q.kind === 'session');
  const bSess = b.queries.find((q) => q.kind === 'session');
  eq('B: form_sessions received identical params', JSON.stringify(bSess && bSess.params), JSON.stringify(aSess && aSess.params));

  /* ── C. The upsert itself fails — the pre-existing guarantee ── */
  const before2 = unhandled;
  quiet(); const c = await drive('session'); loud();
  eq('C: /session still responds 200 when form_sessions fails', c.status, 200);
  eq('C: no unhandled rejection escaped', unhandled, before2);
  ok('C: reports the VISIT as unrecorded',
     c.logged.some((l) => /visit not recorded/.test(l)), c.logged.join(' | ') || '(nothing logged)');
  ok('C: no page view is written when there is no hit number to attach',
     !c.queries.some((q) => q.kind === 'page_view'));

  console.log('');
  if (failures.length) {
    console.log('  FAILURES:');
    for (const f of failures) console.log('   ✗ ' + f);
  }
  console.log(`  passed: ${pass}`);
  console.log(`  failed: ${fail}`);
  console.log('');
  process.exit(fail ? 1 : 0);
})();
