/* ============================================================
   The API, RELEASE against the working tree, without the monitor's token.

   WHY THIS EXISTS. v5.23.0 put a synthetic-check branch into every form
   route, a guard at the top of every outbound choke point, and a wrapper
   around both database pools and the console. All of it is meant to change
   NOTHING for a request without a valid X-GW-Synthetic token. That is a
   claim about every byte a visitor's request produces, and the other
   suites can only check the bytes they already expect.

   So this boots BOTH servers -- RELEASE's index.js taken whole out of git,
   and the working tree's -- each in its own process with the same stubs
   (tests/server-diff-harness.js), sends both the same requests, and
   requires, request by request:
     - the same status, the same body and the same response headers;
     - the same database statements with the same parameters;
     - the same outbound requests with the same headers and bodies.
   Every form route as a visitor, the same routes carrying a WRONG token
   (which must be an ordinary request), the CORS preflight, a foreign
   origin, an oversized body and a missing session id.

   RELEASE is v5.23.0 since v5.24.0 (it was v5.22.0 for v5.23.0's PR). The
   same requests now also go out with the RIGHT token, which must behave
   exactly as v5.23.0 did.

   KNOWN, INTENDED DIFFERENCES (v5.24.0), asserted as such rather than hidden:
     - a WRONG or EMPTY token: v5.23.0 handled it as a visitor; now it is
       401 {"ok":false,"error":"synthetic token rejected","synthetic":false}
       and the trace shows NOTHING ran -- no statement, no request;
     - an unauthenticated /health/deep or /metrics/submissions is answered
       before the monitor's budget, so its rate-limit headers are the
       strangers' budget (30), not the monitor's (120).

   Generated values (UUIDs, timestamps, durations) are normalised; side
   effects that run after the response are compared as a set, because two
   processes do not schedule fire-and-forget work in the same order.

   Needs git. No network, no database. Run:
     node tests/test-server-differential.js
   ============================================================ */

require('./crash-reporter')('test-server-differential');

const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn, execFileSync } = require('child_process');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}

const ROOT = path.join(__dirname, '..');
/* v5.23.0: the merge of PR 152, the release before synthetic checks failed
   closed. (Was v5.22.0, 5db62fb, for v5.23.0's own PR.) */
const RELEASE = 'c2220b1f646cec99c7fb1c08ff367b0981c5c06c';
const RIGHT_TOKEN = 'the-real-token-0123456789abcdefghijklmnopqr';
const REFUSED = JSON.stringify({ ok: false, error: 'synthetic token rejected', synthetic: false });
const PORTS = { rel: 41311, cur: 41312 };
const WRONG_TOKEN = 'not-the-token-0123456789abcdefghijklmnopq';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* ── RELEASE, unpacked beside node_modules ───────────────────────── */
const relDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gw-server-diff-'));
execFileSync('sh', ['-c', `git -C "${ROOT}" archive ${RELEASE} | tar -x -C "${relDir}"`]);
fs.symlinkSync(path.join(ROOT, 'node_modules'), path.join(relDir, 'node_modules'), 'dir');

const children = [];
function boot(which, dir) {
  const trace = path.join(relDir, `trace-${which}.jsonl`);
  fs.writeFileSync(trace, '');
  const env = Object.assign({}, process.env, {
    PORT: String(PORTS[which]), TRACE_FILE: trace,
    DATABASE_URL: 'postgres://stub/stub',
    AWS_PG_HOST: 'stub', AWS_PG_PORT: '5432', AWS_PG_DATABASE: 'stub', AWS_PG_USER: 'stub', AWS_PG_PASSWORD: 'stub',
    SLACK_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB',
    SLACK_ALERTS_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB-ALERTS',
    SF_CLIENT_ID: 'stub', SF_CLIENT_SECRET: 'stub', SF_REFRESH_TOKEN: 'stub',
    META_ACCESS_TOKEN: 'stub', META_PIXEL_ID: '1234567890',
    ELV_API_KEY: 'stub', APOLLO_API_KEY: 'stub-apollo',
    PARTNERSTACK_TRACKING_TOKEN: 'stub', PARTNERSTACK_PUBLIC_KEY: 'stub', PARTNERSTACK_SECRET_KEY: 'stub',
    NON_ICP_BLOCK: 'true', NON_ICP_LLM_ENABLED: 'true', ANTHROPIC_API_KEY: 'stub',
    /* RELEASE reads this; the working tree reads allowed-origins.js, which
       holds the same three. */
    ALLOWED_ORIGIN: 'https://www.gushwork.ai,https://gushwork.ai,https://gushwork.webflow.io',
    MONITOR_TOKEN: 'stub',
    /* Set, so the working tree's synthetic path is LIVE -- and the wrong
       token below is a real near miss, not a feature that is switched off. */
    SYNTHETIC_TOKEN: RIGHT_TOKEN,
  });
  delete env.NODE_OPTIONS;
  const child = spawn(process.execPath, ['-r', path.join(__dirname, 'server-diff-harness.js'), path.join(dir, 'index.js')],
    { cwd: dir, env, stdio: ['ignore', 'ignore', 'pipe'] });
  let err = '';
  child.stderr.on('data', (d) => { err += d; });
  children.push(child);
  return { which, trace, child, base: `http://127.0.0.1:${PORTS[which]}`, err: () => err, read: 0 };
}
process.on('exit', () => { for (const c of children) { try { c.kill('SIGKILL'); } catch (_) {} } try { fs.rmSync(relDir, { recursive: true, force: true }); } catch (_) {} });

async function up(s) {
  for (let i = 0; i < 100; i++) {
    try { const r = await fetch(s.base + '/health'); if (r.ok) return true; } catch (_) {}
    await sleep(100);
  }
  return false;
}
function linesSince(s) {
  const all = fs.readFileSync(s.trace, 'utf8').split('\n').filter(Boolean);
  const out = all.slice(s.read); s.read = all.length;
  return out.map((l) => JSON.parse(l));
}
async function quiet(...servers) {
  let last = -1, still = 0;
  for (let i = 0; i < 60; i++) {
    const n = servers.reduce((a, s) => a + fs.statSync(s.trace).size, 0);
    if (n === last) { if (++still >= 4) return; } else { still = 0; last = n; }
    await sleep(50);
  }
}

/* Values that are generated, not decided: equal in kind, not in bytes. */
function norm(v) {
  return JSON.stringify(v)
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, '<uuid>')
    .replace(/\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z/g, '<iso>')
    .replace(/\b1[6-9]\d{8}(\d{3})?\b/g, '<epoch>')
    /* a quote may arrive escaped, because a body is a JSON string inside JSON */
    .replace(/(\\?")ms\1:\d+/g, '$1ms$1:<n>')
    .replace(/\| \d+ms/g, '| <n>ms')
    .replace(/\d{1,2}:\d\d (AM|PM)/g, '<time>')
    /* the website check's random probe path, and the random tail of a
       Meta event_id */
    .replace(/gw-probe-[a-z0-9]+/g, 'gw-probe-<rand>')
    .replace(/(Lead|Schedule|StartTrial|Contact)_<uuid>_[a-z0-9]+/g, '$1_<uuid>_<rand>')
    /* an express error page's stack trace names the file's path, and
       RELEASE runs from a temporary directory */
    .replace(/<br> (&nbsp;\s?)+at [^<]*/g, '');
}
/* content-length is kept for JSON answers only: an HTML error page's length
   moves with the stack-trace path above. */
const KEEP_HEADERS = ['content-type', 'access-control-allow-origin', 'access-control-allow-methods', 'access-control-allow-headers',
  'access-control-allow-credentials', 'vary', 'cache-control', 'ratelimit-policy', 'ratelimit-limit', 'ratelimit-remaining', 'content-length'];
async function send(s, req) {
  const res = await fetch(s.base + req.path, { method: req.method || 'POST', headers: req.headers, body: req.body });
  const body = await res.text();
  const headers = {};
  for (const h of KEEP_HEADERS) { const v = res.headers.get(h); if (v != null) headers[h] = v; }
  if (!/json/.test(headers['content-type'] || '')) delete headers['content-length'];
  return { status: res.status, headers, body };
}

(async () => {
  const rel = boot('rel', relDir);
  const cur = boot('cur', ROOT);
  const upRel = await up(rel), upCur = await up(cur);
  ok('RELEASE booted', upRel, rel.err().slice(0, 300));
  ok('the working tree booted', upCur, cur.err().slice(0, 300));
  if (!upRel || !upCur) return finish();
  /* Let the boot-time sweeps finish before anything is attributed. */
  await sleep(1500); await quiet(rel, cur); linesSince(rel); linesSince(cur);

  const J = { 'Content-Type': 'application/json', Origin: 'https://www.gushwork.ai', 'User-Agent': 'suite/1.0' };
  const LEAD = {
    email: 'ada@acmewidgets.com', website: 'acmewidgets.com', first_name: 'Ada', last_name: 'Lovelace',
    phone: '+12015550123', company: 'Acme Widgets', sell_to: 'B2B', page_url: 'https://www.gushwork.ai/demo',
    website_check_reason: 'content_clean', website_check_failed: false, ps_xid: 'xid-stub-12345', ps_partner_key: 'pk-stub',
    hear_about_us: 'Google search', utm_source: 'google', utm_medium: 'cpc', utm_campaign: 'AEO_Search',
  };
  const sidOf = (n) => `5e55e55e-0000-4000-8000-${String(n).padStart(12, '0')}`;
  const ROUTES = [
    ['/session', (n) => ({ session_id: sidOf(n), page_url: 'https://www.gushwork.ai/demo', utm_source: 'google' })],
    ['/verify-email', (n) => ({ email: `ada${n}@acmewidgets.com` })],
    ['/non-icp-check', (n) => ({ email: `ada${n}@pumpco${n}.com`, website: `pumpco${n}.com` })],
    ['/enrich', (n) => ({ session_id: sidOf(n), email: 'ada@acmewidgets.com' })],
    ['/verify-website', () => ({ website: 'acmewidgets.com', email: 'ada@acmewidgets.com' })],
    ['/resolve-website', () => ({ website: 'acmewidgets.com' })],
    ['/partial', (n) => Object.assign({ session_id: sidOf(n), step_reached: 1 }, LEAD)],
    ['/submit', (n) => Object.assign({ session_id: sidOf(n) }, LEAD)],
    ['/submit', (n) => Object.assign({ session_id: sidOf(n) }, LEAD, { website: 'kw.com', email: 'agent@kw.com' })],
    ['/submit', (n) => Object.assign({ session_id: sidOf(n) }, LEAD, { email: 'b@g.ai' })],
    ['/booking-confirmed', (n) => ({ session_id: sidOf(n), booking_uid: 'bk-' + n, start_time: '2026-10-20T15:00:00Z', event_type: 'demo' })],
  ];
  const REQS = [];
  let n = 100;
  for (const [route, body] of ROUTES) REQS.push({ label: `${route} as a visitor`, path: route, headers: J, body: JSON.stringify(body(++n)) });
  for (const [route, body] of ROUTES) REQS.push({ label: `${route} with the RIGHT token`, path: route, headers: Object.assign({ 'X-GW-Synthetic': RIGHT_TOKEN }, J), body: JSON.stringify(body(++n)) });
  for (const [route, body] of ROUTES) REQS.push({ label: `${route} with a WRONG token`, refused: true, path: route, headers: Object.assign({ 'X-GW-Synthetic': WRONG_TOKEN }, J), body: JSON.stringify(body(++n)) });
  for (const [route, body] of ROUTES) REQS.push({ label: `${route} with an EMPTY token`, refused: true, path: route, headers: Object.assign({ 'X-GW-Synthetic': '' }, J), body: JSON.stringify(body(++n)) });
  REQS.push({ label: '/submit with no session id', path: '/submit', headers: J, body: JSON.stringify(LEAD) });
  REQS.push({ label: '/submit with a body over 50kb', path: '/submit', headers: J, body: JSON.stringify(Object.assign({ session_id: sidOf(++n) }, LEAD, { about_business: 'x'.repeat(60000) })) });
  REQS.push({ label: '/submit from a foreign origin', path: '/submit', headers: Object.assign({}, J, { Origin: 'https://evil.example' }), body: JSON.stringify({ session_id: sidOf(++n) }) });
  REQS.push({ label: '/session from Webflow staging', path: '/session', headers: Object.assign({}, J, { Origin: 'https://gushwork.webflow.io' }), body: JSON.stringify({ session_id: sidOf(++n) }) });
  REQS.push({ label: '/health', method: 'GET', path: '/health', headers: {} });

  /* Which per-IP visitor budget each route draws on. A refused request
     (v5.24.0) is answered before any of them, where v5.23.0 ran it and spent
     one -- so on later requests ratelimit-remaining is higher by EXACTLY the
     number of refusals on the same budget, and that is what is required. */
  const LIMITER = { '/partial': 'global', '/submit': 'global', '/session': 'global', '/booking-confirmed': 'global',
                    '/verify-email': 'strict', '/enrich': 'strict', '/verify-website': 'strict', '/resolve-website': 'strict' };
  const refusedOn = { global: 0, strict: 0 };
  for (const req of REQS) {
    const a = await send(rel, req);
    const b = await send(cur, req);
    await quiet(rel, cur);
    const ta = linesSince(rel).map(norm).sort(), tb = linesSince(cur).map(norm).sort();
    if (req.refused) {
      /* THE INTENDED DIFFERENCE: v5.23.0 ran it as a visitor; now nothing runs. */
      ok(`${req.label}: v5.23.0 handled it as a visitor (${a.status}, ${ta.length} statements and requests)`, a.status !== 401 && ta.length > 0, a.status + ' ' + ta.length);
      ok(`${req.label}: now 401 with the refusal body`, b.status === 401 && b.body === REFUSED, b.status + ' ' + b.body.slice(0, 120));
      ok(`${req.label}: now NOTHING ran -- no database statement, no outbound request`, tb.length === 0, tb.slice(0, 2).join(' ').slice(0, 200));
      ok(`${req.label}: the 401 carries the CORS header for the allowed origin`, b.headers['access-control-allow-origin'] === 'https://www.gushwork.ai', JSON.stringify(b.headers));
      ok(`${req.label}: the 401 spends no visitor budget (no rate-limit headers)`, !('ratelimit-remaining' in b.headers), JSON.stringify(b.headers));
      if (LIMITER[req.path]) refusedOn[LIMITER[req.path]]++;
      continue;
    }
    const lim = LIMITER[req.path];
    if (lim && 'ratelimit-remaining' in a.headers) {
      const d = Number(b.headers['ratelimit-remaining']) - Number(a.headers['ratelimit-remaining']);
      ok(`${req.label}: visitor budget left is higher by exactly the ${refusedOn[lim]} refused requests on it (v5.23.0 spent one each)`, d === refusedOn[lim], `difference ${d}`);
      a.headers['ratelimit-remaining'] = b.headers['ratelimit-remaining'] = '(compared above)';
    }
    /* Byte for byte where the answer holds nothing generated; otherwise
       after normalising only the generated values. Which one applied is in
       the assertion's name, so a reader can see it. */
    const exact = a.body === b.body;
    const bodyA = exact ? a.body : norm(a.body), bodyB = exact ? b.body : norm(b.body);
    ok(`${req.label}: same status (${a.status})`, a.status === b.status, `${a.status} / ${b.status}`);
    ok(`${req.label}: same body, ${exact ? 'byte for byte' : 'apart from generated values'}`, bodyA === bodyB, `RELEASE ${bodyA.slice(0, 160)} | NOW ${bodyB.slice(0, 160)}`);
    ok(`${req.label}: same response headers`, JSON.stringify(a.headers) === JSON.stringify(b.headers), JSON.stringify([a.headers, b.headers]));
    const onlyA = ta.filter((x) => tb.indexOf(x) === -1), onlyB = tb.filter((x) => ta.indexOf(x) === -1);
    ok(`${req.label}: the same database statements and outbound requests (${ta.length})`,
       ta.length === tb.length && onlyA.length === 0 && onlyB.length === 0,
       `only RELEASE: ${onlyA.slice(0, 2).join(' ').slice(0, 300)} | only NOW: ${onlyB.slice(0, 2).join(' ').slice(0, 300)}`);
    if (/as a visitor/.test(req.label) && req.path === '/submit' && /ada@acmewidgets/.test(req.body)) {
      ok(`${req.label}: the comparison saw real side effects (Salesforce, Meta, Slack, PartnerStack)`,
         ['salesforce.com/services', 'graph.facebook.com', 'hooks.slack.com', 'partnerlinks.io'].every((h) => ta.some((x) => x.indexOf(h) !== -1)), ta.length + ' events');
    }
  }

  /* ── THE INTENDED DIFFERENCES ─────────────────────────────────── */
  const pre = { label: 'preflight', method: 'OPTIONS', path: '/submit',
    headers: { Origin: 'https://www.gushwork.ai', 'Access-Control-Request-Method': 'POST', 'Access-Control-Request-Headers': 'content-type' } };
  const pa = await send(rel, pre), pb = await send(cur, pre);
  ok('preflight: identical to v5.23.0, X-GW-Synthetic allowed', pa.status === 204 && JSON.stringify(pa.headers) === JSON.stringify(pb.headers)
     && pb.headers['access-control-allow-headers'] === 'Content-Type,X-GW-Synthetic', JSON.stringify([pa.headers, pb.headers]));
  for (const route of ['/health/deep', '/metrics/submissions']) {
    const ra = await send(rel, { method: 'GET', path: route, headers: {} });
    const rb = await send(cur, { method: 'GET', path: route, headers: {} });
    ok(`${route} without a token: 401 and the same body, as in v5.23.0`, ra.status === 401 && rb.status === 401 && ra.body === rb.body, ra.status + '/' + rb.status);
    ok(`${route} without a token: answered BEFORE the monitor's budget -- v5.23.0 spent it (limit 120), now the strangers' budget (limit 30)`,
       ra.headers['ratelimit-limit'] === '120' && rb.headers['ratelimit-limit'] === '30', JSON.stringify([ra.headers['ratelimit-limit'], rb.headers['ratelimit-limit']]));
    const ga = await send(rel, { method: 'GET', path: route, headers: { 'X-GW-Synthetic': RIGHT_TOKEN } });
    const gb = await send(cur, { method: 'GET', path: route, headers: { 'X-GW-Synthetic': RIGHT_TOKEN } });
    ok(`${route} with the token: the same status as v5.23.0 (${ga.status})`, ga.status === gb.status, ga.status + '/' + gb.status);
  }
  linesSince(rel); linesSince(cur);
  finish();
})().catch((e) => { failures.push('suite error: ' + e.message); fail++; finish(); });

function finish() {
  for (const c of children) { try { c.kill('SIGKILL'); } catch (_) {} }
  console.log('');
  if (failures.length) {
    console.log('  FAILURES:');
    for (const f of failures) console.log('   ✗ ' + f);
  }
  console.log(`  passed: ${pass}`);
  console.log(`  failed: ${fail}`);
  console.log('');
  process.exit(fail ? 1 : 0);
}
