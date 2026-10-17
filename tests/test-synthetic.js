/* ============================================================
   Synthetic website-monitor checks (v5.23.0) -- BOOTED AND EXECUTED.

   gushwork-web's monitor fills the real forms with a token in the
   X-GW-Synthetic header. Such a run must take the same paths as a visitor
   and leave NOTHING behind: no row in any table, no Salesforce, Slack,
   email, Meta, Google Ads, PartnerStack or mirror write. And a request
   WITHOUT the token, or with a wrong one, must be an ordinary request.

   WHY IT BOOTS. "No side effects" is a claim about what did NOT happen,
   and the only way to see that is to drive the real routes and watch
   every query and every outbound request. pg and global.fetch are stubbed,
   so nothing leaves the box; each stub asks synthetic.isSynthetic() AT THE
   MOMENT OF THE CALL, so a call made by fire-and-forget work after the
   response is still attributed to the request that started it.

   Every route is driven twice with the same body, once as a visitor and
   once as the monitor. The visitor run must PRODUCE its side effects
   (otherwise "none in the synthetic run" proves nothing) and the
   synthetic run must produce none:
     - no database write reaches the database (the pool guard counts any
       that would have, and that count must stay zero: a hit means a
       synthetic branch missed a write);
     - no outbound request but a read (GET or HEAD) or Apollo's lookup,
       the one POST that IS the enrichment;
     - no choke point refused anything that only a missed branch could
       reach (Slack, Salesforce, Meta, PartnerStack, the mirror);
     - the answer is the visitor's answer plus synthetic: true.

   Then: /submit's "would" against what the real path actually fired, over
   six kinds of lead (the parity that stops the two drifting apart); the
   token rules; the rate limit; /health/deep and /metrics/submissions; the
   one list of allowed origins; and the backstops on their own.

   Dependency-free: pg, fetch and DNS are stubbed. Run:
     node tests/test-synthetic.js
   ============================================================ */

require('./crash-reporter')('test-synthetic');

const Module = require('module');
const path   = require('path');
const fs     = require('fs');
const dns    = require('dns');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
function eq(name, a, b) { ok(name, a === b, `got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`); }

const ROOT  = path.join(__dirname, '..');
const PORT  = 41301;
const BASE  = `http://127.0.0.1:${PORT}`;
const TOKEN = 'suite-synthetic-token-0123456789abcdef-XYZ';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const synthetic = require('../synthetic');

/* ── recorders ──────────────────────────────────────────────────── */
const Q = [];        // { pool, text, params, syn }
const F = [];        // { method, url, headers, body, syn }
const LOGS = [];     // { syn, text }
const S = { submitted: new Set(), apolloHealthy: true, rhHang: false, failSubmitFor: new Set(), rows: new Map() };

/* ── stub pg ────────────────────────────────────────────────────── */
function leadsInsertRow(text, params) {
  /* The RETURNING row the real upsert would give for a FIRST write: every
     column read off the parameter bound to it, by parsing the statement's
     own column and VALUES lists -- so the stub cannot quietly disagree
     with the statement it is answering. */
  const cols = (/INSERT INTO leads \(([^)]*)\)/.exec(text) || [])[1];
  /* VALUES holds NOW(), so the list is cut at its BALANCED close paren and
     split at depth 0 -- a first ')' would drop every column after NOW(). */
  const at = text.indexOf('VALUES (');
  if (!cols || at === -1) return {};
  const v = []; let depth = 0, cur = '';
  for (let i = at + 'VALUES ('.length; i < text.length; i++) {
    const ch = text[i];
    if (ch === '(') depth++;
    if (ch === ')') { if (depth === 0) break; depth--; }
    if (ch === ',' && depth === 0) { v.push(cur.trim()); cur = ''; } else cur += ch;
  }
  v.push(cur.trim());
  const c = cols.split(',').map((x) => x.trim());
  const row = {};
  c.forEach((name, i) => { const m = /^\$(\d+)$/.exec(v[i] || ''); row[name] = m ? params[Number(m[1]) - 1] : null; });
  return row;
}
function stubAnswer(label, text, params) {
  const flat = String(text).replace(/\s+/g, ' ').trim();
  if (/^SELECT submitted_at FROM leads WHERE session_id=\$1$/.test(flat)) {
    return { rows: [{ submitted_at: S.submitted.has(params[0]) ? new Date() : null }], rowCount: 1 };
  }
  if (/INSERT INTO leads/.test(flat) && /RETURNING/.test(flat)) {
    if (S.failSubmitFor.has(params[0])) throw new Error('stub: the database is down for this session');
    const r = leadsInsertRow(text, params);
    S.rows.set(params[0], r);
    return { rows: [Object.assign({}, r, { step_reached: 2 })], rowCount: 1 };
  }
  if (/UPDATE leads SET ps_signup_sent_at = NOW/.test(flat)) return { rows: [{ session_id: params[0] }], rowCount: 1 };
  /* runPartnerStackSignup reads the block back off the row it was written to. */
  if (/^SELECT non_icp_blocked, non_icp_reason FROM leads WHERE session_id = \$1$/.test(flat)) {
    const r = S.rows.get(params[0]);
    return { rows: r ? [{ non_icp_blocked: r.non_icp_blocked === true, non_icp_reason: r.non_icp_reason || null }] : [], rowCount: r ? 1 : 0 };
  }
  if (/^SELECT email, company, website, phone, non_icp_blocked, non_icp_reason FROM leads WHERE session_id=\$1$/.test(flat)) {
    return { rows: [{ email: 'ada@acmewidgets.com', non_icp_blocked: false }], rowCount: 1 };
  }
  if (/^SELECT email FROM leads WHERE session_id=\$1$/.test(flat)) return { rows: [{ email: 'ada@acmewidgets.com' }], rowCount: 1 };
  if (/FROM leads l LEFT JOIN enrichment_data e/.test(flat)) {
    return { rows: [{ session_id: params[0], email: 'ada@acmewidgets.com', website: 'acmewidgets.com', page_url: 'https://www.gushwork.ai/demo',
      website_check_reason: 'content_clean', website_check_failed: false, non_icp_blocked: false, non_icp_llm_flagged: false }], rowCount: 1 };
  }
  if (/generate_series/.test(flat)) {
    const hours = Number(params[0]) || 24;
    const now = new Date(); now.setUTCMinutes(0, 0, 0);
    const rows = [];
    for (let i = hours - 1; i >= 0; i--) {
      const h = new Date(now.getTime() - i * 3600e3).toISOString().replace('.000Z', 'Z');
      rows.push({ hour: h, submitted: i === 0 ? 3 : 1, booked: i === 0 ? 2 : 0 });
    }
    return { rows, rowCount: rows.length };
  }
  return { rows: [], rowCount: 0 };
}
class StubClient {
  constructor(label) { this.label = label; }
  async query(q, params) { const text = typeof q === 'string' ? q : q.text; Q.push({ pool: this.label, text, params, syn: synthetic.isSynthetic() }); return stubAnswer(this.label, text, params || q.values || []); }
  release() {}
}
class StubPool {
  constructor(cfg) { this.label = cfg && cfg.host ? 'aws' : 'railway'; }
  async connect() { return new StubClient(this.label); }
  async query(q, params) {
    const text = typeof q === 'string' ? q : q.text;
    Q.push({ pool: this.label, text, params, syn: synthetic.isSynthetic() });
    return stubAnswer(this.label, text, params || (q && q.values) || []);
  }
  on() {} async end() {}
}
const origLoad = Module._load;
Module._load = function (request) {
  if (request === 'pg') return { Pool: StubPool, Client: StubClient, types: { setTypeParser() {} } };
  return origLoad.apply(this, arguments);
};

/* ── stub DNS (the local MX check in /verify-email) ──────────────── */
dns.promises.resolveMx = async () => [{ exchange: 'mx.examplehost.net', priority: 10 }];
dns.promises.resolveNs = async () => ['ns1.examplehost.net'];
dns.promises.resolve   = async () => ['198.51.100.7'];

/* ── stub fetch ─────────────────────────────────────────────────── */
const realFetch = global.fetch.bind(global);
global.fetch = async function (url, opts) {
  const u = String(url);
  if (u.startsWith(BASE)) return realFetch(url, opts);
  opts = opts || {};
  const method = String(opts.method || 'GET').toUpperCase();
  F.push({ method, url: u, headers: opts.headers || null, body: typeof opts.body === 'string' ? opts.body : (opts.body ? String(opts.body) : null), syn: synthetic.isSynthetic() });
  const j = (o, status) => ({ ok: (status || 200) < 300, status: status || 200, headers: new Map(), json: async () => o, text: async () => JSON.stringify(o) });
  if (/oauth2\/token/.test(u))           return j({ access_token: 'stub', instance_url: 'https://stub.my.salesforce.com' });
  if (/\/services\/data\/v60\.0\/limits/.test(u)) return j({});
  if (/\/query\/\?q=/.test(u))            return j({ totalSize: 1, done: true, records: [{ Id: '00Qstub' }] });
  if (/salesforce\.com/.test(u))         return j({ id: '00Qstub', success: true });
  if (/graph\.facebook\.com/.test(u))    return j({ events_received: 1 });
  if (/hooks\.slack\.com/.test(u))       return { ok: true, status: 200, text: async () => 'ok', json: async () => ({}) };
  if (/partnerlinks\.io|api\.partnerstack\.com/.test(u)) return { ok: true, status: 200, text: async () => '{}', json: async () => ({}) };
  if (/api\.apollo\.io\/v1\/auth\/health/.test(u)) return j(S.apolloHealthy ? { healthy: true, is_logged_in: true } : { healthy: true, is_logged_in: false });
  if (/api\.apollo\.io/.test(u))         return j({ person: { first_name: 'Ada', last_name: 'Lovelace', title: 'Head of Growth', organization: { name: 'Acme Widgets', website_url: 'http://acmewidgets.com', estimated_num_employees: 120, industry: 'manufacturing' } } });
  if (/emaillistverify|apps\.emaillistverify/.test(u)) return { ok: true, status: 200, text: async () => 'ok', json: async () => ({}) };
  if (/assets\.revenuehero\.io/.test(u)) {
    if (S.rhHang) return new Promise((resolve, reject) => { if (opts.signal) opts.signal.addEventListener('abort', () => reject(new Error('aborted'))); });
    return { ok: true, status: 200, text: async () => '', json: async () => ({}) };
  }
  if (/api\.anthropic\.com/.test(u))     return j({ error: { message: 'stub' } }, 500);
  if (/ipwho\.is/.test(u))               return j({ success: true, city: 'Austin', region: 'Texas', country: 'United States', country_code: 'US', latitude: 30.2, longitude: -97.7, timezone: { id: 'America/Chicago' }, connection: { isp: 'Stub', asn: 1, org: 'Stub', domain: 'stub.net' } });
  if (/dns\.google|cloudflare-dns/.test(u)) return j({ Status: 0, Answer: [{ name: 'x', type: 1, data: '198.51.100.7' }] });
  /* the company's own website, for the website and model checks */
  return { ok: true, status: 200, url: u, headers: new Map([['content-type', 'text/html']]),
           text: async () => '<html><head><title>Acme Widgets</title></head><body>Industrial pumps for distributors. Contact sales.</body></html>',
           json: async () => ({}) };
};

/* ── env: every integration configured, so each one genuinely calls out ── */
Object.assign(process.env, {
  PORT: String(PORT),
  DATABASE_URL: 'postgres://stub/stub',
  AWS_PG_HOST: 'stub', AWS_PG_PORT: '5432', AWS_PG_DATABASE: 'stub', AWS_PG_USER: 'stub', AWS_PG_PASSWORD: 'stub',
  SLACK_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB',
  SLACK_ALERTS_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB-ALERTS',
  SF_CLIENT_ID: 'stub', SF_CLIENT_SECRET: 'stub', SF_REFRESH_TOKEN: 'stub',
  META_ACCESS_TOKEN: 'stub', META_PIXEL_ID: '1234567890',
  ELV_API_KEY: 'stub', APOLLO_API_KEY: 'stub-apollo',
  PARTNERSTACK_TRACKING_TOKEN: 'stub', PARTNERSTACK_PUBLIC_KEY: 'stub', PARTNERSTACK_SECRET_KEY: 'stub',
  NON_ICP_BLOCK: 'true', NON_ICP_LLM_ENABLED: 'true', ANTHROPIC_API_KEY: 'stub',
  /* Set to a DIFFERENT list on purpose: since v5.23.0 it must be ignored. */
  ALLOWED_ORIGIN: 'https://www.gushwork.ai,https://evil.example',
  MONITOR_TOKEN: 'stub',
  SYNTHETIC_TOKEN: TOKEN,
  SYNTHETIC_RATE_PER_HOUR: '60',
  DEEP_HEALTH_TIMEOUT_MS: '300',
});

/* Console: captured, attributed and silent. tagConsole wraps whatever is
   here when index.js loads, so a line written during a synthetic request
   arrives already prefixed. */
const realLog = console.log, realWarn = console.warn, realErr = console.error;
for (const level of ['log', 'info', 'warn', 'error']) {
  console[level] = (...a) => LOGS.push({ syn: synthetic.isSynthetic(), text: a.map((x) => (typeof x === 'string' ? x : (() => { try { return JSON.stringify(x); } catch (_) { return String(x); } })())).join(' ') });
}
require(path.join(ROOT, 'index.js'));
const say = (...a) => realLog(...a);

/* ── driving ────────────────────────────────────────────────────── */
let sid = 0;
const newSession = () => `0b5e2c1a-0000-4000-8000-${String(++sid).padStart(12, '0')}`;
async function post(route, body, opts) {
  opts = opts || {};
  const headers = { 'Content-Type': 'application/json', Origin: 'https://www.gushwork.ai', 'User-Agent': 'suite/1.0' };
  if (opts.token !== undefined) headers['X-GW-Synthetic'] = opts.token;
  const res = await realFetch(BASE + route, { method: 'POST', headers, body: typeof body === 'string' ? body : JSON.stringify(body) });
  const text = await res.text();
  let json = null; try { json = JSON.parse(text); } catch (_) {}
  return { status: res.status, json, text, headers: res.headers };
}
async function get(route, token) {
  const headers = token === undefined ? {} : { 'X-GW-Synthetic': token };
  const res = await realFetch(BASE + route, { headers });
  const text = await res.text();
  let json = null; try { json = JSON.parse(text); } catch (_) {}
  return { status: res.status, json, text };
}
/* Wait until fire-and-forget work has stopped: no new query or request
   for 200ms, capped at 2s. */
async function quiet() {
  let last = -1, still = 0;
  for (let i = 0; i < 40; i++) {
    const n = Q.length + F.length;
    if (n === last) { if (++still >= 4) return; } else { still = 0; last = n; }
    await sleep(50);
  }
}
/* Everything one request did, attributed by the stubs. */
async function run(route, body, opts) {
  const q0 = Q.length, f0 = F.length, l0 = LOGS.length, g0 = synthetic.guardHits(), s0 = synthetic.skips();
  const r = await post(route, body, opts);
  await quiet();
  const s1 = synthetic.skips();
  const skipped = {};
  for (const k of Object.keys(s1)) { const d = s1[k] - (s0[k] || 0); if (d) skipped[k] = d; }
  return Object.assign(r, { queries: Q.slice(q0), fetches: F.slice(f0), logs: LOGS.slice(l0), guardHits: synthetic.guardHits() - g0, skipped });
}
const writes = (qs) => qs.filter((x) => synthetic.isWriteStatement(x.text));
const outboundWrites = (fs_) => fs_.filter((x) => !(x.method === 'GET' || x.method === 'HEAD') && !/^https:\/\/api\.apollo\.io\/api\/v1\/people\/match/.test(x.url));
/* Choke points a synthetic branch must never let a request reach. An ELV
   or model alert, or the model warm, is an EXPECTED skip on the check
   routes; these are not. */
/* elv_store is NOT on it: /verify-email has no synthetic branch by design,
   and its verdict store is one of the helpers that steps aside instead. */
const BUG_SKIPS = ['slack', 'salesforce', 'meta', 'partnerstack', 'aws_mirror', 'lead_field_changes', 'ip_geo'];
const without = (o, ...keys) => { const c = Object.assign({}, o); keys.forEach((k) => delete c[k]); return c; };

(async () => {
  await sleep(900);   // let express bind

  /* ================================================================
     A. THE TOKEN
     ================================================================ */
  ok('A: the exact token matches', synthetic.tokenMatches(TOKEN));
  ok('A: surrounding whitespace is trimmed', synthetic.tokenMatches('  ' + TOKEN + ' '));
  ok('A: a wrong token of the same length does not match', !synthetic.tokenMatches(TOKEN.slice(0, -1) + (TOKEN.slice(-1) === 'Z' ? 'Y' : 'Z')));
  ok('A: a prefix of the token does not match', !synthetic.tokenMatches(TOKEN.slice(0, 20)));
  ok('A: a longer string does not match', !synthetic.tokenMatches(TOKEN + 'x'));
  ok('A: empty and missing do not match', !synthetic.tokenMatches('') && !synthetic.tokenMatches(undefined) && !synthetic.tokenMatches(null));
  {
    const saved = process.env.SYNTHETIC_TOKEN;
    process.env.SYNTHETIC_TOKEN = 'short-token';
    ok('A: a token under 32 characters turns the feature OFF (nothing matches, not even itself)', !synthetic.tokenMatches('short-token') && synthetic.configuredToken() === null);
    delete process.env.SYNTHETIC_TOKEN;
    ok('A: no SYNTHETIC_TOKEN, nothing matches', !synthetic.tokenMatches(TOKEN) && !synthetic.tokenMatches(''));
    process.env.SYNTHETIC_TOKEN = saved;
  }
  const synSrc = fs.readFileSync(path.join(ROOT, 'synthetic.js'), 'utf8');
  ok('A: the comparison is constant-time over SHA-256 of both sides', /crypto\.timingSafeEqual\(digest\(g\), digest\(expected\)\)/.test(synSrc));

  /* ================================================================
     B. EVERY FORM ROUTE, AS A VISITOR AND AS THE MONITOR
     ================================================================ */
  const LEAD = {
    email: 'ada@acmewidgets.com', website: 'acmewidgets.com', first_name: 'Ada', last_name: 'Lovelace',
    phone: '+12015550123', company: 'Acme Widgets', sell_to: 'B2B', page_url: 'https://www.gushwork.ai/demo',
    website_check_reason: 'content_clean', website_check_failed: false, ps_xid: 'xid-stub-12345', ps_partner_key: 'pk-stub',
    hear_about_us: 'Google search', utm_source: 'google', utm_medium: 'cpc', utm_campaign: 'AEO_Search',
  };
  const ROUTES = [
    ['/session', () => ({ session_id: newSession(), page_url: 'https://www.gushwork.ai/demo', utm_source: 'google' }),
      (n) => writes(n.queries).some((q) => /INSERT INTO form_sessions/.test(q.text)), 'writes form_sessions'],
    ['/verify-email', (i) => ({ email: `ada${i}@acmewidgets.com` }),
      (n) => n.fetches.some((f) => /emaillistverify/.test(f.url)) && writes(n.queries).some((q) => /email_verifications/.test(q.text)), 'asks ELV and stores the verdict'],
    ['/non-icp-check', (i) => ({ email: `ada${i}@pumpco${i}.com`, website: `pumpco${i}.com` }),
      (n) => n.fetches.some((f) => f.method === 'GET' && /pumpco\d+\.com/.test(f.url)) && writes(n.queries).some((q) => /non_icp_domain_verdicts/.test(q.text)), 'warms the model layer (a scrape and a verdict-cache write)'],
    ['/enrich', () => ({ session_id: newSession(), email: 'ada@acmewidgets.com' }),
      (n) => n.fetches.some((f) => /people\/match/.test(f.url)) && writes(n.queries).some((q) => /INSERT INTO enrichment_data/.test(q.text)), 'asks Apollo and stores the answer'],
    ['/verify-website', () => ({ website: 'acmewidgets.com', email: 'ada@acmewidgets.com' }), null, 'answers a website verdict'],
    ['/resolve-website', () => ({ website: 'acmewidgets.com' }), null, 'answers a website verdict'],
    ['/partial', () => Object.assign({ session_id: newSession(), step_reached: 1 }, LEAD),
      (n) => writes(n.queries).some((q) => /INSERT INTO leads/.test(q.text)) && n.fetches.some((f) => /graph\.facebook\.com/.test(f.url)), 'writes the lead row and fires StartTrial'],
    ['/submit', () => Object.assign({ session_id: newSession() }, LEAD),
      (n) => writes(n.queries).some((q) => /INSERT INTO leads/.test(q.text))
        && n.fetches.some((f) => /hooks\.slack\.com\/services\/STUB$/.test(f.url))
        && n.fetches.some((f) => /\/sobjects\/Lead/.test(f.url))
        && n.fetches.some((f) => /graph\.facebook\.com/.test(f.url))
        && n.fetches.some((f) => /partnerlinks\.io/.test(f.url)), 'writes the row, posts Slack, pushes Salesforce, fires Meta Lead, converts PartnerStack'],
    ['/booking-confirmed', () => ({ session_id: newSession(), booking_uid: 'bk-' + sid, start_time: '2026-10-20T15:00:00Z', event_type: 'demo' }),
      (n) => writes(n.queries).some((q) => /UPDATE leads SET booking_uid/.test(q.text)) && n.fetches.some((f) => /graph\.facebook\.com/.test(f.url)), 'records the booking and fires Meta Schedule'],
  ];
  let ri = 0;
  for (const [route, body, visitorDid, did] of ROUTES) {
    ri++;
    const bN = body(ri * 2), bS = body(ri * 2 + 1);
    const n = await run(route, bN);
    const s = await run(route, bS, { token: TOKEN });
    if (visitorDid) ok(`B ${route}: as a visitor it ${did} (so "none" below means something)`, visitorDid(n),
      JSON.stringify({ w: writes(n.queries).map((q) => q.text.replace(/\s+/g, ' ').slice(0, 40)), f: n.fetches.map((f) => f.method + ' ' + f.url.slice(0, 60)) }));
    ok(`B ${route}: as a visitor the answer has no synthetic flag`, n.json && !('synthetic' in n.json) && !('would' in n.json), n.text.slice(0, 120));
    eq(`B ${route}: as the monitor it answers 200`, s.status, n.status);
    ok(`B ${route}: as the monitor the answer says synthetic: true`, s.json && s.json.synthetic === true, s.text.slice(0, 160));
    ok(`B ${route}: as the monitor NOTHING is written to either database`, writes(s.queries).length === 0 && s.guardHits === 0,
       JSON.stringify({ written: writes(s.queries).map((q) => q.text.replace(/\s+/g, ' ').slice(0, 60)), guardHits: s.guardHits }));
    ok(`B ${route}: as the monitor no outbound write leaves (only reads, and Apollo's lookup)`, outboundWrites(s.fetches).length === 0,
       JSON.stringify(outboundWrites(s.fetches).map((f) => f.method + ' ' + f.url.slice(0, 70))));
    ok(`B ${route}: as the monitor no choke point had to stop anything a branch should have`,
       BUG_SKIPS.every((k) => !s.skipped[k]), JSON.stringify(s.skipped));
    ok(`B ${route}: every request the monitor's run made is attributed to it`, s.fetches.every((f) => f.syn) && s.queries.every((q) => q.syn),
       JSON.stringify(s.fetches.filter((f) => !f.syn).map((f) => f.url)));
    ok(`B ${route}: every log line of the monitor's run is tagged [synthetic]`,
       s.logs.filter((l) => l.syn).length > 0 && s.logs.filter((l) => l.syn).every((l) => l.text.indexOf('[synthetic]') === 0),
       JSON.stringify(s.logs.filter((l) => l.syn && l.text.indexOf('[synthetic]') !== 0).slice(0, 2)));
    ok(`B ${route}: the token is in no log line`, !LOGS.some((l) => l.text.indexOf(TOKEN) !== -1));
    /* The answer is the visitor's, plus synthetic: true. Two known,
       documented differences: ms is a stopwatch reading, and /non-icp-check's
       pending is false because a synthetic check never starts the warm. */
    const strip = (j) => without(j || {}, 'synthetic', 'would', 'ms', 'pending');
    ok(`B ${route}: the answer is the visitor's answer, field for field`, JSON.stringify(strip(n.json)) === JSON.stringify(strip(s.json)),
       JSON.stringify([strip(n.json), strip(s.json)]));
    if (route === '/non-icp-check') ok('B /non-icp-check: as the monitor pending is false (no warm was started)', s.json && s.json.pending === false, s.text);
  }

  /* /partial's heartbeat must not be fed by the monitor. Read through the
     health snapshot the dashboard uses would need a token; the source line
     is enough alongside the executed route above. */
  const idx = fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8');
  ok('B /partial: the form-traffic heartbeat is only for real traffic', /if \(!req\.synthetic\) _lastLeadAt = Date\.now\(\);/.test(idx));

  /* ================================================================
     C. A WRONG OR MISSING TOKEN IS AN ORDINARY REQUEST
     ================================================================ */
  for (const [label, tok] of [['a wrong token', TOKEN.replace(/.$/, '!')], ['an empty header', ''], ['a near-miss prefix', TOKEN.slice(0, 32)]]) {
    const r = await run('/session', { session_id: newSession(), page_url: 'https://www.gushwork.ai/demo' }, { token: tok });
    ok(`C: ${label} is an ordinary request (row written, no synthetic flag)`, r.status === 200 && !('synthetic' in (r.json || {}))
       && writes(r.queries).some((q) => /INSERT INTO form_sessions/.test(q.text)), r.text);
    ok(`C: ${label} -- the value is never logged`, !r.logs.some((l) => tok && l.text.indexOf(tok) !== -1));
  }
  {
    const r = await run('/submit', Object.assign({ session_id: newSession() }, LEAD), { token: 'wrong-but-long-enough-0123456789abcdefgh' });
    ok('C: /submit with a wrong token does everything a visitor submit does', !('synthetic' in (r.json || {})) && r.fetches.some((f) => /\/sobjects\/Lead/.test(f.url)), r.text);
  }
  {
    const r = await run('/lm/track', { foo: 1 }, { token: TOKEN });
    ok('C: the header means nothing on a route outside the nine (no synthetic flag)', !(r.json && r.json.synthetic === true), r.text.slice(0, 80));
  }

  /* ================================================================
     D. /submit's "would" AGAINST WHAT THE REAL PATH ACTUALLY FIRED
     Six kinds of lead, each driven both ways. "fire" / "push" / "run"
     must be said exactly when the visitor run made that call.
     ================================================================ */
  const KINDS = [
    ['a normal lead',              {}],
    ['a brand-list block',         { website: 'kw.com', email: 'agent@kw.com' }],
    ['our own test address',       { email: 'b@g.ai', website: 'gushwork-qa.com' }],
    ['a website that did not verify', { website_check_failed: true, website_check_reason: 'parked_confirmed' }],
    ['an agency',                  { email: 'pat@flighted.co', website: 'flighted.co' }],
    ['no partner, free mail',      { email: 'ada.lovelace@gmail.com', ps_xid: undefined, ps_partner_key: undefined }],
  ];
  for (const [label, over] of KINDS) {
    const body = (id) => JSON.parse(JSON.stringify(Object.assign({ session_id: id }, LEAD, over)));
    const n = await run('/submit', body(newSession()));
    const s = await run('/submit', body(newSession()), { token: TOKEN });
    const w = (s.json || {}).would || {};
    const fired = {
      /* a Lead that already exists is UPDATED (PATCH), a new one created (POST) */
      salesforce: n.fetches.some((f) => /\/sobjects\/Lead/.test(f.url) && (f.method === 'POST' || f.method === 'PATCH')),
      meta: n.fetches.some((f) => /graph\.facebook\.com/.test(f.url)),
      partnerstack: n.fetches.some((f) => /partnerlinks\.io/.test(f.url)),
      leadPost: n.fetches.some((f) => /hooks\.slack\.com\/services\/STUB$/.test(f.url) && /Lead Form Completed/.test(f.body || '')),
      blockedPost: n.fetches.some((f) => /hooks\.slack\.com\/services\/STUB$/.test(f.url) && /Non-ICP/.test(f.body || '')),
    };
    eq(`D ${label}: Salesforce -- "would" says push exactly when the real path pushed`, w.salesforce === 'push', fired.salesforce);
    eq(`D ${label}: Meta Lead -- "would" says fire exactly when the real path fired`, w.meta_lead === 'fire', fired.meta);
    eq(`D ${label}: PartnerStack -- "would" says run exactly when the real path converted`, w.partnerstack === 'run', fired.partnerstack);
    eq(`D ${label}: Slack -- "would" names the lead post exactly when it was posted`, w.slack === 'lead_post', fired.leadPost);
    eq(`D ${label}: Slack -- "would" names the blocked post exactly when it was posted`, w.slack === 'non_icp_blocked_post', fired.blockedPost);
    ok(`D ${label}: the non-ICP answer is the visitor's`, JSON.stringify(without(n.json, 'synthetic', 'would')) === JSON.stringify(without(s.json, 'synthetic', 'would')),
       JSON.stringify([n.json, without(s.json, 'would')]));
    ok(`D ${label}: and the monitor's run wrote and sent nothing`, writes(s.queries).length === 0 && s.guardHits === 0 && outboundWrites(s.fetches).length === 0);
  }
  {
    const s = await run('/submit', Object.assign({ session_id: newSession() }, LEAD, { page_url: 'https://www.gushwork.ai/ai-crm' }), { token: TOKEN });
    eq('D: "would" names the team a CRM page routes to', s.json && s.json.would && s.json.would.team, 'crm');
    const a = await run('/submit', Object.assign({ session_id: newSession() }, LEAD), { token: TOKEN });
    eq('D: ...and the team an AEO page routes to', a.json && a.json.would && a.json.would.team, 'aeo');
    ok('D: the answer has exactly the visitor keys plus synthetic and would',
       JSON.stringify(Object.keys(a.json)) === JSON.stringify(['ok', 'non_icp_blocked', 'non_icp_reason', 'non_icp_industry', 'would', 'synthetic']), JSON.stringify(Object.keys(a.json)));
  }

  /* ================================================================
     E. /health/deep AND /metrics/submissions
     ================================================================ */
  eq('E /health stays open and unchanged', JSON.stringify(Object.keys((await get('/health')).json)), JSON.stringify(['status', 'timestamp']));
  eq('E /health/deep without the token is refused', (await get('/health/deep')).status, 401);
  eq('E /health/deep with a wrong token is refused', (await get('/health/deep', TOKEN + 'x')).status, 401);
  {
    const f0 = F.length;
    const r = await get('/health/deep', TOKEN);
    eq('E /health/deep with the token, all well: 200', r.status, 200);
    ok('E /health/deep: { ok, checks } with db, salesforce, revenuehero, enrichment', r.json && r.json.ok === true
       && JSON.stringify(Object.keys(r.json)) === JSON.stringify(['ok', 'checks'])
       && JSON.stringify(Object.keys(r.json.checks)) === JSON.stringify(['db', 'salesforce', 'revenuehero', 'enrichment']), r.text);
    ok('E /health/deep: each check is { ok, ms } with a number', r.json && Object.values(r.json.checks).every((c) => c.ok === true && typeof c.ms === 'number' && !('error' in c)), r.text);
    const calls = F.slice(f0).map((f) => f.method + ' ' + f.url);
    ok('E /health/deep: Salesforce is proved with an authenticated read', calls.some((c) => /GET https:\/\/stub\.my\.salesforce\.com\/services\/data\/v60\.0\/limits/.test(c)), calls.join(' | '));
    ok('E /health/deep: Apollo through its own auth check (no credit)', calls.some((c) => /GET https:\/\/api\.apollo\.io\/v1\/auth\/health/.test(c)) && !calls.some((c) => /people\/match/.test(c)));
    ok('E /health/deep: RevenueHero through the scheduler script the pages load', calls.some((c) => /HEAD https:\/\/assets\.revenuehero\.io\/scheduler\.min\.js/.test(c)));
    ok('E /health/deep: it writes nothing and sends nothing', F.slice(f0).every((f) => f.method === 'GET' || f.method === 'HEAD' || /oauth2\/token/.test(f.url)));
  }
  {
    S.apolloHealthy = false; S.rhHang = true;
    const r = await get('/health/deep', TOKEN);
    S.apolloHealthy = true; S.rhHang = false;
    eq('E /health/deep with a failing check: 503, so a status-only monitor alerts', r.status, 503);
    ok('E /health/deep: ok is false and the failing check says why', r.json && r.json.ok === false && r.json.checks.enrichment.ok === false
       && /not logged in/.test(r.json.checks.enrichment.error), r.text);
    ok('E /health/deep: a check that does not answer in time FAILS (never "fine")', r.json && r.json.checks.revenuehero.ok === false
       && /no answer within 300ms/.test(r.json.checks.revenuehero.error), r.text);
    ok('E /health/deep: the healthy checks are still reported healthy', r.json && r.json.checks.db.ok && r.json.checks.salesforce.ok);
    ok('E /health/deep: no credential in the answer', r.text.indexOf('stub-apollo') === -1 && r.text.indexOf(TOKEN) === -1);
  }
  eq('E /metrics/submissions without the token is refused', (await get('/metrics/submissions')).status, 401);
  {
    /* Two real failures in this hour -- a 500 and a 400 -- and a synthetic
       500, which must not be counted. */
    const failing = newSession();
    S.failSubmitFor.add(failing);
    const f500 = await post('/submit', Object.assign({ session_id: failing }, LEAD));
    const f400 = await post('/submit', Object.assign({}, LEAD));
    const failingSyn = newSession();
    S.failSubmitFor.add(failingSyn);
    await post('/submit', Object.assign({ session_id: failingSyn }, LEAD), { token: TOKEN });
    eq('E: a database failure in /submit answers 500 (the fixture)', f500.status, 500);
    eq('E: a /submit with no session id answers 400 (the fixture)', f400.status, 400);
    const r = await get('/metrics/submissions?hours=6', TOKEN);
    eq('E /metrics/submissions with the token: 200', r.status, 200);
    const j = r.json || {};
    eq('E /metrics/submissions: hours is honoured', j.hours, 6);
    eq('E /metrics/submissions: one row per hour', (j.series || []).length, 6);
    ok('E /metrics/submissions: each row is hour, submitted, booked, failed, rejected', (j.series || []).every((x) =>
      JSON.stringify(Object.keys(x)) === JSON.stringify(['hour', 'submitted', 'booked', 'failed', 'rejected']) && /^\d{4}-\d\d-\d\dT\d\d:00:00Z$/.test(x.hour)), JSON.stringify(j.series && j.series[0]));
    const last = (j.series || [])[5] || {};
    eq('E /metrics/submissions: this hour\'s real failures counted (the 500, not the synthetic one)', last.failed, 1);
    ok('E /metrics/submissions: this hour\'s rejections counted (the 400)', last.rejected >= 1, JSON.stringify(last));
    eq('E /metrics/submissions: submitted and booked come from the database', JSON.stringify([last.submitted, last.booked]), JSON.stringify([3, 2]));
    ok('E /metrics/submissions: says it is UTC, what it leaves out, and since when failures are counted',
       j.timezone === 'UTC' && /our own test submissions/.test(j.excludes || '') && !isNaN(Date.parse(j.failures_counted_since)), JSON.stringify(without(j, 'series')));
    ok('E /metrics/submissions: totals add up', j.totals && j.totals.submitted === (j.series || []).reduce((a, x) => a + x.submitted, 0));
    ok('E /metrics/submissions: no personal data (no address, no session id)', !/@/.test(r.text) && r.text.indexOf(failing) === -1, r.text.slice(0, 200));
    const mq = Q.filter((q) => /generate_series/.test(q.text)).pop();
    /* In EACH count, not just somewhere in the statement: dropping it from
       one of the two would otherwise pass. */
    const seg = (t, from, to) => { const i = t.indexOf(from); return i === -1 ? '' : t.slice(i, t.indexOf(to, i)); };
    const subSeg = mq ? seg(mq.text, 'WHERE l.submitted_at', 'AS submitted') : '';
    const bookSeg = mq ? seg(mq.text, 'WHERE l.booking_uid', 'AS booked') : '';
    ok('E /metrics/submissions: submitted leaves our own submissions out, with IS NOT TRUE', /\) IS NOT TRUE\)?\s*$/.test(subSeg.trim()) && /\$2/.test(subSeg), subSeg.slice(-80));
    ok('E /metrics/submissions: booked leaves our own submissions out, with IS NOT TRUE', /\) IS NOT TRUE\)?\s*$/.test(bookSeg.trim()) && /\$2/.test(bookSeg), bookSeg.slice(-80));
    const h = (await get('/metrics/submissions?hours=99999', TOKEN)).json;
    eq('E /metrics/submissions: hours is capped at a week', h && h.hours, 168);
  }

  /* ================================================================
     F. THE RATE LIMIT (all synthetic traffic shares one budget)
     ================================================================ */
  {
    let limited = null;
    for (let i = 0; i < 80 && !limited; i++) {
      const r = await post('/session', { session_id: newSession() }, { token: TOKEN });
      if (r.status === 429) limited = r;
    }
    ok('F: synthetic checks past the hourly budget are refused with 429', !!limited, 'never limited');
    ok('F: ...saying so, and saying synthetic', limited && limited.json && limited.json.synthetic === true && /rate limit/i.test(limited.json.error), limited && limited.text);
    const v = await run('/session', { session_id: newSession(), page_url: 'https://www.gushwork.ai/demo' });
    ok('F: a visitor is not affected by the synthetic budget', v.status === 200 && writes(v.queries).length > 0, v.text);
    ok('F: the budget is 60 an hour unless SYNTHETIC_RATE_PER_HOUR says otherwise', /Number\(process\.env\.SYNTHETIC_RATE_PER_HOUR\) \|\| 60/.test(idx));
  }

  /* ================================================================
     G. ONE LIST OF ORIGINS
     ================================================================ */
  {
    const { ALLOWED_ORIGINS, allowedHostnames } = require('../allowed-origins');
    const B = require('../tools/build-form-min.js');
    eq('G: the list is the three origins the env held on 17 Oct 2026', JSON.stringify(ALLOWED_ORIGINS), JSON.stringify(['https://www.gushwork.ai', 'https://gushwork.ai', 'https://gushwork.webflow.io']));
    ok('G: index.js takes CORS origins from allowed-origins.js', /const allowedOrigins = ALLOWED_ORIGINS\.slice\(\);/.test(idx) && /require\('\.\/allowed-origins'\)/.test(idx));
    ok('G: index.js no longer builds the list from the env', !/const allowedOrigins = \(process\.env/.test(idx));
    for (const o of ALLOWED_ORIGINS) {
      const r = await realFetch(BASE + '/session', { method: 'OPTIONS', headers: { Origin: o, 'Access-Control-Request-Method': 'POST', 'Access-Control-Request-Headers': 'content-type,x-gw-synthetic' } });
      ok(`G: ${o} is accepted, and may send X-GW-Synthetic`, r.status === 204 && r.headers.get('access-control-allow-origin') === o
         && /x-gw-synthetic/i.test(r.headers.get('access-control-allow-headers') || ''), r.status + ' ' + r.headers.get('access-control-allow-headers'));
    }
    const evil = await realFetch(BASE + '/session', { method: 'POST', headers: { Origin: 'https://evil.example', 'Content-Type': 'application/json' }, body: '{"session_id":"x"}' });
    eq('G: an origin only the ENV names is refused (the env is no longer read)', evil.status, 500);
    ok('G: and the boot says the env is ignored', LOGS.some((l) => /ALLOWED_ORIGIN is set in the environment but is no longer read/.test(l.text)));
    for (const f of B.FILES) {
      for (const file of [f, B.minName(f)]) {
        const src = fs.readFileSync(path.join(ROOT, file), 'utf8');
        if (file === f) {
          eq(`G: ${file} sends /session from exactly the API's hosts`, B.hostsMismatch(src), null);
        } else {
          /* The minified file is a rebuild of the readable one; its list is
             compared as written there. */
          const m = /SESSION_HOSTS\s*=\s*\[([^\]]*)\]|\[("www\.gushwork\.ai"[^\]]*)\]/.exec(src);
          const hosts = m ? (m[1] || m[2]).match(/"([^"]*)"|'([^']*)'/g).map((q) => q.slice(1, -1)) : null;
          eq(`G: ${file} carries exactly the API's hosts`, JSON.stringify(hosts), JSON.stringify(allowedHostnames()));
        }
      }
      /* A DELIBERATE MISMATCH FAILS: a host added to the API's list only, and
         one added to a form file only. */
      const src = fs.readFileSync(path.join(ROOT, f), 'utf8');
      ok(`G: ${f}: an origin added only to the API's list is caught`, B.hostsMismatch(src, ALLOWED_ORIGINS.concat(['https://new.gushwork.ai'])) !== null);
      const doctored = src.replace(/const SESSION_HOSTS = \[[^\]]*\];/, "const SESSION_HOSTS = ['www.gushwork.ai', 'gushwork.ai', 'gushwork.webflow.io', 'extra.example'];");
      ok(`G: ${f}: a host added only to the form file is caught`, B.hostsMismatch(doctored) !== null, B.hostsMismatch(doctored));
      eq(`G: ${f}: the build writes the API's list back over it`, B.hostsMismatch(B.injectHosts(doctored)), null);
    }
    ok('G: a missing hosts line is refused by the build, not silently skipped', (() => { try { B.injectHosts('const x = 1;'); return false; } catch (e) { return /exactly one/.test(e.message); } })());
  }

  /* ================================================================
     H. THE BACKSTOPS ON THEIR OWN (what a missed branch would meet)
     ================================================================ */
  {
    const SF = require('../salesforce');
    const PS = require('../partnerstack');
    const META = require('../meta-capi');
    const { pool } = require('../db');
    const f0 = F.length, g0 = synthetic.guardHits();
    const inside = await synthetic.runSynthetic(async () => ({
      sf: await SF.pushToSalesforce({ email: 'ada@acmewidgets.com', last_name: 'L', company: 'Acme' }),
      sfu: await SF.updateSFLead('00Qstub', { completed__c: true }),
      ps: await PS.sendConversion({ xid: 'x', customer_key: 'acmewidgets.com', email: 'ada@acmewidgets.com' }),
      psa: await PS.sendAction({ customer_key: 'acmewidgets.com', type: 'qualified' }),
      meta: await META.pushFormEventsToMeta({ session_id: 's', email: 'ada@acmewidgets.com', page_url: 'https://www.gushwork.ai/demo' }, {}).catch((e) => ({ threw: e.message })),
      ins: await pool.query("INSERT INTO leads (session_id) VALUES ('x')"),
      sel: await pool.query('SELECT 1'),
    }));
    ok('H: Salesforce, inside a synthetic request, is skipped and asked nothing', inside.sf && inside.sf.skipped === 'synthetic' && inside.sfu && inside.sfu.skipped === 'synthetic');
    ok('H: PartnerStack, inside a synthetic request, is skipped', inside.ps.reason === 'synthetic' && inside.psa.reason === 'synthetic');
    ok('H: Meta, inside a synthetic request, sends nothing and throws nothing', !inside.meta || !inside.meta.threw, JSON.stringify(inside.meta));
    ok('H: no outbound call was made by any of them', F.length === f0, JSON.stringify(F.slice(f0).map((f) => f.url)));
    ok('H: a database write inside a synthetic request resolves EMPTY (never throws) and is counted', inside.ins.rowCount === 0 && inside.ins.command === 'SYNTHETIC_SKIPPED' && synthetic.guardHits() === g0 + 1);
    ok('H: a read inside a synthetic request goes through', Q.some((q) => q.text === 'SELECT 1' && q.syn));
    const q0 = Q.length;
    await pool.query("INSERT INTO leads (session_id) VALUES ('y')");
    ok('H: the same write OUTSIDE a synthetic request goes straight through', Q.length === q0 + 1);
    const outside = await SF.pushToSalesforce({ email: 'b@g.ai' });
    ok('H: outside a synthetic request Salesforce keeps its own rules (our test address still skipped as internal)', outside && outside.skipped === 'internal', JSON.stringify(outside));
    /* What counts as a write. */
    const W = synthetic.isWriteStatement;
    ok('H: reads are reads', !W('SELECT 1') && !W('  /* a comment with INSERT in it */ SELECT x FROM leads') && !W("SELECT 'UPDATE' AS word") && !W('WITH a AS (SELECT 1) SELECT * FROM a') && !W('SELECT updated_at FROM leads'));
    ok('H: writes are writes', W('INSERT INTO x VALUES (1)') && W('UPDATE leads SET a=1') && W('WITH prev AS (SELECT 1) INSERT INTO leads (a) VALUES (1)')
       && W('DELETE FROM x') && W('SELECT * FROM leads FOR UPDATE') && W('BEGIN') && W('ALTER TABLE x ADD y int') && W({ text: 'INSERT INTO z VALUES (1)' }));
    /* Every choke point in index.js opens with its guard. Paired with the
       executed routes in B, which prove no synthetic request ever reaches
       one of them. */
    for (const [sig, point] of [['function sendSlack(blocks, fallbackText) {', 'slack'], ['function sendOpsSlack(blocks, fallbackText) {', 'ops_slack'],
      ['async function sendAlertEmail(subject, details) {', 'alert_email'], ['function alertOps(severity, source, title, details) {', 'alert'],
      ['function recordFailure(source, id, error) {', 'failure_record'], ['function syncToAWS(data) {', 'aws_mirror'],
      ['function persistElvVerdict(email, status, valid, source) {', 'elv_store'], ['function warmNonIcpLlm({ email, website } = {}) {', 'non_icp_warm'],
      ['async function finaliseIpGeo(session_id, ip) {', 'ip_geo'], ['function recordLeadFieldChanges(session_id, row, source_route, ctx) {', 'lead_field_changes']]) {
      const i = idx.indexOf(sig);
      const firstLine = i === -1 ? '' : idx.slice(i + sig.length + 1, idx.indexOf('\n', i + sig.length + 1));
      ok(`H: ${sig.replace(/\(.*/, '')} opens with its synthetic guard`, firstLine.indexOf(`if (synthetic.refuseIfSynthetic('${point}'`) === 2, firstLine);
    }
    ok('H: both pools are guarded', /synthetic\.guardPoolWrites\(pool, 'railway'\)/.test(idx) && /synthetic\.guardPoolWrites\(awsPool, 'aws'\)/.test(idx));
  }

  realLog('');
  if (failures.length) {
    realLog('  FAILURES:');
    for (const f of failures) realLog('   ✗ ' + f);
  }
  realLog(`  passed: ${pass}`);
  realLog(`  failed: ${fail}`);
  realLog('');
  process.exit(fail ? 1 : 0);
})();
