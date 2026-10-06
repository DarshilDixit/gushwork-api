/* ============================================================
   Agency leads (AGENCY_DOMAINS) and our own test submissions never reach
   Salesforce; agency rows are marked on the SDR list and left out of its
   CSV — EXECUTED.

   Every place a Salesforce Lead can be created goes through
   pushToSalesforce, and the check lives inside it. So this suite drives
   EACH of those places for real and counts what reaches Salesforce:

     A. the shared list (agency-domains.js) and what extends it
     B. /submit, booted                      -- agency by email, by website
     C. the Cal booking safety net, booted   -- by email (a booking carries
     D. the RevenueHero safety net, booted      no website), and ours
     E. the Salesforce retry sweep, lifted   -- by email, by website, ours
     F. backfill-sf.js, dry and real         -- by email, by website
     G. /monitor/sdr JSON and CSV, booted
   and, in every one, that a skip is recorded as NEITHER synced NOR failed
   and that a normal lead still reaches Salesforce.

   Dependency-free: pg is stubbed, fetch is stubbed, nothing leaves the box.

   Run:  node tests/test-agency-exclusions.js
   ============================================================ */

require('./crash-reporter')('test-agency-exclusions');

const Module = require('module');
const path   = require('path');
const fs     = require('fs');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
const eq = (name, a, b) => ok(name, a === b, `got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const ROOT = path.join(__dirname, '..');
const src = fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8');
const PORT = 41271;
const BASE = `http://127.0.0.1:${PORT}`;

/* What the stub database returns, and everything that left the process. */
const S = { queries: [], sf: [], sdrRows: [] };
const sfCalls = () => S.sf.length;

function stubQuery(q, params) {
  const flat = (typeof q === 'string' ? q : (q && q.text) || '').replace(/\s+/g, ' ').trim();
  S.queries.push({ sql: flat, params: params || [] });
  if (/SELECT DISTINCT ON \(LOWER\(l\.email\)\)/.test(flat)) return { rows: S.sdrRows.map((r) => ({ ...r })), rowCount: S.sdrRows.length };
  return { rows: [], rowCount: 0 };
}
class StubClient { async query(q, p) { return stubQuery(q, p); } release() {} }
class StubPool { async connect() { return new StubClient(); } async query(q, p) { return stubQuery(q, p); } on() {} async end() {} }
const origLoad = Module._load;
Module._load = function (request) {
  if (request === 'pg') return { Pool: StubPool, Client: StubClient, types: { setTypeParser() {} } };
  return origLoad.apply(this, arguments);
};

const realFetch = global.fetch.bind(global);
const j = (body, status = 200) => ({ ok: status < 300, status, json: async () => body, text: async () => JSON.stringify(body) });
global.fetch = async function (url, opts) {
  const u = String(url);
  if (u.startsWith(BASE)) return realFetch(url, opts);
  if (/salesforce/.test(u)) {
    S.sf.push({ url: u, method: (opts && opts.method) || 'GET' });
    if (/oauth2\/token/.test(u)) return j({ access_token: 'stub', instance_url: 'https://stub.my.salesforce.test' });
    if (/\/query/.test(u)) return j({ totalSize: 0, done: true, records: [] });
    return j({ id: '00Qstub', success: true }, 201);
  }
  return j({});
};

Object.assign(process.env, {
  PORT: String(PORT), DATABASE_URL: 'postgres://stub/stub',
  SLACK_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB', SLACK_ALERTS_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB-ALERTS',
  ALLOWED_ORIGIN: 'https://www.gushwork.ai', MONITOR_TOKEN: 'stub',
  SF_CLIENT_ID: 'stub', SF_CLIENT_SECRET: 'stub', SF_REFRESH_TOKEN: 'stub', SF_LOGIN_URL: 'https://login.salesforce.test',
});
for (const k of ['AGENCY_DOMAINS', 'META_EXCLUDED_DOMAINS', 'GADS_EXCLUDED_DOMAINS', 'CAL_WEBHOOK_SECRET', 'RH_WEBHOOK_SECRET']) delete process.env[k];

const realLog = console.log, realWarn = console.warn, realErr = console.error;
const logs = [];
const quiet = () => { console.log = (...a) => logs.push(a.join(' ')); console.warn = console.error = () => {}; };
const loud  = () => { console.log = realLog; console.warn = realWarn; console.error = realErr; };

quiet();
require(path.join(ROOT, 'index.js'));
const SF = require(path.join(ROOT, 'salesforce.js'));
const AD = require(path.join(ROOT, 'agency-domains.js'));
const META = require(path.join(ROOT, 'meta-capi.js'));
const G = require(path.join(ROOT, 'google-ads-conversions.js'));

const syncedSql = (qs) => qs.filter((q) => /SET sf_synced_at = NOW\(\)/.test(q.sql));
const failedSql = (qs) => qs.filter((q) => /SET sf_sync_failed_at = NOW\(\)/.test(q.sql));
const AGENCY_EMAIL = { email: 'pm@flighted.co', website: 'acme-widgets.test' };
const AGENCY_SITE  = { email: 'owner@acme-widgets.test', website: 'https://www.uprawmedia.com/case-studies' };
const NORMAL       = { email: 'buyer@northwind-trading.test', website: 'northwind-trading.test' };

(async () => {
  await sleep(900);

  /* ================================================================
     A. THE SHARED LIST
     ================================================================ */
  eq('A: defaults are Flighted and Upraw', AD.agencyDomains({}).join(','), 'flighted.co,uprawmedia.com');
  eq('A: AGENCY_DOMAINS extends the defaults, never replaces them', AD.agencyDomains({ AGENCY_DOMAINS: 'www.other-agency.test' }).join(','), 'flighted.co,uprawmedia.com,other-agency.test');
  eq('A: unset, Meta\'s list is exactly what #132 shipped', META.metaExcludedDomains({}).join(','), 'flighted.co,uprawmedia.com');
  eq('A: unset, Google\'s list is exactly what #129 shipped', G.gadsSettings({}).excludedDomains.join(','), 'flighted.co,uprawmedia.com');
  ok('A: AGENCY_DOMAINS reaches Meta AND Google -- one list to maintain',
     META.metaExcludedDomains({ AGENCY_DOMAINS: 'shared.test' }).includes('shared.test') && G.gadsSettings({ AGENCY_DOMAINS: 'shared.test' }).excludedDomains.includes('shared.test'));
  ok('A: META_EXCLUDED_DOMAINS still works and stays Meta-only',
     META.metaExcludedDomains({ META_EXCLUDED_DOMAINS: 'm-only.test' }).includes('m-only.test') && !G.gadsSettings({ META_EXCLUDED_DOMAINS: 'm-only.test' }).excludedDomains.includes('m-only.test') && !AD.agencyDomains({ META_EXCLUDED_DOMAINS: 'm-only.test' }).includes('m-only.test'));
  ok('A: GADS_EXCLUDED_DOMAINS still works and stays Google-only',
     G.gadsSettings({ GADS_EXCLUDED_DOMAINS: 'g-only.test' }).excludedDomains.includes('g-only.test') && !META.metaExcludedDomains({ GADS_EXCLUDED_DOMAINS: 'g-only.test' }).includes('g-only.test'));
  ok('A: the Google module still exports its host helpers, now the shared ones', G.gadsHostOf === AD.hostOf && G.gadsMatchList === AD.matchList);
  eq('A: email domain', JSON.stringify(AD.agencyDomainMatch(AGENCY_EMAIL)), '{"domain":"flighted.co","via":"email"}');
  eq('A: website, with scheme, www and path', JSON.stringify(AD.agencyDomainMatch(AGENCY_SITE)), '{"domain":"uprawmedia.com","via":"website"}');
  eq('A: a subdomain', JSON.stringify(AD.agencyDomainMatch({ email: 'x@clients.flighted.co' })), '{"domain":"flighted.co","via":"email"}');
  eq('A: never a substring', AD.agencyDomainMatch({ email: 'x@notflighted.co', website: 'flighted.co.example.test' }), null);
  eq('A: a normal lead', AD.agencyDomainMatch(NORMAL), null);
  ok('A: INTERNAL_TEST_EMAILS and ELV_EXCLUDED_DOMAINS are untouched', !/flighted|uprawmedia/.test(src.slice(src.indexOf('const ELV_EXCLUDED_DOMAINS'), src.indexOf('function isInternalLead'))));

  /* ================================================================
     B. /submit, BOOTED
     ================================================================ */
  async function submit(who, extra = {}) {
    S.queries = []; S.sf = [];
    const res = await realFetch(BASE + '/submit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Origin: 'https://www.gushwork.ai', 'User-Agent': 'suite/1.0' },
      body: JSON.stringify({
        session_id: '5a5a5a5a-0000-4000-8000-0000000000' + String(Math.floor(Math.random() * 90) + 10),
        first_name: 'Sam', last_name: 'Tester', phone: '+15551234567', company: 'Acme', sell_to: 'B2B',
        page_url: 'https://www.gushwork.ai/demo', website_check_reason: 'resolved', website_check_failed: false,
        ...who, ...extra,
      }),
    });
    await sleep(1200);
    return { status: res.status, sf: sfCalls(), synced: syncedSql(S.queries).length, failed: failedSql(S.queries).length };
  }
  for (const [label, who] of [['by email', AGENCY_EMAIL], ['by website', AGENCY_SITE]]) {
    const r = await submit(who);
    eq(`B /submit agency ${label}: the form still gets a 200`, r.status, 200);
    eq(`B /submit agency ${label}: NOTHING reaches Salesforce`, r.sf, 0);
    eq(`B /submit agency ${label}: not stamped synced (Salesforce does not have it)`, r.synced, 0);
    eq(`B /submit agency ${label}: not stamped failed (so the retry sweep never picks it up)`, r.failed, 0);
  }
  const bn = await submit(NORMAL);
  ok('B /submit normal: still reaches Salesforce', bn.sf >= 2, String(bn.sf));
  eq('B /submit normal: still stamped synced', bn.synced, 1);
  ok('B: the skip is logged with its reason, by domain not address',
     logs.some((m) => /\[SF\] ⏭ not sent to Salesforce — agency domain flighted\.co \(by email\)/.test(m)) &&
     logs.some((m) => /agency domain uprawmedia\.com \(by website\)/.test(m)) &&
     !logs.some((m) => /⏭ not sent to Salesforce.*@/.test(m)));

  /* ================================================================
     C. THE CAL BOOKING SAFETY NET, BOOTED
     ================================================================ */
  async function cal(email) {
    S.queries = []; S.sf = [];
    const res = await realFetch(BASE + '/booking-confirmed-webhook', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ triggerEvent: 'BOOKING_CREATED', payload: { uid: 'cal-' + Math.random(), startTime: '2026-10-20T15:00:00Z', type: 'demo-call', attendees: [{ email, name: 'Pat Person' }] } }),
    });
    const body = await res.json().catch(() => null);
    await sleep(1000);
    return { action: body && body.action, sf: sfCalls(), inserted: S.queries.some((q) => /^INSERT INTO leads/.test(q.sql)) };
  }
  let c = await cal(AGENCY_EMAIL.email);
  ok('C Cal safety net agency: the booking row is still written', c.action === 'created_new' && c.inserted, JSON.stringify(c));
  eq('C Cal safety net agency (by email): NOTHING reaches Salesforce', c.sf, 0);
  c = await cal('qa@gushwork.ai');
  eq('C Cal safety net OURS (gushwork.ai): NOTHING reaches Salesforce -- the gap this closes', c.sf, 0);
  c = await cal(NORMAL.email);
  ok('C Cal safety net normal: still reaches Salesforce', c.sf >= 2, String(c.sf));
  ok('C: a booking webhook carries no website, so these are matched by email only (documented)',
     !/pushToSalesforce\(\{ first_name:slackFirstName[^)]*website/.test(src));

  /* ================================================================
     D. THE REVENUEHERO SAFETY NET, BOOTED
     ================================================================ */
  async function rh(email) {
    S.queries = []; S.sf = [];
    const res = await realFetch(BASE + '/booking-confirmed-webhook-rh', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: 'rh-' + Math.random(), status: 'scheduled', meeting_time: '2026-10-20T15:00:00Z', meeting_type_name: 'demo', prospect: { email, name: 'Pat Person' } }),
    });
    const body = await res.json().catch(() => null);
    await sleep(1000);
    return { action: body && body.action, sf: sfCalls() };
  }
  let d = await rh(AGENCY_EMAIL.email);
  ok('D RH safety net agency: the booking row is still written', d.action === 'created_new', JSON.stringify(d));
  eq('D RH safety net agency (by email): NOTHING reaches Salesforce', d.sf, 0);
  d = await rh('someone@example.com');
  eq('D RH safety net OURS (example.com): NOTHING reaches Salesforce', d.sf, 0);
  d = await rh(NORMAL.email);
  ok('D RH safety net normal: still reaches Salesforce', d.sf >= 2, String(d.sf));

  /* ================================================================
     E. THE SALESFORCE RETRY SWEEP -- the real function, lifted
     ================================================================ */
  function liftFn(decl) {
    const i = src.indexOf('\n' + decl); let k = src.indexOf('(', i), dd = 0;
    for (; k < src.length; k++) { if (src[k] === '(') dd++; else if (src[k] === ')') { dd--; if (!dd) break; } }
    let jj = src.indexOf('{', k); dd = 0;
    for (let m = jj; m < src.length; m++) { if (src[m] === '{') dd++; else if (src[m] === '}') { dd--; if (!dd) { jj = m; break; } } }
    return src.slice(i + 1, jj + 1);
  }
  const sweepSrc = liftFn('async function runSalesforceRetrySweep');
  async function sweep(row) {
    const R = { sql: [], marked: [], alerts: [] };
    const pool = { query: async (q, p) => {
      const flat = String(q).replace(/\s+/g, ' ').trim();
      R.sql.push({ sql: flat, params: p });
      if (/FROM leads WHERE sf_sync_failed_at IS NOT NULL/.test(flat)) return { rows: [{ session_id: 's-retry', sf_sync_attempts: 0, ...row }], rowCount: 1 };
      return { rows: [], rowCount: 1 };
    } };
    const run = new Function('pool', 'pushToSalesforce', 'markSalesforceSynced', 'markSalesforceFailed', 'alertOps', 'recordFailure',
      'let _sfRetryRunning = false; const SF_RETRY_MAX_ATTEMPTS = 5, SF_RETRY_BACKOFF_MIN = 10, SF_RETRY_BATCH = 25;\n' + sweepSrc + '\nreturn runSalesforceRetrySweep;')(
      pool, SF.pushToSalesforce, (sid) => R.marked.push('synced:' + sid), (sid) => R.marked.push('failed:' + sid),
      (sev, srcName, title) => R.alerts.push(title), () => {});
    S.sf = [];
    await run();
    return { ...R, sf: sfCalls(), offQueue: R.sql.find((q) => /SET sf_sync_retryable = FALSE, sf_sync_error = \$2/.test(q.sql)) };
  }
  for (const [label, who] of [['by email', AGENCY_EMAIL], ['by website', AGENCY_SITE], ['OURS (staging page)', { email: 'x@acme-widgets.test', page_url: 'https://gushwork.webflow.io/demo' }]]) {
    const r = await sweep(who);
    eq(`E retry sweep ${label}: NOTHING reaches Salesforce`, r.sf, 0);
    eq(`E retry sweep ${label}: NOT stamped synced and NOT stamped failed again`, r.marked.length, 0);
    ok(`E retry sweep ${label}: taken OFF the queue (retryable false) with the reason`, !!r.offQueue && /^not sent: /.test(r.offQueue.params[1]), JSON.stringify(r.offQueue && r.offQueue.params));
    eq(`E retry sweep ${label}: no "Retries exhausted" page`, r.alerts.length, 0);
  }
  const en = await sweep(NORMAL);
  ok('E retry sweep normal: still re-pushed and stamped synced', en.sf >= 2 && en.marked.join() === 'synced:s-retry', JSON.stringify(en.marked));

  /* ================================================================
     F. backfill-sf.js -- dry and real
     ================================================================ */
  const { runBackfill } = require(path.join(ROOT, 'backfill-sf.js'));
  const bfRows = [
    { email: AGENCY_EMAIL.email, website: AGENCY_EMAIL.website, submitted_at: new Date(), updated_at: new Date(), page_url: 'https://www.gushwork.ai/demo' },
    { email: AGENCY_SITE.email, website: AGENCY_SITE.website, submitted_at: new Date(), updated_at: new Date(), page_url: 'https://www.gushwork.ai/demo' },
    { email: NORMAL.email, website: NORMAL.website, submitted_at: new Date(), updated_at: new Date(), page_url: 'https://www.gushwork.ai/demo' },
  ];
  const bfPool = { query: async (q) => {
    if (/information_schema\.columns/.test(q)) return { rows: ['email', 'website', 'submitted_at', 'updated_at', 'page_url', 'booking_uid'].map((column_name) => ({ column_name })) };
    if (/FROM leads/.test(q)) return { rows: bfRows.map((r) => ({ ...r })) };
    return { rows: [] };
  } };
  for (const dry of [true, false]) {
    S.sf = [];
    const out = await runBackfill(bfPool, { emails: bfRows.map((r) => r.email), dry });
    const by = Object.fromEntries(out.results.map((r) => [r.email, r.action]));
    ok(`F backfill ${dry ? 'DRY' : 'real'}: agency by email is SKIPPED, not "would create" or FAILED`, /^skipped — agency domain flighted\.co \(by email\)/.test(by[AGENCY_EMAIL.email] || ''), by[AGENCY_EMAIL.email]);
    ok(`F backfill ${dry ? 'DRY' : 'real'}: agency by website is SKIPPED`, /^skipped — agency domain uprawmedia\.com \(by website\)/.test(by[AGENCY_SITE.email] || ''), by[AGENCY_SITE.email]);
    ok(`F backfill ${dry ? 'DRY' : 'real'}: the normal lead is pushed`, dry ? /WOULD CREATE/.test(by[NORMAL.email] || '') : by[NORMAL.email] === 'pushed', by[NORMAL.email]);
    eq(`F backfill ${dry ? 'DRY' : 'real'}: skips are counted as skipped, never failed`, `${out.summary.skipped}/${out.summary.failed}`, '2/0');
    const nonNormal = S.sf.filter((x) => /query/.test(x.url) && (/flighted|uprawmedia|acme-widgets/.test(decodeURIComponent(x.url))));
    eq(`F backfill ${dry ? 'DRY' : 'real'}: no Salesforce call is spent on an agency lead`, nonNormal.length, 0);
  }

  /* F2. The after-push skip branch. The pre-check above asks the SAME rule,
     so in practice a skip never reaches the push -- this branch is the net
     for a future rule pushToSalesforce has and the pre-check does not. It is
     reached here with a Salesforce stand-in whose push skips what the
     pre-check let through, and it must count a SKIP, never a FAILED. */
  {
    const bfPath = require.resolve(path.join(ROOT, 'backfill-sf.js'));
    const sfPath = require.resolve(path.join(ROOT, 'salesforce.js'));
    const savedBf = require.cache[bfPath], savedSf = require.cache[sfPath];
    delete require.cache[bfPath];
    require.cache[sfPath] = { id: sfPath, filename: sfPath, loaded: true, exports: {
      ...SF,
      salesforceSkipReason: () => null,
      pushToSalesforce: async () => ({ skipped: 'agency', detail: 'agency domain stand-in.test (by email)' }),
      getSalesforceToken: SF.getSalesforceToken,
    } };
    try {
      const { runBackfill: rb } = require(bfPath);
      const out = await rb(bfPool, { emails: [NORMAL.email], dry: false });
      const r = out.results.find((x) => x.email === NORMAL.email) || {};
      ok('F2 backfill: a skip reported by the push itself is SKIPPED, never FAILED', /^skipped — agency domain stand-in\.test/.test(r.action || '') && out.summary.failed === 0 && out.summary.skipped === out.summary.found && out.summary.found > 0, /* the stub returns every fixture row */ JSON.stringify({ action: r.action, summary: out.summary }));
    } finally {
      delete require.cache[bfPath];
      if (savedBf) require.cache[bfPath] = savedBf;
      require.cache[sfPath] = savedSf;
    }
  }

  /* ================================================================
     G. /monitor/sdr, BOOTED
     ================================================================ */
  S.sdrRows = [
    { email: AGENCY_EMAIL.email, website: AGENCY_EMAIL.website, first_name: 'Pat', created_at: new Date() },
    { email: AGENCY_SITE.email, website: AGENCY_SITE.website, first_name: 'Kim', created_at: new Date() },
    { email: NORMAL.email, website: NORMAL.website, first_name: 'Sam', created_at: new Date() },
  ];
  const sj = await (await realFetch(BASE + '/monitor/sdr?token=stub', { headers: { Origin: 'https://www.gushwork.ai' } })).json();
  const flag = (e) => (sj.leads || []).find((l) => l.email === e) || {};
  ok('G SDR JSON: every row is still listed', (sj.leads || []).length === 3 && sj.total === 3, JSON.stringify(sj.total));
  ok('G SDR JSON: agency by email is MARKED, with its domain', flag(AGENCY_EMAIL.email).is_agency === true && flag(AGENCY_EMAIL.email).agency_domain === 'flighted.co');
  ok('G SDR JSON: agency by website is MARKED', flag(AGENCY_SITE.email).is_agency === true && flag(AGENCY_SITE.email).agency_domain === 'uprawmedia.com');
  ok('G SDR JSON: a normal lead is not', flag(NORMAL.email).is_agency === false);
  eq('G SDR JSON: the agency count', sj.agency, 2);
  const csvRes = await realFetch(BASE + '/monitor/sdr?token=stub&format=csv', { headers: { Origin: 'https://www.gushwork.ai' } });
  const csv = await csvRes.text();
  ok('G SDR CSV: agency by email is LEFT OUT', !csv.includes(AGENCY_EMAIL.email));
  ok('G SDR CSV: agency by website is LEFT OUT', !csv.includes(AGENCY_SITE.email));
  ok('G SDR CSV: the normal lead is in it', csv.includes(NORMAL.email));
  eq('G SDR CSV: the response says how many it left out', csvRes.headers.get('x-agency-rows-excluded'), '2');
  const sdrJs = fs.readFileSync(path.join(ROOT, 'monitor', 'js', 'sdr.js'), 'utf8');
  ok('G SDR tab: an agency row carries a visible "agency" badge naming the domain', /l\.is_agency \? '<span class="badge b-neu" title="' \+ esc\('An agency we work with \(' \+ \(l\.agency_domain/.test(sdrJs));
  ok('G SDR tab: the export button count leaves the agency rows out', /'Export CSV' \+ \(t \? ' \(these ' \+ fmt\(rows\.length - agencyN\)/.test(sdrJs));

  /* ================================================================
     H. pushToSalesforce ITSELF
     ================================================================ */
  S.sf = [];
  let threw = null, res = null;
  try { res = await SF.pushToSalesforce({ ...AGENCY_EMAIL, first_name: 'Pat' }); } catch (e) { threw = e; }
  ok('H: a skip RESOLVES -- it never throws (a throw would mark the lead failed)', !threw, threw && threw.message);
  ok('H: ...to { skipped, detail }, not success', res && res.skipped === 'agency' && res.success === undefined && /flighted\.co/.test(res.detail), JSON.stringify(res));
  eq('H: ...having asked Salesforce NOTHING, not even "does this Lead exist" (no update either)', sfCalls(), 0);
  ok('H: the check runs before the try block, so nothing below it can run',
     /async function pushToSalesforce\(payload\) \{\s*const skip = salesforceSkipReason\(payload \|\| \{\}\);\s*if \(skip\) \{[\s\S]{0,200}return \{ skipped: skip\.reason, detail: skip\.detail \};\s*\}\s*try \{/.test(fs.readFileSync(path.join(ROOT, 'salesforce.js'), 'utf8')));
  ok('H: index.js hands Salesforce the REAL isInternalSubmission at boot', /\nsetSalesforceInternalCheck\(isInternalSubmission\);/.test(src));
  eq('H: ours, by the real rule (b@g.ai)', (SF.salesforceSkipReason({ email: 'b@g.ai' }) || {}).reason, 'internal');
  eq('H: a normal lead has no skip reason', SF.salesforceSkipReason(NORMAL), null);

  loud();
  console.log('');
  if (failures.length) {
    console.log('  FAILURES:');
    for (const x of failures) console.log('   ✗ ' + x);
  }
  console.log(`  passed: ${pass}`);
  console.log(`  failed: ${fail}`);
  console.log('');
  process.exit(fail ? 1 : 0);
})();
