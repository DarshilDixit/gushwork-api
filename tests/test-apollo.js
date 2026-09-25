/* ============================================================
   Apollo enrichment — EXECUTED.

   Apollo ran out of credits three times (24 Jun, 3-10 Sept, 23 Sept
   onward) and nothing noticed, because it refuses a lookup with a JSON
   BODY rather than an exception: fetch resolved, .json() parsed, the
   route read {"error":"You have insufficient credits!"} as "no match",
   wrote an empty row, and the health check counted that row as enriched.
   recordFailure only ran from the catch, so it never ran at all.

   A source assertion could not have caught that -- the route LOOKED like
   it handled failure, it had a catch and a recordFailure in it. So this
   suite boots the real app, drives /enrich with each shape Apollo
   actually answers with, and watches what leaves: the Slack alert, the
   SQL, the response the form gets back.

   It also drives tools/re-enrich-apollo.js against a stubbed database,
   because the one promise that tool makes -- a dry run writes nothing
   and calls nobody -- is only worth anything if it is executed.

   Dependency-free: pg is stubbed, fetch is stubbed, nothing leaves the
   box and no DATABASE_URL is needed.

   Run:  node tests/test-apollo.js
   ============================================================ */

require('./crash-reporter')('test-apollo');

const Module = require('module');
const path   = require('path');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
const eq = (name, a, b) => ok(name, a === b, `got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`);

const PORT = 41241;
const BASE = `http://127.0.0.1:${PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* What Apollo will answer next, and everything that left the process. */
const S = { apollo: null, apolloCalls: 0, queries: [], slack: [] };

/* ── stub pg ─────────────────────────────────────────────────────── */
function stubQuery(q, params) {
  const flat = (typeof q === 'string' ? q : (q && q.text) || '').replace(/\s+/g, ' ').trim();
  S.queries.push({ sql: flat, params: params || (q && q.values) || [] });
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

/* ── stub fetch ──────────────────────────────────────────────────── */
const realFetch = global.fetch.bind(global);
global.fetch = async function (url, opts) {
  const u = String(url);
  if (u.startsWith(BASE)) return realFetch(url, opts);
  if (/api\.apollo\.io/.test(u)) {
    S.apolloCalls++;
    const a = S.apollo;
    return {
      ok: a.status >= 200 && a.status < 300, status: a.status,
      json: async () => { if (a.body === 'NOT JSON') throw new SyntaxError('Unexpected token <'); return a.body; },
    };
  }
  if (/hooks\.slack\.com/.test(u)) {
    S.slack.push({ url: u, body: String(opts && opts.body || '') });
    return { ok: true, status: 200, text: async () => 'ok', json: async () => ({}) };
  }
  return { ok: true, status: 200, json: async () => ({}), text: async () => '{}' };
};

Object.assign(process.env, {
  PORT: String(PORT),
  DATABASE_URL: 'postgres://stub/stub',
  SLACK_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB',
  SLACK_ALERTS_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB-ALERTS',
  APOLLO_API_KEY: 'stub',
  ALLOWED_ORIGIN: 'https://www.gushwork.ai',
  MONITOR_TOKEN: 'stub',
});

const realLog = console.log, realWarn = console.warn, realErr = console.error;
const quiet = () => { console.log = console.warn = console.error = () => {}; };
const loud  = () => { console.log = realLog; console.warn = realWarn; console.error = realErr; };

quiet();
require(path.join(__dirname, '..', 'index.js'));

/* The exact words Apollo sends, from the 413 refused rows in production. */
const CREDITS_BODY = {
  error: "You have insufficient credits! <a href='https://app.apollo.io/#/settings/plans/upgrade?source=api_credit_limit' aria-onclick='close_alert'>Upgrade your plan</a>",
  error_details: { code: 'insufficient_credits' },
};
const FOUND_BODY = { person: {
  first_name: 'Ada', last_name: 'Quill', title: 'VP Growth', linkedin_url: 'https://linkedin.com/in/adaquill',
  city: 'Boston', state: 'Massachusetts', country: 'United States', seniority: 'vp',
  organization: { name: 'Northwind Trading', estimated_num_employees: 137, industry: 'logistics',
                  annual_revenue: 25000000, website_url: 'https://northwindtrading.com' },
} };

async function enrich(email, session_id, apollo) {
  S.apollo = apollo; S.queries = []; S.slack = []; S.apolloCalls = 0;
  const res = await realFetch(BASE + '/enrich', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Origin: 'https://www.gushwork.ai', 'User-Agent': 'suite/1.0' },
    body: JSON.stringify({ email, session_id }),
  });
  const body = await res.json().catch(() => null);
  await sleep(250);   // alertOps posts fire-and-forget
  return { status: res.status, body, queries: S.queries.slice(), slack: S.slack.slice(), apolloCalls: S.apolloCalls };
}
const wrote  = (r, re) => r.queries.filter((q) => re.test(q.sql));
const UPSERT = /INSERT INTO enrichment_data \(session_id,email,enriched_first_name[\s\S]*DO UPDATE/;
const REFUSE = /INSERT INTO enrichment_data \(session_id, email, raw_response\) VALUES \(\$1, \$2, \$3\) ON CONFLICT \(session_id\) DO NOTHING/;
const LEADUP = /^UPDATE leads SET enriched_city=\$2/;

(async () => {
  await sleep(900);

  /* ── A. Out of credits: the shape that went unnoticed three times ── */
  const a = await enrich('buyer@northwindtrading.com', 'a1a1a1a1-0000-4000-8000-000000000001', { status: 422, body: CREDITS_BODY });
  loud();
  eq('A: the form still gets a 200', a.status, 200);
  eq('A: ...with empty fields, so the lead is untouched (fail open)', a.body && a.body.title, '');
  const aAlert = a.slack.filter((s) => /STUB-ALERTS/.test(s.url));
  eq('A: ONE alert reached the alerts channel', aAlert.length, 1);
  ok('A: it says "Out of credits", not "Authentication failed"',
     aAlert[0] && /Out of credits/.test(aAlert[0].body) && !/Authentication failed/.test(aAlert[0].body),
     aAlert[0] && aAlert[0].body.slice(0, 300));
  ok('A: it is CRITICAL', aAlert[0] && /critical/.test(aAlert[0].body));
  ok('A: it carries Apollo’s own words, with the HTML stripped',
     aAlert[0] && /insufficient credits/.test(aAlert[0].body) && !/<a href/.test(aAlert[0].body));
  ok('A: it tells the reader leads are still arriving and booking',
     aAlert[0] && /still arrive/.test(aAlert[0].body));
  eq('A: the refusal is RECORDED, insert-only', wrote(a, REFUSE).length, 1);
  ok('A: the record keeps Apollo’s reply, so the health check can read it',
     wrote(a, REFUSE)[0] && wrote(a, REFUSE)[0].params[2] && /insufficient credits/.test(wrote(a, REFUSE)[0].params[2].error));
  eq('A: the enrichment upsert did NOT run -- a refusal never overwrites a real enrichment', wrote(a, UPSERT).length, 0);
  eq('A: the lead row was NOT blanked', wrote(a, LEADUP).length, 0);

  /* ── B. The next refusal pages nobody: one incident, one page ── */
  quiet();
  const b = await enrich('cfo@northwindtrading.com', 'b2b2b2b2-0000-4000-8000-000000000002', { status: 422, body: CREDITS_BODY });
  loud();
  eq('B: a second refusal inside the cooldown sends no second alert', b.slack.length, 0);
  eq('B: ...but is still recorded', wrote(b, REFUSE).length, 1);

  /* ── C. A real answer: written exactly as before the refactor ── */
  quiet();
  const c = await enrich('ada@northwindtrading.com', 'c3c3c3c3-0000-4000-8000-000000000003', { status: 200, body: FOUND_BODY });
  loud();
  eq('C: the form gets the title back', c.body && c.body.title, 'VP Growth');
  eq('C: ...and the company', c.body && c.body.company, 'Northwind Trading');
  eq('C: ...and the size as a string', c.body && c.body.company_size, '137');
  const up = wrote(c, UPSERT)[0];
  ok('C: the enrichment upsert ran', !!up);
  if (up) {
    eq('C: bound session id', up.params[0], 'c3c3c3c3-0000-4000-8000-000000000003');
    eq('C: bound title (param 5)', up.params[4], 'VP Growth');
    eq('C: bound company (param 6)', up.params[5], 'Northwind Trading');
    eq('C: bound size (param 7)', up.params[6], '137');
    eq('C: bound city (param 10)', up.params[9], 'Boston');
    eq('C: bound revenue, formatted (param 17)', up.params[16], '$25.0M USD');
    eq('C: bound the raw reply last (param 24)', up.params[23], FOUND_BODY);
    eq('C: 24 parameters, matching the 24 placeholders', up.params.length, 24);
  }
  const lu = wrote(c, LEADUP)[0];
  ok('C: the lead row update ran', !!lu);
  if (lu) { eq('C: lead row gets the city', lu.params[1], 'Boston'); eq('C: 15 parameters', lu.params.length, 15); }
  eq('C: a real answer alerts nobody', c.slack.length, 0);

  /* ── D. No match is an ANSWER, not a failure ── */
  quiet();
  const d = await enrich('nobody@northwindtrading.com', 'd4d4d4d4-0000-4000-8000-000000000004', { status: 200, body: { person: null } });
  loud();
  eq('D: no match alerts nobody', d.slack.length, 0);
  eq('D: no match is written like any answer', wrote(d, UPSERT).length, 1);
  eq('D: ...with no title', wrote(d, UPSERT)[0] && wrote(d, UPSERT)[0].params[4], null);

  /* ── E. A reply that is not JSON: an outage, counted as one ── */
  quiet();
  const e1 = await enrich('e1@northwindtrading.com', 'e5e5e5e5-0000-4000-8000-000000000005', { status: 502, body: 'NOT JSON' });
  const e2 = await enrich('e2@northwindtrading.com', 'e5e5e5e5-0000-4000-8000-000000000006', { status: 502, body: 'NOT JSON' });
  const e3 = await enrich('e3@northwindtrading.com', 'e5e5e5e5-0000-4000-8000-000000000007', { status: 502, body: 'NOT JSON' });
  loud();
  eq('E: the form still gets a 200 on a 502', e1.status, 200);
  eq('E: the first two 502s page nobody', e1.slack.length + e2.slack.length, 0);
  ok('E: the third in a row is "Consecutive failures"', e3.slack.some((s) => /Consecutive failures/.test(s.body)),
     e3.slack.map((s) => s.body.slice(0, 120)).join(' | ') || '(nothing sent)');
  eq('E: a 502 is recorded as a refusal, not written as an enrichment', wrote(e1, UPSERT).length, 0);
  ok('E: ...and the record says why', wrote(e1, REFUSE)[0] && /not JSON \(HTTP 502\)/.test(wrote(e1, REFUSE)[0].params[2].error));

  /* ── F. Free mailboxes never reach Apollo, so never cost a credit ── */
  quiet();
  const f = await enrich('someone@gmail.com', 'f6f6f6f6-0000-4000-8000-000000000008', { status: 200, body: FOUND_BODY });
  loud();
  eq('F: a gmail address is not looked up', f.apolloCalls, 0);

  /* ── G. The dashboard carries the corrected labels ── */
  const html = await (await realFetch(BASE + '/monitor?token=stub')).text();
  ok('G: the Completed-no-booking card says what it counts', /Completed, no booking yet/.test(html) && !/No booking yet \(SDR\)/.test(html));
  ok('G: the daily chart no longer calls entries "one bar per person"', !/One bar per person/.test(html) && /counts form entries/.test(html));
  ok('G: the Blocked heading names BOTH mechanisms', /by the brand-domain list or by the website check/.test(html));
  ok('G: the Health row says it counts business-email leads Apollo FOUND', /Business-email leads Apollo found/.test(html));
  ok('G: the funnel draws step-to-step rates', /fStep\(f\.completed,f\.step1,"step 1"\)/.test(html) && /fStep\(f\.booked,f\.completed,"step 2"\)/.test(html));

  /* ── H. tools/re-enrich-apollo.js, against a stubbed database ── */
  const tool = require(path.join(__dirname, '..', 'tools', 're-enrich-apollo.js'));
  function toolDb() {
    const log = [];
    return { log, async query(sql, params) {
      const flat = String(sql).replace(/\s+/g, ' ').trim();
      log.push({ sql: flat, params });
      if (/LEFT JOIN leads l/.test(flat)) return { rows: [
        { session_id: 's1', email: 'a@acme-corp.com', enriched_at: '2026-09-23T13:00:00Z', page_url: 'https://www.gushwork.ai/demo' },
        { session_id: 's2', email: 'a@acme-corp.com', enriched_at: '2026-09-24T13:00:00Z', page_url: 'https://www.gushwork.ai/demo' },
        { session_id: 's3', email: 'b@beta-labs.io',  enriched_at: '2026-09-24T14:00:00Z', page_url: 'https://www.gushwork.ai/start' },
        { session_id: 's4', email: 'c@gamma-co.com',  enriched_at: '2026-09-25T09:00:00Z', page_url: 'https://www.gushwork.ai/demo' },
        { session_id: 's5', email: 'qa@gushwork.ai',  enriched_at: '2026-09-25T10:00:00Z', page_url: 'https://www.gushwork.ai/demo' },
        { session_id: 's6', email: 'x@realco.com',    enriched_at: '2026-09-25T11:00:00Z', page_url: 'https://gushwork.webflow.io/demo' },
      ] };
      if (/DISTINCT ON/.test(flat)) return { rows: [
        { email: 'c@gamma-co.com', session_id: 'g0', raw_response: { person: { title: 'CEO', organization: { name: 'Gamma Co' } } } },
      ] };
      if (/LIMIT 500/.test(flat)) return { rows: [{ answered: '100', matched: '82' }] };
      return { rows: [], rowCount: 1 };
    } };
  }
  const writes = (db) => db.log.filter((q) => /^(INSERT|UPDATE)/.test(q.sql));

  const db1 = toolDb();
  const p = await tool.plan(db1, { since: '2026-09-23' });
  eq('H: six refused sessions read', p.refusedSessions, 6);
  eq('H: our own address AND a staging-page submission are skipped', p.oursSkipped, 2);
  eq('H: three real addresses', p.addresses, 3);
  eq('H: the address already found elsewhere is a COPY', p.copies.length, 1);
  eq('H: ...and the rest need lookups, one per ADDRESS', p.lookups.length, 2);
  eq('H: the address with two sessions is ONE lookup carrying both', (p.lookups.find((x) => x.email === 'a@acme-corp.com') || {}).sessions.length, 2);
  eq('H: expected credits = lookups x recent match rate', p.expectedCredits, 2);
  eq('H: the ceiling is one credit per lookup', p.maxCredits, 2);
  eq('H: the since date reached the query', db1.log[0].params[0], '2026-09-23');
  eq('H: A DRY RUN WRITES NOTHING', writes(db1).length, 0);
  const lines = []; tool.report(p, { log: (x) => lines.push(x) });
  ok('H: the report prices it', lines.some((l) => /expect about 2 credit\(s\); at most 2/.test(l)), lines.join(' / '));

  /* Apply: copy c, find a, then b is refused -> stop. */
  const db2 = toolDb();
  const p2 = await tool.plan(db2, {});
  db2.log.length = 0;
  let calls = 0;
  const answers = { 'a@acme-corp.com': { status: 200, body: FOUND_BODY }, 'b@beta-labs.io': { status: 422, body: CREDITS_BODY } };
  const fakeFetch = async (url, opts) => { calls++; const who = JSON.parse(opts.body).email; const r = answers[who];
    return { status: r.status, json: async () => r.body }; };
  const out = await tool.apply(db2, fakeFetch, p2, { apiKey: 'k', pauseMs: 0, log: () => {} });
  eq('H: apply copied the reusable address', out.copied, 1);
  eq('H: apply made one call per address until the refusal', calls, 2);
  eq('H: apply STOPPED at the refusal', !!(out.stopped && /insufficient credits/.test(out.stopped)), true);
  const ups = db2.log.filter((q) => /^INSERT INTO enrichment_data \(session_id,email,enriched_first_name/.test(q.sql));
  eq('H: upserts = 1 copy + 2 sessions of the found address', ups.length, 3);
  ok('H: the copy wrote the earlier answer to the refused session', ups.some((q) => q.params[0] === 's4' && q.params[4] === 'CEO'));
  ok('H: the found address wrote BOTH its sessions', ups.some((q) => q.params[0] === 's1') && ups.some((q) => q.params[0] === 's2'));
  ok('H: nothing was written for the refused address', !db2.log.some((q) => (q.params || [])[0] === 's3'));
  eq('H: every session written also got the lead-row update',
     db2.log.filter((q) => /^UPDATE leads SET enriched_city=\$2/.test(q.sql)).length, 3);

  const db3 = toolDb(); const p3 = await tool.plan(db3, {}); db3.log.length = 0; let calls3 = 0;
  await tool.apply(db3, async (u, o) => { calls3++; return { status: 200, json: async () => FOUND_BODY }; }, p3, { apiKey: 'k', pauseMs: 0, limit: 1, log: () => {} });
  eq('H: --limit 1 makes one lookup', calls3, 1);

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
