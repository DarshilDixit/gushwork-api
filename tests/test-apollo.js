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

  /* One 502 now, so the success in C has a streak to RESET. Without that
     reset the three 502s in E would alert at the second, not the third. */
  quiet();
  const pre = await enrich('pre@northwindtrading.com', 'b2b2b2b2-0000-4000-8000-00000000000f', { status: 502, body: 'NOT JSON' });
  loud();
  eq('pre: a single 502 pages nobody', pre.slack.length, 0);

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
  /* TWO PATHS, and which one fires is the proof. The streak is reset by a
     success; the six-hour window deliberately is not ("three failed Apollo
     lookups in six hours matters even if interleaved with successes"). So
     pre + E1 + E2 is three in the window and E2 sends "Repeated failures".
     Had C's success NOT reset the streak, it would be 3 at E2 and send
     "Consecutive failures" instead. */
  eq('E: the first 502 after a success pages nobody', e1.slack.length, 0);
  ok('E: the second is the WINDOW alert, so the success in C reset the streak',
     e2.slack.some((s) => /Repeated failures/.test(s.body)) && !e2.slack.some((s) => /Consecutive failures/.test(s.body)),
     e2.slack.map((s) => s.body.slice(0, 120)).join(' | ') || '(nothing sent)');
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
  /* the classic page, at its fallback address since the switch (PR D) */
  const html = await (await realFetch(BASE + '/monitor/classic?token=stub')).text();
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

  /* Refused on the FIRST lookup: one call, then stop -- never walk the list
     stamping fresh refusals. The refused address is first here on purpose;
     when it was last, a run that carried on looked identical to one that
     stopped. */
  const db4 = toolDb(); const p4 = await tool.plan(db4, {}); db4.log.length = 0; let calls4 = 0;
  p4.lookups.sort((x, y) => (x.email === 'b@beta-labs.io' ? -1 : y.email === 'b@beta-labs.io' ? 1 : 0));
  const out4 = await tool.apply(db4, async (u, o) => { calls4++; const who = JSON.parse(o.body).email;
    const r = answers[who]; return { status: r.status, json: async () => r.body }; }, p4, { apiKey: 'k', pauseMs: 0, log: () => {} });
  eq('H: refused on the first lookup -> exactly one call', calls4, 1);
  ok('H: ...and it says it stopped', !!out4.stopped);
  eq('H: ...and no lookup was written', db4.log.filter((q) => /^INSERT INTO enrichment_data/.test(q.sql) && q.params[0] !== 's4').length, 0);

  const db3 = toolDb(); const p3 = await tool.plan(db3, {}); db3.log.length = 0; let calls3 = 0;
  await tool.apply(db3, async (u, o) => { calls3++; return { status: 200, json: async () => FOUND_BODY }; }, p3, { apiKey: 'k', pauseMs: 0, limit: 1, log: () => {} });
  eq('H: --limit 1 makes one lookup', calls3, 1);

  /* ── I. tools/sync-enrichment-out.js, against a stubbed mirror and Salesforce ──
     Run once for real on 26 Sept 2026 (mirror 342 rows, Salesforce 140
     Leads). What must hold if anyone runs it again: FILL-ONLY -- a value
     already there is never replaced -- a targeted UPDATE rather than
     syncToAWS, converted Leads left alone, numeric fields sent as numbers,
     and a partial Salesforce read refused rather than acted on. */
  const sync = require(path.join(__dirname, '..', 'tools', 'sync-enrichment-out.js'));
  ok('I: the Salesforce field names come from salesforce.js', sync.SF_MAP.length >= 10 && sync.SF_MAP.some(([c, sf]) => c === 'enriched_title' && sf === 'enriched_title__c'));
  /* scope(): THE LOOKED-UP ADDRESS MUST BE THE LEAD'S. A refusal row is
     insert-only, so a visitor who changed their email during an outage keeps
     the old address on it -- and carrying that person's Apollo record onto
     the new address's Salesforce Lead would show an AE a stranger. */
  const scopeDb = { query: async (sql) => {
    if (/information_schema/.test(sql)) return { rows: /'leads'|\$1/.test(sql) ? [{ column_name: 'enriched_city' }] : [] };
    return { rows: [
      { session_id: 'k1', email: 'kept@x.test', looked_up: 'kept@x.test', page_url: '/demo', enriched_city: 'Austin', enriched_title: 'CTO' },
      { session_id: 'k2', email: 'new@y.test', looked_up: 'old@z.test', page_url: '/demo', enriched_city: 'Paris', enriched_title: 'VP' },
      { session_id: 'k3', email: 'none@x.test', looked_up: 'none@x.test', page_url: '/demo', enriched_city: null, enriched_title: null } ] }; } };
  const scoped = await sync.scope(scopeDb, '2026-09-26T00:00:00Z');
  ok('I scope: a session whose looked-up address is not the lead\'s is SKIPPED, and counted',
     scoped.rows.length === 1 && scoped.rows[0].session_id === 'k1' && scoped.email_changed === 1, JSON.stringify({ rows: scoped.rows.map((r) => r.session_id), moved: scoped.email_changed }));
  ok('I scope: a session with nothing to carry is counted, not carried', scoped.empty === 1);
  const SC = { all: ['enriched_city', 'enriched_company_size', 'enriched_founded_year', 'enriched_title'], rows: [
    { session_id: 's1', email: 'a@x.test', enriched_title: 'Chief Executive Officer', enriched_city: 'Woburn', enriched_company_size: '11-50', enriched_founded_year: '2015', enriched_seniority: 'c_suite' },
    { session_id: 's2', email: 'b@x.test', enriched_title: 'New title' } ] };
  const mirrorDb = (rows) => { const log = []; return { log, query: async (sql, params) => { log.push({ sql: String(sql), params });
    if (/information_schema/.test(sql)) return { rows: ['enriched_title', 'enriched_city', 'enriched_company_size', 'enriched_other'].map((c) => ({ column_name: c })) };
    if (/^SELECT session_id/.test(String(sql).trim())) return { rows };
    return { rowCount: 1, rows: [] }; } }; };
  const MROWS = [{ session_id: 's1', enriched_title: null, enriched_city: 'Boston', enriched_company_size: '' }];
  let md = mirrorDb(MROWS);
  const dry = await sync.syncMirror(md, SC, { apply: false, log: () => {} });
  ok('I mirror: a dry run writes nothing', !md.log.some((q) => /UPDATE|INSERT/i.test(q.sql)));
  ok('I mirror: blanks only -- title and size to fill, the city it already has is left alone',
     dry.rows_to_fill === 1 && dry.fields_to_fill === 2 && dry.by_field.enriched_title === 1 && dry.by_field.enriched_company_size === 1 && !dry.by_field.enriched_city, JSON.stringify(dry));
  ok('I mirror: a session with no mirror row is counted, never created', dry.not_on_mirror === 1);
  md = mirrorDb(MROWS);
  await sync.syncMirror(md, SC, { apply: true, log: () => {} });
  const mUps = md.log.filter((q) => /^UPDATE/.test(q.sql.trim()));
  ok('I mirror: ONE targeted update, by session, fill-only at write time too',
     mUps.length === 1 && /^UPDATE gw_form_leads SET /.test(mUps[0].sql) && /WHERE session_id = \$1$/.test(mUps[0].sql.trim())
     && /enriched_title = COALESCE\(NULLIF\(enriched_title, ''\), \$\d\)/.test(mUps[0].sql) && mUps[0].params[0] === 's1', mUps[0] && mUps[0].sql);
  ok('I mirror: it never touches the city it did not need, disqualified, or any other column',
     mUps.length === 1 && !/enriched_city/.test(mUps[0].sql) && !/disqualified/.test(mUps[0].sql) && !md.log.some((q) => /INSERT|ON CONFLICT/i.test(q.sql)));

  const sfRun = async (records, { apply, totalSize } = {}) => {
    const updates = [];
    const SF = { getSalesforceToken: async () => ({ accessToken: 't', instanceUrl: 'https://sf.test' }),
      updateSFLead: async (id, patch) => { updates.push([id, patch]); return { success: true, leadId: id }; } };
    const fetchFn = async (u) => ({ ok: true, json: async () => (/describe$/.test(u)
      ? { fields: [{ name: 'enriched_title__c', type: 'string', length: 10, updateable: true },
                   { name: 'enriched_founded_year__c', type: 'double', updateable: true },
                   { name: 'enriched_city__c', type: 'string', length: 255, updateable: true },
                   { name: 'enriched_linkedin__c', type: 'string', length: 255, updateable: false },
                   /* the org's real spelling, capital I, and writable */
                   { name: 'enriched_Seniority__c', type: 'string', length: 255, updateable: true }] }
      : { totalSize: totalSize == null ? records.length : totalSize, done: true, records }) });
    let out = null, err = null;
    try { out = await sync.syncSalesforce(SC, { apply, log: () => {}, SF, fetchFn }); } catch (e) { err = e; }
    return { out, err, updates };
  };
  const RECS = [
    { Id: 'L1', Email: 'A@x.test', IsConverted: false, enriched_title__c: null, enriched_founded_year__c: null, enriched_city__c: 'Boston' },
    { Id: 'L2', Email: 'a@x.test', IsConverted: true },
    { Id: 'L3', Email: 'b@x.test', IsConverted: false, enriched_title__c: 'Existing' } ];
  let r = await sfRun(RECS, { apply: false });
  ok('I SF: a dry run writes nothing', r.updates.length === 0 && r.out, r.err && r.err.message);
  ok('I SF: a converted Lead is counted and skipped', r.out && r.out.converted_skipped === 1);
  ok('I SF: a Lead that already has the value is left alone', r.out && r.out.leads_to_fill === 1, JSON.stringify(r.out));
  ok('I SF: a field our user cannot write is reported, not attempted', r.out && r.out.not_updateable_in_sf.includes('enriched_linkedin__c'));
  r = await sfRun(RECS, { apply: true });
  ok('I SF: exactly one Lead written, the unconverted one, matched on the email whatever its case', r.updates.length === 1 && r.updates[0][0] === 'L1', JSON.stringify(r.updates));
  ok('I SF: blanks only, the length Salesforce declares, and a number sent as a number',
     r.updates[0] && r.updates[0][1].enriched_title__c === 'Chief Exec' && r.updates[0][1].enriched_founded_year__c === 2015 && !('enriched_city__c' in r.updates[0][1]), JSON.stringify(r.updates[0]));
  ok('I SF: a field Salesforce spells in a different CASE is still matched and written (enriched_linkedIn__c, 26 Sept)',
     r.updates[0] && r.updates[0][1].enriched_seniority__c === 'c_suite', JSON.stringify(r.updates[0]));
  r = await sfRun(RECS, { apply: true, totalSize: 9 });
  ok('I SF: a partial Salesforce read is REFUSED -- nothing written', !!r.err && r.updates.length === 0 && /partial/.test(r.err.message), r.err && r.err.message);

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
