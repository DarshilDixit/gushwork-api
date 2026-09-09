/* ============================================================
   /submit's "have we already announced this session" gate — EXECUTED.

   All three booking routes set completed=true on the lead row. So a
   visitor whose booking lands BEFORE their first submit reaches /submit
   with completed already true, and the old gate read that as "already
   announced" — skipping ALL FIVE downstream statements:

     slackSubmit, pushToSalesforce, the Meta CAPI Lead event,
     finaliseElvVerdict, and the four PartnerStack steps.

   No Slack post. No Salesforce Lead created (the booking routes only ever
   UPDATE an existing one). No Meta Lead conversion. No PartnerStack
   conversion. The only trace is a log line reading "Slack skipped", which
   looks exactly like a correct dedup.

   No confirmed production instance -- see the comment at the gate in
   index.js for why booked_at < submitted_at does not isolate one. The
   gate is fixed because it is wrong, not because a lead was counted lost.

   The gate now reads submitted_at, which this route writes and the booking
   routes do not, exactly as alertIfBookingWithoutSubmit has always done.

   WHY THIS SUITE EXECUTES RATHER THAN READS. A source assertion cannot tell
   a reachable statement from an unreachable one — the repo's own lesson from
   the 21 dead PartnerStack call sites and the ordering assertions that
   survive an `if (false)`. "The gate says submitted_at" is not the claim
   worth making. "All five statements actually ran" is, and the only way to
   get it is to drive the route and watch the calls leave.

   Each of the five announces itself with an outbound HTTP call to a distinct
   host, so a stubbed global.fetch observes all five without ambiguity.

   Dependency-free — pg is stubbed, fetch is stubbed, nothing leaves the box
   and no DATABASE_URL is needed.

   Run:  node tests/test-submit-gate.js
   ============================================================ */

require('./crash-reporter')('test-submit-gate');

const Module = require('module');
const path   = require('path');
const fs     = require('fs');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}

const PORT = 41237;
const BASE = `http://127.0.0.1:${PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* Scenario state, swapped between the two runs. */
const S = { leadRow: {}, fetches: [], gateSql: '' };

/* ── stub pg ──────────────────────────────────────────────────────
   Only two queries matter. The gate query returns whatever the scenario
   says the lead row holds; the PartnerStack claim must report a winning
   row so the conversion is actually attempted. Everything else returns
   empty — which is also what makes lookupElvStatus miss, and an ELV miss
   is the condition finaliseElvVerdict runs under. */
function stubQuery(q) {
  const flat = (typeof q === 'string' ? q : (q && q.text) || '').replace(/\s+/g, ' ').trim();
  if (/^SELECT (submitted_at|completed) FROM leads WHERE session_id=\$1$/.test(flat)) {
    S.gateSql = flat;
    return { rows: [S.leadRow], rowCount: 1 };
  }
  if (/UPDATE leads SET ps_signup_sent_at = NOW/.test(flat)) return { rows: [{ session_id: 'x' }], rowCount: 1 };
  return { rows: [], rowCount: 0 };
}
class StubClient { async query(q) { return stubQuery(q); } release() {} }
class StubPool {
  async connect() { return new StubClient(); }
  async query(q) { return stubQuery(q); }
  on() {} async end() {}
}
const origLoad = Module._load;
Module._load = function (request) {
  if (request === 'pg') return { Pool: StubPool, Client: StubClient, types: { setTypeParser() {} } };
  return origLoad.apply(this, arguments);
};

/* ── stub fetch ───────────────────────────────────────────────────
   realFetch is kept so the suite can drive the server over actual HTTP.
   Without that the suite's own POST is swallowed by the stub, the route
   never runs, and every statement reads as "skipped" — a false pass for
   scenario B and a false failure for scenario A. */
const realFetch = global.fetch.bind(global);
global.fetch = async function (url, opts) {
  const u = String(url);
  if (u.startsWith(BASE)) return realFetch(url, opts);
  S.fetches.push(u);
  const j = (o) => ({ ok: true, status: 200, json: async () => o, text: async () => JSON.stringify(o) });
  if (/oauth2\/token/.test(u))        return j({ access_token: 'stub', instance_url: 'https://stub.my.salesforce.com' });
  if (/salesforce\.com/.test(u))      return j({ id: '00Qstub', success: true });
  if (/graph\.facebook\.com/.test(u)) return j({ events_received: 1 });
  if (/hooks\.slack\.com/.test(u))    return { ok: true, status: 200, text: async () => 'ok', json: async () => ({}) };
  if (/partnerlinks\.io/.test(u))     return { ok: true, status: 200, text: async () => '', json: async () => ({}) };
  return j({});
};

/* Every integration configured, so each one genuinely calls out. A missing
   env var would make a statement silently no-op and look skipped. */
Object.assign(process.env, {
  PORT: String(PORT),
  DATABASE_URL: 'postgres://stub/stub',
  SLACK_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB',
  SLACK_ALERTS_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB-ALERTS',
  SF_CLIENT_ID: 'stub', SF_CLIENT_SECRET: 'stub', SF_REFRESH_TOKEN: 'stub',
  META_ACCESS_TOKEN: 'stub', META_PIXEL_ID: '1234567890',
  ELV_API_KEY: 'stub',
  PARTNERSTACK_TRACKING_TOKEN: 'stub',
  PARTNERSTACK_PUBLIC_KEY: 'stub', PARTNERSTACK_SECRET_KEY: 'stub',
  ALLOWED_ORIGIN: 'https://www.gushwork.ai',
  MONITOR_TOKEN: 'stub',
});

/* index.js is chatty at boot and on every request. Silence it, or the
   suite's own output is unreadable among the startup banners. */
const realLog = console.log, realWarn = console.warn, realErr = console.error;
const quiet = () => { console.log = console.warn = console.error = () => {}; };
const loud  = () => { console.log = realLog; console.warn = realWarn; console.error = realErr; };

quiet();
require(path.join(__dirname, '..', 'index.js'));

/* The five statements, each identified by the call it makes. */
const PROBES = [
  ['slackSubmit',        (f) => f.some((u) => /hooks\.slack\.com\/services\/STUB$/.test(u))],
  ['pushToSalesforce',   (f) => f.some((u) => /oauth2\/token/.test(u)) && f.some((u) => /\/sobjects\//.test(u))],
  ['Meta CAPI Lead',     (f) => f.some((u) => /graph\.facebook\.com/.test(u))],
  ['finaliseElvVerdict', (f) => f.some((u) => /emaillistverify/i.test(u))],
  ['PartnerStack steps', (f) => f.some((u) => /partnerlinks\.io|api\.partnerstack\.com/.test(u))],
];

async function drive(leadRow) {
  S.leadRow = leadRow; S.fetches = []; S.gateSql = '';
  const res = await realFetch(BASE + '/submit', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Origin: 'https://www.gushwork.ai', 'User-Agent': 'suite/1.0' },
    body: JSON.stringify({
      session_id: 'a417fd00-eaba-4148-8422-50fdd0ca2d7a',
      email: 'visitor@northwindtrading.com',
      website: 'northwindtrading.com',
      first_name: 'Sam', last_name: 'Tester', phone: '+15551234567',
      company: 'Northwind Trading', sell_to: 'B2B',
      page_url: 'https://www.gushwork.ai/demo',
      prefill_source: 'returning_visitor',
      /* a WEBSITE_VERIFIED_REASON, so the Meta Lead is not suppressed for a
         reason that has nothing to do with the gate under test */
      website_check_reason: 'resolved', website_check_failed: false,
      /* a partner is present, so the PartnerStack steps have real work */
      ps_xid: 'xid-stub-12345', ps_partner_key: 'pk-stub',
    }),
  });
  /* Four of the five are fire-and-forget AFTER res.json(). Returning from
     the request is not the same as the work having happened. */
  await sleep(1500);
  return { status: res.status, fetches: S.fetches.slice(), gateSql: S.gateSql };
}

(async () => {
  await sleep(900);   // let express bind

  /* ── A. Booked before submit — the shape that broke ───────────── */
  const a = await drive({ completed: true, submitted_at: null });
  loud();

  ok('A: the route responded 200', a.status === 200, `got ${a.status}`);
  ok('A: the gate reads submitted_at, not completed',
     /SELECT submitted_at FROM leads/.test(a.gateSql), `gate ran: ${a.gateSql}`);
  for (const [name, test] of PROBES) {
    ok(`A: ${name} RAN for a lead booked before submitting`, test(a.fetches),
        `observed: ${[...new Set(a.fetches)].join(', ') || '(no outbound calls at all)'}`);
  }

  /* ── B. Genuine repeat submit — must still be silent ──────────── */
  quiet();
  const b = await drive({ completed: true, submitted_at: new Date('2026-09-09T19:33:33Z') });
  loud();

  ok('B: the route responded 200', b.status === 200, `got ${b.status}`);
  for (const [name, test] of PROBES) {
    ok(`B: ${name} was skipped on a repeat submit`, !test(b.fetches),
        `unexpectedly called: ${[...new Set(b.fetches)].join(', ')}`);
  }

  /* ── The source half, kept deliberately small ─────────────────────
     The executed assertions above are the real claim. This one only
     guards against the old column name creeping back into the gate. */
  const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
  ok('gate: no SELECT completed remains on the /submit lookup',
     !/SELECT completed FROM leads WHERE session_id=\$1/.test(src));

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
