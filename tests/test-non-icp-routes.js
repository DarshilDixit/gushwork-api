/* ============================================================
   The non-ICP block, DRIVEN OVER REAL HTTP.

   tests/test-non-icp.js asserts on source text and on the lifted
   verdict function. That cannot tell a reachable statement from an
   unreachable one, and it cannot tell you whether /non-icp-check
   serves a request at all — the repo's own lesson from the 21 dead
   PartnerStack call sites.

   So this one BOOTS the real express app and drives it over actual
   HTTP, the same way test-submit-gate.js does. pg is stubbed, fetch is
   stubbed for everything except this suite's own requests, and nothing
   leaves the box.

   What it proves that the other suite cannot:
     - /non-icp-check answers, with the right verdict, over HTTP
     - /partial stamps the row and does NOT fire StartTrial
     - /submit stamps the row, posts the BLOCKED Slack message rather
       than the normal one, does NOT fire Meta Lead, does NOT push to
       Salesforce
     - every booking route REFUSES the booking rather than merely
       suppressing the Schedule event
     - a clean lead is untouched by all of it

   Dependency-free. No DATABASE_URL, no network.

   Run:  node tests/test-non-icp-routes.js
   ============================================================ */

require('./crash-reporter')('test-non-icp-routes');

const Module = require('module');
const path   = require('path');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}

const PORT = 41293;
const BASE = `http://127.0.0.1:${PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* Scenario state, reset between runs. */
const S = { fetches: [], slackPayloads: [], leadRow: null, writes: [], psBlocked: false, verdict: null,
            /* /monitor/non-icp's two inputs, set per scenario. null means
               "this suite is not driving the report", so every other
               scenario keeps the stub's existing behaviour. */
            reportLeads: null, reportVerdicts: null, metaPayloads: [] };

/* ── stub pg ──────────────────────────────────────────────────────
   Both pools (Railway and the AWS warehouse) come through here.
   The customer-domain query returns NOTHING, so no domain is ever a
   known customer and the block is allowed to fire. The known-customer
   bypass gets its own coverage in test-non-icp.js section 7, where the
   set can be controlled directly. */
function stubQuery(q, params) {
  const flat = (typeof q === 'string' ? q : (q && q.text) || '').replace(/\s+/g, ' ').trim();
  S.writes.push({ flat, params });

  /* The two leads upserts. Echo back what was bound, so the route reads
     the same effective block a real Postgres would have returned after
     the sticky OR.

     RESOLVED BY COLUMN NAME, NOT BY COUNTING FROM THE END.

     It counted five back from the end until 15 Sept 2026, and the
     comment here called that "a known fragility" because it had already
     broken once -- when the V2 columns were appended, this stub silently
     started reading non_icp_checked_at as non_icp_blocked, a Date, which
     is truthy, so it did not even fail the way you would expect. It then
     broke a second time the moment product_interest was appended.

     Twice is enough. The column list and the VALUES list are now walked
     together: each column is paired with its value token, and a token of
     the form $N reads params[N-1]. Literals (true, false, NOW(), the
     bare 2 for step_reached) are skipped rather than miscounted. Append,
     insert or reorder anything and this keeps working. */
  if (/INSERT INTO leads \(/.test(flat)) {
    const p = params || [];
    const cols = ((flat.match(/INSERT INTO leads \(([^)]*)\)/) || [])[1] || '')
      .split(',').map((x) => x.trim());
    /* DEPTH-SCANNED, not regexed. The VALUES list contains NOW(), whose
       parens end any lazy [^)]* match at the wrong place -- which is how
       a 48-column statement read as 30 tokens. */
    const vi = flat.indexOf('VALUES (');
    let depth = 0, end = -1;
    for (let k = vi + 7; k < flat.length; k++) {
      if (flat[k] === '(') depth++;
      else if (flat[k] === ')') { depth--; if (depth === 0) { end = k; break; } }
    }
    const vals = (end === -1 ? '' : flat.slice(vi + 8, end))
      .replace(/NOW\(\)/g, 'NOW').split(',').map((x) => x.trim());
    if (cols.length !== vals.length) {
      throw new Error('leads upsert: ' + cols.length + ' columns but ' + vals.length +
                      ' value tokens — the statement could not bind');
    }
    const bound = (name) => {
      const i = cols.indexOf(name);
      if (i === -1) throw new Error('leads upsert lost the column ' + name);
      const m = /^\$(\d+)$/.exec(vals[i]);
      return m ? p[Number(m[1]) - 1] : vals[i];
    };
    return { rows: [{
      non_icp_blocked:     bound('non_icp_blocked') === true,
      non_icp_reason:      bound('non_icp_reason') || null,
      non_icp_source:      bound('non_icp_source') || null,
      non_icp_checked_at:  bound('non_icp_checked_at') || null,
      non_icp_llm_flagged: bound('non_icp_llm_flagged') === true,
      product:             bound('product') || null,
      product_interest:    bound('product_interest') || null,
      prev_booked: false, step_reached: 2,
    }], rowCount: 1 };
  }
  /* ── /monitor/non-icp's three reads ──────────────────────────────
     MATCHED BEFORE THE GENERIC BRANCHES BELOW, and that ordering is
     load-bearing: the report's verdict query also opens with
     "SELECT domain, business_type, blocking" but binds an ARRAY of
     domains rather than one, so the single-domain branch below would
     silently return nothing and the report would read as a cold cache
     for every lead. */
  if (/FROM non_icp_domain_verdicts WHERE domain = ANY/.test(flat)) {
    const want = new Set((params || [])[0] || []);
    const rows = (S.reportVerdicts || []).filter((v) => want.has(v.domain));
    return { rows, rowCount: rows.length };
  }
  if (/FROM leads l WHERE l\.created_at >=/.test(flat)) {
    let rows = S.reportLeads || [];
    /* HONOURS THE PRODUCT PREDICATE rather than ignoring it. A stub that
       returns the whole population whatever the WHERE says can only ever
       prove the SQL contained a filter -- not that the ladder still sums
       to the total once it has been applied, which is the property the
       whole tab rests on. */
    const m = /AND l\.product = \$(\d+)/.exec(flat);
    if (m) {
      const want = (params || [])[Number(m[1]) - 1];
      rows = rows.filter((r) => r.product === want);
    } else if (/AND l\.product IS NULL/.test(flat)) {
      rows = rows.filter((r) => r.product === null || r.product === undefined);
    }
    return { rows, rowCount: rows.length };
  }
  /* The standing cache inventory. Matched on the real column list: an
     approximate pattern here returned rows with no n, which arrived in
     the payload as NaN and serialised to null. */
  if (/SELECT business_type, source, scrape_status, COUNT\(\*\)::int/.test(flat)) {
    return { rows: [{ business_type: 'real_estate', source: 'llm', scrape_status: 'ok', n: 12 },
                     { business_type: 'b2b_services', source: 'llm', scrape_status: 'ok', n: 78 },
                     { business_type: null, source: 'llm_unreachable', scrape_status: 'thin', n: 10 }], rowCount: 3 };
  }
  /* The model layer's verdict cache. S.verdict is the row the classifier
     would have written; null means a cold cache, which must block nobody. */
  if (/^SELECT domain, business_type, blocking/.test(flat)) {
    const v = S.verdict && S.verdict.domain === (params || [])[0] ? [S.verdict] : [];
    return { rows: v, rowCount: v.length };
  }
  if (/^INSERT INTO non_icp_domain_verdicts/.test(flat)) return { rows: [], rowCount: 1 };
  /* What rejectBookingIfNonIcp reads. */
  if (/^SELECT email, company, website, phone, non_icp_blocked, non_icp_reason FROM leads/.test(flat)) {
    return { rows: S.leadRow ? [S.leadRow] : [], rowCount: S.leadRow ? 1 : 0 };
  }
  if (/^SELECT submitted_at FROM leads/.test(flat)) return { rows: [{ submitted_at: null }], rowCount: 1 };
  if (/^SELECT session_id, email, booking_uid FROM leads/.test(flat)) {
    return S.leadRow
      ? { rows: [{ session_id: '00000000-0000-4000-8000-000000000007', email: S.leadRow.email, booking_uid: null }], rowCount: 1 }
      : { rows: [], rowCount: 0 };
  }
  if (/^SELECT booking_uid FROM leads/.test(flat))  return { rows: [{ booking_uid: null }], rowCount: 1 };
  /* Aggregates (/monitor/metrics runs a dozen) must return ONE row, not zero.
     Every field is read through `parseInt(x) || 0`, so an empty object yields
     zeros -- but an empty ROW SET makes rows[0] undefined and the route 500s
     on a different error than the one under test. */
  if (/^SELECT[\s\S]*COUNT\(/i.test(flat)) return { rows: [{}], rowCount: 1 };
  /* What runPartnerStackSignup reads to see the block. */
  if (/^SELECT non_icp_blocked, non_icp_reason FROM leads WHERE session_id = \$1$/.test(flat)) {
    return { rows: [{ non_icp_blocked: S.psBlocked, non_icp_reason: S.psBlocked ? 'allstate.com' : null }], rowCount: 1 };
  }
  /* The conditional claim: reporting a winning row is what lets the
     conversion actually be attempted, so a missing guard really does send. */
  if (/UPDATE leads SET ps_signup_sent_at = NOW/.test(flat)) return { rows: [{ session_id: 'x' }], rowCount: 1 };
  return { rows: [], rowCount: 0 };
}
/* WHAT A NAMED COLUMN WAS BOUND TO, for assertions about a recorded
   write. The suite decoded these by slicing the last five params until
   15 Sept 2026, which broke the moment product_interest was appended --
   the same positional fragility the stub itself had, in the assertions
   rather than the fake. One helper, resolved by name, used by both. */
function boundCols(write) {
  const flat = (write && write.flat) || '';
  const p = (write && write.params) || [];
  const cols = ((flat.match(/INSERT INTO leads \(([^)]*)\)/) || [])[1] || '')
    .split(',').map((x) => x.trim());
  const vi = flat.indexOf('VALUES (');
  let depth = 0, end = -1;
  for (let k = vi + 7; k < flat.length; k++) {
    if (flat[k] === '(') depth++;
    else if (flat[k] === ')') { depth--; if (depth === 0) { end = k; break; } }
  }
  const vals = (end === -1 ? '' : flat.slice(vi + 8, end))
    .replace(/NOW\(\)/g, 'NOW').split(',').map((x) => x.trim());
  const out = {};
  cols.forEach((c, i) => {
    const m = /^\$(\d+)$/.exec(vals[i] || '');
    out[c] = m ? p[Number(m[1]) - 1] : vals[i];
  });
  return out;
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

/* ── stub fetch ───────────────────────────────────────────────────
   realFetch is kept so the suite can drive the server over actual HTTP.
   Without it the suite's own POST is swallowed and every route reads as
   "never ran" — a false pass. */
const realFetch = global.fetch.bind(global);
global.fetch = async function (url, opts) {
  const u = String(url);
  if (u.startsWith(BASE)) return realFetch(url, opts);
  S.fetches.push(u);
  if (/hooks\.slack\.com/.test(u)) {
    try { S.slackPayloads.push(JSON.parse(opts && opts.body)); } catch (_) {}
    return { ok: true, status: 200, text: async () => 'ok', json: async () => ({}) };
  }
  const j = (o) => ({ ok: true, status: 200, json: async () => o, text: async () => JSON.stringify(o) });
  if (/oauth2\/token/.test(u))        return j({ access_token: 'stub', instance_url: 'https://stub.my.salesforce.com' });
  if (/salesforce\.com/.test(u))      return j({ id: '00Qstub', success: true });
  if (/graph\.facebook\.com/.test(u)) {
    /* THE BODY, not just the URL. The one thing worth asserting about a
       Meta call is what content_ids it carried, and a URL cannot say. */
    try { S.metaPayloads.push(JSON.parse(opts && opts.body)); } catch (_) {}
    return j({ events_received: 1 });
  }
  if (/partnerlinks\.io/.test(u))     return { ok: true, status: 200, text: async () => '', json: async () => ({}) };
  return j({});
};

Object.assign(process.env, {
  PORT: String(PORT),
  DATABASE_URL: 'postgres://stub/stub',
  /* The warehouse pool must EXIST or the customer bypass throws
     "not configured", the verdict fails open, and nothing blocks --
     which would make every assertion below a false negative. */
  AWS_PG_HOST: 'stub', AWS_PG_PORT: '5432', AWS_PG_DATABASE: 'stub',
  AWS_PG_USER: 'stub', AWS_PG_PASSWORD: 'stub',
  NON_ICP_BLOCK: 'true',
  /* The model layer, fully on. Everything below that exercises the LLM
     path is therefore driving the SAME configuration production runs,
     not a source assertion about it. */
  NON_ICP_LLM_ENABLED: 'true',
  NON_ICP_LLM_META:    'true',
  NON_ICP_LLM_BLOCK:   'true',
  ANTHROPIC_API_KEY:   'sk-stub',
  SLACK_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB',
  SLACK_ALERTS_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB-ALERTS',
  SF_CLIENT_ID: 'stub', SF_CLIENT_SECRET: 'stub', SF_REFRESH_TOKEN: 'stub',
  META_ACCESS_TOKEN: 'stub', META_PIXEL_ID: '1234567890',
  ELV_API_KEY: 'stub',
  /* Without this the conversion stops inside sendConversion with 'no_token'
     and the CONTROL below passes for the wrong reason -- it would look like
     the guard worked when nothing had been sent by anyone. */
  PARTNERSTACK_TRACKING_TOKEN: 'stub',
  ALLOWED_ORIGIN: 'https://www.gushwork.ai',
  MONITOR_TOKEN: 'stub',
});

const realLog = console.log, realWarn = console.warn, realErr = console.error;
const quiet = () => { console.log = console.warn = console.error = () => {}; };
const loud  = () => { console.log = realLog; console.warn = realWarn; console.error = realErr; };

quiet();
require(path.join(__dirname, '..', 'index.js'));

const post = async (p, body) => {
  const r = await realFetch(BASE + p, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  let j = null; try { j = await r.json(); } catch (_) {}
  return { status: r.status, body: j };
};
const reset = () => { S.fetches = []; S.slackPayloads = []; S.metaPayloads = []; S.writes = []; S.leadRow = null; S.psBlocked = false; S.verdict = null; S.reportLeads = null; S.reportVerdicts = null; };
const metaFired      = () => S.fetches.some((u) => /graph\.facebook\.com/.test(u));
const salesforceHit  = () => S.fetches.some((u) => /\/sobjects\//.test(u));
const leadSlack      = () => S.slackPayloads.filter((p) => /hooks|./.test('') || true);

(async () => {
  await sleep(700);

  /* ========================================================
     1. /non-icp-check answers over real HTTP
     ======================================================== */
  {
    const blocked = await post('/non-icp-check', { email: 'agent@kw.com', website: '' });
    ok('route: /non-icp-check returns 200', blocked.status === 200, String(blocked.status));
    ok('route: a brokerage email is blocked', blocked.body && blocked.body.blocked === true, JSON.stringify(blocked.body));
    ok('route: it names the matched domain', blocked.body && blocked.body.matched_domain === 'kw.com', JSON.stringify(blocked.body));
    ok('route: it carries the label', blocked.body && blocked.body.label === 'Keller Williams', JSON.stringify(blocked.body));

    const web = await post('/non-icp-check', { email: 'someone@gmail.com', website: 'claudiapgomez.kw.com' });
    ok('route: a free email with a brokerage WEBSITE is blocked', web.body && web.body.blocked === true, JSON.stringify(web.body));

    const clean = await post('/non-icp-check', { email: 'someone@acme.com', website: 'acme.com' });
    ok('route: a clean lead is not blocked', clean.body && clean.body.blocked === false, JSON.stringify(clean.body));

    /* The five substring false positives, over the wire this time. */
    for (const d of ['paycompass.com', 'charleslegalpl.com', 'theimagecreatornm.com', 'krevera.com', 'ceterainvestors.com']) {
      const r = await post('/non-icp-check', { email: 'a@' + d, website: d });
      ok(`route: ${d} is NOT blocked`, r.body && r.body.blocked === false, JSON.stringify(r.body));
    }

    const empty = await post('/non-icp-check', {});
    ok('route: an empty body answers rather than erroring', empty.status === 200 && empty.body.blocked === false, String(empty.status));
  }

  /* ========================================================
     2. /partial — stamps the row, suppresses StartTrial, no redirect
     ======================================================== */
  {
    reset();
    const r = await post('/partial', {
      session_id: '00000000-0000-4000-8000-000000000001',
      email: 'agent@kw.com', sell_to: 'B2B', step_reached: 1,
      page_url: 'https://www.gushwork.ai/demo',
    });
    await sleep(500);
    ok('partial: responds ok', r.status === 200, String(r.status));
    const ins = S.writes.find((w) => /INSERT INTO leads \(/.test(w.flat));
    ok('partial: wrote the lead row', !!ins);
    /* The five non-ICP columns, resolved BY NAME. See boundCols. */
    const b = boundCols(ins);
    const np = [b.non_icp_blocked, b.non_icp_reason, b.non_icp_source, b.non_icp_checked_at, b.non_icp_llm_flagged];
    ok('partial: stamped non_icp_blocked = true', np[0] === true, String(np[0]));
    ok('partial: stamped the matched domain',    np[1] === 'kw.com', String(np[1]));
    /* PROVENANCE. A domain-list block must say so, because the two
       mechanisms are checked in completely different ways and a reader who
       cannot tell them apart cannot audit either. */
    ok('partial: stamped source = domain_list',  np[2] === 'domain_list', String(np[2]));
    ok('partial: stamped a checked_at',          np[3] instanceof Date, String(np[3]));
    ok('partial: did NOT set the model flag',    np[4] === false, String(np[4]));
    ok('partial: StartTrial did NOT fire for a blocked lead', !metaFired(),
       S.fetches.filter((u) => /facebook/.test(u)).join(','));
  }
  {
    reset();
    await post('/partial', {
      session_id: '00000000-0000-4000-8000-000000000002',
      email: 'buyer@acme.com', sell_to: 'B2B', step_reached: 1,
      page_url: 'https://www.gushwork.ai/demo',
    });
    await sleep(500);
    ok('partial: StartTrial DOES fire for a clean business lead', metaFired(),
       'no graph.facebook.com call was made');
  }

  /* ========================================================
     3. /submit — blocked Slack post, no Meta Lead, no Salesforce
     ======================================================== */
  {
    reset();
    const r = await post('/submit', {
      session_id: '00000000-0000-4000-8000-000000000003',
      email: 'agent@kw.com', website: 'www.kw.com', sell_to: 'B2B',
      first_name: 'A', last_name: 'Gent', company: 'Keller Williams', phone: '+15550001111',
      page_url: 'https://www.gushwork.ai/demo',
    });
    await sleep(700);
    ok('submit: responds ok', r.status === 200, String(r.status));
    ok('submit: response tells the form to redirect',
       r.body && r.body.non_icp_blocked === true, JSON.stringify(r.body));
    ok('submit: response names the matched domain',
       r.body && r.body.non_icp_reason === 'kw.com', JSON.stringify(r.body));
    ok('submit: Meta Lead did NOT fire', !metaFired());
    ok('submit: Salesforce was NOT called', !salesforceHit(),
       S.fetches.filter((u) => /salesforce/.test(u)).join(','));

    const posts = S.slackPayloads.map((p) => JSON.stringify(p));
    ok('submit: a Slack message was posted', posts.length > 0, String(posts.length));
    ok('submit: it is the BLOCKED message, not the normal one',
       posts.some((p) => /Lead Blocked/.test(p)) && !posts.some((p) => /Lead Form Completed/.test(p)),
       posts.join(' | ').slice(0, 300));
    /* The full record the reviewer asked for.

       Asserted against the BLOCKS, not the whole payload. sendSlack also sets a
       plain-text `text` fallback that repeats the matched domain, so a check
       against JSON.stringify(payload) passes even when the visible message has
       lost the field entirely -- measured: a mutation that stripped the matched
       domain from the rendered block SURVIVED exactly that way. The blocks are
       what a human actually reads. */
    const blockedPayload = S.slackPayloads.find((p) => /Lead Blocked/.test(JSON.stringify(p.blocks || [])));
    ok('submit: the blocked post has rendered blocks', !!blockedPayload);
    const rendered = JSON.stringify((blockedPayload || {}).blocks || []);
    for (const [label, needle] of [
      ['matched domain', 'kw.com'], ['email', 'agent@kw.com'],
      ['website', 'kw.com'], ['company', 'Keller Williams'], ['phone', '15550001111'],
    ]) {
      ok(`submit: the RENDERED blocked post carries the ${label}`, rendered.includes(needle), label);
    }
    /* The matched domain must be ON THE "Matched:" LINE, not merely somewhere
       in the message. 'kw.com' is also this lead's website, so a substring
       check over the whole payload passes even when the Matched line has been
       emptied -- measured, that mutation survived twice before this assertion
       was written. Pin the line itself. */
    ok('submit: the MATCHED LINE names the domain',
       /\*Matched:\*[^\\"]{0,40}kw\.com/.test(rendered), rendered.slice(0, 300));
    ok('submit: the matched line names the brand',
       /Keller Williams/.test(rendered));
  }
  {
    reset();
    await post('/submit', {
      session_id: '00000000-0000-4000-8000-000000000004',
      email: 'buyer@acme.com', website: 'acme.com', sell_to: 'B2B',
      first_name: 'Real', last_name: 'Buyer', company: 'Acme', phone: '+15550002222',
      page_url: 'https://www.gushwork.ai/demo',
    });
    await sleep(700);
    const posts = S.slackPayloads.map((p) => JSON.stringify(p));
    ok('submit: a clean lead gets the NORMAL Slack post',
       posts.some((p) => /Lead Form Completed/.test(p)) && !posts.some((p) => /Lead Blocked/.test(p)),
       posts.join(' | ').slice(0, 200));
    ok('submit: a clean lead DOES fire Meta Lead', metaFired());
    ok('submit: a clean lead IS pushed to Salesforce', salesforceHit());
  }

  /* ========================================================
     4. BOOKING ROUTES REFUSE, not merely suppress

     A suppressed Schedule event still leaves a real slot on a real AE's
     calendar. The write must not happen at all.
     ======================================================== */
  {
    reset();
    S.leadRow = { email: 'agent@kw.com', company: 'KW', website: 'kw.com',
                  phone: '+1555', non_icp_blocked: true, non_icp_reason: 'kw.com' };
    const r = await post('/booking-confirmed', {
      session_id: '00000000-0000-4000-8000-000000000005',
      booking_uid: 'uid-blocked-1', start_time: '2026-09-20T10:00:00.000Z',
    });
    await sleep(500);
    ok('booking: /booking-confirmed REFUSES a blocked lead', r.status === 403, String(r.status));
    ok('booking: it says why', r.body && r.body.error === 'non_icp_blocked', JSON.stringify(r.body));
    const wrote = S.writes.some((w) => /UPDATE leads SET booking_uid/.test(w.flat));
    ok('booking: NO booking_uid was written', !wrote,
       'the refusal did not stop the write — the slot would be recorded as a real booking');
    ok('booking: Schedule did NOT fire', !metaFired());
    ok('booking: a critical alert was raised so a human can cancel the slot',
       S.slackPayloads.some((p) => /blocked lead took a calendar slot/i.test(JSON.stringify(p))),
       S.slackPayloads.map((p) => JSON.stringify(p)).join(' | ').slice(0, 200));
  }
  {
    reset();
    S.leadRow = { email: 'buyer@acme.com', company: 'Acme', website: 'acme.com',
                  phone: '+1555', non_icp_blocked: false, non_icp_reason: null };
    const r = await post('/booking-confirmed', {
      session_id: '00000000-0000-4000-8000-000000000006',
      booking_uid: 'uid-clean-1', start_time: '2026-09-20T10:00:00.000Z',
    });
    await sleep(500);
    ok('booking: a clean lead is accepted', r.status === 200, String(r.status));
    ok('booking: a clean lead DOES get its booking written',
       S.writes.some((w) => /UPDATE leads SET booking_uid/.test(w.flat)));
  }
  {
    /* The Cal webhook. Separate route, separate guard -- "a fix on one is a
       fix on one third". */
    reset();
    S.leadRow = { email: 'agent@kw.com', company: 'KW', website: 'kw.com',
                  phone: '+1555', non_icp_blocked: true, non_icp_reason: 'kw.com' };
    const r = await post('/booking-confirmed-webhook', {
      triggerEvent: 'BOOKING_CREATED',
      payload: { uid: 'uid-blocked-cal', startTime: '2026-09-20T10:00:00.000Z',
                 attendees: [{ email: 'agent@kw.com' }] },
    });
    await sleep(500);
    ok('booking: the Cal webhook refuses too',
       r.body && r.body.action === 'refused_non_icp', JSON.stringify(r.body));
    ok('booking: Cal webhook wrote no booking_uid',
       !S.writes.some((w) => /UPDATE leads SET booking_uid/.test(w.flat)));
  }

  /* ========================================================
     5. THE SAFETY-NET PATHS — a booking with no form row at all

     Both webhooks create a lead from scratch when no session matches.
     There is no row to read a block off, so the verdict is computed from
     the email. Ten such rows exist in production (all rh_webhook), so
     this is the one route that bypasses the form entirely.
     ======================================================== */
  {
    reset();                       // S.leadRow = null -> no existing session
    const r = await post('/booking-confirmed-webhook', {
      triggerEvent: 'BOOKING_CREATED',
      payload: { uid: 'uid-safety-blocked', startTime: '2026-09-20T10:00:00.000Z',
                 attendees: [{ email: 'agent@statefarm.com', name: 'A Gent' }] },
    });
    await sleep(500);
    ok('safety net: Cal refuses a brand-domain email with no form row',
       r.body && r.body.action === 'refused_non_icp_safety_net', JSON.stringify(r.body));
    ok('safety net: Cal created NO lead row',
       !S.writes.some((w) => /INSERT INTO leads \(/.test(w.flat)),
       'a lead row was created for a refused booking');
    /* NO alert assertion here, deliberately. alertOps has a 3-hour cooldown
       keyed on severity:source:title, and section 4 already fired this exact
       key in this process — so a second one inside one test run is CORRECTLY
       suppressed. Asserting it would be asserting a cooldown bug. The alert
       path itself is covered by section 4, and was fired for real against
       production Slack via tools/fire-non-icp-slack.js booking. */
    ok('safety net: Cal recorded the refusal in its response',
       r.body && r.body.ok === true, JSON.stringify(r.body));
  }
  {
    reset();
    const r = await post('/booking-confirmed-webhook', {
      triggerEvent: 'BOOKING_CREATED',
      payload: { uid: 'uid-safety-clean', startTime: '2026-09-20T10:00:00.000Z',
                 attendees: [{ email: 'buyer@acme.com', name: 'Real Buyer' }] },
    });
    await sleep(500);
    ok('safety net: a clean email with no form row is still accepted',
       r.body && r.body.action !== 'refused_non_icp_safety_net', JSON.stringify(r.body));
    ok('safety net: a clean lead row IS created',
       S.writes.some((w) => /INSERT INTO leads \(/.test(w.flat)));
  }

  /* ========================================================
     6. PARTNERSTACK MUST NOT PAY FOR A BLOCKED LEAD

     This fired in production on 11 Sept: agent@allstate.com was blocked,
     Meta and Salesforce were correctly suppressed, and a real conversion
     went out for customer_key allstate.com. The guard read only
     `disqualified`, and the block lives in its own column.

     Driven over HTTP rather than asserted, because the call sits in the
     fire-and-forget tail AFTER res.json() -- exactly the position a source
     assertion cannot tell you is reachable.
     ======================================================== */
  {
    reset();
    S.psBlocked = true;
    await post('/submit', {
      session_id: '00000000-0000-4000-8000-000000000008',
      email: 'agent@allstate.com', website: 'www.allstate.com', sell_to: 'B2B',
      first_name: 'A', last_name: 'Gent', company: 'Allstate', phone: '+15550003333',
      page_url: 'https://www.gushwork.ai/demo',
      ps_xid: 'M7wnDScN0rrUYH',
    });
    await sleep(900);
    ok('MONEY: no conversion was sent for a blocked lead',
       !S.fetches.some((u) => /partnerlinks\.io/.test(u)),
       S.fetches.filter((u) => /partnerlinks/.test(u)).join(','));
    ok('MONEY: the skip was recorded as non_icp_blocked',
       S.writes.some((w) => /ps_signup_skipped_reason/.test(w.flat)
                         && (w.params || []).includes('non_icp_blocked')),
       'no skip row written');
    ok('MONEY: the domain was never even claimed',
       !S.writes.some((w) => /UPDATE leads SET ps_signup_sent_at = NOW/.test(w.flat)),
       'the claim ran for a lead that must never convert');
  }
  {
    /* The control: a partner lead that is NOT blocked still converts.
       Without this, deleting the whole conversion path would pass. */
    reset();
    S.psBlocked = false;
    await post('/submit', {
      session_id: '00000000-0000-4000-8000-000000000009',
      email: 'buyer@acme.com', website: 'acme.com', sell_to: 'B2B',
      first_name: 'Real', last_name: 'Buyer', company: 'Acme', phone: '+15550004444',
      page_url: 'https://www.gushwork.ai/demo',
      ps_xid: 'M7wnDScN0rrUYH',
    });
    await sleep(900);
    ok('MONEY: a clean partner lead DOES still convert',
       S.fetches.some((u) => /partnerlinks\.io/.test(u)),
       'the conversion path is dead for everyone, not just blocked leads');
  }

  /* ========================================================
     7. /monitor/metrics ACTUALLY RESPONDS

     Added because it did not. `const peopleNonIcp = parseInt(p.people_non_icp)`
     was written up with the other counters, above `const p = people.rows[0]`,
     which is a temporal dead zone -- the route 500'd with
     "Cannot access 'p' before initialization" and the whole Overview tab was
     blank in production until it was curled by hand.

     EVERY assertion on this route reads source text, and no boot test drove
     it. A source assertion cannot tell you whether a function runs; that is
     this repo's most-repeated lesson and it landed again here.
     ======================================================== */
  {
    reset();
    const r = await realFetch(BASE + '/monitor/metrics?token=stub', { signal: AbortSignal.timeout(15000) });
    let body = null; try { body = await r.json(); } catch (_) {}
    ok('metrics: responds 200, not 500', r.status === 200, String(r.status) + ' ' + JSON.stringify(body));
    ok('metrics: no error in the body', !(body && body.error), body && body.error);
    for (const k of ['total', 'completed', 'booked', 'disqualified', 'nonIcpBlocked', 'peopleNonIcp']) {
      ok(`metrics: carries ${k}`, body && Object.prototype.hasOwnProperty.call(body, k), JSON.stringify(body).slice(0, 160));
    }
  }

  /* ========================================================
     8. EVERY MONITOR TAB — ROUTE AND RENDER

     Two live dashboard breaks in one night, both invisible to the
     suite and both a different flavour of the same thing:

       /monitor/metrics  a temporal dead zone  -> 500, blank Overview
       Blocked tab       leadRowsHtml declared inside loadLeads
                         -> "leadRowsHtml is not defined" on open

     Neither is a syntax error, so node --check passes. Neither is
     visible in source text, so every assertion in test-non-icp.js
     passes. The first needed the ROUTE driven; the second needs the
     browser JS EVALUATED and the tab loader actually CALLED, because
     the function is defined and reachable -- just not from there.

     So this section does both:
       a) drives every /monitor/* route over HTTP for a 200
       b) evaluates the dashboard's inline script in a stubbed DOM and
          invokes every tab loader, failing on any ReferenceError
     ======================================================== */
  {
    reset();
    const TABS = [
      ['/monitor/metrics',      'Overview'],
      ['/monitor/leads?page=1', 'All Leads'],
      ['/monitor/sdr',          'SDR List'],
      ['/monitor/duplicates',   'Duplicates'],
      ['/monitor/lm-metrics',   'Lead Magnet'],
      ['/monitor/partners',     'Partners'],
      ['/monitor/leads?nonicp=only&page=1', 'Blocked'],
      /* The Model tab. It is the only monitor route that joins leads to
         the verdict cache IN JAVASCRIPT rather than in SQL, so a 200 here
         also proves nonIcpCandidateDomains is reachable from it. */
      ['/monitor/non-icp?days=7', 'Model'],
      ['/monitor/health',       'System Health'],
    ];
    for (const [path, label] of TABS) {
      const sep = path.includes('?') ? '&' : '?';
      let r, body = null;
      try {
        r = await realFetch(BASE + path + sep + 'token=stub', { signal: AbortSignal.timeout(20000) });
        try { body = await r.json(); } catch (_) {}
      } catch (err) { r = { status: 0 }; body = { error: err.message }; }
      ok(`tab route ${label} (${path}) answers 200`, r.status === 200,
         String(r.status) + ' ' + JSON.stringify(body).slice(0, 160));
      ok(`tab route ${label} has no error in the body`, !(body && body.error),
         body && body.error);
    }
  }

  /* ---- the browser half ---- */
  {
    const page = await realFetch(BASE + '/monitor?token=stub', { signal: AbortSignal.timeout(20000) });
    const html = await page.text();
    ok('dashboard: /monitor renders', page.status === 200 && html.length > 5000, String(page.status));

    /* The inline script is the LAST <script> block on the page; the first
       is the Chart.js CDN tag. */
    const open = html.lastIndexOf('<script>');
    const close = html.indexOf('</script>', open);
    const js = (open !== -1 && close !== -1) ? html.slice(open + 8, close) : '';
    ok('dashboard: the inline script was extracted', js.length > 5000, String(js.length));

    /* A DOM stub that is permissive rather than faithful: every element
       answers every property, so the only thing that can throw is a real
       scope or reference error -- which is the whole point. */
    /* Records what each element's innerHTML was set to, keyed by id. That is
       what turns this from "did it throw" into "what did the user see" --
       loadBlocked CAUGHT its own ReferenceError and rendered
       "Could not load: leadRowsHtml is not defined" into the table, which is
       exactly the production symptom and is invisible to a try/catch probe. */
    const painted = {};
    const mkEl = (id) => new Proxy({}, {
      get(t, k) {
        if (k === 'value' || k === 'textContent') return '';
        if (k === 'innerHTML') return painted[id] || '';
        if (k === 'style' || k === 'dataset') return {};
        if (k === 'classList') return { toggle() {}, add() {}, remove() {}, contains() { return false; } };
        if (k === 'checked' || k === 'disabled') return false;
        if (k === Symbol.toPrimitive || k === 'toString') return () => '';
        return typeof k === 'string' ? (() => mkEl(id)) : undefined;
      },
      set(t, k, v) { if (k === 'innerHTML' || k === 'textContent') painted[id] = String(v); return true; },
    });
    const el = mkEl('_generic');
    const doc = {
      getElementById: (id) => mkEl(id), querySelector: () => el, querySelectorAll: () => [],
      createElement: () => el, addEventListener() {}, body: el, documentElement: el,
    };
    const errs = [];
    /* THE PAYLOAD, NAMED. The assertions below read numbers back out
       of the painted HTML and compare them to this object, so it has
       to be reachable by name rather than inlined into the stub. */
    const sandboxPayload = {
          total: 1, page: 1, pages: 1,
          leads: [{ session_id: '00000000-0000-4000-8000-00000000000a', email: 'a@b.com',
                    first_name: 'A', last_name: 'B', company: 'C', sell_to: 'B2B',
                    product: 'aeo', created_at: new Date().toISOString(),
                    non_icp_blocked: true, non_icp_reason: 'kw.com', step_reached: 2 }],
          rows: [], partners: [], domains: [], checks: [], sessions: [],
          duplicates: [], people: 1, byDay: [], funnel: [],
          /* The Model tab's shape. Present so mdlLadderHtml, the
             decisions map and the unreadable map all RUN -- a loader
             that renders an empty state cannot catch a scope error in
             the branch that renders rows. */
          windowDays: 7, truncated: false,
          flags: { list_block: true, llm_enabled: true, llm_block: true,
                   llm_meta: true, model: 'claude-opus-5',
                   confidence_floor: 0.75, prompt_version: 'v1-test' },
          ladder: { total: 4, rows: [
            { key: 'blocked_list',  label: 'Blocked — brand list', n: 1, pct: 25 },
            { key: 'meta_only',     label: 'Meta withheld only',   n: 1, pct: 25 },
            { key: 'checked_clear', label: 'Checked, no action',   n: 1, pct: 25 },
            { key: 'not_decided',   label: 'Not decided',          n: 1, pct: null },
          ] },
          industries: [
            { key: 'blocked_list', label: 'Blocked by the brand-domain list', note: 'n', leads: 7,
              rows: [{ business_type: 'insurance', label: 'Insurance', uncategorised: false,
                       leads: 5, domains: 2, median_confidence: 0.99 },
                     { business_type: '_uncategorised', label: 'Not categorised', uncategorised: true,
                       leads: 2, domains: 0, median_confidence: null }] },
            { key: 'blocked_model', label: 'Blocked by the model', note: 'n', leads: 0, rows: [] },
            { key: 'meta_only', label: 'Meta withheld by the model', note: 'n', leads: 3,
              rows: [{ business_type: 'home_services', label: 'Home services / trades',
                       uncategorised: false, leads: 3, domains: 3, median_confidence: 0.84 }] },
          ],
          decisions: [{ session_id: 's1', created_at: new Date().toISOString(),
                        email: 'a@kw.com', website: 'https://kw.com', company: 'KW',
                        booked: true, product: 'aeo', action: 'blocked_list',
                        source: 'domain_list', domain_judged: 'kw.com',
                        business_type: 'real_estate', business_type_label: 'Real estate',
                        confidence: 0.97, evidence_quote: 'We are a brokerage',
                        reason: 'brokerage', model_id: 'm', prompt_version: 'v1-test',
                        page_url_used: 'https://kw.com', page_text_chars: 900,
                        checked_at: new Date().toISOString() }],
          /* TOP-LEVEL, a sibling of scrape. It was read as d.scrape.cache
             by mdlScrapeHtml, which rendered "0 companies classified all
             time" above a table listing hundreds. The numbers below are
             distinctive on purpose so the assertions can find them in
             the painted HTML rather than merely checking it is not an
             error message. */
          cache: { domains: 2975, judged: 2965, unreadable: 10,
                   byType: [{ business_type: 'real_estate', label: 'Real estate', action: 'block', domains: 141 }] },
          scrape: {
            window: { ok: 9, unreachable: 1, thin: 0, other: 0, no_verdict: 2, total: 12,
                      answered: 10, unreadable_pct: 10 },
            unreadable: [{ domain: 'x.test', scrape_status: 'thin', error: null,
                           checked_at: new Date().toISOString(), email: 'a@x.test',
                           website: 'https://x.test', blocked: false, blocked_by: null }],
            inProcess: { since: Date.now(), ok: 3, errored: 0, unreachable: 1,
                         writeFailed: 0, bypassFailed: 0, cacheHits: 5, cacheMisses: 3,
                         cacheHitPct: 62.5, avgWarmMs: 2793, maxWarmMs: 6671,
                         lastOkAt: Date.now(), lastErrorAt: null, lastError: null },
            notes: ['Latest outcome per domain, not a historical rate.'],
          },
    };
    const sandbox = {
      document: doc, window: { location: { href: '', search: '' }, addEventListener() {} },
      location: { href: '', search: '' },
      localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
      setInterval: () => 0, clearInterval() {}, setTimeout: (f) => 0, clearTimeout() {},
      Chart: function () { return { destroy() {}, update() {} }; },
      AbortSignal: { timeout: () => undefined },
      console: { log() {}, warn() {}, error() {} },
      /* Every loader ends up here. Returning a plausible shape for each
         means each render path actually RUNS rather than bailing early --
         a loader that never reaches its renderer cannot catch a scope
         error in that renderer. */
      fetch: async () => ({
        ok: true, status: 200, text: async () => '{}',
        json: async () => sandboxPayload,
      }),
    };
    let evalErr = null;
    let scope = null;
    try {
      scope = new Function(...Object.keys(sandbox),
        js + '\n; return { leadRowsHtml: typeof leadRowsHtml === "function" ? leadRowsHtml : null,'
           + ' loadLeads: typeof loadLeads === "function" ? loadLeads : null,'
           + ' loadBlocked: typeof loadBlocked === "function" ? loadBlocked : null,'
           + ' loadSDR: typeof loadSDR === "function" ? loadSDR : null,'
           + ' loadDupes: typeof loadDupes === "function" ? loadDupes : null,'
           + ' loadLM: typeof loadLM === "function" ? loadLM : null,'
           + ' loadPartners: typeof loadPartners === "function" ? loadPartners : null,'
           + ' checkHealth: typeof checkHealth === "function" ? checkHealth : null,'
           + ' showTab: typeof showTab === "function" ? showTab : null,'
           + ' esc: typeof esc === "function" ? esc : null,'
           + ' escq: typeof escq === "function" ? escq : null,'
           + ' nonIcpSourceShort: typeof nonIcpSourceShort === "function" ? nonIcpSourceShort : null,'
           + ' nonIcpSourceWhy: typeof nonIcpSourceWhy === "function" ? nonIcpSourceWhy : null,'
           + ' et: typeof et === "function" ? et : null,'
           + ' enrichPanel: typeof enrichPanel === "function" ? enrichPanel : null,'
           + ' stageBadge: typeof stageBadge === "function" ? stageBadge : null,'
           + ' metaMark: typeof metaMark === "function" ? metaMark : null,'
           + ' metaWithheldLabel: typeof metaWithheldLabel === "function" ? metaWithheldLabel : null,'
           + ' metaWithheldShort: typeof metaWithheldShort === "function" ? metaWithheldShort : null,'
           + ' activeFilters: typeof activeFilters === "function" ? activeFilters : null,'
           + ' renderFilterState: typeof renderFilterState === "function" ? renderFilterState : null,'
           + ' loadModel: typeof loadModel === "function" ? loadModel : null,'
           + ' mdlLadderHtml: typeof mdlLadderHtml === "function" ? mdlLadderHtml : null,'
           + ' mdlScrapeHtml: typeof mdlScrapeHtml === "function" ? mdlScrapeHtml : null,'
           + ' mdlChip: typeof mdlChip === "function" ? mdlChip : null,'
           + ' mdlActionChip: typeof mdlActionChip === "function" ? mdlActionChip : null,'
           + ' mdlConf: typeof mdlConf === "function" ? mdlConf : null,'
           + ' mdlPct: typeof mdlPct === "function" ? mdlPct : null,'
           + ' mdlBar: typeof mdlBar === "function" ? mdlBar : null };'
      )(...Object.values(sandbox));
    } catch (err) { evalErr = err; }
    const eq2 = (n, a, b) => ok(n, a === b, `got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`);
    ok('dashboard: the inline script evaluates without throwing', !evalErr, evalErr && evalErr.message);

    /* escq exists because every HTML attribute here is SINGLE-quoted while
       esc escapes only & < > and the double quote. Two attribute values carry
       text a human can put an apostrophe in -- the ack note, typed into a
       prompt(), and a partner display name arriving from PartnerStack as
       first_name plus last_name. O'Brien ends the attribute early and the
       rest of the tooltip becomes markup.

       Asserting the OUTPUT, not that the function exists: "it is defined" is
       one level short of the thing that actually matters, which is the whole
       lesson of the Model tab's confident zero. */
    /* ── WHY WAS THIS LEAD TURNED AWAY ───────────────────────────────
       Two checks can block, and non_icp_reason holds a DOMAIN for both, so
       the row could not say which. Plain words, because SDRs read this tab.

       TWO answers, never three. Eight leads blocked between the 11 Sept brand
       list and the 14 Sept model layer have no source stored, because the
       column did not exist yet -- but the model could not block anything
       before it shipped, so those ARE brand-list blocks. An earlier draft
       showed them as "unrecorded", which made a reader stop and decode a
       third state for no gain. */
    if (scope && scope.nonIcpSourceShort && scope.nonIcpSourceWhy && scope.leadRowsHtml) {
      eq2('blocked/why: the model reads as "AI check"', scope.nonIcpSourceShort('llm'), 'AI check');
      eq2('blocked/why: the list reads as "Brand list"', scope.nonIcpSourceShort('domain_list'), 'Brand list');
      eq2('blocked/why: a pre-column block also reads as "Brand list", not a third state',
          scope.nonIcpSourceShort(null), 'Brand list');
      /* EXACTLY TWO labels can ever appear in that column. A third would make
         the column stop scanning, which is the whole point of the change. */
      const labels = new Set(['llm', 'domain_list', null, undefined, '', 'something_new']
        .map((v) => scope.nonIcpSourceShort(v)));
      eq2('blocked/why: only two distinct labels are reachable', labels.size, 2);

      /* NO SLUGS ON SCREEN. The stored values must not leak to a reader. */
      for (const v of ['llm', 'domain_list', null]) {
        const lab = scope.nonIcpSourceShort(v);
        ok(`blocked/why: "${lab}" is plain words, not a stored slug`,
           !/_/.test(lab) && lab !== 'llm' && lab !== 'domain_list');
      }

      /* The footnote still exists for anyone auditing a pre-column row, even
         though the chip reads the same as a real list block. */
      ok('blocked/why: a pre-column block still explains its provenance on hover',
         /before we started recording which check fired/.test(scope.nonIcpSourceWhy(null)));
      ok('blocked/why: a real list block carries no such footnote',
         !/before we started recording/.test(scope.nonIcpSourceWhy('domain_list')));
      ok('blocked/why: the AI explanation points at the quote on the Model tab',
         /quote/.test(scope.nonIcpSourceWhy('llm')));

      /* PAINTED, not just computed. Blocked-tab only; All Leads keeps it in
         the tooltip so the shared renderer does not clutter a table where the
         column means nothing. */
      const row = (src, ns) => scope.leadRowsHtml([{ session_id: 's1', email: 'a@kw.com',
        non_icp_blocked: true, non_icp_reason: 'kw.com', non_icp_source: src }], ns);
      ok('blocked/why: the Blocked row paints a visible "AI check" chip',
         />AI check</.test(row('llm', 'b')));
      ok('blocked/why: and "Brand list" for a list block',
         />Brand list</.test(row('domain_list', 'b')));
      ok('blocked/why: a pre-column block paints "Brand list" too',
         />Brand list</.test(row(null, 'b')));
      ok('blocked/why: All Leads paints no chip but keeps the reason on hover',
         !/class="pschip"/.test(row('llm', 'l')) && /Blocked by: AI check/.test(row('llm', 'l')));
      ok('blocked/why: an unblocked lead gets nothing at all',
         !/Blocked by:|pschip/.test(scope.leadRowsHtml(
           [{ session_id: 's3', email: 'c@y.com', non_icp_blocked: false }], 'b')));
    }

    if (scope && scope.escq) {
      ok('dashboard/escq: an apostrophe is escaped, so a single-quoted attribute survives',
         scope.escq("O'Brien Marketing").indexOf("'") === -1,
         'got ' + JSON.stringify(scope.escq("O'Brien Marketing")));
      ok('dashboard/escq: and it escapes it as a numeric entity',
         scope.escq("O'Brien") === 'O&#39;Brien',
         'got ' + JSON.stringify(scope.escq("O'Brien")));
      ok('dashboard/escq: it still does everything esc does',
         scope.escq('<b>&"x"</b>') === scope.esc('<b>&"x"</b>'));
      /* The bug it prevents, stated as the assertion: esc alone does NOT. */
      ok('dashboard/escq: esc alone would have left the apostrophe in place',
         scope.esc("O'Brien").indexOf("'") !== -1);
    }

    if (scope) {
      /* THE SCOPE CHECK. leadRowsHtml was declared INSIDE loadLeads, so it
         was reachable from All Leads and undefined from Blocked. A name
         that is not visible at top level cannot be shared between tabs. */
      for (const nm of ['leadRowsHtml', 'esc', 'escq', 'nonIcpSourceShort', 'nonIcpSourceWhy',
                        'et', 'enrichPanel', 'stageBadge',
                        /* The Meta-withheld marker and its label, called
                           from leadRowsHtml -- which renders BOTH All
                           Leads and Blocked, so a declaration tucked
                           inside either loader repeats the 12 Sept break
                           exactly. The filter-state pair is here for the
                           same reason: loadLeads and the empty-state
                           button both call them. */
                        'metaMark', 'metaWithheldLabel', 'metaWithheldShort',
                        'activeFilters', 'renderFilterState',
                        'showTab', 'loadLeads', 'loadBlocked', 'loadSDR',
                        'loadDupes', 'loadLM', 'loadPartners', 'checkHealth',
                        /* The Model tab's own helpers. Every one is used by
                           loadModel and by nothing else today, which is
                           exactly the shape leadRowsHtml had the day it was
                           declared inside loadLeads. */
                        'loadModel', 'mdlLadderHtml', 'mdlScrapeHtml', 'mdlChip',
                        'mdlActionChip', 'mdlConf', 'mdlPct', 'mdlBar']) {
        ok(`dashboard: ${nm} is defined at TOP LEVEL`, typeof scope[nm] === 'function',
           'declared inside another function, so other tabs cannot see it');
      }

      /* And CALL every loader. A name being visible is not the same as its
         render path running -- this is what actually reproduces the
         Blocked tab break. */
      for (const nm of ['loadLeads', 'loadBlocked', 'loadSDR', 'loadDupes',
                        'loadLM', 'loadPartners', 'checkHealth', 'loadModel']) {
        let thrown = null;
        try { await scope[nm](1); } catch (err) { thrown = err; }
        ok(`dashboard: ${nm}() runs without a ReferenceError`,
           !(thrown && thrown instanceof ReferenceError), thrown && thrown.message);
      }

      /* WHAT THE USER ACTUALLY SEES. Every loader wraps its render in a
         try/catch that paints the error into the table, so a scope error
         reads as a tidy "Could not load:" message rather than a crash.
         Probing for a thrown error misses it entirely. */
      const painted_ = Object.entries(painted);
      /* THE MODEL TAB'S CONTAINERS ARE ASSERTED FROM A FIXED LIST, not
         from whatever happened to be painted, and that is the difference
         between a suite that catches a regression and one that can be
         MEASURED catching it.

         Iterating over Object.entries(painted) makes the assertion count
         depend on how far the loader got. Break the ladder renderer and
         mdl-scrape is never reached, so the suite runs one assertion
         fewer -- and measure.js correctly refuses to call that a catch,
         because it cannot tell a caught mutation from a suite that
         quietly ran a different set of checks. That is the
         test-ads-parity failure mode (one failure, 13 of 159 assertions)
         arriving in a new place.

         A fixed list runs the same number of assertions either way: each
         container is asserted to have been painted AND to hold content
         rather than an error. */
      for (const id of ['mdl-flags', 'mdl-ladder', 'mdl-ind', 'mdl-dec',
                        'mdl-scrape', 'mdl-unread', 'mdl-cache']) {
        const html = painted[id];
        ok(`dashboard: ${id} was painted at all`, !!html && String(html).length > 10,
           id + ' -> ' + String(html).slice(0, 140));
        ok(`dashboard: ${id} rendered content, not an error`,
           !!html && !/Could not load|Failed:|is not defined|is not a function/i.test(html),
           id + ' -> ' + String(html).slice(0, 140));
      }

      /* ── THE NUMBERS ON SCREEN MUST BE THE NUMBERS IN THE PAYLOAD ──
         EVERYTHING ABOVE PASSES ON A CONFIDENT ZERO. On 15 Sept 2026
         the standing-cache summary rendered "0 companies classified
         all time — 0 judged, 0 currently unreadable" directly above a
         table listing 224 home services, because mdlScrapeHtml read
         d.scrape.cache when cache is a top-level field. Well-formed,
         non-empty, not an error, and wrong.

         "Did it render" cannot see that. The only thing that can is
         reading the numbers back out of the painted HTML and comparing
         them to what was handed in. The stub payload uses distinctive
         values (2975 / 2965 / 141) precisely so they cannot match by
         accident. */
      {
        const P = sandboxPayload;
        const has = (id, n) => new RegExp('(^|[^0-9])' + n + '([^0-9]|$)').test(String(painted[id] || ''));

        ok('numbers: the cache summary prints the payload company count',
           has('mdl-scrape', P.cache.domains), 'want ' + P.cache.domains + ' in ' + String(painted['mdl-scrape']).slice(0, 200));
        ok('numbers: the cache summary prints the payload judged count',
           has('mdl-scrape', P.cache.judged), 'want ' + P.cache.judged);
        ok('numbers: the cache summary prints the payload unreadable count',
           has('mdl-scrape', P.cache.unreadable), 'want ' + P.cache.unreadable);
        ok('numbers: the cache TABLE prints its own per-type count',
           has('mdl-cache', P.cache.byType[0].domains), 'want ' + P.cache.byType[0].domains);
        /* The bug in one assertion: summary and table disagreeing is
           the symptom, and they are two different readers of one field. */
        ok('numbers: the summary is not zero while the table has rows',
           !/0 companies classified all time/.test(String(painted['mdl-scrape'] || '')),
           String(painted['mdl-scrape']).slice(0, 200));

        for (const r of P.ladder.rows) {
          ok(`numbers: the ladder prints ${r.key} = ${r.n}`, has('mdl-ladder', r.n),
             'want ' + r.n + ' for ' + r.key);
        }
        ok('numbers: the ladder prints the window total',
           has('mdl-ladder', P.ladder.total), 'want ' + P.ladder.total);

        for (const g of P.industries) {
          ok(`numbers: the ${g.key} group prints its lead total (${g.leads})`,
             has('mdl-ind', g.leads), 'want ' + g.leads + ' for ' + g.key);
        }
        /* THE EMPTY GROUP RENDERS AND SAYS SO. This is how the tab
           states that the model has blocked nobody, so it must survive
           being empty rather than vanishing. */
        ok('numbers: an empty group still renders, saying none',
           /Blocked by the model[\s\S]*?None in this window/.test(String(painted['mdl-ind'] || '')),
           String(painted['mdl-ind']).slice(0, 400));

        ok('numbers: the scrape panel prints the window unreadable rate',
           has('mdl-scrape', P.scrape.window.unreadable_pct), 'want ' + P.scrape.window.unreadable_pct);
        ok('numbers: the scrape panel prints never-tried separately',
           has('mdl-scrape', P.scrape.window.no_verdict), 'want ' + P.scrape.window.no_verdict);
      }

      for (const [id, html] of painted_) {
        if (!/tbody|-tbody$/.test(id)) continue;
        ok(`dashboard: ${id} rendered content, not an error`,
           !/Could not load|Failed:|is not defined|is not a function/i.test(html),
           id + ' -> ' + String(html).slice(0, 140));
      }
      ok('dashboard: at least one table actually painted',
         painted_.some(([id, h]) => /tbody/.test(id) && h && h.length > 20),
         Object.keys(painted).join(','));
      /* ── THE SAME LEAD IN BOTH TABLES ────────────────────────────
         All Leads and Blocked render through ONE builder into two
         panels that are both in the document at once -- showTab toggles
         a class, it never clears a panel. So a lead that is blocked AND
         on the loaded All Leads page was emitted with the same
         id="er-<uuid>" twice, and getElementById returns the first in
         document order. tp-leads precedes tp-blocked, so the click on
         Blocked expanded the hidden copy in the inactive All Leads
         panel and nothing happened on screen.

         Reported 15 Sept 2026 as "the top two rows will not expand".
         Top two because All Leads page 1 is the newest 25 leads: a
         blocked lead breaks while it is new enough to be there and
         starts working again on its own once it falls off. A moving
         window, which is why it read as a property of those two rows.

         THIS IS ASSERTED ON THE IDS, NOT ON A CLICK, because the
         symptom is a click that does nothing -- no throw, no error
         painted, a fully rendered row. Every "did it render" check
         passes while the tab is broken. */
      {
        const lead = { session_id: '11111111-2222-4333-8444-555555555555',
          email: 'agent@allstate.test', first_name: 'A', last_name: 'B',
          company: 'Allstate', sell_to: 'B2B', product: 'aeo',
          created_at: new Date().toISOString(),
          non_icp_blocked: true, non_icp_reason: 'allstate.test' };
        const asLeads   = scope.leadRowsHtml([lead], 'l');
        const asBlocked = scope.leadRowsHtml([lead], 'b');
        const ids = (h) => [...String(h).matchAll(/id="(er-[^"]+)"/g)].map((m) => m[1]);
        const both = ids(asLeads).concat(ids(asBlocked));
        ok('rows: the same lead gets DIFFERENT row ids in the two tables',
           new Set(both).size === both.length, both.join(' , '));
        /* And the click has to address the row it is actually inside.
           A namespaced id with an unnamespaced onclick is the same bug
           with the halves swapped. */
        for (const [label, html] of [['All Leads', asLeads], ['Blocked', asBlocked]]) {
          const rowId  = (String(html).match(/id="(er-[^"]+)"/) || [])[1];
          const called = (String(html).match(/toggleRow\('([^']+)'/) || [])[1];
          ok(`rows: ${label} toggles the id it emitted`, rowId === 'er-' + called,
             rowId + ' vs toggleRow(' + called + ')');
        }
        /* The change log is addressed by the SAME key, or first expand
           writes into the other table's panel. */
        for (const [label, html] of [['All Leads', asLeads], ['Blocked', asBlocked]]) {
          const rowId = (String(html).match(/id="(er-[^"]+)"/) || [])[1];
          const lcId  = (String(html).match(/id="(lc-[^"]+)"/) || [])[1];
          ok(`rows: ${label} change-log div matches the row key`,
             rowId && lcId && rowId.slice(3) === lcId.slice(3), rowId + ' / ' + lcId);
        }
        /* THE SESSION ID STILL REACHES loadChanges. The key addresses
           the DOM and the session id addresses the lead; collapsing
           them back into one value is what caused this, and it would
           also send "b-<uuid>" to /monitor/lead-changes. */
        ok('rows: toggleRow is handed the raw session_id as its second argument',
           new RegExp("toggleRow\\('b-" + lead.session_id + "','" + lead.session_id + "'\\)").test(asBlocked),
           (String(asBlocked).match(/toggleRow\([^)]*\)/) || [])[0]);

        /* ── THE PANEL MUST ACTUALLY PAINT ─────────────────────────
           A row with NO enrichment and NO model verdict -- Becky Gerig's
           shape, and the one a "does it render" check misses, because
           an empty panel and a broken toggle look identical to a user:
           you click and nothing appears. */
        const bare = { session_id: '99999999-8888-4777-8666-555555555555',
          email: 'nobody@nowhere.test', created_at: new Date().toISOString(),
          non_icp_blocked: true, non_icp_reason: 'nowhere.test' };
        const barePanel = scope.enrichPanel(bare);
        ok('rows: a lead with no enrichment and no verdict still paints a panel',
           !!barePanel && String(barePanel).length > 40, String(barePanel).slice(0, 120));
        const bareRow = scope.leadRowsHtml([bare], 'b');
        ok('rows: and that panel is inside the expandable row, not empty',
           /<tr class="erow"[^>]*>[\s\S]*?<td colspan="10">[\s\S]{60,}?<\/td>/.test(bareRow),
           String(bareRow).slice(-200));
      }

      /* leadRowsHtml itself, on a real row shape. */
      let rowsErr = null, rowsHtml = '';
      try {
        rowsHtml = scope.leadRowsHtml([{ session_id: 's1', email: 'a@kw.com', first_name: 'A',
          last_name: 'B', company: 'KW', sell_to: 'B2B', product: 'aeo',
          created_at: new Date().toISOString(), non_icp_blocked: true, non_icp_reason: 'kw.com' }], 'l');
      } catch (err) { rowsErr = err; }
      ok('dashboard: leadRowsHtml renders a row', !rowsErr && rowsHtml.includes('<tr'), rowsErr && rowsErr.message);

      /* THE MARKER, READ BACK OUT OF THE PAINTED HTML. "It rendered" is
         one level short of the claim that matters -- the 15 Sept "0
         companies classified" bug passed every structural check. So the
         row is rendered and then inspected for the marker and its
         reason, and a clean row is required NOT to carry one. */
      if (scope.leadRowsHtml && scope.metaMark) {
        /* VISIBLE TEXT, not a tooltip. It shipped as an amber glyph whose
           reason you had to hover to read, which is useless for the job
           this feature exists for -- scanning a page of leads and seeing
           which did not fire and why. So the assertion strips the markup
           and reads what a person would actually see on the row. */
        const seen = (html) => html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
        const row = (over, ns) => scope.leadRowsHtml([{ session_id: 's9', email: 'a@b.com',
          first_name: 'A', created_at: new Date().toISOString(), ...over }], ns || 'l');

        const withReason = row({ meta_withheld_reason: 'model' });
        ok('dashboard: the reason is VISIBLE on the row, not only on hover',
           seen(withReason).includes('Meta: model'), seen(withReason).slice(0, 200));
        ok('dashboard: the chip still carries the long reason as a tooltip',
           /Model flagged the industry/.test(withReason), withReason.slice(0, 400));

        const clean = row({});
        ok('dashboard: a lead with no withheld reason paints NO chip',
           !/Meta: /.test(seen(clean)), seen(clean).slice(0, 200));

        /* Every reason the server can emit must render BOTH forms. A
           reason with no short label paints "Meta: " and reads as a data
           bug rather than a missing case here. */
        for (const r of ['internal', 'blocked', 'model', 'website', 'disqualified']) {
          ok(`dashboard: ${r} has a long label`, scope.metaWithheldLabel(r).length > 0, r);
          ok(`dashboard: ${r} has a short label for the chip`,
             scope.metaWithheldShort(r).length > 0, r);
          ok(`dashboard: ${r} renders as visible text`,
             seen(row({ meta_withheld_reason: r })).includes(
               'Meta: ' + scope.metaWithheldShort(r)), r);
        }

        /* NOT REPEATED ON THE BLOCKED TAB where it says nothing: every row
           there is blocked, so a column of identical "Meta: blocked" chips
           buries the rows whose reason is something else. */
        ok('dashboard: Blocked tab does not repeat "blocked" on every row',
           !/Meta: /.test(seen(row({ meta_withheld_reason: 'blocked' }, 'b'))),
           seen(row({ meta_withheld_reason: 'blocked' }, 'b')).slice(0, 200));
        ok('dashboard: but Blocked tab DOES show a reason that is not "blocked"',
           seen(row({ meta_withheld_reason: 'internal' }, 'b')).includes('Meta: internal'),
           seen(row({ meta_withheld_reason: 'internal' }, 'b')).slice(0, 200));
        /* And All Leads still shows it -- the suppression is scoped to the
           one tab that already asserts it, not global. */
        ok('dashboard: All Leads DOES show "blocked"',
           seen(row({ meta_withheld_reason: 'blocked' }, 'l')).includes('Meta: blocked'),
           seen(row({ meta_withheld_reason: 'blocked' }, 'l')).slice(0, 200));
      }
      ok('dashboard: a blocked row is marked in the rendered HTML', /kw\.com/.test(rowsHtml));
    }
  }

  /* ========================================================
     THE MODEL PATH, DRIVEN. Gate 3.

     Everything above exercises the brand-domain list. This section
     proves the LLM path reaches EXACTLY the same enforcement -- the
     redirect, the refused booking, and every Meta call site -- using a
     domain no list could ever match.
     ======================================================== */
  const VROW = (o = {}) => ({ domain: 'garyrockwellinsurance.test', business_type: 'insurance',
    blocking: true, confidence: 0.95, evidence_quote: 'independent insurance agency serving Ohio',
    reason: 'independent agency', source: 'llm', model_id: 'claude-opus-5',
    prompt_version: 'v1-2026-09-14', scrape_status: 'ok', error: null,
    checked_at: new Date().toISOString(), ...o });

  {
    reset();
    S.verdict = VROW();
    const r = await post('/non-icp-check', { email: 'gary@garyrockwellinsurance.test', website: 'garyrockwellinsurance.test' });
    ok('LLM: /non-icp-check blocks on a model verdict alone', r.body && r.body.blocked === true, JSON.stringify(r.body));
    ok('LLM: …and names the domain it judged', r.body && r.body.matched_domain === 'garyrockwellinsurance.test');
    /* The control that matters: the same domain with a COLD cache must
       not block. Otherwise the assertion above could be passing on the
       brand list or on some unrelated default. */
    reset();
    const cold = await post('/non-icp-check', { email: 'gary@garyrockwellinsurance.test', website: 'garyrockwellinsurance.test' });
    ok('LLM: a cold cache blocks nobody', cold.body && cold.body.blocked === false, JSON.stringify(cold.body));
  }

  {
    reset();
    S.verdict = VROW();
    await post('/partial', { session_id: '00000000-0000-4000-8000-0000000000a1',
      email: 'gary@garyrockwellinsurance.test', website: 'garyrockwellinsurance.test',
      sell_to: 'B2B', step_reached: 1, page_url: 'https://www.gushwork.ai/demo' });
    await sleep(400);
    const ins = S.writes.find((w) => /INSERT INTO leads \(/.test(w.flat));
    const b = boundCols(ins);
    const np = [b.non_icp_blocked, b.non_icp_reason, b.non_icp_source, b.non_icp_checked_at, b.non_icp_llm_flagged];
    ok('LLM/partial: stamped non_icp_blocked', np[0] === true, String(np[0]));
    ok('LLM/partial: source says llm, not domain_list', np[2] === 'llm', String(np[2]));
    ok('LLM/partial: stamped the model flag', np[4] === true, String(np[4]));
    ok('LLM/partial: StartTrial did NOT fire', !metaFired(),
       S.fetches.filter((u) => /facebook/.test(u)).join(','));
  }

  {
    reset();
    S.verdict = VROW();
    const r = await post('/submit', { session_id: '00000000-0000-4000-8000-0000000000a2',
      email: 'gary@garyrockwellinsurance.test', website: 'garyrockwellinsurance.test',
      first_name: 'Gary', last_name: 'Rockwell', company: 'Gary Rockwell Insurance',
      phone: '+15551234567', sell_to: 'B2B', page_url: 'https://www.gushwork.ai/demo' });
    await sleep(700);
    /* THE REDIRECT. This is the field the form reads to send them to
       /thank-you instead of rendering the calendar. */
    ok('LLM/submit: response tells the form to redirect', r.body && r.body.non_icp_blocked === true, JSON.stringify(r.body));
    ok('LLM/submit: response names the judged domain', r.body && r.body.non_icp_reason === 'garyrockwellinsurance.test');
    ok('LLM/submit: Meta Lead did NOT fire', !metaFired(),
       S.fetches.filter((u) => /facebook/.test(u)).join(','));
    ok('LLM/submit: Salesforce was NOT called', !salesforceHit(),
       S.fetches.filter((u) => /sobjects/.test(u)).join(','));
    const post0 = S.slackPayloads[0] || {};
    const txt = JSON.stringify(post0);
    ok('LLM/submit: it is the BLOCKED message, not the normal lead post',
       /Lead Blocked/.test(txt) && !/Lead Form Completed/.test(txt), txt.slice(0, 160));
    /* The three things that make a model block auditable in Slack. */
    ok('LLM/slack: carries the business type', /Insurance/i.test(txt), txt.slice(0, 200));
    ok('LLM/slack: carries the confidence', /95%/.test(txt), txt.slice(0, 200));
    ok('LLM/slack: carries the evidence quote', /independent insurance agency serving Ohio/.test(txt));
    ok('LLM/slack: names the model and prompt version', /claude-opus-5/.test(txt) && /v1-2026-09-14/.test(txt));
    ok('LLM/slack: points at the RIGHT off switch', /NON_ICP_LLM_BLOCK=false/.test(txt));
    /* THE MONEY PATH. An LLM block sets non_icp_blocked, which is the column
       runPartnerStackSignup already guards on -- but "already guards on" is
       exactly the assumption that cost a real conversion on 11 Sept. Driven. */
    ok('LLM/submit: NO PartnerStack conversion was sent',
       !S.fetches.some((u) => /partnerlinks\.io/.test(u)),
       S.fetches.filter((u) => /partnerlinks/.test(u)).join(','));
  }

  /* All three booking routes must REFUSE, not merely suppress Schedule.
     A suppressed Schedule event still leaves a real slot on a real AE's
     calendar. Each route is asserted on ITS OWN refusal shape -- a
     status-or-status check would pass for almost any response. */
  const LEADROW = () => ({ email: 'gary@garyrockwellinsurance.test', company: 'Gary Rockwell Insurance',
    website: 'garyrockwellinsurance.test', phone: '+15551234567',
    non_icp_blocked: true, non_icp_reason: 'garyrockwellinsurance.test' });
  const bookingWritten = () => S.writes.some((w) => /UPDATE leads SET booking_uid/.test(w.flat));
  {
    reset(); S.verdict = VROW(); S.leadRow = LEADROW();
    const r = await post('/booking-confirmed', { session_id: '00000000-0000-4000-8000-0000000000a3',
      booking_uid: 'llm-uid-1', start_time: '2026-10-01T10:00:00.000Z' });
    await sleep(500);
    ok('LLM/booking: /booking-confirmed returns 403', r.status === 403, String(r.status));
    ok('LLM/booking: …and says why', r.body && r.body.error === 'non_icp_blocked', JSON.stringify(r.body));
    ok('LLM/booking: NO booking_uid was written', !bookingWritten());
    ok('LLM/booking: Schedule did NOT fire', !metaFired());
    /* NOT asserted here. alertOps carries a 3-hour cooldown keyed on
       severity:source:title, and the V1 booking section above fires this
       exact alert earlier in the same process -- so a second assertion
       would be testing the cooldown, not the guard. The alert itself is
       covered there; what is new here is that the MODEL path reaches the
       same refusal, which the four assertions around this one prove. */
  }
  {
    reset(); S.verdict = VROW(); S.leadRow = LEADROW();
    const r = await post('/booking-confirmed-webhook', { triggerEvent: 'BOOKING_CREATED',
      payload: { uid: 'llm-uid-2', startTime: '2026-10-01T10:00:00.000Z',
                 attendees: [{ email: 'gary@garyrockwellinsurance.test' }] } });
    await sleep(500);
    ok('LLM/booking: the Cal webhook refuses',
       r.body && r.body.action === 'refused_non_icp', JSON.stringify(r.body));
    ok('LLM/booking: Cal wrote no booking_uid', !bookingWritten());
    ok('LLM/booking: Cal fired no Schedule', !metaFired());
  }
  {
    reset(); S.verdict = VROW(); S.leadRow = LEADROW();
    /* The REAL RevenueHero payload shape: { id, prospect: { email }, ... }.
       The first draft of this test used a guessed {event,data:{guest}} shape,
       got {ok:true,skipped:true} back, and would have passed a loose
       status-code assertion while never reaching the guard at all. */
    const r = await post('/booking-confirmed-webhook-rh', {
      id: 'llm-uid-3', status: 'booked', start_time: '2026-10-01T10:00:00.000Z',
      prospect: { email: 'gary@garyrockwellinsurance.test', name: 'Gary Rockwell' } });
    await sleep(500);
    ok('LLM/booking: the RevenueHero webhook refuses',
       r.body && /refused_non_icp/.test(JSON.stringify(r.body)), JSON.stringify(r.body));
    ok('LLM/booking: RH wrote no booking_uid', !bookingWritten());
    ok('LLM/booking: RH fired no Schedule', !metaFired());
  }

  /* ── The META-ONLY path: four industries suppress Meta and must NOT
        block. If these two ever agree, blocking has silently widened. ── */
  {
    reset();
    S.verdict = VROW({ domain: 'joesdiner.test', business_type: 'restaurant_food',
      blocking: false, confidence: 0.95, evidence_quote: 'family restaurant since 1994' });
    const chk = await post('/non-icp-check', { email: 'joe@joesdiner.test', website: 'joesdiner.test' });
    ok('META-ONLY: a restaurant is NOT blocked', chk.body && chk.body.blocked === false, JSON.stringify(chk.body));

    reset();
    S.verdict = VROW({ domain: 'joesdiner.test', business_type: 'restaurant_food',
      blocking: false, confidence: 0.95, evidence_quote: 'family restaurant since 1994' });
    await post('/partial', { session_id: '00000000-0000-4000-8000-0000000000a4',
      email: 'joe@joesdiner.test', website: 'joesdiner.test', sell_to: 'B2B',
      step_reached: 1, page_url: 'https://www.gushwork.ai/demo' });
    await sleep(400);
    const ins = S.writes.find((w) => /INSERT INTO leads \(/.test(w.flat));
    const b = boundCols(ins);
    const np = [b.non_icp_blocked, b.non_icp_reason, b.non_icp_source, b.non_icp_checked_at, b.non_icp_llm_flagged];
    ok('META-ONLY: NOT stamped as blocked', np[0] === false, String(np[0]));
    ok('META-ONLY: IS stamped as model-flagged', np[4] === true, String(np[4]));
    ok('META-ONLY: StartTrial still suppressed', !metaFired(),
       S.fetches.filter((u) => /facebook/.test(u)).join(','));

    reset();
    S.verdict = VROW({ domain: 'joesdiner.test', business_type: 'restaurant_food',
      blocking: false, confidence: 0.95, evidence_quote: 'family restaurant since 1994' });
    const sub = await post('/submit', { session_id: '00000000-0000-4000-8000-0000000000a5',
      email: 'joe@joesdiner.test', website: 'joesdiner.test', first_name: 'Joe', last_name: 'D',
      company: "Joe's Diner", phone: '+15550000000', sell_to: 'B2B',
      page_url: 'https://www.gushwork.ai/demo' });
    await sleep(700);
    /* The calendar MUST render for these people. */
    ok('META-ONLY: the form is NOT told to redirect', sub.body && sub.body.non_icp_blocked === false, JSON.stringify(sub.body));
    ok('META-ONLY: Meta Lead suppressed', !metaFired());
    ok('META-ONLY: Salesforce IS still called', salesforceHit(),
       'a Meta-only lead must still reach an AE');
    /* The fourth state's whole risk: a flagged-not-blocked lead must not be
       treated as a rejection by anything except Meta. If PartnerStack or
       Salesforce started excluding these, turning on Meta suppression would
       silently stop paying affiliates for restaurants. */
    ok('META-ONLY: the lead row is NOT marked blocked',
       sub.body && sub.body.non_icp_blocked === false);
    const txt = JSON.stringify(S.slackPayloads);
    ok('META-ONLY: Slack says Meta was withheld, NOT that it would have blocked',
       /Meta events withheld/.test(txt) && !/Would have been blocked/.test(txt), txt.slice(0, 200));
  }

  /* ========================================================
     THE LADDER ON /monitor/non-icp, DRIVEN WITH REAL ROWS.

     The four panels are only worth reading because the ladder is
     mutually exclusive and exhaustive: five rows that sum to the lead
     total, exactly like the stage ladder. Nothing asserted that, and a
     mutation deleting the meta_only branch outright SURVIVED the whole
     suite -- the route still answered 200 and the tab still painted,
     with one population silently folded into another.

     So this drives the real route with crafted leads and verdicts and
     checks where each one lands. The five inputs below are one of each
     state, plus the case the lead columns cannot answer on their own.
     ======================================================== */
  {
    reset();
    const now = new Date().toISOString();
    const L = (over) => ({
      session_id: 's', email: 'x@ex.test', website: null, company: null,
      first_name: null, last_name: null, created_at: now, product: 'aeo',
      booked: false, completed: true, non_icp_blocked: false, non_icp_reason: null,
      non_icp_source: null, non_icp_llm_flagged: false, non_icp_checked_at: now, ...over,
    });
    S.reportLeads = [
      L({ session_id: 'a', email: 'a@kw.test',     website: 'https://kw.test',
          non_icp_blocked: true, non_icp_source: 'domain_list', non_icp_reason: 'kw.test' }),
      L({ session_id: 'b', email: 'b@realty.test', website: 'https://realty.test',
          non_icp_blocked: true, non_icp_source: 'llm', non_icp_reason: 'realty.test' }),
      L({ session_id: 'c', email: 'c@solar.test',  website: 'https://solar.test',
          non_icp_llm_flagged: true, non_icp_source: 'llm', non_icp_reason: 'solar.test', booked: true }),
      /* Judged, and the model had no objection. */
      L({ session_id: 'd', email: 'd@saas.test',   website: 'https://saas.test' }),
      /* NOT DECIDED, and this is the case the lead row alone cannot
         answer: non_icp_checked_at is stamped (nonIcpStamp sets it for
         any verdict that is not check_failed or disabled, including a
         plain no-match) while no verdict for this domain exists. Read
         off the lead columns it is indistinguishable from the row
         above. Only the join can split them. */
      L({ session_id: 'e', email: 'e@nosite.test', website: 'https://nosite.test' }),
      /* NEVER TRIED -- no verdict row of ANY kind, not even a failure.
         Distinct from the row above, which has a failure row, and the
         distinction is the whole point of keeping never-tried out of
         the scrape rate: this domain is one the warm path has not
         reached, not one the scraper could not read. Without a lead in
         this state the two denominators are identical and a mutation
         merging them changes nothing. */
      L({ session_id: 'f', email: 'f@untouched.test', website: 'https://untouched.test' }),
      /* OURS. Counted in the ladder like everything else, and reported
         alongside so a quotable figure exists. */
      /* OURS, and the borrowing case in one row. Blocked on
         allstate.com, which has NO verdict -- while carrying
         saas.test as a website, which HAS one (software_technology
         0.95). If the deciding-domain rule ever regresses to "any
         candidate domain with a verdict", this lead is filed under
         Software / technology instead of uncategorised, and the
         assertion below says so by name. */
      L({ session_id: 'g', email: 'agent@allstate.com', website: 'https://saas.test',
          non_icp_blocked: true, non_icp_source: 'domain_list', non_icp_reason: 'allstate.com' }),
    ];
    S.reportVerdicts = [
      { domain: 'kw.test', business_type: 'real_estate', blocking: true, confidence: 0.97,
        evidence_quote: 'We are a brokerage', reason: 'brokerage', source: 'llm',
        model_id: 'm', prompt_version: 'v1', scrape_status: 'ok', error: null,
        page_url_used: 'https://kw.test', page_text_chars: 900, checked_at: now },
      { domain: 'realty.test', business_type: 'real_estate', blocking: true, confidence: 0.96,
        evidence_quote: 'Homes for sale', reason: 'brokerage', source: 'llm',
        model_id: 'm', prompt_version: 'v1', scrape_status: 'ok', error: null,
        page_url_used: 'https://realty.test', page_text_chars: 800, checked_at: now },
      { domain: 'solar.test', business_type: 'home_services', blocking: false, confidence: 0.82,
        evidence_quote: 'Residential Solar Installation', reason: 'installer', source: 'llm',
        model_id: 'm', prompt_version: 'v1', scrape_status: 'ok', error: null,
        page_url_used: 'https://solar.test', page_text_chars: 700, checked_at: now },
      { domain: 'saas.test', business_type: 'software_technology', blocking: false, confidence: 0.95,
        evidence_quote: 'A SaaS platform', reason: 'saas', source: 'llm',
        model_id: 'm', prompt_version: 'v1', scrape_status: 'ok', error: null,
        page_url_used: 'https://saas.test', page_text_chars: 600, checked_at: now },
      /* A failure row, which is NOT a verdict: nosite.test stays "not
         decided" and also shows up in the unreadable list. */
      { domain: 'nosite.test', business_type: null, blocking: false, confidence: null,
        evidence_quote: null, reason: null, source: 'llm_unreachable',
        model_id: 'm', prompt_version: 'v1', scrape_status: 'thin', error: 'text=0',
        page_url_used: null, page_text_chars: null, checked_at: now },
    ];

    const r = await realFetch(BASE + '/monitor/non-icp?days=7&token=stub', { signal: AbortSignal.timeout(20000) });
    const d = await r.json();
    ok('report: answers 200', r.status === 200, String(r.status));

    const by = {};
    for (const row of (d.ladder && d.ladder.rows) || []) by[row.key] = row.n;
    ok('report: the brand-list block lands in blocked_list',  by.blocked_list === 2,  JSON.stringify(by));
    ok('report: the model block lands in blocked_model',      by.blocked_model === 1, JSON.stringify(by));
    ok('report: the flagged-not-blocked lead lands in meta_only', by.meta_only === 1, JSON.stringify(by));
    ok('report: the judged-and-cleared lead lands in checked_clear', by.checked_clear === 1, JSON.stringify(by));
    /* THE ONE THE LEAD COLUMNS CANNOT ANSWER. A failure row is not a
       verdict, so this lead is undecided however stamped it looks. */
    ok('report: a lead with only a FAILURE row is not decided',   by.not_decided === 2, JSON.stringify(by));

    /* EXHAUSTIVE AND MUTUALLY EXCLUSIVE. This is the property the whole
       panel rests on and the one a deleted branch breaks silently. */
    const sum = Object.values(by).reduce((a, b) => a + b, 0);
    ok('report: the five rows sum to the lead total',
       sum === d.ladder.total && d.ladder.total === 7, sum + ' vs ' + (d.ladder && d.ladder.total));

    /* OURS, COUNTED ALONGSIDE AND NEVER SUBTRACTED. The ladder still
       totals every lead -- that is the property it exists for -- and
       the internal count rides beside it so a quotable figure exists
       without a filter. */
    ok('report: our own test submissions are counted, not removed',
       d.ladder.ours === 1 && by.blocked_list === 2, JSON.stringify({ ours: d.ladder.ours, by }));
    const blRow = d.ladder.rows.find((r) => r.key === 'blocked_list');
    ok('report: the row says how many of its own are ours', blRow && blRow.ours === 1, JSON.stringify(blRow));
    ok('report: the decision row is flagged as ours',
       (d.decisions || []).filter((x) => x.is_internal).length === 1,
       JSON.stringify((d.decisions || []).map((x) => [x.email, x.is_internal])));

    /* Panel 3: both actioned populations, and nothing else. */
    const acts = (d.decisions || []).map((x) => x.action).sort();
    ok('report: decisions carries exactly the blocked and suppressed leads',
       JSON.stringify(acts) === JSON.stringify(['blocked_list', 'blocked_list', 'blocked_model', 'meta_only']),
       JSON.stringify(acts));
    /* The allstate row has no verdict of its own, so it legitimately
       has no quote -- every OTHER decision must carry one. */
    ok('report: each decision with a verdict carries its evidence quote',
       (d.decisions || []).filter((x) => x.business_type).every((x) => !!x.evidence_quote),
       JSON.stringify((d.decisions || []).map((x) => [x.email, x.evidence_quote])));

    /* Panel 2: ACTED ON ONLY, and the action per industry is read from
       NON_ICP_BUSINESS_TYPES so a change to the six-industry scope
       cannot be described here as one thing and applied there as
       another.

       IT COUNTED EVERY LEAD WITH A CACHED VERDICT UNTIL 15 SEPT 2026.
       Most of the cache is a backfill of historical domains, so the
       table read "Insurance 19" in a week with five insurance blocks --
       two claims in one column, with the explanation in small print
       under the number that actually gets quoted. */
    const G = {};
    for (const g of d.industries || []) G[g.key] = g;
    const rowsOf = (k) => Object.fromEntries(((G[k] || {}).rows || []).map((r) => [r.business_type, r]));

    /* ALL THREE GROUPS ALWAYS PRESENT, empty ones included. An empty
       "Blocked by the model" is the clearest statement on the tab that
       the model has turned nobody away, and a group that vanishes when
       empty cannot make it. */
    ok('report: all three source groups are present',
       !!G.blocked_list && !!G.blocked_model && !!G.meta_only, JSON.stringify(Object.keys(G)));
    /* THE SPLIT ITSELF. The same business type lands in two different
       groups depending on what decided -- which is the whole point:
       "Real estate 2 / Blocks" hid that one was the list and one was
       the model, and in production the model column is zero. */
    ok('report: a list block and a model block of the SAME type are in different groups',
       rowsOf('blocked_list').real_estate && rowsOf('blocked_list').real_estate.leads === 1
       && rowsOf('blocked_model').real_estate && rowsOf('blocked_model').real_estate.leads === 1,
       JSON.stringify({ list: G.blocked_list.rows, model: G.blocked_model.rows }));
    ok('report: each group carries its own lead total',
       G.blocked_list.leads === 2 && G.blocked_model.leads === 1 && G.meta_only.leads === 1,
       JSON.stringify(Object.values(G).map((g) => [g.key, g.leads])));
    ok('report: home_services sits under the Meta-withheld group',
       !!rowsOf('meta_only').home_services, JSON.stringify(G.meta_only));

    /* THE ONE THAT PROVES THE RESCOPE. saas.test was judged and cleared
       -- a cached verdict, no action taken -- so it appears in no group. */
    ok('report: a judged-but-not-acted-on industry is ABSENT from every group',
       !Object.values(G).some((g) => g.rows.some((r) => r.business_type === 'software_technology')),
       JSON.stringify(Object.values(G).map((g) => g.rows.map((r) => r.business_type))));

    /* Every row is a lead something happened to, so the group totals
       sum to the three actioned ladder rows. */
    const indLeads = (d.industries || []).reduce((a, g) => a + g.leads, 0);
    ok('report: group leads sum to the acted-on ladder rows',
       indLeads === by.blocked_list + by.blocked_model + by.meta_only,
       indLeads + ' vs ' + (by.blocked_list + by.blocked_model + by.meta_only));

    /* ── THE INDUSTRY COMES FROM THE DECIDING DOMAIN, OR NOWHERE ──
       remax.test blocked Becky and has NO verdict row, so her row must
       say "not categorised". Before 15 Sept 2026 the code fell back to
       any candidate domain with a verdict, which filed three
       farmersagent.com blocks under Insurance on the strength of the
       lead's WEBSITE being farmers.com. A block under the wrong
       industry is something somebody acts on without knowing. */
    const unc = rowsOf('blocked_list')._uncategorised;
    ok('report: a block whose deciding domain has no verdict is NOT categorised',
       unc && unc.leads === 1 && unc.uncategorised === true, JSON.stringify(G.blocked_list));
    ok('report: an uncategorised row carries no confidence to read as one',
       unc && unc.median_confidence === null, JSON.stringify(unc));
    /* AND IT DOES NOT BORROW. Lead g is blocked on allstate.com (no
       verdict) while carrying saas.test as its website (a real verdict,
       software_technology). The fallback would file it under Software /
       technology; the deciding-domain rule leaves it uncategorised. */
    ok('report: a block does NOT borrow the industry of another domain on the lead',
       !Object.values(G).some((g) => g.rows.some((r) => r.business_type === 'software_technology')),
       JSON.stringify(Object.values(G).map((g) => [g.key, g.rows.map((r) => r.business_type)])));

    /* THE CACHE IS A SEPARATE CLAIM, in its own block, counted in
       companies and carrying no rate. */
    ok('report: the standing cache is reported separately from the window',
       d.cache && typeof d.cache.domains === 'number' && Array.isArray(d.cache.byType),
       JSON.stringify(d.cache && Object.keys(d.cache)));
    ok('report: the cache block carries no percentage',
       d.cache && !('unreadable_pct' in d.cache) && !JSON.stringify(d.cache).includes('_pct'),
       JSON.stringify(d.cache));

    /* Panel 4: the rate is over THIS WINDOW'S domains, not the cache.
       Five leads, five candidate domains here; four have a verdict and
       one of those four is a scrape failure. */
    const w = d.scrape.window;
    ok('report: the scrape rate is scoped to the window, not the cache',
       w && w.total === 7, JSON.stringify(w));
    ok('report: the unreadable domain is counted as unreadable',
       w && (w.thin + w.unreachable + w.other) === 1, JSON.stringify(w));
    /* NEVER-TRIED IS OUTSIDE THE DENOMINATOR. Six domains have an
       answer, one of them unreadable, so the rate is 1/6 and not 1/7.
       untouched.test is a domain the warm path has not reached; putting
       it in the denominator would make a quiet day read as a working
       scraper and a busy one as a broken scraper. */
    ok('report: never-tried is excluded from the answered denominator',
       w && w.no_verdict === 2 && w.answered === 5, JSON.stringify(w));
    /* 1 of the 5 ANSWERED, not 1 of all 7. Folding never-tried in
       would read 14.3% here -- a rate that moves when the warm path
       falls behind rather than when the scraper struggles. */
    ok('report: the rate is computed over answered domains only',
       w && w.unreadable_pct === 20, JSON.stringify(w));

    /* Panel 4: the unreadable domain arrives WITH its lead attached,
       which is the only form in which it is actionable. */
    const un = (d.scrape && d.scrape.unreadable) || [];
    ok('report: the unreadable domain is listed with its lead',
       un.length === 1 && un[0].domain === 'nosite.test' && un[0].email === 'e@nosite.test',
       JSON.stringify(un));
    ok('report: the scrape panel says it is latest-outcome, not a rate',
       (d.scrape.notes || []).some((n) => /not a historical rate/i.test(n)),
       JSON.stringify(d.scrape.notes));
    /* ── AN EMPTY GROUP STILL COMES BACK ──────────────────────────
       Driven on its own fixture, because the one above has all three
       groups populated -- so a mutation dropping empty groups is a
       no-op against it and SURVIVED the whole suite when it was tried.

       This is the property the split exists for: "Blocked by the model
       - none in this window" is how the tab states that the model has
       turned nobody away, and in production that is the true state. A
       group that disappears when empty cannot say it, and its absence
       reads as "no data" rather than "zero". */
    {
      const only = { ...S };
      S.reportLeads = [L({ session_id: 'z', email: 'z@kw.test', website: 'https://kw.test',
        non_icp_blocked: true, non_icp_source: 'domain_list', non_icp_reason: 'kw.test' })];
      const r2 = await realFetch(BASE + '/monitor/non-icp?days=7&token=stub', { signal: AbortSignal.timeout(20000) });
      const d2 = await r2.json();
      const keys2 = (d2.industries || []).map((g) => g.key);
      ok('report: all three groups return even when two are empty',
         keys2.length === 3 && keys2.includes('blocked_list')
         && keys2.includes('blocked_model') && keys2.includes('meta_only'),
         JSON.stringify(keys2));
      const empty = (d2.industries || []).filter((g) => g.leads === 0);
      ok('report: the empty groups report zero rather than being absent',
         empty.length === 2 && empty.every((g) => g.rows.length === 0),
         JSON.stringify((d2.industries || []).map((g) => [g.key, g.leads])));
      S.reportLeads = only.reportLeads;
    }

    ok('report: the flags block says what is actually switched on',
       d.flags && typeof d.flags.llm_block === 'boolean' && typeof d.flags.llm_meta === 'boolean',
       JSON.stringify(d.flags));
  }

  /* ========================================================
     THE STORED COLUMN AND THE META EVENT MUST NOT DIVERGE.

     This is the sharpest failure mode the product question introduces,
     and it is completely silent. buildEventData resolved the slug from
     page_url alone, which was correct only while product was a pure
     function of the page. The moment a checkbox can decide it, a /demo
     lead who ticks AI-CRM stores 'crm' and fires 'aeo' -- the dashboard
     says one thing, Facebook optimises for another, and nothing
     anywhere reconciles them. No error, no alert, no red row.

     So: drive /submit for real, read what was BOUND to leads.product,
     read what content_ids actually went to graph.facebook.com, and
     require them to be the same string.
     ======================================================== */
  for (const [label, needs, wantProduct, wantIds, wantLtv] of [
    ['AI-CRM only on /demo',  'crm',     'crm', ['crm'],        5000],
    /* THE ONE THAT SEPARATES THE TWO SLUGS. Routing must pick one
       calendar, so product is 'crm'. The event is honestly BOTH, so
       content_ids carries both ids -- and it must follow
       product_interest, not product, exactly as Salesforce already
       does. These agree on every other lead, which is why this row is
       the only place the invariant is visible. */
    ['both ticked on /demo',  'aeo,crm', 'crm', ['aeo', 'crm'], 15000],
    ['Lead Gen only on /demo','aeo',     'aeo', ['aeo'],        12000],
    ['nothing ticked',         '',       'aeo', ['aeo'],        12000],
  ]) {
    reset();
    const sid = '00000000-0000-4000-8000-0000000000c' + (needs.length % 9);
    await realFetch(BASE + '/submit', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: sid, email: 'x@cleanbiz.test', website: 'https://cleanbiz.test',
        company: 'Clean', first_name: 'A', last_name: 'B', sell_to: 'B2B',
        page_url: 'https://www.gushwork.ai/demo',
        product_interest: needs,
      }),
    });
    await sleep(600);

    const ins = S.writes.find((w) => /INSERT INTO leads \(/.test(w.flat));
    const stored = ins ? boundCols(ins).product : undefined;
    ok(`divergence[${label}]: stored product is ${wantProduct}`, stored === wantProduct, String(stored));

    const lead = S.metaPayloads.find((p) => (p.data || []).some((e) => e.event_name === 'Lead'));
    const ev = lead && lead.data.find((e) => e.event_name === 'Lead');
    const ids = ev && ev.custom_data && ev.custom_data.content_ids;
    ok(`divergence[${label}]: Meta fired a Lead event`, !!ev, JSON.stringify(S.metaPayloads).slice(0, 120));
    /* THE ASSERTION THAT MATTERS, and its shape changed on 15 Sept 2026
       when both-ticked became one event carrying both ids. It is no
       longer "Meta matches leads.product" -- routing picks one calendar
       and the event does not have to. It is "Meta matches what they
       TICKED", the same rule Salesforce's Product__c already follows. */
    const storedInterest = ins ? boundCols(ins).product_interest : undefined;
    ok(`divergence[${label}]: Meta content_ids match what they TICKED`,
       !!ids && JSON.stringify(ids) === JSON.stringify(wantIds),
       'ticked=' + storedInterest + ' meta=' + JSON.stringify(ids));
    ok(`divergence[${label}]: the stored interest is what drove the event`,
       (storedInterest || null) === (needs || null), String(storedInterest));
    /* predicted_ltv came from config and is PERSISTED, so a cohort can
       be reconstructed after somebody tunes the number. */
    ok(`divergence[${label}]: the event carries predicted_ltv ${wantLtv}`,
       ev && ev.custom_data && ev.custom_data.predicted_ltv === wantLtv,
       String(ev && ev.custom_data && ev.custom_data.predicted_ltv));
    const storedLtv = ins ? Number(boundCols(ins).meta_predicted_ltv) : undefined;
    ok(`divergence[${label}]: and the row records the SAME number we sent`,
       storedLtv === wantLtv, 'stored=' + storedLtv + ' sent=' + wantLtv);
  }

  /* ========================================================
     THE CAMPAIGN DECIDES THE OFFER — SAME INVARIANT, NEW INPUT
     (17 Sept 2026)

     The selector is hidden for paid traffic now, so a CRM-ad lead
     reaches /submit with NO product_interest at all and the only thing
     saying "crm" is utm_campaign. That is exactly the shape the 15 Sept
     divergence bug had: one input decides the column, a different one
     decides the event, and they agree on every lead except the ones
     that matter.

     DRIVEN END TO END for the same reason as the loop above. The unit
     tests in test-batch2 section 27 prove campaignOffer answers
     correctly; they cannot prove /submit passes it to both resolvers,
     and a route that forgot one would store crm and report aeo with
     nothing anywhere to reconcile them.

     product_interest MUST STAY NULL throughout: the ad is a guess about
     this person, not an answer they gave, and NULL there means "we never
     asked". Writing it would report an inference as a stated preference
     into Salesforce's restricted picklist.
     ======================================================== */
  for (const [label, campaign, medium, wantProduct, wantIds, wantLtv] of [
    ['CRM ad on /demo',        'FLI__Prospecting__CRM-Offer__CBO__StartTrial', 'paid', 'crm', ['crm'], 5000],
    ['AEO ad on /demo',        'FLI__Prospecting__TOF__CBO__StartTrial',       'paid', 'aeo', ['aeo'], 12000],
    /* Brand search decides nothing, so the page default still applies
       and the visitor is the one who answers -- via the selector. */
    ['brand search on /demo',  'UR_G_S_US_BR_Brand-tIS',                      'cpc',  'aeo', ['aeo'], 12000],
    /* A bare campaign id is not an AEO campaign. It must not be read as
       "no crm in this string, therefore aeo". */
    ['bare campaign id',       '120241181781830373',                          'paid', 'aeo', ['aeo'], 12000],
    /* NOT AN AD. The same CRM-shaped string on a newsletter decides
       nothing -- 29 leads over 90 days arrive on channels like this. */
    ['crm-shaped newsletter',  'crm-roundup',                                 'drip', 'aeo', ['aeo'], 12000],
  ]) {
    reset();
    const sid = '00000000-0000-4000-8000-0000000000d' + (campaign.length % 9);
    await realFetch(BASE + '/submit', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: sid, email: 'x@cleanbiz.test', website: 'https://cleanbiz.test',
        company: 'Clean', first_name: 'A', last_name: 'B', sell_to: 'B2B',
        page_url: 'https://www.gushwork.ai/demo',
        utm_campaign: campaign, utm_medium: medium,
        /* nothing ticked -- the selector was hidden for the paid rows */
      }),
    });
    await sleep(600);

    const ins = S.writes.find((w) => /INSERT INTO leads \(/.test(w.flat));
    const cols = ins ? boundCols(ins) : {};
    ok(`campaign[${label}]: stored product is ${wantProduct}`,
       cols.product === wantProduct, String(cols.product));
    ok(`campaign[${label}]: product_interest stays null -- we never asked`,
       cols.product_interest === null || cols.product_interest === undefined,
       JSON.stringify(cols.product_interest));

    const lead = S.metaPayloads.find((p) => (p.data || []).some((e) => e.event_name === 'Lead'));
    const ev = lead && lead.data.find((e) => e.event_name === 'Lead');
    const ids = ev && ev.custom_data && ev.custom_data.content_ids;
    ok(`campaign[${label}]: Meta fired a Lead event`, !!ev);
    ok(`campaign[${label}]: Meta content_ids are ${JSON.stringify(wantIds)}`,
       !!ids && JSON.stringify(ids) === JSON.stringify(wantIds), JSON.stringify(ids));
    /* THE INVARIANT. The column and the event are resolved by two
       different functions from the same inputs; this is the only place
       a route that forgot to pass one of them shows up. */
    ok(`campaign[${label}]: the column and the event agree`,
       !!ids && ids.join(',') === cols.product,
       'column=' + cols.product + ' meta=' + JSON.stringify(ids));
    ok(`campaign[${label}]: predicted_ltv is ${wantLtv}`,
       ev && ev.custom_data && ev.custom_data.predicted_ltv === wantLtv,
       String(ev && ev.custom_data && ev.custom_data.predicted_ltv));
  }

  /* ── /partial CARRIES THE CAMPAIGN TOO ────────────────────────
     MEASURED GAP, not a hypothetical: dropping the campaign from
     /partial's resolveProduct call SURVIVED the whole bar, because
     every case above drives /submit. /partial writes leads.product on
     its own insert and fires StartTrial with its own content_ids, so a
     route that forgot the campaign would store aeo for a CRM-ad lead
     for the entire time they are filling the form in -- and report that
     to Meta -- with /submit quietly correcting the column later and the
     StartTrial conversion already counted against the wrong product. */
  for (const [label, campaign, medium, wantProduct, wantIds] of [
    ['CRM ad', 'FLI__Prospecting__CRM-Offer__CBO__StartTrial', 'paid', 'crm', ['crm']],
    ['AEO ad', 'FLI__Prospecting__TOF__CBO__StartTrial',       'paid', 'aeo', ['aeo']],
    ['brand',  'UR_G_S_US_BR_Brand-tIS',                       'cpc',  'aeo', ['aeo']],
  ]) {
    reset();
    await post('/partial', {
      session_id: '00000000-0000-4000-8000-0000000000f' + (campaign.length % 9),
      email: 'buyer@cleanbiz.test', sell_to: 'B2B', step_reached: 1,
      page_url: 'https://www.gushwork.ai/demo',
      utm_campaign: campaign, utm_medium: medium,
    });
    await sleep(600);
    const ins = S.writes.find((w) => /INSERT INTO leads \(/.test(w.flat));
    const cols = ins ? boundCols(ins) : {};
    ok(`partial-campaign[${label}]: stored product is ${wantProduct}`,
       cols.product === wantProduct, String(cols.product));
    const st = S.metaPayloads.find((x) => (x.data || []).some((e) => e.event_name === 'StartTrial'));
    const ev = st && st.data.find((e) => e.event_name === 'StartTrial');
    ok(`partial-campaign[${label}]: StartTrial content_ids are ${JSON.stringify(wantIds)}`,
       !!ev && JSON.stringify(ev.custom_data.content_ids) === JSON.stringify(wantIds),
       JSON.stringify(ev && ev.custom_data && ev.custom_data.content_ids));
  }

  /* ── THE RETURN VISIT: REMEMBERED OFFER, NO ATTRIBUTION ───────
     The shape that only exists because of the 30-day cookie, and the
     one that nearly corrupted leads.utm_campaign. Somebody clicked a
     CRM ad three weeks ago and comes back today with a clean URL: they
     must see and get the CRM offer, and this visit must still be
     recorded as having NO campaign, because it had none.

     Merging the two would have re-attributed 40 real leads from organic
     to paid, and left them carrying a paid campaign beside an empty
     utm_source -- the field Source_Bucket__c actually reads, so
     Salesforce and this column would have disagreed about one lead. */
  {
    reset();
    await realFetch(BASE + '/submit', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: '00000000-0000-4000-8000-0000000000e2', email: 'x@cleanbiz.test',
        website: 'https://cleanbiz.test', company: 'Clean', first_name: 'A', last_name: 'B',
        sell_to: 'B2B', page_url: 'https://www.gushwork.ai/demo',
        offer_campaign: 'FLI__Prospecting__CRM-Offer__CBO__StartTrial', offer_medium: 'paid',
        /* no utm_* at all -- this visit came in clean */
      }),
    });
    await sleep(600);
    const ins = S.writes.find((w) => /INSERT INTO leads \(/.test(w.flat));
    const cols = ins ? boundCols(ins) : {};
    ok('return-visit: the remembered CRM ad still decides the product',
       cols.product === 'crm', String(cols.product));
    ok('return-visit: THIS visit is still attributed to no campaign',
       !cols.utm_campaign, JSON.stringify(cols.utm_campaign));
    ok('return-visit: and to no medium',
       !cols.utm_medium, JSON.stringify(cols.utm_medium));
    const lead = S.metaPayloads.find((x) => (x.data || []).some((e) => e.event_name === 'Lead'));
    const ev = lead && lead.data.find((e) => e.event_name === 'Lead');
    ok('return-visit: Meta hears crm, matching the column',
       !!ev && JSON.stringify(ev.custom_data.content_ids) === JSON.stringify(['crm']),
       JSON.stringify(ev && ev.custom_data && ev.custom_data.content_ids));
  }

  /* ── /ai-crm END TO END, ADDED 18 SEPT 2026 ───────────────────
     The page was live and routed to the CRM team by its Webflow
     attribute while being absent from PRODUCT_PATHS, so every lead from
     it was stored and reported as aeo: wrong Salesforce picklist, wrong
     Meta event, 12000 of predicted value instead of 5000. The booking
     went to the right team and the record said the wrong thing, which is
     the shape of bug this suite exists for.

     DRIVEN ACROSS JOURNEYS because the failure was journey-dependent --
     the page half-worked from a CRM ad and failed everywhere else. */
  for (const [label, campaign, medium] of [
    ['direct',        '',                                 ''],
    ['CRM ad',        'FLI__Prospecting__CRM-Offer__CBO',  'paid'],
    ['AEO ad',        'FLI__Prospecting__TOF__CBO',        'paid'],
    ['brand search',  'UR_G_S_US_BR_Brand-tIS',            'cpc'],
    ['LinkedIn post', 'gushwork',                          'social'],
  ]) {
    reset();
    await realFetch(BASE + '/submit', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: '00000000-0000-4000-8000-0000000000f' + ((campaign.length % 8) + 1),
        email: 'x@cleanbiz.test', website: 'https://cleanbiz.test', company: 'Clean',
        first_name: 'A', last_name: 'B', sell_to: 'B2C',
        page_url: 'https://www.gushwork.ai/ai-crm',
        utm_campaign: campaign, utm_medium: medium,
      }),
    });
    await sleep(600);
    const ins = S.writes.find((w) => /INSERT INTO leads \(/.test(w.flat));
    const cols = ins ? boundCols(ins) : {};
    ok(`ai-crm[${label}]: stored product is crm`, cols.product === 'crm', String(cols.product));
    /* The page never asks the question, so this stays "we never asked". */
    ok(`ai-crm[${label}]: product_interest stays null`,
       cols.product_interest === null || cols.product_interest === undefined,
       JSON.stringify(cols.product_interest));
    const lead = S.metaPayloads.find((x) => (x.data || []).some((e) => e.event_name === 'Lead'));
    const ev = lead && lead.data.find((e) => e.event_name === 'Lead');
    ok(`ai-crm[${label}]: Meta content_ids are ["crm"]`,
       !!ev && JSON.stringify(ev.custom_data.content_ids) === JSON.stringify(['crm']),
       JSON.stringify(ev && ev.custom_data && ev.custom_data.content_ids));
    ok(`ai-crm[${label}]: predicted_ltv is the CRM number, 5000`,
       ev && ev.custom_data && ev.custom_data.predicted_ltv === 5000,
       String(ev && ev.custom_data && ev.custom_data.predicted_ltv));
    /* THE SERVER BELIEVES THE BOOLEAN IT IS HANDED. The gate is
       client-side, so this asserts the server does not invent a
       disqualification of its own for a B2C answer on a CRM page. */
    ok(`ai-crm[${label}]: a B2C answer is not disqualified server-side`,
       cols.disqualified === false || cols.disqualified === 'false',
       String(cols.disqualified));
  }

  /* A TICK STILL OUTRANKS THE AD. Somebody on a CRM campaign who does
     see the selector -- they arrived before the cookie, or the markup
     is there anyway -- and ticks Lead Gen is a Lead Gen lead. The ad is
     a guess about them; the checkbox is them. */
  {
    reset();
    await realFetch(BASE + '/submit', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: '00000000-0000-4000-8000-0000000000e1', email: 'x@cleanbiz.test',
        website: 'https://cleanbiz.test', company: 'Clean', first_name: 'A', last_name: 'B',
        sell_to: 'B2B', page_url: 'https://www.gushwork.ai/demo',
        product_interest: 'aeo', utm_campaign: 'CRM-Offer', utm_medium: 'paid',
      }),
    });
    await sleep(600);
    const ins = S.writes.find((w) => /INSERT INTO leads \(/.test(w.flat));
    const cols = ins ? boundCols(ins) : {};
    ok('campaign[tick beats ad]: stored product is aeo', cols.product === 'aeo', String(cols.product));
    const lead = S.metaPayloads.find((p) => (p.data || []).some((e) => e.event_name === 'Lead'));
    const ev = lead && lead.data.find((e) => e.event_name === 'Lead');
    ok('campaign[tick beats ad]: Meta content_ids are ["aeo"]',
       !!ev && JSON.stringify(ev.custom_data.content_ids) === JSON.stringify(['aeo']),
       JSON.stringify(ev && ev.custom_data && ev.custom_data.content_ids));
  }

  /* ========================================================
     PAGES WITHOUT THE QUESTION MUST BE UNCHANGED, AND NOTHING
     MAY FIRE TWICE.

     The question exists on /demo only. Eleven other pages run the same
     build with no needs markup, and the whole promise of this release is
     that they behave exactly as they did before it. "product_interest is
     null so the resolvers fall through to the page default" is a
     reasonable thing to believe and a bad thing to assume -- it is the
     same shape as every other silent-wrong-number bug this week. So
     drive each page shape for real and read what reached
     graph.facebook.com.

     The double-fire check is here rather than in the divergence loop
     because it is a different claim: deduplication is documented as
     cross-source only, and every active ad set optimises on conversion
     COUNT, so two events for one person corrupts exactly what they bid
     on. Counting the events is the only way to see it.
     ======================================================== */
  for (const [label, pageUrl, wantIds, wantLtv] of [
    ['/start lander',            'https://www.gushwork.ai/start',      ['aeo'], 12000],
    ['/seo-leads lander',        'https://www.gushwork.ai/seo-leads',  ['aeo'], 12000],
    ['/lead-gen lander',         'https://www.gushwork.ai/lead-gen',   ['aeo'], 12000],
    /* /ai-demo is the one page whose DEFAULT is crm, from PRODUCT_PATHS.
       It has no needs question either, so it must keep firing crm. */
    ['/ai-demo',                 'https://www.gushwork.ai/ai-demo',    ['crm'],  5000],
  ]) {
    reset();
    const sid = '00000000-0000-4000-8000-0000000000d' + (label.length % 9);
    await realFetch(BASE + '/submit', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: sid, email: 'x@cleanbiz.test', website: 'https://cleanbiz.test',
        company: 'Clean', first_name: 'A', last_name: 'B', sell_to: 'B2B',
        page_url: pageUrl,
        /* no product_interest at all -- these pages never ask */
      }),
    });
    await sleep(600);

    const ins = S.writes.find((w) => /INSERT INTO leads \(/.test(w.flat));
    const cols = ins ? boundCols(ins) : {};
    /* NULL, not '' -- "we never asked" is not "they ticked nothing". */
    ok(`nopage[${label}]: product_interest is null`,
       cols.product_interest === null || cols.product_interest === undefined,
       JSON.stringify(cols.product_interest));

    const leadEvents = [];
    for (const payload of S.metaPayloads) {
      for (const e of (payload.data || [])) if (e.event_name === 'Lead') leadEvents.push(e);
    }
    ok(`nopage[${label}]: exactly ONE Lead event fired`, leadEvents.length === 1,
       'count=' + leadEvents.length);
    const ev = leadEvents[0];
    ok(`nopage[${label}]: content_ids are ${JSON.stringify(wantIds)}`,
       !!ev && JSON.stringify(ev.custom_data && ev.custom_data.content_ids) === JSON.stringify(wantIds),
       JSON.stringify(ev && ev.custom_data && ev.custom_data.content_ids));
    ok(`nopage[${label}]: predicted_ltv is ${wantLtv}`,
       !!ev && ev.custom_data && ev.custom_data.predicted_ltv === wantLtv,
       String(ev && ev.custom_data && ev.custom_data.predicted_ltv));
    ok(`nopage[${label}]: value is 0 on the upstream event`,
       !!ev && ev.custom_data && Number(ev.custom_data.value) === 0,
       String(ev && ev.custom_data && ev.custom_data.value));
  }

  /* One more shape: a /demo lead who ticks BOTH must still be ONE event,
     not one per slug. This is the case the count is actually protecting. */
  {
    reset();
    await realFetch(BASE + '/submit', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: '00000000-0000-4000-8000-0000000000e1',
        email: 'x@cleanbiz.test', website: 'https://cleanbiz.test',
        company: 'Clean', first_name: 'A', last_name: 'B', sell_to: 'B2B',
        page_url: 'https://www.gushwork.ai/demo', product_interest: 'aeo,crm',
      }),
    });
    await sleep(600);
    const leadEvents = [];
    for (const payload of S.metaPayloads) {
      for (const e of (payload.data || [])) if (e.event_name === 'Lead') leadEvents.push(e);
    }
    ok('both-ticked fires ONE event, not two', leadEvents.length === 1, 'count=' + leadEvents.length);
    ok('both-ticked carries BOTH ids on that one event',
       leadEvents[0] && JSON.stringify(leadEvents[0].custom_data.content_ids) === JSON.stringify(['aeo', 'crm']),
       JSON.stringify(leadEvents[0] && leadEvents[0].custom_data.content_ids));
  }

  /* ========================================================
     THE "META WITHHELD" FILTER ON ALL LEADS — DRIVEN, NOT READ

     Added 22 Sept 2026 for Swapnil's question in #i-gtm-ops: "how do we
     filter leads jismei CAPI fire nahi hua?"

     Every assertion here goes over HTTP and then reads the SQL the route
     actually built, because the thing that breaks is the wiring -- a
     control the loader never sends, a param the route never parses -- and
     none of that moves a source offset. A structural assertion would pass
     on a filter that silently returns every lead.
     ======================================================== */
  {
    reset();
    const whereFor = async (qs) => {
      S.writes.length = 0;
      let status = 0;
      try {
        const r = await realFetch(BASE + '/monitor/leads?token=stub&page=1' + qs,
                                  { signal: AbortSignal.timeout(20000) });
        status = r.status;
        try { await r.json(); } catch (_) {}
      } catch (err) { status = 0; }
      /* The COUNT carries the same WHERE as the page query and is the
         shortest statement to read it out of. */
      const w = S.writes.map((x) => x.flat).filter((f) => /FROM leads l WHERE true/.test(f));
      return { status, sql: w.join(' || ') };
    };

    const base = await whereFor('');
    ok('meta filter: unfiltered All Leads still answers 200', base.status === 200, String(base.status));
    ok('meta filter: no filter means no Meta predicate in the SQL',
       base.sql && !/non_icp_llm_flagged/.test(base.sql), base.sql.slice(0, 200));

    /* Each single reason reaches SQL, and reaches the RIGHT column. A
       filter that compiled but narrowed on the wrong thing would answer
       200 and show a plausible, wrong list. */
    const REASON_COLUMN = {
      blocked:      /non_icp_blocked IS TRUE/,
      model:        /non_icp_llm_flagged IS TRUE/,
      website:      /website_check_reason NOT IN/,
      disqualified: /l\.disqualified IS TRUE/,
      internal:     /SPLIT_PART/,
    };
    for (const [reason, re] of Object.entries(REASON_COLUMN)) {
      const r = await whereFor('&meta=' + reason);
      ok(`meta filter: meta=${reason} answers 200`, r.status === 200, String(r.status));
      ok(`meta filter: meta=${reason} narrows on its own column`, re.test(r.sql), r.sql.slice(0, 300));
    }

    /* "withheld" is the union and must mention EVERY reason. This is what
       catches a reason added to the push path and forgotten here: the
       union is built by mapping META_WITHHELD_REASONS, so a new reason
       appears automatically -- and if someone hand-writes the list
       instead, this fails. */
    const un = await whereFor('&meta=withheld');
    ok('meta filter: withheld answers 200', un.status === 200, String(un.status));
    for (const re of Object.values(REASON_COLUMN)) {
      ok('meta filter: withheld covers every reason', re.test(un.sql), un.sql.slice(0, 400));
    }

    /* "sent" is the NOT of exactly that union, never a second hand-kept
       list that could disagree with it. */
    const sent = await whereFor('&meta=sent');
    ok('meta filter: sent answers 200', sent.status === 200, String(sent.status));
    ok('meta filter: sent is the negation of the same union',
       /NOT \(/.test(sent.sql) && /non_icp_llm_flagged IS TRUE/.test(sent.sql), sent.sql.slice(0, 300));

    /* An unknown value must be inert rather than an error or, worse, a
       filter that silently matches nothing. */
    const junk = await whereFor('&meta=nonsense');
    ok('meta filter: an unknown value is ignored, not an error', junk.status === 200, String(junk.status));
    ok('meta filter: an unknown value adds no predicate',
       junk.sql && !/non_icp_llm_flagged/.test(junk.sql), junk.sql.slice(0, 200));

    /* THE SEARCH BOX. It matched email, company and FIRST name only, so a
       surname or a website returned "no leads" rather than "not searched".
       Driven because the columns are assembled in the route. */
    const srch = await whereFor('&search=smith');
    ok('search: answers 200', srch.status === 200, String(srch.status));
    /* THE STANDALONE CLAUSE, not merely the column name appearing
       somewhere. Dropping last_name from LEADS_SEARCH_COLUMNS left it
       visible inside the concatenated-names clause, so a bare
       includes('l.last_name') stayed true while surname search was gone
       -- found by mutating it. */
    for (const col of ['l.email', 'l.company', 'l.first_name', 'l.last_name', 'l.website']) {
      ok(`search: covers ${col} in its own right`,
         srch.sql.includes(`LOWER(COALESCE(${col},'')) LIKE`), srch.sql.slice(0, 400));
    }
    ok('search: matches the two names concatenated',
       srch.sql.includes("COALESCE(l.first_name,'') || ' ' || COALESCE(l.last_name,'')"),
       srch.sql.slice(0, 500));
    ok('search: a word does NOT trigger the phone scan',
       !/REGEXP_REPLACE/.test(srch.sql), srch.sql.slice(0, 300));

    /* PRODUCT AND META TOGETHER. Swapnil's actual question was about AEO
       demos specifically, so the two dimensions have to AND rather than
       one quietly replacing the other -- which is what a filter bar that
       rebuilds the URL per control can get wrong without any error. */
    const combo = await whereFor('&product=aeo&meta=withheld');
    ok('meta filter: product AND meta both reach the SQL',
       combo.status === 200 && /l\.product = \$/.test(combo.sql) && /non_icp_llm_flagged IS TRUE/.test(combo.sql),
       combo.sql.slice(0, 400));

    const phone = await whereFor('&search=' + encodeURIComponent('(415) 555-0134'));
    ok('search: a number DOES compare phones as digits',
       /REGEXP_REPLACE/.test(phone.sql), phone.sql.slice(0, 300));
  }

  /* ========================================================
     THE MODEL TAB'S PRODUCT FILTER — DRIVEN, AND THE LADDER
     RE-CHECKED UNDER IT

     The five ladder rows summing to the total is the one property that
     makes this tab worth reading, and a filter is exactly the change
     that can break it: narrow the population after the row cap and the
     rows sum to something that is not the printed total, with no error
     anywhere.

     So this drives the real route with a MIXED-product population and
     re-asserts the sum for each filter value, not just that the SQL
     carried a predicate.
     ======================================================== */
  {
    reset();
    const now = new Date().toISOString();
    const L = (over) => ({
      session_id: 's', email: 'x@ex.test', website: null, company: null,
      first_name: null, last_name: null, created_at: now, product: 'aeo',
      booked: false, completed: true, non_icp_blocked: false, non_icp_reason: null,
      non_icp_source: null, non_icp_llm_flagged: false, non_icp_checked_at: now, ...over,
    });
    /* Three AEO, two CRM, one untagged. Deliberately NOT one of each
       ladder state per product: the point is that the sum holds for a
       lopsided population, which is what real ones are. */
    S.reportLeads = [
      L({ session_id: 'p1', email: 'a@kw.test',    website: 'https://kw.test', product: 'aeo',
          non_icp_blocked: true, non_icp_source: 'domain_list', non_icp_reason: 'kw.test' }),
      L({ session_id: 'p2', email: 'b@realty.test', website: 'https://realty.test', product: 'aeo',
          non_icp_blocked: true, non_icp_source: 'llm', non_icp_reason: 'realty.test' }),
      L({ session_id: 'p3', email: 'c@saas.test',  website: 'https://saas.test', product: 'aeo' }),
      L({ session_id: 'p4', email: 'd@solar.test', website: 'https://solar.test', product: 'crm',
          non_icp_llm_flagged: true, non_icp_source: 'llm', non_icp_reason: 'solar.test' }),
      L({ session_id: 'p5', email: 'e@saas.test',  website: 'https://saas.test', product: 'crm' }),
      L({ session_id: 'p6', email: 'f@saas.test',  website: 'https://saas.test', product: null }),
    ];
    S.reportVerdicts = [
      { domain: 'kw.test', business_type: 'real_estate', blocking: true, confidence: 0.97,
        evidence_quote: 'We are a brokerage', reason: 'brokerage', source: 'llm',
        model_id: 'm', prompt_version: 'v1', scrape_status: 'ok', error: null,
        page_url_used: 'https://kw.test', page_text_chars: 900, checked_at: now },
      { domain: 'realty.test', business_type: 'real_estate', blocking: true, confidence: 0.96,
        evidence_quote: 'Homes for sale', reason: 'brokerage', source: 'llm',
        model_id: 'm', prompt_version: 'v1', scrape_status: 'ok', error: null,
        page_url_used: 'https://realty.test', page_text_chars: 800, checked_at: now },
      { domain: 'solar.test', business_type: 'home_services', blocking: false, confidence: 0.82,
        evidence_quote: 'Residential Solar Installation', reason: 'installer', source: 'llm',
        model_id: 'm', prompt_version: 'v1', scrape_status: 'ok', error: null,
        page_url_used: 'https://solar.test', page_text_chars: 700, checked_at: now },
      { domain: 'saas.test', business_type: 'software_technology', blocking: false, confidence: 0.95,
        evidence_quote: 'A SaaS platform', reason: 'saas', source: 'llm',
        model_id: 'm', prompt_version: 'v1', scrape_status: 'ok', error: null,
        page_url_used: 'https://saas.test', page_text_chars: 600, checked_at: now },
    ];

    const report = async (qs) => {
      S.writes.length = 0;
      const r = await realFetch(BASE + '/monitor/non-icp?days=7&token=stub' + qs,
                                { signal: AbortSignal.timeout(20000) });
      const body = await r.json();
      const sql = S.writes.map((x) => x.flat).filter((f) => /FROM leads l WHERE/.test(f)).join(' || ');
      return { status: r.status, d: body, sql };
    };
    const sums = (d) => (d.ladder.rows || []).reduce((a, r) => a + r.n, 0);

    /* Unfiltered first, so the filtered numbers have something to be
       smaller than. */
    const all = await report('');
    ok('model filter: unfiltered answers 200', all.status === 200, String(all.status));
    ok('model filter: unfiltered echoes product=all', all.d.product === 'all', String(all.d.product));
    ok('model filter: unfiltered counts every lead', all.d.ladder.total === 6, String(all.d.ladder.total));
    ok('model filter: unfiltered ladder sums to its total',
       sums(all.d) === all.d.ladder.total, sums(all.d) + ' vs ' + all.d.ladder.total);
    /* ANCHORED ON THE PREDICATE, not the column name: l.product is in the
       SELECT list of every one of these queries, so a bare /l\.product/
       is true even with no filter applied at all. */
    ok('model filter: unfiltered SQL carries NO product predicate',
       !/AND l\.product/.test(all.sql), all.sql.slice(-200));

    /* AEO. Three leads, and the two blocks are both AEO so they must
       survive the narrowing. */
    const aeo = await report('&product=aeo');
    ok('model filter: aeo answers 200', aeo.status === 200, String(aeo.status));
    ok('model filter: aeo echoes back', aeo.d.product === 'aeo', String(aeo.d.product));
    ok('model filter: aeo narrows the SQL', /l\.product = \$/.test(aeo.sql), aeo.sql.slice(0, 250));
    ok('model filter: aeo counts only its own', aeo.d.ladder.total === 3, String(aeo.d.ladder.total));
    ok('model filter: aeo ladder STILL sums to its total',
       sums(aeo.d) === aeo.d.ladder.total, sums(aeo.d) + ' vs ' + aeo.d.ladder.total);

    /* CRM. The meta_only lead is CRM, so it must be here and the two
       blocks must be gone -- a filter that narrowed the total but not
       the rows would leave blocked_list at 1 and still "sum" if the
       total were computed from the rows. */
    const crm = await report('&product=crm');
    ok('model filter: crm counts only its own', crm.d.ladder.total === 2, String(crm.d.ladder.total));
    ok('model filter: crm ladder STILL sums to its total',
       sums(crm.d) === crm.d.ladder.total, sums(crm.d) + ' vs ' + crm.d.ladder.total);
    const byKey = {};
    for (const r of crm.d.ladder.rows) byKey[r.key] = r.n;
    ok('model filter: crm keeps its own meta_only lead', byKey.meta_only === 1, JSON.stringify(byKey));
    ok('model filter: crm drops the AEO blocks',
       byKey.blocked_list === 0 && byKey.blocked_model === 0, JSON.stringify(byKey));

    /* UNTAGGED is a real population -- a page_url resolveProduct could
       not read -- not a synonym for "all". */
    const none = await report('&product=__none');
    ok('model filter: __none narrows on IS NULL', /l\.product IS NULL/.test(none.sql), none.sql.slice(0, 250));
    ok('model filter: __none counts only the untagged lead', none.d.ladder.total === 1, String(none.d.ladder.total));
    ok('model filter: __none ladder sums to its total',
       sums(none.d) === none.d.ladder.total, sums(none.d) + ' vs ' + none.d.ladder.total);

    /* An unknown value must fall back to the whole population AND SAY
       SO, because the caption is built from what came back. Silently
       returning everything under a filter the reader thinks is applied
       is the failure this echo exists to prevent. */
    const junk = await report('&product=nonsense');
    ok('model filter: an unknown product falls back to all', junk.d.product === 'all', String(junk.d.product));
    ok('model filter: an unknown product adds no predicate',
       !/AND l\.product/.test(junk.sql), junk.sql.slice(-200));
    ok('model filter: an unknown product counts everything', junk.d.ladder.total === 6, String(junk.d.ladder.total));

    /* THE OTHER PANELS NARROW WITH IT. They all derive from the same
       perLead array, so this is really asserting that nothing re-reads
       the unfiltered population behind the ladder's back. */
    ok('model filter: the decisions panel narrows too',
       (crm.d.decisions || []).every((x) => /solar\.test|saas\.test/.test(x.email || '')),
       JSON.stringify((crm.d.decisions || []).map((x) => x.email)));
  }

  loud();
  console.log('');
  console.log(`  passed: ${pass}`);
  console.log(`  failed: ${fail}`);
  if (failures.length) {
    console.log('');
    failures.forEach((f) => console.log('  ✗ ' + f));
  }
  console.log('');
  process.exit(fail === 0 ? 0 : 1);
})().catch((err) => {
  loud();
  console.error('SUITE THREW:', err);
  process.exit(1);
});
