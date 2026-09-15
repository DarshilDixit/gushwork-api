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
const S = { fetches: [], slackPayloads: [], leadRow: null, writes: [], psBlocked: false, verdict: null };

/* ── stub pg ──────────────────────────────────────────────────────
   Both pools (Railway and the AWS warehouse) come through here.
   The customer-domain query returns NOTHING, so no domain is ever a
   known customer and the block is allowed to fire. The known-customer
   bypass gets its own coverage in test-non-icp.js section 7, where the
   set can be controlled directly. */
function stubQuery(q, params) {
  const flat = (typeof q === 'string' ? q : (q && q.text) || '').replace(/\s+/g, ' ').trim();
  S.writes.push({ flat, params });

  /* The two leads upserts. Echo back what was bound for the FIVE non-ICP
     placeholders, which are appended in this order at both call sites:

        non_icp_blocked, non_icp_reason, non_icp_source,
        non_icp_checked_at, non_icp_llm_flagged

     so the route reads the same effective block a real Postgres would have
     returned after the sticky OR.

     COUNTED FROM THE END, AND THAT IS A KNOWN FRAGILITY. It broke once
     already, when the V2 columns were appended and this stub silently
     started reading non_icp_checked_at as non_icp_blocked -- a Date, which
     is truthy, so it did not even fail the way you would expect. The names
     are asserted below rather than only the offsets, so the next append
     fails loudly here instead of somewhere downstream. */
  if (/INSERT INTO leads \(/.test(flat)) {
    const p = params || [];
    const cols = (flat.match(/INSERT INTO leads \(([^)]*)\)/) || [])[1] || '';
    const tail = cols.split(',').slice(-5).map(s => s.trim());
    if (tail.join(',') !== 'non_icp_blocked,non_icp_reason,non_icp_source,non_icp_checked_at,non_icp_llm_flagged') {
      throw new Error('leads upsert column tail moved — this stub decodes by position: ' + tail.join(','));
    }
    return { rows: [{
      non_icp_blocked:     p[p.length - 5] === true,
      non_icp_reason:      p[p.length - 4] || null,
      non_icp_source:      p[p.length - 3] || null,
      non_icp_checked_at:  p[p.length - 2] || null,
      non_icp_llm_flagged: p[p.length - 1] === true,
      prev_booked: false, step_reached: 2,
    }], rowCount: 1 };
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
  if (/graph\.facebook\.com/.test(u)) return j({ events_received: 1 });
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
const reset = () => { S.fetches = []; S.slackPayloads = []; S.writes = []; S.leadRow = null; S.psBlocked = false; S.verdict = null; };
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
    /* The five non-ICP placeholders, counted off the end in the order the
       column tail declares them. See the decoding note in stubQuery. */
    const np = ins ? ins.params.slice(-5) : [];
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
        json: async () => ({
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
          industries: [{ business_type: 'home_services', label: 'Home services / trades',
                         action: 'meta', leads: 2, domains: 2, median_confidence: 0.84 }],
          decisions: [{ session_id: 's1', created_at: new Date().toISOString(),
                        email: 'a@kw.com', website: 'https://kw.com', company: 'KW',
                        booked: true, product: 'aeo', action: 'blocked_list',
                        source: 'domain_list', domain_judged: 'kw.com',
                        business_type: 'real_estate', business_type_label: 'Real estate',
                        confidence: 0.97, evidence_quote: 'We are a brokerage',
                        reason: 'brokerage', model_id: 'm', prompt_version: 'v1-test',
                        page_url_used: 'https://kw.com', page_text_chars: 900,
                        checked_at: new Date().toISOString() }],
          scrape: {
            last24h:  { ok: 9, unreachable: 1, thin: 0, other: 0, total: 10, unreadable_pct: 10 },
            standing: { ok: 90, unreachable: 5, thin: 5, other: 0, total: 100, unreadable_pct: 10 },
            unreadable: [{ domain: 'x.test', scrape_status: 'thin', error: null,
                           checked_at: new Date().toISOString(), email: 'a@x.test',
                           website: 'https://x.test', blocked: false, blocked_by: null }],
            inProcess: { since: Date.now(), ok: 3, errored: 0, unreachable: 1,
                         writeFailed: 0, bypassFailed: 0, cacheHits: 5, cacheMisses: 3,
                         cacheHitPct: 62.5, avgWarmMs: 2793, maxWarmMs: 6671,
                         lastOkAt: Date.now(), lastErrorAt: null, lastError: null },
            notes: ['Latest outcome per domain, not a historical rate.'],
          },
        }),
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
           + ' et: typeof et === "function" ? et : null,'
           + ' enrichPanel: typeof enrichPanel === "function" ? enrichPanel : null,'
           + ' stageBadge: typeof stageBadge === "function" ? stageBadge : null,'
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
    ok('dashboard: the inline script evaluates without throwing', !evalErr, evalErr && evalErr.message);

    if (scope) {
      /* THE SCOPE CHECK. leadRowsHtml was declared INSIDE loadLeads, so it
         was reachable from All Leads and undefined from Blocked. A name
         that is not visible at top level cannot be shared between tabs. */
      for (const nm of ['leadRowsHtml', 'esc', 'et', 'enrichPanel', 'stageBadge',
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
      for (const [id, html] of painted_) {
        /* The Model tab paints into two plain divs (mdl-ladder, mdl-scrape)
           as well as three table bodies, and a div is where its ladder and
           its scrape summary live -- the two panels most likely to carry a
           scope error. An id filter that only knew about tbodies would
           watch the exact containers this tab renders into and assert
           nothing about them. */
        if (!/tbody|-tbody$|^mdl-/.test(id)) continue;
        ok(`dashboard: ${id} rendered content, not an error`,
           !/Could not load|Failed:|is not defined|is not a function/i.test(html),
           id + ' -> ' + String(html).slice(0, 140));
      }
      ok('dashboard: at least one table actually painted',
         painted_.some(([id, h]) => /tbody/.test(id) && h && h.length > 20),
         Object.keys(painted).join(','));
      /* leadRowsHtml itself, on a real row shape. */
      let rowsErr = null, rowsHtml = '';
      try {
        rowsHtml = scope.leadRowsHtml([{ session_id: 's1', email: 'a@kw.com', first_name: 'A',
          last_name: 'B', company: 'KW', sell_to: 'B2B', product: 'aeo',
          created_at: new Date().toISOString(), non_icp_blocked: true, non_icp_reason: 'kw.com' }]);
      } catch (err) { rowsErr = err; }
      ok('dashboard: leadRowsHtml renders a row', !rowsErr && rowsHtml.includes('<tr'), rowsErr && rowsErr.message);
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
    const np = ins ? ins.params.slice(-5) : [];
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
    const np = ins ? ins.params.slice(-5) : [];
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
