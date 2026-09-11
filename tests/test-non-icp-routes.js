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
const S = { fetches: [], slackPayloads: [], leadRow: null, writes: [], psBlocked: false };

/* ── stub pg ──────────────────────────────────────────────────────
   Both pools (Railway and the AWS warehouse) come through here.
   The customer-domain query returns NOTHING, so no domain is ever a
   known customer and the block is allowed to fire. The known-customer
   bypass gets its own coverage in test-non-icp.js section 7, where the
   set can be controlled directly. */
function stubQuery(q, params) {
  const flat = (typeof q === 'string' ? q : (q && q.text) || '').replace(/\s+/g, ' ').trim();
  S.writes.push({ flat, params });

  /* The two leads upserts. Echo back what was bound for the last two
     placeholders -- they are non_icp_blocked and non_icp_reason, appended in
     that order at both call sites -- so the route reads the same effective
     block a real Postgres would have returned after the sticky OR. */
  if (/INSERT INTO leads \(/.test(flat)) {
    const p = params || [];
    return { rows: [{
      non_icp_blocked: p[p.length - 2] === true,
      non_icp_reason:  p[p.length - 1] || null,
      prev_booked: false, step_reached: 2,
    }], rowCount: 1 };
  }
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
const reset = () => { S.fetches = []; S.slackPayloads = []; S.writes = []; S.leadRow = null; S.psBlocked = false; };
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
    ok('partial: stamped non_icp_blocked = true',
       !!ins && ins.params[ins.params.length - 2] === true, ins && String(ins.params[ins.params.length - 2]));
    ok('partial: stamped the matched domain',
       !!ins && ins.params[ins.params.length - 1] === 'kw.com', ins && String(ins.params[ins.params.length - 1]));
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
