/* ============================================================
   lead_field_changes, and what happens when writing it FAILS — EXECUTED.

   Both lead upserts are last-write-wins on 35 columns; only
   hear_about_us_raw keeps the first value. So a visitor who reaches step
   2, edits their email and submits again silently replaces who the row is
   about, and the old value is gone. On 9 Sep 2026 that produced a lead
   row reading one address while the Slack post, the Salesforce Lead and
   the Meta event had all gone out under a different one -- and nothing
   recorded that the switch had happened.

   The load-bearing property is NOT that the change row gets written. It
   is that /partial and /submit are completely unaffected when it cannot
   be. These are the two routes on the lead's critical path; a reporting
   table must never be able to cost a lead. So both are driven with the
   changes insert throwing, and compared against the same run without.

   Also checks the dashboard JS PARSES. The monitor page is built as a
   string, so `node --check index.js` says nothing at all about the
   JavaScript inside it -- a syntax error there ships green and breaks the
   tab silently, which is how /monitor/funnel stayed broken for weeks.

   Dependency-free. Run:  node tests/test-lead-field-changes.js
   ============================================================ */

require('./crash-reporter')('test-lead-field-changes');

const Module = require('module');
const path   = require('path');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
function eq(name, a, b) { ok(name, a === b, `got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`); }

const PORT = 41239;
const BASE = `http://127.0.0.1:${PORT}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const S = { queries: [], failChanges: false, upsertRow: null, submittedAt: null };
let unhandled = 0;
process.on('unhandledRejection', () => { unhandled++; });

function stubQuery(q, params) {
  const flat = (typeof q === 'string' ? q : (q && q.text) || '').replace(/\s+/g, ' ').trim();
  if (/INSERT INTO lead_field_changes/i.test(flat)) {
    S.queries.push({ kind: 'changes', flat, params });
    if (S.failChanges) throw new Error('simulated: relation lead_field_changes is unavailable');
    return { rows: [], rowCount: params ? params[4].length : 0 };
  }
  if (/WITH prev AS/i.test(flat) && /INSERT INTO leads/i.test(flat)) {
    S.queries.push({ kind: 'upsert', flat, params });
    return { rows: [S.upsertRow], rowCount: 1 };
  }
  if (/SELECT submitted_at FROM leads/i.test(flat)) return { rows: [{ submitted_at: S.submittedAt }], rowCount: 1 };
  if (/UPDATE leads SET ps_signup_sent_at = NOW/i.test(flat)) return { rows: [{ session_id: 'x' }], rowCount: 1 };
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
/* Outbound calls are captured so the Slack payload can be inspected as
   sent. The lead webhook and the ops webhook get DIFFERENT urls: a
   Salesforce push failing in this harness fires alertOps, and that must
   not be mistaken for the lead alert. */
const SLACK_LEAD = 'https://hooks.slack.com/services/LEAD';
const SLACK_OPS  = 'https://hooks.slack.com/services/OPS';
let sent = [];
global.fetch = async (url, opts) => {
  if (String(url).startsWith(BASE)) return realFetch(url, opts);
  let body = null;
  try { body = JSON.parse(opts && opts.body); } catch { /* not JSON */ }
  sent.push({ url: String(url), body });
  return { ok: true, status: 200, text: async () => 'ok', json: async () => ({}) };
};

Object.assign(process.env, {
  PORT: String(PORT), DATABASE_URL: 'postgres://stub/stub',
  ALLOWED_ORIGIN: 'https://www.gushwork.ai', MONITOR_TOKEN: 'tok',
  SLACK_WEBHOOK_URL: SLACK_LEAD, SLACK_ALERTS_WEBHOOK_URL: SLACK_OPS,
});

const realLog = console.log, realWarn = console.warn, realErr = console.error;
let logged = [];
const cap = (...a) => { logged.push(a.map(String).join(' ')); };
const quiet = () => { console.log = console.warn = console.error = cap; };
const loud  = () => { console.log = realLog; console.warn = realWarn; console.error = realErr; };

quiet();
require(path.join(__dirname, '..', 'index.js'));

/* An email switch on a lead that had ALREADY booked -- the shape worth
   knowing about. company also moves; phone and sell_to do not. */
const CHANGED = {
  prev_email: 'first@colemangroup.co', prev_company: 'Coleman Group',
  prev_website: 'https://colemangroup.co/', prev_phone: '+16127905259',
  prev_first_name: 'Joseph', prev_last_name: 'Coleman', prev_sell_to: 'B2B',
  prev_booked: true,
  email: 'second@northwind.com', company: 'Northwind',
  website: 'https://colemangroup.co/', phone: '+16127905259',
  first_name: 'Joseph', last_name: 'Coleman', sell_to: 'B2B',
  step_reached: 2,
};
const FIRST_SET = {
  prev_email: null, prev_company: null, prev_website: null, prev_phone: null,
  prev_first_name: null, prev_last_name: null, prev_sell_to: null, prev_booked: false,
  email: 'new@northwind.com', company: 'Northwind', website: 'https://northwind.com/',
  phone: '+1555', first_name: 'Sam', last_name: 'Tester', sell_to: 'B2B', step_reached: 1,
};

const BODY = {
  session_id: 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',
  email: 'second@northwind.com', website: 'northwind.com', sell_to: 'B2B',
  first_name: 'Joseph', last_name: 'Coleman', phone: '+16127905259', company: 'Northwind',
  page_url: 'https://www.gushwork.ai/demo', website_check_reason: 'resolved',
};

async function drive(route, upsertRow, failChanges, submittedAt) {
  S.queries = []; S.upsertRow = upsertRow; S.failChanges = failChanges; logged = []; sent = [];
  S.submittedAt = submittedAt === undefined ? null : submittedAt;
  const res = await realFetch(BASE + route, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Origin: 'https://www.gushwork.ai', 'User-Agent': 'suite/1.0' },
    body: JSON.stringify(Object.assign({ step: 1 }, BODY)),
  });
  let body = null;
  try { body = await res.json(); } catch { /* non-JSON */ }
  await sleep(700);
  return { status: res.status, body, queries: S.queries.slice(), logged: logged.slice(), sent: sent.slice() };
}

const NOT_BOOKED = Object.assign({}, CHANGED, { prev_booked: false });
const NO_CHANGE  = Object.assign({}, CHANGED, { prev_email: CHANGED.email, prev_company: CHANGED.company });

(async () => {
  await sleep(900);

  for (const route of ['/partial', '/submit']) {
    /* ── happy path: the change is recorded ─────────────────── */
    quiet(); const good = await drive(route, CHANGED, false); loud();
    const ch = good.queries.find((q) => q.kind === 'changes');
    ok(`${route}: a change row was written`, !!ch);
    if (ch) {
      const [sid, src, step, booked, arrived, prevStep, backNav, fields, olds, news, attrs, fsteps] = ch.params;
      eq(`${route}: session_id`, sid, BODY.session_id);
      eq(`${route}: source_route names the route`, src, route);
      eq(`${route}: booking_uid_present read from BEFORE the upsert`, booked, true);
      eq(`${route}: only the fields that actually moved`, fields.join(','), 'email,company');
      eq(`${route}: old values`, olds.join(','), 'first@colemangroup.co,Coleman Group');
      eq(`${route}: new values`, news.join(','), 'second@northwind.com,Northwind');
      ok(`${route}: unchanged fields are absent`, !fields.includes('phone') && !fields.includes('sell_to'), fields.join(','));
      eq(`${route}: step_reached recorded`, step, 2);
      /* WHERE it happened. arrived_step is the step of THIS write and
         must NOT track step_reached, which comes back from the upsert's
         GREATEST() and reads 2 for a step-1 /partial. That collapse is
         what hid our own enrichment writing a step-2 field. */
      eq(`${route}: arrived_step is the step of this write, not the row high-water mark`,
         arrived, route === '/partial' ? 1 : 2);
      eq(`${route}: field_step says where each field lives in the form`, fsteps.join(','), '1,2');
      eq(`${route}: prev_step is null when the row had no step yet`, prevStep, null);
      eq(`${route}: back_navigation is false without a prev_step to compare`, backNav, false);
      /* email is a step-1 field, so a step-1 write CAN be the person.
         company is a step-2 field, so the same write cannot be. */
      eq(`${route}: attribution per field`, attrs.join(','),
         route === '/partial' ? 'prospect_edit,ours_earlier_step' : 'prospect_edit,prospect_edit');
    }

    /* ── a first set is not a change ────────────────────────── */
    quiet(); const first = await drive(route, FIRST_SET, false); loud();
    ok(`${route}: a first set writes nothing`, !first.queries.some((q) => q.kind === 'changes'),
       JSON.stringify((first.queries.find((q) => q.kind === 'changes') || {}).params));

    /* ── THE POINT: the write fails, the route does not ─────── */
    const before = unhandled;
    quiet(); const bad = await drive(route, CHANGED, true); loud();
    eq(`${route}: status identical when the change write throws`, bad.status, good.status);
    eq(`${route}: body identical when the change write throws`,
       JSON.stringify(bad.body), JSON.stringify(good.body));
    eq(`${route}: no unhandled rejection escaped`, unhandled, before);
    ok(`${route}: the lead upsert still ran`, bad.queries.some((q) => q.kind === 'upsert'));
    const gU = good.queries.find((q) => q.kind === 'upsert');
    const bU = bad.queries.find((q) => q.kind === 'upsert');
    eq(`${route}: the lead upsert received identical params`,
       JSON.stringify(bU && bU.params), JSON.stringify(gU && gU.params));
    ok(`${route}: the failure is logged, not silent`,
       bad.logged.some((l) => /lead-changes.*not recorded/.test(l)), bad.logged.join(' | ').slice(0, 200));
  }

  /* ── the Slack line: only after a booking ───────────────────────
     slackSubmit sits inside `if (!alreadySubmitted)`, and the gate query
     is stubbed to submitted_at = null, so the alert fires here. That is
     the reachable shape: a lead that BOOKED BEFORE EVER SUBMITTING, then
     came back and submitted under different details. */
  /* Both messages go to the lead webhook, so they are told apart by their
     header rather than by destination. */
  const pick = (r, re) => {
    const hit = r.sent.filter((c) => c.url === SLACK_LEAD).find((c) => re.test(JSON.stringify(c.body)));
    return hit ? JSON.stringify(hit.body) : '';
  };
  const leadAlert = (r) => pick(r, /Lead Form Completed/);
  const followUp  = (r) => pick(r, /follow-up, not a new lead/);
  const ALREADY   = new Date('2026-09-08T19:31:30Z');

  quiet(); const booked = await drive('/submit', CHANGED, false); loud();
  const bookedTxt = leadAlert(booked);
  ok('slack: the lead alert was posted at all', bookedTxt.length > 0,
     booked.sent.map((c) => c.url).join(', ') || '(nothing sent)');
  ok('slack: the changed-after-booking block is present', /Changed after booking/.test(bookedTxt));
  ok('slack: it shows the old value', /first@colemangroup\.co/.test(bookedTxt));
  ok('slack: it shows the new value', /second@northwind\.com/.test(bookedTxt));
  ok('slack: company change is included too', /Coleman Group/.test(bookedTxt) && /Northwind/.test(bookedTxt));
  ok('slack: fields are labelled for an SDR, not by column name',
     /Email:/.test(bookedTxt) && !/"email: /.test(bookedTxt));
  ok('slack: it says why the SDR should care',
     /already had a call booked/.test(bookedTxt) && /old details/.test(bookedTxt));
  ok('slack: unchanged fields are not listed as changed',
     !/Phone: /.test(bookedTxt) && !/Sells to: /.test(bookedTxt));
  ok('slack: it is ONE message, not two',
     booked.sent.filter((c) => c.url === SLACK_LEAD).length === 1,
     String(booked.sent.filter((c) => c.url === SLACK_LEAD).length));

  /* Same changes, but they had NOT booked -- an ordinary correction. */
  quiet(); const unbooked = await drive('/submit', NOT_BOOKED, false); loud();
  const unbookedTxt = leadAlert(unbooked);
  ok('slack: the alert still posts for an unbooked lead', unbookedTxt.length > 0);
  ok('slack: but carries NO changed-after-booking block',
     !/Changed after booking/.test(unbookedTxt), unbookedTxt.slice(0, 200));

  /* Booked, but nothing actually changed. */

  quiet(); const nochange = await drive('/submit', NO_CHANGE, false); loud();
  const nochangeTxt = leadAlert(nochange);
  ok('slack: alert posts when a booked lead changed nothing', nochangeTxt.length > 0);
  ok('slack: and carries no block when nothing changed',
     !/Changed after booking/.test(nochangeTxt), nochangeTxt.slice(0, 200));

  /* /partial never posts a lead alert at all, so it cannot double up. */
  quiet(); const partialRun = await drive('/partial', CHANGED, false); loud();
  ok('slack: /partial posts no lead alert', leadAlert(partialRun) === '',
     leadAlert(partialRun).slice(0, 120));

  /* ── the standalone follow-up ────────────────────────────────────
     submitted_at is SET, so the lead alert is correctly deduped and the
     appended line above cannot reach this lead at all. This is the order
     with real consequences: submitted, booked, then changed who they are
     and submitted again. */
  quiet(); const fu = await drive('/submit', CHANGED, false, ALREADY); loud();
  const fuTxt = followUp(fu);
  ok('followup: a standalone message was posted', fuTxt.length > 0,
     fu.sent.map((c) => c.url).join(', ') || '(nothing sent)');
  ok('followup: the ordinary lead alert did NOT fire', leadAlert(fu) === '');
  ok('followup: header marks it as a follow-up, not a new lead',
     /Details changed after booking/.test(fuTxt) && /follow-up, not a new lead/.test(fuTxt));
  ok('followup: no green tick anywhere, so it cannot be skimmed as a new lead',
     !/\u2705/.test(fuTxt) && !/Lead Form Completed/.test(fuTxt), fuTxt.slice(0, 160));
  ok('followup: says in words that they already submitted and already booked',
     /already filled in the form and already has a call booked/.test(fuTxt));
  ok('followup: warns that earlier messages used the old values', /used the OLD values/.test(fuTxt));
  ok('followup: shows old and new', /first@colemangroup\.co/.test(fuTxt) && /second@northwind\.com/.test(fuTxt));
  ok('followup: labelled for an SDR', /Email:/.test(fuTxt));
  ok('followup: carries the session id so the lead can be found',
     /aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee/.test(fuTxt));

  /* NOT alertOps: no severity footer, and nothing on the ops webhook. */
  ok('followup: does not carry an alertOps severity footer', !/Severity:/.test(fuTxt), fuTxt.slice(0, 200));
  ok('followup: was not routed to the ops webhook',
     !fu.sent.some((c) => c.url === SLACK_OPS && /follow-up, not a new lead/.test(JSON.stringify(c.body))));

  /* THE COOLDOWN REASON, proved rather than asserted: two changes in a
     row both post. Through alertOps the second would be swallowed for an
     hour and folded into an "Also occurred" count on a later alert. */
  quiet(); const fu2 = await drive('/submit', CHANGED, false, ALREADY); loud();
  eq('followup: first change posts exactly one message',
     fu.sent.filter((c) => c.url === SLACK_LEAD).length, 1);
  eq('followup: an immediate second change posts too — no cooldown',
     fu2.sent.filter((c) => c.url === SLACK_LEAD).length, 1);

  /* Booked, already submitted, but nothing changed. */
  quiet(); const fuNo = await drive('/submit', NO_CHANGE, false, ALREADY); loud();
  ok('followup: silent when nothing changed', followUp(fuNo) === '');

  /* Already submitted and changed, but never booked -- an ordinary edit. */
  quiet(); const fuUnbooked = await drive('/submit', NOT_BOOKED, false, ALREADY); loud();
  ok('followup: silent when they had not booked', followUp(fuUnbooked) === '');

  /* ── THE FALSE POSITIVE, driven end to end ──────────────────────
     10 Sep 2026. www.datapartnerinc.com -> https://www.datapartnerinc.com/
     posted a follow-up for a booked lead who had touched nothing:
     /partial had stored Apollo's website_url with the scheme stripped by
     applyEnrichment, /submit stored the website check's canonical_url
     with the scheme and a root slash back on. Two of our own
     normalisations, one round trip apart.

     Driven rather than asserted on source, because that is the only way
     to show the fold is REACHED. An ordering or regex assertion here
     survives an early return above the diff. */
  const OURS_NORMALISED = Object.assign({}, CHANGED, {
    prev_email: CHANGED.email, prev_company: CHANGED.company,
    prev_website: 'www.datapartnerinc.com', website: 'https://www.datapartnerinc.com/',
  });
  quiet(); const norm = await drive('/submit', OURS_NORMALISED, false, ALREADY); loud();
  ok('fold: scheme + trailing slash writes NO change row at all',
     !norm.queries.some((q) => q.kind === 'changes'),
     JSON.stringify((norm.queries.find((q) => q.kind === 'changes') || {}).params));
  ok('fold: and posts no follow-up', followUp(norm) === '', followUp(norm).slice(0, 160));

  /* Same visitor, a genuinely different domain. The fold must not have
     turned the website field off -- this is the mutation that would make
     the test above pass for the wrong reason. */
  const REAL_WEBSITE = Object.assign({}, CHANGED, {
    prev_email: CHANGED.email, prev_company: CHANGED.company,
    prev_website: 'www.datapartnerinc.com', website: 'https://othercorp.io/',
  });
  quiet(); const realw = await drive('/submit', REAL_WEBSITE, false, ALREADY); loud();
  const realCh = realw.queries.find((q) => q.kind === 'changes');
  ok('fold: a real domain change on the SAME field still records', !!realCh);
  if (realCh) eq('fold: recorded as the prospect editing it', realCh.params[10].join(','), 'prospect_edit');
  ok('fold: and still posts the follow-up', followUp(realw).length > 0);

  /* A path change is not formatting either. */
  const PATH_CHANGE = Object.assign({}, CHANGED, {
    prev_email: CHANGED.email, prev_company: CHANGED.company,
    prev_website: 'acme.com', website: 'https://acme.com/uk',
  });
  quiet(); const pathw = await drive('/submit', PATH_CHANGE, false, ALREADY); loud();
  ok('fold: only a BARE trailing slash is stripped, a path survives',
     pathw.queries.some((q) => q.kind === 'changes'));

  /* ── attribution suppresses the rest of our own writes ──────────
     prev_step 1 means the old website was written before the visitor
     could see the field: them filling the form in, not changing an
     answer. Recorded, labelled, and NOT alerted. */
  const OVER_GUESS = Object.assign({}, CHANGED, {
    prev_email: CHANGED.email, prev_company: CHANGED.company,
    prev_website: 'apollos-guess.com', website: 'https://whattheytyped.com/',
    prev_step_reached: 1,
  });
  quiet(); const guess = await drive('/submit', OVER_GUESS, false, ALREADY); loud();
  const guessCh = guess.queries.find((q) => q.kind === 'changes');
  ok('attribution: typing over an Apollo guess is still RECORDED', !!guessCh);
  if (guessCh) {
    eq('attribution: labelled as ours, not a prospect edit', guessCh.params[10].join(','), 'ours_replacing_guess');
    eq('attribution: prev_step recorded', guessCh.params[5], 1);
  }
  ok('attribution: and posts no follow-up', followUp(guess) === '', followUp(guess).slice(0, 160));

  /* A subdomain move is the exact shape domainsMatch accepts before
     storing a canonical_url, so it is honestly labelled uncertain -- and
     it STILL alerts, marked, because dropping a possible real edit
     silently is the worse failure. */
  const NESTED = Object.assign({}, CHANGED, {
    prev_email: CHANGED.email, prev_company: CHANGED.company,
    prev_website: 'acme.com', website: 'https://shop.acme.com/', prev_step_reached: 2,
  });
  quiet(); const nested = await drive('/submit', NESTED, false, ALREADY); loud();
  const nestedCh = nested.queries.find((q) => q.kind === 'changes');
  if (nestedCh) eq('attribution: a subdomain move is labelled uncertain', nestedCh.params[10].join(','), 'maybe_our_canonical');
  ok('attribution: an uncertain change STILL posts, rather than being dropped', followUp(nested).length > 0);
  ok('attribution: and says on the line that it might be our own check',
     /own check resolved it/.test(followUp(nested)), followUp(nested).slice(0, 300));

  /* ── back navigation ────────────────────────────────────────────
     A step-1 /partial landing on a row already at step 2. Derivable
     only because prev_step is read from BEFORE the upsert; step_reached
     comes back through GREATEST() and reads 2 either way. */
  const CAME_BACK = Object.assign({}, CHANGED, { prev_step_reached: 2 });
  quiet(); const back = await drive('/partial', CAME_BACK, false); loud();
  const backCh = back.queries.find((q) => q.kind === 'changes');
  ok('back-nav: a step-1 write after step 2 is flagged', !!backCh && backCh.params[6] === true,
     backCh && String(backCh.params[6]));
  ok('back-nav: and the log line says so in words',
     back.logged.some((l) => /came back from a later step/.test(l)),
     back.logged.filter((l) => /lead-changes/.test(l)).join(' | ').slice(0, 200));
  quiet(); const fwd = await drive('/submit', Object.assign({}, CHANGED, { prev_step_reached: 2 }), false); loud();
  const fwdCh = fwd.queries.find((q) => q.kind === 'changes');
  ok('back-nav: a step-2 write on a step-2 row is NOT flagged',
     !!fwdCh && fwdCh.params[6] === false, fwdCh && String(fwdCh.params[6]));

  /* MUTUALLY EXCLUSIVE. The earlier run with submitted_at null produced
     the appended line; it must not also produce a standalone message. */
  ok('followup: the appended-line case posts no standalone message', followUp(booked) === '');
  ok('followup: the standalone case carries no appended lead alert', leadAlert(fu) === '');

  /* /partial never posts either message. */
  quiet(); const fuPartial = await drive('/partial', CHANGED, false, ALREADY); loud();
  ok('followup: /partial posts nothing', followUp(fuPartial) === '' && leadAlert(fuPartial) === '');

  /* ── the endpoint ───────────────────────────────────────────── */
  quiet();
  const noTok = await realFetch(`${BASE}/monitor/lead-changes?session_id=x`);
  const noSid = await realFetch(`${BASE}/monitor/lead-changes?token=tok`);
  const okRes = await realFetch(`${BASE}/monitor/lead-changes?token=tok&session_id=x`);
  loud();
  eq('endpoint: 401 without the token', noTok.status, 401);
  eq('endpoint: 400 without a session_id', noSid.status, 400);
  eq('endpoint: 200 with both', okRes.status, 200);

  /* ── the dashboard JS must PARSE ────────────────────────────── */
  quiet();
  const page = await (await realFetch(`${BASE}/monitor?token=tok`)).text();
  loud();
  const scripts = page.match(/<script>([\s\S]*?)<\/script>/g) || [];
  ok('dashboard: served an inline script', scripts.length > 0);
  let parsed = true, why = '';
  for (const block of scripts) {
    const code = block.replace(/^<script>/, '').replace(/<\/script>$/, '');
    try { new Function(code); } catch (err) { parsed = false; why = err.message; break; }
  }
  ok('dashboard: every inline script parses as JavaScript', parsed, why);
  ok('dashboard: loadChanges is defined in the page', /function loadChanges\(/.test(page));
  ok('dashboard: toggleRow calls it on expand', /if\(!vis\)loadChanges\(sid\)/.test(page));
  ok('dashboard: an unreadable log renders as unavailable, never as no changes',
     /Change log unavailable/.test(page) && /not the same as no changes/.test(page));

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
