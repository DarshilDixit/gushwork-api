/* ============================================================
   fire-non-icp-slack.js — fire the non-ICP Slack messages for real.

   CLAUDE.md: "Every alert path gets FIRED ONCE ON PURPOSE before
   launch." The non-ICP block adds two message paths and neither had
   ever executed when it was written:

     blocked   slackNonIcpBlocked(), the lead-channel post that is the
               ONLY place a wrong block becomes visible to a human.
     booking   the critical alertOps() from rejectBookingIfNonIcp(),
               fired when a blocked lead somehow holds a real slot.

   IT SENDS FOR REAL. `blocked` goes to SLACK_WEBHOOK_URL (the leads
   channel). `booking` is a critical, so it goes to the alerts channel
   AND emails ALERT_EMAIL_TO. No dry-run flag, deliberately: a dry run
   proves the thing that was already being proved and already wrong.

   It LIFTS the real functions out of index.js rather than
   reimplementing them, exactly like tools/fire-alert.js, so what
   arrives is what production sends. A second copy of the message here
   would be a second source of truth that can drift.

   Every payload names itself a DELIBERATE TEST in a field a human
   reads, so nobody triages it as a real blocked prospect.

   Run:  node tools/fire-non-icp-slack.js blocked
         node tools/fire-non-icp-slack.js llm-blocked
         node tools/fire-non-icp-slack.js llm-meta
         node tools/fire-non-icp-slack.js late-block
         node tools/fire-non-icp-slack.js booking
         node tools/fire-non-icp-slack.js dropoff-digest

   dropoff-digest is the ONLY path here that needs a DATABASE. It runs
   the real weekly digest over real leads, so it needs DATABASE_URL --
   which on a developer machine means the Postgres service's
   DATABASE_PUBLIC_URL, because gushwork-api's own DATABASE_URL is on
   Railway's private network.

   Needs SLACK_WEBHOOK_URL in the environment; `booking` also needs the
   alerts webhook and the Gmail vars. From a developer machine:
     railway run node tools/fire-non-icp-slack.js blocked

   Not mounted anywhere and not called by anything, like
   tools/fire-alert.js and backfill-sf.js.
   ============================================================ */

const fs = require('fs');
const path = require('path');
const ROOT = path.join(__dirname, '..');
const src = fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8');
/* THE REAL POOL, not a second one built from the same URL. Requiring db.js
   creates the Pool but connects nothing and does not run initDB, so this
   costs nothing on the five fires that never touch a database. */
const { pool } = require(path.join(ROOT, 'db'));

/* Same lifter as tools/fire-alert.js. A `const X = new Thing({...})` continues
   past its matched brace with `);` — brace matching alone truncates it and
   surfaces as "missing ) after argument list". */
function liftDecl(decl) {
  const i = src.indexOf('\n' + decl);
  if (i === -1) throw new Error('not found in index.js: ' + decl);
  let j = src.indexOf('{', i);
  const semi = src.indexOf(';', i);
  if (j === -1 || (semi !== -1 && semi < j)) return src.slice(i + 1, semi + 1);
  let d = 0;
  for (let k = j; k < src.length; k++) {
    if (src[k] === '{') d++;
    else if (src[k] === '}') { d--; if (!d) { j = k; break; } }
  }
  if (decl.startsWith('const') || decl.startsWith('let')) {
    const end = src.indexOf(';', j);
    if (end !== -1 && /^[)\]\s]*$/.test(src.slice(j + 1, end))) j = end;
  }
  return src.slice(i + 1, j + 1);
}

/* liftDecl takes ONE declaration and brace-matches it, which truncates an
   array of objects at its first element -- DROPOFF_STAGES came out
   unterminated and the whole lift failed to parse. For a contiguous region
   of related declarations, slice between two markers instead. Same helper
   tools/non-icp-validate.js uses. */
function between(a, b) {
  const i = src.indexOf(a);
  if (i === -1) throw new Error('marker not found in index.js: ' + a);
  const j = src.indexOf(b, i);
  if (j === -1) throw new Error('end marker not found in index.js: ' + b);
  return src.slice(i, j);
}

const lifted = new Function('require', 'pool', [
  `const nodemailer = require('${path.join(ROOT, 'node_modules', 'nodemailer')}');`,
  liftDecl('const DASH_TZ'),
  liftDecl('const ALERT_COOLDOWN_MS'),
  liftDecl('const ALERT_EMAIL_TO'),
  liftDecl('const _alertLastSent'),
  liftDecl('const _alertSuppressed '),
  liftDecl('const _alertSuppressedIds'),
  liftDecl('const _etStampFmt'),
  liftDecl('function etStamp'),
  liftDecl('function bHeader'),
  liftDecl('function bSection'),
  liftDecl('function bDivider'),
  liftDecl('function bFields'),
  liftDecl('function bContext'),
  liftDecl('function sendOpsSlack'),
  liftDecl('function alertSlackBroken'),
  liftDecl('function sendSlack'),
  liftDecl('let _gmailTransport'),
  liftDecl('function getGmailTransport'),
  liftDecl('async function sendAlertEmail'),
  liftDecl('function alertOps'),
  liftDecl('const SLACK_ABOUT_MAX'),
  liftDecl('function slackTruncate'),
  liftDecl('function slackNonIcpBlocked'),
  liftDecl('function slackNonIcpLlmFlagged'),
  liftDecl('function slackNonIcpLateBlock'),
  liftDecl('const NON_ICP_BUSINESS_TYPES'),
  /* ── the weekly dropoff digest, and what it stands on ──────────────
     Lifted rather than reimplemented, like everything else here: the
     whole point is that what lands in Slack is what production sends,
     over the real leads table. */
  /* isInternalLead and internalLeadSqlClause, which the report stamps with. */
  between('const ELV_EXCLUDED_DOMAINS', 'function pruneElvWindow'),
  /* the whole dropoff block: the ladder, the source resolver, the date
     helpers and dropoffReport itself. */
  between('const DROPOFF_STAGE_SQL', 'async function visitorsReport'),
  /* etParts, shared with the near-miss digest. */
  between('function etParts(', 'async function runNearMissDigest'),
  /* the digest, minus its interval starter. */
  between('const DROPOFF_DIGEST_HOUR_ET', 'function startDropoffDigest'),
  /* setOpsSlack returns the ORIGINAL so the caller can wrap rather than
     replace. A function declaration is a mutable binding, which is the
     same property tools/non-icp-validate.js uses to replay a scrape. */
  'return { alertOps, slackNonIcpBlocked, slackNonIcpLlmFlagged, slackNonIcpLateBlock, sendOpsSlack, bHeader, bDivider, bSection,'
  + ' runDropoffDigest, dropoffReport,'
  + ' setOpsSlack(f) { const prev = sendOpsSlack; sendOpsSlack = f; return prev; } };',
].join('\n'))(require, pool);

const TEST_NOTE = 'DELIBERATE TEST fired by hand via tools/fire-non-icp-slack.js — not a real blocked prospect';

const FIRES = {
  /* The lead-channel post. Payload shape copied from the /submit call site;
     keep them in step, because a field that drifts here proves the wrong
     message arrives. This one uses the matched-on-email-but-not-website
     shape deliberately — it is the case the mismatch warning exists for, and
     the only way to see that the warning renders. */
  blocked: () => lifted.slackNonIcpBlocked({
    stage: 'submit',
    matched_domain: 'kw.com',
    matched_label: 'Keller Williams',
    matched_in_email: true,
    matched_in_website: false,
    first_name: 'Deliberate',
    last_name: 'Test',
    email: 'non-icp-test@kw.com',
    phone: '+15550000000',
    company: `TEST — ${TEST_NOTE}`,
    website: 'deliberate-non-icp-test.invalid',
    sell_to: 'B2B (clarified from B2C)',
    hear_about_us: 'Fired by hand to prove this message path executes',
    about_business: TEST_NOTE,
  }),

  /* ── V2, the MODEL paths. Neither had ever executed when this was
        written, which by this repo's own rule makes them untested alert
        paths -- and the blocked post is the only way a wrong model block
        becomes visible to a human. Fired before the flags go on. ── */

  /* A model BLOCK. Deliberately uses a domain no list could ever match,
     because that is the whole point of the layer, and carries the three
     fields that make a model verdict falsifiable at a glance: what it
     judged, how confident it was, and the sentence it decided on. */
  'llm-blocked': () => lifted.slackNonIcpBlocked({
    stage: 'submit',
    source: 'llm',
    matched_domain: 'deliberate-non-icp-test.invalid',
    business_type: 'insurance',
    business_type_label: 'Insurance',
    confidence: 0.95,
    evidence_quote: 'We are an independent insurance agency serving families across Ohio with auto, home, life and commercial insurance.',
    model_id: 'claude-opus-5',
    prompt_version: 'v1-2026-09-14',
    first_name: 'Deliberate',
    last_name: 'Test',
    email: 'non-icp-llm-test@deliberate-non-icp-test.invalid',
    phone: '+15550000000',
    company: `TEST — ${TEST_NOTE}`,
    website: 'deliberate-non-icp-test.invalid',
    sell_to: 'B2B',
    hear_about_us: 'Fired by hand to prove the MODEL block message path executes',
    about_business: TEST_NOTE,
  }),

  /* A Meta-only flag: one of the four industries that suppress Meta and
     never block. Different message on purpose -- calling this one "would
     have been blocked" would be false, and false in the one channel that
     exists to catch mistakes is worse than silent. */
  'llm-meta': () => lifted.slackNonIcpLlmFlagged({
    action: 'meta',
    matched_domain: 'deliberate-non-icp-test.invalid',
    business_type: 'restaurant_food',
    business_type_label: 'Restaurant / food service',
    confidence: 0.95,
    evidence_quote: 'A family-run restaurant serving the neighbourhood since 1994.',
    first_name: 'Deliberate',
    last_name: 'Test',
    email: 'non-icp-meta-test@deliberate-non-icp-test.invalid',
    company: `TEST — ${TEST_NOTE}`,
    website: 'deliberate-non-icp-test.invalid',
  }),

  /* ── THE LATE-VERDICT SWEEP's post, added 23 Sept 2026. ───────────
     The sweep shipped and started running before this path had ever
     executed, which by this repo's own rule made it an untested alert --
     and it is the ONLY thing that tells a human a booked lead turned out
     to be non-ICP, because a stamped lead is not pushed to Salesforce and
     is not on the SDR list. Fired by hand here rather than waiting to
     discover a formatting bug on somebody's real meeting.

     Payload shape copied from the runNonIcpBookedRecheck() call site,
     which spreads the lead row and the verdict row together. A demo time
     in the future, because the whole point of the message is that there is
     still a decision to make. */
  'late-block': () => lifted.slackNonIcpLateBlock({
    first_name: 'Deliberate',
    last_name: 'Test',
    email: 'non-icp-late-test@deliberate-non-icp-test.invalid',
    company: `TEST — ${TEST_NOTE}`,
    website: 'https://deliberate-non-icp-test.invalid/',
    phone: '+15550000000',
    start_time: new Date(Date.now() + 36 * 3600 * 1000).toISOString(),
    domain: 'deliberate-non-icp-test.invalid',
    business_type: 'insurance',
    business_type_label: 'Insurance',
    confidence: 0.97,
    evidence_quote: 'Medicare and Health Insurance for Texans — a deliberate test quote, not a real prospect.',
  }),

  /* THE WEEKLY NEAR-MISS DIGEST, added 23 Sept. Fired by hand for the
     same reason as the four above: a message nobody has watched arrive is
     a message nobody has tested. This one is WEEKLY, so waiting for it to
     happen naturally means shipping it unseen for up to seven days.

     Built from a fixture rather than the live report, because the tool
     must not need a database to prove the MESSAGE renders. */
  'near-digest': () => lifted.sendOpsSlack([
    lifted.bHeader('\u{1F50D} 3 companies looked like real estate or insurance \u2014 we let them through'),
    lifted.bDivider(),
    lifted.bSection('*What this is.* We turn away real-estate and insurance companies. These looked like one, but we were not certain enough to act, so they were treated as normal leads.'),
    lifted.bSection('*How certain we need to be before turning someone away:*\n'
      + '\u2022 We could read their website \u2014 *75%*\n'
      + '\u2022 We could not, so we judged the domain name alone \u2014 *90%*\n'
      + '_The second bar is higher because a name is weaker evidence than a page._'),
    lifted.bSection('*1 of the 3 had a real person fill in the form in the last 7 days.* The rest are judgements sitting in our cache with nobody attached.'),
    lifted.bSection('\u2022 `deliberate-two.invalid` \u2014 *Insurance*, *82% sure*, judged from the domain name (needed 90%)\n     \u21b3 *1 lead came through this week*\n'
      + '\u2022 `deliberate-non-icp-test.invalid` \u2014 *Insurance*, *85% sure*, judged from the domain name (needed 90%)\n'
      + '\u2022 `deliberate-three.invalid` \u2014 *Real estate*, *72% sure*, judged from their website (needed 75%)'),
    lifted.bSection('*What to do.* Nothing, unless several of these look plainly wrong to you. If they do, say so and the bar can be lowered \u2014 but a lower bar turns more people away, including some we want, so it is a decision rather than a setting. _Full list on the dashboard, Model tab._'),
    /* The test marker says NOT REAL DATA. The old one said "not a real
       blocked prospect", which contradicted the message above it: a near
       miss is by definition somebody we did NOT block. */
    lifted.bSection('_DELIBERATE TEST via tools/fire-non-icp-slack.js \u2014 the domains above are fake and no action is needed._'),
  ], 'Near misses: 3 companies came close to being turned away (DELIBERATE TEST)'),

  /* The critical. Payload shape copied from rejectBookingIfNonIcp(). */
  booking: () => lifted.alertOps('critical', 'Non-ICP', 'A blocked lead took a calendar slot', {
    'Email':          'non-icp-test@kw.com',
    'Company':        `TEST — ${TEST_NOTE}`,
    'Website':        'deliberate-non-icp-test.invalid',
    'Matched domain': 'kw.com',
    'Booking':        `deliberate-test-uid at ${new Date().toISOString()}`,
    'Route':          '/booking-confirmed',
    'Impact':         'The booking was NOT recorded here, but the slot still exists in Cal/RevenueHero. Nothing in this service can cancel it.',
    'Action':         `${TEST_NOTE} — no action needed.`,
  }),

  /* ── THE WEEKLY DROPOFF DIGEST ──────────────────────────────────
     Mondays 09:00 ET in production. This fires the REAL function over
     the REAL leads table, so the numbers that land in Slack are the
     numbers next Monday would have sent -- the whole reason this tool
     lifts rather than reimplements.

     IT REPORTS THE COMPLETED WEEK, so the week it names is genuinely
     finished and the figures will not move afterwards.

     ONE EXTRA BLOCK IS APPENDED, and only one. Every other payload in
     this file carries its test note inside a field, because those
     messages are built here. This one is built by production code and
     must stay byte-identical to it, so the marker is added AFTER the
     digest is composed rather than passed into it. sendOpsSlack is
     wrapped, not replaced -- the original still does the sending. */
  'dropoff-digest': async () => {
    if (!process.env.DATABASE_URL) {
      throw new Error('DATABASE_URL is not set. This is the only fire here that reads the database. '
        + 'On a developer machine use the Postgres service DATABASE_PUBLIC_URL rather than '
        + 'gushwork-api\u2019s own DATABASE_URL, which is on the private network.');
    }
    const realSend = lifted.setOpsSlack((blocks, fallback) =>
      realSend([...blocks, lifted.bSection('_' + TEST_NOTE + '_')], fallback));
    try {
      /* force = true skips the Monday-and-09:00 gate. Nothing else is
         bypassed: the window, the query, the comparison band and the
         copy are all production's. */
      await lifted.runDropoffDigest(true);
    } finally {
      await pool.end();
    }
  },
};

const which = process.argv[2];
if (!which || !FIRES[which]) {
  console.error('Usage: node tools/fire-non-icp-slack.js <' + Object.keys(FIRES).join('|') + '>');
  process.exit(1);
}
if (!process.env.SLACK_WEBHOOK_URL) {
  console.error('SLACK_WEBHOOK_URL is not set — nothing would be sent. Try: railway run node tools/fire-non-icp-slack.js ' + which);
  process.exit(1);
}

console.log(`[fire-non-icp-slack] firing "${which}" FOR REAL...`);
Promise.resolve(FIRES[which]())
  /* sendSlack is fire-and-forget, so give its fetch time to land before the
     process exits. Same reason tools/fire-alert.js waits. */
  .then(() => new Promise((r) => setTimeout(r, 4000)))
  .then(() => console.log('[fire-non-icp-slack] done — go and look at Slack.'))
  .catch((err) => { console.error('[fire-non-icp-slack] FAILED:', err); process.exit(1); });
