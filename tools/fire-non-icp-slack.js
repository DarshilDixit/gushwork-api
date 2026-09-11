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
         node tools/fire-non-icp-slack.js booking

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

const lifted = new Function('require', [
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
  'return { alertOps, slackNonIcpBlocked };',
].join('\n'))(require);

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
