/* ============================================================
   fire-alert.js — fire ONE real alert, on purpose.

   CLAUDE.md: "Every alert path gets fired once on purpose before
   launch." This is how. The rule exists because 21 recordFailure call
   sites in this repo had tests, passed them, and had never produced a
   single alert — "we asserted it alerts" and "we watched it alert" are
   different claims, and only the second one is evidence.

   IT SENDS FOR REAL. A critical goes to the Slack alerts channel AND
   emails ALERT_EMAIL_TO. There is no dry-run flag on purpose: a dry run
   would prove exactly the thing that was already being proved and
   already wrong.

   It LIFTS alertOps and its dependencies out of index.js rather than
   reimplementing them, so what arrives is what production would send.
   Reimplementing the message here would make this tool a second source
   of truth that can drift — which is the failure mode of every other
   duplicated thing in this codebase.

   Run:  node tools/fire-alert.js exhaustion
         node tools/fire-alert.js opp-permission
         node tools/fire-alert.js conversion-failed

   Every payload names itself a DELIBERATE TEST in a field a human
   reads, so nobody triages it as a real lost payout.

   Firing from a developer machine uses a FRESH cooldown map, so it
   cannot suppress a genuine production alert later — the two processes
   do not share _alertLastSent.

   Not mounted anywhere and not called by anything, like backfill-sf.js.
   ============================================================ */

const fs = require('fs');
const path = require('path');
const ROOT = path.join(__dirname, '..');
const src = fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8');

/* Lift a top-level declaration whole. A `const X = new Thing({...})` continues
   past its matched brace with `);` — brace matching alone truncates it and
   surfaces as "missing ) after argument list", the same error shape the
   backtick-in-SQL-comment trap produces. */
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

const { alertOps } = new Function('require', [
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
  liftDecl('let _gmailTransport'),
  liftDecl('function getGmailTransport'),
  liftDecl('async function sendAlertEmail'),
  liftDecl('function alertOps'),
  'return { alertOps };',
].join('\n'))(require);

const TEST_NOTE = 'DELIBERATE TEST fired by hand via tools/fire-alert.js — not a real failure';

/* Payload shapes copied from their call sites. Keep them in step: a field that
   drifts here proves the wrong message arrives. */
const ALERTS = {
  exhaustion: () => alertOps('critical', 'PartnerStack', 'Conversion retries exhausted — affiliate NOT credited', {
    'Domain': 'deliberate-alert-test.invalid',
    'Attempts': '5 of 5, all failed',
    'Last reason': `http_400 — ${TEST_NOTE}`,
    'Lead': 'alert-test@example.invalid',
    'Impact': 'This conversion will NEVER be sent again. PartnerStack does not know this customer exists, so the affiliate gets nothing and the qualified-demo action can never fire for them either.',
    'What to do': 'Send the conversion by hand, or acknowledge the domain on the Partners tab if the failure is understood — an acknowledged failure is no longer retried.',
  }),
  'opp-permission': () => alertOps('critical', 'PartnerStack', 'Salesforce refused the Opportunity write', {
    'Field': 'Partner_Source__c',
    'Opportunity': '006DELIBERATETEST',
    'Domain': 'deliberate-alert-test.invalid',
    'Status': `403 — ${TEST_NOTE}`,
    'Detail': 'INSUFFICIENT_ACCESS_OR_READONLY',
    'Impact': 'EVERY partner domain is affected, not just this one. Partner attribution is not reaching the Opportunity an AE looks at.',
    'What to do': 'Check that the integration user can update Partner_Source__c AND has field-level access to it. Creating a field through the Tooling API does NOT grant access. PS_SF_OPP_WRITE=false stops the write from the Railway env without a deploy.',
  }),
  'conversion-failed': () => alertOps('critical', 'PartnerStack', 'Conversion failed — affiliate not credited', {
    'Domain': 'deliberate-alert-test.invalid',
    'Partner': 'Test Account <alert-test@example.invalid>',
    'Lead': 'alert-test@example.invalid',
    'Reason': `http_500 — ${TEST_NOTE}`,
  }),
};

const which = process.argv[2];
if (!ALERTS[which]) {
  console.error('Usage: node tools/fire-alert.js <' + Object.keys(ALERTS).join('|') + '>');
  process.exit(1);
}
console.log(`Firing "${which}" for real. Slack + email if critical.`);
const sent = ALERTS[which]();
console.log(`alertOps returned ${sent}${sent ? '' : ' — a cooldown suppressed it'}`);
/* Slack and email are fire-and-forget promises; give them time to land. */
setTimeout(() => process.exit(0), 8000);
