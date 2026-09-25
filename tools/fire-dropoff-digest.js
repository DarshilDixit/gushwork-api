/* ============================================================
   fire-dropoff-digest.js — send the weekly dropoff digest for real.

   CLAUDE.md: "Every alert path gets FIRED ONCE ON PURPOSE before
   launch." Executing it with the sender stubbed proves the message
   composes; it does not prove a message arrives, and the gap between
   those two claims is where 21 dead call sites hid in this repo.

   WHY ITS OWN FILE. This first went into fire-non-icp-slack.js because
   that tool already had the lifting machinery. That was the wrong
   reason: the digest has nothing to do with the non-ICP block, and a
   tool named for one feature that quietly fires another is how nobody
   finds either of them later. One tool, one purpose, like
   tools/fire-alert.js.

   IT SENDS FOR REAL, to the LEADS channel (SLACK_WEBHOOK_URL), because
   that is where the scheduled digest goes. Nothing in it is broken and
   nobody has to act on it, so it is deliberately not an ops alert.

   IT IS THE ONLY FIRE TOOL HERE THAT READS A DATABASE. It runs the real
   report over the real leads table, so the numbers that land in Slack
   are the numbers Monday would have sent. On a developer machine that
   means the Postgres service's DATABASE_PUBLIC_URL -- gushwork-api's own
   DATABASE_URL is on Railway's private network and will not resolve.

   THE NUMBERS ARE REAL, ONLY THE TIMING IS NOT. So the marker appended
   below says exactly that, rather than calling the data a test. It is
   added AFTER the digest is composed rather than passed into it,
   because the message has to stay byte-identical to what the scheduled
   post sends -- otherwise firing it proves the wrong message works.

   Run:
     DATABASE_URL="<DATABASE_PUBLIC_URL>" \
     SLACK_WEBHOOK_URL="<leads webhook>" \
     node tools/fire-dropoff-digest.js

   Not mounted anywhere and not called by anything, like
   tools/fire-alert.js and tools/fire-non-icp-slack.js.
   ============================================================ */

const fs = require('fs');
const path = require('path');
const ROOT = path.join(__dirname, '..');
const src = fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8');
/* The real pool, not a second one built from the same URL. Requiring db.js
   creates the Pool but connects nothing and does not run initDB. */
const { pool } = require(path.join(ROOT, 'db'));

/* Slice a contiguous region between two markers, rather than matching one
   declaration. fire-non-icp-slack.js's liftDecl brace-matches a single
   statement, which truncates an array of objects at its first element --
   DROPOFF_STAGES came out unterminated and the whole lift failed to parse.
   Same helper tools/non-icp-validate.js uses. */
function between(a, b) {
  const i = src.indexOf(a);
  if (i === -1) throw new Error('marker not found in index.js: ' + a);
  const j = src.indexOf(b, i);
  if (j === -1) throw new Error('end marker not found in index.js: ' + b);
  return src.slice(i, j);
}

/* LIFTED, never reimplemented. A second copy of the message here would be
   a second source of truth, and the whole point is that what arrives is
   what production sends. */
const lifted = new Function('require', 'pool', [
  "const DASH_TZ = 'America/New_York';",
  /* sendSlack plus the block builders, in one contiguous region -- they sit
     together at the top of index.js in that order. */
  between('function sendSlack(blocks, fallbackText)', 'function bContext(text)'),
  /* sendSlack calls this when the webhook is missing or the post fails. It
     lives ~400 lines further down, so it is its own slice. */
  between('function alertSlackBroken', 'const FAILURE_MONITORS'),
  between('const ELV_EXCLUDED_DOMAINS', 'function pruneElvWindow'),
  between('const DROPOFF_STAGE_SQL', 'async function visitorsReport'),
  between('function etParts(', 'async function runNearMissDigest'),
  between('const DROPOFF_DIGEST_HOUR_ET', 'function startDropoffDigest'),
  /* setLeadSlack returns the ORIGINAL so the caller can wrap rather than
     replace. A function declaration is a mutable binding, which is the
     same property tools/non-icp-validate.js uses to replay a scrape. */
  'return { runDropoffDigest, dropoffReport, bSection,'
  + ' setLeadSlack(f) { const prev = sendSlack; sendSlack = f; return prev; } };',
].join('\n'))(require, pool);

const NOTE = 'Sent by hand from tools/fire-dropoff-digest.js to check this message works. '
  + 'The numbers are real; the scheduled post still runs on Monday.';

if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL is not set — this tool reads the leads table.\n'
    + 'On a developer machine use the Postgres service DATABASE_PUBLIC_URL, not\n'
    + 'gushwork-api’s own DATABASE_URL, which is on the private network.');
  process.exit(1);
}
if (!process.env.SLACK_WEBHOOK_URL) {
  console.error('SLACK_WEBHOOK_URL is not set — nothing would be sent.');
  process.exit(1);
}

console.log('[fire-dropoff-digest] firing FOR REAL to the leads channel...');
const realSend = lifted.setLeadSlack((blocks, fallback) =>
  realSend([...blocks, lifted.bSection('_' + NOTE + '_')], fallback));

/* force = true skips the Monday-and-09:00 gate. Nothing else is bypassed:
   the window, the query, the comparison band and the copy are production's. */
lifted.runDropoffDigest(true)
  /* sendSlack is fire-and-forget, so give its fetch time to land before the
     process exits. Same reason tools/fire-alert.js waits. */
  .then(() => new Promise((r) => setTimeout(r, 4000)))
  .then(() => pool.end())
  .then(() => console.log('[fire-dropoff-digest] done — go and look at Slack.'))
  .catch(async (err) => {
    console.error('[fire-dropoff-digest] FAILED:', err && err.message);
    try { await pool.end(); } catch (_) {}
    process.exit(1);
  });
