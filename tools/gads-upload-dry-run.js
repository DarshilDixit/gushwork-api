/* ============================================================
   tools/gads-upload-dry-run.js — what the Google Ads conversion upload
   WOULD do with every Google Ads booking in a window. READS ONLY.

   Runs google-ads-conversions.js's own dryRun over the real leads table:
   the same Google rule, the same gates in the same order, the same click-ID
   choice and the same value. The cutover is ignored, so you see the whole
   window. Nothing is written and nothing leaves the machine:
     - every query runs inside BEGIN TRANSACTION READ ONLY, rolled back;
     - the uploader is handed a fetch that throws, so a bug cannot send;
     - it never names gads_conversion_uploads, so it works before that
       table has been deployed.

   THE FUNCTIONS ARE LIFTED OUT OF index.js, NOT COPIED -- the internal
   test-address rule, the website gate, the free-email matcher, the non-ICP
   brand list and verdict-table read, the value adapter and the Google
   source rule. A copy that drifted would answer a different question to
   the sweep, which is the whole thing a dry run exists to prevent.

   Prints counts only: no email, no phone, no click ID, no URL.

   Run:
     railway run -s Postgres bash -c 'DATABASE_URL="$DATABASE_PUBLIC_URL" node tools/gads-upload-dry-run.js'
     ... --days 30            a different window (default 90)
     ... --free-email         simulate GADS_EXCLUDE_FREE_EMAIL=true

   Not mounted anywhere and not called by anything.
   ============================================================ */
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { predictedLtvFor, resolveEventProduct } = require('../meta-capi');
const { createGadsUploader } = require('../google-ads-conversions');

const ROOT = path.join(__dirname, '..');
const src = fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8');

/* Same lifter as tools/fire-alert.js: one top-level declaration, whole. */
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
/* liftDecl also stops early on a function whose PARAMETERS are destructured
   -- nonIcpLlmCachedVerdict({ email, website } = {}) -- because the first
   brace it finds is the parameter list's. This one closes the parameter
   parentheses first and brace-matches the body. */
function liftFn(decl) {
  const i = src.indexOf('\n' + decl);
  if (i === -1) throw new Error('not found in index.js: ' + decl);
  let k = src.indexOf('(', i), d = 0;
  for (; k < src.length; k++) {
    if (src[k] === '(') d++;
    else if (src[k] === ')') { d--; if (!d) break; }
  }
  let j = src.indexOf('{', k);
  d = 0;
  for (let m = j; m < src.length; m++) {
    if (src[m] === '{') d++;
    else if (src[m] === '}') { d--; if (!d) { j = m; break; } }
  }
  return src.slice(i + 1, j + 1);
}
/* liftDecl brace-matches the FIRST object of an array of objects and stops
   there (CLAUDE.md records the same limit for DROPOFF_STAGES), so arrays of
   objects are lifted by region instead. */
function liftArray(decl) {
  const i = src.indexOf('\n' + decl);
  if (i === -1) throw new Error('not found in index.js: ' + decl);
  const end = src.indexOf('\n];', i);
  return src.slice(i + 1, end + 3);
}

const L = new Function('pool', 'process', 'predictedLtvFor', 'resolveEventProduct', [
  liftDecl('const FREE_EMAIL_DOMAINS'),
  liftFn('function damerauLevenshtein'),
  liftFn('function freeEmailMatch'),
  liftFn('function isFreeEmailDomain'),
  liftDecl('const MULTI_PART_SUFFIXES'),
  liftFn('function registrableDomain'),
  liftFn('function partnerStackCustomerKey'),
  liftDecl('const ELV_EXCLUDED_DOMAINS'),
  liftDecl('const INTERNAL_TEST_EMAILS'),
  liftFn('function isInternalLead'),
  liftDecl('const INTERNAL_STAGING_HOSTS'),
  liftFn('function isStagingSubmission'),
  liftFn('function isInternalSubmission'),
  liftDecl('const WEBSITE_VERIFIED_REASONS'),
  liftFn('function isWebsiteVerified'),
  liftDecl('const NON_ICP_DOMAINS'),
  liftArray('const NON_ICP_PREFIXES'),
  liftFn('function hostMatchesDomain'),
  liftFn('function nonIcpHostForms'),
  liftDecl('const NON_ICP_DOMAINS_BY_SPECIFICITY'),
  liftFn('function nonIcpMatchHost'),
  liftDecl('const NON_ICP_BUSINESS_TYPES'),
  liftFn('function nonIcpTypeBlocks'),
  liftFn('function nonIcpTypeSuppressesMeta'),
  liftDecl('const NON_ICP_LLM_CONFIDENCE_FLOOR'),
  liftDecl('const NON_ICP_NAME_CONFIDENCE_FLOOR'),
  liftFn('function nonIcpFloorFor'),
  liftDecl('const NON_ICP_VERDICT_TTL_D'),
  liftDecl('const NON_ICP_FAILURE_TTL_H'),
  liftFn('async function nonIcpReadVerdictRow'),
  liftFn('function nonIcpCandidateDomains'),
  liftFn('async function nonIcpLlmCachedVerdict'),
  liftFn('async function gadsNonIcpFresh'),
  liftFn('function gadsValueFor'),
  liftDecl('const DROPOFF_SOURCE_SQL'),
  'return { freeEmailMatch, isInternalLead, isStagingSubmission, isInternalSubmission, isWebsiteVerified,',
  '         gadsNonIcpFresh, gadsValueFor, DROPOFF_SOURCE_SQL };',
].join('\n'));

async function main() {
  const args = process.argv.slice(2);
  const daysAt = args.indexOf('--days');
  const sinceDays = daysAt !== -1 ? Number(args[daysAt + 1]) : 90;
  if (!Number.isInteger(sinceDays) || sinceDays < 1 || sinceDays > 365) throw new Error('--days must be 1-365');
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is not set (use the Postgres service DATABASE_PUBLIC_URL)');

  const db = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 1 });
  const client = await db.connect();
  try {
    await client.query('BEGIN TRANSACTION READ ONLY');
    const roPool = { query: (q, p) => client.query(q, p) };
    const F = L(roPool, process, predictedLtvFor, resolveEventProduct);
    const env = { ...process.env, GADS_EXCLUDE_FREE_EMAIL: args.includes('--free-email') ? 'true' : 'false' };
    const uploader = createGadsUploader({
      pool: roPool,
      sourceSql: F.DROPOFF_SOURCE_SQL,
      isInternalLead: F.isInternalLead, isStagingSubmission: F.isStagingSubmission, isInternalSubmission: F.isInternalSubmission,
      isWebsiteVerified: F.isWebsiteVerified, freeEmailMatch: F.freeEmailMatch,
      nonIcpFresh: F.gadsNonIcpFresh, valueFor: F.gadsValueFor,
      env,
      fetchImpl: () => { throw new Error('dry run: the network is not used'); },
      log: { log() {}, warn() {}, info() {} },
    });
    /* meta-capi.js logs each page that falls back to the default product,
       once per page -- true and harmless, but it is not this tool's output. */
    const realLog = console.log;
    console.log = () => {};
    let out;
    try { out = await uploader.dryRun({ sinceDays }); } finally { console.log = realLog; }
    const skipped = Object.values(out.skip).reduce((a, n) => a + n, 0);
    const waiting = Object.values(out.wait).reduce((a, n) => a + n, 0);
    console.log(`Google Ads bookings, last ${sinceDays} days (cutover ignored, free email ${env.GADS_EXCLUDE_FREE_EMAIL === 'true' ? 'EXCLUDED' : 'included'}):`);
    console.log(JSON.stringify({ ...out, skippedTotal: skipped, waitingTotal: waiting,
      check: out.send + skipped + waiting === out.considered ? 'sums to considered' : 'DOES NOT SUM' }, null, 2));
  } finally {
    await client.query('ROLLBACK').catch(() => {});
    client.release();
    await db.end();
  }
}

main().catch((e) => { console.error('dry run failed:', e.message); process.exit(1); });
