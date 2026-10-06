/* ============================================================
   tools/agency-exclusion-dry-run.js — how many leads the Salesforce
   exclusions would stop, and how many agency rows the SDR list carries.
   READS ONLY. Prints counts, never an address.

   Uses the REAL rule: salesforceSkipReason from salesforce.js, with the
   real isInternalSubmission lifted out of index.js and handed in exactly
   as index.js hands it in at boot. A copy that drifted would answer a
   different question to production.

   What it counts, over the last N days (default 90):
     - lead rows that would reach Salesforce today (submitted, or a booking
       safety-net row) and would now be SKIPPED, by reason;
     - of those, how many HAVE reached Salesforce (sf_synced_at set) --
       the leak this closes;
     - the booking safety nets separately, because before this change they
       had no internal check at all;
     - SDR-list rows today that are agency (marked on screen, left out of
       the CSV).

   Run:
     railway run -s Postgres bash -c 'DATABASE_URL="$DATABASE_PUBLIC_URL" node tools/agency-exclusion-dry-run.js'
     ... --days 30

   Not mounted anywhere and not called by anything.
   ============================================================ */
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const SF = require('../salesforce');
const { agencyDomainMatch, agencyDomains } = require('../agency-domains');

const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
function liftDecl(decl) {
  const i = src.indexOf('\n' + decl);
  if (i === -1) throw new Error('not found in index.js: ' + decl);
  let j = src.indexOf('{', i); const semi = src.indexOf(';', i);
  if (j === -1 || (semi !== -1 && semi < j)) return src.slice(i + 1, semi + 1);
  let d = 0; for (let k = j; k < src.length; k++) { if (src[k] === '{') d++; else if (src[k] === '}') { d--; if (!d) { j = k; break; } } }
  return src.slice(i + 1, j + 1);
}
const L = new Function('process', [liftDecl('const ELV_EXCLUDED_DOMAINS'), liftDecl('const INTERNAL_TEST_EMAILS'), liftDecl('function isInternalLead'),
  liftDecl('const INTERNAL_STAGING_HOSTS'), liftDecl('function isStagingSubmission'), liftDecl('function isInternalSubmission'),
  'return { isInternalSubmission };'].join('\n'))(process);
SF.setSalesforceInternalCheck(L.isInternalSubmission);

async function main() {
  const at = process.argv.indexOf('--days');
  const days = at !== -1 ? Number(process.argv[at + 1]) : 90;
  if (!Number.isInteger(days) || days < 1 || days > 730) throw new Error('--days must be 1-730');
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is not set (use the Postgres service DATABASE_PUBLIC_URL)');
  const db = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 1 });
  const c = await db.connect();
  try {
    await c.query('BEGIN TRANSACTION READ ONLY');
    const r = await c.query(
      `SELECT email, website, page_url, prefill_source, submitted_at, booking_uid, sf_synced_at, sf_sync_failed_at, sf_sync_retryable
         FROM leads
        WHERE created_at >= NOW() - ($1::int * INTERVAL '1 day')
          AND (submitted_at IS NOT NULL OR prefill_source IN ('cal_webhook', 'rh_webhook'))`, [days]);
    const out = { days, agency_list: agencyDomains(process.env).join(','), considered: r.rows.length,
      would_skip: { agency: 0, internal: 0 }, already_in_salesforce: { agency: 0, internal: 0 },
      safety_net_rows: { agency: 0, internal: 0, normal: 0 }, retry_queue_now: { agency: 0, internal: 0 }, by_via: { email: 0, website: 0 } };
    for (const l of r.rows) {
      const skip = SF.salesforceSkipReason({ email: l.email, website: l.website, page_url: l.page_url });
      const net = l.prefill_source === 'cal_webhook' || l.prefill_source === 'rh_webhook';
      if (net) out.safety_net_rows[skip ? skip.reason : 'normal']++;
      if (!skip) continue;
      out.would_skip[skip.reason]++;
      if (l.sf_synced_at) out.already_in_salesforce[skip.reason]++;
      if (l.sf_sync_failed_at && !l.sf_synced_at && l.sf_sync_retryable === true) out.retry_queue_now[skip.reason]++;
      if (skip.reason === 'agency') out.by_via[(agencyDomainMatch({ email: l.email, website: l.website }) || {}).via || 'email']++;
    }
    const sdr = await c.query(
      `SELECT DISTINCT ON (LOWER(email)) email, website FROM leads
        WHERE email IS NOT NULL AND completed IS TRUE AND booking_uid IS NULL
          AND created_at >= NOW() - ($1::int * INTERVAL '1 day')
        ORDER BY LOWER(email), created_at DESC`, [days]);
    out.sdr_like_people_agency = sdr.rows.filter((x) => agencyDomainMatch({ email: x.email, website: x.website })).length;
    out.note = 'sdr_like_people_agency approximates the SDR list (completed, never booked); the list itself also requires B2B and excludes disqualified and blocked leads.';
    console.log(JSON.stringify(out, null, 2));
  } finally {
    await c.query('ROLLBACK').catch(() => {});
    c.release();
    await db.end();
  }
}

main().catch((e) => { console.error('dry run failed:', e.message); process.exit(1); });
