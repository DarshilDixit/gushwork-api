/* ============================================================
   re-enrich-apollo.js — re-run the Apollo lookups that were REFUSED.

   Apollo ran out of credits three times -- 24 Jun, 3-10 Sept and from
   23 Sept 2026 -- and every lead in those windows was stored with
   Apollo's refusal instead of an enrichment. Nothing noticed until 25
   Sept, because /enrich read the refusal as "no match". This lists those
   leads, prices re-enriching them, and (only with --apply) does it.

   WHAT IT COSTS. Apollo charges 1 credit per person it FINDS and nothing
   when it finds nobody; a phone number would cost 8 more, and this never
   asks for one. So the dry run prints the recent match rate and the
   expected spend beside the ceiling.

   ONE LOOKUP PER ADDRESS, NOT PER SESSION. Somebody who filled the form
   twice during an outage has two refused rows and costs one credit.
   And an address Apollo already found on another session is COPIED from
   that answer -- zero credits.

   IT STOPS AT THE FIRST REFUSAL. Run with no credits and it makes one
   call, says so, and writes nothing -- rather than walking the whole
   list stamping fresh refusals on top of the old ones.

   LIFTS the parser, the SQL and the internal-address check out of
   index.js rather than copying them, like tools/fire-alert.js, so what
   it writes is exactly what the live /enrich route would have written.

   WHAT IT WRITES: enrichment_data (the refused row is replaced by the
   real answer) and the same lead-row columns /enrich updates. Nothing
   else. It does NOT push to Salesforce and does NOT touch the AWS mirror
   -- those are separate decisions, not a side effect of this.

   Run (dry run, reads only):
     railway run -s Postgres bash -c 'DATABASE_URL="$DATABASE_PUBLIC_URL" node tools/re-enrich-apollo.js'
     ... --since 2026-09-23         only refusals on or after that date
     ... --list                     print every address, not the first ten

   Apply (needs APOLLO_API_KEY from the gushwork-api service as well):
     railway run -s Postgres bash -c 'PUB="$DATABASE_PUBLIC_URL" railway run --service gushwork-api bash -c "DATABASE_URL=\"\$PUB\" node tools/re-enrich-apollo.js --since 2026-09-23 --apply"'
     ... --limit 20                 at most 20 Apollo lookups this run

   Not mounted anywhere and not called by anything.
   ============================================================ */
const fs = require('fs');
const path = require('path');

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

const L = new Function('process', [
  liftDecl('function formatRevenue'),
  liftDecl('function apolloReplyError'),
  liftDecl('function apolloEnrichmentFields'),
  liftDecl('const ENRICHMENT_UPSERT_SQL'),
  liftDecl('function enrichmentUpsertParams'),
  liftDecl('const ENRICHMENT_LEAD_UPDATE_SQL'),
  liftDecl('function enrichmentLeadParams'),
  liftDecl('const ELV_EXCLUDED_DOMAINS'),
  liftDecl('const INTERNAL_TEST_EMAILS'),
  liftDecl('function isInternalLead'),
  liftDecl('const INTERNAL_STAGING_HOSTS'),
  liftDecl('function isStagingSubmission'),
  liftDecl('function isInternalSubmission'),
  'return { apolloReplyError, apolloEnrichmentFields, ENRICHMENT_UPSERT_SQL, enrichmentUpsertParams,',
  '         ENRICHMENT_LEAD_UPDATE_SQL, enrichmentLeadParams, isInternalSubmission };',
].join('\n'))(process);

const APOLLO_URL = 'https://api.apollo.io/api/v1/people/match';

/* What a re-run would do, and what it would cost. Reads only. */
async function plan(db, { since = null } = {}) {
  const refusedRows = (await db.query(`
    SELECT e.session_id, lower(e.email) AS email, e.enriched_at, l.page_url
      FROM enrichment_data e
      LEFT JOIN leads l ON l.session_id = e.session_id
     WHERE e.raw_response ? 'error'
       AND e.email IS NOT NULL
       AND ($1::date IS NULL OR e.enriched_at >= $1::date)
     ORDER BY e.enriched_at`, [since])).rows;

  const ours = new Set();
  const byEmail = new Map();
  for (const r of refusedRows) {
    if (L.isInternalSubmission(r.email, r.page_url)) { ours.add(r.email); continue; }
    if (!byEmail.has(r.email)) byEmail.set(r.email, { email: r.email, sessions: [], first: r.enriched_at });
    byEmail.get(r.email).sessions.push(r.session_id);
  }
  const emails = [...byEmail.keys()];

  /* An address Apollo already FOUND, on any session, needs no credit. Only
     an answer that still carries its person is reusable -- a pre-raw_response
     row would parse to nothing and write blanks over the refusal. */
  const reuse = new Map();
  if (emails.length) {
    const good = (await db.query(`
      SELECT DISTINCT ON (lower(email)) lower(email) AS email, session_id, raw_response
        FROM enrichment_data
       WHERE lower(email) = ANY($1::text[])
         AND NOT COALESCE(raw_response ? 'error', false)
         AND jsonb_typeof(raw_response->'person') = 'object'
         AND (enriched_title IS NOT NULL OR enriched_company IS NOT NULL OR enriched_company_size IS NOT NULL)
       ORDER BY lower(email), enriched_at DESC`, [emails])).rows;
    for (const g of good) reuse.set(g.email, g);
  }

  /* The match rate that prices the lookups: the last 500 lookups Apollo
     actually ANSWERED. Refusals are excluded -- they found nobody because
     nobody was looked for. */
  const rate = (await db.query(`
    SELECT COUNT(*) AS answered,
           COUNT(*) FILTER (WHERE enriched_title IS NOT NULL OR enriched_company IS NOT NULL
                              OR enriched_company_size IS NOT NULL) AS matched
      FROM (SELECT enriched_title, enriched_company, enriched_company_size
              FROM enrichment_data
             WHERE raw_response IS NOT NULL AND NOT (raw_response ? 'error')
             ORDER BY enriched_at DESC LIMIT 500) recent`)).rows[0] || {};
  const answered = parseInt(rate.answered) || 0;
  const matched  = parseInt(rate.matched)  || 0;
  const matchRate = answered ? matched / answered : null;

  const copies  = [...byEmail.values()].filter((x) => reuse.has(x.email)).map((x) => ({ ...x, from: reuse.get(x.email) }));
  const lookups = [...byEmail.values()].filter((x) => !reuse.has(x.email));
  return {
    since, refusedSessions: refusedRows.length, oursSkipped: ours.size,
    addresses: byEmail.size, copies, lookups,
    answered, matched, matchRate,
    expectedCredits: matchRate === null ? null : Math.round(lookups.length * matchRate),
    maxCredits: lookups.length,
  };
}

/* The same two writes /enrich makes, to every session of one address. */
async function writeAnswer(db, email, sessions, apolloData) {
  const f = L.apolloEnrichmentFields(apolloData);
  for (const sid of sessions) {
    await db.query(L.ENRICHMENT_UPSERT_SQL, L.enrichmentUpsertParams(sid, email, f, apolloData));
    await db.query(L.ENRICHMENT_LEAD_UPDATE_SQL, L.enrichmentLeadParams(sid, f));
  }
  return !!(f.title || f.company || f.company_size);
}

async function apply(db, fetchFn, p, { limit = Infinity, apiKey, pauseMs = 300, log = console.log } = {}) {
  const out = { copied: 0, looked_up: 0, found: 0, no_match: 0, stopped: null };
  for (const c of p.copies) {
    await writeAnswer(db, c.email, c.sessions, c.from.raw_response);
    out.copied++;
    log(`  copied   ${c.email}  (${c.sessions.length} session(s), from ${c.from.session_id})`);
  }
  for (const x of p.lookups) {
    if (out.looked_up >= limit) { log(`  --limit ${limit} reached; ${p.lookups.length - out.looked_up} address(es) left for another run.`); break; }
    const res  = await fetchFn(APOLLO_URL, { method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'X-Api-Key': apiKey },
      body: JSON.stringify({ email: x.email, reveal_personal_emails: false, reveal_phone_number: false }) });
    const data = await res.json().catch(() => null);
    out.looked_up++;
    const refused = L.apolloReplyError(res.status, data);
    if (refused) {
      out.stopped = refused;
      log(`  STOPPED  Apollo refused ${x.email}: ${refused}`);
      log('  Nothing was written for it, and no further lookups were made.');
      break;
    }
    const found = await writeAnswer(db, x.email, x.sessions, data);
    if (found) out.found++; else out.no_match++;
    log(`  ${found ? 'found   ' : 'no match'} ${x.email}  (${x.sessions.length} session(s))`);
    if (pauseMs) await new Promise((r) => setTimeout(r, pauseMs));
  }
  return out;
}

function report(p, { list = false, log = console.log } = {}) {
  const pct = p.matchRate === null ? 'unknown' : Math.round(p.matchRate * 100) + '%';
  log(`Refused Apollo lookups${p.since ? ' since ' + p.since : ', all time'}: ${p.refusedSessions} session(s)`);
  log(`  our own test submissions, skipped:          ${p.oursSkipped} address(es)`);
  log(`  real addresses:                             ${p.addresses}`);
  log(`    copied from an earlier Apollo answer:     ${p.copies.length}   (0 credits)`);
  log(`    need an Apollo lookup:                    ${p.lookups.length}`);
  log('');
  log('Apollo charges 1 credit per person it finds and 0 when it finds nobody. Phone reveal is off.');
  log(`Recent match rate ${pct} (${p.matched} of the last ${p.answered} answered lookups),`);
  log(`so expect about ${p.expectedCredits === null ? '?' : p.expectedCredits} credit(s); at most ${p.maxCredits}.`);
  const show = list ? p.lookups : p.lookups.slice(0, 10);
  if (show.length) {
    log('');
    log(list ? 'Every address that needs a lookup:' : 'First ten that need a lookup (--list for all):');
    for (const x of show) log(`  ${x.email}  (${x.sessions.length} session(s), first refused ${new Date(x.first).toISOString().slice(0, 10)})`);
  }
}

async function main() {
  const argv = process.argv.slice(2);
  const arg = (name) => { const i = argv.indexOf(name); return i === -1 ? null : argv[i + 1]; };
  const APPLY = argv.includes('--apply');
  const since = arg('--since');
  if (since && !/^\d{4}-\d{2}-\d{2}$/.test(since)) { console.error('--since takes YYYY-MM-DD'); process.exit(1); }
  const limit = arg('--limit') ? parseInt(arg('--limit'), 10) : Infinity;

  const url = process.env.DATABASE_URL;
  if (!url) { console.error('DATABASE_URL is not set. See the header of this file for the railway command.'); process.exit(1); }
  if (APPLY && !process.env.APOLLO_API_KEY) { console.error('--apply needs APOLLO_API_KEY. See the header of this file.'); process.exit(1); }
  const { Pool } = require('pg');
  const db = new Pool({ connectionString: url, ssl: url.includes('localhost') ? false : { rejectUnauthorized: false } });

  const p = await plan(db, { since });
  console.log(APPLY ? 'APPLYING — this calls Apollo and writes.\n' : 'DRY RUN — reads only. Pass --apply to call Apollo and write.\n');
  report(p, { list: argv.includes('--list') });
  if (APPLY) {
    console.log('');
    const out = await apply(db, fetch, p, { limit, apiKey: process.env.APOLLO_API_KEY });
    console.log(`\nDone: ${out.copied} copied, ${out.looked_up} looked up (${out.found} found, ${out.no_match} no match)${out.stopped ? ' — STOPPED: ' + out.stopped : ''}.`);
    console.log('Salesforce and the AWS mirror were not touched.');
  }
  await db.end();
}

module.exports = { plan, apply, report, APOLLO_URL };
if (require.main === module) main().catch((err) => { console.error('[re-enrich-apollo] FAILED:', err); process.exit(1); });
