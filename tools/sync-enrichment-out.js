/* ============================================================
   sync-enrichment-out.js — carry re-enriched Apollo fields OUT to the two
   places that got blanks: the AWS mirror (gw_form_leads, the dialer's feed)
   and Salesforce.

   WHY IT EXISTS. Apollo ran out of credits three times (24 Jun, 3-10 Sept,
   23 Sept on), and every lead in those windows was pushed to the mirror
   and to Salesforce at submit time WITH NO ENRICHMENT. tools/re-enrich-
   apollo.js fixes our own tables and deliberately stops there; on 26 Sept
   Darshil decided the mirror and Salesforce get the values too.

   FILL-ONLY. A field is written only where the destination is BLANK and we
   now hold a value. Nothing that already has a value is overwritten, no
   other column is touched, no row is created. So it is safe to re-run, and
   it can never undo something a human or a later lookup wrote.

   SCOPE: the sessions whose enrichment_data row was rewritten since
   --since (the start of the backfill run). Our own test submissions are
   skipped. Values are read the way the dashboard reads them:
   COALESCE(lead row, enrichment_data), since title, company size, industry
   and LinkedIn live only on enrichment_data.

   THE MIRROR: a targeted UPDATE ... WHERE session_id, never syncToAWS --
   that upsert sets disqualified = EXCLUDED.disqualified and would clear a
   real disqualification on the dialer's feed. A session with no mirror row
   (never submitted, blocked, ours) matches nothing and is counted.

   SALESFORCE: the field names come from salesforce.js's own map and their
   TYPES and LENGTHS from Salesforce's describe, never restated here. Every
   non-converted Lead with that email is filled; a converted Lead cannot be
   updated and is counted instead. updateSFLead does the write, the same
   function the live service uses.

   Run (dry run, reads only):
     railway run -s Postgres bash -c 'PUB="$DATABASE_PUBLIC_URL" railway run --service gushwork-api bash -c "DATABASE_URL=\"\$PUB\" node tools/sync-enrichment-out.js --since 2026-09-26T14:00:00Z"'
   Then add --apply, and --mirror or --salesforce to do only one of them.
   Not mounted anywhere and not called by anything.
   ============================================================ */
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const ROOT = path.join(__dirname, '..');
const src = fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8');

/* Same lifter as tools/re-enrich-apollo.js: one top-level declaration, whole. */
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
  liftDecl('const ELV_EXCLUDED_DOMAINS'),
  liftDecl('const INTERNAL_TEST_EMAILS'),
  liftDecl('function isInternalLead'),
  liftDecl('const INTERNAL_STAGING_HOSTS'),
  liftDecl('function isStagingSubmission'),
  liftDecl('function isInternalSubmission'),
  'return { isInternalSubmission };',
].join('\n'))(process);

/* The Salesforce enrichment fields, read out of salesforce.js's own map. */
const SF_MAP = [...fs.readFileSync(path.join(ROOT, 'salesforce.js'), 'utf8')
  .matchAll(/^\s+(enriched_[a-z_]+): '(enriched_[a-z_]+__c)',/gm)].map((m) => [m[1], m[2]]);
if (!SF_MAP.length) throw new Error('no enriched_* fields found in salesforce.js');

const blank = (v) => v === null || v === undefined || String(v).trim() === '';

async function columns(db, table) {
  const { rows } = await db.query(
    `SELECT column_name FROM information_schema.columns WHERE table_name = $1 AND column_name LIKE 'enriched_%'`, [table]);
  return new Set(rows.map((r) => r.column_name));
}

/* Every re-enriched session, with its values read as the dashboard reads them. */
async function scope(db, since) {
  const lc = await columns(db, 'leads'), ec = await columns(db, 'enrichment_data');
  const all = [...new Set([...lc, ...ec])].filter((c) => c !== 'enriched_at').sort();
  const expr = (c) => (lc.has(c) && ec.has(c) ? `COALESCE(l.${c}::text, e.${c}::text)` : lc.has(c) ? `l.${c}::text` : `e.${c}::text`);
  const { rows } = await db.query(`
    SELECT l.session_id::text AS session_id, lower(l.email) AS email, lower(e.email) AS looked_up, l.page_url, l.created_at,
           ${all.map((c) => `${expr(c)} AS ${c}`).join(', ')}
      FROM enrichment_data e JOIN leads l ON l.session_id = e.session_id
     WHERE e.enriched_at >= $1 AND l.email IS NOT NULL
     ORDER BY l.created_at DESC`, [since]);
  /* THE LOOKED-UP ADDRESS MUST BE THE LEAD'S ADDRESS. A refusal row is
     insert-only, so a visitor who typed A, then changed it to B during an
     outage, keeps a refusal for A -- and the backfill looked up A. Carrying
     that onto B's Salesforce Lead would put a stranger's title and city in
     front of an AE. The form clears enrichment when the email changes; this
     skips it. Checked on the 26 Sept run: 0 such sessions. */
  const moved = rows.filter((r) => r.looked_up && r.looked_up !== r.email);
  const kept = rows.filter((r) => !(r.looked_up && r.looked_up !== r.email));
  const ours = kept.filter((r) => L.isInternalSubmission(r.email, r.page_url));
  const real = kept.filter((r) => !L.isInternalSubmission(r.email, r.page_url));
  const withValues = real.filter((r) => all.some((c) => !blank(r[c])));
  return { all, rows: withValues, ours: ours.length, empty: real.length - withValues.length, email_changed: moved.length };
}

async function syncMirror(aws, sc, { apply, log }) {
  const mc = [...(await columns(aws, 'gw_form_leads'))].filter((c) => sc.all.includes(c));
  const { rows: m } = await aws.query(
    `SELECT session_id, ${mc.join(', ')} FROM gw_form_leads WHERE session_id = ANY($1::text[])`, [sc.rows.map((r) => r.session_id)]);
  const byId = new Map(m.map((r) => [r.session_id, r]));
  const out = { rows_on_mirror: m.length, not_on_mirror: 0, rows_to_fill: 0, fields_to_fill: 0, written: 0, by_field: {} };
  for (const r of sc.rows) {
    const cur = byId.get(r.session_id);
    if (!cur) { out.not_on_mirror++; continue; }
    const fill = mc.filter((c) => blank(cur[c]) && !blank(r[c]));
    if (!fill.length) continue;
    out.rows_to_fill++; out.fields_to_fill += fill.length;
    fill.forEach((c) => { out.by_field[c] = (out.by_field[c] || 0) + 1; });
    if (apply) {
      /* COALESCE(NULLIF(...)) again at write time, so a value that landed on
         the mirror between the read and this write is still never replaced. */
      const set = fill.map((c, i) => `${c} = COALESCE(NULLIF(${c}, ''), $${i + 2})`).join(', ');
      const res = await aws.query(`UPDATE gw_form_leads SET ${set}, updated_at = NOW() WHERE session_id = $1`,
        [r.session_id, ...fill.map((c) => r[c])]);
      out.written += res.rowCount;
    }
  }
  log('MIRROR (gw_form_leads, the dialer feed):', JSON.stringify(out));
  return out;
}

/* SF and fetchFn are parameters only so tests/test-apollo.js can drive this
   against a stubbed Salesforce; a real run always takes the defaults. */
async function syncSalesforce(sc, { apply, log, SF = require(path.join(ROOT, 'salesforce.js')), fetchFn = fetch } = {}) {
  const { accessToken, instanceUrl } = await SF.getSalesforceToken();
  const get = async (u) => { const r = await fetchFn(u, { headers: { Authorization: `Bearer ${accessToken}` } });
    if (!r.ok) throw new Error(`Salesforce ${r.status}: ${(await r.text()).slice(0, 200)}`); return r.json(); };
  /* types and lengths from Salesforce itself */
  const desc = await get(`${instanceUrl}/services/data/v60.0/sobjects/Lead/describe`);
  /* KEYED IN LOWER CASE. Salesforce API names are case-insensitive, and the
     org spells one of these enriched_linkedIn__c where salesforce.js says
     enriched_linkedin__c. The first run looked names up exactly, missed it,
     reported LinkedIn "not updateable" and skipped it on 140 Leads. */
  const meta = new Map(desc.fields.map((f) => [f.name.toLowerCase(), f]));
  const fieldOf = (sf) => meta.get(sf.toLowerCase());
  const fields = SF_MAP.filter(([, sf]) => fieldOf(sf) && fieldOf(sf).updateable);
  const missing = SF_MAP.filter(([, sf]) => !fieldOf(sf) || !fieldOf(sf).updateable).map(([, sf]) => sf);
  const coerce = (sf, v) => {
    const f = fieldOf(sf);
    if (['double', 'int', 'currency', 'percent'].includes(f.type)) { const n = Number(String(v).replace(/[^0-9.-]/g, '')); return Number.isFinite(n) ? n : null; }
    const s = String(v); return f.length ? s.slice(0, f.length) : s;
  };
  /* newest session with values, per address */
  const byEmail = new Map();
  for (const r of sc.rows) if (!byEmail.has(r.email) && !/['\\]/.test(r.email)) byEmail.set(r.email, r);
  const emails = [...byEmail.keys()];
  const leads = [];
  for (let i = 0; i < emails.length; i += 200) {
    const soql = `SELECT Id, Email, IsConverted, ${fields.map(([, sf]) => sf).join(', ')} FROM Lead WHERE Email IN (${emails.slice(i, i + 200).map((e) => `'${e}'`).join(',')})`;
    let url = `${instanceUrl}/services/data/v60.0/query/?q=${encodeURIComponent(soql)}`, total = null, got = [];
    while (url) { const d = await get(url); if (total === null) total = d.totalSize; got = got.concat(d.records || []);
      url = d.done === false && d.nextRecordsUrl ? instanceUrl + d.nextRecordsUrl : null; }
    if (got.length < total) throw new Error(`Salesforce returned ${got.length} of ${total} Leads — refusing to act on a partial read`);
    leads.push(...got);
  }
  const out = { addresses: emails.length, sf_leads_matched: leads.length, addresses_with_no_sf_lead: 0,
                converted_skipped: 0, leads_to_fill: 0, fields_to_fill: 0, written: 0, failed: 0, by_field: {}, not_updateable_in_sf: missing };
  const matched = new Set(leads.map((x) => String(x.Email || '').toLowerCase()));
  out.addresses_with_no_sf_lead = emails.filter((e) => !matched.has(e)).length;
  for (const x of leads) {
    if (x.IsConverted) { out.converted_skipped++; continue; }
    const ours = byEmail.get(String(x.Email || '').toLowerCase());
    if (!ours) continue;
    const patch = {};
    for (const [col, sf] of fields) if (blank(x[sf]) && !blank(ours[col])) { const v = coerce(sf, ours[col]); if (v !== null) patch[sf] = v; }
    const n = Object.keys(patch).length;
    if (!n) continue;
    out.leads_to_fill++; out.fields_to_fill += n;
    Object.keys(patch).forEach((k) => { out.by_field[k] = (out.by_field[k] || 0) + 1; });
    if (apply) {
      try {
        const res = await SF.updateSFLead(x.Id, patch);
        out.written++;
        /* updateSFLead retries WITHOUT a field Salesforce rejects and still
           succeeds, so a success can carry less than was sent. Counted. */
        if (res && res.droppedFields && res.droppedFields.length) {
          out.dropped = (out.dropped || 0) + res.droppedFields.length;
          log(`  Lead ${x.Id}: Salesforce refused ${res.droppedFields.join(', ')}; the rest was written`);
        }
      }
      catch (err) { out.failed++; log(`  Salesforce update failed for Lead ${x.Id}: ${String(err.message).slice(0, 160)}`); }
    }
  }
  log('SALESFORCE (Lead):', JSON.stringify(out));
  return out;
}

async function main() {
  const argv = process.argv.slice(2);
  const arg = (k) => { const i = argv.indexOf(k); return i >= 0 ? argv[i + 1] : null; };
  const apply = argv.includes('--apply');
  const since = arg('--since');
  if (!since || isNaN(Date.parse(since))) { console.error('--since <ISO time> is required: the start of the backfill run.'); process.exit(1); }
  const doMirror = !argv.includes('--salesforce') || argv.includes('--mirror');
  const doSf = !argv.includes('--mirror') || argv.includes('--salesforce');
  console.log(apply ? 'APPLYING — fills blanks only.\n' : 'DRY RUN — reads only. Pass --apply to write.\n');
  const db = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 2,
    options: '-c default_transaction_read_only=on -c statement_timeout=60000' });
  const sc = await scope(db, since);
  await db.end();
  console.log(`Re-enriched since ${since}: ${sc.rows.length} session(s) with values; ${sc.ours} of ours skipped; ${sc.empty} with nothing to carry; ` +
    `${sc.email_changed} skipped because the address looked up is not the lead's address.`);
  if (doMirror) {
    const aws = new Pool({ host: process.env.AWS_PG_HOST, port: parseInt(process.env.AWS_PG_PORT) || 5432, user: process.env.AWS_PG_USER,
      password: process.env.AWS_PG_PASSWORD, database: process.env.AWS_PG_DATABASE, ssl: { rejectUnauthorized: false }, max: 2,
      options: apply ? '-c statement_timeout=60000' : '-c default_transaction_read_only=on -c statement_timeout=60000' });
    await syncMirror(aws, sc, { apply, log: console.log });
    await aws.end();
  }
  if (doSf) await syncSalesforce(sc, { apply, log: console.log });
}

if (require.main === module) main().catch((e) => { console.error('FAILED', e.message); process.exit(1); });
module.exports = { scope, syncMirror, syncSalesforce, SF_MAP };
