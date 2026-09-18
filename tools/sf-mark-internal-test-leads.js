#!/usr/bin/env node
/* ── MARK OUR OWN TEST SUBMISSIONS AS "Invalid / Test" IN SALESFORCE ──
   Not mounted, not called by anything. Run by hand, like fire-alert.js.

   WHY MARK AND NOT DELETE. Deleting was the ask and it is the wrong
   tool, for a reason only visible after reading the org: 141 Lead
   records match isInternalLead, and roughly 100 of them are NOT ours.
   They are @example.com placeholders carrying outbound campaign
   LeadSource codes (E004P, C240A, C100PA) and they have never appeared
   in our leads table -- somebody typed a placeholder address into
   somebody else's funnel. "Internal email" is a safe test for
   suppressing an OUTBOUND event and an unsafe test for destroying a
   record.

   So provenance is established from OUR database, not from the address:
   the only Leads touched are ones whose email our own form actually
   submitted. That list is an input file, not a rule this script infers.

   AND MARKING IS WHAT THE ORG ALREADY DOES. Source_Bucket__c is a
   FORMULA whose FIRST branch -- above every channel -- reads:

       IF(CONTAINS(" " & SUBSTITUTE(SUBSTITUTE(LOWER(How_Did_You_Hear__c),
            "_", " "), "-", " ") & " ", " test"), "Invalid / Test",

   so writing a How_Did_You_Hear__c containing the word "test" re-buckets
   the Lead through the mechanism Salesforce already has, with no delete,
   no recycle bin, and a full revert path. Two records already read
   "Invalid / Test" this way.

   CLAUDE.md warns that How_Did_You_Hear__c is a production input to
   somebody else's reporting and that changing it silently re-buckets
   leads. That is the INTENT here rather than a side effect -- and it is
   why every previous value is written to a manifest and why --revert
   exists.

   USAGE
     node tools/sf-mark-internal-test-leads.js --emails <file.json>
     node tools/sf-mark-internal-test-leads.js --emails <file.json> --apply
     node tools/sf-mark-internal-test-leads.js --revert <manifest.json>

   Dry run is the DEFAULT and prints every record it would change.
   Needs SF_CLIENT_ID / SF_CLIENT_SECRET / SF_REFRESH_TOKEN, so:
     railway run --service gushwork-api node tools/sf-mark-internal-test-leads.js ...
   ──────────────────────────────────────────────────────────────────── */

const fs = require('fs');
const path = require('path');
const { getSalesforceToken } = require(path.join(__dirname, '..', 'salesforce.js'));

const MARKER = 'Internal test submission';
const API = 'v60.0';

const args = process.argv.slice(2);
const has = (f) => args.includes(f);
const val = (f) => { const i = args.indexOf(f); return i === -1 ? null : args[i + 1]; };
const APPLY = has('--apply');
const REVERT = val('--revert');
const EMAILS = val('--emails');

/* The marker must actually hit the formula's first branch. Asserted here
   rather than assumed: a marker that does not contain the space-padded
   word "test" would rewrite every record and re-bucket none of them. */
function bucketsAsInvalidTest(s) {
  const norm = ' ' + String(s || '').toLowerCase().replace(/_/g, ' ').replace(/-/g, ' ') + ' ';
  return norm.includes(' test');
}
if (!bucketsAsInvalidTest(MARKER)) {
  console.error(`FATAL: marker ${JSON.stringify(MARKER)} does not contain the word "test" and would re-bucket nothing.`);
  process.exit(1);
}

async function sf(token, instanceUrl, url, init) {
  const r = await fetch(url.startsWith('http') ? url : `${instanceUrl}${url}`, {
    ...init,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', ...(init && init.headers) },
  });
  if (r.status === 204) return null;
  const text = await r.text();
  if (!r.ok) throw new Error(`${r.status} ${text}`);
  return text ? JSON.parse(text) : null;
}

async function queryAll(token, instanceUrl, soql) {
  let url = `/services/data/${API}/query/?q=${encodeURIComponent(soql)}`;
  const out = [];
  while (url) {
    const d = await sf(token, instanceUrl, url);
    out.push(...d.records);
    url = d.nextRecordsUrl || null;
  }
  return out;
}

(async () => {
  const { accessToken, instanceUrl } = await getSalesforceToken();

  /* ── revert ──────────────────────────────────────────────────────── */
  if (REVERT) {
    const manifest = JSON.parse(fs.readFileSync(REVERT, 'utf8'));
    console.log(`Reverting ${manifest.changed.length} Lead(s) from ${manifest.written_at}\n`);
    let done = 0;
    for (const c of manifest.changed) {
      if (!APPLY) { console.log(`  would restore ${c.id} ${c.email} -> ${JSON.stringify(c.old_how_did_you_hear)}`); continue; }
      await sf(accessToken, instanceUrl, `/services/data/${API}/sobjects/Lead/${c.id}`, {
        method: 'PATCH', body: JSON.stringify({ How_Did_You_Hear__c: c.old_how_did_you_hear }),
      });
      done++; console.log(`  restored ${c.id} ${c.email}`);
    }
    console.log(APPLY ? `\nReverted ${done}.` : `\nDRY RUN. Add --apply to restore.`);
    return;
  }

  if (!EMAILS) { console.error('Need --emails <file.json> (the provenance list) or --revert <manifest.json>'); process.exit(1); }
  const provenance = JSON.parse(fs.readFileSync(EMAILS, 'utf8'));
  const emails = [...new Set(provenance.map((r) => String(r.email || r).trim().toLowerCase()).filter(Boolean))];
  if (!emails.length) { console.error('provenance file contained no emails'); process.exit(1); }
  console.log(`Provenance: ${emails.length} address(es) our own form actually submitted.\n`);

  /* Chunked so a long list cannot blow the SOQL length limit. */
  const rows = [];
  for (let i = 0; i < emails.length; i += 100) {
    const chunk = emails.slice(i, i + 100).map((e) => `'${e.replace(/'/g, "\\'")}'`).join(',');
    rows.push(...await queryAll(accessToken, instanceUrl,
      `SELECT Id, Email, Name, Company, Status, IsConverted, CreatedDate,
              How_Did_You_Hear__c, Source_Bucket__c, utm_source__c
         FROM Lead WHERE Email IN (${chunk})`));
  }

  const toMark = [], skipped = [];
  for (const r of rows) {
    /* A CONVERTED Lead is an Account and possibly an Opportunity. Never
       touched, whatever the address says. */
    if (r.IsConverted) { skipped.push([r, 'CONVERTED — never touched']); continue; }
    if (r.Source_Bucket__c === 'Invalid / Test') { skipped.push([r, 'already Invalid / Test']); continue; }
    if (r.How_Did_You_Hear__c === MARKER) { skipped.push([r, 'already marked']); continue; }
    toMark.push(r);
  }

  console.log(`Matched ${rows.length} Salesforce Lead(s).`);
  console.log(`  to mark: ${toMark.length}`);
  console.log(`  skipped: ${skipped.length}\n`);

  if (skipped.length) {
    console.log('SKIPPED');
    skipped.forEach(([r, why]) => console.log(`  ${r.Id}  ${(r.Email || '').padEnd(34)} ${why}`));
    console.log('');
  }

  console.log('WOULD MARK  (How_Did_You_Hear__c -> ' + JSON.stringify(MARKER) + ', so Source_Bucket__c becomes "Invalid / Test")');
  console.log('  ' + 'ID'.padEnd(20) + 'EMAIL'.padEnd(34) + 'BUCKET NOW'.padEnd(18) + 'HOW_DID_YOU_HEAR NOW');
  toMark.forEach((r) => console.log('  ' + r.Id.padEnd(20) + (r.Email || '').padEnd(34) +
    String(r.Source_Bucket__c || '—').padEnd(18) + JSON.stringify(r.How_Did_You_Hear__c || null)));

  if (!APPLY) {
    console.log(`\nDRY RUN — nothing was written. Add --apply to perform ${toMark.length} update(s).`);
    return;
  }

  const manifest = { written_at: new Date().toISOString(), marker: MARKER, changed: [], failed: [] };
  for (const r of toMark) {
    try {
      await sf(accessToken, instanceUrl, `/services/data/${API}/sobjects/Lead/${r.Id}`, {
        method: 'PATCH', body: JSON.stringify({ How_Did_You_Hear__c: MARKER }),
      });
      manifest.changed.push({ id: r.Id, email: r.Email, old_how_did_you_hear: r.How_Did_You_Hear__c ?? null,
                              old_source_bucket: r.Source_Bucket__c ?? null });
      console.log(`  ✅ ${r.Id} ${r.Email}`);
    } catch (e) {
      manifest.failed.push({ id: r.Id, email: r.Email, error: e.message });
      console.warn(`  ⚠ ${r.Id} ${r.Email} — ${e.message}`);
    }
  }
  const out = path.join(process.cwd(), `sf-internal-test-manifest-${Date.now()}.json`);
  fs.writeFileSync(out, JSON.stringify(manifest, null, 2));
  console.log(`\nMarked ${manifest.changed.length}, failed ${manifest.failed.length}.`);
  console.log(`Manifest (needed to revert): ${out}`);
  console.log(`Revert with: node tools/sf-mark-internal-test-leads.js --revert ${out} --apply`);
})().catch((e) => { console.error('FAILED:', e.message); process.exit(1); });
