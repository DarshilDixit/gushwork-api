/* ============================================================
   tools/non-icp-release.js — release a company the non-ICP block got wrong.

   WHY IT EXISTS. specialtyprogramgroup.com (a 300-person insurance group)
   was blocked three times in two weeks and the contact wrote in to ask why
   nobody had replied. The only remedy anyone had was NON_ICP_LLM_BLOCK=false,
   which stops ALL model blocking. This releases ONE company.

   THE SAME CODE AS THE BLOCKED TAB'S RELEASE BUTTON. This tool lifts
   nonIcpReleasePlan / nonIcpReleaseApply out of index.js, so the command
   line and the dashboard cannot drift apart. The command line can do one
   thing the dashboard cannot: release a shared brand-list domain, with
   --allow-brand.

   WHAT --apply DOES, in one transaction per company:
     1. adds the company to non_icp_exemptions, so from then on neither the
        brand list nor the model blocks, flags or withholds Meta for it (the
        live server picks it up within a minute);
     2. clears the block and the model flag on that company's existing leads,
        so they stop showing on the Blocked tab and stop being treated as
        blocked anywhere that reads the column.
   It keeps non_icp_reason and non_icp_source on those leads, as the record of
   what decided.

   WHAT IT DOES NOT DO, said plainly:
     - it does NOT push the leads to Salesforce (a blocked lead never went).
       backfill-sf.js does that, once someone decides to run it;
     - it does NOT touch the AWS mirror (gw_form_leads), so the dialer feed
       still says blocked for those rows (the dialer does not read it yet);
     - it does NOT fire Meta for them, because the event would be weeks late;
     - it does NOT message the person. A human does that.

   DRY RUN BY DEFAULT: reads the database, writes nothing, prints counts only.
   --list adds each lead's email, step and date, because a human has to reply
   to them. --apply writes. --note "why" and --by "name" are recorded.

   A RELEASE IS PER DOMAIN, SO A SHARED DOMAIN IS A BIG ONE. farmersagent.com
   and allstate.com belong to thousands of agents; releasing one lifts the
   block for every one of them, for good. So a domain on the brand list is
   REFUSED unless you add --allow-brand, and the dry run says why. A company's
   own domain (specialtyprogramgroup.com) is the normal case.

   The domain is keyed through partnerStackCustomerKey, lifted out of
   index.js, exactly like every other non-ICP key, so agents.farmers.com and
   farmers.com are one company and a free mailbox is refused.

   Run (the Postgres service's DATABASE_PUBLIC_URL):
     PUB=$(railway variables --service Postgres --kv | grep '^DATABASE_PUBLIC_URL=' | cut -d= -f2-)
     DATABASE_URL="$PUB" node tools/non-icp-release.js specialtyprogramgroup.com
     DATABASE_URL="$PUB" node tools/non-icp-release.js specialtyprogramgroup.com --list
     DATABASE_URL="$PUB" node tools/non-icp-release.js specialtyprogramgroup.com --apply --by darshil --note "insurance group, wants AEO"

   Not mounted, not called by anything.
   ============================================================ */
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
function between(a, b) {
  const i = src.indexOf(a);
  if (i === -1) throw new Error('marker not found: ' + a);
  const j = src.indexOf(b, i);
  if (j === -1) throw new Error('end marker not found: ' + b);
  return src.slice(i, j);
}

function lift() {
  const body =
    between('function damerauLevenshtein(a, b)', '\n// Derived from FREE_EMAIL_DOMAINS') +
    between('const FREE_EMAIL_DOMAINS = [', '\napp.set(') +
    between('const MULTI_PART_SUFFIXES = new Set([', 'function isMarketplaceHost') +
    between('const NON_ICP_BLOCK_ENABLED =', '\n/* The verdict, for one lead.') +
    between('function nonIcpReleaseDomainKey', '/* ── The warm path');
  return (new Function('process', body + '\nreturn { partnerStackCustomerKey, nonIcpReleasePlan, nonIcpReleaseApply };'))({ env: process.env });
}

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i !== -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : null;
}

async function main() {
  const apply = process.argv.includes('--apply');
  const list = process.argv.includes('--list');
  const allowBrand = process.argv.includes('--allow-brand');
  const note = argValue('--note');
  const by = argValue('--by');
  const flagValues = new Set([note, by].filter(Boolean));
  const inputs = process.argv.slice(2).filter((a) => !a.startsWith('--') && !flagValues.has(a));
  if (!inputs.length) throw new Error('give at least one domain, e.g. node tools/non-icp-release.js company.com');
  if (inputs.length > 10) throw new Error('at most 10 domains at a time');
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is not set (use the Postgres service DATABASE_PUBLIC_URL)');

  const M = lift();
  const domains = [];
  for (const raw of inputs) {
    const key = M.partnerStackCustomerKey(raw);
    if (!key) throw new Error(`"${raw}" is not a company domain (a free mailbox, an address or junk). Nothing was done.`);
    if (!domains.includes(key)) domains.push(key);
  }

  const db = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 2 });
  try {
    console.log(apply ? 'APPLYING' : 'DRY RUN, nothing is written', '\n');
    /* The table is created by the SERVER on boot (db.js), not by this tool,
       so there is one definition of it. Before the deploy that adds it, a
       dry run says so and carries on; --apply refuses. */
    const hasTable = (await db.query(`SELECT to_regclass('public.non_icp_exemptions') AS t`)).rows[0].t !== null;
    if (!hasTable) {
      if (apply) throw new Error('non_icp_exemptions does not exist yet. It is created when the server deploys the change that adds this tool. Nothing was done.');
      console.log('NOTE: the non_icp_exemptions table does not exist yet (the server creates it when this change deploys). Dry run continues without it.\n');
    }

    /* READ EVERYTHING FIRST, and refuse before writing anything, so a bad
       third domain cannot leave the first two released. */
    const plans = [];
    for (const d of domains) plans.push(await M.nonIcpReleasePlan(db, d, { skipExemptions: !hasTable }));
    if (apply && !allowBrand) {
      const bad = plans.find((p) => p.brand);
      if (bad) throw new Error(`${bad.domain} is a brand-list domain (${bad.brand}). Nothing was done. If you really mean to release every ${bad.brand} address, add --allow-brand.`);
    }

    for (const p of plans) {
      const d = p.domain, l = p.leads;
      console.log(`── ${d}`);
      console.log(p.released ? `   already released ${String(p.released.released_at).slice(0, 10)} by ${p.released.released_by || '?'}` : '   not released yet');
      console.log(p.verdict
        ? `   cached verdict: ${p.verdict.business_type} ${p.verdict.confidence} (${p.verdict.source}), checked ${String(p.verdict.checked_at).slice(0, 10)}`
        : '   no cached verdict');
      if (p.brand) {
        console.log(`   WARNING: this domain is on the brand list (${p.brand}) and is shared by MANY people. Releasing it lifts the block for ALL of them, for good.`);
        if (!allowBrand) console.log('   --apply will REFUSE this one unless you add --allow-brand.');
      }
      console.log(`   leads that ${apply ? 'will be' : 'would be'} unblocked: ${l.leads} (${l.people} ${l.people === 1 ? 'person' : 'people'}), ${l.blocked} blocked, ${l.booked} already booked`);
      if (list && l.leads) {
        const rows = await db.query(
          `SELECT email, created_at, step_reached, (booking_uid IS NOT NULL) AS booked FROM leads
            WHERE non_icp_reason = $1 AND (non_icp_blocked IS TRUE OR non_icp_llm_flagged IS TRUE) ORDER BY created_at`, [d]);
        for (const x of rows.rows) console.log(`     ${x.email || '(no email)'}  ${String(x.created_at).slice(0, 10)}  step ${x.step_reached}${x.booked ? '  BOOKED' : ''}`);
      }
      if (apply) {
        const r = await M.nonIcpReleaseApply(db, { domain: d, note, by: by || 'command line', allowBrand });
        console.log(`   RELEASED. ${r.released_leads} lead row(s) cleared.`);
      }
      console.log('');
    }
    if (apply) {
      console.log('The live server picks the release up within a minute.');
      console.log('Not done by this tool: Salesforce (backfill-sf.js), the AWS mirror, Meta, a Slack post, and telling the person. A human replies.');
    } else {
      console.log('Add --apply to do it. Add --list to see who they are.');
    }
  } finally {
    await db.end();
  }
}

main().catch((e) => { console.error('release failed:', e.message); process.exit(1); });
