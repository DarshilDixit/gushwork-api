/* ============================================================
   tools/non-icp-release.js — release a company the non-ICP block got wrong.

   WHY IT EXISTS. specialtyprogramgroup.com (a 300-person insurance group)
   was blocked three times in two weeks and the contact wrote in to ask why
   nobody had replied. The only remedy anyone had was NON_ICP_LLM_BLOCK=false,
   which stops ALL model blocking. This releases ONE company.

   WHAT --apply DOES, in one transaction:
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
    between('const NON_ICP_BLOCK_ENABLED =', '\n/* The verdict, for one lead.');
  return (new Function('process', body + '\nreturn { partnerStackCustomerKey, NON_ICP_DOMAINS, nonIcpMatchHost, nonIcpBrandIndustry };'))({ env: process.env });
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
    const client = await db.connect();
    try {
      /* The table is created by the SERVER on boot (db.js), not by this tool,
         so there is one definition of it. Before the deploy that adds it, a
         dry run says so and carries on; --apply refuses. */
      const hasTable = (await client.query(`SELECT to_regclass('public.non_icp_exemptions') AS t`)).rows[0].t !== null;
      if (!hasTable) {
        if (apply) throw new Error('non_icp_exemptions does not exist yet. It is created when the server deploys the change that adds this tool. Nothing was done.');
        console.log('NOTE: the non_icp_exemptions table does not exist yet (the server creates it when this change deploys). Dry run continues without it.\n');
      }
      if (apply) await client.query('BEGIN');
      for (const d of domains) {
        const ex = hasTable
          ? await client.query('SELECT note, released_by, released_at FROM non_icp_exemptions WHERE domain = $1', [d])
          : { rows: [] };
        const v = await client.query('SELECT business_type, confidence::float AS confidence, source, checked_at FROM non_icp_domain_verdicts WHERE domain = $1', [d]);
        const brand = Object.prototype.hasOwnProperty.call(M.NON_ICP_DOMAINS, d) ? M.NON_ICP_DOMAINS[d] : null;
        const sel = `FROM leads WHERE non_icp_reason = $1 AND (non_icp_blocked IS TRUE OR non_icp_llm_flagged IS TRUE)`;
        const c = await client.query(
          `SELECT COUNT(*)::int AS leads, COUNT(DISTINCT lower(email))::int AS people,
                  COUNT(*) FILTER (WHERE non_icp_blocked IS TRUE)::int AS blocked,
                  COUNT(*) FILTER (WHERE booking_uid IS NOT NULL)::int AS booked, MIN(created_at) AS first_at, MAX(created_at) AS last_at ${sel}`, [d]);
        const row = c.rows[0];
        console.log(`── ${d}`);
        console.log(ex.rows[0] ? `   already released ${String(ex.rows[0].released_at).slice(0, 10)} by ${ex.rows[0].released_by || '?'}` : '   not released yet');
        console.log(v.rows[0]
          ? `   cached verdict: ${v.rows[0].business_type} ${v.rows[0].confidence} (${v.rows[0].source}), checked ${String(v.rows[0].checked_at).slice(0, 10)}`
          : '   no cached verdict');
        if (brand) {
          console.log(`   WARNING: this domain is on the brand list (${brand}) and is shared by MANY people. Releasing it lifts the block for ALL of them, for good.`);
          if (!allowBrand) {
            if (apply) throw new Error(`${d} is a brand-list domain. Nothing was done. If you really mean to release every ${brand} address, add --allow-brand.`);
            console.log('   --apply will REFUSE this one unless you add --allow-brand.');
          }
        }
        console.log(`   leads that ${apply ? 'were' : 'would be'} unblocked: ${row.leads} (${row.people} ${row.people === 1 ? 'person' : 'people'}), ${row.blocked} blocked, ${row.booked} already booked`);
        if (list && row.leads) {
          const l = await client.query(`SELECT email, created_at, step_reached, (booking_uid IS NOT NULL) AS booked ${sel} ORDER BY created_at`, [d]);
          for (const x of l.rows) console.log(`     ${x.email || '(no email)'}  ${String(x.created_at).slice(0, 10)}  step ${x.step_reached}${x.booked ? '  BOOKED' : ''}`);
        }
        if (apply) {
          await client.query(
            `INSERT INTO non_icp_exemptions (domain, note, released_by) VALUES ($1, $2, $3)
             ON CONFLICT (domain) DO UPDATE SET note = EXCLUDED.note, released_by = EXCLUDED.released_by, released_at = NOW()`,
            [d, note, by]);
          const u = await client.query(`UPDATE leads SET non_icp_blocked = FALSE, non_icp_llm_flagged = FALSE WHERE non_icp_reason = $1 AND (non_icp_blocked IS TRUE OR non_icp_llm_flagged IS TRUE)`, [d]);
          console.log(`   RELEASED. ${u.rowCount} lead row(s) cleared.`);
        }
        console.log('');
      }
      if (apply) await client.query('COMMIT');
    } catch (e) {
      if (apply) await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }
    if (apply) {
      console.log('The live server picks the release up within a minute.');
      console.log('Not done by this tool: Salesforce (backfill-sf.js), the AWS mirror, Meta, and telling the person. A human replies.');
    } else {
      console.log('Add --apply to do it. Add --list to see who they are.');
    }
  } finally {
    await db.end();
  }
}

main().catch((e) => { console.error('release failed:', e.message); process.exit(1); });
