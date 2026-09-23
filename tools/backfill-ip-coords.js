/* ============================================================
   backfill-ip-coords.js — fill in coordinates for leads that were
   resolved BEFORE ip_latitude / ip_longitude existed.

   Those rows have a city and a timezone and no point, so they are
   complete in every table and invisible on the map. The map says so on
   screen rather than drawing fewer dots quietly, but saying so is not the
   same as fixing it.

   Only touches rows that already have ip_checked_at AND no coordinates,
   so it cannot re-ask for a domain that is already complete and cannot
   invent a place for one that was never resolved.

   LIFTS resolveIpGeo out of index.js rather than reimplementing it, like
   tools/fire-alert.js and tools/non-icp-validate.js, so what it writes is
   what production would have written.

   Dry run by default. --apply writes.

   Run:  railway run node tools/backfill-ip-coords.js
         railway run node tools/backfill-ip-coords.js --apply

   Not mounted anywhere and not called by anything.
   ============================================================ */
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const ROOT = path.join(__dirname, '..');
const src = fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8');
const between = (a, b) => { const i = src.indexOf(a), j = src.indexOf(b, i); return src.slice(i, j); };

const M = (new Function('process', 'fetch', 'AbortController', 'setTimeout', 'clearTimeout',
  between('const IP_GEO_ENABLED', '/* Its own targeted write, NOT syncToAWS')
  + '\nreturn { resolveIpGeo };'
))({ env: process.env }, fetch, AbortController, setTimeout, clearTimeout);

const APPLY = process.argv.includes('--apply');

(async () => {
  const url = process.env.DATABASE_URL;
  if (!url) { console.error('DATABASE_URL is not set. Try: railway run node tools/backfill-ip-coords.js'); process.exit(1); }
  const pool = new Pool({ connectionString: url, ssl: url.includes('localhost') ? false : { rejectUnauthorized: false } });

  const { rows } = await pool.query(`
    SELECT session_id, ip_address, ip_city
      FROM leads
     WHERE ip_checked_at IS NOT NULL
       AND ip_address IS NOT NULL
       AND (ip_latitude IS NULL OR ip_longitude IS NULL)
     ORDER BY created_at DESC`);

  console.log(`${rows.length} lead(s) resolved without coordinates.${APPLY ? '' : '  DRY RUN — pass --apply to write.'}\n`);
  let done = 0, missed = 0;
  for (const r of rows) {
    const geo = await M.resolveIpGeo(r.ip_address);
    if (!geo || geo.ip_latitude === null || geo.ip_longitude === null) {
      missed++;
      console.log(`  ${(r.ip_city || '?').padEnd(18)} ${r.ip_address.padEnd(16)} -> no coordinates`);
      continue;
    }
    console.log(`  ${(r.ip_city || '?').padEnd(18)} ${r.ip_address.padEnd(16)} -> ${geo.ip_latitude}, ${geo.ip_longitude}`);
    if (APPLY) {
      /* Coordinates ONLY. The rest of the row was resolved at the time and
         is not re-decided here -- a provider that has since changed its
         mind about a city must not silently rewrite history. */
      await pool.query(
        `UPDATE leads SET ip_latitude = $2, ip_longitude = $3, updated_at = NOW()
          WHERE session_id = $1`, [r.session_id, geo.ip_latitude, geo.ip_longitude]);
      done++;
    }
    /* Polite to a free service, and well inside its 1,000/day. */
    await new Promise((res) => setTimeout(res, 250));
  }
  console.log(`\n${APPLY ? `${done} updated` : 'dry run'}, ${missed} without coordinates.`);
  await pool.end();
})().catch((err) => { console.error('[backfill-ip-coords] FAILED:', err); process.exit(1); });
