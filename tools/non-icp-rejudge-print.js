/* ============================================================
   tools/non-icp-rejudge-print.js — re-judge every cached print_sign
   verdict under the CURRENT prompt, and show what moved.

   WHY IT EXISTS. On 14 Oct 2026 coverlabel.com (a label and sticker
   manufacturer, 14 staff) was judged "Print / sign shop" at 95% and its
   Meta events were withheld. Swapnil: print/sign should withhold Meta for
   LOCAL printers only. The prompt now says so (v2-2026-10-14), but the
   cache ignores prompt_version: a page verdict ("llm") is trusted for 180
   days, so every domain already judged print_sign would keep its old verdict
   until it expired. This replays them.

   WHAT IT DOES. For each non_icp_domain_verdicts row whose business_type
   is print_sign and whose source is 'llm' (a name-only verdict expires in
   six hours on its own):
     1. reads the page live, with the production fetcher;
     2. classifies it with the PRODUCTION classifier, lifted out of index.js
        rather than copied, so the prompt, schema, enum and floor are what
        the server uses;
     3. prints before -> after, and how many still suppress Meta.

   DRY RUN BY DEFAULT: it reads the database and the web and calls the model,
   and writes nothing. --apply writes the new verdict through the production
   write function, ONLY where the re-read produced a real page verdict
   (source 'llm'). A domain whose site would not load this time keeps its old
   row: "we could not check" is never recorded as a new answer.

   Prints domains, business types and confidence only. No lead data.

   Run (the model key lives on gushwork-api, the database on Postgres):
     PUB=$(railway variables --service Postgres --kv | grep '^DATABASE_PUBLIC_URL=' | cut -d= -f2-)
     railway run -s gushwork-api env DATABASE_URL="$PUB" node tools/non-icp-rejudge-print.js
     ... --apply        write the changed verdicts
     ... --only a.com,b.com   a subset

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

function lift(pool) {
  const body =
    between('function damerauLevenshtein(a, b)', '\n// Derived from FREE_EMAIL_DOMAINS') +
    between('const FREE_EMAIL_DOMAINS = [', '\napp.set(') +
    between('const MULTI_PART_SUFFIXES = new Set([', 'function isMarketplaceHost') +
    between('function analyzeSubstance(html, finalHost)', 'function findPhrase(list, visibleLower)') +
    between('function detectCheckWall(html)', 'function flipWww(hostname)') +
    between('const NON_ICP_BLOCK_ENABLED =', '/* The verdict, for one lead.') +
    between('const NON_ICP_LLM_ENABLED =', '/* ── The cache ──') +
    between('async function nonIcpWriteVerdictRow(v)', '/* The domains one lead could be judged on');
  return (new Function('process', 'fetch', 'require', 'console', 'pool', body + `
    return { nonIcpClassifyDomain, nonIcpWriteVerdictRow, nonIcpTypeSuppressesMeta,
             NON_ICP_PROMPT_VERSION, NON_ICP_LLM_CONFIDENCE_FLOOR };`
  ))({ env: { ...process.env, NON_ICP_LLM_ENABLED: 'true' } }, global.fetch, require, console, pool);
}

async function runPool(items, n, fn) {
  const out = new Array(items.length);
  let i = 0;
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, async () => {
    while (i < items.length) {
      const idx = i++;
      try { out[idx] = await fn(items[idx]); } catch (e) { out[idx] = { error: e && e.message }; }
    }
  }));
  return out;
}

async function main() {
  const apply = process.argv.includes('--apply');
  const onlyAt = process.argv.indexOf('--only');
  const only = onlyAt !== -1 ? new Set(process.argv[onlyAt + 1].split(',')) : null;
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is not set (use the Postgres service DATABASE_PUBLIC_URL)');
  if (!process.env.ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY is not set (run through railway run -s gushwork-api)');

  const db = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 2 });
  try {
    const M = lift(db);
    let rows = (await db.query(
      `SELECT domain, business_type, confidence::float AS confidence, prompt_version
         FROM non_icp_domain_verdicts
        WHERE business_type = 'print_sign' AND source = 'llm' ORDER BY domain`)).rows;
    if (only) rows = rows.filter((r) => only.has(r.domain));
    console.log(`${rows.length} cached print_sign page verdicts. New prompt ${M.NON_ICP_PROMPT_VERSION}. ${apply ? 'APPLYING' : 'DRY RUN, nothing is written'}.`);

    const floor = M.NON_ICP_LLM_CONFIDENCE_FLOOR;
    const results = await runPool(rows, 6, async (r) => ({ r, v: await M.nonIcpClassifyDomain(r.domain) }));

    const table = [];
    let suppressBefore = 0, suppressAfter = 0, changed = 0, kept = 0, written = 0;
    for (const x of results) {
      const { r, v } = x;
      const before = r.confidence >= floor;               // a print_sign verdict at/above the floor suppresses Meta
      if (before) suppressBefore++;
      if (!v || v.source !== 'llm') {                       // could not re-read: the old row stays, and still counts
        kept++; if (before) suppressAfter++;
        table.push({ domain: r.domain, before: `print_sign ${r.confidence}`, after: 'COULD NOT RE-READ (old row kept)', suppresses: before ? 'yes' : 'no' });
        continue;
      }
      const after = M.nonIcpTypeSuppressesMeta(v.business_type) && v.confidence >= floor;
      if (after) suppressAfter++;
      if (v.business_type !== 'print_sign') changed++;
      table.push({ domain: r.domain, before: `print_sign ${r.confidence}`, after: `${v.business_type} ${v.confidence}`,
        suppresses: `${before ? 'yes' : 'no'} -> ${after ? 'yes' : 'no'}` });
      if (apply) { await M.nonIcpWriteVerdictRow(v); written++; }
    }
    console.table(table);
    console.log(JSON.stringify({ domains: rows.length, still_print_sign: rows.length - changed - kept, moved_to_another_type: changed,
      could_not_re_read: kept, suppress_meta_before: suppressBefore, suppress_meta_after: suppressAfter, rows_written: written }, null, 2));
  } finally {
    await db.end();
  }
}

main().catch((e) => { console.error('rejudge failed:', e.message); process.exit(1); });
