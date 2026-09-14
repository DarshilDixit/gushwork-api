/* ============================================================
   non-icp-validate.js — score the model layer against HISTORY.

   WHY THIS EXISTS INSTEAD OF A SHADOW MODE. A shadow run measures the
   model against nothing: it tells you what it WOULD have flagged and
   leaves somebody to decide, lead by lead, whether each one was right.
   The labels we need already exist and they are not opinions —

     a lead that became a PAYING CUSTOMER cannot be non-ICP. Any flag
     here is a proven false positive, full stop.
     a lead that BOOKED AND SHOWED UP is a lead an AE chose to spend
     half an hour on. A flag here is probably wrong and is worth reading.

   So this runs the real classifier over every domain that has ever
   reached step 1, joins to those outcomes, and prints precision plus the
   actual rows. It is the whole of the pre-launch evidence.

   IT LIFTS THE REAL FUNCTIONS OUT OF index.js rather than copying them,
   the same rule the test suites follow. A validation run against a
   duplicate of the classifier would measure the duplicate.

   THE PAGE IS SCRAPED ONCE AND ALL THREE MODELS SEE THE SAME BYTES.
   nonIcpFetchPageText is rebound to a disk cache after the first pass.
   Everything downstream of it -- the prompt, the schema, the parsing, the
   enum, the confidence floor, the blocking decision -- is the production
   code path, unmodified. Without this the models would be compared on
   different scrapes of sites that change between requests, and the
   false-positive difference would be partly noise.

   NOT MOUNTED, NOT IMPORTED, NOT CALLED BY ANYTHING. Like fire-alert.js
   and backfill-sf.js, it runs when a human decides to run it.

   Usage:
     SP=<scratch dir> node tools/non-icp-validate.js scrape
     SP=<scratch dir> node tools/non-icp-validate.js classify <model-id>
     SP=<scratch dir> node tools/non-icp-validate.js score
   ============================================================ */

const fs   = require('fs');
const path = require('path');

const SP = process.env.SP;
if (!SP) { console.error('SP=<scratch dir> is required'); process.exit(1); }

const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
function between(a, b, s = src) {
  const i = s.indexOf(a);
  if (i === -1) throw new Error('marker not found: ' + a);
  const j = s.indexOf(b, i);
  if (j === -1) throw new Error('end marker not found: ' + b);
  return s.slice(i, j);
}

/* The real thing, lifted. Note what is NOT stubbed: the prompt, the output
   schema, the enum, the confidence floor, the parse, and the code that
   decides `blocking` from the business type. Only the page source and the
   process env are supplied from here. */
function liftClassifier(modelId) {
  const body =
    between('function damerauLevenshtein(a, b)', '\n// Derived from FREE_EMAIL_DOMAINS') +
    between('const FREE_EMAIL_DOMAINS = [', '\napp.set(') +
    between('const MULTI_PART_SUFFIXES = new Set([', 'function isMarketplaceHost') +
    between('function analyzeSubstance(html, finalHost)', 'function findPhrase(list, visibleLower)') +
    between('function detectCheckWall(html)', 'function flipWww(hostname)') +
    between('const NON_ICP_BLOCK_ENABLED =', '/* The verdict, for one lead.') +
    between('const NON_ICP_LLM_ENABLED =', '/* ── The cache ──');
  return (new Function('process', 'fetch', 'require', 'console', body + `
    return {
      nonIcpClassifyDomain, nonIcpFetchPageText, nonIcpTypeBlocks,
      NON_ICP_BUSINESS_TYPES, NON_ICP_PROMPT_VERSION, NON_ICP_LLM_CONFIDENCE_FLOOR,
      partnerStackCustomerKey, nonIcpMatchHost,
      /* Function declarations are mutable bindings, which is what lets the
         scrape be done once and replayed to three models byte for byte. */
      setPageFetcher(f) { nonIcpFetchPageText = f; },
    };`
  ))({ env: { ...process.env, NON_ICP_LLM_MODEL: modelId, NON_ICP_LLM_ENABLED: 'true' } },
     global.fetch, require, console);
}

const MODELS = ['claude-haiku-4-5', 'claude-sonnet-5', 'claude-opus-5'];
const jsonl  = (f) => fs.existsSync(f)
  ? fs.readFileSync(f, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l))
  : [];

async function pool(items, n, fn) {
  const out = new Array(items.length);
  let i = 0, done = 0;
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, async () => {
    while (i < items.length) {
      const idx = i++;
      try { out[idx] = await fn(items[idx], idx); }
      catch (err) { out[idx] = { error: err && err.message }; }
      if (++done % 100 === 0) process.stderr.write(`  ${done}/${items.length}\n`);
    }
  }));
  return out;
}

/* ── Phase 1: scrape every domain once ───────────────────────────── */
async function scrape() {
  const { groups } = JSON.parse(fs.readFileSync(SP + '/groups.json', 'utf8'));
  const all = [].concat(groups.G1, groups.G2, groups.G3, groups.G4);
  const M = liftClassifier(MODELS[0]);
  const outFile = SP + '/pages.jsonl';
  const seen = new Set(jsonl(outFile).map((r) => r.domain));
  const todo = all.filter((r) => !seen.has(r.key));
  console.error(`scraping ${todo.length} of ${all.length} domains (${seen.size} already cached)`);
  const fh = fs.openSync(outFile, 'a');
  await pool(todo, 14, async (rec) => {
    const page = await M.nonIcpFetchPageText(rec.key);
    fs.writeSync(fh, JSON.stringify({ domain: rec.key, ...page }) + '\n');
    return true;
  });
  fs.closeSync(fh);
  const rows = jsonl(outFile);
  const by = {};
  for (const r of rows) by[r.status] = (by[r.status] || 0) + 1;
  console.error('scrape status:', JSON.stringify(by));
}

/* ── Phase 2: classify from the cached page, one model at a time ──── */
async function classify(modelId) {
  const pages = new Map(jsonl(SP + '/pages.jsonl').map((r) => [r.domain, r]));
  const M = liftClassifier(modelId);
  /* THE REBIND. Everything else in nonIcpClassifyDomain is production
     code; this is the only substitution, and it replays bytes already
     fetched rather than inventing any. */
  M.setPageFetcher(async (domain) => {
    const p = pages.get(domain);
    if (!p) return { status: 'unreachable', detail: 'not scraped' };
    return p;
  });
  const outFile = `${SP}/verdicts.${modelId}.jsonl`;
  const seen = new Set(jsonl(outFile).map((r) => r.domain));
  const todo = [...pages.keys()].filter((d) => !seen.has(d));
  console.error(`${modelId}: classifying ${todo.length} of ${pages.size}`);
  const fh = fs.openSync(outFile, 'a');
  await pool(todo, 10, async (domain) => {
    const v = await M.nonIcpClassifyDomain(domain);
    fs.writeSync(fh, JSON.stringify(v) + '\n');
    return true;
  });
  fs.closeSync(fh);
}

/* ── Phase 3: score ──────────────────────────────────────────────── */
function score() {
  const { groups } = JSON.parse(fs.readFileSync(SP + '/groups.json', 'utf8'));
  const byKey = new Map();
  for (const g of ['G1', 'G2', 'G3', 'G4']) for (const r of groups[g]) byKey.set(r.key, r);

  const report = { models: {} };
  for (const m of MODELS) {
    const rows = jsonl(`${SP}/verdicts.${m}.jsonl`);
    if (!rows.length) continue;
    const g = { G1: [], G2: [], G3: [], G4: [] };
    const counts = { G1: 0, G2: 0, G3: 0, G4: 0 };
    let scraped = 0, errored = 0;
    for (const v of rows) {
      const rec = byKey.get(v.domain);
      if (!rec) continue;
      counts[rec.group]++;
      if (v.source === 'llm') scraped++; else errored++;
      if (v.blocking === true) g[rec.group].push({ ...v, rec });
    }
    report.models[m] = {
      total: rows.length, classified: scraped, no_verdict: errored,
      counts,
      flagged: { G1: g.G1.length, G2: g.G2.length, G3: g.G3.length, G4: g.G4.length },
      rows: g,
    };
  }
  fs.writeFileSync(SP + '/report.json', JSON.stringify(report, null, 1));

  for (const [m, r] of Object.entries(report.models)) {
    console.log('\n══════ ' + m + ' ══════');
    console.log(`classified ${r.classified} / ${r.total}  (no verdict: ${r.no_verdict})`);
    for (const grp of ['G1', 'G2', 'G3', 'G4']) {
      const label = { G1: 'PAYING CUSTOMERS', G2: 'booked and showed', G3: 'domain-list matches', G4: 'everything else' }[grp];
      console.log(`  ${grp} ${label.padEnd(22)} ${String(r.flagged[grp]).padStart(4)} flagged of ${r.counts[grp]}`);
    }
  }
  console.log('\nfull rows in ' + SP + '/report.json');
}

const cmd = process.argv[2];
(async () => {
  if (cmd === 'scrape')        await scrape();
  else if (cmd === 'classify') await classify(process.argv[3]);
  else if (cmd === 'score')    score();
  else { console.error('usage: scrape | classify <model> | score'); process.exit(1); }
})().catch((err) => { console.error(err); process.exit(1); });
