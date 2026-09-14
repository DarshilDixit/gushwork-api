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

/* ── Phase 2: classify from the cached page, one model at a time ────
   TAKES AN OPTIONAL GROUP FILTER. G1, G2 and G3 carry the whole of the
   evidence -- the proven false positives, the probable ones, and the recall
   check against the mechanism we already trust. G4 has no label and yields
   only a count, so it is run last and its absence would not change a
   decision. Ordering the work that way means a run that has to be cut short
   is cut short in the only place where it costs nothing. */
async function classify(modelId, onlyGroups) {
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
  let keys = [...pages.keys()];
  if (onlyGroups) {
    const { groups } = JSON.parse(fs.readFileSync(SP + '/groups.json', 'utf8'));
    const want = new Set();
    for (const g of onlyGroups) for (const r of groups[g]) want.add(r.key);
    keys = keys.filter((d) => want.has(d));
  }
  /* SCRAPEABLE DOMAINS FIRST. An unreadable page never reaches the API, so
     sorting them last means the expensive work starts immediately and a
     progress number means something. */
  keys.sort((a, b) => (pages.get(b).status === 'ok') - (pages.get(a).status === 'ok'));
  const todo = keys.filter((d) => !seen.has(d));
  console.error(`${modelId}: classifying ${todo.length}${onlyGroups ? ' in ' + onlyGroups.join('+') : ''} of ${pages.size}`);
  const fh = fs.openSync(outFile, 'a');
  await pool(todo, Number(process.env.CONCURRENCY || 10), async (domain) => {
    const v = await M.nonIcpClassifyDomain(domain);
    fs.writeSync(fh, JSON.stringify(v) + '\n');
    return true;
  });
  fs.closeSync(fh);
}

/* ── Phase 3: score ──────────────────────────────────────────────
   PRECISION AND THE ACTUAL ROWS. A percentage is not reviewable -- the
   whole point of scoring against history rather than running a shadow mode
   is that somebody reads the false positives and decides. So every flag in
   the two groups that matter is printed in full.

   G1 and G2 are the labels. G3 measures recall against the mechanism we
   already trust. G4 has no label at all and is reported as a count only,
   because a flag there costs nothing and proves nothing. */
function score() {
  const { groups, g3all } = JSON.parse(fs.readFileSync(SP + '/groups.json', 'utf8'));
  const byKey = new Map();
  for (const g of ['G1', 'G2', 'G3', 'G4']) for (const r of groups[g]) byKey.set(r.key, r);
  const listSet = new Set(g3all);

  /* THE CUSTOMER BYPASS, AS PRODUCTION ACTUALLY COMPUTES IT.

     Scoring the classifier alone overstates the risk, and by a lot. A flag
     is not a block: nonIcpVerdict checks the warehouse customer tables
     BEFORE blocking, and that check reads three tables where the G1 label
     here was built from one. So every flag is scored twice — once as the
     model saw it, and once as the system would have acted on it.

     Both numbers are reported on purpose. The bypass FAILS OPEN on a
     warehouse timeout, so the classifier-level number is the real exposure
     during an outage, and quoting only the system-level one would hide
     that. */
  const bypass = new Set(fs.existsSync(SP + '/bypass.json')
    ? JSON.parse(fs.readFileSync(SP + '/bypass.json', 'utf8')) : []);

  const L = [];
  const say = (s = '') => { L.push(s); console.log(s); };

  const pages = new Map(jsonl(SP + '/pages.jsonl').map((r) => [r.domain, r]));

  say('# Non-ICP model layer — validation against history');
  say('');
  say(`Domains scored: **${byKey.size}**. Page scraped ONCE and replayed to all three models, so the`);
  say('comparison is on identical bytes.');
  say('');
  const ps = {};
  for (const p of pages.values()) ps[p.status] = (ps[p.status] || 0) + 1;
  say('| scrape | domains |');
  say('|---|---|');
  for (const [k, v] of Object.entries(ps).sort((a, b) => b[1] - a[1])) say(`| ${k} | ${v} |`);
  say('');
  say(`**${ps.ok || 0} of ${pages.size} domains could be read at all** — ${Math.round(100 * (ps.ok || 0) / pages.size)}%.`);
  say('Everything else gets no verdict and blocks nobody.');
  say('');

  const summary = [];
  const detail = {};
  for (const m of MODELS) {
    const rows = jsonl(`${SP}/verdicts.${m}.jsonl`);
    if (!rows.length) continue;
    const flag = { G1: [], G2: [], G3: [], G4: [] };
    const cnt  = { G1: 0, G2: 0, G3: 0, G4: 0 };
    const seen = { G1: 0, G2: 0, G3: 0, G4: 0 };   // domains that got a real verdict
    let classified = 0;
    const listCaught = [], listMissed = [];
    for (const v of rows) {
      const rec = byKey.get(v.domain);
      if (!rec) continue;
      cnt[rec.group]++;
      if (v.source === 'llm') { classified++; seen[rec.group]++; }
      if (v.blocking === true) flag[rec.group].push({ ...v, rec, bypassed: bypass.has(v.domain) });
      if (listSet.has(v.domain)) (v.blocking === true ? listCaught : listMissed).push(v);
    }
    const blocks = (g) => flag[g].filter((x) => !x.bypassed).length;
    summary.push({ m, classified, total: rows.length, cnt, seen,
                   flagged: { G1: flag.G1.length, G2: flag.G2.length, G3: flag.G3.length, G4: flag.G4.length },
                   blocked: { G1: blocks('G1'), G2: blocks('G2'), G3: blocks('G3'), G4: blocks('G4') },
                   listCaught: listCaught.length, listMissed: listMissed.length });
    detail[m] = { flag, listCaught, listMissed };
  }

  say('## The headline');
  say('');
  say('**What the MODEL flagged** — before the customer bypass runs.');
  say('');
  say('| model | classified | **G1** (paying customers) | G2 (booked+showed) | G3 (list matches) | G4 (everything else) |');
  say('|---|---|---|---|---|---|');
  for (const s of summary) {
    say(`| ${s.m} | ${s.classified} | **${s.flagged.G1}** of ${s.seen.G1} | ${s.flagged.G2} of ${s.seen.G2} | ${s.flagged.G3} of ${s.seen.G3} | ${s.flagged.G4} of ${s.seen.G4} |`);
  }
  say('');
  say('**What would actually have been BLOCKED** — after the known-customer bypass, which reads the');
  say(`same three warehouse tables production reads (${bypass.size} domains).`);
  say('');
  say('| model | **G1 blocked** | G2 blocked | G3 blocked | G4 blocked |');
  say('|---|---|---|---|---|');
  for (const s of summary) {
    say(`| ${s.m} | **${s.blocked.G1}** | ${s.blocked.G2} | ${s.blocked.G3} | ${s.blocked.G4} |`);
  }
  say('');
  say('G1 is the gold standard: a flag there is a **proven** false positive, because the company paid us.');
  say('The bypass **fails open** on a warehouse timeout, so the first table is the real exposure during');
  say('an outage and the second is the exposure on a normal day. Neither replaces the other.');
  say('');

  say('## Recall against the mechanism we already trust');
  say('');
  say(`The brand-domain list matches **${listSet.size}** domains in our history. How many does the model`);
  say('find on its own?');
  say('');
  say('| model | caught | missed | recall |');
  say('|---|---|---|---|');
  for (const s of summary) {
    const t = s.listCaught + s.listMissed;
    say(`| ${s.m} | ${s.listCaught} | ${s.listMissed} | ${t ? Math.round(100 * s.listCaught / t) : 0}% |`);
  }
  say('');

  for (const s of summary) {
    const d = detail[s.m];
    say(`## ${s.m} — the rows`);
    say('');
    for (const [grp, label] of [['G1', 'PAYING CUSTOMERS — every row here is a proven false positive'],
                                ['G2', 'BOOKED AND SHOWED UP — read these, most are probably wrong']]) {
      say(`### ${grp}: ${label}`);
      say('');
      if (!d.flag[grp].length) { say('_None._'); say(''); continue; }
      say('| domain | judged | conf | company | emails | evidence |');
      say('|---|---|---|---|---|---|');
      for (const v of d.flag[grp].sort((a, b) => (b.confidence || 0) - (a.confidence || 0))) {
        const mark = v.bypassed ? ' _(bypassed — known customer)_' : '';
        say(`| \`${v.domain}\`${mark} | ${v.business_type} | ${v.confidence} | ${(v.rec.companies || []).join(' / ') || '—'} | ${(v.rec.emails || []).slice(0, 2).join(' ')} | ${String(v.evidence_quote || '').replace(/\|/g, '/').slice(0, 110)} |`);
      }
      say('');
    }
    /* The misses are where the brand list is load-bearing. */
    say('### Domain-list matches the model did NOT flag');
    say('');
    if (!d.listMissed.length) { say('_None._'); }
    else {
      say('| domain | why no flag | judged |');
      say('|---|---|---|');
      for (const v of d.listMissed) {
        const why = v.source === 'llm' ? 'read the site, judged otherwise' : (v.scrape_status || v.error || v.source);
        say(`| \`${v.domain}\` | ${why} | ${v.business_type || '—'}${v.confidence != null ? ' @' + v.confidence : ''} |`);
      }
    }
    say('');
  }

  /* Where the three models DISAGREE is the most useful page in the report:
     it is the shortlist a human has to arbitrate, and its size is the real
     measure of how non-deterministic this layer is in practice. */
  if (summary.length > 1) {
    const all = {};
    for (const m of MODELS) {
      for (const v of jsonl(`${SP}/verdicts.${m}.jsonl`)) {
        (all[v.domain] = all[v.domain] || {})[m] = v;
      }
    }
    const disagree = Object.entries(all).filter(([, byM]) => {
      const vals = MODELS.map((m) => byM[m] && byM[m].blocking === true);
      return vals.some((x) => x === true) && vals.some((x) => x === false);
    });
    say('## Where the three models disagree about BLOCKING');
    say('');
    say(`**${disagree.length} domains.** This is the practical size of the non-determinism, measured on`);
    say('identical input rather than argued about.');
    say('');
    say('| domain | ' + MODELS.join(' | ') + ' | group |');
    say('|---|---|---|---|---|');
    for (const [dom, byM] of disagree.slice(0, 80)) {
      const rec = byKey.get(dom);
      say(`| \`${dom}\` | ` + MODELS.map((m) => byM[m] ? `${byM[m].blocking ? '**FLAG**' : 'keep'} ${byM[m].business_type || ''}` : '—').join(' | ') + ` | ${rec ? rec.group : '?'} |`);
    }
    if (disagree.length > 80) say(`\n_…and ${disagree.length - 80} more._`);
    say('');
  }

  fs.writeFileSync(SP + '/report.md', L.join('\n'));
  fs.writeFileSync(SP + '/report.json', JSON.stringify({ summary, detail }, null, 1));
  console.error('\nwritten: ' + SP + '/report.md');
}

/* ── Phase 4: load verdicts into the production cache ────────────
   ONE-TIME, AND IT REFUSES TO RUN IF THE VERDICTS DO NOT MATCH WHAT IS
   LIVE. A verdict row is only meaningful alongside the model and prompt
   that produced it -- loading Sonnet rows into an Opus deployment, or
   rows from an older prompt, would put verdicts into production that
   nothing on this deployment would reproduce and nobody could audit.
   So the precondition is checked here rather than trusted.

   ONLY `source = llm` ROWS ARE LOADED. The llm_unreachable rows carry a
   six-hour TTL, so loading them would expire tonight anyway -- and worse,
   they would SUPPRESS a fresh scrape for those six hours. The bulk scrape
   ran 14-way concurrent and under-read: a careful one-at-a-time pass
   reads roughly twice as many of the same domains. Those domains are
   better off being re-tried by the live warm path than pinned to a
   failure this tool already knows was pessimistic.

   ON CONFLICT DO NOTHING: anything production has already written wins.
   A row from live traffic is fresher than anything on disk here. */
async function loadIntoCache() {
  const { Pool } = require('pg');
  const url = process.env.DATABASE_PUBLIC_URL || process.env.DATABASE_URL;
  if (!url) { console.error('DATABASE_PUBLIC_URL or DATABASE_URL required'); process.exit(1); }
  const model = process.env.NON_ICP_LLM_MODEL;
  if (!model) { console.error('NON_ICP_LLM_MODEL must be set so the precondition can be checked'); process.exit(1); }

  const rows = jsonl(`${SP}/verdicts.${model}.jsonl`).filter((v) => v.source === 'llm');
  if (!rows.length) { console.error(`no llm verdicts on disk for ${model}`); process.exit(1); }

  /* PRECONDITION. Every row must name the live model and the live prompt
     version. One mismatch aborts the whole load -- a partial load is worse
     than none, because the half that landed is invisible. */
  const src2 = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
  const livePrompt = (src2.match(/NON_ICP_PROMPT_VERSION = '([^']+)'/) || [])[1];
  const badModel  = rows.filter((v) => v.model_id !== model);
  const badPrompt = rows.filter((v) => v.prompt_version !== livePrompt);
  if (badModel.length || badPrompt.length) {
    console.error(`REFUSING TO LOAD. live model=${model} prompt=${livePrompt}`);
    console.error(`  rows with a different model_id : ${badModel.length}`);
    console.error(`  rows with a different prompt   : ${badPrompt.length}`);
    process.exit(1);
  }
  console.error(`precondition OK — ${rows.length} rows, all ${model} / ${livePrompt}`);

  const pool = new Pool({ connectionString: url, ssl: { rejectUnauthorized: false }, max: 4 });
  let loaded = 0, skipped = 0;
  for (let i = 0; i < rows.length; i += 200) {
    const chunk = rows.slice(i, i + 200);
    const vals = [], params = [];
    chunk.forEach((v, n) => {
      const b = n * 13;
      vals.push(`($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6},$${b+7},$${b+8},$${b+9},$${b+10},$${b+11},$${b+12},$${b+13},NOW())`);
      params.push(v.domain, v.business_type || null, v.blocking === true,
        Number.isFinite(v.confidence) ? v.confidence : null, v.evidence_quote || null,
        v.reason || null, v.source, v.model_id || null, v.prompt_version || null,
        v.page_text_sha256 || null, v.page_url_used || null, v.page_text_chars || null,
        v.scrape_status || null);
    });
    const r = await pool.query(
      `INSERT INTO non_icp_domain_verdicts
         (domain, business_type, blocking, confidence, evidence_quote, reason, source,
          model_id, prompt_version, page_text_sha256, page_url_used, page_text_chars,
          scrape_status, checked_at)
       VALUES ${vals.join(',')}
       ON CONFLICT (domain) DO NOTHING`, params);
    loaded += r.rowCount; skipped += chunk.length - r.rowCount;
    process.stderr.write(`  ${i + chunk.length}/${rows.length}\n`);
  }
  const tot = await pool.query('SELECT count(*) n, count(*) FILTER (WHERE blocking) b FROM non_icp_domain_verdicts');
  console.error(`loaded ${loaded}, skipped ${skipped} (already present)`);
  console.error(`cache now: ${tot.rows[0].n} verdicts, ${tot.rows[0].b} blocking`);
  await pool.end();
}

const cmd = process.argv[2];
(async () => {
  if (cmd === 'scrape')        await scrape();
  else if (cmd === 'classify') await classify(process.argv[3], process.argv[4] ? process.argv[4].split(',') : null);
  else if (cmd === 'score')    score();
  else if (cmd === 'load')     await loadIntoCache();
  else { console.error('usage: scrape | classify <model> | score | load'); process.exit(1); }
})().catch((err) => { console.error(err); process.exit(1); });
