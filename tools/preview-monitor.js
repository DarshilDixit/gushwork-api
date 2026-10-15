/* ============================================================================
   preview-monitor.js -- run THIS BRANCH's /monitor against live data,
   on a developer machine, without deploying and without writing anything.

   There is no staging here, so a dashboard change would otherwise be seen
   with real numbers for the first time in production. This closes that gap:

     /monitor               this branch's page, built by monitor-next.js
                            (/monitor/next redirects to it, as in production)
     /monitor/classic       production's classic page: /monitor/classic once
                            PR D is deployed, /monitor before that (where the
                            classic lived until the switch)
     /monitor/overview      this branch's overviewReport, LIFTED out of
                            index.js and run on connections that are READ ONLY
                            at the database (default_transaction_read_only)
     /monitor/non-icp-review  this branch's nonIcpReviewReport (the Blocked tab's
                            "Needs a look"), lifted the same way: a new route has
                            no production version to proxy to until it deploys
     /monitor/duplicates    this branch's duplicatesReport, lifted the same
                            way -- the branch changed it (is_internal), and a
                            proxy to production would show the OLD query
     /monitor/dropoff       this branch's dropoffReport, lifted by REGION like
                            tools/fire-dropoff-digest.js does, on the same
                            read-only pool -- PR D leaves our own tests out
     every other GET        proxied to production, unchanged -- the routes the
       /monitor/*, /health  page reads already exist there
     anything not a GET     REFUSED with 405. The Lead magnet tab has two write
                            buttons; in a preview they can never fire.

   It never requires index.js, so no migration, cron, sweep or alert runs.

   Run (needs the Postgres service's public URL AND gushwork-api's token):
     railway run -s Postgres bash -c 'PUB="$DATABASE_PUBLIC_URL" railway run --service gushwork-api bash -c "DATABASE_URL=\"\$PUB\" node tools/preview-monitor.js"'
   then open http://localhost:4411/monitor?token=<MONITOR_TOKEN>
   (the token is printed masked; tools/check-monitor-layout.mjs reads it itself).

   Not mounted anywhere and not called by anything.
   ============================================================================ */
const fs = require('fs');
const path = require('path');
const express = require('express');
const { Pool } = require('pg');

const ROOT = path.join(__dirname, '..');
const src = fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8');
const PORT = +(process.env.PREVIEW_PORT || 4411);
const UPSTREAM = process.env.PREVIEW_UPSTREAM || 'https://gushwork-api-production.up.railway.app';
const TOKEN = process.env.MONITOR_TOKEN || '';

/* Lift one top-level declaration whole. For a function the brace match starts
   at the BODY: starting at the first "{" would stop inside a destructured
   parameter like ({ view, asof } = {}) and truncate the function. */
function liftDecl(decl) {
  const i = src.indexOf('\n' + decl);
  if (i === -1) throw new Error('not found in index.js: ' + decl);
  let j;
  if (/^(async )?function /.test(decl)) {
    const m = /\)\s*\{/.exec(src.slice(i));
    j = i + m.index + m[0].length - 1;
  } else {
    j = src.indexOf('{', i);
    const semi = src.indexOf(';', i);
    if (j === -1 || (semi !== -1 && semi < j)) return src.slice(i + 1, semi + 1);
  }
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
function between(a, b) {
  const i = src.indexOf(a); if (i === -1) throw new Error('not found in index.js: ' + a);
  const j = src.indexOf(b, i); if (j === -1) throw new Error('end not found in index.js: ' + b);
  return src.slice(i, j);
}
/* dropoffReport reads the global pool, so it is lifted into its own scope
   with the read-only pool handed in -- never the proxied production route. */
const dropoffLift = new Function('pool', [
  liftDecl('const DASH_TZ'), liftDecl('const ELV_EXCLUDED_DOMAINS'), liftDecl('const INTERNAL_TEST_EMAILS'),
  liftDecl('const INTERNAL_STAGING_HOSTS'), liftDecl('function internalLeadSqlClause'),
  between('const DROPOFF_STAGE_SQL', 'async function visitorsReport'),
  'return { dropoffReport };',
].join('\n'));
/* The Blocked tab's "Needs a look" feed: new in this branch, so it must be lifted, not proxied. */
const reviewLift = new Function('process', [
  liftDecl('const ELV_EXCLUDED_DOMAINS'), liftDecl('const INTERNAL_TEST_EMAILS'),
  liftDecl('const INTERNAL_STAGING_HOSTS'), liftDecl('function internalLeadSqlClause'),
  liftDecl('const NON_ICP_REVIEW_MIN_STAFF'), liftDecl('const NON_ICP_AGENT_TITLE'), liftDecl('const NON_ICP_LEVEL_LABEL'),
  liftDecl('const NON_ICP_BUYER_MIN_STAFF'), liftDecl('function nonIcpRoleHint'), liftDecl('async function nonIcpReviewReport'), liftDecl('async function nonIcpReleasedReport'),
  'return { nonIcpReviewReport, nonIcpRoleHint, nonIcpReleasedReport };',
].join('\n'));
const L = new Function([
  liftDecl('const DASH_TZ'), liftDecl('const BOT_RE'), liftDecl('const DROPOFF_STAGE_SQL'), liftDecl('const DROPOFF_SOURCE_SQL'),
  liftDecl('const OVERVIEW_VIEWS'), liftDecl('const OVERVIEW_WEBHOOK_SOURCES'), liftDecl('function recoveredBookingsSql'), liftDecl('const RECOVERED_BOOKINGS_SQL'),
  liftDecl('function overviewWindowsSql'), liftDecl('async function overviewReport'), liftDecl('function dropoffTodayEtOf'),
  liftDecl('function dropoffAddDays'), liftDecl('function dropoffStep'),
  liftDecl('const ELV_EXCLUDED_DOMAINS'), liftDecl('const INTERNAL_TEST_EMAILS'), liftDecl('const INTERNAL_STAGING_HOSTS'),
  liftDecl('function internalLeadSqlClause'), liftDecl('async function duplicatesReport'),
  liftDecl('const WEBSITE_REASON_LABELS'), liftDecl('const META_WITHHELD_LABELS'),
  'return { overviewReport, duplicatesReport, DASH_TZ, LABELS: { website: WEBSITE_REASON_LABELS, meta: META_WITHHELD_LABELS } };',
].join('\n'))();

function start() {
  const url = process.env.DATABASE_URL;
  if (!url) { console.error('DATABASE_URL is not set -- see the header for the railway command.'); process.exit(1); }
  if (!TOKEN) { console.error('MONITOR_TOKEN is not set -- run it under the gushwork-api service, see the header.'); process.exit(1); }
  /* READ ONLY AT THE DATABASE, for every connection this pool ever opens. */
  const db = new Pool({ connectionString: url, ssl: url.includes('localhost') ? false : { rejectUnauthorized: false }, max: 4,
    options: '-c default_transaction_read_only=on -c statement_timeout=20000' });
  const monitorNext = require(path.join(ROOT, 'monitor-next.js'));
  const app = express();
  app.use((req, res, next) => {
    if (req.method !== 'GET' && req.method !== 'HEAD') return res.status(405).json({ error: 'The preview is read-only: ' + req.method + ' ' + req.path + ' was not sent.' });
    next();
  });
  /* THE PAGE IS REBUILT FROM A FRESH monitor-next.js ON EVERY LOAD, not only
     its CSS and scripts. reload() re-reads monitor/, but the MARKUP lives in
     page(), which was bound once at start -- so on 26 Sept the preview served
     new CSS around old sidebar markup, and a full layout run tested a mix
     that could never ship. Re-requiring the module re-reads both. */
  const MN = path.join(ROOT, 'monitor-next.js');
  app.get('/monitor', (req, res) => {
    if (req.query.token !== TOKEN) return res.status(401).send('401 — Unauthorized.');
    delete require.cache[require.resolve(MN)];
    res.set('Cache-Control', 'no-store');
    res.type('html').send(require(MN).page({ token: req.query.token, tz: L.DASH_TZ, labels: L.LABELS }));
  });
  monitorNext.mount(app, { tz: L.DASH_TZ, labels: L.LABELS });   /* the font route and the /monitor/next redirect; /monitor above wins */
  /* THE CLASSIC, from production, wherever production keeps it: after the
     switch that is /monitor/classic, before it the classic WAS /monitor. A
     404 is the only thing that falls back -- never a 200 from the new page. */
  app.get('/monitor/classic', async (req, res) => {
    try {
      const q = req.originalUrl.slice(req.originalUrl.indexOf('?') >= 0 ? req.originalUrl.indexOf('?') : req.originalUrl.length);
      let r = await fetch(UPSTREAM + '/monitor/classic' + q);
      if (r.status === 404) r = await fetch(UPSTREAM + '/monitor' + q);
      res.status(r.status).type('html').send(await r.text());
    } catch (err) { res.status(502).send('upstream: ' + err.message); }
  });
  app.get('/monitor/overview', async (req, res) => {
    if (req.query.token !== TOKEN) return res.status(401).json({ error: 'Unauthorized' });
    try { res.json(await L.overviewReport(db, { view: req.query.view, asof: req.query.asof })); }
    catch (err) { res.status(err.status || 500).json({ error: err.message }); }
  });
  const dropoffReport = dropoffLift(db).dropoffReport;
  app.get('/monitor/dropoff', async (req, res) => {
    if (req.query.token !== TOKEN) return res.status(401).json({ error: 'Unauthorized' });
    try { res.json(await dropoffReport({ from: req.query.from, to: req.query.to, grain: req.query.grain, source: req.query.source, mode: req.query.mode })); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });
  const reviewReport = reviewLift(process).nonIcpReviewReport;
  const roleHint = reviewLift(process).nonIcpRoleHint;
  /* /monitor/leads is NOT lifted (it is a 400-line route), so production's copy is proxied. This branch changes it
     in two ways production does not yet have: every row carries role_hint / role_reason, and an emails filter. The
     preview adds the first from the REAL nonIcpRoleHint, and does the second in memory (reads every page of
     blocked leads from production, keeps the requested addresses, re-pages by 25). The SQL of the real filter is
     checked separately, by running its condition read-only. */
  app.get('/monitor/leads', async (req, res) => {
    try {
      const want = req.query.emails ? String(req.query.emails).split(',').map((x) => x.trim().toLowerCase()).filter(Boolean) : null;
      const q = new URLSearchParams(req.query); q.delete('emails');
      const get = async (page) => { q.set('page', String(page)); const r = await fetch(UPSTREAM + '/monitor/leads?' + q.toString()); if (r.status !== 200) throw new Error('upstream ' + r.status); return r.json(); };
      const shim = (rows) => rows.map((x) => { const h = roleHint(x.enriched_title, x.enriched_seniority, x.enriched_departments, x.enriched_company_size); return { ...x, role_hint: h.hint, role_reason: h.reason }; });
      if (!want) { const d = await get(req.query.page || 1); return res.json({ ...d, leads: shim(d.leads || []) }); }
      const first = await get(1); let rows = first.leads || [];
      for (let p = 2; p <= first.pages; p++) rows = rows.concat((await get(p)).leads || []);
      rows = rows.filter((x) => want.includes(String(x.email || '').toLowerCase()));
      const page = Math.max(1, +req.query.page || 1);
      res.json({ total: rows.length, page, pages: Math.max(1, Math.ceil(rows.length / 25)), leads: shim(rows.slice((page - 1) * 25, page * 25)) });
    } catch (err) { res.status(502).json({ error: err.message }); }
  });
  /* the released list gained tries and where-from fields, so it is lifted too, not proxied */
  app.get('/monitor/non-icp-released', async (req, res) => {
    if (req.query.token !== TOKEN) return res.status(401).json({ error: 'Unauthorized' });
    try {
      res.json(await reviewLift(process).nonIcpReleasedReport(db));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });
  app.get('/monitor/non-icp-review', async (req, res) => {
    if (req.query.token !== TOKEN) return res.status(401).json({ error: 'Unauthorized' });
    try { res.json(await reviewReport(db)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });
  app.get('/monitor/duplicates', async (req, res) => {
    if (req.query.token !== TOKEN) return res.status(401).json({ error: 'Unauthorized' });
    try { res.json(await L.duplicatesReport(db)); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });
  /* Everything else the page reads: the live routes, unchanged. */
  app.get(/^\/(monitor(\/.*)?|health)$/, async (req, res) => {
    try {
      const r = await fetch(UPSTREAM + req.originalUrl, { headers: { accept: req.headers.accept || '*/*' } });
      res.status(r.status);
      const ct = r.headers.get('content-type'); if (ct) res.set('content-type', ct);
      res.send(Buffer.from(await r.arrayBuffer()));
    } catch (err) { res.status(502).json({ error: 'upstream: ' + err.message }); }
  });
  app.listen(PORT, () => {
    console.log(`preview on http://localhost:${PORT}/monitor?token=${TOKEN.slice(0, 3)}… (reads ${UPSTREAM}, database read-only)`);
    if (process.env.PREVIEW_READY_FILE) fs.writeFileSync(process.env.PREVIEW_READY_FILE, JSON.stringify({ port: PORT, token: TOKEN }));
  });
}

module.exports = { liftDecl, L, dropoffLift };
if (require.main === module) start();
