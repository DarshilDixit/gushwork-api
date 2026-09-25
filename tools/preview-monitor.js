/* ============================================================================
   preview-monitor.js -- run THIS BRANCH's /monitor/next against live data,
   on a developer machine, without deploying and without writing anything.

   There is no staging here, so a dashboard change would otherwise be seen
   with real numbers for the first time in production. This closes that gap:

     /monitor/next          this branch's page, built by monitor-next.js
     /monitor/overview      this branch's overviewReport, LIFTED out of
                            index.js and run on connections that are READ ONLY
                            at the database (default_transaction_read_only)
     /monitor/duplicates    this branch's duplicatesReport, lifted the same
                            way -- the branch changed it (is_internal), and a
                            proxy to production would show the OLD query
     every other GET        proxied to production, unchanged -- the routes the
       /monitor/*, /health  page reads already exist there
     anything not a GET     REFUSED with 405. The Lead magnet tab has two write
                            buttons; in a preview they can never fire.

   It never requires index.js, so no migration, cron, sweep or alert runs.

   Run (needs the Postgres service's public URL AND gushwork-api's token):
     railway run -s Postgres bash -c 'PUB="$DATABASE_PUBLIC_URL" railway run --service gushwork-api bash -c "DATABASE_URL=\"\$PUB\" node tools/preview-monitor.js"'
   then open http://localhost:4411/monitor/next?token=<MONITOR_TOKEN>
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
const L = new Function([
  liftDecl('const DASH_TZ'), liftDecl('const BOT_RE'), liftDecl('const DROPOFF_STAGE_SQL'), liftDecl('const DROPOFF_SOURCE_SQL'),
  liftDecl('const OVERVIEW_VIEWS'), liftDecl('const OVERVIEW_WEBHOOK_SOURCES'), liftDecl('const RECOVERED_BOOKINGS_SQL'),
  liftDecl('function overviewWindowsSql'), liftDecl('async function overviewReport'), liftDecl('function dropoffTodayEtOf'),
  liftDecl('function dropoffAddDays'), liftDecl('function dropoffStep'),
  liftDecl('const ELV_EXCLUDED_DOMAINS'), liftDecl('const INTERNAL_TEST_EMAILS'), liftDecl('const INTERNAL_STAGING_HOSTS'),
  liftDecl('function internalLeadSqlClause'), liftDecl('async function duplicatesReport'),
  'return { overviewReport, duplicatesReport, DASH_TZ };',
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
  app.get('/monitor/next', (req, res, next) => { monitorNext.reload(); next(); });   /* edits show on the next load */
  monitorNext.mount(app, { tz: L.DASH_TZ });
  app.get('/monitor/overview', async (req, res) => {
    if (req.query.token !== TOKEN) return res.status(401).json({ error: 'Unauthorized' });
    try { res.json(await L.overviewReport(db, { view: req.query.view, asof: req.query.asof })); }
    catch (err) { res.status(err.status || 500).json({ error: err.message }); }
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
    console.log(`preview on http://localhost:${PORT}/monitor/next?token=${TOKEN.slice(0, 3)}… (reads ${UPSTREAM}, database read-only)`);
    if (process.env.PREVIEW_READY_FILE) fs.writeFileSync(process.env.PREVIEW_READY_FILE, JSON.stringify({ port: PORT, token: TOKEN }));
  });
}

module.exports = { liftDecl, L };
if (require.main === module) start();
