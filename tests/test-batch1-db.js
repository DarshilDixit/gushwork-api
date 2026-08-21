/* ============================================================
   Batch 1 — database integration.

   Runs the REAL initDB() from db.js and the REAL SQL from index.js
   against a live Postgres. Everything else in batch 1 is pure logic and
   is covered by test-batch1.js; this file exists because the funnel query
   uses a FULL OUTER JOIN, an interval cast and a regex operator, none of
   which a syntax check can validate.

   Run:  DATABASE_URL=... node tests/test-batch1-db.js
   ============================================================ */

const fs = require('fs');
const path = require('path');

process.env.DATABASE_URL = process.env.DATABASE_URL
  || fs.readFileSync(path.join(__dirname, '..', '.pguri'), 'utf8').trim();

const { pool, initDB } = require('../db');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) pass++;
  else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
function eq(name, a, b) {
  ok(name, JSON.stringify(a) === JSON.stringify(b), `got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`);
}

/* Pull the funnel SQL out of the shipped route so this test cannot drift
   from what actually runs. */
const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
function extractFunnelSql() {
  const i = src.indexOf("app.get('/monitor/funnel'");
  const start = src.indexOf('WITH s AS (', i);
  const end = src.indexOf('`, [String(days), BOT_RE])', start);
  if (start === -1 || end === -1) throw new Error('funnel SQL not found in index.js');
  return src.slice(start, end).trim();
}
const BOT_RE = (src.match(/const BOT_RE = "([^"]+)"/) || [])[1];

(async () => {
  try {
    /* ---- 1. initDB runs clean on an empty database ---- */
    await initDB();
    ok('initDB: completes on a fresh database', true);

    /* ---- 2. and is idempotent, which is how Railway actually uses it ---- */
    await initDB();
    await initDB();
    ok('initDB: idempotent across restarts', true);

    const tables = await pool.query(
      `SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY 1`
    );
    const names = tables.rows.map((r) => r.table_name);
    for (const t of ['leads', 'enrichment_data', 'lead_magnet_leads', 'form_sessions']) {
      ok(`schema: ${t} exists`, names.includes(t), names.join(','));
    }

    /* ---- 3. form_sessions shape ---- */
    const cols = await pool.query(
      `SELECT column_name, data_type, is_nullable FROM information_schema.columns
        WHERE table_name='form_sessions' ORDER BY ordinal_position`
    );
    const colNames = cols.rows.map((c) => c.column_name);
    for (const c of ['session_id', 'page_url', 'referrer', 'utm_source', 'utm_medium',
                     'utm_campaign', 'utm_content', 'utm_term', 'user_agent', 'hits',
                     'created_at', 'updated_at']) {
      ok(`form_sessions: has ${c}`, colNames.includes(c), colNames.join(','));
    }
    const sid = cols.rows.find((c) => c.column_name === 'session_id');
    eq('form_sessions: session_id is NOT NULL', sid.is_nullable, 'NO');

    const idx = await pool.query(`SELECT indexname FROM pg_indexes WHERE tablename='form_sessions'`);
    const idxNames = idx.rows.map((r) => r.indexname);
    ok('form_sessions: created_at indexed', idxNames.some((n) => n.includes('created_idx')), idxNames.join(','));
    ok('form_sessions: session_id unique', idxNames.some((n) => n.includes('session_id_key')), idxNames.join(','));

    /* ---- 4. the exact /session upsert, run for real ---- */
    const SESSION_SQL = `INSERT INTO form_sessions
         (session_id, page_url, referrer, utm_source, utm_medium, utm_campaign, utm_content, utm_term, user_agent)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (session_id) DO UPDATE SET
         hits         = form_sessions.hits + 1,
         page_url     = COALESCE(form_sessions.page_url,     EXCLUDED.page_url),
         referrer     = COALESCE(form_sessions.referrer,     EXCLUDED.referrer),
         utm_source   = COALESCE(form_sessions.utm_source,   EXCLUDED.utm_source),
         utm_medium   = COALESCE(form_sessions.utm_medium,   EXCLUDED.utm_medium),
         utm_campaign = COALESCE(form_sessions.utm_campaign, EXCLUDED.utm_campaign),
         utm_content  = COALESCE(form_sessions.utm_content,  EXCLUDED.utm_content),
         utm_term     = COALESCE(form_sessions.utm_term,     EXCLUDED.utm_term),
         user_agent   = COALESCE(form_sessions.user_agent,   EXCLUDED.user_agent),
         updated_at   = NOW()`;

    ok('session SQL: matches the shipped route', src.includes('hits         = form_sessions.hits + 1'));

    await pool.query(SESSION_SQL, ['sess-a', 'https://gushwork.ai/demo', 'direct',
      'facebook', 'paid', 'q3-b2b', 'carousel-b', 'seo agency', 'Mozilla/5.0 Chrome/120']);
    let r = await pool.query(`SELECT * FROM form_sessions WHERE session_id='sess-a'`);
    eq('session: first write recorded', r.rows.length, 1);
    eq('session: hits starts at 1', r.rows[0].hits, 1);
    eq('session: utm_campaign stored', r.rows[0].utm_campaign, 'q3-b2b');

    // A repeat call must increment, not duplicate, and must not blank fields.
    await pool.query(SESSION_SQL, ['sess-a', null, null, null, null, null, null, null, null]);
    r = await pool.query(`SELECT * FROM form_sessions WHERE session_id='sess-a'`);
    eq('session: repeat call does not duplicate', r.rows.length, 1);
    eq('session: hits incremented', r.rows[0].hits, 2);
    eq('session: existing utm preserved on a blank repeat', r.rows[0].utm_campaign, 'q3-b2b');
    eq('session: existing page_url preserved', r.rows[0].page_url, 'https://gushwork.ai/demo');

    // A long ads URL — the fbc truncation case — must store whole.
    const longUrl = 'https://www.gushwork.ai/lp/seo?utm_campaign=' + 'c'.repeat(200) + '&fbclid=' + 'A'.repeat(300);
    await pool.query(SESSION_SQL, ['sess-long', longUrl.slice(0, 1000), null, null, null, null, null, null, null]);
    r = await pool.query(`SELECT page_url FROM form_sessions WHERE session_id='sess-long'`);
    ok('session: 1000-char URL stored intact', r.rows[0].page_url.includes('fbclid=' + 'A'.repeat(300)),
       String(r.rows[0].page_url.length));

    /* ---- 5. seed leads and run the REAL funnel query ---- */
    const uuid = (n) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
    await pool.query(
      `INSERT INTO leads (session_id, email, submitted_at, booking_uid, created_at) VALUES
         ($1,'a@acme.com', NOW(), 'bk-1', NOW()),
         ($2,'b@acme.com', NOW(), NULL,   NOW()),
         ($3,NULL,         NULL,  NULL,   NOW()),
         ($4,'d@acme.com', NULL,  NULL,   NOW() - INTERVAL '3 days')`,
      [uuid(1), uuid(2), uuid(3), uuid(4)]
    );
    for (const s of ['sess-b', 'sess-c']) {
      await pool.query(SESSION_SQL, [s, 'https://gushwork.ai/demo', null, null, null, null, null, null, 'Mozilla/5.0']);
    }
    // Bots must be excluded at read time, not dropped at write time.
    await pool.query(SESSION_SQL, ['sess-bot', null, null, null, null, null, null, null,
      'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)']);
    await pool.query(SESSION_SQL, ['sess-ua-null', null, null, null, null, null, null, null, null]);

    const FUNNEL_SQL = extractFunnelSql();
    ok('funnel: BOT_RE extracted from source', !!BOT_RE, String(BOT_RE));
    const funnel = await pool.query(FUNNEL_SQL, ['30', BOT_RE]);
    ok('funnel: query executes against Postgres', true);
    ok('funnel: returns rows', funnel.rows.length >= 1, String(funnel.rows.length));

    const today = funnel.rows.find((x) => Number(x.sessions) > 0);
    ok('funnel: today has sessions', !!today);
    // sess-a, sess-long, sess-b, sess-c, sess-ua-null = 5 human; sess-bot excluded.
    eq('funnel: bots excluded from the human count', Number(today.sessions), 5);
    eq('funnel: bots counted separately', Number(today.bot_sessions), 1);
    eq('funnel: step1 counts leads with an email', Number(today.step1), 2);
    eq('funnel: submitted counted', Number(today.submitted), 2);
    eq('funnel: booked counted', Number(today.booked), 1);

    // A day with leads but no sessions must still appear — that is the whole
    // point of the FULL OUTER JOIN, and the reason historical days are visible.
    const older = funnel.rows.find((x) => Number(x.sessions) === 0 && Number(x.step1) > 0);
    ok('funnel: a leads-only day still appears (FULL OUTER JOIN works)', !!older,
       JSON.stringify(funnel.rows.map((x) => [x.day, x.sessions, x.step1])));

    // Boundary values for the days parameter.
    for (const d of ['1', '180']) {
      await pool.query(FUNNEL_SQL, [d, BOT_RE]);
      ok(`funnel: runs with days=${d}`, true);
    }

    /* ---- 6. leads table must be untouched by all of this ---- */
    const leadCols = await pool.query(
      `SELECT column_name FROM information_schema.columns WHERE table_name='leads'`
    );
    const ln = leadCols.rows.map((c) => c.column_name);
    for (const c of ['session_id', 'email', 'submitted_at', 'booking_uid', 'website_check_reason', 'fbc']) {
      ok(`leads: ${c} still present`, ln.includes(c));
    }
    const leadCount = await pool.query('SELECT COUNT(*) FROM leads');
    eq('leads: only the four seeded rows — /session wrote nothing here', Number(leadCount.rows[0].count), 4);

  } catch (err) {
    fail++;
    failures.push('EXCEPTION: ' + err.message);
  }

  console.log('');
  console.log(`  passed: ${pass}`);
  console.log(`  failed: ${fail}`);
  if (failures.length) { console.log(''); failures.forEach((f) => console.log('  ✗ ' + f)); }
  console.log('');
  await pool.end();
  process.exit(fail === 0 ? 0 : 1);
})();
