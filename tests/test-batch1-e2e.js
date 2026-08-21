/* ============================================================
   Batch 1 — end to end.

   Boots the REAL index.js as a child process against a real Postgres and
   drives it over HTTP. This is the only test that proves /session, /partial
   and /submit still work together after the changes: the unit tests read
   functions, the db test reads SQL, but neither runs the express stack.

   No outbound credentials are set, so Slack / Meta / Salesforce / Apollo /
   ELV all no-op — which is exactly the fail-open path we want exercised.

   Run:  node tests/test-batch1-e2e.js   (DATABASE_URL must be set)
   ============================================================ */

const { spawn } = require('child_process');
const path = require('path');
const crypto = require('crypto');

const PORT = 3999;
const BASE = `http://127.0.0.1:${PORT}`;

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) pass++;
  else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
function eq(name, a, b) {
  ok(name, JSON.stringify(a) === JSON.stringify(b), `got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function post(route, body) {
  const res = await fetch(BASE + route, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0 Chrome/120 e2e' },
    body: JSON.stringify(body),
  });
  let json = null;
  try { json = await res.json(); } catch { /* non-JSON */ }
  return { status: res.status, json };
}

(async () => {
  const child = spawn('node', [path.join(__dirname, '..', 'index.js')], {
    env: {
      ...process.env,
      PORT: String(PORT),
      MONITOR_TOKEN: 'e2e-token',
      ALLOWED_ORIGIN: 'https://www.gushwork.ai',
      NODE_ENV: 'test',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let bootLog = '';
  child.stdout.on('data', (d) => { bootLog += d; });
  child.stderr.on('data', (d) => { bootLog += d; });

  try {
    // Wait for the server to answer /health.
    let up = false;
    for (let i = 0; i < 60; i++) {
      await sleep(250);
      try {
        const r = await fetch(BASE + '/health');
        if (r.ok) { up = true; break; }
      } catch { /* not listening yet */ }
    }
    ok('boot: server starts and answers /health', up, bootLog.slice(-600));
    if (!up) throw new Error('server never came up');

    ok('boot: form_sessions table initialised', /Form-sessions table ready/.test(bootLog), bootLog.slice(-400));
    ok('boot: no unhandled startup error', !/Failed to start/.test(bootLog), bootLog.slice(-400));

    /* ---- /session now persists ---- */
    const sid = crypto.randomUUID();
    const s1 = await post('/session', {
      session_id: sid,
      page_url: 'https://www.gushwork.ai/demo?utm_source=facebook',
      referrer: 'https://facebook.com/',
      utm_source: 'facebook',
      utm_medium: 'paid',
      utm_campaign: 'q3-b2b',
    });
    eq('/session: returns 200', s1.status, 200);
    eq('/session: replies ok', s1.json, { ok: true });

    const missing = await post('/session', {});
    eq('/session: still rejects a missing session_id', missing.status, 400);

    await sleep(400); // the write happens after the reply, by design

    const funnel = await fetch(`${BASE}/monitor/funnel?token=e2e-token&days=7`);
    const fjson = await funnel.json();
    eq('/monitor/funnel: returns 200', funnel.status, 200);
    ok('/monitor/funnel: reports the visit', fjson.total_sessions_recorded >= 1, JSON.stringify(fjson).slice(0, 300));
    ok('/monitor/funnel: reports when tracking began', !!fjson.session_tracking_live_since);
    ok('/monitor/funnel: rows present', Array.isArray(fjson.rows) && fjson.rows.length >= 1);

    const noAuth = await fetch(`${BASE}/monitor/funnel?token=wrong`);
    eq('/monitor/funnel: rejects a bad token', noAuth.status, 401);

    /* ---- /session is idempotent and never touches leads ---- */
    await post('/session', { session_id: sid, page_url: 'https://www.gushwork.ai/demo' });
    await sleep(300);
    const f2 = await (await fetch(`${BASE}/monitor/funnel?token=e2e-token&days=7`)).json();
    eq('/session: a repeat visit does not create a second session', f2.total_sessions_recorded, fjson.total_sessions_recorded);

    const leadsBefore = await (await fetch(`${BASE}/monitor/leads?token=e2e-token`)).json();
    const countBefore = (leadsBefore.leads || leadsBefore.rows || []).length;

    /* ---- /partial: the long-URL path that fbc truncation lives on ---- */
    // 600 is a stress value, not a typical fbclid — the point is to prove the
    // cap itself moved. The realistic case is the landing URL below, which
    // carries the fbclid plus a full UTM set and clears 500 on its own.
    const fbclid = 'A'.repeat(600);
    const longLanding = 'https://www.gushwork.ai/lp/seo?utm_campaign=' + 'c'.repeat(180) + '&fbclid=' + fbclid;
    const fbc = 'fb.1.' + Date.now() + '.' + fbclid;
    const p1 = await post('/partial', {
      session_id: sid,
      email: 'founder@examplecorp.com',
      website: 'examplecorp.com',
      sell_to: 'B2B',
      page_url: longLanding,
      landing_page: longLanding,
      fbc,
      fbp: 'fb.1.123.456',
      utm_source: 'facebook',
    });
    eq('/partial: returns 200', p1.status, 200);

    /* ---- /submit ---- */
    const s2 = await post('/submit', {
      session_id: sid,
      email: 'founder@examplecorp.com',
      website: 'examplecorp.com',
      sell_to: 'B2B',
      first_name: 'Ada',
      last_name: 'Lovelace',
      phone: '+91 63886 39290',
      company: 'Example Corp',
      page_url: longLanding,
      landing_page: longLanding,
      fbc,
      website_check_failed: false,
      website_check_reason: 'content_clean',
    });
    eq('/submit: returns 200', s2.status, 200);

    /* ---- the long values survived the round trip ---- */
    const leads = await (await fetch(`${BASE}/monitor/leads?token=e2e-token`)).json();
    const rows = leads.leads || leads.rows || [];
    const row = rows.find((l) => l.email === 'founder@examplecorp.com');
    ok('/submit: lead recorded', !!row, JSON.stringify(rows).slice(0, 300));
    if (row) {
      ok('fbc: stored without truncation', row.fbc === fbc, `len ${row.fbc ? row.fbc.length : 'null'} vs ${fbc.length}`);
      ok('fbc: longer than the old 500 cap', fbc.length > 500, String(fbc.length));
      ok('landing_page: present in the monitor payload', typeof row.landing_page === 'string',
         'got ' + typeof row.landing_page);
      ok('landing_page: realistic ads URL exceeds the old cap', longLanding.length > 500, String(longLanding.length));
      ok('landing_page: fbclid survived the round trip',
         typeof row.landing_page === 'string' && row.landing_page.includes('fbclid=' + fbclid),
         `stored len ${row.landing_page ? row.landing_page.length : 'n/a'} of ${longLanding.length}`);
    }

    ok('leads: exactly one new lead row (session write did not leak in)',
       rows.length === countBefore + 1, `${countBefore} -> ${rows.length}`);

    /* ---- booking integrity: this session DID submit, so no alert ---- */
    const before = bootLog.length;
    await post('/booking-confirmed', {
      session_id: sid, booking_uid: 'bk-e2e-1', start_time: '2026-09-01T10:00:00Z', event_type: 'demo',
    });
    await sleep(300);
    ok('booking: no false alert for a session that completed the form',
       !/Booking with no completed submit/.test(bootLog.slice(before)), bootLog.slice(before).slice(0, 400));

    /* ---- booking integrity: a session that never submitted MUST alert ---- */
    const sid2 = crypto.randomUUID();
    await post('/session', { session_id: sid2, page_url: 'https://www.gushwork.ai/demo' });
    await post('/partial', { session_id: sid2, email: 'ghost@examplecorp.com', sell_to: 'B2B' });
    await sleep(300);
    const before2 = bootLog.length;
    await post('/booking-confirmed', { session_id: sid2, booking_uid: 'bk-e2e-2', event_type: 'demo' });
    await sleep(500);
    ok('booking: alerts when a booking arrives with no /submit',
       /Booking with no completed submit/.test(bootLog.slice(before2)), bootLog.slice(before2).slice(0, 500));
    ok('booking: the alert names the arriving route',
       /\[\/booking-confirmed\]/.test(bootLog.slice(before2)));

    /* ---- the booking is still honoured despite the alert ---- */
    const after = await (await fetch(`${BASE}/monitor/leads?token=e2e-token`)).json();
    const ghost = (after.leads || after.rows || []).find((l) => l.email === 'ghost@examplecorp.com');
    ok('booking: honoured even when flagged', ghost && ghost.booking_uid === 'bk-e2e-2',
       JSON.stringify(ghost || {}).slice(0, 200));

    /* ---- website check still fails open with no network ---- */
    const w = await post('/verify-website', { website: 'definitely-not-a-real-domain-xyzzy.invalid' });
    eq('/verify-website: returns 200', w.status, 200);
    ok('/verify-website: fails open on an unreachable domain', w.json && w.json.ok === true, JSON.stringify(w.json));

    ok('runtime: no ReferenceError or TypeError in the log',
       !/ReferenceError|TypeError|is not defined|is not a function/.test(bootLog),
       (bootLog.match(/(ReferenceError|TypeError)[^\n]*/g) || []).join(' | '));

  } catch (err) {
    fail++;
    failures.push('EXCEPTION: ' + err.message);
  } finally {
    child.kill('SIGKILL');
  }

  console.log('');
  console.log(`  passed: ${pass}`);
  console.log(`  failed: ${fail}`);
  if (failures.length) { console.log(''); failures.forEach((f) => console.log('  ✗ ' + f)); }
  console.log('');
  process.exit(fail === 0 ? 0 : 1);
})();
