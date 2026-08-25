/* ============================================================
   Batch A verification — monitor dashboard correctness.

   Same convention as test-batch1.js / test-batch2.js: the real code is
   LIFTED out of index.js and evaluated, rather than copied here. A test
   that exercises a duplicate of the source can pass while production is
   broken.

   Dependency-free — no DATABASE_URL, no network. SQL is asserted by
   shape (which timezone expression is present at which site), because
   the one thing that actually breaks here is a day boundary silently
   resolving in the wrong zone, and that is visible in the text.

   Run:  node tests/test-batch-a.js
   ============================================================ */

const fs = require('fs');
const path = require('path');
const src   = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const lmsrc = fs.readFileSync(path.join(__dirname, '..', 'lead-magnet.js'), 'utf8');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; }
  else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
function eq(name, actual, expected) {
  ok(name, JSON.stringify(actual) === JSON.stringify(expected),
     `got ${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`);
}
function between(s, startMarker, endMarker) {
  const i = s.indexOf(startMarker);
  if (i === -1) throw new Error('marker not found: ' + startMarker);
  const j = s.indexOf(endMarker, i);
  if (j === -1) throw new Error('end marker not found: ' + endMarker);
  return s.slice(i, j);
}

/* ============================================================
   1. TIMEZONE — the helpers, actually executed
   ============================================================ */
{
  // Lift the real ET helpers out of the shipped source and run them.
  const tzSrc = between(src, "const DASH_TZ = 'America/New_York';", 'const ALERT_COOLDOWN_MS');
  const T = (new Function(tzSrc + '\n return { DASH_TZ, etDateOnly, etStamp };'))();

  eq('tz: DASH_TZ is the IANA name', T.DASH_TZ, 'America/New_York');

  /* A fixed offset is the classic way a report shifts by an hour every
     March. These two instants are 4h and 5h off UTC respectively — if the
     helper were pinned to one offset, one of them would be wrong. */
  const summer = new Date('2026-08-25T03:30:00Z'); // 23:30 Aug 24, EDT (UTC-4)
  const winter = new Date('2026-01-15T03:30:00Z'); // 22:30 Jan 14, EST (UTC-5)
  eq('tz: EDT instant resolves to the previous ET day', T.etDateOnly(summer), '2026-08-24');
  eq('tz: EST instant resolves to the previous ET day', T.etDateOnly(winter), '2026-01-14');
  ok('tz: EDT is UTC-4 (23:30, not 22:30)', T.etStamp(summer).includes('23:30'), T.etStamp(summer));
  ok('tz: EST is UTC-5 (22:30, not 23:30)', T.etStamp(winter).includes('22:30'), T.etStamp(winter));
  ok('tz: stamps are labelled ET', T.etStamp(summer).endsWith(' ET'), T.etStamp(summer));

  // Spring forward: 02:00 EST never exists, the clock jumps to 03:00 EDT.
  ok('tz: spring-forward gap is handled by Intl, not arithmetic',
     T.etStamp(new Date('2026-03-08T06:59:00Z')).includes('01:59')
     && T.etStamp(new Date('2026-03-08T07:01:00Z')).includes('03:01'));

  // etDateOnly feeds CSV filenames. A 02:00 ET export used to be stamped
  // with the previous day because it went through toISOString().
  eq('tz: filename date is the ET calendar day',
     T.etDateOnly(new Date('2026-08-25T05:00:00Z')), '2026-08-25');

  /* No ACTIVE use of the old zone or of a fixed offset. Matched against
     usage sites rather than the whole file on purpose: the comment above
     DASH_TZ names both Asia/Kolkata and '-05:00' as the things it replaced,
     and a comment anchored to the incident is what the house style asks
     for. Grepping the raw text would fail on its own documentation. */
  const activeZone = (s) => (s.match(/timeZone:\s*[^,}]+|AT TIME ZONE '[^']+'/g) || []).join('|');
  const zonesUsed = activeZone(src) + '|' + activeZone(lmsrc);
  ok('tz: no Asia/Kolkata in an active zone position', !/Asia\/Kolkata/.test(zonesUsed), zonesUsed);
  ok('tz: no literal EST/EDT zone', !/E[SD]T/.test(zonesUsed), zonesUsed);
  ok('tz: no fixed numeric offset used as a zone', !/[-+]0[0-9]:[0-9]{2}/.test(zonesUsed), zonesUsed);
  ok('tz: every active zone is ET or the injected constant',
     activeZone(src).split('|').filter(Boolean)
       .every((z) => /America\/New_York|TZ|DASH_TZ/.test(z)), activeZone(src));
  ok('tz: no (IST) column labels remain', !/\(IST\)/.test(src));
  ok('tz: dashboard advertises the zone', /All times ET/.test(src));
}

/* ============================================================
   2. TIMEZONE — which SQL sites bucket in ET, and which must not
   ============================================================ */
{
  const AT_TZ = "AT TIME ZONE '${DASH_TZ}'";

  /* ── MUST be ET ── */

  // Overview chart: both the generate_series spine and the join predicate.
  // If only one of them is converted the join silently stops matching and
  // every bar reads 0 — which is why both are asserted, not just one.
  const chart = between(src, "SELECT to_char(d.day, 'Mon DD')", 'ORDER BY d.day ASC');
  eq('tz-sql: chart converts the series bounds and the join', (chart.match(/AT TIME ZONE/g) || []).length, 3);
  ok('tz-sql: chart join is ET', chart.includes(`l.created_at ${AT_TZ}`), chart.slice(0, 80));
  ok('tz-sql: chart zero-fills with generate_series', /generate_series/.test(chart));
  ok('tz-sql: chart counts leads rows, not distinct emails',
     /COUNT\(l\.id\)/.test(chart) && !/DISTINCT/.test(chart));

  // All Leads date filter — the site that produced the "Today hides
  // this morning's leads" bug.
  ok('tz-sql: dateFrom boundary is ET midnight',
     src.includes(`l.created_at >= ($\${params.length}::date::timestamp ${AT_TZ})`));
  ok('tz-sql: dateTo boundary is ET midnight',
     src.includes(`l.created_at < (($\${params.length}::date + INTERVAL '1 day')::timestamp ${AT_TZ})`));
  ok('tz-sql: no bare ::date comparison survives on created_at',
     !/l\.created_at\s*>=\s*\$\$\{params\.length\}::date[^:]/.test(src));

  // Lead-magnet daily chart.
  const lmDaily = between(lmsrc, "SELECT to_char(d.day, 'YYYY-MM-DD') AS day", 'GROUP BY d.day ORDER BY d.day');
  eq('tz-sql: LM daily chart converts series + join', (lmDaily.match(/AT TIME ZONE/g) || []).length, 3);
  ok('tz-sql: LM router receives DASH_TZ rather than redeclaring it',
     /deps\.DASH_TZ/.test(lmsrc) && /DASH_TZ\s*\}\)\);/.test(src));

  /* ── MUST NOT be ET: /monitor/funnel keeps UTC day buckets ──
     Its coverage logic is anchored to the go-live INSTANT (21 Aug 2026
     10:32 UTC). Re-bucketing to ET moves every boundary 4-5 hours and
     changes which day reads "partial", so this is deliberate. Asserted so
     a later tidy-up cannot quietly "bring it into line". */
  const funnel = between(src, "app.get('/monitor/funnel'", "app.get('/monitor/duplicates'");
  ok('tz-sql: funnel sessions CTE stays on UTC days',
     funnel.includes("SELECT date_trunc('day', created_at) AS d,"));
  ok('tz-sql: funnel leads CTE stays on UTC days',
     funnel.includes("SELECT date_trunc('day', l.created_at) AS d,"));
  ok('tz-sql: funnel has no ET conversion at all', !/AT TIME ZONE/.test(funnel));
  ok('tz-sql: funnel explains WHY it differs', /DELIBERATE/.test(funnel) && /go_live/.test(funnel));
  // Both CTEs must agree, or the FULL OUTER JOIN on s.d = l.d splits every day.
  eq('tz-sql: funnel uses one zone for both CTEs',
     (funnel.match(/date_trunc\('day', l?\.?created_at\)/g) || []).length, 2);
}

/* ============================================================
   3. TIMEZONE — client-side formatting reads ET, never the laptop
   ============================================================ */
{
  ok('tz-client: one TZ constant, injected from DASH_TZ', /'var TZ="' \+ DASH_TZ \+ '";'/.test(src));
  ok('tz-client: et() formatter defined',        /function et\(ts\)/.test(src));
  ok('tz-client: lmET() formatter defined',      /function lmET\(t\)/.test(src));
  ok('tz-client: etDay\\(\\) formatter defined', /function etDay\(d\)/.test(src));
  ok('tz-client: formatters pass an explicit timeZone',
     (src.match(/timeZone:TZ/g) || []).length >= 3);
  ok('tz-client: no en-IN locale remains', !/en-IN/.test(src));
  ok('tz-client: the ist\\(\\) formatter is gone', !/function ist\(/.test(src));

  /* THE PRESET BUG. getFullYear/getMonth/getDate are local-time getters,
     so "Today" meant whatever day it was on the viewer's machine. Two SDRs
     in different zones clicking the same button got different row sets. */
  const preset = between(src, "'function datePreset(v)", "'function dateManual()");
  ok('tz-client: presets no longer read the laptop clock',
     !/getFullYear|getMonth\(\)|getDate\(\)/.test(preset), preset.slice(0, 120));
  ok('tz-client: presets derive dates in ET', /etDayShift/.test(preset));

  /* etDayShift anchors at noon UTC on purpose: shifting midnight by 24h
     multiples lands on the wrong calendar date across a DST change. */
  const shiftSrc = between(src, "'function etDay(d)", "'function lmPct(") // both helpers live in the same run
    .replace(/^\s*'|'\s*\+\s*$/gm, '')      // strip the JS-string wrapper
    .split('\n').filter((l) => l.includes('function et')).join('\n');
  ok('tz-client: etDayShift anchors at noon UTC (DST-immune)', /Date\.UTC\([^)]*12,\s*0,\s*0\)/.test(src));
  ok('tz-client: etDayShift shifts in whole UTC days', /setUTCDate\(/.test(src));
  ok('tz-client: lifted shift helper source found', shiftSrc.length > 0);

  // Execute the client helpers for real, out of the shipped string.
  const clientJs = (src.match(/'function etDay\(d\)\{[^']+\}' \+/) || [''])[0]
    .replace(/^'/, '').replace(/' \+$/, '')
    + (src.match(/'function etDayShift\(n\)\{[^']+\}' \+/) || [''])[0]
        .replace(/^'/, '').replace(/' \+$/, '');
  const C = (new Function('var TZ="America/New_York";' + clientJs + '\n return { etDay, etDayShift };'))();
  eq('tz-client: etDay is the ET calendar day',
     C.etDay(new Date('2026-08-25T03:30:00Z')), '2026-08-24');
  // 10 Mar 2026 is after the spring-forward; -6d must be exactly 4 Mar.
  const anchor = new Date('2026-03-10T15:00:00Z');
  const realShift = (n) => {
    const p = C.etDay(anchor).split('-');
    const d = new Date(Date.UTC(+p[0], +p[1] - 1, +p[2], 12, 0, 0));
    d.setUTCDate(d.getUTCDate() + n);
    return d.toISOString().slice(0, 10);
  };
  eq('tz-client: 7d preset spans exactly 7 ET days across DST', realShift(-6), '2026-03-04');
  eq('tz-client: 30d preset spans exactly 30 ET days across DST', realShift(-29), '2026-02-09');
}

/* ============================================================
   4. TIMEZONE — Slack / email / filenames
   ============================================================ */
{
  ok('tz-alerts: alert email stamps in ET',   /Time: \$\{etStamp\(\)\}/.test(src));
  ok('tz-alerts: Slack severity line in ET',  /Severity: \*\$\{sev\}\* · \$\{etStamp\(\)\}/.test(src));
  ok('tz-alerts: recovery context line in ET',/Severity: \*info\* · \$\{etStamp\(\)\}/.test(src));
  ok('tz-alerts: leads CSV filename in ET',   /filename="leads-\$\{etDateOnly\(\)\}\.csv"/.test(src));
  ok('tz-alerts: SDR CSV filename in ET',     /filename="sdr-list-\$\{etDateOnly\(\)\}\.csv"/.test(src));
  ok('tz-alerts: LM CSV filename in ET',      /a\.download="lead-magnet-"\+etDay\(new Date\(\)\)/.test(src));
  /* toISOString() is UTC. Any surviving use in a user-facing stamp puts UTC
     next to a dashboard showing ET. Health/diagnostic payloads may keep it. */
  ok('tz-alerts: no toISOString in a filename', !/filename="[^"]*toISOString/.test(src));
}

/* ============================================================
   5. THE RECOVERY CRON IS NOT PART OF THIS BATCH
   Guard, not a feature test. The booked_at >= l.created_at comparison in
   the cron is deliberate (May 2026): a booking that PREDATES a session
   does not resolve that session's drop-off, so relaxing it to
   "has ever booked" would suppress follow-ups that should be sent.
   Asserted here because it looks like an inconsistency and invites a fix.
   ============================================================ */
{
  const cron = between(src, "app.post('/cron/send-partials'", '[Cron] Found');
  ok('cron: the time-ordered booking test is intact',
     cron.includes('booked.booked_at >= l.created_at'));
  ok('cron: no COALESCE was added to the cron booking test',
     !/COALESCE\([^)]*booked_at/.test(cron));
  ok('cron: the reason is documented in place so it survives the next tidy-up',
     /DELIBERATE/.test(cron) && /May 2026/.test(cron));
  ok('cron: names the two distinct questions',
     /SDR target/i.test(cron) && /drop-off/i.test(cron));
}

/* ============================================================ */
console.log('');
if (failures.length) {
  console.log('  FAILURES:');
  failures.forEach((f) => console.log('   ✗ ' + f));
  console.log('');
}
console.log(`  passed: ${pass}`);
console.log(`  failed: ${fail}`);
console.log('');
process.exit(fail ? 1 : 0);
