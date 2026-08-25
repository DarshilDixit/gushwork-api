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

/* ── Lift client-side JS out of the dashboard's concatenated string ──
   The dashboard ships its JS as a run of single-quoted literals joined with
   '+'. Each line is a real JS string literal, so letting JS itself unquote
   it gives back exactly what the browser executes — escapes and all. Beats
   asserting on source text: these tests then run the SHIPPED function. */
function liftClientJs(startMarker, endMarker) {
  const block = between(src, startMarker, endMarker);
  return block.split('\n').map((line) => {
    const m = line.match(/^\s*'(.*)'\s*\+\s*$/);
    if (!m) return '';
    return (new Function('return \'' + m[1] + '\''))();
  }).join('');
}

/* ============================================================
   5. OVERVIEW FUNNEL — a real ladder, and "not tracked" is not zero
   ============================================================ */
{
  const metrics = between(src, "app.get('/monitor/metrics'", "app.get('/monitor/funnel'");

  // Scoped to the tracked window, or the numerator outlives its denominator.
  ok('funnel: top-of-funnel is scoped to go_live',
     /gl\.go_live IS NOT NULL/.test(metrics) && /l\.created_at >= gl\.go_live/.test(metrics));
  ok('funnel: go_live comes from form_sessions',
     /MIN\(created_at\) AS go_live FROM form_sessions/.test(metrics));
  ok('funnel: sessions come from form_sessions, not leads',
     /COUNT\(\*\) FILTER \(WHERE user_agent IS NULL OR user_agent !~\* \$1\) AS sessions/.test(metrics));
  ok('funnel: bots counted separately, empty UA kept as human',
     /COUNT\(\*\) FILTER \(WHERE user_agent ~\* \$1\)\s+AS bot_sessions/.test(metrics));
  ok('funnel: people stages dedupe on lower(email)',
     (metrics.match(/COUNT\(DISTINCT LOWER\(l\.email\)\)/g) || []).length >= 3);
  ok('funnel: stage flags use IS TRUE, not = true',
     /l\.completed IS TRUE/.test(metrics));

  // ONE bot regex for the whole file.
  eq('funnel: exactly one BOT_RE definition', (src.match(/const BOT_RE\s*=/g) || []).length, 1);
  ok('funnel: BOT_RE lives at module scope', /^const BOT_RE\s*=/m.test(src));
  /* Three consumers now: the Overview funnel, /monitor/funnel, and the
     Step 1 health check — which needs it, because a night of crawler traffic
     would otherwise push the session denominator over the floor and
     manufacture a red. The count is asserted as a floor and the definition as
     exactly one, so the thing that matters (no second copy of the regex)
     still fails loudly. */
  ok('funnel: every consumer passes the shared const',
     (src.match(/\[BOT_RE\]|BOT_RE\]/g) || []).length >= 3);

  // Nulls, not zeros, with no tracking.
  ok('funnel: no-coverage branch returns nulls',
     /coverage: 'none'[\s\S]{0,200}sessions: null/.test(metrics));
  ok('funnel: covered branch is gated on go_live', /tf\.go_live\s*\?/.test(metrics));

  /* Execute the shipped renderer. */
  const clientSrc = liftClientJs("'function fRow(label,unit,val,top,col,note,nullMsg){'", "'function renderChart(rows)");
  const sink = {};
  const doc = { getElementById: (id) => ({ set innerHTML(v) { sink[id] = v; }, get innerHTML() { return sink[id]; } }) };
  const C = (new Function('document', 'et',
    clientSrc + '\n return { fRow, renderFunnel, setFunnelMode };'))(doc, () => '21/08/2026, 10:32');

  // A null stage must never render as 0 — that reads as "nobody", which is a
  // claim about demand rather than about our instrumentation.
  const nullRow = C.fRow('Sessions', 'visits', null, null, '#000', '');
  ok('funnel: null stage renders "not tracked"', /not tracked/.test(nullRow), nullRow);
  ok('funnel: null stage never prints 0 or 0%', !/>0</.test(nullRow) && !/0%/.test(nullRow), nullRow);

  const realRow = C.fRow('Entered step 1', 'people', 25, 100, '#000', '');
  ok('funnel: real stage shows count and % of top', /25/.test(realRow) && /25%/.test(realRow), realRow);

  /* REGRESSION. `note` decorates a MEASURED row; a null row needs its own
     message. Collapsing them onto one parameter (which I did, briefly) made
     "Entered step 1" render its decoration — "· visits → people" — as its
     VALUE whenever coverage was missing, which is worse than the zero it
     replaced: it looks like a real reading. */
  const nullWithNote = C.fRow('Entered step 1', 'people', null, null, '#000', '&#183; visits &rarr; people');
  ok('funnel: a null stage with a decorative note still says not tracked',
     /not tracked/.test(nullWithNote), nullWithNote);
  ok('funnel: a null stage never renders its decoration as a value',
     !/visits &rarr; people/.test(nullWithNote), nullWithNote);
  const nullCustom = C.fRow('Sessions', 'visits', null, null, '#000', '', 'not tracked before 21 Aug 2026');
  ok('funnel: a null stage can carry a specific reason',
     /not tracked before 21 Aug 2026/.test(nullCustom), nullCustom);

  // coverage none — every stage not tracked, and an explanation.
  C.renderFunnel({ topFunnel: { coverage: 'none', since: null, sessions: null, botSessions: null,
                                step1: null, completed: null, booked: null } });
  const none = sink.funnel;
  eq('funnel: all four stages read not-tracked with no coverage',
     (none.match(/not tracked/g) || []).length, 4);
  ok('funnel: no-coverage explains itself', /was not running/.test(none), none.slice(-160));

  // coverage full — four stages, bot exclusion stated, unit change stated.
  C.renderFunnel({ topFunnel: { coverage: 'full', since: '2026-08-21T10:32:00Z', sessions: 1000,
                                botSessions: 90, step1: 250, completed: 120, booked: 40 } });
  const full = sink.funnel;
  ok('funnel: four stages in ladder order',
     full.indexOf('Sessions') < full.indexOf('Entered step 1')
     && full.indexOf('Entered step 1') < full.indexOf('Completed step 2')
     && full.indexOf('Completed step 2') < full.indexOf('Booked'), 'order wrong');
  ok('funnel: bot exclusion is visible, not silent', /90 bots excluded/.test(full), full.slice(0, 200));
  ok('funnel: the visits-to-people unit change is stated', /visits/.test(full) && /people/.test(full));
  ok('funnel: every bar is a % of the top', /25%/.test(full) && /12%/.test(full) && /4%/.test(full), full);
  ok('funnel: no stage reads not-tracked when covered', !/not tracked/.test(full));

  /* "Disqualified" is OUT of the funnel on purpose: it is not a later stage
     than booked, and a disqualified person can also be booked, so the bars
     overlapped and never summed. It stays as its own Overview card. */
  ok('funnel: disqualified is no longer a funnel stage', !/disqualified/i.test(full));
  ok('funnel: disqualified still has its own card', /id="m-disq"/.test(src));

  /* ── ALL-TIME MODE ──
     Three stages, no sessions bar (form_sessions does not exist before
     go-live), and the missing bar must EXPLAIN itself rather than showing a
     blank row or a zero-width bar that reads as "no traffic". */
  const payload = {
    peopleTotal: 500, peopleCompleted: 200, peopleBooked: 50,
    topFunnel: { coverage: 'full', since: '2026-08-21T10:32:00Z', sessions: 1000,
                 botSessions: 90, step1: 250, completed: 120, booked: 40 },
  };
  C.setFunnelMode('all');
  C.renderFunnel(payload);
  const allT = sink.funnel;

  ok('all-time: sessions stage is not tracked, not zero',
     /not tracked before 21 Aug 2026/.test(allT), allT.slice(0, 260));
  ok('all-time: the missing sessions bar has no zero-width bar drawn',
     !/width:0%/.test(allT.slice(0, allT.indexOf('Entered step 1'))), allT.slice(0, 300));
  eq('all-time: exactly three measured stages',
     (allT.match(/font-weight:500/g) || []).length, 3);

  /* Byte-for-byte the pre-existing calculation: step 1 at 100%, the other two
     as a share of it. 200/500 = 40%, 50/500 = 10%. If this drifts, historical
     numbers stop reconciling and that is the one thing all-time mode is for. */
  ok('all-time: step 1 is the denominator at 100%', /500 <span style="color:#aaa">\(100%\)/.test(allT), allT);
  ok('all-time: completed is % of step 1',          /200 <span style="color:#aaa">\(40%\)/.test(allT), allT);
  ok('all-time: booked is % of step 1',             /50 <span style="color:#aaa">\(10%\)/.test(allT), allT);
  ok('all-time: does NOT use the windowed figures',
     !/250/.test(allT) && !/1000/.test(allT), 'windowed numbers leaked into all-time');

  // The two modes divide by different things. Say so, in both.
  ok('all-time: names its denominator', /Percentages are % of step 1/.test(allT), allT.slice(-260));
  ok('all-time: warns the modes are not comparable', /not comparable/.test(allT));
  C.setFunnelMode('tracked');
  C.renderFunnel(payload);
  ok('tracked: names its denominator', /Percentages are % of sessions/.test(sink.funnel));
  ok('tracked: warns the modes are not comparable', /not comparable/.test(sink.funnel));

  // Default on load must be the tracked view, so top-of-funnel loss leads.
  ok('funnel: defaults to the tracked window', /var funnelMode="tracked"/.test(src));
  ok('funnel: toggle offers both windows',
     /Since 21 Aug/.test(src) && /All time/.test(src) && /setFunnelMode/.test(src));

  /* No-coverage in tracked mode should point at the view that does have data,
     rather than leaving a dead end. */
  C.renderFunnel({ topFunnel: { coverage: 'none', sessions: null, botSessions: null,
                                step1: null, completed: null, booked: null, since: null } });
  ok('funnel: no-coverage points at the All-time view', /All time/.test(sink.funnel), sink.funnel.slice(-200));
}

/* ============================================================
   6. THE RECOVERY CRON IS NOT PART OF THIS BATCH
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


/* ============================================================
   7. SYSTEM HEALTH — nine rows that check something

   Seven of the nine used to pass a hardcoded green class. The
   canonical line was:

     badge("s-partial", d.total+" sessions saved", "bg")

   These tests drive the REAL check functions, lifted out of index.js,
   against stubbed query results. No database and no network.

   The AWS row carries an extra obligation: it must be structurally
   incapable of returning insufficient_data, because a mirror we cannot
   reach is indistinguishable from one that has gone stale and both
   starve the sdr-calling dialer. That is asserted twice — once by
   reading the function's own source, once by driving it.
   ============================================================ */
{
  const healthSrc = between(src, '/* ══ SYSTEM HEALTH', "app.get('/monitor/health'");

  // Everything the block reaches for that lives elsewhere in the file.
  const sentAlerts = [];
  const sentSlack  = [];
  const H = (new Function(
    'pool', 'awsPool', 'BOT_RE', 'CRON_STALE_MS',
    '_lastCronRunAt', '_cronRanThisProcess', '_processStartedAt',
    'alertOps', 'sendOpsSlack', 'bHeader', 'bDivider', 'bFields', 'bContext', 'etStamp',
    healthSrc + `
    return { checkPartialHealth, checkSubmitHealth, checkApolloHealth, checkBookingHealth,
             checkCronHealth, checkAwsHealth, checkRecoveryHealth,
             evaluateHealthAlerts, runHealthChecks, withTimeout, fmtAge,
             HEALTH_SEVERITY, HEALTH_MIN_SAMPLE, HEALTH_SUBMIT_MIN_STEP1,
             HEALTH_BOOKING_MIN_COMPLETED, HEALTH_RECOVERY_STUCK_H, HEALTH_AWS_TIMEOUT_MS };`
  ))(
    null, null, '(bot|crawl)', 3 * 60 * 60 * 1000,
    Date.now(), false, Date.now(),
    (sev, source, title, details) => { sentAlerts.push({ sev, source, title, details }); return true; },
    (blocks, text) => { sentSlack.push(text); },
    (t) => ({ t }), () => ({ d: 1 }), (f) => ({ f }), (c) => ({ c }), () => '25/08/2026, 12:00 ET'
  );

  const rowsPool  = (row) => ({ query: async () => ({ rows: [row] }) });
  const deadPool  = (msg) => ({ query: async () => { throw new Error(msg); } });
  const st = async (p) => (await p).state;

  /* ── Step 1 — /partial ── */
  (async () => {
    eq('health/partial: sessions arriving with zero leads is RED',
       await st(H.checkPartialHealth(rowsPool({ sessions: 40, leads: 0 }))), 'red');
    eq('health/partial: a quiet window is grey, not red',
       await st(H.checkPartialHealth(rowsPool({ sessions: 3, leads: 0 }))), 'insufficient_data');
    eq('health/partial: leads flowing is green',
       await st(H.checkPartialHealth(rowsPool({ sessions: 40, leads: 12 }))), 'green');
    eq('health/partial: an empty window is grey, never green',
       await st(H.checkPartialHealth(rowsPool({ sessions: 0, leads: 0 }))), 'insufficient_data');
    eq('health/partial: a failed query is RED, not grey',
       await st(H.checkPartialHealth(deadPool('connection refused'))), 'red');

    /* ── Step 2 — /submit ── */
    eq('health/submit: step-1 traffic with zero completions is RED',
       await st(H.checkSubmitHealth(rowsPool({ step1: 60, completions: 0 }))), 'red');
    eq('health/submit: below the step-1 floor is grey',
       await st(H.checkSubmitHealth(rowsPool({ step1: 4, completions: 0 }))), 'insufficient_data');
    eq('health/submit: completions in the window is green',
       await st(H.checkSubmitHealth(rowsPool({ step1: 60, completions: 9 }))), 'green');
    eq('health/submit: a failed query is RED',
       await st(H.checkSubmitHealth(deadPool('timeout'))), 'red');

    /* The old row asked whether there had EVER been a completion, so it went
       green in July and had no way back. */
    const submitFn = between(healthSrc, 'async function checkSubmitHealth', 'Apollo enrichment');
    ok('health/submit: the window is in the query, not a lifetime count',
       (submitFn.match(/INTERVAL '\$\{HEALTH_SUBMIT_WINDOW_H\} hours'/g) || []).length === 2
       && /submitted_at >= NOW\(\)/.test(submitFn), submitFn.slice(0, 300));

    /* ── Apollo ── */
    eq('health/apollo: zero enrichments against a real sample is RED',
       await st(H.checkApolloHealth(rowsPool({ leads: 100, enriched: 0, last_enriched: null }))), 'red');
    eq('health/apollo: a healthy rate is green',
       await st(H.checkApolloHealth(rowsPool({ leads: 100, enriched: 80, last_enriched: new Date() }))), 'green');
    eq('health/apollo: a middling rate is amber',
       await st(H.checkApolloHealth(rowsPool({ leads: 100, enriched: 40, last_enriched: new Date() }))), 'amber');
    eq('health/apollo: a bad rate is red',
       await st(H.checkApolloHealth(rowsPool({ leads: 100, enriched: 10, last_enriched: new Date() }))), 'red');
    eq('health/apollo: below the lead floor is grey',
       await st(H.checkApolloHealth(rowsPool({ leads: 3, enriched: 0, last_enriched: null }))), 'insufficient_data');
    eq('health/apollo: a failed query is RED',
       await st(H.checkApolloHealth(deadPool('relation does not exist'))), 'red');

    /* ── Booking ── */
    eq('health/booking: zero bookings against real completions is RED',
       await st(H.checkBookingHealth(rowsPool({ completed_people: 60, booked_people: 0 }))), 'red');
    eq('health/booking: a healthy rate is green',
       await st(H.checkBookingHealth(rowsPool({ completed_people: 60, booked_people: 40 }))), 'green');
    /* A low booking rate is a business outcome, not an outage. Amber keeps it
       off the alerting path, which only ever fires on green to red. */
    eq('health/booking: a low rate is amber, not red',
       await st(H.checkBookingHealth(rowsPool({ completed_people: 60, booked_people: 12 }))), 'amber');
    eq('health/booking: below the completions floor is grey',
       await st(H.checkBookingHealth(rowsPool({ completed_people: 4, booked_people: 0 }))), 'insufficient_data');
    eq('health/booking: a failed query is RED',
       await st(H.checkBookingHealth(deadPool('boom'))), 'red');

    /* ── Cron — pure function of the clock ── */
    const NOW = 1_800_000_000_000;
    const H3  = 3 * 60 * 60 * 1000;
    eq('health/cron: a recent run is green',
       H.checkCronHealth(NOW, NOW - 10 * 60 * 1000, true, NOW - H3).state, 'green');
    eq('health/cron: a stale run is RED — the old row could not go red at all',
       H.checkCronHealth(NOW, NOW - 5 * H3, true, NOW - 6 * H3).state, 'red');
    /* _lastCronRunAt is seeded to boot time. Without the ran-this-process flag
       this case would read green for three hours after every redeploy. */
    eq('health/cron: not yet run, inside the window, is grey — never green',
       H.checkCronHealth(NOW, NOW, false, NOW - 10 * 60 * 1000).state, 'insufficient_data');
    eq('health/cron: not yet run, past the window, is RED',
       H.checkCronHealth(NOW, NOW, false, NOW - 2 * H3).state, 'red');
    ok('health/cron: a never-run cron can never be green',
       [10 * 60 * 1000, H3 - 1, H3 + 1, 50 * H3]
         .every((up) => H.checkCronHealth(NOW, NOW, false, NOW - up).state !== 'green'));

    /* ── AWS mirror — the owner's top priority ──
       Requirement, verbatim: it must fail red on a connection error, not
       degrade to grey. Asserted structurally AND behaviourally. */
    const awsFn = between(healthSrc, 'async function checkAwsHealth', 'Email recovery');
    ok('health/aws: the function body contains no insufficient_data at all',
       !/insufficient_data/.test(awsFn), 'a grey path was added to the AWS check');
    ok('health/aws: the function body contains no amber either — green or red only',
       !/'amber'/.test(awsFn));
    ok('health/aws: it really queries the mirror table',
       /FROM gw_form_leads/.test(awsFn) && /awsDb\.query/.test(awsFn));
    ok('health/aws: it compares against leads over the same window',
       /FROM leads/.test(awsFn) && /railwayDb\.query/.test(awsFn)
       && (awsFn.match(/INTERVAL '\$\{HEALTH_AWS_WINDOW_H\} hours'/g) || []).length === 2);
    ok('health/aws: the query is bounded by a timeout',
       /withTimeout\(/.test(awsFn) && /HEALTH_AWS_TIMEOUT_MS/.test(awsFn));
    ok('health/aws: and the bound is short enough to matter',
       H.HEALTH_AWS_TIMEOUT_MS > 0 && H.HEALTH_AWS_TIMEOUT_MS <= 15000, String(H.HEALTH_AWS_TIMEOUT_MS));
    ok('health/aws: the reason grey is banned is written down where it can be read',
       /indistinguishable/.test(awsFn) && /dialer/.test(awsFn));

    const railway = (n) => rowsPool({ recent: n });
    const mirror  = (n, lastWrite) => rowsPool({ recent: n, last_write: lastWrite === undefined ? new Date() : lastWrite });

    eq('health/aws: an unreachable mirror is RED, not grey',
       await st(H.checkAwsHealth(deadPool('ECONNREFUSED'), railway(40))), 'red');
    eq('health/aws: an auth failure is RED',
       await st(H.checkAwsHealth(deadPool('password authentication failed'), railway(40))), 'red');
    eq('health/aws: AWS_PG_HOST unset is RED, not green and not grey',
       await st(H.checkAwsHealth(null, railway(40))), 'red');
    eq('health/aws: a Railway-side failure is RED too',
       await st(H.checkAwsHealth(mirror(40), deadPool('railway down'))), 'red');
    eq('health/aws: leads on Railway and nothing mirrored is RED',
       await st(H.checkAwsHealth(mirror(0), railway(40))), 'red');
    eq('health/aws: drift past tolerance is RED',
       await st(H.checkAwsHealth(mirror(28), railway(40))), 'red');
    eq('health/aws: a couple of in-flight writes are not an incident',
       await st(H.checkAwsHealth(mirror(39), railway(40))), 'green');
    eq('health/aws: in sync is green',
       await st(H.checkAwsHealth(mirror(40), railway(40))), 'green');

    /* Both zero is a VERIFIED positive — we connected, we queried, the two
       agree — so it is green, and the text has to say what was verified
       rather than implying traffic we did not see. */
    const quiet = await H.checkAwsHealth(mirror(0), railway(0));
    eq('health/aws: reachable with nothing to mirror is green', quiet.state, 'green');
    ok('health/aws: and says reachable rather than implying traffic',
       /Reachable/.test(quiet.text) && !/In sync/.test(quiet.text), quiet.text);

    /* Every reachable outcome, driven end to end: none of them is grey. */
    const awsCases = [
      [deadPool('x'), railway(40)], [null, railway(40)], [mirror(40), deadPool('x')],
      [mirror(0), railway(40)], [mirror(28), railway(40)], [mirror(39), railway(40)],
      [mirror(40), railway(40)], [mirror(0), railway(0)],
    ];
    const awsStates = [];
    for (const [a, r] of awsCases) awsStates.push(await st(H.checkAwsHealth(a, r)));
    ok('health/aws: no driven case returns insufficient_data',
       awsStates.every((s) => s === 'green' || s === 'red'), awsStates.join(','));

    // The timeout wrapper itself, so "bounded by a timeout" is not just a grep.
    let timedOut = false;
    try { await H.withTimeout(new Promise(() => {}), 20, 'stub'); }
    catch (e) { timedOut = /timed out/.test(e.message); }
    ok('health/aws: withTimeout rejects a hung query', timedOut);

    /* ── Email recovery ── */
    eq('health/recovery: a stuck queue is RED',
       await st(H.checkRecoveryHealth(rowsPool({ processed: 20, stuck: 3 }))), 'red');
    eq('health/recovery: sends happening and nothing stuck is green',
       await st(H.checkRecoveryHealth(rowsPool({ processed: 20, stuck: 0 }))), 'green');
    eq('health/recovery: nothing to do is grey, not green',
       await st(H.checkRecoveryHealth(rowsPool({ processed: 0, stuck: 0 }))), 'insufficient_data');
    eq('health/recovery: a failed query is RED',
       await st(H.checkRecoveryHealth(deadPool('boom'))), 'red');

    /* This row asks whether the CRON is clearing its own queue, so it has to
       count the rows the cron would actually pick up. Widen the predicate and
       a row the cron never selects sits red here forever. */
    const recFn = between(healthSrc, 'async function checkRecoveryHealth', 'One place that runs them all');
    ok('health/recovery: mirrors the cron predicate exactly',
       /disqualified = false/.test(recFn) && /loops_sent = false/.test(recFn)
       && /booked\.booked_at >= l\.created_at/.test(recFn), recFn);
    ok('health/recovery: does NOT widen the cron predicate to IS NOT TRUE',
       !/IS NOT TRUE/.test(recFn));
    ok('health/recovery: stuck is measured past the cron staleness window, not at 2h',
       H.HEALTH_RECOVERY_STUCK_H === 5);

    /* ── Minimum denominators everywhere but AWS ── */
    const windowed = { partial: 'checkPartialHealth', submit: 'checkSubmitHealth',
                       apollo: 'checkApolloHealth', booking: 'checkBookingHealth' };
    Object.keys(windowed).forEach((id) => {
      const fn = between(healthSrc, 'async function ' + windowed[id], '/* ──');
      ok('health/' + id + ': has a minimum denominator, so a quiet night is grey',
         /HEALTH_MIN_SAMPLE|HEALTH_SUBMIT_MIN_STEP1|HEALTH_BOOKING_MIN_COMPLETED/.test(fn)
         && /insufficient_data/.test(fn), fn.slice(0, 120));
    });

    /* ── ALERTING — transitions only, through the existing alertOps ── */
    const red   = (id) => ({ [id]: { id, state: 'red',   text: 'broken', detail: 'd' } });
    const green = (id) => ({ [id]: { id, state: 'green', text: 'fine',   detail: 'd' } });
    const grey  = (id) => ({ [id]: { id, state: 'insufficient_data', text: 'quiet', detail: 'd' } });

    sentAlerts.length = 0; sentSlack.length = 0;
    H.evaluateHealthAlerts(red('partial'));
    eq('health/alert: green to red fires once', sentAlerts.length, 1);
    H.evaluateHealthAlerts(red('partial'));
    H.evaluateHealthAlerts(red('partial'));
    eq('health/alert: staying red does not re-fire on every poll', sentAlerts.length, 1);
    eq('health/alert: /partial is critical — Slack and email', sentAlerts[0].sev, 'critical');

    /* Grey is inert. It means "not enough evidence", and alerting on an
       absence of evidence is the same mistake as recording "we could not
       check" as "we checked and it is bad". */
    H.evaluateHealthAlerts(grey('partial'));
    eq('health/alert: grey neither alerts nor clears', sentAlerts.length + sentSlack.length, 1);

    H.evaluateHealthAlerts(green('partial'));
    eq('health/alert: red to green sends a recovery message', sentSlack.length, 1);
    ok('health/alert: the recovery message is not another alert', sentAlerts.length === 1);
    ok('health/alert: and it names the row that recovered',
       /Step 1 \/partial/.test(sentSlack[0]) && /recovered/.test(sentSlack[0]), sentSlack[0]);
    H.evaluateHealthAlerts(green('partial'));
    eq('health/alert: staying green does not repeat the recovery', sentSlack.length, 1);

    // A row that is green from the first observation has not "recovered".
    sentAlerts.length = 0; sentSlack.length = 0;
    H.evaluateHealthAlerts(green('booking'));
    eq('health/alert: a first-ever green is silent', sentAlerts.length + sentSlack.length, 0);

    // A row that is red on the first observation after a restart must alert.
    sentAlerts.length = 0;
    H.evaluateHealthAlerts(red('aws'));
    eq('health/alert: red on the first observation still alerts', sentAlerts.length, 1);
    eq('health/alert: AWS is critical', sentAlerts[0].sev, 'critical');

    // Severity split — the owner's decision, Aug 2026.
    eq('health/alert: /submit is critical',   H.HEALTH_SEVERITY.submit,   'critical');
    eq('health/alert: Apollo is a warning',   H.HEALTH_SEVERITY.apollo,   'warning');
    eq('health/alert: Booking is a warning',  H.HEALTH_SEVERITY.booking,  'warning');
    eq('health/alert: Cron is a warning',     H.HEALTH_SEVERITY.cron,     'warning');
    eq('health/alert: Recovery is a warning', H.HEALTH_SEVERITY.recovery, 'warning');
    eq('health/alert: exactly three rows page by email',
       Object.keys(H.HEALTH_SEVERITY).filter((k) => H.HEALTH_SEVERITY[k] === 'critical').sort(),
       ['aws', 'partial', 'submit']);

    /* ── No second alerting system ── */
    ok('health: alerting goes through the existing alertOps',
       /alertOps\(HEALTH_SEVERITY\[id\]/.test(healthSrc));
    ok('health: recovery goes through the existing sendOpsSlack',
       /sendOpsSlack\(/.test(healthSrc));
    ok('health: no private cooldown map was introduced',
       !/_healthLastSent|HEALTH_COOLDOWN/.test(healthSrc));

    /* ── Wired into the EXISTING heartbeat, not a new timer ── */
    const hb = between(src, 'function startHeartbeat()', 'PHASE 3: startup configuration audit');
    ok('health: the heartbeat runs the checks', /runHealthChecks\(\)/.test(hb));
    ok('health: and evaluates transitions', /evaluateHealthAlerts/.test(hb));
    ok('health: no second setInterval was added for health',
       (hb.match(/setInterval/g) || []).length === 1, hb);

    /* ── The route ── */
    const route = between(src, "app.get('/monitor/health'", "app.get('/monitor/metrics'");
    ok('health: the route is token-gated like every other monitor route',
       /MONITOR_TOKEN/.test(route) && /Unauthorized/.test(route), route);

    /* ── The dashboard: no literal green class survives ── */
    const rows = between(src, "'<div class=\"tp\" id=\"tp-health\">'", "'<div class=\"sl\">Enrichment coverage</div>'");
    ok('health: the hardcoded green badge for /partial is gone',
       !/badge\("s-partial",d\.total/.test(src));
    ok('health: no health badge is assigned from a lifetime counter',
       !/badge\("s-submit",d\.completed>0/.test(src) && !/badge\("s-aws",d\.awsSynced/.test(src)
       && !/badge\("s-loops",d\.loopsSent/.test(src));
    ok('health: /partial no longer claims to cover the AWS write',
       !/Email \+ lead saved to Railway \+ AWS/.test(rows), rows);
    ok('health: /submit no longer claims Slack fired',
       !/Lead completed &#43; Slack fired|Lead completed \+ Slack fired/.test(rows), rows);
    ok('health: the AWS row says it queries the mirror',
       /queried live against Railway/.test(rows), rows);

    /* ── The client badge renderer, executed ── */
    const clientSrc = liftClientJs("'var HCLS={green:\"bg\",amber:\"ba\",red:\"br\",insufficient_data:\"bx\"};'",
                                   "'function renderAlerts(d){");
    const painted = {};
    const cdoc = { getElementById: (id) => (painted[id] = painted[id] || { textContent: '', className: '', title: '' }) };
    const lastSet = {};
    const CB = (new Function('document', 'fetch', 'API', 'TP', 'TZ', 'AbortSignal', 'set',
      'function badge(id,text,cls){var el=document.getElementById(id);if(!el)return;el.textContent=text;el.className="badge "+cls;}'
      + clientSrc + '\n return { paintHealth, checkHealth, HCLS, HIDS };'))(
      cdoc,
      async () => { throw new Error('NetworkError'); },
      'http://x', '', 'America/New_York', { timeout: () => null },
      (id, v) => { lastSet[id] = v; }
    );

    eq('health/ui: grey renders grey, never the green class', CB.HCLS.insufficient_data, 'bx');
    eq('health/ui: red renders red', CB.HCLS.red, 'br');
    eq('health/ui: every check id maps to a row', Object.keys(CB.HIDS).sort(),
       ['apollo', 'aws', 'booking', 'cron', 'partial', 'recovery', 'submit']);

    CB.paintHealth({ checks: {
      partial: { state: 'green', text: 'ok', detail: 'why' },
      aws:     { state: 'red',   text: 'unreachable', detail: 'why' },
      cron:    { state: 'insufficient_data', text: 'quiet', detail: 'why' },
    } });
    eq('health/ui: a green check paints green', painted['s-partial'].className, 'badge bg');
    eq('health/ui: a red check paints red',     painted['s-aws'].className,     'badge br');
    eq('health/ui: a grey check paints grey',   painted['s-cron'].className,    'badge bx');
    /* A row with no result is not a quiet row. It has to look wrong. */
    eq('health/ui: a missing check paints RED, not grey', painted['s-enrich'].className, 'badge br');
    eq('health/ui: and says so', painted['s-enrich'].textContent, 'No result');

    /* THE FETCH ITSELF FAILING IS NOT A QUIET NIGHT. Grey on this tab means
       "not enough traffic to judge"; it must never also mean "we could not
       ask". Driven through the real checkHealth with a fetch that throws. */
    await CB.checkHealth();
    ok('health/ui: an unreachable health route paints every row RED',
       Object.values(CB.HIDS).every((rid) => painted[rid].className === 'badge br'),
       Object.values(CB.HIDS).map((rid) => rid + '=' + painted[rid].className).join(' '));
    ok('health/ui: and none of them is left grey or green',
       !Object.values(CB.HIDS).some((rid) => /badge (bx|bg|ba)$/.test(painted[rid].className)));
    ok('health/ui: the timestamp line says the check did not happen',
       /unreachable/i.test(lastSet.hupd || ''), lastSet.hupd);

    // An unknown state from the server must not fall through to green.
    CB.paintHealth({ checks: { booking: { state: 'wat', text: 'x' } } });
    eq('health/ui: an unrecognised state paints RED', painted['s-cal'].className, 'badge br');

    /* ── The Overview alerts panel ── */
    ok('health: the alerts panel no longer claims "All systems healthy"',
       !/All systems healthy/.test(src));
    ok('health: it points at the tab that does the checking',
       /Live service checks are on the System Health tab/.test(src));


    /* ============================================================
       8. THE STAGE LADDER — four stages that actually partition

       CLAUDE.md Definitions: four stages, mutually exclusive and exhaustive,
       resolved in priority order, so the four always sum to the total.

       What was there did not do that. 'disqualified' was a bare
       disqualified = true, so a disqualified lead who booked appeared under
       BOTH Booked and Disqualified. 'step1' was
       completed = false AND disqualified = false, which let booked leads
       through and dropped every row where the flag is NULL — those rows were
       in none of the four and could not be found under any stage.

       The predicates are lifted from index.js and evaluated as SQL three-
       valued logic against every combination of the three flags INCLUDING
       NULLS: 2 booking states x 3 disqualified x 3 completed = 18 rows.
       ============================================================ */
    {
      const filters = between(src, "  if (stage === 'booked')", '  if (sellTo ===');
      const grab = (name) => {
        const m = filters.match(new RegExp("stage === '" + name + "'\\)\\s*conditions\\.push\\('([^']+)'\\)"));
        if (!m) throw new Error('stage predicate not found: ' + name);
        return m[1];
      };
      const PREDS = { booked: grab('booked'), disqualified: grab('disqualified'),
                      completed: grab('completed'), step1: grab('step1') };

      /* A tiny SQL three-valued-logic evaluator. NULL is null, and the point of
         the whole exercise is that `null IS TRUE` is false while
         `null IS NOT TRUE` is true — which is why = false loses rows and
         IS NOT TRUE does not. */
      function evalPred(sql, row) {
        return sql.split(' AND ').every((clause) => {
          const c = clause.trim();
          let m;
          if ((m = c.match(/^l\.(\w+) IS NOT NULL$/)))  return row[m[1]] !== null;
          if ((m = c.match(/^l\.(\w+) IS NULL$/)))      return row[m[1]] === null;
          if ((m = c.match(/^l\.(\w+) IS NOT TRUE$/)))  return row[m[1]] !== true;
          if ((m = c.match(/^l\.(\w+) IS TRUE$/)))      return row[m[1]] === true;
          if ((m = c.match(/^l\.(\w+) = true$/)))       return row[m[1]] === true;
          if ((m = c.match(/^l\.(\w+) = false$/)))      return row[m[1]] === false;
          throw new Error('unhandled clause: ' + c);
        });
      }

      const TRI = [true, false, null];
      const matrix = [];
      for (const booking_uid of ['uid-1', null])
        for (const disqualified of TRI)
          for (const completed of TRI)
            matrix.push({ booking_uid, disqualified, completed });

      eq('ladder: the matrix covers every flag combination including nulls', matrix.length, 18);

      let everyRowInExactlyOne = true;
      const orphans = [];
      const doubles = [];
      for (const row of matrix) {
        const hits = Object.keys(PREDS).filter((k) => evalPred(PREDS[k], row));
        if (hits.length !== 1) {
          everyRowInExactlyOne = false;
          const desc = JSON.stringify(row) + ' -> [' + hits.join(',') + ']';
          (hits.length === 0 ? orphans : doubles).push(desc);
        }
      }
      ok('ladder: every row lands in exactly one stage', everyRowInExactlyOne,
         'orphans: ' + orphans.join(' | ') + '  doubles: ' + doubles.join(' | '));
      ok('ladder: no row falls out of all four stages', orphans.length === 0, orphans.join(' | '));
      ok('ladder: no row is counted under two stages', doubles.length === 0, doubles.join(' | '));

      /* The four counts sum to the row count for every combination — the
         "exhaustive" half stated as arithmetic rather than as set membership. */
      const summed = Object.keys(PREDS)
        .reduce((n, k) => n + matrix.filter((r) => evalPred(PREDS[k], r)).length, 0);
      eq('ladder: the four stage counts sum to the total', summed, matrix.length);

      // Priority order, named case by case, so a regression says which rule broke.
      const stageOf = (row) => Object.keys(PREDS).find((k) => evalPred(PREDS[k], row));
      eq('ladder: booked beats disqualified',
         stageOf({ booking_uid: 'u', disqualified: true, completed: true }), 'booked');
      eq('ladder: disqualified beats completed',
         stageOf({ booking_uid: null, disqualified: true, completed: true }), 'disqualified');
      eq('ladder: completed beats step 1',
         stageOf({ booking_uid: null, disqualified: false, completed: true }), 'completed');
      eq('ladder: everything else is step 1',
         stageOf({ booking_uid: null, disqualified: false, completed: false }), 'step1');

      /* THE NULL CASE, named on its own. An old row with a null flag used to be
         unreachable from every filter. */
      eq('ladder: a null disqualified flag still lands in a stage',
         stageOf({ booking_uid: null, disqualified: null, completed: null }), 'step1');
      eq('ladder: a null disqualified flag does not hide a completion',
         stageOf({ booking_uid: null, disqualified: null, completed: true }), 'completed');

      ok('ladder: no filter uses = true or = false',
         !/= true|= false/.test(filters), filters);

      /* The client badge is the same ladder in the same order. If the two
         disagree, a row can be filtered under one stage and badged as another. */
      const badge = between(src, "'function stageBadge(l){", 'Step 1</span>');
      const badgeOrder = (badge.match(/booking_uid|disqualified|completed/g) || []);
      eq('ladder: the badge resolves in the same priority order',
         badgeOrder.slice(0, 3), ['booking_uid', 'disqualified', 'completed']);
    }

    /* ============================================================
       9. BOOKINGS — one definition of "holds a call slot"
       ============================================================ */
    {
      /* Question 1: "is this person an SDR target?" — no time comparison.
         One fragment, two call sites, so the SDR List and the "No booking yet"
         headline cannot drift into disagreeing about who has a booking while
         both claim to exclude bookers. */
      ok('booking: there is exactly one has-any-booking fragment',
         (src.match(/const noBookingAnywhereSql\s*=/g) || []).length === 1);
      const frag = between(src, 'const noBookingAnywhereSql', 'app.get(\'/monitor/metrics\'');
      ok('booking: the shared fragment has no time comparison',
         !/booked_at/.test(frag), frag);
      ok('booking: it dedupes on lower(email), never raw email',
         /LOWER\(booked\.email\) = LOWER\(/.test(frag));

      const uses = (src.match(/\$\{noBookingAnywhereSql\(/g) || []).length;
      eq('booking: both question-1 sites use it', uses, 2);

      const sdrRoute = between(src, "app.get('/monitor/sdr'", 'const leads = result.rows');
      ok('booking: the SDR route consumes the fragment',
         /\$\{noBookingAnywhereSql\('l\.email'\)\}/.test(sdrRoute), sdrRoute.slice(-400));
      ok('booking: and has no hand-rolled copy left behind',
         !/SELECT 1 FROM leads booked/.test(sdrRoute));

      const metrics = between(src, "app.get('/monitor/metrics'", "app.get('/monitor/funnel'");
      ok('booking: the no-booking-yet headline consumes the fragment',
         /\$\{noBookingAnywhereSql\('leads\.email'\)\}/.test(metrics));

      /* Question 3, "recovered bookings", is a THIRD shape and keeps its
         COALESCE: it is definitionally about ordering, and it reads the full
         history including rows that predate the booked_at column, where
         comparing a null yields null and the row quietly counts as un-booked. */
      ok('booking: recovered bookings still COALESCEs booked_at with created_at',
         /COALESCE\(booked\.booked_at, booked\.created_at\)|COALESCE\(b\.booked_at, b\.created_at\)/.test(metrics),
         (metrics.match(/COALESCE\([^)]*booked_at[^)]*\)/g) || []).join(' | '));

      /* The Pending recovery card asks question 2 and now says so. The old
         tooltip claimed "no booking on any other session of that email", which
         would make it agree with the SDR List — it does not, deliberately. */
      ok('booking: the pending-recovery tooltip no longer claims any-session',
         !/no booking on any other session of that email/.test(src));
      ok('booking: it names the since-the-session rule in words',
         /SINCE the session started/.test(src));
      ok('booking: and explains why it can disagree with the SDR List',
         /SDR List can disagree/.test(src));
    }


    /* ============================================================
       10. CLAIMS THE DASHBOARD MAKES ABOUT ITSELF
       ============================================================ */
    {
      /* The Overview card said "This is exactly the SDR List." It is not. The
         Overview number filters completed = true; the SDR route has no completed
         filter, so the SDR List is a strict SUPERSET. Anyone reconciling the two
         would have found them off by that group and gone hunting for a bug. */
      ok('claims: the card no longer says it is exactly the SDR List',
         !/This is exactly the SDR List/.test(src));
      ok('claims: it says which way the two differ',
         /SDR List is deliberately wider/.test(src) && /no completed filter/.test(src));
      ok('claims: the alert prose drops the same implication',
         !/have no booking on any session \\u2014 see SDR List/.test(src));

      /* The claim is only false because of this asymmetry — assert it still
         holds, so if someone adds a completed filter to the SDR route the
         tooltip becomes wrong in the other direction. */
      const sdrRoute = between(src, "app.get('/monitor/sdr'", 'const leads = result.rows');
      const metrics  = between(src, "app.get('/monitor/metrics'", "app.get('/monitor/funnel'");
      ok('claims: the SDR route still has no completed filter',
         !/l\.completed = true|l\.completed IS TRUE/.test(sdrRoute), sdrRoute.slice(0, 400));
      ok('claims: the Overview number still has one',
         /completed = true/.test(metrics));
    }

    /* ============================================================
       11. SDR EXPORT MATCHES WHAT IS ON SCREEN

       exportSDR sent format=csv and nothing else, so someone who searched
       "acme", saw four rows and hit Export got the entire list.
       ============================================================ */
    {
      // Both halves of the field list, lifted, and asserted equal.
      const serverList = (() => {
        const m = src.match(/const SDR_SEARCH_COLUMNS = (\[[^\]]+\]);/);
        if (!m) throw new Error('SDR_SEARCH_COLUMNS not found');
        return (new Function('return ' + m[1]))();
      })();
      const clientList = (() => {
        const m = src.match(/'var SDR_SEARCH_FIELDS=(\[[^\]]+\]);'/);
        if (!m) throw new Error('SDR_SEARCH_FIELDS not found');
        return (new Function('return ' + m[1]))();
      })();
      eq('sdr: the export searches the same four fields as the table',
         serverList.slice().sort(), clientList.slice().sort());
      eq('sdr: and it is the four the audit named', serverList.slice().sort(),
         ['company', 'email', 'enriched_industry', 'first_name']);

      const sdrRoute = between(src, 'const SDR_SEARCH_COLUMNS', 'const leads = result.rows');
      /* The placeholder is built as a string, not interpolated into a
         template literal. A first attempt wrote it as an interpolation and
         the "$" was eaten by String.replace's special patterns, producing
         "LIKE 1" instead of "LIKE $1" — so this asserts the generated SQL,
         not just the shape of the source. */
      ok('sdr: the search term is parameterised, never interpolated',
         /searchParams\.push\('%' \+ search \+ '%'\)/.test(sdrRoute)
         && /const ph = '\$' \+ searchParams\.length/.test(sdrRoute), sdrRoute.slice(0, 900));
      {
        // Build the clause with the real column list and check the output.
        const built = 'WHERE (' + serverList
          .map((c) => 'LOWER(COALESCE(' + c + ", '')) LIKE " + '$1')
          .join(' OR ') + ')';
        ok('sdr: the generated clause carries a real $1 placeholder',
           /LIKE \$1/.test(built) && !/LIKE 1\b/.test(built), built);
        eq('sdr: one placeholder per searched column',
           (built.match(/\$1/g) || []).length, serverList.length);
      }
      ok('sdr: the query receives the params array',
         /\`, searchParams\);/.test(sdrRoute));
      /* Filtering inside the deduped set would change WHICH row survives
         DISTINCT ON per email, so a search could return a different row than the
         unsearched table shows for the same person. */
      ok('sdr: the filter is applied outside the DISTINCT ON, not inside it',
         sdrRoute.indexOf('${searchSql}') > sdrRoute.indexOf(') deduped'), 'searchSql moved inside the dedupe');
      ok('sdr: an empty search adds no WHERE clause at all',
         /let searchSql = '';/.test(sdrRoute) && /if \(search\) \{/.test(sdrRoute));

      // And the export actually sends it.
      ok('sdr: export sends the search term',
         /format=csv"\+\(q\?"&search="\+encodeURIComponent\(q\)/.test(src));
      ok('sdr: the table search stays client-side',
         /function renderSDRTable\(allLeads\)\{'/.test(src) && /SDR_SEARCH_FIELDS\.some/.test(src));
    }

    /* ============================================================
       12. LEAD MAGNET — pill counts are totals, not a page

       The pills were computed client-side over whatever /monitor/lm-leads
       returned. That route is LIMIT 500 and the dashboard never sends a
       limit, so it is always 500 — and the pills read as totals. Past 500
       rows in the window every pill silently understated.
       ============================================================ */
    {
      const metricsRoute = between(lmsrc, "router.get('/monitor/lm-metrics'", "router.get('/monitor/lm-leads'");
      const leadsRoute   = between(lmsrc, "router.get('/monitor/lm-leads'", "router.post('/monitor/lm-delivered");

      ok('lm: the metrics route returns real status totals',
         /statusTotals:/.test(metricsRoute) && /AS all_count/.test(metricsRoute));
      const totalsQuery = between(metricsRoute, 'AS all_count', "SELECT COALESCE(entry_point");
      const leadsWhere  = between(leadsRoute, 'FROM lead_magnet_leads l', 'ORDER BY');
      ok('lm: the totals cover the same population as the leads route',
         /WHERE email IS NOT NULL/.test(totalsQuery)
         && /created_at > NOW\(\) - INTERVAL '\$\{days\} days'/.test(totalsQuery), totalsQuery);
      /* Stated as a comparison rather than two separate greps: if the leads
         route's window or email guard changes, the totals have to follow or
         the pills start describing a different set of people than the table. */
      const norm = (s) => s.replace(/\s+/g, ' ').replace(/\bl\./g, '').trim();
      ok('lm: and the two WHERE clauses agree, not just each look plausible',
         norm(leadsWhere).includes('email IS NOT NULL')
         && norm(totalsQuery).includes('email IS NOT NULL')
         && norm(leadsWhere).includes("created_at > NOW() - INTERVAL '${days} days'")
         && norm(totalsQuery).includes("created_at > NOW() - INTERVAL '${days} days'"),
         norm(leadsWhere) + '  ||  ' + norm(totalsQuery));

      /* The pills bucket by the status the leads route derives, so the two must
         agree about the null cases. delivered can be null, and the CASE lets a
         null fall through to 'awaiting'; a plain NOT delivered in the totals
         would have counted it nowhere. */
      ok('lm: sent is delivered IS TRUE, matching the CASE',
         /completed IS TRUE\s*\n?\s*AND delivered IS TRUE\)?\s*AS sent/.test(metricsRoute.replace(/\s+/g, ' '))
         || /AND delivered IS TRUE\)\s+AS sent/.test(metricsRoute.replace(/\s+/g, ' ')),
         metricsRoute.replace(/\s+/g, ' ').match(/COUNT[^)]*delivered[^)]*\)[^A]*AS \w+/g));
      ok('lm: awaiting is everything else that completed',
         /AND delivered IS NOT TRUE\) AS awaiting/.test(metricsRoute.replace(/\s+/g, ' ')));
      ok('lm: abandoned uses IS NOT TRUE so a null completed still lands somewhere',
         /completed IS NOT TRUE\) AS abandoned/.test(metricsRoute.replace(/\s+/g, ' ')));
      ok('lm: internal is split out, matching the pill that shows it',
         /is_internal IS TRUE\) AS internal/.test(metricsRoute.replace(/\s+/g, ' ')));

      /* Every bucket the client renders has to exist in the payload, or a pill
         silently reads zero. */
      const pillKeys = (() => {
        const m = src.match(/'var lmPillDefs=(\[.*?\]);'/);
        return (new Function('return ' + m[1]))().map((p) => p[0]);
      })();
      const payloadKeys = (metricsRoute.match(/^\s{10}(\w+):\s+parseInt/gm) || [])
        .map((s) => s.trim().split(':')[0]);
      ok('lm: every pill has a matching total in the payload',
         pillKeys.every((k) => payloadKeys.includes(k)),
         'pills ' + pillKeys.join(',') + ' vs payload ' + payloadKeys.join(','));

      ok('lm: the leads route reports the matching total, not just a page',
         /res\.json\(\{ leads: rows, total: totalRows\[0\]\.total, limit \}\)/.test(leadsRoute));
      ok('lm: the LIMIT is still there — this fixes the label, not the pagination',
         /LIMIT \$1/.test(leadsRoute));

      // Client side: pills read the server totals, and say so when they cannot.
      ok('lm: the totals are captured from the metrics payload',
         /'lmTotals=d\.statusTotals\|\|null;'/.test(src));
      {
        const render = between(src, "'function lmRender(){'", "'var rows=lmSearched()");
        ok('lm: and the pills actually READ them',
           /counts=\{all:lmTotals\.all,awaiting:lmTotals\.awaiting,sent:lmTotals\.sent,abandoned:lmTotals\.abandoned,internal:lmTotals\.internal\}/.test(render),
           render);
        ok('lm: the page-count fallback only runs when the totals are missing',
           /var counts,fromPage=!lmTotals;/.test(render) && /if\(lmTotals\)\{/.test(render), render);
      }
      ok('lm: a missing totals payload is labelled, not passed off as a total',
         /pill counts are for the loaded rows only, totals unavailable/.test(src));
      ok('lm: the table says how many of how many it is showing',
         /table shows the most recent "\+lmLeads\.length\+" of "\+lmShownOf/.test(src));
    }


    /* ============================================================
       13. ROUTE HARDENING
       ============================================================ */
    {
      /* A GET that rewrites lead rows and runs two ALTER TABLEs. A link
         prefetch, a chat client unfurling the URL, or a browser restoring the
         tab is enough to fire a GET — with apply=1 still in the query string. */
      ok('routes: website-recheck is a POST',
         /app\.post\('\/monitor\/website-recheck'/.test(src));
      ok('routes: and no GET for it survives',
         !/app\.get\('\/monitor\/website-recheck'/.test(src));

      const recheckDoc = between(src, 'HISTORICAL WEBSITE RECHECK', 'not look like a burst of scraping');
      ok('routes: the doc block above it says POST',
         /POST \/monitor\/website-recheck/.test(recheckDoc) && !/GET \/monitor\/website-recheck/.test(recheckDoc),
         recheckDoc);
      ok('routes: and gives the curl that actually works',
         /curl -X POST/.test(recheckDoc));
      ok('routes: the dry-run default is still documented',
         /Dry run is the default/.test(recheckDoc));

      const recheckBody = between(src, 'burst of scraping', 'const scope');
      ok('routes: it still refuses outright when MONITOR_TOKEN is unset',
         /MONITOR_TOKEN must be set before using this endpoint/.test(recheckBody), recheckBody);

      /* Nothing in the UI fetched it, so the method change has no client half.
         If that ever stops being true, this catches it. */
      {
        // Scoped to the dashboard's client JS, not a count of mentions in
        // the file — the doc block above the route names it three times.
        const clientBlock = between(src, "  const js = '<script>' +", "<\\/script></body></html>");
        ok('routes: the dashboard never fetches website-recheck',
           !/website-recheck/.test(clientBlock));
        const htmlBlock = between(src, "'</style></head><body>' +", "  const js = '<script>' +");
        ok('routes: and no link or form in the page points at it',
           !/website-recheck/.test(htmlBlock));
      }

      /* elv-health was the only /monitor* route with no token check. */
      const elvRoute = between(src, "app.get('/monitor/elv-health'", 'verify-website');
      ok('routes: elv-health is token-gated',
         /MONITOR_TOKEN/.test(elvRoute) && /Unauthorized/.test(elvRoute), elvRoute);
      ok('routes: and the dashboard sends the token',
         /fetch\(API\+"\/monitor\/elv-health"\+TP/.test(src));

      /* EVERY /monitor route, enumerated from the source, must check the token.
         Stated as a sweep rather than one assertion per route so a NEW ungated
         route fails this too — which is how elv-health slipped through. */
      const monitorRoutes = [...src.matchAll(/app\.(get|post)\('(\/monitor[^']*)'/g)]
        .map((m) => ({ method: m[1], path: m[2], at: m.index }));
      ok('routes: the sweep found the expected number of monitor routes',
         monitorRoutes.length >= 8, String(monitorRoutes.length));
      const ungated = monitorRoutes.filter((r, i) => {
        // Bounded at the NEXT handler, not a fixed window: a 700-char slice
        // ran past the end of a short route and picked up its neighbour's
        // token check, so an ungated route read as gated.
        const end = i + 1 < monitorRoutes.length ? monitorRoutes[i + 1].at : r.at + 700;
        return !/MONITOR_TOKEN/.test(src.slice(r.at, end));
      }).map((r) => r.method + ' ' + r.path);
      eq('routes: every monitor route checks MONITOR_TOKEN', ungated, []);

      /* The ELV row used to paint a calm grey "Unknown" when the probe could
         not be reached — the same "we could not check, styled as fine" the
         health tab was rebuilt to remove. It also never checked r.ok, so a 401
         would have rendered from an error body. */
      const elvClient = between(src, "'async function checkElv(){", "'var HCLS=");
      ok('routes: the ELV row checks the response status', /if\(!r\.ok\)throw/.test(elvClient), elvClient);
      ok('routes: an unreachable ELV probe paints red, not grey',
         /catch\(e\)\{badge\("s-elv","Could not check","br"\)/.test(elvClient), elvClient);
      ok('routes: no grey "Unknown" left on the ELV row',
         !/badge\("s-elv","Unknown","bx"\)/.test(src));
    }


    /* ============================================================
       14. FINAL PASS — labels match their source

       todayCount is COUNT(*) over leads in the last 24 hours. The alert
       called them "sessions", which is the same mislabel the chart above it
       had already had fixed to "Form entries per day (ET)".
       ============================================================ */
    {
      ok('final: the 24h alert calls them form entries, matching the chart',
         /No new form entries in the last 24 hours/.test(src));
      ok('final: and no longer calls a leads row count a session count',
         !/No new sessions in the last 24 hours/.test(src));

      /* The source of that number, asserted so the label and the query cannot
         drift apart again. */
      const metrics = between(src, "app.get('/monitor/metrics'", "app.get('/monitor/funnel'");
      ok('final: todayCount really is a leads row count',
         /SELECT COUNT\(\*\) AS count FROM leads WHERE created_at >= NOW\(\) - INTERVAL '24 hours'/.test(metrics));

      /* Batch A's own invariant, stated once: nothing here touches the lead path.
         The three blocking verdicts are decided client-side in gushwork-form.js
         and must not appear as server-side blocking decisions, and no health or
         monitor code may reach for a Meta event. */
      const healthBlock = between(src, '/* ══ SYSTEM HEALTH', "app.get('/monitor/health'");
      ok('final: the health checks fire no Meta events',
         !/sendMetaEvent|metaCapi|meta-capi/i.test(healthBlock));
      ok('final: and write nothing',
         !/\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bALTER\b/.test(healthBlock), healthBlock.slice(0, 200));

      /* The health route is read-only by construction — assert every query in it
         is a SELECT, so a future "while we are here" write cannot slip in. */
      const verbs = (healthBlock.match(/\b(SELECT|INSERT|UPDATE|DELETE|ALTER|DROP|CREATE)\b/g) || []);
      ok('final: every SQL verb in the health block is SELECT',
         verbs.length > 0 && verbs.every((v) => v === 'SELECT'), verbs.join(','));
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
  })();
}
