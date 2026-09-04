/* ============================================================
   Batch 2 verification.

   Same convention as test-batch1.js: the real functions are lifted out
   of index.js rather than copied, because a test that exercises a
   duplicate of the source can pass while production is broken.

   Dependency-free — no DATABASE_URL, no network. Where a function needs
   the pool, a stub records what it would have written, which is exactly
   the thing worth asserting: WHAT gets stored and what deliberately
   does not.

   Run:  node tests/test-batch2.js
   ============================================================ */

const fs = require('fs');
const path = require('path');
const src   = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const dbsrc = fs.readFileSync(path.join(__dirname, '..', 'db.js'), 'utf8');

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
function between(startMarker, endMarker) {
  const i = src.indexOf(startMarker);
  if (i === -1) throw new Error('marker not found: ' + startMarker);
  const j = src.indexOf(endMarker, i);
  if (j === -1) throw new Error('end marker not found: ' + endMarker);
  return src.slice(i, j);
}

/* ── Lift the real rule out of the shipped source ─────────────── */
const ruleSrc = [
  between('function normaliseElvStatus(raw)', '// ── THREE MUTUALLY EXCLUSIVE BUCKETS ──'),
  between('const WEBSITE_UNREACHABLE_REASONS = [', '\n];') + '\n];',
  between('const CATCHALL_STATUSES = [', 'function elvSoftTypoHint'),
].join('\n');

const R = (new Function(ruleSrc + `
  return { normaliseElvStatus, isUnverifiablePair,
           WEBSITE_UNREACHABLE_REASONS, CATCHALL_STATUSES, UNVERIFIABLE_PAIR_NOTE };
`))();

/* ============================================================
   1. The pair rule — the whole point of Item 1
   ============================================================ */
{
  const P = (elv, reason) => R.isUnverifiablePair({ elv_status: elv, website_check_reason: reason });

  // The lead that prompted this. Catch-all mailbox, website check timed out
  // and failed open. Both halves passed, neither verified anything.
  ok('pair: yo@yoyo.com shape (ok_for_all + timeout) flags', P('ok_for_all', 'timeout') === true);
  ok('pair: accept_all + dns_unresolved flags',              P('accept_all', 'dns_unresolved') === true);
  ok('pair: ok_for_all + doh_error flags',                   P('ok_for_all', 'doh_error') === true);
  ok('pair: ok_for_all + backend_error flags',               P('ok_for_all', 'backend_error') === true);

  // ELV prefixes probable/temporary variants. Missing the prefix would mean
  // the flag silently never fires for p_ok_for_all.
  ok('pair: p_ok_for_all is normalised and still flags',     P('p_ok_for_all', 'timeout') === true);

  // NEITHER SIGNAL ALONE MEANS ANYTHING. This is the half that keeps the
  // flag quiet enough to be worth reading.
  ok('pair: catch-all with a live site does not flag',       P('ok_for_all', 'content_clean') === false);
  ok('pair: catch-all with thin content does not flag',      P('ok_for_all', 'thin_content') === false);
  ok('pair: catch-all behind a bot wall does not flag',      P('ok_for_all', 'check_blocked') === false);
  ok('pair: catch-all + http_403 does not flag',             P('ok_for_all', 'http_403') === false);
  ok('pair: catch-all on a parked domain does not flag',     P('ok_for_all', 'parked_confirmed') === false);
  ok('pair: catch-all on nxdomain does not flag',            P('ok_for_all', 'nxdomain') === false);
  ok('pair: verified mailbox + timeout does not flag',       P('ok', 'timeout') === false);
  ok('pair: role mailbox + timeout does not flag',           P('role', 'timeout') === false);

  // Absence of a verdict is not a verdict.
  ok('pair: no elv_status does not flag',                    P(null, 'timeout') === false);
  ok('pair: no website reason does not flag',                P('ok_for_all', null) === false);
  ok('pair: empty website reason does not flag',             P('ok_for_all', '') === false);
  ok('pair: null row does not throw and does not flag',      R.isUnverifiablePair(null) === false);
  ok('pair: undefined row does not flag',                    R.isUnverifiablePair(undefined) === false);

  // AT ANY DOMAIN. The v5.6.0 approach only fired at ~30 hand-listed
  // household names, which is why yoyo.com sailed through. The rule must
  // not consult a domain list at all.
  ok('pair: flags at an unknown domain, not just big brands',
     P('ok_for_all', 'timeout') === true && !/BRAND_MAILBOX_DOMAINS/.test(src));
  ok('pair: the old hand-written domain list is gone',
     !/BRAND_MAILBOX_DOMAINS|isUnverifiableBrandMailbox/.test(src));
  ok('pair: the rule takes no domain argument',
     /function isUnverifiablePair\(row\)/.test(src));
  ok('pair: has one plain-English sentence for SDRs',
     typeof R.UNVERIFIABLE_PAIR_NOTE === 'string' && R.UNVERIFIABLE_PAIR_NOTE.length > 40
     && !/[_]{1}[a-z]+_/.test(R.UNVERIFIABLE_PAIR_NOTE), R.UNVERIFIABLE_PAIR_NOTE);
}

/* ============================================================
   2. The unreachable list must not overlap the other three
   ============================================================ */
{
  const lift = (name) => (new Function(
    between(`const ${name} = [`, '\n];') + '\n];\nreturn ' + name + ';'
  ))();
  const VERIFIED   = lift('WEBSITE_VERIFIED_REASONS');
  const WRITEABLE  = lift('RECHECK_WRITEABLE');
  const NEGATIVE   = lift('WEBSITE_NEGATIVE_REASONS');
  const UNREACH    = R.WEBSITE_UNREACHABLE_REASONS;

  const overlap = (a, b) => a.filter((x) => b.includes(x));
  // "We could not reach it" and "we checked and it passed" are mutually
  // exclusive statements. If a verdict is ever in both, the flag and the
  // Meta gate disagree about the same lead.
  eq('lists: unreachable vs verified do not overlap',  overlap(UNREACH, VERIFIED), []);
  eq('lists: unreachable vs writeable do not overlap', overlap(UNREACH, WRITEABLE), []);
  eq('lists: unreachable vs negative do not overlap',  overlap(UNREACH, NEGATIVE), []);

  // The specific exclusions the plan committed to, asserted so a later
  // "tidy-up" cannot quietly add them.
  ['check_blocked', 'thin_content', 'thin_content_wildcard', 'non_html',
   'parked_confirmed', 'for_sale_lander', 'nxdomain', 'no_dns_records',
   'content_clean', 'mx_only', 'ok', 'resolved'].forEach((r) => {
    ok(`lists: ${r} is NOT treated as unreachable`, !UNREACH.includes(r));
  });
  ['dns_unresolved', 'timeout', 'doh_error', 'fetch_error', 'backend_error'].forEach((r) => {
    ok(`lists: ${r} IS treated as unreachable`, UNREACH.includes(r));
  });
}

/* ============================================================
   3. What gets persisted, and what deliberately does not
   ============================================================ */
{
  const storeSrc = [
    between('const ELV_BLOCK = [', 'const ELV_INDETERMINATE'),
    between('function persistElvVerdict(email, status, valid, source)', 'function elvCheckUrl'),
    between('async function lookupElvStatus(email)', '/* The flag.'),
  ].join('\n');

  const writes = [];
  const reads  = [];
  let dbRow = null, dbThrows = false, memHit = null;
  const stubPool = { query: (sql, params) => {
    if (/^\s*INSERT INTO email_verifications/.test(sql)) { writes.push(params); return Promise.resolve({ rows: [] }); }
    reads.push(params);
    if (dbThrows) return Promise.reject(new Error('relation does not exist'));
    return Promise.resolve({ rows: dbRow ? [dbRow] : [] });
  } };
  const S = (new Function('pool', 'elvCacheGet', 'console', storeSrc + `
    return { persistElvVerdict, lookupElvStatus, ELV_BLOCK, ELV_PASS };
  `))(stubPool, () => memHit, { log() {}, warn() {} });

  // Only DEFINITIVE verdicts are stored. This single guard is what makes an
  // empty column mean "we deliberately do not know" rather than "fine".
  writes.length = 0;
  S.persistElvVerdict('a@x.com', 'ok_for_all', true, 'elv');
  S.persistElvVerdict('b@x.com', 'ok', true, 'elv');
  S.persistElvVerdict('c@x.com', 'disposable', false, 'elv');
  eq('persist: definitive verdicts are written', writes.length, 3);

  writes.length = 0;
  ['timeout', 'http_error', 'network_error', 'unknown', 'skipped', 'smtp_error',
   'no_connect', 'antispam_system', 'error_fallback'].forEach((s) =>
    S.persistElvVerdict('d@x.com', s, true, 'elv'));
  eq('persist: NOTHING inconclusive is ever written', writes.length, 0);

  // Guard matches the cache's, which is the invariant that lets /submit read
  // the table back and trust it.
  const cacheGuard   = between('function elvCacheSet(email, valid, status)', '\n}');
  const persistGuard = between('function persistElvVerdict(email, status, valid, source)', 'pool.query');
  ok('persist: uses the same guard as the in-memory cache',
     /!ELV_BLOCK\.includes\(status\) && !ELV_PASS\.includes\(status\)/.test(cacheGuard) &&
     /!ELV_BLOCK\.includes\(status\) && !ELV_PASS\.includes\(status\)/.test(persistGuard));

  // Read order: Postgres is the source of truth, memory is only a
  // sub-second backstop, and a miss is a miss rather than a guess.
  return (async () => {
    dbRow = { status: 'ok_for_all', checked_at: new Date('2026-08-20T10:00:00Z') };
    memHit = { status: 'ok', valid: true, at: Date.now() };
    let r = await S.lookupElvStatus('e@x.com');
    eq('lookup: the database wins over the memory cache', r.status, 'ok_for_all');
    eq('lookup: reports where the verdict came from', r.from, 'db');

    dbRow = null;
    r = await S.lookupElvStatus('e@x.com');
    eq('lookup: falls back to the memory cache on a DB miss', r && r.status, 'ok');
    eq('lookup: labels the memory fallback', r && r.from, 'memory');

    memHit = null;
    r = await S.lookupElvStatus('e@x.com');
    eq('lookup: a miss returns null, never a guess', r, null);

    dbThrows = true; memHit = { status: 'ok_for_all', valid: true, at: Date.now() };
    r = await S.lookupElvStatus('e@x.com');
    eq('lookup: a DB error degrades to memory instead of throwing', r && r.status, 'ok_for_all');
    dbThrows = false;

    eq('lookup: empty email short-circuits', await S.lookupElvStatus(''), null);
    eq('lookup: null email short-circuits', await S.lookupElvStatus(null), null);

    // It must never spend an ELV credit inline — that is the whole reason
    // the re-check runs after the response.
    const lookupBody = between('async function lookupElvStatus(email)', '/* The flag.');
    ok('lookup: never calls ELV on the critical path',
       !/fetch\(|elvRecheckStatusOnly/.test(lookupBody));

    /* ============================================================
       syncToAWS: a COALESCE against a never-NULL value is a NO-OP
       ------------------------------------------------------------
       This is the bug that put 14 people on the mirror with
       completed = false while Railway had true, so the sdr-calling dialer
       read form completers as step-1 drop-offs. The clause was
         completed = COALESCE(EXCLUDED.completed, gw_form_leads.completed)
       and the bound value is `data.completed || false` — never null. So the
       COALESCE never fell through and the incoming false always won. A
       /partial sync after a /submit sync clobbered the flag.

       Asserted STRUCTURALLY rather than column by column, so a future column
       added with the same shape is caught by this test rather than by a
       dialer calling the wrong people for three months.
       ============================================================ */
    {
      const sync = src.slice(src.indexOf('function syncToAWS'), src.indexOf('function syncBookingToAWS'));
      /* The boundary is the end of the SQL template literal, backtick-comma-
         bracket. Slicing on '], [' finds nothing — that was the first version
         of this test and it silently gave empty strings, which made three
         assertions fail loudly rather than pass vacuously. Worth the note:
         an empty slice that passes is the same class of bug as everything
         else in this file. */
      const bound   = /`,\s*\[/.exec(sync).index;
      const values  = sync.slice(bound, sync.indexOf(']).then'));
      const clauses = sync.slice(sync.indexOf('ON CONFLICT'), bound);

      /* Every column whose bound value can never be NULL. `|| null` is the
         safe majority and is deliberately excluded. */
      const neverNull = Array.from(
        values.matchAll(/data\.([a-z_]+)\s*(?:\|\||\?\?)\s*(?:false|true|1)\b/g)
      ).map((m) => m[1]);
      ok('mirror: the never-NULL binds are still the four we know about',
         JSON.stringify(neverNull.slice().sort()) ===
         JSON.stringify(['completed', 'disqualified', 'loops_sent', 'step_reached']),
         neverNull.join(','));

      /* THE RULE. For a never-NULL bind, a plain
         COALESCE(EXCLUDED.x, gw_form_leads.x) is a no-op that silently lets
         the incoming value win. It must be something that cannot regress:
         an OR for a monotonic flag, GREATEST for a monotonic number, or a
         deliberate bare EXCLUDED that someone has signed off. */
      for (const col of neverNull) {
        const m = new RegExp('^\\s*' + col + '\\s*=\\s*(.+?),\\s*$', 'm').exec(clauses);
        ok(`mirror: ${col} has a conflict clause at all`, !!m, clauses.slice(0, 120));
        if (!m) continue;
        const rhs = m[1];
        const noop = new RegExp('^COALESCE\\(EXCLUDED\\.' + col + ',\\s*gw_form_leads\\.' + col + '\\)$').test(rhs.trim());
        ok(`mirror: ${col} is not guarded by a no-op COALESCE`, !noop, rhs);
      }

      /* The two monotonic flags specifically. A follow-up email cannot be
         un-sent and a form submission cannot be un-submitted, so neither flag
         may ever go from true back to false. */
      for (const col of ['completed', 'loops_sent']) {
        const m = new RegExp('^\\s*' + col + '\\s*=\\s*(.+?),\\s*$', 'm').exec(clauses);
        ok(`mirror: ${col} can only ever turn ON`,
           /^\(COALESCE\(gw_form_leads\.\w+, false\) OR COALESCE\(EXCLUDED\.\w+, false\)\)$/.test(m[1].trim()),
           m[1]);
      }
      /* step_reached is monotonic by GREATEST, which is the same idea. */
      ok('mirror: step_reached is monotonic via GREATEST',
         /step_reached\s*=\s*GREATEST\(/.test(clauses));
      /* disqualified is a bare EXCLUDED and that is KNOWN and documented —
         it is why syncBookingToAWS and friends exist. Pinned so the day
         someone changes it, they do it deliberately. */
      ok('mirror: disqualified is still the known bare-EXCLUDED exception',
         /disqualified\s*=\s*EXCLUDED\.disqualified,/.test(clauses));

      /* submitted_at is NOT a submission time on the mirror. It is
         `new Date()` at sync time, written only when data.completed is
         truthy. Pinned with the comment that says so, because the column name
         invites every reader to assume the opposite. */
      ok('mirror: submitted_at is still bound to sync time, not a lead field',
         /data\.completed \? new Date\(\) : null/.test(values), values.slice(0, 80));
      ok('mirror: and the trap is documented at the bind site',
         /NOT the lead's submission time|sync clock|sync time, not/i.test(sync));
    }

    /* ============================================================
       backfill-sf.js — the selector, and the allow-list
       ------------------------------------------------------------
       5 Sept 2026. This tool writes to Salesforce and is reached for during
       a recovery, i.e. under time pressure, which is the worst moment to
       discover it replays a whole window. Two fixes, both asserted here.

       The real runBackfill is lifted and driven with stubs — the module-level
       require of ./salesforce would otherwise put a live Salesforce call in a
       dependency-free suite.
       ============================================================ */
    {
      const bfRaw = fs.readFileSync(path.join(__dirname, '..', 'backfill-sf.js'), 'utf8');
      const bfSrc = bfRaw
        .replace(/const \{[^}]*\} = require\('\.\/salesforce'\);/, '')
        .replace(/module\.exports = \{[^}]*\};/, '');
      const noC = (t) => t.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
      const bfCode = noC(bfRaw);

      /* ── The selector ──
         completed does not mean "submitted the form": the Cal and RevenueHero
         safety-net branches set it for someone who booked without touching the
         form, and pushToSalesforce runs from /submit. Selecting on completed
         creates Salesforce Leads for people who filled nothing in. */
      ok('backfill: selects on submitted_at, not completed',
         /WHERE submitted_at IS NOT NULL/.test(bfCode));
      ok('backfill: the old completed = true selector is gone',
         !/WHERE completed = true/.test(bfCode), bfCode.match(/WHERE completed[^\n]*/g));
      /* Equals-true also drops NULL rows entirely — its own open ticket. */
      ok('backfill: no equals-true flag test survives anywhere in the query',
         !/completed = true/.test(bfCode));
      /* An address list from a query string must never be interpolated. */
      ok('backfill: the allow-list is parameterised, not interpolated',
         /= ANY\(\$\$\{params\.length\}::text\[\]\)/.test(bfCode) ||
         /ANY\(\$\$\{params\.length\}/.test(bfCode), bfCode.match(/emailClause[^\n]*/g));

      /* ── parseEmails ── */
      const B = (new Function('pushToSalesforce', 'getSalesforceToken', 'fetch', 'setTimeout',
        bfSrc + '\n return { runBackfill, parseEmails };'))(
        async () => ({ success: true, leadId: 'L1' }),
        async () => ({ accessToken: 't', instanceUrl: 'https://sf' }),
        async () => ({ ok: true, json: async () => ({ records: [] }) }),
        (f) => f()
      );
      eq('backfill: parseEmails takes a comma string',
         B.parseEmails('A@x.com, b@y.com'), ['a@x.com', 'b@y.com']);
      eq('backfill: and whitespace', B.parseEmails('a@x.com b@y.com'), ['a@x.com', 'b@y.com']);
      eq('backfill: and an array', B.parseEmails(['A@x.com']), ['a@x.com']);
      eq('backfill: dedupes case variants', B.parseEmails('a@x.com,A@X.COM'), ['a@x.com']);
      eq('backfill: empty means no allow-list, not an empty one', B.parseEmails(''), null);
      eq('backfill: undefined likewise', B.parseEmails(undefined), null);

      /* ── DRIVEN ── */
      const mkPool = (rows) => {
        const seen = [];
        return {
          seen,
          query: async (sql, params) => {
            seen.push({ sql, params });
            if (/information_schema/.test(sql))
              return { rows: [{ column_name: 'email' }, { column_name: 'completed' },
                              { column_name: 'submitted_at' }, { column_name: 'booking_uid' },
                              { column_name: 'created_at' }, { column_name: 'updated_at' }] };
            return { rows };
          },
        };
      };
      const row = (email, extra) => Object.assign(
        { email, completed: true, submitted_at: '2026-06-15T00:00:00Z', booking_uid: null,
          created_at: '2026-06-15T00:00:00Z', updated_at: '2026-06-15T00:00:00Z' }, extra || {});

      /* An allow-list must widen the window. The default start is the July
         incident, and two of the six rows this was written for predate it —
         naming a June row and getting "found: 0" reads as "nothing to do". */
      {
        const pool = mkPool([]);
        const out = await B.runBackfill(pool, { emails: 'x@y.com', dry: true });
        ok('backfill: an allow-list opens the window past the July default',
           new Date(out.window.from) < new Date('2026-07-08T12:20:00Z'), out.window.from);
        const q = pool.seen.find((c) => /FROM leads/.test(c.sql));
        ok('backfill: the addresses travel as a bound parameter',
           Array.isArray(q.params[q.params.length - 1]) &&
           q.params[q.params.length - 1].includes('x@y.com'), JSON.stringify(q.params));
        /* Built is not applied. Deleting the interpolation while still
           building the clause and binding the parameter left every other
           assertion here green and quietly replayed the whole window — the
           computed-but-not-rendered bug, one layer down. So assert the clause
           reached the SQL that was actually EXECUTED. */
        ok('backfill: the clause reaches the executed SQL, not just the variable',
           /LOWER\(email\) = ANY\(\$\d+::text\[\]\)/.test(q.sql), q.sql);
        /* And that a run WITHOUT an allow-list carries no such clause, so the
           assertion above cannot pass by accident. */
        const bare = mkPool([]);
        await B.runBackfill(bare, { dry: true });
        const bq = bare.seen.find((c) => /FROM leads/.test(c.sql));
        ok('backfill: and is absent when no allow-list was given',
           !/= ANY\(/.test(bq.sql), bq.sql);
        /* Asked for and not found is REPORTED, never left as the gap between
           two counts: a typo, a row outside the window and a row that never
           submitted otherwise look identical from the summary. */
        eq('backfill: an address asked for and not matched is named',
           out.requestedNotFound, ['x@y.com']);
        eq('backfill: and counted', out.summary.notFound, 1);
        eq('backfill: the request size is reported too', out.summary.requested, 1);
      }
      /* No allow-list keeps the historical default, so an existing caller is
         unaffected. */
      {
        const out = await B.runBackfill(mkPool([]), { dry: true });
        eq('backfill: with no allow-list the default window is unchanged',
           out.window.from, '2026-07-08T12:20:00Z');
        eq('backfill: and nothing is reported as requested', out.summary.requested, null);
      }
      /* A dry run must not write, and must say which way each row would go. */
      {
        const pushes = [];
        const B2 = (new Function('pushToSalesforce', 'getSalesforceToken', 'fetch', 'setTimeout',
          bfSrc + '\n return { runBackfill };'))(
          async (p) => { pushes.push(p.email); return { success: true, leadId: 'L1' }; },
          async () => ({ accessToken: 't', instanceUrl: 'https://sf' }),
          async () => ({ ok: true, json: async () => ({ records: [] }) }),
          (f) => f()
        );
        const out = await B2.runBackfill(mkPool([
          row('real@customer.com'),
          row('someone@gushwork.ai'),
        ]), { emails: 'real@customer.com,someone@gushwork.ai', dry: true });
        eq('backfill: a dry run pushes nothing', pushes.length, 0);
        const byEmail = Object.fromEntries(out.results.map((r) => [r.email, r.action]));
        ok('backfill: a dry run says it WOULD create',
           /WOULD CREATE/.test(byEmail['real@customer.com'] || ''), JSON.stringify(byEmail));
        /* Internal addresses are skipped by design, so a gushwork.ai row is
           not a backfill candidate however it was asked for. */
        ok('backfill: an internal address is skipped even when named explicitly',
           /skipped \(test\/internal\)/.test(byEmail['someone@gushwork.ai'] || ''),
           JSON.stringify(byEmail));
      }
      /* The hard guard: a converted Lead is never touched. */
      {
        const pushes = [];
        const B3 = (new Function('pushToSalesforce', 'getSalesforceToken', 'fetch', 'setTimeout',
          bfSrc + '\n return { runBackfill };'))(
          async (p) => { pushes.push(p.email); return { success: true, leadId: 'L1' }; },
          async () => ({ accessToken: 't', instanceUrl: 'https://sf' }),
          async () => ({ ok: true, json: async () => ({ records: [{ Id: '00Q', IsConverted: true }] }) }),
          (f) => f()
        );
        const out = await B3.runBackfill(mkPool([row('converted@customer.com')]),
                                         { emails: 'converted@customer.com' });
        eq('backfill: a CONVERTED lead is never pushed', pushes.length, 0);
        ok('backfill: and says why',
           /CONVERTED/.test(out.results[0].action), JSON.stringify(out.results[0]));
      }
    }

    finish();
  })();
}

function finish() {

/* ============================================================
   4. Wiring — the store is written and read where it must be
   ============================================================ */
{
  const verifyRoute = between("app.post('/verify-email'", "app.get('/monitor/elv-health'");
  eq('wiring: /verify-email persists at every cache-write site',
     (verifyRoute.match(/persistElvVerdict\(/g) || []).length,
     (verifyRoute.match(/elvCacheSet\(/g) || []).length);
  ok('wiring: the persist call is never awaited (lead is waiting)',
     !/await persistElvVerdict/.test(src));
  ok('wiring: one place knows the ELV endpoint',
     (src.match(/apps\.emaillistverify\.com/g) || []).length === 1);

  const partial = between("app.post('/partial'", "app.post('/submit'");
  ok('partial: reads the stored verdict', /await lookupElvStatus\(email\)/.test(partial));
  /* [,)] not just ): the pair has to be present and adjacent in the column
     list, but it no longer has to END it — the PartnerStack columns follow. */
  ok('partial: writes elv_status into the row', /elv_status,elv_checked_at[,)]/.test(partial));
  ok('partial: never overwrites a stored verdict with a blank',
     /elv_status\s+= COALESCE\(EXCLUDED\.elv_status,\s+leads\.elv_status\)/.test(partial));

  const submit = between("app.post('/submit'", "app.post('/booking-confirmed'");
  ok('submit: reads the stored verdict', /await lookupElvStatus\(email\)/.test(submit));
  /* [,)] not just ): the pair has to be present and adjacent in the column
     list, but it no longer has to END it — the PartnerStack columns follow. */
  ok('submit: writes elv_status into the row', /elv_status,elv_checked_at[,)]/.test(submit));
  ok('submit: COALESCE-guarded like every other column',
     /elv_status\s+= COALESCE\(EXCLUDED\.elv_status,\s+leads\.elv_status\)/.test(submit));
  ok('submit: evaluates the flag', /alertUnverifiablePair\(\{ email, elv_status: elv\?\.status, website_check_reason \}\)/.test(submit));
  ok('submit: re-checks on a miss rather than storing a blank',
     /if \(!elv && !alreadyCompleted\) finaliseElvVerdict\(/.test(submit));
  ok('submit: the re-check runs AFTER the response, not before',
     submit.indexOf('res.json({ ok: true })') < submit.indexOf('finaliseElvVerdict({'));

  /* Adding two columns to a 30-parameter INSERT is the kind of edit that
     fails at runtime, on a real lead, with "bind message supplies 30
     parameters but prepared statement requires 32". There is no database in
     this suite to catch that, so the arithmetic is checked directly:
     columns == values, and the highest $N == the length of the params
     array. Both INSERTs, both routes. */
  const splitTop = (s) => {
    const out = []; let depth = 0, cur = '';
    for (const ch of s) {
      if (ch === '(') depth++;
      if (ch === ')') depth--;
      if (ch === ',' && depth === 0) { out.push(cur.trim()); cur = ''; } else cur += ch;
    }
    out.push(cur.trim());
    return out;
  };
  [['/partial', partial], ['/submit', submit]].forEach(([name, seg]) => {
    const body    = seg.slice(0, seg.indexOf('UPDATE leads SET enriched_city'));
    const cols    = splitTop(body.match(/INSERT INTO leads \(([\s\S]*?)\)\n/)[1]);
    const values  = splitTop(body.match(/VALUES \(([\s\S]*?)\)\n/)[1]);
    const arr     = body.slice(body.lastIndexOf('`, ['));
    const params  = splitTop(arr.slice(arr.indexOf('[') + 1, arr.lastIndexOf(']')));
    const highest = Math.max(...(body.match(/\$(\d+)/g) || []).map((s) => +s.slice(1)));
    eq(`${name}: INSERT column count matches the VALUES list`, values.length, cols.length);
    eq(`${name}: highest placeholder matches the params array length`, params.length, highest);
    /* elv_status and elv_checked_at must stay ADJACENT and in that order,
       because they are bound to two consecutive placeholders — swap them and
       a timestamp lands in the status column. This used to assert they were
       the LAST two, which was the same thing only for as long as nothing else
       was ever appended; the PartnerStack columns (v5.8.0) are now after them.
       Position was never the property worth protecting, pairing was, and the
       two arity assertions above cover the count. */
    const elvAt = cols.indexOf('elv_status');
    ok(`${name}: elv columns are present, adjacent and in order`,
       elvAt !== -1 && cols[elvAt + 1] === 'elv_checked_at',
       cols.slice(Math.max(0, elvAt), elvAt + 2).join(','));
  });

  // The re-check only ever returns something conclusive, and records its own
  // outcome in the ELV health window like any other check.
  const recheck = between('async function elvRecheckStatusOnly(email)', 'Read the stored verdict');
  ok('recheck: returns null unless the status is definitive',
     /return ELV_BLOCK\.includes\(status\) \|\| ELV_PASS\.includes\(status\) \? \{ status, valid \} : null/.test(recheck));
  ok('recheck: an HTTP failure returns null, not a verdict',
     /if \(!response\.ok\) \{ recordElvOutcome\('http_error', email\); return null; \}/.test(recheck));
  ok('recheck: a timeout returns null', /recordElvOutcome\(err && err\.name === 'AbortError' \? 'timeout' : 'network_error', email\);\s*\n\s*return null;/.test(recheck));
  ok('recheck: feeds the ELV health signal', /recordElvOutcome\(known \? status : 'unknown', email\)/.test(recheck));
  ok('recheck: tagged as its own source', /'submit_recheck'/.test(recheck));
  ok('recheck: skips internal test addresses',
     /if \(!email \|\| elvIsInternal\(email\)\) return;/.test(between('async function finaliseElvVerdict', '\n}')));
  ok('recheck: only fills a NULL column, never overwrites',
     /WHERE session_id=\$1 AND elv_status IS NULL/.test(src));
}

/* ============================================================
   5. The flag is visible where the lead is visible
   ============================================================ */
{
  ok('surface: alerts from /submit as a warning, never higher',
     /alertOps\('warning', 'Form', 'Nothing verified this lead'/.test(src));
  ok('surface: internal testing does not alert',
     /if \(elvIsInternal\(row\.email\)\) return false;/.test(src));
  ok('surface: on the completed-lead Slack card',
     between('function slackSubmit(d)', 'buildEnrichmentBlocks').includes('isUnverifiablePair(d)'));
  // yo@yoyo.com never submitted — it came through the partials cron. A flag
  // only on the submit path would have missed the lead that prompted it.
  ok('surface: on the drop-off Slack card too',
     between('function slackPartial(d)', 'sendSlack(blocks, label)').includes('isUnverifiablePair(d)'));
  const cron = between("app.post('/cron/send-partials'", 'const leads = result.rows');
  ok('surface: the cron actually selects both halves of the pair',
     /l\.elv_status/.test(cron) && /l\.website_check_reason/.test(cron));
  ok('surface: /monitor/leads returns the derived flag',
     /unverifiable_pair: isUnverifiablePair\(r\)/.test(src));
  ok('surface: /monitor/leads selects elv_status', /l\.elv_status, l\.elv_checked_at,/.test(src));
  ok('surface: the CSV carries it', /'elv_status','unverifiable_pair',/.test(src));
  ok('surface: the dashboard renders the precomputed flag',
     /l\.unverifiable_pair\?/.test(src));
  // One copy of the rule. The label map already taught us what two copies
  // cost, and the dashboard's script is a JS string where a second copy
  // would be invisible to every normal search.
  const dashStart = src.indexOf("app.get('/monitor', (req, res) => {");
  const dashboard = src.slice(dashStart, src.indexOf('\n});', dashStart));
  // Website verdict codes DO legitimately appear in there — the dashboard's
  // own copy of the label map needs them. The catch-all statuses are the
  // tell: they have no reason to exist client-side unless the pair rule has
  // been reimplemented in the browser.
  ok('surface: the dashboard renders the flag but does not re-derive it',
     /l\.unverifiable_pair/.test(dashboard)
     && !/WEBSITE_UNREACHABLE_REASONS|CATCHALL_STATUSES/.test(dashboard)
     && !/ok_for_all|accept_all/.test(dashboard));
  ok('surface: stored as a derived value, not a boolean column',
     !/unverifiable_pair (BOOLEAN|boolean)/.test(dbsrc) && !/SET unverifiable_pair/.test(src));
}

/* ============================================================
   6. db.js declares everything index.js writes
   ============================================================ */
{
  ok('db: email_verifications table declared', /CREATE TABLE IF NOT EXISTS email_verifications/.test(dbsrc));
  ok('db: keyed by email', /email\s+TEXT PRIMARY KEY/.test(dbsrc));
  ok('db: records the status', /status\s+TEXT NOT NULL/.test(dbsrc));
  ok('db: records when it was checked', /checked_at\s+TIMESTAMPTZ DEFAULT NOW\(\)/.test(dbsrc));
  ok('db: table creation is non-fatal (cannot take the form down)',
     /Email-verifications table init FAILED \(non-fatal\)/.test(dbsrc));
  ok('db: elv_status declared on leads', /ALTER TABLE leads ADD COLUMN IF NOT EXISTS elv_status TEXT/.test(dbsrc));
  ok('db: elv_checked_at declared on leads', /ALTER TABLE leads ADD COLUMN IF NOT EXISTS elv_checked_at TIMESTAMPTZ/.test(dbsrc));
  // Every column index.js writes to `leads` must be declared here — this is
  // the check that would have caught the website_check_* omission.
  const declared = new Set((dbsrc.match(/ADD COLUMN IF NOT EXISTS (\w+)/g) || [])
    .map((m) => m.replace('ADD COLUMN IF NOT EXISTS ', '')));
  ['elv_status', 'elv_checked_at', 'website_check_failed', 'website_check_reason'].forEach((c) => {
    ok(`db: ${c} is in the migrations array`, declared.has(c));
  });
  ok('db: migrations still run before the server listens',
     /await initDB\(\);[\s\S]{0,80}app\.listen/.test(src));
}

/* ============================================================
   7. Funnel — the 266.7 bug, reconstructed from the shipped rule
   ============================================================ */
{
  const funnel = between("app.get('/monitor/funnel'", "app.get('/monitor/duplicates'");

  /* Extracted from the ROUTE, not the whole file: prose in the comment
     block above it discusses step1_rate in English, and a regex over the
     file happily lifted a sentence instead of the expression. */
  const coveredSrc  = funnel.match(/const covered\s+=\s+(.+);/)[1];
  const coverageSrc = funnel.match(/const coverage\s+=\s+(.+);/)[1];
  const rateSrc     = funnel.match(/\n\s+step1_rate: (.+),\n/)[1];
  const orphanSrc   = funnel.match(/\n\s+orphan_leads: (.+),\n/)[1];
  ok('funnel: the extracted rate really is an expression, not prose',
     /covered/.test(rateSrc) && /sessions/.test(rateSrc), rateSrc);
  const decide = new Function('goLive', 'day', 'sessions', 'step1', 'rawOrphan', `
    const r = { day, orphan_leads: rawOrphan === undefined ? 0 : rawOrphan };
    const covered  = ${coveredSrc};
    const coverage = ${coverageSrc};
    return { coverage, step1_rate: ${rateSrc}, orphan_leads: ${orphanSrc} };
  `);

  const goLive = new Date('2026-08-20T10:32:00Z').getTime();
  const D = (s) => new Date(s + 'T00:00:00Z');

  // The reported number: sessions counted from 10:32, leads from midnight.
  const firstDay = decide(goLive, D('2026-08-20'), 3, 8);
  eq('funnel: the partial first day reports no rate', firstDay.step1_rate, null);
  eq('funnel: and says why', firstDay.coverage, 'partial');
  ok('funnel: the old formula really did produce 266.7',
     +(8 / 3 * 100).toFixed(1) === 266.7);

  const fullDay = decide(goLive, D('2026-08-21'), 40, 12);
  eq('funnel: a fully covered day still reports a rate', fullDay.step1_rate, 30);
  eq('funnel: and is labelled covered', fullDay.coverage, 'full');

  const before = decide(goLive, D('2026-08-19'), 0, 5);
  eq('funnel: a day before tracking reports no rate', before.step1_rate, null);
  eq('funnel: and is distinguished from partial', before.coverage, 'none');

  const never = decide(null, D('2026-08-21'), 0, 5);
  eq('funnel: no session tracking at all reports no rate', never.step1_rate, null);
  eq('funnel: and no coverage', never.coverage, 'none');

  // Boundary: tracking that began exactly at midnight covers the whole day.
  eq('funnel: go-live at midnight counts as full coverage',
     decide(new Date('2026-08-20T00:00:00Z').getTime(), D('2026-08-20'), 10, 3).coverage, 'full');
  // A covered day with a zero denominator still must not divide by zero.
  eq('funnel: zero sessions on a covered day yields null, not Infinity',
     decide(goLive, D('2026-08-21'), 0, 4).step1_rate, null);

  // Webhook leads: excluded from ALL THREE form counters. Excluding them from
  // step1 alone would push submit_rate over 100%, since these rows are
  // inserted with submitted_at and booking_uid already set.
  ok('funnel: webhook rule defined once', /const WEBHOOK_LEAD_SQL = /.test(src));
  ok('funnel: matches both fallback branches',
     /rh_webhook.*cal_webhook/.test(src.match(/const WEBHOOK_LEAD_SQL = "(.+)"/)[1]));
  ['step1', 'submitted', 'booked'].forEach((c) => {
    const line = funnel.split('\n').find((l) => l.includes(`AS ${c},`));
    ok(`funnel: ${c} excludes webhook leads`, !!line && line.includes('NOT (${WEBHOOK_LEAD_SQL})'), line);
  });
  ok('funnel: webhook leads reported separately', /AS webhook_leads/.test(funnel) && /AS webhook_booked/.test(funnel));
  ok('funnel: and summed for the window', /webhook_leads_in_window/.test(funnel));
  ok('funnel: the prefill values really are what the webhooks write',
     /prefill_source:'rh_webhook'/.test(src) && /'cal_webhook','B2B'/.test(src));
  ok('funnel: the update-existing paths do not set prefill_source (so form leads keep theirs)',
     !/UPDATE leads SET[^`]*prefill_source/.test(src));

  // submit_rate is leads/leads, so coverage is symmetric and it was never
  // affected by the first-day problem. Must stay unguarded by `covered`.
  ok('funnel: submit_rate is not nulled by coverage',
     /submit_rate: step1 > 0 \?/.test(funnel));

  /* ── orphan_leads coverage, from the numbers it got wrong ──
     Shipped counting every pre-go-live lead as an orphan, because a lead
     cannot match a session row that was never written. Real first run:
     session tracking began 2026-08-21T10:32:37Z and orphan_leads exactly
     equalled step1 on all seven prior days. */
  const GO_LIVE = new Date('2026-08-21T10:32:37Z').getTime();
  const REAL_PRE_GO_LIVE = [
    ['2026-08-14', 29], ['2026-08-15', 30], ['2026-08-16', 26], ['2026-08-17', 32],
    ['2026-08-18', 33], ['2026-08-19', 40], ['2026-08-20', 8],
  ];

  // The regression, stated in the terms it was reported in.
  eq('orphan: a "none" day with step1=29 yields null, not 29',
     decide(GO_LIVE, D('2026-08-14'), 0, 29, 29).orphan_leads, null);

  REAL_PRE_GO_LIVE.forEach(([day, n]) => {
    const row = decide(GO_LIVE, D(day), 0, n, n);
    eq(`orphan: ${day} (real: ${n}/${n}) is null`, row.orphan_leads, null);
    eq(`orphan: ${day} is labelled uncovered`, row.coverage, 'none');
  });

  eq('orphan: the seven real days summed to 198, none of it evidence',
     REAL_PRE_GO_LIVE.reduce((a, [, n]) => a + n, 0), 198);

  // The go-live day still reports — it has real coverage for part of itself,
  // and the SQL restricts the count to leads created after tracking began.
  eq('orphan: the partial go-live day still reports a number',
     decide(GO_LIVE, D('2026-08-21'), 12, 15, 3).orphan_leads, 3);
  eq('orphan: a fully covered day reports a number',
     decide(GO_LIVE, D('2026-08-22'), 40, 12, 0).orphan_leads, 0);
  eq('orphan: zero on a covered day is a real zero, not null',
     decide(GO_LIVE, D('2026-08-22'), 40, 12, 0).orphan_leads, 0);
  eq('orphan: no session tracking at all yields null everywhere',
     decide(null, D('2026-08-22'), 0, 5, 5).orphan_leads, null);

  // The SQL half of the same rule: pre-go-live leads are excluded in the
  // query, so even without the JS guard the count would not be 213.
  ok('orphan: the query itself excludes pre-go-live leads',
     /AND gl\.go_live IS NOT NULL\s*\n\s*AND l\.created_at >= gl\.go_live/.test(funnel));
  ok('orphan: go_live comes from a joinable CTE, not a subquery in FILTER',
     /gl AS \(SELECT MIN\(created_at\) AS go_live FROM form_sessions\)/.test(funnel)
     && /CROSS JOIN gl/.test(funnel));

  // The window total must skip the nulls rather than summing through them:
  // a naive reduce over null gives NaN, and `|| 0` would give a quiet zero
  // that reads as "checked, nothing dropped".
  const measuredSrc = funnel.match(/const measured = (.+);/)[1];
  const totalSrc    = funnel.match(/orphan_leads_in_window:\s+(.+),\n/)[1];
  // Caught rather than thrown: if the expression is changed to something
  // that needs a helper this sandbox does not have (sum('orphan_leads'), for
  // instance) that is a failure to report, not a reason to kill the run.
  const windowCalc = (rows) => {
    try {
      return (new Function('rows', `
        const measured = ${measuredSrc};
        return { total: ${totalSrc}, days: measured.length };
      `))(rows);
    } catch (err) {
      return { total: 'lift failed: ' + err.message, days: -1 };
    }
  };
  const realWindow = windowCalc([
    ...REAL_PRE_GO_LIVE.map(() => ({ orphan_leads: null })),
    { orphan_leads: 3 },
  ]);
  eq('orphan: the window total counts only measured days', realWindow.total, 3);
  eq('orphan: and reports how many days that was', realWindow.days, 1);
  ok('orphan: the total is a number, not NaN', Number.isFinite(realWindow.total));
  eq('orphan: an all-null window totals 0 over 0 days',
     windowCalc([{ orphan_leads: null }, { orphan_leads: null }]), { total: 0, days: 0 });
  ok('orphan: the response says how many days were measured',
     /orphan_leads_days_measured/.test(funnel) && /orphan_leads_days_in_window/.test(funnel));

  /* Counts of rows that EXIST in form_sessions are not nulled: zero
     recorded page loads is literally true on an uncovered day, and it is an
     input rather than a derived claim. Asserted so a later "consistency"
     pass does not null them by analogy. */
  ok('orphan: bot_sessions stays a plain count',
     /bot_sessions: Number\(r\.bot_sessions\)/.test(funnel));
  ok('orphan: multi_page_sessions stays a plain count',
     /multi_page_sessions: Number\(r\.multi_page_sessions\)/.test(funnel));
  ok('orphan: only orphan_leads is coverage-gated',
     (funnel.match(/coverage === 'none' \? null/g) || []).length === 1);
  ok('orphan: rate_note explains the null and why',
     /orphan_leads is null wherever session_coverage is "none"/.test(funnel)
     && /measure when tracking started rather than whether writes are being dropped/.test(funnel));

  // Orphan leads and multi-page sessions — measured, not corrected.
  ok('funnel: orphan leads counted', /AS orphan_leads/.test(funnel));
  ok('funnel: orphan check survives either session_id column type',
     /fs\.session_id = l\.session_id::text/.test(funnel));
  ok('funnel: orphan check excludes webhook rows (they can never have a session)',
     /AND NOT \(\$\{WEBHOOK_LEAD_SQL\}\)\s*\n\s*AND fs\.session_id IS NULL/.test(funnel));
  ok('funnel: multi-page sessions counted from hits', /hits > 1/.test(funnel));
  ok('funnel: multi-page count uses the same bot exclusion as sessions',
     /hits > 1 AND \(user_agent IS NULL OR user_agent !~\* \$2\)/.test(funnel));

  // Postgres rejects a subquery inside an aggregate FILTER clause
  // ("cannot use subquery in FILTER"). This suite cannot run SQL, so the
  // shape is asserted instead — it is the mistake this query nearly shipped.
  const query = funnel.slice(funnel.indexOf('WITH s AS ('), funnel.indexOf('ORDER BY 1 DESC'));
  const filters = query.match(/FILTER\s*\(([\s\S]*?)\)\s*AS/g) || [];
  ok('funnel: FILTER clauses exist to check', filters.length >= 6, String(filters.length));
  ok('funnel: no FILTER clause contains a subquery',
     filters.every((f) => !/\bSELECT\b/i.test(f)), filters.filter((f) => /\bSELECT\b/i.test(f)).join(' | '));
  ok('funnel: the join that replaced it cannot multiply rows (session_id is UNIQUE)',
     /session_id\s+TEXT UNIQUE NOT NULL/.test(dbsrc) && /LEFT JOIN form_sessions fs/.test(funnel));
  // Two placeholders, used by both CTEs. A third would silently break.
  ok('funnel: still exactly two bound parameters',
     /\[String\(days\), BOT_RE\]/.test(funnel) && !/\$3/.test(query));
}

/* ============================================================
   8. Regression guards — blocking and Meta must be untouched
   ============================================================ */
{
  // The three lists, byte for byte.
  ok('regress: WEBSITE_VERIFIED_REASONS unchanged',
     src.includes(`const WEBSITE_VERIFIED_REASONS = [
  'resolved', 'mx_only', 'content_clean', 'test_email_skipped', 'ok',
  'forwarded_to_live_site', 'live_despite_dns_hint',
  'thin_content', 'thin_content_wildcard', 'nxdomain_contradicted',
  'check_blocked',
];`));
  ok('regress: RECHECK_PROTECTED unchanged',
     /const RECHECK_PROTECTED = \['brand_mismatch', 'mailbox_domain', 'social_profile_url', 'test_email_skipped', 'unparseable'\]/.test(src));
  ok('regress: WEBSITE_NEGATIVE_REASONS unchanged',
     /const WEBSITE_NEGATIVE_REASONS = \['for_sale_lander', 'marketplace_redirect', 'parked_confirmed', 'hosting_placeholder'\]/.test(src));

  // No fourth blocking verdict. Blocking still lives entirely in the frontend.
  ok('regress: index.js still defines no blocking list',
     !/const WEBSITE_BLOCKING_REASONS\s*=/.test(src));
  ok('regress: the Meta gate is unchanged',
     src.includes(`function isWebsiteVerified(row) {
  if (!row) return true;
  if (row.website_check_failed === true) return false;
  const reason = row.website_check_reason;
  if (reason === null || reason === undefined || reason === '') return true; // pre-feature rows
  return WEBSITE_VERIFIED_REASONS.includes(reason);
}`));
  // The flag must be inert to everything that decides an outcome.
  ok('regress: the flag never gates Meta',
     !/isUnverifiablePair[\s\S]{0,200}pushFormEventsToMeta/.test(src) &&
     !/isUnverifiablePair[\s\S]{0,200}pushStartTrialToMeta/.test(src));
  ok('regress: the flag never gates the CAPI Lead call',
     /if \(isWebsiteVerified\(\{ website_check_failed, website_check_reason \}\)\) \{/.test(src));
  ok('regress: the flag never returns valid:false',
     !/isUnverifiablePair[\s\S]{0,120}valid: false/.test(src));
  ok('regress: the flag never blocks or disqualifies',
     !/isUnverifiablePair[\s\S]{0,200}(disqualified\s*=|res\.status\(4)/.test(src));
  ok('regress: catch-all statuses still PASS in ELV',
     /const ELV_PASS = \['ok', 'ok_for_all', 'accept_all', 'role'\]/.test(src));
  ok('regress: /verify-email still fails open on error',
     /res\.json\(\{ valid: true, status: 'error_fallback' \}\)/.test(src));
  ok('regress: /verify-email still fails open on HTTP failure',
     /recordElvOutcome\('http_error', email\);\s*\n\s*return res\.json\(\{ valid: true, status: 'http_error' \}\)/.test(src));
  ok('regress: StartTrial gate untouched',
     /const isBusinessEmail = !!email && !freeMatch;/.test(src));
  ok('regress: the follow-up email is not suppressed for flagged leads',
     !/isUnverifiablePair[\s\S]{0,200}sendFollowUpEmail/.test(src));
  ok('regress: the duplicate-booking guard is untouched (known, deferred)',
     /ORDER BY created_at DESC LIMIT 1', \[email\]\)/.test(src));
}

/* ============================================================ */
console.log('');
console.log(`  passed: ${pass}`);
console.log(`  failed: ${fail}`);
if (failures.length) {
  console.log('');
  failures.forEach((f) => console.log('  ✗ ' + f));
}
console.log('');
process.exit(fail === 0 ? 0 : 1);

}
