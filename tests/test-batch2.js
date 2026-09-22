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

require('./crash-reporter')('test-batch2');

const fs = require('fs');
const path = require('path');
const src   = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const dbsrc = fs.readFileSync(path.join(__dirname, '..', 'db.js'), 'utf8');

let pass = 0, fail = 0;
let results16 = async () => [];
let results19 = async () => [];   // section 19 is async too; invoked by the tail   // section 16 is async; invoked by the tail AFTER section 12
let results20 = async () => [];   // section 20 is async too; invoked by the tail
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
      /* FIVE as of Sept 2026: non_icp_blocked joined them with the non-ICP
         block. Like the other four its bind can never be NULL, so the rule
         below applies to it and its conflict clause is an OR, not a COALESCE
         no-op -- a block must not be clearable by a later partial sync. */
      ok('mirror: the never-NULL binds are still the five we know about',
         JSON.stringify(neverNull.slice().sort()) ===
         JSON.stringify(['completed', 'disqualified', 'loops_sent', 'non_icp_blocked', 'step_reached']),
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
     /if \(!elv && !alreadySubmitted\) finaliseElvVerdict\(/.test(submit));
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
    /* Match the closing bracket by depth from the opening one, rather
       than taking the LAST ']' in the segment. lastIndexOf reached past
       the end of the params array into ordinary code that follows the
       query -- recordLeadFieldChanges(session_id, upsert.rows[0], ...)
       was enough to inflate the count by one and fail an assertion whose
       arithmetic was correct. The check is unchanged; only the slice that
       feeds it is now anchored. */
    const closeBracket = (str, open) => {
      let depth = 0;
      for (let i = open; i < str.length; i++) {
        if (str[i] === '[') depth++;
        else if (str[i] === ']') { depth--; if (depth === 0) return i; }
      }
      return str.length;
    };
    const open    = arr.indexOf('[');
    const params  = splitTop(arr.slice(open + 1, closeBracket(arr, open)));
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

/* ── EVERY recordFailure SOURCE MUST HAVE A MONITOR ENTRY ─────────────
   recordFailure opens with `const cfg = FAILURE_MONITORS[source]; if (!cfg)
   return;`. A source string with no entry in that table is therefore a SILENT
   NO-OP — it looks like alerting at the call site, reads like alerting in a
   review card, and does nothing at all.

   That is not hypothetical. 'PartnerStack' had no entry from the day the
   integration shipped, so 21 call sites across the money path had never once
   produced an alert: the conversion-retry exhaustion, the Partner_Source__c
   permission failure, an unreadable Salesforce during the qualification poll,
   a phantom conversion, every stuck claim, the SF-state cap, the batched-write
   failure. Several of those were described as "loud" in review cards. Found
   7 Sept 2026 by executing recordFailure rather than reading it.

   So this asserts the PROPERTY rather than the instance: derive every source
   string actually passed to recordFailure anywhere in index.js, and require
   each one to exist in FAILURE_MONITORS. A future source added without an
   entry fails here instead of silently alerting nobody.

   Note what is NOT affected, so a reader does not over-correct: a conversion
   or qualification failure goes through recordPartnerStackFailure, which calls
   alertOps directly and never consults this table, and the health rows run
   their own queries. Those always worked. */
{
  const monBlock = src.slice(src.indexOf('const FAILURE_MONITORS'),
                             src.indexOf('\n};', src.indexOf('const FAILURE_MONITORS')));
  const configured = new Set(
    /* WIDENED 15 Sept 2026. The class was [A-Za-z][A-Za-z ]*? — letters and
       spaces only — so a HYPHENATED source name was invisible to it and
       this check reported a correctly-registered source as a silent no-op.
       'Non-ICP model' was registered, worked, and failed here anyway.

       The direction of the error is what made it worth fixing rather than
       silencing: a derivation that cannot see a key will always claim the
       alert is dead, which is the false positive that gets a real check
       ignored. Matches any quoted key now. */
    Array.from(monBlock.matchAll(/^\s*'([^']+)'\s*:\s*\{\s*alertAfter/gm))
         .map((m) => m[1]));
  ok('failmon: the monitor table was parsed at all', configured.size >= 5, [...configured].join(','));

  /* Literal first arguments only. A computed source cannot be checked here and
     there are none today — asserted, so introducing one is a decision. */
  const used = new Set(
    Array.from(src.matchAll(/recordFailure\(\s*'([^']+)'/g)).map((m) => m[1]));
  ok('failmon: recordFailure call sites were found', used.size >= 4, [...used].join(','));
  /* Excludes the declaration `function recordFailure(source, id, error)`,
     which is the only non-literal match and is obviously not a call site. */
  const callSites = src.replace(/function recordFailure\([^)]*\)/g, '');
  ok('failmon: every recordFailure source is a literal string',
     !/recordFailure\(\s*[^'\s)]/.test(callSites), 'a computed source cannot be verified');

  for (const source of [...used].sort()) {
    ok(`failmon: '${source}' has a FAILURE_MONITORS entry, so it can actually alert`,
       configured.has(source), `'${source}' is a SILENT no-op — ${(src.match(new RegExp("recordFailure\\(\\s*'" + source.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + "'", 'g')) || []).length} call site(s) affected`);
  }
  /* Each entry needs the two fields recordFailure and its alerts actually
     read, or the alert fires with an undefined impact line. */
  for (const source of [...configured].sort()) {
    const entry = (new RegExp("'?" + source + "'?\\s*:\\s*\\{([^}]*)\\}").exec(monBlock) || [])[1] || '';
    ok(`failmon: '${source}' declares alertAfter and impact`,
       /alertAfter:\s*\d+/.test(entry) && /impact:\s*'/.test(entry), source);
  }
}

/* ============================================================
   9. The Schedule payload — no column may be shadowed by the join

   SELECT * over leads LEFT JOIN enrichment_data was silently stripping em
   off every Schedule event for a lead Apollo never enriched. Both tables
   carry session_id and email; node-postgres assigns row fields in column
   order, so enrichment_data (last) won, and a LEFT JOIN miss wrote NULL
   over a real address. Free-email leads skip /enrich entirely, so they
   have no enrichment row at all — and em is the primary match key.

   The guard above it did not catch this: it reads a separate
   SELECT email FROM leads, so it saw an address the payload then dropped.

   Asserted two ways on purpose. The source assertions pin the shape; the
   last three EXECUTE node-postgres's own row builder against the real
   column list, because "the columns are named" and "a real address
   survives the join" are different claims.
   ============================================================ */
{
  const m = /const SCHEDULE_LEAD_SQL = `([\s\S]*?)`;/.exec(src);
  ok('schedule: SCHEDULE_LEAD_SQL is defined', !!m);
  const sql = m ? m[1] : '';

  ok('schedule: the Schedule lead lookup is not SELECT *', !/SELECT\s+\*/i.test(sql));
  ok('schedule: no SELECT * over this join is left anywhere in index.js',
     !/SELECT \* FROM leads l LEFT JOIN enrichment_data/i.test(src));

  /* All three booking routes share ONE query. Booking arrives by three
     routes, and a fix on one is a fix on one third. */
  ok('schedule: all three booking routes use the shared query',
     (src.match(/pool\.query\(SCHEDULE_LEAD_SQL,/g) || []).length === 3);

  // Output column names, as Postgres would name them.
  const body = sql.slice(sql.indexOf('SELECT') + 6, sql.indexOf('FROM'));
  const outCols = [], srcOf = {};
  for (const raw of body.split(/,(?![^(]*\))/)) {
    const part = raw.trim();
    if (!part) continue;
    const alias = /\bAS\s+(\w+)\s*$/i.exec(part);
    if (alias) { outCols.push(alias[1]); srcOf[alias[1]] = 'coalesce'; continue; }
    const plain = /^([le])\.(\w+)$/.exec(part);
    if (plain) { outCols.push(plain[2]); srcOf[plain[2]] = plain[1]; }
  }
  const dupes = outCols.filter((c, i) => outCols.indexOf(c) !== i);
  ok('schedule: every output column name is unique', dupes.length === 0,
     'duplicated: ' + dupes.join(', '));
  ok('schedule: email is taken from leads, never the join',      srcOf.email === 'l');
  ok('schedule: session_id is taken from leads, never the join', srcOf.session_id === 'l');

  /* Every column the two consumers read must be selected. A missing one is
     invisible here and shows up only as a quietly worse event in Meta. */
  for (const f of ['session_id','email','phone','first_name','last_name','company',
                   'sell_to','page_url','landing_page','fbc','fbp',
                   'enriched_city','enriched_state','enriched_country',
                   'enriched_company_size','enriched_industry','enriched_seniority',
                   'enriched_funding_stage'])
    ok('schedule: sendEvent input ' + f + ' is selected', outCols.includes(f));
  for (const f of ['website_check_failed','website_check_reason'])
    ok('schedule: isWebsiteVerified input ' + f + ' is selected', outCols.includes(f));

  /* Every referenced column must exist, on the table it is read from. A typo
     here does not fail loudly — the query rejects and Schedule never fires. */
  const leadsCols = new Set(), enrichCols = new Set();
  for (const table of [['leads', leadsCols], ['enrichment_data', enrichCols]]) {
    const b = new RegExp('CREATE TABLE IF NOT EXISTS ' + table[0] + '\\s*\\(([\\s\\S]*?)\\n      \\);').exec(dbsrc);
    if (b) for (const line of b[1].split('\n')) {
      const c = /^\s*(\w+)\s+(SERIAL|UUID|TEXT|INT|INTEGER|BOOLEAN|TIMESTAMPTZ|JSONB)/.exec(line);
      if (c) table[1].add(c[1]);
    }
  }
  for (const mm of dbsrc.matchAll(/ALTER TABLE (leads|enrichment_data) ADD COLUMN IF NOT EXISTS (\w+)/g))
    (mm[1] === 'leads' ? leadsCols : enrichCols).add(mm[2]);
  const unknown = [];
  for (const ref of sql.matchAll(/\b([le])\.(\w+)/g))
    if (!(ref[1] === 'l' ? leadsCols : enrichCols).has(ref[2]))
      unknown.push((ref[1] === 'l' ? 'leads.' : 'enrichment_data.') + ref[2]);
  ok('schedule: every referenced column exists in the schema', unknown.length === 0,
     'unknown: ' + unknown.join(', '));

  /* Each COALESCE must read enrichment_data FIRST and leads second, on the
     same column, aliased to that column. Flipping the arms, or pointing both
     at one table, silently stops consulting enrichment and every assertion
     above still passes — measured, it survived. */
  let badCoalesce = [];
  for (const c of sql.matchAll(/COALESCE\(\s*(\w+)\.(\w+),\s*(\w+)\.(\w+)\s*\)\s*AS\s+(\w+)/g)) {
    const [, t1, c1, t2, c2, alias] = c;
    if (!(t1 === 'e' && t2 === 'l' && c1 === c2 && alias === c1)) badCoalesce.push(c[0].replace(/\s+/g, ' '));
  }
  ok('schedule: every COALESCE is enrichment-first, leads-second, same column',
     badCoalesce.length === 0, badCoalesce.join(' | '));
  ok('schedule: the enriched columns are all COALESCEd, not taken from one table',
     (sql.match(/COALESCE\(/g) || []).length === 7);

  /* EXECUTED, not read. node-postgres's own row builder, driven with the real
     column list, for the exact case that caused this: no enrichment row. */
  const PgResult = require('pg/lib/result.js');
  const build = (names, values) => {
    const r = new PgResult(undefined, undefined);
    r.addFields(names.map((n) => ({ name: n, dataTypeID: 25, format: 'text' })));
    return r.parseRow(values);
  };
  const before = build(
    ['session_id', 'email', 'page_url', 'id', 'session_id', 'email'],
    ['sess-1', 'lead@gmail.com', 'https://gushwork.ai/demo', null, null, null]);
  ok('schedule: (control) the old SELECT * shape really did null out email',
     before.email === null && before.session_id === null);
  const after = build(outCols, outCols.map((c) =>
    c === 'session_id' ? 'sess-1' :
    c === 'email'      ? 'lead@gmail.com' :
    c === 'page_url'   ? 'https://gushwork.ai/demo' : null));
  ok('schedule: a free-email lead now keeps its email through the join',
     after.email === 'lead@gmail.com', JSON.stringify(after.email));
  ok('schedule: and keeps its session_id, so event_id stays stable',
     after.session_id === 'sess-1');
  ok('schedule: and keeps page_url, which is what carries the product',
     after.page_url === 'https://gushwork.ai/demo');
}

/* ============================================================
   10. Product tagging — AEO (/demo) vs CRM (/ai-demo)

   Meta has to tell two products apart on ONE pixel. The slug is resolved
   from page_url's pathname and nothing else: real ad traffic lands on
   /start and submits on /demo, so a landing_page lookup would match
   neither page and tag every ad lead as unknown.

   This section EXECUTES meta-capi.js rather than reading it. The payload
   is what Meta actually receives, and no source-level assertion can tell
   you what a built object contains.

   The Contact golden below was captured by running the PREVIOUS module
   (git show HEAD:meta-capi.js) against the same input and diffing — not
   typed by hand. The lead magnet must not acquire product fields.
   ============================================================ */
{
  const META = require('../meta-capi.js');
  /* Neither buildEventData nor resolveProduct may EVER throw: both run inside
     the lead path, where an exception would take the event with it, and an
     unrecognised form page must send its event untagged rather than fail.

     So every call in this section goes through these two, which RECORD a throw
     and carry on. Without that, a throwing build does not fail an assertion —
     it crashes the suite, and a crash prints no totals and reads as neither a
     pass nor a catch. Measured: three mutations that made this code throw came
     back UNMEASURED until this existed. The tally is asserted at the end. */
  let threwCount = 0, threwFirst = '';
  const record = (e) => { threwCount++; if (!threwFirst) threwFirst = e.message; };
  const cd = (ev, payload) => {
    try { return META.buildEventData(ev, payload, {}).custom_data; }
    catch (e) { record(e); return {}; }
  };
  const rp = (url) => {
    try { return META.resolveProduct({ page_url: url }); }
    catch (e) { record(e); return '__threw__'; }
  };
  const build = (ev, payload, options) => {
    try { return META.buildEventData(ev, payload, options); }
    catch (e) { record(e); return { user_data: {}, custom_data: {} }; }
  };
  const FIVE = ['content_ids', 'content_type', 'value', 'currency', 'predicted_ltv'];

  /* Calls buildEventData DIRECTLY, not through cd(): cd already swallows a
     throw into the tally, so routing this through it would leave every
     "builds rather than throwing" assertion below unable to fail. Not
     hypothetical — it was written that way first, and only the section-end
     tally was catching a throwing build. */
  const safeCd = (ev, payload) => {
    try { return { threw: false, custom_data: META.buildEventData(ev, payload, {}).custom_data }; }
    catch (e) { return { threw: true, custom_data: {}, error: e.message }; }
  };

  /* ── The catalogue. THREE KEYS, and the third is not a product you
     can tick -- 'aeo,crm' is what ticking both produces. content_ids is
     an array by design, which is how ONE event covers both products:
     two events would count one person twice, and every active ad set
     optimises on conversion count. */
  eq('product: the catalogue has three event slugs, including the combined one',
     Object.keys(META.PRODUCTS).sort(), ['aeo', 'aeo,crm', 'crm']);
  eq('product: aeo carries one content id', META.PRODUCTS.aeo, { content_ids: ['aeo'] });
  eq('product: crm carries one content id', META.PRODUCTS.crm, { content_ids: ['crm'] });
  eq('product: the combined slug carries BOTH content ids',
     META.PRODUCTS['aeo,crm'], { content_ids: ['aeo', 'crm'] });
  /* And the tickable vocabulary is NOT the catalogue keys. Deriving one
     from the other would let 'aeo,crm' through as a single checkbox
     value and into the restricted Salesforce picklist as a duplicate. */
  eq('product: only aeo and crm are tickable',
     META.PRODUCT_INTEREST_SLUGS.slice().sort(), ['aeo', 'crm']);

  /* ── PREDICTED LTV IS CONFIG ──────────────────────────────────────
     All three are PROVISIONAL and all three are env-settable, so real
     numbers from the agency are a Railway change rather than a deploy.
     The defaults are pinned because they move ad spend the day anyone
     switches a campaign to value optimisation. */
  eq('product: the three provisional defaults', META.PREDICTED_LTV,
     { aeo: 12000, crm: 5000, 'aeo,crm': 15000 });
  /* COMBINED IS 15000, NOT THE 17000 SUM. predicted_ltv predicts what
     the PERSON is worth; the sum asserts they buy both at full price
     with certainty. */
  ok('product: combined is not the naive sum of the parts',
     META.PREDICTED_LTV['aeo,crm'] !== META.PREDICTED_LTV.aeo + META.PREDICTED_LTV.crm,
     String(META.PREDICTED_LTV['aeo,crm']));
  ok('product: combined is above the higher single product',
     META.PREDICTED_LTV['aeo,crm'] > Math.max(META.PREDICTED_LTV.aeo, META.PREDICTED_LTV.crm));
  /* Every catalogue key must have a number, or an event fires without
     the field and the cohort becomes unreconstructable. */
  for (const k of Object.keys(META.PRODUCTS)) {
    ok(`product: ${k} has a predicted_ltv`, Number.isFinite(META.predictedLtvFor(k)), k);
  }
  eq('product: an unknown slug has no ltv rather than a wrong one',
     META.predictedLtvFor('enterprise'), null);

  /* AEO is the DEFAULT and the exceptions are listed. An AEO allowlist
     would rot: the form is already on a dozen pages and new SEO landers
     get added routinely by people who will never open this file. Measured
     on 90 days of real leads, a /demo-only list left 631 leads (18%, 408
     completed) sending unlabelled events. A default only fails when a new
     product launches, which is rare and deliberate — and logged. */
  /* THE EXCEPTIONS ARE PINNED BY VALUE, not merely counted, so adding a
     page here is a decision somebody made rather than a diff nobody
     read. Both entries are the CRM product: /ai-crm is the rebuilt page
     that will replace /ai-demo's content, and until that swap happens
     both are live and both must resolve the same way. */
  eq('product: the mapped exceptions are exactly the two CRM pages',
     META.PRODUCT_PATHS, { '/ai-demo': 'crm', '/ai-crm': 'crm' });
  ok('product: the default is aeo', META.DEFAULT_PRODUCT === 'aeo');
  ok('product: every mapped path names a real product',
     Object.values(META.PRODUCT_PATHS).every((s) => s in META.PRODUCTS));
  ok('product: the default names a real product', META.DEFAULT_PRODUCT in META.PRODUCTS);
  /* Excluded by EVENT NAME, not by hoping a page resolves to null. With a
     default in place the lead-magnet LP resolves to aeo, so the only thing
     keeping a PDF download off a 12000 LTV is this list. */
  eq('product: Contact is excluded by event name',
     META.PRODUCT_EXCLUDED_EVENTS, ['Contact']);

  // ── resolveProduct
  const RESOLVE = [
    ['https://gushwork.ai/ai-demo',                    'crm'],
    ['https://gushwork.ai/ai-demo/',                   'crm'],
    ['https://gushwork.ai/ai-demo?utm_campaign=crm',   'crm'],
    ['https://gushwork.ai/AI-DEMO',                    'crm'],
    ['https://gushwork.webflow.io/ai-demo',            'crm'],
    ['/ai-demo',                                       'crm'],
    // Everything else that IS a page takes the default.
    ['https://gushwork.ai/demo',                       'aeo'],
    ['https://gushwork.ai/demo/',                      'aeo'],
    ['https://gushwork.ai/demo?utm_source=fb&fbclid=x','aeo'],
    ['https://gushwork.ai/DEMO',                       'aeo'],
    ['/demo',                                          'aeo'],
    /* The pages a /demo-only allowlist was silently missing — 631 leads
       in 90 days. These are the whole reason aeo is the default. */
    ['https://gushwork.ai/start',                      'aeo'],
    ['https://gushwork.ai/start-now',                  'aeo'],
    ['https://gushwork.ai/pricing',                    'aeo'],
    ['https://gushwork.ai/consulting-lead-generation',  'aeo'],
    ['https://gushwork.ai/manufacturing-seo-services',  'aeo'],
    ['https://gushwork.ai/',                           'aeo'],
    /* Near-misses on the CRM path must NOT become crm. */
    ['https://gushwork.ai/ai-demo-v2',                 'aeo'],
    ['https://gushwork.ai/x/ai-demo',                  'aeo'],
    /* No readable page at all. NOT the default — "we could not tell which
       page this was" is not "this was the default page". */
    ['',                                               null],
    [null,                                             null],
    [undefined,                                        null],
    ['not a url',                                      null],
    ['http://',                                        null],
    ['ht!tp://%%%',                                    null],
  ];
  for (const [url, expected] of RESOLVE) {
    const r = { threw: false, slug: rp(url) };
    ok('product: ' + JSON.stringify(url) + ' resolves to ' + JSON.stringify(expected),
       !r.threw && r.slug === expected,
       r.threw ? 'THREW: ' + r.error : JSON.stringify(r.slug));
  }

  /* /ai-demo contains the string "demo". A prefix or substring match would
     tag every CRM lead as AEO and nothing downstream would notice. */
  ok('product: /ai-demo is never mistaken for /demo', rp('https://gushwork.ai/ai-demo') === 'crm');
  ok('product: /demo does not leak into crm via a substring match', rp('https://gushwork.ai/demo') === 'aeo');
  ok('product: resolveProduct() with no argument returns null rather than throwing',
     (() => { try { return META.resolveProduct() === null; } catch { return false; } })());
  ok('product: resolveProduct never reads landing_page', rp('') === null);

  // ── The five fields on a recognised page
  const aeo = cd('Lead', { page_url: 'https://gushwork.ai/demo', email: 'a@b.com' });
  eq('product: aeo content_ids',  aeo.content_ids, ['aeo']);
  ok('product: content_type is product', aeo.content_type === 'product');
  ok('product: value is the number 0',   aeo.value === 0 && typeof aeo.value === 'number');
  ok('product: currency is USD',         aeo.currency === 'USD');
  ok('product: aeo predicted_ltv is 12000', aeo.predicted_ltv === 12000);
  const crm = cd('Lead', { page_url: 'https://gushwork.ai/ai-demo', email: 'a@b.com' });
  eq('product: crm content_ids', crm.content_ids, ['crm']);
  ok('product: crm predicted_ltv is 5000', crm.predicted_ltv === 5000);

  /* Per spec: predicted_ltv is the same per product on EVERY event, and only
     value varies — which is 0 on all three upstream events. */
  for (const [slug, url, ltv] of [['aeo', 'https://gushwork.ai/demo', 12000],
                                  ['crm', 'https://gushwork.ai/ai-demo', 5000]]) {
    const ltvs = [], vals = [];
    for (const ev of ['StartTrial', 'Lead', 'Schedule']) {
      const c = cd(ev, { page_url: url, email: 'a@b.com' });
      ltvs.push(c.predicted_ltv); vals.push(c.value);
    }
    ok('product: ' + slug + ' predicted_ltv is ' + ltv + ' on all three upstream events',
       ltvs.every((v) => v === ltv), JSON.stringify(ltvs));
    ok('product: ' + slug + ' value is 0 on all three upstream events',
       vals.every((v) => v === 0), JSON.stringify(vals));
  }

  /* A page we can READ but have not mapped takes the default and is tagged.
     This is the case a /demo-only allowlist was silently dropping. */
  for (const url of ['https://gushwork.ai/start', 'https://gushwork.ai/pricing',
                     'https://gushwork.ai/some-lander-nobody-told-us-about']) {
    const r = safeCd('Lead', { page_url: url, email: 'a@b.com', company: 'Acme' });
    ok('product: ' + JSON.stringify(url) + ' builds an event rather than throwing', !r.threw, r.error);
    ok('product: ' + JSON.stringify(url) + ' takes the aeo default',
       JSON.stringify(r.custom_data.content_ids) === '["aeo"]' &&
       r.custom_data.predicted_ltv === 12000,
       JSON.stringify(r.custom_data));
  }

  /* A value we cannot read as a page is NOT the default. "We could not tell
     which page this was" is not "this was the default page" — the same rule
     the lead-path checkers follow, pointed the other way.

     The garbage strings matter: new URL(x, base) succeeds for almost
     anything, so without the leading-slash guard "not a url" resolves to
     /not%20a%20url and would be reported to Meta as a real aeo lead. */
  for (const url of ['', null, undefined, 'not a url', 'ht!tp://%%%',
                     'http://', 12345, {}, [], true]) {
    const r = safeCd('Lead', { page_url: url, email: 'a@b.com', company: 'Acme' });
    ok('product: ' + JSON.stringify(url) + ' builds an event rather than throwing',
       !r.threw, r.error);
    const present = FIVE.filter((k) => k in r.custom_data);
    ok('product: ' + JSON.stringify(url) + ' is untagged, not defaulted',
       present.length === 0, 'present: ' + present.join(','));
  }
  eq('product: an unreadable page still sends exactly the event it always sent',
     cd('Lead', { page_url: '', company: 'Acme', sell_to: 'B2B' }),
     { company_name: 'Acme', sell_to: 'B2B' });
  for (const ev of ['StartTrial', 'Lead', 'Schedule']) {
    const r = safeCd(ev, { page_url: '', email: 'a@b.com' });
    ok('product: ' + ev + ' with no readable page is untagged, not an error',
       !r.threw && FIVE.every((k) => !(k in r.custom_data)), r.error);
    const d = safeCd(ev, { page_url: 'https://gushwork.ai/whatever', email: 'a@b.com' });
    ok('product: ' + ev + ' on an unmapped page is tagged aeo',
       !d.threw && JSON.stringify(d.custom_data.content_ids) === '["aeo"]', d.error);
  }

  /* Contact, the lead magnet. Golden captured from the previous module. */
  const LM = { session_id: 's', email: 'a@b.com', website: 'w.com', sell_to: 'B2B',
    page_url: 'https://gushwork.ai/150-buyer-questions',
    landing_page: 'https://gushwork.ai/150-buyer-questions',
    fbc: 'fb.1.2.3', fbp: 'fb.1.2.4', industry_category: 'SaaS',
    product_or_service: 'CRM software', is_free_email: false };
  ok('product: Contact custom_data is byte-identical to before product tagging',
     JSON.stringify(cd('Contact', LM)) ===
     '{"sell_to":"B2B","industry_category":"SaaS","product_or_service":"CRM software","is_free_email":"false"}',
     JSON.stringify(cd('Contact', LM)));
  ok('product: Contact carries none of the five', FIVE.every((k) => !(k in cd('Contact', LM))));
  /* Contact is excluded by EVENT NAME, and that is the only thing keeping a
     PDF download off a 12000 LTV now that aeo is the default: the lead-magnet
     LP resolves to aeo like any other unmapped page. Asserted from the page
     that WOULD tag, so a regression to page-based exclusion fails here. */
  ok('product: resolveProduct alone would tag the lead-magnet LP',
     rp(LM.page_url) === 'aeo');
  ok('product: Contact stays untagged even from a mapped product page',
     FIVE.every((k) => !(k in cd('Contact', { ...LM, page_url: 'https://gushwork.ai/ai-demo' }))));
  ok('product: the same page DOES tag a non-excluded event',
     cd('Lead', { ...LM, page_url: 'https://gushwork.ai/ai-demo' }).content_type === 'product');

  /* The tag must come from page_url ALONE. event_source_url falls back to
     landing_page, and letting the product tag do the same would mis-tag every
     ad lead: real traffic lands on /start and submits on /demo, and a lander
     that happened to sit at a product path would tag a lead that never saw
     that form. Measured — without this, adding the fallback survived. */
  ok('product: an empty page_url is NOT rescued by a recognised landing_page',
     FIVE.every((k) => !(k in cd('Lead',
       { email: 'a@b.com', page_url: '', landing_page: 'https://gushwork.ai/demo' }))));
  ok('product: a crm landing_page cannot pull an aeo page onto crm',
     JSON.stringify(cd('Lead',
       { email: 'a@b.com', page_url: 'https://gushwork.ai/start',
         landing_page: 'https://gushwork.ai/ai-demo' }).content_ids) === '["aeo"]');
  ok('product: landing_page cannot change a tag page_url already decided',
     JSON.stringify(cd('Lead', { page_url: 'https://gushwork.ai/demo', landing_page: 'https://gushwork.ai/ai-demo' }).content_ids) === '["aeo"]');

  /* A path that fell through to the default is LOGGED, once, so a new
     product page that should have been mapped surfaces instead of quietly
     becoming aeo. This is the safety net that makes a default acceptable:
     the failure mode of a default is silent mis-attribution, and a log line
     is what turns it back into something someone can notice.

     Executed against a captured console.log, because "there is a log call
     in the source" and "a log line comes out" are different claims. */
  {
    const realLog = console.log;
    const lines = [];
    console.log = (...a) => lines.push(a.join(' '));
    let unmapped, mappedLines, repeatLines;
    try {
      const uniq = '/brand-new-lander-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8);
      rp('https://gushwork.ai' + uniq);
      unmapped = lines.filter((l) => l.includes(uniq));
      lines.length = 0;
      rp('https://gushwork.ai' + uniq + '?utm_source=fb');   // same path, second time
      rp('https://gushwork.ai' + uniq + '/');                // and with a trailing slash
      repeatLines = lines.filter((l) => l.includes(uniq));
      lines.length = 0;
      rp('https://gushwork.ai/ai-demo');
      mappedLines = lines.slice();
    } finally {
      console.log = realLog;
    }
    ok('product: an unmapped path logs exactly once', unmapped.length === 1,
       unmapped.length + ' line(s)');
    ok('product: the log names the path and the default it took',
       unmapped.length === 1 && /aeo/.test(unmapped[0]) && /PRODUCT_PATHS/.test(unmapped[0]),
       unmapped[0]);
    ok('product: the same path does not log again', repeatLines.length === 0,
       repeatLines.join(' | '));
    ok('product: a mapped path logs nothing', mappedLines.length === 0,
       mappedLines.join(' | '));
    ok('product: the logger is bounded so junk paths cannot grow it forever',
       /_defaultedPaths\.size >= \d+/.test(
         fs.readFileSync(path.join(__dirname, '..', 'meta-capi.js'), 'utf8')));
  }

  /* Nothing outside custom_data moved, and the catalogue cannot be mutated
     by a caller holding a built event. */
  const full = build('Lead',
    { page_url: 'https://gushwork.ai/demo', email: 'a@b.com', phone: '+91 63886 39290',
      first_name: 'A', last_name: 'B', enriched_city: 'Pune', fbc: 'f', fbp: 'g' },
    { clientIpAddress: '1.2.3.4', clientUserAgent: 'UA' });
  eq('product: user_data is untouched by tagging', Object.keys(full.user_data).sort(),
     ['client_ip_address','client_user_agent','ct','em','fbc','fbp','fn','ln','ph']);
  ok('product: event_source_url is still page_url', full.event_source_url === 'https://gushwork.ai/demo');
  ok('product: action_source is still website', full.action_source === 'website');
  /* Defensive: this must FAIL, never throw. A test that crashes prints no
     totals, and an UNMEASURED run reads like neither a pass nor a catch. */
  ok('product: the catalogue cannot be mutated through a built event',
     (() => { try {
                const ids = cd('Lead', { page_url: 'https://gushwork.ai/demo' }).content_ids;
                if (!Array.isArray(ids)) return false;
                ids.push('x');
                return JSON.stringify(META.PRODUCTS.aeo.content_ids) === '["aeo"]';
              } catch { return false; } })());

  /* sendEvent must keep going through the builder. Inlining the payload back
     into sendEvent would leave every assertion above passing against a
     function production no longer calls. */
  const mcsrc = fs.readFileSync(path.join(__dirname, '..', 'meta-capi.js'), 'utf8');
  ok('product: sendEvent builds its payload through buildEventData',
     /const eventData = buildEventData\(eventName, payload, options\);/.test(mcsrc));
  ok('product: sendEvent does not build an event payload of its own',
     (mcsrc.match(/event_name: eventName/g) || []).length === 1);

  ok('product: nothing in this section made buildEventData or resolveProduct throw',
     threwCount === 0, threwCount + ' throw(s), first: ' + threwFirst);
}

/* ============================================================
   11. The product column — one slug, both write paths

   leads.product on Railway and gw_form_leads.product on the AWS mirror.
   The nightly jobs in the other repo read the mirror, so a product they
   cannot see is a product they cannot report on.

   The stored value is the slug resolveProduct returns — the SAME function
   that decides the Meta content_ids — so the column and the event cannot
   disagree about which product a lead came in for. Never req.body, so no
   page can write an arbitrary value into the column.

   Most of this section is placeholder arithmetic, which sounds dull and
   is the thing that actually breaks: an off-by-one in a 39-parameter
   INSERT does not fail loudly, it binds the wrong value into the wrong
   column on a live lead.
   ============================================================ */
{
  /* Balanced-paren slice, because VALUES contains NOW() and a naive
     [^)]* stops inside it — which silently truncates the list and makes
     every count below agree with itself while being wrong. */
  const parens = (s, from) => {
    const start = s.indexOf('(', from);
    let d = 0;
    for (let i = start; i < s.length; i++) {
      if (s[i] === '(') d++;
      else if (s[i] === ')') { d--; if (!d) return s.slice(start + 1, i); }
    }
    throw new Error('unbalanced parens');
  };
  const topSplit = (s) => {
    let d = 0, cur = '', out = [];
    for (const ch of s) {
      if ('([{'.includes(ch)) d++;
      if (')]}'.includes(ch)) d--;
      if (ch === ',' && d === 0) { out.push(cur.trim()); cur = ''; } else cur += ch;
    }
    if (cur.trim()) out.push(cur.trim());
    return out.filter(Boolean);
  };
  const strip = (s) => s.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

  const checkInsert = (label, table, routeMarker, endMarker) => {
    const i = src.indexOf(routeMarker);
    ok(label + ': the route is still here', i >= 0);
    if (i < 0) return;
    const insAt = src.indexOf('INSERT INTO ' + table, i);
    const cols  = topSplit(strip(parens(src, insAt)));
    const vi    = src.indexOf('VALUES', insAt);
    const vals  = topSplit(parens(src, vi));
    const bs    = src.indexOf('`, [', vi);
    const binds = topSplit(strip(src.slice(bs + 4, src.indexOf(endMarker, bs))
                                   .replace(/\]\);?\s*$/, '')));
    const nums  = vals.filter((v) => /^\$\d+$/.test(v)).map((v) => +v.slice(1));
    const maxN  = Math.max(...nums);

    ok(label + ': columns == VALUES items', cols.length === vals.length,
       cols.length + ' cols vs ' + vals.length + ' values');
    ok(label + ': $n run 1..' + maxN + ' with no gaps or repeats',
       new Set(nums).size === nums.length && maxN === nums.length &&
       nums.slice().sort((a, b) => a - b).every((v, k) => v === k + 1));
    ok(label + ': placeholders == binds', maxN === binds.length,
       maxN + ' placeholders vs ' + binds.length + ' binds');
    ok(label + ': product is in the column list', cols.includes('product'));
    /* Each column maps to its OWN placeholder, and that placeholder's bind
       is the variable of the same name. Checked by NAME rather than by
       position: an assertion that "product is the last bind" silently
       becomes wrong the moment another column is appended, which is exactly
       what happened when about_business landed. */
    const bindFor = (name) => {
      const v = vals[cols.indexOf(name)];
      const n = /^\$(\d+)$/.exec(v || '');
      return n ? binds[+n[1] - 1] : null;
    };
    for (const name of ['product', 'about_business']) {
      ok(label + ': ' + name + ' maps to a placeholder', /^\$\d+$/.test(vals[cols.indexOf(name)] || ''),
         'got ' + vals[cols.indexOf(name)]);
      ok(label + ': ' + name + ' is bound to the variable of the same name',
         bindFor(name) === name, name + ' -> ' + bindFor(name));
    }
    /* Anything in VALUES that is not a $n must be a literal. A stray bare
       identifier there means the placeholder run has been shifted. */
    ok(label + ': every non-placeholder VALUES item is a literal',
       vals.filter((v) => !/^\$\d+$/.test(v))
           .every((v) => /^(false|true|\d+|NOW\(\)|'[^']*')$/.test(v)),
       vals.filter((v) => !/^\$\d+$/.test(v)).join(' | '));
    return { cols, vals, binds, maxN };
  };

  const p = checkInsert('product /partial', 'leads', "app.post('/partial'", '\n\n');

  const s2 = checkInsert('product /submit', 'leads', "app.post('/submit'", '\n\n');


  // ── syncToAWS, the mirror the other repo reads
  {
    const insAt = src.indexOf('INSERT INTO gw_form_leads');
    const cols  = topSplit(strip(parens(src, insAt)));
    const vi    = src.indexOf('VALUES', insAt);
    const vals  = topSplit(parens(src, vi));
    const bs    = src.indexOf('`, [', vi);
    const binds = topSplit(strip(src.slice(bs + 4, src.indexOf('  ]).then', bs))));
    const nums  = vals.filter((v) => /^\$\d+$/.test(v)).map((v) => +v.slice(1));
    const maxN  = Math.max(...nums);

    ok('product syncToAWS: columns == VALUES items', cols.length === vals.length,
       cols.length + ' vs ' + vals.length);
    ok('product syncToAWS: $n run 1..' + maxN + ' with no gaps',
       new Set(nums).size === nums.length && maxN === nums.length);
    ok('product syncToAWS: placeholders == binds', maxN === binds.length,
       maxN + ' vs ' + binds.length);
    ok('product syncToAWS: updated_at is still last and bound to NOW()',
       cols[cols.length - 1] === 'updated_at' && vals[vals.length - 1] === 'NOW()');
    /* By name, not by position — see the note in checkInsert. */
    const awsBindFor = (name) => {
      const v = vals[cols.indexOf(name)];
      const n = /^\$(\d+)$/.exec(v || '');
      return n ? binds[+n[1] - 1] : null;
    };
    for (const name of ['product', 'about_business']) {
      ok('product syncToAWS: ' + name + ' maps to a placeholder',
         /^\$\d+$/.test(vals[cols.indexOf(name)] || ''), 'got ' + vals[cols.indexOf(name)]);
      ok('product syncToAWS: ' + name + ' is bound to data.' + name,
         new RegExp('^data\\.' + name + '\\s*\\|\\|\\s*null$').test(awsBindFor(name) || ''),
         name + ' -> ' + awsBindFor(name));
    }
    ok('product syncToAWS: the conflict clause COALESCEs product',
       /product\s*=\s*COALESCE\(EXCLUDED\.product,\s*gw_form_leads\.product\)/
         .test(src.slice(insAt, src.indexOf('  ]).then', insAt))));
  }

  /* COALESCE, never a bare overwrite. /partial fires repeatedly through
     step 1; a later call must not blank a slug an earlier one resolved.
     Exactly the reason the ps_ columns are COALESCEd. */
  ok('product: both Railway conflict clauses COALESCE it',
     (src.match(/product\s+=\s+COALESCE\(EXCLUDED\.product,\s+leads\.product\)/g) || []).length === 2);
  ok('product: it is never overwritten unconditionally',
     !/product\s*=\s*EXCLUDED\.product\s*[,\n]/.test(src));

  // ── migrations, both instances
  ok('product: db.js adds leads.product',
     /ALTER TABLE leads ADD COLUMN IF NOT EXISTS product TEXT/.test(dbsrc));
  ok('product: initAWSTable adds gw_form_leads.product',
     /ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS product TEXT/.test(src));

  /* One resolver. A second copy in index.js is how the column and the Meta
     event end up disagreeing about the same lead. */
  /* By SYMBOL, not by the exact import line: pinning the whole line means
     the assertion fails the next time anything else is imported from the
     same module, which says nothing about resolveProduct. */
  {
    const imp = /const \{([^}]*)\} = require\('\.\/meta-capi'\);/.exec(src);
    const names = imp ? imp[1].split(',').map((x) => x.trim()) : [];
    ok('product: index.js imports resolveProduct from meta-capi',
       names.includes('resolveProduct'), names.join(','));
    ok('product: it imports the push functions from the same module',
       names.includes('pushFormEventsToMeta') && names.includes('pushStartTrialToMeta'), names.join(','));
  }
  /* index.js may READ the ltv for the column it persists, but must not
     declare a catalogue or a number of its own -- the bare identifier,
     not the meta_predicted_ltv column name that legitimately appears. */
  ok('product: index.js defines no catalogue of its own',
     !/const PRODUCTS\s*=/.test(src)
     && !/const PREDICTED_LTV\s*=/.test(src)
     && !/[^_]predicted_ltv\s*[:=]\s*\d/.test(src));
  /* BOTH ROUTES RESOLVE FROM THE SAME TWO INPUTS, and Meta is handed the
     second one so it resolves identically. Before 15 Sept 2026 product was
     a pure function of the page and this read `{ page_url }`; the moment a
     checkbox can decide it, a route that forgot the selection would store
     one slug and fire another with nothing anywhere to reconcile them. */
  ok('product: both routes resolve from page_url, the selection AND the campaign',
     (src.match(/resolveProduct\(\{ page_url, product_interest, utm_campaign: offer_campaign, utm_medium: offer_medium \|\| utm_medium \}\)/g) || []).length === 2);
  /* THE OFFER'S CAMPAIGN IS NOT THE ATTRIBUTION CAMPAIGN, and both
     routes must read the offer one. Folding the 30-day cookie into
     utm_campaign would re-attribute real leads from organic to paid and
     leave them carrying a paid campaign beside an empty utm_source,
     which is the field Source_Bucket__c actually reads. */
  ok('product: offer_campaign falls back to this visit, never the other way round',
     /const offer_campaign\s+= \(req\.body\.offer_campaign\s+\|\| utm_campaign \|\| ''\)/.test(src));

  /* ── THE COLUMNS, ADDED 19 SEPT 2026 ──────────────────────────────
     offer_campaign was read, used to resolve the product and tag Meta,
     and then thrown away -- so "did the 30-day cookie rescue this lead"
     was arguable but not queryable. It matters because the population it
     serves is not small: 424 leads in 90 days arrived with the campaign
     plainly visible in previous_page and nothing in utm_campaign, and
     the loss is 31.2% inside Facebook and Instagram in-app browsers
     against 0% on desktop.

     BOTH FORM UPSERTS, and COALESCEd like every other attribution
     column -- /partial fires repeatedly through step 1 and must never
     blank what an earlier call captured. */
  ok('offer: both form upserts store the columns',
     (src.match(/ps_partner_name,ps_partner_email,offer_campaign,offer_medium\)/g) || []).length === 2);
  ok('offer: both bind the values',
     (src.match(/psIdentity\?\.name\|\|null,psIdentity\?\.email\|\|null,offer_campaign\|\|null,offer_medium\|\|null\]/g) || []).length === 2);
  ok('offer: both COALESCE on conflict, so a later /partial cannot blank them',
     (src.match(/offer_campaign\s+= COALESCE\(EXCLUDED\.offer_campaign/g) || []).length === 2
     && (src.match(/offer_medium\s+= COALESCE\(EXCLUDED\.offer_medium/g) || []).length === 2);
  ok('offer: the schema declares both columns',
     /ADD COLUMN IF NOT EXISTS offer_campaign TEXT/.test(dbsrc)
     && /ADD COLUMN IF NOT EXISTS offer_medium\s+TEXT/.test(dbsrc));
  /* NEVER BACKFILLED. A null here means "this lead predates the column",
     and inferring one would put a measurement where an absence belongs --
     the same rule non_icp_checked_at follows. */
  ok('offer: neither column is backfilled or defaulted',
     !/ADD COLUMN IF NOT EXISTS offer_(campaign|medium)[^,;`]*DEFAULT/.test(dbsrc));
  ok('product: utm_campaign itself is never read from the offer field',
     !/const utm_campaign[^;]*offer_campaign/.test(src));
  ok('product: the selection reaches Meta, so the event cannot diverge',
     /pushStartTrialToMeta\(\{[^}]*product_interest/.test(src)
     && /pushFormEventsToMeta\(\{[^}]*product_interest/.test(src));
  /* THE CAMPAIGN HAS TO REACH META FOR THE SAME REASON THE SELECTION DID.
     A CRM-campaign lead on /demo is stored crm by resolveProduct above; an
     event payload without the campaign resolves the same lead to aeo from
     the page and reports a different product than the column holds. */
  ok('product: the campaign reaches Meta on both form paths',
     /pushStartTrialToMeta\(\{[^}]*utm_campaign:offer_campaign/.test(src)
     && /pushFormEventsToMeta\(\{[^}]*utm_campaign:offer_campaign/.test(src));
  /* AND ON THE THREE BOOKING ROUTES, which share one statement. Schedule
     resolved its product from the page alone until 17 Sept 2026 because
     product_interest was never selected here -- so a lead who ticked
     AI-CRM fired Lead as crm and Schedule as aeo. */
  ok('product: the Schedule statement selects the selection and the campaign',
     /l\.product_interest, l\.utm_campaign, l\.utm_medium/.test(src));
  /* THE COLUMN HOLDS A SLUG THIS CODE RESOLVED, never a string a page
     sent. product is never read from the body at all -- a hidden field
     would otherwise let any page write anything into it.

     product_interest IS read from the body, because a checkbox is the
     only way to know what somebody ticked. What makes that safe is that
     it goes through canonicalProductInterest, which drops anything that
     is not a slug we sell. That matters beyond tidiness: the value
     reaches a RESTRICTED Salesforce picklist, and an unknown string
     there is INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST, which
     sfUnknownFields does not retry -- the whole Lead is lost, not the
     one field. */
  ok('product: nothing reads a page-supplied product slug',
     !/req\.body\.product\b/.test(src));
  ok('product: the selection is read from the body ONLY through the canonicaliser',
     (src.match(/req\.body\.product_interest/g) || []).length === 2
     && (src.match(/canonicalProductInterest\(req\.body\.product_interest\)/g) || []).length === 2);
}

/* ============================================================
   13. No JS comment syntax inside a SQL string

   `// COALESCE for the same reason as /partial.` shipped inside the
   /submit INSERT and took EVERY form completion down with it: Postgres
   has no // comment, so the whole statement failed with a 42601 syntax
   error, /submit returned 500, and nothing after the INSERT ran — no
   Slack, no Salesforce, no Meta Lead, no PartnerStack conversion.

   It survived review, six green suites and a merge because every
   assertion in this repo reads the SQL as TEXT. Source-level assertions
   cannot tell you whether a query parses. This one is still textual —
   it is a lint, not an execution — but it targets the exact shape that
   got through, and it is cheap enough to run on every statement.

   CLAUDE.md already warns that a BACKTICK inside a SQL comment breaks
   the file. This is its sibling: valid JavaScript, invalid SQL, and
   silent until a query runs.
   ============================================================ */
{
  /* Every backtick template literal that looks like SQL, from the files
     that talk to Postgres. */
  const SQL_FILES = ['index.js', 'db.js', 'lead-magnet.js', 'backfill-sf.js'];
  const offenders = [];
  for (const file of SQL_FILES) {
    const text = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
    const lits = text.match(/`(?:[^`\\]|\\.)*`/gs) || [];
    for (const lit of lits) {
      if (!/\b(SELECT|INSERT INTO|UPDATE|DELETE FROM|ALTER TABLE|CREATE TABLE)\b/i.test(lit)) continue;
      /* A backtick inside a JS comment can make the scanner pair the wrong
         delimiters, so skip anything that is obviously not a statement. */
      if (!/\$\d|FROM|INTO|TABLE/i.test(lit)) continue;
      for (const line of lit.split('\n')) {
        if (/^\s*\/\//.test(line)) offenders.push(file + ': ' + line.trim().slice(0, 70));
      }
    }
  }
  /* One known false positive: alertIfBookingWithoutSubmit has a JS comment
     containing a backtick, which makes the literal scanner pair across it.
     Its SQL is single-quoted, so it cannot carry a comment at all. */
  const real = offenders.filter((o) => !/completed. is ALSO set by/.test(o));
  ok('sqlcomment: no // comment inside any SQL template literal',
     real.length === 0, real.join(' | '));

  /* Pin the two statements that actually broke, by execution shape: a SQL
     comment in this repo is /* *\/ and nothing else. */
  const submitIns = src.slice(src.indexOf('INSERT INTO leads', src.indexOf("app.post('/submit'")));
  const submitSql = submitIns.slice(0, submitIns.indexOf('`'));
  ok('sqlcomment: the /submit INSERT carries no // line', !/^\s*\/\//m.test(submitSql));
  const partialIns = src.slice(src.indexOf('INSERT INTO leads', src.indexOf("app.post('/partial'")));
  const partialSql = partialIns.slice(0, partialIns.indexOf('`'));
  ok('sqlcomment: the /partial INSERT carries no // line', !/^\s*\/\//m.test(partialSql));
  const awsIns = src.slice(src.indexOf('INSERT INTO gw_form_leads'));
  const awsSql = awsIns.slice(0, awsIns.indexOf('`'));
  ok('sqlcomment: the syncToAWS INSERT carries no // line', !/^\s*\/\//m.test(awsSql));
}

/* ============================================================
   14. Meta auth failures — narrowed to the codes that mean it

   isAuthFailure bypasses BOTH thresholds and pages critical, Slack and
   email, on the first occurrence. AUTH_FAILURE_PATTERNS contains
   /OAuth/i, /401/, /403/ and /access.?token/i — and Meta stamps
   type:"OAuthException" on nearly every Graph API error, including a bad
   parameter (100), a rate limit (80004) and a transient server error (2).
   Measured on realistic bodies, 8 of 11 would have paged and only 4 were
   credential problems.

   Meta now matches only the four codes that mean the token is dead:
   190, 102, 463, 467. Every other source is untouched — the three
   non-Meta callers pass no source at all and keep the full list.

   The REAL function is lifted and executed here, against the message
   format throwIfAnyFailed actually builds, because the question is what
   a live Meta error body does to it — not whether a regex exists.
   ============================================================ */
{
  const grab = (from, to) => src.slice(src.indexOf(from), src.indexOf(to, src.indexOf(from)) + to.length);
  const patterns = grab('const AUTH_FAILURE_PATTERNS = [', '];');
  const codes    = grab('const META_AUTH_CODES = [', '];');
  const metaRe   = grab('const META_AUTH_RE = ', ');');
  const fnSrc    = grab('function isAuthFailure(error, source) {', '\n}');
  const isAuthFailure = new Function(patterns + '\n' + codes + '\n' + metaRe + '\n' + fnSrc +
                                     '\nreturn isAuthFailure;')();
  const genericOnly = new Function(patterns +
    '\nreturn (m) => AUTH_FAILURE_PATTERNS.some((r) => r.test(m));')();
  const msg = (ev, e) => ev + ': ' + (typeof e === 'string' ? e : JSON.stringify(e));

  eq('metaauth: the four auth codes are pinned', JSON.parse(codes.replace(/[^[]*/, '').replace(/;$/, '')),
     [190, 102, 463, 467]);

  /* Real Meta bodies that are NOT credential problems. Each one of these
     paged critical before this change. */
  const NOT_AUTH = [
    ['bad parameter (100)',        { message: 'Invalid parameter', type: 'OAuthException', code: 100 }],
    ['unknown custom_data field',  { message: '(#100) param custom_data[predicted_ltv] must be a number', type: 'OAuthException', code: 100 }],
    ['rate limit (80004)',         { message: '(#80004) There have been too many calls', type: 'OAuthException', code: 80004 }],
    ['transient 500 (code 2)',     { message: 'An unexpected error has occurred', type: 'OAuthException', code: 2 }],
    ['network drop',               'ECONNREFUSED'],
    ['non-JSON gateway page',      'Unexpected token < in JSON at position 0'],
    ['pixel not configured',       'Missing credentials'],
  ];
  for (const [label, e] of NOT_AUTH)
    ok('metaauth: ' + label + ' goes through the threshold, not an instant page',
       isAuthFailure(msg('Lead', e), 'Meta CAPI') === false);

  const AUTH = [
    ['190 expired token',   { message: 'Error validating access token: Session has expired', type: 'OAuthException', code: 190 }],
    ['102 session invalid', { message: 'Session key invalid or no longer valid', type: 'OAuthException', code: 102 }],
    ['463 expired token',   { message: 'Error validating access token', type: 'OAuthException', code: 463 }],
    ['467 invalid token',   { message: 'Error validating access token', type: 'OAuthException', code: 467 }],
  ];
  for (const [label, e] of AUTH)
    ok('metaauth: ' + label + ' still pages critical immediately',
       isAuthFailure(msg('Lead', e), 'Meta CAPI') === true);

  /* Every other integration keeps the old behaviour. The same body that is
     ignored for Meta must still page for Salesforce. */
  ok('metaauth: Gmail 535 still pages with no source',
     isAuthFailure('535-5.7.8 Username and Password not accepted') === true);
  ok('metaauth: Salesforce INVALID_SESSION_ID still pages',
     isAuthFailure('INVALID_SESSION_ID', 'Salesforce') === true);
  ok('metaauth: an OAuthException from Salesforce still pages',
     isAuthFailure('{"type":"OAuthException","code":100}', 'Salesforce') === true);
  ok('metaauth: the SAME body from Meta does not',
     isAuthFailure('{"type":"OAuthException","code":100}', 'Meta CAPI') === false);
  ok('metaauth: an empty error is never an auth failure',
     isAuthFailure('', 'Meta CAPI') === false);
  ok('metaauth: recordFailure passes the source through',
     /if \(isAuthFailure\(errStr, source\)\) \{/.test(src));

  /* The point of the change, as a number. */
  const count = (f) => [...NOT_AUTH, ...AUTH].filter(([, e]) => f(msg('Lead', e))).length;
  const before = count((m) => genericOnly(m));
  const after  = count((m) => isAuthFailure(m, 'Meta CAPI'));
  ok('metaauth: the generic list over-paged on these bodies', before > after, before + ' vs ' + after);
  ok('metaauth: the new rule pages on exactly the four real auth codes', after === AUTH.length, String(after));

  /* recordSuccess('Meta CAPI') was never called anywhere, so the streak only
     reset when an alert fired. Wired through an injected reporter so
     meta-capi.js does not have to require index.js. */
  ok('metaauth: index.js wires a Meta outcome reporter',
     /setMetaOutcomeReporter\(\(outcome\) => \{/.test(src));
  ok('metaauth: it calls recordSuccess only on ok',
     /if \(outcome && outcome\.ok\) recordSuccess\('Meta CAPI'\);/.test(src));
  const mc = fs.readFileSync(path.join(__dirname, '..', 'meta-capi.js'), 'utf8');
  ok('metaauth: meta-capi reports an outcome only on a real success',
     /reportOutcome\(\{ ok: true, eventName \}\);/.test(mc) &&
     (mc.match(/reportOutcome\(/g) || []).length === 2);   // the definition + the one call
  ok('metaauth: a throwing reporter cannot break a send',
     /try \{ _outcomeReporter\(outcome\); \} catch/.test(mc));
  ok('metaauth: meta-capi does not require index.js', !/require\('\.\/index/.test(mc));
}

/* ============================================================
   15. Slack — product and the free-text answer

   BUILDS the blocks with the real slackSubmit rather than asserting the
   source mentions the fields. What matters is what arrives in Slack: an
   untagged lead must have NO Product line rather than an empty one, and
   the textarea must be truncated with a visible ellipsis so a cut
   sentence does not read as the whole answer.

   Slack rejects a whole section over 3000 characters, so an untruncated
   1000-char answer is close enough to the edge to bound deliberately.
   ============================================================ */
{
  const lift = (name) => {
    const m = new RegExp('(?:^|\\n)(?:async )?function ' + name + '\\s*\\(').exec(src);
    let d = 0, start = src.indexOf('{', m.index);
    for (let j = start; j < src.length; j++) {
      if (src[j] === '{') d++; else if (src[j] === '}') { d--; if (!d) return src.slice(m.index, j + 1); }
    }
  };
  const grab = (from, to) => src.slice(src.indexOf(from), src.indexOf(to, src.indexOf(from)) + to.length);

  /* bFields is instrumented to record every field set it is handed, so the
     assertions below read the real payload the real function assembled. */
  const build = new Function('CAP', `
    var isUnverifiablePair = function () { return false; };
    var UNVERIFIABLE_PAIR_NOTE = 'x';
    var WEBSITE_SALES_HINTS = {};
    var websiteReasonLabel = function (r) { return r || ''; };
    ${grab('function bHeader(text)', '\n')}
    ${grab('function bSection(text)', '\n')}
    ${lift('bFields').replace('function bFields(fields) {', 'function bFields(fields) { CAP.push(fields);')}
    ${grab('function bDivider()', '\n')}
    ${grab('function bContext(text)', '\n')}
    ${grab('const SLACK_ABOUT_MAX', ';')}
    ${lift('slackTruncate')}
    ${grab('const LEAD_CHANGE_ALERTABLE', ';')}
    ${grab('const LEAD_FIELD_LABELS = {', '};')}
    ${lift('alertableIdentityChanges')}
    ${lift('slackSubmit')}
    return slackSubmit;
  `);
  const fieldsFor = (payload) => {
    const CAP = [];
    const fn = build(CAP);
    /* slackSubmit goes on to call a sender this sandbox does not define; the
       blocks are already captured by then, so the throw is expected. */
    try { fn(payload); } catch (e) { /* expected */ }
    return CAP.flat().filter((f) => f && f.value);
  };
  const find = (f, label) => f.find((x) => x.label.includes(label));

  const crm = fieldsFor({ email: 'a@b.com', product: 'crm', sell_to: 'B2B' });
  ok('slack: a crm lead shows Product = crm', (find(crm, 'Product') || {}).value === 'crm');
  const aeo = fieldsFor({ email: 'a@b.com', product: 'aeo' });
  ok('slack: an aeo lead shows Product = aeo', (find(aeo, 'Product') || {}).value === 'aeo');
  /* An empty field would read as "we know the product and it is blank". */
  for (const v of [null, undefined, ''])
    ok('slack: product ' + JSON.stringify(v) + ' produces NO Product line',
       !find(fieldsFor({ email: 'a@b.com', product: v }), 'Product'));

  const long = 'We are a 40-person logistics firm. '.repeat(60);
  const ab = find(fieldsFor({ email: 'a@b.com', about_business: long }), 'About their business');
  ok('slack: a long answer is present', !!ab);
  ok('slack: truncated to the cap', ab && ab.value.length <= 280, ab && String(ab.value.length));
  ok('slack: ends with an ellipsis, so a cut sentence is visible as cut',
     ab && ab.value.endsWith('…'));
  ok('slack: keeps the opening words', ab && ab.value.startsWith('We are a 40-person'));

  const shortAb = find(fieldsFor({ email: 'a@b.com', about_business: 'We sell pallets.' }), 'About their business');
  ok('slack: a short answer is passed through untouched',
     shortAb && shortAb.value === 'We sell pallets.', shortAb && shortAb.value);
  for (const v of [undefined, null, '', '   '])
    ok('slack: about_business ' + JSON.stringify(v) + ' adds no block',
       !find(fieldsFor({ email: 'a@b.com', about_business: v }), 'About their business'));

  /* Worst case still has to fit Slack's per-section limit. */
  const worst = fieldsFor({ email: 'a@b.com', about_business: 'x'.repeat(5000), company: 'y'.repeat(300) });
  const total = worst.reduce((n, x) => n + x.label.length + String(x.value).length + 3, 0);
  ok('slack: even a pathological payload stays under the 3000-char section limit',
     total < 3000, String(total));

  /* And /submit must actually pass them, or none of the above ever runs.

     Checked by SLICING the call rather than by pinning its opening keys.
     The old form matched the literal prefix
     `slackSubmit({first_name,last_name,...`, so prepending any new key
     broke it -- which is exactly what happened when identity_changes and
     changed_after_booking were added, and the failure had nothing to do
     with product or about_business. An assertion should fail for the
     thing it names. */
  {
    const seg  = src.slice(src.indexOf("app.post('/submit'"));
    const from = seg.indexOf('slackSubmit({');
    const call = from === -1 ? '' : seg.slice(from, seg.indexOf('});', from));
    ok('slack: /submit calls slackSubmit at all', call.length > 0);
    ok('slack: /submit passes product to slackSubmit', /\bproduct\b/.test(call));
    ok('slack: /submit passes about_business to slackSubmit', /\babout_business\b/.test(call));
  }
}

/* ============================================================
   16. Salesforce — product, the free-text answer, and NOT losing the lead

   Salesforce rejects the ENTIRE record when it does not recognise one
   field. Not the field, the record. So a Product__c that is missing,
   renamed, or invisible to the integration user does not cost a column,
   it costs the whole lead.

   Both fields exist today: created 8 Sept 2026 via the Tooling API, with
   FieldPermissions granted and a real write/read round-trip — creating a
   field does not grant access to it, which is the 4 Sept lesson. This
   guard is for the day that stops being true.

   Driven against a stubbed fetch, because "does the lead survive" is a
   question about behaviour. test-sf-readers.js is the other suite that
   executes salesforce.js; this lives here with the rest of the product
   work.
   ============================================================ */
{
  const sfsrc = fs.readFileSync(path.join(__dirname, '..', 'salesforce.js'), 'utf8');
  ok('sf: product maps to Product__c',            /product: 'Product__c',/.test(sfsrc));
  ok('sf: about_business maps to About_Business__c', /about_business: 'About_Business__c',/.test(sfsrc));
  /* SALESFORCE GETS THE RICHER VALUE. Product__c carries three values --
     aeo, crm and aeo,crm, added and verified against the live org on
     15 Sept 2026 -- so a both-ticked lead shows what they asked for, not
     just the calendar they were routed to. Falls back to product where
     nothing was ticked, which is every page but /demo and every lead
     before the question existed. */
  ok('sf: /submit passes the selection, falling back to the routing slug',
     /pushToSalesforce\(\{first_name,last_name,email,phone,company,website,sell_to,product:\(product_interest\|\|product\),about_business,/.test(src));
  /* Narrow on purpose: a blanket strip-anything-and-retry would quietly
     post half a lead forever. */
  ok('sf: only the two unknown-field codes trigger the retry',
     /const SF_UNKNOWN_FIELD_CODES = \['INVALID_FIELD_FOR_INSERT_UPDATE', 'INVALID_FIELD'\]/.test(sfsrc));
  ok('sf: the offender is read from fields[] AND from the message text',
     /for \(const f of e\.fields \|\| \[\]\) named\.add\(f\);/.test(sfsrc) &&
     /matchAll\(\/'\(\[A-Za-z0-9_\]\+__c\)'\/g\)/.test(sfsrc));

  /* EXECUTED: the real function, a stubbed Salesforce. */
  const sf = require('../salesforce.js');
  const realFetch = global.fetch;
  /* ALWAYS restores the console, even on a throw. Without the finally, a
     throw between muting and restoring leaves the suite silent — it prints
     no totals at all and the run reads as UNMEASURED rather than as a
     failure. Measured: breaking the guard came back UNMEASURED until this
     existed, for exactly that reason. */
  const silenced = async (fn) => {
    const l = console.log, w = console.warn, e = console.error;
    console.log = console.warn = console.error = () => {};
    try { return await fn(); }
    finally { console.log = l; console.warn = w; console.error = e; }
  };
  const PAYLOAD = { first_name: 'A', last_name: 'B', email: 'a@b.com', company: 'Acme',
    product: 'crm', about_business: 'We move pallets.', sell_to: 'B2B' };
  const harness = (behaviour, existing = false) => {
    const posts = [];
    global.fetch = async (url, opts) => {
      if (String(url).includes('/oauth2/token'))
        return { ok: true, json: async () => ({ access_token: 't', instance_url: 'https://sf.invalid' }) };
      if (String(url).includes('/query'))
        return { ok: true, json: async () => ({ totalSize: existing ? 1 : 0, records: existing ? [{ Id: '00Qx' }] : [] }) };
      const body = JSON.parse(opts.body);
      body.__method = opts.method;
      posts.push(body);
      posts.__headers = opts.headers;
      /* A REAL fetch Response carries BOTH .json() and .text(), and a body
         can only be consumed once. salesforce.js reads .text() and parses it
         itself, so that an HTML maintenance page produces a named outage
         instead of "Unexpected token <". These fixtures only ever defined
         .json(), so they stopped modelling a Response the moment that
         changed -- and a stub that is not a Response tests nothing.
         Derived rather than written out at each fixture, so a new one cannot
         forget it. */
      const r = behaviour(body, posts.length);
      if (r && typeof r.json === 'function' && typeof r.text !== 'function') {
        r.text = async () => JSON.stringify(await r.json());
      }
      if (r && r.status === undefined) r.status = r.ok ? 200 : 400;
      return r;
    };
    return posts;
  };
  const reject = (fields) => (body) => {
    const bad = fields.filter((f) => f in body);
    if (bad.length) return { ok: false, json: async () => ([{ errorCode: 'INVALID_FIELD_FOR_INSERT_UPDATE',
      message: "No such column '" + bad[0] + "' on sobject of type Lead", fields: bad }]) };
    return { ok: true, json: async () => ({ id: '00Qok', success: true }) };
  };

  /* The catch is attached HERE, not at the drain site: a rejection with no
     handler in the same tick fires unhandledRejection, and crash-reporter
     turns that into a crashed suite — UNMEASURED, which reads as neither a
     pass nor a catch. Measured: without this, breaking the guard came back
     UNMEASURED instead of failing. */
  /* A FUNCTION, not a started promise. Section 12 also stubs global.fetch,
     and an IIFE here begins executing immediately — the two then race for
     the same global and section 12's stub throws into section 16's calls.
     The tail invokes this only after section 12 has finished. */
  results16 = async () => {
    const out = [];
    let posts = harness(() => ({ ok: true, json: async () => ({ id: '00Q1', success: true }) }));
    let r = await silenced(() => sf.pushToSalesforce(PAYLOAD).catch(() => null));
    out.push(['sf: happy path creates the lead with both fields',
      !!r && r.success === true && posts[0].Product__c === 'crm' && posts[0].About_Business__c === 'We move pallets.', JSON.stringify(r)]);
    out.push(['sf: and needs only one POST', posts.length === 1, String(posts.length)]);

    posts = harness(reject(['Product__c']));
    r = await silenced(() => sf.pushToSalesforce(PAYLOAD).catch(() => null));
    out.push(['sf: a rejected Product__c does NOT lose the lead', !!r && r.success === true, JSON.stringify(r)]);
    out.push(['sf: it retries exactly once', posts.length === 2, String(posts.length)]);
    /* posts[1] does not exist when no retry happened. Reading through it
       THROWS, which loses every assertion after it and turns a broken guard
       into an UNMEASURED run instead of a failure — measured, twice. */
    const retryBody = posts[1] || {};
    out.push(['sf: the retry drops only the named field',
      !!posts[1] && !('Product__c' in retryBody) && retryBody.About_Business__c === 'We move pallets.']);
    out.push(['sf: every other field survives the retry',
      !!posts[1] && retryBody.Email === 'a@b.com' && retryBody.Company === 'Acme' && retryBody.sell_to__c === 'B2B']);
    out.push(['sf: the caller is told what was dropped', JSON.stringify((r||{}).droppedFields) === '["Product__c"]', JSON.stringify(r.droppedFields)]);

    posts = harness((body) => ('Product__c' in body)
      ? { ok: false, json: async () => ([{ errorCode: 'INVALID_FIELD', message: "No such column 'Product__c' on sobject of type Lead" }]) }
      : { ok: true, json: async () => ({ id: '00Q4', success: true }) });
    r = await silenced(() => sf.pushToSalesforce(PAYLOAD).catch(() => null));
    out.push(['sf: a field named only in the message text is still found',
      !!r && r.success === true && !!posts[1] && !('Product__c' in posts[1])]);

    /* Failures now REJECT rather than resolving { success:false } — that is
       the whole point of the change, so the assertions catch the rejection. */
    posts = harness(() => ({ ok: false, json: async () => ([{ errorCode: 'REQUIRED_FIELD_MISSING',
      message: 'Required fields are missing: [LastName]', fields: ['LastName'] }]) }));
    let threw = false;
    r = await silenced(() => sf.pushToSalesforce(PAYLOAD).catch(() => { threw = true; }));
    out.push(['sf: an unrelated error rejects, and does NOT strip-and-retry',
      threw === true && posts.length === 1, 'threw=' + threw + ' posts=' + posts.length]);

    posts = harness(() => ({ ok: false, json: async () => ([{ errorCode: 'INVALID_FIELD_FOR_INSERT_UPDATE',
      message: "No such column 'Product__c' on sobject of type Lead", fields: ['Product__c'] }]) }));
    threw = false;
    r = await silenced(() => sf.pushToSalesforce(PAYLOAD).catch(() => { threw = true; }));
    out.push(['sf: a retry that also fails REJECTS, so the critical alert fires',
      threw === true && posts.length === 2, 'threw=' + threw + ' posts=' + posts.length]);

    /* THE SILENCE, pinned. Every one of these resolved quietly before, so the
       .catch raising alertOps('critical','Salesforce','Lead not created')
       could never fire — for any write failure, on either path. */
    const alertsOn = async (existing, resp) => {
      harness(resp, existing);
      let a = false;
      await silenced(() => sf.pushToSalesforce(PAYLOAD).catch(() => { a = true; }));
      return a;
    };
    const rejectWith = (code) => () => ({ ok: false, json: async () => ([{ errorCode: code, message: 'x' }]) });
    out.push(['sf: an UPDATE rejected by a duplicate rule alerts',
      await alertsOn(true, rejectWith('DUPLICATES_DETECTED')) === true]);
    out.push(['sf: an UPDATE rejected by a validation rule alerts',
      await alertsOn(true, rejectWith('FIELD_CUSTOM_VALIDATION_EXCEPTION')) === true]);
    out.push(['sf: a CREATE rejection alerts',
      await alertsOn(false, rejectWith('REQUIRED_FIELD_MISSING')) === true]);
    out.push(['sf: a SUCCESSFUL write still does not alert (no storm)',
      await alertsOn(true, () => ({ ok: true, status: 204, json: async () => ({}) })) === false]);

    /* The duplicate-rule header the create path always had and the update
       path never did — which is the bug that surfaced all of this.

       Asserted on the CAPTURED REQUEST, with no source-text fallback. The
       first version of this had `|| /header/.test(sfsrc)`, which the CREATE
       path satisfies on its own, so deleting the header from the UPDATE path
       still passed. Measured — that mutation survived. */
    const hdrPosts = harness(() => ({ ok: true, status: 204, json: async () => ({}) }), true);
    await silenced(() => sf.pushToSalesforce(PAYLOAD).catch(() => null));
    const hdrs = hdrPosts.__headers || {};
    out.push(['sf: the UPDATE is a PATCH', (hdrPosts[0] || {}).__method === 'PATCH', (hdrPosts[0] || {}).__method]);
    out.push(['sf: the UPDATE sends Sforce-Duplicate-Rule-Header allowSave=true',
      hdrs['Sforce-Duplicate-Rule-Header'] === 'allowSave=true',
      JSON.stringify(hdrs['Sforce-Duplicate-Rule-Header'])]);

    /* The unknown-field retry on the UPDATE path. It guarded creates only
       until now, so an unknown field on an update took the whole record
       down — and this case was missing here too, so removing the retry
       survived. */
    const upPosts = harness((body) => ('Product__c' in body)
      ? { ok: false, json: async () => ([{ errorCode: 'INVALID_FIELD_FOR_INSERT_UPDATE',
          message: "No such column 'Product__c' on sobject of type Lead", fields: ['Product__c'] }]) }
      : { ok: true, status: 204, json: async () => ({}) }, true);
    /* .catch, always: an unguarded rejection here aborts the rest of section
       16 and the run comes back UNMEASURED instead of failing. Measured —
       removing the retry did exactly that. */
    const upRes = await silenced(() => sf.pushToSalesforce(PAYLOAD).catch(() => null));
    out.push(['sf: an UPDATE survives an unknown field', !!upRes && upRes.success === true, JSON.stringify(upRes)]);
    out.push(['sf: the UPDATE retried once, dropping only the named field',
      upPosts.length === 2 && !('Product__c' in (upPosts[1] || {})) && !!(upPosts[1] || {}).About_Business__c,
      'posts=' + upPosts.length]);
    out.push(['sf: and the UPDATE reports what it dropped',
      JSON.stringify((upRes || {}).droppedFields) === '["Product__c"]', JSON.stringify((upRes || {}).droppedFields)]);

    global.fetch = realFetch;
    return out;
  };
}

/* ============================================================
   17. /monitor/metrics — queries are bound BY NAME, not by position

   Twelve queries run at once here. They used to be destructured
   POSITIONALLY, and on 8 Sept a query inserted at position 2 with its name
   appended at the end shifted nine bindings by one: 0 people with 497
   completed, "no new form entries in the last 24 hours" on a night with
   real leads, "not tracked" on every funnel stage. One line, no database
   involvement, and invisible to both the query test and the renderer test
   because the binding lives between them.

   The array is now an object, so the LANGUAGE enforces the pairing and a
   whole class of bug is gone rather than guarded. These assertions keep it
   that way, and keep the weaker column check underneath as a second net —
   it is the one that would notice a key labelled with the wrong name.
   ============================================================ */
{
  const mLines = src.split('\n');
  const L = mLines.findIndex((l) => /const \{ totals, people,.*\} = await allNamed\(\{/.test(l));
  ok('metrics: the twelve queries are bound BY NAME, not positionally', L > 0);
  ok('metrics: no positional Promise.all destructuring is left in this route',
     !/const \[totals, people,/.test(src));
  ok('metrics: allNamed preserves concurrency by awaiting an already-started object',
     /const settled = await Promise\.all\(keys\.map\(\(k\) => jobs\[k\]\)\);/.test(src));

  const names = L > 0 ? /const \{([^}]*)\}/.exec(mLines[L])[1].split(',').map((x) => x.trim()) : [];

  /* Every destructured name must be a key that actually exists in the
     object, or it is silently undefined at the use site. */
  const keys = [];
  for (let i = L + 1; i < mLines.length; i++) {
    if (/^    \}\);/.test(mLines[i])) break;
    const m = /^      (\w+): pool\.query\(/.exec(mLines[i]);
    if (m) keys.push(m[1]);
  }
  ok('metrics: one labelled query per destructured name',
     names.length === keys.length, names.length + ' names vs ' + keys.length + ' keys');
  const missing = names.filter((n) => !keys.includes(n));
  ok('metrics: every destructured name exists as a key', missing.length === 0, missing.join(','));
  const unused = keys.filter((k) => !names.includes(k));
  ok('metrics: every key is destructured (an unread query is a wasted round trip)',
     unused.length === 0, unused.join(','));

  /* Second net: a key CAN still be given the wrong name. For each name,
     check the query under that key selects the columns the route reads. */
  const qText = {};
  {
    let cur = null, key = null;
    for (let i = L + 1; i < mLines.length; i++) {
      if (/^    \}\);/.test(mLines[i])) break;
      const m = /^      (\w+): pool\.query\(/.exec(mLines[i]);
      if (m) { if (key) qText[key] = cur; key = m[1]; cur = ''; }
      if (key) cur += mLines[i] + '\n';
    }
    if (key) qText[key] = cur;
  }
  const bodyStart = src.indexOf('    });', src.indexOf('await allNamed({'));
  const body = src.slice(bodyStart, src.indexOf('\napp.', bodyStart));
  const selected = (q) => {
    const set = new Set();
    for (const m of (q || '').matchAll(/\bAS\s+([a-z_][a-z0-9_]*)/gi)) set.add(m[1].toLowerCase());
    const sel = /SELECT([\s\S]*?)FROM/i.exec(q || '');
    if (sel) for (const m of sel[1].matchAll(/(?:^|,)\s*(?:[a-z]\.)?([a-z_][a-z0-9_]*)\s*(?:,|$)/gim))
      set.add(m[1].toLowerCase());
    return set;
  };
  const readFrom = (name) => {
    const set = new Set();
    for (const m of body.matchAll(new RegExp(name + '\\.rows\\[0\\]\\.([a-z_][a-z0-9_]*)', 'gi')))
      set.add(m[1].toLowerCase());
    for (const a of body.matchAll(new RegExp('const (\\w+)\\s*=\\s*' + name + '\\.rows\\[0\\]', 'g')))
      for (const c of body.matchAll(new RegExp('\\b' + a[1] + '\\.([a-z_][a-z0-9_]*)', 'gi')))
        set.add(c[1].toLowerCase());
    for (const m of body.matchAll(new RegExp(name + '\\.rows\\.map\\(\\s*\\(?(\\w+)\\)?\\s*=>\\s*\\(?\\{([\\s\\S]*?)\\}\\)?\\s*\\)', 'g')))
      for (const c of m[2].matchAll(new RegExp('\\b' + m[1] + '\\.([a-z_][a-z0-9_]*)', 'gi')))
        set.add(c[1].toLowerCase());
    return set;
  };
  const mismatched = [];
  for (const n of names) {
    const bad = [...readFrom(n)].filter((c) => !selected(qText[n]).has(c));
    if (bad.length) mismatched.push(n + ' cannot supply ' + bad.join(','));
  }
  ok('metrics: each name reads only columns its own query selects',
     mismatched.length === 0, mismatched.join(' | '));

  ok('metrics: byProduct is the per-product query',
     /COALESCE\(product, 'untagged'\)/.test(qText.byProduct || ''));
  ok('metrics: and it groups by product, so it returns one row per product',
     /GROUP BY 1/.test(qText.byProduct || ''));
}

/* ============================================================
   18. The RevenueHero router allowlist

   /rh-webhook is the SAFETY NET for a booking the browser never reports.
   It compared router_name against a single hardcoded string, so when CRM
   got its own router every CRM booking webhook was dropped. Nothing looked
   wrong on 8 Sept because the browser path had already recorded the
   booking — which is exactly the condition under which a safety net is
   never exercised and never missed.

   Executed against the payload shapes RevenueHero actually sends, rather
   than asserted from source, because the question is which bookings get
   through.
   ============================================================ */
{
  const listSrc = src.slice(src.indexOf('const RH_ALLOWED_ROUTERS = ['),
                            src.indexOf('];', src.indexOf('const RH_ALLOWED_ROUTERS = [')) + 2);
  ok('rhrouter: the allowlist exists', listSrc.length > 20);
  /* true = the webhook is SKIPPED */
  const skips = new Function('payload', listSrc + `
    const rhRouter = (payload.router_name || '').toString().trim().toLowerCase();
    return !!(rhRouter && !RH_ALLOWED_ROUTERS.some((r) => r.toLowerCase() === rhRouter));
  `);

  for (const [name, shouldSkip, why] of [
    ['Inbound Router - Website',        false, 'the original AEO router'],
    ['New Product Router - Website',    false, 'the CRM router'],
    [undefined,                         false, 'no router_name (pre-field payloads)'],
    ['',                                false, 'empty router_name'],
    ['  New Product Router - Website ', false, 'stray whitespace'],
    ['new product router - website',    false, 'different casing'],
    ['Outbound Router - SDR',           true,  'a genuinely different router'],
    ['Partner Router',                  true,  'another unrelated router'],
  ]) ok('rhrouter: ' + (shouldSkip ? 'skips  ' : 'accepts') + ' ' + JSON.stringify(name) + ' — ' + why,
        skips({ router_name: name }) === shouldSkip);

  /* The regression, pinned: the old single-value check dropped CRM. */
  const oldCheck = (p) => !!(p.router_name && p.router_name !== 'Inbound Router - Website');
  ok('rhrouter: (control) the old single-value check DID drop the CRM router',
     oldCheck({ router_name: 'New Product Router - Website' }) === true);
  ok('rhrouter: index.js no longer contains that single-value check',
     !/router_name !== 'Inbound Router - Website'/.test(src));
  ok('rhrouter: the skip log names the allowed routers, so a mismatch is diagnosable',
     /Allowed: \$\{RH_ALLOWED_ROUTERS\.join/.test(src));
  /* Adding a product means adding a router; keep them visible together. */
  ok('rhrouter: both routers are listed', /'Inbound Router - Website',/.test(listSrc) &&
     /'New Product Router - Website',/.test(listSrc));
}

/* ============================================================
   19. META_TEST_EVENT_CODE — off unless set, invisible when unset

   test_event_code is a TOP-LEVEL field on the request, a sibling of data.
   Putting it inside the event object is the usual mistake and Meta ignores
   it silently there, which looks like "the code did not work" rather than
   "the code was in the wrong place".

   The byte-identity assertion below compares the REQUEST BODY the module
   builds with the flag unset against a hand-written expectation of the same
   shape — event_time and the random event_id suffix normalised, because
   those legitimately vary per call and nothing else may.
   ============================================================ */
{
  const META = require('../meta-capi.js');
  const realFetch = global.fetch;
  const realCode = process.env.META_TEST_EVENT_CODE;
  process.env.META_PIXEL_ID = process.env.META_PIXEL_ID || 'test-pixel';
  process.env.META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN || 'test-token';

  const silenced = async (fn) => {
    const l = console.log, w = console.warn, e = console.error;
    console.log = console.warn = console.error = () => {};
    try { return await fn(); } finally { console.log = l; console.warn = w; console.error = e; }
  };
  const capture = async () => {
    let body = null;
    global.fetch = async (u, o) => { body = o.body; return { ok: true, json: async () => ({ events_received: 1 }) }; };
    await silenced(() => META.pushFormEventsToMeta(
      { session_id: 'FIXED', email: 'a@b.com', page_url: 'https://gushwork.ai/ai-demo', sell_to: 'B2B' }, {}
    ).catch(() => null));
    return body;
  };

  results19 = async () => {
    const out = [];
    try {
      delete process.env.META_TEST_EVENT_CODE;
      const off = JSON.parse(await capture());
      out.push(['tec: unset -> no test_event_code key at all',
        !Object.prototype.hasOwnProperty.call(off, 'test_event_code'), Object.keys(off).join(',')]);
      out.push(['tec: unset -> the body is exactly { data: [...] }',
        JSON.stringify(Object.keys(off)) === '["data"]', Object.keys(off).join(',')]);

      process.env.META_TEST_EVENT_CODE = 'TEST00000';
      const on = JSON.parse(await capture());
      out.push(['tec: set -> the code is present', on.test_event_code === 'TEST00000', String(on.test_event_code)]);
      out.push(['tec: set -> it is a SIBLING of data, not a property of the event',
        'test_event_code' in on && !('test_event_code' in on.data[0]),
        Object.keys(on).join(',') + ' | event: ' + Object.keys(on.data[0]).join(',')]);

      /* The event must be untouched by the flag — only the envelope changes. */
      const norm = (e) => { const c = { ...e }; delete c.event_time; delete c.event_id; return JSON.stringify(c); };
      out.push(['tec: the event object itself is identical with and without the code',
        norm(on.data[0]) === norm(off.data[0])]);

      process.env.META_TEST_EVENT_CODE = '';
      const empty = JSON.parse(await capture());
      out.push(['tec: an empty env var counts as unset',
        !Object.prototype.hasOwnProperty.call(empty, 'test_event_code')]);
    } finally {
      global.fetch = realFetch;
      if (realCode === undefined) delete process.env.META_TEST_EVENT_CODE;
      else process.env.META_TEST_EVENT_CODE = realCode;
    }
    return out;
  };
}

/* ============================================================
   12. A Meta failure must reach recordFailure

   Every push function ended in Promise.allSettled, which never rejects.
   So the .catch(...) at each call site in index.js — the one calling
   recordFailure('Meta CAPI', ...) — could not fire, at five call sites
   that all read like alerting. 'Meta CAPI' has always had a
   FAILURE_MONITORS entry, so unlike the PartnerStack case the table was
   never the problem; the promise shape was.

   Asserted by EXECUTION. A source-level assertion cannot tell you whether
   a handler ran — that is the whole lesson of the 21 dead PartnerStack
   call sites. Each case drives the real function through the real
   call-site shape (fire-and-forget with a .catch) and asserts on whether
   the handler was reached.

   The two failure shapes are separate cases on purpose: reporting only
   the thrown one would leave the resolved success:false one exactly as
   silent as it was before.

   This is the one ASYNC section, which is why the totals below are
   printed from its continuation rather than at the end of the file.
   ============================================================ */
async function section12() {
  const META = require('../meta-capi.js');
  const realFetch = global.fetch;
  const realPixel = process.env.META_PIXEL_ID;
  const realToken = process.env.META_ACCESS_TOKEN;
  process.env.META_PIXEL_ID = 'test-pixel';
  process.env.META_ACCESS_TOKEN = 'test-token';

  // Silence the module's own logging so a suite run stays readable.
  const quiet = () => {
    const l = console.log, w = console.warn, e = console.error;
    console.log = console.warn = console.error = () => {};
    return () => { console.log = l; console.warn = w; console.error = e; };
  };
  /* The exact call-site shape from index.js. `seen` stands in for
     recordFailure: if it stays empty, nothing alerted. */
  const callSite = async (fn, payload) => {
    const seen = [];
    const un = quiet();
    try { await fn(payload, {}).catch((err) => seen.push(err.message)); }
    finally { un(); }
    return seen;
  };

  const DEMO = 'https://gushwork.ai/demo';
  const CASES = [
    ['Lead',       'pushFormEventsToMeta', { session_id: 's', email: 'a@b.com', page_url: DEMO }],
    ['Schedule',   'pushFormEventsToMeta', { session_id: 's', email: 'a@b.com', booking_uid: 'bk', page_url: DEMO }],
    ['StartTrial', 'pushStartTrialToMeta', { session_id: 's', email: 'a@b.com', sell_to: 'B2B', page_url: DEMO }],
    ['Contact',    'pushContactToMeta',    { session_id: 's', email: 'a@b.com', page_url: 'https://gushwork.ai/lm' }],
  ];

  try {
    // 1. the network failure that was proved dead
    global.fetch = async () => { throw new Error('ECONNREFUSED'); };
    for (const [label, fnName, payload] of CASES) {
      const seen = await callSite(META[fnName], payload);
      ok('capi: ' + label + ' — a network failure reaches the handler',
         seen.length === 1, JSON.stringify(seen));
      ok('capi: ' + label + ' — the error names the event and the cause',
         seen.length === 1 && seen[0].includes(label) && seen[0].includes('ECONNREFUSED'), seen[0]);
    }

    /* 2. Meta answered, and said no. This RESOLVED with success:false and
       printed at console.log level, in a line that reads like a success. */
    global.fetch = async () => ({ ok: false,
      json: async () => ({ error: { message: 'Invalid parameter', code: 100 } }) });
    for (const [label, fnName, payload] of CASES) {
      const seen = await callSite(META[fnName], payload);
      ok('capi: ' + label + ' — a 4xx from Meta reaches the handler',
         seen.length === 1, JSON.stringify(seen));
      ok('capi: ' + label + " — the error carries Meta's own message",
         seen.length === 1 && seen[0].includes('Invalid parameter'), seen[0]);
    }

    // 3. pixel not configured at all — every event silently dropped
    delete process.env.META_PIXEL_ID;
    {
      const seen = await callSite(META.pushFormEventsToMeta, CASES[0][2]);
      ok('capi: an unconfigured pixel reaches the handler',
         seen.length === 1 && seen[0].includes('Missing credentials'), JSON.stringify(seen));
    }
    process.env.META_PIXEL_ID = 'test-pixel';

    /* 4. a gateway error page instead of JSON: res.json() throws inside
       sendEvent, which used to degrade into the same silence. */
    global.fetch = async () => ({ ok: true,
      json: async () => { throw new Error('Unexpected token < in JSON'); } });
    {
      const seen = await callSite(META.pushFormEventsToMeta, CASES[0][2]);
      ok('capi: a non-JSON response reaches the handler', seen.length === 1, JSON.stringify(seen));
    }

    /* 5. Success must still RESOLVE. A handler that fires on a good send
       would alert on every single lead, which is its own kind of broken. */
    global.fetch = async () => ({ ok: true, json: async () => ({ events_received: 1 }) });
    for (const [label, fnName, payload] of CASES) {
      const seen = await callSite(META[fnName], payload);
      ok('capi: ' + label + ' — a successful send does NOT reach the handler',
         seen.length === 0, JSON.stringify(seen));
    }

    /* 6. StartTrial short-circuits for non-B2B before sending anything.
       That is a skip, not a failure, and must not alert. */
    {
      const seen = await callSite(META.pushStartTrialToMeta,
        { session_id: 's', email: 'a@b.com', sell_to: 'B2C', page_url: DEMO });
      ok('capi: StartTrial skipped for non-B2B does not alert', seen.length === 0, JSON.stringify(seen));
    }

    /* 7. Every call site in index.js must still have a handler attached.
       Making these reject is only safe while that is true — a caller
       without one turns a Meta outage into an unhandled rejection. */
    const lm = fs.readFileSync(path.join(__dirname, '..', 'lead-magnet.js'), 'utf8');
    const callsites = [...src.matchAll(/push(?:FormEvents|StartTrial|Contact)ToMeta\(/g)].length;
    ok('capi: index.js still has five Meta call sites', callsites === 5, String(callsites));
    ok('capi: neither file ever awaits a push (a rejection must not 500 a route)',
       !/await\s+push(?:FormEvents|StartTrial|Contact)ToMeta\(/.test(src) &&
       !/await\s+push(?:FormEvents|StartTrial|Contact)ToMeta\(/.test(lm));
    ok('capi: the lead-magnet Contact call still has a .catch',
       /pushContactToMeta\([\s\S]{0,1200}?\.catch\(/.test(lm));
    ok('capi: both /partial and /submit still record the failure they catch',
       (src.match(/recordFailure\('Meta CAPI'/g) || []).length === 5);
  } finally {
    global.fetch = realFetch;
    if (realPixel === undefined) delete process.env.META_PIXEL_ID; else process.env.META_PIXEL_ID = realPixel;
    if (realToken === undefined) delete process.env.META_ACCESS_TOKEN; else process.env.META_ACCESS_TOKEN = realToken;
  }
}

/* ============================================================
   20. backfill-sf keeps going when Salesforce rejects a lead

   552db39 made pushToSalesforce THROW on failure instead of returning
   { success: false }, so that a dead Salesforce could reach alertOps
   from /submit. runBackfill's loop had no try/catch, so from that
   commit the first rejected lead aborted the entire backfill: no FAILED
   entry for it, no attempt at any lead after it, and the summary counts
   simply stopped where the throw happened.

   That is the worst possible time for it. This tool only ever runs
   after Salesforce has been rejecting things, so "rejections are
   likely" is its normal operating condition.

   Driven by EXECUTION, not read from the source. The whole point is
   whether the loop reaches the second lead, and no source-text
   assertion can answer that — an ordering or presence assertion here
   passes just as happily with the abort still in place.
   ============================================================ */
{
  const { runBackfill } = require('../backfill-sf.js');

  const COLS = ['email','first_name','last_name','company','phone','website',
                'submitted_at','created_at','updated_at','completed','booking_uid','start_time'];

  results20 = async () => {
    const out = [];
    const realFetch = global.fetch;
    const realTimeout = global.setTimeout;
    const realLog = console.log, realWarn = console.warn, realErr = console.error;
    const realEnv = {
      SF_LOGIN_URL: process.env.SF_LOGIN_URL,
      SF_CLIENT_ID: process.env.SF_CLIENT_ID,
      SF_CLIENT_SECRET: process.env.SF_CLIENT_SECRET,
      SF_REFRESH_TOKEN: process.env.SF_REFRESH_TOKEN,
    };
    try {
      process.env.SF_LOGIN_URL = 'https://stub.invalid';
      process.env.SF_CLIENT_ID = 'x';
      process.env.SF_CLIENT_SECRET = 'x';
      process.env.SF_REFRESH_TOKEN = 'x';

      /* The loop paces the SF API with a 500ms sleep per lead. Run the
         timer immediately so the suite stays about a second long. */
      global.setTimeout = (fn) => { fn(); return 0; };

      const attempted = [];
      const json = (status, body) => ({
        ok: status >= 200 && status < 300,
        status,
        json: async () => body,
        text: async () => JSON.stringify(body),
      });
      global.fetch = async (url, opts = {}) => {
        const u = String(url);
        if (u.includes('/oauth2/token')) return json(200, { access_token: 't', instance_url: 'https://stub.invalid' });
        if (u.includes('/query/')) return json(200, { records: [] });   // no existing lead, not converted
        if (u.includes('/sobjects/Lead')) {
          const body = JSON.parse(opts.body || '{}');
          attempted.push(body.Email);
          /* A rejection Salesforce really returns, and NOT an unknown-field
             one — those take the strip-and-retry path instead of throwing. */
          if (body.Email === 'reject@example.net') {
            return json(400, [{ message: 'REQUIRED_FIELD_MISSING', errorCode: 'REQUIRED_FIELD_MISSING', fields: [] }]);
          }
          return json(201, { id: '00Q' + body.Email });
        }
        throw new Error('unexpected fetch in section 20: ' + u);
      };

      const pool = {
        query: async (sql) => {
          if (/information_schema/.test(sql)) return { rows: COLS.map((c) => ({ column_name: c })) };
          return { rows: [
            { email: 'reject@example.net', first_name: 'A', last_name: 'One',   company: 'C1', submitted_at: '2026-09-01T00:00:00Z' },
            { email: 'after@example.net',  first_name: 'B', last_name: 'Two',   company: 'C2', submitted_at: '2026-09-02T00:00:00Z' },
            { email: 'last@example.net',   first_name: 'C', last_name: 'Three', company: 'C3', submitted_at: '2026-09-03T00:00:00Z' },
          ] };
        },
      };

      console.log = console.warn = console.error = () => {};
      let res = null, threw = null;
      try { res = await runBackfill(pool, { from: '2026-01-01T00:00:00Z' }); }
      catch (err) { threw = err; }
      console.log = realLog; console.warn = realWarn; console.error = realErr;

      out.push(['backfill: a rejected lead does not abort the run',
        threw === null, threw && threw.message]);

      /* The regression in one line: pre-fix this was ['reject@example.net']. */
      out.push(['backfill: every lead is still attempted after a rejection',
        attempted.length === 3, JSON.stringify(attempted)]);

      const rows = (res && res.results) || [];
      out.push(['backfill: one log entry per lead', rows.length === 3, String(rows.length)]);
      out.push(['backfill: the rejected lead is recorded FAILED',
        !!rows[0] && rows[0].action === 'FAILED', rows[0] && rows[0].action]);
      out.push(['backfill: the FAILED entry carries the thrown reason, not undefined',
        !!rows[0] && /Lead creation failed/.test(String(rows[0].error)), rows[0] && String(rows[0].error)]);
      out.push(['backfill: the lead AFTER the rejection is pushed',
        !!rows[1] && rows[1].action === 'pushed', rows[1] && rows[1].action]);
      out.push(['backfill: the last lead is pushed',
        !!rows[2] && rows[2].action === 'pushed', rows[2] && rows[2].action]);
      out.push(['backfill: summary counts 2 pushed, 1 failed',
        !!res && res.summary.pushed === 2 && res.summary.failed === 1,
        res && JSON.stringify(res.summary)]);
    } finally {
      console.log = realLog; console.warn = realWarn; console.error = realErr;
      global.fetch = realFetch;
      global.setTimeout = realTimeout;
      for (const k of Object.keys(realEnv)) {
        if (realEnv[k] === undefined) delete process.env[k]; else process.env[k] = realEnv[k];
      }
    }
    return out;
  };
}

/* ============================================================
   21. The identity-change comparator — EXECUTED, fold by fold.

   10 Sep 2026: a "changed after booking" follow-up fired for
   www.datapartnerinc.com -> https://www.datapartnerinc.com/ and the
   visitor had touched nothing. /partial stores Apollo's website_url with
   the scheme stripped by our own applyEnrichment; /submit stores the
   website check's canonical_url, which is response.url and therefore
   absolute with a root slash. Two of our own normalisations in opposite
   directions, one round trip apart.

   The rule is FOLD EXACTLY WHAT OUR OWN CODE VARIES, NOTHING MORE, so
   every fold below is paired with the nearest difference it must NOT
   swallow. Each pair is separately mutation-testable: breaking one fold
   fails its own assertion and leaves the others green.
   ============================================================ */
{
  const idSrc = between("const LEAD_IDENTITY_FIELDS = ['email'", "app.post('/partial'");
  const I = (new Function('pool', idSrc + `
    return { normaliseIdentityValue, identityChangeAttribution, identityHostsAreNested,
             diffLeadIdentityFields, alertableIdentityChanges,
             LEAD_IDENTITY_FIELDS, LEAD_IDENTITY_FIELD_STEP,
             LEAD_CHANGE_ATTRIBUTIONS, LEAD_CHANGE_ALERTABLE };
  `))({ query: () => Promise.resolve({ rows: [] }) });

  const same = (f, a, b) => I.normaliseIdentityValue(f, a) === I.normaliseIdentityValue(f, b);

  /* website — the fold, and what must survive it */
  ok('fold: scheme and a root trailing slash are ours (the datapartnerinc pair)',
     same('website', 'www.datapartnerinc.com', 'https://www.datapartnerinc.com/'));
  ok('fold: a leading www. is ours', same('website', 'acme.com', 'www.acme.com'));
  ok('fold: host case is ours', same('website', 'ACME.com', 'acme.com'));
  ok('fold: http vs https is ours', same('website', 'http://acme.com', 'https://acme.com'));
  ok('fold: a PATH still differs', !same('website', 'acme.com', 'acme.com/uk'));
  ok('fold: path CASE still differs — nothing here recases a path',
     !same('website', 'acme.com/UK', 'acme.com/uk'));
  ok('fold: a SUBDOMAIN still differs', !same('website', 'acme.com', 'shop.acme.com'));
  ok('fold: a TLD correction still differs', !same('website', 'acme.co', 'acme.com'));
  ok('fold: an unrelated domain still differs', !same('website', 'acme.com', 'othercorp.io'));
  ok('fold: a trailing slash is stripped from a path too, but the path stays',
     same('website', 'acme.com/uk/', 'acme.com/uk') && !same('website', 'acme.com/uk/', 'acme.com'));

  /* phone — intlTelInput E.164 vs the raw fallback when utils.js has not loaded */
  ok('fold: phone punctuation is ours', same('phone', '+91 63886 39290', '+916388639290'));
  ok('fold: phone brackets and dashes are ours', same('phone', '+1 (612) 790-5259', '+16127905259'));
  ok('fold: a different number still differs', !same('phone', '+916388639290', '+916388639291'));

  /* email — a no-op today because both routes lowercase first */
  ok('fold: email case is ours', same('email', 'Bob@Acme.com', 'bob@acme.com'));
  ok('fold: a different local part still differs', !same('email', 'bob@acme.com', 'rob@acme.com'));

  /* the free-text fields — whitespace only, DELIBERATELY not case */
  ok('fold: company whitespace is ours', same('company', 'Acme   Inc ', 'Acme Inc'));
  ok('fold: company CASE is NOT folded — nothing in this repo recases a company',
     !same('company', 'acme inc', 'Acme Inc'));
  ok('fold: a name is whitespace-folded but not recased',
     same('first_name', ' Joseph ', 'Joseph') && !same('first_name', 'joseph', 'Joseph'));

  /* ── the attribution ladder ──────────────────────────────────── */
  const A = (f, from, to, arrived, prev) => I.identityChangeAttribution(f, from, to, arrived, prev);
  eq('ladder: a step-2 field written at step 1 is ours, never the visitor',
     A('website', 'a.com', 'b.com', 1, 2), 'ours_earlier_step');
  eq('ladder: a step-2 field on a row that had only reached step 1 is them filling it in',
     A('website', 'apollo-guess.com', 'typed.com', 2, 1), 'ours_replacing_guess');
  /* BOTH ours_* conditions hold here, so only the ORDER decides which
     label the row gets. Without this case the two rungs are swappable
     and nothing notices -- measured: swapping them survived every
     other assertion in all ten suites. arrived_step wins because it is
     the more specific truth: the write itself came from step 1, which
     is a stronger statement than "the row had not got to step 2 yet". */
  /* ── the sell_to clarification: 18 of the 27 rows in the table ──
     Every one of them was reading as a prospect edit until 10 Sep 2026.
     sell_to is a radio button, and this exact string is composed by
     handleDisqualifiedNext, not chosen by anyone. */
  eq('ladder: B2C -> the clarified label is ours',
     A('sell_to', 'B2C', 'B2B (clarified from B2C)', 1, 1), 'ours_sell_to_clarified');
  eq('ladder: Mixed -> the clarified label is ours',
     A('sell_to', 'Mixed', 'B2B (clarified from Mixed)', 1, 1), 'ours_sell_to_clarified');
  /* One real row today, and its own bug — see OPEN-ITEMS item 14. The
     rule matches by composition, so it covers the nested form for free. */
  eq('ladder: the compounded form is ours too',
     A('sell_to', 'B2B (clarified from Mixed)', 'B2B (clarified from B2B (clarified from Mixed))', 1, 1),
     'ours_sell_to_clarified');
  /* Matched by COMPOSITION, not by field. Somebody going back and
     picking a different radio is a real change and must keep alerting. */
  eq('ladder: a genuine switch between options is still the prospect',
     A('sell_to', 'B2B', 'B2C', 1, 1), 'prospect_edit');
  eq('ladder: a clarified-looking value that is not OUR composition is still the prospect',
     A('sell_to', 'B2C', 'B2B (clarified from Mixed)', 1, 1), 'prospect_edit');
  ok('ladder: the clarification is suppressed from Slack',
     I.LEAD_CHANGE_ALERTABLE.indexOf('ours_sell_to_clarified') === -1);
  ok('ladder: and is a declared state', I.LEAD_CHANGE_ATTRIBUTIONS.indexOf('ours_sell_to_clarified') !== -1);

  /* THREE COPIES OF ONE LITERAL. The server now depends on the exact
     string both form files compose; change the wording in either and
     every clarification silently starts alerting as a prospect edit
     again. Same shape as the three lists and the label map. */
  const formSrc  = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form.js'), 'utf8');
  const popupSrc = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form-popup.js'), 'utf8');
  const composed = "formState.sell_to = 'B2B (clarified from ' + formState.sell_to + ')';";
  ok('sync: gushwork-form.js composes the exact string the server matches', formSrc.includes(composed));
  ok('sync: gushwork-form-popup.js composes it identically', popupSrc.includes(composed));
  ok('sync: and the server prefix is that same literal',
     /const SELL_TO_CLARIFIED_PREFIX = 'B2B \(clarified from ';/.test(src));

  eq('ladder: arrived_step outranks prev_step when both say it was us',
     A('website', 'a.com', 'b.com', 1, 1), 'ours_earlier_step');
  eq('ladder: one host containing the other could be our canonical resolution',
     A('website', 'acme.com', 'shop.acme.com', 2, 2), 'maybe_our_canonical');
  eq('ladder: an unrelated domain cannot be ours — domainsMatch would have refused it',
     A('website', 'acme.com', 'othercorp.io', 2, 2), 'prospect_edit');
  eq('ladder: email is a step-1 field, so a step-1 write CAN be the visitor',
     A('email', 'a@x.com', 'b@y.com', 1, 2), 'prospect_edit');
  eq('ladder: unknown steps fall through to the visitor, never to us',
     A('website', 'a.com', 'b.com', null, null), 'prospect_edit');
  ok('ladder: every field has a step', I.LEAD_IDENTITY_FIELDS.every((f) => I.LEAD_IDENTITY_FIELD_STEP[f] > 0));
  ok('ladder: is exhaustive — every verdict is a declared state',
     [['website','a.com','b.com',1,2],['website','a.com','b.com',2,1],
      ['website','acme.com','shop.acme.com',2,2],['email','a@x','b@y',1,1]]
       .every((args) => I.LEAD_CHANGE_ATTRIBUTIONS.indexOf(A.apply(null, args)) !== -1));
  ok('ladder: both ours_* states are excluded from Slack',
     I.LEAD_CHANGE_ALERTABLE.indexOf('ours_earlier_step') === -1 &&
     I.LEAD_CHANGE_ALERTABLE.indexOf('ours_replacing_guess') === -1);
  ok('ladder: the uncertain state still alerts',
     I.LEAD_CHANGE_ALERTABLE.indexOf('maybe_our_canonical') !== -1);
  ok('ladder: alertable is a subset of the declared states',
     I.LEAD_CHANGE_ALERTABLE.every((a) => I.LEAD_CHANGE_ATTRIBUTIONS.indexOf(a) !== -1));

  /* A change carrying NO attribution — a row from before this existed —
     alerts. "We do not know who changed this" must not become "we did". */
  eq('alertable: an unlabelled change is still alerted',
     I.alertableIdentityChanges([{ field: 'email' }]).length, 1);
  eq('alertable: our own writes are dropped',
     I.alertableIdentityChanges([{ field: 'website', attribution: 'ours_earlier_step' },
                                 { field: 'website', attribution: 'ours_replacing_guess' }]).length, 0);

  /* ── the diff itself, driven ─────────────────────────────────── */
  const row = (o) => Object.assign({
    prev_email: 'a@x.com', email: 'a@x.com', prev_company: 'C', company: 'C',
    prev_website: null, website: null, prev_phone: null, phone: null,
    prev_first_name: null, first_name: null, prev_last_name: null, last_name: null,
    prev_sell_to: null, sell_to: null, prev_booked: false, prev_step_reached: 2, step_reached: 2,
  }, o);
  eq('diff: the datapartnerinc pair produces NO change at all',
     I.diffLeadIdentityFields(row({ prev_website: 'www.datapartnerinc.com',
                                    website: 'https://www.datapartnerinc.com/' }), { arrived_step: 2 }).changes.length, 0);
  eq('diff: a real change on the same field still produces one',
     I.diffLeadIdentityFields(row({ prev_website: 'www.datapartnerinc.com',
                                    website: 'https://othercorp.io/' }), { arrived_step: 2 }).changes.length, 1);
  eq('diff: a first set is still not a change',
     I.diffLeadIdentityFields(row({ prev_website: null, website: 'acme.com' }), { arrived_step: 2 }).changes.length, 0);
  /* Indexed defensively. A mutation that empties `changes` must FAIL
     this assertion, not throw out of the suite -- a crash prints no
     totals and reads as UNMEASURED, which is neither a catch nor a
     clean run. Caught doing exactly that while mutation-testing the
     www. fold. */
  eq('diff: each change carries its field step',
     (I.diffLeadIdentityFields(row({ prev_website: 'a.com', website: 'b.com' }), { arrived_step: 2 }).changes[0] || {}).field_step, 2);
  ok('diff: back navigation is arrived < prev, read from before the upsert',
     I.diffLeadIdentityFields(row({ prev_email: 'a@x.com', email: 'b@y.com' }), { arrived_step: 1 }).backNavigation === true &&
     I.diffLeadIdentityFields(row({ prev_email: 'a@x.com', email: 'b@y.com' }), { arrived_step: 2 }).backNavigation === false);
  ok('diff: RAW values are stored, only the comparison is folded',
     (I.diffLeadIdentityFields(row({ prev_website: 'a.com', website: 'HTTPS://B.com/' }), { arrived_step: 2 }).changes[0] || {}).to === 'HTTPS://B.com/');
}

/* ============================================================
   22. arrived_step must not be taken from the upsert's step_reached.

   step_reached comes back through GREATEST(EXCLUDED.step_reached,
   leads.step_reached), so a step-1 /partial on a row already at step 2
   reads 2 -- and the out-of-step signal that identifies our own
   enrichment writing a step-2 field disappears. This is a REACHABILITY
   check on the call sites, not an ordering one: an offset comparison
   would survive the wrong argument being passed.
   ============================================================ */
{
  ok('call site: /partial passes the request step, not the row high-water mark',
     /recordLeadFieldChanges\(session_id, upsert\.rows\[0\], '\/partial', \{ arrived_step: step_reached \}\)/.test(src));
  ok('call site: /submit passes 2', /recordLeadFieldChanges\(session_id, upsert\.rows\[0\], '\/submit', \{ arrived_step: 2 \}\)/.test(src));
  ok('call site: the Slack diff gets the same step as the log',
     /diffLeadIdentityFields\(upsert\.rows\[0\], \{ arrived_step: 2 \}\)/.test(src));
  ok('prev CTE: both upserts read step_reached from BEFORE the write',
     (src.match(/sell_to, booking_uid, step_reached/g) || []).length === 2);
  ok('prev CTE: both expose it as prev_step_reached',
     (src.match(/AS prev_step_reached/g) || []).length === 2);
  /* BOTH Slack surfaces filter. Filtering only the one you are looking
     at leaves the other firing on our own writes -- and the standalone
     follow-up is the one that actually fired on 10 Sep. */
  ok('slack: the appended line filters', /const alertable = alertableIdentityChanges\(d\.identity_changes\);\s*\n\s*if \(alertable\.length > 0\)/.test(src));
  ok('slack: the standalone follow-up filters its own list',
     /value: alertableIdentityChanges\(d\.changes\)/.test(src));
  ok('slack: and its GUARD filters too, so an all-ours change posts nothing',
     /identityDiff\.wasBooked && alertableIdentityChanges\(identityDiff\.changes\)\.length > 0/.test(src));
  /* The new columns must be written, and hit_no must come from a
     subquery rather than a second round trip on the lead path. */
  const ins = between('INSERT INTO lead_field_changes', 'AS t(f, o, n, a, fs)');
  ok('insert: carries the attribution', /attribution/.test(ins));
  ok('insert: carries all four location columns',
     /field_step/.test(ins) && /arrived_step/.test(ins) && /prev_step/.test(ins) && /back_navigation/.test(ins));
  ok('insert: hit_no is a scalar subquery on the indexed columns',
     /SELECT v\.hit_no FROM form_page_views v\s+WHERE v\.session_id = \$1 ORDER BY v\.created_at DESC/.test(ins));
  ok('insert: no // comment anywhere in it', !/\/\//.test(ins.replace(/https?:\/\//g, '')));
  /* db.js adds them as nullable ALTERs with no default: a row written
     before this deploy does not know its arrived_step, and NULL says so. */
  ok('schema: the new columns are added to the existing table',
     /ADD COLUMN IF NOT EXISTS attribution\s+TEXT/.test(dbsrc) &&
     /ADD COLUMN IF NOT EXISTS back_navigation BOOLEAN/.test(dbsrc));
  ok('schema: none of them is backfilled or defaulted',
     !/ADD COLUMN IF NOT EXISTS (attribution|field_step|arrived_step|prev_step|back_navigation|hit_no)[^,;]*DEFAULT/.test(dbsrc));
}

/* ============================================================
   21. THE CRM PRODUCT ALLOWS B2C — AND THE FOURTH COPY OF THE LIST

   The sell_to gate is entirely client-side. B2C or Mixed at step 1 marks
   the lead disqualified and shows a terminal step, and the server just
   believes the boolean it is handed. So "the CRM product allows B2C"
   could only be built in the two form files -- which means the set of
   CRM paths now exists in FOUR places:

     meta-capi.js          PRODUCT_PATHS          the catalogue
     index.js              (imports it)           resolveProduct
     gushwork-form.js      B2C_ALLOWED_PATHS
     gushwork-form-popup.js B2C_ALLOWED_PATHS

   CLAUDE.md names three separate pairs of lists that must stay in sync
   and the repo has been bitten by every one of them. This section is
   the thing that stops the fourth: if a product page is added to the
   catalogue and not to the forms, the visitor is disqualified on a page
   the server has already decided is CRM, and nothing else in the repo
   would say so.

   IT ASSERTS AGREEMENT, NOT A LITERAL. Hardcoding ['/ai-demo'] here
   would make this a fifth copy.
   ============================================================ */
{
  const capisrc = fs.readFileSync(path.join(__dirname, '..', 'meta-capi.js'), 'utf8');
  const formA   = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form.js'), 'utf8');
  const formB   = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form-popup.js'), 'utf8');

  /* The catalogue, lifted and executed rather than regexed, so the
     answer is the same object resolveProduct reads. */
  const { PRODUCT_PATHS, DEFAULT_PRODUCT } = require('../meta-capi');
  const crmPaths = Object.keys(PRODUCT_PATHS)
    .filter((p) => PRODUCT_PATHS[p] === 'crm').sort();
  ok('21: the catalogue has at least one CRM path', crmPaths.length > 0);
  ok('21: aeo is still the default', DEFAULT_PRODUCT === 'aeo');

  const liftAllowed = (fsrc, name) => {
    const m = fsrc.match(/const B2C_ALLOWED_PATHS = (\[[^\]]*\]);/);
    ok(`21: ${name} declares B2C_ALLOWED_PATHS`, !!m, name);
    return m ? JSON.parse(m[1].replace(/'/g, '"')) : null;
  };
  const allowedA = liftAllowed(formA, 'gushwork-form.js');
  const allowedB = liftAllowed(formB, 'gushwork-form-popup.js');

  eq('21: /demo form allows exactly the CRM paths', (allowedA || []).slice().sort(), crmPaths);
  eq('21: ads form allows exactly the CRM paths',   (allowedB || []).slice().sort(), crmPaths);
  eq('21: the two form files agree with each other', allowedA, allowedB);

  /* THE GATE ITSELF. Both files must compute the condition ONCE and use
     it for both the flag and the step. Two independent copies is how a
     lead gets marked disqualified and shown step 2 anyway -- the state
     we record and the screen the visitor sees disagreeing. */
  for (const [name, f] of [['/demo', formA], ['ads', formB]]) {
    /* PATH OR SELECTION since 15 Sept 2026. Ticking AI-CRM unlocks the
       B2C exception exactly as being on /ai-demo does -- the exception is
       about the product, and the page was only ever a proxy for it.
       Still computed ONCE and used for both the flag and the step. */
    ok(`21: ${name} computes the gate once, from path OR selection`,
       /const crmChosen = b2cAllowedHere\(\) \|\| wantsCrm\(\);/.test(f)
       && /const gateOnSellTo = \(sellTo === 'B2C' \|\| sellTo === 'Mixed'\) && !crmChosen;/.test(f), name);
    ok(`21: ${name} flags disqualified from that one value`,
       /if \(gateOnSellTo\) \{\s*formState\.disqualified = true;/.test(f), name);
    ok(`21: ${name} chooses the step from that same one value`,
       /if \(gateOnSellTo\) showStep\('step-disqualified'\);/.test(f), name);
    /* No stray copy of the raw condition left behind. If one survives,
       half the decision is still gating CRM. */
    eq(`21: ${name} has no leftover raw B2C condition`,
       (f.match(/sellTo === 'B2C' \|\| sellTo === 'Mixed'/g) || []).length, 1);
    /* ── PROGRESSIVE REVEAL, AND WHY IT IS SAFE ─────────────────
       The question appears only once sell-to is answered. That is a
       layout decision, but it would have been a correctness bug if the
       B2C gate fired on SELECTION rather than at the Next click: a B2C
       click would jump to the DQ step before the checkboxes existed and
       the CRM exception could never apply.

       It fires at Next. These three assertions are what keep it there. */
    ok(`21: ${name} reveals the question only once sell-to is answered`,
       /function syncNeedsVisibility\(\)/.test(f)
       /* The MECHANISM is section 26's business, not this one's -- pinning
          the literal here is what let the '' reveal bug sit green, and it
          broke again when the reveal moved to a class toggle. This asserts
          only what this section is about: the reveal is gated on `chosen`. */
       && /setWrapHidden\(wrap, !chosen\);/.test(f), name);
    ok(`21: ${name} only demands an answer once the question is visible`,
       /needsAsked\(\) && needsVisible\(\) && !selectedNeeds\(\)\.length/.test(f), name);
    /* THE GATE STAYS AT THE NEXT CLICK. The sell-to change handler may
       only show and hide -- if it ever calls showStep or touches
       disqualified, the reveal has become a verdict. */
    {
      const handler = (f.match(/function syncNeedsVisibility\(\)[\s\S]*?\n    \}/) || [''])[0];
      ok(`21: ${name} the sell-to handler never decides anything`,
         !/showStep|disqualified|savePartial/.test(handler), name);
    }
    /* The STATEMENT, not the string -- the comment above it names the
       step too, and counting mentions would make prose a test failure. */
    ok(`21: ${name} the DQ step is still reached from exactly one place`,
       (f.match(/if \(gateOnSellTo\) showStep\('step-disqualified'\);/g) || []).length === 1, name);

    /* about_business follows the CRM tick, and is cleared on untick --
       the ELEMENT, not just the state, because it is read from the DOM
       at step 2 and a hidden populated textarea would still submit. */
    ok(`21: ${name} clears the about-business element on untick`,
       /function syncAboutBusiness\(\)/.test(f)
       && /if \(el\) el\.value = '';/.test(f)
       && /formState\.about_business = '';/.test(f), name);
    ok(`21: ${name} does nothing where there is no wrapper`,
       /var wrap = document\.getElementById\('about-business-wrap'\);\s*\n\s*if \(!wrap\) return;/.test(f), name);

    /* Normalised the same way resolveProduct normalises, so /ai-demo/
       and /AI-Demo cannot disagree with the server about what they are. */
    ok(`21: ${name} lowercases and strips the trailing slash`,
       /toLowerCase\(\)\.replace\(\/\\\/\+\$\/, ''\)/.test(f), name);
  }

  /* THE SERVER HALF. A CRM lead who answers B2C is no longer
     disqualified, so the two sell_to predicates would drop them
     silently -- present in every headline number, absent from the one
     surface anybody acts on. Both, and they move together. */
  const sdr     = between("app.get('/monitor/sdr'", "app.get('/monitor'");
  const metrics = between("app.get('/monitor/metrics'", "app.get('/monitor/funnel'");
  ok('21: the SDR list keeps CRM B2C leads',
     /\(l\.sell_to ILIKE 'B2B%' OR l\.product = 'crm'\)/.test(sdr));
  ok('21: the No booking yet card keeps them too',
     /\(sell_to ILIKE 'B2B%' OR product = 'crm'\)/.test(metrics));
  /* PINNED AS A PAIR. The card is the COUNT of the list, so one moving
     without the other is a number that disagrees with the page it links
     to -- the SDR_SEARCH_COLUMNS / SDR_SEARCH_FIELDS lesson again. */
  eq('21: exactly two sell_to B2B predicates exist, and both are excepted',
     (src.match(/sell_to ILIKE 'B2B%'/g) || []).length,
     (src.match(/sell_to ILIKE 'B2B%' OR l?\.?product = 'crm'/g) || []).length);
}

/* ============================================================
   22. ONE ROW BUILDER, TWO TABLES, AND THE IDS MUST NOT COLLIDE

   leadRowsHtml renders All Leads AND Blocked. showTab toggles a class
   and never clears a panel, so both tables are in the document at the
   same time -- which means an unnamespaced row id exists twice for any
   lead that is blocked and also on the loaded All Leads page, and
   getElementById silently returns the All Leads copy.

   The symptom is a click that does nothing: no throw, no error painted,
   a perfectly rendered row. The behavioural half of this lives in
   test-non-icp-routes.js, which evaluates the dashboard and compares the
   ids the two tables emit. This half is the one that catches a NEW
   caller added without a namespace.
   ============================================================ */
{
  const dash = between("'function leadRowsHtml(leads,ns)", "'function renderPag(");

  ok('22: leadRowsHtml takes a namespace', /function leadRowsHtml\(leads,ns\)/.test(src));
  ok('22: the row key is built from it', /key=esc\(ns\|\|"x"\)\+"-"\+sid/.test(src));

  /* EVERY caller passes one. A bare leadRowsHtml(x) is the collision
     coming back, and it reads as perfectly ordinary code. */
  const calls = [...src.matchAll(/leadRowsHtml\(([^)]*)\)/g)]
    .map((m) => m[1])
    .filter((a) => !/^leads,ns$/.test(a));          // the declaration itself
  ok('22: leadRowsHtml has callers', calls.length >= 2, JSON.stringify(calls));
  for (const a of calls) {
    ok(`22: caller passes a namespace — leadRowsHtml(${a})`, /,\s*"[a-z]+"\s*$/.test(a), a);
  }
  /* And the namespaces are DISTINCT. Two callers both passing "l" is
     the same bug with extra steps. */
  const namespaces = calls.map((a) => (a.match(/,\s*"([a-z]+)"\s*$/) || [])[1]).filter(Boolean);
  eq('22: every caller uses a different namespace',
     namespaces.length, new Set(namespaces).size);

  /* The key addresses the DOM, the session id addresses the lead, and
     they are separate arguments now. Collapsing them back would both
     re-collide the ids and send a namespaced key to
     /monitor/lead-changes as a session_id. */
  ok('22: toggleRow takes a key AND a session id',
     /function toggleRow\(key,sid\)/.test(src));
  ok('22: loadChanges is addressed by the key and fetches by the session id',
     /function loadChanges\(key,sid\)/.test(src)
     && /getElementById\("lc-"\+key\)/.test(src)
     && /session_id="\+encodeURIComponent\(sid\)/.test(src));
  /* The Model tab emits its own er- ids under a third prefix. It has no
     change-log div, so it deliberately passes no session id -- stated
     by the call rather than left to a getElementById miss. */
  ok('22: the Model tab uses its own row prefix',
     /toggleRow\(\\'md-"\+sid\+"\\'\)/.test(src));
}

/* ============================================================
   23. OUR OWN TEST SUBMISSIONS ARE MARKED, NEVER SILENTLY EXCLUDED

   Four of the ten non-ICP blocks on 15 Sept 2026 were Darshil testing
   the walkthrough as agent@allstate.com, which makes "10 blocked" a
   number nobody can quote.

   CLAUDE.md is explicit that internal addresses are counted in every
   leads number today, that this is a known distortion nobody chose, and
   that excluding them would move every historical number at once. So
   the fix adds a flag, a marker, an opt-in filter and a figure printed
   BESIDE the count -- and changes no total. This section is what stops
   that turning into a quiet subtraction later.
   ============================================================ */
{
  const M = require('module');
  ok('23: the internal list is an explicit constant',
     /const INTERNAL_TEST_EMAILS = \[/.test(src));
  ok('23: it names the address that caused this, with a dated reason',
     /'agent@allstate\.com',\s*\/\/[^\n]*Sept 2026/.test(src));
  ok('23: it is extensible from the environment without a deploy',
     /process\.env\.INTERNAL_TEST_EMAILS/.test(src));
  /* NOT inferred from the name. "Darshil Test" is a tempting signal and
     a wrong one: a real prospect may be called Darshil, and the block
     list already contains allstate.com because it is a real brokerage. */
  /* SCOPED TO THE FUNCTION BODY, NOT A BYTE WINDOW FROM THE IDENTIFIER.
     The window version read "isInternalLead followed within 400 chars by
     first_name" and broke on 19 Sept 2026 the moment a CALLER was added
     whose next statement was pushToSalesforce({first_name,...}). Nothing
     had started inferring anything; the assertion was measuring distance
     in the file. Same failure mode as the ordering assertions documented
     in CLAUDE.md -- a byte window is not a scope. */
  const internalFnBody = (() => {
    const i = src.indexOf('function isInternalLead(email) {');
    return i === -1 ? '' : src.slice(i, src.indexOf('\n}', i) + 2);
  })();
  ok('23: isInternalLead is liftable for the name check', internalFnBody.length > 0);
  ok('23: test-ness is never inferred from a person name',
     !/first_name[^\n]{0,80}[Tt]est['"]/.test(src)
     && !/first_name|last_name/.test(internalFnBody));

  const fn = between('function isInternalLead(email)', 'function internalLeadSqlClause');
  ok('23: it matches on the whole address, lowercased', /trim\(\)\.toLowerCase\(\)/.test(fn));
  ok('23: and still covers the existing test DOMAINS',
     /ELV_EXCLUDED_DOMAINS\.includes/.test(fn));

  /* ONE DEFINITION, TWO CONSUMERS. The SQL clause exists because paging
     happens in the database; it must be built from the same constants,
     not a retyped list. */
  const sql = between('function internalLeadSqlClause(emailCol, pageCol, params)', '\n/* ── Rule (a)');
  ok('23: the SQL clause is built from the same two constants',
     /params\.push\(INTERNAL_TEST_EMAILS\)/.test(sql) && /params\.push\(ELV_EXCLUDED_DOMAINS\)/.test(sql));
  /* An env var must never be interpolated into query text. */
  ok('23: the values go through bound parameters, never string interpolation',
     /ANY\(\$\$\{a\}::text\[\]\)/.test(sql) && !/\$\{INTERNAL_TEST_EMAILS/.test(sql));
  /* THE STAGING ARM, ADDED 19 SEPT 2026. It must be an EXACT host match:
     a LIKE '%gushwork.webflow.io%' would make
     gushwork.webflow.io.evil.com read as our own staging site. */
  ok('23: the SQL clause carries the staging arm, from the same constant',
     /params\.push\(INTERNAL_STAGING_HOSTS\)/.test(sql));
  ok('23: the staging arm matches the HOST exactly, never a substring',
     /SPLIT_PART\(SPLIT_PART\(COALESCE\(\$\{pageCol\}/.test(sql) && !/ILIKE/.test(sql) && !/LIKE '%/.test(sql));
  /* NULL-SAFE, and this is a correctness bug not a tidy-up.

     SPLIT_PART(NULL, ...) is NULL, so on a lead with no page_url the arm
     was NULL rather than false. Postgres is three-valued: the whole OR
     then goes NULL, and NOT(NULL) is NULL as well -- so such a row
     matched neither the clause NOR its negation and fell out of both
     halves of any filter built on the pair.

     Caught on 22 Sept 2026 by adding up the two halves of the All Leads
     Meta filter against production: 869 withheld + 4706 sent = 5575
     against a population of 5582. Ten rows carry a null page_url.

     The JS has always answered false here -- isStagingSubmission coerces
     null to '' -- so the two renderings of this one rule were
     disagreeing, which is the exact drift sharing a definition is for. */
  ok('23: the staging arm reads a NULL page_url as false, not as NULL',
     /COALESCE\(\$\{pageCol\}, ''\)/.test(sql));
  /* The JS half of this pair is executed in section 28, where
     isStagingSubmission is already lifted. */

  /* EVERY CONSUMER ASKS THE SAME QUESTION.

     isInternalLead reads the ADDRESS only; isInternalSubmission also
     matches the staging host. Anything deciding "is this one of ours"
     must use the second, or it silently disagrees with the rest of the
     repo -- which nonIcpModelReport did until 22 Sept 2026, counting a
     staging submission under a personal Gmail as an ordinary prospect
     while Meta, Salesforce, the dialer and the All Leads marker all
     treated it as ours.

     Anchored on the model report specifically, because that is the one
     that drifted and the drift was invisible: both spellings compile,
     both return a boolean, and the weaker one is simply wrong less
     often. 19 of 5,582 rows separate them. */
  ok('23: the Model tab report asks the SAME question as every other consumer',
     /const mine = isInternalSubmission\(lead\.email, lead\.page_url\);/.test(src),
     'nonIcpModelReport must use isInternalSubmission, not isInternalLead');
  ok('23: and it selects the column that question needs',
     /SELECT[\s\S]{0,400}l\.page_url,[\s\S]{0,400}FROM leads l\s+WHERE l\.created_at/.test(src));

  /* THE DEFAULT IS STILL "COUNT EVERYTHING". A default that excluded
     would be the quiet fix CLAUDE.md forbids. */
  const leads = between("app.get('/monitor/leads'", "app.get('/monitor/lead-changes'");
  ok('23: the filter is opt-in, defaulting to null',
     /const internal\s+= req\.query\.internal\s+\|\| null;/.test(leads));
  ok('23: both directions are offered', /internal === 'exclude'/.test(leads) && /internal === 'only'/.test(leads));
  ok('23: no query filters on it unless asked',
     !/conditions\.push\([^)]*internalLeadSqlClause[^)]*\);\s*\n\s*(?!.*internal ===)/.test(leads));

  /* The per-row flag is computed in JS, beside unverifiable_pair, and
     for a stated reason: baseSelect is built before the count query and
     they share a params array, so a clause pushing parameters from the
     SELECT list leaves the count bound to parameters it never uses. */
  ok('23: the row flag is computed in JS, not in the SELECT list',
     /is_internal: isInternalSubmission\(r\.email, r\.page_url\)/.test(leads)
     && !/AS is_internal/.test(leads));
  ok('23: the CSV carries it from the same function',
     /'is_internal'/.test(leads) && /c === 'is_internal'\s*\? isInternalSubmission\(r\.email, r\.page_url\)/.test(leads));
  /* BOTH SURFACES ASK THE SAME QUESTION. The marker and the outbound
     guards drifting apart would show a lead as ours while still pushing
     it, or the reverse -- which is how the staging gap stayed invisible:
     Meta was suppressed by address and the dashboard agreed, so nobody
     saw that the PAGE was never consulted by either. */
  ok('23: the dashboard marker uses the same predicate as the outbound guards',
     /is_internal: isInternalSubmission\(/.test(leads)
     && /function isInternalSubmission\(email, page_url\)/.test(src));

  /* The Model tab counts ours in PARALLEL and never subtracts, so the
     ladder still sums to the lead total. */
  const rep = between('async function nonIcpModelReport', "app.get('/monitor/non-icp'");
  ok('23: the ladder counts ours alongside rather than removing them',
     /const internalIn = \{/.test(rep) && /ladder\[bucket\]\+\+;/.test(rep) && /if \(mine\) internalIn\[bucket\]\+\+;/.test(rep));
  ok('23: every decision row says whether it is ours', /is_internal: mine/.test(rep));
}

/* ============================================================
   24. NOTHING OUTSIDE THE SALESFORCE PICKLIST CAN EVER BE PUSHED

   Product__c is a RESTRICTED picklist carrying exactly three values --
   aeo, crm and aeo,crm -- added and verified against the live org on
   15 Sept 2026. An unknown value there returns
   INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST, and sfUnknownFields retries
   only INVALID_FIELD and INVALID_FIELD_FOR_INSERT_UPDATE. So a bad
   value does not cost us the field, it costs us THE WHOLE LEAD, with a
   critical alert saying "This lead is NOT in Salesforce. Add it
   manually."

   product_interest is the only thing that reaches that field which
   originates in a form body, so the canonicaliser is the only thing
   standing between a tampered checkbox and a lost lead. A mutation that
   dropped its allow-list filter SURVIVED the whole suite before this
   section existed.
   ============================================================ */
{
  const capi = require('../meta-capi');
  const canonicalProductInterest = capi.canonicalProductInterest;
  const PRODUCT_INTEREST_SLUGS = capi.PRODUCT_INTEREST_SLUGS;

  /* What Salesforce will accept. Written out rather than derived, so
     adding a product without adding its picklist value fails HERE
     rather than in production on the first both-ticked lead. */
  const SF_PRODUCT_PICKLIST = ['aeo', 'crm', 'aeo,crm'];

  eq('24: the slug vocabulary is exactly the catalogue keys',
     PRODUCT_INTEREST_SLUGS.slice().sort(), ['aeo', 'crm']);

  /* EVERY non-empty sorted subset of the slugs must be in the picklist.
     This is what breaks the day a third product is added: the new
     combinations will not be there, and Salesforce would reject them. */
  const subsets = [];
  const S2 = PRODUCT_INTEREST_SLUGS.slice().sort();
  for (let mask = 1; mask < (1 << S2.length); mask++) {
    subsets.push(S2.filter((_, i) => mask & (1 << i)).join(','));
  }
  for (const v of subsets) {
    ok(`24: "${v}" is a value Salesforce will accept`, SF_PRODUCT_PICKLIST.includes(v), v);
  }
  eq('24: and the picklist has no values the form cannot produce',
     SF_PRODUCT_PICKLIST.slice().sort(), subsets.slice().sort());

  /* ADVERSARIAL INPUT, EXECUTED. The field is a form body: anything can
     arrive in it. Every one of these must come back null or a picklist
     value -- never a passthrough. */
  const hostile = [
    'crm; DROP TABLE leads', 'aeo,crm,enterprise', 'ENTERPRISE', 'crm,,,', ',',
    'aeo crm', 'crm\naeo', '  CRM  ', 'aeo,CRM', 'crm,aeo', ['crm', 'enterprise'],
    ['<script>'], 'null', 'undefined', '0', 0, 1, true, false, {}, [], null, undefined,
    'a'.repeat(500), 'aeo,'.repeat(50) + 'crm',
  ];
  for (const h of hostile) {
    let out;
    try { out = canonicalProductInterest(h); } catch (e) { out = 'THREW: ' + e.message; }
    const safe = out === null || SF_PRODUCT_PICKLIST.includes(out);
    /* String(): JSON.stringify(undefined) is undefined, not a string. */
    ok(`24: hostile input cannot reach the picklist — ${String(JSON.stringify(h)).slice(0, 34)}`,
       safe, 'got ' + JSON.stringify(out));
  }

  /* And the guarantee stated directly: the ONLY values this function can
     ever return are null or a picklist member. */
  const seen = new Set(hostile.concat(subsets).concat(['aeo', 'crm'])
    .map((h) => { try { return canonicalProductInterest(h); } catch { return 'THREW'; } }));
  const bad = [...seen].filter((v) => v !== null && !SF_PRODUCT_PICKLIST.includes(v));
  eq('24: the canonicaliser emits nothing outside the picklist', bad, []);
}

/* ============================================================
   25. A REVEAL MUST NAME A DISPLAY VALUE, NEVER ''

   syncAboutBusiness shipped in PR 75 as:

       wrap.style.display = show ? '' : 'none';

   and could not have revealed anything on /demo. #about-business-wrap is
   hidden by a CLASS (.field-wrapper.about-biz-wrap { display:none }), and
   '' does not set display to its default -- it REMOVES the inline
   declaration. There was never one, so the class kept winning and the
   textarea stayed hidden for every AI-CRM lead. syncNeedsVisibility was
   written the same way and would have failed the same way.

   IT IS A CLASS BECAUSE WEBFLOW GIVES NO CHOICE. The Designer converts an
   inline style into a generated combo class, and the Data API rejects a
   style attribute outright -- both confirmed by attempt on 15 Sept 2026.
   So every wrapper this code reveals is hidden by a class, always, and ''
   can never reveal any of them.

   WHY THIS IS A LINT AND NOT A DRIVEN TEST. The bug lives in the
   interaction between our JavaScript and CSS that is not in this repo at
   all -- it is in Webflow. A stubbed DOM has no stylesheet, so
   style.display = '' followed by reading style.display returns '' and
   every behavioural assertion passes. There is no fixture that makes this
   visible: the suite cannot see the rule that wins. Same ceiling as the
   SQL lint in section 13 -- a source assertion cannot tell you whether a
   query parses, and no DOM test here can tell you which rule applied.

   So this bans the SHAPE. Any empty-string display assignment in either
   form file fails, whatever it is called and whoever writes it next.
   ============================================================ */
{
  const FORM_FILES = ['gushwork-form.js', 'gushwork-form-popup.js'];

  for (const file of FORM_FILES) {
    const text = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
    const lines = text.split('\n');

    /* Any assignment to .style.display whose value is an empty string --
       plain (= '') or either branch of a ternary (? '' : / : ''). Quotes
       both ways, because a future edit may not match the house style. */
    const offenders = [];
    lines.forEach((line, i) => {
      if (!/\.style\.display\s*=/.test(line)) return;
      const rhs = line.slice(line.indexOf('.style.display') + '.style.display'.length)
        .replace(/^\s*=/, '');
      if (/(^|[?:]\s*)(''|"")\s*(:|;|$)/.test(rhs.trim())) {
        offenders.push(`${file}:${i + 1}: ${line.trim().slice(0, 80)}`);
      }
    });
    eq(`25: ${file} never reveals with an empty display string`, offenders, []);
  }

  /* And the two reveals specifically, by name, so deleting the functions
     cannot quietly satisfy the lint above. Each must route through
     setWrapHidden -- see section 26 for why the mechanism is a class. */
  const REVEALS = [
    ['syncAboutBusiness', 'about-business-wrap'],
    ['syncNeedsVisibility', 'needs-wrap'],
  ];
  for (const file of FORM_FILES) {
    const text = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
    for (const [fn, domId] of REVEALS) {
      const start = text.indexOf(`function ${fn}(`);
      ok(`25: ${file} declares ${fn}`, start !== -1, fn);
      if (start === -1) continue;
      /* The function body, to its closing brace at the same indent. */
      const body = text.slice(start, start + 2000);
      /* The reveal goes through setWrapHidden and NOTHING in these two
         functions touches style.display directly any more. Both halves
         matter: the first that the mechanism is the class, the second
         that no one has quietly re-added an inline override beside it. */
      ok(`25: ${fn} reveals via setWrapHidden`,
         /setWrapHidden\(wrap, !\w+\);/.test(body), fn);
      ok(`25: ${fn} does not touch style.display directly`,
         !/\.style\.display\s*=/.test(body), fn);
      /* It must still be the right element -- a reveal pointed at the
         wrong wrapper would pass everything above. */
      ok(`25: ${fn} still targets #${domId}`,
         body.indexOf(`'${domId}'`) !== -1, domId);
    }
  }
}

/* ============================================================
   26. THE REVEAL IS A CLASS TOGGLE, AND BOTH FILES AGREE ON THE CLASS

   The question did not appear on staging. Every static layer checked
   out -- markup, ids, served JS, CSS -- because the mechanism itself was
   wrong: Webflow cannot store an inline style, so both wrappers are
   hidden by a CLASS, and driving them through style.display meant this
   code and the page disagreed about what "hidden" even was.

   The class is now the mechanism. That makes the reveal animatable
   (display does not transition; opacity and transform do) and it makes
   needsVisible correct from first paint rather than only after init.

   THE CEILING, STATED: no test in this repo can see Webflow's CSS. This
   pins the literal in both form files and pins that they agree with each
   other -- it CANNOT prove the published stylesheet uses the same name.
   If is-hidden is renamed in Webflow, these assertions stay green and
   the field stops revealing. That is the same gap as every other
   Webflow-coupled value here, and naming it is the best available.
   ============================================================ */
{
  const FORM_FILES = ['gushwork-form.js', 'gushwork-form-popup.js'];
  const lifted = {};

  for (const file of FORM_FILES) {
    const text = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
    lifted[file] = text;

    const m = text.match(/const HIDDEN_CLASS = '([^']+)';/);
    ok(`26: ${file} declares HIDDEN_CLASS`, !!m, file);
    eq(`26: ${file} HIDDEN_CLASS is the class the markup carries`,
       m && m[1], 'is-hidden');

    /* The toggle must use the CONSTANT. A literal here would be a second
       copy of the name, which is how the three sync pairs in CLAUDE.md
       each drifted. */
    ok(`26: ${file} toggles the constant, not a literal`,
       /classList\.toggle\(HIDDEN_CLASS, hidden\)/.test(text), file);
    ok(`26: ${file} needsVisible reads the class`,
       /!wrap\.classList\.contains\(HIDDEN_CLASS\)/.test(text), file);

    /* The dead selector is gone from CODE. A comment naming it is the
       point -- it records why ids are the lookup. */
    const codeHits = text.split('\n').filter((l) => {
      const t = l.trim();
      return l.includes('name="sell-to"') && !t.startsWith('*') && !t.startsWith('/*');
    });
    eq(`26: ${file} has no code keyed on the dead sell-to name`, codeHits, []);

    /* Sell-to is addressed by id, and the listener uses the same set the
       reader does -- otherwise the handler binds to radios whose state
       nothing reads. */
    ok(`26: ${file} listener uses SELL_TO_SELECTOR`,
       /querySelectorAll\(SELL_TO_SELECTOR\)/.test(text), file);
    const ids = text.match(/const SELL_TO_IDS = \[([^\]]+)\]/);
    const sel = text.match(/const SELL_TO_SELECTOR = '([^']+)'/);
    ok(`26: ${file} declares both SELL_TO_IDS and SELL_TO_SELECTOR`, !!ids && !!sel, file);
    if (ids && sel) {
      const fromIds = ids[1].split(',').map((x) => '#' + x.trim().replace(/'/g, '')).join(', ');
      eq(`26: ${file} selector and id list are the same set`, sel[1], fromIds);
    }
  }

  /* The two forked files must agree on the class, or a fix lands on half
     the traffic -- the fork hazard CLAUDE.md opens with. */
  const cls = FORM_FILES.map((f) => (lifted[f].match(/const HIDDEN_CLASS = '([^']+)';/) || [])[1]);
  eq('26: both form files agree on HIDDEN_CLASS', cls[0], cls[1]);

  /* ── EXECUTED, not read ──────────────────────────────────────
     Lifting setWrapHidden and driving it is the only part of this
     section that can tell you the toggle actually works. The stub mirrors
     the published shape: a wrapper whose hidden state is a class, with no
     inline style, exactly as Webflow emits it. */
  {
    const text = lifted['gushwork-form.js'];
    const i = text.indexOf('function setWrapHidden(');
    ok('26: setWrapHidden is liftable', i !== -1);
    let depth = 0, end = -1;
    for (let k = text.indexOf('{', i); k < text.length; k++) {
      if (text[k] === '{') depth++;
      else if (text[k] === '}') { depth--; if (depth === 0) { end = k + 1; break; } }
    }
    const src = `const HIDDEN_CLASS = 'is-hidden';\n` + text.slice(i, end) +
                '\nreturn setWrapHidden;';
    const setWrapHidden = new Function('getComputedStyle', src)(undefined);

    const mk = (classes) => {
      const set = new Set(classes);
      return {
        style: { display: '', removeProperty(k) { if (k === 'display') this.display = ''; } },
        classList: {
          contains: (c) => set.has(c),
          toggle: (c, force) => { if (force) set.add(c); else set.delete(c); },
        },
        has: (c) => set.has(c),
      };
    };

    const w = mk(['field-wrapper', 'is-hidden']);
    ok('26: starts hidden, as the markup ships it', w.has('is-hidden'));
    setWrapHidden(w, false);
    ok('26: revealing removes the class', !w.has('is-hidden'));
    setWrapHidden(w, true);
    ok('26: hiding restores the class', w.has('is-hidden'));

    /* The migration guard: a legacy inline display:none must not survive
       a reveal, or the class toggle is decorative and the field stays
       shut -- which is the bug this whole section exists for. */
    const legacy = mk(['field-wrapper', 'is-hidden']);
    legacy.style.display = 'none';
    setWrapHidden(legacy, false);
    ok('26: a legacy inline display is cleared on reveal',
       legacy.style.display === '' && !legacy.has('is-hidden'),
       JSON.stringify(legacy.style.display));

    /* And it must not invent an inline style when there was none --
       that would defeat the CSS transition the class exists to enable. */
    const clean = mk(['field-wrapper', 'is-hidden']);
    setWrapHidden(clean, false);
    eq('26: no inline display is introduced on a clean reveal', clean.style.display, '');
  }
}

/* ============================================================
   27. THE CAMPAIGN DECIDES THE OFFER — THIRD COPY, AND EXECUTED

   Until 17 Sept 2026 the pathname decided which offer a visitor saw, so
   a CRM ad landing anywhere but /ai-demo sold AEO. The campaign decides
   now, and the rule therefore exists in THREE places:

     meta-capi.js            campaignOffer   the server's resolveProduct
     gushwork-form.js        campaignOffer   what /demo shows
     gushwork-form-popup.js  campaignOffer   the ad landers

   Same shape as B2C_ALLOWED_PATHS in section 21 and for the same
   reason: three copies of one rule, no shared module, and a drift means
   the offer on screen disagrees with the product we store and report.

   EXECUTED, NOT READ. A source assertion cannot tell a reachable branch
   from an `if (false)`, which is the lesson this repo has now learned
   three times, so each copy is lifted and CALLED against the same
   table. The fixtures are real campaigns out of leads.utm_campaign,
   with their real utm_medium.
   ============================================================ */
{
  const META  = require('../meta-capi');
  /* Brace-matched rather than regexed to a closing line: these sit at
     four-space indent inside the form files' IIFE, and a lazy match
     would stop at the first nested closing brace. */
  const liftFunction = (fsrc, name) => {
    const m = new RegExp('\\n    (?:async )?function ' + name + '\\s*\\(').exec(fsrc);
    if (!m) throw new Error('function not found: ' + name);
    let d = 0;
    for (let j = fsrc.indexOf('{', m.index); j < fsrc.length; j++) {
      if (fsrc[j] === '{') d++;
      else if (fsrc[j] === '}') { d--; if (!d) return fsrc.slice(m.index, j + 1); }
    }
    throw new Error('unbalanced braces in: ' + name);
  };
  const demo  = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form.js'), 'utf8');
  const popup = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form-popup.js'), 'utf8');

  const liftOffer = (src, tag) => {
    /* Each PIECE must be present. Checked before joining, because the
       lifted function legitimately contains blank lines and testing the
       joined text for them would fail on correct input. A missing piece
       would otherwise surface as a confusing ReferenceError from inside
       new Function rather than as a named assertion. */
    const parts = [
      (/const OFFER_CRM\s+= '[^']*';/.exec(src) || [''])[0],
      (/const OFFER_AEO\s+= '[^']*';/.exec(src) || [''])[0],
      (/const OFFER_SELECTOR\s+= '[^']*';/.exec(src) || [''])[0],
      (/const OFFER_AD_MEDIUMS = \[[^\]]*\];/.exec(src) || [''])[0],
      liftFunction(src, 'campaignOffer'),
    ];
    ok(`27: ${tag} carries the whole rule`, parts.every((x) => x && x.length > 0),
       parts.map((x, i) => (x ? '' : 'piece ' + i + ' missing')).filter(Boolean).join(', '));
    return new Function(parts.concat('return campaignOffer;').join('\n'))();
  };

  const impls = [
    ['meta-capi', (c, m) => META.campaignOffer(c, m)],
    ['form',      liftOffer(demo,  'form')],
    ['form-popup',liftOffer(popup, 'form-popup')],
  ];

  /* Every row is a campaign that has actually arrived, with the medium
     it actually arrived on, and the answer it must get. */
  const CASES = [
    ['FLI__Prospecting__AudienceTesting__CBO__StartTrial', 'paid',  'aeo',      'the biggest Meta campaign, 1282 leads'],
    ['FLI__Prospecting__CRM-Offer__CBO__StartTrial',       'paid',  'crm',      'the one CRM campaign, 39 leads'],
    ['UR_G_S_US_BR_Brand-tIS',                             'cpc',   'selector', 'Google brand, and BR is UPPERCASE in the real value'],
    ['Search_US_Brand_tImp-Share',                         'cpc',   'selector', 'the other brand campaign'],
    ['UR_G_PMAX_US_NB_Prospecting',                        'cpc',   'aeo',      'Google non-brand'],
    ['120241181781830373',                                 'paid',  'selector', 'a bare Meta campaign ID, 17 leads'],
    ['{{campaign.name}}',                                  'paid',  'selector', 'an unrendered macro, 6 leads'],
    ['gushwork',                                           'social','selector', 'a LinkedIn post, not an ad'],
    ['lead_estimator',                                     'drip',  'selector', 'our own drip email'],
    ['ai-agents-cta',                                      'email', 'selector', 'an email-signature link'],
    ['footer-backlink',                                    'referral','selector','a backlink from another site'],
    ['',                                                   '',      'selector', 'direct or organic'],
    [null,                                                 null,    'selector', 'nothing captured at all'],
    /* THE TRAP. "br" as a substring would make this a selector campaign
       and nobody would ever see why -- the identical bug that routed
       "client", "link" and the name "Jolian" to LinkedIn through
       Source_Bucket__c. Broad targeting is standard Meta naming. */
    ['FLI__Prospecting__Broad__CBO',                       'paid',  'aeo',      'Broad must NOT read as brand'],
    /* CRM outranks brand, matching the order in the request. */
    ['UR_G_S_US_BR_Brand-CRM-Offer',                       'cpc',   'crm',      'crm is tested before brand'],
  ];

  for (const [name, fn] of impls)
    for (const [campaign, medium, want, why] of CASES)
      eq(`27: ${name}: ${JSON.stringify(campaign)} @ ${JSON.stringify(medium)} -> ${want}  (${why})`,
         fn(campaign, medium), want);

  /* THE THREE MUST AGREE ON EVERY ROW, which is the point of the
     section. Asserted separately so a drift reads as a drift rather
     than as three unrelated failures. */
  for (const [campaign, medium] of CASES) {
    const answers = impls.map(([, fn]) => fn(campaign, medium));
    ok(`27: all three copies agree on ${JSON.stringify(campaign)}`,
       answers.every((a) => a === answers[0]), answers.join(' / '));
  }

  /* ── THE SERVER'S PRECEDENCE ───────────────────────────────────
     campaignOffer only answers "what were they sold". resolveProduct
     has to rank it, and the ranking is the part that can quietly go
     wrong in both directions. */
  eq('27: a CRM campaign on /demo stores crm',
     META.resolveProduct({ page_url: 'https://www.gushwork.ai/demo', utm_campaign: 'CRM-Offer', utm_medium: 'paid' }), 'crm');
  /* THE PAGE THAT SELLS CRM OUTRANKS AN AEO CAMPAIGN. Letting the
     campaign win here would demote a real /ai-demo lead to aeo and book
     them with the wrong team, on the one page that was never ambiguous. */
  eq('27: an AEO campaign on /ai-demo still stores crm',
     META.resolveProduct({ page_url: 'https://www.gushwork.ai/ai-demo', utm_campaign: 'TOF', utm_medium: 'paid' }), 'crm');
  /* THE PAGE-WINS GUARD IS INVISIBLE ON TODAY'S CATALOGUE, and that is
     precisely why it needs this test. PRODUCT_PATHS has exactly one
     entry, /ai-demo -> crm, and the only campaign answer that can
     override anything is also crm -- so the two agree and removing the
     guard changes no result. Measured: mutating it away survived the
     entire bar.

     It stops being invisible the moment a second product page exists,
     and at that point a CRM campaign would silently re-tag every
     visitor to that page. So the catalogue is given a temporary second
     entry here and taken away again. */
  {
    const PP = META.PRODUCT_PATHS;
    const had = Object.prototype.hasOwnProperty.call(PP, '/seo-only-demo');
    PP['/seo-only-demo'] = 'aeo';
    try {
      eq('27: a page that names its product beats a CRM campaign',
         META.resolveProduct({ page_url: '/seo-only-demo', utm_campaign: 'CRM-Offer', utm_medium: 'paid' }), 'aeo');
      eq('27: and the event slug agrees with it',
         META.resolveEventProduct({ page_url: '/seo-only-demo', utm_campaign: 'CRM-Offer', utm_medium: 'paid' }), 'aeo');
      /* The campaign still decides on a page with no entry, so this is
         not just "the campaign never wins". */
      eq('27: the campaign still decides on an unmapped page',
         META.resolveProduct({ page_url: '/some-lander', utm_campaign: 'CRM-Offer', utm_medium: 'paid' }), 'crm');
    } finally {
      if (!had) delete PP['/seo-only-demo'];
    }
  }

  /* WHAT THEY TICKED STILL OUTRANKS THE AD. The ad is a guess about
     them; the checkbox is them. */
  eq('27: an AEO tick beats a CRM campaign',
     META.resolveProduct({ page_url: '/demo', product_interest: 'aeo', utm_campaign: 'CRM-Offer', utm_medium: 'paid' }), 'aeo');
  /* AN UNREADABLE PAGE IS STILL UNTAGGED. "We could not tell which page
     this was" must not be rescued into a product by the campaign -- the
     null is the honest report and a test already pins it for the
     no-campaign case. */
  eq('27: an unreadable page stays null even with a CRM campaign',
     META.resolveProduct({ page_url: 'not a url', utm_campaign: 'CRM-Offer', utm_medium: 'paid' }), null);
  /* AND THE EVENT SLUG MOVES WITH IT. The column and the Meta event
     disagreeing is the failure the ticked-selection fix closed on 15
     Sept; the campaign is the same hazard one input later. */
  eq('27: the event slug follows the campaign too',
     META.resolveEventProduct({ page_url: '/demo', utm_campaign: 'CRM-Offer', utm_medium: 'paid' }), 'crm');
  eq('27: the event slug still honours a both-ticked selection',
     META.resolveEventProduct({ page_url: '/demo', product_interest: 'aeo,crm', utm_campaign: 'CRM-Offer', utm_medium: 'paid' }), 'aeo,crm');

  /* ── THE SELECTOR ACTUALLY HIDES ───────────────────────────────
     syncNeedsVisibility is where the decision lands. Driven, because an
     ordering or presence assertion here would survive the condition
     being dropped entirely. */
  for (const [tag, src] of [['form', demo], ['form-popup', popup]]) {
    const mk = () => {
      const cls = new Set(['field-wrapper', 'is-collapsible', 'is-hidden']);
      return { classList: { toggle: (c, on) => (on ? cls.add(c) : cls.delete(c)), contains: (c) => cls.has(c) },
               style: {}, has: (c) => cls.has(c) };
    };
    const run = (campaign, medium) => {
      const wrap = mk();
      const body = [
        (/const HIDDEN_CLASS = '[^']*';/.exec(src) || [''])[0],
        (/const OFFER_CRM\s+= '[^']*';/.exec(src) || [''])[0],
        (/const OFFER_AEO\s+= '[^']*';/.exec(src) || [''])[0],
        (/const OFFER_SELECTOR\s+= '[^']*';/.exec(src) || [''])[0],
        (/const OFFER_AD_MEDIUMS = \[[^\]]*\];/.exec(src) || [''])[0],
        liftFunction(src, 'campaignOffer'),
        liftFunction(src, 'currentOffer'),
        liftFunction(src, 'offerIsSelector'),
        liftFunction(src, 'setWrapHidden'),
        liftFunction(src, 'syncNeedsVisibility'),
        'syncNeedsVisibility(); return wrap;',
      ].join('\n');
      return new Function('document', 'formState', 'sellToChecked', 'hideError', 'getComputedStyle', 'wrap', body)(
        { getElementById: (id) => (id === 'needs-wrap' ? wrap : null) },
        /* offer_*, not utm_* -- the offer reads what we REMEMBER them
           coming for, which is a different column to what this visit is
           attributed to. A stub using utm_* here would pass while the
           real page showed the wrong offer. */
        { offer_campaign: campaign, offer_medium: medium },
        () => ({ id: 'sell-b2b' }), () => {}, undefined, wrap);
    };
    ok(`27: ${tag} SHOWS the selector for direct traffic`,   !run('', '').has('is-hidden'));
    ok(`27: ${tag} SHOWS the selector for a brand campaign`, !run('UR_G_S_US_BR_Brand-tIS', 'cpc').has('is-hidden'));
    ok(`27: ${tag} SHOWS the selector for a LinkedIn post`,  !run('gushwork', 'social').has('is-hidden'));
    ok(`27: ${tag} SHOWS the selector for a bare campaign id`, !run('120241181781830373', 'paid').has('is-hidden'));
    ok(`27: ${tag} HIDES the selector for an AEO ad`,         run('FLI__Prospecting__TOF__CBO', 'paid').has('is-hidden'));
    ok(`27: ${tag} HIDES the selector for a CRM ad`,          run('FLI__Prospecting__CRM-Offer__CBO', 'paid').has('is-hidden'));
  }

  /* ── B2C IS ALLOWED WHEREVER CRM IS SOLD ───────────────────────
     A CRM-campaign lead on /demo is being sold the CRM product, so the
     B2C dead end removed for /ai-demo on 15 Sept must not come back for
     them. The const is untouched, so section 21 still holds. */
  for (const [tag, src] of [['form', demo], ['form-popup', popup]]) {
    const run = (path, campaign, medium) => {
      const body = [
        (/const B2C_ALLOWED_PATHS = \[[^\]]*\];/.exec(src) || [''])[0],
        (/const OFFER_CRM\s+= '[^']*';/.exec(src) || [''])[0],
        (/const OFFER_AEO\s+= '[^']*';/.exec(src) || [''])[0],
        (/const OFFER_SELECTOR\s+= '[^']*';/.exec(src) || [''])[0],
        (/const OFFER_AD_MEDIUMS = \[[^\]]*\];/.exec(src) || [''])[0],
        liftFunction(src, 'campaignOffer'),
        liftFunction(src, 'currentOffer'),
        liftFunction(src, 'b2cAllowedHere'),
        'return b2cAllowedHere();',
      ].join('\n');
      return new Function('window', 'formState', body)(
        { location: { pathname: path } }, { offer_campaign: campaign, offer_medium: medium });
    };
    ok(`27: ${tag} still allows B2C on /ai-demo`,           run('/ai-demo', '', '') === true);
    ok(`27: ${tag} still refuses B2C on /demo by default`,  run('/demo', '', '') === false);
    ok(`27: ${tag} allows B2C for a CRM campaign on /demo`, run('/demo', 'CRM-Offer', 'paid') === true);
    ok(`27: ${tag} still refuses B2C for an AEO campaign`,  run('/demo', 'TOF', 'paid') === false);

    /* ── /ai-crm, ADDED 18 SEPT 2026 ──────────────────────────
       The page shipped live, routed to the CRM team by its Webflow
       attribute, and was missing from this list -- so a B2C answer
       dead-ended a CRM prospect on a page that sells to B2C happily.
       It half-worked, which is why it survived: a visitor arriving on a
       CRM campaign got through the OTHER door, currentOffer() === crm,
       so the page looked fine to anyone who tested it from an ad.

       EVERY JOURNEY, deliberately. The page sells one product whoever
       shows up, and most of its traffic will carry no campaign at all --
       which is exactly the case the campaign clause cannot rescue.
       Measured on /ai-demo, the page /ai-crm replaces: 9 of 40 leads
       answered B2C or Mixed. */
    ok(`27: ${tag} allows B2C on /ai-crm direct`,            run('/ai-crm', '', '') === true);
    ok(`27: ${tag} allows B2C on /ai-crm from a CRM ad`,     run('/ai-crm', 'CRM-Offer', 'paid') === true);
    ok(`27: ${tag} allows B2C on /ai-crm from an AEO ad`,    run('/ai-crm', 'TOF', 'paid') === true);
    ok(`27: ${tag} allows B2C on /ai-crm from brand search`, run('/ai-crm', 'UR_G_S_US_BR_Brand-tIS', 'cpc') === true);
    /* Normalised the same way resolveProduct normalises, so a trailing
       slash or a capitalised path cannot disagree with the server about
       what page this is. */
    ok(`27: ${tag} allows B2C on /ai-crm/ with a trailing slash`, run('/ai-crm/', '', '') === true);
    ok(`27: ${tag} allows B2C on /AI-CRM uppercased`,             run('/AI-CRM', '', '') === true);
    /* And nothing near it opened by accident. */
    ok(`27: ${tag} still refuses B2C on /ai-crm-pricing`,    run('/ai-crm-pricing', '', '') === false);
    ok(`27: ${tag} still refuses B2C on /aeo`,               run('/aeo', '', '') === false);
  }

  /* ── /ai-crm IS A CRM PAGE TO THE SERVER TOO ──────────────────
     The Webflow attribute routes the booking; this is what decides the
     stored product, the Salesforce picklist and the Meta event. Before
     this entry existed the page fell through to the aeo default, so a
     CRM lead was reported as an AEO lead at 12000 instead of 5000 with
     nothing anywhere saying so. */
  for (const [label, url] of [
    ['plain',            'https://www.gushwork.ai/ai-crm'],
    ['trailing slash',   'https://www.gushwork.ai/ai-crm/'],
    ['uppercased',       'https://www.gushwork.ai/AI-CRM'],
    ['with a query',     'https://www.gushwork.ai/ai-crm?utm_campaign=x'],
    ['bare path',        '/ai-crm'],
  ]) {
    eq(`27: /ai-crm resolves crm (${label})`, META.resolveProduct({ page_url: url }), 'crm');
    eq(`27: /ai-crm event slug is crm (${label})`, META.resolveEventProduct({ page_url: url }), 'crm');
  }
  /* THE PAGE OUTRANKS THE CAMPAIGN, and /ai-crm is the first entry that
     makes that guard matter in production: an AEO prospecting campaign
     pointed at the CRM page must not demote the lead. */
  eq('27: an AEO campaign on /ai-crm still resolves crm',
     META.resolveProduct({ page_url: '/ai-crm', utm_campaign: 'FLI__Prospecting__TOF__CBO', utm_medium: 'paid' }), 'crm');
  eq('27: brand search on /ai-crm still resolves crm',
     META.resolveProduct({ page_url: '/ai-crm', utm_campaign: 'UR_G_S_US_BR_Brand-tIS', utm_medium: 'cpc' }), 'crm');
  /* Nothing adjacent moved. */
  eq('27: /ai-demo is untouched',   META.resolveProduct({ page_url: '/ai-demo' }), 'crm');
  eq('27: /demo is untouched',      META.resolveProduct({ page_url: '/demo' }), 'aeo');
  eq('27: /aeo is untouched',       META.resolveProduct({ page_url: '/aeo' }), 'aeo');
  eq('27: /ai-crm-pricing does NOT match by prefix',
     META.resolveProduct({ page_url: '/ai-crm-pricing' }), 'aeo');
}

/* ============================================================
   28. OUR OWN TEST SUBMISSIONS DO NOT FEED META OR SALESFORCE

   Measured 19 Sept 2026: 21 internal addresses had produced 81 lead
   rows since March and, since Meta CAPI went live on 8 April, had
   fired 61 StartTrial, 31 Lead and 14 Schedule events -- 1.14%, 0.81%
   and 0.41% of each -- plus 41 Salesforce Lead records. Nobody chose
   that. An ad audience optimised partly toward our own staff is noise
   we were paying to inject.

   b@g.ai WAS THE LEAST PROTECTED ADDRESS, NOT THE MOST. It is
   special-cased in four hardcoded lists -- both form files,
   PS_TEST_EMAILS and the two booking webhooks -- and was in none of
   the one that matters, so isInternalLead('b@g.ai') answered FALSE and
   it was not even marked on the dashboard as ours.

   EXECUTED, NOT READ, and BOTH HALVES ARE NEEDED. Driving the function
   proves it decides correctly; it says nothing about whether the five
   Meta call sites reach it, and an ordering assertion cannot see an
   `if (false)`. So the wiring is asserted per call site by name.
   ============================================================ */
{
  /* Top-level in index.js, so zero indent -- the section 27 lifter
     looks for four spaces and would not find these. */
  const liftTop = (name) => {
    const m = new RegExp('\\nfunction ' + name + '\\s*\\(').exec(src);
    if (!m) throw new Error('function not found: ' + name);
    let d = 0;
    for (let j = src.indexOf('{', m.index); j < src.length; j++) {
      if (src[j] === '{') d++;
      else if (src[j] === '}') { d--; if (!d) return src.slice(m.index, j + 1); }
    }
    throw new Error('unbalanced braces in: ' + name);
  };

  const parts = [
    (/const ELV_EXCLUDED_DOMAINS = \[[^\]]*\];/.exec(src) || [''])[0],
    (/const INTERNAL_TEST_EMAILS = \[[\s\S]*?\n\];/.exec(src) || [''])[0],
    (/const INTERNAL_STAGING_HOSTS = \[[^\]]*\];/.exec(src) || [''])[0],
    liftTop('isInternalLead'),
    liftTop('isStagingSubmission'),
    liftTop('isInternalSubmission'),
    liftTop('internalLeadSuppressesMeta'),
  ];
  ok('28: every piece of the internal-lead rule is liftable',
     parts.every((x) => x && x.length > 0),
     parts.map((x, i) => (x ? '' : 'piece ' + i + ' missing')).filter(Boolean).join(', '));

  /* console is stubbed so the suite stays readable; the calls still
     happen, which is what reachability means here. */
  const suppresses = new Function('console', 'process',
    parts.concat('return internalLeadSuppressesMeta;').join('\n')
  )({ log() {} }, { env: {} });
  const internal = new Function('process',
    parts.concat('return isInternalLead;').join('\n')
  )({ env: {} });
  const staging = new Function('URL', 'process',
    parts.concat('return isStagingSubmission;').join('\n')
  )(URL, { env: {} });

  /* ---- it decides correctly ---- */
  [
    ['b@g.ai',                    true,  'the form test address -- THE regression this closes'],
    ['B@G.AI',                    true,  'case folded'],
    ['  b@g.ai  ',                true,  'trimmed'],
    ['darshil.dixit@gushwork.ai', true,  'staff address'],
    ['test@test.com',             true,  'test domain'],
    ['x@example.com',             true,  'example domain'],
    ['agent@allstate.com',        true,  'the non-ICP walkthrough address'],
    ['john@acme.com',             false, 'a real prospect'],
    ['',                          false, 'empty'],
    [null,                        false, 'null'],
    ['someone@notgushwork.ai',    false, 'MUST NOT substring-match gushwork.ai'],
    ['a@gushwork.ai.evil.com',    false, 'MUST NOT match a lookalike domain'],
  ].forEach(([email, want, why]) => {
    eq(`28: isInternalLead(${JSON.stringify(email)}) -- ${why}`, internal(email), want);
    eq(`28: suppresses Meta for ${JSON.stringify(email)}`, suppresses(email, '/demo', '/t'), want);
  });

  /* A NULL page_url IS NOT EVIDENCE OF STAGING, and the SQL has to agree.

     The JS has always answered false here. The SQL arm did not: it read
     SPLIT_PART(NULL, ...), which is NULL, so the whole OR went NULL and
     NOT(NULL) went NULL with it -- a lead with no page_url matched
     neither the clause nor its negation. Ten such rows exist and seven
     of them were falling out of both halves of the All Leads Meta
     filter, found by adding the halves up against production on
     22 Sept 2026. Section 23 asserts the COALESCE that fixes it; this
     is the JS side of the same claim. */
  [[null, 'null'], [undefined, 'undefined'], ['', 'empty'], ['/demo', 'a bare path']]
    .forEach(([u, why]) => {
      eq(`28: isStagingSubmission(${JSON.stringify(u)}) is false -- ${why}`, staging(u), false);
    });

  /* ---- THE PAGE IS A SIGNAL TOO, added 19 Sept 2026 ----
     40 rows were submitted from gushwork.webflow.io, 19 of them under
     addresses no list could catch -- the team's personal Gmails. Nobody
     FINDS the staging site, so everyone on it was handed the URL. */
  [
    ['https://gushwork.webflow.io/demo',              true,  'the staging site'],
    ['https://gushwork.webflow.io/demo-testing-rh',   true,  'the actual page the 40 rows came from'],
    ['http://gushwork.webflow.io/x?a=1',              true,  'scheme and query are irrelevant'],
    ['https://GUSHWORK.WEBFLOW.IO/demo',              true,  'host is case-insensitive'],
    ['https://www.gushwork.ai/demo',                  false, 'the REAL site must never match'],
    ['https://gushwork.webflow.io.evil.com/demo',     false, 'MUST NOT substring-match a lookalike host'],
    ['https://evil.com/?x=gushwork.webflow.io',       false, 'MUST NOT match it inside a query string'],
    ['/demo',                                         false, 'a bare path resolves to no host, so it is not evidence'],
    ['not a url',                                     false, 'garbage is not staging'],
    ['',                                              false, 'empty'],
    [null,                                            false, 'null'],
  ].forEach(([page, want, why]) => {
    eq(`28: isStagingSubmission(${JSON.stringify(page)}) -- ${why}`, staging(page), want);
    /* A real prospect's address on the staging site is still ours. */
    eq(`28: suppresses Meta for buyer@acme.com on ${JSON.stringify(page)}`,
       suppresses('buyer@acme.com', page, '/t'), want);
  });
  eq('28: a real lead on a real page is NEVER suppressed',
     suppresses('buyer@acme.com', 'https://www.gushwork.ai/demo', '/t'), false);

  /* ---- and the five Meta call sites actually reach it ---- */
  const CALLS = [
    ["internalLeadSuppressesMeta(email, page_url, '/partial')",              'StartTrial'],
    ["internalLeadSuppressesMeta(email, page_url, '/submit')",               'Lead'],
    ["internalLeadSuppressesMeta(fullLead.email, fullLead.page_url, '/booking-confirmed')", 'Schedule 1 of 3'],
    ["internalLeadSuppressesMeta(fullLead.email, fullLead.page_url, '/cal-webhook')",       'Schedule 2 of 3'],
    ["internalLeadSuppressesMeta(fullLead.email, fullLead.page_url, '/rh-webhook')",        'Schedule 3 of 3'],
  ];
  CALLS.forEach(([call, what]) => {
    eq(`28: ${what} is guarded -- ${call}`, src.split(call).length - 1, 1);
  });
  eq('28: exactly five call sites, so a sixth Meta event cannot be added unnoticed',
     (src.match(/internalLeadSuppressesMeta\(/g) || []).length, 1 + CALLS.length);

  /* Ordering: ours is decided BEFORE the non-ICP question at all three
     Schedule sites. Our own address is ours whatever the model thinks
     of its domain, and the logged reason should say so. */
  ['/booking-confirmed', '/cal-webhook', '/rh-webhook'].forEach((tag) => {
    ok(`28: ${tag} checks internal before non-ICP`,
       src.indexOf(`internalLeadSuppressesMeta(fullLead.email, '${tag}')`) <
       src.indexOf(`nonIcpScheduleSuppressed(fullLead, '${tag}')`));
  });

  /* ---- Salesforce ---- */
  ok('28: the /submit Salesforce push is inside an isInternalSubmission guard',
     /if \(isInternalSubmission\(email, page_url\)\) \{[\s\S]{0,200}?Salesforce push skipped[\s\S]{0,200}?\} else \{\s*\n\s*pushToSalesforce\(/.test(src));
  ok('28: SCHEDULE_LEAD_SQL selects page_url, or the three booking guards read undefined',
     /SELECT[\s\S]{0,2000}?l\.page_url/.test(src));
  ok('28: nothing is stamped when the push is skipped, so the retry sweep ignores it',
     !/Salesforce push skipped[\s\S]{0,300}?markSalesforceSynced/.test(src));
}

/* ============================================================
   29. OUR OWN TEST SUBMISSIONS DO NOT REACH THE DIALER

   gw_form_leads is not a reporting surface -- it is the feed the
   dialer reads, so a row there is a person somebody may ring. That is
   a DIFFERENT harm to the Meta and Salesforce ones section 28 closes:
   those were about signal, this one wastes an SDR's time.

   The guard sits INSIDE syncToAWS rather than at its four call sites,
   because two of those are the Cal and RevenueHero safety nets -- the
   pair most likely to be missed by someone guarding the two obvious
   routes.

   EXECUTED. The function is lifted and CALLED against a stub pool, so
   what is asserted is whether a query was issued, not whether a line
   of source exists.
   ============================================================ */

/* ============================================================
   31. VISITOR GEOLOCATION FROM THE IP

   Storing an IP was asked for on 23 Sept 2026. Nothing here had ever
   stored one: the PartnerStack fraud context and the Meta payloads both
   read it off the request and discarded it, so the only location we held
   was Apollo's COMPANY HQ, which covers 44% of leads.

   EXECUTED against a stubbed fetch, because "we could not look it up"
   must never be recorded as a fact about where somebody was.
   ============================================================ */
{
  const idx = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
  const between = (a, b) => { const i = idx.indexOf(a), j = idx.indexOf(b, i); return idx.slice(i, j); };
  const lift = (env, fetchStub) => (new Function(
    'process', 'fetch', 'AbortController', 'setTimeout', 'clearTimeout',
    between('const IP_GEO_ENABLED', '/* Its own targeted write, NOT syncToAWS')
    + '\nreturn { resolveIpGeo, isPrivateIp, IP_GEO_ENABLED };'
  ))({ env }, fetchStub, AbortController, setTimeout, clearTimeout);

  const okResp = (body) => async () => ({ ok: true, json: async () => body });
  const ENV = {};

  /* PRIVATE RANGES NEVER SPEND A REQUEST. If the first XFF entry is
     private something is wrong upstream, and the honest record is no geo
     rather than a shrug from a third party. */
  {
    let called = 0;
    const M = lift(ENV, async () => { called++; return { ok: true, json: async () => ({ success: true, city: 'X' }) }; });
    for (const ip of ['10.0.0.5', '192.168.1.1', '127.0.0.1', '172.16.5.5', '172.31.255.1', '169.254.1.1', '::1'])
      ok(`31: ${ip} is treated as private`, M.isPrivateIp(ip) === true);
    for (const ip of ['8.8.8.8', '203.0.113.45', '1.1.1.1'])
      ok(`31: ${ip} is treated as public`, M.isPrivateIp(ip) === false);
  }

  /* The mapping assertions live in results31 below, with the rest of the
     async ones. An earlier draft did them here with a bare `return` inside
     this block -- which in CommonJS RETURNS FROM THE MODULE, so the file
     exited 0 having printed no totals at all. Exactly the shape
     crash-reporter.js exists for: a suite that reads as a clean run to
     anything counting exit codes. */
}

/* The failure modes, which are the point. Split out so the block above can
   return its promise. */
const results31 = (async () => {
  const out = [];
  const idx = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
  const between = (a, b) => { const i = idx.indexOf(a), j = idx.indexOf(b, i); return idx.slice(i, j); };
  const lift = (env, fetchStub) => (new Function(
    'process', 'fetch', 'AbortController', 'setTimeout', 'clearTimeout',
    between('const IP_GEO_ENABLED', '/* Its own targeted write, NOT syncToAWS')
    + '\nreturn { resolveIpGeo, isPrivateIp, IP_GEO_ENABLED };'
  ))({ env }, fetchStub, AbortController, setTimeout, clearTimeout);
  const okResp = (body) => async () => ({ ok: true, json: async () => body });

  /* A REAL RESPONSE, mapped. The field names are ipwho.is's, so a rename
     at their end must fail here rather than silently store nulls. */
  const mapped = await lift({}, okResp({
    success: true, city: 'San Jose', region: 'California', country_code: 'US',
    country: 'United States', postal: '95025',
    timezone: { id: 'America/Los_Angeles' },
    connection: { isp: 'Google LLC', org: 'Google LLC', domain: 'google.com' },
  })).resolveIpGeo('8.8.8.8');
  out.push(['31: city is mapped', mapped.ip_city === 'San Jose', mapped.ip_city]);
  out.push(['31: region is mapped', mapped.ip_region === 'California', mapped.ip_region]);
  out.push(['31: country CODE is mapped', mapped.ip_country === 'US', mapped.ip_country]);
  out.push(['31: country NAME is mapped', mapped.ip_country_name === 'United States', mapped.ip_country_name]);
  out.push(['31: the IANA timezone comes from the nested object', mapped.ip_timezone === 'America/Los_Angeles', mapped.ip_timezone]);
  out.push(['31: the ISP is mapped', mapped.ip_isp === 'Google LLC', mapped.ip_isp]);
  out.push(['31: the org domain is mapped', mapped.ip_org_domain === 'google.com', mapped.ip_org_domain]);

  /* success:false ARRIVES WITH A 200. That is how ipwho.is reports a rate
     limit, and reading the status alone would stamp an empty place as a
     real one -- the same shape as Meta answering 200 to a malformed IP. */
  const rateLimited = await lift({}, okResp({ success: false, message: 'RateLimited' })).resolveIpGeo('8.8.8.8');
  out.push(['31: success:false with a 200 is NOT a location', rateLimited === null, String(rateLimited)]);

  /* AN EMPTY BODY IS NOT A PLACE. Without this a row would read "we looked
     and they are nowhere", which is a measurement nobody made. */
  const empty = await lift({}, okResp({ success: true })).resolveIpGeo('8.8.8.8');
  out.push(['31: an empty response is NOT a location', empty === null, String(empty)]);

  const errored = await lift({}, okResp({ error: true, reason: 'RateLimited' })).resolveIpGeo('8.8.8.8');
  out.push(['31: an error flag is NOT a location', errored === null]);

  const http500 = await lift({}, async () => ({ ok: false, status: 500, json: async () => ({}) })).resolveIpGeo('8.8.8.8');
  out.push(['31: a non-200 is NOT a location', http500 === null]);

  const threw = await lift({}, async () => { throw new Error('ECONNRESET'); }).resolveIpGeo('8.8.8.8');
  out.push(['31: a thrown fetch fails silent', threw === null]);

  /* THE KILL SWITCH. A free geo service is exactly the dependency that
     stops being free, and turning it off must need no deploy. */
  let calledWhenOff = 0;
  const off = await lift({ IP_GEO_ENABLED: 'false' }, async () => { calledWhenOff++; return { ok: true, json: async () => ({ success: true, city: 'X' }) }; }).resolveIpGeo('8.8.8.8');
  out.push(['31: IP_GEO_ENABLED=false returns null', off === null]);
  out.push(['31: IP_GEO_ENABLED=false makes no request at all', calledWhenOff === 0, String(calledWhenOff)]);

  /* A private address must not reach the network either. */
  let calledPrivate = 0;
  await lift({}, async () => { calledPrivate++; return { ok: true, json: async () => ({ success: true, city: 'X' }) }; }).resolveIpGeo('10.0.0.5');
  out.push(['31: a private address spends no request', calledPrivate === 0, String(calledPrivate)]);

  /* ── The wiring, which execution cannot reach ─────────────────────── */
  /* NEVER ON THE LEAD PATH. */
  out.push(['31: the geo call is never awaited',
            /\n    finaliseIpGeo\(session_id, clientIpOf\(req\)\)\.catch/.test(idx)]);
  /* BOTH ROUTES. 41 leads a day reach step 1 and 11 never submit, so
     resolving only at /submit would leave every drop-off -- the SDR list
     and the recovery cron -- with no location at all. */
  out.push(['31: it runs from BOTH /partial and /submit',
            (idx.match(/finaliseIpGeo\(session_id, clientIpOf\(req\)\)/g) || []).length === 2]);
  /* /partial fires repeatedly through step 1 and the "actually we're B2B"
     button calls savePartial(1) again, which 47% of leads press. Without a
     guard one visitor spends several lookups. */
  out.push(['31: at most one lookup per session, guarded in process',
            /_ipGeoInFlight/.test(idx)]);
  out.push(['31: and guarded durably by ip_checked_at, so a deploy buys no second lookup',
            /SELECT ip_checked_at FROM leads WHERE session_id/.test(idx)]);
  /* The address is free and must be written on every call; only the
     network half is rationed. */
  out.push(['31: the address is still written before the dedup check',
            idx.indexOf('UPDATE leads SET ip_address = COALESCE') < idx.indexOf('_ipGeoInFlight.has(session_id)')]);
  /* AFTER THE RESPONSE IN BOTH ROUTES. Checked per route rather than with
     a bare indexOf, which finds /partial's call and silently compares it
     against /submit's response -- it failed exactly that way when the
     second call site was added. */
  {
    const calls = [];
    let at = idx.indexOf('finaliseIpGeo(session_id, clientIpOf(req))');
    while (at !== -1) { calls.push(at); at = idx.indexOf('finaliseIpGeo(session_id, clientIpOf(req))', at + 1); }
    const partialRes = idx.indexOf("console.log(`[/partial] \u2705 Saved session");
    const submitRes  = idx.indexOf("res.json({ ok: true, non_icp_blocked");
    out.push(['31: /partial resolves AFTER its response',
              calls.some((c) => c > partialRes && c < submitRes), String(calls)]);
    out.push(['31: /submit resolves AFTER its response',
              calls.some((c) => c > submitRes), String(calls)]);
  }
  /* THE ADDRESS AND THE PLACE ARE TWO WRITES, so a geo outage cannot lose
     the address. */
  out.push(['31: the address is written before the lookup',
            /UPDATE leads SET ip_address = COALESCE\(ip_address, \$2\)/.test(idx)]);
  /* ONE DEFINITION of "what is this visitor's address". There were two and
     they disagreed, which is the whole reason PR 111 exists. */
  out.push(['31: PartnerStack reads the shared extractor, not its own split',
            /function readPartnerStackRequestContext\(req, page_url\) \{\s*const ip_address = clientIpOf\(req\);/.test(idx)]);
  /* THE MIRROR GETS THE PLACE, NOT THE ADDRESS. Copying personal data into
     a second database needs a reason for it being there. */
  out.push(['31: the mirror stores geo but NOT the raw address',
            /gw_form_leads ADD COLUMN IF NOT EXISTS ip_city/.test(idx)
            && !/gw_form_leads ADD COLUMN IF NOT EXISTS ip_address/.test(idx)]);
  out.push(['31: the mirror write is targeted, never syncToAWS',
            /function syncIpGeoToAWS[\s\S]{0,500}UPDATE gw_form_leads/.test(idx)]);
  /* The dashboard must not let a reader take company HQ for visitor
     location. */
  out.push(['31: the panel labels it "Visitor location", distinctly from Apollo’s',
            /lb:"Visitor location"/.test(idx) && /lb:"Location"/.test(idx)]);
  return out;
})();

/* ============================================================
   30. THE CLIENT IP SENT TO META

   Every Meta call site passed `req.headers['x-forwarded-for']` RAW.
   Railway sits behind a proxy chain, so that can be a comma-separated
   list, and readPartnerStackRequestContext ~4,000 lines earlier already
   splits it with a comment explaining why a list "looks like a value and
   will never match anything". One integration was fixed, the other was
   not.

   VERIFIED AGAINST THE LIVE API on 23 Sept 2026: a clean IP, a two-hop
   list and a three-hop list each returned HTTP 200, events_received 1 and
   an EMPTY messages array. Meta never complains, so nothing downstream can
   catch this -- it has to be right on the way out.
   ============================================================ */
{
  const capi = require('../meta-capi');
  const n = capi.normalizeClientIp;

  /* THE BUG, executed. */
  eq('30: a two-hop XFF yields the client address', n('203.0.113.45, 10.0.0.1'), '203.0.113.45');
  eq('30: a three-hop XFF yields the client address', n('203.0.113.45, 10.0.0.1, 172.16.0.9'), '203.0.113.45');
  /* THE NO-OP, which is why this fix is safe whatever the header holds. */
  eq('30: a single-entry XFF is unchanged', n('203.0.113.45'), '203.0.113.45');
  eq('30: whitespace is trimmed', n('  203.0.113.45  '), '203.0.113.45');
  /* IPv6, which Railway does send. */
  eq('30: IPv6 survives', n('2001:db8::1'), '2001:db8::1');
  eq('30: IPv6 with a hop yields the client address', n('2001:db8::1, 10.0.0.1'), '2001:db8::1');
  eq('30: bracketed IPv6 is unwrapped', n('[2001:db8::1]'), '2001:db8::1');

  /* JUNK IS DROPPED, NOT FORWARDED. A key absent from user_data is
     honest; a key holding a non-address is a value that looks real and
     matches nobody -- the same reason normalizePhone returns undefined
     rather than an empty string. */
  for (const bad of ['', '   ', 'not-an-ip', '999.1.1.1', '1.2.3', 'localhost', ',,,']) {
    ok(`30: junk is dropped, not sent: ${JSON.stringify(bad)}`, n(bad) === undefined, String(n(bad)));
  }
  ok('30: undefined in, undefined out', n(undefined) === undefined);
  ok('30: null in, undefined out', n(null) === undefined);

  /* THE CHOKE POINT IS THE POINT. Six call sites pass this value and a
     seventh will exist one day; normalising at any of them is how the
     PartnerStack path and the Meta path drifted apart in the first place.
     So the payload must read the normaliser, and no call site may be
     trusted to have done it. */
  const src = fs.readFileSync(path.join(__dirname, '..', 'meta-capi.js'), 'utf8');
  ok('30: the payload normalises rather than passing the raw option',
     /client_ip_address: normalizeClientIp\(options\.clientIpAddress\)/.test(src));
  ok('30: the raw option never reaches the payload',
     !/client_ip_address: options\.clientIpAddress/.test(src));

  /* META'S WARNINGS WERE DISCARDED. Only events_received was logged, so a
     malformed field could be wrong for months while every line read like a
     success. */
  ok('30: Meta’s messages array is read, not thrown away',
     /result\.messages/.test(src) && /Meta returned/.test(src));

  /* The webhook Schedule sites deliberately send NO ip: the request comes
     from Cal or RevenueHero, so their address is not the visitor's and
     sending it would be worse than sending nothing. */
  const idx = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
  eq('30: the two webhook Schedule sites still send no client ip',
     (idx.match(/clientIpAddress:''/g) || []).length, 2);
}
{
  const liftTop = (name) => {
    const m = new RegExp('\\nfunction ' + name + '\\s*\\(').exec(src);
    if (!m) throw new Error('function not found: ' + name);
    let d = 0;
    for (let j = src.indexOf('{', m.index); j < src.length; j++) {
      if (src[j] === '{') d++;
      else if (src[j] === '}') { d--; if (!d) return src.slice(m.index, j + 1); }
    }
    throw new Error('unbalanced braces in: ' + name);
  };
  const base = [
    (/const ELV_EXCLUDED_DOMAINS = \[[^\]]*\];/.exec(src) || [''])[0],
    (/const INTERNAL_TEST_EMAILS = \[[\s\S]*?\n\];/.exec(src) || [''])[0],
    (/const INTERNAL_STAGING_HOSTS = \[[^\]]*\];/.exec(src) || [''])[0],
    liftTop('isInternalLead'),
    liftTop('isStagingSubmission'),
    liftTop('isInternalSubmission'),
  ];

  /* A synchronous thenable, so the .then() logging runs before the
     assertion rather than after the suite has printed its totals. */
  const thenable = (rowCount) => ({
    then(f) { f({ rowCount }); return { catch() {} }; },
    catch() { return this; },
  });

  const makeSync = (pool, logs) => new Function(
    'awsPool', 'console', 'recordFailure', 'process',
    base.concat(liftTop('syncToAWS'), 'return syncToAWS;').join('\n')
  )(pool, { log: (m) => logs.push(m), warn: (m) => logs.push(m) }, () => {}, { env: {} });

  /* ---- the mirror itself ---- */
  [
    ['b@g.ai',                    false, 'the form test address'],
    ['darshil.dixit@gushwork.ai', false, 'staff address'],
    ['x@test.com',                false, 'test domain'],
    ['agent@allstate.com',        false, 'the non-ICP walkthrough address'],
    ['buyer@acme.com',            true,  'a REAL lead still mirrors -- the dialer must keep working'],
    [undefined,                   true,  'a webhook row with no email still mirrors'],
  ].forEach(([email, shouldWrite, why]) => {
    const seen = [], logs = [];
    const sync = makeSync({ query: (sql, params) => { seen.push(sql); return thenable(1); } }, logs);
    sync({ session_id: 's1', email });
    eq(`29: mirror write for ${JSON.stringify(email)} -- ${why}`, seen.length, shouldWrite ? 1 : 0);
    if (!shouldWrite) {
      ok(`29: and it says why it skipped ${JSON.stringify(email)}`,
         logs.some((l) => /Mirror skipped/.test(l)), logs.join(' | '));
    }
  });

  /* A missing awsPool must still short-circuit first -- the guard must
     not have moved above it and started touching a null pool. */
  {
    const logs = [];
    const sync = new Function('awsPool', 'console', 'recordFailure', 'process',
      base.concat(liftTop('syncToAWS'), 'return syncToAWS;').join('\n')
    )(null, { log: (m) => logs.push(m), warn: (m) => logs.push(m) }, () => {}, { env: {} });
    let threw = false;
    try { sync({ session_id: 's1', email: 'buyer@acme.com' }); } catch (e) { threw = true; }
    ok('29: no awsPool is still a clean no-op', !threw);
  }

  /* ---- the three targeted writes no-op rather than erroring ---- */
  ['syncBookingToAWS', 'syncPartnerIdentityToAWS', 'syncHearAboutUsToAWS'].forEach((name) => {
    const fsrc = liftTop(name);
    ok(`29: ${name} is a targeted UPDATE keyed on session_id, so a missing mirror row matches nothing`,
       /UPDATE gw_form_leads/.test(fsrc) && /WHERE\s+session_id\s*=\s*\$1/.test(fsrc));
    ok(`29: ${name} is NOT an upsert, so it cannot resurrect a skipped row`,
       !/INSERT INTO/.test(fsrc));
  });

  /* ---- and the booking log reports what it actually did ---- */
  [[0, /not mirrored/, 'no row'], [1, /Booking synced/, 'a row']].forEach(([rowCount, want, what]) => {
    const logs = [];
    const fn = new Function('awsPool', 'console', 'recordFailure',
      liftTop('syncBookingToAWS') + '; return syncBookingToAWS;'
    )({ query: () => thenable(rowCount) }, { log: (m) => logs.push(m), warn: (m) => logs.push(m) }, () => {});
    fn('s1', 'uid', null, null, null);
    ok(`29: booking sync against ${what} logs the truth`,
       logs.some((l) => want.test(l)), logs.join(' | '));
  });
}

/* ============================================================ */
/* Section 12 is async, so the totals are printed from its continuation.
   The catch is not optional: without it a throw in there escapes as an
   unhandledRejection and the suite prints no totals at all, which reads
   as UNMEASURED rather than as a failure. */
section12()
  .then(() => results16().catch((e) => [['sf: section 16 ran to completion', false, e && e.message]]))
  .then((rows) => { for (const [n, c, x] of rows) ok(n, c, x); return results19().catch((e) => [['tec: section 19 ran to completion', false, e && e.message]]); })
  .then((rows) => { for (const [n, c, x] of rows) ok(n, c, x); return results20().catch((e) => [['backfill: section 20 ran to completion', false, e && e.message]]); })
  .then((rows) => { for (const [n, c, x] of rows) ok(n, c, x); })
  .catch((err) => { ok('capi: section 12 completed', false, err && err.message); })
  /* ══ SALESFORCE: AN OUTAGE IS NOT A LOST LEAD, AND A CONVERTED LEAD
        IS NOT A MISSING ONE ══════════════════════════════════════════
     16 Sept 2026: four alerts, two causes, and both messages were wrong.
     Salesforce served an HTML maintenance page with a 503 and every write
     path called res.json() on it, so the critical alert read "Unexpected
     token < in JSON at position 0" -- which reads as a bug in this repo
     rather than a third party being down, and named a lead that really was
     missing. Separately, a returning customer whose Lead had been converted
     produced "This lead is NOT in Salesforce. Add it manually", which is
     false and creates a duplicate against a live Account if anyone does it. */
  .then(async () => {
    const sfSrc = fs.readFileSync(path.join(__dirname, '..', 'salesforce.js'), 'utf8');

    /* getSalesforceToken always checked res.ok and read .text(). The WRITE
       paths never did, so one path reported the outage in plain words and
       the others reported a parse error for the same minute of it. */
    ok('sfbody: one shared reader handles Salesforce response bodies',
       /async function readSfBody\(res\)/.test(sfSrc));
    ok('sfbody: it reads text FIRST, then parses — a body can only be read once',
       /const text = await res\.text\(\)[\s\S]{0,140}JSON\.parse\(text\)/.test(sfSrc));
    {
      const create = sfSrc.slice(sfSrc.indexOf('async function pushToSalesforce'),
                                 sfSrc.indexOf('async function findSFLeadByEmail'));
      const update = sfSrc.slice(sfSrc.indexOf('async function updateSFLead'),
                                 sfSrc.indexOf('const SF_MAX_PAGES'));
      ok('sfbody: the CREATE path no longer parses an unchecked body',
         /readSfBody\(r\)/.test(create) && !/result: await r\.json\(\)/.test(create));
      ok('sfbody: the UPDATE path no longer parses an unchecked body',
         /readSfBody\(r\)/.test(update) && !/result: await r\.json\(\)/.test(update));
    }

    /* EXECUTED. The whole point is the sentence a human reads at 3am. */
    const readSfBody = eval('(' + sfSrc.slice(
      sfSrc.indexOf('async function readSfBody(res)'),
      sfSrc.indexOf('async function pushToSalesforce')).trim().replace(/\s*$/, '') + ')');
    const res = (status, body) => ({ status, text: async () => body });

    const maint = await readSfBody(res(503,
      '<html><body>We are down for maintenance. Sorry for the inconvenience.</body></html>'));
    ok('sfbody: a maintenance page is named as an OUTAGE, not a parse error',
       maint[0].errorCode === 'SALESFORCE_UNAVAILABLE' && !/Unexpected token/.test(maint[0].message),
       JSON.stringify(maint).slice(0, 150));
    ok('sfbody: and it says the lead itself is fine',
       /Nothing is wrong with this lead/.test(maint[0].message));
    const gw = await readSfBody(res(502, '<html>bad gateway</html>'));
    ok('sfbody: a non-JSON body that is not maintenance still reports its status',
       gw[0].errorCode === 'NON_JSON_RESPONSE' && /502/.test(gw[0].message), gw[0].message);
    const real = await readSfBody(res(400, '[{"errorCode":"X","message":"y"}]'));
    ok('sfbody: a genuine Salesforce error array passes through unchanged',
       Array.isArray(real) && real[0].errorCode === 'X');

    /* ── A CONVERTED LEAD IS A CUSTOMER, NOT A MISSING LEAD ────────── */
    const { sfConvertedLeadError } = require('../salesforce.js');
    ok('sfconv: the converted-lead error is recognised',
       sfConvertedLeadError([{ errorCode: 'CANNOT_UPDATE_CONVERTED_LEAD' }]) === true);
    ok('sfconv: and an unrelated Salesforce error is not mistaken for it',
       sfConvertedLeadError([{ errorCode: 'REQUIRED_FIELD_MISSING' }]) === false);
    ok('sfconv: a null or empty result does not throw',
       sfConvertedLeadError(null) === false && sfConvertedLeadError([]) === false);
    ok('sfconv: updateSFLead throws a FLAGGED error for it',
       /err\.sfConvertedLead = true/.test(sfSrc));
    /* The thrown message carries the warning too, so it is right even at a
       call site that only logs err.message and never reads the flag. */
    ok('sfconv: and the thrown message itself warns against adding a duplicate',
       /do NOT add them manually/i.test(sfSrc));

    /* ── ONE ALERT BUILDER, SIX CALL SITES ─────────────────────────── */
    const idx = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
    ok('sfalert: one function builds the Salesforce failure alert',
       /function salesforceFailureAlert\(kind, err, ctx\)/.test(idx));
    {
      const calls = (idx.match(/salesforceFailureAlert\(/g) || []).length;
      /* Six call sites plus the declaration. A guard added to the obvious
         site misses its siblings -- this repo's oldest lesson, and the
         reason these six are one function rather than six edits. */
      ok('sfalert: all SIX call sites go through it', calls === 7, String(calls));
      ok('sfalert: no call site still hardcodes the dangerous advice',
         (idx.match(/This lead is NOT in Salesforce\. Add it manually\./g) || []).length === 1);
      ok('sfalert: and the surviving copy is the fallback INSIDE the builder',
         idx.indexOf('This lead is NOT in Salesforce') > idx.indexOf('function salesforceFailureAlert')
         && idx.indexOf('This lead is NOT in Salesforce') < idx.indexOf('function recordFailure'));
    }
    /* EXECUTED: the message a converted lead produces, and its severity. */
    {
      const sent = [];
      const build = new Function('alertOps',
        idx.slice(idx.indexOf('function salesforceFailureAlert(kind, err, ctx)'),
                  idx.indexOf('function recordFailure(source, id, error)'))
        + '; return salesforceFailureAlert;')((sev, src, title, f) => sent.push({ sev, title, f }));

      const conv = Object.assign(new Error('already converted'), { sfConvertedLead: true });
      build('lead', conv, { Email: 'a@b.com' });
      ok('sfalert/converted: it is NOT called a missing lead',
         !/not created/i.test(sent[0].title), sent[0].title);
      ok('sfalert/converted: the impact says they ARE in Salesforce',
         /IS in Salesforce/.test(sent[0].f.Impact));
      /* CONVERTED IS NOT CUSTOMER. Converting a Lead creates a Contact and an
         Account, usually with an Opportunity -- it records that the lead was
         QUALIFIED, not that they bought. The Opportunity behind the incident
         that produced this alert reads IsWon false, IsClosed false, stage
         "Demo Completed": an open deal mid-pipeline. Telling an AE they are
         already a customer says the deal is done. */
      ok('sfalert/converted: it is NOT called a customer',
         !/customer/i.test(sent[0].title) && !/as a customer/i.test(sent[0].f.Impact),
         sent[0].title + ' | ' + sent[0].f.Impact.slice(0, 90));
      ok('sfalert/converted: and it says outright that converted is not customer',
         /does NOT mean they are a customer/i.test(sent[0].f.Impact));
      ok('sfalert/converted: the title names the real state — converted',
         /converted/i.test(sent[0].title), sent[0].title);
      ok('sfalert/converted: and explicitly says do NOT add them manually',
         /do NOT add them manually/i.test(sent[0].f.Impact));
      ok('sfalert/converted: downgraded from critical — nothing is lost',
         sent[0].sev === 'warning', sent[0].sev);

      build('lead', new Error('Salesforce is down for maintenance (HTTP 503).'), { Email: 'c@d.com' });
      ok('sfalert/outage: an outage is not a lost lead either',
         sent[1].sev === 'warning' && /Re-run it once Salesforce is back/.test(sent[1].f.Impact));

      build('lead', new Error('REQUIRED_FIELD_MISSING'), { Email: 'e@f.com' });
      ok('sfalert/real: a genuine failure is STILL critical and still says add it manually',
         sent[2].sev === 'critical' && /Add it manually/.test(sent[2].f.Impact));
      build('booking', new Error('REQUIRED_FIELD_MISSING'), { Session: 's' });
      ok('sfalert/real: and a booking failure stays a warning with its own impact',
         sent[3].sev === 'warning' && /booking is missing/.test(sent[3].f.Impact));
    }
  })
  .then(async () => {
    /* ══ THE SALESFORCE RETRY SWEEP ═══════════════════════════════════
       A maintenance window on 16 Sept 2026 dropped a BOOKED lead and the
       only trace was a Slack alert. Nothing retried and nothing recorded,
       so recovery was one manual Salesforce record per lost lead. */
    const idx2 = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
    const dbSrc = fs.readFileSync(path.join(__dirname, '..', 'db.js'), 'utf8');
    const { sfIsRetryable } = require('../salesforce.js');

    /* ── 1. What may be retried, and what may never be ─────────────────
       EXECUTED. Getting this wrong in the generous direction is expensive:
       a sweep that retries terminal failures burns its budget and refills
       the queue with entries no retry can clear. */
    const retry = (m, extra) => sfIsRetryable(Object.assign(new Error(m), extra || {}));
    ok('sfretry: a maintenance window is retryable', retry('Salesforce is down for maintenance (HTTP 503).'));
    ok('sfretry: a non-JSON body is retryable', retry('returned HTTP 502 with a non-JSON body: NON_JSON_RESPONSE'));
    ok('sfretry: a dead socket is retryable', retry('ECONNRESET') && retry('fetch failed'));
    ok('sfretry: a token 5xx is retryable', retry('Salesforce token error: 503 — <html>'));
    ok('sfretry: a CONVERTED lead is NEVER retried', !retry('CANNOT_UPDATE_CONVERTED_LEAD'));
    ok('sfretry: and the flag beats a retryable-looking message',
       !retry('down for maintenance', { sfConvertedLead: true }));
    ok('sfretry: a restricted-picklist rejection is terminal',
       !retry('INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST'));
    ok('sfretry: a validation failure is terminal', !retry('REQUIRED_FIELD_MISSING'));
    /* THE DEFAULT IS TERMINAL. An error nobody has classified is left for a
       human rather than hammered — the lead is never lost by that, because
       the alert still fires and the row keeps its error text. */
    ok('sfretry: an UNKNOWN error defaults to terminal, not retryable',
       !retry('something nobody has ever seen before'));
    ok('sfretry: null and empty do not throw', !sfIsRetryable(null) && !sfIsRetryable(undefined));

    /* ── 2. The columns, and that history is NOT backfilled ───────────── */
    for (const col of ['sf_synced_at', 'sf_sync_failed_at', 'sf_sync_attempts',
                       'sf_sync_error', 'sf_sync_retryable']) {
      ok(`sfretry/db: ${col} is added IF NOT EXISTS`,
         new RegExp('ADD COLUMN IF NOT EXISTS ' + col + '\\b').test(dbSrc));
    }
    /* A migration that backfilled history would have queued thousands of
       re-pushes on the first boot. The sweep keys off an OBSERVED failure
       precisely so it does not have to guess about the past. */
    ok('sfretry/db: nothing backfills the new columns',
       !/UPDATE leads SET sf_synced_at/i.test(dbSrc));
    ok('sfretry/db: the sweep index is PARTIAL, so it stays small',
       /leads_sf_sync_failed_idx[\s\S]{0,160}WHERE sf_sync_failed_at IS NOT NULL/.test(dbSrc));

    /* ── 3. The sweep query's guards ──────────────────────────────────
       Executed against a real Postgres shadow separately; these pin the
       clauses so none can be dropped silently. */
    const sweep = idx2.slice(idx2.indexOf('async function runSalesforceRetrySweep'),
                             idx2.indexOf('function startSalesforceRetrySweep'));
    ok('sfretry/sql: it retries only writes we WATCHED fail',
       /sf_sync_failed_at IS NOT NULL/.test(sweep));
    ok('sfretry/sql: not ones that have since landed',
       /sf_synced_at IS NULL/.test(sweep));
    ok('sfretry/sql: only failures classified retryable',
       /sf_sync_retryable IS TRUE/.test(sweep));
    /* THE CLAUSE THAT WOULD HAVE BEEN MISSED. A blocked lead is deliberately
       absent from Salesforce; this sweep is a NEW consumer of that rule, and
       a second column meaning "we rejected this lead" silently re-scopes
       every consumer of the first. */
    ok('sfretry/sql: a NON-ICP BLOCKED lead is never pushed by the sweep',
       /non_icp_blocked IS NOT TRUE/.test(sweep));
    ok('sfretry/sql: only leads that actually submitted, not merely completed',
       /submitted_at IS NOT NULL/.test(sweep) && !/completed\s*=\s*true/.test(sweep));
    ok('sfretry/sql: attempts are bounded', /sf_sync_attempts, 0\) < \$1/.test(sweep));
    ok('sfretry/sql: and a backoff keeps it off a live outage',
       /sf_sync_failed_at < NOW\(\) - \(\$2/.test(sweep));

    /* ── 4. Shape: claim before push, reuse the real writer ──────────── */
    ok('sfretry: the attempt is claimed BEFORE the push, not after',
       sweep.indexOf('sf_sync_attempts = COALESCE') < sweep.indexOf('await pushToSalesforce'));
    ok('sfretry: it reuses pushToSalesforce rather than reimplementing the write',
       /await pushToSalesforce\(/.test(sweep));
    ok('sfretry: success clears the row out of the sweep',
       /markSalesforceSynced\(l\.session_id\)/.test(sweep));
    ok('sfretry: it only pages once retries are EXHAUSTED, not every tick',
       />= SF_RETRY_MAX_ATTEMPTS/.test(sweep) && /Retries exhausted/.test(sweep));
    ok('sfretry: one at a time — a slow run cannot race itself',
       /_sfRetryRunning/.test(sweep));

    /* ── 5. Both outcomes are recorded on the live path ──────────────── */
    ok('sfretry: /submit records a SUCCESS, not just a failure',
       /\.then\(\(\) => markSalesforceSynced\(session_id\)\)/.test(idx2));
    ok('sfretry: and records the failure with its classification',
       /markSalesforceFailed\(session_id, err\)/.test(idx2));
    ok('sfretry: marking is fire-and-forget — a lead never waits on bookkeeping',
       /markSalesforceSynced\(session_id\) \{[\s\S]{0,400}\.catch\(/.test(idx2)
       || /could not mark synced \(ignored\)/.test(idx2));
    /* The first failure must leave attempts at 0, or every lead gets one
       fewer retry than the budget claims. */
    ok('sfretry: the original failure does not spend a retry attempt',
       !/markSalesforceFailed[\s\S]{0,600}sf_sync_attempts = COALESCE\(sf_sync_attempts, 0\) \+ 1/.test(idx2));
    ok('sfretry: the sweep is started at boot', /startSalesforceRetrySweep\(\);/.test(idx2));
  })
  .then(async () => {
    /* ══ THE FORMULA'S MISSING INPUT ══════════════════════════════════
       Lead.Source_Bucket__c is a Salesforce FORMULA reading utm_source__c
       and How_Did_You_Hear__c. We have always written the first and never
       the second, so the ~14 branches keying off what the visitor told us
       read an empty field and fell to "Others". Measured: 763 of 2,539
       leads (30%) bucketed wrong; "Others" 871 -> 108 once fixed.

       Its value is also what lands on the Opportunity at conversion —
       verified, 1,626 of 1,729 identical — so this one field decides
       channel attribution all the way to closed-won. */
    const sfSrc2 = fs.readFileSync(path.join(__dirname, '..', 'salesforce.js'), 'utf8');
    const a = sfSrc2.indexOf('const STANDARD_FIELD_MAP');
    const b = sfSrc2.indexOf('async function pushToSalesforce');
    const build = new Function(sfSrc2.slice(a, b) + '; return buildLeadFields;')();

    const out = build({ email: 'a@b.com', hear_about_us: 'Facebook (Paid)', utm_source: 'facebook' });
    ok('hdyh: How_Did_You_Hear__c is written', out.How_Did_You_Hear__c === 'Facebook (Paid)', JSON.stringify(out.How_Did_You_Hear__c));
    ok('hdyh: hear_about_us__c is STILL written — the mirror adds, never replaces',
       out.hear_about_us__c === 'Facebook (Paid)');
    /* They must be provably identical. Two fields that can disagree about
       one answer is worse than one field that is empty. */
    ok('hdyh: the two are identical', out.How_Did_You_Hear__c === out.hear_about_us__c);

    /* Absent, not empty-string. An empty string would make the formula's
       LOWER() comparisons run against "" and land in a bucket by accident;
       a missing key leaves the field untouched. */
    const none = build({ email: 'a@b.com' });
    ok('hdyh: omitted entirely when the visitor answered nothing',
       !('How_Did_You_Hear__c' in none) && !('hear_about_us__c' in none));
    const blank = build({ email: 'a@b.com', hear_about_us: '' });
    ok('hdyh: an empty answer is omitted too, not written as ""',
       !('How_Did_You_Hear__c' in blank));

    /* The real values that were falling into "Others" — these are the
       branches the formula could never reach. */
    for (const [said, why] of [['Referral','Referral'], ['linkedin','LinkedIn'],
                               ['chatgpt.com','AI / LLM'], ['Friend','Referral']]) {
      const r = build({ email: 'x@y.com', hear_about_us: said });
      ok(`hdyh: "${said}" now reaches the formula (would bucket ${why})`,
         r.How_Did_You_Hear__c === said);
    }

    /* The mirror is declared once. A second CUSTOM_FIELD_MAP entry would be
       a place to forget. */
    ok('hdyh: it is a MIRROR, not a duplicated map entry',
       /const MIRROR_FIELDS = \{/.test(sfSrc2)
       && (sfSrc2.match(/How_Did_You_Hear__c/g) || []).length <= 3);
    ok('hdyh: mirrors are applied after the custom-field loop, so truncation is inherited',
       sfSrc2.indexOf('MIRROR_FIELDS)') > sfSrc2.indexOf('CUSTOM_FIELD_MAP)'));
  })
  .catch((err) => { ok('hdyh: the How_Did_You_Hear section completed', false, err && err.message); })
  .catch((err) => { ok('sfretry: the retry-sweep section completed', false, err && err.message); })
  .catch((err) => { ok('sf: the Salesforce outage section completed', false, err && err.message); })
  .then(() => results31)
  .then((rows) => { for (const [n, c, x] of rows) ok(n, c, x); })
  .catch((err) => { ok('31: the IP geo section completed', false, err && err.message); })
  .then(() => {
    console.log('');
    console.log(`  passed: ${pass}`);
    console.log(`  failed: ${fail}`);
    if (failures.length) {
      console.log('');
      failures.forEach((f) => console.log('  ✗ ' + f));
    }
    console.log('');
    process.exit(fail === 0 ? 0 : 1);
  });

}
