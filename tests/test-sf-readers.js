/* ============================================================
   Salesforce readers — EXECUTED, not read.

   Every other dependency-free suite asserts on the SOURCE of these functions.
   That is the right default here — it keeps tests pointed at the real code
   rather than a copy — but it cannot tell you whether the code WORKS. Two
   paths shipped in PR 21 (7 Sept 2026) and had never once run:

     - the PAGINATION. There are about 3 ticked Opportunities in the org, so
       every real call has been a single page with done:true. The whole reason
       the LIMIT 200 was a bug is what happens past one page, and that was
       exactly the part no test and no production call had exercised.
     - the ok:false BRANCH. Salesforce has not failed during a poll, so the
       "we could not read this, do not conclude that nothing is ticked" path
       was source-asserted only.

   docs/partnerstack.md: "verified-as-rendered and verified-as-computed are two
   assertions". This file is the third of that family — verified as EXECUTED.
   Asserting that a `while (url && pages < SF_MAX_PAGES)` loop is present in
   the file says nothing about whether it terminates, concatenates, or counts.

   salesforce.js requires cleanly with no database and no server, so the only
   stub needed is global.fetch. The token exchange is stubbed alongside the
   queries, which also means these tests never touch a real org.

   Dependency-free — no DATABASE_URL, no network.

   Run:  node tests/test-sf-readers.js
   ============================================================ */

require('./crash-reporter')('test-sf-readers');

const path = require('path');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
function eq(name, actual, expected) {
  ok(name, JSON.stringify(actual) === JSON.stringify(expected),
     `got ${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`);
}

/* Every scenario runs inside this. A test file that CRASHES instead of
   reporting is worse than one that fails: it prints a stack trace, no ✗ lines,
   and a non-zero exit that looks identical to a green run to anything counting
   failures. That is not hypothetical — mutation-testing this very file found
   three "surviving" mutations that were really crashes on `at(out.records, 259).id`
   after a broken reader returned an empty list. The mutation was caught; the
   measurement was not. */
async function scenario(name, fn) {
  try { await fn(); }
  catch (err) { fail++; failures.push(`${name} THREW: ${err && err.message}`); }
}
/* Reading a record that a broken reader never returned must be a reported
   failure, not a TypeError. */
const at = (arr, i) => (Array.isArray(arr) && arr[i]) || {};

const SF_PATH = path.join(__dirname, '..', 'salesforce.js');

/* A fresh module per scenario. salesforce.js caches the access token in module
   scope, so a stale require would let one scenario's token leak into the
   next and skip the token exchange the next one is asserting on. */
function freshSalesforce() {
  delete require.cache[require.resolve(SF_PATH)];
  return require(SF_PATH);
}

/* The fetch stub. Records every request so the emitted SOQL can be asserted —
   which is the only way to check "no LIMIT" against what actually goes over
   the wire rather than against the source text that builds it. */
function stubFetch(handler) {
  const calls = [];
  global.fetch = async (url, opts) => {
    calls.push({ url: String(url), opts });
    if (String(url).includes('/services/oauth2/token')) {
      return { ok: true, status: 200,
        json: async () => ({ access_token: 'tok', instance_url: 'https://example.my.salesforce.com',
                             issued_at: String(Date.now()) }),
        text: async () => '' };
    }
    return handler(String(url), calls.length);
  };
  return calls;
}

const OK = (body) => ({ ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) });

/* One Opportunity record in the shape Salesforce actually returns, including
   the OpportunityContactRoles subquery. */
const rec = (i, qualified) => ({
  Id: '006' + String(i).padStart(5, '0'),
  Name: 'Opp ' + i,
  Qualified_Demo__c: !!qualified,
  Account: { Website: `https://co${i}.com/`, Name: 'Co ' + i },
  OpportunityContactRoles: { records: [{ Contact: { Email: `person${i}@co${i}.com` } }] },
});

const page = (recs, total, next) => ({
  totalSize: total, done: !next, records: recs,
  ...(next ? { nextRecordsUrl: next } : {}),
});

(async () => {

  /* ── 1. A single page. The only shape production has ever seen. ────── */
  await scenario('one page — the only shape production has seen', async () => {
    const sf = freshSalesforce();
    stubFetch(() => OK(page([rec(1, true), rec(2, true)], 2, null)));
    const out = await sf.findQualifiedDemoOpportunities();
    ok('one page: ok', out.ok === true, JSON.stringify(out).slice(0, 200));
    eq('one page: both records come back', out.records.length, 2);
    eq('one page: one page counted', out.pages, 1);
    eq('one page: the website is mapped', at(out.records, 0).website, 'https://co1.com/');
    eq('one page: the primary contact email is mapped', at(out.records, 0).contactEmail, 'person1@co1.com');
    eq('one page: the id is mapped', at(out.records, 1).id, '00600002');
  });

  /* ── 2. THREE PAGES. The path that had never run. ─────────────────────
     This is the bug in its original form: 260 ticked Opportunities, which
     `LIMIT 200` would have silently cut to 200 in an undefined order, with a
     newly ticked partner Opportunity able to sit in the missing 60 and the
     affiliate never paid. */
  await scenario('THREE PAGES — the path that had never run', async () => {
    const sf = freshSalesforce();
    const p1 = Array.from({ length: 100 }, (_, i) => rec(i + 1, true));
    const p2 = Array.from({ length: 100 }, (_, i) => rec(i + 101, true));
    const p3 = Array.from({ length: 60 },  (_, i) => rec(i + 201, true));
    const calls = stubFetch((url) => {
      if (url.includes('/q2')) return OK(page(p2, 260, '/q3'));
      if (url.includes('/q3')) return OK(page(p3, 260, null));
      return OK(page(p1, 260, '/q2'));
    });
    const out = await sf.findQualifiedDemoOpportunities();
    ok('3 pages: ok', out.ok === true, JSON.stringify(out).slice(0, 200));
    eq('3 pages: every record is concatenated, none lost', out.records.length, 260);
    eq('3 pages: and it counted the pages it read', out.pages, 3);
    eq('3 pages: totalSize is reported', out.totalSize, 260);
    /* Past 200 is the whole point — the old LIMIT would have returned exactly
       200 here and reported success. */
    ok('3 pages: MORE than the old LIMIT of 200 comes back', out.records.length > 200, String(out.records.length));
    /* Order preserved across pages, so the first and last are the real ends. */
    eq('3 pages: the first record is page one’s first', at(out.records, 0).id, '00600001');
    eq('3 pages: the last record is page three’s last', at(out.records, 259).id, '00600260');
    /* nextRecordsUrl comes back RELATIVE ("/services/data/..."), so it has to
       be resolved against the instance or fetch gets a bare path. Asserted
       from the captured requests rather than taken on trust. */
    const follow = calls.filter((c) => /\/q[23]$/.test(c.url)).map((c) => c.url);
    eq('3 pages: both follow-up pages were actually requested', follow.length, 2);
    ok('3 pages: each follow-up URL is absolute against the instance',
       follow.every((u) => u.startsWith('https://example.my.salesforce.com/')), follow.join(' '));
    /* And each carried the token — a paged request that drops the header 401s
       halfway through a read.

       The OAuth token POST is excluded deliberately: its URL also contains
       salesforce.com and it correctly carries no bearer header, so a filter on
       the host alone fails this assertion for the one request that is supposed
       to look like that. */
    const dataCalls = calls.filter((c) => !c.url.includes('/services/oauth2/token'));
    ok('3 pages: three data requests were made, one per page', dataCalls.length === 3, String(dataCalls.length));
    ok('3 pages: every page is authorised, not just the first',
       dataCalls.every((c) => c.opts && c.opts.headers && c.opts.headers.Authorization === 'Bearer tok'),
       JSON.stringify(dataCalls.map((c) => (c.opts && c.opts.headers) || null)));
  });

  /* ── 3. A SHORT READ must refuse, not hand back what it got. ─────────
     A partial list read as complete is the bug the pagination fixed. Handing
     back ok:true with 60% of the records reproduces it one page later. */
  await scenario('a short read must refuse', async () => {
    const sf = freshSalesforce();
    stubFetch(() => OK(page([rec(1, true)], 5, null)));   // says 5, gives 1
    const out = await sf.findQualifiedDemoOpportunities();
    eq('short read: ok is false', out.ok, false);
    eq('short read: the reason names it', out.reason, 'incomplete');
    eq('short read: NO records are handed back', out.records.length, 0);
    eq('short read: and it says how short it was', [out.fetched, out.totalSize], [1, 5]);
  });

  /* ── 4. The PAGE CAP must refuse too. ────────────────────────────────
     An endless nextRecordsUrl chain. SF_MAX_PAGES is the bound; hitting it is
     not a success with fewer records. */
  await scenario('the page cap must refuse', async () => {
    const sf = freshSalesforce();
    let n = 0;
    stubFetch(() => { n++; return OK(page([rec(n, true)], 99999, '/next' + n)); });
    const out = await sf.findQualifiedDemoOpportunities();
    eq('page cap: ok is false', out.ok, false);
    eq('page cap: the reason names it', out.reason, 'pagination_incomplete');
    eq('page cap: NO records are handed back', out.records.length, 0);
    ok('page cap: it stopped at the cap rather than looping forever', n > 1 && n <= 30, String(n));
  });

  /* ── 5. An HTTP failure is ok:false, never an empty success. ─────────
     This is the branch that mattered most: it used to `return []`, so the
     poller could not tell "no AE has ticked anything" from "Salesforce did not
     answer" and returned quietly either way. */
  await scenario('an HTTP failure is ok:false, never an empty success', async () => {
    const sf = freshSalesforce();
    stubFetch(() => ({ ok: false, status: 503, text: async () => 'Service Unavailable', json: async () => ({}) }));
    const out = await sf.findQualifiedDemoOpportunities();
    eq('http 503: ok is false', out.ok, false);
    eq('http 503: the status is in the reason', out.reason, 'http_503');
    ok('http 503: records is an empty array, not undefined', Array.isArray(out.records) && out.records.length === 0);
    /* The distinction the poller depends on. */
    ok('http 503: a failure is DISTINGUISHABLE from an empty success',
       out.ok === false);
  });

  /* ── 6. A thrown error is caught and reported, not propagated. ───────
     The poller runs on a timer with nothing awaiting it, so a Salesforce blip
     must not become an unhandled rejection that takes the process down. */
  await scenario('a thrown error is caught, not propagated', async () => {
    const sf = freshSalesforce();
    global.fetch = async () => { throw new Error('ECONNRESET'); };
    let threw = false;
    let out;
    try { out = await sf.findQualifiedDemoOpportunities(); } catch { threw = true; }
    ok('network error: it does not throw', threw === false);
    eq('network error: ok is false', out.ok, false);
    eq('network error: the reason is generic', out.reason, 'error');
    ok('network error: the message is carried for the log', /ECONNRESET/.test(out.error || ''));
  });

  /* ── 7. An EMPTY result is a SUCCESS, and must not read as a failure. ─
     The mirror image of 5. Nobody has ticked anything is the normal state
     today, and it has to be ok:true with zero records so the poller returns
     quietly instead of recording a failure every two minutes. */
  await scenario('an empty result is a SUCCESS', async () => {
    const sf = freshSalesforce();
    stubFetch(() => OK(page([], 0, null)));
    const out = await sf.findQualifiedDemoOpportunities();
    eq('empty: ok is TRUE', out.ok, true);
    eq('empty: zero records', out.records.length, 0);
  });

  /* ── 8. The EMITTED SOQL. Asserted against the wire, not the source. ─
     "The source has no LIMIT" and "no LIMIT goes to Salesforce" are different
     claims. This checks the second one. */
  await scenario('the EMITTED SOQL, asserted against the wire', async () => {
    const sf = freshSalesforce();
    const calls = stubFetch(() => OK(page([rec(1, true)], 1, null)));
    await sf.findQualifiedDemoOpportunities();
    const q = decodeURIComponent((calls.find((c) => c.url.includes('/query/')) || {}).url || '');
    ok('emitted SOQL: the query was actually sent', /FROM Opportunity/.test(q), q.slice(0, 200));
    ok('emitted SOQL: it filters on the ticked checkbox', /Qualified_Demo__c = true/.test(q));
    /* The outer result set carries no LIMIT. The subquery's LIMIT 1 is correct
       and must survive — it picks the single primary contact.

       Strip the parenthesised subquery rather than slicing from
       "FROM Opportunity": that substring matches FROM OpportunityContactRoles
       INSIDE the subquery first, which is where the legitimate LIMIT lives, so
       the naive slice reported a LIMIT on the outer query that is not there.
       The test's own bug, caught on the first run. */
    const outer = q.replace(/\([^)]*\)/g, '');
    ok('emitted SOQL: NO LIMIT on the outer result set', !/LIMIT/.test(outer), outer);
    ok('emitted SOQL: the primary-contact LIMIT 1 is still in the subquery',
       /FROM OpportunityContactRoles ORDER BY IsPrimary DESC LIMIT 1/.test(q));
    /* No date bound, deliberately: an Opportunity created before a window and
       ticked today would otherwise be invisible forever. */
    ok('emitted SOQL: no date window on the ticked query', !/LAST_N_DAYS/.test(q), q.slice(0, 200));
    /* The token was fetched before the query, and the query carried it. */
    const qc = calls.find((c) => c.url.includes('/query/'));
    eq('emitted SOQL: the query is authorised with the bearer token',
       ((qc.opts || {}).headers || {}).Authorization, 'Bearer tok');
  });

  /* ── 9. findOpportunityDomains, the sibling that HAS paginated for real.
     Asserted here too so the pair cannot drift: they are the same shape and
     the same refusals, and this one carries the date bound the other must not. */
  await scenario('findOpportunityDomains — the sibling that HAS paginated', async () => {
    const sf = freshSalesforce();
    const p1 = Array.from({ length: 2 }, (_, i) => rec(i + 1, false));
    const p2 = [rec(3, true)];
    const calls = stubFetch((url) => url.includes('/q2')
      ? OK(page(p2, 3, null))
      : OK(page(p1, 3, '/q2')));
    const out = await sf.findOpportunityDomains({ sinceDays: 180 });
    ok('domains: ok', out.ok === true, JSON.stringify(out).slice(0, 200));
    eq('domains: all three records across two pages', out.records.length, 3);
    eq('domains: the ticked flag rides along on the same query', at(out.records, 2).qualified, true);
    eq('domains: and the unticked ones are false', at(out.records, 0).qualified, false);
    const q = decodeURIComponent((calls.find((c) => c.url.includes('/query/')) || {}).url || '');
    ok('domains: THIS one is bounded to a date window', /LAST_N_DAYS:180/.test(q), q.slice(0, 200));
    ok('domains: and it has no LIMIT either', !/LIMIT/.test(q.replace(/\([^)]*\)/g, '')),
       q.replace(/\([^)]*\)/g, ''));
  });
  await scenario('findOpportunityDomains — a short read refuses too', async () => {
    const sf = freshSalesforce();
    stubFetch(() => OK(page([rec(1, false)], 9, null)));
    const out = await sf.findOpportunityDomains({});
    eq('domains: a short read refuses, same as the ticked query', out.ok, false);
    eq('domains: with the same reason string', out.reason, 'incomplete');
    eq('domains: and no records', out.records.length, 0);
  });

  /* ── 10. updateOpportunityFields — THE FIRST WRITE TO OPPORTUNITY ────
     Everything else this module does on Opportunity is read-only, so a write
     rejection has never been exercised once in production. It will keep
     succeeding while the integration user is a System Administrator, and
     start 403ing the day someone reduces that profile — which is an open
     ticket. So the failure paths are exercised HERE, because they cannot be
     exercised there.

     A silent failure would look exactly like a partner with no Opportunity,
     which the Partners tab renders for real reasons. That is why this returns
     a discriminated result rather than a boolean. */
  await scenario('updateOpportunityFields — the first write to Opportunity', async () => {
    const sf = freshSalesforce();

    /* A successful PATCH is 204 with NO body. Treating "no body" as a failure
       would make every successful write look broken. */
    let calls = stubFetch(() => ({ status: 204, ok: true, text: async () => '', json: async () => ({}) }));
    let out = await sf.updateOpportunityFields('006ABC', { Partner_Source__c: 'Acme Partners' });
    eq('oppWrite: a 204 with no body is a SUCCESS', out.ok, true);
    const req = calls.find((c) => c.url.includes('/sobjects/Opportunity/'));
    ok('oppWrite: it PATCHes the Opportunity by id', req && req.opts.method === 'PATCH', req && req.url);
    ok('oppWrite: the id is in the path', req.url.endsWith('/sobjects/Opportunity/006ABC'));
    eq('oppWrite: the field is in the body', JSON.parse(req.opts.body), { Partner_Source__c: 'Acme Partners' });
    eq('oppWrite: authorised with the bearer token', req.opts.headers.Authorization, 'Bearer tok');

    /* ── The failure that has never happened in production ──────────
       403, and the two Salesforce error codes that mean "no field-level
       access". Creating a field through the Tooling API does not grant access
       to it — both fields created on 4 Sept came back 201 and were invisible
       to the user that created them. A 400 INVALID_FIELD_FOR_INSERT_UPDATE
       therefore means a permission problem, not a missing field, and the two
       read identically from outside. */
    for (const [status, body, label] of [
      [403, 'INSUFFICIENT_ACCESS_OR_READONLY', 'a 403'],
      [400, '[{"errorCode":"INVALID_FIELD_FOR_INSERT_UPDATE","message":"Unable to create/update fields: Partner_Source__c"}]', 'a 400 with no field access'],
      [401, 'Session expired or invalid', 'a 401'],
    ]) {
      freshSalesforce();
      const s2 = freshSalesforce();
      stubFetch(() => ({ status, ok: false, text: async () => body, json: async () => ({}) }));
      const r = await s2.updateOpportunityFields('006ABC', { Partner_Source__c: 'X' });
      eq(`oppWrite: ${label} is ok:false`, r.ok, false);
      eq(`oppWrite: ${label} is classified as a PERMISSION problem`, r.reason, 'permission');
      ok(`oppWrite: ${label} carries the body for the alert`, !!r.body);
    }

    /* An ordinary per-record failure must NOT be classified as permission —
       it affects one domain and a retry may fix it, whereas a permission
       failure affects every domain and needs Salesforce setup changed. */
    {
      const s3 = freshSalesforce();
      stubFetch(() => ({ status: 404, ok: false, text: async () => 'NOT_FOUND', json: async () => ({}) }));
      const r = await s3.updateOpportunityFields('006GONE', { Partner_Source__c: 'X' });
      eq('oppWrite: a 404 is NOT a permission problem', r.reason, 'http_404');
    }

    /* Never throws: the caller is a 15-minute sweep with nothing awaiting it,
       so an unhandled rejection would be an unhandled rejection in the
       process. */
    {
      const s4 = freshSalesforce();
      global.fetch = async () => { throw new Error('ETIMEDOUT'); };
      let threw = false, r;
      try { r = await s4.updateOpportunityFields('006ABC', { Partner_Source__c: 'X' }); } catch { threw = true; }
      ok('oppWrite: a network error does not throw', threw === false);
      /* (r || {}) because if it DOES throw, r is undefined and reading .reason
         aborts the rest of this scenario — taking the guard assertions below
         down with it and turning a caught mutation into an unmeasured one.
         Same discipline as the `at()` helper above. */
      eq('oppWrite: and is reported as an error', (r || {}).reason, 'error');
    }

    /* Guards, so a missing id or an empty object cannot become a PATCH to
       /sobjects/Opportunity/undefined. */
    {
      const s5 = freshSalesforce();
      const seen = stubFetch(() => ({ status: 204, ok: true, text: async () => '', json: async () => ({}) }));
      eq('oppWrite: no id is refused', (await s5.updateOpportunityFields(null, { a: 1 })).reason, 'no_opportunity_id');
      eq('oppWrite: no fields is refused', (await s5.updateOpportunityFields('006ABC', {})).reason, 'no_fields');
      eq('oppWrite: and neither reached Salesforce', seen.filter((c) => c.url.includes('/sobjects/')).length, 0);
    }
  });

  console.log('');
  if (failures.length) {
    console.log('  FAILURES:');
    for (const f of failures) console.log('   ✗ ' + f);
  }
  console.log(`  passed: ${pass}`);
  console.log(`  failed: ${fail}`);
  console.log('');
  process.exit(fail ? 1 : 0);
})();
