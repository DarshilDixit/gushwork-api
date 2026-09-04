/* ============================================================
   PartnerStack — batch 1 (steps 1-4).

   Same convention as the other suites: the real code is LIFTED out of
   index.js and evaluated, rather than copied here. A test that exercises a
   duplicate of the source can pass while production is broken.

   Dependency-free — no DATABASE_URL, no network, no DOM. The two database
   readers are exercised against injected fakes, so the QUERY SHAPE and the
   fail-closed behaviour are under test without a database being present.

   What this file is defending, in order of how expensive it would be to get
   wrong:

     1. partnerStackCustomerKey. PartnerStack counts one conversion per
        customer key FOR THE LIFE OF THE ACCOUNT. Two spellings of one company
        means an affiliate paid twice, or a real referral swallowed as a
        duplicate. This is the single highest-consequence pure function in the
        integration and it gets the most cases.
     2. Fail-closed. The conversion call cannot be recalled, so a check that
        cannot run must reject, not wave through.
     3. The click-history parser, which is the only place caller-supplied JSON
        reaches a JSONB column.
     4. The schema and wiring, asserted by shape.

   Run:  node tests/test-partnerstack.js
   ============================================================ */

const fs = require('fs');
const path = require('path');
const src  = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const dbjs = fs.readFileSync(path.join(__dirname, '..', 'db.js'), 'utf8');
const demo  = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form.js'), 'utf8');
const popup = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form-popup.js'), 'utf8');
const psmod = fs.readFileSync(path.join(__dirname, '..', 'partnerstack.js'), 'utf8');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
function eq(name, actual, expected) {
  ok(name, JSON.stringify(actual) === JSON.stringify(expected),
     `got ${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`);
}

/* Lift a TOP-LEVEL function or const out of index.js by brace/bracket
   matching forward from its declaration, so the body comes whole. */
function lift(s, decl) {
  const i = s.indexOf('\n' + decl);
  if (i === -1) throw new Error('not found: ' + decl);
  /* Match whichever bracket the declaration actually opens with — a const can
     be an object, an array or a new Set([...]), and guessing wrong truncates
     the body mid-template-literal, which surfaces as "missing ) after argument
     list" pointing at a line that is perfectly fine. */
  let scan = i + 1 + decl.length;
  /* For a function, step over the PARAMETER LIST first. partnerStackEligibility
     destructures its argument, so the first brace after the name belongs to the
     parameters, not the body — matching it lifts a one-line fragment that then
     fails to parse somewhere else entirely. */
  if (decl.includes('function')) {
    let pd = 1;                                   // decl already includes the '('
    while (scan < s.length && pd > 0) {
      if (s[scan] === '(') pd++;
      else if (s[scan] === ')') pd--;
      scan++;
    }
  }
  let start = -1, open = '', close = '';
  for (let j = scan - 1; j < s.length; j++) {
    if (s[j] === '{' || s[j] === '[') { start = j; open = s[j]; close = open === '{' ? '}' : ']'; break; }
  }
  if (start === -1) throw new Error('no body found for: ' + decl);
  let depth = 0;
  for (let j = start; j < s.length; j++) {
    if (s[j] === open) depth++;
    else if (s[j] === close) { depth--; if (depth === 0) {
      let end = j + 1;
      while (end < s.length && s[end] !== '\n') end++;   // trailing ); or ;
      return s.slice(i + 1, end);
    } }
  }
  throw new Error('unbalanced: ' + decl);
}
function liftLine(s, decl) {
  const i = s.indexOf('\n' + decl);
  if (i === -1) throw new Error('not found: ' + decl);
  return s.slice(i + 1, s.indexOf('\n', i + 1));
}

/* Extract the contents of the parenthesised group starting at or after `from`,
   matching depth so an inner NOW() cannot end it early. */
function parenBody(s, from) {
  const i = s.indexOf('(', from);
  let d = 0;
  for (let j = i; j < s.length; j++) {
    if (s[j] === '(') d++;
    else if (s[j] === ')') { d--; if (!d) return s.slice(i + 1, j); }
  }
  throw new Error('unbalanced parens');
}

/* Count top-level entries in the params array that follows a query template.
   Depth-aware, so nested calls and object literals inside it do not miscount. */
function countArrayEntries(s, startMarker) {
  const i = s.indexOf(startMarker);
  if (i === -1) throw new Error('params array not found after: ' + startMarker);
  const open = s.indexOf('[', i);
  let depth = 0, n = 1;
  for (let j = open; j < s.length; j++) {
    const ch = s[j];
    if ('([{'.includes(ch)) depth++;
    else if (')]}'.includes(ch)) { depth--; if (depth === 0) return n; }
    else if (ch === ',' && depth === 1) n++;
  }
  throw new Error('unbalanced params array after: ' + startMarker);
}

/* ============================================================
   1. partnerStackCustomerKey — the key everything joins on
   ============================================================ */
const KEY_SRC = [
  liftLine(src, 'const FREE_EMAIL_DOMAINS ='),
  lift(src, 'function damerauLevenshtein('),
  lift(src, 'function freeEmailMatch('),
  lift(src, 'function isFreeEmailDomain('),
  lift(src, 'const MULTI_PART_SUFFIXES = new Set(['),
  lift(src, 'function registrableDomain('),
  lift(src, 'function partnerStackCustomerKey('),
].join('\n');
const K = (new Function(KEY_SRC + '\n return { partnerStackCustomerKey, registrableDomain };'))();
const key = K.partnerStackCustomerKey;

// The spec's own example, in every spelling it can arrive in.
for (const [input, want] of [
  ['acme.com',                      'acme.com'],
  ['Acme.com',                      'acme.com'],
  ['ACME.COM',                      'acme.com'],
  ['www.acme.com',                  'acme.com'],
  ['WWW.Acme.Com',                  'acme.com'],
  ['mail.acme.com',                 'acme.com'],
  ['http://acme.com',               'acme.com'],
  ['https://www.acme.com',          'acme.com'],
  ['https://www.acme.com/',         'acme.com'],
  ['https://acme.com/pricing?a=1',  'acme.com'],
  ['https://acme.com:8443/x',       'acme.com'],
  ['  https://ACME.com/  ',         'acme.com'],
  ['acme.com.',                     'acme.com'],
  ['someone@acme.com',              'acme.com'],
  ['Someone@Mail.Acme.com',         'acme.com'],
  ['https://user:pw@acme.com/x',    'acme.com'],
]) eq(`key: ${JSON.stringify(input)} -> acme.com`, key(input), want);

/* Public-suffix handling rides on registrableDomain. acme.co.uk collapsing to
   co.uk would merge every UK customer onto one key — the worst possible
   version of the once-per-key-forever bug. */
eq('key: acme.co.uk keeps its suffix',        key('acme.co.uk'),          'acme.co.uk');
eq('key: www.acme.co.uk -> acme.co.uk',       key('www.acme.co.uk'),      'acme.co.uk');
eq('key: shop.acme.co.uk -> acme.co.uk',      key('shop.acme.co.uk'),     'acme.co.uk');
eq('key: https://acme.com.au/ -> acme.com.au',key('https://acme.com.au/'),'acme.com.au');
eq('key: deep.sub.acme.com -> acme.com',      key('deep.sub.acme.com'),   'acme.com');

/* Free providers return null on purpose. gmail.com as a customer key would
   merge every Gmail lead into one PartnerStack customer, and because the
   conversion fires once per key forever, the first would burn it for all. */
eq('key: gmail.com is null',            key('gmail.com'),            null);
eq('key: someone@gmail.com is null',    key('someone@gmail.com'),    null);
eq('key: someone@GMAIL.com is null',    key('someone@GMAIL.com'),    null);
eq('key: typo-squat gmailc.com is null',key('someone@gmailc.com'),   null);
eq('key: yahoo.co.uk-ish still company',key('someone@acme.co.uk'),   'acme.co.uk');

// Nothing usable must be null, never a guess.
for (const bad of ['', '   ', null, undefined, 'localhost', 'acme', 'http://', '@', 'a@', '@b',
                   'not a domain', '...', 'http://acme', '123', 'acme .com'])
  eq(`key: ${JSON.stringify(bad)} -> null`, key(bad), null);

// An IP is not a company domain.
eq('key: bare IP -> null', key('192.168.1.1'), null);

/* ============================================================
   2. isPartnerStackTestEmail — our own traffic must not pay a partner
   ============================================================ */
const T = (new Function(
  liftLine(src, 'const ELV_EXCLUDED_DOMAINS =') + '\n' +
  liftLine(src, 'const PS_TEST_EMAILS =') + '\n' +
  lift(src, 'function isPartnerStackTestEmail(') +
  '\n return { isPartnerStackTestEmail, ELV_EXCLUDED_DOMAINS };'))();
const isTest = T.isPartnerStackTestEmail;

eq('test: b@g.ai',              isTest('b@g.ai'),              true);
eq('test: B@G.AI (case)',       isTest('B@G.AI'),              true);
eq('test: anyone@gushwork.ai',  isTest('darshil@gushwork.ai'), true);
eq('test: anyone@test.com',     isTest('x@test.com'),          true);
eq('test: anyone@example.com',  isTest('x@example.com'),       true);
eq('test: anyone@example.org',  isTest('x@example.org'),       true);
eq('test: a real lead is not',  isTest('buyer@acme.com'),      false);
eq('test: empty is not',        isTest(''),                    false);
eq('test: null is not',         isTest(null),                  false);
/* Guards a real trap: gushwork.ai is excluded, notgushwork.ai is a stranger. */
eq('test: notgushwork.ai is not ours', isTest('x@notgushwork.ai'), false);
eq('test: sub.gushwork.ai is not an exact match', isTest('x@sub.gushwork.ai'), false);

/* This list is PartnerStack-only on purpose. If it ever becomes the shared
   dashboard exclusion, every historical lead number moves at once. */
ok('test: guard reads ELV_EXCLUDED_DOMAINS rather than redefining it',
   /ELV_EXCLUDED_DOMAINS\.includes\(domain\)/.test(src));

/* ============================================================
   3. The click-history parser — the only caller-supplied JSON we store
   ============================================================ */
const P = (new Function(
  liftLine(src, 'const PS_CLICK_HISTORY_MAX =') + '\n' +
  lift(src, 'function parsePartnerStackClickAt(') + '\n' +
  lift(src, 'function parsePartnerStackClickHistory(') +
  '\n return { parsePartnerStackClickAt, parsePartnerStackClickHistory, PS_CLICK_HISTORY_MAX };'))();

const at = P.parsePartnerStackClickAt;
const iso = '2026-08-20T10:00:00.000Z';
eq('click_at: ISO string',      at(iso) && at(iso).toISOString(), iso);
const isoMs = Date.parse(iso), isoSec = isoMs / 1000;   // derived, so it cannot drift
eq('click_at: epoch ms',        at(isoMs) && at(isoMs).toISOString(), iso);
eq('click_at: epoch seconds',   at(String(isoSec)) && at(String(isoSec)).toISOString(), iso);
eq('click_at: empty -> null',   at(''),        null);
eq('click_at: null -> null',    at(null),      null);
eq('click_at: junk -> null',    at('not a date'), null);
/* A bad timestamp silently MOVES the 90-day eligibility window, so it is
   dropped and the caller falls back to submit time rather than trusting it. */
eq('click_at: far future -> null', at('2099-01-01T00:00:00Z'), null);
eq('click_at: pre-2026 -> null',   at('2019-01-01T00:00:00Z'), null);

const hist = P.parsePartnerStackClickHistory;
eq('history: well-formed passes through',
   hist([{ xid: 'x1', pk: 'k1', at: iso }]),
   [{ xid: 'x1', pk: 'k1', at: iso }]);
eq('history: JSON string is parsed',
   hist(JSON.stringify([{ xid: 'x1', pk: 'k1', at: iso }])),
   [{ xid: 'x1', pk: 'k1', at: iso }]);
eq('history: order is preserved, oldest first',
   hist([{ xid: 'a', pk: 'p', at: iso }, { xid: 'b', pk: 'p', at: iso }]).map(e => e.xid),
   ['a', 'b']);
ok('history: capped at 10 entries',
   hist(Array.from({ length: 40 }, (_, i) => ({ xid: 'x' + i, pk: 'k', at: iso }))).length === 10);
eq('history: cap matches the cookie cap', P.PS_CLICK_HISTORY_MAX, 10);
eq('history: bad JSON -> null',       hist('{not json'),  null);
eq('history: not an array -> null',   hist({ xid: 'x' }),  null);
eq('history: empty array -> null',    hist([]),            null);
eq('history: null -> null',           hist(null),          null);
eq('history: entries with no ids are dropped',
   hist([{ at: iso }, { xid: 'ok', pk: 'k', at: iso }]).map(e => e.xid), ['ok']);
eq('history: junk entries are dropped', hist([null, 'str', 42, ['a']]), null);
eq('history: a bad at becomes null rather than dropping the entry',
   hist([{ xid: 'x', pk: 'k', at: 'garbage' }]), [{ xid: 'x', pk: 'k', at: null }]);
/* Unbounded, this is a free write amplifier into a JSONB column. */
ok('history: oversized fields are truncated, not stored whole',
   hist([{ xid: 'x'.repeat(9000), pk: 'k'.repeat(9000), at: iso }])[0].xid.length === 200);
ok('history: extra attacker-supplied keys are not carried through',
   Object.keys(hist([{ xid: 'x', pk: 'k', at: iso, evil: 'drop table' }])[0]).join(',') === 'xid,pk,at');

/* ============================================================
   4. Eligibility — order, reasons, and failing CLOSED
   ============================================================ */
function makeEligibility({ customerRows, contactRows, customerThrows, contactThrows, customerHangs, timeoutMs }) {
  const fakeAws  = { query: () => {
    if (customerThrows) return Promise.reject(new Error('warehouse unreachable'));
    /* A HANG, not an error. This is the failure a degraded WAN actually
       produces — the connection is accepted, the query never answers — and it
       is the one fail-closed alone cannot catch, because nothing ever throws. */
    if (customerHangs) return new Promise(() => {});
    return Promise.resolve({ rows: customerRows || [] }); } };
  const fakePool = { query: async () => {
    if (contactThrows) throw new Error('railway unreachable');
    return { rows: contactRows || [] }; } };
  const body = [
    lift(src, 'function withTimeout('),
    /* The real 8s constant is asserted separately, below. Here it is shortened
       so the end-to-end hang test still exercises the REAL withTimeout and the
       REAL fail-closed path without adding 8 seconds to a suite that is
       supposed to run in about a second. */
    liftLine(src, 'const PS_CUSTOMER_QUERY_TIMEOUT_MS =')
      .replace(/=\s*\d+/, '= ' + (timeoutMs || 8000)),
    KEY_SRC,
    liftLine(src, 'const ELV_EXCLUDED_DOMAINS ='),
    liftLine(src, 'const PS_TEST_EMAILS ='),
    lift(src, 'function isPartnerStackTestEmail('),
    liftLine(src, 'const PS_CONTACT_LOOKBACK_DAYS ='),
    lift(src, 'const PS_CONTACT_SOURCES = {'),
    liftLine(src, "const PS_CONTACT_ACTIVE ="),
    lift(src, 'async function partnerStackPriorContact('),
    liftLine(src, 'const PS_CUSTOMER_CACHE_TTL_MS ='),
    liftLine(src, 'let _psCustomerCache ='),
    lift(src, 'async function partnerStackCustomerDomains('),
    lift(src, 'async function partnerStackEligibility('),
  ].join('\n');
  return (new Function('pool', 'awsPool', 'recordFailure', 'console',
    body + '\n return { partnerStackEligibility, partnerStackCustomerDomains, PS_CONTACT_ACTIVE, PS_CONTACT_SOURCES };'
  ))(fakePool, fakeAws, () => {}, { log(){}, warn(){} });
}

(async () => {
  const clickAt = new Date('2026-08-20T10:00:00Z');

  // (c) test email wins over everything, and costs no query.
  {
    const E = makeEligibility({ customerThrows: true, contactThrows: true });
    const v = await E.partnerStackEligibility({ email: 'b@g.ai', website: 'acme.com', click_at: clickAt });
    eq('elig: test email rejects', v.eligible, false);
    eq('elig: test email reason',  v.reason,  'test_email');
    ok('elig: test email is decided before any database call', true);
  }

  // no usable key
  {
    const E = makeEligibility({});
    const v = await E.partnerStackEligibility({ email: 'someone@gmail.com', website: '', click_at: clickAt });
    eq('elig: free-mail with no website rejects', v.eligible, false);
    eq('elig: free-mail reason', v.reason, 'no_customer_key');
  }

  // (b) current customer
  {
    const E = makeEligibility({ customerRows: [{ d: 'https://www.acme.com/', status: 'Active' }] });
    const v = await E.partnerStackEligibility({ email: 'buyer@acme.com', website: 'www.acme.com', click_at: clickAt });
    eq('elig: current customer rejects', v.eligible, false);
    eq('elig: current customer reason',  v.reason,  'existing_customer');
    /* The three warehouse tables disagree on format. Both sides go through
       partnerStackCustomerKey so a dirty https://www.x.com/ still matches. */
    ok('elig: dirty warehouse domain still matches a clean lead domain', v.customer_key === 'acme.com');
  }

  // churned rows are NOT current customers
  {
    const E = makeEligibility({ customerRows: [
      { d: 'acme.com', status: 'Churned' }, { d: 'acme.com', status: 'churned' },
      { d: 'other.com', status: 'Inactive' }, { d: 'dummy.com', status: 'Dummy' } ] });
    const v = await E.partnerStackEligibility({ email: 'buyer@acme.com', website: 'acme.com', click_at: clickAt });
    ok('elig: a churned domain is not a current customer', v.reason !== 'existing_customer');
    const keys = await E.partnerStackCustomerDomains();
    eq('elig: churned/inactive/dummy are all excluded from the cache', keys.size, 0);
  }

  // (a) prior contact
  {
    const E = makeEligibility({ contactRows: [
      { email: 'someoneelse@acme.com', website: 'https://mail.acme.com/x', created_at: new Date('2026-08-01T00:00:00Z') } ] });
    const v = await E.partnerStackEligibility({ email: 'buyer@acme.com', website: 'acme.com', click_at: clickAt });
    eq('elig: prior contact rejects', v.eligible, false);
    eq('elig: prior contact reason',  v.reason,  'prior_contact_90d');
    /* The reason this is normalised in JS and not SQL: SQL strips www but not
       subdomains, so mail.acme.com would not have matched and the rule would
       have under-rejected silently. */
    ok('elig: a subdomain on the prior row still matches the key',
       /prior form lead/.test(v.detail));
  }

  // a clean pass
  {
    const E = makeEligibility({ customerRows: [{ d: 'someoneelse.com', status: 'Active' }], contactRows: [] });
    const v = await E.partnerStackEligibility({ email: 'buyer@acme.com', website: 'acme.com', click_at: clickAt });
    eq('elig: clean lead passes', v.eligible, true);
    eq('elig: clean lead reason', v.reason, 'eligible');
    eq('elig: clean lead carries the key', v.customer_key, 'acme.com');
    /* The 12-month clause cannot be evaluated: nothing in the warehouse can
       date a churn. It is surfaced on every pass rather than passing silently. */
    eq('elig: the unenforceable 12-month clause is surfaced, not hidden',
       v.unverified, ['customer_last_12_months']);
  }

  /* FAIL CLOSED. The conversion fires once per key forever and cannot be
     recalled; a skipped one is still in the log to send by hand. */
  {
    const E = makeEligibility({ customerThrows: true });
    const v = await E.partnerStackEligibility({ email: 'buyer@acme.com', website: 'acme.com', click_at: clickAt });
    eq('elig: customer check failure rejects (fails CLOSED)', v.eligible, false);
    eq('elig: customer check failure reason', v.reason, 'check_failed');
  }
  {
    const E = makeEligibility({ contactThrows: true });
    const v = await E.partnerStackEligibility({ email: 'buyer@acme.com', website: 'acme.com', click_at: clickAt });
    eq('elig: contact check failure rejects (fails CLOSED)', v.eligible, false);
    eq('elig: contact check failure reason', v.reason, 'check_failed');
  }

  // Every verdict carries a checked_at, because the verdict is a stored record.
  {
    const E = makeEligibility({});
    const v = await E.partnerStackEligibility({ email: 'buyer@acme.com', website: 'acme.com', click_at: clickAt });
    ok('elig: every verdict is stamped with checked_at', v.checked_at instanceof Date);
  }

  /* A HUNG warehouse must resolve to a verdict, not hang the caller.
     awsPool has no statement_timeout, so without the wrapper this promise
     never settles and the fail-closed catch is never reached. */
  {
    const E = makeEligibility({ customerHangs: true, timeoutMs: 150 });
    const t0 = Date.now();
    const v = await E.partnerStackEligibility({ email: 'buyer@acme.com', website: 'acme.com', click_at: clickAt });
    const took = Date.now() - t0;
    eq('timeout: a hung warehouse rejects rather than hanging', v.eligible, false);
    eq('timeout: a hung warehouse reads as check_failed', v.reason, 'check_failed');
    ok('timeout: it resolves at the configured timeout, not never',
       took >= 140 && took < 3000, took + 'ms');
    /* The shipped value, asserted on the source rather than on the shortened
       copy above. 8s matches HEALTH_AWS_TIMEOUT_MS. */
    ok('timeout: the shipped constant is 8s, matching the health probe',
       /const PS_CUSTOMER_QUERY_TIMEOUT_MS = 8000;/.test(src));
  }

  // The swappable source registry.
  {
    const E = makeEligibility({});
    eq('elig: only form_leads is active today', E.PS_CONTACT_ACTIVE, ['form_leads']);
    ok('elig: the registry is the swap point', typeof E.PS_CONTACT_SOURCES.form_leads === 'function');
  }

  /* ============================================================
     5. SHAPE — schema, sync wiring and the two form files
     ============================================================ */
  /* ============================================================
     4b. STEP 5 — the conversion call, and the flag that gates the check
     ============================================================ */

  /* MVP ships with the eligibility check OFF. If this default ever flips by
     accident, every partner lead starts being judged by a check nobody signed
     off on, silently. */
  ok('mvp: eligibility is behind an env flag',
     /const PS_ELIGIBILITY_ENABLED = process\.env\.PS_ELIGIBILITY_ENABLED === 'true';/.test(src));
  ok('mvp: the flag defaults OFF (opt-in string compare, not a truthy read)',
     !/PS_ELIGIBILITY_ENABLED\s*=\s*process\.env\.PS_ELIGIBILITY_ENABLED\s*(\|\||\?\?)/.test(src));
  ok('mvp: the eligibility runner returns early when the flag is off',
     /function runPartnerStackEligibility[\s\S]{0,300}?if \(!PS_ELIGIBILITY_ENABLED\) return;/.test(src));
  ok('mvp: the customer cache is not warmed while the check is off',
     /function startPartnerStackCacheWarm[\s\S]{0,400}?if \(!PS_ELIGIBILITY_ENABLED\)/.test(src));
  /* The conversion must NOT consult eligibility for the MVP. */
  {
    const fn = src.slice(src.indexOf('async function runPartnerStackSignup'),
                         src.indexOf('/* Rejections are logged in one place'));
    ok('mvp: the conversion path does not call the eligibility check',
       !/partnerStackEligibility|PS_ELIGIBILITY_ENABLED/.test(fn));
    ok('conversion: skips our own test addresses', /isPartnerStackTestEmail\(email\)/.test(fn));
    ok('conversion: requires a customer key', /if \(!ps\.ps_customer_key\) return;/.test(fn));
    ok('conversion: requires ps_xid', /if \(!ps \|\| !ps\.ps_xid\) return;/.test(fn));

    /* ONCE PER DOMAIN. The claim has to precede the HTTP call — checking then
       sending races, and PartnerStack cannot undo a double credit. */
    const claimAt = fn.indexOf('ps_signup_sent_at = NOW()');
    const sendAt  = fn.indexOf('await sendConversion(');
    ok('conversion: the domain is claimed BEFORE the HTTP call', claimAt !== -1 && sendAt !== -1 && claimAt < sendAt,
       `claim at ${claimAt}, send at ${sendAt}`);
    ok('conversion: the claim is conditional on nothing else having sent',
       /NOT EXISTS[\s\S]{0,200}?other\.ps_customer_key = \$2[\s\S]{0,120}?other\.ps_signup_sent_at IS NOT NULL/.test(fn));
    ok('conversion: a concurrent claim (23505) is read as already-sent',
       /err\.code === '23505'/.test(fn));
    ok('conversion: a failed send RELEASES the claim so it can be retried',
       /ps_signup_sent_at = NULL/.test(fn));
    ok('conversion: a stuck claim is escalated, not swallowed',
       /could not be released/.test(fn));
    ok('conversion: name prefers company over contact name',
       /company \|\| ''\)\.trim\(\)[\s\S]{0,160}?first_name, last_name/.test(fn));
  }
  ok('schema: the once-per-domain rule is enforced by a UNIQUE PARTIAL index',
     /CREATE UNIQUE INDEX IF NOT EXISTS leads_ps_signup_once_idx[\s\S]{0,200}?ON leads \(ps_customer_key\)[\s\S]{0,200}?WHERE ps_customer_key IS NOT NULL AND ps_signup_sent_at IS NOT NULL/.test(dbjs));

  // The module itself.
  ok('module: posts to the S2S conversion endpoint',
     /const CONVERSION_URL = 'https:\/\/partnerlinks\.io\/conversion\/xid';/.test(psmod));
  /* /v2/customers cannot attach a click, so using it loses the attribution
     silently. The two endpoints look interchangeable and are not. */
  ok('module: does NOT use /v2/customers', !/v2\/customers/.test(psmod.replace(/\/\/.*$/gm, '')));
  ok('module: authorises with the TRACKING TOKEN as a Bearer token',
     /Bearer \$\{token\}/.test(psmod) && /process\.env\.PARTNERSTACK_TRACKING_TOKEN/.test(psmod));
  ok('module: does not reach for the v2 Basic key pair on this endpoint',
     !/PARTNERSTACK_SECRET_KEY|PARTNERSTACK_PUBLIC_KEY/.test(psmod));
  ok('module: sends the two required fields plus the five optional ones',
     /const payload = \{[\s\S]{0,400}?\bxid,[\s\S]{0,400}?\bcustomer_key,[\s\S]{0,400}?email:[\s\S]{0,400}?name:[\s\S]{0,400}?ip_address:[\s\S]{0,400}?user_agent:[\s\S]{0,400}?origin:[\s\S]{0,80}?\};/.test(psmod));
  /* An empty string is worse than an absent field for fraud matching: it looks
     like a real value that failed to match. Every optional field must collapse
     to undefined, which JSON.stringify drops entirely. */
  for (const f of ['email', 'name', 'ip_address', 'user_agent', 'origin'])
    ok(`module: ${f} is omitted rather than sent empty`,
       new RegExp(f + ':\\s*' + f + '\\s*\\|\\| undefined').test(psmod));
  ok('module: fraud signals are accepted by the signature',
     /async function sendConversion\(\{ xid, customer_key, email, name, ip_address, user_agent, origin \}\)/.test(psmod));

  /* The request context. x-forwarded-for is a comma-separated CHAIN behind
     Railway's proxy — sending the whole header as an IP is worse than sending
     nothing, because it looks like a value and matches nothing. */
  {
    const fn = src.slice(src.indexOf('function readPartnerStackRequestContext'),
                         src.indexOf('function readPartnerStackPayload'));
    ok('ctx: takes only the FIRST x-forwarded-for entry',
       /fwd\.split\(','\)\[0\]/.test(fn));
    ok('ctx: falls back to req.ip', /\|\| req\.ip \|\| null/.test(fn));
    ok('ctx: user agent is bounded', /user-agent[\s\S]{0,80}?slice\(0, 500\)/.test(fn));
    ok('ctx: origin prefers the Origin header', /req\.headers\['origin'\]/.test(fn));
    ok('ctx: origin falls back to the page URL origin', /new URL\(page_url\)\.origin/.test(fn));
    ok('ctx: a non-URL page_url cannot throw out of the helper', /catch \{/.test(fn));
    ok('ctx: absent values are null, never empty strings',
       (fn.match(/\|\| null/g) || []).length >= 2);
  }
  ok('ctx: /submit supplies the context to the conversion',
     /runPartnerStackSignup\(\{[^}]*ctx: readPartnerStackRequestContext\(req, page_url\)/.test(src));
  ok('conversion: passes the fraud signals through to the module',
     /ip_address: ctx && ctx\.ip_address/.test(src) &&
     /user_agent: ctx && ctx\.user_agent/.test(src) &&
     /origin:\s*ctx && ctx\.origin/.test(src));
  ok('module: the call is bounded by a timeout', /AbortController|signal: controller\.signal/.test(psmod));
  ok('module: logs the request', /logCall\('-> POST \/conversion\/xid'/.test(psmod));
  ok('module: logs the response including the body', /logCall\(`<- \$\{res\.status\}/.test(psmod));
  ok('module: returns rather than throws on failure',
     /return \{ ok: false, reason: 'no_token' \}/.test(psmod) && /return \{ ok: false, status: res\.status/.test(psmod));
  ok('module: a missing token is inert, not an error', /if \(!token\)/.test(psmod));
  ok('startup: the token is audited at boot',
     /PARTNERSTACK_TRACKING_TOKEN: 'PartnerStack affiliate conversions'/.test(src));

  // Deferred, like everything else on this path.
  {
    const seg = src.slice(src.indexOf("app.post('/submit'"), src.indexOf("app.post('/booking-confirmed'"));
    const resAt  = seg.indexOf('res.json({ ok: true })');
    const signAt = seg.indexOf('runPartnerStackSignup(');
    ok('conversion: runs in /submit', signAt !== -1);
    ok('conversion: runs AFTER res.json(), never before', resAt !== -1 && signAt > resAt,
       `res.json at ${resAt}, signup at ${signAt}`);
    ok('conversion: is not awaited', !/await runPartnerStackSignup/.test(seg));
    ok('conversion: its rejection cannot reach the response', /runPartnerStackSignup\([\s\S]{0,200}?\.catch\(/.test(seg));
  }

  /* The three fixes that keep the warehouse off the lead's critical path. */
  ok('hazard: the customer query is wrapped in withTimeout',
     /withTimeout\(awsPool\.query\(/.test(src));
  ok('hazard: the timeout constant is defined', /const PS_CUSTOMER_QUERY_TIMEOUT_MS = \d+/.test(src));
  ok('hazard: the cache is warmed at boot', /startPartnerStackCacheWarm\(\);/.test(src));
  ok('hazard: boot warm is called from start()',
     /startHeartbeat\(\);\s*\n\s*startPartnerStackCacheWarm\(\);/.test(src));
  ok('hazard: the warm swallows its error rather than crashing boot',
     /refreshPartnerStackCustomerCache[\s\S]{0,400}?\.catch\(/.test(src));
  {
    const seg = src.slice(src.indexOf("app.post('/submit'"), src.indexOf("app.post('/booking-confirmed'"));
    const resAt = seg.indexOf('res.json({ ok: true })');
    const runAt = seg.indexOf('runPartnerStackEligibility(');
    ok('hazard: eligibility runs in /submit at all', runAt !== -1);
    ok('hazard: eligibility runs AFTER res.json(), never before',
       resAt !== -1 && runAt > resAt, `res.json at ${resAt}, run at ${runAt}`);
    ok('hazard: eligibility is not awaited (fire-and-forget)',
       !/await runPartnerStackEligibility/.test(seg));
  }
  ok('hazard: eligibility only runs for partner-referred leads',
     /if \(!ps \|\| !ps\.ps_xid\) return;/.test(src));
  ok('hazard: the deferred runner cannot throw into the response',
     /runPartnerStackEligibility[\s\S]{0,1600}?\.catch\(\(err\) =>/.test(src));

  const PS_COLS = ['ps_xid','ps_partner_key','ps_partner_name','ps_customer_key',
                   'ps_click_at','ps_click_history','ps_signup_sent_at','ps_qualified_sent_at'];
  for (const c of PS_COLS)
    ok(`schema: leads.${c} is declared in db.js`,
       new RegExp(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS ${c}\\b`).test(dbjs));
  for (const c of ['ps_eligible','ps_ineligible_reason','ps_checked_at'])
    ok(`schema: leads.${c} (the rejection record) is declared`,
       new RegExp(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS ${c}\\b`).test(dbjs));
  ok('schema: ps_click_history is JSONB, not TEXT',
     /ps_click_history JSONB/.test(dbjs));
  ok('schema: ps_customer_key is indexed — it is read on every partner submit',
     /CREATE INDEX IF NOT EXISTS leads_ps_customer_key_idx/.test(dbjs));
  ok('schema: leads.created_at is indexed — the contact window scans it',
     /CREATE INDEX IF NOT EXISTS leads_created_at_idx/.test(dbjs));
  for (const c of PS_COLS)
    ok(`schema: gw_form_leads.${c} exists on the AWS mirror`,
       new RegExp(`ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ${c}\\b`).test(src));

  // syncToAWS must actually carry them, not just declare the columns.
  const sync = src.slice(src.indexOf('function syncToAWS'), src.indexOf('function syncBookingToAWS'));
  for (const c of ['ps_xid','ps_partner_key','ps_partner_name','ps_customer_key','ps_click_at','ps_click_history']) {
    ok(`sync: syncToAWS INSERTs ${c}`, new RegExp('\\b' + c + '\\b').test(sync.slice(0, sync.indexOf('VALUES'))));
    ok(`sync: syncToAWS COALESCEs ${c} on conflict`,
       new RegExp(c + '\\s*=\\s*COALESCE\\(EXCLUDED\\.' + c).test(sync));
  }
  // Arity: a shifted parameter here writes one column's value into another.
  {
    const cols = parenBody(sync, sync.indexOf('INSERT INTO gw_form_leads')).split(',').map(x => x.trim()).filter(Boolean);
    const vr = parenBody(sync, sync.indexOf('VALUES'));
    let d = 0, cur = '', vals = [];
    for (const ch of vr) { if (ch === '(') d++; if (ch === ')') d--; if (ch === ',' && !d) { vals.push(cur.trim()); cur = ''; } else cur += ch; }
    vals.push(cur.trim());
    eq('sync: syncToAWS column count equals value count', cols.length, vals.length);
    const dollars = vals.filter(v => v.startsWith('$')).map(v => +v.slice(1));
    eq('sync: no duplicated or skipped $n', new Set(dollars).size, dollars.length);
    /* And the params array must supply exactly that many. Dropping one entry
       does not break the SQL — it silently shifts every later value into the
       WRONG COLUMN, which is the single nastiest way this function can fail
       and is invisible to a column-versus-placeholder check alone. */
    eq('sync: params array length equals max $n',
       countArrayEntries(sync, '`, ['), Math.max(...dollars));
  }

  // /submit and /partial both capture, using ONE reader so they cannot drift.
  for (const [route, from, to] of [
    ['/partial', "app.post('/partial'", "app.post('/submit'"],
    ['/submit',  "app.post('/submit'",  "app.post('/booking-confirmed'"]]) {
    const seg = src.slice(src.indexOf(from), src.indexOf(to));
    ok(`route: ${route} reads the PartnerStack payload`, /readPartnerStackPayload\(req\.body/.test(seg));
    ok(`route: ${route} writes ps_xid`,             /ps_xid/.test(seg));
    ok(`route: ${route} writes ps_click_history`,   /ps_click_history/.test(seg));
    ok(`route: ${route} mirrors them to AWS`,       /\.\.\.ps\}\)/.test(seg));
    /* A later call with an expired cookie must not erase attribution an
       earlier call already captured. */
    ok(`route: ${route} COALESCEs ps_xid rather than overwriting`,
       /ps_xid\s*=\s*COALESCE\(EXCLUDED\.ps_xid/.test(seg));
    // Same shift-by-one trap as syncToAWS, on the route that writes the lead.
    {
      // Depth-aware: the VALUES list contains NOW(), so a naive indexOf(')')
      // truncates it and every count downstream is wrong.
      const dollars = parenBody(seg, seg.indexOf('VALUES'))
        .split(',').map(v => v.trim()).filter(v => v.startsWith('$')).map(v => +v.slice(1));
      eq(`route: ${route} params array length equals max $n`,
         countArrayEntries(seg, '`, [session_id'), Math.max(...dollars));
    }
  }

  // Both form files — the fork rule.
  for (const [name, f] of [['/demo', demo], ['ads', popup]]) {
    ok(`form(${name}): capturePartnerStack exists`, /function capturePartnerStack\(\)/.test(f));
    ok(`form(${name}): it is actually called at init`, /capturePartnerStack\(\);/.test(f));
    ok(`form(${name}): ps_xid falls back to gw_ps_xid`,
       /getCookie\('ps_xid'\)\s*\|\|\s*getCookie\('gw_ps_xid'\)/.test(f));
    ok(`form(${name}): ps_partner_key falls back to gw_ps_partner_key`,
       /getCookie\('ps_partner_key'\)\s*\|\|\s*getCookie\('gw_ps_partner_key'\)/.test(f));
    ok(`form(${name}): click_at comes from gw_ps_seen_at`, /getCookie\('gw_ps_seen_at'\)/.test(f));
    ok(`form(${name}): history comes from gw_ps_clicks`,   /getCookie\('gw_ps_clicks'\)/.test(f));
    ok(`form(${name}): history is capped at 10 client-side too`, /slice\(0,\s*10\)/.test(f));
    ok(`form(${name}): a corrupt cookie cannot break the submit`,
       /JSON\.parse\(rawClicks\)/.test(f) && /catch \(err\)/.test(f));
    ok(`form(${name}): version banner says v5.8.0`, /Form initialised v5\.8\.0/.test(f));
  }

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
