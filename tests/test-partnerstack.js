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

require('./crash-reporter')('test-partnerstack');

const fs = require('fs');
const path = require('path');
const src  = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const dbjs = fs.readFileSync(path.join(__dirname, '..', 'db.js'), 'utf8');
const demo  = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form.js'), 'utf8');
const popup = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form-popup.js'), 'utf8');
const psmod = fs.readFileSync(path.join(__dirname, '..', 'partnerstack.js'), 'utf8');
const sfmod = fs.readFileSync(path.join(__dirname, '..', 'salesforce.js'), 'utf8');

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
/* Lift a const whose value is a TEMPLATE LITERAL. The brace/bracket matcher
   cannot be used here: PS_LADDER_SQL contains a regex with [0-9]{4} in it, so
   bracket matching truncates the SQL mid-statement and the eval fails
   somewhere unrelated. Match the backticks instead. */
/* Negative assertions over source must ignore comments: the comment that
   explains WHY we avoid a construct necessarily contains that construct, so a
   raw match fails against correct code. Third time tonight.

   ARITY counting needs it too, for a different reason. The house style puts a
   comment right at the line it explains, including inside the syncToAWS column
   list and its params array — and a comment containing a comma counted as one
   extra column and three extra parameters, failing four assertions that were
   otherwise correct. A comment is not a column. */
function codeOnly(s) {
  return s.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
}

function liftTemplate(s, decl) {
  const i = s.indexOf('\n' + decl);
  if (i === -1) throw new Error('not found: ' + decl);
  const open = s.indexOf('`', i);
  const close = s.indexOf('`', open + 1);
  if (open === -1 || close === -1) throw new Error('unterminated template: ' + decl);
  let end = close + 1;
  while (end < s.length && s[end] !== '\n') end++;
  return s.slice(i + 1, end);
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
  lift(src, 'function normalisePartnerKey(') + '\n' +
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
  /* TWO consumers as of Sept 2026: the eligibility check and the non-ICP
     block, on separate env flags. The cache must be warmed if EITHER is on --
     gating it on PS_ELIGIBILITY_ENABLED alone left it cold with the block
     enabled, and the first blocked lead paid for a cross-WAN fetch. Still
     asserts the original point: it is NOT warmed when nothing needs it. */
  ok('mvp: the customer cache is not warmed while BOTH consumers are off',
     /function startPartnerStackCacheWarm[\s\S]{0,900}?if \(!PS_ELIGIBILITY_ENABLED && !NON_ICP_BLOCK_ENABLED\)/.test(src));
  /* The conversion must NOT consult eligibility for the MVP. */
  {
    const fn = src.slice(src.indexOf('async function runPartnerStackSignup'),
                         src.indexOf('/* Rejections are logged in one place'));
    ok('mvp: the conversion path does not call the eligibility check',
       !/partnerStackEligibility|PS_ELIGIBILITY_ENABLED/.test(fn));
    ok('conversion: skips our own test addresses', /isPartnerStackTestEmail\(email\)/.test(fn));
    /* A B2C or waitlist signup must never pay an affiliate. Today no
       disqualified lead reaches /submit at all — that is a property of the
       frontend flow, and the frontend is two forked files that have drifted
       before. The guard does not rely on it. */
    ok('conversion: skips DISQUALIFIED leads', /if \(disqualified\) \{[\s\S]{0,200}?return;/.test(fn));
    ok('conversion: the disqualified guard runs before the domain and email checks',
       fn.indexOf('if (disqualified)') !== -1 &&
       fn.indexOf('if (disqualified)') < fn.indexOf('isPartnerStackTestEmail'));
    /* Without this, an organic lead logs nothing at all and the logs cannot
       distinguish "no partner traffic" from "capture is broken". */
    ok('conversion: every submit logs whether a partner was present',
       /No partner on this submit/.test(fn));
    ok('conversion: each skip says WHY', (fn.match(/Skipped conversion —/g) || []).length >= 3);
    ok('conversion: requires a customer key',
       /if \(!ps\.ps_customer_key\) \{[\s\S]{0,200}?return;/.test(fn));
    ok('conversion: requires ps_xid',
       /if \(!ps \|\| !ps\.ps_xid\) \{[\s\S]{0,200}?return;/.test(fn));

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
    /* Deliberately INVERTED from the original. `name` titles the record in
       PartnerStack, so it must be the contact; sending the company left every
       customer titled with the company and Company Name / Website / Phone
       reading "Not Available". Asserted in full under "payload:" below. */
    ok('conversion: name is the contact, with company only as a last resort',
       /const contactName = \[first_name, last_name\][\s\S]{0,160}?\|\| \(company \|\| ''\)\.trim\(\)/.test(fn));
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
  /* Scoped to sendConversion, not the whole module: the v2 helpers alongside it
     legitimately use the key pair. What must never happen is the CONVERSION
     reaching for it — that endpoint takes the tracking token, and swapping the
     two returns a 401 that reads like a bad password rather than a wrong
     scheme. */
  {
    const fn = psmod.slice(psmod.indexOf('async function sendConversion'),
                           psmod.indexOf('/* ============================================================\n   The v2 API'));
    ok('module: sendConversion does NOT reach for the v2 Basic key pair',
       fn.length > 200 && !/PARTNERSTACK_SECRET_KEY|PARTNERSTACK_PUBLIC_KEY|v2AuthHeader/.test(fn));
    ok('module: sendConversion uses the tracking token',
       /process\.env\.PARTNERSTACK_TRACKING_TOKEN/.test(fn));
  }
  ok('module: sends the two required fields plus the five optional ones',
     /const payload = \{[\s\S]{0,400}?\bxid,[\s\S]{0,400}?\bcustomer_key,[\s\S]{0,400}?email:[\s\S]{0,400}?name:[\s\S]{0,400}?ip_address:[\s\S]{0,400}?user_agent:[\s\S]{0,400}?origin:[\s\S]{0,80}?\};/.test(psmod));
  /* An empty string is worse than an absent field for fraud matching: it looks
     like a real value that failed to match. Every optional field must collapse
     to undefined, which JSON.stringify drops entirely. */
  for (const f of ['email', 'name', 'ip_address', 'user_agent', 'origin'])
    ok(`module: ${f} is omitted rather than sent empty`,
       new RegExp(f + ':\\s*' + f + '\\s*\\|\\| undefined').test(psmod));
  ok('module: fraud signals and meta are accepted by the signature',
     /async function sendConversion\(\{ xid, customer_key, email, name, ip_address, user_agent, origin, meta \}\)/.test(psmod));

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
    /* origin is the FULL page URL, deliberately: a bare scheme+host cannot
       tell /demo from an ads lander, and the Origin header is absent on
       same-origin non-CORS posts anyway. */
    ok('ctx: origin is the full page URL', /const origin = \(page_url \|\| ''\)/.test(fn));
    ok('ctx: origin does NOT use the Origin header', !/req\.headers\['origin'\]/.test(fn));
    ok('ctx: origin is bounded', /slice\(0, 1000\)/.test(fn));
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
    /* /submit's response carries non_icp_blocked as of Sept 2026, so the
       literal '{ ok: true }' is gone. Anchor on the call, not its argument. */
    const resAt  = seg.indexOf('res.json({ ok: true');
    const signAt = seg.indexOf('runPartnerStackSignup(');
    ok('conversion: runs in /submit', signAt !== -1);
    ok('conversion: runs AFTER res.json(), never before', resAt !== -1 && signAt > resAt,
       `res.json at ${resAt}, signup at ${signAt}`);
      ok('conversion: is not awaited', !/await runPartnerStackSignup/.test(seg));
  ok('conversion: /submit passes disqualified through to the guard',
       /runPartnerStackSignup\(\{[^}]*\bdisqualified\b/.test(seg));
    ok('conversion: its rejection cannot reach the response', /runPartnerStackSignup\([\s\S]{0,200}?\.catch\(/.test(seg));
  }

  /* ============================================================
     4c. STEPS 6-10 — identity, hear_about_us, Slack, dashboard, qualification
     ============================================================ */

  /* Step 6. The auth split is the expensive mistake here: the conversion uses
     a Bearer tracking token, everything on api.partnerstack.com uses Basic
     base64(public:secret), and both credentials live in the same env. */
  ok('v6: partnerships lookup hits the v2 API',
     /\$\{V2_BASE\}\/partnerships\/\$\{encodeURIComponent\(partnerKey\)\}/.test(psmod));
  ok('v6: v2 base is api.partnerstack.com',
     /const V2_BASE = 'https:\/\/api\.partnerstack\.com\/api\/v2';/.test(psmod));
  ok('v6: v2 auth is Basic base64(public:secret)',
     /Buffer\.from\(`\$\{pub\}:\$\{sec\}`\)\.toString\('base64'\)/.test(psmod));
  ok('v6: v2 auth does NOT use the tracking token',
     !/v2AuthHeader[\s\S]{0,300}?TRACKING_TOKEN/.test(psmod));
  ok('v6: the conversion still uses Bearer, not Basic',
     /sendConversion[\s\S]{0,1400}?Bearer \$\{token\}/.test(psmod));
  ok('v6: a missing key pair is inert, not an error', /reason: 'no_credentials'/.test(psmod));
  ok('v6: the response is unwrapped defensively',
     /const d = \(json && json\.data\) \|\| json \|\| \{\}/.test(psmod) && /d\.partnership \|\| d/.test(psmod));
  ok('v6: name falls back through first\/last, name, company_name',
     /company_name/.test(psmod));
  {
    const fn = src.slice(src.indexOf('async function resolvePartnerIdentity'),
                         src.indexOf('async function runPartnerStackIdentity'));
    ok('v6: memory cache is checked first',
       /_psPartnerCache\.has\(partnerKey\)/.test(src));
    ok('v6: an earlier lead row is checked before the API',
       /FROM leads[\s\S]{0,200}?ps_partner_key = \$1/.test(src));
    /* Caching a failure would pin every future lead from this partner to
       "unknown" for the life of the process. */
    /* Caching a failure would pin every future lead from this partner to
       "unknown" for the life of the process. The failure branch must return
       WITHOUT touching the cache. */
    const failBranch = fn.slice(fn.indexOf('if (!out.ok)'), fn.indexOf('const identity ='));
    ok('v6: the failure branch does not write to the cache',
       failBranch.length > 40 && !/_psPartnerCache\.set/.test(failBranch), failBranch.slice(0, 120));
    ok('v6: the cache is only written on success',
       /const identity = \{ name: out\.name, email: out\.email \};\s*\n\s*_psPartnerCache\.set\(partnerKey, identity\);/.test(fn));
  }
  ok('v6: ps_partner_email column exists on leads',
     /ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_partner_email TEXT/.test(dbjs));
  ok('v6: ps_partner_email exists on the AWS mirror',
     /ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_partner_email TEXT/.test(src));
  /* The name resolves after the row was already mirrored, so the ordinary
     upsert has been and gone — it needs its own targeted write. */
  ok('v6: a late-resolved identity is mirrored to AWS',
     /function syncPartnerIdentityToAWS[\s\S]{0,400}?UPDATE gw_form_leads/.test(src));

  /* Step 7. A human referral outranks an affiliate link. */
  {
    const fn = src.slice(src.indexOf('function partnerHearAboutUs'), src.indexOf('async function upgradePartnerHearAboutUs'));
    const H = (new Function(
      liftLine(src, 'const PS_HEAR_PREFIX =') + '\n' +
      lift(src, 'function partnerDisplayName(') + '\n' +
      fn + '\n return partnerHearAboutUs;'))();
    eq('v7: sets Partner - <name> when resolved',
       H({ hear_about_us: '', ps: { ps_partner_key: 'k1' }, identity: { name: 'Jane Smith' } }), 'Partner - Jane Smith');
    /* name -> email -> raw key. An email tells an AE who the partner is; a hex
       key tells them nothing they can search for. */
    eq('v7: falls back to the partner EMAIL when the name is missing',
       H({ hear_about_us: '', ps: { ps_partner_key: 'k1' }, identity: { email: 'p@x.com' } }), 'Partner - p@x.com');
    eq('v7: falls back to the raw key when nothing is resolved',
       H({ hear_about_us: '', ps: { ps_partner_key: 'k1' }, identity: null }), 'Partner - k1');
    eq('v7: a name outranks an email',
       H({ hear_about_us: '', ps: { ps_partner_key: 'k1' }, identity: { name: 'Jane', email: 'p@x.com' } }), 'Partner - Jane');
    eq('v7: an existing REFERRAL wins',
       H({ hear_about_us: 'Referral - bob@x.com', ps: { ps_partner_key: 'k1' }, identity: { name: 'Jane' } }), null);
    eq('v7: referral match is case-insensitive',
       H({ hear_about_us: 'referral - bob@x.com', ps: { ps_partner_key: 'k1' }, identity: { name: 'Jane' } }), null);
    eq('v7: no partner means no change', H({ hear_about_us: 'Google', ps: {}, identity: null }), null);
    eq('v7: an unrelated value is overwritten by the partner',
       H({ hear_about_us: 'Google', ps: { ps_partner_key: 'k1' }, identity: { name: 'Jane' } }), 'Partner - Jane');
    eq('v7: no rewrite when it already matches',
       H({ hear_about_us: 'Partner - Jane', ps: { ps_partner_key: 'k1' }, identity: { name: 'Jane' } }), null);
  }
  {
    const fn = src.slice(src.indexOf('async function upgradePartnerHearAboutUs'), src.indexOf('/* ── STEP 5'));
    /* Only ever rewrites the placeholder this code wrote — never a referral
       and never anything a human typed. */
    /* Both weaker rungs are upgradeable — a row may carry the raw key (nothing
       known) or the email (email known, name not) — but ONLY values this code
       wrote. A referral or a human-typed value is never a candidate. */
    ok('v7: the upgrade only replaces placeholders this code wrote',
       /WHERE session_id = \$1 AND hear_about_us = ANY\(\$3\)/.test(fn));
    ok('v7: the email placeholder is upgradeable to a name',
       /if \(identity\.email\) weaker\.push\(PS_HEAR_PREFIX \+ identity\.email\)/.test(fn));
    ok('v7: it never rewrites to the value it already has',
       /candidates = weaker\.filter\(v => v !== resolved\)/.test(fn));
    ok('v7: the upgrade reaches Salesforce, where the AE looks',
       /findSFLeadByEmail\(email\)/.test(fn) && /hear_about_us__c: resolved/.test(fn));
    /* A targeted UPDATE, never syncToAWS. That upsert sets
       `disqualified = EXCLUDED.disqualified` with no COALESCE, so a partial
       object passes false and CLEARS a real disqualification on the mirror the
       dialer reads. */
    ok('v7: the upgrade reaches the AWS mirror', /syncHearAboutUsToAWS\(session_id, resolved\)/.test(fn));
    ok('v7: the upgrade does NOT go through the whole-row upsert', !/syncToAWS\(/.test(fn));
  }
  for (const route of ['/partial', '/submit']) {
    const [from, to] = route === '/partial'
      ? ["app.post('/partial'", "app.post('/submit'"]
      : ["app.post('/submit'", "app.post('/booking-confirmed'"];
    const seg = src.slice(src.indexOf(from), src.indexOf(to));
    ok(`v7: ${route} computes the partner hear_about_us`, /partnerHearAboutUs\(\{ hear_about_us, ps/.test(seg));
    ok(`v7: ${route} binds the final value, not the raw one`, /hearAboutUsFinal\|\|null/.test(seg));
    /* A colon in an INSERT column list is invalid SQL that node --check
       cannot see, because the query lives in a template literal. */
    const cols = /INSERT INTO leads \(([^)]*)\)/.exec(seg)[1];
    ok(`v7: ${route} INSERT column list is still valid SQL`,
       cols.split(',').every(c => /^[a-z_][a-z0-9_]*$/.test(c.trim())), cols.slice(0, 80));
  }
  ok('v7: Salesforce receives the final hear_about_us',
     /pushToSalesforce\(\{[^}]*hear_about_us:hearAboutUsFinal/.test(src));

  /* The regression that prompted the display chain: a deploy clears the
     in-memory Map, so the memory-only peek fell back to a raw hex key in Slack
     even though the database already had the name from an earlier lead. */
  {
    const fn = src.slice(src.indexOf('async function partnerIdentityNoNetwork'),
                         src.indexOf('async function resolvePartnerIdentity'));
    /* EXECUTED, not text-matched. Asserting the query string is present passes
       even when a `return null` above it makes the query unreachable — a
       mutation survived on exactly that, which is the original bug restored. */
    {
      let queried = 0;
      const fakePool = { query: async () => { queried++; return { rows: [{ ps_partner_name: 'Jane', ps_partner_email: 'p@x.com' }] }; } };
      const L = (new Function('pool', 'console',
        'const _psPartnerCache = new Map();\n' + fn + '\n return { partnerIdentityNoNetwork, _psPartnerCache };'
      ))(fakePool, { warn() {}, log() {} });
      const got = await L.partnerIdentityNoNetwork('k1');
      eq('identity: an empty memory cache still returns the DB row',
         got, { name: 'Jane', email: 'p@x.com' });
      ok('identity: it actually hit the database', queried === 1, `queried ${queried}x`);
      // Second call must be served from memory, not re-queried.
      await L.partnerIdentityNoNetwork('k1');
      ok('identity: the result is memoised, so repeat leads cost nothing', queried === 1, `queried ${queried}x`);
      eq('identity: no partner key means no query at all',
         await L.partnerIdentityNoNetwork(null), null);
    }
    {
      // A partner nobody has resolved yet must come back null, not throw.
      const emptyPool = { query: async () => ({ rows: [] }) };
      const L = (new Function('pool', 'console',
        'const _psPartnerCache = new Map();\n' + fn + '\n return { partnerIdentityNoNetwork };'
      ))(emptyPool, { warn() {}, log() {} });
      eq('identity: an unknown partner returns null', await L.partnerIdentityNoNetwork('nope'), null);
    }
    {
      // A database blip must not throw into the route.
      const badPool = { query: async () => { throw new Error('db down'); } };
      const L = (new Function('pool', 'console',
        'const _psPartnerCache = new Map();\n' + fn + '\n return { partnerIdentityNoNetwork };'
      ))(badPool, { warn() {}, log() {} });
      eq('identity: a DB failure returns null rather than throwing',
         await L.partnerIdentityNoNetwork('k1'), null);
    }
    ok('identity: it makes NO network call', !/fetchPartnership/.test(fn));
    ok('identity: it accepts a row with only an email',
       /ps_partner_name IS NOT NULL OR ps_partner_email IS NOT NULL/.test(fn));
    ok('identity: the memory-only peek is gone', !/function peekPartnerIdentity/.test(src));
  }
  for (const [route, from, to] of [
    ['/partial', "app.post('/partial'", "app.post('/submit'"],
    ['/submit',  "app.post('/submit'",  "app.post('/booking-confirmed'"]]) {
    const seg = src.slice(src.indexOf(from), src.indexOf(to));
    ok(`identity: ${route} awaits the DB-backed lookup`,
       /await partnerIdentityNoNetwork\(ps\.ps_partner_key\)/.test(seg));
  }
  ok('identity: Slack reuses the already-fetched identity, no second lookup',
     /ps_partner_name:\(psIdentity\|\|\{\}\)\.name/.test(src));

  /* One chain, three surfaces. If they drift, the same partner reads three
     different ways across Slack, the dashboard and Salesforce. */
  {
    const D = (new Function(lift(src, 'function partnerDisplayName(') + '\n return partnerDisplayName;'))();
    eq('chain: name wins',        D({ name: 'Jane', email: 'p@x.com' }, 'k1'), 'Jane');
    eq('chain: email is next',    D({ email: 'p@x.com' }, 'k1'),               'p@x.com');
    eq('chain: key is the floor', D(null, 'k1'),                               'k1');
    eq('chain: nothing at all',   D(null, null),                               null);
    eq('chain: empty name falls through to email', D({ name: '', email: 'p@x.com' }, 'k1'), 'p@x.com');
  }
  ok('chain: hear_about_us uses it', /const label = partnerDisplayName\(identity, ps\.ps_partner_key\)/.test(src));
  ok('chain: Slack has an email rung', /d\.ps_partner_email\s*\n?\s*\? `\*\$\{d\.ps_partner_email\}\*/.test(src));
  ok('chain: the dashboard has an email rung', /l\.ps_partner_email\?\(esc\(l\.ps_partner_email\)/.test(src));

  /* ── Payload: contact name, and company/website via meta ────────────── */
  {
    const fn = src.slice(src.indexOf('async function runPartnerStackSignup'),
                         src.indexOf('/* Rejections are logged in one place'));
    /* `name` titles the record in PartnerStack and whoever approves payouts
       opens it. Sending the company there titled every customer with the
       company and left Company Name / Website / Phone "Not Available". */
    ok('payload: name is the CONTACT name, not the company',
       /const contactName = \[first_name, last_name\]/.test(fn));
    ok('payload: company is only a last-resort title',
       fn.indexOf('[first_name, last_name]') < fn.indexOf("(company || '').trim()"));
    ok('payload: name is passed as the contact name', /name: contactName/.test(fn));
    ok('payload: company and website go via meta',
       /meta: \{[\s\S]{0,200}?\[PS_META_COMPANY\]: company,[\s\S]{0,80}?\[PS_META_WEBSITE\]: website,/.test(fn));
  }
  /* A typo in a meta key is invisible — PartnerStack drops unrecognised keys
     silently, which looks exactly like the integration working. */
  eq('payload: the company meta field name', /const PS_META_COMPANY = '([^']+)'/.exec(src)[1], 'company_name');
  eq('payload: the website meta field name', /const PS_META_WEBSITE = '([^']+)'/.exec(src)[1], 'website');
  {
    const fn = psmod.slice(psmod.indexOf('async function sendConversion'),
                           psmod.indexOf('/* ============================================================\n   The v2 API'));
    ok('payload: sendConversion accepts meta', /ip_address, user_agent, origin, meta \}/.test(fn));
    ok('payload: empty meta values are dropped, not sent blank',
       /if \(val\) metaClean\[k\] = val\.slice\(0, 500\)/.test(fn));
    ok('payload: an entirely empty meta is omitted from the payload',
       /if \(Object\.keys\(metaClean\)\.length\) payload\.meta = metaClean;/.test(fn));
  }

  /* ── Partners tab ───────────────────────────────────────────────────── */
  {
    const fn = src.slice(src.indexOf('async function partnerOverview'),
                         src.indexOf("app.get('/monitor/partners'"));
    /* Per CLAUDE.md: headline numbers are PEOPLE, but a conversion is per
       DOMAIN because that is the unit PartnerStack counts. The two are
       different units and the column headers have to say so. */
    /* The two queries are asserted SEPARATELY. Testing against the whole
       function let the per-partner query satisfy a unit changed in the totals
       query — two mutations survived on exactly that. */
    const qTotals = fn.slice(fn.indexOf('pool.query('), fn.indexOf('pool.query(', fn.indexOf('pool.query(') + 5));
    const qRows   = fn.slice(fn.indexOf('pool.query(', fn.indexOf('pool.query(') + 5));
    /* The per-partner stage expressions moved into PS_FUNNEL_STAGE_SQL in PR2,
       shared with the programme query. Asserting there covers BOTH, which is
       strictly better than checking one query's inline text. */
    const qShared = src.slice(src.indexOf('const PS_FUNNEL_STAGE_SQL = `'), src.indexOf('const PS_FUNNEL_FROM'));
    for (const [label, q] of [['totals', qTotals], ['funnel-stages', qShared]]) {
      /* DOMAINS throughout as of batch C. These counted PEOPLE while the funnel
         counted companies, which put two units on one screen — exactly what the
         per-domain rework removes. */
      ok(`partners (${label}): leads count DOMAINS, not people`,
         /COUNT\(DISTINCT (l\.)?ps_customer_key\)\s+AS (leads|step1)/.test(q), q.slice(0, 80));
      ok(`partners (${label}): leads are NOT a raw row count`,
         !/COUNT\(\*\)\s+AS leads/.test(q));
      ok(`partners (${label}): no column counts people`,
         !/COUNT\(DISTINCT LOWER\(email\)\)/.test(q));
      ok(`partners (${label}): bookings count DOMAINS`,
         /COUNT\(DISTINCT (l\.)?ps_customer_key\) FILTER \([\s\S]{0,200}?booking_uid IS NOT NULL/.test(q));
      ok(`partners (${label}): conversions are per DOMAIN`,
         /COUNT\(DISTINCT (l\.)?ps_customer_key\) FILTER \([\s\S]{0,200}?ps_signup_sent_at IS NOT NULL/.test(q));
      ok(`partners (${label}): qualified is per DOMAIN`,
         /COUNT\(DISTINCT (l\.)?ps_customer_key\) FILTER \([\s\S]{0,300}?ps_qualified_sent_at IS NOT NULL/.test(q));
      /* A domain count and a people count are different units. Presenting one
         as the other is the exact drift the Definitions section exists to
         stop. */

    }
    ok('partners: only partner-sourced leads are counted',
       /WHERE ps_partner_key IS NOT NULL/.test(fn) &&
       /WHERE l\.ps_partner_key IS NOT NULL AND l\.ps_customer_key IS NOT NULL/.test(src));
    /* bookingRate was removed in PR2: its card was dropped in batch C, so it
       had one reference and no consumer. Rates now live in the funnel, where
       they are suppressed below PS_RATE_MIN. */
    ok('partners: the dead bookingRate is gone', !/bookingRate/.test(src));
    ok('partners: the per-partner list resolves a name via MAX over the key',
       /GROUP BY l\.ps_partner_key/.test(fn) && /MAX\(l\.ps_partner_name\)/.test(fn));
  }
  ok('partners: the route is token-guarded',
     /partners'[\s\S]{0,200}?req\.query\.token !== token/.test(src));
  ok('partners: it is a TAB, and Partner gaps stays on Overview',
     /id="t-partners"/.test(src) && /id="tp-partners"/.test(src) && /id="psgapbox"/.test(src));
  /* "blocked" joined the list in Sept 2026. Asserted as "partners is in the
     array" rather than as the whole literal, so adding a seventh tab does not
     fail a PartnerStack assertion that is not about tabs. */
  ok('partners: the tab is registered in showTab',
     /function showTab\(n\)\{\[[^\]]*"partners"[^\]]*\]/.test(src));
  ok('partners: it loads lazily on first open',
     /n==="partners"&&document\.getElementById\("ptbody"\)/.test(src));
  {
    const i = src.indexOf("'var partnerRows=[],pSort=");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const mkEl = () => ({ textContent: '', innerHTML: '', style: {}, querySelectorAll: () => [], options: [], appendChild(o) { this.options.push(o); }, value: '' });
    const doc = { getElementById: (id) => (els[id] = els[id] || mkEl()), createElement: () => ({ value: '', textContent: '' }) };
    let tabShown = null, leadsLoaded = 0;
    const F = (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array',
      client + '; return {loadPartners,renderPartners,sortPartners,partnerDrill};'))(
      '', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
      (id, v) => { doc.getElementById(id).textContent = String(v); },
      async () => ({ ok: true, json: async () => ({
        totals: { leads: 12, leads24h: 3, conversions: 5, qualified: 2, booked: 7, bookingRate: 58.3 },
        partners: [
          { partner_key: 'k1', partner_name: 'Jane', partner_email: 'j@x.com', step1: 8, completed: 6, conversions: 4, booked: 5, qualified: 2, last_click: '2026-09-04' },
          { partner_key: 'k2', partner_name: null, partner_email: null, step1: 4, completed: 2, conversions: 1, booked: 2, qualified: 0, last_click: null }] }) }),
      { timeout: () => null }, doc,
      (t) => { tabShown = t; }, async () => {}, () => { leadsLoaded++; }, Array);
    await F.loadPartners();
    eq('partners UI: lead card', els['p-leads'].textContent, '12');
    eq('partners UI: 24h subtitle', els['p-leads24'].textContent, '3 in the last 24h');
    eq('partners UI: conversions card', els['p-conv'].textContent, '5');
    eq('partners UI: qualified card', els['p-qual'].textContent, '2');
    /* Partner bookings and Lead->booking were dropped in batch C: structurally
       zero at this volume, and a card reading 0 makes the tab look broken. */
    ok('partners UI: the two zero-volume cards are gone',
       els['p-booked'] === undefined && els['p-rate'] === undefined);
    ok('partners UI: one row per partner', (els['ptbody'].innerHTML.match(/<tr /g) || []).length === 2);
    ok('partners UI: an unresolved partner falls back to the key',
       els['ptbody'].innerHTML.includes("data-pk='k2'"));
    /* The key would need quotes nested three deep in an inline handler, which
       is how this markup broke the first time. */
    ok('partners UI: rows use data-pk + delegation, not an inline onclick',
       !els['ptbody'].innerHTML.includes('onclick'));
    /* "leads" became "step1" in batch C: the funnel counts companies now, so
       every column nests. */
    F.sortPartners('step1');
    ok('partners UI: clicking the active column flips direction',
       els['psar-step1'].textContent === '▲');
    F.sortPartners('partner_name');
    ok('partners UI: switching column resets the arrow', els['psar-step1'].textContent === '');
    // Drill-down reuses All Leads and its existing partner filter.
    await F.partnerDrill('k1');
    eq('partners UI: drill-down switches to the leads tab', tabShown, 'leads');
    eq('partners UI: drill-down sets the existing partner filter', els['fpartner'].value, 'k1');
    ok('partners UI: drill-down reloads the leads table', leadsLoaded === 1);
  }
  /* A zero-lead programme must show a dash, not 0% or NaN. */
  {
    const i = src.indexOf("'var partnerRows=[],pSort=");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const mkEl = () => ({ textContent: '', innerHTML: '', style: {}, querySelectorAll: () => [], options: [], appendChild() {}, value: '' });
    const doc = { getElementById: (id) => (els[id] = els[id] || mkEl()), createElement: () => ({ value: '', textContent: '' }) };
    const F = (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array',
      client + '; return {loadPartners};'))(
      '', '', (x) => String(x), (x) => String(x), (id, v) => { doc.getElementById(id).textContent = String(v); },
      async () => ({ ok: true, json: async () => ({ totals: { leads: 0, bookingRate: null }, partners: [] }) }),
      { timeout: () => null }, doc, () => {}, async () => {}, () => {}, Array);
    await F.loadPartners();
    ok('partners UI: an empty list says so', els['ptbody'].innerHTML.includes('No partner-sourced leads yet'));
  }

  /* ── Regression: the All Leads partner filter and click panel still work ── */
  ok('regression: the All Leads partner filter control survives',
     /id="fpartner"/.test(src) && /url\+="&partner="\+encodeURIComponent\(partner\)/.test(src));
  ok('regression: the leads API still accepts the partner filter',
     /const partner      = req\.query\.partner/.test(src));
  ok('regression: psPanel is still wired into enrichPanel',
     /'var pp=psPanel\(l\);' \+/.test(src) && /'var out=pp;' \+/.test(src));

  /* ── Read-back guard ────────────────────────────────────────────────
     /conversion/xid answers 200 with an EMPTY body, so a 200 that created
     nothing would still stamp ps_signup_sent_at and burn the domain forever
     under the once-per-domain rule. */
  {
    const fn = src.slice(src.indexOf('async function runPartnerStackConversionVerify'),
                         src.indexOf('function startPartnerStackConversionVerify'));

    /* THE GRACE PERIOD IS THE DESIGN. Measured lag on 4 Sept was under 2 min
       for one record and ~6 for another; checking immediately would report
       healthy conversions missing and release good claims. */
    ok('readback: there is a grace period before checking',
       /const PS_VERIFY_GRACE_MIN   = 15;/.test(src) &&
       /ps_signup_sent_at < NOW\(\) - INTERVAL '\$\{PS_VERIFY_GRACE_MIN\} minutes'/.test(fn));
    ok('readback: only unverified conversions are swept',
       /ps_signup_sent_at IS NOT NULL[\s\S]{0,120}?ps_signup_verified_at IS NULL/.test(fn));
    ok('readback: the batch is bounded', /LIMIT \$\{PS_VERIFY_BATCH\}/.test(fn));
    ok('readback: overlapping sweeps are prevented',
       /if \(_psVerifyRunning\) return;/.test(fn) && /_psVerifyRunning = true;/.test(fn));

    /* A SWEEP, not a setTimeout: a timer dies with the process and a deploy in
       the wrong ten minutes loses the verification silently. */
    /* The scheduler gained a boot run in PR 25, so the interval is now
       registered through a local helper rather than by bare function
       reference. The substance of this assertion is the second half: the
       verification must be a SWEEP, never a setTimeout after the send. A timer
       dies with the process and a deploy in the wrong ten minutes would lose
       it silently — the same class of failure the read-back exists to catch. */
    ok('readback: it is a sweep on an interval, not a post-send timer',
       /setInterval\(\(\) => run\('scheduled'\), PS_VERIFY_INTERVAL_MS\)/.test(src) &&
       !/setTimeout\([\s\S]{0,80}?fetchCustomer/.test(src));
    ok('readback: it is started at boot', /startPartnerStackConversionVerify\(\);/.test(src));

    /* The distinction that matters most: "could not tell" is not "missing". */
    const cantTell = fn.slice(fn.indexOf('if (!out.ok)'), fn.indexOf('if (out.exists)'));
    ok('readback: an unreachable API leaves the claim alone',
       /continue;/.test(cantTell) && !/ps_signup_sent_at = NULL/.test(cantTell), cantTell.slice(0, 140));
    ok('readback: a 404 releases the claim so the domain can retry',
       /NO customer exists[\s\S]{0,400}?ps_signup_sent_at = NULL/.test(fn));
    ok('readback: a phantom conversion is escalated, not just logged',
       /recordFailure\('PartnerStack', r\.ps_customer_key \+ ' \(phantom conversion\)'/.test(fn));
    /* ── The invariant C8 rests on ──────────────────────────────────────
       Requiring ps_signup_verified_at before the $50 fires is only airtight
       because a verified row can never become a phantom afterwards. Two
       properties make that true and both are asserted here rather than left as
       something a reader has to work out: the sweep only ever SETS the
       verification stamp, never clears it, and it only ever looks at rows
       where the stamp is still NULL. Break either and a verified domain could
       be un-verified while a qualification is in flight, which is the exact
       permanent loss C8 exists to close. */
    ok('readback/C8: the sweep only ever SETS ps_signup_verified_at',
       /SET ps_signup_verified_at = NOW\(\)/.test(fn)
       && !/ps_signup_verified_at = NULL/.test(fn));
    ok('readback/C8: and only ever reads rows that are not yet verified',
       /AND ps_signup_verified_at IS NULL/.test(fn));
    /* The phantom release must clear the SENT stamp and leave the rest alone —
       releasing verification too would reopen the hole. */
    ok('readback/C8: a phantom release clears only ps_signup_sent_at',
       /SET ps_signup_sent_at = NULL, updated_at = NOW\(\)/.test(fn));
    ok('readback: a verified conversion is stamped',
       /ps_signup_verified_at = NOW\(\)/.test(fn));
    /* A production integration writing test records pays nobody and looks
       perfectly healthy from here. */
    ok('readback: a test-flagged record is called out',
       /if \(out\.test === true\) \{[\s\S]{0,300}?recordFailure\([\s\S]{0,120}?\(test record\)/.test(fn));
  }
  {
    const fn = psmod.slice(psmod.indexOf('async function fetchCustomer'), psmod.indexOf('module.exports'));
    ok('readback: 404 is reported as exists:false, not as an error',
       /res\.status === 404[\s\S]{0,160}?return \{ ok: true, exists: false/.test(fn));
    ok('readback: a non-404 failure is ok:false, so it cannot be read as missing',
       /if \(!res\.ok\)[\s\S]{0,200}?return \{ ok: false/.test(fn));
    ok('readback: a timeout is ok:false too',
       /AbortError' \? 'timeout' : 'network_error'[\s\S]{0,80}?return \{ ok: false/.test(fn));
    ok('readback: it reads back data.test', /test = d\.test/.test(fn));
    ok('readback: the call is bounded by a timeout', /signal: controller\.signal/.test(fn));
  }
  ok('readback: the verified column exists on leads',
     /ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_signup_verified_at TIMESTAMPTZ/.test(dbjs));
  ok('readback: and on the AWS mirror',
     /ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_signup_verified_at TIMESTAMPTZ/.test(src));
  ok('readback: the sweep query is indexed',
     /CREATE INDEX IF NOT EXISTS leads_ps_signup_unverified_idx/.test(dbjs));

  /* ── Phone in meta ──────────────────────────────────────────────────
     Optional on our form (only required for free-mail addresses), so it is
     absent more often than not and must never block the conversion. */
  {
    const fn = src.slice(src.indexOf('async function runPartnerStackSignup'),
                         src.indexOf('/* ── READ-BACK'));
    ok('phone: sent in meta', /\[PS_META_PHONE\]:   phone,/.test(fn));
    ok('phone: threaded into the signup runner', /company, phone, first_name/.test(fn));
  }
  eq('phone: the meta field name', /const PS_META_PHONE   = '([^']+)'/.exec(src)[1], 'phone');
  ok('phone: /submit passes it', /runPartnerStackSignup\(\{[^}]*\bphone\b/.test(src));
  /* An absent phone must omit the KEY, not send a blank, and must not stop the
     conversion being created. */
  {
    const fn = psmod.slice(psmod.indexOf('async function sendConversion'),
                           psmod.indexOf('/* ============================================================\n   The v2 API'));
    const M = (new Function('meta', `
      const metaClean = {};
      for (const [k, v] of Object.entries(meta || {})) {
        const val = (v === null || v === undefined) ? '' : String(v).trim();
        if (val) metaClean[k] = val.slice(0, 500);
      }
      return metaClean;`));
    eq('phone: a null phone omits the key entirely',
       M({ company_name: 'Acme', website: 'a.com', phone: null }), { company_name: 'Acme', website: 'a.com' });
    eq('phone: an empty phone omits the key entirely',
       M({ company_name: 'Acme', phone: '' }), { company_name: 'Acme' });
    eq('phone: a whitespace-only phone omits the key entirely',
       M({ company_name: 'Acme', phone: '   ' }), { company_name: 'Acme' });
    eq('phone: a real phone is sent',
       M({ phone: ' +91 98765 43210 ' }), { phone: '+91 98765 43210' });
    /* The cleaner is what the module actually runs — asserted so this test
       cannot drift from it. */
    ok('phone: the cleaner under test matches the shipped one',
       /const val = \(v === null \|\| v === undefined\) \? '' : String\(v\)\.trim\(\);/.test(fn) &&
       /if \(val\) metaClean\[k\] = val\.slice\(0, 500\);/.test(fn));
  }

  /* ── Batch A: the lifecycle ladder, skip/failure reasons, alerting ── */
  for (const [c, t] of [['ps_signup_skipped_reason','TEXT'],['ps_signup_skipped_at','TIMESTAMPTZ'],
                        ['ps_signup_failed_at','TIMESTAMPTZ'],['ps_signup_fail_reason','TEXT'],
                        ['ps_qualify_failed_at','TIMESTAMPTZ'],['ps_qualify_fail_reason','TEXT']]) {
    ok(`ladderA: leads.${c} declared as ${t}`,
       new RegExp(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS ${c} ${t}`).test(dbjs));
    ok(`ladderA: gw_form_leads.${c} declared`,
       new RegExp(`ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ${c} ${t}`).test(src));
  }
  /* Separate from ps_ineligible_reason on purpose: that means "eligibility
     rejected this", and conflating them once eligibility is on would make a
     correct skip indistinguishable from a rejection. */
  ok('ladderA: skip reason is NOT overloaded onto ps_ineligible_reason',
     !/ps_ineligible_reason\s*=\s*\$2/.test(src.slice(src.indexOf('async function recordPartnerStackSkip'),
                                                     src.indexOf('async function recordPartnerStackFailure'))));

  /* The ladder: order, exclusivity, and the unit seam. */
  {
    const l = src.slice(src.indexOf('const PS_LADDER_SQL'), src.indexOf('const PS_LADDER_FAILED'));
    const order = [...l.matchAll(/THEN '([a-z_]+)'/g)].map(m => m[1]);
    eq('ladderA: resolution order is failure-aware, not plain progression', order,
       ['qualified','qualification_failed','conversion_failed','demo_done_not_qualified',
        'awaiting_demo','converted','skipped']);
    ok('ladderA: there is an exhaustive ELSE so every domain lands somewhere',
       /ELSE 'conversion_pending'/.test(l));
    /* A success outranks its OWN failure... */
    ok('ladderA: qualified outranks qualification_failed',
       order.indexOf('qualified') < order.indexOf('qualification_failed'));
    /* ...but an UNRESOLVED conversion failure outranks the stages it blocks,
       or a domain that can never be qualified shows as "awaiting demo". */
    ok('ladderA: an unresolved conversion failure outranks awaiting_demo',
       order.indexOf('conversion_failed') < order.indexOf('awaiting_demo'));
    ok('ladderA: conversion_failed only fires when NOT since converted',
       /ps_signup_failed_at IS NOT NULL\)\s*\n\s*AND NOT BOOL_OR\(ps_signup_sent_at IS NOT NULL\)/.test(l));
    /* start_time is TEXT — a bare cast takes the query down on one bad row. */
    ok('ladderA: the start_time cast is CASE-guarded',
       /CASE WHEN start_time ~ '\^\[0-9\]\{4\}/.test(l));
  }
  {
    const fn = src.slice(src.indexOf('async function partnerLifecycle'), src.indexOf('async function partnerOverview'));
    ok('ladderA: keyed by DOMAIN', /GROUP BY ps_customer_key/.test(fn));
    ok('ladderA: only partner-sourced leads', /ps_xid IS NOT NULL AND ps_customer_key IS NOT NULL/.test(fn));
    /* Bounded so it does not scan every partner lead ever — but the bound must
       never hide a failure, or a domain that failed months ago and was never
       fixed drops out of "Needs attention", the one number that has to be
       complete. */
    ok('ladderA: the query is date-bounded', /INTERVAL '\$\{PS_LADDER_WINDOW_D\} days'/.test(fn));
    ok('ladderA: an unresolved failure is included regardless of age',
       /OR ps_signup_failed_at IS NOT NULL\s*\n\s*OR ps_qualify_failed_at IS NOT NULL\)/.test(fn));
    ok('ladderA: the window matches the Salesforce lookback',
       /const PS_LADDER_WINDOW_D = 180;/.test(src));
    /* The unit seam: leads with no domain cannot be keyed by one, so they are
       counted as LEADS in their own field rather than folded into a domain
       count. */
    ok('ladderA: no-customer-key leads are counted separately, as leads',
       /ps_xid IS NOT NULL AND ps_customer_key IS NULL/.test(fn) && /noCustomerKeyLeads/.test(fn));
    /* EXECUTED. Asserting the field name exists passes even when the value is
       hardcoded to 0 — a mutation survived on exactly that, which would hide
       the unit seam rather than surface it. */
    {
      const calls = [];
      const fakePool = { query: async (q) => {
        calls.push(q);
        if (q.includes('partner_domain_sf_state')) return { rows: [{ customer_key: 'a.com', sf_state: 'ticked' }] };
        /* R2: the UNBOUNDED failure count. "Needs attention" is the one number
           on the tab that means somebody must act today, and the domain query
           beside it is ordered by recency and capped — so past the cap a
           failure would silently drop off the headline. It gets its own query
           over the whole population, and this fake answers it. */
        /* 5 and 1, NOT 2 and 0. The four domain rows below contain exactly
           two red states and zero acknowledged, so a fixture returning 2 here
           cannot distinguish "read the unbounded query" from "counted the
           capped page" — both give 2, and reverting to the page count survived
           the mutation. Deliberately different numbers, so only one source can
           produce them. */
        if (q.includes('AS needs_attention')) return { rows: [{ needs_attention: '5', acknowledged: '1' }] };
        return q.includes('ps_customer_key IS NULL')
          ? { rows: [{ leads: '7' }] }
          : { rows: [
              { customer_key: 'a.com', state: 'qualified' },
              { customer_key: 'b.com', state: 'conversion_failed' },
              { customer_key: 'c.com', state: 'qualification_failed' },
              { customer_key: 'd.com', state: 'skipped' }] };
      } };
      const L = (new Function('pool', 'console',
        liftLine(src, 'const PS_LADDER_FAILED =') + '\n' +
        liftLine(src, 'const PS_LADDER_WINDOW_D =') + '\n' +
        liftLine(src, 'const PS_LADDER_LIMIT') + '\n' +
        liftLine(src, 'const PS_SF_STALE_MIN') + '\n' +
        liftLine(src, 'let _psSfLastRead =') + '\n' +
        liftLine(src, 'const PS_SF_STATES =') + '\n' +
        liftTemplate(src, 'const PS_LADDER_SQL =') + '\n' +
        lift(src, 'async function partnerLifecycle(') +
        '\n return partnerLifecycle;'))(fakePool, { warn() {}, log() {} });
      const out = await L();
      eq('ladderA: no-key leads reach the response with their real count', out.noCustomerKeyLeads, 7);
      /* 5, which ONLY the unbounded query can produce — the page holds two
         red states. Reverting to the page count gives 2 and fails here. */
      eq('ladderA/R2: needsAttention comes from the UNBOUNDED query, not the page',
         out.needsAttention, 5);
      eq('ladderA/R2: acknowledged comes from the unbounded query too', out.acknowledged, 1);
      eq('ladderA/R2: and the count is flagged COMPLETE', out.needsAttentionComplete, true);
      eq('ladderA: totalDomains counts domains, not leads', out.totalDomains, 4);
      eq('ladderA: the state counts sum to the domain total',
         Object.values(out.byState).reduce((a, b) => a + b, 0), out.totalDomains);
      ok('ladderA: no-key leads are NOT folded into the domain total',
         out.totalDomains === 4 && out.noCustomerKeyLeads === 7);
    }
    /* Was a reduce over byState; PR1.8 made it a filter over the domains so it
       can also exclude acknowledged failures. The property is unchanged: only
       the red states count. */
    ok('ladderA: needsAttention counts only the red states',
       /PS_LADDER_FAILED\.includes\(d\.state\)/.test(fn));
    eq('ladderA: exactly two red states',
       (/const PS_LADDER_FAILED = \[([^\]]+)\]/.exec(src)[1].match(/'/g) || []).length / 2, 2);
  }
  ok('ladderA: the ladder is exposed on /monitor/partners',
     /partnerOverview\(\), partnerLifecycle\(\)/.test(src));
  ok('ladderA: the unit seam is stated in the UI, not just in comments',
     /no customer key \(leads, not companies\)/.test(src));

  /* The one-off backfill. Two historical rows would otherwise render in the
     WRONG state on day one — test.com grey when its conversion really was a
     phantom, which is the exact bug this batch fixes. */
  {
    const bf = dbjs.slice(dbjs.indexOf('ONE-OFF BACKFILL'), dbjs.indexOf('console.log(\'[DB] Tables ready\')'));
    ok('backfill: test.com is marked as a phantom conversion failure',
       /ps_customer_key = 'test\.com'[\s\S]{0,300}?ps_signup_fail_reason = 'phantom_200'/.test(bf) ||
       /ps_signup_fail_reason = 'phantom_200'[\s\S]{0,300}?ps_customer_key = 'test\.com'/.test(bf));
    ok('backfill: gushwork.ai is marked as a test-address skip',
       /ps_signup_skipped_reason = 'test_email'[\s\S]{0,300}?ps_customer_key = 'gushwork\.ai'/.test(bf));
    /* Idempotent by construction, not by a ledger: it can only touch rows
       where the target is still NULL, nothing has since been sent, and the row
       predates the cutoff. So a genuine later lead from either domain is
       untouched and a reboot cannot re-fire it. */
    ok('backfill: only fires where the target column is still NULL',
       /ps_signup_failed_at IS NULL/.test(bf) && /ps_signup_skipped_reason IS NULL/.test(bf));
    ok('backfill: never overwrites a domain that has since converted',
       (bf.match(/ps_signup_sent_at IS NULL/g) || []).length === 2);
    /* Two kinds of backfill, and only one needs the cutoff.

       A statement that targets a NAMED identity — a specific email or domain —
       is a one-off historical repair and must be cutoff-bounded, or it could
       touch a real lead that arrives on that domain later.

       The click-history repair is different: it targets no identity, it is
       keyed purely on the data condition, and it is idempotent because a
       normalised row stops matching. Bounding it would stop it fixing a future
       regression of the cookie, which is exactly what it is for. Asserting a
       flat count across both broke the moment a third statement was added and
       said nothing about whether the new one was guarded. */
    {
      const stmts = bf.split('UPDATE leads').slice(1);
      const identity = stmts.filter((t) => /ps_customer_key = '|LOWER\(email\) = '/.test(t));
      const general  = stmts.filter((t) => !/ps_customer_key = '|LOWER\(email\) = '/.test(t));
      ok('backfill: there are both identity-targeted and general statements',
         identity.length >= 3 && general.length >= 1, `${identity.length} identity, ${general.length} general`);
      eq('backfill: every identity-targeted statement is cutoff-bounded',
         identity.filter((t) => t.includes('${CUTOFF}')).length, identity.length);
      /* The general one is bounded by its own predicate instead. */
      ok('backfill: the general repair is idempotent by its predicate',
         general.every((t) => /AND EXISTS \(/.test(t)));
    }
    /* Honest timestamps rather than NOW(): the release moment for the phantom,
       the submit time for the skip. */
    ok('backfill: the phantom uses the claim-release time, not NOW()',
       /ps_signup_failed_at   = updated_at/.test(bf) && !/ps_signup_failed_at\s*=\s*NOW\(\)/.test(bf));
    ok('backfill: the skip uses the submit time, not NOW()',
       /ps_signup_skipped_at     = created_at/.test(bf));
    /* initDB throwing exits the process; a cosmetic backfill must not be able
       to take the service down. */
    ok('backfill: wrapped so it cannot kill boot',
       /catch \(err\)[\s\S]{0,120}?backfill failed \(non-fatal\)/.test(bf));
  }

  /* Alerting — the reason today's 400 was invisible is that nobody was
     watching, so the state alone is half a fix. */
  {
    const fn = src.slice(src.indexOf('async function recordPartnerStackFailure'),
                         src.indexOf('async function clearPartnerStackFailure'));
    ok('alertA: a failure alerts Slack immediately', /alertOps\('critical', 'PartnerStack'/.test(fn));
    ok('alertA: the qualification alert names the money', /the \$50 did not fire/.test(fn));
    ok('alertA: the alert says the claim was released and it will retry',
       /claim has been released/.test(fn));
    ok('alertA: both kinds write their own column pair',
       /ps_signup_failed_at = NOW\(\), ps_signup_fail_reason = \$2/.test(fn) &&
       /ps_qualify_failed_at = NOW\(\), ps_qualify_fail_reason = \$2/.test(fn));
    ok('alertA: recording a failure can never throw into the caller',
       /catch \(err\)[\s\S]{0,160}?non-blocking/.test(fn));
  }

  /* THE REGRESSION THAT MATTERS: the claim release is what kept hello.com
     retryable when the qualification 400'd. Recording must be additive. */
  {
    const sig = src.slice(src.indexOf('async function runPartnerStackSignup'), src.indexOf('/* ── READ-BACK'));
    const rel = sig.indexOf('ps_signup_sent_at = NULL');
    const rec = sig.indexOf("recordPartnerStackFailure('signup'");
    ok('regression: the signup claim release still exists', rel !== -1);
    ok('regression: the failure is recorded AFTER the release, not instead of it', rel < rec);
    const qual = src.slice(src.indexOf('async function sendQualificationForDomain'),
                           src.indexOf('function startPartnerStackQualificationPoll'));
    const qrel = qual.indexOf('ps_qualified_sent_at = NULL');
    const qrec = qual.indexOf("recordPartnerStackFailure('qualify'");
    ok('regression: the qualification claim release still exists', qrel !== -1);
    ok('regression: qualification failure recorded AFTER the release', qrel < qrec);
    ok('regression: the claim is still taken BEFORE the send',
       qual.indexOf('ps_qualified_sent_at = NOW()') < qual.indexOf('await sendAction('));
  }
  /* A domain that failed and later recovered must not sit red forever. */
  /* ORDER, not a character budget. These were /X[\s\S]{0,160}?Y/, and a comment
     added between the log line and the clear pushed them past the window — the
     assertion failed for a reason that had nothing to do with what it checks.
     Scoped to the function and compared by position instead. */
  {
    const sign = src.slice(src.indexOf('async function runPartnerStackSignup'),
                           src.indexOf('/* \u2500\u2500 READ-BACK: did the conversion actually create a customer?'));
    const at = sign.indexOf('Conversion sent:');
    ok('recovery: a successful conversion clears the failure and skip reasons',
       at !== -1 && at < sign.indexOf("clearPartnerStackFailure('signup', session_id)"));
    const qual = src.slice(src.indexOf('async function sendQualificationForDomain'),
                           src.indexOf('function startPartnerStackQualificationPoll'));
    const qat = qual.indexOf('Qualification sent:');
    ok('recovery: a successful qualification clears its failure',
       qat !== -1 && qat < qual.indexOf("clearPartnerStackFailure('qualify', claimedSession)"));
  }
  ok('recovery: clearing signup also clears the skip reason',
     /ps_signup_failed_at = NULL, ps_signup_fail_reason = NULL, ps_signup_skipped_reason = NULL/.test(src));
  /* Every skip guard records WHY, or "not sent" stays ambiguous on screen. */
  for (const r of ['disqualified', 'no_customer_key', 'test_email', 'already_sent'])
    ok(`skipA: the ${r} guard records its reason`,
       new RegExp(`recordPartnerStackSkip\\(session_id, '${r}'\\)`).test(src));
  ok('skipA: a phantom conversion records a failure, not a skip',
     /reason: 'phantom_200'/.test(src));

  /* ── Batch B: gap card derived from the ladder, per-domain SF state ── */

  /* The live inconsistency this fixes: Overview said "2 no conversion" while
     the Partners tab said one failure and one correct skip — two independent
     queries telling two stories about the same four domains. */
  {
    const fn = src.slice(src.indexOf('async function partnerRevenueGaps'),
                         src.indexOf("app.get('/monitor/partner-gaps'"));
    ok('gapB: check A reads the ladder rather than its own query',
       /const lifecycle = await partnerLifecycle\(\)/.test(fn));
    ok('gapB: it no longer runs a second bespoke missed-conversion query',
       !/HAVING COUNT\(ps_signup_sent_at\) = 0/.test(fn));
    /* A skip is the system working. Counting it meant gushwork.ai sat in the
       alert forever and could never clear. */
    /* Scoped to the `missed` filter itself. Asserting against the whole
       function passed even with 'skipped' added back INTO the gap list,
       because skippedDomains legitimately mentions it just below — a mutation
       survived on exactly that, restoring the live inconsistency. */
    {
      const missedFilter = fn.slice(fn.indexOf('const missed ='), fn.indexOf('const skippedDomains'));
      ok('gapB: a SKIPPED domain is not counted as a gap',
         missedFilter.length > 40 && !/skipped/.test(missedFilter), missedFilter.slice(0, 160));
      ok('gapB: skipped domains are collected separately', /skippedDomains = lifecycle\.domains\.filter/.test(fn));
    }
    ok('gapB: a failed conversion IS a gap', /d\.state === 'conversion_failed'/.test(fn));
    ok('gapB: never-attempted counts only past the grace window',
       /d\.state === 'conversion_pending'[\s\S]{0,200}?graceMs/.test(fn));
    ok('gapB: skips are still reported, separately from gaps', /skipped: skippedDomains\.map/.test(fn));
    /* "Nothing was eligible to check" must not render as "checked and clean". */
    ok('gapB: no-candidates is flagged checked:false',
       /checked: false, reason: 'no_candidates'/.test(fn));
    ok('gapB: a real check is flagged checked:true with the candidate count',
       /checked: true, opportunityDomains: have\.size, candidates:/.test(fn));
  }
  ok('gapB: the UI distinguishes unavailable / none-eligible / checked',
     /oc\.checked===false\?"none eligible to check yet"/.test(src) &&
     /Opportunity check unavailable/.test(src));
  /* "step 10" meant nothing to anyone but the person who wrote it. */
  ok('gapB: the label names the money, not the step number',
     /the \$50 can never fire either/.test(src) && !/step 10 can never fire/.test(src));

  /* Per-domain Salesforce state. */
  ok('sfB: the state table is declared', /CREATE TABLE IF NOT EXISTS partner_domain_sf_state/.test(dbjs));
  ok('sfB: keyed by domain', /customer_key      TEXT PRIMARY KEY/.test(dbjs));
  ok('sfB: table init cannot kill boot',
     /Partner SF-state table init FAILED \(non-fatal\)/.test(dbjs));
  {
    const fn = src.slice(src.indexOf('async function refreshPartnerDomainSfState'),
                         src.indexOf('function startPartnerStackQualificationPoll'));
    eq('sfB: four states', /const PS_SF_STATES = \[([^\]]+)\]/.exec(src)[1].split(',').length, 4);
    /* The point of the assertion is the ABSENCE of a conversion filter: the row
       worth acting on daily is "Opportunity exists, checkbox unticked", and a
       domain that has not converted still needs a state. The table alias moved
       to `l` in PR 24 when the query gained a LEFT JOIN for the partner
       identity, so the old literal `FROM leads\n WHERE ps_xid` no longer
       matched — asserted on the predicates rather than the layout now. */
    ok('sfB: refreshed for ALL partner domains, not just qualify candidates',
       /FROM leads l\b/.test(fn)
       && /WHERE l\.ps_xid IS NOT NULL AND l\.ps_customer_key IS NOT NULL/.test(fn)
       && !/ps_signup_sent_at IS NOT NULL/.test(fn));
    /* Qualified_Demo__c rides along on the existence query, so ticked vs
       unticked costs no extra Salesforce call. */
    ok('sfB: ticked state comes from the same Opportunity query',
       /Qualified_Demo__c/.test(sfmod) && /qualified: r\.Qualified_Demo__c === true/.test(sfmod));
    ok('sfB: a ticked Opportunity wins over an unticked one for the same domain',
       /if \(!prev \|\| \(r\.qualified && !prev\.qualified\)\)/.test(fn));
    /* sf_lead_conversion_log is keyed by prospect_email, NOT by domain —
       assuming a domain key would silently match nothing. */
    ok('sfB: the sfopp log is joined on EMAIL, not on domain',
       /LOWER\(prospect_email\) AS email/.test(fn) && /errorsByEmail/.test(fn));
    ok('sfB: it costs no Salesforce call — the log is on the warehouse',
       /awsPool\.query\([\s\S]{0,120}?gist\.sf_lead_conversion_log/.test(fn));
    ok('sfB: the warehouse read is bounded by a timeout',
       /withTimeout\(awsPool\.query\([\s\S]{0,400}?PS_CUSTOMER_QUERY_TIMEOUT_MS/.test(fn));
    /* A stale row saying what we last verified beats one overwritten with a
       guess during an outage. */
    ok('sfB: an unavailable Salesforce leaves the table untouched',
       /if \(!sf\.ok\)[\s\S]*?return \{ ok: false, reason: sf\.reason \}/.test(fn));
    ok('sfB: a missing sfopp log degrades to no_opportunity, it does not throw',
       /Could not read sf_lead_conversion_log/.test(fn));
  }
  /* IT IS ITS OWN JOB NOW, and this assertion is inverted from batch B on
     purpose. Chaining it onto the qualification poll meant it never ran once:
     that poll has three `return`s inside its try, and a return there exits the
     whole function — the finally still fires so everything looks healthy, but
     anything after the try/finally is skipped. The common case takes an early
     return, so it was unreachable on every tick forever. A boot-time call
     alone would have made it run once per deploy and look correct. */
  {
    /* Scoped to the poll function ONLY. Slicing to the scheduler constant ran
       through refreshPartnerDomainSfState's own definition, so the name was
       always present and the assertion could never fail. */
    const poll = src.slice(src.indexOf('async function runPartnerStackQualificationPoll'),
                           src.indexOf('async function sendQualificationForDomain'));
    ok('sfC: the refresh is NOT chained onto the qualification poll',
       !/refreshPartnerDomainSfState/.test(poll));
    /* The pattern itself, so it cannot come back: nothing meaningful may sit
       after the try/catch/finally in a function whose try contains a return. */
    const afterFinally = poll.slice(poll.lastIndexOf('_psQualifyRunning = false;'));
    ok('sfC: nothing is chained after the poll try/finally at all',
       !/await [a-zA-Z]/.test(afterFinally.replace(/\/\*[\s\S]*?\*\//g, '')), afterFinally.slice(0, 120));
  }
  /* ── PR 23 / C2: the three stamps reach the MIRROR ────────────────────
     They were in syncToAWS's column list with COALESCE clauses and were still
     NULL on gw_form_leads for every row, permanently. Not a missing column —
     call order. syncToAWS runs from /partial and /submit, and all three stamps
     are written after res.json() by a deferred job or a sweep, so the bind
     value was always null. The handover doc said "the next real form submit
     proves it"; no submit could ever have proved it. */
  {
    ok('C2: there is a targeted mirror write for the stamps',
       /function syncPartnerStackStampToAWS\(session_id, column, value\)/.test(src));
    /* The column is interpolated into SQL, so the allow-list is what makes
       that safe — not the fact that today's callers are all internal. */
    ok('C2: the column name is allow-listed, not interpolated freely',
       /const PS_AWS_STAMP_COLUMNS = \['ps_signup_sent_at', 'ps_signup_verified_at', 'ps_qualified_sent_at'\];/.test(src));
    const fn = src.slice(src.indexOf('function syncPartnerStackStampToAWS'),
                         src.indexOf('function sendSlack'));
    ok('C2: an unknown column is refused rather than interpolated',
       /PS_AWS_STAMP_COLUMNS\.includes\(column\)/.test(fn) && /Refusing to mirror unknown/.test(fn));
    /* A targeted UPDATE, never syncToAWS with a partial object: that upsert
       sets disqualified = EXCLUDED.disqualified with NO COALESCE, so a partial
       object passes false and clears a real disqualification on the mirror the
       dialer reads. */
    ok('C2: it is a targeted UPDATE on one column',
       /UPDATE gw_form_leads SET \$\{column\} = \$2, updated_at = NOW\(\) WHERE session_id = \$1/.test(fn));
    ok('C2: it never calls syncToAWS', !/syncToAWS/.test(fn));
    /* Two of the six sites RELEASE a claim by writing NULL, so unlike
       syncPartnerIdentityToAWS this must NOT be COALESCE'd — the mirror has to
       be able to follow Railway back to null. */
    ok('C2: it can clear a stamp as well as set one, so a release mirrors',
       /\[session_id, value \|\| null\]/.test(fn) && !/COALESCE/.test(fn));
    /* File-wide, so a site added later cannot be unawaited either. Excludes
       the definition itself and the doc comment above it. */
    {
      const after = src.slice(src.indexOf('function sendSlack'));
      const total   = (after.match(/syncPartnerStackStampToAWS\(/g) || []).length;
      const awaited = (after.match(/await syncPartnerStackStampToAWS\(/g) || []).length;
      ok('C2: every mirror call site in the file is awaited',
         total >= 6 && awaited === total, `${awaited} of ${total} awaited`);
    }
    ok('C2: a mirror failure is recorded, not swallowed',
       /recordFailure\('AWS sync'/.test(fn));
    ok('C2: and it never rejects, so a caller can await it to order two writes',
       /\.catch\(\(err\) => \{/.test(fn) && /return Promise\.resolve\(\);/.test(fn));

    /* ── THE PAIRING PROPERTY ──────────────────────────────────────────
       Every Railway write of a stamp column must be followed by a mirror write
       of THAT column. This is the invariant C2 actually rests on, and it is
       asserted here rather than by counting call sites — counting is what the
       first version of this test did (`mirrored >= railway`, plus a file-wide
       `total >= 6`) and dropping any ONE of the eight mirror lines still
       passed. Six line-targeted mutations, six survivors.

       Writing this assertion then found a real defect in the change it was
       meant to protect: the verify sweep mirrored ps_signup_verified_at
       WITHOUT ps_signup_sent_at, so a process that died between the claim and
       the success mirror left gw_form_leads reading "verified but never sent".

       Enumerated from the source, so a ninth write added later has to pair up
       or fail here.

       THE THREE EXCEPTIONS ARE THE CLAIMS, and they are checked to BE claims
       rather than merely tolerated. A claim is a Railway-internal lock taken
       before the HTTP call — not a fact about PartnerStack — and mirroring it
       would put a WAN write in front of the send. Each is identified by the
       conditional guard that makes it a claim. */
    {
      const STAMPS = ['ps_signup_sent_at', 'ps_signup_verified_at', 'ps_qualified_sent_at'];
      const re = new RegExp(`(${STAMPS.join('|')})\\s*=\\s*(NOW\\(\\)|NULL|COALESCE\\([^)]*\\))`, 'g');
      const writes = [];
      let m;
      while ((m = re.exec(src)) !== null) {
        /* Only writes inside an UPDATE leads statement — the funnel and the
           ladder mention these columns constantly in FILTER clauses. */
        const back = src.slice(Math.max(0, m.index - 900), m.index);
        if (!/UPDATE leads/.test(back)) continue;
        writes.push({ col: m[1], op: m[2], at: m.index, end: m.index + m[0].length,
                      line: src.slice(0, m.index).split('\n').length });
      }
      ok('C2/pairing: the Railway stamp writes were found at all', writes.length >= 9, String(writes.length));

      /* THE WINDOW IS SEMANTIC, NOT A CHARACTER COUNT. A confirmed write is
         mirrored before anything else happens; a CLAIM's mirror can only come
         after the HTTP call has returned, because until then there is nothing
         to vouch for. So the boundary is the next sendConversion/sendAction,
         and "is it mirrored before the next send?" separates the two kinds
         exactly — measured on the real source, the three that cross a send are
         precisely the three claims.

         A fixed character budget cannot do this. The first attempt used 1400
         chars and found ONE unpaired write instead of three, because a claim's
         far-away success mirror fell inside the window. It also breaks the
         moment someone adds a comment, which is how two assertions in this
         file already failed for reasons unrelated to what they check. */
      const unpaired = [];
      for (const w of writes) {
        const rest = src.slice(w.end, w.end + 4000);
        const sendAt = rest.search(/await (?:sendConversion|sendAction)\(/);
        const window = sendAt === -1 ? rest : rest.slice(0, sendAt);
        const mirrored = new RegExp(`syncPartnerStackStampToAWS\\([^,]+,\\s*'${w.col}'`).test(window);
        if (!mirrored) unpaired.push(w);
      }

      /* Exactly three, and each must BE a claim: a conditional UPDATE that
         only wins while the stamp is still NULL, sitting before a send. */
      eq('C2/pairing: exactly three Railway stamp writes are unmirrored',
         unpaired.length, 3);
      for (const w of unpaired) {
        const around = src.slice(Math.max(0, w.at - 600), w.end + 600);
        ok(`C2/pairing: the unmirrored write at line ${w.line} is a CLAIM, not an oversight`,
           w.op === 'NOW()' && new RegExp(`AND ${w.col} IS NULL`).test(around),
           `${w.col} = ${w.op}`);
      }
      /* Every OTHER write pairs up. Stated as its own assertion so the count
         above cannot pass by having three different writes unmirrored. */
      ok('C2/pairing: every non-claim stamp write is mirrored before any send',
         writes.length - unpaired.length >= 6, `${writes.length - unpaired.length} paired`);

      /* And the confirmation paths must vouch for every column they can. The
         verify sweep reads the customer back, so BOTH sent and verified are
         true there and both must reach the mirror — this is the assertion that
         caught the real defect described above. */
      const ver = src.slice(src.indexOf('async function runPartnerStackConversionVerify'),
                            src.indexOf('/* \u2500\u2500 C3: A FAILED CONVERSION IS RETRIED'));
      const exists = ver.slice(ver.indexOf('if (out.exists) {'));
      for (const col of ['ps_signup_sent_at', 'ps_signup_verified_at'])
        ok(`C2/pairing: a verified conversion mirrors ${col}, closing the claim window`,
           new RegExp(`syncPartnerStackStampToAWS\\([^,]+,\\s*'${col}'`).test(exists.slice(0, 2200)), col);
    }

    /* ── All SIX Railway write sites must mirror ──────────────────────
       Derived from the Railway writes rather than listed by hand: a seventh
       site added later fails this instead of silently not mirroring. */
    const sites = [
      ['signup claim',        "async function runPartnerStackSignup",       "/* ── READ-BACK", 'ps_signup_sent_at'],
      ['verify sweep',        "async function runPartnerStackConversionVerify", "function startPartnerStackConversionVerify", 'ps_signup_verified_at'],
      ['qualification claim', "async function sendQualificationForDomain",  "function startPartnerStackQualificationPoll", 'ps_qualified_sent_at'],
    ];
    for (const [label, from, to, col] of sites) {
      const body = src.slice(src.indexOf(from), src.indexOf(to));
      const railway = (body.match(/UPDATE leads[\s\S]{0,140}?ps_(?:signup_sent_at|signup_verified_at|qualified_sent_at)\s*=\s*(?:NOW\(\)|NULL)/g) || []).length;
      const mirrored = (body.match(/syncPartnerStackStampToAWS\(/g) || []).length;
      ok(`C2: every Railway stamp write in the ${label} has a mirror write`,
         mirrored >= railway && railway > 0, `railway=${railway} mirrored=${mirrored}`);
      ok(`C2: the ${label} mirrors ${col}`,
         new RegExp("syncPartnerStackStampToAWS\\([^)]*'" + col + "'").test(body));
      /* EVERY call, not "at least one". The qualify site has two — a set and
         a release — and dropping the await from one of them left a
         `/await sync…/` test green while the release could overtake the claim
         it follows. Both are promises against the same row and only order
         decides which value the mirror keeps. */
      const total   = (body.match(/syncPartnerStackStampToAWS\(/g) || []).length;
      const awaited = (body.match(/await syncPartnerStackStampToAWS\(/g) || []).length;
      ok(`C2: EVERY mirror write in the ${label} is awaited`,
         total > 0 && awaited === total, `${awaited} of ${total} awaited`);
    }
  }

  /* ── PR 23 / C3: a failed conversion is RETRIED ────────────────────────
     The release carried the comment "so this domain can be retried rather than
     silently lost" and nothing retried it. One timeout at submit time was one
     affiliate permanently unpaid. */
  {
    const fn = src.slice(src.indexOf('async function runPartnerStackConversionRetry'),
                         src.indexOf('function startPartnerStackConversionRetry'));
    ok('C3: the retry sweep exists', fn.length > 200);
    ok('C3: overlapping runs are prevented',
       /if \(_psRetryRunning\) return;/.test(fn) && /_psRetryRunning = true;/.test(fn));

    /* THE WHOLE DESIGN. A conversion that reported failure may still have
       landed — a timeout after PartnerStack processed it, a 5xx from a proxy in
       front of a successful write. Re-sending blind credits the affiliate
       twice and PartnerStack cannot undo a double credit. */
    const fetchAt = fn.indexOf('await fetchCustomer(');
    const sendAt  = fn.indexOf('await sendConversion(');
    ok('C3: it asks whether the customer exists BEFORE considering a re-send',
       fetchAt !== -1 && sendAt !== -1 && fetchAt < sendAt, `fetch@${fetchAt} send@${sendAt}`);
    ok('C3: an existing customer is stamped, NOT re-sent',
       /the original conversion DID land, not re-sending/.test(fn));
    /* Stamped verified too — the existence check just did the read-back
       sweep's job for this row. */
    ok('C3: a recovered conversion is stamped verified as well as sent',
       /ps_signup_verified_at = COALESCE\(ps_signup_verified_at, NOW\(\)\)/.test(fn));
    ok('C3: "could not tell" leaves the row completely alone',
       /if \(!out\.ok\)/.test(fn) && /cannot tell whether the original landed/.test(fn));

    /* The backoff must exceed PartnerStack's indexing lag or the existence
       check asks about a conversion that landed and is not yet visible, gets a
       404, and re-sends it — the exact double credit the fetch prevents. */
    ok('C3: the backoff is DERIVED from the read-back grace, not chosen apart from it',
       /const PS_RETRY_BACKOFF_MIN\s+= Math\.max\(30, PS_VERIFY_GRACE_MIN \* 2\);/.test(src));

    /* ps_signup_sent_at is once per DOMAIN. Stamping a second row for a domain
       that already converted violates leads_ps_signup_once_idx — and the
       affiliate has already been credited, so nothing is owed. */
    ok('C3: domains where another lead already converted are excluded',
       /NOT EXISTS \([\s\S]{0,200}?o\.ps_customer_key = l\.ps_customer_key[\s\S]{0,80}?o\.ps_signup_sent_at IS NOT NULL/.test(fn));
    /* A disqualified lead never fires a conversion. CLAUDE.md: a GUARD, not a
       flow property, and this is a new path into the same send. */
    ok('C3: a disqualified lead is never retried',
       /l\.disqualified IS NOT TRUE/.test(fn));

    /* Bounded, or a permanent 400 retries every quarter hour forever and
       buries the rows that could still succeed. */
    ok('C3: attempts are bounded', /ps_signup_retry_count, 0\) < \$\{PS_RETRY_MAX_ATTEMPTS\}/.test(fn));
    /* ── An ACKNOWLEDGED failure is not retried ────────────────────────
       The ack's scope was written for alerting, before this sweep existed.
       "Stop telling me" without "stop trying" means an ack cannot express the
       case it was built for: test.com's phantom_200 was a customer deleted in
       PartnerStack BY HAND, so retrying re-creates the record somebody removed
       on purpose. That is not theoretical — the sweep's first boot run on
       7 Sept 2026 did it, and PartnerStack answered 200 while creating nothing
       again, so the domain was heading for five attempts and an exhaustion
       alert over test data. */
    ok('C3/ack: an acknowledged failure is excluded from the retry sweep',
       /AND l\.ps_failure_ack_at IS NULL/.test(fn));
    /* All THREE consumers of the ack, asserted together. If a fourth is added
       and leaves the ack out, an acknowledged failure starts demanding action
       again through the new one — which is how this gap appeared in the first
       place. */
    {
      const consumers = [
        ['the unbounded Needs-attention count', /COUNT\(\*\) FILTER \(WHERE NOT acknowledged\) AS needs_attention/],
        ['the page-derived fallback count',     /PS_LADDER_FAILED\.includes\(d\.state\) && d\.acknowledged !== true/],
        ['the health row',                      /AND ps_failure_ack_at IS NULL\)/],
        ['the conversion retry sweep',          /AND l\.ps_failure_ack_at IS NULL/],
      ];
      for (const [label, re] of consumers)
        ok(`C3/ack: ${label} respects the acknowledgement`, re.test(src), label);
    }
    ok('C3: and there is a give-up window as well as a count',
       /ps_signup_failed_at > NOW\(\) - INTERVAL '\$\{PS_RETRY_GIVE_UP_D\} days'/.test(fn));
    /* The count must survive a crash mid-attempt, or the bound never binds. */
    const countAt = fn.indexOf('ps_signup_retry_count = COALESCE');
    ok('C3: the attempt is counted BEFORE it is made, so a crash cannot loop forever',
       countAt !== -1 && countAt < fetchAt, `count@${countAt} fetch@${fetchAt}`);

    /* Claim-first on the re-send, exactly like the first attempt. */
    const claimAt = fn.indexOf('SET ps_signup_sent_at = NOW()');
    ok('C3: the re-send claims the domain BEFORE sending', claimAt !== -1 && claimAt < sendAt);
    ok('C3: a concurrent claim is read as already-sent', /err\.code === '23505'/.test(fn));
    const relAt = fn.lastIndexOf('ps_signup_sent_at = NULL');
    ok('C3: a failed re-send releases the claim again', relAt !== -1 && relAt > sendAt);

    /* Exhaustion is its own event: the per-attempt failures are noise once the
       bound is reached, and this is the line that means a human must send the
       conversion by hand. */
    /* ── IMMEDIATE, not through recordFailure ──────────────────────────
       It went through recordFailure until 7 Sept 2026, which turned out to be
       silent for this source entirely — and even once fixed, recordFailure
       alerts on a streak of three, so waiting for it means waiting for THREE
       exhausted domains before anyone hears about the first. Exhaustion is
       terminal: nothing retries after it, so there is no later signal to
       accumulate towards.

       The alertOps CALL, not just the message text — a previous version of
       this assertion matched only the string and stayed green when
       recordFailure( was replaced with void (. */
    ok('C3: exhausting the retries alerts IMMEDIATELY, not on a streak',
       /alertOps\('critical', 'PartnerStack', 'Conversion retries exhausted — affiliate NOT credited', \{/.test(fn)
       && /GIVING UP on/.test(fn));
    ok('C3: and it no longer routes exhaustion through the streak path',
       !/recordFailure\([^)]*conversion retries exhausted/.test(fn));
    /* The alert has to say what a human must actually do — an alert nobody can
       act on gets ignored, which is this file's other recurring lesson. */
    ok('C3: the exhaustion alert names the consequence and the action',
       /'Impact': 'This conversion will NEVER be sent again/.test(fn)
       && /'What to do': 'Send the conversion by hand/.test(fn));
    ok('C3: exhaustion is gated on the attempt count reaching the bound',
       /const exhausted = attemptsNow >= PS_RETRY_MAX_ATTEMPTS;/.test(fn)
       && /if \(exhausted\) \{/.test(fn));
    ok('C3: and it says the affiliate is still owed',
       /the affiliate is still owed and nothing else will retry/.test(fn));

    /* ── Each of the retry's THREE outcomes mirrors ────────────────────
       These are asserted per-outcome because the pairing property above cannot
       see them: they all mirror the ONE Railway write the re-claim made, so
       from the pairing rule's point of view they are a claim's deferred mirror
       and it stops looking after the first. Dropping the re-send success
       mirror survived every other assertion in this file. */
    {
      const recovered = fn.slice(fn.indexOf('if (out.exists) {'), fn.indexOf('/* A definitive 404'));
      for (const col of ['ps_signup_sent_at', 'ps_signup_verified_at'])
        ok(`C3: a recovered conversion mirrors ${col}`,
           new RegExp(`syncPartnerStackStampToAWS\\([^,]+,\\s*'${col}'`).test(recovered), col);

      const success = fn.slice(fn.indexOf('Conversion RETRY sent'));
      const successBlock = success.slice(0, success.indexOf('continue;'));
      ok('C3: a successful re-send mirrors ps_signup_sent_at',
         /syncPartnerStackStampToAWS\([^,]+,\s*'ps_signup_sent_at', new Date\(\)\)/.test(successBlock),
         successBlock.slice(0, 200));

      const release = fn.slice(fn.lastIndexOf('ps_signup_sent_at = NULL'));
      ok('C3: a released re-claim mirrors the release',
         /syncPartnerStackStampToAWS\([^,]+,\s*'ps_signup_sent_at', null\)/.test(release));
    }

    /* Boot-then-interval: a sweep that only runs on an interval loses its
       first window to every deploy. */
    ok('C3: it runs at boot as well as on the interval',
       /run\('boot'\);/.test(src.slice(src.indexOf('function startPartnerStackConversionRetry'),
                                       src.indexOf('function startPartnerStackConversionVerify'))));
    ok('C3: it is started from start()', /startPartnerStackConversionRetry\(\);/.test(src));
    ok('C3: the retry columns and a partial index exist',
       /ps_signup_retry_count INTEGER DEFAULT 0/.test(dbjs)
       && /leads_ps_signup_retryable_idx/.test(dbjs));
  }

  /* ── PR 22: the events under the snapshot ─────────────────────────────
     sf_state moves in BOTH directions — correctly, it is what Salesforce says
     right now. first_ticked_at and first_opportunity_at are the events beneath
     it and may only ever move one way. Everything that must not go backwards
     reads those; an untick leaves them alone. */
  {
    ok('sfC/C6: the table carries both set-once event columns',
       /ADD COLUMN IF NOT EXISTS first_ticked_at\s+TIMESTAMPTZ/.test(dbjs)
       && /ADD COLUMN IF NOT EXISTS first_opportunity_at TIMESTAMPTZ/.test(dbjs));
    /* The whole point: COALESCE keeps the EARLIEST observation, so a later
       refresh seeing exists_unticked cannot clear a tick that happened. */
    const fn = src.slice(src.indexOf('async function refreshPartnerDomainSfState'),
                         src.indexOf('let _psSfLastRead') >= 0
                           ? src.indexOf('const PS_SF_REFRESH_INTERVAL_MS')
                           : src.length);
    /* The CASEs read the UNNEST alias since PR 25 batched the write; they were
       on $2 when it was one statement per domain. */
    ok('sfC/C6: first_ticked_at is stamped only when the box is actually ticked',
       /CASE WHEN st = 'ticked' THEN NOW\(\) END/.test(fn));
    ok('sfC/C6: first_opportunity_at is stamped for either Opportunity state',
       /CASE WHEN st IN \('ticked','exists_unticked'\) THEN NOW\(\) END/.test(fn));
    ok('sfC/C6: neither can ever be cleared or moved later',
       /first_ticked_at\s+= COALESCE\(partner_domain_sf_state\.first_ticked_at,\s+EXCLUDED\.first_ticked_at\)/.test(fn)
       && /first_opportunity_at = COALESCE\(partner_domain_sf_state\.first_opportunity_at, EXCLUDED\.first_opportunity_at\)/.test(fn));
    ok('sfC/C6: and neither is ever set to NULL anywhere in the refresh',
       !/first_ticked_at\s*=\s*NULL/.test(fn) && !/first_opportunity_at\s*=\s*NULL/.test(fn));
    /* first_ticked_at set implies first_opportunity_at set — the 'ticked' arm
       of the second CASE is what makes the payment tail of the funnel nest by
       construction rather than by luck. Asserted because it is exactly the
       kind of load-bearing property nothing else enforces. */
    ok('sfC/C6: a ticked domain also stamps first_opportunity_at, so the tail nests',
       /CASE WHEN st IN \('ticked','exists_unticked'\)/.test(fn));
    /* The same-table backfill exists only so a currently-true state is stamped
       now rather than 15 minutes from now, and must be idempotent. */
    ok('sfC/C6: the backfill only ever touches rows where the stamp is NULL',
       /first_ticked_at IS NULL AND sf_state = 'ticked'/.test(dbjs));
    ok('sfC/C6: and the same for the Opportunity stamp',
       /first_opportunity_at IS NULL AND sf_state IN \('exists_unticked', 'ticked'\)/.test(dbjs));
    /* NOT backfilled from leads: these columns mean "we observed this", and an
       inferred timestamp in an observational column gets read as a
       measurement. The funnel ORs the two sources instead. */
    ok('sfC/C6: the stamps are NOT backfilled from ps_qualified_sent_at',
       !/first_ticked_at[\s\S]{0,200}?ps_qualified_sent_at/.test(dbjs));
    /* The client cannot label an untick-after-payment without them. */
    /* Scoped to partnerLifecycle: the client cannot label an
       untick-after-payment if the columns never leave the server. */
    const life = src.slice(src.indexOf('async function partnerLifecycle'),
                           src.indexOf('const PS_FUNNEL_STAGE_SQL'));
    ok('sfC/C6: both are selected out of the table',
       /first_ticked_at, first_opportunity_at/.test(life));
    ok('sfC/C6: and both are attached to each domain row',
       /d\.first_ticked_at = sf \? sf\.first_ticked_at : null;/.test(life)
       && /d\.first_opportunity_at = sf \? sf\.first_opportunity_at : null;/.test(life));
  }

  /* ── C7: an already-paid domain is not a revenue loss ── */
  {
    const frag = lift(src, 'const PS_FUNNEL_STAGE_SQL = `');
    /* The EXPRESSION only, back to its own COUNT — slicing a fixed number of
       characters swept in the comment above it, which names the very column
       the second assertion checks is absent. */
    const at   = frag.indexOf('AS lost_no_opp');
    const lost = frag.slice(frag.lastIndexOf('COUNT(DISTINCT', at), at);
    ok('C7: booked-with-no-Opportunity excludes a domain that already got paid',
       /ps_qualified_sent_at IS NULL/.test(lost), lost.slice(-200));
    /* Deliberately NOT excluded on first_opportunity_at: a domain that had an
       Opportunity, was never paid, and no longer has one is a genuine leak and
       must stay counted. */
    ok('C7: it is NOT excluded merely for having once had an Opportunity',
       !/first_opportunity_at/.test(lost), lost.slice(-200));
  }

  /* ── PR 24: the two Salesforce writes ─────────────────────────────────
     Both fields existed and were verified with a live round-trip on 4 Sept;
     the code was never written. */
  {
    /* ── hear_about_us_raw__c on Lead ──────────────────────────────
       A partner-referred lead who arrived on a paid ad is TWO real facts and
       hear_about_us__c can only hold one. partnerHearAboutUs() used to destroy
       the other; hear_about_us_raw keeps it, and this puts it in front of the
       AE rather than only on our dashboard. */
    ok('sfw: hear_about_us_raw__c is in the Lead field map',
       /hear_about_us_raw: 'hear_about_us_raw__c'/.test(sfmod));
    /* One entry in CUSTOM_FIELD_MAP is not enough — the payload has to carry
       it. This is the computed-vs-plumbed seam: the map is the renderer, the
       call site is the producer, and asserting only the map would be the same
       half-assertion as C5's. */
    ok('sfw: /submit passes the RAW value, not the partner-overwritten one',
       /hear_about_us:hearAboutUsFinal,hear_about_us_raw:hear_about_us,/.test(src));
    /* buildLeadFields drops empty strings, so an organic lead with nothing
       typed sends no field at all rather than a blank. */
    ok('sfw: an empty value is dropped rather than sent blank',
       /payload\[srcKey\] !== undefined && payload\[srcKey\] !== null && payload\[srcKey\] !== ''/.test(sfmod));

    /* ── Partner_Source__c on Opportunity ──────────────────────────── */
    const fn = src.slice(src.indexOf('async function refreshPartnerDomainSfState'),
                         src.indexOf('/* ITS OWN JOB, not chained'));
    ok('sfw: Partner_Source__c is written from the SF-state refresh',
       /updateOpportunityFields\(oppId, \{ Partner_Source__c: source \}\)/.test(fn));
    /* Written from the poll rather than mapped through Lead conversion,
       because it runs strictly AFTER sfopp creates the Opportunity — no race,
       no conversion field mapping — and it covers Opportunities created any
       way, which a Lead field cannot: none of our 29 custom Lead fields
       survive conversion. */
    ok('sfw: it only writes where an Opportunity actually exists',
       /if \(PS_SF_OPP_WRITE_ENABLED\) \{/.test(fn)
       && /const oppId = oppIdByKey\.get\(d\.customer_key\);\s*\n\s*if \(!oppId\) continue;/.test(fn));
    /* ONE display chain, three surfaces: Slack, the dashboard and
       hear_about_us. A fourth spelling of a partner name would be a fourth
       thing an SDR cannot search for. */
    ok('sfw: the value comes from the shared display chain, not a fresh guess',
       /partnerDisplayName\(\s*\n?\s*\{ name: d\.partner_name, email: d\.partner_email \}, d\.partner_key\)/.test(fn));
    ok('sfw: and the identity is selected in the domains query to feed it',
       /MAX\(l\.ps_partner_name\)\s+AS partner_name/.test(fn)
       && /MAX\(l\.ps_partner_email\) AS partner_email/.test(fn));

    /* IDEMPOTENT, or every partner Opportunity is PATCHed every 15 minutes
       forever — 2,880 writes a day at 30 domains, growing linearly. */
    ok('sfw: the write is idempotent on what was last written',
       /if \(!source \|\| source === d\.sf_partner_source\) continue;/.test(fn));
    ok('sfw: and what was written is recorded so it can be compared next tick',
       /SET sf_partner_source = \$2, sf_partner_source_at = NOW\(\)/.test(fn));
    ok('sfw: the idempotence columns exist',
       /ADD COLUMN IF NOT EXISTS sf_partner_source\s+TEXT/.test(dbjs)
       && /ADD COLUMN IF NOT EXISTS sf_partner_source_at TIMESTAMPTZ/.test(dbjs));
    /* The stamp must NOT be written on failure, or one transient 5xx means the
       value never reaches Salesforce and nothing ever retries. Safe because
       the PATCH is idempotent on the Salesforce side too. */
    {
      const okAt   = fn.indexOf('if (res.ok) {');
      const stamp  = fn.indexOf('SET sf_partner_source = $2');
      const elseAt = fn.indexOf('} else {', okAt);
      ok('sfw: the stamp is only written on SUCCESS, so a failure retries',
         okAt !== -1 && stamp > okAt && stamp < elseAt, `ok@${okAt} stamp@${stamp} else@${elseAt}`);
    }

    /* ── LOUD. This is the requirement the handover doc calls out ──────
       A silent failure here looks EXACTLY like a partner with no Opportunity,
       which the tab renders for real reasons. And it is the first write this
       service has ever made to Opportunity, so the failure has never been
       exercised. */
    ok('sfw: a failed Opportunity write is escalated, not just logged',
       /recordFailure\('PartnerStack',\s*\n?\s*`\$\{d\.customer_key\} \(Partner_Source__c/.test(fn));
    /* A permission or field-security rejection is not a per-domain blip — it
       is the write being broken for every partner domain, so it pages
       immediately rather than accumulating. A per-record failure (a deleted
       Opportunity, a transient 5xx) still goes through recordFailure. */
    ok('sfw: a permission failure is called out as affecting EVERY domain',
       /const perm = res\.reason === 'permission';/.test(fn)
       && /EVERY partner domain is affected, not just this one/.test(fn));
    ok('sfw: a permission failure alerts IMMEDIATELY, not on a streak',
       /alertOps\('critical', 'PartnerStack', 'Salesforce refused the Opportunity write', \{/.test(fn));
    /* POSITION IS NOT REACHABILITY — and this one was caught by mutation
       testing an hour after that lesson was written into a ticket. Changing
       `if (perm)` to `if (false)` leaves the alertOps call exactly where it is
       and never reaches it, so every assertion above stayed green. The branch
       has to be asserted GUARDED, not merely present.
       See docs/tickets/ordering-assertions-do-not-check-reachability.md. */
    ok('sfw: and that alert is actually reachable — guarded by perm, not dead code',
       /if \(perm\) \{/.test(fn));
    ok('sfw: a per-record failure still accumulates rather than paging',
       /\} else \{[\s\S]{0,400}?recordFailure\('PartnerStack', `\$\{d\.customer_key\} \(Partner_Source__c\)`/.test(fn));
    ok('sfw: and the permission alert names the Tooling-API grant trap',
       /Creating a field through the Tooling API does NOT grant access/.test(fn)
       && /PS_SF_OPP_WRITE=false/.test(fn));
    ok('sfw: and it names the Tooling-API field-security trap',
       /Creating a field through the Tooling API does NOT grant access/.test(fn));
    /* One domain's failure must not abort the loop for the others. */
    ok('sfw: a failure does not throw out of the per-domain loop',
       !/throw /.test(fn.slice(fn.indexOf('} else {', fn.indexOf('if (res.ok) {')))));

    /* A KILL SWITCH, default ON: the write is the point of the work, but this
       is a new class of write and it needs stopping from the env without a
       deploy. */
    ok('sfw: there is a kill switch, defaulting to ON',
       /const PS_SF_OPP_WRITE_ENABLED = process\.env\.PS_SF_OPP_WRITE !== 'false';/.test(src));

    /* The read side stays read-only. The qualification poll and the funnel must
       not have acquired a write as a side effect. */
    const poll = src.slice(src.indexOf('async function runPartnerStackQualificationPoll'),
                           src.indexOf('async function sendQualificationForDomain'));
    ok('sfw: the qualification poll still writes nothing to Salesforce',
       !/updateOpportunityFields/.test(poll));
  }

  /* ── PR 25: the scale items ────────────────────────────────────────── */
  {
    /* R1: a frozen state refresh must ALERT, not only render a chip. When this
       column freezes, "waiting on an AE", "no Opportunity" and the funnel's
       Opportunity and ticked stages all keep rendering their last values as if
       they were current. Health checks fail LOUD — that is the house rule. */
    const hfn = src.slice(src.indexOf('async function checkPartnerStackHealth'),
                          src.indexOf('const HEALTH_SEVERITY'));
    ok('R1: the health check reads the state table for staleness',
       /MAX\(checked_at\) AS newest/.test(hfn) && /FROM partner_domain_sf_state/.test(hfn));
    ok('R1: a stale refresh turns the row RED, not green',
       /hc\('partnerstack', 'red',\s*\n?\s*`Salesforce state has not refreshed/.test(hfn));
    ok('R1: the tolerance is the shared constant, not a second number',
       /age >= PS_SF_STALE_MIN/.test(hfn));
    /* Order matters: a real failure is more urgent than staleness, but
       staleness must be checked BEFORE green, or a frozen refresh reports
       "verified working, just now". */
    {
      const fail = hfn.indexOf("failed in the last ${win}");
      const st   = hfn.indexOf('Salesforce state has not refreshed');
      const grn  = hfn.indexOf("hc('partnerstack', 'green'");
      ok('R1: staleness is checked after failures and BEFORE green',
         fail !== -1 && st > fail && grn > st, `fail@${fail} stale@${st} green@${grn}`);
      /* POSITION IS NOT REACHABILITY. Making the green branch unconditional
         (`if (true) return ... 'green'`) leaves every offset above unchanged
         and skips the staleness check entirely — it survived exactly that.
         So the green return must also be asserted to be GUARDED. */
      ok('R1: green is still conditional on there being activity to verify',
         /if \(conv \|\| qual \|\| lds\) return hc\('partnerstack', 'green'/.test(hfn));
    }
    /* Zero rows is not stale — it is a programme with no partner domains, and
       the counts already report that as insufficient_data. */
    ok('R1: an empty state table is not reported as stale',
       /Number\(stale\.rows\[0\]\.domains\) > 0/.test(hfn));
    /* Its own query, so a table that does not exist yet cannot take the whole
       health row down with it. */
    ok('R1: the staleness query cannot break the rest of the check',
       /FROM partner_domain_sf_state`\)\.catch\(\(\) => null\)/.test(hfn));

    const rfn = src.slice(src.indexOf('async function refreshPartnerDomainSfState'),
                          src.indexOf('/* ITS OWN JOB, not chained'));
    /* R3: the ladder's cap renders "capped at 500 domains"; this one was a
       bare LIMIT 1000 with no signal, so a domain past it rendered as "not
       checked yet" — honest by accident, and indistinguishable from a domain
       the poller had simply not reached. */
    ok('R3: the refresh cap is a named constant, not a bare LIMIT',
       /LIMIT \$\{PS_SF_REFRESH_DOMAIN_LIMIT\}/.test(rfn)
       && /const PS_SF_REFRESH_DOMAIN_LIMIT = \d+;/.test(src));
    ok('R3: hitting the cap is reported, not silent',
       /domains\.length >= PS_SF_REFRESH_DOMAIN_LIMIT/.test(rfn)
       && /recordFailure\('PartnerStack', 'sf state refresh capped'/.test(rfn));

    /* R4: one batched upsert instead of one round trip per domain. */
    ok('R4: the state write is a single batched statement',
       /FROM UNNEST\(\$1::text\[\], \$2::text\[\], \$3::text\[\], \$4::text\[\]\) AS t\(k, st, oid, err\)/.test(rfn));
    ok('R4: there is no per-domain INSERT left in the loop',
       (rfn.match(/INSERT INTO partner_domain_sf_state/g) || []).length === 1);
    ok('R4: a failed batch write is reported and stops the refresh',
       /recordFailure\('PartnerStack', 'sf state batch write'/.test(rfn)
       && /return \{ ok: false, reason: 'write_failed' \}/.test(rfn));
    /* THE ORDERING TRAP batching would have introduced. The Partner_Source__c
       pass records its idempotence stamp with an UPDATE on
       partner_domain_sf_state, so on a domain's first refresh there is no row
       yet — the UPDATE would match nothing, the stamp would be lost, and the
       PATCH would re-fire every tick forever. The batch must land FIRST. */
    {
      const batchAt = rfn.indexOf('INSERT INTO partner_domain_sf_state');
      const patchAt = rfn.indexOf('updateOpportunityFields(oppId');
      ok('R4: the state rows are written BEFORE the Partner_Source pass',
         batchAt !== -1 && patchAt !== -1 && batchAt < patchAt, `batch@${batchAt} patch@${patchAt}`);
    }
    /* The HTTP call stays per domain — it is third-party and already bounded
       by its own idempotence check. */
    ok('R4: the Opportunity PATCH is deliberately NOT batched',
       /Still per domain, not batched/.test(rfn));

    /* The verify sweep was interval-only, and the comment above the SF-state
       scheduler claimed otherwise. Every deploy restarted its timer, so a
       phantom conversion stayed undetected longer than the design intends. */
    const vfn = src.slice(src.indexOf('function startPartnerStackConversionVerify'),
                          src.indexOf('/* \u2500\u2500 STEP 10: the qualification action'));
    ok('R7: the read-back sweep runs at BOOT as well as on the interval',
       /run\('boot'\);/.test(vfn) && /setInterval\(\(\) => run\('scheduled'\), PS_VERIFY_INTERVAL_MS\)/.test(vfn));
    ok('R7: and its boot run cannot throw out of start()',
       /\.catch\(\(err\) =>/.test(vfn));
    /* All four partner background jobs now share one shape. Asserted as a set
       so a fifth cannot be added interval-only. */
    for (const fn of ['startPartnerStackCacheWarm', 'startPartnerStackSfStateRefresh',
                      'startPartnerStackConversionRetry', 'startPartnerStackConversionVerify']) {
      /* 1600, not 900: startPartnerStackCacheWarm grew an explanatory comment
         when the non-ICP block became its second consumer, which pushed
         setInterval past a 900-char window. The window is a proximity heuristic,
         not the contract -- both markers must still be inside the function. */
      const body = src.slice(src.indexOf('function ' + fn), src.indexOf('function ' + fn) + 1600);
      /* Matches the ARGUMENT, not a particular wrapper name: cacheWarm calls
         refreshPartnerStackCustomerCache('boot') directly while the other
         three go through a local `run` helper. Both are boot-then-interval;
         pinning the helper name would fail on a correct implementation. */
      ok(`R7: ${fn} does boot-then-interval`,
         /\('boot'\)/.test(body) && /setInterval\(/.test(body), fn);
    }
  }

  ok('sfC: the refresh has its own scheduler', /function startPartnerStackSfStateRefresh/.test(src));
  ok('sfC: it runs at BOOT, not only on the interval',
     /const run = \(why\) => refreshPartnerDomainSfState\(\)[\s\S]{0,200}?run\('boot'\);/.test(src));
  ok('sfC: and on its own interval', /setInterval\(\(\) => run\('scheduled'\), PS_SF_REFRESH_INTERVAL_MS\)/.test(src));
  ok('sfC: it is started from start()', /startPartnerStackSfStateRefresh\(\);/.test(src));
  ok('sfC: its failure is non-blocking', /refreshPartnerDomainSfState\(\)\s*\n\s*\.catch/.test(src));
  /* "Ran and found nothing" must be distinguishable from "never ran" — that
     ambiguity is exactly what made this bug invisible. */
  ok('sfC: a refresh that finds no domains says so',
     /SF state refresh: no partner domains yet/.test(src));
  ok('sfB: the lifecycle reads the table but never recomputes it',
     /FROM partner_domain_sf_state/.test(src));
  ok('sfB: a missing SF row does not crash the join', /\.catch\(\(\) => \(\{ rows: \[\] \}\)\)/.test(src));
  /* "Waiting on an AE" is the daily action row. */
  ok('sfB: waiting-on-an-AE is its own card', /id="p-sfwait"/.test(src));
  /* Pinned to the CHIP, not to the phrase: "not checked yet" also appears in
     the tooltip and the empty-state fallback, so a loose match passed with the
     chip itself deleted. */
  ok('sfB: a domain not yet checked says so, rather than reading as clean',
     /\+unchecked\+" not checked yet<\/span>"/.test(src));
  ok('sfB: unchecked is derived from the domain total minus checked states',
     /var unchecked=\(lc\.totalDomains\|\|0\)-Object\.values\(sf\)\.reduce/.test(src));
  ok('sfB: create_errored renders red', /k==="create_errored"\?" bad":""/.test(src));

  /* ── Batch C: mirror sync, key normalisation, display ─────────────── */

  /* The two mirror-sync bugs. These three columns existed on gw_form_leads and
     were never written, so the mirror showed NULL for every row and could not
     answer "did this convert?" — which is why an earlier question had to be
     answered from logs instead of data. */
  {
    const sync = src.slice(src.indexOf('function syncToAWS'), src.indexOf('function syncBookingToAWS'));
    for (const c of ['ps_signup_sent_at', 'ps_signup_verified_at', 'ps_qualified_sent_at']) {
      ok(`mirrorC: syncToAWS now writes ${c}`,
         new RegExp('\\b' + c + '\\b').test(sync.slice(0, sync.indexOf('VALUES'))));
      ok(`mirrorC: ${c} is COALESCEd, so a partial sync cannot wipe a stamp`,
         new RegExp(c + '\\s*=\\s*COALESCE\\(EXCLUDED\\.' + c).test(sync));
    }
    const syncNC = codeOnly(sync);
    const cols = parenBody(syncNC, syncNC.indexOf('INSERT INTO gw_form_leads')).split(',').map(x => x.trim()).filter(Boolean);
    const vr = parenBody(syncNC, syncNC.indexOf('VALUES'));
    let d = 0, cur = '', vals = [];
    for (const ch of vr) { if (ch === '(') d++; if (ch === ')') d--; if (ch === ',' && !d) { vals.push(cur.trim()); cur = ''; } else cur += ch; }
    vals.push(cur.trim());
    const dollars = vals.filter(v => v.startsWith('$')).map(v => +v.slice(1));
    eq('mirrorC: column count still equals value count', cols.length, vals.length);
    eq('mirrorC: params array still equals max $n', countArrayEntries(syncNC, '`, ['), Math.max(...dollars));
    eq('mirrorC: no duplicated or skipped $n', new Set(dollars).size, dollars.length);
  }
  /* Scoped to the /monitor/leads SELECT. Matching anywhere in the file passed
     with the column removed from the route, because the same name appears in
     the sync and the ladder. */
  {
    const seg = src.slice(src.indexOf("app.get('/monitor/leads'"), src.indexOf("app.get('/monitor/filter-options'"));
    const sel = seg.slice(seg.indexOf('const baseSelect'), seg.indexOf('FROM leads l'));
    ok('mirrorC: ps_signup_verified_at is returned by /monitor/leads',
       /l\.ps_signup_verified_at/.test(sel));
    ok('mirrorC: the reasons are returned too, so "not sent" can say why',
       /l\.ps_signup_skipped_reason/.test(sel) && /l\.ps_signup_fail_reason/.test(sel) &&
       /l\.ps_qualify_fail_reason/.test(sel));
  }

  /* Partner-key normalisation. The cookie carried BOTH forms on 4 Sept —
     base64 before 12:07, decoded after — so one partner had two strings in one
     JSONB column. */
  {
    const N = (new Function(lift(src, 'function normalisePartnerKey(') + '\n return normalisePartnerKey;'))();
    eq('keyC: base64 is decoded', N('Nzg1ZWM3OGUxZWU0Njg4'), '785ec78e1ee4688');
    eq('keyC: an already-decoded key is untouched', N('785ec78e1ee4688'), '785ec78e1ee4688');
    eq('keyC: empty stays empty', N(''), '');
    eq('keyC: non-base64 is untouched', N('not base64!'), 'not base64!');
    /* Decoding blindly would mangle a decoded value that happens to be valid
       base64, so the decode must round-trip AND look like a key. */
    eq('keyC: a short decode is rejected rather than mangling the value', N('YWJj'), 'YWJj');
    eq('keyC: a value that does not round-trip is untouched', N('abcd='), 'abcd=');
    ok('keyC: the guard is a round-trip, not a regex guess',
       /reencoded !== pk/.test(src));
    ok('keyC: normalisation is applied on WRITE', /pk  = normalisePartnerKey\(/.test(src));
  }
  ok('keyC: the one base64 row is backfilled',
     /jsonb_set\(e, '\{pk\}', to_jsonb\(convert_from\(decode\(e->>'pk','base64'\),'UTF8'\)\)\)/.test(dbjs));
  /* The guard has to be in BOTH places — the CASE that rewrites and the EXISTS
     that selects rows. Asserting once passed with the CASE stripped, which
     would rewrite entries that do not round-trip. */
  eq('keyC: the round-trip guard appears in both the CASE and the EXISTS',
     (dbjs.match(/encode\(decode\(e->>'pk','base64'\),'base64'\) = e->>'pk'/g) || []).length, 2);
  /* THE TRAP THIS ACTUALLY HIT. A flat AND chain does not guarantee the regex
     and length checks run before decode(), so a 15-char key reaches decode and
     raises "invalid base64 end sequence" — the statement failed on real data
     in a read-only dry run before it ever shipped. The decode must sit INSIDE
     a CASE whose WHEN has already established the value looks like base64.
     Same evaluation-order trap as the start_time::timestamptz cast. */
  eq('keyC: decode is gated by a nested CASE in both places, not a flat AND',
     (dbjs.match(/THEN CASE WHEN encode\(decode|THEN encode\(decode/g) || []).length, 2);
  ok('keyC: no decode sits in a bare AND chain',
     !/AND encode\(decode\(e->>'pk'/.test(dbjs));
  eq('keyC: the shape check appears in both too',
     (dbjs.match(/convert_from\(decode\(e->>'pk','base64'\),'UTF8'\) ~ '\^\[A-Za-z0-9\._-\]\{6,120\}\$'/g) || []).length, 2);
  ok('keyC: the backfill is idempotent — it stops matching once normalised',
     /AND EXISTS \([\s\S]{0,400}?jsonb_array_elements\(ps_click_history\)/.test(dbjs));

  /* Display. The funnel must NEST — mixing people and domains is what made the
     old row impossible to read left to right. */
  {
    const fn = src.slice(src.indexOf('async function partnerOverview'), src.indexOf("app.get('/monitor/partners'"));
    /* Moved into the shared fragment in PR2. */
    const q = src.slice(src.indexOf('const PS_FUNNEL_STAGE_SQL = `'), src.indexOf('const PS_FUNNEL_FROM'));
    for (const c of ['step1', 'completed', 'conversions', 'booked', 'qualified'])
      ok(`funnelC: ${c} counts DOMAINS`,
         new RegExp('COUNT\\(DISTINCT l\\.ps_customer_key\\)[\\s\\S]{0,400}?AS +' + c).test(q));
    ok('funnelC: no column counts people, so the funnel nests',
       !/COUNT\(DISTINCT LOWER\(email\)\)/.test(q));
  }
  ok('funnelC: the table says companies, not people', /companies, not people/.test(src));
  /* Clicks are not in our data at all — only PartnerStack has them. */
  ok('funnelC: the tab states that clicks are not ours',
     /Clicks that never reached the form are NOT in our data/.test(src));
  /* Structurally zero at this volume; they made the tab look broken. */
  ok('funnelC: the two zero-volume cards are gone',
     !/id="p-booked"/.test(src) && !/id="p-rate"/.test(src));

  /* The per-domain table: same source as the chips, so they cannot disagree. */
  ok('tableC: there is a per-domain lifecycle table', /id="pdtbody"/.test(src));
  ok('tableC: it renders from lc.domains, the same source as the chips',
     /var dl=lc\.domains\|\|\[\]/.test(src));
  ok('tableC: failed states render red in the table', /failed\.indexOf\(x\.state\)>=0/.test(src));
  ok('tableC: an unchecked Salesforce state says so', /"not checked yet"/.test(src));

  /* The detail panel: grouped, partner first. */
  {
    const i = src.indexOf("'function psPanel(l){");
    const j = src.indexOf('  /* Loaded on its OWN cadence');
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const F = (new Function('esc', 'et', 'wlabel', client + '; return {enrichPanel,psPanel};'))(
      (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x), (x) => String(x));
    const html = F.enrichPanel({ ps_partner_key: 'k1', ps_partner_name: 'Jane', company: 'Acme',
      utm_source: 'google', session_id: 'sid', fbc: 'fb.1' });
    const secs = (html.match(/class="psm">[^<]+/g) || []).map((x) => x.split('>')[1]);
    eq('panelC: three groups, in order', secs,
       ['Form &amp; enrichment', 'Journey &amp; attribution', 'Technical']);
    ok('panelC: the partner block comes first', html.indexOf('psb') < html.indexOf('psm'));
    const organic = F.enrichPanel({ company: 'Acme', utm_source: 'google', session_id: 'sid' });
    ok('panelC: an organic lead renders no partner block', !organic.includes('psb'));
    ok('panelC: an organic lead still gets its groups',
       (organic.match(/class="psm">/g) || []).length === 3);
    /* Sent and VERIFIED are different facts. */
    ok('panelC: verified is shown beside sent',
       F.psPanel({ ps_partner_key: 'k', ps_signup_sent_at: 'T1', ps_signup_verified_at: 'T2' }).includes('Conversion verified'));
    ok('panelC: sent-but-unverified says it is awaiting read-back',
       F.psPanel({ ps_partner_key: 'k', ps_signup_sent_at: 'T1' }).includes('awaiting read-back'));
    /* "not sent" meaning two different things is the ambiguity C removes. */
    ok('panelC: a failed conversion says why',
       F.psPanel({ ps_partner_key: 'k', ps_signup_fail_reason: 'http_400' }).includes('http_400'));
    ok('panelC: a skipped conversion says it was skipped, not failed',
       F.psPanel({ ps_partner_key: 'k', ps_signup_skipped_reason: 'test_email' }).includes('skipped: test_email'));
    ok('panelC: a failed qualification says why',
       F.psPanel({ ps_partner_key: 'k', ps_qualify_fail_reason: 'http_400' }).includes('failed: http_400'));
  }

  /* ── PR1: clicks from our own history, and the health row ─────────── */

  /* COUNT(DISTINCT xid), never SUM(jsonb_array_length). The cookie is
     cumulative per VISITOR, so someone who clicks, submits, clicks again and
     submits again carries the first click in BOTH leads' histories. Against
     real data the naive sum said 5 and the truth was 4 — one xid appeared
     under two domains. */
  {
    const fn = src.slice(src.indexOf('async function partnerOverview'), src.indexOf("app.get('/monitor/partners'"));
    ok('clicksC: counted as DISTINCT xid', /COUNT\(DISTINCT e->>'xid'\)/.test(fn));
    /* Comments stripped first: the comment explaining WHY we don't sum array
       lengths contains the very string this forbids, so the raw negative match
       failed against correct code. Any negative assertion over source needs
       this. */
    const fnCode = fn.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
    ok('clicksC: never summed as array lengths', !/SUM\(jsonb_array_length/.test(fnCode));
    ok('clicksC: only well-formed arrays are expanded',
       /jsonb_typeof\([a-z]\.ps_click_history\) = 'array'/.test(fn));
    /* "not measured" and "none" are different answers. */
    ok('clicksC: a failed clicks query yields null, not 0',
       /clicks: clicksRow\.rows\[0\]\.clicks === null \? null : Number/.test(fn));
    ok('clicksC: per-partner clicks too', /AS clicks/.test(fn.slice(fn.indexOf('AS partner_key'))));
  }

  /* System Health had nothing for the path that pays affiliates. */
  {
    const fn = src.slice(src.indexOf('async function checkPartnerStackHealth'), src.indexOf('/* ── Step 2 — /submit'));
    ok('healthC: red on ANY failure in the window', /if \(fails > 0\)[\s\S]{0,300}?'red'/.test(fn));
    ok('healthC: counts both failure kinds', /signup_failures/.test(fn) && /qualify_failures/.test(fn));
    /* Green with no traffic would be a badge meaning "we checked nothing". */
    ok('healthC: no activity is insufficient_data, not green',
       /return hc\('partnerstack', 'insufficient_data'/.test(fn));
    ok('healthC: it says the claim was released and these retry', /Claims are released/.test(fn));
    ok('healthC: a query failure is red, never green', /'Could not check'/.test(fn));
    ok('healthC: the failure counts are per DOMAIN', /COUNT\(DISTINCT ps_customer_key\) FILTER \([\s\S]{0,120}?ps_signup_failed_at/.test(fn));
  }
  ok('healthC: the check is actually CALLED, not just defined',
     /safeCheck\('partnerstack', \(\) => checkPartnerStackHealth\(pool\)\)/.test(src));
  /* WARNING not critical: critical pages by email, the three that do are a
     recorded owner decision, and the money path already alerts at the moment
     of failure. */
  ok('healthC: severity is warning, not a new email page',
     /partnerstack: 'warning'/.test(src));
  ok('healthC: it has alert metadata', /partnerstack: \{ source: 'PartnerStack'/.test(src));
  ok('healthC: it has a dashboard row', /id="s-ps"/.test(src));
  ok('healthC: the client id map includes it', /partnerstack:"s-ps"/.test(src));

  /* ── PR1.5: pagination, and completeness signals that reach the UI ── */
  {
    const fn = sfmod.slice(sfmod.indexOf('async function findOpportunityDomains'), sfmod.indexOf('module.exports'));
    /* Salesforce pages at ~1,250 regardless of LIMIT. Taking page one and
       stopping meant 5,898 Opportunities existed and we read 1,252 — every
       domain among the other 79% reported "no Opportunity", including one we
       had just qualified. */
    ok('pageD: it follows nextRecordsUrl', /data\.nextRecordsUrl/.test(fn) && /while \(url && pages < SF_MAX_PAGES\)/.test(fn));
    ok('pageD: there is a page cap', /const SF_MAX_PAGES = \d+/.test(sfmod));
    /* A LIMIT caps totalSize as well as the rows, so records.length ===
       totalSize is satisfied by a truncated result — the LIMIT defeats the
       guard meant to catch it. Measured: LIMIT 2000 reported totalSize 2000
       and "completed" at 2,000 of 5,898. */
    ok('pageD: the SOQL has NO LIMIT',
       !/LIMIT \$\{/.test(codeOnly(fn)) && !/LIMIT 2000/.test(codeOnly(fn)));
    ok('pageD: the limit parameter is gone from the signature',
       /async function findOpportunityDomains\(\{ sinceDays = 180 \} = \{\}\)/.test(sfmod));
    /* FAIL LOUDLY. Returning ok:true with a partial set reproduces the bug one
       page later. */
    ok('pageD: hitting the page cap refuses to return a partial set',
       /reason: 'pagination_incomplete', records: \[\]/.test(fn));
    ok('pageD: a short read refuses too', /reason: 'incomplete', records: \[\]/.test(fn));
    ok('pageD: a successful read reports what it saw',
       /return \{ ok: true, records, pages, totalSize, truncated: false \}/.test(fn));
  }
  {
    const fn = src.slice(src.indexOf('async function refreshPartnerDomainSfState'),
                         src.indexOf('const PS_SF_REFRESH_INTERVAL_MS'));
    ok('signalD: an incomplete read lands in the not-ok branch and writes nothing',
       /if \(!sf\.ok\)[\s\S]*?return \{ ok: false, reason: sf\.reason \}/.test(fn) &&
       /_psSfLastRead = \{ ok: false/.test(fn));
    ok('signalD: what the last read saw is recorded', /_psSfLastRead = \{ ok: true, records: sf\.records\.length/.test(fn));
    ok('signalD: a failed read is recorded too', /_psSfLastRead = \{ ok: false, reason: sf\.reason/.test(fn));
  }
  /* THE ACTUAL LESSON. `truncated` was computed from the first commit, was
     true on every call, and nothing ever read it. A completeness signal that
     does not reach the UI is the same as not computing it. */
  ok('signalD: the last-read result reaches the payload', /sfLastRead: _psSfLastRead/.test(src));
  ok('signalD: it reaches the UI', /read "\+lr\.records\+"\/"\+lr\.totalSize\+" opportunities/.test(src));
  ok('signalD: a failed read renders RED, not as an absence',
     /lr\.ok===false\)sfc\+="<span class=\\'pschip bad\\'/.test(src));
  /* The domain list is capped; a page must not read as the population. */
  ok('signalD: the ladder cap is a named constant', /const PS_LADDER_LIMIT    = 500;/.test(src));
  ok('signalD: hitting the cap is reported', /domainsCapped: domains\.rows\.length >= PS_LADDER_LIMIT/.test(src));
  ok('signalD: and rendered', /capped at "\+lc\.domainsLimit\+" domains/.test(src));

  /* Executed: the three states the strip can be in. */
  {
    const i = src.indexOf("'var partnerRows=[],pSort=");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const mk = () => ({ textContent: '', innerHTML: '', style: {}, className: '', querySelectorAll: () => [], options: [], appendChild() {}, value: '' });
    const doc = { getElementById: (id) => (els[id] = els[id] || mk()), createElement: () => ({ value: '', textContent: '' }) };
    const F = (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array',
      client + '; return {loadPartners};'));
    const run = (lifecycle) => F('', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
      (id, v) => { doc.getElementById(id).textContent = String(v); },
      async () => ({ ok: true, json: async () => ({ totals: {}, partners: [], lifecycle }) }),
      { timeout: () => null }, doc, () => {}, async () => {}, () => {}, Array).loadPartners();
    const base = { byState: { qualified: 1 }, totalDomains: 1, needsAttention: 0, failedStates: [], bySfState: { ticked: 1 } };

    await run({ ...base, sfLastRead: { ok: true, records: 5900, totalSize: 5900, pages: 6 }, domainsCapped: false, domainsLimit: 500 });
    ok('signalD UI: a complete read shows what it read',
       els['p-sfstates'].innerHTML.includes('read 5900/5900 opportunities · 6 pages'));
    await run({ ...base, bySfState: {}, sfLastRead: { ok: false, reason: 'incomplete' }, domainsCapped: false, domainsLimit: 500 });
    ok('signalD UI: a failed read is red and named',
       els['p-sfstates'].innerHTML.includes('pschip bad') && els['p-sfstates'].innerHTML.includes('incomplete'));
    await run({ ...base, totalDomains: 500, bySfState: { ticked: 500 }, sfLastRead: { ok: true, records: 10, totalSize: 10, pages: 1 }, domainsCapped: true, domainsLimit: 500 });
    ok('signalD UI: a capped domain list says so', els['p-sfstates'].innerHTML.includes('capped at 500 domains'));
  }

  /* ── PR1.6: display fixes ─────────────────────────────────────────── */

  /* Clicks were computed server-side in PR1, returned in the payload, and had
     no header, no cell and no sort entry. Computed and dropped — the fifth
     instance of that pattern tonight. */
  ok('clickCol: there is a Clicks header', /id="psar-clicks"/.test(src));
  /* The onclick lives inside a JS string, so the quotes are backslash-escaped
     in the source. */
  ok('clickCol: it is sortable', /sortPartners\(\\'clicks\\'\)/.test(src));
  ok('clickCol: it is in the sortable column list',
     /\["partner_name","clicks","step1"/.test(src));
  ok('clickCol: the row renders it', /p\.clicks===null\|\|p\.clicks===undefined\?"&#8212;":p\.clicks/.test(src));
  /* It is the one column that is not a company count. */
  ok('clickCol: the header says it is not a company count',
     /This is the one column that is not a company count/.test(src));

  /* "Waiting on an AE" must only count where a qualification could succeed.
     gushwork.ai has an unticked Opportunity but never had a conversion sent,
     so ticking the box would fire an action against a customer PartnerStack
     has never heard of. */
  {
    const fn = src.slice(src.indexOf('async function partnerLifecycle'), src.indexOf('async function partnerOverview'));
    ok('actionable: requires the conversion to have been sent',
       /sfActionable = domains\.rows\.filter\([\s\S]{0,200}?d\.signup_sent === true/.test(fn));
    ok('actionable: excludes domains already qualified',
       /d\.qualified_sent !== true/.test(fn));
    ok('actionable: the unactionable ones are counted separately, not hidden',
       /sfUnactionable = domains\.rows\.filter\([\s\S]{0,160}?d\.signup_sent !== true/.test(fn));
    /* ── C5: the third bucket must be COMPUTED, not only rendered ──────
       The renderer test proves the chip appears when the server sends a count.
       This proves the server actually derives one. Asserting only the render
       is the recurring bug pointed the other way round: a server that returned
       0 here would make the chip vanish and the sub-chips stop summing to
       bySfState.exists_unticked, with every test still green.

       These three PARTITION exists_unticked — signup_sent false; signup_sent
       true and not yet qualified; signup_sent true and already qualified — so
       no shape can fall out of all of them and off the screen, which is what
       happened to hello.com on 7 Sept 2026. */
    ok('C5: the untick-after-payment bucket is derived from the domain rows',
       /sfUntickedAfterPaid = domains\.rows\.filter\(\(d\) =>\s*\n?\s*d\.sf_state === 'exists_unticked' && d\.signup_sent === true && d\.qualified_sent === true\)\.length;/.test(fn), 'not derived');
    ok('C5: and it is sent to the client rather than computed and dropped',
       /^\s*sfUntickedAfterPaid,$/m.test(fn));
    /* The partition, asserted on the predicates themselves: every one of the
       three tests signup_sent, and the two that agree on it disagree on
       qualified_sent. */
    {
      const preds = ['d.signup_sent !== true',
                     'd.signup_sent === true && d.qualified_sent !== true',
                     'd.signup_sent === true && d.qualified_sent === true'];
      for (const pr of preds)
        ok(`C5: exists_unticked is split on "${pr}"`, fn.includes(pr), pr);
    }
    ok('C5: qualified_sent comes from the query too, like signup_sent',
       /BOOL_OR\(ps_qualified_sent_at IS NOT NULL\)\s+AS qualified_sent/.test(fn));
    ok('actionable: signup_sent comes from the query', /BOOL_OR\(ps_signup_sent_at IS NOT NULL\)\s+AS signup_sent/.test(fn));
  }
  ok('actionable: the card reads the actionable count, not the raw state count',
     /set\("p-sfwait",lc\.sfActionable\|\|0\)/.test(src));
  ok('actionable: the non-actionable ones say why', /not actionable/.test(src));

  /* hello.com's $50 has already landed; "will fire" was wrong. */
  ok('fired: a ticked domain whose qualification fired says so',
     /ticked, \$50 fired/.test(src));
  ok('fired: split on qualified_sent, not on the SF state alone',
     /x\.sf_state==="ticked"&&x\.qualified_sent/.test(src));
  /* The per-domain TABLE has its own label path, and fixing only the summary
     chip left the row a reader actually looks at still saying "will fire" —
     a mutation survived on exactly that. */
  ok('fired: the per-domain row label is per-row, not a static map',
     /function sfLabel\(x\)/.test(src) && /st==="ticked"\)return x\.qualified_sent\?"ticked, \$50 fired"/.test(src));
  /* THREE branches now, and the order matters: qualified_sent is tested first,
     because a paid domain that an AE unticked would otherwise fall through to
     "waiting on an AE" — a false errand for something that can never fire
     again. That is the bug from 7 Sept 2026 (hello.com). */
  ok('fired/C4: an untick after payment does NOT read as waiting on an AE',
     /st==="exists_unticked"\)return x\.qualified_sent\?"unticked in Salesforce \\u2014 the \$50 already fired/.test(src));
  ok('fired: the row still flags an unticked domain with no conversion sent',
     /:\(x\.signup_sent\?"waiting on an AE":"unticked \\u2014 no conversion sent, not actionable"\)/.test(src));
  ok('fired: the stale static map no longer carries a ticked label',
     !/var sfl=\{ticked:/.test(src));
  /* The server must SEND the unactionable count, not just compute it. */
  ok('actionable: sfUnactionable reaches the payload', /^\s*sfUnactionable,$/m.test(src));
  ok('fired: one still awaiting the poll reads differently',
     /ticked, will fire next poll/.test(src));

  /* A failed refresh freezes checked_at and every row looks current. */
  ok('stale: the newest check time is returned', /sfNewestCheckedAt: newestCheck/.test(src));
  ok('stale: there is a staleness threshold', /const PS_SF_STALE_MIN    = 45;/.test(src));
  ok('stale: going stale renders RED',
     /ageMin>=\(lc\.sfStaleAfterMin\|\|45\)\)sfc\+="<span class=\\'pschip bad\\'/.test(src));
  ok('stale: a fresh check shows its age too', /checked "\+ageMin\+" min ago/.test(src));

  /* Executed. */
  {
    const i = src.indexOf("'var partnerRows=[],pSort=");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const mk = () => ({ textContent: '', innerHTML: '', style: {}, className: '', querySelectorAll: () => [], options: [], appendChild() {}, value: '' });
    const doc = { getElementById: (id) => (els[id] = els[id] || mk()), createElement: () => ({ value: '', textContent: '' }) };
    const F = (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array',
      client + '; return {loadPartners};'));
    const run = (lifecycle, partners) => F('', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
      (id, v) => { doc.getElementById(id).textContent = String(v); },
      async () => ({ ok: true, json: async () => ({ totals: { clicks: 4 }, partners: partners || [], lifecycle }) }),
      { timeout: () => null }, doc, () => {}, async () => {}, () => {}, Array).loadPartners();

    const base = {
      byState: { qualified: 1 }, totalDomains: 2, needsAttention: 0, failedStates: [],
      bySfState: { ticked: 1, exists_unticked: 1 }, sfActionable: 0, sfUnactionable: 1,
      sfNewestCheckedAt: new Date().toISOString(), sfStaleAfterMin: 45,
      domains: [
        { customer_key: 'hello.com', state: 'qualified', sf_state: 'ticked', qualified_sent: true, signup_sent: true },
        { customer_key: 'gushwork.ai', state: 'skipped', sf_state: 'exists_unticked', qualified_sent: false, signup_sent: false }],
      sfLastRead: { ok: true, records: 10, totalSize: 10, pages: 1 }, domainsCapped: false, domainsLimit: 500,
    };
    await run(base, [{ partner_key: 'k1', partner_name: 'T', clicks: 4, step1: 4, completed: 3, conversions: 2, booked: 0, qualified: 1 }]);
    eq('PR16 UI: the AE card excludes the unactionable domain', els['p-sfwait'].textContent, '0');
    ok('PR16 UI: it is labelled not actionable', els['p-sfstates'].innerHTML.includes('not actionable'));
    ok('PR16 UI: a fired qualification reads "$50 fired"', els['p-sfstates'].innerHTML.includes('ticked, $50 fired'));
    ok('PR16 UI: not "will fire" for one already fired',
       !els['p-sfstates'].innerHTML.includes('ticked, will fire next poll'));
    ok('PR16 UI: a fresh check shows its age', /checked \d+ min ago/.test(els['p-sfstates'].innerHTML));
    ok('PR16 UI: the clicks cell renders', els['ptbody'].innerHTML.includes('>4</td>'));

    // stale
    await run({ ...base, sfNewestCheckedAt: new Date(Date.now() - 90 * 60000).toISOString() });
    ok('PR16 UI: a stale column renders red and says how old',
       els['p-sfstates'].innerHTML.includes('pschip bad') && /STALE — last checked 9\d min ago/.test(els['p-sfstates'].innerHTML));
    // clicks unknown
    await run(base, [{ partner_key: 'k1', partner_name: 'T', clicks: null, step1: 1 }]);
    ok('PR16 UI: an unknown click count shows a dash, not 0', els['ptbody'].innerHTML.includes('&#8212;'));
  }

  /* ── PR1.7: keep what the visitor came in saying ──────────────────── */

  /* hear_about_us is one column with three possible authors — the ad prefill,
     the visitor, and partnerHearAboutUs — and the last one wins, so the first
     two were DESTROYED rather than hidden. */
  ok('rawH: the column is declared', /ALTER TABLE leads ADD COLUMN IF NOT EXISTS hear_about_us_raw TEXT/.test(dbjs));
  ok('rawH: and on the mirror', /ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS hear_about_us_raw TEXT/.test(src));
  for (const [route, from, to] of [
    ['/partial', "app.post('/partial'", "app.post('/submit'"],
    ['/submit',  "app.post('/submit'",  "app.post('/booking-confirmed'"]]) {
    const seg = src.slice(src.indexOf(from), src.indexOf(to));
    ok(`rawH: ${route} stores the raw client value`, /hear_about_us_raw/.test(seg));
    /* EXISTING first — the opposite way round from every other COALESCE here.
       The first non-empty value must stick and nothing may overwrite it. */
    ok(`rawH: ${route} keeps the FIRST value, never overwrites`,
       /hear_about_us_raw     = COALESCE\(leads\.hear_about_us_raw,         EXCLUDED\.hear_about_us_raw\)/.test(seg));
    ok(`rawH: ${route} binds the RAW value, not the final one`,
       /elv\?\.checked_at\|\|null,hear_about_us\|\|null,ps\.ps_xid/.test(seg));
    /* A shifted parameter writes one column's value into another. */
    {
      const paren = (x, f) => { const i = x.indexOf('(', f); let d = 0;
        for (let j = i; j < x.length; j++) { if (x[j] === '(') d++; else if (x[j] === ')') { d--; if (!d) return x.slice(i + 1, j); } } };
      const split = (t) => { let d = 0, cur = '', out = [];
        for (const ch of t) { if ('([{'.includes(ch)) d++; if (')]}'.includes(ch)) d--; if (ch === ',' && d === 0) { out.push(cur.trim()); cur = ''; } else cur += ch; }
        out.push(cur.trim()); return out; };
      const cols = split(paren(seg, seg.indexOf('INSERT INTO leads')));
      const vals = split(paren(seg, seg.indexOf('VALUES')));
      eq(`rawH: ${route} column count still equals value count`, cols.length, vals.length);
      const dollars = vals.filter((v) => v.startsWith('$')).map((v) => +v.slice(1));
      ok(`rawH: ${route} placeholders are sequential`, dollars.every((v, i) => v === i + 1));
      const ai = seg.indexOf('`, [session_id');
      const params = split(paren('[' + seg.slice(ai).replace('`, [', '('), 0));
      eq(`rawH: ${route} params match the highest placeholder`, params.length, Math.max(...dollars));
      /* The one that matters: the raw column must be bound to the raw value. */
      const k = cols.indexOf('hear_about_us_raw');
      const pv = params[+vals[k].slice(1) - 1];
      ok(`rawH: ${route} hear_about_us_raw is bound to the client value`,
         pv === 'hear_about_us||null', `${vals[k]} -> ${pv}`);
      const kf = cols.indexOf('hear_about_us');
      const pvf = params[+vals[kf].slice(1) - 1];
      ok(`rawH: ${route} hear_about_us still gets the final value`,
         pvf === 'hearAboutUsFinal||null', `${vals[kf]} -> ${pvf}`);
    }
  }
  ok('rawH: it syncs to the mirror, first-value-wins there too',
     /hear_about_us_raw       = COALESCE\(gw_form_leads\.hear_about_us_raw,   EXCLUDED\.hear_about_us_raw\)/.test(src));
  ok('rawH: /monitor/leads returns it', /l\.hear_about_us_raw/.test(src));
  /* One row is recoverable; the rest are gone from every store. */
  ok('rawH: the recoverable row is backfilled from Salesforce',
     /hear_about_us_raw = 'Testing RevenueHero'/.test(dbjs) && /this\.is\.darshil@gmail\.com/.test(dbjs));
  ok('rawH: the backfill cannot fire twice', /AND hear_about_us_raw IS NULL/.test(dbjs));

  /* RENDERED, not merely present — the fifth-instance rule. */
  {
    const i = src.indexOf("'function psPanel(l){");
    const j = src.indexOf('  /* Loaded on its OWN cadence');
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const P = (new Function('esc', 'et', 'wlabel', client + '; return {psPanel};'))(
      (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x), (x) => String(x));
    ok('rawH UI: the panel renders it', P.psPanel({ ps_partner_key: 'k', hear_about_us_raw: 'Facebook (Paid)' }).includes('Facebook (Paid)'));
    ok('rawH UI: it falls back to hear_about_us when that is not the partner value',
       P.psPanel({ ps_partner_key: 'k', hear_about_us: 'linkedin' }).includes('linkedin'));
    /* Falling back to "Partner - X" would show the partner twice and claim it
       was what they said. */
    ok('rawH UI: it never falls back to the partner value',
       !P.psPanel({ ps_partner_key: 'k', hear_about_us: 'Partner - X' }).includes('Partner - X'));
    ok('rawH UI: nothing recorded says so', P.psPanel({ ps_partner_key: 'k' }).includes('nothing recorded'));
  }
  {
    const i = src.indexOf('function buildJourneyBlocks'), j = src.indexOf('function slackPartial');
    const B = (new Function('bDivider', 'bSection', 'bContext', 'bFields', 'etStamp',
      src.slice(i, j) + '; return buildJourneyBlocks;'))(() => ({ d: 1 }), (t) => ({ s: t }), (t) => ({ c: t }), () => null, (x) => String(x));
    const b = []; B(b, { ps_partner_key: 'k1', ps_partner_name: 'T', hear_about_us_raw: 'Facebook (Paid)' });
    ok('rawH Slack: the journey block shows what they came in saying',
       JSON.stringify(b).includes('Came in saying') && JSON.stringify(b).includes('Facebook (Paid)'));
    const b2 = []; B(b2, { ps_partner_key: 'k1', ps_partner_name: 'T' });
    ok('rawH Slack: no line when there is nothing to show', !JSON.stringify(b2).includes('Came in saying'));
  }
  ok('rawH Slack: /submit passes it', /hear_about_us_raw:hear_about_us/.test(src));

  /* The alert has to be actionable from Slack alone. */
  {
    const fn = src.slice(src.indexOf('async function recordPartnerStackFailure'),
                         src.indexOf('async function clearPartnerStackFailure'));
    ok('alertH: it names the partner', /'Partner': partner \?/.test(fn));
    ok('alertH: it names the lead email', /'Lead':    email/.test(fn));
    ok('alertH: it names the domain and reason', /'Domain':/.test(fn) && /'Reason':/.test(fn));
    ok('alertH: an unresolved partner says so rather than being blank', /\(unresolved\)/.test(fn));
    /* Looking the partner up must never stop the alert going out. */
    ok('alertH: the lookup cannot suppress the alert',
       /catch \{ \/\* identity is a nicety; never let it stop the alert \*\/ \}/.test(fn));
    ok('alertH: the caller passes the partner key so no query is needed',
       /partner_key: ps\.ps_partner_key/.test(src));
  }

  /* ── PR1.8: acknowledge, CSV, SDR list ───────────────────────────── */

  /* The raw value has to be able to LEAVE the dashboard. */
  /* By MEMBERSHIP, not adjacency. This asserted the literal string
     'sell_to','hear_about_us','hear_about_us_raw' and broke the moment
     'product','about_business' were inserted between them — a correct
     change failing a test that was pinning column ORDER while claiming to
     check column PRESENCE. Same shape as "product is the last bind". */
  {
    const m = /const cols = \[([\s\S]*?)\];/.exec(src);
    const csvCols = m ? [...m[1].matchAll(/'([a-z_]+)'/g)].map((x) => x[1]) : [];
    ok('exportH: the CSV carries hear_about_us_raw', csvCols.includes('hear_about_us_raw'),
       csvCols.length + ' columns found');
    ok('exportH: and the columns it sits among', ['sell_to', 'hear_about_us'].every((c) => csvCols.includes(c)));
  }
  /* An SDR opening a call wants to know how the person said they found us —
     on a partner lead hear_about_us reads "Partner - X". */
  ok('sdrH: the SDR list selects it', /l\.hear_about_us_raw,\s*\n\s*l\.ps_partner_name,/.test(src));

  /* The acknowledge flag. It SUPPRESSES ALERTS, so its failure mode is
     silence — every guard below matters more than usual. */
  ok('ackH: the columns exist', /ps_failure_ack_at TIMESTAMPTZ/.test(dbjs) && /ps_failure_ack_note TEXT/.test(dbjs));
  ok('ackH: and on the mirror', /gw_form_leads ADD COLUMN IF NOT EXISTS ps_failure_ack_at/.test(src));
  {
    const fn = src.slice(src.indexOf("app.post('/monitor/partner-ack'"), src.indexOf("app.get('/monitor/partners'"));
    /* Second mutating /monitor route ever. The first shipped as a GET that
       rewrote lead rows. */
    ok('ackH: it is a POST', /app\.post\('\/monitor\/partner-ack'/.test(src));
    ok('ackH: token-guarded', /req\.query\.token !== token/.test(fn));
    ok('ackH: customer_key is required', /if \(!customer_key\) return res\.status\(400\)/.test(fn));
    /* It must NEVER clear the failure — the history is the point. */
    ok('ackH: it never clears the failure stamp',
       !/ps_signup_failed_at = NULL/.test(fn) && !/ps_qualify_failed_at = NULL/.test(fn));
    ok('ackH: it only writes the ack columns',
       /SET ps_failure_ack_at = \$\{acknowledged \? 'NOW\(\)' : 'NULL'\}/.test(fn));
    /* Acknowledging a domain with nothing wrong would pre-silence a future
       genuine failure. */
    ok('ackH: only rows that actually carry a failure can be acknowledged',
       /AND \(ps_signup_failed_at IS NOT NULL OR ps_qualify_failed_at IS NOT NULL\)/.test(fn));
    ok('ackH: acknowledging nothing is a 404, not a silent success',
       /if \(r\.rowCount === 0\)[\s\S]{0,160}?404/.test(fn));
    ok('ackH: it is reversible', /acknowledged = req\.body\.acknowledged !== false/.test(fn));
    ok('ackH: every ack is logged', /Acknowledged' : 'Un-acknowledged'/.test(fn));
  }
  /* Both consumers must respect it, or the alert still fires. Since PR 25 the
     headline comes from the unbounded query, so the exclusion lives in SQL —
     and the page-derived version survives as the fallback for when that query
     cannot run. Both are asserted, because either one alone leaving the ack
     out would let an acknowledged failure keep demanding action. */
  ok('ackH: Needs attention excludes acknowledged failures (unbounded query)',
     /COUNT\(\*\) FILTER \(WHERE NOT acknowledged\) AS needs_attention/.test(src));
  ok('ackH: and the page fallback excludes them too',
     /PS_LADDER_FAILED\.includes\(d\.state\) && d\.acknowledged !== true/.test(src));
  /* Counted separately rather than hidden — an acknowledged failure keeps its
     state and its red chip, it just stops demanding action. Since PR 25 the
     primary count is a FILTER in the unbounded query and the page-derived
     filter is the fallback; both must keep the two apart. */
  ok('ackH: acknowledged failures are counted separately, not hidden',
     /COUNT\(\*\) FILTER \(WHERE acknowledged\)\s+AS acknowledged/.test(src)
     && /d\.acknowledged === true\)\.length/.test(src));
  eq('ackH: the health row ignores acknowledged failures on BOTH kinds',
     (src.match(/AND ps_failure_ack_at IS NULL\)/g) || []).length, 2);

  /* Rendered, per the five-for-five rule. */
  {
    const i = src.indexOf("'var partnerRows=[],pSort=");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const mk = () => ({ textContent: '', innerHTML: '', style: {}, className: '', querySelectorAll: () => [], options: [], appendChild() {}, value: '' });
    const doc = { getElementById: (id) => (els[id] = els[id] || mk()), createElement: () => ({ value: '', textContent: '' }) };
    const F = (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array','prompt','alert',
      client + '; return {loadPartners};'));
    const lc = {
      byState: { conversion_failed: 2 }, totalDomains: 2, needsAttention: 1, acknowledged: 1,
      failedStates: ['conversion_failed', 'qualification_failed'], bySfState: {}, sfActionable: 0, sfUnactionable: 0,
      sfNewestCheckedAt: new Date().toISOString(), sfStaleAfterMin: 45,
      domains: [
        { customer_key: 'test.com', state: 'conversion_failed', signup_fail_reason: 'phantom_200', acknowledged: true, ack_note: 'deleted by hand' },
        { customer_key: 'real.com', state: 'conversion_failed', signup_fail_reason: 'http_500', acknowledged: false }],
      sfLastRead: { ok: true, records: 1, totalSize: 1, pages: 1 }, domainsCapped: false, domainsLimit: 500,
    };
    await F('', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
      (id, v) => { doc.getElementById(id).textContent = String(v); },
      async () => ({ ok: true, json: async () => ({ totals: {}, partners: [], lifecycle: lc }) }),
      { timeout: () => null }, doc, () => {}, async () => {}, () => {}, Array, () => '', () => {}).loadPartners();
    const h = els['pdtbody'].innerHTML;
    eq('ackH UI: the count excludes the acknowledged one', els['p-attn'].textContent, '1');
    ok('ackH UI: the acknowledged row is marked', h.includes("ack'd"));
    /* Still visible and still red — the history is the point. */
    eq('ackH UI: both failures still render red', (h.match(/pschip bad/g) || []).length, 2);
    ok('ackH UI: an un-ack is offered on the acknowledged one', h.includes('un-ack'));
    ok('ackH UI: the button carries the key as data, not an inline handler',
       h.includes('data-ack=') && !h.includes('onclick='));
    ok('ackH UI: the note is shown on hover', h.includes('deleted by hand'));
  }

  /* ── PR2: the funnel, and the SDR Source column ───────────────────── */

  /* Defined ONCE and interpolated into both queries, so a partner column and
     the headline are computed by the same expressions. */
  ok('funnel: the stage SQL is a shared fragment', /const PS_FUNNEL_STAGE_SQL = `/.test(src));
  ok('funnel: the per-partner query uses it', /\$\{PS_FUNNEL_STAGE_SQL\},\s*\n\s*MAX\(l\.ps_click_at\)/.test(src));
  ok('funnel: the programme query uses the same one',
     /pool\.query\(`SELECT \$\{PS_FUNNEL_STAGE_SQL\}, \$\{PS_CLICKS_SQL\} \$\{PS_FUNNEL_FROM\}`\)/.test(src));
  /* Not a sum of the per-partner rows: a domain can carry leads from two
     partners and summing would count it twice. */
  ok('funnel: the programme row is its own query, not a sum',
     /NOT a sum of the per-partner rows/.test(src));

  /* CUMULATIVE, or it does not nest. A dry run showed opportunity=2 after
     booked=0, because Salesforce Opportunities exist for companies that never
     booked through our form. */
  {
    const frag = src.slice(src.indexOf('const PS_FUNNEL_STAGE_SQL = `'), src.indexOf('const PS_FUNNEL_FROM'));
    const stage = (name) => {
      const i = frag.indexOf('AS ' + name);
      return frag.slice(frag.lastIndexOf('COUNT(DISTINCT', i), i);
    };
    ok('funnel: conversions requires completed', /completed IS TRUE/.test(stage('conversions')));
    ok('funnel: verified requires the conversion to have been sent',
       /ps_signup_sent_at IS NOT NULL/.test(stage('verified')));
    ok('funnel: booked requires verified', /ps_signup_verified_at IS NOT NULL/.test(stage('booked')));
    ok('funnel: opportunity requires booked', /booking_uid IS NOT NULL/.test(stage('opportunity')));
    /* ── EVENTS, NOT THE CHECKBOX ────────────────────────────────────
       These read sf_state = 'ticked' until 7 Sept 2026, and sf_state is a
       snapshot that moves in BOTH directions while every other stage keys off
       an immutable stamp. An AE unticked hello.com and "Qualified Demo ticked"
       went down to 1 under "The $50 fired" at 2. A stage that can drop below
       the stage after it is not a funnel. */
    ok('funnel/C6: ticked reads the EVENT, never the current checkbox',
       /first_ticked_at IS NOT NULL/.test(stage('ticked'))
       && !/sf_state = 'ticked'/.test(stage('ticked')));
    ok('funnel/C6: opportunity reads the event too',
       /first_opportunity_at IS NOT NULL/.test(stage('opportunity'))
       && !/sf_state IN/.test(stage('opportunity')));
    /* ps_qualified_sent_at is the stronger and OLDER evidence — a qualification
       can only have fired because the poller saw the box ticked — and it covers
       domains ticked before these columns existed. */
    ok('funnel/C6: and ORs the older evidence for pre-existing domains',
       /OR l\.ps_qualified_sent_at IS NOT NULL/.test(stage('ticked'))
       && /OR l\.ps_qualified_sent_at IS NOT NULL/.test(stage('opportunity')));
    ok('funnel: the payment stage still requires ticked',
       /first_ticked_at IS NOT NULL[\s\S]{0,160}?ps_qualified_sent_at IS NOT NULL\)\s*$/.test(stage('qualified').trim()));
    /* Every stage counts domains; clicks are the exception and live elsewhere. */
    ok('funnel: every stage counts DISTINCT DOMAINS',
       !/COUNT\(DISTINCT LOWER\(email\)\)/.test(frag) && !/COUNT\(\*\)/.test(frag));
  }
  /* Losses beside the stage where the money leaks, never inferred from a gap. */
  {
    const L = (new Function(lift(src, 'const PS_FUNNEL_LOSSES = {') + '\n return PS_FUNNEL_LOSSES;'))();
    ok('funnel: conversion losses are attached to the conversion stage',
       (L.conversions || []).some((x) => x.key === 'lost_conversion'));
    ok('funnel: the sfopp gap is attached to the Opportunity stage',
       (L.opportunity || []).some((x) => x.key === 'lost_no_opp') &&
       (L.opportunity || []).some((x) => x.key === 'lost_sfopp'));
    ok('funnel: a failed qualification is attached to the payment stage',
       (L.qualified || []).some((x) => x.key === 'lost_qualification'));
    ok('funnel: real losses are flagged bad, a skip is not',
       (L.conversions || []).find((x) => x.key === 'lost_conversion').bad === true &&
       !(L.conversions || []).find((x) => x.key === 'lost_skipped').bad);
  }
  eq('funnel: rates are suppressed below 10', /const PS_RATE_MIN = (\d+)/.exec(src)[1], '10');

  /* RENDERED — executed, per the five-for-five rule. */
  {
    const i = src.indexOf("'var partnerRows=[],pSort=");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const mk = () => ({ textContent: '', innerHTML: '', style: {}, className: '', querySelectorAll: () => [], options: [], appendChild() {}, value: '' });
    const doc = { getElementById: (id) => (els[id] = els[id] || mk()), createElement: () => ({ value: '', textContent: '' }) };
    const F = (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array','prompt','alert',
      client + '; return {loadPartners};'));
    const stages = (new Function(lift(src, 'const PS_FUNNEL_STAGES = [') + '\n return PS_FUNNEL_STAGES;'))();
    const losses = (new Function(lift(src, 'const PS_FUNNEL_LOSSES = {') + '\n return PS_FUNNEL_LOSSES;'))();
    const prog = { clicks: 40, step1: 30, completed: 24, conversions: 20, verified: 19, booked: 12,
      opportunity: 9, ticked: 6, qualified: 5, lost_conversion: 2, lost_skipped: 2, lost_no_opp: 3, lost_sfopp: 0, lost_qualification: 1 };
    await F('', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
      (id, v) => { doc.getElementById(id).textContent = String(v); },
      async () => ({ ok: true, json: async () => ({ totals: {}, partners: [],
        funnel: { stages, losses, rateMin: 10, programme: prog },
        lifecycle: { byState: {}, totalDomains: 0, needsAttention: 0, failedStates: [], bySfState: {}, domains: [] } }) }),
      { timeout: () => null }, doc, () => {}, async () => {}, () => {}, Array, () => '', () => {}).loadPartners();
    const h = els['pfn'].innerHTML;
    eq('funnel UI: all nine stages render', h.split("class='pfs'").length - 1, 9);
    ok('funnel UI: the counts are shown', h.includes('>40<') && h.includes('>5<'));
    ok('funnel UI: a rate over a large base is shown', h.includes('80% of previous'));
    /* 1 of 2 is "50%" and means nothing. */
    ok('funnel UI: a rate over a small base is suppressed and names the base',
       h.includes('too few to rate (n=9)'));
    ok('funnel UI: clicks carry no rate and are flagged as a different unit',
       h.includes('clicks, not companies'));
    ok('funnel UI: losses render beside their stage', h.includes('2 conversion failed') && h.includes('3 booked, no Opportunity'));
    ok('funnel UI: a real loss renders red, a skip does not',
       /pfl bad'>2 conversion failed/.test(h) && /pfl'>2 skipped/.test(h));
    ok('funnel UI: a zero loss is not rendered at all', !h.includes('sfopp errored'));
    ok('funnel UI: the note says clicks are not ours',
       els['pfn-note'].innerHTML.includes('only PartnerStack has those'));
  }

  /* ── The funnel may not read 0 about something that happened ────────
     5 Sept: the funnel said "The $50 fired: 0" while the summary card said 1,
     the per-domain row said "ticked, $50 fired", and the commission was
     sitting in PartnerStack. hello.com had a hand-made Opportunity and never
     booked through our form, so the cumulative chain dropped it at BOOKED and
     it could not appear in any stage after that.

     The cumulative rule stays — it is the only column that nests. What was
     wrong is that it was the only number on screen. */
  {
    const frag = src.slice(src.indexOf('const PS_FUNNEL_STAGE_SQL = `'), src.indexOf('const PS_FUNNEL_FROM'));
    const stages = (new Function(lift(src, 'const PS_FUNNEL_STAGES = [') + '\n return PS_FUNNEL_STAGES;'))();
    const expr = (name) => {
      const i = frag.indexOf('AS ' + name);
      return i < 0 ? null : frag.slice(frag.lastIndexOf('COUNT(DISTINCT', i), i);
    };

    /* Every stage that a domain can skip into carries an unchained twin. */
    for (const k of ['conversions', 'verified', 'booked', 'opportunity', 'ticked', 'qualified']) {
      const st = stages.find((x) => x.key === k);
      ok(`abs: stage ${k} declares its unchained twin`, st && st.abs === 'abs_' + k);
      ok(`abs: ${'abs_' + k} exists in the shared SQL`, !!expr('abs_' + k));
    }
    /* step 1 cannot be skipped into, and completed already filters the full
       base on one condition, so a twin there would be a duplicate column. */
    ok('abs: step1 and completed have no twin, because they cannot differ',
       !stages.find((x) => x.key === 'step1').abs &&
       !stages.find((x) => x.key === 'completed').abs &&
       !expr('abs_step1') && !expr('abs_completed'));
    ok('abs: clicks has no twin — a different unit, not a stage',
       !stages.find((x) => x.key === 'clicks').abs);

    /* UNCHAINED is the whole point. A twin carrying a prior stage's condition
       would reproduce the bug with extra steps: it would still be capable of
       reading 0 for a domain that skipped. One condition each, no more. */
    const chain = {
      abs_conversions: 'ps_signup_sent_at IS NOT NULL',
      abs_verified:    'ps_signup_verified_at IS NOT NULL',
      abs_booked:      'booking_uid IS NOT NULL',
      /* The two that were snapshots. Now events, ORed with the older evidence
         — see funnel/C6 above. */
      abs_opportunity: 'first_opportunity_at IS NOT NULL\n           OR l.ps_qualified_sent_at IS NOT NULL',
      abs_ticked:      'first_ticked_at IS NOT NULL\n           OR l.ps_qualified_sent_at IS NOT NULL',
      abs_qualified:   'ps_qualified_sent_at IS NOT NULL',
    };
    for (const [col, cond] of Object.entries(chain)) {
      const e = expr(col);
      ok(`abs: ${col} filters on exactly its own condition`, e.includes(cond), e);
      ok(`abs: ${col} carries no earlier stage's condition`,
         !/\bAND\b/.test(e.slice(e.indexOf('WHERE'))), e);
    }
    /* Every twin still counts DOMAINS. A people count here would put two
       units in one funnel column, which is what the per-domain rework
       removed. */
    for (const col of Object.keys(chain))
      ok(`abs: ${col} counts DISTINCT DOMAINS`,
         /^COUNT\(DISTINCT l\.ps_customer_key\)/.test(expr(col).trim()));

    /* The structural half of the fix: the twins are the SAME expressions the
       summary cards use, so funnel-vs-card cannot drift apart again. Asserted
       against the totals query, not against a copy. */
    const fn = src.slice(src.indexOf('async function partnerOverview'), src.indexOf('/* ── ACKNOWLEDGING A FAILURE'));
    const qTotals = fn.slice(fn.indexOf('pool.query('), fn.indexOf('pool.query(', fn.indexOf('pool.query(') + 5));
    for (const [col, card] of [['abs_conversions', 'conversions'], ['abs_qualified', 'qualified'], ['abs_booked', 'booked']]) {
      const cond = chain[col];
      ok(`abs: ${col} matches the "${card}" card's condition, so the two cannot disagree`,
         new RegExp('FILTER \\([\\s\\S]{0,120}?' + cond.replace(/[()]/g, '\\$&') +
                    '[\\s\\S]{0,80}?AS ' + card + '\\b').test(qTotals), card);
    }
  }

  /* RENDERED, with hello.com's real shape. */
  {
    const i = src.indexOf("'var partnerRows=[],pSort=");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const mk = () => ({ textContent: '', innerHTML: '', style: {}, className: '', querySelectorAll: () => [], options: [], appendChild() {}, value: '' });
    const doc = { getElementById: (id) => (els[id] = els[id] || mk()), createElement: () => ({ value: '', textContent: '' }) };
    const stages = (new Function(lift(src, 'const PS_FUNNEL_STAGES = [') + '\n return PS_FUNNEL_STAGES;'))();
    const losses = (new Function(lift(src, 'const PS_FUNNEL_LOSSES = {') + '\n return PS_FUNNEL_LOSSES;'))();
    /* The night of the bug: four domains, one qualified, none booked through
       the form. Small on purpose — docs/partnerstack.md, "small datasets catch
       bugs that large ones hide". */
    const prog = { clicks: 5, step1: 4, completed: 4,
      conversions: 3, verified: 3, booked: 0, opportunity: 0, ticked: 0, qualified: 0,
      abs_conversions: 3, abs_verified: 3, abs_booked: 0,
      abs_opportunity: 1, abs_ticked: 1, abs_qualified: 1,
      lost_conversion: 0, lost_skipped: 1, lost_no_opp: 0, lost_sfopp: 0, lost_qualification: 0 };
    await (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array','prompt','alert',
      client + '; return {loadPartners};'))('', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
      (id, v) => { doc.getElementById(id).textContent = String(v); },
      async () => ({ ok: true, json: async () => ({ totals: {}, partners: [],
        funnel: { stages, losses, rateMin: 10, programme: prog },
        lifecycle: { byState: {}, totalDomains: 0, needsAttention: 0, failedStates: [], bySfState: {}, domains: [] } }) }),
      { timeout: () => null }, doc, () => {}, async () => {}, () => {}, Array, () => '', () => {}).loadPartners();
    const h = els['pfn'].innerHTML;

    /* Parse the rendered cards rather than grepping the string: a substring
       match cannot tell which stage a number belongs to, and this whole bug
       was one stage showing another stage's answer. */
    const cards = h.split("class='pfs'").slice(1).map((c) => ({
      label: (/class='pfsl'>([^<]*)</.exec(c) || [])[1],
      value: (/class='pfsv'>([^<]*)</.exec(c) || [])[1],
      off:   (/class='pfso'[^>]*>([\s\S]*?)<\/div>/.exec(c) || [])[1] || '',
      rate:  (/class='pfsr'>([^<]*)</.exec(c) || [])[1] || '',
    }));
    const card = (label) => cards.find((c) => c.label === label);

    /* THE REGRESSION. This read 0 on the night and the money was real. */
    eq('absUI: the $50 stage shows the payment that actually fired', card('The $50 fired').value, '1');
    eq('absUI: Qualified Demo ticked shows the tick that happened', card('Qualified Demo ticked').value, '1');
    eq('absUI: Opportunity created shows the Opportunity that exists', card('Opportunity created').value, '1');
    /* The general form of it, so a future stage cannot regress the same way. */
    for (const st of stages) {
      if (!st.abs) continue;
      const a = Number(prog[st.abs]);
      if (!a) continue;
      ok(`absUI: ${st.label} does not read 0 when it happened ${a} time(s)`,
         card(st.label).value !== '0' && Number(card(st.label).value) === a, card(st.label).value);
    }

    /* Both numbers, and the smaller one named. Never left as the gap between
       two cards, which is arithmetic the reader should not have to do. */
    ok('absUI: the skipped stage is named on the row, not inferred',
       /0 on the funnel path/.test(card('The $50 fired').off) &&
       /1 skipped an earlier stage/.test(card('The $50 fired').off));
    /* The funnel path is still there, and still nests. */
    eq('absUI: Booked still reports the form path honestly', card('Booked').value, '0');

    /* No noise where nothing skipped: those rows must look exactly as before. */
    eq('absUI: a stage where the two agree gets no off-path line', card('Conversion sent').off, '');
    eq('absUI: and neither does one with no twin at all', card('Reached step 1').off, '');

    /* Rates come off the CUMULATIVE counts. A rate between two absolutes is
       not a step-to-step rate and is not bounded by 100%. */
    ok('absUI: rates still run down the funnel path', h.includes('too few to rate (n=3)'));
    ok('absUI: no rate exceeds 100%', !/\b[1-9]\d\d(\.\d)?% of previous/.test(h));

    /* Whatever the funnel says about the $50, the card must say the same. */
    eq('absUI: the funnel and the summary card now agree by construction',
       card('The $50 fired').value, String(prog.abs_qualified));

    ok('absUI: the note explains the two numbers',
       /skipped an earlier stage/.test(els['pfn-note'].innerHTML) &&
       /does nest/.test(els['pfn-note'].innerHTML));
  }

  /* ── PR 22: RENDERED, with hello.com's shape AFTER the untick ──────
     7 Sept 2026. google.ai converted from a real Salesforce Lead and its $50
     fired; then an AE unticked Qualified_Demo__c on the hello.com Opportunity,
     whose $50 had fired on 4 Sept. Two things broke, both of them the
     recurring bug rather than anything about Salesforce:

       - the per-domain row said "waiting on an AE" for a domain where
         ps_qualified_sent_at is stamped, once-per-domain is a UNIQUE PARTIAL
         index, and nothing can ever fire again. A false errand.
       - "Qualified Demo ticked" read 1 while "The $50 fired" read 2, because
         ticked read the current checkbox and everything else reads a
         historical stamp.

     Rendered, not asserted against the payload: five for five in
     docs/partnerstack.md says computed server-side is not the same as on
     screen. */
  {
    const i = src.indexOf("'var partnerRows=[],pSort=");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const mk = () => ({ textContent: '', innerHTML: '', style: {}, className: '', querySelectorAll: () => [], options: [], appendChild() {}, value: '' });
    const doc = { getElementById: (id) => (els[id] = els[id] || mk()), createElement: () => ({ value: '', textContent: '' }) };
    const stages = (new Function(lift(src, 'const PS_FUNNEL_STAGES = [') + '\n return PS_FUNNEL_STAGES;'))();
    const losses = (new Function(lift(src, 'const PS_FUNNEL_LOSSES = {') + '\n return PS_FUNNEL_LOSSES;'))();

    const paid = (key, sf, ticks) => ({ customer_key: key, state: 'qualified',
      partner_name: 'Test Account', signup_sent: true, signup_verified: true,
      qualified_sent: true, sf_state: sf, first_ticked_at: ticks,
      first_opportunity_at: '2026-09-04T05:00:00Z', last_seen: '2026-09-04T00:00:00Z' });
    /* hello.com: paid, then UNTICKED. google.ai: paid, still ticked.
       gushwork.ai: an Opportunity exists, unticked, and no conversion was ever
       sent because it is a test address — the state that produced the FIRST
       false errand, kept here so the fix for the second cannot undo it. */
    const domains = [
      paid('hello.com', 'exists_unticked', '2026-09-04T05:41:00Z'),
      paid('google.ai', 'ticked', '2026-09-07T05:41:00Z'),
      { customer_key: 'gushwork.ai', state: 'skipped', skipped_reason: 'test_email',
        signup_sent: false, qualified_sent: false, sf_state: 'exists_unticked',
        first_ticked_at: null, first_opportunity_at: '2026-09-04T05:00:00Z',
        last_seen: '2026-09-04T00:00:00Z' },
    ];
    const bySfState = { exists_unticked: 2, ticked: 1 };
    /* What partnerLifecycle now computes: three buckets PARTITIONING
       exists_unticked. The third one is new — before it, hello.com fell out of
       both of the others and rendered no chip at all. */
    const lifecycle = { byState: { qualified: 2, skipped: 1 }, totalDomains: 3,
      needsAttention: 0, acknowledged: 0, failedStates: ['conversion_failed', 'qualification_failed'],
      bySfState, sfActionable: 0, sfUnactionable: 1, sfUntickedAfterPaid: 1,
      domains, noCustomerKeyLeads: 0,
      sfNewestCheckedAt: new Date().toISOString(), sfStaleAfterMin: 45,
      sfLastRead: { ok: true, records: 5898, totalSize: 5898, pages: 6 },
      domainsCapped: false, domainsLimit: 500 };
    /* Both $50s fired, so ticked and qualified are both 2 — the point being
       that ticked may no longer read 1 just because a checkbox moved. */
    const prog = { clicks: 9, step1: 6, completed: 6,
      conversions: 4, verified: 4, booked: 1, opportunity: 1, ticked: 1, qualified: 1,
      abs_conversions: 4, abs_verified: 4, abs_booked: 1,
      abs_opportunity: 2, abs_ticked: 2, abs_qualified: 2,
      lost_conversion: 0, lost_skipped: 1, lost_no_opp: 0, lost_sfopp: 0, lost_qualification: 0 };

    await (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array','prompt','alert',
      client + '; return {loadPartners};'))('', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
      (id, v) => { doc.getElementById(id).textContent = String(v); },
      async () => ({ ok: true, json: async () => ({ totals: {}, partners: [],
        funnel: { stages, losses, rateMin: 10, programme: prog }, lifecycle }) }),
      { timeout: () => null }, doc, () => {}, async () => {}, () => {}, Array, () => '', () => {}).loadPartners();

    const rows = els['pdtbody'].innerHTML;
    const chips = els['p-sfstates'].innerHTML;
    const rowFor = (key) => (rows.split('<tr>').find((r) => r.includes('>' + key + '<')) || '');

    /* ── C4: the false errand ── */
    ok('untickUI: the paid-then-unticked row does NOT say waiting on an AE',
       !/waiting on an AE/.test(rowFor('hello.com')), rowFor('hello.com').slice(0, 300));
    ok('untickUI: it says the $50 already fired and nothing more can',
       /the \$50 already fired, nothing more can/.test(rowFor('hello.com')));
    /* The still-ticked one is unaffected. */
    ok('untickUI: the still-ticked paid domain still reads ticked, $50 fired',
       /ticked, \$50 fired/.test(rowFor('google.ai')));
    /* And the FIRST false errand stays fixed — an unticked Opportunity whose
       conversion was never sent is informational, not an action. */
    ok('untickUI: the no-conversion-sent row is still not an action item',
       /not actionable/.test(rowFor('gushwork.ai')));
    /* sfActionable is 0 here, so the words must appear nowhere on the tab. */
    ok('untickUI: nothing on the tab claims an AE is being waited on',
       !/waiting on an AE/.test(rows) && !/waiting on an AE/.test(chips));

    /* ── R2: an incomplete headline must SAY SO on screen ──────────
       needsAttentionComplete === false means the count is the capped page's,
       not the population's. A floor rendered as a total is this integration's
       recurring bug, and per the two-assertions rule the server deriving the
       flag is only half of it — the client has to render it. Dropping the
       render survived every server-side assertion. */
    {
      const els2 = {};
      const doc2 = { getElementById: (id) => (els2[id] = els2[id] || mk()), createElement: () => ({ value: '', textContent: '' }) };
      await (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array','prompt','alert',
        client + '; return {loadPartners};'))('', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
        (id, v) => { doc2.getElementById(id).textContent = String(v); },
        async () => ({ ok: true, json: async () => ({ totals: {}, partners: [],
          funnel: { stages, losses, rateMin: 10, programme: prog },
          lifecycle: Object.assign({}, lifecycle, { needsAttention: 3, needsAttentionComplete: false }) }) }),
        { timeout: () => null }, doc2, () => {}, async () => {}, () => {}, Array, () => '', () => {}).loadPartners();
      ok('untickUI/R2: an incomplete Needs-attention count says it is a floor',
         /AT LEAST this many/.test(els2['p-attn-sub'].textContent), els2['p-attn-sub'].textContent);
      ok('untickUI/R2: and it does NOT read as an actionable total',
         !/^act on these today$/.test(els2['p-attn-sub'].textContent));
      /* The healthy case must be unchanged — this adds no noise until it
         means something, same rule as the funnel's off-path line. */
      ok('untickUI/R2: a complete count still reads normally',
         els['p-attn-sub'].textContent === 'nothing failing'
         || !/AT LEAST/.test(els['p-attn-sub'].textContent), els['p-attn-sub'].textContent);
    }

    /* ── C5: the domain that rendered no chip at all ── */
    ok('untickUI: the third bucket renders its own chip',
       /1 unticked after the \$50 fired/.test(chips), chips.slice(0, 400));
    /* THE STRUCTURAL GUARD, not just the one case: whatever the sub-chips say
       must add up to the state count they are splitting. Before the fix these
       were 0 + 1 against a state count of 2 and the missing one was simply
       absent from the screen. */
    {
      const seg = chips.split('unticked after the $50 fired')[0] + 'unticked after the $50 fired';
      const nums = (seg.match(/>(\d+) (?:waiting on an AE|unticked)/g) || [])
        .map((m) => Number(/\d+/.exec(m)[0]));
      const summed = nums.reduce((a, b) => a + b, 0);
      eq('untickUI: the exists_unticked chips SUM to the state count they split',
         summed, bySfState.exists_unticked);
    }

    /* ── C6: the stage that went backwards ── */
    const cards = chips && els['pfn'].innerHTML.split("class='pfs'").slice(1).map((c) => ({
      label: (/class='pfsl'>([^<]*)</.exec(c) || [])[1],
      value: (/class='pfsv'>([^<]*)</.exec(c) || [])[1],
    }));
    const val = (label) => Number(cards.find((c) => c.label === label).value);
    eq('untickUI: Qualified Demo ticked counts the tick that happened, not the checkbox', val('Qualified Demo ticked'), 2);
    eq('untickUI: and the $50 stage agrees with it', val('The $50 fired'), 2);
    /* The tail of the funnel is now guaranteed to nest, and this is the
       assertion that would have caught the bug: first_ticked_at is set for
       every domain first_opportunity_at is, and ps_qualified_sent_at implies
       both, so opportunity >= ticked >= qualified holds by construction.

       ONLY this tail. The earlier absolute stages genuinely do NOT nest — a
       domain can be booked without ever converting — which is exactly why the
       cumulative column is kept alongside them. */
    ok('untickUI: the payment tail nests — Opportunity >= ticked >= $50',
       val('Opportunity created') >= val('Qualified Demo ticked')
       && val('Qualified Demo ticked') >= val('The $50 fired'),
       [val('Opportunity created'), val('Qualified Demo ticked'), val('The $50 fired')].join(' >= '));
  }

  /* Rates must keep running down the FUNNEL PATH, not between two absolutes.
     A separate render with a base over PS_RATE_MIN, because hello.com's four
     domains suppress every rate and a suppressed rate cannot tell the two
     apart. This is the case that survived the first mutation pass. */
  {
    const i = src.indexOf("'var partnerRows=[],pSort=");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const mk = () => ({ textContent: '', innerHTML: '', style: {}, className: '', querySelectorAll: () => [], options: [], appendChild() {}, value: '' });
    const doc = { getElementById: (id) => (els[id] = els[id] || mk()), createElement: () => ({ value: '', textContent: '' }) };
    const stages = (new Function(lift(src, 'const PS_FUNNEL_STAGES = [') + '\n return PS_FUNNEL_STAGES;'))();
    const prog = { clicks: 300, step1: 200, completed: 150,
      conversions: 120, verified: 100, booked: 40, opportunity: 20, ticked: 12, qualified: 10,
      abs_conversions: 120, abs_verified: 100, abs_booked: 45,
      abs_opportunity: 120, abs_ticked: 60, abs_qualified: 50 };
    await (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array','prompt','alert',
      client + '; return {loadPartners};'))('', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
      (id, v) => { doc.getElementById(id).textContent = String(v); },
      async () => ({ ok: true, json: async () => ({ totals: {}, partners: [],
        funnel: { stages, losses: {}, rateMin: 10, programme: prog },
        lifecycle: { byState: {}, totalDomains: 0, needsAttention: 0, failedStates: [], bySfState: {}, domains: [] } }) }),
      { timeout: () => null }, doc, () => {}, async () => {}, () => {}, Array, () => '', () => {}).loadPartners();
    const h = els['pfn'].innerHTML;
    const cards = h.split("class='pfs'").slice(1).map((c) => ({
      label: (/class='pfsl'>([^<]*)</.exec(c) || [])[1],
      value: (/class='pfsv'>([^<]*)</.exec(c) || [])[1],
      rate:  (/class='pfsr'>([^<]*)</.exec(c) || [])[1] || '',
    }));
    const card = (label) => cards.find((c) => c.label === label);

    /* booked cumulative is 40, so Opportunity's rate is 20/40. Reading the
       absolute booked count (45) instead gives 44.4% — close enough to look
       plausible on screen, which is exactly why it is asserted exactly. */
    eq('rateUI: Opportunity rates against the cumulative Booked, not the absolute',
       card('Opportunity created').rate, '50% of previous');
    eq('rateUI: Qualified Demo ticked rates against cumulative Opportunity',
       card('Qualified Demo ticked').rate, '60% of previous');
    eq('rateUI: the $50 stage rates against cumulative Ticked',
       card('The $50 fired').rate, '83.3% of previous');
    /* And the headline is still the absolute, at this scale too. */
    eq('rateUI: the headline is the absolute even where the rate is not',
       card('The $50 fired').value, '50');
    /* A rate between two absolutes is not a step-to-step rate and is not
       bounded by 100. If one ever appears, the base is wrong. */
    ok('rateUI: no rate exceeds 100%', !/\b(?:[1-9]\d\d|100\.\d)(?:\.\d)?% of previous/.test(h), h);
  }

  /* The per-partner row had the identical bug one level down: "Qualified 0"
     for the partner whose $50 had fired. */
  {
    const i = src.indexOf("'var partnerRows=[],pSort=");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const mk = () => ({ textContent: '', innerHTML: '', style: {}, className: '', querySelectorAll: () => [], options: [], appendChild() {}, value: '' });
    const doc = { getElementById: (id) => (els[id] = els[id] || mk()), createElement: () => ({ value: '', textContent: '' }) };
    const stages = (new Function(lift(src, 'const PS_FUNNEL_STAGES = [') + '\n return PS_FUNNEL_STAGES;'))();
    const partners = [
      { partner_key: 'k1', partner_name: 'Test Account', clicks: 5, step1: 4, completed: 4,
        conversions: 3, verified: 3, booked: 0, opportunity: 0, ticked: 0, qualified: 0,
        abs_conversions: 3, abs_verified: 3, abs_booked: 0, abs_opportunity: 1, abs_ticked: 1, abs_qualified: 1 },
      { partner_key: 'k2', partner_name: 'Other', clicks: 2, step1: 2, completed: 1,
        conversions: 1, verified: 1, booked: 1, opportunity: 1, ticked: 0, qualified: 0,
        abs_conversions: 1, abs_verified: 1, abs_booked: 1, abs_opportunity: 1, abs_ticked: 0, abs_qualified: 0 },
    ];
    const api = (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array','prompt','alert',
      client + '; return {loadPartners,sortPartners};'))('', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
      (id, v) => { doc.getElementById(id).textContent = String(v); },
      async () => ({ ok: true, json: async () => ({ totals: {}, partners,
        funnel: { stages, losses: {}, rateMin: 10, programme: {} },
        lifecycle: { byState: {}, totalDomains: 0, needsAttention: 0, failedStates: [], bySfState: {}, domains: [] } }) }),
      { timeout: () => null }, doc, () => {}, async () => {}, () => {}, Array, () => '', () => {});
    await api.loadPartners();
    const row = (n) => els['ptbody'].innerHTML.split('<tr').filter((r) => r.includes('Test Account'))[0] || '';
    const cells = (r) => r.split('</td>').map((c) => c.replace(/[\s\S]*>/, '').trim());
    /* Columns: name, email, key, clicks, step1, completed, conversions,
       verified, booked, opportunity, ticked, qualified, last click. */
    const c = cells(row());
    eq('absRow: the Qualified cell shows the $50 that fired', c[11], '1 &#8224;');
    eq('absRow: Ticked likewise', c[10], '1 &#8224;');
    eq('absRow: Booked still reports the form path', c[8], '0');
    /* An untouched cell keeps its bare number — the marker has to mean
       something or it becomes decoration. */
    eq('absRow: a cell where the two agree carries no marker', c[6], '3');
    ok('absRow: the marked cell explains itself on hover',
       /title='0 on the funnel path, 1 skipped an earlier stage/.test(row()));

    /* A column that sorts on a number it is not showing is the same class of
       bug as a number computed and never rendered. */
    /* Both partners have a CUMULATIVE qualified of 0, so a sort reading that
       column cannot reorder them and a stable sort leaves them alone. Only a
       sort reading the displayed value can put Other first ascending. */
    api.sortPartners('qualified');   // desc
    api.sortPartners('qualified');   // asc
    const first = els['ptbody'].innerHTML.split('<tr')[1] || '';
    ok('absRow: sorting a stage column orders by the value on screen',
       first.includes('Other') && !first.includes('Test Account'), first.slice(0, 160));
  }

  /* The per-partner table renders all NINE stages. PR2 computed verified,
     opportunity and ticked and rendered none of them — the same
     computed-but-not-rendered trap, caught in review. */
  for (const c of ['clicks', 'step1', 'completed', 'conversions', 'verified', 'booked', 'opportunity', 'ticked', 'qualified']) {
    ok(`perPartner: ${c} has a sortable header`, new RegExp('id="psar-' + c + '"').test(src));
    ok(`perPartner: ${c} is in the sortable column list`,
       new RegExp('"' + c + '"').test(/\["partner_name","clicks"[^\]]*\]/.exec(src)[0]));
  }
  {
    const i = src.indexOf("'var partnerRows=[],pSort=");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const mk = () => ({ textContent: '', innerHTML: '', style: {}, className: '', querySelectorAll: () => [], options: [], appendChild() {}, value: '' });
    const doc = { getElementById: (id) => (els[id] = els[id] || mk()), createElement: () => ({ value: '', textContent: '' }) };
    await (new Function('API','TP','esc','et','set','fetch','AbortSignal','document','showTab','loadFilterOptions','loadLeads','Array','prompt','alert',
      client + '; return {loadPartners};'))('', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
      (id, v) => { doc.getElementById(id).textContent = String(v); },
      async () => ({ ok: true, json: async () => ({ totals: {},
        funnel: { stages: [], losses: {}, rateMin: 10, programme: {} },
        partners: [{ partner_key: 'k1', partner_name: 'T', clicks: 40, step1: 30, completed: 24,
          conversions: 20, verified: 19, booked: 12, opportunity: 9, ticked: 6, qualified: 5 }],
        lifecycle: { byState: {}, totalDomains: 0, needsAttention: 0, failedStates: [], bySfState: {}, domains: [] } }) }),
      { timeout: () => null }, doc, () => {}, async () => {}, () => {}, Array, () => '', () => {}).loadPartners();
    const cells = els['ptbody'].innerHTML.split('</td>').slice(0, -1).map((c) => c.replace(/.*>/, ''));
    const stages = cells.slice(4).map(Number).filter((n) => !isNaN(n));
    eq('perPartner UI: all nine stage cells render', cells.slice(3).filter((c) => /^\d+$/.test(c)).length, 9);
    /* Left to right must read as a funnel or the row is unreadable. */
    ok('perPartner UI: the row nests left to right',
       stages.every((v, i) => i === 0 || v <= stages[i - 1]), stages.join(' >= '));
  }

  /* The SDR Source column — rendered, not merely selected. */
  ok('sdrCol: the query selects it', /l\.hear_about_us_raw,\s*\n\s*l\.ps_partner_name,/.test(src));
  ok('sdrCol: there is a Source header', /<th title="How they said they found us/.test(src));
  ok('sdrCol: the cell is built', /var src2=\[l\.ps_partner_name/.test(src));
  ok('sdrCol: and rendered into the row', /\+src2\+"<\/td>/.test(src));
  ok('sdrCol: the colspans were widened with it', /colspan="11" class="nd">Loading/.test(src));

  /* Step 8. */
  {
    const fn = src.slice(src.indexOf('function buildJourneyBlocks'), src.indexOf('function slackPartial'));
    ok('v8: a partner alone is enough to render the section', /!hasPartner\) return;/.test(fn));
    ok('v8: shows the resolved name', /d\.ps_partner_name/.test(fn));
    ok('v8: shows the partner email', /d\.ps_partner_email/.test(fn));
    ok('v8: shows the click date', /d\.ps_click_at \? ` — clicked \$\{etStamp\(d\.ps_click_at\)\}`/.test(fn));
    ok('v8: falls back to the raw key when unresolved', /name not resolved yet/.test(fn));
  }
  ok('v8: /submit passes the partner fields to Slack',
     /slackSubmit\(\{[^}]*ps_partner_key:ps\.ps_partner_key/.test(src));

  /* Step 9. A dimension on the existing view, not a new tab. */
  {
    const seg = src.slice(src.indexOf("app.get('/monitor/leads'"), src.indexOf("app.get('/monitor/filter-options'"));
    ok('v9: the leads API accepts a partner filter', /req\.query\.partner/.test(seg));
    ok('v9: __any selects every partner-sourced lead', /partner === '__any'[\s\S]{0,80}?ps_partner_key IS NOT NULL/.test(seg));
    ok('v9: __none selects the rest', /partner === '__none'[\s\S]{0,80}?ps_partner_key IS NULL/.test(seg));
    /* Whoever filters may have the key, the name or the email to hand. */
    ok('v9: a specific value matches key, name OR email',
       /ps_partner_key,''\)\) LIKE[\s\S]{0,120}?ps_partner_name,''\)\) LIKE[\s\S]{0,120}?ps_partner_email,''\)\) LIKE/.test(seg));
    for (const c of ['ps_partner_key','ps_partner_name','ps_partner_email','ps_click_at','ps_click_history','ps_signup_sent_at','ps_qualified_sent_at'])
      ok(`v9: the leads API returns ${c}`, new RegExp('l\\.' + c).test(seg));
  }
  ok('v9: filter-options lists partners', /partners:\s*partnerRows\.rows\.map/.test(src));
  ok('v9: the partner list groups by key and keeps the resolved name',
     /GROUP BY ps_partner_key/.test(src) && /MAX\(ps_partner_name\)/.test(src));
  ok('v9: there is a partner filter control, not a new tab',
     /id="fpartner"/.test(src) && !/id="tab-partner"/.test(src));
  ok('v9: clearF resets the partner filter', /getElementById\("fpartner"\)\.value="all"/.test(src));
  ok('v9: the CSV export carries the partner filter',
     (src.match(/url\+="&partner="\+encodeURIComponent\(partner\)/g) || []).length === 2);
  /* The panel is real client JS living inside a JS string; lift it and run it. */
  {
    const i = src.indexOf("'function psPanel(l){");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const F = (new Function('esc', 'et', 'wlabel', client + '; return { psPanel };'))(
      (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x), (x) => String(x));
    eq('v9: an organic lead renders no partner panel', F.psPanel({}), '');
    const html = F.psPanel({
      ps_partner_key: 'k1', ps_partner_name: 'Jane Smith', ps_partner_email: 'j@x.com',
      ps_click_at: '2026-09-01', ps_customer_key: 'acme.com', ps_signup_sent_at: '2026-09-02',
      ps_click_history: [{ xid: 'x1', pk: 'kA', at: '2026-08-01' }, { xid: 'x2', pk: 'k1', at: '2026-09-01' }],
    });
    ok('v9: panel shows the partner name', html.includes('Jane Smith'));
    ok('v9: panel shows the partner email', html.includes('j@x.com'));
    ok('v9: panel shows the click date', html.includes('2026-09-01'));
    ok('v9: panel shows every click in the history', html.includes('kA') && html.includes('k1'));
    /* Attribution is last-click, so exactly one row is the winner. */
    eq('v9: exactly one click is marked WON', (html.match(/WON/g) || []).length, 1);
    const lastIdx = html.lastIndexOf('kA'), wonIdx = html.indexOf('WON');
    ok('v9: the WON badge is on the LAST click, not the first', wonIdx > lastIdx);
    ok('v9: a partner with only an email shows the email, not the key',
       F.psPanel({ ps_partner_key: 'k9', ps_partner_email: 'p@x.com' }).includes('p@x.com'));
    ok('v9: a fully unresolved partner shows the key and says so',
       F.psPanel({ ps_partner_key: 'k9' }).includes('partner not resolved'));
    ok('v9: a stringified history is still parsed',
       F.psPanel({ ps_partner_key: 'k1', ps_click_history: JSON.stringify([{ xid: 'a', pk: 'b', at: 'c' }]) }).includes('WON'));
    ok('v9: a corrupt history cannot break the panel',
       F.psPanel({ ps_partner_key: 'k1', ps_click_history: '{not json' }).includes('Partner'));
  }

  /* Step 10. */
  ok('v10: the poller queries the Qualified_Demo__c checkbox',
     /Qualified_Demo__c = true/.test(sfmod));
  ok('v10: it reads the account website and a contact email as fallback',
     /Account\.Website/.test(sfmod) && /OpportunityContactRoles/.test(sfmod));
  /* ── PR 21: the read side is paginated and answers {ok, records} ──
     It was `LIMIT 200`, unpaginated, unchecked, and it returned [] on every
     failure. Nobody unticks Qualified_Demo__c so the ticked set only grows:
     past 200 org-wide, Salesforce returns an arbitrary 200 in an undefined
     order and a newly ticked partner Opportunity can sit outside them —
     affiliate never paid, nothing anywhere saying so. Third instance of this
     exact shape in the repo. */
  {
    const fn = sfmod.slice(sfmod.indexOf('async function findQualifiedDemoOpportunities'),
                           sfmod.indexOf('async function findOpportunityDomains'));
    /* The outer query carries NO LIMIT. The one inside the
       OpportunityContactRoles subquery is correct and must survive — it picks
       the single primary contact and does not bound the result set — so this
       asserts on the FROM Opportunity clause rather than the absence of the
       word. */
    ok('v21: the ticked query carries no LIMIT on the outer result set',
       /FROM Opportunity WHERE Qualified_Demo__c = true`/.test(fn));
    ok('v21: the primary-contact subquery LIMIT 1 is still there',
       /FROM OpportunityContactRoles ORDER BY IsPrimary DESC LIMIT 1/.test(fn));
    ok('v21: it paginates on nextRecordsUrl', /data\.nextRecordsUrl/.test(fn));
    ok('v21: pagination is bounded by SF_MAX_PAGES', /pages < SF_MAX_PAGES/.test(fn));
    /* A short read is indistinguishable from "those AEs have not ticked yet",
       and the caller would skip exactly the domains it could not see. */
    ok('v21: hitting the page cap refuses to return a partial set',
       /reason: 'pagination_incomplete'/.test(fn));
    ok('v21: a short read against totalSize refuses too',
       /records\.length < totalSize/.test(fn) && /reason: 'incomplete'/.test(fn));
    /* "No AE has ticked anything" and "Salesforce did not answer" are opposite
       conclusions and used to be the same value. */
    ok('v21: it answers {ok, records}, never a bare array',
       /return \{ ok: true, records/.test(fn) && !/return \[\];/.test(fn));
    ok('v21: every failure path carries ok:false and a reason',
       (fn.match(/return \{ ok: false/g) || []).length >= 4);
    /* No date bound here, unlike findOpportunityDomains: an Opportunity created
       before the window and ticked today would otherwise be invisible forever,
       which is a silently unpaid affiliate. */
    ok('v21: the ticked query is NOT bounded to a date window',
       !/LAST_N_DAYS/.test(fn));
  }
  /* Against the whole module, not the function slice: this reasoning lives in
     the doc comment ABOVE the function, which is outside it. Asserted because
     folding the two Salesforce reads into one looks like an obvious win and is
     the mistake that would quietly undo the interval change below. */
  ok('v21: and it records why it stays separate from the existence scan',
     /KEPT SEPARATE from findOpportunityDomains/.test(sfmod)
     && /expensive question's 15-minute schedule/.test(sfmod));
  ok('v21: SF_MAX_PAGES is declared above its first use',
     sfmod.indexOf('const SF_MAX_PAGES') < sfmod.indexOf('async function findQualifiedDemoOpportunities'));
  {
    /* Scoped to the POLL function only. Slicing through to
       startPartnerStackQualificationPoll swallowed sendQualificationForDomain
       too, so a guard deleted from the poll still matched the identical text
       in the claim below it — a mutation survived on exactly that. */
    const fn = src.slice(src.indexOf('async function runPartnerStackQualificationPoll'),
                         src.indexOf('async function sendQualificationForDomain'));
    /* Two minutes, down from fifteen on 7 Sept 2026. One small page of ticked
       Opportunities per tick, ~720 Salesforce calls a day. */
    ok('v21: the qualification poll runs every 2 minutes',
       /const PS_QUALIFY_INTERVAL_MS = 2 \* 60 \* 1000;/.test(src));
    /* The two intervals must NOT move together. The state refresh scans every
       Opportunity in 180 days across six growing pages; dragging it to two
       minutes is the mistake this comment exists to prevent. */
    ok('v21: the SF state refresh was NOT shortened with it',
       /const PS_SF_REFRESH_INTERVAL_MS = 15 \* 60 \* 1000;/.test(src));
    /* Different sweep, and load-bearing: PartnerStack's own indexing lags a
       conversion by 2 to 6 minutes, so checking sooner releases good claims
       and re-fires conversions. */
    ok('v21: the read-back grace is untouched at 15 minutes',
       /const PS_VERIFY_GRACE_MIN   = 15;/.test(src));
    ok('v10: the action type is qualified_demo', /const PS_QUALIFY_ACTION_TYPE = 'qualified_demo';/.test(src));
    ok('v10: overlapping runs are prevented',
       /if \(_psQualifyRunning\) \{[\s\S]{0,200}?return;/.test(fn) && /_psQualifyRunning = true;/.test(fn));
    /* The domain is the only identifier both systems share. */
    ok('v10: the domain is derived with the SAME helper as everything else',
       /partnerStackCustomerKey\(o\.website\) \|\| partnerStackCustomerKey\(o\.contactEmail\)/.test(fn));
    ok('v10: several Opportunities on one account collapse to one action', /byKey/.test(fn));
    /* An action for a customer_key PartnerStack has never seen is a no-op. */
    /* Still "only domains we already converted", now asked per DOMAIN because
       that is the unit PartnerStack pays on and the unit the ladder uses. */
    ok('v10: only domains we already converted can be qualified',
       /BOOL_OR\(ps_signup_sent_at\s+IS NOT NULL\) AS sent/.test(fn)
       && /BOOL_OR\(ps_qualified_sent_at\s+IS NOT NULL\) AS qualified/.test(fn));
    /* ── C8: VERIFIED, not merely sent ──────────────────────────────────
       ps_signup_sent_at only means PartnerStack answered 200, and
       /conversion/xid answers 200 with an empty body. Qualifying on that stamp
       and then having the read-back sweep 404 releases ps_signup_sent_at while
       leaving ps_qualified_sent_at stamped — the domain re-converts on the next
       lead and can NEVER be qualified again, because once-per-domain is a
       UNIQUE PARTIAL index and nothing releases a qualification that
       succeeded. $50 gone, no error, no red chip. */
    ok('v21/C8: a conversion must be VERIFIED before the $50 can fire',
       /BOOL_OR\(ps_signup_verified_at IS NOT NULL\) AS verified/.test(fn));
    ok('v21/C8: and the filter actually requires all three',
       /r\.sent && r\.verified && !r\.qualified/.test(fn));
    /* The filter must not become a silent drop. */
    ok('v21/C8: a ticked demo held back by the filter is NAMED',
       /Ticked demo CANNOT be qualified/.test(fn));
    ok('v21/C8: and escalated through recordFailure, not just logged',
       /recordFailure\('PartnerStack', r\.ps_customer_key \+ ' \(ticked, conversion unverified\)'/.test(fn));
    /* Only once genuinely stuck. A conversion sent four minutes ago is not
       verified because the grace period is working; at a two-minute tick a
       bare warn would print 720 times a day and bury everything. */
    ok('v21/C8: the held-back warning waits until it is stuck, not merely waiting',
       /const stuckAfterMs = PS_VERIFY_GRACE_MIN \* 2 \* 60 \* 1000;/.test(fn)
       && /Date\.now\(\) - sentAt < stuckAfterMs\) continue;/.test(fn));
    /* A domain with no conversion at all is almost always a non-partner
       Opportunity that happens to carry a ticked box. */
    ok('v21/C8: a domain that never converted is not reported as held back',
       /if \(r\.qualified \|\| !r\.sent \|\| r\.verified\) continue;/.test(fn));
    /* A failed read must not read as "nobody ticked anything". */
    ok('v21: an unreadable Salesforce stops the poll rather than concluding zero',
       /if \(!sf\.ok\)/.test(fn) && /NOT concluding that nothing is ticked/.test(fn));
    ok('v21: and that read failure is recorded',
       /recordFailure\('PartnerStack', 'qualified-demo read'/.test(fn));
    ok('v10: an unmatchable Opportunity is logged, not silently dropped',
       /no usable domain/.test(fn));

    const send = src.slice(src.indexOf('async function sendQualificationForDomain'),
                           src.indexOf('function startPartnerStackQualificationPoll'));
    const claimAt = send.indexOf('ps_qualified_sent_at = NOW()');
    const sendAt  = send.indexOf('await sendAction(');
    ok('v10: the domain is claimed BEFORE the action is sent', claimAt !== -1 && sendAt !== -1 && claimAt < sendAt);
    /* Position alone is not the guarantee — the claim also has to be
       CONDITIONAL on nothing else having qualified this domain, or two
       overlapping runs both claim and both pay. */
    ok('v10: the claim is conditional on no prior qualification',
       /NOT EXISTS \([\s\S]{0,200}?o\.ps_customer_key = \$1 AND o\.ps_qualified_sent_at IS NOT NULL/.test(send));
    ok('v10: the claim only fires on an unqualified row',
       /AND ps_qualified_sent_at IS NULL/.test(send));
    ok('v10: a concurrent claim is read as already-qualified', /err\.code === '23505'/.test(send));
    ok('v10: a failed send releases the claim', /ps_qualified_sent_at = NULL/.test(send));
    ok('v10: a stuck claim is escalated', /stuck qualify claim/.test(send));
  }
  ok('v10: once-per-domain is enforced by a UNIQUE PARTIAL index',
     /CREATE UNIQUE INDEX IF NOT EXISTS leads_ps_qualified_once_idx[\s\S]{0,220}?WHERE ps_customer_key IS NOT NULL AND ps_qualified_sent_at IS NOT NULL/.test(dbjs));
  ok('v10: the poller is started at boot', /startPartnerStackQualificationPoll\(\);/.test(src));
  ok('v10: the action posts to /v2/actions with Basic auth',
     /`\$\{V2_BASE\}\/actions`/.test(psmod) && /sendAction[\s\S]{0,600}?v2AuthHeader\(\)/.test(psmod));
  /* FOUR required fields, and there is no customer_key on this endpoint —
     that name belongs to /conversion/xid. Sending it returned 400
     "'target_type' is a required property", which reads like one missing
     field and was actually two missing plus one unrecognised. */
  {
    const fn = psmod.slice(psmod.indexOf('async function sendAction'), psmod.indexOf('module.exports'));
    ok('v10: the action payload carries all four required fields',
       /const payload = \{\s*type,\s*value: value === undefined \? 1 : value,\s*target_type: 'customer',\s*target_key: customer_key,\s*\}/.test(fn));
    /* The payload's KEYS, not a substring search — `target_key: customer_key`
       legitimately mentions the identifier as a VALUE, so a naive negative
       match on "customer_key" fails against correct code. */
    {
      const lit = fn.slice(fn.indexOf('const payload = {'), fn.indexOf('};', fn.indexOf('const payload = {')));
      const keys = [...lit.matchAll(/^\s*([a-z_]+)\s*[:,]/gm)].map(m => m[1]).filter(k => k !== 'payload');
      eq('v10: the payload sends exactly the four documented fields',
         keys.sort(), ['target_key', 'target_type', 'type', 'value']);
      ok('v10: customer_key is not one of them', !keys.includes('customer_key'));
    }
    /* "customer" not "partnership": the action attaches to the customer the
       conversion created and PartnerStack resolves the partner from its
       attribution. Targeting the partnership is a different event that would
       still return 200. */
    ok("v10: target_type is 'customer', not 'partnership'",
       /target_type: 'customer'/.test(fn) && !/target_type: 'partnership'/.test(fn));
    ok('v10: target_key is the customer key', /target_key: customer_key/.test(fn));
    ok('v10: a missing customer key is still rejected before the call',
       /if \(!customer_key\) return \{ ok: false, reason: 'no_customer_key' \};/.test(fn));
  }

  /* ============================================================
     4d. PARTNER REVENUE GAPS — the two money-leak checks
     ============================================================ */
  {
    const fn = src.slice(src.indexOf('async function partnerRevenueGaps'),
                         src.indexOf("app.get('/monitor/partner-gaps'"));

    /* Check A no longer has a query of its own — batch B replaced it with a
       read of the ladder, because two independent queries told two different
       stories about the same four domains. Its properties are now inherited
       from the ladder and asserted under "gapB:" below. What remains here is
       that the bespoke query is genuinely GONE, not merely bypassed. */
    ok('gaps A: the bespoke missed-conversion query is gone',
       !/HAVING COUNT\(ps_signup_sent_at\) = 0/.test(fn));
    ok('gaps A: check A is derived from the ladder',
       /const lifecycle = await partnerLifecycle\(\)/.test(fn));
    /* IS NOT TRUE, never = false — a null flag has to land somewhere. */
    ok('gaps A: uses IS NOT TRUE, not = false', !/disqualified = false/.test(fn));
    ok('gaps A: the grace window still applies to never-attempted domains',
       /PS_GAP_CONVERSION_GRACE_H \* 3600000/.test(fn));

    /* start_time is TEXT. A WHERE clause does not guarantee the regex runs
       before the cast, so one malformed row would take the query down. */
    /* EVERY cast, not merely some: one unguarded start_time::timestamptz is
       enough to take the whole query down on a single malformed row, and a
       WHERE clause does not guarantee the regex runs first. */
    {
      const casts  = (fn.match(/start_time::timestamptz/g) || []).length;
      const guarded = (fn.match(/CASE WHEN start_time ~ '\^\[0-9\]\{4\}[^]*?THEN start_time::timestamptz/g) || []).length;
      ok('gaps B: every start_time cast is guarded by CASE', casts > 0 && casts === guarded,
         `${casts} cast(s), ${guarded} guarded`);
    }
    ok('gaps B: keys off the meeting time plus a 3-day grace',
       /const PS_GAP_QUALIFY_GRACE_D    = 3;/.test(src) &&
       /INTERVAL '\$\{PS_GAP_QUALIFY_GRACE_D\} days'/.test(fn));
    ok('gaps B: only leads that actually booked', /booking_uid IS NOT NULL/.test(fn));

    /* The whole point of B: "no Opportunity exists", not "not yet qualified". */
    ok('gaps B: filters candidates against real Opportunity domains',
       /missingOpportunity = qualifyCandidates\.rows\.filter\(r => !have\.has\(r\.customer_key\)\)/.test(fn));
    ok('gaps B: domains normalised with the same helper as everything else',
       /partnerStackCustomerKey\(r\.website\) \|\| partnerStackCustomerKey\(r\.contactEmail\)/.test(fn));

    /* "We could not check" is not "we checked and it is fine". */
    ok('gaps B: an unreachable Salesforce is reported, not counted as zero',
       /opportunityCheck = \{ ok: false, reason: sf\.reason \|\| 'unavailable' \}/.test(fn));
    ok('gaps B: the unreachable branch does not populate missingOpportunity',
       !/sf\.ok[\s\S]{0,400}?else[\s\S]{0,200}?missingOpportunity =/.test(fn));
    ok('gaps: leads merely awaiting an AE are counted separately, not flagged',
       /awaitingQualification: qualifyCandidates\.rows\.length/.test(fn));
    ok('gaps: cached, because check B crosses the network',
       /_psGapCache/.test(fn) && /PS_GAP_CACHE_TTL_MS/.test(src));
  }
  ok('gaps: exposed on its own route', /app\.get\('\/monitor\/partner-gaps'/.test(src));
  ok('gaps: the route is token-guarded like the rest of /monitor',
     /partner-gaps'[\s\S]{0,200}?req\.query\.token !== token/.test(src));
  /* Deliberately NOT a health check: a lead waiting on an AE is normal
     latency, and System Health going amber for it would train people to
     ignore it. */
  /* Deliberately not a health check: a green badge there means "verified
     working, just now", and a lead waiting on an AE is normal latency. Wiring
     this in would leave System Health permanently amber and train people to
     ignore it. The property that matters is that runHealthChecks never calls
     it — not merely that no id happens to be spelled "partner". */
  {
    const hcFn = src.slice(src.indexOf('async function runHealthChecks'),
                           src.indexOf("app.get('/monitor/health'"));
    ok('gaps: runHealthChecks does not call the gap check',
       hcFn.length > 200 && !/partnerRevenueGaps/.test(hcFn), `slice ${hcFn.length}`);
  }
  /* It must SELECT Qualified_Demo__c (batch B needs ticked vs unticked) but
     must never FILTER on it — filtering would hide every domain whose
     Opportunity exists and has not been ticked, which is the row worth acting
     on daily. */
  {
    const fn = sfmod.slice(sfmod.indexOf('async function findOpportunityDomains'), sfmod.indexOf('module.exports'));
    ok('gaps: the Salesforce query asks for ANY Opportunity, not just qualified ones',
       /CreatedDate = LAST_N_DAYS/.test(fn) && !/WHERE[^`]*Qualified_Demo__c/.test(fn));
    ok('gaps: it selects the ticked flag so state can be derived',
       /SELECT Id, Account\.Website, Qualified_Demo__c/.test(fn));
  }
  ok('gaps: the SF helper distinguishes "none" from "could not ask"',
     /return \{ ok: false, reason: `http_\$\{res\.status\}`, records: \[\] \}/.test(sfmod) &&
     /return \{ ok: true, records/.test(sfmod));

  /* The card and list are real client JS; lift and run them. */
  {
    const i = src.indexOf("'async function loadPartnerGaps()");
    const j = src.indexOf("'function debounce()");
    const client = eval(src.slice(i, j).replace(/\+\s*$/, ''));
    const els = {};
    const doc = { getElementById: (id) => (els[id] = els[id] || { textContent: '', innerHTML: '', style: {} }) };
    const mk = (payload) => (new Function('API', 'TP', 'esc', 'et', 'fetch', 'AbortSignal', 'document',
      client + '; return loadPartnerGaps;'))(
      '', '', (x) => String(x == null ? '' : x), (x) => String(x == null ? '' : x),
      async () => ({ ok: true, json: async () => payload }),
      { timeout: () => null }, doc);

    // Clean state: no gaps, Salesforce answered.
    await mk({ missedConversions: [], missingOpportunity: [], awaitingQualification: 0,
               opportunityCheck: { ok: true }, graceDays: 3 })();
    eq('gaps UI: a clean state shows 0', els['m-psgap'].textContent, '0');
    eq('gaps UI: the list is hidden when there is nothing to act on', els['psgapbox'].style.display, 'none');

    // Real gaps.
    await mk({ missedConversions: [{ customer_key: 'a.com', partner_name: 'Jane', email: 'x@a.com', first_seen: 'T1' }],
               missingOpportunity: [{ customer_key: 'b.com', partner_key: 'k2', email: 'y@b.com', met_at: 'T2' }],
               awaitingQualification: 5, opportunityCheck: { ok: true }, graceDays: 3 })();
    eq('gaps UI: counts both checks', els['m-psgap'].textContent, '2');
    ok('gaps UI: the list becomes visible', els['psgapbox'].style.display === 'block');
    ok('gaps UI: names the no-conversion domain', els['psgapbox'].innerHTML.includes('a.com'));
    ok('gaps UI: names the no-Opportunity domain', els['psgapbox'].innerHTML.includes('b.com'));
    ok('gaps UI: says how many are merely awaiting an AE', els['psgapbox'].innerHTML.includes('5 partner demo'));

    /* The inversion that matters: Salesforce down must never render as a
       clean bill of health. */
    await mk({ missedConversions: [{ customer_key: 'a.com', email: 'x@a.com', first_seen: 'T1' }],
               missingOpportunity: [], awaitingQualification: 2,
               opportunityCheck: { ok: false, reason: 'http_503' }, graceDays: 3 })();
    ok('gaps UI: an unreachable Salesforce does NOT read as zero',
       els['m-psgap'].textContent === '1+?', els['m-psgap'].textContent);
    ok('gaps UI: the subtitle says the check was unavailable',
       els['m-psgap-sub'].textContent.includes('unavailable'));
    ok('gaps UI: the panel warns it is not a clean result',
       els['psgapbox'].innerHTML.includes('NOT a clean result'));
    ok('gaps UI: the panel is shown even with only check A failing open',
       els['psgapbox'].style.display === 'block');
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
    /* See the note at the other res.json anchor: the argument now carries
       non_icp_blocked. */
    const resAt = seg.indexOf('res.json({ ok: true');
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
    const syncNC = codeOnly(sync);
    const cols = parenBody(syncNC, syncNC.indexOf('INSERT INTO gw_form_leads')).split(',').map(x => x.trim()).filter(Boolean);
    const vr = parenBody(syncNC, syncNC.indexOf('VALUES'));
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
       countArrayEntries(syncNC, '`, ['), Math.max(...dollars));
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
    /* Pinned to the CURRENT version on purpose: this is what catches a form
       file shipped without its version bumped, which is how the Webflow re-pin
       silently ships half a fix. Bump both when you bump the files. */
    ok(`form(${name}): version banner says v5.10.0`, /Form initialised v5\.10\.0/.test(f));
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
