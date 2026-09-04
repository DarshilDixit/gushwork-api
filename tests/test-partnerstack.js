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
    const resAt  = seg.indexOf('res.json({ ok: true })');
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
    for (const [label, q] of [['totals', qTotals], ['per-partner', qRows]]) {
      ok(`partners (${label}): leads are deduped by email (people)`,
         /COUNT\(DISTINCT LOWER\(email\)\)\s+AS leads/.test(q), q.slice(0, 80));
      ok(`partners (${label}): leads are NOT a raw row count`,
         !/COUNT\(\*\)\s+AS leads/.test(q));
      ok(`partners (${label}): bookings are deduped by email (people)`,
         /COUNT\(DISTINCT LOWER\(email\)\) FILTER \(\s*WHERE booking_uid IS NOT NULL\)/.test(q));
      ok(`partners (${label}): conversions are per DOMAIN`,
         /COUNT\(DISTINCT ps_customer_key\) FILTER \([\s\S]{0,80}?ps_signup_sent_at IS NOT NULL\)/.test(q));
      ok(`partners (${label}): qualified is per DOMAIN`,
         /COUNT\(DISTINCT ps_customer_key\) FILTER \([\s\S]{0,80}?ps_qualified_sent_at IS NOT NULL\)/.test(q));
      /* A domain count and a people count are different units. Presenting one
         as the other is the exact drift the Definitions section exists to
         stop. */
      ok(`partners (${label}): conversions are NOT deduped by email`,
         !/COUNT\(DISTINCT LOWER\(email\)\) FILTER \([\s\S]{0,80}?ps_signup_sent_at IS NOT NULL\)/.test(q));
    }
    ok('partners: only partner-sourced leads are counted',
       (fn.match(/WHERE ps_partner_key IS NOT NULL/g) || []).length === 2);
    /* A rate over zero leads is undefined, not 0%. */
    ok('partners: the booking rate is null when there are no leads',
       /leads \? Math\.round\(\(booked \/ leads\) \* 1000\) \/ 10 : null/.test(fn));
    ok('partners: the per-partner list resolves a name via MAX over the key',
       /GROUP BY ps_partner_key/.test(fn) && /MAX\(ps_partner_name\)/.test(fn));
  }
  ok('partners: the route is token-guarded',
     /partners'[\s\S]{0,200}?req\.query\.token !== token/.test(src));
  ok('partners: it is a TAB, and Partner gaps stays on Overview',
     /id="t-partners"/.test(src) && /id="tp-partners"/.test(src) && /id="psgapbox"/.test(src));
  ok('partners: the tab is registered in showTab',
     /\["overview","leads","sdr","dupes","health","lm","partners"\]/.test(src));
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
          { partner_key: 'k1', partner_name: 'Jane', partner_email: 'j@x.com', leads: 8, conversions: 4, booked: 5, qualified: 2, last_click: '2026-09-04' },
          { partner_key: 'k2', partner_name: null, partner_email: null, leads: 4, conversions: 1, booked: 2, qualified: 0, last_click: null }] }) }),
      { timeout: () => null }, doc,
      (t) => { tabShown = t; }, async () => {}, () => { leadsLoaded++; }, Array);
    await F.loadPartners();
    eq('partners UI: lead card', els['p-leads'].textContent, '12');
    eq('partners UI: 24h subtitle', els['p-leads24'].textContent, '3 in the last 24h');
    eq('partners UI: conversions card', els['p-conv'].textContent, '5');
    eq('partners UI: qualified card', els['p-qual'].textContent, '2');
    eq('partners UI: bookings card', els['p-booked'].textContent, '7');
    eq('partners UI: booking rate', els['p-rate'].textContent, '58.3%');
    ok('partners UI: one row per partner', (els['ptbody'].innerHTML.match(/<tr /g) || []).length === 2);
    ok('partners UI: an unresolved partner falls back to the key',
       els['ptbody'].innerHTML.includes("data-pk='k2'"));
    /* The key would need quotes nested three deep in an inline handler, which
       is how this markup broke the first time. */
    ok('partners UI: rows use data-pk + delegation, not an inline onclick',
       !els['ptbody'].innerHTML.includes('onclick'));
    F.sortPartners('leads');
    ok('partners UI: clicking the active column flips direction',
       els['psar-leads'].textContent === '▲');
    F.sortPartners('partner_name');
    ok('partners UI: switching column resets the arrow', els['psar-leads'].textContent === '');
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
    eq('partners UI: no leads shows a dash, not 0% or NaN', els['p-rate'].textContent, '—');
    ok('partners UI: an empty list says so', els['ptbody'].innerHTML.includes('No partner-sourced leads yet'));
  }

  /* ── Regression: the All Leads partner filter and click panel still work ── */
  ok('regression: the All Leads partner filter control survives',
     /id="fpartner"/.test(src) && /url\+="&partner="\+encodeURIComponent\(partner\)/.test(src));
  ok('regression: the leads API still accepts the partner filter',
     /const partner      = req\.query\.partner/.test(src));
  ok('regression: psPanel is still wired into enrichPanel',
     /'var pp=psPanel\(l\);' \+/.test(src) && /'return pp\+"<div class=/.test(src));

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
    ok('readback: it is a sweep on an interval, not a post-send timer',
       /setInterval\(runPartnerStackConversionVerify/.test(src) &&
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
        liftLine(src, 'const PS_SF_STATES =') + '\n' +
        liftTemplate(src, 'const PS_LADDER_SQL =') + '\n' +
        lift(src, 'async function partnerLifecycle(') +
        '\n return partnerLifecycle;'))(fakePool, { warn() {}, log() {} });
      const out = await L();
      eq('ladderA: no-key leads reach the response with their real count', out.noCustomerKeyLeads, 7);
      eq('ladderA: needsAttention counts exactly the two red states', out.needsAttention, 2);
      eq('ladderA: totalDomains counts domains, not leads', out.totalDomains, 4);
      eq('ladderA: the state counts sum to the domain total',
         Object.values(out.byState).reduce((a, b) => a + b, 0), out.totalDomains);
      ok('ladderA: no-key leads are NOT folded into the domain total',
         out.totalDomains === 4 && out.noCustomerKeyLeads === 7);
    }
    ok('ladderA: needsAttention sums only the red states',
       /PS_LADDER_FAILED\.reduce/.test(fn));
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
    ok('backfill: bounded to rows predating the cutoff',
       (bf.match(/created_at < \$\{CUTOFF\}/g) || []).length === 2);
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
  ok('recovery: a successful conversion clears the failure and skip reasons',
     /Conversion sent[\s\S]{0,160}?clearPartnerStackFailure\('signup', session_id\)/.test(src));
  ok('recovery: a successful qualification clears its failure',
     /Qualification sent[\s\S]{0,160}?clearPartnerStackFailure\('qualify', claimedSession\)/.test(src));
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
    ok('sfB: refreshed for ALL partner domains, not just qualify candidates',
       /FROM leads\s*\n\s*WHERE ps_xid IS NOT NULL AND ps_customer_key IS NOT NULL/.test(fn) &&
       !/ps_signup_sent_at IS NOT NULL/.test(fn));
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
       /if \(!sf\.ok\)[\s\S]{0,300}?return \{ ok: false, reason: sf\.reason \}/.test(fn));
    ok('sfB: a missing sfopp log degrades to no_opportunity, it does not throw',
       /Could not read sf_lead_conversion_log/.test(fn));
  }
  /* The refresh must not be able to stop actions firing. */
  ok('sfB: the refresh runs AFTER the qualification work, outside its try',
     /_psQualifyRunning = false;\s*\n\s*\}[\s\S]{0,300}?await refreshPartnerDomainSfState\(\)/.test(src));
  ok('sfB: its failure is non-blocking',
     /refreshPartnerDomainSfState\(\)\s*\n\s*\.catch/.test(src));
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
  ok('v10: a Salesforce failure returns [] rather than throwing',
     /return \[\];/.test(sfmod));
  {
    /* Scoped to the POLL function only. Slicing through to
       startPartnerStackQualificationPoll swallowed sendQualificationForDomain
       too, so a guard deleted from the poll still matched the identical text
       in the claim below it — a mutation survived on exactly that. */
    const fn = src.slice(src.indexOf('async function runPartnerStackQualificationPoll'),
                         src.indexOf('async function sendQualificationForDomain'));
    ok('v10: runs every 15 minutes', /const PS_QUALIFY_INTERVAL_MS = 15 \* 60 \* 1000;/.test(src));
    ok('v10: the action type is qualified_demo', /const PS_QUALIFY_ACTION_TYPE = 'qualified_demo';/.test(src));
    ok('v10: overlapping runs are prevented',
       /if \(_psQualifyRunning\) \{[\s\S]{0,200}?return;/.test(fn) && /_psQualifyRunning = true;/.test(fn));
    /* The domain is the only identifier both systems share. */
    ok('v10: the domain is derived with the SAME helper as everything else',
       /partnerStackCustomerKey\(o\.website\) \|\| partnerStackCustomerKey\(o\.contactEmail\)/.test(fn));
    ok('v10: several Opportunities on one account collapse to one action', /byKey/.test(fn));
    /* An action for a customer_key PartnerStack has never seen is a no-op. */
    ok('v10: only domains we already converted can be qualified',
       /AND ps_signup_sent_at IS NOT NULL/.test(fn) && /AND ps_qualified_sent_at IS NULL/.test(fn));
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
