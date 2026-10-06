/* ============================================================
   Google Ads conversion upload — EXECUTED.

   Three parts, and the third is the one that matters most:

     A. The module's own rules, driven directly: Google's email and phone
        normalisation, the Eastern-time timestamp across a DST change, the
        click-ID choice, the settings and their safe defaults, what each
        Google reply means, and the exact request body.

     B. The exclusion rules, with the REAL functions lifted out of
        index.js -- isInternalSubmission, isWebsiteVerified, freeEmailMatch
        and the value adapter -- so "agency leads are skipped" is tested
        against the same code the sweep runs, not a copy of it.

     C. The real app BOOTED with the upload switched on, pg and fetch
        stubbed: the sweep starts from start(), reads leads, skips an
        agency lead, and sends one validate-only request whose body, token
        and headers are read back here. Then the dry-run tool is run
        against the same stub and must write nothing and call nobody.

   Every real SQL statement was ALSO executed on 6 Oct 2026 against temp
   tables on the AWS mirror inside a rolled-back transaction (see the PR's
   review card). This suite cannot do that -- it is dependency-free by
   rule -- so it tests behaviour, and the card records the SQL run.

   Dependency-free: pg is stubbed, fetch is stubbed, nothing leaves the
   box and no DATABASE_URL is needed.

   Run:  node tests/test-gads-upload.js
   ============================================================ */

require('./crash-reporter')('test-gads-upload');

const Module = require('module');
const path   = require('path');
const fs     = require('fs');
const crypto = require('crypto');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
const eq = (name, a, b) => ok(name, a === b, `got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const ROOT = path.join(__dirname, '..');
const G = require(path.join(ROOT, 'google-ads-conversions.js'));
const META = require(path.join(ROOT, 'meta-capi.js'));
const src = fs.readFileSync(path.join(ROOT, 'index.js'), 'utf8');
const modSrc = fs.readFileSync(path.join(ROOT, 'google-ads-conversions.js'), 'utf8');

/* ── lifting the real functions out of index.js ──────────────────────── */
function liftDecl(decl) {
  const i = src.indexOf('\n' + decl);
  if (i === -1) throw new Error('not found in index.js: ' + decl);
  let j = src.indexOf('{', i);
  const semi = src.indexOf(';', i);
  if (j === -1 || (semi !== -1 && semi < j)) return src.slice(i + 1, semi + 1);
  let d = 0;
  for (let k = j; k < src.length; k++) {
    if (src[k] === '{') d++;
    else if (src[k] === '}') { d--; if (!d) { j = k; break; } }
  }
  return src.slice(i + 1, j + 1);
}
function liftFn(decl) {
  const i = src.indexOf('\n' + decl);
  if (i === -1) throw new Error('not found in index.js: ' + decl);
  let k = src.indexOf('(', i), d = 0;
  for (; k < src.length; k++) { if (src[k] === '(') d++; else if (src[k] === ')') { d--; if (!d) break; } }
  let j = src.indexOf('{', k); d = 0;
  for (let m = j; m < src.length; m++) { if (src[m] === '{') d++; else if (src[m] === '}') { d--; if (!d) { j = m; break; } } }
  return src.slice(i + 1, j + 1);
}
const REAL = new Function('process', 'predictedLtvFor', 'resolveEventProduct', [
  liftDecl('const FREE_EMAIL_DOMAINS'),
  liftFn('function damerauLevenshtein'),
  liftFn('function freeEmailMatch'),
  liftDecl('const ELV_EXCLUDED_DOMAINS'),
  liftDecl('const INTERNAL_TEST_EMAILS'),
  liftFn('function isInternalLead'),
  liftDecl('const INTERNAL_STAGING_HOSTS'),
  liftFn('function isStagingSubmission'),
  liftFn('function isInternalSubmission'),
  liftDecl('const WEBSITE_VERIFIED_REASONS'),
  liftFn('function isWebsiteVerified'),
  liftFn('function gadsValueFor'),
  'return { freeEmailMatch, isInternalLead, isStagingSubmission, isInternalSubmission, isWebsiteVerified, gadsValueFor };',
].join('\n'))({ env: {} }, META.predictedLtvFor, META.resolveEventProduct);

/* ── fixtures ─────────────────────────────────────────────────────────── */
const NOW = Date.UTC(2026, 9, 6, 16, 0, 0);              // 6 Oct 2026, 12:00 ET
const H = 3600000, D = 24 * H;
const GCLID  = 'Cj0KCQjw' + 'A'.repeat(60);
const GCLID2 = 'Cj0KCQjw' + 'B'.repeat(60);
const GBRAID = '0AAAAAD' + 'C'.repeat(40);
const WBRAID = 'ClEKCQ' + 'D'.repeat(40);
const { privateKey } = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 });
const SA = { type: 'service_account', client_email: 'gads-test@stub.iam.gserviceaccount.com', private_key_id: 'kid-1',
  private_key: privateKey.export({ type: 'pkcs8', format: 'pem' }), token_uri: 'https://oauth2.googleapis.com/token' };
const SA_B64 = Buffer.from(JSON.stringify(SA)).toString('base64');

function lead(over = {}) {
  return {
    session_id: '11111111-2222-4333-8444-555555555555',
    email: 'buyer@northwind-trading.test', phone: '+12025550123', website: 'northwind-trading.test',
    page_url: 'https://www.gushwork.ai/demo?utm_source=google&gclid=' + GCLID,
    previous_page: null, landing_page: null,
    created_at: new Date(NOW - 2 * D), booked_at: new Date(NOW - 2 * D + 10 * 60000),
    disqualified: false, non_icp_blocked: false, non_icp_llm_flagged: false,
    website_check_failed: false, website_check_reason: 'content_clean',
    product_interest: null, utm_campaign: null, utm_medium: null, offer_campaign: null, offer_medium: null,
    ...over,
  };
}

(async () => {
  /* ================================================================
     A. THE MODULE'S OWN RULES
     ================================================================ */

  // A1. Email, Google's way -- not Meta's
  eq('A1: gmail loses dots and the +suffix', G.normaliseEmailForGoogle(' Cloudy.San.Francisco+shopping@Gmail.com '), 'cloudysanfrancisco@gmail.com');
  eq('A1: googlemail is treated like gmail', G.normaliseEmailForGoogle('a.b+x@googlemail.com'), 'ab@googlemail.com');
  eq('A1: any other domain KEEPS its dots and +', G.normaliseEmailForGoogle('User.Name+NYC@Example.com'), 'user.name+nyc@example.com');
  eq('A1: intermediate whitespace is removed', G.normaliseEmailForGoogle('a b@acme.test'), 'ab@acme.test');
  eq('A1: no @ is no email', G.normaliseEmailForGoogle('not-an-email'), null);
  eq('A1: a gmail address that is ONLY a +suffix is refused', G.normaliseEmailForGoogle('+x@gmail.com'), null);

  // A2. Phone: E.164 WITH the +
  eq('A2: separators are removed, the + kept', G.normalisePhoneForGoogle('+1 (202) 555-0123'), '+12025550123');
  eq('A2: no + is refused, never guessed', G.normalisePhoneForGoogle('2025550123'), null);
  eq('A2: too short is refused', G.normalisePhoneForGoogle('+12345'), null);
  const metaPhone = crypto.createHash('sha256').update('12025550123').digest('hex');   // Meta's: digits only
  const googlePhone = G.googleUserIdentifiers({ phone: '+12025550123' })[0].phoneNumber;
  ok('A2: the Google phone hash is NOT the Meta one (reusing Meta\'s normaliser would match nobody)', googlePhone !== metaPhone);
  eq('A2: the Google phone hash is of "+12025550123"', googlePhone, crypto.createHash('sha256').update('+12025550123').digest('hex'));
  eq('A2: sha256Hex known vector', G.sha256Hex('abc'), 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');

  // A3. Eastern time with the DAY'S offset
  eq('A3: summer is -04:00', G.etRfc3339('2026-07-01T16:30:45.900Z'), '2026-07-01T12:30:45-04:00');
  eq('A3: winter is -05:00', G.etRfc3339('2026-12-01T16:30:45Z'), '2026-12-01T11:30:45-05:00');
  eq('A3: 01:30 before the fall-back hour is EDT', G.etRfc3339('2026-11-01T05:30:00Z'), '2026-11-01T01:30:00-04:00');
  eq('A3: 01:30 after it is EST -- same wall clock, different offset', G.etRfc3339('2026-11-01T06:30:00Z'), '2026-11-01T01:30:00-05:00');
  ok('A3: seconds are truncated, never rounded up past the booking', G.etRfc3339('2026-07-01T16:30:45.999Z').endsWith(':45-04:00'));

  // A4. Click IDs in a URL
  let ids = G.clickIdsInUrl('https://x.test/a?gclid=' + GCLID + '&gbraid=' + GBRAID);
  ok('A4: both IDs are read', ids.gclid && ids.gclid.valid && ids.gbraid && ids.gbraid.valid);
  ids = G.clickIdsInUrl('https://x.test/a?gclid=Cj0KCQ&gbraid=' + GBRAID);
  ok('A4: a cut gclid (the pre-21-Aug 500-char cut left 6-17 chars) is NOT valid', ids.gclid && !ids.gclid.valid);
  let cc = G.clickCandidates({ page_url: 'https://x.test/a?gclid=Cj0KCQ&gbraid=' + GBRAID });
  ok('A4: a malformed gclid does not hide a valid gbraid in the same field', cc.candidates.length === 1 && cc.candidates[0].type === 'gbraid' && cc.malformed === 1, JSON.stringify(cc));
  cc = G.clickCandidates({ page_url: 'https://x.test/?wbraid=' + WBRAID + '&gbraid=' + GBRAID + '&gclid=' + GCLID });
  ok('A4: within a field gclid beats gbraid beats wbraid', cc.candidates.length === 1 && cc.candidates[0].type === 'gclid');
  cc = G.clickCandidates({ page_url: 'https://x.test/?gclid=' + GCLID, landing_page: 'https://x.test/?gclid=' + GCLID });
  eq('A4: the same ID in two fields is one candidate', cc.candidates.length, 1);
  eq('A4: field order is page_url, previous_page, landing_page', G.CLICK_ID_FIELDS.join(','), 'page_url,previous_page,landing_page');

  // A5. The choice
  const booked = new Date(NOW - 2 * D);
  const two = [{ field: 'page_url', order: 0, type: 'gclid', value: GCLID }, { field: 'landing_page', order: 2, type: 'gclid', value: GCLID2 }];
  let ch = G.chooseClickId(two, new Map([[GCLID, new Date(NOW - 3 * D)], [GCLID2, new Date(NOW - 2.5 * D)]]), booked, NOW);
  eq('A5: the NEWER click wins across fields, even from landing_page', ch.pick && ch.pick.value, GCLID2);
  ch = G.chooseClickId(two, new Map([[GCLID, new Date(NOW - 3 * D)], [GCLID2, new Date(NOW - 3 * D)]]), booked, NOW);
  eq('A5: a tie goes to field order (page_url first)', ch.pick && ch.pick.value, GCLID);
  ch = G.chooseClickId(two.slice(0, 1), new Map([[GCLID, new Date(booked.getTime() - 91 * D)]]), booked, NOW);
  eq('A5: a click over 90 days before the booking is too_old', ch.skip, 'too_old');
  ch = G.chooseClickId(two.slice(0, 1), new Map([[GCLID, new Date(booked.getTime() - 89 * D)]]), booked, NOW);
  eq('A5: 89 days is fine', ch.pick && ch.pick.value, GCLID);
  ch = G.chooseClickId(two.slice(0, 1), new Map([[GCLID, new Date(booked.getTime() + H)]]), booked, NOW);
  eq('A5: a click first seen AFTER the booking is refused', ch.skip, 'click_after_booking');
  ch = G.chooseClickId(two, new Map([[GCLID, new Date(booked.getTime() - 100 * D)], [GCLID2, new Date(booked.getTime() - 1 * D)]]), booked, NOW);
  eq('A5: an expired click does not stop an older field\'s valid one being used', ch.pick && ch.pick.value, GCLID2);
  ch = G.chooseClickId(two.slice(0, 1), new Map([[GCLID, new Date(NOW - 5 * H)]]), new Date(NOW - 4 * H), NOW);
  eq('A5: a click under 6 hours old is WAITED for, not skipped', ch.wait, 'click_too_recent');
  eq('A5: no candidates is no_click_id', G.chooseClickId([], new Map(), booked, NOW).skip, 'no_click_id');

  // A6. Settings and their safe defaults
  let s = G.gadsSettings({});
  eq('A6: OFF by default', s.enabled, false);
  eq('A6: validate-only by default', s.validateOnly, true);
  eq('A6: a typo in the validate switch stays validate-only', G.gadsSettings({ GADS_UPLOAD_VALIDATE_ONLY: 'False' }).validateOnly, true);
  eq('A6: only the exact string "false" turns validate-only off', G.gadsSettings({ GADS_UPLOAD_VALIDATE_ONLY: 'false' }).validateOnly, false);
  eq('A6: free email is included by default', s.excludeFreeEmail, false);
  eq('A6: the account default', s.customerId, '5442288209');
  eq('A6: the action default', s.conversionActionId, '7825004775');
  ok('A6: Flighted and Upraw are excluded by default', s.excludedDomains.includes('flighted.co') && s.excludedDomains.includes('uprawmedia.com'));
  s = G.gadsSettings({ GADS_EXCLUDED_DOMAINS: 'other-agency.test' });
  ok('A6: the env EXTENDS the agency list and cannot drop the defaults', s.excludedDomains.includes('other-agency.test') && s.excludedDomains.includes('flighted.co'));
  ok('A6: other *.webflow.io and loopback hosts are excluded by default', G.gadsSettings({}).excludedHosts.join(',') === 'webflow.io,localhost,127.0.0.1');
  eq('A6: a cutover WITHOUT an offset is refused', G.parseCutover('2026-10-20T00:00:00').at, null);
  eq('A6: a bare date is refused', G.parseCutover('2026-10-20').at, null);
  eq('A6: an impossible date is refused', G.parseCutover('2026-13-40T00:00:00Z').at, null);
  eq('A6: a cutover with its ET offset is read to the right instant', G.parseCutover('2026-10-20T00:00:00-04:00').at.toISOString(), '2026-10-20T04:00:00.000Z');
  ok('A6: no cutover is a config problem', G.gadsConfigProblems(G.gadsSettings({ GADS_SERVICE_ACCOUNT_JSON_B64: SA_B64 })).some((p) => /CUTOVER/.test(p)));
  ok('A6: an unreadable key is a config problem', G.gadsConfigProblems(G.gadsSettings({ GADS_SERVICE_ACCOUNT_JSON_B64: 'bm90IGpzb24=', GADS_UPLOAD_CUTOVER: '2026-10-20T00:00:00-04:00' })).some((p) => /readable/.test(p)));
  eq('A6: a full config has no problems', G.gadsConfigProblems(G.gadsSettings({ GADS_SERVICE_ACCOUNT_JSON_B64: SA_B64, GADS_UPLOAD_CUTOVER: '2026-10-20T00:00:00-04:00' })).length, 0);

  // A7. Host matching: exact or subdomain, never a substring
  const AG = G.GADS_DEFAULT_EXCLUDED_DOMAINS;
  eq('A7: an email domain', G.gadsMatchList(G.gadsHostOf('Pat@Flighted.co'), AG), 'flighted.co');
  eq('A7: a website with scheme and www', G.gadsMatchList(G.gadsHostOf('https://www.uprawmedia.com/about?x=1'), AG), 'uprawmedia.com');
  eq('A7: a subdomain', G.gadsMatchList(G.gadsHostOf('agents.flighted.co'), AG), 'flighted.co');
  eq('A7: NOT a longer name that contains it', G.gadsMatchList(G.gadsHostOf('notflighted.co'), AG), null);
  eq('A7: NOT a lookalike under another domain', G.gadsMatchList(G.gadsHostOf('flighted.co.evil.test'), AG), null);
  eq('A7: a port is ignored', G.gadsMatchList(G.gadsHostOf('http://localhost:3000/demo'), G.GADS_DEFAULT_EXCLUDED_HOSTS), 'localhost');

  // A8. What each Google reply means
  eq('A8: 200 is ok', G.classifyIngestReply(200, { requestId: 'r' }).kind, 'ok');
  const bad = G.classifyIngestReply(404, { error: { details: [{ reason: 'INVALID_CONVERSION_ACTION_ID' }] } });
  ok('A8: 404 INVALID_CONVERSION_ACTION_ID is PERMANENT and flagged as the action', bad.kind === 'permanent' && bad.action === true);
  eq('A8: the reason is also read from fieldViolations', G.classifyIngestReply(404, { error: { details: [{ fieldViolations: [{ reason: 'INVALID_CONVERSION_ACTION_ID' }] }] } }).action, true);
  eq('A8: another 400 is permanent', G.classifyIngestReply(400, { error: {} }).kind, 'permanent');
  for (const st of [408, 429, 500, 503]) eq(`A8: ${st} is retryable`, G.classifyIngestReply(st, null).kind, 'retryable');
  ok('A8: 401 and 403 are retryable but marked as auth', ['401', '403'].every((st) => { const r = G.classifyIngestReply(Number(st), { error: {} }); return r.kind === 'retryable' && r.auth; }));

  // A9. The event and the request
  const ev = G.buildGadsEvent({ sessionId: 'sid-1', click: { type: 'gbraid', value: GBRAID }, bookedAt: '2026-12-01T16:30:45Z', value: 12000, email: 'A.B+x@gmail.com', phone: '+12025550123' });
  eq('A9: exactly one click ID', Object.keys(ev.adIdentifiers).join(','), 'gbraid');
  eq('A9: the transaction ID is the session ID', ev.transactionId, 'sid-1');
  eq('A9: booking time in ET with the winter offset', ev.eventTimestamp, '2026-12-01T11:30:45-05:00');
  ok('A9: value with USD', ev.conversionValue === 12000 && ev.currency === 'USD');
  eq('A9: email AND phone go on a gbraid row', ev.userData.userIdentifiers.length, 2);
  eq('A9: the email hash is of the Google-normalised address', ev.userData.userIdentifiers[0].emailAddress, G.sha256Hex('ab@gmail.com'));
  ok('A9: NO consent field', !('consent' in ev));
  const noVal = G.buildGadsEvent({ sessionId: 's', click: { type: 'gclid', value: GCLID }, bookedAt: NOW, value: null, email: 'x@acme.test', phone: '' });
  ok('A9: an unknown product sends no value and no currency', !('conversionValue' in noVal) && !('currency' in noVal));
  eq('A9: no phone, email only', noVal.userData.userIdentifiers.length, 1);
  const req = G.buildIngestRequest(ev, G.gadsSettings({}), true);
  ok('A9: the destination is account 5442288209, action 7825004775', req.destinations[0].operatingAccount.accountId === '5442288209' && req.destinations[0].operatingAccount.accountType === 'GOOGLE_ADS' && req.destinations[0].productDestinationId === '7825004775');
  ok('A9: NO loginAccount', !('loginAccount' in req.destinations[0]));
  ok('A9: validateOnly is exactly true, encoding HEX', req.validateOnly === true && req.encoding === 'HEX');
  eq('A9: validateOnly is false only when asked for false', G.buildIngestRequest(ev, G.gadsSettings({}), 'yes').validateOnly, false);

  // A10. The token: scope, signature, cache, and nothing secret in an error
  const tokCalls = [];
  const tokFetch = async (url, opts) => { tokCalls.push({ url, body: String(opts.body) }); return { ok: true, status: 200, json: async () => ({ access_token: 'tok-1', expires_in: 3600 }) }; };
  const getTok = G.createTokenSource(tokFetch, () => NOW);
  eq('A10: a token comes back', await getTok(SA_B64), 'tok-1');
  await getTok(SA_B64);
  eq('A10: it is cached for the second call', tokCalls.length, 1);
  const assertion = new URLSearchParams(tokCalls[0].body).get('assertion');
  const [h64, c64, s64] = assertion.split('.');
  const claim = JSON.parse(Buffer.from(c64, 'base64').toString('utf8'));
  eq('A10: the scope is exactly datamanager', claim.scope, 'https://www.googleapis.com/auth/datamanager');
  eq('A10: issued by the service account', claim.iss, SA.client_email);
  ok('A10: the signature verifies with the key\'s public half',
     crypto.createVerify('RSA-SHA256').update(h64 + '.' + c64).verify(crypto.createPublicKey(privateKey), Buffer.from(s64.replace(/-/g, '+').replace(/_/g, '/'), 'base64')));
  const refusing = G.createTokenSource(async () => ({ ok: false, status: 400, json: async () => ({ error: 'invalid_grant' }) }), () => NOW);
  let tokErr = null; try { await refusing(SA_B64); } catch (e) { tokErr = e; }
  ok('A10: a refused token is an AUTH error that names invalid_grant', tokErr && tokErr.kind === 'auth' && /invalid_grant/.test(tokErr.message));
  ok('A10: the error never carries the key', tokErr && !tokErr.message.includes('PRIVATE KEY') && !tokErr.message.includes(SA.private_key.slice(40, 80)));

  // A11. Consent: a switch, off by default, exact Data Manager names and values
  eq('A11: consent is OFF by default', G.gadsSettings({}).consentGranted, false);
  eq('A11: a typo leaves it off', G.gadsSettings({ GADS_CONSENT_GRANTED: 'True' }).consentGranted, false);
  eq('A11: only the exact string "true" turns it on', G.gadsSettings({ GADS_CONSENT_GRANTED: 'true' }).consentGranted, true);
  const evOn = G.buildGadsEvent({ sessionId: 's-on', click: { type: 'gclid', value: GCLID }, bookedAt: NOW, value: 12000, email: 'x@acme.test', phone: '', consentGranted: true });
  eq('A11: ON puts both consents on the event, GRANTED, with the reference\'s field names',
     JSON.stringify(evOn.consent), '{"adUserData":"CONSENT_GRANTED","adPersonalization":"CONSENT_GRANTED"}');
  const reqOn = G.buildIngestRequest(evOn, G.gadsSettings({ GADS_CONSENT_GRANTED: 'true' }), true);
  eq('A11: ...and it reaches the request inside the event', JSON.stringify(reqOn.events[0].consent), '{"adUserData":"CONSENT_GRANTED","adPersonalization":"CONSENT_GRANTED"}');
  ok('A11: ...on the event only, not duplicated at request level', !('consent' in reqOn));
  const evOff = G.buildGadsEvent({ sessionId: 's-off', click: { type: 'gclid', value: GCLID }, bookedAt: NOW, value: 12000, email: 'x@acme.test', phone: '' });
  ok('A11: OFF (the default) leaves the field ABSENT, not denied or unspecified', !('consent' in evOff));
  ok('A11: OFF leaves it absent from the request too', !('consent' in G.buildIngestRequest(evOff, G.gadsSettings({}), true).events[0]));
  ok('A11: a truthy string is not true', !('consent' in G.buildGadsEvent({ sessionId: 's', click: { type: 'gclid', value: GCLID }, bookedAt: NOW, value: null, email: 'x@acme.test', consentGranted: 'true' })));
  /* Guarded: if consent were never sent, this must FAIL, not crash the suite. */
  ok('A11: the event holds its own consent object, never the shared constant', !!evOn.consent && evOn.consent !== G.GADS_CONSENT_GRANTED);
  ok('A11: the shared constant is frozen, so nothing can rewrite it for every event', Object.isFrozen(G.GADS_CONSENT_GRANTED));

  /* ================================================================
     B. THE EXCLUSION RULES, WITH THE REAL FUNCTIONS FROM index.js
     ================================================================ */
  const firstSeenAt = new Date(NOW - 2 * D);
  function uploaderWith(env = {}, nonIcpFresh = async () => null) {
    const queries = [];
    const pool = { query: async (q, p) => {
      queries.push(String(q));
      if (/AS first_seen/.test(q)) return { rows: [{ first_seen: firstSeenAt }], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    } };
    const u = G.createGadsUploader({
      pool, sourceSql: "'Google'",
      isInternalLead: REAL.isInternalLead, isStagingSubmission: REAL.isStagingSubmission, isInternalSubmission: REAL.isInternalSubmission,
      isWebsiteVerified: REAL.isWebsiteVerified, freeEmailMatch: REAL.freeEmailMatch,
      nonIcpFresh, valueFor: REAL.gadsValueFor, env, now: () => NOW,
      fetchImpl: () => { throw new Error('part B never sends'); }, log: { log() {}, warn() {}, info() {} },
    });
    return { u, queries };
  }
  /* meta-capi.js logs each page that takes the default product, once per
     page. True and harmless here; it is not this suite's output. */
  const quietLog = console.log;
  console.log = () => {};
  const S0 = G.gadsSettings({});
  const S0free = G.gadsSettings({ GADS_EXCLUDE_FREE_EMAIL: 'true' });
  const why = async (over, s = S0, fresh) => {
    const v = await uploaderWith({}, fresh).u.evaluate(lead(over), s);
    return v.skip || v.wait || (v.send ? 'send' : '?');
  };

  eq('B: a clean Google booking is sent', await why({}), 'send');
  eq('B: agency by EMAIL domain (flighted.co)', await why({ email: 'pat@flighted.co' }), 'agency');
  eq('B: agency by WEBSITE domain (uprawmedia.com), with a company email', await why({ website: 'https://www.uprawmedia.com/' }), 'agency');
  eq('B: agency by a website SUBDOMAIN', await why({ website: 'clients.flighted.co' }), 'agency');
  eq('B: a lookalike agency domain is NOT excluded', await why({ email: 'x@notflighted.co', website: 'notflighted.co' }), 'send');
  eq('B: our own address (gushwork.ai) is internal', await why({ email: 'darshil@gushwork.ai' }), 'internal');
  eq('B: the form\'s test address b@g.ai is internal', await why({ email: 'b@g.ai' }), 'internal');
  eq('B: agent@allstate.com, our block walkthrough, is internal', await why({ email: 'agent@allstate.com' }), 'internal');
  eq('B: test.com is internal', await why({ email: 'qa@test.com' }), 'internal');
  eq('B: example.com is internal', await why({ email: 'someone@example.com' }), 'internal');
  eq('B: the staging site gushwork.webflow.io is staging', await why({ page_url: 'https://gushwork.webflow.io/demo?gclid=' + GCLID }), 'staging');
  eq('B: ANOTHER webflow.io site is staging (Google-only list)', await why({ page_url: 'https://gushwork-v2.webflow.io/demo?gclid=' + GCLID }), 'staging');
  eq('B: a localhost landing page is staging', await why({ landing_page: 'http://localhost:3000/demo' }), 'staging');
  eq('B: disqualified, stated explicitly', await why({ disqualified: true }), 'disqualified');
  eq('B: non-ICP blocked', await why({ non_icp_blocked: true }), 'non_icp');
  eq('B: non-ICP FLAGGED (Meta-only industry) is skipped too', await why({ non_icp_llm_flagged: true }), 'non_icp');
  eq('B: a FRESH verdict the lead row never got is skipped', await why({}, S0, async () => ({ action: 'meta', business_type: 'restaurant_food' })), 'non_icp');
  eq('B: a fresh read that THROWS waits rather than sends', await why({}, S0, async () => { throw new Error('db down'); }), 'non_icp_read_failed');
  eq('B: a failed website check', await why({ website_check_failed: true, website_check_reason: 'parked_confirmed' }), 'website');
  eq('B: a "could not check" website verdict, as Meta treats it', await why({ website_check_reason: 'dns_indeterminate' }), 'website');
  eq('B: no website verdict at all passes, as Meta treats it', await why({ website_check_reason: null }), 'send');
  eq('B: free email is SENT while the setting is off', await why({ email: 'founder@gmail.com' }), 'send');
  eq('B: free email is SKIPPED when GADS_EXCLUDE_FREE_EMAIL is on', await why({ email: 'founder@gmail.com' }, S0free), 'free_email');
  eq('B: a one-letter typo of a free domain counts as free', await why({ email: 'founder@gmial.com' }, S0free), 'free_email');
  eq('B: a company email is unaffected by the free-email setting', await why({}, S0free), 'send');
  const cutAfter = { ...S0, cutover: new Date(NOW - 1 * D) };
  eq('B: a booking BEFORE the cutover is never sent', await why({}, cutAfter), 'before_cutover');
  const cutBefore = { ...S0, cutover: new Date(NOW - 3 * D) };
  eq('B: a booking after the cutover is sent', await why({}, cutBefore), 'send');
  eq('B: inside the 30-minute hold it waits', await why({ booked_at: new Date(NOW - 10 * 60000) }), 'hold');
  eq('B: no click ID', await why({ page_url: 'https://www.gushwork.ai/demo' }), 'no_click_id');
  const reason = await uploaderWith().u.gate(lead({ email: 'pat@flighted.co', website: 'acme.test' }), S0);
  eq('B: the recorded detail names the matched domain', reason && reason.detail, 'email domain matches flighted.co');
  const sendV = await uploaderWith().u.evaluate(lead({ product_interest: 'crm' }), S0);
  eq('B: the value comes from the product lookup (CRM -> 5000), not meta_predicted_ltv', sendV.send && sendV.send.value, 5000);
  const sendA = await uploaderWith().u.evaluate(lead(), S0);
  eq('B: an AEO /demo lead is 12000', sendA.send && sendA.send.value, 12000);
  const sendU = await uploaderWith().u.evaluate(lead({ page_url: 'not a url?gclid=' + GCLID }), S0);
  ok('B: an unreadable page sends NO value (unknown product)', sendU.send && sendU.send.value === null && !('conversionValue' in sendU.send.event), JSON.stringify(sendU.send && sendU.send.event));

  console.log = quietLog;

  // B2. The flag is read in code at upload time, never as a SQL filter
  ok('B2: the module never filters SQL on non_icp_llm_flagged (test-non-icp.js 10f)', !/non_icp_llm_flagged\s+IS\s+(NOT\s+)?TRUE/i.test(modSrc));
  ok('B2: the module never filters SQL on disqualified', !/disqualified\s+IS\s+(NOT\s+)?TRUE/i.test(modSrc));
  ok('B2: the module writes to no table but its own', !/(INSERT INTO|UPDATE|DELETE FROM)\s+(?!\$\{TABLE\})(?!gads_conversion_uploads)[a-z_]+/i.test(modSrc.replace(/\/\*[\s\S]*?\*\//g, '')));
  ok('B2: index.js still does not change INTERNAL_TEST_EMAILS for this', !/flighted|uprawmedia/.test(liftDecl('const INTERNAL_TEST_EMAILS') + liftDecl('const ELV_EXCLUDED_DOMAINS')));

  // B4. Consent through evaluate(), with the real gates
  const consOn = await uploaderWith().u.evaluate(lead(), G.gadsSettings({ GADS_CONSENT_GRANTED: 'true' }));
  eq('B4: with the setting ON the event to send carries consent', consOn.send && consOn.send.event.consent && consOn.send.event.consent.adPersonalization, 'CONSENT_GRANTED');
  const consOff = await uploaderWith().u.evaluate(lead(), G.gadsSettings({}));
  ok('B4: with the setting OFF it does not', consOff.send && !('consent' in consOff.send.event));

  // B5. The whole sweep, both ways, reading back the body that reaches Google
  async function sweepBody(extraEnv) {
    const sent = [];
    const pool = { query: async (q, p) => {
      const flat = String(q).replace(/\s+/g, ' ').trim();
      if (/FROM leads l LEFT JOIN gads_conversion_uploads g/.test(flat)) return { rows: [{ ...lead({ session_id: '99999999-0000-4000-8000-000000000009', booked_at: new Date(NOW - 2 * D + 600000) }), upload_status: null }], rowCount: 1 };
      if (/AS first_seen/.test(flat)) return { rows: [{ first_seen: firstSeenAt }], rowCount: 1 };
      if (/^INSERT INTO gads_conversion_uploads .*RETURNING session_id/.test(flat)) return { rows: [{ session_id: p[0] }], rowCount: 1 };
      return { rows: [], rowCount: 1 };
    } };
    const u = G.createGadsUploader({ pool, sourceSql: "'Google'",
      isInternalLead: REAL.isInternalLead, isStagingSubmission: REAL.isStagingSubmission, isInternalSubmission: REAL.isInternalSubmission,
      isWebsiteVerified: REAL.isWebsiteVerified, freeEmailMatch: REAL.freeEmailMatch, nonIcpFresh: async () => null, valueFor: REAL.gadsValueFor,
      env: { GADS_UPLOAD_ENABLED: 'true', GADS_UPLOAD_CUTOVER: '2026-09-01T00:00:00-04:00', GADS_SERVICE_ACCOUNT_JSON_B64: SA_B64, ...extraEnv },
      now: () => NOW, log: { log() {}, warn() {}, info() {} },
      fetchImpl: async (url, opts) => {
        if (url === SA.token_uri) return { ok: true, status: 200, json: async () => ({ access_token: 't', expires_in: 3600 }) };
        sent.push(JSON.parse(opts.body));
        return { ok: true, status: 200, json: async () => ({ requestId: 'v-x' }) };
      } });
    await u.runSweep();
    return sent;
  }
  const bodiesOn = await sweepBody({ GADS_CONSENT_GRANTED: 'true' });
  eq('B5: ON: one request left', bodiesOn.length, 1);
  eq('B5: ON: the request body carries GRANTED for both', bodiesOn[0] && JSON.stringify(bodiesOn[0].events[0].consent), '{"adUserData":"CONSENT_GRANTED","adPersonalization":"CONSENT_GRANTED"}');
  eq('B5: ON: still validate-only', bodiesOn[0] && bodiesOn[0].validateOnly, true);
  const bodiesOff = await sweepBody({});
  eq('B5: OFF: one request left', bodiesOff.length, 1);
  ok('B5: OFF: the request body has NO consent anywhere', bodiesOff[0] && !JSON.stringify(bodiesOff[0]).includes('consent'), JSON.stringify(bodiesOff[0]));

  // B3. The sweep's switches
  const off = uploaderWith({});
  eq('B3: switched off, the sweep does nothing at all', JSON.stringify(await off.u.runSweep()), '{"off":true}');
  eq('B3: ...and asks the database nothing', off.queries.length, 0);
  const alerts = [];
  const noCredsU = G.createGadsUploader({ pool: { query: async () => { throw new Error('must not query'); } }, sourceSql: "'Google'",
    isInternalLead: REAL.isInternalLead, isStagingSubmission: REAL.isStagingSubmission, isInternalSubmission: REAL.isInternalSubmission,
    isWebsiteVerified: REAL.isWebsiteVerified, freeEmailMatch: REAL.freeEmailMatch, nonIcpFresh: async () => null, valueFor: REAL.gadsValueFor,
    alertOps: (sev, srcName, title) => alerts.push(`${sev}|${srcName}|${title}`),
    env: { GADS_UPLOAD_ENABLED: 'true', GADS_UPLOAD_CUTOVER: '2026-10-01T00:00:00-04:00' }, now: () => NOW,
    fetchImpl: () => { throw new Error('must not send'); }, log: { log() {}, warn() {}, info() {} } });
  const nc = await noCredsU.runSweep();
  ok('B3: ON with no credentials -> it alerts, critical', alerts.some((a) => a.startsWith('critical|Google Ads|')), alerts.join(' / '));
  ok('B3: ...and names the missing variable', (nc.problems || []).some((p) => /GADS_SERVICE_ACCOUNT_JSON_B64/.test(p)));
  const noCut = [];
  const noCutU = G.createGadsUploader({ pool: { query: async () => { throw new Error('must not query'); } }, sourceSql: "'Google'",
    isInternalLead: REAL.isInternalLead, isStagingSubmission: REAL.isStagingSubmission, isInternalSubmission: REAL.isInternalSubmission,
    isWebsiteVerified: REAL.isWebsiteVerified, freeEmailMatch: REAL.freeEmailMatch, nonIcpFresh: async () => null, valueFor: REAL.gadsValueFor,
    alertOps: (sev, srcName, title) => noCut.push(title),
    env: { GADS_UPLOAD_ENABLED: 'true', GADS_SERVICE_ACCOUNT_JSON_B64: SA_B64 }, now: () => NOW,
    fetchImpl: () => { throw new Error('must not send'); }, log: { log() {}, warn() {}, info() {} } });
  const ncut = await noCutU.runSweep();
  ok('B3: ON with no cutover -> nothing is sent, and it says why', (ncut.problems || []).some((p) => /CUTOVER/.test(p)) && noCut.length === 1);
  const logs = [];
  /* A rejecting runSweep, not a throwing one: if the switch were wrongly ON
     the sweep would START, and that has to read as a failed assertion here,
     not crash the suite (which measure.js counts as UNMEASURED). */
  let started;
  try { started = G.startGadsUploadSweep({ runSweep: async () => { throw new Error('must not run'); } }, {}, { log: (m) => logs.push(m), warn() {} }); }
  catch (e) { started = 'threw: ' + e.message; }
  if (started && typeof started === 'object') clearInterval(started);
  eq('B3: startGadsUploadSweep with the switch off starts nothing', started, null);
  ok('B3: ...and says it is off', logs.some((m) => /Conversion upload is OFF/.test(m)));

  /* ================================================================
     C. THE REAL APP, BOOTED WITH THE UPLOAD ON
     ================================================================ */
  const PORT = 41263;
  const SEND_ID   = 'aaaaaaaa-0000-4000-8000-000000000001';
  const AGENCY_ID = 'aaaaaaaa-0000-4000-8000-000000000002';
  const bootNow = Date.now();
  const S = { queries: [], datamanager: [], token: [], slack: [] };
  const fixtureRows = [
    lead({ session_id: SEND_ID, created_at: new Date(bootNow - 2 * D), booked_at: new Date(bootNow - 2 * D + 600000) }),
    lead({ session_id: AGENCY_ID, email: 'pm@uprawmedia.com', website: 'uprawmedia.com', created_at: new Date(bootNow - 2 * D), booked_at: new Date(bootNow - 2 * D + 600000) }),
  ].map((r) => ({ ...r, upload_status: null }));
  function stubQuery(q, params) {
    const flat = (typeof q === 'string' ? q : (q && q.text) || '').replace(/\s+/g, ' ').trim();
    S.queries.push({ sql: flat, params: params || [] });
    if (/FROM leads l LEFT JOIN gads_conversion_uploads g/.test(flat)) return { rows: fixtureRows, rowCount: fixtureRows.length };
    if (/AS first_seen/.test(flat)) return { rows: [{ first_seen: new Date(bootNow - 2 * D) }], rowCount: 1 };
    if (/^INSERT INTO gads_conversion_uploads .*RETURNING session_id/.test(flat)) return { rows: [{ session_id: params[0] }], rowCount: 1 };
    if (/^(INSERT INTO|UPDATE) gads_conversion_uploads/.test(flat)) return { rows: [], rowCount: 1 };
    return { rows: [], rowCount: 0 };
  }
  class StubClient { async query(q, p) { return stubQuery(q, p); } release() {} }
  class StubPool { async connect() { return new StubClient(); } async query(q, p) { return stubQuery(q, p); } on() {} async end() {} }
  const origLoad = Module._load;
  Module._load = function (request) {
    if (request === 'pg') return { Pool: StubPool, Client: StubClient, types: { setTypeParser() {} } };
    return origLoad.apply(this, arguments);
  };
  const realFetch = global.fetch.bind(global);
  global.fetch = async function (url, opts) {
    const u = String(url);
    if (u.startsWith(`http://127.0.0.1:${PORT}`)) return realFetch(url, opts);
    if (u === SA.token_uri) { S.token.push(String(opts.body)); return { ok: true, status: 200, json: async () => ({ access_token: 'boot-token', expires_in: 3600 }) }; }
    if (u === G.DATA_MANAGER_INGEST_URL) {
      S.datamanager.push({ headers: opts.headers, body: JSON.parse(opts.body) });
      return { ok: true, status: 200, json: async () => ({ requestId: 'v-boot' }) };
    }
    if (/hooks\.slack\.com/.test(u)) { S.slack.push(String(opts && opts.body || '')); return { ok: true, status: 200, text: async () => 'ok', json: async () => ({}) }; }
    return { ok: true, status: 200, json: async () => ({}), text: async () => '{}' };
  };
  Object.assign(process.env, {
    PORT: String(PORT), DATABASE_URL: 'postgres://stub/stub',
    SLACK_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB', SLACK_ALERTS_WEBHOOK_URL: 'https://hooks.slack.com/services/STUB-ALERTS',
    ALLOWED_ORIGIN: 'https://www.gushwork.ai', MONITOR_TOKEN: 'stub',
    GADS_UPLOAD_ENABLED: 'true',
    GADS_UPLOAD_CUTOVER: new Date(bootNow - 10 * D).toISOString().replace(/\.\d+Z$/, 'Z'),
    GADS_SERVICE_ACCOUNT_JSON_B64: SA_B64,
  });
  delete process.env.GADS_UPLOAD_VALIDATE_ONLY;
  const realLog = console.log, realWarn = console.warn, realErr = console.error;
  const bootLogs = [];
  console.log = (...a) => bootLogs.push(a.join(' ')); console.warn = console.error = () => {};
  require(path.join(ROOT, 'index.js'));
  for (let i = 0; i < 40 && S.datamanager.length === 0; i++) await sleep(100);
  await sleep(300);
  console.log = realLog; console.warn = realWarn; console.error = realErr;

  ok('C: the boot log says the upload is ON and VALIDATE-ONLY', bootLogs.some((m) => /Conversion upload ON, VALIDATE-ONLY/.test(m)), bootLogs.filter((m) => /Google Ads/.test(m)).join(' | '));
  const sel = S.queries.find((q) => /FROM leads l LEFT JOIN gads_conversion_uploads g/.test(q.sql));
  ok('C: the sweep ran from start() and read leads', !!sel);
  ok('C: ...using the ONE Google source rule (DROPOFF_SOURCE_SQL)', sel && sel.sql.includes("WHEN hear_about_us ILIKE 'Google Ads%'") && sel.sql.includes("= 'Google'"));
  ok('C: ...bounded by the cutover and the 30-minute hold', sel && sel.sql.includes('l.booked_at >= $1') && sel.params[1] === G.GADS_HOLD_MS);
  eq('C: exactly ONE request reached Google', S.datamanager.length, 1);
  const body = S.datamanager[0] && S.datamanager[0].body;
  eq('C: ...for the clean lead, keyed by its session ID', body && body.events[0].transactionId, SEND_ID);
  eq('C: ...validate-only, because nothing set it otherwise', body && body.validateOnly, true);
  ok('C: ...to 5442288209 / 7825004775 with no loginAccount', body && body.destinations[0].operatingAccount.accountId === '5442288209' && body.destinations[0].productDestinationId === '7825004775' && !('loginAccount' in body.destinations[0]));
  eq('C: ...carrying the gclid from page_url', body && body.events[0].adIdentifiers.gclid, GCLID);
  ok('C: ...and no consent field', body && !('consent' in body.events[0]));
  eq('C: the bearer token is the one the token endpoint returned', S.datamanager[0] && S.datamanager[0].headers.Authorization, 'Bearer boot-token');
  const bootClaim = S.token[0] && JSON.parse(Buffer.from(new URLSearchParams(S.token[0]).get('assertion').split('.')[1], 'base64').toString('utf8'));
  eq('C: the token was asked for with the datamanager scope', bootClaim && bootClaim.scope, G.DATA_MANAGER_SCOPE);
  const skipW = S.queries.find((q) => /^INSERT INTO gads_conversion_uploads \(session_id, status, skip_reason/.test(q.sql) && q.params[0] === AGENCY_ID);
  ok('C: the Upraw lead was recorded as skipped, reason agency, by the REAL wiring', skipW && skipW.params[1] === 'agency', JSON.stringify(skipW && skipW.params));
  const claimW = S.queries.find((q) => /^INSERT INTO gads_conversion_uploads \(session_id, status, click_id_type/.test(q.sql));
  ok('C: the send was CLAIMED before the request (INSERT ... DO NOTHING RETURNING)', claimW && /ON CONFLICT \(session_id\) DO NOTHING RETURNING session_id/.test(claimW.sql));
  const outW = S.queries.find((q) => /^UPDATE gads_conversion_uploads SET status = \$2, attempts = \$3, request_id/.test(q.sql));
  ok('C: the outcome was recorded as validated with Google\'s request ID', outW && outW.params[1] === 'validated' && outW.params[3] === 'v-boot', JSON.stringify(outW && outW.params));
  ok('C: no lead row was written by the uploader', !S.queries.some((q) => /^UPDATE leads /.test(q.sql) && /gads/i.test(q.sql)));
  ok('C: index.js builds the uploader inside start(), not at the top level (TDZ)',
     /async function start\(\)[\s\S]*startGadsUploadSweep\(createGadsUploader\(\{[\s\S]*sourceSql: DROPOFF_SOURCE_SQL/.test(src));
  ok('C: index.js hands it the real internal-submission rule', /startGadsUploadSweep\(createGadsUploader\(\{[\s\S]{0,400}isInternalSubmission[\s\S]{0,200}isWebsiteVerified, freeEmailMatch/.test(src));
  ok('C: Google Ads has a FAILURE_MONITORS entry, so recordFailure can alert', /\n  'Google Ads': \{ alertAfter: 3,/.test(src));

  // C2. The dry-run tool, against the same stub: reads only, sends nothing
  const before = S.queries.length, dmBefore = S.datamanager.length, tokBefore = S.token.length;
  const toolOut = [];
  console.log = (...a) => toolOut.push(a.join(' '));
  const savedArgv = process.argv;
  process.argv = [process.argv[0], 'gads-upload-dry-run.js', '--days', '30'];
  require(path.join(ROOT, 'tools', 'gads-upload-dry-run.js'));
  for (let i = 0; i < 40 && !toolOut.some((m) => /"check"/.test(m)); i++) await sleep(100);
  process.argv = savedArgv;
  console.log = realLog;
  const toolQ = S.queries.slice(before).map((q) => q.sql);
  eq('C2: the tool opens a READ ONLY transaction first', toolQ[0], 'BEGIN TRANSACTION READ ONLY');
  eq('C2: ...and rolls it back last', toolQ[toolQ.length - 1], 'ROLLBACK');
  ok('C2: it never writes', !toolQ.some((q) => /^(INSERT|UPDATE|DELETE)\b/i.test(q)), toolQ.join(' || '));
  ok('C2: it never names the upload table, so it runs before the migration deploys', !toolQ.some((q) => /gads_conversion_uploads/.test(q)));
  ok('C2: it never calls Google', S.datamanager.length === dmBefore && S.token.length === tokBefore);
  ok('C2: it reports a total that sums', toolOut.some((m) => /"check": "sums to considered"/.test(m)), toolOut.join('\n').slice(0, 400));

  console.log('');
  if (failures.length) {
    console.log('  FAILURES:');
    for (const x of failures) console.log('   ✗ ' + x);
  }
  console.log(`  passed: ${pass}`);
  console.log(`  failed: ${fail}`);
  console.log('');
  process.exit(fail ? 1 : 0);
})();
