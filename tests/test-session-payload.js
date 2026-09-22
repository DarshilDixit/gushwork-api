/* ============================================================
   What /session actually receives — EXECUTED, from both form files.

   form_sessions.utm_term was null on all 15,309 rows. The server had
   always read req.body.utm_term; neither form file ever SENT it. So
   leads.utm_term (posted by /partial) was populated while
   form_sessions.utm_term was not, and comparing the two tables showed a
   UTM that appeared to change between them when nothing had changed.
   That is what got escalated on 9 Sep 2026.

   A source-text assertion would only prove the string "utm_term" appears
   in the file. This EXECUTES the real initSession / captureUTMs /
   saveSession lifted out of each form file, against a faked
   sessionStorage, document and location, and inspects the body that
   actually reaches fetch.

   It also pins the distinction that makes the second field worth having:
   `referrer` is gw_referrer, written ONCE per session by the site-wide
   Webflow script, so it is first-touch and identical on every hit.
   `page_referrer` is document.referrer for THIS load. The fixture gives
   them different values, so a regression that collapses one into the
   other fails on the value, not on a name.

   Both files are driven identically -- parity proven by behaviour rather
   than by diffing source, which is what test-ads-parity.js does.

   Dependency-free. Run:  node tests/test-session-payload.js
   ============================================================ */

require('./crash-reporter')('test-session-payload');

const fs   = require('fs');
const path = require('path');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
function eq(name, actual, expected) {
  ok(name, actual === expected, `got ${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`);
}

/* Pull a function's real source out of the file by brace matching, so the
   test runs the shipped text rather than a copy that can drift from it. */
function lift(src, name) {
  const start = src.indexOf(`function ${name}(`);
  if (start === -1) throw new Error(`function ${name} not found`);
  let i = src.indexOf('{', start), depth = 0;
  for (let j = i; j < src.length; j++) {
    if (src[j] === '{') depth++;
    else if (src[j] === '}') { depth--; if (depth === 0) return src.slice(start, j + 1); }
  }
  throw new Error(`unbalanced braces in ${name}`);
}

const PAGE  = 'https://www.gushwork.ai/start?utm_source=facebook&utm_medium=paid&utm_campaign=CAMP&utm_content=CONT&utm_term=TERM_VALUE_HERE';
const FIRST_TOUCH_REF = 'https://l.facebook.com/';   // gw_referrer, written once by the site-wide script
const THIS_HIT_REF    = 'https://www.gushwork.ai/pricing'; // document.referrer for THIS load

function drive(file, opts = {}) {
  const src = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');

  const store = Object.assign({
    gw_referrer: FIRST_TOUCH_REF,
    gw_landing_page: 'https://www.gushwork.ai/start',
    gw_session_id: 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',
  }, opts.session || {});
  const sessionStorage = {
    getItem: (k) => (k in store ? store[k] : null),
    setItem: (k, v) => { store[k] = String(v); },
  };
  /* document.cookie ACCUMULATES, it does not overwrite. A plain string
     property makes the second of two writes erase the first, so a file
     correctly setting both campaign and medium reads back as having set
     only the medium -- a harness artefact that looks exactly like a
     bug in the code under test. */
  const cookieJar = (opts.cookies || []).slice();
  const documentFake = {
    /* Overridable, because the third offer rung reads document.referrer
       and the whole question is what it carries. */
    referrer: 'referrer' in opts ? opts.referrer : THIS_HIT_REF,
    getElementById: () => null,
    get cookie() { return cookieJar.join('; '); },
    set cookie(v) { cookieJar.push(String(v)); },
  };
  const page = opts.page || PAGE;
  const windowFake = { location: {
    href: page,
    search: page.indexOf('?') === -1 ? '' : page.slice(page.indexOf('?')),
    hostname: 'www.gushwork.ai',
  } };

  let posted = null;
  const fetchWithTimeout = (url, opts) => {
    posted = { url, body: JSON.parse(opts.body) };
    return Promise.resolve({ ok: true });
  };

  const formState = {};
  const factory = new Function(
    'formState', 'sessionStorage', 'document', 'window', 'crypto',
    'fetchWithTimeout', 'isRailwayReady', 'RAILWAY_API_URL', 'NET_TIMEOUT_MS',
    'setHidden', 'URLSearchParams',
    /* captureUTMs writes and reads the 30-day campaign cookie since 17
       Sept 2026, so its two helpers have to come across or this factory
       throws and the suite reports a crash rather than a payload. */
    [lift(src, 'initSession'), lift(src, 'captureUTMs'), lift(src, 'saveSession'),
     lift(src, 'rememberCampaign'), lift(src, 'getCookie'),
     /* THE THIRD OFFER RUNG, added 22 Sept 2026. captureUTMs calls it, so
        without it here the factory throws and the suite reports a crash
        instead of a payload -- which is how it announced itself. */
     lift(src, 'offerFromReferrer'),
     (/const OFFER_COOKIE_DAYS = \d+;/.exec(src) || [''])[0],
     'return { initSession, captureUTMs, saveSession };'].join('\n')
  );
  const fns = factory(
    formState, sessionStorage, documentFake, windowFake, { randomUUID: () => 'x' },
    fetchWithTimeout, () => true, 'https://api.example.test', { session: 5000 },
    () => {}, URLSearchParams
  );

  fns.initSession();
  fns.captureUTMs();
  fns.saveSession();
  return { posted, cookie: documentFake.cookie, state: formState };
}

for (const file of ['gushwork-form.js', 'gushwork-form-popup.js']) {
  const tag = file.replace('gushwork-form', 'form').replace('.js', '');
  let p;
  let driven;
  try { driven = drive(file); } catch (err) { ok(`${tag}: drives without throwing`, false, err.message); continue; }
  p = driven.posted;

  ok(`${tag}: posted to /session`, !!p && /\/session$/.test(p.url), p && p.url);
  if (!p) continue;

  /* THE BUG. Absent before this change; the field simply was not sent. */
  ok(`${tag}: utm_term is present in the payload at all`, 'utm_term' in p.body);
  eq(`${tag}: utm_term carries the value from the URL`, p.body.utm_term, 'TERM_VALUE_HERE');

  /* THE NEW FIELD, and the reason it is separate. */
  ok(`${tag}: page_referrer is present in the payload`, 'page_referrer' in p.body);
  eq(`${tag}: page_referrer is THIS hit's document.referrer`, p.body.page_referrer, THIS_HIT_REF);
  eq(`${tag}: referrer is still the first-touch gw_referrer`, p.body.referrer, FIRST_TOUCH_REF);
  ok(`${tag}: the two referrer fields are genuinely different values`,
     p.body.referrer !== p.body.page_referrer,
     `both were ${JSON.stringify(p.body.referrer)}`);

  /* Unchanged fields, so this cannot pass by rewriting the payload. */
  eq(`${tag}: page_url is this page load`, p.body.page_url, PAGE);
  eq(`${tag}: utm_source still sent`, p.body.utm_source, 'facebook');
  eq(`${tag}: utm_campaign still sent`, p.body.utm_campaign, 'CAMP');

  /* THE 30-DAY MEMORY, EXECUTED rather than read. The offer is decided
     from this pair, so a cookie that is never written means a return
     visit is asked again which product they came for. Asserted on what
     document.cookie actually received, because a rememberCampaign that
     silently threw would leave every source assertion above intact. */
  ok(`${tag}: the campaign was written to a cookie`,
     /gw_utm_campaign=CAMP/.test(driven.cookie), driven.cookie);
  ok(`${tag}: the medium was written beside it`,
     /gw_utm_medium=paid/.test(driven.cookie), driven.cookie);
  ok(`${tag}: the cookie lasts 30 days`,
     /max-age=2592000/.test(driven.cookie), driven.cookie);

  /* ATTRIBUTION MUST NOT MOVE. The offer remembers; utm_campaign does
     not. Folding the cookie into the attribution column would have
     silently re-attributed 40 real leads from organic to paid, and left
     them carrying a paid campaign beside an EMPTY utm_source -- which is
     the field Source_Bucket__c reads, so Salesforce and this column
     would have disagreed about the same lead. Caught before shipping;
     pinned here so it cannot come back. */
  eq(`${tag}: offer_campaign is sent`, driven.state.offer_campaign, 'CAMP');
  eq(`${tag}: offer_medium is sent`,   driven.state.offer_medium,   'paid');
  ok(`${tag}: /session is NOT given the offer fields`,
     !('offer_campaign' in p.body) && !('offer_medium' in p.body),
     Object.keys(p.body).join(','));
}

/* ── THE RETURN VISIT, DRIVEN ─────────────────────────────────────
   A clean URL, no campaign in this session, and a 30-day cookie left by
   an ad click three weeks ago. THIS is the only shape where the two
   fields differ, so it is the only shape that can catch the cookie
   leaking back into the attribution column -- and every case above
   carries a campaign in the URL, which is why putting it back survived
   a full mutation run.

   What must be true: the offer still remembers, and THIS visit is still
   recorded as having arrived with nothing. Folding them together would
   re-attribute 40 real leads from organic to paid, and pair a paid
   campaign with an empty utm_source, which is the field
   Source_Bucket__c reads. */
for (const file of ['gushwork-form.js', 'gushwork-form-popup.js']) {
  const tag = file.replace('gushwork-form', 'form').replace('.js', '');
  let d;
  try {
    d = drive(file, {
      page: 'https://www.gushwork.ai/demo',
      cookies: ['gw_utm_campaign=FLI__Prospecting__CRM-Offer__CBO__StartTrial',
                'gw_utm_medium=paid'],
    });
  } catch (err) { ok(`${tag}: return visit drives without throwing`, false, err.message); continue; }

  eq(`${tag}: return visit attributes THIS visit to no campaign`, d.state.utm_campaign, '');
  eq(`${tag}: return visit attributes THIS visit to no medium`,   d.state.utm_medium,   '');
  eq(`${tag}: but the offer still remembers the campaign`,
     d.state.offer_campaign, 'FLI__Prospecting__CRM-Offer__CBO__StartTrial');
  eq(`${tag}: and the medium beside it`, d.state.offer_medium, 'paid');
  eq(`${tag}: /session is still told there was no campaign`,
     d.posted.body.utm_campaign, '');
  /* And nothing re-stamps the cookie on a visit that carried no
     campaign -- otherwise one ad click renews itself forever. */
  ok(`${tag}: a clean visit does not refresh the cookie`,
     !/max-age/.test(d.cookie), d.cookie);
}

/* Both files must send the SAME key set -- the fork has silently drifted
   before, and a payload that differs between them splits the data by
   which page the visitor happened to land on. */
try {
  const a = Object.keys(drive('gushwork-form.js').posted.body).sort();
  const b = Object.keys(drive('gushwork-form-popup.js').posted.body).sort();
  eq('both files send an identical key set', a.join(','), b.join(','));
} catch (err) {
  ok('both files send an identical key set', false, err.message);
}

/* ── COUNTRY FROM THE VISITOR'S IP — EXECUTED, both files ─────────────
   The phone field showed a US flag to a visitor in India. country.is was
   never the problem: checked live on 16 Sept 2026 it answered an Indian IP
   with {"country":"IN"} in 300ms and sends access-control-allow-origin: *.
   initialCountry was hardcoded 'us' and corrected afterwards by setCountry,
   so the field asserted a country nobody had checked and then overrode
   whatever the visitor had picked in the meantime.

   EXECUTED rather than read, because every interesting property here is
   behavioural: what the callback receives, whether it can fire twice,
   whether a failure is cached, and whether it fires at all when the network
   never answers. A source assertion sees a fetch and a callback and can tell
   you none of that. */
function driveLookup(file, opts) {
  const o = opts || {};
  const src = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
  const store = Object.assign({}, o.store || {});
  const sessionStorage = {
    getItem: (k) => { if (o.storageThrows) throw new Error('denied'); return k in store ? store[k] : null; },
    setItem: (k, v) => { if (o.storageThrows) throw new Error('denied'); store[k] = v; },
  };
  const timers = [];
  const setTimeout_ = (fn, ms) => { timers.push({ fn, ms }); return timers.length; };
  const fetch_ = () => (o.fetch ? o.fetch() : Promise.reject(new Error('offline')));

  /* The three constants come from the file too, so the test uses the real
     cache key and the real timeout rather than numbers of its own. */
  const consts = src.split('\n').filter((l) => /^\s*var GEO_[A-Z_]+\s*=/.test(l)).join('\n');
  /* Five now: the three settings plus GEO_VALUE / GEO_WAITING, which memoise
     the answer for the page so the prewarm below can hand it to the real
     caller instantly. They are named GEO_* precisely so this lift keeps
     finding them. */
  ok(`${file}: the geo state and constants were found to lift`, consts.split('\n').length === 5, consts);

  const fn = new Function('fetch', 'sessionStorage', 'setTimeout', 'String',
    consts + '\n' + lift(src, 'geoSettle') + '\n' + lift(src, 'lookupCountry')
    + '\n return lookupCountry;')(fetch_, sessionStorage, setTimeout_, String);

  const got = [];
  fn((cc) => got.push(cc));
  return { got, store, timers, fn, flushTimers: () => timers.forEach((t) => t.fn()) };
}

/* Async because the lookup resolves on a microtask. This is a CommonJS
   file, so a top-level await is a hard error -- the summary moves inside. */
(async () => {
for (const file of ['gushwork-form.js', 'gushwork-form-popup.js']) {
  const tag = file.replace('gushwork-form', 'form').replace('.js', '');

  /* THE BUG, as a test: a visitor in India must get 'in', not 'us'. */
  {
    const r = driveLookup(file, { fetch: () => Promise.resolve({ ok: true, json: async () => ({ ip: '1.2.3.4', country: 'IN' }) }) });
    await new Promise((res) => setImmediate(res));
    eq(`${tag}/geo: an Indian IP resolves to 'in', lowercased`, r.got[0], 'in');
    eq(`${tag}/geo: and it is cached for the rest of the session`, r.store.gw_phone_country, 'in');
    eq(`${tag}/geo: the callback fires exactly once`, r.got.length, 1);
    /* The timeout must not then fire a SECOND callback with the fallback --
       that would drag a correctly-resolved Indian visitor back to the US. */
    r.flushTimers();
    eq(`${tag}/geo: a late timeout cannot override a real answer`, r.got.join(','), 'in');
  }

  /* A cached value answers synchronously and costs no round trip. */
  {
    let called = 0;
    const r = driveLookup(file, { store: { gw_phone_country: 'de' }, fetch: () => { called++; return Promise.reject(new Error('x')); } });
    eq(`${tag}/geo: a cached country answers immediately`, r.got[0], 'de');
    eq(`${tag}/geo: and does not call country.is again`, called, 0);
  }

  /* A failure falls back to 'us' -- and must NOT be cached, or one blip
     pins the whole session to the wrong country. */
  {
    const r = driveLookup(file, { fetch: () => Promise.reject(new Error('offline')) });
    await new Promise((res) => setImmediate(res));
    eq(`${tag}/geo: a failed lookup falls back to 'us'`, r.got[0], 'us');
    eq(`${tag}/geo: a failure is NOT cached`, r.store.gw_phone_country, undefined);
  }

  /* A 200 THAT CARRIES NO COUNTRY. This is the only path that reaches the
     cache write with an empty value, so it is the only thing that can catch
     a cache guard that has been loosened -- caching the fallback here would
     pin the whole session to the US after one malformed response. The
     rejected-fetch case below never reaches that line at all, which is why
     it survived the mutation run on its own. */
  {
    const r = driveLookup(file, { fetch: () => Promise.resolve({ ok: true, json: async () => ({ ip: '1.2.3.4' }) }) });
    await new Promise((res) => setImmediate(res));
    eq(`${tag}/geo: a 200 with no country still falls back to 'us'`, r.got[0], 'us');
    eq(`${tag}/geo: and that fallback is NOT written to the cache`, r.store.gw_phone_country, undefined);
  }

  /* An HTTP error is a failure, not a country. */
  {
    const r = driveLookup(file, { fetch: () => Promise.resolve({ ok: false, status: 503, json: async () => ({}) }) });
    await new Promise((res) => setImmediate(res));
    eq(`${tag}/geo: a 503 falls back rather than resolving to nothing`, r.got[0], 'us');
  }

  /* THE REASON THE TIMEOUT EXISTS. With initialCountry 'auto', a callback
     that never arrives leaves the field with NO country at all -- worse than
     the wrong one. A hung request must still produce a flag. */
  {
    const r = driveLookup(file, { fetch: () => new Promise(() => {}) });
    eq(`${tag}/geo: a hung lookup has not answered yet`, r.got.length, 0);
    r.flushTimers();
    eq(`${tag}/geo: but the timeout guarantees a country`, r.got[0], 'us');
    eq(`${tag}/geo: and still only once`, r.got.length, 1);
  }

  /* Privacy modes throw on sessionStorage access outright. */
  {
    const r = driveLookup(file, { storageThrows: true, fetch: () => Promise.resolve({ ok: true, json: async () => ({ country: 'FR' }) }) });
    await new Promise((res) => setImmediate(res));
    eq(`${tag}/geo: an unusable sessionStorage does not break the lookup`, r.got[0], 'fr');
  }

  /* MEMOISED FOR THE PAGE. Two callers must share one lookup: the prewarm
     fires at page parse and the real caller arrives later, after three serial
     script loads. If the second caller re-fetched, the prewarm would buy
     nothing. */
  {
    let calls = 0;
    const r = driveLookup(file, { fetch: () => { calls++; return Promise.resolve({ ok: true, json: async () => ({ country: 'IN' }) }); } });
    const second = [];
    r.fn(function (cc) { second.push(cc); });        // arrives while in flight
    await new Promise((res) => setImmediate(res));
    eq(`${tag}/geo: a second caller shares the in-flight lookup, no second fetch`, calls, 1);
    eq(`${tag}/geo: and both callers get the same answer`, r.got[0] + ',' + second[0], 'in,in');
    /* A caller arriving AFTER it resolved gets the memoised value with no
       fetch at all -- this is the path the real phone input takes once the
       prewarm has landed. */
    const third = [];
    r.fn(function (cc) { third.push(cc); });
    eq(`${tag}/geo: a later caller is answered from memory, synchronously`, third[0], 'in');
    eq(`${tag}/geo: and still no extra fetch`, calls, 1);
  }

  /* THE PREWARM ITSELF. Without this call the lookup does not begin until
     three serial script loads have finished, which is the whole bug. It is a
     bare statement at module level, so only source can pin it. */
  {
    const src2 = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
    ok(`${tag}/geo: the lookup is PREWARMED at parse time, not left to init`,
       /\n\s*lookupCountry\(function \(\) \{\}\);/.test(src2));
    /* It must fire BEFORE the script chain, or it is not a prewarm. */
    ok(`${tag}/geo: and the prewarm runs before the jQuery load starts`,
       src2.indexOf('lookupCountry(function () {});') < src2.indexOf("loadScript('https://cdnjs.cloudflare.com/ajax/libs/jquery"));
  }

  /* The options the library is actually given. Behaviour above proves the
     lookup; these two lines are what wire it in, and 'us' coming back would
     restore the whole defect. */
  {
    const src = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
    const opts = src.slice(src.indexOf('window.intlTelInput(input, {'), src.indexOf('input._iti = iti;'));
    ok(`${tag}/geo: initialCountry is 'auto', never a hardcoded country`,
       /initialCountry: 'auto'/.test(opts) && !/initialCountry: 'us'/.test(opts));
    ok(`${tag}/geo: geoIpLookup is wired to the real lookup`,
       /geoIpLookup: lookupCountry/.test(opts));
    /* The override race: setCountry must not be called behind the visitor. */
    ok(`${tag}/geo: nothing calls setCountry after init any more`,
       !/\.setCountry\(/.test(src));
  }
}

/* ── THE THIRD OFFER RUNG: an internal referrer that still has the
      campaign ───────────────────────────────────────────────────────

   WHY IT EXISTS. The cookie above it has never fired once in production
   -- 0 of 207 leads since the campaign rule shipped, including all 14
   whose campaign was provably lost in transit, which is the exact case
   it was written for. Whatever Meta's in-app browser does to storage
   between webviews takes the cookie with it. document.referrer survives,
   because the browser supplies it.

   EXECUTED, not read: captureUTMs is driven for real and the resulting
   payload inspected, because the thing that matters is which of the
   three rungs wins and what ends up in WHICH column. */
for (const file of ['gushwork-form.js', 'gushwork-form-popup.js']) {
  const tag = file.replace('gushwork-form', 'form').replace('.js', '');
  const BARE = 'https://www.gushwork.ai/demo';                 // no UTMs at all
  const AD_REF = 'https://www.gushwork.ai/start?utm_source=facebook&utm_medium=paid&utm_campaign=FLI__Prospecting__CreativeTesting__CBO';

  /* The exact shape of the 22 Sept lead: bare landing page, session
     wiped, campaign only in the referrer. */
  const lost = drive(file, { page: BARE, referrer: AD_REF,
                             session: { gw_referrer: '', gw_utm_campaign: '', gw_utm_medium: '' } });
  ok(`${tag}/offer: an internal referrer supplies the offer campaign`,
     lost.state.offer_campaign === 'FLI__Prospecting__CreativeTesting__CBO',
     JSON.stringify(lost.state.offer_campaign));
  ok(`${tag}/offer: and its medium, from the SAME referrer`,
     lost.state.offer_medium === 'paid', JSON.stringify(lost.state.offer_medium));

  /* THE INVARIANT THAT MATTERS MOST. utm_campaign answers "which campaign
     brought THIS visit". Feeding a remembered value into it would
     re-attribute organic leads to paid and desync Source_Bucket__c --
     the comment in the file says so at length, and this is that comment
     as an assertion. */
  ok(`${tag}/offer: utm_campaign is NOT polluted by the referrer`,
     !lost.state.utm_campaign, JSON.stringify(lost.state.utm_campaign));
  ok(`${tag}/offer: utm_medium is NOT polluted either`,
     !lost.state.utm_medium, JSON.stringify(lost.state.utm_medium));

  /* AN OUTSIDE PAGE MUST NOT DECIDE WHAT WE SELL. Anyone can put
     utm_campaign=crm on a link to us. */
  const external = drive(file, { page: BARE,
                                 referrer: 'https://evil.example.com/?utm_campaign=crm-offer&utm_medium=paid',
                                 session: { gw_referrer: '', gw_utm_campaign: '', gw_utm_medium: '' } });
  ok(`${tag}/offer: an EXTERNAL referrer is ignored`,
     !external.state.offer_campaign, JSON.stringify(external.state.offer_campaign));
  /* And a lookalike host, which an includes() test would wave through. */
  const lookalike = drive(file, { page: BARE,
                                  referrer: 'https://gushwork.ai.evil.com/?utm_campaign=crm-offer',
                                  session: { gw_referrer: '', gw_utm_campaign: '', gw_utm_medium: '' } });
  ok(`${tag}/offer: a lookalike host is ignored`,
     !lookalike.state.offer_campaign, JSON.stringify(lookalike.state.offer_campaign));

  /* PRECEDENCE. The referrer is the LAST resort: this visit's own URL
     wins, and the cookie wins over the referrer. */
  const urlWins = drive(file, { referrer: AD_REF });   // PAGE carries utm_campaign=CAMP
  ok(`${tag}/offer: this visit's own URL outranks the referrer`,
     urlWins.state.offer_campaign === 'CAMP', JSON.stringify(urlWins.state.offer_campaign));
  const cookieWins = drive(file, { page: BARE, referrer: AD_REF,
                                   cookies: ['gw_utm_campaign=FROM_COOKIE', 'gw_utm_medium=cpc'],
                                   session: { gw_referrer: '', gw_utm_campaign: '', gw_utm_medium: '' } });
  ok(`${tag}/offer: the cookie outranks the referrer`,
     cookieWins.state.offer_campaign === 'FROM_COOKIE',
     JSON.stringify(cookieWins.state.offer_campaign));

  /* No referrer at all must be inert, not a throw -- this runs during
     init, before the form exists. */
  const none = drive(file, { page: BARE, referrer: '',
                             session: { gw_referrer: '', gw_utm_campaign: '', gw_utm_medium: '' } });
  ok(`${tag}/offer: no referrer is inert`, !none.state.offer_campaign,
     JSON.stringify(none.state.offer_campaign));
  const junk = drive(file, { page: BARE, referrer: 'not a url',
                             session: { gw_referrer: '', gw_utm_campaign: '', gw_utm_medium: '' } });
  ok(`${tag}/offer: an unparseable referrer does not throw`,
     !junk.state.offer_campaign, JSON.stringify(junk.state.offer_campaign));
}

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
