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

function drive(file) {
  const src = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');

  const store = {
    gw_referrer: FIRST_TOUCH_REF,
    gw_landing_page: 'https://www.gushwork.ai/start',
    gw_session_id: 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',
  };
  const sessionStorage = {
    getItem: (k) => (k in store ? store[k] : null),
    setItem: (k, v) => { store[k] = String(v); },
  };
  const documentFake = { referrer: THIS_HIT_REF, getElementById: () => null, cookie: '' };
  const windowFake   = { location: { href: PAGE, search: PAGE.slice(PAGE.indexOf('?')) } };

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
    [lift(src, 'initSession'), lift(src, 'captureUTMs'), lift(src, 'saveSession'),
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
  return posted;
}

for (const file of ['gushwork-form.js', 'gushwork-form-popup.js']) {
  const tag = file.replace('gushwork-form', 'form').replace('.js', '');
  let p;
  try { p = drive(file); } catch (err) { ok(`${tag}: drives without throwing`, false, err.message); continue; }

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
}

/* Both files must send the SAME key set -- the fork has silently drifted
   before, and a payload that differs between them splits the data by
   which page the visitor happened to land on. */
try {
  const a = Object.keys(drive('gushwork-form.js').body).sort();
  const b = Object.keys(drive('gushwork-form-popup.js').body).sort();
  eq('both files send an identical key set', a.join(','), b.join(','));
} catch (err) {
  ok('both files send an identical key set', false, err.message);
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
