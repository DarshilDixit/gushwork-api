/* ============================================================
   Batch 1 verification.

   Reads the REAL functions out of index.js rather than a copy, in line
   with the existing convention: a test that exercises a duplicate of the
   source can pass while production is broken.

   Run:  node tests/test-batch1.js
   ============================================================ */

const fs = require('fs');
const path = require('path');
const src = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');

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

/* ── Lift functions + constants out of the shipped source ─────── */
function lift(startMarker, endMarker) {
  const i = src.indexOf(startMarker);
  if (i === -1) throw new Error('marker not found: ' + startMarker);
  const j = src.indexOf(endMarker, i);
  if (j === -1) throw new Error('end marker not found: ' + endMarker);
  return src.slice(i, j);
}

const clean = [
  src.slice(src.indexOf('const REDIRECT_WRAPPERS = {'), src.indexOf('/* ── Bot wall detection')),
  src.slice(src.indexOf('function detectCheckWall(html)'), src.indexOf('function isPrivateOrLocalHost')),
  src.slice(src.indexOf('const WEBSITE_VERIFIED_REASONS = ['), src.indexOf('// Reasons that mean')),
  src.slice(src.indexOf('const RECHECK_WRITEABLE = ['), src.indexOf('/* Verdicts the recheck must NEVER')),
].join('\n');

const load = new Function(clean + `
  return { unwrapRedirectUrl, detectCheckWall, REDIRECT_WRAPPERS,
           WEBSITE_VERIFIED_REASONS, RECHECK_WRITEABLE };
`);
const M = load();

/* ============================================================
   1. fbc truncation
   ============================================================ */
{
  const slices = src.match(/req\.body\.fbc\s*\|\|\s*''\)\.toString\(\)\.trim\(\)\.slice\(0, (\d+)\)/g) || [];
  eq('fbc: exactly two capture sites', slices.length, 2);
  ok('fbc: both raised to 1000', slices.every((s) => s.includes('slice(0, 1000)')), slices.join(' | '));

  // A long-but-plausible fbc survives the new cap.
  const fbclid = 'IwAR' + 'x'.repeat(520);
  const fbc = 'fb.1.' + Date.now() + '.' + fbclid;
  ok('fbc: test value exceeds the old cap', fbc.length > 500, String(fbc.length));
  eq('fbc: survives the new cap', fbc.slice(0, 1000), fbc);
  ok('fbc: would have been cut by the old cap', fbc.slice(0, 500) !== fbc);

  // The field that ACTUALLY truncates in production: the landing URL. An
  // ads click carries the fbclid plus a full UTM set, and 500 characters is
  // not much for that. Reconstructing fbc from a cut URL is the documented
  // failure mode, so these four fields matter as much as fbc itself.
  const urlSlices = src.match(/req\.body\.(?:page_url|referrer|landing_page|previous_page)\s*\|\| ''\)\.toString\(\)\.trim\(\)\.slice\(0, (\d+)\)/g) || [];
  eq('urls: eight capture sites (four fields x two routes)', urlSlices.length, 8);
  ok('urls: all raised to 1000', urlSlices.every((u) => u.includes('slice(0, 1000)')), urlSlices.join(' | '));

  const adsUrl = 'https://www.gushwork.ai/lp/seo-services?utm_source=facebook&utm_medium=paid&utm_campaign=q3-b2b-saas-prospecting-broad&utm_content=carousel-variant-b-founder-testimonial&utm_term=seo+agency+for+saas&fbclid=' + 'A'.repeat(300);
  ok('urls: a realistic ads landing URL exceeds the old cap', adsUrl.length > 500, String(adsUrl.length));
  ok('urls: fbclid survives the new cap', adsUrl.slice(0, 1000).includes('fbclid=' + 'A'.repeat(300)));
  ok('urls: fbclid was being cut by the old cap', !adsUrl.slice(0, 500).includes('fbclid=' + 'A'.repeat(300)));
}

/* ============================================================
   2. Bot wall detection (SiteGround captcha)
   ============================================================ */
{
  // The exact signature from the handover — 168 bytes, status 202.
  const sg = '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F&y=ipr:1.2.3.4"></head><body></body></html>';
  eq('wall: SiteGround captcha detected', M.detectCheckWall(sg), 'sgcaptcha');

  // Same wall on a large page still detected via the path rule.
  const sgBig = '<html>' + 'x'.repeat(5000) + '<meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F">' + '</html>';
  eq('wall: SiteGround detected regardless of body size', M.detectCheckWall(sgBig), 'sgcaptcha');

  // Generic tiny meta-refresh stub.
  const stub = '<html><head><meta http-equiv="refresh" content="0;url=/somewhere"></head></html>';
  eq('wall: tiny meta-refresh stub detected', M.detectCheckWall(stub), 'meta_refresh_stub');

  // A REAL page that happens to contain a meta refresh must NOT match.
  const realPage = '<html><head><title>Marathon Technology</title>'
    + '<meta http-equiv="refresh" content="600">'
    + '</head><body>' + 'Real company content. '.repeat(200) + '</body></html>';
  eq('wall: large real page with a refresh tag is not a wall', M.detectCheckWall(realPage), null);

  // Ordinary pages.
  eq('wall: normal page returns null', M.detectCheckWall('<html><body>Hello world</body></html>'), null);
  eq('wall: empty input returns null', M.detectCheckWall(''), null);
  eq('wall: null input returns null', M.detectCheckWall(null), null);
  eq('wall: undefined input returns null', M.detectCheckWall(undefined), null);

  // Attribute-order and quoting variations seen in the wild.
  eq('wall: single quotes', M.detectCheckWall("<meta content='0;/.well-known/sgcaptcha/?r=1' http-equiv='refresh'>"), 'sgcaptcha');
  eq('wall: unquoted http-equiv', M.detectCheckWall('<meta http-equiv=refresh content="0;/.well-known/sgcaptcha/">'), 'sgcaptcha');
  eq('wall: uppercase tag', M.detectCheckWall('<META HTTP-EQUIV="REFRESH" CONTENT="0;/.well-known/SGCAPTCHA/">'), 'sgcaptcha');

  // The boundary: 1024 bytes is the stub ceiling.
  const metaTag = '<meta http-equiv="refresh" content="0;url=/x">';
  const justUnder = metaTag + ' '.repeat(1024 - metaTag.length);
  eq('wall: body length is exactly the ceiling', justUnder.length, 1024);
  ok('wall: 1024-byte body is still a stub', M.detectCheckWall(justUnder) === 'meta_refresh_stub', String(justUnder.length));
  const justOver = '<meta http-equiv="refresh" content="0;url=/x">' + ' '.repeat(1200);
  eq('wall: body over the ceiling is not a stub', M.detectCheckWall(justOver), null);
}

/* ============================================================
   3. check_blocked wiring — must not change Meta behaviour
   ============================================================ */
{
  ok('check_blocked: is VERIFIED (Meta behaviour unchanged vs thin_content)',
     M.WEBSITE_VERIFIED_REASONS.includes('check_blocked'));
  ok('check_blocked: thin_content still verified (no regression)',
     M.WEBSITE_VERIFIED_REASONS.includes('thin_content'));
  ok('check_blocked: NOT writeable by the recheck tool',
     !M.RECHECK_WRITEABLE.includes('check_blocked'));
  ok('check_blocked: returns ok:true in evaluateWebsite',
     /reason: 'check_blocked'/.test(src) && /ok: true, reason: 'check_blocked'/.test(src));
  ok('check_blocked: has a plain-English label',
     /check_blocked:\s*'Site blocked our check/.test(src));
  ok('check_blocked: tested before analyzeSubstance',
     src.indexOf('const wall = detectCheckWall(rawHtml)') < src.indexOf('const sub = analyzeSubstance(rawHtml, finalHost)'));
}

/* ============================================================
   4. Search-result URL unwrapping
   ============================================================ */
{
  // The real lead from the recheck table.
  const google = 'https://www.google.com/url?q=https://phpagency.com/&sa=u&ved=2ahukewjqxrrbrfivaxu4msyfhvvvi38qfnoecb4qaq&usg=aovvaw0cfnvqezwhroaqz8lqg3xi';
  eq('unwrap: real Google redirect from the recheck table',
     M.unwrapRedirectUrl(google), 'https://phpagency.com/');

  eq('unwrap: google.com without www', M.unwrapRedirectUrl('https://google.com/url?q=https://acme.io/'), 'https://acme.io/');
  eq('unwrap: facebook link shim', M.unwrapRedirectUrl('https://l.facebook.com/l.php?u=https://acme.io/&h=abc'), 'https://acme.io/');

  // Idempotency — this function runs twice on the same value by design.
  const once = M.unwrapRedirectUrl(google);
  eq('unwrap: idempotent (safe to apply twice)', M.unwrapRedirectUrl(once), once);

  // Must NEVER rewrite an ordinary address.
  eq('unwrap: plain domain untouched', M.unwrapRedirectUrl('phpagency.com'), 'phpagency.com');
  eq('unwrap: plain https URL untouched', M.unwrapRedirectUrl('https://acme.io/about'), 'https://acme.io/about');
  eq('unwrap: bare www untouched', M.unwrapRedirectUrl('www.acme.io'), 'www.acme.io');

  // A real Google-owned company site must not be mangled.
  eq('unwrap: google.com homepage untouched', M.unwrapRedirectUrl('https://google.com'), 'https://google.com');
  eq('unwrap: google.com with an unrelated path untouched',
     M.unwrapRedirectUrl('https://www.google.com/maps'), 'https://www.google.com/maps');

  // Hostile / malformed input must fall through, never throw.
  eq('unwrap: q= present but not a URL', M.unwrapRedirectUrl('https://www.google.com/url?q=notaurl'), 'https://www.google.com/url?q=notaurl');
  eq('unwrap: q= with a javascript: payload is refused',
     M.unwrapRedirectUrl('https://www.google.com/url?q=javascript:alert(1)'), 'https://www.google.com/url?q=javascript:alert(1)');
  eq('unwrap: q= with a data: payload is refused',
     M.unwrapRedirectUrl('https://www.google.com/url?q=data:text/html,<script>'), 'https://www.google.com/url?q=data:text/html,<script>');
  eq('unwrap: q= with file: is refused',
     M.unwrapRedirectUrl('https://www.google.com/url?q=file:///etc/passwd'), 'https://www.google.com/url?q=file:///etc/passwd');
  eq('unwrap: empty string', M.unwrapRedirectUrl(''), '');
  eq('unwrap: null', M.unwrapRedirectUrl(null), '');
  eq('unwrap: undefined', M.unwrapRedirectUrl(undefined), '');
  eq('unwrap: garbage', M.unwrapRedirectUrl('!!!not a url!!!'), '!!!not a url!!!');

  ok('unwrap: applied in serverSideWebsiteCheck',
     /const raw = unwrapRedirectUrl\(String\(website \|\| ''\)\.trim\(\)\)/.test(src));
  ok('unwrap: applied in evaluateWebsite (covers /verify-website)',
     /const raw = unwrapRedirectUrl\(rawInput\)/.test(src));
}

/* ============================================================
   5. dns_unresolved split
   ============================================================ */
{
  // Rebuild the exact decision from source so the test tracks the shipped rule.
  const seg = src.slice(src.indexOf("const NO_ANSWER = ["), src.indexOf("return out;", src.indexOf("const NO_ANSWER = [")));
  const decide = new Function('apexCode', 'wwwCode', `
    const out = {};
    ${seg.replace(/out\.status/g, 'out.status')}
    return out.status;
  `);

  eq('dns: ENOTFOUND on both  -> nxdomain',        decide('ENOTFOUND', 'ENOTFOUND'), 'nxdomain');
  eq('dns: ENODATA on both    -> no_dns_records',  decide('ENODATA', 'ENODATA'), 'no_dns_records');
  eq('dns: ENODATA + ENOTFOUND-> no_dns_records',  decide('ENODATA', 'ENOTFOUND'), 'no_dns_records');
  eq('dns: SERVFAIL on both   -> dns_unresolved',  decide('ESERVFAIL', 'ESERVFAIL'), 'dns_unresolved');
  eq('dns: SERVFAIL + NOTFOUND-> dns_unresolved',  decide('ENOTFOUND', 'ESERVFAIL'), 'dns_unresolved');
  eq('dns: timeout            -> dns_unresolved',  decide('ETIMEOUT', 'ETIMEOUT'), 'dns_unresolved');
  eq('dns: ETIMEDOUT variant  -> dns_unresolved',  decide('ETIMEDOUT', 'ETIMEDOUT'), 'dns_unresolved');
  eq('dns: EAI_AGAIN          -> dns_unresolved',  decide('EAI_AGAIN', 'EAI_AGAIN'), 'dns_unresolved');
  eq('dns: EREFUSED           -> dns_unresolved',  decide('EREFUSED', 'EREFUSED'), 'dns_unresolved');

  // gslgraphics.com — real resolver output captured from Node.
  eq('dns: gslgraphics.com (live NS, no A record) -> no_dns_records',
     decide('ENODATA', 'ENOTFOUND'), 'no_dns_records');

  ok('dns_unresolved: fails OPEN', /ok: true,  reason: 'dns_unresolved'/.test(src));
  ok('dns_unresolved: NOT writeable by the recheck tool', !M.RECHECK_WRITEABLE.includes('dns_unresolved'));
  ok('dns_unresolved: NOT counted as verified (Meta stays suppressed)',
     !M.WEBSITE_VERIFIED_REASONS.includes('dns_unresolved'));
  ok('dns_unresolved: has a plain-English label', /dns_unresolved:\s*'Could not look up the domain/.test(src));
  ok('no_dns_records: still returns ok:false (unchanged)', /ok: false, reason: 'no_dns_records'/.test(src));
}

/* ============================================================
   6. Booking integrity check on all three paths
   ============================================================ */
{
  const calls = src.match(/await alertIfBookingWithoutSubmit\(/g) || [];
  eq('booking: three call sites', calls.length, 3);
  ok('booking: /booking-confirmed', /alertIfBookingWithoutSubmit\(session_id, '\/booking-confirmed'\)/.test(src));
  ok('booking: /cal-webhook',       /alertIfBookingWithoutSubmit\(lead\.session_id, '\/cal-webhook'\)/.test(src));
  ok('booking: /rh-webhook',        /alertIfBookingWithoutSubmit\(lead\.session_id, '\/rh-webhook'\)/.test(src));
  ok('booking: old inline copy removed', !/pre-booking integrity check failed \(ignored\):', err\.message\);\n    \}/.test(src));
  ok('booking: uses submitted_at, not completed',
     /SELECT email, step_reached, submitted_at, website, website_check_reason FROM leads WHERE session_id=\$1/.test(src));

  // The check must run BEFORE the UPDATE that sets completed=true, or it
  // would be reading state the booking itself created.
  const calIdx = src.indexOf("alertIfBookingWithoutSubmit(lead.session_id, '/cal-webhook')");
  const calUpd = src.indexOf('UPDATE leads SET booking_uid=$2,start_time=$3,end_time=$4', calIdx - 400);
  ok('booking: cal check runs before its UPDATE', calIdx < src.indexOf('UPDATE leads SET booking_uid=$2,start_time=$3,end_time=$4,event_type=$5,completed=true', calIdx));
  const rhIdx = src.indexOf("alertIfBookingWithoutSubmit(lead.session_id, '/rh-webhook')");
  ok('booking: rh check runs before its UPDATE', rhIdx < src.indexOf('UPDATE leads SET booking_uid=$2,start_time=$3,event_type=$4,completed=true', rhIdx));
  ok('booking: helper swallows its own errors', /pre-booking integrity check failed \(ignored\)/.test(src));
}

/* ============================================================
   7. /session persistence
   ============================================================ */
{
  ok('session: writes to form_sessions', /INSERT INTO form_sessions/.test(src));
  ok('session: does NOT write to leads',
     !/INSERT INTO leads[\s\S]{0,400}form_sessions/.test(src));
  ok('session: replies before writing (no added page-load latency)',
     src.indexOf("res.json({ ok: true });", src.indexOf("app.post('/session'")) <
     src.indexOf('INSERT INTO form_sessions'));
  ok('session: upsert on repeat calls', /ON CONFLICT \(session_id\) DO UPDATE SET[\s\S]{0,80}hits\s*=\s*form_sessions\.hits \+ 1/.test(src));
  ok('session: captures user agent from the header', /s\(req\.headers\['user-agent'\], 500\)/.test(src));
  ok('session: write failure is swallowed', /\[\/session\] visit not recorded \(ignored\)/.test(src));

  const dbsrc = fs.readFileSync(path.join(__dirname, '..', 'db.js'), 'utf8');
  ok('db: form_sessions table created', /CREATE TABLE IF NOT EXISTS form_sessions/.test(dbsrc));
  ok('db: form_sessions creation is non-fatal', /Form-sessions table init FAILED \(non-fatal\)/.test(dbsrc));
  ok('db: session_id is unique', /session_id\s+TEXT UNIQUE NOT NULL/.test(dbsrc));
  ok('db: created_at is indexed', /form_sessions_created_idx/.test(dbsrc));
  ok('db: leads table definition untouched',
     dbsrc.includes('CREATE TABLE IF NOT EXISTS leads (') && dbsrc.includes('session_id     UUID UNIQUE NOT NULL'));

  ok('funnel: read route exists', /app\.get\('\/monitor\/funnel'/.test(src));
  ok('funnel: token-gated', /app\.get\('\/monitor\/funnel'[\s\S]{0,200}MONITOR_TOKEN/.test(src));
  ok('funnel: reports when tracking started', /session_tracking_live_since/.test(src));
  ok('funnel: null rate when there is no denominator', /sessions > 0 \? \+\(step1 \/ sessions \* 100\)\.toFixed\(1\) : null/.test(src));
}

/* ============================================================
   8. Regression guards — things that must NOT have changed
   ============================================================ */
{
  // Blocking is decided in gushwork-form.js. index.js must not have grown a
  // blocking list of its own — every new verdict here fails open.
  ok('regress: index.js defines no blocking list',
     !/const WEBSITE_BLOCKING_REASONS\s*=/.test(src));
  ok('regress: both new verdicts fail open',
     /ok: true, reason: 'check_blocked'/.test(src) && /ok: true,  reason: 'dns_unresolved'/.test(src));
  ok('regress: thin_content still fires Meta', M.WEBSITE_VERIFIED_REASONS.includes('thin_content'));
  ok('regress: social_profile_url still suppressed', !M.WEBSITE_VERIFIED_REASONS.includes('social_profile_url'));
  ok('regress: parked_confirmed still suppressed', !M.WEBSITE_VERIFIED_REASONS.includes('parked_confirmed'));
  ok('regress: nxdomain still writeable by recheck', M.RECHECK_WRITEABLE.includes('nxdomain'));
  ok('regress: no_dns_records still writeable by recheck', M.RECHECK_WRITEABLE.includes('no_dns_records'));
  ok('regress: RECHECK_PROTECTED untouched', /const RECHECK_PROTECTED = \['brand_mismatch', 'mailbox_domain', 'social_profile_url', 'test_email_skipped', 'unparseable'\]/.test(src));
  ok('regress: Salesforce fbc cap left at 255 on purpose',
     /fbc: 255/.test(fs.readFileSync(path.join(__dirname, '..', 'salesforce.js'), 'utf8')));

  // The dashboard label map is a JS string; it must still evaluate cleanly
  // and every new reason must be present in BOTH copies.
  const i = src.indexOf("'var WLBL=");
  const end = src.indexOf("};'", i);
  const emitted = eval(src.slice(i, end + 3));
  const WLBL = (new Function(emitted + '; return WLBL;'))();
  for (const r of ['check_blocked', 'dns_unresolved', 'thin_content', 'nxdomain', 'mx_only']) {
    ok(`dashboard: label present for ${r}`, typeof WLBL[r] === 'string' && WLBL[r].length > 0);
    ok(`dashboard: ${r} label has no stray escape`, !WLBL[r].includes('\\u'));
  }
  const serverLabels = src.slice(src.indexOf('const WEBSITE_REASON_LABELS = {'), src.indexOf('// SEO is the product'));
  for (const r of ['check_blocked', 'dns_unresolved']) {
    ok(`labels: ${r} in the server-side map too`, serverLabels.includes(r + ':'));
  }
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
