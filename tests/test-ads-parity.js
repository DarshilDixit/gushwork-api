/* ============================================================
   Ads-form parity — gushwork-form-popup.js against gushwork-form.js.

   gushwork-form-popup.js is a FORK of gushwork-form.js. It exists only to
   present the booking step as a fullscreen modal opened after step 2; every
   other line is meant to be the same code. It forked at /demo v5.3.0 on
   14 Aug and silently missed v5.6.0 and v5.7.0/v5.7.1, which is what this
   file is here to stop happening again.

   Same convention as the other suites: the real code is LIFTED out of both
   shipped files and evaluated, rather than copied here. A test that
   exercises a duplicate of the source can pass while production is broken.

   Two kinds of assertion, deliberately:

     - EQUIVALENCE. Where a unit is pure, it is lifted from BOTH files and
       run against the same inputs. The demo answer is the expected value,
       so drift in either file fails here rather than in production.
     - SHAPE. The DNS fallback needs a DOM and a network, so it is asserted
       by shape — which construct sits at which site. The failure mode that
       actually ships is the branch moving or a guard being dropped, and
       that is visible in the text.

   The modal is NOT under test for sameness. Section 6 asserts the opposite:
   that the Ads file still has its own booking presentation.

   Dependency-free — no DATABASE_URL, no network, no DOM.

   Run:  node tests/test-ads-parity.js
   ============================================================ */

const fs = require('fs');
const path = require('path');
const demo  = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form.js'), 'utf8');
const popup = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form-popup.js'), 'utf8');

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

/* Lift a top-level function by brace-matching forward from its signature,
   so the body is taken whole however long it is. */
function liftFn(src, name) {
  const re = new RegExp('\\n    (?:async )?function ' + name + '\\s*\\(');
  const m = re.exec(src);
  if (!m) throw new Error('function not found: ' + name);
  let depth = 0, start = src.indexOf('{', m.index);
  for (let j = start; j < src.length; j++) {
    if (src[j] === '{') depth++;
    else if (src[j] === '}') { depth--; if (depth === 0) return src.slice(m.index, j + 1); }
  }
  throw new Error('unbalanced braces in: ' + name);
}
function liftConst(src, name) {
  let m = new RegExp('\\n    const ' + name + ' = \\[[\\s\\S]*?\\n    \\];').exec(src)
       || new RegExp('\\n    const ' + name + ' = \\{[\\s\\S]*?\\n    \\};').exec(src);
  if (!m) throw new Error('const not found: ' + name);
  return m[0];
}

/* ============================================================
   0. PRESENCE — is each ported unit even here?

   Checked first, and reported as named assertions. Without this the lift
   below throws an uncaught "function not found", and a stack trace is a
   much worse handover than a line saying which unit went missing. This is
   the section that fires if the Ads file is ever re-forked or reverted.
   ============================================================ */
const REQUIRED_FNS = [
  ['resolveWebsiteViaServer', 'server DNS fallback (v5.7.0)'],
  ['emailInWebsiteField',     'email typed into the website field (v5.7.0)'],
  ['localEmailTypoHint',      'soft email typo nudge (v5.6.0/v5.7.1)'],
  ['typoHintEl',              'soft email typo nudge (v5.6.0/v5.7.1)'],
  ['hideEmailTypoHint',       'soft email typo nudge (v5.6.0/v5.7.1)'],
  ['showEmailTypoHint',       'soft email typo nudge (v5.6.0/v5.7.1)'],
  ['suggestWebsiteDomainFix', 'website typo suggester'],
  ['damerauLevenshtein',      'shared edit-distance helper'],
  ['isValidEmail',            'shared validator'],
  ['isValidURL',              'shared validator'],
  ['capturePartnerStack',     'PartnerStack cookie capture (v5.8.0)'],
];
const REQUIRED_CONSTS = [
  ['SERVER_DNS_DECISIVE',    'server DNS fallback (v5.7.0)'],
  ['PERSONAL_EMAIL_DOMAINS', 'shared provider list'],
  ['WEBSITE_BAD_TLDS',       'website typo suggester'],
];
let missing = 0;
function present(kind, name, feature) {
  const re = kind === 'fn'
    ? new RegExp('\\n    (?:async )?function ' + name + '\\s*\\(')
    : new RegExp('\\n    const ' + name + ' = ');
  const inPopup = re.test(popup), inDemo = re.test(demo);
  ok(`port present: ${name} — ${feature}`, inPopup,
     inDemo ? 'in gushwork-form.js but MISSING from the Ads file' : 'missing from BOTH files');
  if (!inPopup) missing++;
}
for (const [n, f] of REQUIRED_FNS) present('fn', n, f);
for (const [n, f] of REQUIRED_CONSTS) present('const', n, f);

/* Everything below lifts and EXECUTES these units, so there is nothing to
   run if one is absent. Stop here with the named failures above rather
   than throwing a stack trace out of the lift. */
if (missing) {
  console.log('');
  console.log('  FAILURES:');
  failures.forEach((f) => console.log('   ✗ ' + f));
  console.log('');
  console.log(`  ${missing} ported unit(s) missing from gushwork-form-popup.js.`);
  console.log('  The Ads form has fallen behind gushwork-form.js again.');
  console.log('');
  console.log(`  passed: ${pass}`);
  console.log(`  failed: ${fail}`);
  console.log('');
  process.exit(1);
}

/* Build a live module out of each file's own text. */
function build(src) {
  const lifted = [
    liftConst(src, 'PERSONAL_EMAIL_DOMAINS'),
    liftConst(src, 'WEBSITE_BAD_TLDS'),
    liftConst(src, 'SERVER_DNS_DECISIVE'),
    liftFn(src, 'damerauLevenshtein'),
    liftFn(src, 'isValidEmail'),
    liftFn(src, 'isValidURL'),
    liftFn(src, 'emailInWebsiteField'),
    liftFn(src, 'localEmailTypoHint'),
    liftFn(src, 'suggestWebsiteDomainFix'),
  ].join('\n');
  return new Function(lifted + `
    return { PERSONAL_EMAIL_DOMAINS, SERVER_DNS_DECISIVE, damerauLevenshtein,
             isValidEmail, isValidURL, emailInWebsiteField, localEmailTypoHint,
             suggestWebsiteDomainFix };`)();
}

const D = build(demo);
const P = build(popup);

/* ============================================================
   1. The shared vocabulary must be one list, not two
   ============================================================ */
{
  eq('lists: PERSONAL_EMAIL_DOMAINS identical', P.PERSONAL_EMAIL_DOMAINS, D.PERSONAL_EMAIL_DOMAINS);
  eq('lists: SERVER_DNS_DECISIVE identical', P.SERVER_DNS_DECISIVE, D.SERVER_DNS_DECISIVE);

  /* A reason in this list REPLACES the browser's verdict, so anything
     meaning "we could not look" must stay out or a blocked lookup would
     start overriding a good answer with nothing. */
  ok('lists: decisive set excludes doh_error', !P.SERVER_DNS_DECISIVE.includes('doh_error'));
  ok('lists: decisive set excludes dns_indeterminate', !P.SERVER_DNS_DECISIVE.includes('dns_indeterminate'));
  ok('lists: decisive set excludes resolved (stage 1 is not a conclusion)',
     !P.SERVER_DNS_DECISIVE.includes('resolved'));
  ok('lists: decisive set carries the hard negatives',
     ['nxdomain', 'no_dns_records', 'mx_only'].every((r) => P.SERVER_DNS_DECISIVE.includes(r)));

  const url = 'https://gushwork-api-production.up.railway.app';
  ok('lists: both files point at the same API host',
     demo.includes(`RAILWAY_API_URL = '${url}'`) && popup.includes(`RAILWAY_API_URL = '${url}'`));
}

/* ============================================================
   2. emailInWebsiteField — executed, both files, same inputs
   ============================================================ */
{
  const cases = [
    ['chris@chrishennessyteam.com', 'chrishennessyteam.com', 'a business address offers its domain'],
    ['https://user@acme.co.uk',     'acme.co.uk',            'multi-label TLD survives'],
    ['user@www.acme.com',           'acme.com',              'www. is stripped'],
    ['someone@gmail.com',           '',                      'a mailbox provider is not their company site'],
    ['acme.com',                    '',                      'no @ at all is not this branch’s business'],
    ['user@localhost',              '',                      'not domain-shaped'],
    ['@acme.com',                   '',                      'no username, so not an address'],
    ['',                            '',                      'empty'],
  ];
  for (const [input, expected, why] of cases) {
    eq(`emailInWebsite: ${why} (${input || 'empty'})`, P.emailInWebsiteField(input), expected);
    eq(`emailInWebsite: demo agrees (${input || 'empty'})`, P.emailInWebsiteField(input), D.emailInWebsiteField(input));
  }

  /* The whole reason this branch exists: isValidURL ACCEPTS the thing, so
     ordering it after isValidURL would be the same as not having it. */
  ok('emailInWebsite: isValidURL still accepts user@domain.com, which is why order matters',
     P.isValidURL('user@acme.com') === true);
}

/* ============================================================
   3. localEmailTypoHint — executed, both files, same inputs
   ============================================================ */
{
  const cases = [
    ['darshildixit21@gmailc.com', 'darshildixit21@gmail.com', 'the 20 Aug case that ELV timed out on'],
    ['a@hotmial.com',             'a@hotmail.com',            'transposition in a provider'],
    ['bob@outlok.com',            'bob@outlook.com',          'deletion in a provider'],
    ['a@gmail.com',               '',                          'an exact provider is correct as typed'],
    ['bob@acme.com',              '',                          'a business domain is never corrected toward a provider'],
    ['bob@me.com',                '',                          'short providers are excluded'],
    ['bob@we.com',                '',                          'me.com/we.com are unrelated domains, not a typo pair'],
    ['bob@gnailc.com',            '',                          'two edits away is not a confident suggestion'],
    ['bob@gmailcom',              '',                          'a domain with no dot is a different problem'],
    ['a b@gmailc.com',            '',                          'the candidate must itself be a legal address'],
    ['nodomain',                  '',                          'no @ at all'],
  ];
  for (const [input, expected, why] of cases) {
    eq(`typoHint: ${why} (${input})`, P.localEmailTypoHint(input), expected);
    eq(`typoHint: demo agrees (${input})`, P.localEmailTypoHint(input), D.localEmailTypoHint(input));
  }

  /* Advisory only. Every provider in the list must be reachable as a
     SUGGESTION target or the 8-char floor is quietly excluding one that
     matters. */
  const targets = P.PERSONAL_EMAIL_DOMAINS.filter((d) => d.length >= 8);
  ok('typoHint: there are long-enough providers to suggest', targets.length > 0);
  for (const t of targets) {
    // one substitution in the first label
    const typo = 'x@' + t[0] + 'z' + t.slice(2);
    const got = P.localEmailTypoHint(typo);
    ok(`typoHint: ${t} is reachable as a suggestion`, got === 'x@' + t || got === '', got);
  }
}

/* ============================================================
   4. suggestWebsiteDomainFix — the three rules, in order
   ============================================================ */
{
  const cases = [
    // rule 1 — the lead's own business email domain
    ['chrishenenssyteam.com', 'chrishennessyteam.com', '',              'chrishennessyteam.com', 'r1 transposition'],
    ['acme.com',              'gmail.com',             '',              '',                      'r1 needs a business domain'],
    // rule 2 — the local part of a personal address (v5.7.0)
    ['gslgraphics.com',       'gmail.com',             'glsgraphics1',  'glsgraphics.com',       'r2 the changelog case'],
    ['gslgraphics.com',       'gmail.com',             'gls.graphics1', 'glsgraphics.com',       'r2 punctuation is stripped'],
    ['acme.com',              'gmail.com',             'john.smith',    '',                      'r2 nowhere near, stay quiet'],
    ['abce.com',              'gmail.com',             'abcd',          '',                      'r2 local part too short'],
    ['abcdefgh.com',          'gmail.com',             'abcdxygh',      '',                      'r2 two edits is not confident'],
    ['gslgraphics.com',       'acmecorp.com',          'glsgraphics1',  '',                      'r2 must not fire for a business email'],
    // rule 3 — mistyped TLD (was rule 2 before the v5.7.0 port)
    ['acme.con',              '',                      '',              'acme.com',              'r3 .con'],
    ['acme.cmo',              '',                      '',              'acme.com',              'r3 .cmo'],
    ['acme.couk',             '',                      '',              'acme.co.uk',            'r3 multi-label target'],
    ['acme.ner',              '',                      '',              'acme.net',              'r3 .ner'],
    ['acme.com',              '',                      '',              '',                      'r3 a good TLD is left alone'],
    /* Renumbering rule 2 -> rule 3 is exactly the edit that silently
       strands the tail of a chain, so assert rule 3 is still reachable for
       a personal-email lead whose rule 2 found nothing. */
    ['acme.con',              'gmail.com',             'zz',            'acme.com',              'r3 still reachable past rule 2'],
    // precedence
    ['acmecorp.con',          'acmecorp.com',          '',              'acmecorp.com',          'rule 1 beats rule 3'],
  ];
  for (const [d, ed, lp, expected, why] of cases) {
    eq(`suggest: ${why}`, P.suggestWebsiteDomainFix(d, ed, lp), expected);
    eq(`suggest: demo agrees (${why})`, P.suggestWebsiteDomainFix(d, ed, lp), D.suggestWebsiteDomainFix(d, ed, lp));
  }

  /* The signature itself: dropping the third parameter would make rule 2
     dead code that still reads as if it works. */
  ok('suggest: takes the local part as a third parameter',
     /function suggestWebsiteDomainFix\(domain, emailDomain, emailLocalPart\)/.test(popup));
  ok('suggest: the call site actually passes it',
     /suggestWebsiteDomainFix\(domain, emailDomain, emailLocal\)/.test(popup));
  ok('suggest: rules are numbered 1, 2, 3 in source order', (() => {
    const s = popup.slice(popup.indexOf('function suggestWebsiteDomainFix'));
    const i1 = s.indexOf('// 1. The lead'), i2 = s.indexOf("2. The email's LOCAL PART"), i3 = s.indexOf('// 3. Mistyped TLD');
    return i1 > -1 && i2 > i1 && i3 > i2;
  })());
  ok('suggest: no stale "2. Mistyped TLD" left behind', !/\/\/ 2\. Mistyped TLD/.test(popup));
}

/* ============================================================
   5. Server DNS fallback — asserted by shape (needs a DOM + network)
   ============================================================ */
{
  ok('dns: resolveWebsiteViaServer defined exactly once',
     (popup.match(/async function resolveWebsiteViaServer/g) || []).length === 1);
  ok('dns: posts to /resolve-website', /RAILWAY_API_URL\}\/resolve-website/.test(popup));
  ok('dns: the route is a POST', /\/resolve-website`, \{\s*\n\s*method: 'POST'/.test(popup));
  ok('dns: 15s abort budget', /controller\.abort\(\), 15000\); \/\/ matches the content check/.test(popup));

  /* Every failure path must return null, which the caller reads as "keep
     the browser's verdict". A thrown error or an undefined here would
     surface as a blocked lead. */
  ok('dns: no backend -> null',
     /async function resolveWebsiteViaServer\(rawValue\) \{\s*\n\s*if \(!isRailwayReady\(\)\) return null;/.test(popup));
  ok('dns: non-200 -> null', /\/resolve-website[\s\S]{0,700}?if \(!res\.ok\) return null;/.test(popup));
  ok('dns: missing reason -> null', /if \(!data \|\| !data\.reason\) return null;/.test(popup));
  ok('dns: a throw is caught and returns null',
     /catch \(err\) \{\s*\n\s*console\.warn\('\[GW\] Server DNS fallback failed[\s\S]{0,120}?return null;/.test(popup));

  const base = popup.slice(popup.indexOf('function baseWebsiteVerdict'));
  const iDns = base.indexOf('const dnsWasBlocked');
  const iNeed = base.indexOf('const needsContentCheck');
  ok('dns: the branch lives in baseWebsiteVerdict', iDns > -1);
  ok('dns: it runs BEFORE the content check', iDns > -1 && iNeed > -1 && iDns < iNeed);

  ok('dns: fires on exactly the two blocked reasons',
     /const dnsWasBlocked = \(v\.reason === 'doh_error' \|\| v\.reason === 'dns_indeterminate'\);/.test(popup));
  ok('dns: test emails are exempt like every other check',
     /if \(dnsWasBlocked && !isTestEmail\(getField\('email'\)\)\) \{/.test(popup));
  ok('dns: only a decisive server reason may replace the verdict',
     /if \(sv && SERVER_DNS_DECISIVE\.indexOf\(sv\.reason\) !== -1\) \{/.test(popup));
  ok('dns: otherwise the browser verdict stands',
     /return v; \/\/ server couldn't help either; the original already passes/.test(popup));

  /* The reasons it triggers on must actually be producible upstream, or the
     branch is unreachable. */
  ok('dns: doh_error is a reason this file can produce', /reason: 'doh_error'/.test(popup));
  ok('dns: dns_indeterminate is a reason this file can produce', /reason: 'dns_indeterminate'/.test(popup));
}

/* ============================================================
   6. The typo nudge must not cannibalise the work-email nudge
   ============================================================ */
{
  ok('nudge: writes to its own element', /el\.id = 'gw-email-typo-hint';/.test(popup));
  ok('nudge: has a style rule', /#gw-email-typo-hint \{/.test(popup));
  /* #email-protip is an existing Webflow node with its own icon and copy.
     Reusing it for this would destroy the work-email nudge permanently. */
  ok('nudge: never writes into #email-protip',
     !/gw-email-typo-hint[\s\S]{0,1400}getElementById\('email-protip'\)/.test(popup));
  ok('nudge: built from DOM nodes, never innerHTML', (() => {
    const s = popup.slice(popup.indexOf('function showEmailTypoHint'));
    const body = s.slice(0, s.indexOf("el.style.display = 'flex';"));
    // Strip comments first — the house style says "never innerHTML" in a
    // comment right above the code, and matching that would pass forever.
    const code = body.replace(/\/\/[^\n]*/g, '').replace(/\/\*[\s\S]*?\*\//g, '');
    return !/innerHTML/.test(code) && /createTextNode/.test(code);
  })());
  ok('nudge: shown before the ELV round trip',
     popup.indexOf('showEmailTypoHint(val);') < popup.indexOf('verifyEmail(val).then'));
  ok('nudge: cleared when ELV rejects the address',
     /if \(!v\.valid\) \{ hideEmailTypoHint\(\); showEmailVerdictError\(v\); return; \}/.test(popup));
  ok('nudge: cleared on further typing',
     /el\.addEventListener\('input', function \(\) \{ hideEmailSuggestion\(\); hideEmailTypoHint\(\); \}\);/.test(popup));
  /* Advisory only — it must never touch validity. */
  ok('nudge: never sets valid = false', (() => {
    const s = popup.slice(popup.indexOf('function showEmailTypoHint'));
    return !/valid = false/.test(s.slice(0, s.indexOf("el.style.display = 'flex';")));
  })());
}

/* ============================================================
   7. The @-in-website branch must precede isValidURL
   ============================================================ */
{
  const v2 = popup.slice(popup.indexOf("const website = getField('website');"));
  const iAt = v2.indexOf("website.indexOf('@') !== -1");
  const iUrl = v2.indexOf('!isValidURL(website)');
  ok('website: the @ branch exists', iAt > -1);
  ok('website: it is checked BEFORE isValidURL', iAt > -1 && iUrl > -1 && iAt < iUrl);
  ok('website: the domain is offered as a one-tap fix',
     /showWebsiteVerdictError\(emailFix\s*\n\s*\? \{ msg: 'Did you mean ' \+ emailFix \+ '\?', suggestion: emailFix \}/.test(popup));
  /* The chip renderer is shared; if it drifts from /demo the suggestion
     stops being tappable in one file only. */
  ok('website: showWebsiteVerdictError is byte-identical to /demo',
     liftFn(popup, 'showWebsiteVerdictError') === liftFn(demo, 'showWebsiteVerdictError'));
}

/* ============================================================
   8. The ported units are byte-identical to /demo
   ============================================================ */
{
  const ported = [
    'resolveWebsiteViaServer', 'emailInWebsiteField', 'localEmailTypoHint',
    'typoHintEl', 'hideEmailTypoHint', 'showEmailTypoHint',
    'suggestWebsiteDomainFix', 'damerauLevenshtein', 'prewarmEmail',
    'baseWebsiteVerdict', 'verifyWebsiteContent',
  ];
  for (const name of ported) {
    ok(`identical: ${name}`, liftFn(popup, name) === liftFn(demo, name));
  }
  for (const name of ['PERSONAL_EMAIL_DOMAINS', 'WEBSITE_BAD_TLDS', 'SERVER_DNS_DECISIVE', 'CACHEABLE_REASONS', 'TYPO_TLDS']) {
    ok(`identical: ${name}`, liftConst(popup, name) === liftConst(demo, name));
  }
}

/* ============================================================
   9. The fork's OWN identity must survive a parity pass

   This section asserts DIFFERENCE, not sameness. The modal booking step
   is the reason this file exists; a future sync that "tidied" it away
   would be a regression, so it is pinned here.
   ============================================================ */
{
  ok('fork: still declares itself the Ads page version', /\(ADS PAGE VERSION\)/.test(popup));
  /* Assert the header and the banner AGREE rather than hard-coding a
     number. A pinned version rots at every release and gets "fixed" by
     bumping the test, which teaches nothing; the failure worth catching is
     the two disagreeing, which is what actually misleads a person reading
     the file or the console. */
  ok('fork: version header is an -ads build', /MULTI-STEP FORM  v5\.[0-9]+\.[0-9]+-ads/.test(popup));
  ok('fork: init banner agrees with the header', (() => {
    const h = /MULTI-STEP FORM  (v[0-9.]+-ads)/.exec(popup);
    const b = /Form initialised (v[0-9.]+-ads) \(Google Ads\)/.exec(popup);
    return h && b && h[1] === b[1];
  })());
  /* /demo gets the SAME self-consistency check the Ads file has. It had
     none, which is exactly why its init banner sat at v5.2.0 while its
     header read v5.7.1 — through v5.3.0, v5.6.0, v5.7.0 and v5.7.1. The
     banner is how you confirm which build a page is actually running, so a
     banner that disagrees with its own header is worse than none. */
  ok('demo: init banner agrees with the header', (() => {
    const h = /MULTI-STEP FORM  (v[0-9.]+)  \(\/demo/.exec(demo);
    const b = /Form initialised (v[0-9.]+) \(\/demo\)/.exec(demo);
    return h && b && h[1] === b[1];
  })(), (() => {
    const h = /MULTI-STEP FORM  (v[0-9.]+)  \(\/demo/.exec(demo);
    const b = /Form initialised (v[0-9.]+) \(\/demo\)/.exec(demo);
    return `header=${h && h[1]} banner=${b && b[1]}`;
  })());

  /* The close affordances are Ads-only by design and have no /demo
     counterpart. Pinned here so a future parity sweep cannot delete them
     as "divergence"; behaviour is covered in tests/test-ads-modal.js. */
  ok('fork: the modal keeps its close affordances', /window\.__gwRhOverlay = \{/.test(popup));
  ok('fork: the RevenueHero booking listener is still wired', /initRHBookingListener/.test(popup));
  ok('fork: it still has modal presentation of its own',
     /modal/i.test(popup) && !/modal/i.test(demo.slice(0, 60)));
  /* The two files are NOT meant to be the same file. If they ever become
     byte-identical, the modal has been lost. */
  ok('fork: the two files are still distinct', popup !== demo);
}

/* ============================================================ */
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
