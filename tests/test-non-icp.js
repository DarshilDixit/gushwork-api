/* ============================================================
   Non-ICP block (V1) — real estate + insurance brand domains.

   Same convention as the other dependency-free suites: the real
   functions are LIFTED out of index.js rather than copied, because a
   test that exercises a duplicate of the source can pass while
   production is broken.

   THE FIXTURES ARE REAL ROWS. Every address and website in section 2
   and section 3 was read out of production `leads` on 11 Sept 2026 —
   including the five that a substring matcher would have blocked by
   mistake, which is the specific regression this file exists to stop.

   Run:  node tests/test-non-icp.js
   ============================================================ */

require('./crash-reporter')('test-non-icp');

const fs   = require('fs');
const path = require('path');
const src  = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
const dbsrc = fs.readFileSync(path.join(__dirname, '..', 'db.js'), 'utf8');
const formSrc  = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form.js'), 'utf8');
const popupSrc = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form-popup.js'), 'utf8');

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
function between(startMarker, endMarker, s = src) {
  const i = s.indexOf(startMarker);
  if (i === -1) throw new Error('marker not found: ' + startMarker);
  const j = s.indexOf(endMarker, i);
  if (j === -1) throw new Error('end marker not found: ' + endMarker);
  return s.slice(i, j);
}

/* ── Lift the real matcher and everything it stands on ───────────── */
const liftSrc = [
  between('function damerauLevenshtein(a, b)', '\n// Derived from FREE_EMAIL_DOMAINS'),
  between('const FREE_EMAIL_DOMAINS = [', '\napp.set('),
  between('const MULTI_PART_SUFFIXES = new Set([', 'function isMarketplaceHost'),
  between('const NON_ICP_BLOCK_ENABLED =', '\n/* The verdict, for one lead.'),
].join('\n');

const M = (new Function(liftSrc + `
  return { NON_ICP_DOMAINS, NON_ICP_PREFIXES, hostMatchesDomain, nonIcpMatchHost,
           partnerStackCustomerKey, registrableDomain, nonIcpHostForms };
`))();

const hit = (x) => { const r = M.nonIcpMatchHost(x); return r ? r.domain : null; };

/* ============================================================
   1. Subdomain boundary — the whole matching contract
   ============================================================ */
{
  ok('exact host matches',            M.hostMatchesDomain('kw.com', 'kw.com'));
  ok('proper subdomain matches',      M.hostMatchesDomain('agents.farmers.com', 'farmers.com'));
  ok('deep subdomain matches',        M.hostMatchesDomain('a.b.farmers.com', 'farmers.com'));
  ok('suffix without a dot does NOT', !M.hostMatchesDomain('notfarmers.com', 'farmers.com'));
  ok('longer label does NOT',         !M.hostMatchesDomain('farmersagency.com', 'farmers.com'));
  ok('prefix does NOT',               !M.hostMatchesDomain('farmers.com.evil.net', 'farmers.com'));
  ok('empty is not a match',          !M.hostMatchesDomain('', 'kw.com'));
  ok('empty target is not a match',   !M.hostMatchesDomain('kw.com', ''));
}

/* ============================================================
   2. REAL BLOCKED ROWS — production leads, 11 Sept 2026
   ============================================================ */
{
  const realMatches = [
    // [what the lead actually typed, which brand domain must catch it]
    ['tungle@allstate.com',                  'allstate.com'],
    ['b.sheffield@allstate.com',             'allstate.com'],
    ['agents.allstate.com/060929',           'agents.allstate.com'],
    ['scott.neil.puzk@statefarm.com',        'statefarm.com'],
    ['taylor.green.f26d@statefarm.com',      'statefarm.com'],
    ['robert.parish@compass.com',            'compass.com'],
    ['denise.madan@compass.com',             'compass.com'],
    ['candystump@kw.com',                    'kw.com'],
    ['claudiapgomez.kw.com',                 'kw.com'],           // website, subdomain form
    ['janene.davis@exprealty.com',           'exprealty.com'],
    ['www.exprealty.com',                    'exprealty.com'],
    ['andrea.cataldi@cbrealty.com',          'cbrealty.com'],
    ['tkott@farmersagent.com',               'farmersagent.com'],
    ['agents.farmers.com/co/pueblo-west',    'agents.farmers.com'],
    ['carly.fcrespin@farmersagency.com',     'farmersagency.com'],
    ['wifill@ft.newyorklife.com',            'ft.newyorklife.com'],
    ['bjacorn@ft.newyorklife.com',           'ft.newyorklife.com'],
    ['robin.paeplow@remax.net',              'remax.net'],
    ['pat.pezick@foxroach.com',              'foxroach.com'],
    ['jlamb@amfam.com',                      'amfam.com'],
    ['https://www.allstateagencies.com/wesnorwood', 'allstateagencies.com'],
  ];
  for (const [input, expected] of realMatches) {
    eq(`blocks real row: ${input}`, hit(input), expected);
  }

  // BHHS regional suffixes cannot be enumerated — the prefix rule carries them.
  ok('bhhsrmr.com matches by prefix', M.nonIcpMatchHost('kgiddens@bhhsrmr.com') !== null);
  eq('bhhsrmr.com is labelled BHHS',
     (M.nonIcpMatchHost('kgiddens@bhhsrmr.com') || {}).label,
     'Berkshire Hathaway HomeServices');
}

/* ============================================================
   3. THE SUBSTRING REGRESSION — five real leads that must NOT block

   Measured 11 Sept 2026 against 5,123 production leads: substring
   matching caught 116 where exact/subdomain caught 110. Five of the six
   extra were wrong. If any of these five ever starts blocking, somebody
   has reintroduced a LIKE '%brand%'.
   ============================================================ */
{
  const mustNotBlock = [
    ['rlambe@paycompass.com',               'PayCompass, a payments company — contains "compass.com"'],
    ['paycompass.com',                      'same, as a website'],
    ['steve@charleslegalpl.com',            'Charles Injury Law — contains "lpl.com"'],
    ['john@theimagecreatornm.com',          'an image company — contains "nm.com"'],
    ['sebastian.schneeweiss@krevera.com',   'Krevera — contains "era.com"'],
    ['sean.mainwaring@ceterainvestors.com', 'Cetera Investors — contains "era.com", and advisors are in ICP anyway'],
  ];
  for (const [input, why] of mustNotBlock) {
    eq(`SUBSTRING REGRESSION — must not block ${input} (${why})`, hit(input), null);
  }
}

/* ============================================================
   4. SCOPE — what V1 deliberately leaves alone

   The Non-ICP doc keeps financial advisors, mortgage and lending in ICP
   by name, and nobody has reversed that. Independent local agencies are
   not national brands. Each of these is a REAL production domain.
   ============================================================ */
{
  const inIcp = [
    ['jennifer.cooper@edwardjones.com', 'financial advisor network'],
    ['lpl.com',                         'financial advisor network'],
    ['northwesternmutual.com',          'financial advisor network'],
    ['primerica.com',                   'financial advisor network'],
    ['loandepot.com',                   'mortgage'],
    ['canopymortgage.com',              'mortgage'],
    ['gglendinggroup.com',              'lending'],
    ['espero-insurance.com',            'independent local agency'],
    ['garyrockwellinsurance.com',       'independent local agency'],
    ['tradetryonrealty.com',            'independent local brokerage'],
    ['goldstonerealty.com',             'independent local brokerage'],
    ['daniellegrassarealtor.com',       'independent realtor — and a paying customer'],
  ];
  for (const [input, why] of inIcp) {
    eq(`SCOPE — stays in ICP: ${input} (${why})`, hit(input), null);
  }

  const names = Object.keys(M.NON_ICP_DOMAINS);
  for (const banned of ['edwardjones.com','lpl.com','nm.com','northwesternmutual.com',
                        'primerica.com','ceterainvestors.com','loandepot.com','mottomortgage.com']) {
    ok(`SCOPE — ${banned} is not on the list`, !names.includes(banned));
  }
}

/* ============================================================
   5. Free-mailbox and junk inputs never match

   partnerStackCustomerKey rejects free providers and IP literals, so a
   gmail address can never be blocked on its EMAIL — only on its website.
   That is what keeps 25% of the funnel out of this feature entirely.
   ============================================================ */
{
  for (const x of ['someone@gmail.com','someone@yahoo.com','someone@outlook.com',
                   'shortsalesreo@gmail.com','127.0.0.1','','not a url','com','@','kw']) {
    eq(`never matches junk/free input: ${JSON.stringify(x)}`, hit(x), null);
  }
  // ...but the same person's brokerage WEBSITE is still caught.
  eq('gmail lead is caught on the website half', hit('Claudiapgomez.kw.com'), 'kw.com');
}

/* ============================================================
   6. The list itself
   ============================================================ */
{
  const names = Object.keys(M.NON_ICP_DOMAINS);
  ok('every entry is lowercase',   names.every((d) => d === d.toLowerCase()));
  ok('no entry carries a scheme',  names.every((d) => !d.includes('//')));
  ok('no entry carries a www.',    names.every((d) => !d.startsWith('www.')));
  ok('no entry carries a path',    names.every((d) => !d.includes('/')));
  ok('every entry has a dot',      names.every((d) => d.includes('.')));
  ok('every entry has a label',    names.every((d) => typeof M.NON_ICP_DOMAINS[d] === 'string' && M.NON_ICP_DOMAINS[d].length > 0));
  ok('zero-hit entries are kept',  ['century21.com','sothebysrealty.com','goosehead.com','nyl.com'].every((d) => names.includes(d)));
  ok('near-misses found in data are present',
     ['ft.newyorklife.com','cbrealty.com','remax.net','agents.farmers.com','agents.allstate.com',
      'farmersagency.com','allstateagencies.com','foxroach.com','healthmarketsjax.com'].every((d) => names.includes(d)));
  /* Every entry must be reachable through partnerStackCustomerKey, or it can
     never fire. A free-email domain or a bare label would be silently inert. */
  ok('every entry survives normalisation',
     names.every((d) => M.partnerStackCustomerKey(d) !== null || d.split('.').length > 2));
}

/* ============================================================
   7. FAIL-OPEN — the verdict never blocks because something broke

   nonIcpVerdict is lifted WITH its real dependencies stubbed, so this
   drives the actual function rather than asserting on its text. An
   ordering or source assertion cannot tell a reachable statement from
   an unreachable one; this executes it.
   ============================================================ */
const results7 = (async () => {
  const out = [];
  const verdictSrc = [
    between('function damerauLevenshtein(a, b)', '\n// Derived from FREE_EMAIL_DOMAINS'),
    between('const FREE_EMAIL_DOMAINS = [', '\napp.set('),
    between('const MULTI_PART_SUFFIXES = new Set([', 'function isMarketplaceHost'),
    between('const NON_ICP_BLOCK_ENABLED =', '\n/* The verdict, for one lead.'),
    between('const NON_ICP_CUSTOMER_TIMEOUT_MS', '\n/* The verdict. Reason strings'),
  ].join('\n');

  const build = ({ enabled, customers, customersThrow, testEmail }) => (new Function(
    'process', 'withTimeout', 'partnerStackCustomerDomains', 'isPartnerStackTestEmail', 'console',
    verdictSrc + '\n return nonIcpVerdict;'
  ))(
    { env: { NON_ICP_BLOCK: enabled ? 'true' : 'false' } },
    (p, ms, label) => p,
    async () => { if (customersThrow) throw new Error('RDS timeout'); return customers; },
    (e) => testEmail === true,
    { log() {}, warn() {} }
  );

  // Flag off -> never blocks, whatever the domain.
  {
    const v = await build({ enabled: false, customers: new Set() })({ email: 'a@kw.com', website: 'kw.com' });
    out.push(['FAIL-OPEN: flag off never blocks', v.blocked === false, JSON.stringify(v)]);
    out.push(['FAIL-OPEN: flag off says why', v.reason === 'disabled', v.reason]);
  }
  // Flag on, not a customer -> blocks, and names the matched domain.
  {
    const v = await build({ enabled: true, customers: new Set() })({ email: 'a@kw.com', website: '' });
    out.push(['blocks a brokerage email when enabled', v.blocked === true, JSON.stringify(v)]);
    out.push(['reason is the matched domain', v.reason === 'kw.com', v.reason]);
  }
  // KNOWN CUSTOMER BYPASS — the jacobsfamilyinsurance.net case, verified Active
  // in gist.customer_contract_terms on 11 Sept 2026.
  {
    const v = await build({ enabled: true, customers: new Set(['jacobsfamilyinsurance.net']) })(
      { email: 'nedjacobs@allstate.com', website: 'www.jacobsfamilyinsurance.net' });
    out.push(['KNOWN CUSTOMER is not blocked', v.blocked === false, JSON.stringify(v)]);
    out.push(['known customer says why', v.reason === 'known_customer', v.reason]);
    out.push(['known customer still reports what matched', v.matched_domain === 'allstate.com', v.matched_domain]);
  }
  // Warehouse unreachable -> MUST NOT BLOCK. "Could not check" is not "bad".
  {
    const v = await build({ enabled: true, customersThrow: true })(
      { email: 'a@statefarm.com', website: 'statefarm.com' });
    out.push(['FAIL-OPEN: warehouse timeout does NOT block', v.blocked === false, JSON.stringify(v)]);
    out.push(['FAIL-OPEN: timeout says check_failed', v.reason === 'check_failed', v.reason]);
    out.push(['FAIL-OPEN: timeout still reports what matched', v.matched_domain === 'statefarm.com', v.matched_domain]);
  }
  // Our own test addresses are never blocked.
  {
    const v = await build({ enabled: true, customers: new Set(), testEmail: true })(
      { email: 'b@g.ai', website: 'kw.com' });
    out.push(['internal/test address is never blocked', v.blocked === false, JSON.stringify(v)]);
  }
  // A clean lead never touches the warehouse at all.
  {
    let asked = false;
    const fn = (new Function(
      'process', 'withTimeout', 'partnerStackCustomerDomains', 'isPartnerStackTestEmail', 'console',
      verdictSrc + '\n return nonIcpVerdict;'
    ))({ env: { NON_ICP_BLOCK: 'true' } }, (p) => p,
       async () => { asked = true; return new Set(); }, () => false, { log() {}, warn() {} });
    const v = await fn({ email: 'someone@acme.com', website: 'acme.com' });
    out.push(['a clean lead is not blocked', v.blocked === false, JSON.stringify(v)]);
    out.push(['a clean lead never queries the warehouse', asked === false,
              'the customer bypass ran for a lead that did not match']);
  }
  return out;
})();

/* ============================================================
   8. Where the block is EVALUATED — reachability, not just ordering

   An ordering assertion survives an early return, an if (false), or a
   new wrong guard above the statement. These check that the guarded
   call sits inside the branch it is supposed to.
   ============================================================ */
{
  /* All THREE booking routes suppress Schedule. A fix on one is a fix on one
     third.

     ASSERTED AS CONDITION-ADJACENT-TO-ROUTE, not as "the log string exists".
     The first version of this counted occurrences of the message and a
     mutation replacing `if (fullLead.non_icp_blocked === true)` with
     `if (false)` SURVIVED it -- the message is still in the file, the guard
     just never fires. That is the reachability blind spot CLAUDE.md records
     for ordering assertions, arriving here in a third form. The regex below
     requires the real condition immediately before the route's own log line,
     so neutering the condition fails the suite. */
  for (const tag of ['/booking-confirmed', '/cal-webhook', '/rh-webhook']) {
    const re = new RegExp(
      'if \\(fullLead\\.non_icp_blocked === true\\) \\{ console\\.log\\(`\\['
      + tag.replace(/\//g, '\\/')
      + '\\] ⏭ Meta CAPI Schedule suppressed');
    ok(`Schedule guard on ${tag} is live, not just present`, re.test(src));
  }
  const scheduleGuards = (src.match(/if \(fullLead\.non_icp_blocked === true\) \{ console\.log/g) || []).length;
  eq('exactly THREE live Schedule guards', scheduleGuards, 3);
  /* And each one must actually return, or it logs and fires anyway. */
  const returning = (src.match(/if \(fullLead\.non_icp_blocked === true\) \{[\s\S]{0,220}?return; \}/g) || []).length;
  eq('all three Schedule guards return', returning, 3);
  ok('SCHEDULE_LEAD_SQL selects the column',
     between('const SCHEDULE_LEAD_SQL', 'WHERE l.session_id').includes('l.non_icp_blocked'));

  // StartTrial, at /partial.
  ok('StartTrial is suppressed for a blocked lead',
     src.includes('StartTrial suppressed — non-ICP'));
  const partialBlock = between("const freeMatch = email ? freeEmailMatch", "console.log(`[/partial] ✅ Saved");
  ok('StartTrial non-ICP branch comes BEFORE the free-email branch',
     partialBlock.indexOf('if (nonIcp.blocked)') < partialBlock.indexOf('else if (!disqualified && isBusinessEmail)'),
     'a blocked business-email lead would otherwise fire StartTrial');
  ok('StartTrial guard is an if, not a comment',
     /if \(nonIcp\.blocked\) \{/.test(partialBlock));

  // Lead, at /submit — the blocked branch must skip Slack, Salesforce and Meta.
  const submitBranch = between('if (!alreadySubmitted && nonIcpBlocked) {', '} else if (!alreadySubmitted) {');
  ok('blocked branch posts the blocked-lead Slack message', submitBranch.includes('slackNonIcpBlocked('));
  ok('blocked branch does NOT call slackSubmit',            !submitBranch.includes('slackSubmit('));
  ok('blocked branch does NOT push to Salesforce',          !submitBranch.includes('pushToSalesforce('));
  ok('blocked branch does NOT fire Meta',                   !submitBranch.includes('pushFormEventsToMeta('));

  // The effective flag must come from the row, not the in-memory verdict.
  ok('/submit reads the effective block from the upsert row',
     src.includes("const nonIcpBlocked = upsert.rows[0]?.non_icp_blocked === true"));
  ok('upsert RETURNS the effective block', src.includes('leads.non_icp_blocked, leads.non_icp_reason'));
}

/* ============================================================
   9. STICKY — a realtor cannot clear their own block

   88% of the known population reached step 2 by clicking "actually
   we're B2B", which calls savePartial again with disqualified=false.
   An EXCLUDED assignment here would let that clear the block.
   ============================================================ */
{
  const sticky = (src.match(/non_icp_blocked\s+= \(leads\.non_icp_blocked IS TRUE OR EXCLUDED\.non_icp_blocked IS TRUE\)/g) || []).length;
  eq('both leads upserts set non_icp_blocked stickily', sticky, 2);
  ok('mirror upsert is sticky too',
     src.includes('non_icp_blocked         = (gw_form_leads.non_icp_blocked IS TRUE OR EXCLUDED.non_icp_blocked IS TRUE)'));
  ok('non_icp_reason keeps the FIRST value',
     (src.match(/non_icp_reason\s+= COALESCE\(leads\.non_icp_reason/g) || []).length === 2);
  ok('nothing assigns non_icp_blocked = EXCLUDED unconditionally',
     !/non_icp_blocked\s+= EXCLUDED\.non_icp_blocked/.test(src));
}

/* ============================================================
   10. leads.disqualified is NOT touched
   ============================================================ */
{
  ok('db.js adds the two new columns',
     dbsrc.includes('non_icp_blocked BOOLEAN DEFAULT FALSE') && dbsrc.includes('non_icp_reason TEXT'));
  ok('the mirror gets them too',
     src.includes('ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS non_icp_blocked'));
  /* The feature must never write disqualified. Every occurrence of
     "disqualified =" in a SQL assignment is pre-existing; if this count
     moves, something in this feature started writing that column. */
  /* THREE pre-existing assignment sites: the mirror upsert, /partial and
     /submit. Indented, so the two prose mentions of the same expression in
     comments do not count. If this number moves, this feature (or something
     near it) started writing a column it must never write. */
  const dqWrites = (src.match(/^ +disqualified +=  *EXCLUDED\.disqualified,$/gm) || []).length;
  eq('disqualified assignment sites unchanged (mirror, /partial, /submit)', dqWrites, 3);
  ok('nonIcpVerdict never mentions disqualified',
     !between('async function nonIcpVerdict', '\n/* The verdict. Reason strings').includes('disqualified'));
}

/* ============================================================
   11. Recovery cron excludes blocked leads
   ============================================================ */
{
  const cron = between("app.post('/cron/send-partials'", 'const leads = result.rows;');
  ok('recovery cron excludes blocked leads', cron.includes('l.non_icp_blocked IS NOT TRUE'));
  ok('cron uses IS NOT TRUE, not = false',   !cron.includes('l.non_icp_blocked = false'));
}

/* ============================================================
   12. BOTH form files — the Ads fork is half the traffic
   ============================================================ */
{
  for (const [name, s] of [['/demo', formSrc], ['ads', popupSrc]]) {
    ok(`${name}: has the non-ICP check`,        s.includes('function checkNonIcp('));
    ok(`${name}: fails open on a non-200`,      s.includes("return { blocked: false, status: 'backend_error' };"));
    ok(`${name}: redirects to /thank-you`,      s.includes("const NON_ICP_REDIRECT = '/thank-you';"));
    ok(`${name}: step 1 checks the email`,      s.includes("const icp1 = await checkNonIcp(formState.email, '');"));
    ok(`${name}: step 2 checks email+website`,  s.includes('const icp2 = await checkNonIcp(formState.email, formState.website);'));
    ok(`${name}: prewarms on email blur`,       s.includes("checkNonIcp(val, '').catch(() => {});"));
    ok(`${name}: prewarms on website blur`,     s.includes("checkNonIcp(getField('email'), val).catch(() => {});"));
    ok(`${name}: honours the server backstop`,  s.includes('submitRes.non_icp_blocked === true'));
    ok(`${name}: only caches a real answer`,    s.includes("if (v.status === 'ok') _nonIcpVerdicts.set(key, v);"));

    /* THE ORDERING THAT MATTERS. hero.submit() is what produces the booking
       widget, so the check has to sit above it or a blocked lead gets a
       calendar. Paired with a reachability assertion, because an offset
       comparison survives an early return. */
    const iCheck = s.indexOf('const icp2 = await checkNonIcp(');
    const iHero  = s.indexOf('const hero = new RevenueHero(');
    ok(`${name}: step-2 check runs BEFORE RevenueHero`, iCheck > 0 && iHero > 0 && iCheck < iHero,
       `check at ${iCheck}, hero at ${iHero}`);
    const guarded = s.slice(iCheck, iHero);
    ok(`${name}: the step-2 block actually returns`,
       /if \(icp2\.blocked\) \{[\s\S]*await submitLead\(\);[\s\S]*redirectNonIcp\(icp2\);[\s\S]*return;[\s\S]*\}/.test(guarded));
    ok(`${name}: the step-1 block actually returns`,
       /if \(icp1\.blocked\) \{[\s\S]*await savePartial\(1\);[\s\S]*redirectNonIcp\(icp1\);[\s\S]*return;[\s\S]*\}/.test(s));
    /* Step 1 must check BEFORE spending an Apollo credit. */
    ok(`${name}: step-1 check precedes enrichment`,
       s.indexOf("const icp1 = await checkNonIcp") < s.indexOf('await triggerEnrichment(formState.email);'));
  }

  /* NO CLIENT-SIDE COPY OF THE LIST. The two form files already carry
     duplicated website-verdict lists and have drifted before; a fourth copy
     deciding whether a real person can book would drift the same way. */
  for (const [name, s] of [['/demo', formSrc], ['ads', popupSrc]]) {
    for (const brand of ['statefarm.com', 'exprealty.com', 'cbrealty.com', 'farmersagent.com']) {
      ok(`${name}: does not hardcode ${brand}`, !s.includes(brand));
    }
  }

  /* Both files must be pinned to the same version, or the Webflow re-pin
     shipped half the fix. */
  const v = (s) => (s.match(/Form initialised (v[0-9.]+)/) || [])[1];
  eq('both form files are the same version', v(formSrc), v(popupSrc));
}

/* ============================================================ */
results7
  .then((rows) => { for (const [n, c, x] of rows) ok(n, c, x); })
  .catch((err) => { ok('non-icp: section 7 completed', false, err && err.message); })
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
