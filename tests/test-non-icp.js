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
  /* REFUSING THE BOOKING, not merely suppressing the event. A suppressed
     Schedule still leaves a real slot on a real AE's calendar. */
  eq('all three booking routes call the refusal helper',
     (src.match(/await rejectBookingIfNonIcp\(/g) || []).length, 3);
  for (const tag of ['/booking-confirmed', '/cal-webhook', '/rh-webhook']) {
    ok(`booking refusal wired on ${tag}`,
       new RegExp("rejectBookingIfNonIcp\\('" + tag.replace(/\//g, '\\/') + "'").test(src));
  }
  ok('the refusal bails before any booking write',
     /rejectBookingIfNonIcp\([^)]*\)\) \{\s*return res/.test(src));
  ok('the refusal raises a CRITICAL, because only a human can cancel the slot',
     /alertOps\('critical', 'Non-ICP', 'A blocked lead took a calendar slot'/.test(src));
  ok('the refusal says out loud that it cannot cancel the slot',
     src.includes('Nothing in this service can cancel it.'));
  ok('the refusal fails OPEN when the row cannot be read',
     /Could not check non-ICP[\s\S]{0,120}return false;/.test(src));
  /* The safety-net paths create a lead from a booking with no form row -- the
     one route that bypasses the form entirely. Ten such rows exist. */
  eq('both webhook safety nets are guarded',
     (src.match(/refused_non_icp_safety_net/g) || []).length, 2);
  ok('the safety net computes the verdict from the email',
     /SAFETY-NET PATH[\s\S]{0,900}?await nonIcpVerdict\(\{ email, website: '' \}\)/.test(src));

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
  /* The full record a reviewer needs to spot a wrong block. */
  const slackFn = between('function slackNonIcpBlocked(d)', 'function slackSubmit(d)');
  for (const field of ['d.email', 'd.website', 'd.company', 'd.phone', 'd.matched_domain']) {
    ok(`blocked Slack post carries ${field}`, slackFn.includes(field));
  }
  ok('blocked Slack post flags an email/website mismatch in words',
     slackFn.includes('d.matched_in_email && !d.matched_in_website'));
  ok('/submit passes both match sides to Slack',
     submitBranch.includes('matched_in_email:') && submitBranch.includes('matched_in_website:'));
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
   10b. EVERY `disqualified` GUARD IS NOW HALF A GUARD

   The block lives in its own column on purpose, which means every
   pre-existing guard that reads `disqualified` answers only half the
   question. One of them sent a real PartnerStack conversion for
   agent@allstate.com on 11 Sept 2026 -- StartTrial, Meta Lead and the
   Salesforce push were all correctly suppressed and this one was not.

   This section is an AUDIT, not three spot checks: it derives every
   place `disqualified` is used as a predicate and requires each to have
   been considered. A new one added without a non_icp decision fails here
   rather than in production.
   ============================================================ */
{
  /* The three that MUST also exclude blocked leads -- they spend money,
     take SDR time, or report a population that can never clear. */
  const signup = between('async function runPartnerStackSignup', 'async function sendQualificationForDomain');
  ok('MONEY: the conversion guard reads non_icp_blocked',
     signup.includes('non_icp_blocked'));
  ok('MONEY: it reads the ROW, not a parameter that a call site can forget',
     /SELECT non_icp_blocked, non_icp_reason FROM leads WHERE session_id = \$1/.test(signup));
  ok('MONEY: it returns before sending',
     /non_icp_blocked === true\) \{[\s\S]{0,320}?return;/.test(signup));
  ok('MONEY: it records why it skipped',
     signup.includes("recordPartnerStackSkip(session_id, 'non_icp_blocked')"));
  /* Ordering: the block guard must sit ABOVE the domain and test-email
     checks, so a blocked lead never even claims the domain. */
  ok('MONEY: the block guard precedes the customer-key claim',
     signup.indexOf('non_icp_blocked === true') < signup.indexOf("recordPartnerStackSkip(session_id, 'no_customer_key')"));

  const sdr = between("app.get('/monitor/sdr'", 'const leads = result.rows;');
  ok('SDR TIME: the SDR list excludes blocked leads', sdr.includes('l.non_icp_blocked IS NOT TRUE'));

  const rec = between('async function checkRecoveryHealth', 'function safeCheck(id, fn)');
  ok('HEALTH: the recovery row excludes blocked leads', rec.includes('l.non_icp_blocked IS NOT TRUE'));

  /* THE FOURTH, and the one that undoes a manual cleanup. A customer deleted
     in the PartnerStack UI makes the verify sweep 404; the 404 releases
     ps_signup_sent_at AND stamps ps_signup_failed_at, which is exactly this
     sweep's selection criteria. Unguarded, hand-deleting a wrongly created
     customer re-creates it within fifteen minutes. */
  const retry = between('async function runPartnerStackConversionRetry', 'const claim = await pool.query');
  ok('MONEY: the conversion RETRY sweep excludes blocked leads',
     retry.includes('l.non_icp_blocked IS NOT TRUE'));
  ok('MONEY: retry guards on both columns, not just disqualified',
     retry.includes('l.disqualified IS NOT TRUE') && retry.includes('l.non_icp_blocked IS NOT TRUE'));

  /* EVERY money path, named. Counting predicates is not the same as checking
     them -- the count passed while the retry sweep was still unguarded. */
  for (const [what, seg] of [['submit conversion', signup], ['retry sweep', retry]]) {
    ok(`MONEY: ${what} cannot send for a blocked lead`, seg.includes('non_icp_blocked'));
  }

  /* THE AUDIT. Every SQL predicate on `disqualified` and every JS read of
     it, each one either paired with a non_icp decision or listed here as
     deliberately unpaired. If this count moves, a new guard was added and
     nobody decided what it should do about a blocked lead. */
  /* Comments stripped first -- the stage-ladder comment quotes two of these
     expressions in prose, and a tripwire that counts prose moves when someone
     rewords a comment. */
  const codeOnly = src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
  const sqlPreds = (codeOnly.match(/\b(l\.|pa\.|booked\.)?disqualified\s*(=\s*(true|false)|IS (NOT )?TRUE)/g) || []);
  eq('the number of disqualified predicates is known', sqlPreds.length, 13);
  /* Deliberately NOT paired, and why:
       - the stage ladder and the metrics counters: a blocked lead is still a
         lead and still belongs in a stage. The Blocked tab is its own surface.
       - slackPartial: only ever called from the recovery cron, which already
         excludes blocked leads, so it is unreachable for them. */
  ok('stage ladder still counts blocked leads as leads',
     src.includes("if (stage === 'completed')    conditions.push('l.booking_uid IS NULL AND l.disqualified IS NOT TRUE AND l.completed IS TRUE')")
     && !between("if (stage === 'completed')", "if (stage === 'step1')").includes('non_icp'));
  ok('slackPartial is reached only through the cron',
     (src.match(/slackPartial\(/g) || []).length === 2);
}

/* ============================================================
   10c. THE DASHBOARD MUST STILL RECONCILE

   A blocked lead is still a lead. Hiding it from All Leads would make
   the tab stop agreeing with the Overview cards and with itself, which
   is the class of bug the Definitions section of CLAUDE.md exists to
   prevent. It is MARKED, not removed.
   ============================================================ */
{
  const leadsRoute = between("app.get('/monitor/leads'", "app.get('/monitor/sdr'");

  /* The filter exists in both directions... */
  ok('dash: /monitor/leads can filter to blocked only',   leadsRoute.includes("nonIcp === 'only'"));
  ok('dash: /monitor/leads can exclude blocked',          leadsRoute.includes("nonIcp === 'exclude'"));
  /* ...and NEITHER is the default. This is the reconciliation guarantee:
     no unconditional non_icp predicate anywhere in the route. */
  /* THE RECONCILIATION GUARANTEE, stated as: every non_icp predicate in this
     route is gated on the nonIcp parameter. If one ever appears ungated, All
     Leads has started hiding rows and its totals stop matching Overview. */
  {
    const pushes = leadsRoute.split('\n').filter((ln) => /conditions\.push\([^)]*non_icp_blocked/.test(ln));
    eq('dash: exactly two non_icp predicates in /monitor/leads', pushes.length, 2);
    ok('dash: blocked leads are INCLUDED by default (every predicate is gated)',
       pushes.every((ln) => /if \(nonIcp === '(only|exclude)'\)/.test(ln)), pushes.join(' | '));
  }
  ok('dash: the client default is "included"',
     src.includes('<option value="">Blocked: included</option>'));
  ok('dash: the row carries the columns it needs to mark',
     leadsRoute.includes('l.non_icp_blocked, l.non_icp_reason'));
  ok('dash: the CSV export carries them too',
     leadsRoute.includes("'disqualified','non_icp_blocked','non_icp_reason','step_reached'"));

  /* ONE row builder, both tabs -- the Blocked tab gets the expandable
     panel for free and cannot drift from All Leads. */
  ok('dash: there is a single shared row builder', src.includes("'function leadRowsHtml(leads){"));
  eq('dash: both tabs render through it',
     (src.match(/leadRowsHtml\(d\.leads\)/g) || []).length, 2);
  ok('dash: the Blocked tab reuses /monitor/leads rather than its own route',
     src.includes('"/monitor/leads"+(TP||"?")+(TP?"&":"")+"nonicp=only'));
  ok('dash: the dead /monitor/blocked route is gone', !src.includes("app.get('/monitor/blocked'"));
  ok('dash: the Blocked tab keeps the expandable panel',
     src.includes('<tbody id="blk-tbody"><tr><td colspan="11"'));

  /* The row marker, and the Overview card. */
  ok('dash: a blocked row is visibly marked in place',
     /l\.non_icp_blocked\?"<span title=[\s\S]{0,40}Blocked/.test(src));
  ok('dash: the Overview card exists',                 src.includes('id="m-nonicp"'));
  ok('dash: the Overview card links to the tab',
     /showTab\(\\?'blocked\\?'\)"><div class="ml">Blocked/.test(src));
  ok('dash: metrics count blocked leads',              src.includes("COUNT(*) FILTER (WHERE non_icp_blocked IS TRUE)"));
  ok('dash: metrics expose both a session and a people count',
     src.includes('nonIcpBlocked, peopleNonIcp'));
  /* The card says so in words, because a number that is counted twice
     across two cards is exactly what the Definitions section forbids. */
  ok('dash: the card says blocked leads are still in every total',
     src.includes('still counted in every total'));
}

/* ============================================================
   10d. THE QUALIFICATION MUST BE ABOUT THE REFERRED LEAD

   The poller collapses ticked Opportunities to a DOMAIN, so any tick on
   a shared corporate domain paid that domain's partner. allstate.com
   carries a referred lead and an unreferred BOOKED lead; ticking hers
   would have paid a partner with no connection to her.

   Executed, not read: the gate is lifted and driven against controlled
   row sets, because "the function mentions email" is not the claim
   worth making.
   ============================================================ */
const results10d = (async () => {
  const out = [];
  const gateSrc = between('async function qualificationTargetCheck', '\nasync function sendQualificationForDomain');
  const build = (rows) => (new Function('pool', gateSrc + '\n return qualificationTargetCheck;'))(
    { query: async () => ({ rows }) });

  const L = (email, partner, referred, converted) =>
    ({ email, ps_partner_key: partner, referred, converted });

  /* THE ONE WAY IT FIRES: the ticked Opportunity names the referred lead. */
  {
    const g = await build([L('agent@kw.com', 'P1', true, true)])('kw.com', 'agent@kw.com');
    out.push(['qual: fires on an exact lead match', g.fire === true, JSON.stringify(g)]);
    out.push(['qual: reports the match', g.reason === 'lead_match' && g.matched_email === 'agent@kw.com', JSON.stringify(g)]);
    out.push(['qual: email match is case-insensitive',
      (await build([L('agent@kw.com', 'P1', true, true)])('kw.com', 'Agent@KW.com')).fire === true]);
  }

  /* THE allstate.com SHAPE -- referred lead plus an unreferred BOOKED lead,
     and the tick is about the unreferred one. */
  {
    const rows = [L('agent@allstate.com', 'P1', true, true), L('brittanyvisin@allstate.com', null, false, false)];
    const g = await build(rows)('allstate.com', 'brittanyvisin@allstate.com');
    out.push(['qual: REFUSES a tick about an unreferred lead', g.fire === false, JSON.stringify(g)]);
    out.push(['qual: says the Opportunity names nobody we referred', g.reason === 'no_lead_match', g.reason]);
  }

  /* NO DOMAIN FALLBACK. This is the whole point of the email-only design:
     a sole referred lead on a domain does NOT get paid when the ticked
     Opportunity names nobody. 1.8% of form-sourced Opportunities, measured.
     If this ever starts firing, a fallback has been reintroduced. */
  {
    const g = await build([L('swapnilsinha07@gmail.com', 'P1', true, true)])('google.ai', '');
    out.push(['qual: NO domain fallback — a sole referred lead with no contact email is refused',
      g.fire === false, JSON.stringify(g)]);
    out.push(['qual: names the missing contact email as the reason',
      g.reason === 'opportunity_has_no_contact_email', g.reason]);
  }
  {
    const rows = [L('a@acme.com', 'P1', true, true), L('b@acme.com', 'P1', true, false)];
    const g = await build(rows)('acme.com', '');
    out.push(['qual: no fallback even when every lead is the same partner', g.fire === false, JSON.stringify(g)]);
  }

  /* Two partners on one domain: only an exact match resolves it. */
  {
    const rows = [L('a@acme.com', 'P1', true, true), L('b@acme.com', 'P2', true, false)];
    out.push(['qual: two partners, no contact email -> refuse',
      (await build(rows)('acme.com', '')).fire === false]);
    out.push(['qual: two partners, exact match -> fire',
      (await build(rows)('acme.com', 'a@acme.com')).fire === true]);
  }

  /* An unreferred lead can never trigger a payout, even alone on its domain. */
  {
    const g = await build([L('someone@acme.com', null, false, true)])('acme.com', 'someone@acme.com');
    out.push(['qual: an unreferred lead never fires', g.fire === false, JSON.stringify(g)]);
  }

  /* Degenerate inputs. */
  {
    out.push(['qual: no leads -> no fire', (await build([])('x.com', 'a@x.com')).fire === false]);
    const g = await build([L('a@x.com', 'P1', true, false)])('x.com', 'a@x.com');
    out.push(['qual: a lead that never converted -> no fire', g.fire === false, JSON.stringify(g)]);
  }
  return out;
})();

/* ── Wiring: the gate must actually be consulted, and refusals visible ── */
{
  const poll = between('const sf = await findQualifiedDemoOpportunities();', 'async function qualificationTargetCheck');
  ok('qual: the poll consults the gate',        poll.includes('await qualificationTargetCheck('));
  ok('qual: the gate runs BEFORE the send',
     poll.indexOf('qualificationTargetCheck(') < poll.indexOf('sendQualificationForDomain('));
  ok('qual: a refusal skips the send',          /if \(!gate\.fire\) \{[\s\S]{0,1200}?continue;/.test(poll));
  ok('qual: a refusal is recorded, not just logged',
     /if \(!gate\.fire\)[\s\S]{0,700}?recordFailure\('PartnerStack'/.test(poll));
  ok('qual: a gate ERROR also refuses (fails closed on payment)',
     /catch \(err\)[\s\S]{0,400}?qualify target[\s\S]{0,200}?continue;/.test(poll));
  ok('qual: the ticked Opportunity is what gets passed in', poll.includes('opp.contactEmail'));
  /* The stamp lands on the matched lead, not blindly the earliest. */
  const send = between('async function sendQualificationForDomain', 'const result = await sendAction');
  ok('qual: the claim prefers the matched lead row',
     send.includes('ORDER BY (lower(email) IS NOT DISTINCT FROM $2) DESC, ps_signup_sent_at ASC'));
  /* No domain fallback anywhere in the gate. */
  const gate = between('async function qualificationTargetCheck', '\nasync function sendQualificationForDomain');
  ok('qual: the gate has no all-same-partner fallback', !/allSamePartner/.test(gate));
  ok('qual: the gate fires on exactly one condition',
     (gate.match(/fire: true/g) || []).length === 1, String((gate.match(/fire: true/g) || []).length));
}

/* ============================================================
   10e. A VERIFIED CONVERSION IS RE-CHECKED

   The verify sweep selects `ps_signup_verified_at IS NULL`, so a verified
   row was never looked at again -- and the PartnerStack UI is the only
   place a conversion can be reversed. The one supported way to undo a
   mistake desynced us permanently.
   ============================================================ */
{
  const rc = between('async function runPartnerStackConversionRecheck', 'async function runPartnerStackConversionVerify');

  ok('recheck: it selects VERIFIED rows, the ones the other sweep ignores',
     rc.includes('ps_signup_verified_at IS NOT NULL'));
  ok('recheck: it is paced per DOMAIN, not per tick',
     /ps_signup_recheck_at < NOW\(\) - INTERVAL '\$\{PS_RECHECK_AFTER_DAYS\} days'/.test(rc)
     || rc.includes("PS_RECHECK_AFTER_DAYS} days'"));
  ok('recheck: a row never re-checked is picked up first',
     rc.includes('ps_signup_recheck_at IS NULL') && rc.includes('NULLS FIRST'));
  ok('recheck: it is batch-capped so cost cannot spike with domain count',
     rc.includes('LIMIT ${PS_VERIFY_BATCH}'));

  /* THE LOAD-BEARING PART. A 404 must DEMOTE, never release: the release
     path has a grace period and an alert, and the 7 Sept lesson is that one
     404 is never enough to act on. Two independent 404s, fifteen minutes
     apart, before anything is released. */
  ok('recheck: a 404 clears ONLY the verification stamp',
     /ps_signup_verified_at = NULL/.test(rc));
  ok('recheck: it does NOT release the claim itself',
     !/ps_signup_sent_at\s*=\s*NULL/.test(rc),
     'the re-check released a claim directly instead of demoting for a second opinion');
  ok('recheck: it does not stamp a failure that would trigger the retry sweep',
     !/ps_signup_failed_at/.test(rc));
  ok('recheck: a disappearance is recorded, not just logged',
     rc.includes("recordFailure('PartnerStack'") && rc.includes('customer disappeared'));
  ok('recheck: the demotion is mirrored to AWS',
     rc.includes("syncPartnerStackStampToAWS(r.session_id, 'ps_signup_verified_at', null)"));

  /* "Could not tell" is never "gone" -- the rule the whole repo runs on. */
  ok('recheck: a non-OK read leaves the row completely alone',
     /if \(!out\.ok\) \{[\s\S]{0,400}?continue;/.test(rc));
  ok('recheck: only a definitive answer demotes', rc.includes('if (out.exists) continue;'));

  /* The recheck stamp is its own column, not an overload of the first one. */
  ok('recheck: it uses a SEPARATE column from ps_signup_verified_at',
     rc.includes('ps_signup_recheck_at = NOW()') && !/ps_signup_verified_at = NOW\(\)/.test(rc));
  ok('recheck: db.js creates that column',
     dbsrc.includes('ps_signup_recheck_at TIMESTAMPTZ'));

  /* Scheduled like the other partner jobs. */
  ok('recheck: boot-then-interval',
     /function startPartnerStackConversionRecheck[\s\S]{0,700}?run\('boot'\)/.test(src)
     && /function startPartnerStackConversionRecheck[\s\S]{0,700}?setInterval\(/.test(src));
  ok('recheck: started from start()', src.includes('      startPartnerStackConversionRecheck();'));
  ok('recheck: its boot run cannot throw out of start()',
     /function startPartnerStackConversionRecheck[\s\S]{0,400}?\.catch\(/.test(src));
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
    /* /thank-you greets the visitor from attendeeName. Without it a blocked
       lead reads "Thank you, !". */
    ok(`${name}: the redirect carries attendeeName`,
       s.includes("'?attendeeName=' + encodeURIComponent(name)"));
    ok(`${name}: attendeeName is omitted when there is no name`,
       /var url  = NON_ICP_REDIRECT\s*\+ \(name \? '\?attendeeName='/.test(s));
    ok(`${name}: the name is URL-encoded`, s.includes('encodeURIComponent(name)'));
    /* STEP 1 DETECTS BUT MUST NOT REDIRECT (changed 11 Sept 2026). The lead
       completes the form so we capture website, company and phone -- four of
       the 84 matched leads were not agents and only their step-2 fields show
       that. savePartial still stamps the row and /partial still suppresses
       StartTrial. */
    ok(`${name}: step 1 warms the verdict`,     s.includes("checkNonIcp(formState.email, '').catch(() => {});"));
    ok(`${name}: step 1 does NOT redirect`,     !/icp1[\s\S]{0,200}redirectNonIcp/.test(s));
    ok(`${name}: no icp1 block branch remains`, !s.includes('if (icp1.blocked)'));
    /* STEP 2 READS CACHE ONLY -- no network call at the moment of decision and
       no timeout to fall through. A lead who slips past because our own
       request was slow is a realtor on an AE's calendar. */
    ok(`${name}: step 2 decides from cache`,    s.includes('const icp2 = nonIcpCached(formState.email, getField(\'website\'))'));
    ok(`${name}: step 2 never awaits the check`, !/const icp2 = await/.test(s));
    ok(`${name}: step 2 falls back to the email-only verdict`,
       s.includes('|| nonIcpCachedEmail(formState.email);'));
    ok(`${name}: a null verdict does not block`, s.includes('if (icp2 && icp2.blocked) {'));
    ok(`${name}: the cache reader never fetches`,
       /function nonIcpCached\(email, website\) \{[\s\S]{0,220}?\}/.test(s)
       && !/function nonIcpCached\(email, website\) \{[\s\S]{0,220}?fetch/.test(s));
    ok(`${name}: prewarms on email blur`,       s.includes("checkNonIcp(val, '').catch(() => {});"));
    ok(`${name}: prewarms on website blur`,     s.includes("checkNonIcp(getField('email'), val).catch(() => {});"));
    ok(`${name}: honours the server backstop`,  s.includes('submitRes.non_icp_blocked === true'));
    ok(`${name}: only caches a real answer`,    s.includes("if (v.status === 'ok') _nonIcpVerdicts.set(key, v);"));

    /* THE ORDERING THAT MATTERS. hero.submit() is what produces the booking
       widget, so the check has to sit above it or a blocked lead gets a
       calendar. Paired with a reachability assertion, because an offset
       comparison survives an early return. */
    const iCheck = s.indexOf('const icp2 = nonIcpCached(');
    const iHero  = s.indexOf('const hero = new RevenueHero(');
    ok(`${name}: step-2 check runs BEFORE RevenueHero`, iCheck > 0 && iHero > 0 && iCheck < iHero,
       `check at ${iCheck}, hero at ${iHero}`);
    const guarded = s.slice(iCheck, iHero);
    ok(`${name}: the step-2 block actually returns`,
       /if \(icp2 && icp2\.blocked\) \{[\s\S]*await submitLead\(\);[\s\S]*redirectNonIcp\(icp2\);[\s\S]*return;[\s\S]*\}/.test(guarded));
    /* Step 1 must warm BEFORE enrichment, so the verdict is in memory early
       and an Apollo credit is not the thing that gates it. */
    ok(`${name}: step-1 warm precedes enrichment`,
       s.indexOf("checkNonIcp(formState.email, '').catch") < s.indexOf('await triggerEnrichment(formState.email);'));
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
  .then((rows) => { for (const [n, c, x] of rows) ok(n, c, x); return results10d; })
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
