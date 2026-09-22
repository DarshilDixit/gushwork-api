# PR 109 — Close the late-verdict gaps: booking guard, sweep, brand list, name fallback

Branch `feat/non-icp-close-the-gaps`. Four commits. **Not merged.**

Server-side only. **Neither form file is touched, so there is no Webflow re-pin.**

---

## Read this first: I committed and pushed a mutation

The flag that decides whether the name-only fallback runs at all was on the
pushed branch as `!== 'false'` when it must be `=== 'true'`. **That inverts the
default** — the feature would have gone live on merge and started blocking
leads, while the PR body and three of my messages said it ships dark.

Corrected in `338b081`, verified by reading the file back off `origin` rather
than from my working copy.

**How:** a background mutation run was still going while I checked `git status`,
ran the bar and committed with `git add -A`. It applied the mutation in the gap
between the check and the commit. The `git status` I read was true when I read
it and false a second later.

**Why nothing caught it:** the two assertions that should have build the module
with the variable *absent*, and with it absent both spellings behave identically
for the enabled case. The bar was green at the moment of the commit because the
mutation had not landed yet, and nothing re-ran it afterwards. The mutation run
then reported that one as `PATTERN NOT UNIQUE (0) — SKIPPED`, which I read as a
gap in my script when it was actually evidence the file had already changed.

Now pinned two ways that discriminate: the source must contain `=== 'true'`, and
`NON_ICP_NAME_FALLBACK=yes` must leave it off. Both verified to fail against the
mutation before committing.

**The line to check on the diff is `index.js:8390`.** If it reads `=== 'true'`
the fallback is inert until you set the env var. If it does not, it is live.

---

## What was actually verified

- **Full bar, run bare, read in full**: 12 suites, **4186 assertions**, zero
  failures. Baseline re-saved four times as assertions were added.
- **13 of 13 mutations CAUGHT.** Table in the PR body. The first mutation run
  was unreliable — a background script and my own `git checkout` calls fought
  over the working tree, so several results were measured against a
  half-mutated file. Re-run serially against a committed baseline; only the
  serial run is quoted.
- **`nonIcpScheduleSuppressed` is EXECUTED**, not read — lifted through the
  AsyncFunction constructor with a stubbed verdict reader, 13 cases including
  the late blocking verdict, the Meta-only split, and fail-open on a thrown
  read.
- **`runNonIcpBookedRecheck` is EXECUTED** against a stubbed pool, 5 cases.
  **This exists because a mutation survived**: `if (bypass) continue;` →
  `if (false) continue;` left all 625 assertions passing, because the test
  checked the customer bypass was *called*, not that it *did anything*. A
  paying customer could have been stamped non-ICP with nothing to notice.
- **`nonIcpClassifyFromName` is EXECUTED** against a stubbed fetch — the enum,
  the higher floor, `unknown` falling back to the 6h TTL, and fail-open on
  throw, timeout, API error, refusal and off-enum type.
- **The client-side source labels are EXECUTED**, not read. They live inside a
  JS string sent to the browser, so a slip in them is invisible to
  `node --check`.
- **Both new SQL statements were EXECUTED** against the real Railway and AWS
  databases (`EXPLAIN` inside a transaction, rolled back). Both use index scans.
- **The sweep's SELECT was run against production** and returns 34 rows in a
  48h window.
- **The name fallback was run against 60 real unreadable domains with the real
  API** — real model calls, real money. 6 would block, all real estate, no
  false positives. 35 came back `unknown`. Output is in the PR body.
- **The three scrape failures were checked by hand, one at a time**, with the
  exact headers `attemptFetch` sends: `longandfoster.com` 403,
  `westexinsurance.com` no connection, `adrianadearaujorealtor.com` 200 with 64
  characters. None is fixable by fetching harder.

## What was NOT verified — read this before trusting it

- **The sweep has never run end to end against a real database.** The SELECT
  was run against production and the UPDATE was `EXPLAIN`-ed, but no row has
  ever been stamped by it.
- **`slackNonIcpLateBlock` has NEVER FIRED.** Not once, not in staging, not by
  hand. CLAUDE.md says every alert path is fired once on purpose before launch
  and that "we asserted it alerts" and "we watched it alert" are different
  claims. **This one has only been asserted.** It should be fired through
  `tools/fire-non-icp-slack.js` — which does not yet know about it — before
  anyone relies on it.
- **The booking guard's new branch has never run in production.** It is
  executed in tests against a stub; no real late verdict has gone through it.
- **No lead has ever been blocked by the name fallback.** The 60-domain run
  classified domains; it did not block anybody, because the flag is off.
- **The Model tab has not been opened in a browser** since the source-value
  change. The bucketing and labels are asserted through a stubbed DOM only.
- **The AWS mirror write has never run.** `syncNonIcpBlockToAWS` was
  `EXPLAIN`-ed against `gw_form_leads`, never executed.

## The defect I nearly shipped in the card instead of the code

Writing this card is what found it. Adding `llm_name_only` and `llm_late`
**re-scoped every consumer that compared against the literal `'llm'`**, and I
had updated none of them:

- `nonIcpSourceShort` would have labelled a model block **"Brand list"** to an
  SDR, and the sentence underneath would have said the domain is on a list it
  is not on. A blocked lead is not in Salesforce and not on the SDR list, so
  that tooltip and the Slack post are its only human-facing record.
- All three Model-tab buckets would have counted an `llm_late` block under the
  brand list.
- The ladder's `checked_clear` arm counted a name-only classification as no
  classification.

Fixed in `d2b08ce` with one helper, `nonIcpSourceIsModel`, and a test that
forbids `non_icp_source === 'llm'` anywhere. **This is the "a second column is
not additive" lesson arriving as a second enum value**, and it got past the
first three commits.

## Judgement calls that need a second opinion

**1. `realtor.com` blocks at 0.98.** It is a listings portal, not a brokerage —
by the prompt's own rule, software sold *to* realtors is `software_technology`.
But a lead who types `realtor.com` as their website is an agent without their
own site. I think blocking is right. It is the one verdict in the 60-domain run
I would want you to agree with rather than accept.

**2. The sweep marks and posts; it never cancels.** Authorised shape, 23 Sept
2026. Option (c) — auto-cancel — was rejected as the most aggressive action in
the codebase taken on a model verdict with nobody in the loop. If you want
cancellation, that is a separate decision.

**3. Brand-list additions.** `longandfoster.com`, `globelife.com`, `ailife.com`,
`brightway.com`. `primerica.com` deliberately **not** added — financial advisory
is in ICP by name. `windermereca.com` not added: the only safe catch-all would
be a `windermere` prefix rule that also hits Windermere Dental.
`deleyorganizationglobelife.com` not added: it *ends with* `globelife.com` but
is not a subdomain, so catching it needs substring matching, which is the
`paycompass.com` trap.

**4. The name fallback rests on two different abilities.** Reading a word out of
a hostname is reliable. Recalling that Long & Foster is a brokerage is memory,
with a training cutoff and no way to verify — there is no web search in this
call, and I confirmed that from the request body. Measured: the model reports
lower confidence when recalling (`howardhanna.com` 0.85, below the floor, did
not block) and returned `unknown` for an invented domain rather than inventing a
business. The 0.9 floor sits between the two abilities roughly where it should.

## Blocking / Meta behaviour — BOTH MOVE

**Changes 1–3 are live on merge.** More insurance and real-estate leads are
turned away and stop firing Meta, roughly **+19/month** on the measured rate.

- The booking guard now withholds `Schedule` for late verdicts.
- The sweep stamps `non_icp_blocked`, which drops those leads out of Salesforce
  pushes and the SDR list.
- Four more brand domains block.

**Change 4 does nothing until `NON_ICP_NAME_FALLBACK=true` is set in Railway.**
Not set as of writing.

**The Meta `Lead` event sent at `/submit` cannot be recalled.** Nothing here
recovers one; these changes only reduce how often the cache is cold at submit
time.

## Known flake, not from this branch

`test-non-icp-routes.js` intermittently reports `NO SUMMARY` under `measure.js`
while passing 449/449 alone. **Reproduced on clean `main` with these changes
stashed.** It matters for mutation testing, where an unmeasured suite must never
be read as a catch.

## What this does not fix

**Nothing here would have caught Peter Thomas**, the lead that started this. He
typed `no.com` because the website field is mandatory, and there is no business
in that name to read. That needs the "I don't have a website" option and the
about-business question on `/start` — a form change, waiting on Swapnil.
