# gushwork-api

Inbound lead capture, verification and routing for gushwork.ai. Node + Express on
Railway, Postgres, no build step. `index.js` is ~6,000 lines and holds most of the
system.

Explain things in plain language — no jargon, no making things sound more complex
than they are.

**Working on the monitor dashboard (`/monitor`, `/monitor/next`)? Read
`docs/monitor-plan.md` FIRST.** It holds:
- the plan (PRs A–D) and where it stands;
- the decisions waiting on Darshil;
- his rulings;
- the rules learned the hard way.

It is kept current, so a new session can resume from it.

---

## Deploying

`git push` to `main` → Railway builds and deploys. **There is no staging and no CI.**
A push is a production release.

So: work on a branch, run the tests, and only merge when they pass.

### The working agreement: branch, push, PR, WAIT

**Never merge to `main` without being told to, in that message.** Not "the tests
are green", not "the card is written", not "it's behind a flag". The merge is
Darshil's call and it is made on a diff he has read.

The flow, every time:

1. Work on a branch.
2. Run the full bar — `node tests/measure.js --check`, bare, never piped.
3. **Push the BRANCH** (`git push -u origin <branch>`), never `main`.
4. **Open a PR** (`gh pr create`), so there is a diff to review rather than a
   description of one.
5. Write the review card to `cards/` and say it is ready.
6. **Stop.** Merge only when Darshil says merge, in words, in a later message.

Two things this is guarding against, both of which have already happened here:

- **A green bar is not a review.** PR 51 and PR 52 both merged with
  `test-batch2.js` failing, because the bar was piped and nobody read it. A PR
  gives a second surface where that is visible.
- **A push to `main` deploys instantly.** There is no staging, so "merged but
  not released" does not exist in this repo. The moment it lands on `main` it is
  serving real leads, which is why the merge and the release are one decision
  and one person's.

**`Bash(git push *)` is allow-listed in `.claude/settings.local.json`**, so a
push is not gated by a permission prompt — the discipline is the agreement, not
the tooling. If a push is ever refused anyway, run it as a BARE command rather
than chained after other git commands with `;`; a compound command does not
match the allow rule cleanly and falls through to the classifier.

```bash
node tests/test-batch1.js       # logic, no dependencies
node tests/test-batch2.js       # logic, no dependencies
node tests/test-batch-a.js      # logic, no dependencies
node tests/test-ads-parity.js   # the two form files against each other, no dependencies
node tests/test-partnerstack.js # PartnerStack steps 1-10, no dependencies
node tests/test-sf-readers.js   # EXECUTES the Salesforce readers against a stubbed fetch
node tests/test-submit-gate.js       # BOOTS /submit and watches all five announcements fire
node tests/test-session-payload.js   # EXECUTES the real form-file functions, both files
node tests/test-session-page-views.js # BOOTS /session, incl. what happens when the write fails
node tests/test-lead-field-changes.js # BOOTS /partial + /submit, and parses the dashboard JS
node tests/test-non-icp.js           # the non-ICP block: list, matcher, guards, the disqualified AUDIT,
                                    #   and section 13, which EXECUTES the model classifier against a
                                    #   stubbed fetch — fail-open, the enum, injection handling
node tests/test-non-icp-routes.js    # BOOTS every /monitor route AND evaluates the dashboard JS
node tests/test-apollo.js            # BOOTS /enrich against every shape Apollo replies with, and drives
                                    #   tools/re-enrich-apollo.js against a stubbed database
node tests/test-monitor-next.js      # BOOTS /monitor/next and /monitor/overview, and EVALUATES the new
                                    #   dashboard's served JS: painted numbers against the payload
node tests/test-gads-upload.js      # the Google Ads upload: Google's normalisation, the click-ID choice,
                                    #   every exclusion with the REAL index.js gates, and BOOTS the app
                                    #   with the upload on to read back the one validate-only request
node tests/test-agency-exclusions.js # agency and our own leads never reach Salesforce, on EVERY Lead path
                                    #   (/submit, both safety nets BOOTED; the retry sweep and backfill
                                    #   executed), and the SDR list's mark and CSV

node tests/measure.js --check   # or just this: runs all sixteen and checks the totals
node tests/test-batch1-db.js    # needs DATABASE_URL
node tests/test-batch1-e2e.js   # boots the real server, needs DATABASE_URL
```

**The sixteen dependency-free suites are the bar.** They run anywhere in about a
second each — run all sixteen after any change to `index.js`, `lead-magnet.js`,
or either form file, always. Do not install Postgres and do not point anything at
the production database from a feature branch.

**NEVER PIPE `measure.js`. Not through `tail`, not through `grep`, not through
`head`. Run it bare and read all of it, or do not claim the bar.**

This is a hard rule, not advice, because **a truncated pass and a truncated
failure are indistinguishable**. The grand-total line is the LAST thing
`measure.js` prints, so any pipe short enough to be convenient shows a
plausible total with every per-suite result hidden above it. `tail -3` on a
green bar and `tail -3` on a red bar look identical — and the total does not
move when a suite fails, because a failed assertion still counts toward it.

`--save` compounds it: it re-baselines whether or not suites failed, so one
truncated read plus a `--save` records a red bar as the new normal.

It happened **three times on 10 Sept**, twice after this warning was already
written. Two of those shipped: PR 51 and PR 52 both merged with
`test-batch2.js` failing. Neither changed production behaviour, but neither bar
had actually been read.

If the output is genuinely too long to read, that is a reason to fix the
output, not to pipe it.

**Nine of the sixteen BOOT A ROUTE** rather than reading source text —
`test-submit-gate`, `test-session-page-views`, `test-lead-field-changes`,
`test-session-payload`, `test-apollo`, `test-monitor-next`, `test-gads-upload`, `test-agency-exclusions` and `test-non-icp-routes`. The last one goes furthest:
it also **evaluates the dashboard's inline JavaScript** in a stubbed DOM and
calls every tab loader, because three production breaks in one night were
runtime behaviour no source assertion could see. They stub `pg` and `global.fetch` and drive the real
express stack over HTTP, so they need no database and no network. They exist
because a source assertion cannot tell a reachable statement from an
unreachable one, and cannot tell you whether the dashboard's inline JavaScript
parses at all.

Tests read the real functions out of `index.js` rather than a copy. A test that
exercises a duplicate of the source can pass while production is broken. Keep it
that way.

**All sixteen suites require `tests/crash-reporter.js` first, and it is not
optional.** A suite that crashes prints a stack trace, zero `✗` lines and exits
1 — which reads as a clean run to anything counting markers and as a caught
mutation to anything counting exit codes. Three of the six did exactly that
before 7 Sept 2026, so every mutation result measured by counting markers may
have been counting crashes. The reporter turns any escape into a `✗` line plus
an explicit `SUITE DID NOT COMPLETE` marker, and deliberately prints no totals.

### Mutation testing: use `tests/measure.js`, do not count markers

**Do not measure a mutation by counting `✗` lines or by looking at the exit
code.** Both were the practice here until 7 Sept 2026 and both are broken:

```bash
node tests/measure.js --check       # the normal bar: all ten suites + totals
node tests/measure.js --mutation    # after breaking a line, for a verdict
node tests/measure.js --save        # re-baseline, when a count intentionally moves
```

**A mutation is CAUGHT only if the suite COMPLETED and FAILED and ran its USUAL
NUMBER OF ASSERTIONS.** All three, and the third is the one every ad-hoc check
omits. `--mutation` applies the rule and prints `CAUGHT` / `survived` /
`UNMEASURED`; it exits non-zero on `UNMEASURED`, so a run that could not be
measured can never be recorded as a catch.

`failed > 0` alone is not enough: `test-ads-parity.js` once reported one
failure while silently running **13 of its 159** assertions, which passes every
check based on exit code and failure lines.

**An assertion about ORDER is not an assertion about REACHABILITY.** Roughly
17 assertions in these suites check that X happens before Y by comparing source
character offsets — including claim-before-send on both PartnerStack payment
paths. Every one of them survives an early `return` above the guarded
statement, an `if (false)` around it, or a new wrong guard clause placed above
it, because none of those move an offset. Measured, not assumed: three of three
reachability mutations on the money-path guards survived. So pair the ordering
assertion with a reachability one — assert the enclosing branch is still
guarded by the condition it should be, or drive the function and assert the
call happened. See
`docs/tickets/ordering-assertions-do-not-check-reachability.md`.

**A review card is trusted rather than checked, so a wrong one is worse than a
missing one.** These cards are the record of what was verified and they get
believed. Before writing one: re-read what you actually ran, and do not carry a
claim forward from a previous card without re-checking it. This has already
gone wrong once — PR 25's card said an earlier fix had been made "in passing"
when it had not, because a boot run had been added to a *different* sweep and
the two were conflated. State plainly what was verified, what was asserted only
structurally, and what has never executed; correct an earlier card in the next
one rather than leaving it standing.

**And treat the mutation tallies in review cards and commits before 7 Sept 2026
as UNVERIFIED rather than as evidence.** They were produced with the broken
measurement — three of the six suites crashed with zero markers, so a crash and
a catch were indistinguishable. The catches were probably real; they were never
verified, and which runs crashed cannot be reconstructed. The full account,
including the measured per-suite table, is
`docs/tickets/mutation-testing-measurement-was-broken.md`.

`tests/.baseline.json` is committed on purpose: a baseline nobody can see is a
baseline nobody checks.

**`test-sf-readers.js` is the one that EXECUTES rather than reads.** The others
assert on source text, which keeps them pointed at the real code but cannot
tell you whether it works. That suite requires `salesforce.js` — which needs no
database and no server — and drives it against a stubbed `global.fetch`, so
pagination, the completeness refusals, the `ok: false` branches and the SOQL
that actually goes over the wire are all exercised. Three defects it catches
are invisible to every source-based assertion: a `nextRecordsUrl` resolved
relative instead of against the instance, `records = map(data)` instead of
`concat`, and a paged request that drops the bearer header after page one.

When you add a Salesforce reader, it goes in there too.

---

## Style

Comments explain **why**, anchored to the incident that caused the code. Real
examples in the file: the spam lead that booked without submitting, the Shenzhen
manufacturer blocked by a DNS-over-HTTPS failure, the SiteGround captcha that looked
like a thin page. Match that. A comment restating what the line does is noise; a
comment naming the lead that broke is worth keeping.

Keep the existing structure. This is one large file on purpose for now — don't
reorganise it as a side effect of another change. Splitting it up is a real idea,
but a deliberate one, on its own.

Plain, direct language in Slack alerts and dashboard labels. They're read by SDRs,
not engineers. "Domain registered but no website on it", not `parked_confirmed`.

---

## Working with Darshil

- Say what you changed and what you did **not** change.
- Flag anything that alters blocking or Meta behaviour, loudly.
- If a fix has a half you can't do here (anything in the two frontend files), say
  which half is missing rather than implying it's complete.
- Don't claim something is verified unless it was actually run. "Syntax checks" and
  "tests pass" are different statements.
- When you're unsure whether something is a bug or intentional, ask. This codebase
  has a lot of deliberate-looking oddities that really are deliberate.
- Never print the Google Ads key at `~/.config/gushwork/gads-sa.json`, or the value of any `GADS_` variable.

---

## Where the background lives

On 7 Oct 2026 this file was cut from about 172k characters to about 122k, under the 150k load limit, by MOVING history, measurements and worked examples into `docs/background/`, word for word, never deleting them. Every warning that moved left a one-line rule behind, in place, with a link to its background. When you add to this file, put the rule in this file and the story in `docs/background/`.

- `docs/background/non-icp.md`: The non-ICP block's name-only fallback, floors and history; the `disqualified` audit's three waves; the async Schedule guard
- `docs/background/meta.md`: The `x-forwarded-for` bug Meta was sent, and how it was proved
- `docs/background/webflow.md`: Every correction to the pin process (per-page tags, head pins, page and block counts), what headless Webflow editing can and cannot do, the stale Designer tree
- `docs/background/product-and-forms.md`: The /demo "What are you looking for?" question, product vs product_interest, router attributes, the collapsible CSS and its measurements, the `/ai-crm` catalogue miss, the about-business cap
- `docs/background/attribution.md`: The shared cookie namespace, the in-app browser loss, and the Salesforce `Source_Bucket__c` formula fixes
- `docs/background/testing.md`: Source assertions vs runtime, the confident zero, the SQL that passed six suites, the hand-run API read that nearly double-paid
- `docs/background/alerting.md`: Apollo out-of-credit refusals, how Meta CAPI failures reach `recordFailure`, the four Meta auth codes
- `docs/background/dashboard.md`: Visitors (tiles, circles, networks, IP columns), the row-builder id bug, Model tab and Blocked counts, the CSV phone measurements
- `docs/background/internal-test-submissions.md`: Why our own submissions stopped leaving the building, `b@g.ai`, the staging host
- `docs/background/dropoff-and-digest.md`: "Left on step 2", the source fields, leads vs people, the bucket-as-text bug, the weekly digest
- `docs/background/partnerstack.md`: Partner identity, the revenue-gaps queue, the poller and its intervals, `sf_state`, `Partner_Source__c`, eligibility rules (a) and (b). The handover is still `docs/partnerstack.md`
- `docs/background/definitions.md`: The two completed-not-submitted populations, the internal-submissions measurement, the `/careers` session rows

`node tools/check-claude-md-move.js b924ce7` proves the move: every non-blank line of `CLAUDE.md` at that commit appears exactly as many times across today's `CLAUDE.md` and `docs/background/`, and every line that is new is printed.

## The one rule everything else follows

**A lead is worth more than a verdict.**

Every check in this system exists to add information, never to stand between a real
person and a demo booking. When a checker breaks, times out, gets rate limited, or
hits something it doesn't understand, it **fails open** — the lead goes through and
the check reports that it couldn't decide.

Three things follow from that, and they are not negotiable:

1. **"We could not check" is never recorded as "we checked and it is bad."**
   A DNS resolver failure is not a fact about someone's domain. A captcha wall is
   not a thin page. A timeout is not a dead site. Each of these gets its own
   verdict that says what actually happened.

2. **Blocking is the highest-risk action in the codebase.** Three *website
   verdicts* block, all decided client-side in `gushwork-form.js`: `nxdomain`,
   `brand_mismatch`, `mailbox_domain`. Do not add a fourth without asking.

   **There are now TWO server-side blocking mechanisms, and they are the
   exception that proves this rule rather than a loosening of it.** Both are
   decided by `nonIcpVerdict` in `index.js`, stamped on
   `leads.non_icp_blocked`, and enforced in `/partial` and `/submit`.

   1. `NON_ICP_BLOCK` (default OFF, **`true` in Railway**) — the
      **brand-domain list**. A pure string comparison against the national
      real-estate brokerage and insurance carrier domains in
      `NON_ICP_DOMAINS`. **Checked FIRST**, because it is deterministic,
      re-derivable by reading a list, and it still works when a site
      refuses a scraper. This said "41 domains" and was already wrong by
      one before four more were added on 23 Sept; **count the constant,
      do not trust a number written down here.**
   2. `NON_ICP_LLM_BLOCK` (default OFF, **`true` in Railway**) — the
      **model layer**. Reads the company's own website and classifies it
      into one enumerated `business_type`; exactly two of those block,
      `real_estate` and `insurance`. Stamped with `non_icp_source='llm'`.

   **The model layer has a second, weaker input since 23 Sept: `NON_ICP_NAME_FALLBACK`** (default OFF, **`true` in Railway**). It runs ONLY when the scrape failed and judges the domain name alone, with its own floor, its own `non_icp_source='llm_name_only'` and prompt version; `unknown` returns null. **Its floor is 0.85, set by Darshil on 26 Sept 2026, down from a provisional 0.9** (a page verdict's floor is 0.75). **One floor per kind of evidence serves BOTH block and Meta**, picked only by `nonIcpFloorFor(source)`: until 26 Sept the Meta-only read used the page floor for every verdict, so `hernandezins.com`, insurance at 0.82 from the letters "ins", still withheld Meta. Blocking is read at decision time from the type and its floor, as well as from the stored `blocking` column, so a floor change reaches the cache at once. Name-only verdicts are cached six hours, not 180 days. The cache is keyed by DOMAIN, never the company field the visitor typed. **Neither mechanism replaces the other and the brand list is NOT being retired**; an earlier version of the V1 ticket said it would be, and the CORRECTION section at the end of `docs/tickets/non-icp-v1-block.md` records why that was wrong. **The non-ICP block as a whole** was authorised by Swapnil on 11 Sept 2026 and reverses two written positions in the Non-ICP doc (same ticket). Measurements and history: `docs/background/non-icp.md`.

   Everything else here still holds. The block **fails open** on any throw,
   timeout or cold cache, it checks the warehouse customer tables first so a
   paying customer is never blocked, and it is the only server-side check that
   blocks. Every other server-side check still informs and never blocks.

3. **Suppressing a Meta event is a real cost, not a safe default.** It removes a
   conversion signal from the ad algorithm. Treat "should this fire Meta?" as a
   business decision to surface, not a judgement call to make quietly.

   **THREE things gate Meta today, and every one was surfaced as a decision
   rather than taken quietly.** `isWebsiteVerified` is the oldest. The second
   is `non_icp_blocked`: a blocked lead fires none of `StartTrial`, `Lead` or
   `Schedule`. That was the explicit point of the change — an ad audience
   optimised toward realtors is what produced the complaint — and it was
   authorised on 11 Sept 2026.

   **The third is `NON_ICP_LLM_META`, and it is a SEPARATE FLAG from
   `NON_ICP_LLM_BLOCK` on purpose.** The model layer can run in flag-only
   mode: it stamps `non_icp_llm_flagged`, the lead reaches the calendar and
   goes to Salesforce exactly as today, and Meta is untouched. Switching on
   observation must never reshape an ad audience as a side effect — that is
   this rule, applied to the switch itself. `NON_ICP_LLM_BLOCK` implies
   `NON_ICP_LLM_META`, because a lead we turn away that still feeds the
   algorithm a conversion is incoherent.

   **And two more gate it by WHO the lead is, not what the business is.**
   Our own test submissions (`internalLeadSuppressesMeta`, 19 Sept, five call
   sites) and, since 7 Oct 2026, **agency domains — `META_EXCLUDED_DOMAINS`,
   default `flighted.co,uprawmedia.com`**. The agency check lives INSIDE
   `sendEvent` in `meta-capi.js`, which every Meta event passes through
   (StartTrial, Lead, all three Schedule routes, and the lead magnet's
   Contact), so no call site can miss it. It matches the email's domain OR
   the website's host, exact or subdomain, never a substring; the env extends
   the defaults and cannot shrink them. A skip returns `{ skipped }`, neither
   a failure (no alert) nor a success (no streak reset), and
   `meta_predicted_ltv` is not stamped for those leads.
   `INTERNAL_TEST_EMAILS` / `ELV_EXCLUDED_DOMAINS` are unchanged.

   **ONE AGENCY LIST SINCE 7 OCT 2026: `AGENCY_DOMAINS`** (`agency-domains.js`,
   default `flighted.co,uprawmedia.com`). Meta, the Google upload, Salesforce
   and the SDR list all read it; `META_EXCLUDED_DOMAINS` and
   `GADS_EXCLUDED_DOMAINS` still work and EXTEND it for their own system
   only, so with it unset every list is exactly what shipped before. Add an
   agency to `AGENCY_DOMAINS`, not to one system's list. **Salesforce** skips
   agency leads (and our own test submissions) INSIDE `pushToSalesforce`, so
   `/submit`, the retry sweep, both booking safety nets and `backfill-sf.js`
   are all covered; a skip is a RETURN (`{ skipped }`), recorded as neither
   synced nor failed (the retry sweep takes it off the queue with
   `sf_sync_retryable = false`). The **SDR list** marks agency rows and leaves
   them out of its CSV. **Not** the AWS mirror and **not** the dialer:
   `sdr-calling` keeps its own deny list (it already has both agencies, by
   email domain only). Measured before it
   shipped, by replaying the gates: 7 agency leads in 90 days fired about 7
   StartTrial, 4 Lead and 3 Schedule. **The dashboard's "why was Meta
   withheld" filter (`metaWithheldReason`) does not know this reason yet.**

   **Read `suppress_meta` off the verdict, never `blocked`.** A flagged
   lead is not blocked and still has to stop firing Meta when META is on.
   Three call sites read it: `/partial` (StartTrial), `/submit` (Lead), and
   `nonIcpScheduleSuppressed` (Schedule, for all three booking routes).

   **`Schedule` fires from THREE call sites, so it needs three guards.** They
   are `/booking-confirmed`, `/booking-confirmed-webhook` and
   `/booking-confirmed-webhook-rh`. Counting event *names* and concluding
   "three call sites" is wrong and is how one leaks: there are six in the
   repo, five in `index.js` plus `Contact` in `lead-magnet.js`.

   **The three guards are now ONE function, `nonIcpScheduleSuppressed`,
   called three times.** They were three copies reading `non_icp_blocked`
   off `SCHEDULE_LEAD_SQL`, and they stayed in step by luck. Adding the
   second condition (the model flag) to a guard that exists in triplicate is
   exactly when the third copy gets missed, so the condition moved into one
   place. `tests/test-non-icp.js` section 8 both asserts the three call
   sites and **executes** the function, which is what closes the
   reachability hole an `if (false)` inside it would otherwise leave.

   **`nonIcpScheduleSuppressed` is async since 23 Sept and also re-reads `non_icp_domain_verdicts`**, because the column stamped at `/submit` says false whenever the verdict lands seconds later. It fails open on any throw, withholds only the Meta `Schedule` event, and all three routes `await` it; `tests/test-non-icp.js` section 8 builds it through the AsyncFunction constructor. Background: `docs/background/non-icp.md`.

When a change would alter which leads get blocked or which fire Meta events, say so
explicitly in your summary. Never let that happen as a side effect.

---

## Layout

Every file in the repo root, so this list can't quietly go stale the way it did
before — a file missing from here reads as "forgotten," not "not documented yet."

| File | What it's for |
|---|---|
| `index.js` | Routes, website checking, email verification, alerting, the monitor dashboard, cron |
| `db.js` | Schema + migrations. Runs on every boot; everything is `IF NOT EXISTS` |
| `salesforce.js` | Lead upsert by email. Refresh-token OAuth |
| `meta-capi.js` | Conversions API — `Lead`, `Schedule`, `StartTrial`, `Contact`. Also owns the product catalogue (`PRODUCTS`, `resolveProduct`), which `index.js` imports |
| `agency-domains.js` | The ONE agency list, `AGENCY_DOMAINS` (default Flighted and Upraw), and the host-matching helpers every list uses: exact or subdomain, never a substring. Read by Meta, the Google upload, Salesforce and the SDR list |
| `google-ads-conversions.js` | Google Ads offline click conversions for booked Google Ads leads, through the **Data Manager API** (`events:ingest`), not the Google Ads API. Shaped like `meta-capi.js`; everything `index.js` owns is INJECTED into `createGadsUploader`. OFF and validate-only by default. Writes only `gads_conversion_uploads` |
| `loops.js` | Loops.so contact push for the lead-magnet landing page |
| `partnerstack.js` | PartnerStack API. TWO hosts and TWO auth schemes: `partnerlinks.io` conversion (Bearer tracking token) and `api.partnerstack.com` v2 partnerships + actions (Basic public:secret) |
| `lead-magnet.js` | `/lm/*` routes. Separate table, deliberately not joined to `leads` |
| `backfill-sf.js` | Manual recovery tool for re-syncing leads to Salesforce after a broken connection or outage. Not mounted by default — see below |
| `tools/non-icp-validate.js` | Scores the model layer against history — scrapes every lead domain once, replays the same bytes to three models, joins to paying customers and showed-up bookings. LIFTS the real classifier out of `index.js` rather than copying it. Not mounted, run by hand |
| `tools/fire-non-icp-slack.js` | Fires the non-ICP Slack paths for real: `blocked`, `llm-blocked`, `llm-meta`, `late-block`, `booking`. Lifts them out of `index.js` like `fire-alert.js`. `late-block` is the sweep's post and was fired by hand on 23 Sept — a stamped lead is NOT in Salesforce and NOT on the SDR list, so that post is the only thing telling a human a booked meeting turned out non-ICP. Not mounted, not called |
| `tools/fire-dropoff-digest.js` | Sends the weekly dropoff digest for real, to the **leads** channel. **Its own file on purpose:** it first went into `fire-non-icp-slack.js` because that tool already had the lifting machinery, which is the wrong reason — the digest has nothing to do with the non-ICP block, and a tool named for one feature that quietly fires another is how nobody finds either later. **The only fire tool that reads a DATABASE**, so on a developer machine it needs the Postgres service's `DATABASE_PUBLIC_URL`, not `gushwork-api`'s private-network `DATABASE_URL`. The numbers are real and only the timing is not, so its marker says that rather than calling the data a test — and the marker is appended AFTER the digest is composed, because the message must stay byte-identical to what Monday sends. **`liftDecl` cannot take `DROPOFF_STAGES`** (it brace-matches one declaration and truncates an array of objects at its first element), so this lifts by REGION with a `between()` helper. Not mounted, not called |
| `tools/sf-mark-internal-test-leads.js` | Marks our own test submissions as **Invalid / Test** in Salesforce, by writing the `How_Did_You_Hear__c` the `Source_Bucket__c` formula already reads. **Marks, never deletes** — 141 Lead records match `isInternalLead` and ~100 are NOT ours. Provenance comes from `tools/internal-test-emails.json`, not from the address. Dry run by default; `--apply` writes a manifest and `--revert` undoes it. Not mounted, run by hand |
| `tools/internal-test-emails.json` | The provenance list for the tool above: addresses our OWN form actually submitted. Point-in-time, carries the SQL that regenerates it |
| `tools/re-enrich-apollo.js` | Re-runs the Apollo lookups that were REFUSED — the three out-of-credit windows (24 Jun, 3–10 Sept, 23 Sept on). Dry run by default: prices it (1 credit per person FOUND, 0 for no match), one lookup per address, copies from an earlier answer for the same address at zero cost, skips our own submissions. `--since`, `--limit`, `--apply`. **Stops at the first refusal.** Writes `enrichment_data` and the lead row exactly as `/enrich` does; never Salesforce, never the mirror. Lifts the parser out of `index.js`. Not mounted, not yet run |
| `tools/sync-enrichment-out.js` | Carries re-enriched Apollo fields OUT to the AWS mirror (`gw_form_leads`, the dialer feed) and Salesforce, for the sessions `tools/re-enrich-apollo.js` rewrote since `--since`. **Fill-only**: writes a field only where the destination is blank, touches no other column, creates no row, never writes a converted Lead. Mirror by targeted `UPDATE ... WHERE session_id`, never `syncToAWS`. Salesforce field names from `salesforce.js`'s map, types and lengths from Salesforce's describe, writes through `updateSFLead`. Dry run by default; `--apply`, `--mirror`, `--salesforce`. Run once on 26 Sept 2026 after the backfill. Not mounted |
| `tools/backfill-ip-coords.js` | Fills `ip_latitude` / `ip_longitude` for leads resolved BEFORE those columns existed — they have a city and no point, so they are complete in every table and invisible on the map. Only touches rows that already resolved and have no coordinates. Lifts `resolveIpGeo` out of `index.js`. Dry run by default; `--apply` writes. Run once on 23 Sept (17 rows). Not mounted |
| `tools/agency-exclusion-dry-run.js` | Read-only count of what the Salesforce exclusions (agency and ours) would stop over a window, how many already reached Salesforce, and agency rows on the SDR list. Uses the real `salesforceSkipReason` with the real `isInternalSubmission` lifted from `index.js`. Counts only. Not mounted |
| `tools/gads-upload-dry-run.js` | What the Google Ads upload WOULD send over a window (default 90 days), and why each other booking is skipped. Runs the module's own `dryRun` with the real gates lifted out of `index.js`; `BEGIN TRANSACTION READ ONLY`, rolled back, and a fetch that throws. Counts only. `--days`, `--free-email`. Not mounted |
| `tools/fire-alert.js` | Fires ONE real alert on purpose, to satisfy the fire-every-alert-path-once rule. Sends for real (Slack + email on a critical). Lifts `alertOps` out of `index.js` rather than reimplementing it, so what arrives is what production sends. Not mounted, not called by anything |
| `monitor-next.js` | Builds and serves THE dashboard at `/monitor` (since PR D, 26 Sept 2026). `/monitor/next`, where it was built side by side, redirects there keeping its query. Reads `monitor/` once at boot and stitches one page behind the same token -- no build step. Also the token-gated font route, an allowlist, never a path. The OLD dashboard is `/monitor/classic` in `index.js`, kept one week as the fallback |
| `monitor/` | The new dashboard's front end, in real files: `tokens.css` (the design system's tokens, copied verbatim from gushwork-design v1.49.0), `app.css` (both themes, components, responsive), `js/*.js` (classic scripts on one `GW` namespace, loaded in `JS_ORDER`), `icons/` (the Phosphor icons it uses, MIT), `fonts/` (Inter, Vert Grotesk Display) |
| `tools/preview-monitor.js` | Runs THIS BRANCH's `/monitor` against live data: its own `overviewReport`, `duplicatesReport` and `dropoffReport` lifted out of `index.js` on connections that are read-only AT THE DATABASE, `/monitor/classic` from production (from `/monitor` before the switch deployed), every other `/monitor/*` GET proxied to production, every non-GET refused. Never boots `index.js`. Not mounted |
| `tools/check-monitor-layout.mjs` | Real Chrome over every rebuilt tab and view at 360, 390, 414, 768, 1024, 1280 and 1440px in both themes (1280 because it lands in the 900-1099px content band where optional columns hide); FAILS on sideways scroll, anything off-screen or clipped, a tap target under 44px, overlapping chart labels, a floating element, console errors, junk values or a missing font, a data table (rtable OR plain) wider than its card -- and, with REAL key presses, on the skip link, focus kept across a repaint and the drawer closing when focus leaves. `tab+open` also expands the first rows, so detail panels are measured too. **Screenshots go to the OS temp dir, never the repo** -- the opened rows photograph real people's details and the repo is public -- and its Chrome profile is thrown away on exit. Run against the preview. Not mounted |
| `tools/crosscheck-monitor.mjs` | The new dashboard against the classic, number for number, on live data through the preview: every payload fetched once and served to both pages, then each page's painted numbers compared with the payload and each other, plus the partitions every `/monitor/leads` filter must keep. Prints numbers only, never lead data. Not mounted |
| `gushwork-form.js` | The `/demo` form frontend. Lives here and is served live by jsDelivr — see below |
| `gushwork-form-popup.js` | The Google Ads popup/modal form frontend. Lives here and is served live by jsDelivr — see below |
| `package.json` | Dependencies, scripts, Node engine constraint |
| `package-lock.json` | Locked dependency versions, committed so Railway installs exactly what was tested |
| `.gitignore` | Keeps `node_modules/`, `.env`, logs, and local Claude settings out of the repo |
| `README.md` | Repo landing blurb, not living documentation. This file is |
| `tests/` | The test files described under Deploying, plus `crash-reporter.js` (required first by every suite), `measure.js` (the test bar and the mutation-testing rule) and the committed `.baseline.json` |
| `docs/partnerstack.md` | PartnerStack handover: the two-step model, every ps_ column, env vars, test procedure, known gaps |
| `docs/monitor-plan.md` | The dashboard rebuild's handoff: PRs A–D, current status, decisions waiting on Darshil, his rulings, learnings, and exactly where the next PR starts. **Read first for any dashboard work** |
| `docs/OPEN-ITEMS.md` | What is still open or deliberately decided in THIS repo, as of 9 Sept 2026. The meta-capi repo has its own; neither is complete alone |
| `docs/tickets/non-icp-v1-block.md` | The non-ICP block: what the Non-ICP doc says, the two positions this reverses, the domain list with per-domain evidence, and the `sdr-calling` dependency |
| `docs/tickets/non-icp-verdict-arrives-after-submit.md` | Why a correct model verdict can land after the lead has already booked, the four options considered, and the measurement that reframed it — booking→demo median is 35 hours, so there is no 2.6-second race to win. Records that the calendar HOLD was built and the sweep was not, and that the sweep landed on 23 Sept |
| `docs/Non-ICP-flagging-rules-*.pdf` | Swapnil's Non-ICP flagging rules, as exported. **A screenshot with no text layer** — it does not grep. The ticket above quotes the parts that matter |
| `docs/background/*.md` | History, measurements and worked examples MOVED out of this file on 7 Oct 2026, one file per topic. The rules stay here, each linking to its file; see "Where the background lives" |
| `tools/check-claude-md-move.js` | Proves the 7 Oct 2026 move lost nothing: every non-blank line of the old `CLAUDE.md` appears exactly as many times across the new one and `docs/background/`. Read-only. Not mounted |
| `CLAUDE.md` | This file |

**`gushwork-form.js` and `gushwork-form-popup.js` are in this repo, not a separate
one,** and jsDelivr serves both from it. **They are pinned to a commit SHA, not to
`main`** — see below, because it changes what "deploying a form fix" means.
`darshildixit.github.io/gushwork-embeds` is a genuinely different repo — it holds
the Webflow CSS/JS embeds, not these two files. Don't confuse the two.

### Deploying a form change — the Webflow step

**A `git push` does NOT ship a form change.** The two form files reach production
through a `<script src>` that names an immutable commit SHA:

```
https://cdn.jsdelivr.net/gh/DarshilDixit/gushwork-api@<40-char-sha>/gushwork-form.js
https://cdn.jsdelivr.net/gh/DarshilDixit/gushwork-api@<40-char-sha>/gushwork-form-popup.js
```

**The tags are PER PAGE, in each page's own custom code, not in Project Settings**, and there is no single place to edit. Never ADD a tag in Project Settings: it would load the form script twice, at two SHAs, with no error. History: `docs/background/webflow.md`.

So shipping a form fix is **two** steps, and the second one is outside this repo:

1. Merge to `main` as usual.
2. Take the new `main` SHA (`git rev-parse HEAD`) and update the script tag in
   **each page's footer custom code**, then republish. Both files move together
   even though no page carries both — `/demo` and `/ai-demo` carry
   `gushwork-form.js`, the ten ad landers carry `gushwork-form-popup.js`.

Miss step 2 and the fix is in `main`, the tests pass, Railway has redeployed — and
every real lead is still running the old file. Nothing in this repo will tell you.

**Why SHA and not `@main`.** jsDelivr treats a SHA path as immutable and caches it
permanently, so it either serves those exact bytes or 404s. `@main` is a mutable
ref served best-effort, which needs a cache purge — and purges do not reliably
take. Pinning removes the purge from the process entirely.

**Use the full 40-character SHA.** Short SHAs work today but are ambiguous as the
repo grows, and a collision resolves to the wrong file rather than erroring.

**Scan BOTH custom-code blocks, head and footer, on EVERY page, through the API, and count pinned BLOCKS, not pages.** A pin can sit in the head (`/ai-demo`'s does), and the `curl` sweep below is a hand-kept list that has missed pages, drafts such as `/start-old` included. "No pin" on a page you know serves the form is a bug in the scan. Do not trust any page or block count written down, here or anywhere: run the scan. History: `docs/background/webflow.md`.

**AND RETRY THE READS.** A dry run over 85 pages hit `429 Too Many
Requests` on four of them and skipped them with a printed warning. On a
dry run that is noise; on an apply run it is a stale pin nobody will ever
notice. Back off and retry, sleep between pages, and make a read failure
fatal rather than a line of output.

**Repin through the Webflow site API, not the MCP tool**, which needs each page's whole custom-code block retyped: `GET /v2/pages/{page_id}/custom_code/freeform`, then `PUT .../freeform/{location}`, which is location-scoped (the collection path answers 404 to every write, which is not "no access"). Token scopes and details: `docs/background/webflow.md`.

**Don't create Webflow form fields headlessly.** A field's Name stores but never renders, its Placeholder cannot be set (visitors see "Example Text"), and a textarea needs the four page-level CSS rules `/ai-demo` keeps in its head code. What works headlessly and what does not: `docs/background/webflow.md`.

**AND SWEEP EVERY PAGE, not just the two you changed.** The pin lives in a
`<script src>`, and Webflow lets a *page* carry its own script tag that a
Project-Settings republish never touches. Two were found stale on 10 Sept, both
invisible from inside this repo:

```bash
for p in /demo /ai-demo /aeo /ai-crm /start /start-now /lead-gen /seo-leads \
         /consulting-lead-generation /consulting-seo-services \
         /financial-services-lead-generation /financial-services-seo \
         /manufacturing-lead-generation /manufacturing-seo-services; do
  echo "$p -> $(curl -s "https://www.gushwork.ai$p" | grep -oE 'gushwork-api@[0-9a-f]{7,40}' | sort -u | tr '\n' ' ')"
done
```

Anything not on the SHA you just pinned is serving different code to real
visitors. A page with **no** `@sha` at all is worse than a stale one: jsDelivr
then serves the default branch best-effort, so what a visitor gets depends on
that CDN's cache and drifts on its own.

**Pin both files to the same SHA**, even when only one of them changed. The bytes
would be identical either way — a commit SHA names a snapshot of the whole repo,
so asking for an untouched file at a newer SHA returns the same blob — but pinning
them together is the only thing that records that the pair was *tested* together.

**Confirming the swap actually took**: load the page and read the console banner.
`/demo` logs `Form initialised v…`, the Ads page logs `… (Google Ads)`. If the
version there is not the one you just merged, Webflow is still serving the old
pin — the deploy is not done, however green this repo looks.

**The console banner is only a check if the version moved.** On 16 Sept 2026 it did not, and it read `v5.13.0` on the old pin and the new one. Background: `docs/background/webflow.md`.

**So bump the version on EVERY form change**, even a one-line one. It costs
nothing and it is the only thing that makes the banner mean something. The
version lives in the `Form initialised v…` string in both files and must move in
both, like everything else in the fork.

**THE BUMP IS FIVE EDITS, NOT THREE — CORRECTED 22 SEPT 2026.** This said
three and it was wrong by two, because the version does not live only in the
banner: each form file carries it in a **header comment at the top** as well.

| Where | Count |
|---|---|
| `Form initialised v…` banner, one per form file | 2 |
| Header comment at the top of each form file | 2 |
| The pinned assertion in `tests/test-partnerstack.js` | 1 |

`test-partnerstack.js` asserts the banner reads the current version in both
files. Bump the files without bumping the assertion and the bar goes red on
`main` — which has happened here before, and sat there for days as a red bar
nobody read rather than as a caught bug.

**The two header edits are caught by a DIFFERENT test**, and that is the only
reason this correction exists: `test-ads-parity.js` asserts *"init banner
agrees with the header"* in each file, so bumping the banner alone fails with
`header=v5.16.0 banner=v5.17.0`. Found on the v5.17.0 bump by following this
very paragraph and getting a red bar. Do all five in one commit.

**And when the version did not move, these two are what actually discriminate:**

1. **The SHA in the page HTML** — `curl` the page and read `gushwork-api@<sha>`.
   Exact, and it is the thing you just changed.
2. **A distinctive string in the file that SHA serves** — fetch the jsDelivr URL
   and grep for something only the new code has. For the phone fix that was
   `initialCountry: 'auto'` against the old `initialCountry: 'us'`.

Check 2 matters on its own: check 1 only proves the page asks for the right
bytes, not that jsDelivr can serve them. Fetch the new SHA BEFORE editing any
page — a pin to a SHA jsDelivr 404s takes the form off every page at once.

**`publish_site` IS ASYNCHRONOUS, and the first sweep after it will lie.**
Webflow returns success immediately and then compiles; on 16 Sept the live pages
still served the old HTML for about two minutes after the call returned. Poll the
page until the SHA flips rather than reading one red sweep as a failed publish
and starting to undo things.

**Publishing ships the WHOLE SITE, not just your pages.** Single-page publishing
needs Enterprise, which this site is not on — the same entitlement that blocks
branching. So a republish also releases anything anyone else has staged in the
Designer since the last publish. Check `lastUpdated` against `lastPublished` on
the site record first, and if they differ, ask whose work is in there before
publishing.

**`backfill-sf.js` is a kept tool, not dead code.** It re-syncs leads to Salesforce
after a broken connection or outage. Its `/admin/backfill-sf` route is deliberately
*not* mounted in `index.js` — it should only run when someone decides to run it.
Mount it temporarily when a recovery is needed, then remove the route again. Don't
delete the file.

## Tables

- **`leads`** — one row per form session, upserted as the visitor progresses.
  A row here means someone reached at least step 1 (entered an email).
- **`form_sessions`** — one row per page load. Kept separate on purpose: writing
  page loads into `leads` would break every query that assumes a row means someone
  reached step 1. Join on `session_id` when you want a funnel.
- **`lead_magnet_leads`** — the LP funnel. Never joined to `leads` at write time.
- **`enrichment_data`** — Apollo responses.
- **`form_page_views`** — one row per `/session` call, i.e. per load of a
  form-bearing page. The detail behind `form_sessions.hits`, which is only a
  counter. **Named for its scope on purpose:** `gushwork-form.js` is on 16
  pages only, so these are loads of form pages, not of the site — the
  homepage and `/pricing` have zero rows here while hundreds of leads arrived
  at the form from them. **No referrer column in v1**, deliberately: the only
  referrer in the payload is first-touch and identical on every hit, so
  storing it per page view would repeat one value down the column and read as
  a per-hit fact it is not. `source` is `NOT NULL` and today only ever
  `'session_route'` — see `docs/OPEN-ITEMS.md` for why anything
  client-reported must never share that value.
- **`leads.non_icp_blocked` / `non_icp_reason`** — the non-ICP block. **NOT
  `disqualified`**, deliberately: that column means exactly one thing (the
  prospect self-declared B2C/Mixed) and is clearable by the "actually we're
  B2B" button, which 47% of leads press. `non_icp_reason` holds the matched
  brand domain, e.g. `kw.com`. **Blocked leads are counted in every headline
  number** — they are still leads — and are marked rather than hidden on All
  Leads. The dedicated surface is the **Blocked** tab. Mirrored to
  `gw_form_leads` for the dialer, which does not yet read them (see
  `docs/tickets/non-icp-v1-block.md` OPEN ITEMS #1).
- **`non_icp_domain_verdicts`** — the model layer's cache. One row per
  registrable domain, keyed through `partnerStackCustomerKey` like
  everything else here. **The model is never asked at request time**:
  `/submit` reads this table and nothing else, and a miss is "we could not
  check", which fails open. Caching per domain is also what makes the block
  deterministic — a model asked twice can answer twice differently, and
  `temperature` cannot be pinned on current models (it is rejected). **Two
  TTLs**: a real verdict lasts 180 days, a failure row six hours, so a brief
  outage cannot pin a domain to "could not check" until spring.
- **The model layer's dashboard surface is `/monitor/non-icp` and the Model tab, separate from Blocked on purpose** (flagged is not blocked). Its five-state ladder is mutually exclusive and exhaustive and sums to the lead total; the lead-to-verdict join is done in JavaScript through `nonIcpCandidateDomains`, so there is no second domain normaliser; the scrape panel reports the latest outcome per domain, never a historical rate, and says so on screen. Background: `docs/background/dashboard.md`.
- **`leads.non_icp_source` / `non_icp_checked_at` / `non_icp_llm_flagged`** —
  provenance for the block. `non_icp_reason` holds a domain for **both**
  mechanisms, so without `non_icp_source` a reader cannot tell a string
  comparison they can re-derive from a model verdict they cannot.
  `non_icp_checked_at` closes OPEN ITEM #3 and is **only stamped when
  something was actually decided** — never for `check_failed` or `disabled`,
  because an inferred timestamp in an observational column reads as a
  measurement to the next person. **`non_icp_llm_flagged` is NOT
  `non_icp_blocked`**: a flagged lead in flag-only mode books, goes to
  Salesforce and gets dialled exactly as today. Folding the two together
  would make every existing guard on `non_icp_blocked` start refusing leads
  nobody decided to refuse — the V1 incident, arriving one column earlier.

  **FOUR SOURCE VALUES SINCE 23 SEPT, NOT TWO, AND EVERY CONSUMER THAT
  COMPARED AGAINST THE LITERAL `'llm'` ANSWERED WRONG FOR THE NEW ONES.**

  | value | means |
  |---|---|
  | `domain_list` | the brand list |
  | `llm` | the model, having read the page |
  | `llm_name_only` | the model, having read only the domain name |
  | `llm_late` | a verdict that landed AFTER `/submit`, stamped by the recheck sweep. The lead row carries this; the verdict row under it is `llm` or `llm_name_only` |

  A null is a pre-feature row and means the brand list, which was the only
  mechanism that existed then.

  **Ask `nonIcpSourceIsModel(src)`, never `=== 'llm'`.** Adding two values
  silently re-scoped six readers: `nonIcpSourceShort` labelled a model
  block **"Brand list"** to an SDR and the sentence under it claimed the
  domain was on a list it is not on, and all three Model-tab buckets
  counted an `llm_late` block under the brand list. A test now forbids the
  bare literal appearing anywhere. Same shape as "a second column that
  means we rejected this lead is not additive" below — arriving as a
  second enum value instead.
- **`leads.ip_address` and the `ip_*` columns** — where the visitor actually was, from their IP (count the migration in `db.js`, not a number written here). NOT `enriched_city/state/country`, which are Apollo's record of the PERSON, and not `enriched_org_hq`, the company. `ip_address` is personal data, kept indefinitely (decided 23 Sept); the mirror gets the place, never the address; `ip_checked_at` is stamped only when a lookup decided something. Background: `docs/background/dashboard.md`.
- **`leads.ps_signup_recheck_at`** — when a verified PartnerStack conversion
  was last RE-checked, as opposed to `ps_signup_verified_at` which is when it
  was first seen to exist. Two observations, two columns.
- **`gads_conversion_uploads`** — one row per lead the Google Ads upload has
  decided about: `sent`, `validated` (checked by Google, recorded by nobody),
  `skipped` with a `skip_reason`, `failed_retryable` or `failed_permanent`, or
  `sending` while a request is out. **The primary key is the claim**: the row
  exists before any request leaves, so two sweeps cannot both send a lead.
  `payload` is the exact event sent, **TEXT and not JSONB** — JSONB reorders
  keys, so a retry would send the same event in different bytes (measured on
  a temp table). `session_id` is UUID because `leads.session_id` is.
  **`google_outcome` is what Google DID with a real send**, read back from
  `requestStatus:retrieve`: `accepted`, `accepted_with_warnings`, `unmatched`,
  `duplicate`, `dropped`, or `unknown` after 7 days with no final answer.
  NULL until checked, and always NULL for a validate-only row.
- **`lead_field_changes`** — append-only log of the seven identity fields
  (`email`, `company`, `website`, `phone`, `first_name`, `last_name`,
  `sell_to`) changing on a lead row, because both upserts are last-write-wins
  on 35 columns and the old value is otherwise gone. A row is written only
  when old and new are both non-null and **differ** — a first set is not a
  change. `booking_uid_present` is read from **before** the upsert, so it
  answers "had they already booked when they changed it?".

---

## Definitions

Every number on the monitor dashboard must conform to this section. Where a number
deliberately departs from it, the label on screen has to say so in words a
non-engineer reads correctly. Added because the dashboard had accumulated numbers
whose labels and calculations disagreed — a chart called "sessions" that counted
leads, a tooltip claiming two different populations were identical.

### The four nouns

**Session** — one row in `form_sessions`. One visitor arriving on a form page.
The id lives in `sessionStorage`, so a landing-page → `/demo` journey in one tab is
**one** session with `hits` incremented, not two. Keys on `form_sessions.created_at`
(first seen). Session recording only began **21 Aug 2026, 10:32 UTC**; there is no
session data before that and queries must say "not tracked" rather than 0.

**Lead** — one row in `leads`. A session that got as far as entering an email
(step 1). Keys on `leads.created_at`. One person can have several leads. A lead is
never deleted, so counts only go up.

**Completed** — `leads.completed = true`. Keys on **`submitted_at`** where a time
is needed.

**`completed = true` does NOT mean "submitted the form", and `submitted_at` is
not set everywhere `completed` is.** Seven branches write one or both. Read this
table before using either column as a proxy for the other — the previous version
of this caveat named only one of the two shapes below, and that omission is what
misled a reader on 5 Sept 2026 with the doc open in front of them.

| Branch | `completed` | `submitted_at` |
|---|---|---|
| `/partial` (~7632) | `false` on insert, and **absent from the conflict clause** | **never written** |
| `/submit` (~7760) | `true` | `NOW()` |
| `/booking-confirmed` (~7869) | `true` | **left alone** |
| `/booking-confirmed-webhook` (~7945) | `true` | **left alone** |
| `/booking-confirmed-webhook` safety net (~7972) | `true` | `NOW()` |
| `/booking-confirmed-webhook-rh` (~8247) | `true` | **left alone** |
| `/booking-confirmed-webhook-rh` safety net (~8283) | `true` | `NOW()` |

`/submit` is the **only** branch that means "this person filled the form in".
Note also that `/partial` leaving `completed` out of its conflict clause is
load-bearing: it is why a visitor who submits and then goes back to edit step 1
does not lose the flag on Railway.

Two populations are `completed` without being form completions: someone who reached step 1, dropped and booked through a link later (`submitted_at IS NULL`), and the two safety-net `INSERT`s for a booking with no form row at all. Counts and examples: `docs/background/definitions.md`.

So:

- **"Did this person fill the form in?"** → `submitted_at IS NOT NULL`. Never
  `completed`.
- **"Which stage is this lead in?"** → the stage ladder above, which uses
  `completed IS TRUE` and is correct as written.
- They are **not** interchangeable, in either direction.

Both populations are real leads and neither is a form completion. Counting them
under "Completed" on the dashboard is deliberate and documented; using
`completed` to mean "submitted" in a *query* is a bug, and it is why
`backfill-sf.js` selects on `submitted_at IS NOT NULL`.

**On the AWS mirror, `gw_form_leads.submitted_at` is a different thing entirely
— a sync timestamp, not a submission time.** Its presence is meaningful, its
value is our clock. See the mirror traps in `docs/partnerstack.md`.

**Booked** — `leads.booking_uid IS NOT NULL`. Keys on **`booked_at`**, falling back
to `created_at` where `booked_at` is null (rows predating that column). Always use
`COALESCE(booked_at, created_at)` when ordering by when a booking happened —
comparing a null `booked_at` yields null, the comparison quietly fails, and the row
is counted as un-booked.

### The stage ladder

Exactly four stages, **mutually exclusive and exhaustive**, resolved in this
priority order. Any lead is in exactly one, so the four always sum to the total:

1. **Booked** — `booking_uid IS NOT NULL`
2. **Disqualified** — not booked, and `disqualified IS TRUE`
3. **Completed** — not booked, not disqualified, and `completed IS TRUE`
4. **Step 1** — everything else

Use `IS TRUE` / `IS NOT TRUE`, never `= true` / `= false`: a null flag on an old row
must land in a stage rather than vanishing from all four. The stage filter, the
stage badge and any stage count all read from this one ladder. If you add a stage,
it goes in the ladder or it doesn't exist.

### The PartnerStack lifecycle ladder

Exactly eight states, **mutually exclusive and exhaustive**, one per partner
**domain**, resolved in this priority order. Every counter on the Partners tab
is a `COUNT FILTER` over this one column, so the numbers cannot disagree with
each other. Same rule as the stage ladder above: if you add a state, it goes in
the ladder or it does not exist.

1. **qualified** — `ps_qualified_sent_at IS NOT NULL`
2. **qualification_failed** — `ps_qualify_failed_at IS NOT NULL`
3. **conversion_failed** — `ps_signup_failed_at IS NOT NULL` **and not since converted**
4. **demo_done_not_qualified** — earliest `start_time` is in the past
5. **awaiting_demo** — `booking_uid IS NOT NULL`
6. **converted** — `ps_signup_sent_at IS NOT NULL`
7. **skipped** — `ps_signup_skipped_reason IS NOT NULL`
8. **conversion_pending** — everything else

**The order is not the progression order, deliberately.** A success always
outranks its own failure, because a domain that failed and later succeeded is
fine. But an *unresolved* conversion failure outranks every later stage it
blocks: a domain whose conversion never landed can never be qualified, so
showing it as "awaiting demo" would hide the only fact worth acting on. That is
exactly what happened on 4 Sept — a 400 on the qualification released the claim
correctly and nothing on any card moved.

A domain matching two states takes the **first** match, never a blend.

**Keyed by DOMAIN, because that is the unit PartnerStack pays on** — one
conversion and one qualification per customer key, ever. Leads with **no usable
domain** cannot be keyed that way, so they are counted **separately, as leads**,
in their own field, and the UI says "leads, not companies" on that chip. Folding
them into a domain count would reintroduce the mixed-unit arithmetic that made
the old counters irreconcilable.

The query is bounded to `PS_LADDER_WINDOW_D` (180 days, matching the Salesforce
lookback) **except for unresolved failures, which are included regardless of
age** — otherwise a domain that failed months ago and was never fixed would
silently drop out of "Needs attention", the one number that has to be complete.

**conversion_failed and qualification_failed are the two red states** and are
summed into "Needs attention" — the only number on the tab that means someone
has to act today. Both also fire a Slack alert at the moment of failure, via
`alertOps` with its normal cooldown, because a state you have to remember to
check is half a fix.

### Bookings: two different questions, and they are not interchangeable

These look like one question and are not. Collapsing them onto a single rule
breaks whichever one you didn't have in mind.

**1. "Is this person an SDR target?" — no time comparison.**
Does any lead row sharing this `lower(email)` have a `booking_uid`? If yes, they
have a booking; don't call them. When they booked is irrelevant — an SDR ringing
someone who already has a call on the calendar is wrong whether that call was
booked yesterday or in May. Used by the SDR List and by "No booking yet".

**2. "Should this session get a drop-off recovery email?" — the time comparison is
required.** `NOT EXISTS (a booking by this email with booked_at >= this session's
created_at)`. A booking that *predates* the session does not resolve that session's
drop-off: the person came back, started again, and dropped again. Suppressing on
"has ever booked" would silently kill legitimate follow-ups. This is deliberate,
dates from a May 2026 fix, and lives at the recovery cron (`index.js` ~4499) with a
comment saying so. **Do not "unify" it.**

**3. "Recovered bookings"** is a third shape — a completed session with no booking,
followed *later* by one — and uses `COALESCE(booked_at, created_at) >= l.created_at`.

Note the asymmetry between 2 and 3: the cron compares bare `booked_at` and does
**not** COALESCE, because production has zero rows with a null `booked_at` and
there is nothing to defend against. Recovered bookings does COALESCE because it
reads the full history including rows that predate the column. Both are correct for
what they ask.

The dashboard's "Pending recovery" card reports question 2's population, so its
label says what it counts and no longer claims to mirror the cron's exact rule.

### Default population for dashboard numbers

Unless a label says otherwise:

| Question | Default |
|---|---|
| All leads, or completed only? | **All leads.** Filter to completed only where the label says "completed" |
| Deduped by email, or by session? | **Headline numbers are people** — `COUNT(DISTINCT lower(email))`. Session counts are legitimate but must be labelled "sessions" every time they appear |
| Named exception | **"Form entries per day"** on Overview is a deliberate ROW count — see below |
| Dedup key | `lower(email)`, always. Never raw `email` |
| Internal / test addresses | **Left out of the Overview, Dropoff and the Monday digest, and counted in words there; included wherever rows are listed.** See below |
| Webhook-origin leads | **Included**, except `/monitor/funnel` |

**OUR OWN TEST SUBMISSIONS ARE LEFT OUT OF THE OVERVIEW, DROPOFF AND THE MONDAY
DIGEST — Darshil's decision, 26 Sept 2026.** Until then they were included in
every `leads` number as "a known distortion, not a decision anyone made". The
rule is `internalLeadSqlClause` (the one behind the "ours" marker:
`INTERNAL_TEST_EMAILS`, `ELV_EXCLUDED_DOMAINS`, the staging host).

- **The three move TOGETHER**, or "Overview and Dropoff agree exactly in Leads
  mode" breaks. The digest reads `dropoffReport`, so it follows on its own.
- **Left out ROW by row, before people are formed**, so a person with one
  staging row and one real row still counts, from the real row.
- **Every surface says how many it left out, in words**: `ours` on
  `/monitor/overview`, `internal` / `internal_by_period` on `/monitor/dropoff`
  (with `internal_excluded: true`), and a line in the digest. A subtraction
  nobody can see is how a number stops reconciling.
- **`IS NOT TRUE`, never `NOT`**, around the clause: a lead with no email makes
  it NULL, and `NOT NULL` drops the row.
- **Sessions cannot be separated** (a session has no email), so they still
  include ours, and the Overview says so.
- **Still INCLUDED:** All leads, Blocked, Duplicates, the SDR list, Visitors,
  Partners and the classic `/monitor/metrics`. Those are where a person
  reconciles a row. **Marked** on All leads, Blocked and Duplicates; the SDR
  list, Visitors and Partners have no marker yet (in `docs/monitor-plan.md`).
- **Lead magnet's own totals already left them out** (`is_internal IS NOT
  TRUE`, "All real leads") before this, and still do.
- `ELV_EXCLUDED_DOMAINS` and `b@g.ai` are also out of ELV health and
  alerting, as before.

**853 session rows from `/careers` and `/meeting-booked`, which carried the form script by mistake until 10 Sept 2026, are staying**: they are true, and deleting them would move every historical session number. They are 5.7% of Sessions and bias the `partial` health row toward a false red. The numbers, and the measurement behind leaving our own submissions out: `docs/background/definitions.md`.

**"Form entries per day" is deliberately a row count, not a people count.** It
counts rows in `leads` per ET day — everyone who reached step 1, undeduped, all
stages. That is a departure from the people-by-default rule and it is intentional:
the chart's job is daily inbound volume, and deduping by email would flatten the
repeat-attempt spikes that make a bad day visible. The label says "entries", not
"people" and not "sessions", so it reads correctly. Do not "fix" it to
`COUNT(DISTINCT lower(email))`. Proper session-based charting is separate work.

**Webhook-origin leads** (`prefill_source IN ('rh_webhook','cal_webhook')`) are
included everywhere except `/monitor/funnel`, which excludes them from step1,
submitted and booked and explains why at length. They never touched the form, so
they inflate any form-conversion rate from both sides. Left in deliberately.

**10 rows on the mirror as of 5 Sept 2026, and all 10 are `rh_webhook`** — the
Cal safety net has never fired once. The filter keeps `cal_webhook` because the
branch exists and could fire tomorrow, not because it has. Was recorded as
"9 rows" before 5 Sept; a count in a doc goes stale, so treat it as an order of
magnitude and re-run the query rather than quoting it.

### Timezone

**The dashboard is Eastern Time.** Use the IANA name `America/New_York`, never a
fixed offset — DST has to move on its own.

- Every displayed timestamp and every day boundary is ET.
- The Postgres session timezone is `Etc/UTC` (confirmed). So a bare `date_trunc`
  or `::date` buckets in UTC, which is **not** a correct day boundary for this
  dashboard. Write `AT TIME ZONE 'America/New_York'` explicitly.
- Never derive a calendar date in browser code from `getFullYear/getMonth/getDate`
  — those read the viewer's laptop. Format through
  `Intl.DateTimeFormat` with an explicit `timeZone`.
- `/monitor/funnel` is the one deliberate exception and keeps UTC day buckets. Its
  go-live and coverage reasoning is anchored to a UTC instant, and re-bucketing it
  would silently change which day counts as partially covered. Its comments say so.

### Health checks fail LOUD

"A lead is worth more than a verdict" governs the lead path: checkers fail open.
**Health checks are the opposite and must be, because no lead depends on them.** A
health probe that cannot reach its dependency reports red, never green and never
"unknown-styled-as-fine". A green badge means "verified working, just now". If it
can't verify, it must not be green.

---

## Things that will bite you

**A source assertion cannot see runtime behaviour, and a confident zero passes every structural check.** Wherever a number is displayed, assert it MATCHES the payload, with fixture values distinctive enough not to match by accident. Drive the real thing (boot the route, evaluate the dashboard JS, stub Leaflet) and assert on what the user sees: each loader paints its own error into the table instead of throwing, so a probe for a thrown error misses it. Background: `docs/background/testing.md`.

**After a headless Webflow write, the open Designer serves a stale tree, so a snapshot can lie.** `switch_page` away and back, then `select_element` the new element, before treating any snapshot as evidence. Background: `docs/background/webflow.md`.

**`leads.disqualified` has fourteen call sites that no single grep reaches, and adding `non_icp_blocked` turned every one into half a guard**, found in three waves, one of them a PartnerStack conversion that cost money. Counting predicates is not deciding them: `tests/test-non-icp.js` section 10b resolves each to its enclosing query and requires it guarded or named in `DELIBERATE` with a reason. Background: `docs/background/non-icp.md`.

**Run that audit whenever you touch either column.** A new predicate added
without a decision fails the suite, which is the only mechanism in this repo
that reaches all fourteen sites.

**The generalisable rule: a second column that means "we rejected this lead"
is not additive.** It silently re-scopes every consumer of the first one. The
work is not "update the guard I am thinking about", it is "enumerate which
predicates on the old column now answer only half the question".

**THE LATE-VERDICT SWEEP, AND WHY THE RACE WAS THE WRONG FRAME.**
`runNonIcpBookedRecheck` runs at boot and every 5 minutes over leads booked
in the last 48 hours, re-reads `non_icp_domain_verdicts`, and stamps
anything that now resolves blocking.

It exists because the cost lands at the MEETING, not the booking (booking to demo, median 35 hours), so there are hours to catch a late verdict, not seconds. Background: `docs/background/non-icp.md`.

**IT MARKS AND TELLS A HUMAN. IT NEVER CANCELS.** Authorised shape,
Darshil, 23 Sept 2026. Cancelling on a model verdict with nobody in the
loop would be the most aggressive action in this codebase, and the person
is already holding a confirmation email. `slackNonIcpLateBlock` posts
direct rather than through `alertOps`, whose cooldown keys on
severity+source+title and would fold a second person into a counter.

**Only BLOCKING verdicts.** A Meta-only flag is already handled at booking
time by `nonIcpScheduleSuppressed` and does not need a human — those
people keep their slot by design, so waking somebody for one trains them
to ignore the channel. The `non_icp_blocked` stamp is its own cursor, so
nothing is alerted twice and no new column was needed.

---

**Meta was sent the whole `x-forwarded-for` header — two entries on Railway — as `client_ip_address`, and nothing complained.** The client IP is `clientIpOf(req)` and nothing else. Background: `docs/background/meta.md`.

**Normalised in `sendEvent`, not at the call sites.** There are six and a
seventh will exist one day; a choke point every event already passes
through cannot be bypassed by a new caller. `normalizeClientIp` drops junk
rather than forwarding it, so the key is absent from `user_data` instead of
holding a value that matches nobody.

**`req.ip` IS ALSO THE WRONG ONE.** It resolves to the LAST entry, a proxy
hop, so the `|| req.ip` fallback would have been wrong too had the header
been absent. `clientIpOf(req)` is now the ONE definition and PartnerStack
reads it too.

**Meta never complains:** a malformed field still gets HTTP 200 and `events_received: 1`, so the `messages` array is printed whenever it is non-empty. Background: `docs/background/meta.md`.

---

**THE ORIGIN CHECK ADMITS THE API'S OWN ADDRESS, EXACTLY — SINCE 27 SEPT
2026.** The `cors` origin function throws for any Origin that isn't in
`ALLOWED_ORIGIN`, and that throw is a **500 before any route runs**. A
browser sends Origin on every font request and every POST, even to the site
the page came from (Chrome for fonts; every browser for POSTs), and the
dashboards are served BY this API.

**What that broke:**
- the dashboard's fonts, which never once loaded on production;
- the three write buttons on both dashboards (Lead magnet delivered and
  retry, Partners acknowledge), which could never have worked: 0 delivered,
  0 acknowledged, ever.

**Why nothing caught it:** the preview and the layout check serve the page
from localhost with no origin check. After a front-end deploy, look at the
fonts in a real browser on production.

**The fix:**
- `SELF_ORIGIN` is `https://` plus Railway's own `RAILWAY_PUBLIC_DOMAIN`,
  compared as a whole string: no wildcard, no other subdomain, no http, no
  port.
- `selfOriginFrom` refuses anything that isn't a bare hostname.
- Unset (a laptop, a test) admits nothing extra.
- **Foreign origins still get the 500, deliberately:** a page on another
  site still cannot make any **POST** route run. This was never cross-site
  protection for GETs: a link or an image sends no Origin, and no-Origin is
  admitted, as always.
- **If a custom domain is ever attached,** `RAILWAY_PUBLIC_DOMAIN` may name
  it instead, and the railway.app address is refused again. That's the safe
  direction; the answer is the other exact name, never a pattern.
- Never widen this to `endsWith` or a pattern.
- `tests/test-monitor-next.js` §2b drives each case over HTTP.

**The Visitors tab (`/monitor/visitors`):** check a map tile provider by DOWNLOADING a tile and looking at it, never by its status code (OpenStreetMap 403s in a browser; Carto prints "API KEY REQUIRED" across a 200; Esri Canvas is in use, and the rejected ones are listed in `tests/test-non-icp-routes.js`). Circle AREA scales with lead count, never radius. Networks are merged by NAME in the browser. Background: `docs/background/dashboard.md`.

**Geo is `ipwho.is`, NOT `ipapi.co`.** ipapi.co answers `RateLimited` on
the first request of the day from this machine and from Railway's egress
alike, so its free tier is not a tier. ipwho.is allows 1,000/day against
our ~41 leads/day, and returns more: an IANA timezone, and the ISP, ASN and
org domain that are the only corporate-versus-residential signal here.
`IP_GEO_URL` swaps it from the env, because a free geo service is exactly
the dependency that stops being free.

**The lookup NEVER touches the lead path.** Fire-and-forget after
`res.json()` in BOTH `/partial` and `/submit` — 41 leads a day reach step 1
and 11 never submit, so resolving only at submit would leave every drop-off
with no location. `/partial` fires repeatedly, so `finaliseIpGeo` does at
most ONE lookup per session, guarded in process and by `ip_checked_at`. The
ADDRESS is written on every call because it costs no network; only the
network half is rationed. **Two writes, not one** — a geo outage must not
lose the address too.

**The model layer's four traps.** (1) **Blocking is decided in CODE from the enum (`NON_ICP_BUSINESS_TYPES`), never by the model**: the output schema has no `blocking` field and a model-supplied one is ignored. (2) **Never key a block on the free-text `category`**, which holds 65 spellings of real estate and insurance; `business_type` is a closed enum. (3) **Page text is untrusted, and the schema is not the defence**: it goes in a user message inside `<untrusted_page_text>`, never the system prompt, markup stripped and capped at 12k chars, and the guardrails aim at accidental false blocks (the floor, the known-customer bypass checked BEFORE any block, the Slack evidence quote). (4) **The verdict is a CACHE READ at decision time**: no scrape, no model call, no network, and no time-out-and-let-the-lead-through path, because a lead reaching the calendar only because our own request was slow is the failure this design prevents; warming rides the `/non-icp-check` calls the form already makes, so neither form file changes. Background: `docs/background/non-icp.md`.

**The brand-list block's three traps.** (1) `partnerStackCustomerKey` collapses subdomains, so `nonIcpHostForms` matches the full host too, or every subdomain entry on `NON_ICP_DOMAINS` is dead while looking live. (2) **`non_icp_blocked` is STICKY in all three upserts** (`leads.non_icp_blocked IS TRUE OR EXCLUDED.non_icp_blocked IS TRUE`), and a test pins all three sites: `/partial` re-fires all through step 1, and an `= EXCLUDED` lets a realtor clear their own block with the "actually we're B2B" button, which calls `savePartial(1)` again. (3) **Never match a brand domain by substring**, only exact or subdomain; the false matches are pinned as negative fixtures in `tests/test-non-icp.js`. Background: `docs/background/non-icp.md`.

**And the scope is narrower than "non-ICP" sounds.** V1 is national real-estate
brokerage and insurance carrier brands only. Financial advisors (Edward Jones,
LPL, Northwestern Mutual, Primerica, Cetera), mortgage and lending are **in
ICP by name in the Non-ICP doc** and are not blocked; nor are independent local
agencies and brokerages, of which we have ~70. The other four rule-6 industries
— restaurants, spas and salons, home services, print and sign shops — are not
covered by V1 at all and fire Meta normally.

**The three lists.** `WEBSITE_VERIFIED_REASONS` (line ~424), `RECHECK_WRITEABLE`
(~3386) and `RECHECK_PROTECTED` (~3398) must stay in sync with `gushwork-form.js`
SECTION 3C **and `gushwork-form-popup.js`, which carries its own copy of the same
lists** — see the fork note below. There's a warning comment above them. Adding a
website verdict means deciding its place in all three:

- **`WEBSITE_VERIFIED_REASONS`** → does Meta fire?
- **`RECHECK_WRITEABLE`** → can the historical recheck tool overwrite it?
  Anything meaning "we didn't get a real answer" stays **out**.
- **`RECHECK_PROTECTED`** → verdicts that depend on the lead's email and can't be
  re-derived from the domain alone.

**Backticks inside SQL comments break the file.** The SQL in this repo lives in
JS template literals, and the house style is a long `/* ... */` comment inside the
query explaining the incident behind it. A backtick in that comment — writing
`` `leads` `` or quoting an expression — **terminates the template literal**, and
the error surfaces as `SyntaxError: missing ) after argument list` pointing at the
`pool.query(` line, not at the comment. Four of these happened in one sitting. Use
plain words inside SQL comments, and run `node --check index.js` before committing.

**"What are you looking for?" is on `/demo` only.** `leads.product` stays SINGLE-valued, the routing slug (CRM wins a both-ticked lead); `leads.product_interest` holds what they ticked, canonical and sorted (`aeo`, `crm`, `aeo,crm`), NULL meaning "we never asked", never backfilled. Salesforce gets `product_interest ?? product`. Background: `docs/background/product-and-forms.md`.

**The routing slug and the Meta event slug are different functions.** `resolveProduct` is single (calendar, picklist); `resolveEventProduct` sends a both-ticked lead as ONE event with `content_ids: ['aeo','crm']`, never two, which would count one person twice. The Meta event follows `product_interest`, not `product`. Background: `docs/background/product-and-forms.md`.

**`predicted_ltv` is config: `META_LTV_AEO` / `META_LTV_CRM` / `META_LTV_AEO_CRM`, 12000 / 5000 / 15000, all provisional**, and `leads.meta_predicted_ltv` records what was actually sent (NULL where Meta did not fire). Value optimisation is blocked on closed-won revenue joined to leads, not on the config. Background: `docs/background/product-and-forms.md`.

**`resolveProduct` takes the selection AND the page, and both the column and the Meta event must be resolved from both.** An unknown `product_interest` loses the whole Salesforce LEAD (`Product__c` is a restricted picklist and that error is not retried), so `canonicalProductInterest` dropping unknown slugs is load-bearing; a third product needs its combinations added to the picklist (`tests/test-batch2.js` section 24). Background: `docs/background/product-and-forms.md`.

**The B2C gate fires at the next click, never on selection**: the sell-to change handler must not call `showStep` or `savePartial`, or touch `disqualified`. The `/demo` markup it depends on (`#needs-wrap`, `#need-aeo`/`#need-crm`, `#needs-error`, `#about-business-wrap`, and `data-rh-router-crm="6804"` with NO `data-rh-router`) is listed in `docs/background/product-and-forms.md`.

**The two router attributes are not interchangeable.** `data-rh-router` always applies (a page selling ONE product: `/ai-demo`, `/ai-crm`); `data-rh-router-crm` applies only when this visitor is a CRM lead (`/demo`). A CRM page must NOT use the `-crm` one, or its direct and organic traffic goes to the AEO team. Background: `docs/background/product-and-forms.md`.

**The collapsible wrappers are hidden by the `is-hidden` CLASS, never `display:none`** (Webflow cannot store an inline style). The 240px max-height ceiling clips silently if the content grows, so re-measure rather than just raising it, and the needs cards hold 50/50 with `flex: 1 1 0%` and NO `min-width`. Measurements: `docs/background/product-and-forms.md`.

**The `sell_to` gate is client-side only, and CRM pages are excepted through `B2C_ALLOWED_PATHS`**, which makes four copies of the CRM path set (`PRODUCT_PATHS`, its import, both form files; `tests/test-batch2.js` section 21). **When a new product page appears, check it is IN the catalogue**, not just that the lists agree: `/ai-crm` was live, routed right and in none of them. The two `sell_to ILIKE 'B2B%'` predicates (`/monitor/sdr` and the `noBooking` card) carry `OR product = 'crm'` and move together. A CRM B2C lead fires Meta, can fire PartnerStack and goes to Salesforce, authorised 15 Sept 2026. Background: `docs/background/product-and-forms.md`.

**The cookie namespace on `.gushwork.ai` is shared with Webflow, and `gw_utm_campaign` is taken** (the form's 30-day offer memory): the site-wide attribution mirror uses `gwa_*`, and must never re-write a `gw_` cookie. That script lives in Webflow's site-wide footer, in no repo, so changing attribution capture is a Webflow change with no git diff. It exists because the Facebook and Instagram in-app browsers drop `sessionStorage`. Background: `docs/background/attribution.md`.

**`gushwork-form-popup.js` is a FORK of `gushwork-form.js`, not a sibling.**
The Ads file exists only to present the booking step as a fullscreen modal
opened after step 2. Every other line is meant to be the same code, and there is
no shared module — the two files are edited independently, so a fix applied to
one is a fix applied to half the traffic. This has already gone wrong once: the
Ads file forked at `/demo` v5.3.0 on 14 Aug 2026 and silently missed v5.6.0 and
v5.7.0/v5.7.1, so for twelve days Google Ads leads got no DNS fallback, no
email-in-website-field catch, and no typo nudge. **A change to `gushwork-form.js`
is not finished until it is in `gushwork-form-popup.js` too.**
`node tests/test-ads-parity.js` now enforces that — it lifts both files and
compares them, and it also pins the modal as deliberate so a future sync cannot
"tidy" the fork's own presentation away.

**A `//` COMMENT INSIDE SQL TAKES THE STATEMENT DOWN, and this already
happened.** Postgres has no `//`. On 8 Sept 2026 a single line —
`// COALESCE for the same reason as /partial.` — shipped inside the
`/submit` INSERT and broke **every form completion**: SQLSTATE 42601,
`syntax error at or near "//"`, a 500 from the route, and nothing after
the INSERT ran. No `completed`, no `submitted_at`, no step-2 fields, no
Slack, no Salesforce, no Meta `Lead`, no PartnerStack conversion. It was
live for 31 minutes and caught one real lead.

Sibling of the backtick trap above, and worse, because a backtick fails at
**parse time in Node** where you cannot miss it. This is valid JavaScript
and invalid SQL, so it fails only when the query runs. Use `/* ... */`
inside SQL, always. `tests/test-batch2.js` section 13 lints for it.

**Execute any SQL you touch.** Every SQL assertion here reads the query as text, so a statement that could not parse passed six suites, a review card and a merge. Build a `CREATE TEMP TABLE` from the real migrations, run the real statement in a transaction and `ROLLBACK`; for a read-only query `EXPLAIN` is enough (`42601` is a syntax error, `42P01` a missing table). Background: `docs/background/testing.md`.

**Product tagging: AEO is the DEFAULT, and only the exceptions are listed.**
`PRODUCTS`, `PRODUCT_PATHS`, `DEFAULT_PRODUCT` and `resolveProduct` live in
`meta-capi.js` and are imported by `index.js` — one catalogue, so the
stored column and the Meta event cannot disagree. Today `PRODUCT_PATHS` is
`{'/ai-demo': 'crm', '/ai-crm': 'crm'}`; everything else is `aeo`.

`/ai-crm` and `/ai-demo` both resolve `crm`, so swapping `/ai-crm`'s content into `/ai-demo` changes nothing here. Why a default beats an allowlist (82% tagged against 99.7%): `docs/background/product-and-forms.md`.

**Adding a product** means one entry in `PRODUCT_PATHS` and one in
`PRODUCTS`. **Adding a new AEO landing page means nothing at all**, which
is the entire point.

**An unreadable `page_url` is NOT the default — it returns null and the
event goes untagged.** "We could not tell which page this was" is not "this
was the default page", the same rule the lead-path checkers follow. The
leading-slash guard in `resolveProduct` is load-bearing: `new URL(x, base)`
succeeds for almost any string, so without it `'not a url'` becomes
`/not%20a%20url` and reaches Meta as a real AEO lead.

**`Contact` is excluded from product tagging by EVENT NAME, not by its
page.** `PRODUCT_EXCLUDED_EVENTS`. With a default in place the lead-magnet
LP resolves to `aeo` like any other unmapped page, so this list is the only
thing keeping a PDF download off a 12000 `predicted_ltv`. A test asserts it
from the page that WOULD tag, so a regression to page-based exclusion
fails.

**`predicted_ltv` is the same per product on every event; only `value`
varies, and it is 0 on all three upstream events.** 12000 aeo / 5000 crm,
PROVISIONAL. Changing one changes how Meta weights these conversions — a
business decision to surface, not a tidy-up.

**`leads.product` and `gw_form_leads.product` hold the resolved slug**,
never a raw page and never anything a page author typed; nothing reads
`req.body.product`. Both conflict clauses COALESCE it. Historical rows were
backfilled to `aeo` once by hand; that backfill is deliberately NOT a boot
migration, because those run on every deploy and a NULL now means
"page_url was unreadable".

**`about_business` is capped at 1000 chars, and the cap is NOT what
protects the request.** `express.json({ limit: '50kb' })` is. A
server-side `slice` runs inside the route, and express rejects an oversized
body with a 413 **before any handler runs** — so an unbounded textarea
loses the whole lead, not the one field. The limit was raised from 10kb
when the textarea landed; measured, a worst-case body was already 12,755
bytes without it. The `slice(0, 1000)` is a backstop for the column and
matches `maxlength="1000"` on the Webflow textarea, so what the visitor
sees on screen is what the column keeps.

**The about-business textarea is on THREE pages** (`/demo` behind the CRM tick, `/ai-demo` and `/ai-crm` always visible), and each must carry `maxlength="1000"`; Webflow's default is 5000. Background: `docs/background/product-and-forms.md`.

**Apollo refuses with a reply, not an exception** (`{"error":"You have insufficient credits!"}`). `apolloReplyError` reads it and pages "Out of credits" at once (repeating every 3 hours); a refusal is recorded insert-only so it never overwrites a real enrichment; health counts what Apollo FOUND over business-email leads. Re-enrich afterwards with `tools/re-enrich-apollo.js`. Background: `docs/background/alerting.md`.

**A lifted `recordFailure` needs every name it reads.** Two suites and one
tool lift it; adding `isCreditsExhausted` made the PartnerStack streak
harness throw inside `recordFailure`'s own try/catch, which swallowed it
and left the streak silently at zero. Five assertions caught it. Add the
name to every lift, or that is what a new dependency looks like.

**A Meta CAPI failure reaches `recordFailure` only because the push functions THROW** (`throwIfAnyFailed` after `Promise.allSettled`); every caller is fire-and-forget WITH a `.catch`. **Meta auth failures match four codes (190/102/463/467), not `/OAuth/i`**, because Meta stamps OAuthException on nearly every error. `recordSuccess('Meta CAPI')` arrives through the injected `setMetaOutcomeReporter`, success only. Background: `docs/background/alerting.md`.

**`git checkout <file>` restores from HEAD, not from "before my scratch
edit".** Used as an undo for a mutation-test tweak while the real change
was still uncommitted, it silently deletes the work. That happened four
times in one session. **Commit before mutation testing** — which is also
the correct order, because a mutation must be measured against a committed
baseline.

**One row builder renders All Leads and Blocked, both in the DOM at once, so row ids must be namespaced.** `toggleRow(key, sid)` and `loadChanges(key, sid)` take two arguments, the key for the DOM and the session id for the lead; never collapse them. Background: `docs/background/dashboard.md`.

**The Model tab keeps one claim per number.** Its industry table counts leads acted on in the window, SPLIT by what decided (empty groups shown); the standing cache is a separate block in companies, all time, with no rate; an industry comes from the domain that actually blocked, or reads "not categorised". **Blocked counts come in three units** (leads, people, leads excluding ours) and every label says which. Background: `docs/background/dashboard.md`.

**Our own test submissions are MARKED, never silently excluded.**
`INTERNAL_TEST_EMAILS` (extensible from the Railway env) plus
`ELV_EXCLUDED_DOMAINS`, behind `isInternalLead`. Five of the first ten
non-ICP blocks were ours. They are subtracted only where the Definitions
section says — the Overview, Dropoff and the digest, each saying how many,
since 26 Sept — and nowhere else: rows carry a marker, the Blocked tab and
the ladder print the excluding-ours figure **beside** the count, and the
filter is opt-in. **Never inferred from a person's name**: a
real prospect may be called Darshil, and `allstate.com` is a real brokerage
domain that is on the block list for that reason.

**"MARKED, NEVER EXCLUDED" IS ABOUT OUR OWN NUMBERS, AND AS OF 19 SEPT
2026 IT STOPS AT OUR OWN NUMBERS.** Internal submissions are still on
the dashboard, still booking, and still counted wherever rows are listed
(the Overview, Dropoff and the digest leave them out and say so, since 26
Sept) — that is about reconciling a number against the database. But they no longer leave the building. Three outbound systems
now refuse them:

| System | Guard | Why |
|---|---|---|
| Meta CAPI (5 call sites) | `internalLeadSuppressesMeta` | a conversion tells Facebook to find more people like us |
| Salesforce (every Lead write, since 7 Oct 2026) | `isInternalSubmission`, handed to `pushToSalesforce` at boot | `Source_Bucket__c` recomputes on read, so each junk Lead is reported as real inbound. Until 7 Oct only `/submit` checked; the two booking safety nets did not |
| AWS mirror (`syncToAWS`) | inside the function | `gw_form_leads` is the DIALER FEED — a row is a person somebody may ring |

`b@g.ai` is special-cased in four hardcoded lists and is now in `INTERNAL_TEST_EMAILS` as well; the website gate let our tests through because a null reason and `test_email_skipped` both count as verified. Background: `docs/background/internal-test-submissions.md`.

**The AWS guard is INSIDE `syncToAWS`, not at its four call sites**,
because two of those are the Cal and RevenueHero **safety nets** — the
pair most likely to be missed by someone guarding the two obvious
routes. The three targeted writes (`syncBookingToAWS`,
`syncPartnerIdentityToAWS`, `syncHearAboutUsToAWS`) need no guard: each
is a plain `UPDATE ... WHERE session_id`, so with no mirror row they
match nothing and no-op. `syncBookingToAWS` now **reads `rowCount`** and
says which happened — it used to log a tick unconditionally, so an
update that matched nothing read as success.

**The page is a signal, not only the address:** every outbound guard asks `isInternalSubmission(email, page_url)`, which also catches the Webflow staging host `gushwork.webflow.io`, where 19 rows carried addresses no list could catch. Background: `docs/background/internal-test-submissions.md`.

**Exact HOST match, never a substring.** `INTERNAL_STAGING_HOSTS` is
compared against `new URL(page_url).hostname`, so
`gushwork.webflow.io.evil.com` and `evil.com/?x=gushwork.webflow.io` do
not match. A bare path resolves to no host and is therefore **not**
evidence of staging — same rule as `resolveProduct`: what we cannot read
is not a default.

Nothing is lost when it fires: the lead is still written and still on the dashboard, only not pushed outward. Background: `docs/background/internal-test-submissions.md`.

**The dashboard marker and the guards ask the SAME question**, and that
is the fix for how this stayed hidden: Meta was suppressed by address and
the dashboard agreed with it, so nobody could see that **neither had ever
consulted the page**. `internalLeadSqlClause(emailCol, pageCol, params)`
carries the third arm too, as an exact `SPLIT_PART` host match rather
than a `LIKE`.

`tests/test-batch2.js` §28 and §29 execute all of this rather than
reading it, and all five mutations are caught.

**Two copies of the label map.** `WEBSITE_REASON_LABELS` is a normal JS object.
The monitor dashboard has a second copy (`var WLBL=`) inside a JS string that gets
sent to the browser. Both need updating, and the string one uses `\u2014` for em
dashes with a **single** backslash. Getting that wrong renders a literal `\u2014`
in the dashboard.

**Two more pairs that must stay in sync.** Same shape as the label map above:

- `SDR_SEARCH_COLUMNS` (server, in the `/monitor/sdr` route) and
  `SDR_SEARCH_FIELDS` (client, in the dashboard JS) are the fields the SDR
  search matches. The table filters in the browser; the CSV export filters on
  the server. If they drift, the export silently stops matching what is on
  screen. A test lifts both and asserts they are equal.
- The System Health check ids in `HEALTH_SEVERITY` / `HEALTH_ALERT_META`
  (server) and `HIDS` (client) map checks to dashboard rows. A check with no
  entry in `HIDS` renders nowhere; a row id with no check paints red as
  "No result". Both are asserted.

**`/monitor/health` is a real probe, and slow on purpose.** It queries the AWS
mirror across a WAN, so it is deliberately kept OFF the dashboard's 60-second
poll — it runs at load, every five minutes, on tab open and on the Re-check
button. The same checks run from `startHeartbeat()` every 30 minutes so a
failure alerts with the tab closed. Do not fold it into `/monitor/metrics`.

**`/monitor/website-recheck` is a POST.** It was a GET that rewrites lead rows
and runs two `ALTER TABLE`s, which a link prefetch or an unfurled URL could
have fired. Nothing in the UI calls it; run it with
`curl -X POST`.

**`/monitor` is not one page.** It's the dashboard plus several sub-routes that
feed it data. A reader who greps for a single `/monitor` handler expecting to find
everything will miss most of it.

**THE DASHBOARD IS `/monitor` SINCE PR D (26 Sept 2026), AND THE OLD ONE IS
`/monitor/classic` FOR ONE WEEK.** It was built side by side at
`/monitor/next`: five tabs in PR B (Overview, System health, Dropoff,
Duplicates, Lead magnet) and six in PR C (All leads, Blocked, SDR list,
Model, Visitors, Partners).

- **`/monitor/next` now 302-redirects to `/monitor`, keeping its query.** The
  browser keeps the `#tab=...` fragment across a redirect, so old bookmarks
  still land on their tab.
- **The classic is one click away** in the sidebar footer, and it says on the
  page that it is the fallback. It still honours `#tab=`.
- **Removing it is its own PR**, a week after the switch deploys. It is fed by
  its own inline JS in `index.js`. Four suites still fetch it by name
  (`test-non-icp-routes`, `test-lead-field-changes`, `test-apollo`,
  `test-monitor-next`), and three regions use its route as a marker, so the
  removal PR must move or retire those tests.
- **The running plan, decisions and progress are in `docs/monitor-plan.md`.**

- **Numbers come from the existing routes, plus ONE new read,
  `overviewReport` / `/monitor/overview`**, which applies one definition set
  to every window: "completed" is `submitted_at`; booked is AS OF the window's
  end; disqualified and blocked are the DROPOFF LADDER (`DROPOFF_STAGE_SQL`);
  **sessions** (form_sessions rows, never "page loads" -- that label was 6-13%
  short of the page loads it named) exclude `BOT_RE`; the funnel drops
  webhook-origin leads. **Overview and Dropoff agree exactly in Leads mode,
  and in People mode only when Dropoff's window is that one week** -- Dropoff
  places a person in the period they FIRST arrived within its whole window.
  Both are right; the Dropoff tab says so under its table. Every window is cut in ET wall-clock
  terms inside SQL, so "the same point last week" survives a DST change. It
  takes `db` as an argument and writes nothing, which is what lets the
  preview lift it; its two model-flag COUNTERS are named in
  `test-non-icp.js` 10f with reasons. **`duplicatesReport` is lifted the same
  way**: a route the branch CHANGES must never be proxied to production in
  the preview, or the screenshots show the old query and read as evidence.
- **Last week's bars line up by POSITION, never by date.** The server keys the
  comparison week by its own dates (the 14th-20th); the first build looked
  them up with this week's (the 21st-27th), so every grey bar was a confident
  zero under a "Last week" legend and every test passed -- because a
  zero-height bar is still a `<path d="">`. The test now counts bars WITH a
  shape and reads the value back out of the painted table.
- **Every theme alias is declared in BOTH `[data-theme]` blocks** -- a test
  compares the two sets, because a missing dark alias silently keeps its
  light value. **No calendar date from the viewer's clock**: all formatting
  goes through `Intl` with `GW.TZ`; a test forbids `getFullYear`/`getMonth`/
  `getDate`/`getHours` in `monitor/js`. **Every colour is a token** in
  `app.css`; a test forbids raw hex outside comments. **Every icon the code
  asks for must be in `monitor/icons`**; a missing one renders as nothing.
- **Text contrast is measured, not taken from the spec.** Light muted text is
  neutral-600 (5.0:1), not the design system's neutral-500 (3.4:1, under the
  4.5 floor); focus is solid brand blue, because the token ring (primary at
  40% alpha) is under 3:1 everywhere. Both declared to Utsav.
- **Every repaint goes through `GW.paint`**, which puts keyboard focus, a
  text field's caret and every open row back after the HTML is replaced. A
  tab that assigns `innerHTML` directly drops focus to the page body on
  every refresh -- every 60 seconds on Today.
- **PR C adds no server route and changes none.** Its six tabs render
  routes that already existed, unchanged; the server's label maps
  (`WEBSITE_REASON_LABELS`, `META_WITHHELD_LABELS`) travel in the page
  config, so there is no third copy to drift. Partner revenue gaps moved
  from its own spot onto Partners.
- **Plain tables go through `U.grid`, and the rtable card rules are
  CHILD-scoped.** Both were found only when the layout check was made to
  measure plain tables and to open rows. Visitors, the Partners gaps list
  and Model ran up to 277px past their card on a phone, scrolling sideways
  with the last column cut off; and the card rules, written as descendant
  selectors, restyled every table nested in an expanded row, so the lead's
  change log kept a header row above cells stacked with no labels.
  `U.grid` names each cell's column so a plain table stacks, labelled,
  below 560; the `.rt` rules are `.rt > tbody > tr > td`; a test forbids
  the descendant form. **A new plain table goes through `U.grid`** -- a
  test fails on a hand-built `class="tbl"` anywhere but the chart's own
  table view.
- **`tools/crosscheck-monitor.mjs` is the number-for-number check against
  the classic.** Each payload is fetched ONCE and served to BOTH pages
  through a fetch override, so a lead arriving between two reads cannot
  make them differ; it also checks the partitions every filter must keep
  against the live total. It prints numbers and pass/fail only. Run it
  against the preview after any change to how a tab reads its payload.
- **The map follows the theme.** Esri Canvas light and dark basemaps, both
  checked by downloading tiles at three zooms. What shows past the edge of
  the world on a short map is `--map-sea`, the tiles' own ocean colour,
  sampled, so it reads as sea rather than a gap. Fractional zoom was tried
  to remove that band and rejected: it draws seams between tiles.
- **Touch targets are 44px** below 1024 wide or on any coarse pointer, and
  **nothing floats over content** -- the design system's phone dock was
  dropped for that rule, and theme and refresh live in the drawer instead.
- **Two tools replace "it looked fine on my laptop"**:
  `tools/preview-monitor.js` (the branch against live data, read-only) and
  `tools/check-monitor-layout.mjs` (every width, both themes, fails on
  layout). The test suite sees numbers; the layout check sees layout. Run
  both before asking for a merge of anything under `monitor/`.

**Eleven tabs as of 25 Sept** on the OLD dashboard — the list lives in `showTab`, and `Dropoff`
is the newest. A tab needs a `t-<name>` button, a `tp-<name>` panel, an entry
in that array and a loader guard, or it renders nowhere and nothing says
so. **Count the array, do not trust this number** — it said ten the day
before.

**THE DROPOFF TAB ANSWERS "where do the people who start the form go", AND
ITS LADDER IS THE CONTRACT.** `/monitor/dropoff`, filtered by date range,
week or month, source, and leads-or-people. Seven outcomes in
`DROPOFF_STAGES`, resolved top-down by the single `DROPOFF_STAGE_SQL`
expression, mutually exclusive and exhaustive — so the rows **always sum to
the period total**, and `test-non-icp-routes.js` asserts that sum per period
and overall rather than trusting it. A second copy of that CASE anywhere is
how two consumers start disagreeing; it is the `disqualified` /
`non_icp_blocked` lesson waiting to arrive a third time.

Built 25 Sept because the question had been asked twice by hand in one
Slack thread and a hand-answer is wrong the next morning. It changes no
blocking behaviour and fires no Meta events — it is a read.

**"Left on step 2" (`step_reached=1`) means the visitor COMPLETED step 1 and reached step 2**: the row is written at the end of `handleStep1Next`. Never call it a step-1 drop-off. A lead stopped on step 2 by a blocking website verdict leaves NO row. Background: `docs/background/dropoff-and-digest.md`.

**Source reads `utm_source`, then `hear_about_us`** (`DROPOFF_SOURCE_SQL`): `utm_source` alone understates Meta by about a quarter, because in-app browsers lose the UTMs and the form records the referrer. Human free text stays in "Direct / organic" on purpose. Leads or people is a toggle, not a correction. The bucket comes back as TEXT through `to_char`, because `node-postgres` parses a `date` in the process's own zone. Background: `docs/background/dropoff-and-digest.md`.

**The weekly digest** (`runDropoffDigest`) reports the COMPLETED week, Mondays in the 09:00 ET hour, to the LEADS channel (`sendSlack`, not the alerts one), leaving out our own test submissions and saying how many. It ticks every 15 minutes, not 60, so a deploy inside the hour cannot skip a week; the near-miss digest ticks the same and the two stay in step. Background: `docs/background/dropoff-and-digest.md`.

**THE CSV EXPORTS NEUTRALISE FORMULAS BUT NEVER TOUCH A PHONE NUMBER, and
the second half is the hard part.** `csvCell` in `index.js` builds every cell
of the All leads and SDR exports (the routes both dashboards call). Decided
26 Sept 2026.

- **`=`, `@`, a tab or a carriage return at the start:** always gets a
  leading apostrophe.
- **`+` or `-` at the start:** gets one ONLY when the cell is not just a
  number (`CSV_NUMBER_ONLY`: digits, spaces, `( ) . -`, and at least one
  digit).
- **Then quoted** on a comma, a quote, a CR or an LF.

Why: 2,669 of 2,676 stored phones start with "+". A spreadsheet still reads a number-only cell as a number, which is accepted; splitting on `;` in some locales is an open decision for Darshil. Background: `docs/background/dashboard.md`.

**THE GOOGLE ADS CONVERSION UPLOAD — 6 Oct 2026, and its traps.**
`google-ads-conversions.js` sends booked Google Ads leads to account
5442288209, action 7825004775 ("CRM - Qualified Demo Request").

- **Data Manager API, NOT the Google Ads API.** `UploadClickConversions`
  refuses new adopters since 15 June 2026
  (`CUSTOMER_NOT_ALLOWLISTED_FOR_THIS_FEATURE`), and this repo never adopted
  it. Data Manager needs **no developer token** and the scope
  `https://www.googleapis.com/auth/datamanager` — not `adwords`. Credential:
  `GADS_SERVICE_ACCOUNT_JSON_B64`, the key of
  `gads-conversion-uploader@gushwork-crm.iam.gserviceaccount.com`, which is a
  user DIRECTLY on 5442288209, so no login account is sent.
- **Off, then validate-only, by default.** `GADS_UPLOAD_ENABLED=true` turns
  the sweep on; it stays validate-only (Google checks, records nothing) until
  `GADS_UPLOAD_VALIDATE_ONLY=false`. **No `GADS_UPLOAD_CUTOVER` means nothing
  sends**, and a cutover without its offset is refused — Lorenzo's sheet
  covers everything before it, so the boundary must be exact.
- **A validate-only 200 DOES check the action exists** (a made-up ID answers
  404 `INVALID_CONVERSION_ACTION_ID`, which is permanent and alerts) but
  proves nothing about the click: matching and the 6-hour rules only happen
  on a real send.
- **It skips more than Meta does, on purpose, and the lists are Google-only.**
  Flighted and Upraw (`GADS_EXCLUDED_DOMAINS`) and any other `*.webflow.io`
  or loopback host (`GADS_EXCLUDED_HOSTS`) are NOT in `INTERNAL_TEST_EMAILS`
  or `ELV_EXCLUDED_DOMAINS`, because those also gate Meta, Salesforce and the
  dialer. Both env lists EXTEND their defaults and cannot shrink them.
- **`non_icp_llm_flagged` is read in CODE at upload time, never as a SQL
  filter** — section 10f of `test-non-icp.js` forbids a flagged lead being
  kept out of Salesforce, PartnerStack, the SDR list or the dialer. This is
  an ad signal, like Meta, and it skips flagged leads whatever
  `NON_ICP_LLM_META` says. The **fresh verdict read fails CLOSED** (waits a
  sweep) — the opposite of the lead path, because an upload can wait and a
  realtor sent to Google's bidder cannot be taken back.
- **Phone is E.164 WITH the `+`.** Meta's `normalizePhone` strips it; reusing
  it would hash a different string and match nobody, silently.
- **Built inside `start()`, never at the top level** — it reads
  `DROPOFF_SOURCE_SQL`, a `const`, which is the temporal-dead-zone break.
- **Value is the product lookup, not `meta_predicted_ltv`**, which is empty
  whenever Meta did not fire.
- **A 200 from `events:ingest` is NOT a conversion.** After every REAL send
  the sweep asks `GET /v1/requestStatus:retrieve?requestId=…` (from 30 minutes
  after, backing off to daily) and records the outcome per row. One event per
  request is what makes a request's status that event's result. Unmatched,
  duplicate, dropped and never-answered each alert once, as a warning; the
  write that finalises a row is conditional, so two sweeps cannot alert twice.
  Google's reference gives no timing for PROCESSING or for how long a request
  ID stays readable, hence the 7-day give-up rather than a guessed number.
  Validate-only requests cannot be asked about at all (Google's own 400).
- **Consent is a switch, `GADS_CONSENT_GRANTED`, off by default.** On, every
  event carries `consent: { adUserData: CONSENT_GRANTED, adPersonalization:
  CONSENT_GRANTED }` — the Data Manager reference's names, the same claim
  Lorenzo's sheet makes. Off, the field is ABSENT, never denied. It sits on
  the EVENT, so it is frozen with the payload: a row claimed before the
  switch is flipped retries without it, while a `validated` row is rebuilt
  when real sending starts and picks up the current setting. Validate-only
  accepted it on 6 Oct 2026 (200).

**Booking arrives by three routes.** `/booking-confirmed` (browser-fired),
`/booking-confirmed-webhook` (Cal), `/booking-confirmed-webhook-rh` (RevenueHero).
Any change to booking behaviour has to be applied to all three. A fix on one is a
fix on one third.

**PartnerStack: `partnerStackCustomerKey` is the only place a domain becomes a
customer key.** PartnerStack counts one conversion per customer key for the life
of the account, so two spellings of one company means either an affiliate paid
twice or a real referral swallowed as a duplicate. Website, email and the three
warehouse customer tables all go through this one function. Do not normalise a
domain inline anywhere near this integration.

**A disqualified lead never fires a conversion, and that is a GUARD now, not a
flow property.** No disqualified lead reaches `/submit` today — `b2c_or_mixed`
and `waitlist` both call `savePartial(1)` and then show a terminal step — but
that lives in two forked frontend files which have drifted apart before, and the
cost of the drift here is paying an affiliate $50 for a B2C waitlist signup.
`runPartnerStackSignup` checks the flag explicitly and a test asserts it runs
before the domain and test-email checks.

**Every `/submit` logs whether a partner was present.** An organic lead used to
produce no PartnerStack line at all, so the logs could not tell "no partner
traffic yet" from "capture is broken" — which cost real time on the first
deploy. `[PartnerStack] No partner on this submit (…)` is the negative case, and
each skip says which guard stopped it.

**The PartnerStack handover doc is `docs/partnerstack.md`.** Anything below is
the short version; that file has the full picture including the known gaps.

**Two PartnerStack hosts, two auth schemes, one env.** The conversion goes to
`partnerlinks.io/conversion/xid` with `PARTNERSTACK_TRACKING_TOKEN` as a
**Bearer** token. The partnerships lookup and the qualification action go to
`api.partnerstack.com/api/v2/*` with **Basic** base64(`PARTNERSTACK_PUBLIC_KEY`:
`PARTNERSTACK_SECRET_KEY`). Both credentials sit in the same environment and
using one where the other belongs returns a 401 that reads like a bad password
rather than the wrong scheme. A test asserts `sendConversion` never reaches for
the key pair.

**Partner identity resolves in three layers (memory, the database, the API) and never blocks a lead.** Only the first two are awaited before Slack, a failed lookup is never cached, and every surface shows name, then email, then raw key (`partnerDisplayName()`). A blocked lead's Slack post is its only human-facing record, so `slackPartnerHearLabel` relabels an unresolved key in words, display only: the stored `Partner - <key>` placeholder must stay exact for `upgradePartnerHearAboutUs`. Background: `docs/background/partnerstack.md`.

**`hear_about_us`: an existing referral outranks a partner.** `gw_ref_email` is a
named human vouching for the lead and is a stronger signal than an affiliate
link, so anything already starting with `Referral -` is left alone. The upgrade
only ever rewrites the exact `Partner - <key>` placeholder this code wrote; a
referral or anything a human typed is never touched.

**A late-arriving single field needs its own targeted AWS write, NEVER
`syncToAWS`.** That upsert's conflict clause sets
`disqualified = EXCLUDED.disqualified` with no COALESCE, so handing it a partial
object passes `disqualified` as false and CLEARS a real disqualification on the
mirror the dialer reads. `syncBookingToAWS`, `syncPartnerIdentityToAWS` and
`syncHearAboutUsToAWS` all exist for this reason. A test catches the regression.

**Every alert path gets FIRED ONCE ON PURPOSE before launch** — `node
tools/fire-alert.js <name>` does it, and it sends for real.**  "We asserted it
alerts" and "we watched it alert" are different claims, and the gap between
them hid 21 dead alert call sites in this repo — all of them tested, all of the
tests passing, none of them ever producing an alert. No assertion about a call
site can tell you the call did anything; that is the ceiling of a source-level
assertion, not a flaw in a particular test. Only executing the code or watching
the real thing arrive gets past it.

**A hand-run API read is not a better oracle than the sweep that waits.** When a check is built to wait before believing a negative, a quicker manual `curl` is evidence of nothing; acting on one nearly re-fired a PartnerStack conversion that had landed. Wait for the sweep, or read what it last concluded. Background: `docs/background/testing.md`.

**Do not reduce `PS_VERIFY_GRACE_MIN`.** PartnerStack's read-after-write lag
was measured at **over 11 minutes** on 7 Sept 2026 — a customer created at
16:38 still 404'd on a direct API read at 16:49. The margin at 15 minutes is
about **four** minutes, not the nine a 2-to-6 minute range implied. A manual API read is
also not a better oracle than the sweep: that 404 was nearly used as grounds to
release a claim for a conversion that had in fact landed, which is the exact
duplicate-credit failure the grace period prevents.

**`recordFailure(source, …)` is a SILENT NO-OP for any source with no
`FAILURE_MONITORS` entry** — it opens with `if (!cfg) return;`. `'PartnerStack'`
had no entry from the day the integration shipped, so 21 call sites across the
money path had never produced a single alert, several of which were described
as "loud". A test in `tests/test-batch2.js` now derives every source used
anywhere and requires an entry, so this cannot recur for a new source. Keep the
two paths straight: `recordPartnerStackFailure` calls `alertOps` **directly**
and always worked; the health rows run their own queries and never touched the
table.

**An ACK means "this failure is understood, leave it alone" — not just "stop
alerting me".** Four things act on a partner failure and all four must respect
`ps_failure_ack_at`: the unbounded Needs-attention count, its page-derived
fallback, the `partnerstack` health row, and the **conversion retry sweep**.
The retry was missed when it shipped, because the ack's scope had been written
back when only an alert acted on a failure — and the sweep then re-created a
PartnerStack customer that had been deleted by hand. If you add a fifth
consumer, it respects the ack or an acknowledged failure starts demanding
action again through it. An ack never clears the failure stamp: the row keeps
its state, its red chip and its history.

**"Needs attention" is counted by its OWN unbounded query, not from the
capped domain list beside it.** The ladder includes an unresolved failure
regardless of age so it cannot drop out of the one number somebody must act on
— and `LIMIT 500` on the domain list would undo that. When the unbounded query
cannot run, `needsAttentionComplete: false` reaches the screen as "AT LEAST
this many". A floor is never rendered as a total.

**A stale `partner_domain_sf_state` is a RED health row.** When that column
freezes, "waiting on an AE", "no Opportunity" and the funnel's Opportunity and
ticked stages all keep rendering their last values as if current. Checked after
the failure states but before green.

**Partner revenue gaps (`/monitor/partner-gaps`) is a WORK QUEUE, not a health check; never wire it into System Health.** It finds (A) a domain where no conversion was EVER sent, keyed by domain, and (B) a demo with no Opportunity. "Booked but not qualified" is the wrong check, because an AE's deliberate no-tick is permanent and correct. `leads.start_time` is TEXT, so every cast sits inside a regex `CASE`; unreachable Salesforce shows `N+?`, never zero. Background: `docs/background/partnerstack.md`.

**Step 10 is a 2-minute POLLER, not a Salesforce Flow callout**, joined on DOMAIN. Three timings must never be collapsed onto each other: the 2-minute ticked-Opportunity poll, `PS_SF_REFRESH_INTERVAL_MS` (15 min, the full scan) and `PS_VERIFY_GRACE_MIN` (15, the read-back grace). Background: `docs/background/partnerstack.md`.

**Only domains whose conversion is VERIFIED (`ps_signup_verified_at IS NOT
NULL`) can be qualified** — not merely sent. `ps_signup_sent_at` only means
PartnerStack answered 200, and `/conversion/xid` answers 200 with an empty body.
Qualifying on the weaker stamp and then having the read-back sweep 404 releases
the conversion claim and leaves the qualification claim stamped forever, so the
domain re-converts and can never be qualified again: $50 gone with no error
anywhere. Fixed 7 Sept 2026. And beyond that, an action for a `customer_key`
PartnerStack has never seen is a no-op at best.

**`partner_domain_sf_state.sf_state` is a SNAPSHOT that moves both ways**, because an AE can untick. For a fact, read `first_ticked_at` / `first_opportunity_at` (stamped once, never cleared) OR'd with `ps_qualified_sent_at`, and never backfill those two stamps. Nothing reacts to an untick, deliberately. Background: `docs/background/partnerstack.md`.

**`Partner_Source__c` is this service's only write to Opportunity, and a permission failure looks exactly like "no Opportunity".** `updateOpportunityFields` classifies `permission` apart from per-record failures and never throws, `PS_SF_OPP_WRITE=false` stops it from the env, and it writes only when the value changed. Background: `docs/background/partnerstack.md`.

**Creating a Salesforce custom field through the Tooling API does NOT grant
access to it.** Both fields created on 4 Sept came back 201 and were invisible
to the user that created them until `FieldPermissions` rows were added. So a
`400 INVALID_FIELD_FOR_INSERT_UPDATE` means "no field-level access", not "no
such field". Create, then grant, then verify with a real round-trip.

**The automated eligibility check is BUILT AND OFF for the MVP.** Rejections are
decided by hand at payout approval. Everything below is dormant behind
`PS_ELIGIBILITY_ENABLED`, default off — set it to the string `true` in the
Railway env to turn it on, no rebuild needed. The conversion call deliberately
does **not** consult it: today every partner lead that is not one of our own
test addresses converts. Wiring the check into that path is a decision for
someone to make, not a tidy-up, and a test asserts the two stay unconnected.

**"Once per domain, ever" is enforced by the DATABASE, not by application
code.** `leads_ps_signup_once_idx` is a UNIQUE PARTIAL index on
`ps_customer_key WHERE ps_signup_sent_at IS NOT NULL`. `runPartnerStackSignup`
CLAIMS the domain with a conditional UPDATE *before* the HTTP call and only the
winner sends; a concurrent claim surfaces as `23505` and is read as
"already sent". Checking then sending would race — two submits for one domain
arriving together would both fire, and PartnerStack cannot undo a double credit.
If the send fails the claim is RELEASED, because a stamp on a conversion that
never arrived is silent, permanent, and costs the affiliate a real payout.

**PartnerStack eligibility FAILS CLOSED, and that is deliberate.** It is the one
check in the repo that does not follow "a lead is worth more than a verdict" —
because it does not touch the lead at all. It decides whether an *affiliate* gets
paid. The conversion call fires once per key forever and cannot be recalled,
while a conversion we skipped is still in the log to send by hand, so a check
that cannot run returns `check_failed` rather than waving it through. No lead is
ever blocked, delayed or de-Meta'd by it.

**PartnerStack eligibility runs AFTER `res.json()`, and must stay there.** It is
fire-and-forget in `/submit`, next to `finaliseElvVerdict`, and it only runs when
`ps_xid` is present. Three things keep the warehouse off a lead's critical path
and all three are load-bearing:

- the rule (b) query is wrapped in `withTimeout(..., PS_CUSTOMER_QUERY_TIMEOUT_MS)`
  — `awsPool` has **no** `statement_timeout`, so an RDS instance that accepts
  connections but answers slowly hangs forever otherwise. Fail-closed does not
  save you here: a hang never reaches the catch. Three hung queries also exhaust
  `awsPool` (`max: 3`) and starve `syncToAWS`.
- the customer cache is warmed at boot and every 30 min by
  `startPartnerStackCacheWarm()`, so no lead ever pays for the fetch. Measured
  cost when healthy: 0.8–3.4s across the WAN.
- eligibility is never awaited in the route.

**Step 5's conversion call goes after this verdict, not before it.** Moving it
earlier to "save a round trip" puts a third-party HTTP call in front of a waiting
lead. `tests/test-partnerstack.js` asserts the ordering and that it is not
awaited; both mutations are caught.

**Eligibility rule (a)'s contact source is a registry (`PS_CONTACT_SOURCES` / `PS_CONTACT_ACTIVE`), today prior form leads only; measure before switching an outbound log on.** Rule (b)'s 12-month churn clause cannot be enforced from `gw_prod` and comes back `unverified` on every pass. Background: `docs/background/partnerstack.md`.

**Six verification columns never reach the AWS mirror.** `elv_status`,
`elv_checked_at`, `website_check_failed`, `website_check_reason`,
`website_check_reason_prev` and `website_rechecked_at` exist on Railway `leads`
and not on `gw_form_leads`. The dialer cannot see whether a lead was verified.
Known, not fixed here.

**THE CHANNEL ATTRIBUTION LOGIC LIVES IN SALESFORCE, NOT IN THIS REPO — AND WE
FEED IT.** `Lead.Source_Bucket__c` is a **formula field**. There is no stored
value; it recomputes every time anyone reads it, from exactly two inputs:

| Input | Written by |
|---|---|
| `utm_source__c` | this repo, from the ad click |
| `How_Did_You_Hear__c` | this repo, via `MIRROR_FIELDS` in `salesforce.js` |

So **nothing in this repo decides a bucket, and nothing in this repo can be
grepped to find out what one means.** The logic is Salesforce metadata, read and
written through the Tooling API on `CustomField` `Source_Bucket` /
`TableEnumOrId='Lead'`. Read it there before reasoning about attribution.

**The consequence that will bite: `hear_about_us` is not just a display string.**
It is a production input to somebody else's reporting. Changing its format,
prefixing it, or reusing it for a new purpose silently re-buckets leads with **no
error anywhere** — the formula keeps returning a value, it is just the wrong one.
The `Partner - <key>` and `Referral - <email>` placeholders already flow into it,
which is exactly the kind of prefix that can start matching a branch by accident.

**`How_Did_You_Hear__c` had no writer until 17 Sept 2026, and the formula matched two-letter substrings with no word boundary** (fixed 16 Sept). **Verify a formula change by REPLAYING it** over every real record, old logic first, and require the old one to disagree with live Salesforce zero times. Background: `docs/background/attribution.md`.

**`Source_Bucket_New__c` is NOT a newer `Source_Bucket__c`**: it is a live Opportunity picklist for the winning sales motion; never consolidate or delete it. **`ConvertedOpportunityId` is the wrong join for "did this lead become a deal"**; go through `ConvertedAccountId`. Background: `docs/background/attribution.md`.

**Known open bug:** the duplicate-booking guard looks up the *newest* lead row per
email and asks whether that row has a booking. A second form submission creates a
newer row, so the same person can take two calendar slots. Known, deferred by the
owner — do not "fix" it without asking.

