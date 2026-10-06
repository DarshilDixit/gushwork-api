# Background: The non-ICP block and the model layer

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## The model layer's name-only input, floors and history (was `CLAUDE.md` lines 59-119)

   **THE MODEL LAYER HAS TWO INPUTS SINCE 23 SEPT, NOT ONE, AND THE SECOND
   IS WEAKER ON PURPOSE.** `NON_ICP_NAME_FALLBACK` (default OFF, **`true`
   in Railway**) runs ONLY when the scrape failed, and judges the **domain
   name alone** — no page, no company field. Measured: 36 of 254 domains
   came back unreadable, and three of the five insurance / real-estate
   demos that leaked through were exactly those. None was fixable by
   fetching harder — `longandfoster.com` answers 403 behind a bot wall,
   `westexinsurance.com` does not connect, `adrianadearaujorealtor.com`
   returns 200 with 64 characters because it renders client-side.

   It carries **its own higher confidence floor (0.85 against 0.75)**, its
   own `non_icp_source='llm_name_only'` and its own prompt version, because
   a 0.75 meaning "the page says we sell insurance" and a 0.75 meaning "the
   hostname contains those letters" are not the same claim. `unknown`
   returns null rather than a verdict.

   **0.85 since 26 Sept 2026, down from a provisional 0.9 (Darshil),** after
   the misses it was waiting for: `homes.com` 0.88 and the www-typo of
   `planrightlegacyins.com` 0.85 now block; the 0.82s still do not.

   **ONE FLOOR PER KIND OF EVIDENCE, FOR BOTH ACTIONS.** The Meta-only read
   used the page floor for every verdict until 26 Sept, so a name-only guess
   too weak to block still withheld Meta — `hernandezins.com`, insurance
   from the letters "ins" at 0.82, whose website read as a business-funding
   coach. `nonIcpFloorFor(source)` is now the one place a read picks its
   floor, and the lead row records `llm_name_only` (it said `llm`, "read
   their website", for every model verdict).

   **A name-only verdict is cached for SIX HOURS, not 180 days, on purpose**
   (the comment above the TTL in `nonIcpReadVerdictRow` says why: it exists
   only because the site would not load, so the site should be tried again
   soon). This file used to imply 180 days.

   **Blocking is read at decision time from the type and its floor, as well
   as from the stored `blocking` column** — so a floor change applies to the
   cache at once. Measured 26 Sept: every page verdict reads the same either
   way; only name-only rows between the old and new floor gain a block.

   **The domain, never the company field the visitor typed.** The cache is
   keyed by domain; a per-lead input would make the stored verdict depend
   on whichever lead warmed it first. Validated against 60 real unreadable
   domains before switching on: 6 would block, all real estate, no false
   positives, and the near-misses (`creativelendersllc.com` →
   mortgage_lending, `tcwealthadvisors.com` → financial_advisory at 0.90)
   correctly did not — the **enum** kept them safe, not the prompt.

   **NEITHER REPLACES THE OTHER AND THE LIST IS NOT BEING RETIRED.** An
   earlier version of the V1 ticket said it would be, attributed to Swapnil.
   It was not his, and it is wrong on the measurement: `farmers.com` and
   `farmersagent.com` both read `scrape_status=failed` in the warehouse
   classifier and are three of the first four real blocks. National brands
   are exactly the sites a scraper cannot read. See the CORRECTION section
   at the end of `docs/tickets/non-icp-v1-block.md`.

   It exists because AEs reported State Farm agents and realtors taking demo
   slots, and because the measurement backed them: those leads book at 71%
   against 64% overall and then attend only 47% of the time against 66%. It
   was authorised explicitly by Swapnil on 11 Sept 2026, and it **reverses two
   written positions in the Non-ICP doc** — see
   `docs/tickets/non-icp-v1-block.md`, which is the record of that.

## Why the Schedule guard is async (was `CLAUDE.md` lines 196-216)

   **IT IS ASYNC SINCE 23 SEPT, AND IT RE-READS THE VERDICT TABLE.** The
   two conditions above read a COLUMN stamped at `/submit`, and that column
   can be wrong in one direction: `/submit` is a cache read and nothing
   else, so when the warm has not finished it correctly stamps
   `non_icp_blocked=false` — "we could not check" — and the verdict lands
   seconds later with nobody listening. Measured on 18 Sept:
   `steenhoekinsurance.com` came back `insurance` at **0.97 confidence 2.6
   SECONDS after** the submit, and `dla1972@me.com` 11.7 seconds after.
   Both booked.

   So the guard now also asks `non_icp_domain_verdicts` directly, and
   **fails open on any throw**. It withholds the Meta `Schedule` event and
   NOTHING else — the booking is already real and the person is looking at
   a confirmation. All three routes `await` it.

   Turning it async is what broke `tests/test-non-icp.js` §8, which LIFTS
   AND RUNS the function: `new Function` cannot hold an `await`, so the
   suite died at load. That is the test doing its job on a signature change
   touching all three call sites; it now builds through the AsyncFunction
   constructor.

## The `disqualified` guards, missed in three waves (was `CLAUDE.md` lines 992-1015)

**A GUARD ADDED TO THE OBVIOUS SITE MISSES ITS SIBLINGS. This happened three
times in one night, to the same column.** `leads.disqualified` has **fourteen
call sites** across routes, crons, sweeps, health checks and metrics queries,
and **no single grep reaches them all** — they are spread over `/submit`, the
recovery cron, two PartnerStack sweeps, the SDR list, three Overview cards, a
work queue, the stage ladder and a per-email history subquery.

Introducing `non_icp_blocked` — a second column meaning "we rejected this
lead" — turned **every** existing `disqualified` guard into half a guard
overnight. Found in three waves: the PartnerStack conversion (cost money, fired
in production), then the SDR list and recovery health, then three Overview
counters a day later.

**COUNTING PREDICATES IS NOT DECIDING THEM, and that mistake shipped too.**
The first audit pinned the *number* of `disqualified` predicates at 13. The
number was correct and it let two of the three waves through.

`tests/test-non-icp.js` §10b is now a real audit: it resolves every predicate
to its enclosing route, reads the **enclosing query** rather than a byte
window, and requires each to be **either guarded or named in a `DELIBERATE`
list with a written reason**. Two are deliberately exempt — the
`/monitor/metrics` disqualified counters, and `/monitor/leads`' stage ladder
and `prior_disqualified`.

## Why the late-verdict sweep has hours, not seconds (was `CLAUDE.md` lines 1030-1036)

**It exists because the cost is incurred at the MEETING, not at the
booking.** Measured: booking→demo median **35 hours**, 10th percentile
**3.3 hours**, only 3.9% of demos within an hour of booking. There is no
2.6-second race to win — there are hours. The 4s calendar hold in the two
form files covers the seconds; this covers everything slower, including a
19.6s scrape, a cold cache after a deploy, and the name-only fallback.

## The model layer's four traps (was `CLAUDE.md` lines 1171-1206)

**THE MODEL LAYER'S OWN TRAPS.** Four, and the first is the one that
decides whether this feature is safe at all.

*One.* **Blocking is decided in CODE from the enum, never by the model.**
The model is only ever asked what the company *is*; `NON_ICP_BUSINESS_TYPES`
in `index.js` says which types block, and exactly two do. If the model could
return its own verdict, a page could talk its way out of a block in one
sentence — and, worse, talk somebody else into one. The output schema does
not even offer a `blocking` field, and a test asserts that a model-supplied
one is ignored.

*Two.* **Never key a block on the free-text `category`.** The warehouse
classifier's `category` column holds **65 distinct spellings** of real
estate and insurance — `Real Estate Tech`, `Real Estate / PropTech`,
`Software / Insurance Tech`. A block on `category ILIKE '%real estate%'`
turns a proptech SaaS company into a blocked lead. That is `paycompass.com`
one layer up, and it is why `business_type` is a closed enum declared to the
API as a structured output.

*Three.* **The page text is untrusted and the schema is not the defence.**
It goes in a user message inside `<untrusted_page_text>` delimiters, never
in the system prompt, markup stripped and capped at 12k chars. What actually
bounds the damage is the *direction* of the error: an injection that makes
us **not** block is the status quo, and the costly direction — a wrong block
of a real prospect — is one an attacker has no reason to aim at. So the
guardrails point at accidental false blocks: the confidence floor, the
known-customer bypass checked **before** any block, and a Slack post
carrying the verbatim evidence quote so a wrong block is visible in minutes.

*Four.* **The verdict is a CACHE READ at the moment of decision.** No
scrape, no model call, no network, and deliberately no timeout to fall
through — a lead who reaches the calendar because our own request was slow
is the failure this shape exists to prevent. Warming happens on blur, via
the `/non-icp-check` calls `gushwork-form.js` **already makes** for V1,
which is why **neither form file changes** and the Ads fork needs no port.

## The brand-list block's three traps (was `CLAUDE.md` lines 1207-1230)

**The non-ICP block has three traps, and two of them look like working code.**

*One.* `partnerStackCustomerKey` **collapses subdomains** — it ends in
`registrableDomain`, so `agents.farmers.com` becomes `farmers.com`. Matching
only on its output makes every subdomain entry on `NON_ICP_DOMAINS`
(`agents.allstate.com`, `ft.newyorklife.com`, `agents.farmers.com`) permanently
dead while looking live. `nonIcpHostForms` keeps the full host alongside the
collapsed one and both are matched. Caught by a test, not by reading.

*Two.* **`non_icp_blocked` is STICKY in all three upserts**
(`leads.non_icp_blocked IS TRUE OR EXCLUDED.non_icp_blocked IS TRUE`), and that
is load-bearing rather than tidy. `/partial` fires repeatedly through step 1,
and the "actually we're B2B" button calls `savePartial(1)` again — **74 of the
84 known realtor and insurance leads reached the calendar through exactly that
button**. An `= EXCLUDED` assignment lets a realtor clear their own block by
clicking it. A test pins all three sites.

*Three.* **Never match a brand domain by substring.** Measured on 5,123 real
leads: substring caught 116 where exact-or-subdomain caught 110, and five of
the six extra were wrong — `paycompass.com` (a payments company, contains
`compass.com`), `charleslegalpl.com` (a law firm, contains `lpl.com`),
`theimagecreatornm.com`, `krevera.com` and `ceterainvestors.com`. All five are
pinned as negative fixtures in `tests/test-non-icp.js`.
