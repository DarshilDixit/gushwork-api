# Open items — gushwork-api

Written 9 Sept 2026, derived from `git log fe07b0a..ae15777f5434a458e99eef6670b6a3c6fcabfe43` and from reading
the code, not from anyone's recollection. Line numbers are as of
`ae15777` and will drift; the symbol names will not.

**The other half of this lives in the meta-capi repo, `docs/OPEN-ITEMS.md` at
`1ef3c58`.** Neither file is complete on its own: the two repos share the
`gw_form_leads` mirror and the Meta pixel, and at least one item here
(`submitted_at` as an event-time fallback) is only visible if you read both.

**Extended 17 Sept 2026** with items 15-17 and six Decided entries, from the
Source Bucket investigation. Those items **cross into Salesforce**, which this
file was previously scoped away from — they are here because nothing else in
either repo records them, and because half of each one is our code.

Everything fixed on 8 Sept is deliberately absent. This file is what is still
true, split into **open** — someone has to decide or do something — and
**decided** — settled, recorded so it is not re-litigated. A thing that was
merely *discussed* is open. A thing that was *fixed* is neither.

---

## Open

### 1. A total `/submit` outage is invisible for up to 24 hours

`HEALTH_SUBMIT_WINDOW_H = 24` (`index.js:1590`). The `submit` health row is
green whenever `completions > 0` in a trailing 24-hour window, so a route
that is failing 100% keeps reporting green until the last pre-outage
completion ages out.

This is not hypothetical. On 8 Sept `/submit` returned 500 for every lead for
31 minutes and the health row stayed green the whole time on 16 completions
from before the break. The bug was found by someone writing an unrelated test.

The asymmetry is the tell: `/partial` uses a **2-hour** window
(`HEALTH_PARTIAL_WINDOW_H`), and `/submit` — the more valuable event — uses
24. A shape that would work: compare completions against step-1 volume over a
short window, rather than testing for absolute zero over a long one.

### 2. During a `/submit` outage the recovery cron emails real completers as drop-offs

`/cron/send-partials` (`index.js:9077`) selects on
`completed = false AND booking_uid IS NULL AND loops_sent = false AND created_at < NOW() - INTERVAL '2 hours'`.

Someone who filled in step 2 during an outage has `completed = false`, so
they receive a "you didn't finish" email — and it sets `loops_sent = true`,
so that path can never reach them again.

**Verified untouched by anything on 8 Sept**: no commit in
`fe07b0a..ae15777` modifies that route.

### 3. `backfill-sf.js` now aborts on the first Salesforce rejection — REGRESSION from 8 Sept

`backfill-sf.js:170` does `const result = await pushToSalesforce(payload)` and
branches on `result.success`. Commit `552db39` made `pushToSalesforce` **throw**
on failure so the critical alert could fire — and that loop is **not inside a
try/catch** (checked: nearest `try` is at char -1, there isn't one).

So the first lead Salesforce rejects now aborts the whole run. The
`action: 'FAILED'` branch and the per-lead reporting below it are unreachable
for a real rejection, and the caller gets a rejected promise instead of a
report naming which leads failed.

This only bites when the tool is actually needed — recovering after a
Salesforce outage, which is exactly when rejections are likely. CLAUDE.md
calls this "a kept tool, not dead code".

The fix is a try/catch inside the loop that records `FAILED` and continues.
It was not done on 8 Sept because nobody noticed the coupling.

### 4. `about_business` is not searchable, and nothing records whether that was deliberate

`SDR_SEARCH_COLUMNS` (`index.js:2949`, server) and `SDR_SEARCH_FIELDS`
(`index.js:4127`, client) both hold exactly
`['email', 'company', 'first_name', 'enriched_industry']`. `about_business`
was left out when the field shipped.

Leaving it out may well be right — it is free prose, the SDR query is deduped,
and adding a column there changes what the CSV export matches. But the
decision was never made, only skipped, and the two lists must change together
or the export silently stops matching the screen (a test asserts they are
equal).

### 5. Schedule re-resolves the product instead of reading the stored column

`SCHEDULE_LEAD_SQL` (`index.js`) selects `l.page_url` but **not** `l.product`
(verified), and `meta-capi.js` derives the slug with
`resolveProduct({ page_url: payload.page_url })` at send time.

Identical today. It stops being identical the moment `PRODUCT_PATHS` changes:
a booking made after the change is tagged by the new rule while its stored
column keeps the old one. One source of truth in code, two in time.

### 6. `followup_attempts` is created by the cron, not by `db.js`

`index.js:9081` runs `ALTER TABLE leads ADD COLUMN IF NOT EXISTS
followup_attempts INTEGER DEFAULT 0` inline inside the recovery cron.
`db.js` has no mention of it (verified).

So the column exists only after the cron has run at least once, and a reader
of `db.js` — the file whose whole job is the schema — cannot know it exists.
On a fresh database anything querying it before the first cron run fails.

### 7. `websiteCheckNote()` is dead

`index.js:770`. Declared, referenced nowhere in any source file, tool or test
(verified repo-wide). Pre-dates 8 Sept.

### 8. 382 mirror rows would take the sync-time fallback as their event time

`gw_form_leads.submitted_at` is **the time the row was synced**, not the time
the form was submitted — the value is `new Date()` at the moment `syncToAWS`
runs, sometimes hours later. This is documented at the bind site and in
`docs/partnerstack.md`, and it is a trap because the column name says
otherwise.

The meta-capi repo's CompleteRegistration script uses it as an `event_time`
fallback when `start_time` is absent. Measured from this side, 9 Sept 2026:

| | rows |
|---|---|
| `gw_form_leads` total | 5,028 |
| completed | 3,575 |
| completed with **no** `start_time` | **382** |
| …of those, with `submitted_at` set | 382 |
| **would therefore use the fallback** | **382** |
| of which, created in the last 90 days | **222** |

So the fallback is not a rare edge: it decides the event time for roughly
one completed lead in nine, and the time it supplies is our sync clock. Worth
deciding whether that is acceptable for attribution, or whether those events
should carry no `event_time` and let Meta stamp receipt.

### 9. Nothing reconciles Railway against the AWS mirror

`syncToAWS` is fire-and-forget and never awaited. On failure it logs and calls
`recordFailure('AWS sync', …)`, and **nothing retries** — there is no
reconciliation job anywhere in the repo. `if (!awsPool) return` silently
no-ops every sync when `AWS_PG_HOST` is unset.

Direction matters: the code can only produce **mirror ⊂ Railway** (there is no
`DELETE` on either table, and every `syncToAWS` sits after an awaited Railway
write in the same `try`). Duplicates are impossible — `session_id` is UNIQUE
on both.

Values can also diverge without any row going missing:
`disqualified = EXCLUDED.disqualified` has no COALESCE and binds
`data.disqualified ?? false`, so any partial sync object clears a real
disqualification on the mirror. Both webhook safety-net `syncToAWS` calls omit
the key.

The other repo reads the mirror. Nothing in this one reads it back, which is
precisely why divergence would go unnoticed.

### 10. Two alert paths went live on 8 Sept and their background rate is unmeasured

`145f75c` made Meta failures reach `recordFailure`, and `552db39` made every
Salesforce write failure reach `alertOps`. Both were completely silent before,
so **nobody knows the normal failure rate of either**. Volume is bounded —
`alertOps` cools down at 1h for warnings and 3h for criticals — but if there
is a steady trickle, the first people to find out will be whoever is on call.

The rate is knowable without waiting: both paths have always logged their
failures to Railway, they simply never alerted. Grepping a week of
`[SF] Lead … failed` and `[Meta CAPI] … error` gives the answer before an
alert does.

### 11. Route-level coverage exists now, but only for four routes — PARTLY CLOSED 10 Sept 2026

**Was:** every suite read source text or drove a unit; none booted a route.
That is the seam that produced the two worst bugs of 8 Sept — the `//` in
SQL, which no source-level assertion can see, and the `byProduct`
misbinding, where the query and the renderer were each verified in
isolation and the binding between them was in neither.

**Now:** four suites boot the real `index.js` in-process with `pg` and
`global.fetch` stubbed, and drive routes over real HTTP with no database
and no network — `test-submit-gate.js` (`/submit`),
`test-session-page-views.js` (`/session`), `test-lead-field-changes.js`
(`/partial`, `/submit`, `/monitor/lead-changes`, and the monitor page's
inline JavaScript), and `test-session-payload.js`, which executes the real
form-file functions rather than diffing their text.

They are in the bar, they need no `DATABASE_URL`, and they caught things a
source assertion structurally cannot: that all five `/submit`
announcements were unreachable, and that the dashboard's inline JS parses
at all — `node --check index.js` says nothing about JavaScript inside an
HTML string, which is how `/monitor/funnel` stayed broken for weeks.

**Still open:** the other routes. `/booking-confirmed` and both booking
webhooks have no route-level test, and booking arrives by three routes, so
a fix on one is a fix on one third. `tests/test-batch1-e2e.js` still needs
`DATABASE_URL` and is still not in the bar; whether to bring it in is
still a real decision about the bar, not a tidy-up.

### 12. Two live pages loaded the form script by mistake — REMOVED AND VERIFIED 9 Sept 2026

Found 10 Sept 2026 by sweeping every form page after a Webflow republish,
not by anything in this repo. Both were page-level script tags, which a
Project-Settings republish does not touch. **Both tags were removed rather
than repinned** — neither page is supposed to carry the form.

**Verified after the republish:** a sweep of all fourteen URLs shows the
twelve real form pages on `5d3bbb14…` and `/meeting-booked` and `/careers`
serving no `gushwork-form` reference at all. The full path list in
`form_sessions` confirms the sweep is complete — the only other entry is
`/demo#`, which is `/demo` with a fragment (1 session, 21 Aug).

The database side is corroborating but was still thin at the time of
writing: zero sessions from either page in the 30 minutes after the
republish, against an expected rate of roughly 1.4/hour for `/careers` and
0.5/hour for `/meeting-booked`. At those rates a quiet hour proves little,
and Webflow's own CDN may serve a cached page briefly, so a trickle
immediately afterwards would not have meant the removal failed. **The
served HTML is the proof; the row counts confirm it over a day**, where ~34
and ~12 sessions respectively would otherwise have been expected.

An earlier draft of this entry called `/meeting-booked` "a working lead
form six months out of date" and suggested repinning it. That was wrong on
the evidence, and repinning would have been the harmful fix. What the data
actually shows:

**`/meeting-booked`** — pinned to `6f10ad92` (`v3.4`, 25 March 2026). It
does carry leftover `step-1` / `step-2` / `step-3` markup and an email
field, which is what made it look live. In **224 non-bot sessions it has
produced exactly one `leads` row, on 27 March 2026**, and that row is not a
capture: this is the RevenueHero post-booking confirmation page, the URL
carries `?theme=light&name=…&email=…`, the row has
`prefill_source = url_param`, and the person **already had the booking that
sent them there**. So the script read the confirmation redirect's own query
string back into a second lead row for someone who had just booked.
Nothing in the six months since. Repinning it to `v5.9.0` would have turned
a dormant duplicate-row generator into a working one.

**`/careers`** — no SHA at all:
`cdn.jsdelivr.net/gh/DarshilDixit/gushwork-api/gushwork-form.js`. jsDelivr
serves the default branch best-effort, so it was still on `v5.8.0` hours
after `v5.9.0` was pinned everywhere else — the mutable-ref problem the
pinning rule exists to remove, in its loosest form. It has **no** form
elements and has **never** produced a lead. It only ever fired `/session`:
**629 non-bot sessions**. (Five leads carry `/careers` as their
`landing_page`, but all five submitted on `/demo` — that is ordinary
attribution and is unaffected by removing the tag.)

**Historical `form_sessions` rows: NOTE, do not delete.** See the
Definitions note in `CLAUDE.md`. 853 non-bot rows across the two pages,
5.7% of the Sessions card. The rows are *true* — somebody did load a page
carrying the script — and deleting them would move every historical
session number at once to correct a 0.30-point error, which is the same
trade the internal-address distortion is already deliberately left alone
for. Once both tags are removed the population is closed, bounded and
dated, which is much easier to explain than a silent retroactive edit.

The one number to watch while it closes is the `partial` health row: it
turns red on `sessions >= HEALTH_MIN_SAMPLE` (8) with zero leads in a
2-hour window, and these pages contributed roughly four such sessions per
window — sessions that could never produce a lead. That biases toward a
**false red**, which is the safe direction for a health check, and it stops
entirely once the tags are gone.

### 13. `/partial` stores an Apollo-derived website that `/submit` then overwrites

**Opened 10 Sept 2026**, out of the false-positive follow-up alert for
`www.datapartnerinc.com` → `https://www.datapartnerinc.com/`. The alert
itself is fixed; the thing that produced the two spellings is not, and that
half was deliberately deferred.

**What happens.** Two different pieces of our own code write two different
representations of one website, one round trip apart, and the visitor may
have typed neither:

- **Step 1, `/partial`.** `applyEnrichment` (`gushwork-form.js:1999`) takes
  Apollo's `website_url`, strips the scheme and any trailing slash, and puts
  it in `formState`. `savePartial(1)` posts the whole `formState`, so that
  cleaned guess reaches `/partial` and lands in `leads.website` — even
  though website is a step-2 field the visitor has not seen yet.
- **Step 2, `/submit`.** `handleStep2Next` (`gushwork-form.js:2188`)
  replaces it with the website check's `canonical_url`, which is
  `response.url` (`index.js:5453`) and therefore absolute, with a trailing
  slash on the root.

**It is not only the change log.** `leads.website` genuinely flips value, so
the new spelling also goes to the AWS mirror through `syncToAWS`, to
Salesforce through `pushToSalesforce`, and onto the SDR's screen — and where
Apollo's guess and the typed answer are different *domains*, not just
different spellings, the row shows Apollo's guess to anyone who looks
between step 1 and step 2. The comparator fix makes the log honest about
this; it does not stop it happening.

**Two candidate approaches, neither taken:**

1. **Stop `/partial` sending the enrichment-derived website at all.** The
   most honest — a field the visitor has not seen has no answer yet, and
   `leads.website` would only ever hold something they saw. Costs the
   website of every lead who drops between step 1 and step 2, which is a
   real loss for the SDR list. **Needs a Webflow re-pin** (both form files).
2. **Normalise on write so both steps store the same shape.** Cheapest, and
   server-side only, so **no re-pin** — `/partial` and `/submit` would both
   run the value through one normaliser before the upsert. Does not fix the
   Apollo-guess-versus-typed-answer case at all, only the spelling.

**Both move stored data.** Historical `leads.website` values are a mix of
the two shapes today, so either approach makes new rows disagree with old
ones — the same trade as the internal addresses and the 853 stray session
rows above. Nothing should be backfilled without deciding that separately.

**Two things that are NOT separable from what we store**, recorded so
nobody re-derives them:

- **A website change where one host contains the other** —
  `acme.com` → `shop.acme.com`. `domainsMatch` (`gushwork-form.js:1008`)
  accepts exactly that shape before storing a `canonical_url`, so our own
  redirect-following and a person typing a different subdomain are
  identical in the column. Logged as `maybe_our_canonical` and still
  alerted, marked. **To separate them** the form would have to send the
  typed value alongside the resolved one (`website_typed`) or a
  `website_canonicalised` boolean — a form change, so a Webflow re-pin.
- **Whether a lower step arriving after a higher one was browser-back.**
  `back_navigation` records that it happened; `_isPopstateNav`
  (`gushwork-form.js:287`) is client-side only and never sent, so a reload
  or a second tab looks the same. **To separate them** the form would have
  to send that flag on `/partial` — again a re-pin.

### 14. The "actually B2B" clarification re-wraps its own output

**Found 10 Sept 2026** while checking what was producing 18 of the 27
rows in `lead_field_changes`. Not fixed — it lives in the two form files,
so it needs a Webflow re-pin, and it is cosmetic rather than costly.

`handleDisqualifiedNext` (`gushwork-form.js:2130`,
`gushwork-form-popup.js:2538`) composes the label from whatever `sell_to`
currently holds:

```js
formState.sell_to = 'B2B (clarified from ' + formState.sell_to + ')';
```

Reaching that branch twice without an intervening `handleStep1Next` — a
browser-back to the disqualified step and a second click on "actually
B2B" — wraps the label again. **One real row today:**

```
B2B (clarified from Mixed) -> B2B (clarified from B2B (clarified from Mixed))
```

`_submitting` stops a double-click but not a popstate return, because the
`finally` has already cleared it and `initBrowserBack` re-shows the step.

**Cost is low and bounded.** `sell_to` is capped at 50 characters
server-side, so a third wrap truncates rather than growing without limit;
nothing keys or filters on the string; and the SDR still reads "B2B". The
attribution rule matches by composition, so the nested form is already
labelled `ours_sell_to_clarified` and does not alert.

**If it is ever fixed**, the fix is to compose from the ORIGINAL radio
value rather than from `formState.sell_to` — keep the first pick in its
own field and build the label from that. Both files, so both pins.

**The literal is now load-bearing in three places** —
`gushwork-form.js`, `gushwork-form-popup.js` and
`SELL_TO_CLARIFIED_PREFIX` in `index.js`. Change the wording in any one
and every clarification starts alerting as a prospect edit again. A test
asserts all three match; add it to the sync list in `CLAUDE.md` if that
list is ever restructured.

---

### 15. `How_Did_You_Hear__c` has no writer outside our own form — 1,078 Opportunities show it

**This item and the two below cross into Salesforce, which this file was
previously scoped away from.** They are recorded here because nothing else in
either repo records them, and because half of each one is our code.

`Lead.Source_Bucket__c` is a Salesforce **formula** with exactly two inputs:
`utm_source__c` and `How_Did_You_Hear__c`. Since 17 Sept 2026 this repo writes
both, via `MIRROR_FIELDS` in `salesforce.js` — see the Decided section below.
That closes the hole **for leads that come through our form and nothing else**.

Every other lead in the org — outbound, imports, manual entry — still has no
writer for `How_Did_You_Hear__c`. Measured 17 Sept 2026:

| | |
|---|---|
| Our Website leads with an answer | **2,497 of 3,559 (70%)**, up from 37% |
| Opportunities org-wide with a **blank** `Source_Bucket__c` | **1,078** |
| Opportunities traceable to a Website lead with a blank bucket | **0** |

So the 1,078 are entirely the non-form population, and they are roughly **17x
the size of everything fixed in this session**.

**The decision this needs is not ours to take.** Two shapes, and they lead
different places:

1. **Restart or replace the dead upstream writer.** Whatever populated
   `How_Did_You_Hear__c` before July 2026 — almost certainly the Clientell
   managed package — stopped. If it comes back with us also writing, there are
   **two writers on one field** and last-write-wins decides attribution.
2. **Accept that the formula is an inbound-only measure** and let outbound keep
   using `Source_Bucket_New__c`, which it already does (see Decided).

Option 2 is probably right and costs nothing, but it means "blank" stops being
a defect and starts being a category, which the dashboards have to say out loud.

### 16. Three Accounts carry duplicate Opportunities, and one bucket stays blank

Found while backfilling. `robert.parish@compass.com`'s Account has **three**
Opportunities, all named some casing of "COMPASS- Inbound", all at Demo
Completed — created 15 April and two more on 20 April:

```
(BLANK)   Demo Completed   2026-04-15   COMPASS- Inbound
Meta      Demo Completed   2026-04-20   Compass- Inbound
Meta      Demo Completed   2026-04-20   COMPASS- Inbound
```

Two more Accounts are in the same shape: `bob@renewalbuilders.com` (2
Opportunities) and `anusha.nambiar@karix.com` (3).

The backfill **deliberately refused to write** to any of them — with more than
one Opportunity on an Account there is no way to tell which one the lead became,
and guessing would file revenue against the wrong record. So one blank remains,
by choice, and it is the only Website-lead Opportunity still blank.

The real defect is the duplicates, not the blank field. Fixing the blank without
deduping just makes three wrong records look tidy. Needs whoever owns those
Accounts. (Aside: `compass.com` is a national brokerage and is on
`NON_ICP_DOMAINS` today — that block did not exist in April.)

### 17. A deactivated Salesforce user owns live records

Jess Pinote is deactivated and still owns Mark Dorf's Opportunity and Contact.
Ownership does not transfer on deactivation, so those records have no live owner
and drop out of any owner-scoped view or assignment rule. Untouched; needs a
human to reassign.

## Decided

Recorded so they are not reopened. Each has a reason, not just an outcome.

### AEO is the DEFAULT product; only exceptions are listed

`PRODUCT_PATHS = { '/ai-demo': 'crm' }`, everything else `aeo`. The opposite of
how the rest of the repo works, and deliberate: an AEO allowlist rots. The
form is live on a dozen pages and new SEO landers get added by people who will
never open `meta-capi.js`. Measured against 90 days of real leads, a
`/demo`-only allowlist tagged 82% and left 631 leads (408 completed) sending
unlabelled events; the default tags 99.7%. A default fails only when a new
product launches — rare, deliberate, and logged once per unmapped path.

### An unreadable `page_url` returns null, not the default

"We could not tell which page this was" is not "this was the default page" —
the rule the lead-path checkers already follow. The leading-slash guard is
load-bearing: `new URL(x, base)` succeeds for almost any string, so without it
`'not a url'` becomes `/not%20a%20url` and reaches Meta as a real AEO lead.

### `Contact` is excluded from product tagging by EVENT NAME

`PRODUCT_EXCLUDED_EVENTS`. With a default in place the lead-magnet landing
page resolves to `aeo` like any other unmapped page, so this list is the only
thing keeping a PDF download off a 12,000 `predicted_ltv`.

### `predicted_ltv` is per product; `value` is 0 on all three upstream events

12,000 aeo / 5,000 crm. **Provisional.** Changing either changes how Meta
weights these conversions — a business decision, not a tidy-up.

### The Salesforce field placement stands

`Product__c` and `About_Business__c` are on the single Lead layout
(`00hdN000008OLa0QAG`) in section **Lead Information**, not beside
`sell_to__c` / `hear_about_us__c` in **About**.

Moving them programmatically was attempted and abandoned: the Tooling API
returns `platformActionList.actionListContext: "Record"` and then refuses that
same value back, and null-stripping does not help. The only route through is
omitting `platformActionList` from the write, with no way to know in advance
whether Salesforce preserves or wipes the 16 platform actions — and no working
restore path if it wipes them. Not worth that risk to move two fields between
sections on a layout the whole team uses. If it is ever wanted, it is a drag
in Setup.

### `META_TEST_EVENT_CODE` must never be left set in production

Off unless set, and the request body is byte-identical when unset. An event
carrying the code is routed to the Test Events tab and is **not** used for
optimisation or attribution — so leaving the variable set on Railway would
send every real conversion to the test tab instead of to the ad algorithm.
Set it for a verification run, unset it afterwards.

### The duplicate-booking guard stays as it is

It looks up the newest lead row per email and asks whether that row has a
booking, so a second form submission lets the same person take two slots.
Known, and deferred by the owner. Pre-dates 8 Sept.

### Internal and test addresses stay in every `leads` number

`ELV_EXCLUDED_DOMAINS` and `b@g.ai` are excluded from ELV health and alerting
and from nothing else. A known distortion, left in because excluding them
moves every historical number at once.

---

### The channel formula lives in SALESFORCE, and this repo feeds it

Settled 17 Sept 2026, after the Source Bucket investigation that began with a
Slack thread about leads bucketing as `Others`.

`Lead.Source_Bucket__c` is a **formula field** — no stored value, recomputed on
every read — held as Salesforce metadata on `CustomField` `Source_Bucket` /
`TableEnumOrId='Lead'`. The chain, one value under four names:

```
Webflow "How did you hear about us?"
  -> leads.hear_about_us              our Postgres column
  -> How_Did_You_Hear__c              Salesforce, written by MIRROR_FIELDS in salesforce.js
  -> Source_Bucket__c                 the FORMULA (+ utm_source__c), recomputes on read
  -> Opportunity.Source_Bucket__c     copied at lead conversion, then FROZEN as text
```

**No copy of the formula is committed to this repo, deliberately.** A committed
copy cannot be enforced and goes stale the moment someone edits it in Setup,
which is the failure mode half this file is about. CLAUDE.md records how to read
the live one instead.

**The last hop is why converted leads behaved differently.** The Opportunity
value is plain text taken at conversion time and does not follow the formula
afterwards — so fixing the formula fixed every Lead retroactively and every
Opportunity had to be backfilled by hand.

What was done, 17 Sept 2026:

| | |
|---|---|
| `How_Did_You_Hear__c` coverage | 37% -> **70%** (2,497 of 3,559) |
| Leads backfilled | **1,179 of 1,180** (one `CANNOT_UPDATE_CONVERTED_LEAD`) |
| Opportunities corrected | 278 + 63 blanks + 12 account-route = **353** |
| Closed-won Opportunities touched | **0** |

Stage 1, the `How_Did_You_Hear__c` write and backfill (Website leads):
Others 767->498, Meta 1341->2515, Google 120->238, LinkedIn 0->111,
Referral 0->91, Email 0->38, Organic Search 0->24.

### The formula's substring matching was a real bug, and it was fixed

The formula matched two- and four-letter substrings with **no word boundary**,
so free text routed by accident: `CONTAINS(..., "li")` sent "client", "while",
"link" and the name "Jolian" to **LinkedIn**; `CONTAINS(..., "ig")` and
`CONTAINS(..., "book")` sent "Right here", "SIG Investor" and "TEST BOOKING" to
**Meta**, from a branch eight above Invalid/Test.

It was mostly harmless while the field was **empty**. Filling it to 70% is
exactly what made it fire, which is the generalisable part: *raising coverage on
an input amplifies every precision bug in whatever consumes it.*

Fixed and deployed 16-17 Sept 2026. Short tokens are space-padded; `_` and `-`
are normalised to spaces with `SUBSTITUTE` **first**, so `meta_ads` and
`diag-test` still match; Invalid/Test moved to the top of the ladder. Two
additions bundled after: `"coworker"` joins the Referral synonyms, and
`utm_source` containing `chatgpt` maps to AI / LLM **from the last branch before
Others**, so a referrer never overrides what the person actually said.

Net effect on all 4,708 leads carrying either input: LinkedIn 124->113,
Meta 2600->2594, Referral 95->99, AI / LLM 10->13, Invalid / Test 10->11,
Others 806->815. Every single move was a correction.

### How a formula change gets verified here: REPLAY it, do not read it

The method that worked, and the only reason three regressions did not ship:

1. Implement the **old** logic in JS and run it over every real record.
2. Require it to disagree with live Salesforce **zero** times. Until that holds,
   the harness is wrong and anything it predicts is worthless.
3. Only then trust what the **new** logic predicts, and diff the two.

Validated at 4,708 leads, 0 drift, before and after each deploy. It caught three
regressions a careful reading of the formula did not: naive space-padding broke
`meta_ads`, `Testing` and `diag-test`.

A formula recomputes on read, so a bad deploy silently rewrites all of history at
once — and so does a good one, which is why **no backfill is needed after fixing
a formula**, only after fixing the Opportunity text copies.

### `Source_Bucket_New__c` is NOT a newer version of `Source_Bucket__c` — and it stays

The name invites that reading and it is wrong. It is a writable restricted
picklist on **Opportunity** — `Outbound | Cold Email | Meta | Philly | Others` —
answering *which sales motion won the deal*, where the formula answers *which
inbound channel the person arrived from*. That is why it has no Google bucket.

It is **live**, not dead weight: 210 Opportunities in the 60 days to 17 Sept
2026, most recent the day before — Others 123, Outbound 75, Cold Email 8,
Meta 4, touched by Neil Clientell (117), Growth Gushwork (42), Sriram and several
AEs. An earlier read of this file's author said it was abandoned; that was
measured on **Lead**, where it genuinely is dead, and was wrong about Opportunity.
Do not consolidate or delete it.

### `ConvertedOpportunityId` is the WRONG join for "did this lead become a deal"

Whoever converts a Lead can tick "do not create a new opportunity", normally
because the Account already has one. **37** of our converted Website leads are in
that state and **36 of them do have an Opportunity** — reachable only through
`ConvertedAccountId`.

A backfill keyed on `ConvertedOpportunityId` silently skips every one, which is
how 12 blank Opportunity buckets were missed on the first pass. The 37 break down
as: 21 already correct, 2 hand-set and left alone, 12 filled via the Account
route, 3 on multi-Opportunity Accounts (item 16), 1 with nothing attached — and
that last one is our own `darshil.dixit@gushwork.com` test. **No real lead is
stranded**, and an earlier claim in this session that these were "permanently
wrong and invisible to revenue reporting" was wrong on both counts.

### `utm_source` has 17 distinct values, and the two-letter ones are standalone

Checked before word-bounding them, because padding `"fb"` would have broken live
Meta attribution had the real values looked like `fbads`. They do not:
facebook 1855, google 163, **ig 134**, **fb 51**, linkedin 47, cold_email 35,
email 23, chatgpt.com 6, cold_email/ 5, meta 2, and seven one-off hostnames.

## Lesson

**A source-level assertion cannot tell you whether the code runs.**

Every serious bug on 8 Sept had a passing test sitting next to it:

- `// COALESCE for the same reason as /partial.` shipped inside the `/submit`
  INSERT and broke **every form completion** for 31 minutes. Six green suites,
  a review card and a merge all passed over it, because every SQL assertion in
  this repo reads the query as text. Postgres has no `//`.
- `/monitor/funnel` had been returning 500 since the Eastern Time migration
  for the same reason, and was found by a lint written for the first bug —
  not by anyone opening the tab.
- `byProduct` was bound to the wrong query. The query was verified by
  execution. The renderer was verified by execution. **The binding between
  them was in neither**, and it took the dashboard down.
- `safeCd` wrapped a function that already caught, so thirteen assertions
  claiming to check "builds rather than throwing" could not fail.
- Three separate assertions pinned column or bind **order** while claiming to
  check **presence**, and each failed a correct change.
- `recordSuccess('Meta CAPI')` was never called; the Meta `.catch` could never
  fire; every Salesforce write failure resolved quietly. All three looked like
  working alerting at the call site.

The pattern is one thing: **verification that reads the code proves the code
was written, not that it works.** The remedies are cheap and all of them were
used on 8 Sept once someone insisted:

- **Execute SQL.** `CREATE TEMP TABLE` from the real migrations, run the real
  statement, `ROLLBACK`. For a read-only query `EXPLAIN` is enough — `42601`
  is a syntax error and `42P01` is a missing table, so the two are
  distinguishable with no schema at all.
- **Drive the function.** Stub `fetch`, call the real thing, assert on what it
  built or on whether the handler ran.
- **Check the seam.** If a test covers A and another covers B, ask what binds
  them — and note that positional binding cannot be checked at all, which is
  why `/monitor/metrics` is now an object.
- **Make tests fail rather than crash.** A throw that escapes prints no
  totals, and `measure.js` correctly reports UNMEASURED — which is neither a
  pass nor a catch. Four separate sections had to be made abort-proof before
  their mutations could even be measured.
- **Mutate the guard and watch it fail.** Several assertions written on 8 Sept
  survived their first mutation, including two that looked thorough.

### The sessionStorage path array is PARKED, and what would justify it

**Proposed and not built, 10 Sept 2026.** The site-wide Webflow script would
append each page load to a `sessionStorage` array, which the form script then
posts as one field — capturing the whole journey including non-form pages,
which `form_page_views` structurally cannot.

Parked for four reasons, strongest first:

- **Server-observed beats client-asserted.** A `form_page_views` row means a
  request reached this server at this timestamp. An array entry means the
  browser said, in bulk and after the fact, where it had been. Unverifiable.
- **It needs the riskiest deploy step twice** — the Webflow global custom
  code *and* both script pins — for a gain nobody can currently size.
- **`form_page_views` is what would size it.** Its per-hit URLs and
  timestamps let you measure how often a session's form-page hits are
  non-contiguous, which implies non-form browsing in between. That is
  unmeasurable today because nothing records it.
- **Frequency.** 93.6% of sessions were single-hit when this was written,
  and that is a floor, not a total: a bfcache restore does not re-fire
  `DOMContentLoaded`, so back-navigation is invisible to the counter.

**If it ever lands, its rows must stay distinguishable from server-observed
hits rather than being blended into the same column.** `form_page_views.source`
is `NOT NULL` for exactly this reason and today only ever holds
`'session_route'`. Merging a client-reported hop into that value would destroy
the one property that makes the table trustworthy.

**What would flip the decision:** `form_page_views` showing routine gaps or
page jumps implying non-form browsing; someone needing content-page
attribution (blog → demo) for a real decision; or a Webflow deploy already
scheduled for another reason, which collapses the marginal cost to one
global-script edit.

### Tracking logic is split across two scripts and one of them is not in this repo

**Recorded for the site-wide analytics ticket, 10 Sept 2026,** so it is not
rediscovered the hard way. This cost real time twice in one investigation,
including one wrong conclusion stated confidently from a `grep` of a single
file.

- **The site-wide script is Webflow global custom code.** It runs on **every**
  page and writes **only to `sessionStorage`** — `gw_referrer`,
  `gw_landing_page`, `gw_utm_*`, the PartnerStack cookies. It never touches
  Postgres. It is **not in this repository** and a repo grep cannot see it.
- **`gushwork-form.js` is on the 16 form-bearing pages only**, and is the only
  thing that writes to Postgres — `/session`, `/partial`, `/submit`.
- **So attribution IS captured on non-form pages**, client-side, and posted
  when the visitor reaches a form. The **entry point is not lost; the middle
  hops are.** Measured: the homepage, `/pricing` and `/who-its-for` have zero
  `form_sessions` rows while 990, 263 and 76 leads respectively arrived at the
  form *from* them.

**`captureUTMs()` applies three different persistence policies in one
function**, and nothing on screen or in the schema says so:

| Key | Policy |
|---|---|
| `gw_referrer`, `gw_landing_page` | **first-touch** — written once, never updated |
| `gw_utm_*` | **overwrite unconditionally** — a fresh ad click replaces them |
| `gw_previous_page` | **last-touch** — rewritten on every internal hop (`gushwork-form.js:328`) |

A session can therefore hold hit 1's landing page next to hit 3's UTMs, and
both look equally authoritative. That is a reporting hazard, not a bug to
"fix" in passing — changing any one of the three moves historical numbers.

**No analytics infrastructure beyond `form_page_views` belongs in this repo.**
That is the backlog ticket's job.
