# Open items — gushwork-api

Written 9 Sept 2026, derived from `git log fe07b0a..ae15777f5434a458e99eef6670b6a3c6fcabfe43` and from reading
the code, not from anyone's recollection. Line numbers are as of
`ae15777` and will drift; the symbol names will not.

**The other half of this lives in the meta-capi repo, `docs/OPEN-ITEMS.md` at
`1ef3c58`.** Neither file is complete on its own: the two repos share the
`gw_form_leads` mirror and the Meta pixel, and at least one item here
(`submitted_at` as an event-time fallback) is only visible if you read both.

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

### 11. No test exercises a route end to end

Every one of the six suites reads source text or drives a unit. None boots a
route. That is the seam that produced the two worst bugs of 8 Sept — the
`//` in SQL, which no source-level assertion can see, and the `byProduct`
misbinding, where the query and the renderer were each verified in isolation
and the binding between them was in neither.

`tests/test-batch1-e2e.js` does boot the real server but needs
`DATABASE_URL` and is not in the six-suite bar. Whether to bring it in is a
real decision about the bar, not a tidy-up.

---

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
