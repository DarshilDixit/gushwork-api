# PR review card — Non-ICP block V1 (real estate + insurance brand domains)

**Branch:** `feat/non-icp-v1-block` · **11 Sept 2026** · **Not merged.**
**Ships behind `NON_ICP_BLOCK`, default OFF.** Merging changes nothing until
that variable is set to the string `true` on Railway.

Written for someone who was not in the session that produced it. Everything
below was either run or is flagged as not run.

---

## 0. Read this first

This PR **changes what blocks and what fires Meta**. Both were standing rules in
`CLAUDE.md` and both are now edited in the same commit.

- **It is the first server-side block in the repo.** The three website verdicts
  (`nxdomain`, `brand_mismatch`, `mailbox_domain`) are unchanged and still
  client-side.
- **It suppresses all three upstream Meta events** for a blocked lead.
- **It reverses two written, dated positions** in Swapnil's Non-ICP doc.
  Authorised by Swapnil on 11 Sept. The record is
  `docs/tickets/non-icp-v1-block.md`; read that before arguing with the code.

If you disagree with any of the three, the argument is with the decision, not
with this implementation.

---

## 1. What it does

A lead whose **email domain** or **website host** matches a national real-estate
brokerage or insurance carrier brand is:

| | Blocked lead | Normal lead |
|---|---|---|
| Step 1 | detected, row stamped, **no redirect** | — |
| Step 2 | fills the whole form | — |
| Calendar | **redirected to `/thank-you` at submit, before RevenueHero** | RevenueHero as usual |
| A booking that slips through | **refused outright**, critical alert raised | recorded |
| Meta `StartTrial` | suppressed | fires |
| Meta `Lead` | suppressed | fires |
| Meta `Schedule` | suppressed (all 3 routes) | fires |
| Slack | `🚫 Lead Blocked — Non-ICP`, same channel | normal lead post |
| Salesforce | **not pushed** | pushed |
| Recovery email cron | excluded | eligible |
| Row in `leads` | kept, `non_icp_blocked = true` | kept |
| `leads.disqualified` | **untouched** | untouched |

Matching is a **pure string comparison**. No network call, no LLM.

---

## 2. Why — the evidence

Measured against 5,123 production leads, 20 Mar – 11 Sep 2026 (Railway
`leads`), and 13,199 bookings (`gist.gtm_inbound_demo_bookings`).

- **84 leads** matched a national brand domain; 75 distinct people.
- **They book more:** 60 of 84 = **71.4%**, against **64.1%** overall.
- **They attend less:** **47.1%** of decided bookings against **66.4%**
  (16 of 34, against 3,409 of 5,133).

Book more, turn up less — the shape of a wasted AE calendar.

**Cost of the block:** ~14 leads/month, ~10 bookings/month, ~5 attended
demos/month given up.

---

## 3. Every place the block is evaluated

| # | Where | Input | What it does | File |
|---|---|---|---|---|
| 1 | `POST /non-icp-check` | email, website | answers the browser | `index.js` |
| 2 | `handleStep1Next` | email | **warms the verdict only — no redirect** | both form files |
| 3 | `handleStep2Next` | email + website | redirects, **before RevenueHero**, **cache-only** | both form files |
| 4 | `POST /partial` | email, website | **stamps the row**, suppresses `StartTrial` | `index.js` |
| 5 | `POST /submit` | email, website | **stamps the row**, suppresses `Lead`, Slack, no Salesforce | `index.js` |
| 6 | `/booking-confirmed` | row | **refuses the booking** + suppresses `Schedule` | `index.js` |
| 7 | `/booking-confirmed-webhook` | row | **refuses** + suppresses `Schedule` | `index.js` |
| 8 | `/booking-confirmed-webhook-rh` | row | **refuses** + suppresses `Schedule` | `index.js` |
| 9 | Cal webhook **safety net** | email | **refuses**, creates no lead row | `index.js` |
| 10 | RH webhook **safety net** | email | **refuses**, creates no lead row | `index.js` |
| 11 | `/cron/send-partials` | row | excludes from recovery email | `index.js` |

**2 and 3 are UX only.** Anyone can skip them with devtools. **4 and 5 are the
enforcement** — they stamp the column and suppress Meta regardless of what the
browser did. **6–10 are the last line**: they refuse the booking itself, so even
a lead who bypasses the browser entirely does not end up on an AE's calendar.

### Why step 2 reads cache only

The step-2 click makes **no network call and has no timeout it can fall
through**. A lead who slips past because our own request was slow is a realtor
on an AE's diary. Both halves are warmed on blur — the email at step 1, an
entire step earlier — and the click reads `nonIcpCached()`, which never fetches.
A null verdict means "not warmed yet" and does **not** block; that lead is
caught by the server backstop on the `/submit` response, and if they still reach
a slot, the booking routes refuse it.

### The safety-net paths were a real hole

Both webhooks create a lead from scratch when no session matches — the one route
that bypasses the form entirely, and **ten such rows exist in production** (all
`rh_webhook`). There is no row to read a block off, so the verdict is computed
from the email there. Found while writing the route tests, not by review.

### ⚠ There are SIX Meta CAPI call sites in this repo, not three

You asked me to confirm three. **There are more.** Three event *types*, six
*call sites*:

| Event | Call sites |
|---|---|
| `StartTrial` | 1 — `/partial` |
| `Lead` | 1 — `/submit` |
| **`Schedule`** | **3** — `/booking-confirmed`, `/booking-confirmed-webhook`, `/booking-confirmed-webhook-rh` |
| `Contact` | 1 — `lead-magnet.js` (separate funnel, **out of scope, unchanged**) |

All three `Schedule` sites are guarded. Counting event names and concluding
"three sites" is precisely how one leaks; `CLAUDE.md` now says so.

---

## 4. The domain list

**41 entries.** Full table with per-domain lead and booking counts is in
`docs/tickets/non-icp-v1-block.md` §"The domain list, with evidence". Highest
volume:

`allstate.com` 16/10 booked · `compass.com` 12/7 · `exprealty.com` 10/8 ·
`kw.com` 9/7 · `farmersagent.com` 9/6 · `cbrealty.com` 6/5 ·
`statefarm.com` 6/6 · `farmers.com` 6/6 · `newyorklife.com` 5/4 ·
`ft.newyorklife.com` 5/4 · `agents.farmers.com` 4/4

**Three of your original names were near-misses for what leads actually use:**

| You said | Hits | What they actually use | Hits |
|---|---|---|---|
| `nyl.com` | 0 | `ft.newyorklife.com` | 5 |
| `coldwellbanker.com` | 1 | `cbrealty.com` | 6 |
| `remax.com` | 1 (website only) | `remax.net` | 3 |

All six are on the list. Zero-hit entries (`century21.com`,
`sothebysrealty.com`, `goosehead.com`, `nyl.com`) are **kept on purpose** — one
string comparison each, and they are the ones most likely to arrive next.

**Not on the list, deliberately:** financial advisors (Edward Jones, LPL,
Northwestern Mutual, Primerica, Cetera), mortgage and lending (~35 domains in
our data), independent local agencies and brokerages (~70). The Non-ICP doc
keeps all of these **in ICP by name** and nobody reversed that.

---

## 5. Failure modes — what happens when each thing breaks

**The rule: we never block because something broke.**

| Failure | Behaviour | Lead reaches calendar? | Meta? |
|---|---|---|---|
| `NON_ICP_BLOCK` unset | `{blocked:false, reason:'disabled'}` | yes | fires |
| Warehouse unreachable / timeout (2.5s) | `check_failed` — **does not block** | yes | fires |
| Customer cache cold | same as above | yes | fires |
| `nonIcpVerdict` throws | caught, `check_failed` | yes | fires |
| `/non-icp-check` non-200 | client reads `blocked:false` | yes | server still decides |
| `/non-icp-check` times out (6s) | client fails open | yes | **server still suppresses** |
| Verdict not warmed by the step-2 click | `nonIcpCached` returns null — **does not block** | yes, briefly | **server still suppresses, and the booking is refused** |
| Blocked lead reaches a booking route | booking **refused**, critical alert | no | suppressed |
| Booking route cannot read the lead row | **allows the booking** (fails open) | yes | suppressed only if the row says so |
| Blocked lead books with no form row at all | safety net **refuses**, no lead created | no | none fired |
| `awsPool` not configured | `partnerStackCustomerDomains` throws → `check_failed` | yes | fires |
| Lead IS a known customer | bypassed, not blocked | yes | fires |
| Internal/test address | never blocked | yes | as today |

**The asymmetry is deliberate and is the opposite of PartnerStack eligibility,
which fails closed.** Eligibility decides whether an affiliate gets paid and
touches no lead. This stands between a real person and a demo. A broken checker
here should cost us a realtor's call-back, not a customer.

**Consequence worth naming: a warehouse outage disables the block entirely.**
That is the correct direction, but it means the feature is silently off during
one. Nothing alerts on that today — see §10.

---

## 5b. Confirmed decisions (Swapnil, 11 Sept) — do not re-litigate

**Redirect at step 2, not step 1.** Detection and Meta suppression stay at step
1; only the redirect moved. A blocked lead fills the whole form so we capture
website, company and phone. Four of the 84 matched leads are not agents at all —
a claims employee at a carrier, a retired address, a tax preparer on a carrier
address, a dance instructor on a brokerage address — and every one was
identifiable *only* from step-2 fields. Redirecting at step 1 would have hidden
all four.

**A matched EMAIL blocks regardless of the website.** OR logic stays.
Reasoning: open the site and it is company-owned anyway. This is the decision
most likely to produce a wrong block, so it is made reviewable rather than
invisible — the Slack post prints the matched domain and the website **together**
and, when the email matched but the website did not, says so in words:
"⚠️ Their email is a brand domain but their website is not. Blocked on the
email, by design — worth a look if this shape keeps appearing." **If that line
starts appearing often, the decision is worth revisiting.**

**Scope is the business type, not "agents under national brands".** Doc-only,
no code. V1 matches domains so it cannot reach the ~70 independent agencies and
realtors doing the same job. Recorded in the ticket so the LLM rules get scoped
against what the company *is*, and so V1 gets **retired rather than extended** —
growing a domain list toward "every realtor" is the wrong shape and each
addition is another chance at a `paycompass.com`.

---

## 5c. Your question: blur `@kw.com`, then edit to gmail

Two different answers depending on whether they clicked Next in between.
**Both verified** — the client half by reading the cache key, the server half by
executing the real upsert twice against Postgres.

**They only blurred, never clicked Next.** The verdict *is* recomputed: the
cache is keyed on `email|website`, so `a@kw.com|` and `a@gmail.com|` are
different entries and the second blur fetches a fresh verdict. `/partial` is
only ever called by the Next click, so the server never saw `@kw.com` and
**the row is not blocked**. Correct — they never submitted that address.

**They clicked Next with `@kw.com` first, then went back and edited.** The row
was stamped on that first `/partial`. The second `/partial` carries gmail and a
`false` verdict, and **the row stays blocked**. Executed against real Postgres:

```
--- 1st /partial: a@kw.com, verdict BLOCKED ---
 a@kw.com    | t | kw.com
--- 2nd /partial: SAME session, email edited to gmail, verdict NOT blocked ---
 a@gmail.com | t | kw.com      <-- email changed, block held
```

That is the sticky `IS TRUE OR EXCLUDED IS TRUE` doing its job, and it is the
same defence that stops the "actually we're B2B" button clearing a block. The
email change is separately recorded in `lead_field_changes`.

---

## 6. The known-customer bypass

**One of the 84 is an active paying customer.**

`nedjacobs@allstate.com` — "JACOBS FAMILY INSURANCE @Allstate", website
`jacobsfamilyinsurance.net`, **status `Active` in
`gist.customer_contract_terms`**. The V1 email rule on `allstate.com` would have
shut the door on a renewal conversation.

So `nonIcpVerdict` checks `gist.customer_contract_terms`,
`gist.gist_accountsmaster` and `gist.customer_enrichment` **before** blocking,
reusing `partnerStackCustomerDomains()` (30-min cache) and
`partnerStackCustomerKey` for normalisation. Wrapped in `withTimeout` at 2.5s —
`awsPool` is `max: 3` with no `statement_timeout`, so an RDS instance that
accepts connections but answers slowly would otherwise hang forever.

**A clean lead never touches the warehouse.** The lookup runs only after a
domain has already matched. Asserted by a test that fails if the bypass runs for
a non-matching lead.

**`startPartnerStackCacheWarm` changed.** It previously returned early unless
`PS_ELIGIBILITY_ENABLED`; it now warms if **either** flag is on. Without that
the cache would be cold with the block enabled and the first blocked lead would
pay for a cross-WAN fetch on step 1.

---

## 7. Three traps, and how each is pinned

**1 — Substring matching.** Measured: exact-or-subdomain caught **110** leads,
substring caught **116**, and five of the six extra were wrong:

| Would have been blocked | Actually | Because it contains |
|---|---|---|
| `paycompass.com` | a payments company | `compass.com` |
| `charleslegalpl.com` | Charles Injury Law | `lpl.com` |
| `theimagecreatornm.com` | an image company | `nm.com` |
| `krevera.com` | Krevera | `era.com` |
| `ceterainvestors.com` | Cetera Investors | `era.com` |

All five are **negative fixtures** in `tests/test-non-icp.js` §3.

**2 — `partnerStackCustomerKey` collapses subdomains.** It ends in
`registrableDomain`, so `agents.farmers.com` → `farmers.com`. Matching only on
its output made every subdomain entry **dead code that looked live**. Caught by
the test, not by review. `nonIcpHostForms` now keeps both forms.

**3 — The block must be sticky.** `/partial` fires repeatedly through step 1 and
the "actually we're B2B" button calls `savePartial(1)` again. **74 of the 84
matched leads (88%) reached the calendar through that button.** All three
upserts use `IS TRUE OR EXCLUDED … IS TRUE`.

For scale: **2,389 of 5,123 leads (46.6%)** take the clarification path and they
book at 74–77% against 70.8% for B2B-direct. It is the main road, not a side
door — which is why the block is independent of `sell_to` and `disqualified`.

---

## 8. What is deliberately NOT covered

- **The other four rule-6 industries** — restaurants and food service, spas and
  salons, home services and trades, print and sign shops. V1 cannot see them.
  **They fire Meta normally and reach the calendar normally.** Your instruction
  said "don't fire Meta for any of the six flagged industries"; V1 delivers that
  for **two of six**. The rest needs the LLM flagger.
- **Rules 1–5** (agencies, nonprofits, publishers, sub-$1,000 ticket, local
  service-area). Not implemented.
- **Salesforce**: blocked leads are simply not pushed. `salesforce.js` is
  unchanged. No field, no sync, no backfill.
- **The residual gap no domain list closes:** `zaljames@comcast.net`, website
  `movewithkw.com`, company "Keller Williams Success Realty" — a real KW agent
  on a personal-brand domain. Substring would have caught it; exact does not.
  That is what the LLM pass is for.

---

## 9. The `sdr-calling` dependency — **alongside or after, not before**

Blocked leads look **identical to normal drop-offs** in `gw_form_leads`: form
completed, no booking. `sdr-calling`'s No Booking workflow selects exactly that
shape, so it will dial people we just turned away.

**This PR adds** `non_icp_blocked` / `non_icp_reason` to `gw_form_leads` and
syncs them. **Necessary, not sufficient.**

**`sdr-calling` must add `AND non_icp_blocked IS NOT TRUE` to:**
`no-booking/sync-form-leads-campaign.js :: fetchFormLeads`,
`lib/population.js :: noBookingPopulation`,
`lib/workflow-stats.js :: formLeadsNoBookingRows`.

**Not a blocker, and here is why:**

- A **step-1 block collects no phone** (phone is a step-2 field), and No Booking
  requires a non-empty phone. Those can never reach the dialer.
- Only **submit-time blocks** are dialable — the "personal email + brokerage
  website" shape: **12 of 84 over six months, ≈2/month**.
- Failure mode is an awkward call, not a lost lead or a wrong charge.

**That repo was not touched.**

---

## 10. Weak points — read these

1. **A warehouse outage silently disables the block.** Correct direction, but
   nothing alerts. There is no health row for this feature. If you want one, it
   belongs in `runHealthChecks` and I did not add it.
2. **No timestamp column.** You specified `non_icp_blocked` and
   `non_icp_reason`; I added exactly those. There is no `non_icp_checked_at`, so
   "when was this decided" is only answerable from `updated_at`.
3. **The show-rate evidence is a small sample.** 34 decided bookings. The
   47.1%-vs-66.4% gap is ~2.4 standard errors. And `show_status = 'N'` mixes
   cancellations with no-shows — split for the matched group it is 11 cancelled
   / 7 genuine, so the *true no-show* gap (20.6% vs 16.9%) is much thinner than
   the headline. The attendance gap is real; do not quote the no-show gap.
4. **A client fail-open still shows the calendar briefly.** If
   `/non-icp-check` times out at step 2, RevenueHero fires, then `/submit`
   returns `non_icp_blocked` and the backstop redirects. They may see a booking
   widget for a moment. Meta is suppressed throughout.
5. **`submitLead()` now returns the response body instead of `res.ok`.** Both
   form files. Every existing caller ignored the return value and the two early
   returns still return `true`, but it is a signature change in a hot path.
6. **41 hardcoded domains will rot.** No process refreshes them. The Blocked tab
   is the only feedback loop and it has to be opened deliberately; the Slack
   post is what actually surfaces a bad block.
7. **`/thank-you` copy is unchanged**, per your instruction. A blocked realtor
   sees the same page as a successful booking. That will read as confusing to
   anyone who looks.
8. **Browser walkthrough complete.** `/demo` block path, `/demo` normal-address
   control (calendar still appears), and the Ads fork blocking with the
   greeting rendered. No longer a weak point. Everything server-side is now driven
   over real HTTP (§11) and both Slack paths were fired for real, but the
   blur→cache→click sequence, the `/thank-you` redirect actually navigating,
   and the modal fork's behaviour have only ever been asserted from source.
   **This is the highest-value thing for you to check by hand after the
   Webflow re-pin** (§12, step 7).
9. **Cache-only at step 2 trades certainty for speed — SHIPPED AS IS, ON
   PURPOSE.** If neither blur had time to resolve, the click does not block and
   the lead sees a calendar until `/submit` answers. The booking routes refuse
   it, so they cannot actually take a slot, but they will see the widget.

   **This is a deliberate decision, not an oversight.** Darshil, 11 Sept, at
   merge time. The reasoning, recorded so nobody "fixes" it blind:

   - The **email check fires a step earlier** and covers most of it. A matched
     email blocks on its own, and it is warmed at step 1 — an entire screen
     before the click that needs it. Only the website-only shape (personal
     email + brokerage site, 12 of 84 historically) depends on a step-2 blur
     resolving in time.
   - The **flag makes it reversible**: `NON_ICP_BLOCK=false` turns the whole
     feature off from the Railway env with no deploy.
   - The **critical alert tells us if it ever actually happens.** A lead who
     slips through to a slot fires *"A blocked lead took a calendar slot"* with
     the booking id. That is the measurement.

   **The review point: if that alert fires more than once or twice in the first
   week, come back with the real numbers.** The fix would be a short awaited
   wait at the click, and choosing its length without data is guessing. Do not
   pick a timeout from first principles — wait for the alert count.
10. **Refusing a booking does not cancel it.** The slot lives in Cal or
   RevenueHero and nothing in this repo can delete it. The refusal raises a
   *critical* naming the booking id and start time precisely because a human
   has to go and cancel it. If nobody watches that alert, the AE still loses
   the slot — the refusal only stops it counting as a lead.
11. **A step-1-only block produces no Slack post.** Someone detected at step 1
   who abandons before submitting appears on the Blocked tab and nowhere else.
   Deliberate — the post carries website, company and phone, none of which
   exist yet — but it means the dashboard is the only record for that
   population.

---

## 11. What was verified, and how

**Run and passing:**

- `node tests/measure.js --check` — **12 suites, 2,648 assertions, 0 failures.**
  Run bare, never piped. Baseline 2,419 → 2,648 across this branch.
- `tests/test-non-icp.js` — **177 assertions**, source-level. Fixtures are real
  production rows, including all five substring false positives.
- `tests/test-non-icp-routes.js` — **50 assertions**, NEW, boots the app and
  drives it over real HTTP.
- Three pre-existing suites were updated because this PR changes contracts they
  encode. **None were loosened** — each asserts the new intended contract.

**SQL was EXECUTED, not asserted.** Temp schema built from the real migrations
on Railway, statements run inside a transaction, `ROLLBACK`. Re-run after every
change on this branch.

- `/partial` upsert, `/submit` upsert, `/monitor/blocked`, `/cron/send-partials`
  — all four execute clean.
- **The blur-then-edit case was executed, not reasoned about** — see §5c for the
  output.
- Afterwards: **0 test rows in production**, columns still absent (created at
  boot by `db.js`).
- Placeholder arithmetic: `/partial` 42=42, `/submit` 41=41, mirror 60=60.

**Mutation testing — 21 mutations across three passes.**

| Mutation | Result |
|---|---|
| Sticky block → `= EXCLUDED` | CAUGHT |
| Substring matching reintroduced | CAUGHT ×2 suites |
| Known-customer bypass removed | CAUGHT |
| Warehouse failure blocks instead of failing open | CAUGHT |
| `StartTrial` suppression removed | CAUGHT ×2 |
| Blocked branch pushes to Salesforce | CAUGHT ×2 |
| Recovery-cron exclusion removed | CAUGHT |
| Env flag defaults ON | CAUGHT |
| `statefarm.com` removed from the list | CAUGHT |
| Step-2 check moved after RevenueHero | CAUGHT |
| Client redirect never fires | CAUGHT |
| Ads fork: step-2 check removed from popup only | CAUGHT |
| **Booking refusal removed** (`/booking-confirmed`) | CAUGHT ×2 |
| **Refusal logs but does not return** (event suppressed, slot kept) | CAUGHT |
| **Safety-net guard removed** (Cal) | CAUGHT ×2 |
| **Step 2 awaits the network again** (timeout window reopens) | CAUGHT |
| **Step-1 redirect reinstated** | CAUGHT |
| One `Schedule` guard neutered → `if (false)` | **SURVIVED → fixed → CAUGHT** |
| **Slack drops the matched domain** | **SURVIVED TWICE → fixed → CAUGHT** |

**Two survivors, and both are worth your attention** — they are the same class
of mistake twice:

1. **The `Schedule` guard.** The assertion counted occurrences of the log
   *message*, which stays in the file when the condition becomes `if (false)`.
   Fixed to require the real condition adjacent to each route's own log line.

2. **The matched domain in Slack.** Harder, and it took two goes. The first
   assertion checked the whole Slack payload — but `sendSlack` also writes a
   plain-text fallback repeating the domain, so it passed with the visible
   message gutted. The second checked `payload.blocks` — but `kw.com` is *also*
   the test lead's website, so it passed again. Only pinning the `*Matched:*`
   line itself caught it. **You said that field is how you catch a bad block;
   it took three attempts to actually test it.**

Both are the reachability blind spot `CLAUDE.md` records for ordering
assertions, arriving in new disguises. Treat "the string is in the file" as
evidence of nothing.

**Previously untested paths — two of three now closed:**

| Path | Status |
|---|---|
| `slackNonIcpBlocked` | ✅ **CLOSED.** Fired for real via `tools/fire-non-icp-slack.js blocked` against production `SLACK_WEBHOOK_URL`. Slack returned **200 ok**. The message used the matched-on-email-but-not-website shape deliberately, so the mismatch warning rendered too. |
| The booking-refusal critical | ✅ **CLOSED.** Fired via `tools/fire-non-icp-slack.js booking`. Slack **200**, and the alert email sent (`messageId cdc301c7-…@gushwork.ai`). New path, so it was fired under the same rule. |
| `/non-icp-check` never served a request | ✅ **CLOSED.** `tests/test-non-icp-routes.js` boots the real app and drives it over actual HTTP — 47 assertions covering `/non-icp-check`, `/partial`, `/submit`, all three booking routes and both safety nets. |
| No form file loaded in a browser | ✅ **CLOSED.** `/demo` block path end to end (11 Sept, v5.10.0); `/demo` normal-address control shows the calendar; the Ads fork blocks and lands on `/thank-you` with the greeting, covering both the fork and `attendeeName` on v5.11.0. |
| The Blocked dashboard tab has not been rendered | ❌ **STILL OPEN.** The route is tested; the rendered page is not. |

**NOT verified:**

- The two form files in a real browser — the redirect, the blur/cache timing,
  the Ads modal fork.
- The Blocked tab rendered in a browser.
- No production lead has ever actually been blocked (the flag is off).

---

## 12. How to verify after deploy

Deploy with `NON_ICP_BLOCK` **unset** first and confirm nothing changed. Then:

```bash
# 1. Columns exist (db.js creates them at boot)
#    Expect 2.
railway variables --service Postgres --kv | grep DATABASE_PUBLIC_URL   # then psql
#    SELECT count(*) FROM information_schema.columns
#     WHERE table_name='leads' AND column_name LIKE 'non_icp%';

# 2. The endpoint answers, flag still OFF -> blocked:false
curl -s -X POST https://gushwork-api-production.up.railway.app/non-icp-check \
  -H 'Content-Type: application/json' \
  -d '{"email":"someone@kw.com","website":"kw.com"}'
# expect {"blocked":false,...}

# 3. Turn it on in the Railway env (NON_ICP_BLOCK=true), wait for redeploy, repeat
# expect {"blocked":true,"matched_domain":"kw.com","label":"Keller Williams"}

# 4. The five that must NEVER block
for d in paycompass.com charleslegalpl.com theimagecreatornm.com krevera.com ceterainvestors.com; do
  echo -n "$d -> "
  curl -s -X POST https://gushwork-api-production.up.railway.app/non-icp-check \
    -H 'Content-Type: application/json' -d "{\"email\":\"a@$d\",\"website\":\"$d\"}"
  echo
done
# every one must be blocked:false

# 5. The known customer must NOT block
curl -s -X POST https://gushwork-api-production.up.railway.app/non-icp-check \
  -H 'Content-Type: application/json' \
  -d '{"email":"nedjacobs@allstate.com","website":"jacobsfamilyinsurance.net"}'
# expect blocked:false  (if this says true, the warehouse bypass is not working)

# 6. Dashboard tab
open "https://gushwork-api-production.up.railway.app/monitor?token=$MONITOR_TOKEN"   # Blocked tab
```

**7. The browser check — the one thing nobody has done.** After the Webflow
re-pin (§15), on a real page:

1. `/demo` → type `agent@kw.com`, tab out of the field, pick B2B, click Next.
   **You should reach step 2 normally.** If you are redirected here, step 1 is
   still redirecting and the fix did not ship.
2. Fill step 2 with any website and click Next. **You should land on
   `/thank-you` and never see a calendar.**
3. Check the console for `[GW] Non-ICP — redirecting to /thank-you (matched kw.com)`.
4. Check Slack for the `🚫 Lead Blocked — Non-ICP` post, and that it carries
   website, company and phone.
5. Repeat the whole thing on the Google Ads page — it is a **fork**, and it has
   silently missed releases before.
6. Then do it once with an ordinary address and confirm the calendar still
   appears.

---

## 13. Rollback

**Fastest — no deploy:** set `NON_ICP_BLOCK=false` (or delete it) in the Railway
env. The verdict short-circuits to `{blocked:false, reason:'disabled'}`, nothing
blocks, all Meta events resume. Already-stamped rows keep their flag and stay on
the Blocked tab; they are not re-processed.

**One bad domain:** delete its entry from `NON_ICP_DOMAINS` in `index.js` and
deploy. The Slack post names the matched domain precisely so this is a one-line
fix.

**Full revert:** `git revert` the two commits. The columns and indexes are
`IF NOT EXISTS` additions and can be left in place — nothing reads them once the
code is gone. The mirror columns should be left alone regardless, since
`sdr-calling` may by then depend on them.

**The form half rolls back separately.** Re-pin Webflow to the previous SHA
(`99597ed`) and republish. Reverting this repo does **not** revert the browser.

---

## 13b. FOLLOW-UP PR — PartnerStack fired for a blocked lead

**Branch `fix/non-icp-partnerstack-guard`. Found in production within an hour
of the flag going on.**

```
[/submit] 🚫 Lead blocked: agent@allstate.com
[PartnerStack] ✅ Conversion sent: allstate.com | xid=M7wnDScN0rrUYH
```

`StartTrial`, Meta `Lead` and the Salesforce push were all correctly
suppressed. `runPartnerStackSignup` guards on `leads.disqualified`, and the
block deliberately lives in `non_icp_blocked`.

**This card is where the mistake is visible.** §3 listed
`runPartnerStackSignup` as one of the five `disqualified` consumers, and §0 used
that list to justify *not* touching `disqualified` — rather than to audit what
each consumer would now miss. Listing a consumer is not checking it.

**Three were wrong, not one:**

| Consumer | Did | Cost |
|---|---|---|
| `runPartnerStackSignup` | sent a paid conversion | money |
| `/monitor/sdr` | listed blocked leads for SDRs | wasted calls |
| `checkRecoveryHealth` | counted them stuck forever | a red row nothing clears |

Two checked and deliberately left: the stage ladder and metrics counters still
count blocked leads as leads (they are); `slackPartial` is cron-only and the
cron already excludes them.

`tests/test-non-icp.js` §10b is now an **audit** — it derives every
`disqualified` predicate in `index.js` and pins the count at 13 (comments
stripped), so a new guard cannot be added without someone deciding what it does
about a blocked lead.

**Exposure: $50, conditional, not yet realised.** No money has moved — a
conversion is a customer record; the payout needs an AE to tick
`Qualified_Demo__c`. But **three existing Opportunities already resolve to
`allstate.com`** (`tungle@`, `b.sheffield@`, `dawnbeaulieu1@`, all currently
false). The step-10 poller keys on domain, so ticking any of them fires the
qualification for a lead we turned away. Options are in the ticket; **none
taken — that is a human decision.**

Nothing in this repo can reverse a conversion: `sendAction` only ever sends
`value: 1`, and `docs/partnerstack.md` says a reversal has to happen in the
PartnerStack UI.

### Mutation results for this fix

| Mutation | Result |
|---|---|
| Conversion guard removed (the actual bug) | CAUGHT ×3 suites |
| Guard reads a param instead of the row | CAUGHT ×3 |
| Blocked leads back in the SDR list | CAUGHT ×2 |
| Recovery row counts blocked leads again | CAUGHT |
| Guard moved below the domain claim | **UNMEASURED — see below** |

**The last one is not a pass and not a catch.** Two suites fail with exactly
the intended assertions, but `test-partnerstack.js` *crashes*
(`ReferenceError: session_id is not defined`) because it lifts and executes a
slice of `runPartnerStackSignup`, and moving code out of that slice breaks its
scope. Per this repo's own rule a crashed suite makes the run UNMEASURED, so it
cannot be recorded as a catch. It is a harness artifact of an artificial
mutation rather than a coverage gap — but it does mean **that suite would crash
rather than fail cleanly if someone legitimately reorders that function.**
Pre-existing brittleness, not introduced here, not fixed here.

---

## 14b. Parked, deliberately — not in this PR

All four were raised, considered and left out. None is a gap nobody noticed.

| Parked | Why it can wait | What would reopen it |
|---|---|---|
| **`sdr-calling` No Booking exclusion** | Only submit-time blocks are dialable (a step-1 block has no phone, and that workflow requires one) — **~2 leads/month**. The failure is an awkward call, not a lost lead or a wrong charge. The mirror columns ship here, so that repo's fix is one `WHERE` clause when someone picks it up. | A blocked lead actually being dialled, or the rate rising above a couple a month. |
| **Health row for the warehouse dependency** | A warehouse outage silently disables the block. Correct direction — we would rather let a realtor through than block a customer — but nothing alerts on it today. | Wanting to know the block is *on*, as opposed to merely enabled. |
| **`non_icp_checked_at`** | Only the two specified columns ship. "When was this decided" is answerable from `updated_at` for now. | Needing to date a verdict independently of the row's last write. |
| **RevenueHero cancellation API** | Unknown whether one exists. The refusal stops the booking counting and pages a human; actually freeing the diary is manual. | The critical alert firing often enough that manual cancellation becomes a chore. |

---

## 15. Webflow handover — `git push` does NOT ship the form half

Both form files went to **v5.10.0** / **v5.10.0-ads**. Until the Webflow pin
moves, every real visitor runs the old file and **nothing in this repo will tell
you.** The tests pass, Railway redeploys, and the block silently does not exist
in the browser.

### The SHA, and the two tags — generated, not transcribed

**Do not copy a SHA out of a document.** Run this on `main` *after* the merge
lands and paste the two lines it prints straight into Webflow. A SHA written
down by hand is a SHA that goes stale the moment anything else merges — which
is how a handover ships half a fix.

```bash
git checkout main && git pull --ff-only
SHA=$(git rev-parse HEAD)
printf '<script src="https://cdn.jsdelivr.net/gh/DarshilDixit/gushwork-api@%s/gushwork-form.js"></script>\n' "$SHA"
printf '<script src="https://cdn.jsdelivr.net/gh/DarshilDixit/gushwork-api@%s/gushwork-form-popup.js"></script>\n' "$SHA"
```

Three things that are not negotiable:

- **The full 40 characters.** Short SHAs work today but are ambiguous as the
  repo grows, and a collision resolves to the wrong file rather than erroring.
- **Both tags, same SHA**, even though a commit SHA names a snapshot of the
  whole repo so an untouched file returns identical bytes. Pinning them together
  is the only thing that records the pair was *tested* together.
- **Never `@main`.** jsDelivr treats a SHA as immutable and caches it
  permanently; `@main` is a mutable ref served best-effort that needs a cache
  purge, and purges do not reliably take.

Then **republish** in Webflow.

### Sweep every page — a Project-Settings republish does not reach a page-level tag

Two pages were found stale on 10 Sept for exactly this reason. Run this after
republishing; anything not on the SHA above is serving different code to real
visitors, and a page with **no** `@sha` at all is worse than a stale one —
jsDelivr then serves the default branch best-effort and it drifts on its own.

```bash
SHA=$SHA
for p in /demo /start /start-now /ai-demo /meeting-booked /careers \
         /consulting-lead-generation /manufacturing-lead-generation \
         /financial-services-lead-generation /lead-gen /seo-leads \
         /financial-services-seo /manufacturing-seo-services /consulting-seo-services; do
  FOUND=$(curl -s "https://www.gushwork.ai$p" | grep -oE 'gushwork-api@[0-9a-f]{7,40}' | sort -u | tr '\n' ' ')
  case "$FOUND" in
    *"$SHA"*) echo "OK    $p" ;;
    "")       echo "NO PIN $p   <-- serving the default branch, fix this first" ;;
    *)        echo "STALE $p -> $FOUND" ;;
  esac
done
```

`/careers` and `/meeting-booked` should have had their tags removed on 10 Sept.
If they still report a pin, that removal did not take.

### The console banner — the only proof the swap took

Load each page and read the console. If the version is not the one you just
pinned, Webflow is still serving the old file and the deploy is **not done**,
however green this repo looks.

| Page | Expected banner |
|---|---|
| `/demo` and every SEO lander | `[GW] ✅ Form initialised v5.10.0 (/demo).` |
| The Google Ads page | `[GW] ✅ Form initialised v5.10.0-ads (Google Ads).` |

A page showing `v5.9.0` is the pre-block file: no redirect, no `/non-icp-check`
call, and a realtor books normally.

---

## 14. Files changed

| File | What |
|---|---|
| `index.js` | List, matcher, `nonIcpVerdict`, `/non-icp-check`, `/monitor/blocked`, Blocked tab, `slackNonIcpBlocked`, `/partial` + `/submit` enforcement, 3 `Schedule` guards, cron clause, mirror columns + sync, cache-warm gate |
| `db.js` | `non_icp_blocked`, `non_icp_reason`, partial index |
| `gushwork-form.js` | SECTION 4C, step-1 and step-2 checks, prewarms, backstop, v5.10.0 |
| `gushwork-form-popup.js` | Identical port, v5.10.0-ads |
| `tests/test-non-icp.js` | New, 150 assertions |
| `tests/test-partnerstack.js`, `tests/test-batch2.js` | Updated to the new contracts |
| `tests/measure.js`, `tests/.baseline.json` | New suite registered; 2,419 → 2,571 |
| `CLAUDE.md` | Both overridden rules; traps; scope; layout table |
| `docs/tickets/non-icp-v1-block.md` | The two reversals, evidence, domain list, dependency |
| `docs/Non-ICP-flagging-rules-*.pdf` | Swapnil's doc, committed. **Screenshot, no text layer — it does not grep** |
