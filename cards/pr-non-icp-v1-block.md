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
| Calendar | redirected to `/thank-you` | RevenueHero as usual |
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
| 2 | `handleStep1Next` | email | redirects to `/thank-you` | both form files |
| 3 | `handleStep2Next` | email + website | redirects, **before RevenueHero** | both form files |
| 4 | `POST /partial` | email, website | **stamps the row**, suppresses `StartTrial` | `index.js` |
| 5 | `POST /submit` | email, website | **stamps the row**, suppresses `Lead`, Slack, no Salesforce | `index.js` |
| 6 | `/booking-confirmed` | row | suppresses `Schedule` | `index.js` |
| 7 | `/booking-confirmed-webhook` | row | suppresses `Schedule` | `index.js` |
| 8 | `/booking-confirmed-webhook-rh` | row | suppresses `Schedule` | `index.js` |
| 9 | `/cron/send-partials` | row | excludes from recovery email | `index.js` |

**2 and 3 are UX only.** Anyone can skip them with devtools. **4 and 5 are the
enforcement** — they stamp the column and suppress Meta regardless of what the
browser did.

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
8. **Untested in a browser.** No form file has been loaded in a real page. The
   step-1 and step-2 paths are asserted by source and by the executed server
   statements, not by a click-through.

---

## 11. What was verified, and how

**Run and passing:**

- `node tests/measure.js --check` — **11 suites, 2,571 assertions, 0 failures.**
  Run bare, not piped. Baseline updated 2,419 → 2,571.
- `tests/test-non-icp.js` — **150 assertions**, new. Fixtures are **real
  production rows**, including all five substring false positives.
- Three pre-existing suites needed updating because this PR changes contracts
  they encode: `test-partnerstack.js` (cache-warm now has two consumers;
  `res.json` argument changed; `showTab` gained a tab; form version banner),
  `test-batch2.js` (never-NULL mirror binds went from four to five),
  `test-ads-parity.js` (version header). **None were loosened** — each now
  asserts the new intended contract.

**SQL was EXECUTED, not just asserted.** Per `CLAUDE.md`: built a temp schema
from the real migrations on Railway inside a transaction, ran the real
statements, and rolled back.

- `/partial` upsert, `/submit` upsert, `/monitor/blocked`, `/cron/send-partials`
  — all four **executed successfully**.
- **Stickiness proved by execution**, not assertion: inserted with
  `non_icp_blocked = true`, ran the `/partial` upsert again with the verdict
  `false`, and the row stayed `t / kw.com`. That is the
  realtor-clicks-"actually-we're-B2B" scenario.
- Confirmed afterwards: **0 test rows in production**, columns still absent
  (they are created at boot by `db.js`).
- Placeholder arithmetic checked by counting: `/partial` 42=42, `/submit` 41=41,
  mirror 60=60.

**Mutation testing — 13 mutations, via `tests/measure.js --mutation`:**

| Mutation | Result |
|---|---|
| Sticky block → `= EXCLUDED` (realtor clears own block) | CAUGHT |
| Substring matching reintroduced | CAUGHT |
| Known-customer bypass removed | CAUGHT |
| Warehouse failure blocks instead of failing open | CAUGHT |
| `StartTrial` suppression removed | CAUGHT |
| Blocked branch pushes to Salesforce anyway | CAUGHT |
| Recovery-cron exclusion removed | CAUGHT |
| Step-2 check moved after RevenueHero | CAUGHT |
| Client redirect never fires | CAUGHT |
| Ads fork check removed (popup only) | CAUGHT |
| `statefarm.com` removed from the list | CAUGHT |
| Env flag defaults ON | CAUGHT |
| **One `Schedule` guard neutered to `if (false)`** | **SURVIVED → fixed → CAUGHT** |

**That survivor is worth your attention.** The first assertion counted
occurrences of the log *message*, which stays in the file when the condition is
replaced by `if (false)`. Third appearance of the reachability blind spot
`CLAUDE.md` records for ordering assertions. Fixed in a follow-up commit
(`Schedule guards: assert reachability, not just the log string`) — the
assertion now requires the real condition adjacent to each route's own log line,
plus that all three return.

**NOT verified:**

- No form file loaded in a browser. No real block observed end to end.
- No real Slack message sent. `slackNonIcpBlocked` has **never executed**;
  `tools/fire-alert.js` does not cover it. Per the repo's own rule ("we asserted
  it alerts" ≠ "we watched it alert") **this is an untested alert path.**
- The Blocked dashboard tab has not been rendered.
- `/non-icp-check` has never served a real request.

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

**Then the Webflow half, which `git push` does NOT do.** Take
`git rev-parse HEAD`, update **both** script tags in Webflow → Project Settings
→ Custom Code, republish, and **sweep every page** with the loop in `CLAUDE.md`.
Confirm the console banner reads `Form initialised v5.10.0` on `/demo` and
`v5.10.0-ads` on the Ads page. Until that is done, **the form half of this PR is
not live** however green the repo looks.

**Finally, watch one real block arrive in Slack.** Submit the form with a
`@kw.com` address from a browser. Nobody has seen this message.

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
