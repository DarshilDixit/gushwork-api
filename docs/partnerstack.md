# PartnerStack affiliate integration

Handover doc. Covers what the integration does, every moving part, how to test
it, and what is known to be broken or missing.

Read `CLAUDE.md` first for the house rules — particularly the Definitions
section, which governs how any number here is counted.

---

## The two-step model, and why it matters

PartnerStack pays an affiliate in **two separate events**, and they are not
interchangeable:

| Step | What fires it | What it does |
|---|---|---|
| **Conversion** | `/submit`, immediately, for any partner-referred lead | Creates the *customer* in PartnerStack and attaches it to the partner's click. Pays nothing on its own |
| **Qualified demo action** | The Salesforce poller, after an AE ticks `Qualified_Demo__c` | Fires the commission — this is the $50 |

**The conversion must land first.** An action for a `customer_key` PartnerStack
has never seen is a no-op at best, so the poller only ever qualifies domains
where `ps_signup_sent_at` is already set. A missed conversion therefore breaks
*both* steps for that domain, silently. That is why "no conversion sent" is the
first of the two gap checks.

They also use **different hosts and different auth**, which is the single
easiest thing to get wrong here:

```
Conversion   POST https://partnerlinks.io/conversion/xid
             Authorization: Bearer  <PARTNERSTACK_TRACKING_TOKEN>

Partnerships GET  https://api.partnerstack.com/api/v2/partnerships/{key}
Actions      POST https://api.partnerstack.com/api/v2/actions
             Authorization: Basic  base64(PUBLIC_KEY:SECRET_KEY)
```

Both credentials sit in the same environment. Using one where the other belongs
returns a 401 that reads like a bad password rather than the wrong scheme.

**Do not use `POST /v2/customers` for the conversion.** It cannot attach a
click, so the customer is created with no partner against it and the
attribution is lost with no error to tell you why.

---

## Data flow

```
partner link click
  └─ site-wide script sets cookies on .gushwork.ai (90-day window)
       ps_xid / gw_ps_xid                 the click id
       ps_partner_key / gw_ps_partner_key the DECODED partner key
       gw_ps_seen_at                      restamped when the click id changes,
                                          so it is always the WINNING click
       gw_ps_clicks                       JSON array of every click, max 10

  └─ gushwork-form.js + gushwork-form-popup.js
       capturePartnerStack() reads them into formState

  └─ POST /partial (step 1) and POST /submit (step 2)
       readPartnerStackPayload() sanitises and derives ps_customer_key
       partnerIdentityNoNetwork() resolves the partner name from memory or DB
       partnerHearAboutUs() sets "Partner - <name>"

  └─ after res.json(), off the critical path:
       runPartnerStackIdentity()  resolve via the v2 API, stamp the row,
                                  upgrade hear_about_us here + AWS + Salesforce
       runPartnerStackSignup()    claim the domain, POST the conversion
       runPartnerStackEligibility() DORMANT — see the flag below

  └─ every 15 minutes:
       runPartnerStackQualificationPoll()
         Salesforce: Opportunities WHERE Qualified_Demo__c = true
         join to leads by DOMAIN
         claim, then POST /v2/actions
```

**Nothing above blocks or delays a lead.** Every PartnerStack call runs after
`res.json()` and is never awaited. Eligibility decides whether an *affiliate*
is paid; it has no bearing on whether a person gets a demo.

---

## The `ps_*` columns

All on `leads`, and all mirrored to `gw_form_leads` on the AWS warehouse.

| Column | Written by | Notes |
|---|---|---|
| `ps_xid` | `/partial`, `/submit` from the cookie | The click id. Presence of this is what makes a lead "partner-sourced" |
| `ps_partner_key` | `/partial`, `/submit` from the cookie | The **decoded** key. The URL param is base64 of it and is never stored |
| `ps_partner_name` | `runPartnerStackIdentity` (deferred) | From the v2 partnerships API |
| `ps_partner_email` | `runPartnerStackIdentity` (deferred) | Ditto |
| `ps_customer_key` | `/partial`, `/submit` | Normalised root domain from `partnerStackCustomerKey()`. **The join key for everything** |
| `ps_click_at` | `/partial`, `/submit` from `gw_ps_seen_at` | The winning click. Falls back to submit time when absent |
| `ps_click_history` | `/partial`, `/submit` from `gw_ps_clicks` | JSONB, oldest first, max 10. Reporting and disputes only — **attribution reads `ps_xid` and nothing else** |
| `ps_signup_sent_at` | `runPartnerStackSignup` | Claimed *before* the HTTP call, released if it fails |
| `ps_signup_verified_at` | The read-back sweep | Proof the customer really exists, not just that PartnerStack said 200 |
| `hear_about_us_raw` | `/partial`, `/submit` | What the visitor came in saying, before the partner overwrite. First non-empty value wins and is never overwritten |
| `ps_failure_ack_at` / `_note` | `POST /monitor/partner-ack` | Acknowledges a failure. Never clears the stamp — the row keeps its state and stays red; it only drops out of Needs attention and the health row |
| `ps_signup_skipped_reason` / `_at` | The skip guards | `test_email`, `disqualified`, `no_customer_key`, `already_sent` |
| `ps_signup_fail_reason` / `ps_signup_failed_at` | Conversion failure + phantom sweep | Cleared on a later success |
| `ps_qualify_fail_reason` / `ps_qualify_failed_at` | Qualification failure | Cleared on a later success |
| `ps_qualified_sent_at` | `sendQualificationForDomain` | Same claim-first pattern |
| `ps_eligible` | `runPartnerStackEligibility` | **Null today** — the check is off |
| `ps_ineligible_reason` | `runPartnerStackEligibility` | **Null today.** The contractual rejection record |
| `ps_checked_at` | `runPartnerStackEligibility` | **Null today** |

### `partnerStackCustomerKey()` is the only place a domain becomes a key

PartnerStack counts **one conversion per customer key, for the life of the
account**. Two spellings of one company means either an affiliate paid twice or
a real referral swallowed as a duplicate. Website, email, and all three
warehouse customer tables go through this one function.

It builds on the existing `registrableDomain()`, so `acme.co.uk` does not
collapse to `co.uk`. It returns `null` — never a guess — for free-mail
providers and IP literals. `gmail.com` as a customer key would merge every
Gmail lead into one PartnerStack customer, and because the conversion fires
once per key forever, the first would burn it for everyone after.

---

## Custom fields, `meta`, and what is already built in

`company_name`, `website` and `phone` **already exist** as built-in customer
fields with those exact `api_name` values, and all three are sent.

Phone is **optional on our form** — only required for free-mail addresses — so
it is absent more often than not. `sendConversion` drops empty meta values, so
a missing phone omits the key entirely rather than sending a blank, and can
never fail or block the conversion. Read them back any time with:

```
GET /v2/customers/{customer_key}    ->  data.fields[]  (api_name, name, value)
```

Do **not** create fields called "Company Name" or "Website" in Settings — you
would get duplicates with slugged api_names like `company_name_1`, and the ones
we send to would then be the wrong ones.

**VERIFIED 4 Sept 2026.** `meta` does write to the built-in fields, despite
every field reporting `read_only: true`. A conversion for `hello.com` carrying
`meta: {"company_name":"Gushwork","website":"https://hello.com/"}` came back
with both populated, alongside the contact name in `name`. Nothing needs
creating in Settings.

**Read the values back from `data.fields[]` or `data.mdata`, NOT `data.meta`.**
PartnerStack accepts `meta` on write and returns it as `{}` on read, surfacing
the values under `mdata` and `field_data` instead. Checking `data.meta` alone
would make a working integration look broken — which is the same silent-failure
shape this section exists to warn about.

`website` is stored as the raw form value (`https://hello.com/`), not
normalised. That is deliberate: `ps_customer_key` is the normalised join key
and appears as `external_key`, while the Website field shows what the lead
actually typed, which is more use to a human reviewing the record.

### Which environment did a conversion land in?

Nothing in a response header names the environment, and both test and
production public keys are prefixed `pk_`. The reliable indicator is on the
customer record itself:

```
GET /v2/customers/{customer_key}   ->  data.test    // false = production
```

A conversion fired with the current tracking token produced `test: false` and
is visible to the production key pair, so the token and the production keys
address the same environment. The conversion endpoint uses **only** the
tracking token, so swapping the `pk`/`sk` pair cannot change where a conversion
lands — it changes only what the API can read back.

## Environment variables

| Variable | Scope today | Used by | Swap before go-live? |
|---|---|---|---|
| `PARTNERSTACK_TRACKING_TOKEN` | **Production** | The conversion (Bearer) | Already production |
| `PARTNERSTACK_PUBLIC_KEY` | **Production** (`pk_…`) | v2 partnerships + actions (Basic) | Done |
| `PARTNERSTACK_SECRET_KEY` | **Production** | v2 partnerships + actions (Basic) | Done |
| `PS_ELIGIBILITY_ENABLED` | Unset (off) | The dormant eligibility check | Only if you want the check on |

All three are production as of 4 Sept 2026, verified: a conversion fired with
the tracking token produced a customer flagged `test: false` and readable by the
production key pair. Note the conversion endpoint uses **only** the tracking
token — swapping the key pair changes what the API can read, never where a
conversion lands.

Also relevant, and already present: `MONITOR_TOKEN` guards every `/monitor/*`
route including the two new ones.

---

## The eligibility check (built, off)

The MVP ships **without** automated eligibility. Rejections are decided by hand
at payout approval. Everything is written and tested; the whole thing is behind
one flag:

```
PS_ELIGIBILITY_ENABLED=true     # exact string; anything else is off
```

No rebuild, no code change. Turning it on also starts the warehouse customer
cache warming at boot.

When on, `partnerStackEligibility()` returns `{ eligible, reason, detail }` with
stable reason strings — they are stored and are what an affiliate is eventually
told. Three rules:

- **(c) test address** — `b@g.ai` plus `ELV_EXCLUDED_DOMAINS`. Scoped to
  PartnerStack only; the dashboard still counts internal addresses everywhere
  else, deliberately.
- **(b) current customer** — the union of `gist.customer_contract_terms`,
  `gist.gist_accountsmaster` and `gist.customer_enrichment` on the AWS
  warehouse, cached 30 minutes, bounded by an 8s timeout.
- **(a) prior contact** — a prior inbound form lead on the same root domain in
  the 90 days before the click. The source is a swappable registry
  (`PS_CONTACT_SOURCES` / `PS_CONTACT_ACTIVE`).

**It fails CLOSED.** A check that cannot run returns `check_failed` rather than
waving the conversion through, because the conversion fires once per key
forever and cannot be recalled, whereas a skipped one is still in the log to
send by hand.

**The 12-month clause is not enforceable.** Nothing in the warehouse can date a
churn — `customer_contract_terms` has zero churned rows, `gist_accountsmaster`
has an `End_Date` on 2 of 330, and `public.subscriptions` has no row with a
future billing date. Every passing verdict carries
`unverified: ['customer_last_12_months']` so the gap is visible rather than
silently passing. The clause stays in the affiliate terms; we cannot enforce it
here yet.

### Measured rejection rates (90 days of real leads, Sept 2026)

| Rule (a) source | Rejects |
|---|---|
| Prior inbound form lead (**active**) | 9.7% |
| Cold email in the 90 days before submit | 25.6% |
| Cold call in the 90 days before submit | 4.9% |

A naive "cold-emailed in the last 90 calendar days" reading looks like 39.6%,
but 17.4 points of that is our own sequencer following **up** on an inbound
lead, which is not prior contact. Measure the same way before switching a
source on.

---

## Claim-first, and why

Both the conversion and the qualification are **once per domain, ever**, and
both **claim the domain before the HTTP call, not after**.

Checking "has this been sent?" and then sending is a race: two submits for the
same domain arriving together both read no stamp, both fire, and PartnerStack
credits the affiliate twice with no way to undo it.

So the stamp is taken first, as a conditional `UPDATE` backed by a unique
partial index, and only the winner sends:

```
leads_ps_signup_once_idx     UNIQUE (ps_customer_key) WHERE ps_signup_sent_at    IS NOT NULL
leads_ps_qualified_once_idx  UNIQUE (ps_customer_key) WHERE ps_qualified_sent_at IS NOT NULL
```

A concurrent claim surfaces as Postgres `23505` and is read as "already sent".

**If the send then fails, the claim is RELEASED** so a later attempt can retry.
Leaving the stamp on a conversion that never arrived is the worse failure: it is
silent, permanent, and costs the affiliate a real payout with nothing in the
system saying so.

---

## The Salesforce poller (step 10)

Every 15 minutes, `runPartnerStackQualificationPoll()`:

1. `SELECT Id, Name, Account.Website, (contact email) FROM Opportunity WHERE Qualified_Demo__c = true`
2. Derives a domain per Opportunity — `Account.Website` first, the primary
   contact's email domain as fallback, both through `partnerStackCustomerKey()`
3. Keeps only domains where `ps_signup_sent_at IS NOT NULL` and
   `ps_qualified_sent_at IS NULL`
4. Claims, then `POST /v2/actions`:

```json
{ "type": "qualified_demo", "value": 1, "target_type": "customer", "target_key": "<domain>" }
```

**All four fields are required, and there is no `customer_key` on this
endpoint** — that name belongs to `/conversion/xid`, and the two endpoints do
not share a schema. Sending `customer_key` returns
`400 'target_type' is a required property`, which reads like one missing field
and is actually two missing plus one unrecognised.

`target_type` is `"customer"`, not `"partnership"`: the action attaches to the
customer the conversion created, and PartnerStack resolves the partner from
that customer's existing attribution. Targeting the partnership would record a
partner-level event with no customer context — a *different* event that would
still return 200.

**A poller, not a Salesforce Flow callout**, deliberately: a Flow that calls out
fails inside Salesforce where nobody on this team would see it, and it couples an
AE ticking a box to our service being up at that instant. A missed window is
just a later window.

**`Qualified_Demo__c`** is a checkbox on Opportunity, default unchecked, visible
and editable for AE / SDR / SDR Manager / System Administrator / Minimum Access
– API Only Integrations. If it is missing or invisible to the API user, the SOQL
fails, logs `[SF] Qualified-demo query failed`, and returns `[]` — the poller
degrades to a no-op rather than breaking anything.

**The domain is the join.** It is the only identifier both systems share:
PartnerStack knows the customer by the `customer_key` we sent at signup, which
came from the lead's website.

---

## The funnel shows TWO numbers per stage, and both are load-bearing

Nine stages: Clicks, Reached step 1, Completed, Conversion sent, Conversion
verified, Booked, Opportunity created, Qualified Demo ticked, the payment
fired. Defined once in `PS_FUNNEL_STAGE_SQL` and interpolated into both the
programme-wide and per-partner queries, so a partner column and the headline
are computed by the same expressions and cannot disagree.

Each stage carries **two counts**:

- the **cumulative** one repeats all prior conditions, so each is a strict
  subset of the one before and the funnel nests by construction. Filtering
  each stage independently does NOT nest — a dry run against real data showed
  "Opportunity created" at 2 sitting after "Booked" at 0, because Salesforce
  Opportunities exist for companies that never booked through our form.
- the **absolute** twin (`abs_*`) asks only "did this happen for this domain",
  with none of the prior conditions.

**The absolute is the headline; the cumulative is named underneath as "on the
funnel path", and only when the two differ.** In the healthy case a row looks
exactly as it did before.

**Why both, and not just the cumulative one.** On 5 Sept the funnel read
`The $50 fired: 0` while the summary card read 1, the per-domain row read
"ticked, $50 fired", and the commission was sitting in PartnerStack.
`hello.com` had a hand-made Salesforce Opportunity and never booked through our
form, so the cumulative chain dropped it at BOOKED and it could not appear in
any stage after that. The cumulative rule was not the fault — it is still the
only column that nests, and it stays. The fault was that **it was the only
number on screen**, which is this integration's recurring bug once more: we
held the evidence the answer was incomplete and rendered it as fact.

This is not a test-only shape. A company that books directly with an AE, or an
Opportunity an SDR raises by hand, reaches the payment stage without a form
booking and with real money attached.

**The twins are deliberately the same expressions the summary cards use**, so a
funnel-vs-card contradiction is structurally impossible rather than fixed once.
Change one and you must change the other; `tests/test-partnerstack.js` asserts
they match, that no twin carries an earlier stage's condition, and that the
rendered headline never reads 0 for a stage that happened.

**Rates still run stage to stage down the cumulative path.** A rate between two
absolutes is not a step-to-step rate and is not bounded by 100%.

**The funnel is still not a census of Salesforce**; the per-domain table is. But
a number here can no longer read *lower than the thing that happened* — it
reads the true count and says how many of them skipped a stage.

The per-partner table gets the same treatment, because it had the identical bug
one level down: `Qualified 0` for the partner whose $50 had already fired. Its
stage cells show the absolute count and carry a dagger with both numbers in the
tooltip where some of it skipped. `psVal` is the single function behind both the
cell and the sort comparator — a column that sorts on a number it is not
showing is the same class of bug as one computed and never rendered.

The programme row is **its own ungrouped query, not a sum of the per-partner
rows**: a domain can carry leads from two partners, and summing would count it
twice.

**Losses sit beside the stage where the money leaks** — conversion failed and
skipped at the conversion stage, booked-with-no-Opportunity and sfopp-errored
at the Opportunity stage, qualification failed at the payment stage. Never left
as the gap between two numbers: that is arithmetic the reader should not have
to do, and it hides which of several causes it was. A zero loss is not
rendered.

**Rates are suppressed below `PS_RATE_MIN` (10)** and say so, naming the base:
`too few to rate (n=9)`. One of two is "50%" and means nothing.

## Units: everything on the Partners tab counts COMPANIES

The funnel is **step 1 → completed → converted → booked → qualified**, and every
column counts distinct `ps_customer_key`. That is deliberate and it is a change
from the first version, which mixed people and domains: a funnel implies each
column is a subset of the last, and a people count sitting next to a domain
count does not nest. Companies is also the truthful unit, since PartnerStack
pays per customer.

The one exception is stated on screen: leads with **no usable domain** cannot be
keyed by one, so they are counted as **leads** and the chip says
"leads, not companies".

**Clicks that never reached the form are not in our data at all.** They exist
only in PartnerStack, and the v2 API exposes no click endpoint we can reach —
`/v2/links`, `/v2/clicks` and `/v2/partnerships/{key}/stats` all 404 with our
credentials. The funnel therefore starts at step 1, not at the click, and the
tab says so. The only remaining route is a custom report configured in the
PartnerStack UI and pulled via `/v2/vendor/report-export/{key}`, which nobody
has set up.

## hear_about_us has three possible authors, and only one used to survive

One column, written in sequence by:

1. **`prefillHearAboutUs()`** (both form files, at init) — derives from
   UTM/referrer into `Facebook (Paid)`, `Instagram (UGC) — Creator`,
   `Google Ads`, `linkedin`, and **hides the input**, so the visitor cannot
   type over it. A `gw_ref_email` cookie wins first as `Referral - <email>`.
2. **The visitor** — reachable only when no prefill matched.
3. **`partnerHearAboutUs()`** (server, both routes) — overwrites with
   `Partner - {name}` unless the value starts with `Referral -`.

1 and 2 are mutually exclusive on the client, so exactly one value arrives —
and step 3 was **destroying** it, not hiding it. A partner-referred lead who
came in on a paid ad is two real facts and we kept one.

`hear_about_us_raw` now stores what arrived. **Its COALESCE runs the opposite
way round from every other one in these queries** —
`COALESCE(leads.hear_about_us_raw, EXCLUDED.hear_about_us_raw)` — because the
FIRST non-empty value must stick and nothing may overwrite it.

There is deliberately no third `_auto` column reconstructing the ad-derived
value from the UTMs: since 1 and 2 are mutually exclusive, it would either
duplicate `_raw` or be empty, and it would be a reconstruction rather than a
record.

**Recovery of the four historical leads: one of four.** The value is gone from
Railway (single column, upserted), from the AWS mirror (it receives the final
value), and from `form_sessions` (no such column). `test.com` is the exception
— its Salesforce Lead still reads `Testing RevenueHero`, written before the
partner logic shipped — and that one row is backfilled. The other three had
`utm_source = null` and `referrer = direct`, so there was no ad attribution to
lose either way.

## The read-back guard

`/conversion/xid` answers **200 with an empty body**. There is nothing in the
response to check, so a 200 that created nothing would still stamp
`ps_signup_sent_at` — and the once-per-domain rule would then burn that domain
**permanently**, with no error anywhere. The only real proof is reading the
customer back.

`runPartnerStackConversionVerify()` sweeps every 15 minutes for conversions
sent but not yet verified:

- **exists** → stamp `ps_signup_verified_at`
- **definitive 404** → release the claim (`ps_signup_sent_at = NULL`) so the
  domain can convert again, and `recordFailure` a *phantom conversion*
- **anything else** (5xx, timeout, network) → leave the row alone, retry next
  sweep

**The grace period is the whole design.** PartnerStack's API and dashboard lag
behind a conversion — measured at under 2 minutes for one record and about 6
for another on 4 Sept 2026. Checking immediately would report healthy
conversions as missing and release good claims, causing duplicate conversions
on the retry. `PS_VERIFY_GRACE_MIN` is 15, comfortably past the worst lag seen.

**"Could not tell" is never "missing".** Only a definitive 404 releases a
claim. Treating a 5xx as missing would un-stamp every pending conversion during
a PartnerStack outage and re-fire them all.

**A sweep, not a `setTimeout` after the send.** A timer dies with the process,
and a deploy in the wrong ten minutes would lose the verification silently —
the same class of failure this exists to catch.

It also flags a record that comes back `test: true`, because a production
integration writing test records pays nobody and looks completely healthy
otherwise.

## The recurring bug: a completeness signal computed and dropped

This has now happened four times in one integration, and every instance had the
same shape — **we had evidence the answer was incomplete and rendered it as
fact**:

1. The sfopp log joined on **domain** when it is keyed by `prospect_email`.
   Zero rows would have rendered as "no sfopp errors anywhere".
2. The gap card's `0 no Opportunity` rendered identically whether Salesforce
   came back clean or was **never asked**.
3. `already_opp` rows with **no `sf_opportunity_id`** (124 of 391) look like
   every other row. *Still open — to be surfaced with the Salesforce view.*
4. `findOpportunityDomains` returned `truncated`, it was **true on every call
   since the first commit**, and nothing ever read it. 5,898 Opportunities
   existed and we read 1,252.

5. **Clicks** were computed server-side, returned in the payload, and had no
   column, no cell and no sort entry. Present in the API, absent from the
   screen — and it would have been reported as working.

**The rule now: any completeness or confidence signal must reach the UI or fail
loudly. It may never be silently dropped.**

**And its corollary, learned the hard way five times: anything computed
server-side must be VERIFIED AS RENDERED, not merely confirmed present in the
payload.** "It is in the response" is not evidence anyone can see it. Check the
header, the cell, and the sort entry — or assert it in a test that executes the
renderer. Five for five: the email-not-domain join, the gap card's silent zero,
`truncated` never read, a `LIMIT` defeating its own guard, and a column that
was never added.

Concretely —

- An incomplete Opportunity read returns `ok: false` and writes nothing, rather
  than handing back a partial list. Returning `ok: true` with 40% of the records
  reproduces the bug one page later.
- The tab shows what the last read actually saw
  (`read 5,900/5,900 opportunities · 6 pages`), red when it failed.
- The domain list cap is a named constant and hitting it renders
  `capped at 500 domains`, so a page never reads as the population.

**A `LIMIT` can defeat the guard that is meant to catch it.** With
`LIMIT 2000`, Salesforce reports `totalSize: 2000`, so `records.length ===
totalSize` is satisfied by a truncated result. The completeness check passed on
2,000 of 5,898. The SOQL now carries no `LIMIT` at all; pagination plus
`SF_MAX_PAGES` is the bound.

## Small datasets catch bugs that large ones hide

This was found because **`hello.com` contradicted itself at n=4**: the tab said
`no_opportunity` for a domain we had qualified an hour earlier, which requires
an Opportunity to exist. With four domains that contradiction is unmissable.

At 400 domains, `no_opportunity` on most of them would have looked entirely
plausible — a young partner programme where few companies have reached an
Opportunity yet. The number would have been wrong by 79% and nobody would have
had a reason to doubt it.

So: **verify against the smallest real dataset available, not the largest.** A
handful of rows you can check by hand beats a plausible aggregate. The same
logic applies to the dry-run habit below — every bug caught tonight by running
a statement against real data was caught because the result set was small enough
to read.

## A control-flow trap worth knowing

`refreshPartnerDomainSfState` was chained onto the end of
`runPartnerStackQualificationPoll`, after its `try/finally`. **It never ran
once.** That poll has three `return`s inside its `try`, and a return there
exits the whole function — the `finally` still fires, so the flag resets and
everything looks healthy, but anything after the `try/finally` is skipped. The
common case takes an early return: once everything qualified has been sent, the
pending-domains query is empty and the poll returns at that line on every
subsequent tick, forever.

Adding a boot-time call would have been the *worst* fix — it would have run
once per deploy, populated the column, and looked correct. The fix is separate
scheduling, matching `startPartnerStackCacheWarm` and
`startPartnerStackConversionVerify`, which both already do boot-then-interval.

The pattern was audited across the repo. `lookupElvStatus`,
`partnerIdentityNoNetwork` and `sendQualificationForDomain` all have returns
inside a try with code after it, and all three are **correct** — the early
return is "found" or "did not win the claim", and the code after is the
intended fallback. The poller was the only genuine instance.

## Acknowledging a failure

`POST /monitor/partner-ack` — **the second mutating route on `/monitor`**. The
first, `/monitor/website-recheck`, shipped as a GET that rewrote lead rows,
which a link prefetch could have fired. Same rules: POST only, token-guarded,
never linked as a URL.

It exists because `phantom_200` covers two different things. `test.com`'s was a
genuine 200-with-no-customer, but the cause was a customer deleted in
PartnerStack by hand — housekeeping, not a lost $50. The two produce the same
stamp, and **an alert that is wrong the first time it fires gets ignored**
(which is also why the `/partial` health row is worth revisiting).

**This route suppresses alerts, so its failure mode is silence.** It therefore
does the least it can:

- it never clears `ps_signup_failed_at` or the reason — the history stays and
  the domain keeps its red chip
- it only removes the domain from **Needs attention** and from the **health
  row**, and both consumers are asserted
- it refuses to acknowledge a domain with no failure, which would otherwise
  pre-silence a future genuine one
- acknowledging nothing is a 404, never a silent success
- it is reversible with `acknowledged: false`

## Monitoring

**Overview → Partner gaps** — the alert. **Derived from the lifecycle ladder,
not from its own query.** Two independent queries once told two different
stories about the same four domains: this card said "2 no conversion" while the
Partners tab said one failure and one correct skip. A skip is the system working
and is reported separately, never counted as a gap — counting it meant a
test-address lead sat in the alert forever with no way to clear.

"0 no Opportunity" now distinguishes three states: *unavailable* (Salesforce
could not be reached), *none eligible to check yet* (nothing had a past demo, so
Salesforce was never called), and *N no Opportunity of M checked*.

**Per-domain Salesforce state** (`partner_domain_sf_state`, refreshed every 15
minutes for EVERY partner domain): `ticked`, `exists_unticked`,
`create_errored`, `no_opportunity`. `exists_unticked` is the row to action
daily — the Opportunity exists and no AE has ticked `Qualified_Demo__c`, so the
$50 cannot fire. `create_errored` comes from `gist.sf_lead_conversion_log`,
joined on **`prospect_email`, not on domain** — that table is email-keyed and a
domain join would silently match nothing. It is a warehouse table, so the error
state costs no Salesforce API call, and `Qualified_Demo__c` rides along on the
Opportunity query already being made.

The old note: Two ways a referral silently never pays:

- **No conversion ever sent** for that domain (pure DB, unambiguous)
- **Demo happened 3+ days ago and no Opportunity exists** (the sfopp gap)

Deliberately *not* a System Health row: a lead waiting on an AE is normal
latency, and a permanently amber badge trains people to ignore it.

It is deliberately **not** "booked but not qualified" either. That conflates no
Opportunity (broken), Opportunity awaiting an AE (normal), and Opportunity the
AE deliberately did not tick (correct, and permanent). The third never clears,
so the queue would fill with correct non-payments and hide the real failures.

When Salesforce cannot be reached, the card shows `N+?` and the panel says the
result is not clean. "We could not check" is never "we checked and it is fine".

**The whole partner view is now ONE point of failure.** The gap card used to
run its own query and would survive `partnerLifecycle()` breaking. It no longer
does: check A reads the ladder, so if `partnerLifecycle()` throws,
`/monitor/partner-gaps` 500s as well as `/monitor/partners`. That was a
deliberate trade — two independent queries told two different stories about the
same domains, and one classification is worth more than independent failure
modes — but it means a single bad ladder query takes out both surfaces at once.
If you are changing `PS_LADDER_SQL`, you are changing both.

**Partners tab** — the operational view. Cards for partner leads (total and last
24h), conversions sent, qualified demos fired, partner bookings, and lead→booking
rate. A per-partner table, sortable, where clicking a row drills into All Leads
with the existing partner filter applied.

Note the units, which are deliberately different and labelled as such: leads and
bookings are **people** (`COUNT(DISTINCT lower(email))`), conversions and
qualified demos are **domains**, because that is what PartnerStack counts.

**All Leads** also has a partner filter (matching key, name or email) and a
partner panel in the row detail showing the full click history with the winning
click badged.

---

## Testing end to end

```bash
node tests/test-partnerstack.js     # 419 assertions, no DB, no network
```

Run all five dependency-free suites after any change — see CLAUDE.md.

**A real end-to-end test:**

1. Open a partner link so the cookies get set on `.gushwork.ai`.
2. Fill in `/demo` with a **non-`gushwork.ai`** email. Any `gushwork.ai` or
   `test.com` / `example.com` / `example.org` address hits the test-address
   guard and the conversion is skipped by design.
3. Watch Railway logs:
   ```
   [PartnerStack] -> GET  /v2/partnerships {"partner_key":"…"}
   [PartnerStack] <- 200 OK
   [PartnerStack] Resolved partner … -> Name <email>
   [PartnerStack] -> POST /conversion/xid {"xid":…}
   [PartnerStack] <- 200 OK
   [PartnerStack] ✅ Conversion sent: <domain>
   [PartnerStack] hear_about_us upgraded to "Partner - <name>"
   ```
   An organic lead logs `No partner on this submit (…)`, so silence means
   something is wrong rather than "no traffic".
4. Check the customer appears in PartnerStack, attributed to the partner.
5. For the qualification: tick `Qualified_Demo__c` on that Opportunity and wait
   up to 15 minutes for `[PartnerStack] ✅ Qualification sent: <domain>`.

**Re-testing needs a new domain.** Once per domain forever means the second test
from the same domain is correctly skipped with `already sent`.

### Frontend changes need a Webflow repin

`git push` does **not** ship a form change. Both form files are pinned to a
commit SHA in Webflow → Project Settings → Custom Code. See CLAUDE.md.

---

## Status: what is verified and what is not

Verified end to end against live production data (4 Sept 2026):

| Piece | Evidence |
|---|---|
| Conversion | `hello.com`, `test: false`, correct attribution, `meta` populated |
| Partner identity | `Test Account <growth@gushwork.ai>` resolved via v2 |
| `hear_about_us` | `"Partner - Test Account"`, referral still wins |
| Slack journey line | name, email, click date |
| Qualification | `act_3yZ2M4ZGbIqGz1`, `"Action tracked successfully"` |
| Read-back guard | caught a real phantom (`test.com`) within 15 min of going live |
| Claim release | exercised twice by genuine failures, both retryable afterwards |
| Lifecycle ladder | states reconcile with the domain total |
| Click-history backfill | one row, dry-run first, Railway and mirror both normalised |

**NOT verified — waiting on real traffic, not on work:**

- **`syncToAWS` writing `ps_signup_sent_at`, `ps_signup_verified_at` and
  `ps_qualified_sent_at`.** The code shipped in batch C but no lead has been
  written since. The next real form submit proves it: those columns should stop
  being NULL for new rows in `gw_form_leads`. Until then the mirror still shows
  NULL for every existing row, because the backfills only touched Railway.
- **The Slack alert on `conversion_failed` / `qualification_failed`.** Built in
  batch A and never fired. It cannot be triggered without a genuine failure and
  should not be faked. The first real one is the test.
- **The Partners tab rendered in a browser.** The SQL runs and the JSON is
  correct; nobody has looked at the page.

Do not mark any of these done on the strength of the code existing.

## Next up — not built

**The two Salesforce writes.** Both fields exist and were verified with a live
round-trip on 4 Sept:

| Object | Field | Write from |
|---|---|---|
| Lead | `hear_about_us_raw__c` | `pushToSalesforce`, at submit — one entry in `CUSTOM_FIELD_MAP` |
| Opportunity | `Partner_Source__c` | `refreshPartnerDomainSfState`, which already holds the Opportunity id, the partner name and the domain in one loop |

Writing `Partner_Source__c` from the poller happens strictly **after** sfopp has
created the Opportunity, so there is no race and no lead-conversion field
mapping to configure. It also covers Opportunities created any way, not only by
conversion — which a Lead field cannot.

**Make a permission failure LOUD.** Everything the service does on Opportunity
today is read-only, so a write rejection has never been exercised. A silent
failure here would look exactly like a partner with no Opportunity.

## Related open tickets

- **`docs/tickets/apollo-enrichment-not-reaching-salesforce.md`** — Apollo
  enrichment reaches 0% of Salesforce Leads while we hold it for 49% of leads.
  Found while investigating the partner list view. Not a PartnerStack bug, but
  the same shape as the four below.
- **`disqualified` read inconsistently** across six sites — see gap 3.
- **`docs/tickets/health-alert-state-is-in-memory.md`** — a red health row
  re-alerts on every deploy, because the cooldown and the last-reported state
  are in-memory Maps. Affects every check, not just PartnerStack.
- **`docs/tickets/salesforce-integration-user-is-a-system-administrator.md`** —
  the integration user has Customize Application, Modify All Data and delete on
  Opportunity. Far more than it needs.

## Known gaps

**1. sfopp failures mean no Opportunity, and no Opportunity means no payout.**
`sfopp` is a separate Railway service that creates the Opportunities this poller
depends on. Its log shows some show-ups stuck at `not_in_sf` or `error`. When
that happens no Opportunity exists, no AE can tick the box, and the affiliate is
never paid — with nothing in this repo failing. That is what the second gap
check surfaces; it does not fix it.

**2. `syncToAWS` is fire-and-forget with no retry.** A failed mirror write logs
a warning, calls `recordFailure`, and the lead proceeds. Nothing retries it, so
Railway and `gw_form_leads` can drift with no alarm beyond the 24h/10% health
tolerance. Whether Railway currently holds rows the mirror never received is
**unverified** — the Railway database is on a private network and is not
reachable from a developer machine.

Related: six verification columns exist on Railway `leads` and **not** on
`gw_form_leads` — `elv_status`, `elv_checked_at`, `website_check_failed`,
`website_check_reason`, `website_check_reason_prev`, `website_rechecked_at`. The
dialer reading the mirror cannot see whether a lead was verified.

Also: any **single late-arriving field** must use a targeted `UPDATE`, never
`syncToAWS` with a partial object. That upsert sets
`disqualified = EXCLUDED.disqualified` with no COALESCE, so a partial object
passes `false` and clears a real disqualification on the mirror.
`syncBookingToAWS`, `syncPartnerIdentityToAWS` and `syncHearAboutUsToAWS` exist
for this reason.

**Postgres does not guarantee predicate order in an AND chain.** Two things in
this integration have been bitten by it: `start_time::timestamptz` in the
lifecycle ladder and `decode(pk,'base64')` in the click-history backfill. Both
are guarded by a `CASE` whose `WHEN` establishes the value is safe to convert,
because a flat `WHERE regex AND cast(...)` lets the cast run first and one bad
row takes the whole statement down. The backfill version was caught by running
the statement as a read-only SELECT against real data before shipping —
`invalid base64 end sequence` — which is worth doing for any statement that
converts or casts untrusted text.

**3. OPEN TICKET — `disqualified` is read inconsistently across six sites.**
The stage ladder uses `IS TRUE` / `IS NOT TRUE`; the dashboard metric counts
(`index.js` ~1805), the recovery cron, the recovery health check, the backlog
count and the SDR list all use `= true` / `= false`. A NULL flag lands in
neither bucket and vanishes from those queries entirely. `DEFAULT FALSE` makes
it unlikely, not impossible.

Deliberately left open and NOT bundled into the PartnerStack batches — it
touches the recovery cron and the SDR list, which are unrelated to partners and
would have made those reviews about two things at once. Its own ticket, on
purpose.

**4. RESOLVED — `ps_click_history` base64 partner key.** The `gw_ps_clicks`
cookie carried the key base64 before 12:07 on 4 Sept and decoded after, so one
partner had two strings in one JSONB column. Normalised on write behind a
round-trip guard (batch C), and the single affected row backfilled on both
Railway and the AWS mirror. Kept as a numbered entry so older links do not
renumber.

**5. The eligibility check has never run against production data.** It is built
and unit-tested but dormant. Turning the flag on for the first time should be
watched, not assumed.

**6. Nothing outstanding on the payload.** The `meta` mapping is verified —
see the custom fields section. Kept as a numbered entry so the list does not
renumber against older links.
