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
whose conversion is **verified**, not merely sent (`ps_signup_verified_at`, not
`ps_signup_sent_at` — see the poller section for why that distinction is worth
$50). A missed conversion therefore breaks
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

  └─ every 2 minutes:
       runPartnerStackQualificationPoll()
         Salesforce: Opportunities WHERE Qualified_Demo__c = true
                     (paginated, complete or it refuses to answer)
         join to leads by DOMAIN, VERIFIED conversions only
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

Every **2 minutes** (was 15 until 7 Sept 2026),
`runPartnerStackQualificationPoll()`:

1. `SELECT Id, Name, Account.Website, (contact email) FROM Opportunity WHERE Qualified_Demo__c = true`
   — **no `LIMIT`, paginated, and it returns `{ ok, records }`.** See below.
2. Derives a domain per Opportunity — `Account.Website` first, the primary
   contact's email domain as fallback, both through `partnerStackCustomerKey()`
3. Keeps only domains where the conversion is **verified**
   (`ps_signup_verified_at IS NOT NULL`) and `ps_qualified_sent_at IS NULL`
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

**Two minutes, and the read is cheap enough to justify it.** The interval was 15
minutes until 7 Sept 2026, and that number was never chosen for cost — it was
what the other partner jobs happened to use. This query reads only the *ticked*
Opportunities: 3 records today, one page for years, so ~720 Salesforce calls a
day, which is nothing against the org's limit.

**Do not put this poller on `partner_domain_sf_state` instead.** It is the
tempting consolidation — that table already carries `Qualified_Demo__c` for
every partner domain, so one Salesforce read would serve both. It was
considered and rejected on 7 Sept: `refreshPartnerDomainSfState` scans **every**
Opportunity in 180 days (5,898 records, 6 pages) because it has to tell
`no_opportunity` from `exists_unticked`, and it is right to run every 15
minutes. Reading the qualification off that table would put the cheap,
latency-sensitive question on the expensive question's schedule and silently
undo the 2-minute interval — a poll every 2 minutes over a table that changes
every 15 sees the same rows seven times. Two queries, two schedules, on purpose.

**Equally: do not shorten `PS_SF_REFRESH_INTERVAL_MS` to match.** Different
question, six growing pages. And `PS_VERIFY_GRACE_MIN` is a third thing again
and stays at 15 — PartnerStack's own indexing lags a conversion by 2 to 6
minutes, so checking sooner releases good claims and re-fires conversions.

### The ticked query had the `LIMIT` bug, for the third time

Fixed 7 Sept 2026. `findQualifiedDemoOpportunities` was
`WHERE Qualified_Demo__c = true LIMIT 200`, unpaginated, with no completeness
check and no `ORDER BY`, and it returned `[]` on every failure.

Nobody unticks the box, so the ticked set only ever grows. Past 200 across the
whole org, Salesforce returns an arbitrary 200 in an undefined order, and a
newly ticked partner Opportunity can sit outside them: **the affiliate is never
paid and nothing anywhere says so.** The trigger was not hypothetical — it is
the direct consequence of telling AEs the checkbox exists, which is the whole
point of the field.

It is now paginated with no `LIMIT`, checked against `totalSize`, bounded by
`SF_MAX_PAGES`, and it answers `{ ok, records }` so the poller can tell "no AE
has ticked anything" from "Salesforce did not answer" — which used to be the
same value. A failed read logs, calls `recordFailure`, and **returns without
concluding that nothing is ticked**.

It carries **no date bound**, deliberately, unlike `findOpportunityDomains`
which bounds to 180 days. A bound here would make an Opportunity created before
the window and ticked today invisible forever, which is a silently unpaid
affiliate. The ticked set is small enough that reading all of it costs nothing.

### VERIFIED, not merely sent — the $50 that could never fire

Found and fixed 7 Sept 2026. Nobody had spotted it, and it is the same
silent-permanent-loss shape as the rest of this file.

The poller used to qualify any domain with `ps_signup_sent_at` set. But that
stamp only means PartnerStack answered 200, and `/conversion/xid` answers 200
with an empty body — a conversion that created nothing is indistinguishable
from a real one until the read-back sweep looks. So:

1. Conversion 200s, creating nothing. `ps_signup_sent_at` is stamped.
2. An AE ticks the box. The poller qualifies the domain and stamps
   `ps_qualified_sent_at`. The action lands against a customer that does not
   exist.
3. The read-back sweep gets its definitive 404 and releases
   `ps_signup_sent_at` — correctly, so the domain can convert again.
4. `ps_qualified_sent_at` is **still stamped**. `leads_ps_qualified_once_idx`
   is once per domain forever, the poller filters on
   `ps_qualified_sent_at IS NULL`, and nothing anywhere releases a
   qualification claim that succeeded.

The domain re-converts on the next lead and can never be qualified again. $50
gone, no error, no red chip, no line in any log.

The fix is one clause: the poller requires `ps_signup_verified_at IS NOT NULL`.
**It costs nothing in practice** — the grace period is 15 minutes and no demo
happens within 15 minutes of the form submit, so a real qualification never
waits on it.

It is airtight only because a verified row can never become a phantom
afterwards, and that rests on two properties of the read-back sweep, both now
asserted by tests: it only ever **sets** `ps_signup_verified_at`, never clears
it, and it only ever reads rows where that stamp is still NULL.

**The new filter is not allowed to drop a demo silently**, which would be this
integration's recurring bug pointed at its own fix. A domain that is ticked and
converted but still unverified **past twice the grace period** logs
`⛔ Ticked demo CANNOT be qualified` and goes through `recordFailure`, so it
reaches the PartnerStack health row and, on a streak, Slack. Under that
threshold nothing is said, because that is the grace period working rather than
anything being wrong. A domain with no conversion at all is skipped entirely:
that is almost always a non-partner Opportunity that happens to carry a ticked
box.

**`Qualified_Demo__c`** is a checkbox on Opportunity, default unchecked, visible
and editable for AE / SDR / SDR Manager / System Administrator / Minimum Access
– API Only Integrations. If it is missing or invisible to the API user, the SOQL
fails, logs `[SF] Qualified-demo query failed`, and returns `[]` — the poller
degrades to a no-op rather than breaking anything.

**The domain is the join.** It is the only identifier both systems share:
PartnerStack knows the customer by the `customer_key` we sent at signup, which
came from the lead's website.

### Unticking `Qualified_Demo__c` does NOTHING. The commission stands

Asked and confirmed 7 Sept 2026, after an AE unticked the box on the
`hello.com` Opportunity. **Nothing in this system reacts to an untick, by
design and in four independent places**, so a partner who has been paid stays
paid:

1. The poll only ever looks at rows with `ps_qualified_sent_at IS NULL`. Once
   that stamp exists the domain is invisible to it, forever.
2. `sendQualificationForDomain` re-checks with a `NOT EXISTS` on the same
   column before it claims.
3. `leads_ps_qualified_once_idx` is a UNIQUE PARTIAL index on
   `ps_customer_key WHERE ps_qualified_sent_at IS NOT NULL`. Even if 1 and 2
   were both wrong, the database refuses the second stamp.
4. There is no reversal call anywhere in this repo. `sendAction` is only ever
   invoked with `value: 1`; nothing sends a negative, a void or a delete.

So the money is one-shot: **the claim is permanent, PartnerStack keeps the
commission, and we never ask for it back.** Anyone who wants a commission
reversed has to do it in the PartnerStack UI — this service cannot.

**Re-ticking it does not fire a second $50 either, and it does not "retry"
anything.** That matters because it is the first thing an AE will try. Untick,
re-tick, wait: nothing happens, no log line, no error. It is a silent no-op and
it is meant to be.

**The one thing an untick DOES move** is
`partner_domain_sf_state.sf_state`, which flips `ticked` back to
`exists_unticked` at the next refresh — because that column is a snapshot of
the current checkbox, not a record of an event. That is the whole reason the
untick was visible at all, and it is what produced two display bugs on 7 Sept:
the domain read "waiting on an AE" on the per-domain table when nothing can
ever fire for it again, and the funnel's "Qualified Demo ticked" went *down*
while every other stage stayed put. Everything else in this integration keys
off an immutable stamp; `sf_state` is the exception, and any number built on it
can move backwards.

**Both were fixed in PR 22, and the fix was to stop reading the snapshot where
a fact was wanted.** `partner_domain_sf_state` now also carries
`first_ticked_at` and `first_opportunity_at`, stamped once and never cleared:

- **The funnel's `ticked` and `opportunity` stages read those**, ORed with
  `leads.ps_qualified_sent_at` — which is the stronger and *older* evidence,
  since a qualification can only ever have fired because the poller saw the box
  ticked, and it covers domains ticked before the columns existed (hello.com
  among them). They are deliberately **not** backfilled from
  `ps_qualified_sent_at`: those columns mean "we observed this", and writing an
  inferred timestamp into an observational column is how a reconstruction gets
  read as a measurement later.
- **The payment tail of the funnel now nests by construction.** A `ticked`
  stamp implies an `opportunity` stamp (the refresh writes the latter for both
  Opportunity states) and `ps_qualified_sent_at` implies both, so
  Opportunity ≥ ticked ≥ $50 always holds. Only that tail — the earlier
  absolute stages genuinely do not nest, since a domain can book without ever
  converting, which is why the cumulative column is kept beside them.
- **The per-domain row says what actually happened**: "unticked in Salesforce
  — the $50 already fired, nothing more can", instead of "waiting on an AE".
- **`sf_state` itself still moves in both directions, and should.** It is the
  answer to "what does Salesforce say right now", which is a real question and
  the reason the untick was noticed at all.

**A third thing was found while fixing those two.** The `exists_unticked` chip
row rendered only two sub-counts — waiting-on-an-AE and no-conversion-sent — so
a domain that was unticked *after* payment fell out of both and rendered **no
chip at all**, while still counting in `bySfState.exists_unticked`. The number
and the chips disagreed, and the chips are what anyone reads. There are now
three sub-counts that **partition** the state, and a test asserts they sum to
it, so a fourth shape cannot vanish the same way.


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

**A loss must not name a domain that already got paid.** "Booked, no
Opportunity" means nobody can tick the box so the money cannot move — and it
read `sf_state = 'no_opportunity'` alone, so deleting an Opportunity in
Salesforce, or letting one age past `PS_GAP_SF_LOOKBACK_D`, turned a domain
whose $50 landed weeks ago into a red loss. It now excludes
`ps_qualified_sent_at IS NOT NULL`. Deliberately **not** excluded on
`first_opportunity_at`: a domain that had an Opportunity, was never paid, and
no longer has one is a genuine leak and must stay in the count.

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

6. **A rate whose denominator was a `LIMIT`** rather than a population, in
   an org where 99.5% of the sampled table is a different kind of record.
   Reported as "enrichment reaches 0% of Salesforce" for what was actually
   46.5%. See the analysis note under the `LIMIT` rule below — the first
   instance of this class found in *analysis* rather than in shipped code, and
   it cost an evening on a bug that did not exist.

7. **`findQualifiedDemoOpportunities` was `LIMIT 200`**, unpaginated, with no
   completeness check and no `ORDER BY`, and it returned `[]` on every failure
   so "no AE has ticked anything" and "Salesforce did not answer" were the same
   value. Found 7 Sept 2026 in a sweep, before it could fire. Past 200 ticked
   Opportunities org-wide an affiliate is silently never paid — and the trigger
   is telling AEs the checkbox exists, which is the point of the field. **Third
   instance of the `LIMIT` shape in shipped code, fourth counting the analysis
   one.** Fixed the same day.

8. **A qualification could fire against an unverified conversion**, burning the
   once-per-domain claim forever when the read-back later 404'd. Not a
   completeness signal dropped but the same family: `ps_signup_sent_at` was
   treated as proof of a customer when it only ever meant "PartnerStack
   answered 200 with an empty body". Found and fixed 7 Sept 2026, never fired.
   Full walk-through under the poller section.

## When a fix rests on a property nothing enforces, assert the PROPERTY

Added 7 Sept 2026, out of the C8 fix, and it is a different rule from the ones
below it.

Requiring `ps_signup_verified_at` before the $50 can fire only closes the hole
if **a verified row can never become a phantom afterwards**. That was true, and
nothing anywhere enforced it — it was true because of two incidental properties
of the read-back sweep: it only ever *sets* the verification stamp, and it only
ever reads rows where that stamp is still NULL. Either could have been changed
by someone tidying that function, with no test failing and no comment saying
what depended on it. The fix would have gone on looking correct while the loss
it prevents came back.

So a test now asserts both properties, not just the clause that relies on them.

**The general shape:** when you write a guard, say out loud what has to stay
true for it to work. If that thing is enforced somewhere — a UNIQUE index, a
NOT NULL, a type — the guard is safe and you are done. If it is merely *true
today*, it is a load-bearing accident, and the assertion belongs on the
property rather than on your guard. A test that pins the guard alone proves the
line is still there; it proves nothing about whether the line still does
anything.

Worked examples already in this repo, for calibration:

- `leads_ps_signup_once_idx` and `leads_ps_qualified_once_idx` — once-per-domain
  is enforced by the DATABASE, so the application code above them is allowed to
  be simple. That is the good case.
- The claim-first ordering — nothing enforces it, so
  `tests/test-partnerstack.js` asserts the claim's character offset is *before*
  the send's, in both the conversion and the qualification.
- The never-NULL bind columns in `syncToAWS` — `tests/test-batch2.js` derives
  the column list from the bind site and asserts structurally that none of them
  is guarded by a no-op `COALESCE`, rather than pinning the four clauses that
  exist today.
- Eligibility running after `res.json()` and never being awaited — a property of
  call order, asserted directly.

### A crashing suite reports as CAUGHT. The measurement was wrong, not the test

Found 7 Sept 2026 while mutation-testing PR 23, and it is a **measurement** bug
rather than a test bug, which makes it worse: it silently inflated confidence in
every earlier mutation run in this project.

Every mutation result here has been measured by counting `✗` lines in a suite's
output. A suite that **crashes** prints a stack trace, **zero `✗` lines**, and
exits 1. To a mark-counter that is indistinguishable from a clean run; to an
exit-code checker it is indistinguishable from a caught mutation. It is
neither — it is an **unmeasured result**.

It surfaced on three mutations of the new reader suite that looked like
survivors and were really crashes on `out.records[259].id`, after a broken
reader returned an empty list.

**Any earlier mutation run in this project that happened to crash was recorded
as caught.** There is no way to tell retrospectively which ones did, so the
right reading is that mutation confidence before 7 Sept 2026 is weaker than it
was reported to be — not wrong, but not evidence either.

#### The exposure across the six suites, measured rather than assumed

Each suite was run against a mutation that breaks a declaration it depends on:

| Suite | Before the fix |
|---|---|
| `test-batch1.js` | **crashed**, 0 markers, no summary |
| `test-batch2.js` | **crashed**, 0 markers, no summary |
| `test-partnerstack.js` | **crashed**, 0 markers, no summary |
| `test-ads-parity.js` | reported 1 marker — but its assertion total **collapsed from 159 to 13** |
| `test-batch-a.js` | not demonstrated; the probe was outside its remit |
| `test-sf-readers.js` | reports correctly (it has the scenario wrapper) |

So **three of six** had the exposure outright, and `test-ads-parity.js`
exhibited a *third* mode nobody was looking for: a suite that completes, prints
a normal summary, counts as caught — and silently ran 8% of itself. Counting
markers cannot see that either.

#### The three modes, and the rule

1. **Crash** — no summary, no markers. Unmeasured.
2. **Partial run** — a summary, some markers, far fewer assertions than the
   baseline. Also unmeasured, and it looks completely normal.
3. **Reports** — a summary, markers, and the assertion total intact.

**A mutation counts as CAUGHT only when the suite completed AND failed AND ran
its usual number of assertions.** `failed > 0` on its own is not the test;
`passed + failed == baseline` is half of it.

#### What was changed

`tests/crash-reporter.js`, required first by all six suites, turns any escape —
a synchronous throw at module load or an unhandled rejection in an async suite
— into a `✗` line **and** an explicit `SUITE DID NOT COMPLETE` marker. It
deliberately prints no `passed:`/`failed:` totals, because a fabricated total
would be the same class of lie as the one it fixes. Verified by re-running the
three crashing probes: all three now emit both signals.

The partial-run mode cannot be fixed generically — only measured. Hence the
rule above.

### Verified-as-rendered and verified-as-computed are TWO assertions

Added 7 Sept 2026 out of PR 22, and it is the sharp edge of the rule above.

The C5 fix needed a third sub-count on the `exists_unticked` chip row. The
first version of its test drove the real client and asserted the chip appears
when the server sends a count — a proper rendered test, of the kind the
corollary below demands. Then a mutation replaced the server's whole
computation with `const sfUntickedAfterPaid = 0`, and **the entire suite stayed
green.** In production that chip would have vanished, the sub-counts would have
stopped summing to the state they partition, and the number and the chips would
have disagreed again — the exact bug the fix was for.

Because the rendered test proves the *renderer* is wired. It says nothing about
whether anything upstream produces a value to render. The old corollary catches
"computed and never shown". This is the mirror of it: **shown, and never
computed.** Same class, opposite direction, and a suite can be fully green with
only one of the two assertions present.

So for any number that reaches a screen, assert both ends:

- the **server derives it** — the predicate or expression itself, scoped to the
  function, not a substring search over the file;
- the **client renders it** — through the real renderer, reading the cell.

And where several numbers are meant to account for a whole, assert the
**partition**: that they sum to the thing they split. That is the assertion that
would have caught C5 from either end, because a bucket falling out of every
sub-count breaks the sum whichever side the bug is on.

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

**All three paginated Salesforce readers now follow the same shape**, and
`findQualifiedDemoOpportunities` was the last one to get it (7 Sept 2026): no
`LIMIT`, follow `nextRecordsUrl`, bound with `SF_MAX_PAGES`, check
`records.length` against `totalSize`, and return `{ ok, records }` so a caller
can never read a failed answer as an empty one. If a fourth reader is added, it
starts from that shape rather than arriving at it after an incident.

**And it has now bitten us in ANALYSIS as well as in code.** On 4 Sept the
Apollo enrichment ticket recorded "Salesforce Leads, last 7 days: 200 leads, 0
with enrichment (0%)" and read it as a months-long silent outage. It was a
measurement error of exactly this shape: the denominator was whatever a
`LIMIT 200` returned over the whole `Lead` object, with **no `LeadSource`
filter**. This org's `Lead` table is ~99.5% outbound list imports — `SG01P`,
`NG01P`, `E004P` and about eighty other campaign codes, over 100,000 rows a
month — which never touched Apollo and correctly hold no enrichment. Our form's
leads are `LeadSource = 'Website'`, roughly 200 a week. Reproduced on 5 Sept: a
`LIMIT 200` over the last 7 days returns 169 `E004P` and **6** `Website`.
Measured properly, the same 7 days were 215 `Website` Leads with 100 enriched —
46.5%, matching the ~49% we hold. Nothing was broken and nothing had been for
the five months the field had existed.

**The rule: a denominator is a POPULATION, not whatever a `LIMIT` returned.**
Before believing a rate, say out loud what the denominator is a population
*of*, and check that the filter defining it is actually in the query. A sample
drawn from a mixed table measures the mixture, not the thing you asked about.
The tell here was available for free and nobody looked: 200 leads in 7 days is
not our form's volume, and the `enriched_title__c` count for the whole year was
one query away — it showed 2 in March rising to 393 in August, which is a
five-month ramp, not an outage.

The counter that would have made this unmissable now exists — see
`enrichmentCoverage()` and `GET /monitor/enrichment-coverage` in `index.js`,
rendered on the System Health tab. It compares what we hold against what
Salesforce received, over a population rather than a sample, and reports
UNAVAILABLE rather than zero when Salesforce cannot be read.

## The three stamps reach the mirror

`ps_signup_sent_at`, `ps_signup_verified_at` and `ps_qualified_sent_at` are in
`syncToAWS`'s column list with `COALESCE` clauses, and were NULL on
`gw_form_leads` for every row, permanently. Not a missing column — **call
order.** `syncToAWS` runs from `/partial` and `/submit`; all three stamps are
written after `res.json()` by a deferred job or a sweep, and nothing calls
`syncToAWS` for that session again. The bind value was always NULL.

So they get targeted writes, via `syncPartnerStackStampToAWS`, like the
booking, the partner identity and `hear_about_us` before them. Never
`syncToAWS` with a partial object: that upsert sets
`disqualified = EXCLUDED.disqualified` with no `COALESCE`.

Three things about it are deliberate and easy to get wrong:

- **Not `COALESCE`'d**, unlike `syncPartnerIdentityToAWS`. Two of the sites
  *release* a claim by writing NULL and the mirror has to be able to follow.
- **The column name is allow-listed.** It is interpolated into SQL, and the
  allow-list is what makes that safe — not the fact that today's callers are
  all internal.
- **Awaited**, so a release can never overtake the claim it follows. Both are
  promises against the same row and only order decides what the mirror keeps.

**The three CLAIM writes are deliberately NOT mirrored.** A claim is a
Railway-internal lock taken *before* the HTTP call, not a fact about
PartnerStack, and mirroring it would put a WAN write in front of the send. That
leaves a window — a process dying between the claim and the success mirror —
and the read-back sweep is where it closes: reaching that branch means the
customer has been read back, so it mirrors **both** `ps_signup_sent_at` and
`ps_signup_verified_at`. Mirroring only the latter would leave the mirror
reading "verified but never sent", which the dialer would read as no
conversion at all.

That last defect was found by mutation-testing this very change: dropping
either mirror line went uncaught, and writing the assertion that caught it
showed the pair was wrong to begin with. A test now enumerates every Railway
stamp write from the source and requires a mirror of the same column before
the next send — the three that cross a send must each *be* a claim, checked,
not merely tolerated.

## A failed conversion is retried

The claim release carried the comment "so this domain can be retried rather
than silently lost", and **nothing retried it.** The conversion only ever fired
from `/submit`, so the only thing that would try again was another lead from
the same domain, which may never come. One timeout or one 429 at submit time
was one affiliate permanently unpaid, visible only as a red chip somebody had
to notice.

`runPartnerStackConversionRetry()` sweeps every 15 minutes, boot included.

**It FETCHES BEFORE IT SENDS, and that is the whole design.** A conversion that
reported failure may still have landed: the request can time out after
PartnerStack processed it, or a 5xx can come back from a proxy in front of a
successful write. Re-sending blind credits the affiliate twice, and PartnerStack
cannot undo a double credit — it is the one failure in this repo that costs real
money in the wrong direction.

| `fetchCustomer` says | What happens |
|---|---|
| **exists** | It DID land. Stamp sent + verified, clear the failure, **never send** |
| **definitive 404** | It genuinely did not. Re-claim and re-send |
| **anything else** | We could not tell. Leave the row completely alone |

**The backoff is derived from the read-back grace, not chosen separately.**
`PS_RETRY_BACKOFF_MIN = Math.max(30, PS_VERIFY_GRACE_MIN * 2)`. PartnerStack's
indexing lags a conversion by 2–6 minutes, so a retry sooner than that could
ask "does this exist?" about a conversion that landed and is not yet visible,
get a 404, and re-send it — precisely the double credit the fetch prevents.
Tying the two together means shortening one cannot silently break the other.

**Bounded, and the bound survives a crash.** Not every failure reason is
transient: a 400 on a bad payload fails identically forever. The attempt is
counted *before* it is made, so a crash mid-attempt cannot leave the count
untouched and the row retrying forever. After `PS_RETRY_MAX_ATTEMPTS` (5) the
row stops and stays red for a human, with its own alert saying the affiliate
is still owed and nothing else will retry — which is the correct end state, not
a failure of the sweep.

**Two guards on the selector that are not obvious:**

- **Domains where another lead already converted are excluded.**
  `ps_signup_sent_at` is once per DOMAIN, enforced by
  `leads_ps_signup_once_idx`, so stamping a second row for a converted domain
  would violate it — and the affiliate has already been credited, so nothing is
  owed. The lifecycle ladder excludes such a domain from `conversion_failed`
  for the same reason.
- **`disqualified IS NOT TRUE`.** CLAUDE.md is explicit that this is a guard
  rather than a flow property, and the retry is a new path into the same send.
  The cost of getting it wrong is paying an affiliate $50 for a B2C waitlist
  signup.

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
   up to **2 minutes** for `[PartnerStack] ✅ Qualification sent: <domain>`.
   If the conversion itself is less than ~15 minutes old, add the read-back
   grace to that — the qualification waits for `ps_signup_verified_at`, and
   nothing before it can fire.

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

- ~~**`syncToAWS` writing the three PartnerStack stamps.**~~ **CORRECTED
  7 Sept 2026 — this entry was wrong, and wrong in the worst direction: it
  recorded something as working-pending-proof that could never have happened.**
  No form submit could have proved it. `syncToAWS` runs from `/partial` and
  `/submit`, and all three stamps are written *after* `res.json()` by a
  deferred job or a sweep, so the bind value was always NULL and nothing calls
  `syncToAWS` for that session again. The columns were dead in that statement.
  Fixed in PR 23 with targeted writes — see "The three stamps reach the mirror"
  below.
- **The Slack alert on `conversion_failed` / `qualification_failed`.** Built in
  batch A and never fired. It cannot be triggered without a genuine failure and
  should not be faked. The first real one is the test. **Owner will fire one
  deliberately against a throwaway domain on the next deploy** (agreed 7 Sept
  2026), alongside watching the `[DB] Partner SF-state table ready` line for
  PR 22's migration. Lifting `alertOps` out of `index.js` to test it in
  process was considered and rejected: it drags in the cooldown map, the
  last-reported-state map, `sendSlack` and the block builders, and a brittle
  lift is worse than the gap it closes.
- **`Partner_Source__c` actually reaching Salesforce, and its permission
  failure.** Both PATCH paths are executed against a stubbed `fetch` in
  `tests/test-sf-readers.js`, and neither has run against the real org. The
  success path will work — the integration user is a System Administrator. The
  *failure* path cannot be exercised in production without deliberately
  removing field-level access, which is a thing to do on purpose, once, not to
  discover by accident later.
- **`hear_about_us_raw__c` on a real Lead.** One `CUSTOM_FIELD_MAP` entry and
  one payload key; the round-trip on the field itself was verified 4 Sept, but
  no Lead has been written since the code existed. The next partner submit that
  came in on a paid ad proves it.
- **The Partners tab rendered in a browser.** The SQL runs and the JSON is
  correct; nobody has looked at the page.
- ~~**`findQualifiedDemoOpportunities`'s pagination and its `ok: false`
  branch.**~~ **DONE 7 Sept 2026, PR 23** — `tests/test-sf-readers.js` executes
  both against a stubbed `fetch`: 260 records over three pages, the short-read
  and page-cap refusals, `http_503`, a thrown `ECONNRESET`, and an empty result
  that must stay a *success*. Still unexercised against a real org, which needs
  201 ticked Opportunities and is not worth manufacturing.
- **The held-back-domain path in the poll** (`⛔ Ticked demo CANNOT be
  qualified`). Needs a domain that is ticked and converted and stuck unverified
  for over 30 minutes, which has never happened.

Do not mark any of these done on the strength of the code existing.

## The two Salesforce writes — BUILT, PR 24 (7 Sept 2026)

Both fields existed and were verified with a live round-trip on 4 Sept; the
code was the missing half.

| Object | Field | Written from |
|---|---|---|
| Lead | `hear_about_us_raw__c` | `pushToSalesforce` at `/submit` — one entry in `CUSTOM_FIELD_MAP` plus the payload key |
| Opportunity | `Partner_Source__c` | `refreshPartnerDomainSfState`, which already holds the Opportunity id, the partner identity and the domain in one loop |

**`hear_about_us_raw__c`** puts the visitor's original answer in front of the
AE, not just on our dashboard. A partner-referred lead who arrived on a paid ad
is two real facts and `hear_about_us__c` can only hold one. Note that a map
entry alone is not enough — the payload has to carry the key, and it must carry
the **raw** value rather than `hearAboutUsFinal`, which is the
partner-overwritten one. Passing the wrong variable there would destroy the
exact fact the field exists to keep, and it is the mutation this pair is
tested against.

**`Partner_Source__c` is written from the poll, not mapped through Lead
conversion**, and that is the whole reason it lives there: the poll runs
strictly *after* sfopp has created the Opportunity, so there is no race and no
lead-conversion field mapping to configure. It also covers Opportunities
created **any** way — by hand, by an SDR, by a direct AE deal — which a Lead
field cannot, because none of our 29 custom Lead fields survive conversion.

The value comes from `partnerDisplayName()`, the same three-rung chain Slack,
the dashboard and `hear_about_us` use, so one partner cannot read four
different ways across four surfaces. When `runPartnerStackIdentity` later
upgrades a raw hex key to a real name, the next sweep sees a changed value and
corrects Salesforce too.

**It is idempotent on `sf_partner_source`**, a column added for exactly that.
Without it every partner Opportunity would be PATCHed every 15 minutes forever
— 2,880 pointless Salesforce writes a day at 30 domains, growing linearly. The
stamp is written **only on success**, so a transient failure retries; that is
safe because the PATCH is idempotent on the Salesforce side too.

### The permission failure is LOUD, and it is the one thing here never exercised

Everything else this service does on Opportunity is read-only, so a write
rejection has **never happened once**. It will keep succeeding while the
integration user is a full System Administrator — see
`docs/tickets/salesforce-integration-user-is-a-system-administrator.md` — and
start 403ing the day someone does the right thing and reduces that profile.

A silent failure would look **exactly** like a partner with no Opportunity,
which the Partners tab renders for real reasons. So:

- `updateOpportunityFields` returns a discriminated result, never a boolean,
  and never throws — its caller is a 15-minute sweep with nothing awaiting it.
- A 403, a 401, or any of `INSUFFICIENT_ACCESS` /
  `INVALID_FIELD_FOR_INSERT_UPDATE` / `FIELD_INTEGRITY_EXCEPTION` is classified
  as `permission` and alerted differently from a per-record failure. A
  permission problem affects **every** domain and needs Salesforce setup
  changed; a 404 affects one and may fix itself.
- The alert names the field-security trap: **creating a custom field through
  the Tooling API does not grant access to it.** Both fields created on 4 Sept
  came back `201` and were then invisible to the very user that created them
  until `FieldPermissions` rows were added. So a `400
  INVALID_FIELD_FOR_INSERT_UPDATE` means "no field-level access", not "no such
  field", and the two are indistinguishable from outside.
- A failure on one domain does not abort the loop for the others.

All of those paths are **executed** in `tests/test-sf-readers.js` against a
stubbed `fetch`, because they cannot be executed in production without
deliberately breaking permissions. That includes the 204-with-no-body success,
which a naive implementation reads as a failure.

**There is a kill switch, defaulting ON:** `PS_SF_OPP_WRITE=false` in the
Railway env stops the write without a deploy. Default ON because the write is
the point of the work; the switch exists because this is a new class of write
whose failure has never been seen.

## Related open tickets

- **`docs/tickets/apollo-enrichment-not-reaching-salesforce.md`** — the
  enrichment regression **does not exist**; investigated 5 Sept and closed.
  The "0 of 200" was a `LIMIT` with no `LeadSource` filter. The ticket has been
  re-pointed at what the investigation did find: form submissions with no
  Salesforce Lead at all, 6 people in 90 days, 3 of them booked.
- **`disqualified` read inconsistently** across six sites — see gap 3.
- **`docs/tickets/completed-is-not-submitted-a-form.md`** — `completed = true`
  does not mean the visitor submitted the form, and the AWS mirror disagrees
  with Railway about the flag for 14 people. Same class as gap 3. Audited
  5 Sept: nothing outward-facing is wrong today, and `backfill-sf.js` selects
  on the wrong column.
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

**`gw_form_leads.submitted_at` IS NOT A SUBMISSION TIME.** It is `new Date()`
at the moment `syncToAWS` runs, written only when the lead is completed — the
lead's real `submitted_at` is never sent to the mirror. On this table:

- its **presence** is meaningful and safe to filter on: "this row was synced
  while completed";
- its **value** is a clock reading from our sync, sometimes hours later.

Railway's `leads.submitted_at` is the real one. **The column name invites
exactly the wrong assumption**, which is why this entry is here rather than in
a code comment alone: anyone querying the warehouse — a BI tool, a spreadsheet,
the next person sizing a funnel — will read it as submission time and be
quietly wrong about when things happened. It is now also a
`COMMENT ON COLUMN` on the table itself, so a SQL client shows the warning
without anyone reading this file.

Renaming it is deliberately **not** done: the dialer and any saved warehouse
queries read this table from outside this repo, and a `RENAME COLUMN` breaks
every one of them at once and silently. See
`docs/tickets/completed-is-not-submitted-a-form.md` for the migration shape if
someone decides it is worth the coordination.

**A `COALESCE` against a value that can never be NULL is a NO-OP, and it
silently lets the incoming value win.** This is how 14 people ended up on the
mirror with `completed = false` while Railway had `true`, so the sdr-calling
dialer read form completers as step-1 drop-offs for up to three months. The
clause was `completed = COALESCE(EXCLUDED.completed, gw_form_leads.completed)`
while the bound value is `data.completed || false` — never null. Fixed 18 July
2026 by making it an OR; `loops_sent` carried the identical latent defect and
was fixed 5 Sept.

The rule for `syncToAWS`: **a flag whose bound value can never be NULL needs a
clause that cannot regress** — an OR for a monotonic boolean, `GREATEST` for a
monotonic number, or a bare `EXCLUDED` that someone signed off in writing
(which is what `disqualified` is). A plain `COALESCE` there does nothing at
all. `tests/test-batch2.js` now derives the never-NULL column list from the
bind site and asserts this structurally, so a future column added with the
same shape fails a test instead of misleading a dialer.

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
