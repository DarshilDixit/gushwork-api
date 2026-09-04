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

## Monitoring

**Overview → Partner gaps** — the alert. Two ways a referral silently never pays:

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

**3. `disqualified` is read inconsistently across six sites.** The stage ladder
uses `IS TRUE` / `IS NOT TRUE`; the dashboard metric counts, the recovery cron,
the recovery health check, the backlog count and the SDR list all use
`= true` / `= false`. A NULL flag lands in neither bucket and vanishes from
those queries entirely. `DEFAULT FALSE` makes it unlikely, not impossible.
Tracked separately; not fixed here.

**4. `ps_click_history` stores the base64 partner key.** The `gw_ps_clicks`
cookie carries the URL-param form, so each entry's `pk` is base64 while
`ps_partner_key` is decoded — the same partner, two spellings, sitting next to
each other in the dashboard panel. Confirmed by round-trip. Not fixed: the
choice between decoding on read here and fixing the site-wide script is open.

**5. The eligibility check has never run against production data.** It is built
and unit-tested but dormant. Turning the flag on for the first time should be
watched, not assumed.

**6. Nothing outstanding on the payload.** The `meta` mapping is verified —
see the custom fields section. Kept as a numbered entry so the list does not
renumber against older links.
