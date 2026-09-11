# The non-ICP block (V1), and the two positions it reverses

**Date:** 11 September 2026
**Decided by:** Swapnil
**Built by:** this PR, behind `NON_ICP_BLOCK`, default off

This file exists because the change contradicts a written, dated, deliberate
document, and a contradiction that lives only in a Slack thread becomes a bug
report six weeks later. Anyone who reads `Non-ICP-flagging-rules` and then reads
`index.js` will find them disagreeing. This says which one is current and why.

---

## What started it

AEs reported that State Farm insurance agents and real-estate agents were
booking demo slots and wasting their time. The ask was to stop them reaching the
calendar.

The measurement supports the complaint. Against 5,123 production leads
(20 Mar – 11 Sep 2026):

- **84 leads** matched a national brokerage or carrier brand domain; 75 people.
- They **book more than average**: 60 of 84, **71.4%**, against **64.1%** overall.
- They **attend much less**: of 45 such bookings in
  `gist.gtm_inbound_demo_bookings`, **47.1%** attended against **66.4%** for
  everyone else (16 of 34 decided, against 3,409 of 5,133).

Book more, turn up less. That is the shape of a wasted calendar.

**Caveat, stated up front:** 34 decided bookings is a small sample — the gap is
about 2.4 standard errors. And `show_status = 'N'` mixes cancellations with true
no-shows: for the matched group it splits 11 cancelled / 7 genuine no-show, so
the *true* no-show gap (20.6% vs 16.9%) is much narrower than the headline.
The attendance gap is the real finding; the no-show gap is thin.

---

## Reversal 1 — the flags were never meant to block

The Non-ICP doc says so twice, in its own words.

Header:

> Six rules, checked automatically from a company's website. A company is
> flagged if any one fires. Flags are used to suppress ad audiences and lower
> lead priority. **They do not block anyone from booking a demo.**

"How it runs", step 4 (Apply):

> Flagged companies are excluded from ad audiences first, because that is where
> suppression is cheapest and stops the spend at source. Then lead routing, so
> flagged leads go to a lighter-touch flow. Then reporting, so we can see which
> campaigns and creatives attract them. **Never at the booking form.**

**V1 blocks at the booking form.** That is the reversal. It was authorised
explicitly.

Worth carrying forward: the doc's own stated basis for rules 5 and 6 is
**retention and refund rate, not close rate** —

> It would not be surprising if the group they flag closes perfectly well. That
> is what they were added to prevent.

So the doc predicts these leads *buy*. We are now blocking people it expects to
convert, on the grounds that they waste AE time and do not stay. That may well
be the right trade, but it is a different argument from the one the doc makes,
and nobody had written it down.

---

## Reversal 2 — insurance was a deliberate keep, four days ago

This is not an oversight in rule 6. The doc has a section headed **"What we
deliberately do not flag"**, and one of its rows is *Insurance, mortgage and
lending*:

> All of it stays in, **down to the individual State Farm agent** and the solo
> loan officer. A rule used to take those out on the grounds that a licensed rep
> under a carrier has no budget authority; **it was dropped 2026-09-07**. Only
> the sub-$1,000 ticket (rule 4) excludes anyone here now. Real estate no longer
> belongs on this row: it is left under rule 6.

Rule 6's five industries are real estate (agents, brokerages and property
management), restaurants and food service, spas and salons, home services and
trades, and print and sign shops. **Insurance is not among them, on purpose.**

### The AE complaint is a predicted side effect of the 7 Sept change

The rule that was dropped on 7 September was precisely *"an individual licensed
rep working under a named carrier or brokerage"* — the State Farm agent case.
The doc's stated reason for dropping it:

> an individual realtor now fires the blacklist rule anyway

True for real estate, which rule 6 covers. **False for insurance, which it does
not.** The doc even sizes the gap:

> Dropping the licensed-rep rule un-flagged 61 companies, 29 of which the
> blacklist rule will pick straight back up once it can see the industry field.

61 minus 29 leaves roughly 32 companies that nothing catches. Insurance reps are
in that 32. The complaint arrived four days later.

**So V1 is closer to restoring the dropped rule than to inventing a new one** —
narrower, because it is brand domains rather than an LLM reading a website, and
it blocks rather than deprioritises.

---

## What V1 is, precisely

A hardcoded list of **national real-estate brokerage and insurance carrier brand
domains**, checked as a pure string comparison against the lead's email domain
and website host. No network call, no LLM, no model pass.

**In scope:** real estate brokerage brands, insurance carrier and captive-agent
brands.

**Deliberately out of scope, and each of these is a real population in our data:**

| Not blocked | Why | Roughly how many domains |
|---|---|---|
| Financial advisors — Edward Jones, LPL, Northwestern Mutual, Primerica, Cetera | The doc keeps them in ICP by name; nobody has reversed that | ~6 |
| Mortgage and lending | Same row of the doc | ~35 |
| Independent local insurance agencies | Not national brands. Espero, Beacon Light, Gary Rockwell… | ~30 |
| Independent local realtors and brokerages | Same | ~40 |
| Restaurants, spas/salons, home services, print/sign shops | The other four rule-6 industries. V1 cannot see them; **they fire Meta normally** | n/a |

V1 covers **two of the six** rules in the doc, and only the part of those two
that a brand domain can identify.

---

## The domain list, with evidence

Counts are distinct `leads` rows matching on email **or** website, 20 Mar –
11 Sep 2026. "Booked" is `booking_uid IS NOT NULL`.

### Real estate

| Domain | Leads | Booked | Note |
|---|---|---|---|
| `compass.com` | 12 | 7 | |
| `exprealty.com` | 10 | 8 | |
| `kw.com` | 9 | 7 | |
| `cbrealty.com` | 6 | 5 | **Coldwell Banker's agent domain.** `coldwellbanker.com` had 1 |
| `foxroach.com` + `bhhs*` prefix | 4 | 2 | BHHS regional suffixes cannot be enumerated; matched by prefix |
| `remax.net` | 3 | 3 | **The email domain RE/MAX agents use.** `remax.com` had 1, website-only |
| `serhant.com` | 3 | 2 | Named as flagged in the doc's own rule-6 examples |
| `elliman.com` | 2 | 1 | Named as flagged in the doc |
| `atproperties.com` | 2 | 2 | |
| `coldwellbanker.com` | 1 | 1 | |
| `remax.com` | 1 | 1 | |
| `century21.com`, `c21.com`, `sothebysrealty.com`, `kwrealty.com`, `howardhanna.com`, `randrealty.com`, `weichert.com`, `corcoran.com`, `realtyonegroup.com`, `lptrealty.com`, `mcgrawrealtors.com`, `bhhs.com` | 0 | 0 | Kept on purpose — see below |

### Insurance

| Domain | Leads | Booked | Note |
|---|---|---|---|
| `allstate.com` | 16 | 10 | |
| `farmersagent.com` | 9 | 6 | |
| `statefarm.com` | 6 | 6 | Every one booked |
| `farmers.com` | 6 | 6 | Website-only in every case |
| `newyorklife.com` | 5 | 4 | |
| `ft.newyorklife.com` | 5 | 4 | **The subdomain NYL agents use.** `nyl.com` has never appeared |
| `agents.farmers.com` | 4 | 4 | |
| `healthmarkets.com` | 3 | 2 | |
| `ushadvisors.com` | 3 | 3 | |
| `goldencare.com` | 2 | 2 | |
| `bankerslife.com` | 2 | 2 | |
| `geico.com` | 2 | 2 | |
| `healthmarketsjax.com` | 2 | 1 | Franchise sub-brand |
| `amfam.com` | 1 | 0 | |
| `agents.allstate.com`, `allstateagencies.com`, `farmersagency.com` | — | — | Seen as websites on leads already counted above |
| `goosehead.com`, `nyl.com` | 0 | 0 | Kept on purpose |

**Zero-hit entries are kept deliberately.** They cost one string comparison and
they are the ones most likely to arrive next. Treating "unused" as "remove" is
how a blocklist rots.

---

## Three implementation traps

**1. Substring matching is unsafe, and this is measured.** Against the same
5,123 leads: exact-or-subdomain caught **110**, substring caught **116**. Five of
the six extra were wrong:

| Wrongly caught by substring | Actually | Contains |
|---|---|---|
| `paycompass.com` | PayCompass, a payments company | `compass.com` |
| `charleslegalpl.com` | Charles Injury Law | `lpl.com` |
| `theimagecreatornm.com` | an image company | `nm.com` |
| `krevera.com` | Krevera | `era.com` |
| `ceterainvestors.com` | Cetera Investors (in ICP anyway) | `era.com` |

The sixth was a true catch exact matching misses: `zaljames@comcast.net`,
website `movewithkw.com`, company "Keller Williams Success Realty" — a real KW
agent on a personal-brand domain. **That is the residual gap no domain list
closes**, and it is what the LLM flagging is for.

All five negatives are pinned in `tests/test-non-icp.js` section 3.

**2. `partnerStackCustomerKey` collapses subdomains.** It ends in
`registrableDomain`, so `agents.farmers.com` → `farmers.com`. Matching only on
its output would make every subdomain entry permanently dead while looking live.
`nonIcpHostForms` keeps both forms. This was caught by the test, not by review.

**3. The block must be sticky.** `/partial` fires repeatedly through step 1, and
the "actually we're B2B" button calls `savePartial(1)` again. **74 of the 84
matched leads (88%) reached the calendar through that button** — they picked B2C
or Mixed, hit the disqualification step, and clicked through. All three upserts
use `IS TRUE OR EXCLUDED ... IS TRUE` so a block cannot be cleared that way.

For scale: **2,389 of 5,123 leads (46.6%)** take the clarification path, and they
book at 74–77% against 70.8% for B2B-direct. It is the main road through the
form, not a side door — which is exactly why the block must be independent of
`sell_to` and of `disqualified`.

---

## One of the 84 is a paying customer

`nedjacobs@allstate.com`, company "JACOBS FAMILY INSURANCE @Allstate", website
`jacobsfamilyinsurance.net` — **status `Active` in
`gist.customer_contract_terms`**. An Allstate captive agency that bought from us.

Without a bypass, the V1 email rule on `allstate.com` would have shut the door
on a renewal conversation. One of 52 matched domains. That is why
`nonIcpVerdict` checks the warehouse customer tables **before** blocking, and
why a failure to check does not block.

(A second realtor customer exists — `daniellegrassarealtor.com` — but V1 would
not catch her anyway: gmail address, own domain.)

---

## Confirmed decisions, 11 September 2026

Three questions were put to Swapnil during the build. All three are answered;
recorded here so they are not re-litigated as bugs.

### 1. The redirect happens at STEP 2, not step 1

**Detection and Meta suppression stay at step 1.** The moment a carrier or
brokerage email matches, `/partial` stamps `non_icp_blocked` and `StartTrial`
is suppressed. What does *not* happen at step 1 is the redirect.

**A blocked lead completes the whole form and is turned away at submit**, so we
capture their website, company and phone.

**Why:** four of the 84 matched leads in the 11 Sept sample are not agents at
all — a claims employee at a carrier, a retired address, a tax preparer using a
carrier address, and a dance instructor on a brokerage address. Every one of
those was identifiable *only* from the website and company typed at step 2.
Redirecting at step 1 would have hidden all four, and with them the evidence
that the list needs narrowing.

The cost is that a blocked lead spends another thirty seconds on a form that
will not book them. That was judged worth it against a blocklist nobody can
audit.

### 2. A matched EMAIL blocks regardless of the website — OR logic stays

This came up because the two can disagree: `mayra@menchacaagency.com` with
website `allstate.com`, and the mirror shape, `nedjacobs@allstate.com` with
website `jacobsfamilyinsurance.net`.

**Swapnil's reasoning: open the site and it is company-owned anyway.** A captive
agent's website is the carrier's brand whatever the domain says, so a matched
email is sufficient on its own.

**This is a confirmed decision, not an oversight.** It is also the decision most
likely to produce a wrong block, so it is made reviewable rather than invisible:
the Slack post prints the matched domain and the website **together**, and when
the email matched but the website did not it says so in words —

> ⚠️ *Their email is a brand domain but their website is not. Blocked on the
> email, by design — worth a look if this shape keeps appearing.*

If that line starts appearing often, the decision is worth revisiting. Until
then it is working as intended.

### 3. Scope is the BUSINESS TYPE, not "agents under national brands"

**No code change tonight.** Recorded because it scopes the LLM rules later.

The intent is to exclude **real estate and insurance as business types**, not
merely people employed by a national brand. V1 cannot express that: it matches
domains, so it reaches captive agents on `statefarm.com` and misses the
independent agency on `garyrockwellinsurance.com` doing exactly the same job.

That gap is **structural to a domain list**, not a bug in this one:

| Population | Reachable by V1? | Roughly, in our data |
|---|---|---|
| Captive agents on a brand domain | yes | 84 leads |
| Independent insurance agencies | **no** | ~30 domains |
| Independent realtors and brokerages | **no** | ~40 domains |
| Agent on a personal-brand domain (e.g. `movewithkw.com`) | **no** | seen, unquantified |

**So when the six-rule LLM flagger is scoped, rule 6's real-estate row and a new
insurance row should be written against what the company IS, in the doc's own
phrasing — "fires on what the company *is*, never on who it sells to" — and not
against a brand list.** V1 should then be retired rather than extended: growing
a domain list toward "every realtor" is the wrong shape, and each addition costs
another chance at a `paycompass.com`.

**This means V1 is deliberately partial and everyone should know it.** It stops
the specific complaint the AEs raised (national-brand agents), and it leaves
most of the intended population untouched.

---

## INCIDENT — a blocked lead fired a PartnerStack conversion, 11 Sept 2026

Found by Darshil during the browser walkthrough, within an hour of the flag
going on.

```
[/submit] 🚫 Lead blocked: agent@allstate.com
[PartnerStack] ✅ Conversion sent: allstate.com | xid=M7wnDScN0rrUYH
```

`StartTrial`, the Meta `Lead` event and the Salesforce push were all correctly
suppressed. `runPartnerStackSignup` was not, because it guards on
`leads.disqualified` and the block deliberately lives in `non_icp_blocked`.

### Why it was missed

The turn-3 reasoning for keeping the columns separate included: *"a blocked
lead can't book, so PartnerStack never applies."* **That was wrong, and it was
wrong even before the redirect moved to step 2** — the conversion fires from the
fire-and-forget tail of `/submit`, and a blocked lead has always called
`/submit` in order to be recorded. The argument was about bookings; the
conversion does not need one.

`runPartnerStackSignup` was even named in the original PR card as one of the
five `disqualified` consumers. It was listed and then not acted on, because the
list was used to justify *not* touching `disqualified` rather than to audit what
each consumer now missed.

### The generalisable lesson

**Introducing a second column that means "we rejected this lead" turns every
existing guard on the first one into half a guard.** The audit is not "did I
update the guard I was thinking about" — it is "which predicates on the old
column now answer only half the question". Three did:

| Consumer | What it did wrong | Cost |
|---|---|---|
| `runPartnerStackSignup` | sent a paid conversion | money |
| `/monitor/sdr` | listed blocked leads for SDRs to ring | wasted calls |
| `checkRecoveryHealth` | counted them permanently stuck | a red row nothing clears |

Two more were checked and deliberately left alone: the stage ladder and the
metrics counters still count blocked leads as leads, because they *are* leads
and the Blocked tab is their surface; and `slackPartial` is only reachable
through the recovery cron, which already excludes them.

`tests/test-non-icp.js` §10b now derives every `disqualified` predicate in
`index.js` and pins the count, so a new one cannot be added without somebody
deciding what it does about a blocked lead.

### What happened to the conversion — and what can be done about it

**Nothing in this repo can reverse it.** `docs/partnerstack.md`:

> There is no reversal call anywhere in this repo. `sendAction` is only ever
> invoked with `value: 1`; nothing sends a negative, a void or a delete. […]
> Anyone who wants a commission reversed has to do it in the PartnerStack UI —
> this service cannot.

**No money has moved.** A conversion creates a *customer record*; the $50 fires
on the *qualification*, which requires an AE to tick `Qualified_Demo__c` on a
Salesforce Opportunity for that domain. State at the time of writing:

```
agent@allstate.com | non_icp_blocked t | ps_customer_key allstate.com
ps_signup_sent_at  2026-09-11 18:59:55
ps_signup_verified_at  NULL      ps_qualified_sent_at  NULL
```

**But the payout path is real and is not closed by the code fix.** The blocked
lead was never pushed to Salesforce, so *its* Opportunity does not exist — yet
**three existing Opportunities already resolve to `allstate.com`**, from real
Allstate-agent leads that predate the block:

| Opportunity | Contact | `Qualified_Demo__c` |
|---|---|---|
| `006OX00000XvnKMYAZ` | tungle@allstate.com | false |
| `006OX00000cQQ5rYAG` | b.sheffield@allstate.com | false |
| `006OX00000cZiMrYAK` | dawnbeaulieu1@allstate.com | false |

The step-10 poller keys on **domain**, not on lead. If an AE ticks any of those
three, it will match `allstate.com`, find the stamped conversion, and fire the
qualification — **$50 to partner `785ec78e1ee4688`, for a lead we turned away.**

That is a live exposure and a decision for a human, not a code change. The
options, none of them taken here:

1. **Void it in the PartnerStack UI.** The only true reversal.
2. **Stamp `ps_qualified_sent_at` on the row** to make the qualification
   unreachable — the UNIQUE PARTIAL index and the `NOT EXISTS` check both read
   it. Effective, but it writes a false observational stamp, which is exactly
   what `docs/partnerstack.md` warns against for `first_ticked_at`.
3. **Null the `ps_customer_key` on that row** so the poller cannot match it.
   A data edit, but arguably honest: a blocked lead should never have held a
   customer key.
4. **Leave it and watch.** All three Opportunities are unticked today.

Whatever is chosen, `allstate.com` is now burned as a PartnerStack customer key
— one conversion per key, forever. Since `allstate.com` is on the block list
that costs nothing going forward.

---

## FINDING — domain-keying over-claims on shared corporate domains

**Investigated 12 Sept 2026 at Darshil's request. Nothing fixed; this is the
written-up finding.**

### The worry, and it is correct

> For a shared corporate domain like allstate.com — 54,000 people, thousands of
> independent agencies — one partner referring one agent might claim every lead
> from that domain, and any AE ticking any Opportunity on it pays them $50.

**That is exactly what the code does.** `sendQualificationForDomain` claims the
earliest converted lead on the domain
(`ORDER BY ps_signup_sent_at ASC LIMIT 1`) and sends
`{ target_type: 'customer', target_key: <domain> }`. PartnerStack then resolves
the partner **from that customer's existing attribution** — i.e. from whoever's
`xid` created it. Nothing in the path consults which *lead* was ticked.

### It is already instantiated, on `allstate.com`

| Lead | Partner? | Converted | Blocked | Booked |
|---|---|---|---|---|
| `brittanyvisin@allstate.com` | no | no | no | **yes — demo 11 Sept 20:00** |
| `agent@allstate.com` | yes (`785ec78e…`) | **yes** | yes | no |

Both carry `ps_customer_key = allstate.com`. **If an AE ticks Brittany's
Opportunity, the poller matches `allstate.com`, claims the converted row, and
pays a partner who had nothing to do with her.** She is a real lead with a real
demo; the converted row is the blocked test one.

### Answering the four questions

**1. Does `partnerStackCustomerKey` already handle this? No.** It lowercases,
strips scheme/www/path/port, collapses to the registrable domain, and rejects
IP literals and free-email domains. It has no concept of a shared domain —
`allstate.com` and a three-person agency's own domain are treated identically.

Worth noting: **the mechanism already exists and is scoped too narrowly.**
`if (isFreeEmailDomain(key)) return null;` *is* a shared-domain exclusion — it
just only covers consumer mailboxes. Shared *corporate* domains are the
unsolved half of the same idea.

**2. Is domain-keying PartnerStack's design, or ours? Ours.** Their docs are
explicit that `customer_key` is "a unique identifier for the customer, this is
the ID you will use to reference the customer in our API" — any format the
integrator chooses. PartnerStack enforces *uniqueness per key* and attaches the
partner to it; **choosing the domain as that key is our decision.** CLAUDE.md's
"keyed by DOMAIN, because that is the unit PartnerStack pays on" conflates the
two: the uniqueness is theirs, the choice of domain is ours.

**3. Has it happened in the data? Partly — and the integration is tiny.**

| | |
|---|---|
| Partner leads, all time | **6** |
| Distinct partners | **1** |
| Conversions sent | **5** |
| Qualifications sent | **2** |

Four of the five conversions are test domains (`test.com`, `google.ai`,
`hello.com`, `darshildixit.com`); the fifth is the accidental `allstate.com`.

- **No domain has ever had two different partners.** The partner-vs-partner
  double-claim has not happened.
- **The partner-vs-nobody claim has now happened once**, on `allstate.com`.

Measuring shared domains directly — domains used by more than one distinct
company name:

| Domain | Leads | Distinct companies | On the block list? |
|---|---|---|---|
| `allstate.com` | 17 | **7** | yes |
| `farmersagent.com` | 8 | 5 | yes |
| `kw.com` | 7 | 5 | yes |
| `statefarm.com` | 5 | 5 | yes |
| `exprealty.com` | 7 | 3 | yes |
| `cbrealty.com` | 6 | 3 | yes |
| `lpl.com` | 3 | 3 | **no — in ICP** |
| `sandler.com` | 3 | 3 | **no** |
| `deleyorganizationglobelife.com` | 3 | 2 | **no — in ICP** |

**The non-ICP block closes most of this by accident.** The six worst shared
domains are all now blocked, and a blocked lead cannot convert at all. What
remains is the in-ICP tail — franchise and advisor networks like `lpl.com` and
`deleyorganizationglobelife.com`, 2–3 companies each.

**4. Bug, design limitation, or working as intended?**

**A design limitation that is correct for the common case and wrong for shared
domains.** For an ordinary SMB the domain *is* the company, and domain-keying is
the thing that stops two spellings of one company paying an affiliate twice —
that is a real problem it really solves. It over-claims only where one domain
spans many independent buyers, which is precisely the franchise and captive-agent
shape.

So: not a bug in the sense of code doing something other than intended, and not
"working as intended" either — the intent was never tested against a 54,000-person
domain. **A limitation nobody had written down, now written down.**

### If it is ever worth fixing

Not tonight, and possibly never at this volume. The options, in increasing cost:

1. **A shared-domain deny-list**, alongside `isFreeEmailDomain` in
   `partnerStackCustomerKey` — a lead on one of those domains gets no customer
   key, so it never converts. Smallest change; reuses the existing mechanism.
   Costs the ability to ever convert a genuine referral from such a company.
2. **Key on domain + something narrowing** (company name, or the lead's own
   website where it differs from the email domain). Changes the key format, so
   it must never be applied retroactively — existing keys are already claimed
   in PartnerStack.
3. **Key per lead.** Removes over-claiming entirely and reintroduces exactly the
   double-payment the domain key exists to prevent. Not recommended.

**Any change to the key format is one-way**: PartnerStack holds the old keys
forever, so a re-key means new keys convert alongside old ones rather than
replacing them.

---

## The `sdr-calling` dependency

**Not urgent, but real, and it is not fixed by this PR.**

`sdr-calling`'s No Booking workflow selects leads from `gw_form_leads` that
completed and never booked — which is exactly what a submit-time block looks
like. Without a change there, an SDR rings somebody we turned away at the
calendar minutes earlier.

**What this PR does:** adds `non_icp_blocked` and `non_icp_reason` to
`gw_form_leads` and syncs them. Necessary, **not sufficient**.

**What `sdr-calling` must do:** add `AND non_icp_blocked IS NOT TRUE` to
`no-booking/sync-form-leads-campaign.js :: fetchFormLeads`, and to
`lib/population.js :: noBookingPopulation` and `lib/workflow-stats.js ::
formLeadsNoBookingRows` so the dashboard and the campaign agree.

**Before, alongside, or after?** **Alongside or after.** Not a blocker, and here
is the reasoning:

- A **step-1 block collects no phone** — phone is a step-2 field, and No Booking
  requires a non-empty phone. Those leads can never reach the dialer.
- Only **submit-time blocks** are dialable, and those are the "personal email +
  brokerage website" shape: **12 of 84 over six months, about 2 per month.**
- The failure mode is an awkward call, not a lost lead or a wrong charge.

Two per month of awkwardness is worth accepting to ship today. It should not be
left open for long.

---

## What would make this obsolete

The six-rule LLM flagger. It reads the company's website with a model and covers
all five rule-6 industries plus agencies, nonprofits, publishers and sub-$1,000
tickets. V1 is two of six rules, by brand domain, with no model pass.

The doc's own status note, as at 11 Sept:

> Rules 5 and 6, and the widened rule 1, currently flag nobody. […] The 3,165
> domains already classified were read under the old questions, so those fields
> are simply absent and the new rules return false on every one of them. That is
> silence, not a clean bill of health.

So the flagger cannot do this job today even where it is built. V1 is the
stopgap, and it should be retired — not extended — when the flagger can see the
industry field.
