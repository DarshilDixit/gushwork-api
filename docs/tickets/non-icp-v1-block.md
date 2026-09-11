# The non-ICP block (V1), and the two positions it reverses

**Date:** 11 September 2026
**Decided by:** Swapnil
**Built by:** this PR, behind `NON_ICP_BLOCK`, default off

This file exists because the change contradicts a written, dated, deliberate
document, and a contradiction that lives only in a Slack thread becomes a bug
report six weeks later. Anyone who reads `Non-ICP-flagging-rules` and then reads
`index.js` will find them disagreeing. This says which one is current and why.

---

## STATUS — read this first

**Live in production since 11 September 2026.** `NON_ICP_BLOCK=true` on the
`gushwork-api` Railway service. Form pinned in Webflow at
`4b419c392256754c105c86791d2a854dd5ca6fed`, **v5.11.0** / **v5.11.0-ads**, all
12 form pages verified on that SHA.

### What is live

| | |
|---|---|
| **Blocks** | A lead whose email domain **or** website host matches a national real-estate brokerage or insurance carrier brand. 41 entries in `NON_ICP_DOMAINS` (`index.js`). |
| **Where** | Detected and stamped at `/partial` (step 1). **Redirected at `/submit`** (step 2) to `/thank-you?attendeeName=…`. |
| **Meta** | All three upstream events suppressed — `StartTrial`, `Lead`, and `Schedule` on **all three** booking routes. |
| **Bookings** | A blocked lead's booking is **refused** at every booking route, including both webhook safety nets, with a critical alert. |
| **PartnerStack** | No conversion, no qualification. Guarded at `/submit`, at the retry sweep, and at the qualification poll. |
| **Salesforce** | Blocked leads are **not pushed**. |
| **Slack** | `🚫 Lead Blocked — Non-ICP` in the leads channel, carrying email, website, company, phone and the matched domain. |
| **Dashboard** | Overview card, dedicated **Blocked** tab with the full expandable panel, row marker and opt-in filter in All Leads. |
| **Off switch** | `NON_ICP_BLOCK=false` on Railway. No deploy needed. |

### Turning it off, fastest first

1. `railway variable set NON_ICP_BLOCK=false --service gushwork-api`
2. One bad domain → delete its entry from `NON_ICP_DOMAINS` and deploy. The
   Slack post names the matched domain precisely so this is a one-line fix.
3. Full revert → the columns and indexes are `IF NOT EXISTS` additions and can
   be left. The **form half rolls back separately**: re-pin Webflow to the
   previous SHA and republish. Reverting this repo does not revert the browser.

### Swapnil's three decisions, 11 September

1. **Redirect at step 2, not step 1.** Detection and Meta suppression stay at
   step 1; only the redirect moved, so a blocked lead completes the form and we
   capture website, company and phone. Four of the 84 matched leads are not
   agents at all and were identifiable *only* from step-2 fields.
2. **A matched EMAIL blocks regardless of the website.** OR logic. Reasoning:
   open the site and it is company-owned anyway. The Slack post prints the
   matched domain and the website together and calls out a mismatch in words,
   so the decision stays reviewable. **If that warning line starts appearing
   often, revisit this.**
3. **Scope is the BUSINESS TYPE, not "agents under national brands."** V1 can
   only reach brand domains, so it misses the ~70 independent agencies and
   realtors doing the same job. **When the LLM rules are scoped, write them
   against what the company *is* — and retire V1 rather than extending it.**
   Growing a domain list toward "every realtor" is the wrong shape and each
   addition is another chance at a `paycompass.com`.

### Deliberate choices, so nobody "fixes" them

- **No Salesforce push for blocked leads.** Decision, not an omission.
  `salesforce.js` is unchanged.
- **The unwarmed-verdict gap is shipped as is.** Step 2 decides from cache only
  — no network call at the moment of decision and no timeout to fall through.
  If neither blur resolved in time the lead sees a calendar until `/submit`
  answers; the booking routes then refuse the booking, so they cannot take a
  slot. **Do not pick a timeout from first principles.** The critical alert is
  the measurement: if it fires more than once or twice in a week, come back with
  real numbers.
- **A step-1-only block produces no Slack post.** Someone detected at step 1 who
  abandons appears on the Blocked tab and nowhere else — the post carries
  website, company and phone, none of which exist yet.
- **The show-rate evidence has a caveat that matters.** Matched leads attend
  47.1% against 66.4%, but that is **34 decided bookings** and `show_status='N'`
  mixes cancellations with true no-shows. Split for the matched group it is
  **11 cancelled / 7 genuine**, so the true no-show gap (20.6% vs 16.9%) is much
  narrower than the headline. **Quote the attendance gap, not the no-show gap.**

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

## The Partners tab was stale on `allstate.com` — RESOLVED 12 Sept

**Fixed. Kept because the general problem behind it is real and recurs.**

The row was cleared by hand (see below) and the tab now reads correctly:

| | Before | After |
|---|---|---|
| `totals.conversions` | 5 | **4** |
| `allstate.com` state | `converted` | **`skipped`** (`non_icp_blocked`) |
| `signup_verified` | true | **false** |
| **"Waiting on an AE"** | **1** | **0** |

`bySfState: exists_unticked` is unchanged and correct — there really is an
unticked Opportunity on that domain. It is simply no longer counted as
actionable.

**Three metrics queries had the same gap and were also fixed 12 Sept**:
`pendingPartials` (the "Pending recovery" card, which read 1 for this lead),
`noBooking` ("No booking yet"), and `partnerRevenueGaps`. See the CLAUDE.md
entry on the `disqualified` audit for why they were missed.

The re-check sweep (`runPartnerStackConversionRecheck`, 7-day cadence) now
catches this class automatically. What follows is the original analysis.


Checked 12 Sept after the customer was deleted from PartnerStack. The tab reads
**our stamps**, not PartnerStack, so three numbers are now wrong:

| Shown | Reality |
|---|---|
| `totals.conversions = 5` | 4. `allstate.com`'s customer was deleted. |
| `allstate.com  state=converted` | There is no conversion. `GET /v2/customers/allstate.com` → 404. |
| `allstate.com  signup_verified=true` | Verified at **19:23**, deleted at **20:0x**. True then, false now. |
| `allstate.com  sf=exists_unticked` → "waiting on an AE" | Reads as an action item. It is not one. |

### The general problem, not just this row

**`ps_signup_verified_at` is a once-only observation and nothing re-checks it.**
The verify sweep selects `WHERE ps_signup_verified_at IS NULL`, so the moment a
row verifies it is never looked at again. A customer deleted in the PartnerStack
UI *after* verification leaves our row permanently asserting a conversion that
does not exist, and **no sweep, alert or health row will ever notice.**

That is not specific to tonight — it applies to any hand-cleanup anyone ever
does in the PartnerStack UI, which is the only place a conversion *can* be
reversed. The one documented way to undo a mistake silently desynchronises the
dashboard from reality.

Not fixed here. If it is ever worth fixing, the cheap version is to let the
verify sweep re-check verified rows on a long cadence (daily, not 15-minutely)
and clear the stamp on a definitive 404 — but note that clearing it also makes
the row eligible for the retry sweep again, so it must be paired with the
`non_icp_blocked` guard that already exists and with something for the general
case.

### What it cannot do any more

**It cannot pay.** The qualification gate added in the same PR refuses
`allstate.com`: `brittanyvisin@allstate.com` carries no partner key, so the
domain is ambiguous and a tick on her Opportunity is refused rather than paid.
The tab entry is now cosmetically wrong rather than financially dangerous.

The row itself is untouched — clearing `ps_signup_sent_at` and
`ps_signup_verified_at` on it would make the tab correct, and is a production
data edit nobody has authorised.

---

## The `sdr-calling` dependency

**Moved to OPEN ITEMS #1 below**, which has the three exact WHERE clauses,
the reason it can wait, and what would make it urgent.

---

## OPEN ITEMS — each actionable cold

Nothing here is blocking. Each says what to change, where, and what would make
it urgent.

### 1. `sdr-calling` still dials blocked leads — three WHERE clauses

**Separate repo. Not touched.** Blocked leads look identical to normal
drop-offs in `gw_form_leads`: completed, no booking.

The mirror columns **already ship** from here (`non_icp_blocked`,
`non_icp_reason` on `gw_form_leads`, synced by `syncToAWS`), so that side needs
no further change. `sdr-calling` needs `AND non_icp_blocked IS NOT TRUE` in
**three** places, and all three so the campaign and the dashboard agree:

```
no-booking/sync-form-leads-campaign.js :: fetchFormLeads
lib/population.js                      :: noBookingPopulation
lib/workflow-stats.js                  :: formLeadsNoBookingRows
```

**Why it can wait:** only submit-time blocks are dialable — a step-1 block
collects no phone, and No Booking requires one. That is the "personal email +
brokerage website" shape: **12 of 84 historically, about 2 a month.** The
failure is an awkward call, not a lost lead or a wrong charge.

**Urgent if:** a blocked lead is actually dialled, or the rate rises above a
couple a month.

### 2. No health row for the warehouse dependency

A warehouse outage **silently disables the block** — `nonIcpVerdict` fails open
when the customer bypass cannot run, which is the correct direction and
completely invisible. Nothing alerts, and there is no System Health row.

Would go in `runHealthChecks` alongside the existing checks, and must follow the
house rule that health checks fail LOUD: if it cannot verify the block is
working it reports red, never green.

**Urgent if:** you want to know the block is *on* as opposed to merely *enabled*.

### 3. `non_icp_checked_at`

Only the two specified columns shipped. "When was this decided" is answerable
only from `updated_at`, which moves for unrelated reasons.

**Urgent if:** you need to date a verdict independently of the row's last write
— e.g. to tell a block made under one version of the list from one made under
another.

### 4. The `customer_deleted` webhook is the better primary

PartnerStack emits one (`POST /v2/webhooks` to subscribe; payload carries `key`,
`email`, `partner_key`). Event-driven would be instant and cost nothing per
domain.

What shipped instead is a **7-day poll** (`runPartnerStackConversionRecheck`),
deliberately, because the webhook needs a public unauthenticated endpoint,
signature verification and an out-of-repo subscription — and **delivery is
best-effort**, so a missed POST leaves exactly the permanent desync the poll
exists to prevent.

**Add the webhook as the fast path and keep the poll underneath it.** Not as a
replacement.

### 5. 1.8% of Opportunities have no contact email — the gate refuses them

The qualification gate is **email-only**: it fires only when the ticked
Opportunity's primary contact email matches a referred lead. Measured over 180
days of real Salesforce:

| | Count | No contact email |
|---|---|---|
| All Opportunities | 6,134 | 695 (11.3%) |
| **Form-sourced** | **1,833** | **33 (1.8%)** |

So roughly **one legitimate qualification in fifty-five** cannot be matched and
is refused. Accepted deliberately: a refusal goes through `recordFailure` to the
PartnerStack health row, so a missed payout is a work item somebody can send by
hand — and every fallback rule is a new way to pay the wrong partner on a shared
domain.

**A domain fallback was built and then removed.** Its evidence was a ticked
Opportunity with no contact email that turned out to be hand-made test data. Do
not reintroduce one without measuring first; a test asserts the gate has exactly
one `fire: true` path.

**Urgent if:** partner volume grows enough that 1.8% is a real number of missed
payouts, or the `opportunity_has_no_contact_email` refusal starts appearing in
the health row regularly.

### 6. PartnerStack domain-keying over-claims on shared domains

Full analysis in the FINDING section below. Short version: `customer_key` is our
choice, not PartnerStack's model — their docs say it is any unique identifier —
and keying on the domain means one partner referring one person at a large
employer claims the whole domain.

**Mostly closed by accident:** the six worst shared domains in our data
(`allstate.com` 7 companies, `farmersagent.com`, `kw.com`, `statefarm.com` 5
each, `exprealty.com`, `cbrealty.com`) are all on the block list now, and a
blocked lead cannot convert. The tail is in-ICP franchise and advisor networks —
`lpl.com`, `sandler.com`, `deleyorganizationglobelife.com`, 2–3 companies each.

**Any change to the key format is one-way.** PartnerStack holds the old keys
forever, so a re-key means new keys convert *alongside* old ones rather than
replacing them.

### 7. The LLM flagging layer — waiting on an API key from Punit

The six-rule flagger is what V1 is a stopgap for. It reads the company's website
with a model and covers all five rule-6 industries plus agencies, nonprofits,
publishers and sub-$1,000 tickets.

**Blocked on an API key from Punit.** Nothing to build here until that lands.

When it does, two things from tonight carry into its scoping:

- Scope the rules against **what the company is** (decision 3 above), not
  against a brand list.
- **Retire V1 rather than extending it.** A domain list that grows toward
  "every realtor" accumulates `paycompass.com`-shaped mistakes.

Note the doc's own status: rules 5 and 6 and the widened rule 1 currently flag
**nobody**, because the 3,165 classified domains were read under the old
questions. That is silence, not a clean bill of health — a fresh sweep over the
cached page text is needed before any count from it means anything.

---

## WEEK ONE — what to watch

Four things, in the order they will tell you something.

**1. The Blocked tab, daily.** Every row is somebody we turned away. The
question is not "how many" but **"does any of these look like a real
prospect?"** The expandable panel carries the website, company and enrichment
precisely so that is answerable in one glance. If one is wrong, the fix is one
line in `NON_ICP_DOMAINS`.

**2. The critical alert: "A blocked lead took a calendar slot."** This is the
measurement for the unwarmed-verdict gap. It means somebody got past the
client-side check, reached a calendar, and had their booking refused — and the
slot still exists in Cal/RevenueHero, so **a human has to cancel it**.

- Fires **once or twice in the week** → the gap is as small as predicted; leave
  it.
- Fires **more than that** → come back with the real count and we size a wait at
  the step-2 click against data rather than guessing.

**3. The count against the prediction: ~14 leads/month, ~10 bookings/month.**
Overview card, or the Blocked tab count.

- **Far above** → the list is catching more than brand-domain agents. Check the
  matched domains for a false positive.
- **Far below** → the block may not be reaching the browser. Check the console
  banner reads `v5.11.0`, and re-run the page sweep.
- **Zero after a few days** → suspect the Webflow pin or `NON_ICP_BLOCK`, not
  the absence of realtors.

**4. The email/website mismatch warning in Slack.** *"Their email is a brand
domain but their website is not."* One or two is the expected shape of a captive
agent with their own site. **A steady stream means decision 2 (OR logic) is
worth revisiting** — that is exactly the signal it was made visible for.

Also worth one look in week one: **the browser walkthrough has still never been
done.** Everything server-side is driven over real HTTP and both Slack paths
were fired for real, but no form file has been loaded in a real page. The
checklist is in `cards/pr-non-icp-v1-block.md` §12 step 7.

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
