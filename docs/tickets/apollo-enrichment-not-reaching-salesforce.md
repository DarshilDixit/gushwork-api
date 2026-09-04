# Form submissions with no Salesforce Lead

**Status:** open, sized, not fixed
**Was:** "Apollo enrichment is not reaching Salesforce" — **that regression does
not exist.** See "The false alarm" below; it is kept in full because the way it
was measured is the reusable lesson, not a footnote.
**Found:** 4 Sept 2026 (as the false alarm), investigated and re-pointed
5 Sept 2026.
**Severity:** low volume, real consequence. 6 people in 90 days reached step 2
and have no Salesforce Lead; 3 of them booked a call, so an AE met someone the
CRM has no record of.
**Filename kept deliberately** even though the title changed — `partnerstack.md`
and the git history both point here, and a rename would break those links to
save nothing.

---

## The real finding

Joined the AWS mirror to Salesforce by email, deduped to people, over 90 days:

```
people in the mirror                                3229
reached /submit (a Salesforce Lead is owed)         2361   73.12%
  in Salesforce                                     2355   99.75%
  NOT in Salesforce  <-- the gap                       6    0.25%
never reached /submit (none owed)                    868   26.88%
  in Salesforce anyway (booking webhooks)             17    1.96%

enrichment, among people who reached /submit AND are in Salesforce (2355)
  we held it at submit, Salesforce has it           1452   61.66%
  we held it at submit, Salesforce has NOT            11    0.47%
  we held nothing (Apollo genuinely missed)          892   37.88%
  Salesforce has it, we do not                         0    0.00%
```

**The six with no Lead at all**, of which three booked:

```
pratyush@maino.ai              booked=false  2026-06-23
hari@pickyourtrail.com         booked=TRUE   2026-06-15
shikhar.verma@gushwork.ai      booked=TRUE   2026-07-08   <- our own address
kaavian@gtmguy.xyz             booked=false  2026-07-15
analytics@uprawmedia.com       booked=TRUE   2026-08-04
abhisheklokapur@gmail.com      booked=false  2026-07-13
```

So five real people and one of our own test addresses. Two booked calls sit
outside Salesforce entirely.

**The eleven where enrichment did not travel** (0.47%) are a second, smaller
thread; two of those are also `gushwork.ai` addresses. Worth a look only after
the six above.

### What is NOT the bug

- **Not a missing-Lead-for-drop-offs problem.** 868 people never reached
  `/submit`, and `pushToSalesforce` runs from `/submit`, so no Lead is owed for
  them. That is correct behaviour and it is 27% of everyone — big enough to
  swamp the real signal if you measure carelessly, which is exactly what the
  first pass of this join did.
- **`completed IS TRUE` is the wrong test for "a Lead is owed."** The Cal and
  RevenueHero safety-net branches set `completed = true` for someone who booked
  without touching the form. Using it put `aasnj@meta.com` — a booking-webhook
  row with `submitted_at = null` — in the gap column and called it a lost form
  submission. Use `submitted_at IS NOT NULL`. This is in CLAUDE.md's
  Definitions section and it caught me anyway.

## START HERE — do not begin with a cold read of pushToSalesforce

The rate is 0.25%, so this is not a broken path — it is a path that fails
occasionally and silently. A code read will not tell you why six specific
calls failed. Look for the evidence of the failures first:

1. **Railway logs for those six sessions.** `pushToSalesforce` failures are
   caught and logged, not thrown. Search the six addresses. If they logged an
   error, that names the cause in one step.
2. **Is it retried?** Check whether a failed push has any retry at all. If it
   does not, that is the fix: a lead lost to one HTTP blip is lost forever, and
   `backfill-sf.js` exists precisely because this has happened before at
   scale — see CLAUDE.md.
3. **The three booked ones are worth checking by hand.** Booking arrives by
   three routes (`/booking-confirmed`, and the Cal and RevenueHero webhooks) and
   all three call `pushToSalesforce`. If the booked failures cluster on one
   route, that is the bug and it is a third of the paths.
4. Only then read the push path.

`backfill-sf.js` re-syncs leads to Salesforce after exactly this kind of loss.
It is deliberately not mounted.

### DECISION 5 Sept 2026: the six were deliberately NOT backfilled

Not an oversight, and not a pending task — a call made with the numbers in
hand. Once the six were looked at properly, the case collapsed:

- `hari@pickyourtrail.com` and `shikhar.verma@gushwork.ai` **already exist as
  Contacts** with Accounts (created 26 Mar and 9 Jul), so they are not invisible
  to an AE. The missing record is a Lead, not the person.
- `shikhar.verma@gushwork.ai` is our own address, and `backfill-sf.js` skips
  `gushwork.ai` by design anyway.
- That leaves **one** genuinely absent-from-the-CRM booked prospect,
  `analytics@uprawmedia.com` — and its row carries
  `first_name = 'test', last_name = 'test'`, so a created Lead would read
  "test test" to whoever opened it.

Against that: five Salesforce writes, a temporary `/admin/backfill-sf` route, a
production deploy to mount it and a second to remove it. The cost is higher
than the value of one record of dubious quality.

**If the rate moves, revisit.** The counter on the System Health tab
("No Salesforce Lead") now makes that visible without anyone re-running this
investigation.

**What was done instead**, because it outlives the decision: `backfill-sf.js`
gained an `emails` allow-list and its selector was corrected from
`completed = true` to `submitted_at IS NOT NULL`. So the next person who
reaches for it can target named rows instead of replaying a whole window —
which for a mid-June start would have been ~2,180 people — and it no longer
replays booking-webhook rows as if they were form submissions. See
`completed-is-not-submitted-a-form.md`.

## Why nobody noticed

Nothing compares what we hold against what we sent. A lead absent from
Salesforce looks identical to a lead that never submitted, and 27% of people
genuinely never submit.

**That counter now exists.** `enrichmentCoverage()` in `index.js`, served by
`GET /monitor/enrichment-coverage` and rendered on the System Health tab as
"Enrichment reaching Salesforce". Four numbers: what we hold, what arrived,
held-but-never-arrived, and **no Salesforce Lead at all** — which is this
ticket's population, now on a screen. Its denominator is every person who
reached step 2 in the window, not a sample, and when Salesforce cannot be read
it shows `?` rather than 0.

---

## The false alarm, and why it is worth keeping

The original ticket said:

```
our mirror (gw_form_leads), last 7 days : 301 leads, 148 with enrichment (49%)
Salesforce Leads,           last 7 days : 200 leads,   0 with enrichment (0%)
```

Zero of 200, read as a months-long silent data loss that AEs had never seen.

**It was a measurement error.** Enrichment has reached Salesforce continuously
since the field shipped:

```
LeadSource = Website (our form). Salesforce Leads.
month      leads    title      size      industry   seniority
2026-03       3      2  67%     3 100%     3 100%     2  67%
2026-04     237    125  53%   164  69%   160  68%   125  53%
2026-05     576    316  55%   361  63%   345  60%   316  55%
2026-06     666    334  50%   384  58%   367  55%   334  50%
2026-07     889    390  44%   426  48%   413  46%   390  44%
2026-08     755    393  52%   460  61%   440  58%   393  52%
2026-09      95     40  42%    49  52%    49  52%    40  42%
```

42–55% on title every month, against the ~49% we hold. No cutover, no
regression, five months of it working.

**The cause of the wrong number: the denominator was a `LIMIT`, with no
`LeadSource` filter.** This org's `Lead` object is ~99.5% outbound list imports
— `SG01P`, `NG01P`, `E004P` and about eighty other campaign codes, over 100,000
rows a month — which never touched Apollo and correctly hold nothing. Our form's
leads are `LeadSource = 'Website'`, about 200 a week. Reproduced on 5 Sept:

```
SELECT Id, LeadSource, enriched_title__c FROM Lead
WHERE CreatedDate = LAST_N_DAYS:7 LIMIT 200
  -> 200 sampled, 169 of them E004P, 6 Website, 3 enriched
```

The same 7 days filtered properly: **215 Website Leads, 100 enriched, 46.5%.**

The two tells were both free and neither was taken. 200 Leads in 7 days is not
our form's volume — it is a page size. And the year-by-month count was one
query away.

**The three named examples were also wrong.** Two of the three have all four
fields populated in Salesforce, last modified *before* the ticket was written:

```
laura.hart@nexusw2v.com  title="Director of Organic Logistics"  size=10   industry="renewables & environment"  seniority=director
rbowers@icihomes.com     title="Content Marketing Specialist"   size=400  industry="real estate"               seniority=entry
```

The third, `audra@audrainteriors.com`, is genuinely absent from Salesforce —
and correctly so: `completed = false`, `submitted_at = null`. She reached
step 1 and dropped, so `/submit` never ran and no Lead was owed. She is not
evidence of the enrichment bug and not evidence of the real one either.

**The lesson, and it is now in `../partnerstack.md` beside the others:** a
denominator is a POPULATION, not whatever a `LIMIT` returned. This is the same
shape as the SOQL pagination bug in that doc — where `LIMIT 2000` made
`records.length === totalSize` true on 2,000 of 5,898 rows and the completeness
check passed on a truncated read. It has now cost us once in shipped code and
once in analysis.

## Still open, separately

Conversion drops everything regardless: none of our 29 custom Lead fields exist
on the Opportunity object, so nothing survives Lead conversion. Confirmed 4 Sept,
unrelated to the above, and not covered by this ticket.
