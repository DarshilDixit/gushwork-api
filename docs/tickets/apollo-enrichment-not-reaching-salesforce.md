# Apollo enrichment is not reaching Salesforce

**Status:** open, not investigated
**Found:** 4 Sept 2026, while checking whether custom Lead fields survive
conversion for the PartnerStack partner list view.
**Severity:** data loss, silent, long-running. Not urgent — nothing is broken
that was previously working *today* — but AEs have never seen this data.

## What was observed

```
our mirror (gw_form_leads), last 7 days : 301 leads, 148 with enrichment (49%)
Salesforce Leads,           last 7 days : 200 leads,   0 with enrichment (0%)
```

Zero of 200. Checked `enriched_title__c`, `enriched_company_size__c`,
`enriched_industry__c`, `enriched_seniority__c`.

We hold the data. These leads have a populated `enriched_title` in our database
and nothing in Salesforce:

- `laura.hart@nexusw2v.com` — "Director of Organic Logistics"
- `audra@audrainteriors.com` — "CEO / Interior Designer"
- `rbowers@icihomes.com` — "Content Marketing Specialist"

The fields **exist** on the Lead object — `enriched_title__c` queries fine and
returns null, rather than erroring. So this is not a missing-field problem.

It is also **not** a conversion problem. The values are absent on the Lead
itself, before any conversion happens. (Separately confirmed: none of our 29
custom Lead fields exist on the Opportunity object at all, so conversion drops
everything regardless — but that is a different issue.)

## START HERE — do not begin with a cold read of pushToSalesforce

**First query: `enriched_title__c` on Leads by created month, going back a
year.** The 7-day window above cannot distinguish "never worked" from "broke on
some date", and there is reason to believe Salesforce leads used to carry
enrichment.

```sql
-- SOQL
SELECT CALENDAR_YEAR(CreatedDate), CALENDAR_MONTH(CreatedDate), COUNT(Id)
FROM Lead
WHERE enriched_title__c != null AND CreatedDate = LAST_N_DAYS:365
GROUP BY CALENDAR_YEAR(CreatedDate), CALENDAR_MONTH(CreatedDate)
```

If there is a cutover month, that dates the regression and points at whatever
changed then — far faster than reading the push path cold. If it has been zero
for the whole year, then read the code.

## Then, if a code read is needed

- `/submit` reads `enrichment_data` by `session_id` and passes `enrich.*` into
  `pushToSalesforce`.
- `CUSTOM_FIELD_MAP` in `salesforce.js` maps `enriched_title` ->
  `enriched_title__c` and 12 similar fields.
- Worth checking: whether `enrichment_data` is populated at the moment
  `/submit` runs (Apollo fires on email blur, so usually but not always), and
  whether the booking webhooks — which also call `pushToSalesforce` — pass
  enrichment at all.

## Why nobody noticed

The field exists and reads empty rather than missing. An empty field looks like
a lead Apollo could not enrich, which is a normal and expected outcome roughly
half the time. There is no counter anywhere comparing what we hold against what
we sent.

This is the same shape as the four PartnerStack bugs in `../partnerstack.md`:
we had the evidence the answer was incomplete and rendered it as fact.
