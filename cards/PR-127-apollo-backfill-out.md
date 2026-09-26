# PR 127 — Apollo backfill: carry it out to the dialer and Salesforce; an honest health rate

Branch `tools/enrichment-sync-out`, from `main`. Written 26–27 Sept 2026.
**Not merged — waiting for your review.**

## What happened, in order

1. **You swapped the key.** `APOLLO_API_KEY` now ends **…VvTQ**. It was checked
   with Apollo's free health call, and with one real lookup (1 credit),
   which found a person.
2. **The backfill** (`tools/re-enrich-apollo.js --apply`, an existing tool,
   unchanged) re-ran every lookup Apollo had refused:
   - **325** people found;
   - **88** no match;
   - **17** copied from an earlier answer, for 0 credits;
   - about **326 credits** used;
   - no refusals.

   The dashboard and SDR list show it immediately, since both read our own
   tables.
3. **The mirror and Salesforce** had received those leads with blank
   enrichment at submit time. This PR's tool filled them. You ran both
   writes, because auto mode blocked mine:
   - **mirror** (`gw_form_leads`, the dialer's feed): **342 rows, 3,193
     fields**;
   - **Salesforce:** **140 Leads, 979 fields**, 0 failed.

   Both were re-read afterwards and showed nothing left to fill.
4. **Not reached, on purpose:**
   - **127 converted Leads**: Salesforce won't take updates to them; they
     are Contacts now;
   - **81 addresses with no Salesforce Lead**: never submitted, or blocked.

## LinkedIn: a second run, and a bug it exposed

**The first Salesforce run skipped LinkedIn on every Lead.** The org spells
the field `enriched_linkedIn__c` (capital I). My tool matched names exactly,
missed it, and reported it "not updateable". **I told you our integration
user lacked permission. That was wrong; it was my tool's bug.**

**Fixed, and you ran it a second time: 98 Leads written, 0 failed, LinkedIn
only.**

**⚠ But the fill-only check couldn't see that field either.** Salesforce
returns each record under the field's own spelling, so the tool read every
LinkedIn as blank. **Those 98 writes went ahead without seeing what was
there.** Any value a Lead already had was replaced. Salesforce keeps no
history on the field, so the old values can't be read back.

**What I checked instead, read-only:**
- Every LinkedIn Salesforce got from us came from our own records. For
  **218** of the people in scope, our records hold exactly **one** LinkedIn
  value ever, so what we wrote equals anything we'd sent before. **203**
  never had one.
- **One** person has had two different values. Their Lead is **converted**,
  so the tool skipped it and wrote nothing.
- What I **can't** rule out is a LinkedIn someone typed into Salesforce by
  hand. On an integration field that's unlikely, but it isn't proven.

**Fixed:** the tool now reads the record regardless of case too, and a test
covers a Lead that already holds a value under Salesforce's spelling. With
the fix, a dry run finds **0 left to fill**.

## What the PR changes

**`tools/sync-enrichment-out.js` (new, not mounted, run by hand):**
- **Fill-only:** writes a field only where the destination is blank, and
  re-checks that at write time.
- **Touches nothing else:** no other column, no new rows, no converted
  Leads.
- **Mirror:** a targeted `UPDATE … WHERE session_id`, never `syncToAWS`
  (whose upsert clears real disqualifications on the dialer's feed).
- **Salesforce:**
  - field names come from `salesforce.js`'s own map; types and lengths from
    Salesforce's describe;
  - writes go through `updateSFLead`, the same function the live service
    uses;
  - a partial read is refused.
- **Skips any session whose looked-up email differs from the lead's email.**
  See the review below.
- Dry run by default; `--apply`, `--mirror`, `--salesforce`.

**`checkApolloHealth` in `index.js`:**
- **The bug:** after the backfill, System Health read **"761% enriched —
  350 of 46 business-email leads"**. The top of the fraction counted lookups
  made in the last 24h; the bottom counted leads that *arrived* in the last
  24h. The backfill made 350 lookups in an hour for leads from months back.
- **The fix:** found and refused now count only the window's own leads,
  matched by session. A session has one enrichment row, so the rate can't
  pass 100%.
- **Run on the real table, read-only:** **green, 67%, 31 of 46**. The
  version live today reads 761% on the same data, until the backfill ages
  out of the 24-hour window.
- **The red line keeps counting by time.** "N refused in the last 24h" is a
  count of lookups, not a rate, so it still counts by when they happened.
  Keyed on the window's leads, it would read 0 while Apollo refused
  visitors who never pressed Next.

**Nothing here changes which leads are blocked or which Meta events fire.**
Enrichment feeds neither.

**Not changed:**
- the live `/enrich` route;
- `tools/re-enrich-apollo.js`;
- the form files.

## The review (independent, read-only), and what it found

| Finding | Verdict | What was done |
|---|---|---|
| The looked-up email and the lead's email can differ (a visitor changes their email during an outage; a refusal row is insert-only), so one person's details could land on another's Lead | **Real mechanism; didn't happen.** Checked read-only: **0** such sessions in the run | The tool now skips and counts them. Tested |
| Number fields could be mis-converted ("$10.5M" → 10.5) | **Didn't happen.** Salesforce's describe says all 13 fields are text | Kept: numbers go as numbers only if a field ever is numeric. Tested |
| The red line could say "0 refused" while Apollo refuses | **Real** | Counted by time again, as above. Tested |
| The branch's own tests failed at one commit | **Real, fixed before push** | The commit that fixed them is on the branch |
| **Found by running it, not by the review:** the fill-only check read Salesforce's record by our spelling of the field, so LinkedIn always looked blank | **Real.** It shaped the 98-Lead LinkedIn run; see above | Record keys read regardless of case. Tested |
| (Old, not this PR) A zero-credit copy by the re-enrich tool stamps a fresh "answered" row, which can hide the red "failing now" state | Noted, not changed | Worth a look in its own PR |

## Verified — actually run

- **The full bar, bare: 14 suites, 4,921 assertions, 0 failures.** Baseline
  saved.
- **New tests:**
  - **`test-apollo` (17):** the tool driven against a stubbed mirror and a
    stubbed Salesforce. Covers fill-only, the targeted update, converted
    Leads skipped, field-name case, a partial read refused, and the email
    guard.
  - **`test-batch-a` (3):** the new rate query, and that the red line reads
    the timed count.
- **Real data, read-only:**
  - the email-mismatch check (0);
  - the Salesforce field types;
  - the fixed health check (67%);
  - the read-backs after all three of your runs (nothing left).

## Mutations

**11 mutations, all 11 caught** (scratch copy, `measure.js --mutation`,
restored by copying back):
- **the health rate:** its numerator not keyed on the window's leads; the
  amber count likewise; the red line reading the window's refusals instead
  of the timed ones;
- **the mirror:** overwriting a value;
- **Salesforce:**
  - writing a converted Lead;
  - acting on a partial read;
  - overwriting a value;
  - field names matched by exact case;
  - the record read by our spelling;
  - numbers sent as text;
- **the email guard** removed.

## Process

- Merged nothing. The branch is pushed and the PR is open.
- **Auto mode blocked my writes** to the mirror and Salesforce. I stopped
  and gave you the commands rather than route around it.
