# Agency leads out of Salesforce and the SDR CSV, one agency list, our own tests out of Salesforce

Branch `feat/agency-salesforce-sdr`, from `main` after #132. Written 7 Oct
2026. **Not merged: waiting for your review. Nothing was sent anywhere.**

## What it does

1. **One agency list: `AGENCY_DOMAINS`**, default `flighted.co` and
   `uprawmedia.com`, in a new file, `agency-domains.js`.
   - **Who reads it:** Meta, the Google upload, Salesforce and the SDR list.
   - **The older lists still work:** `META_EXCLUDED_DOMAINS` and
     `GADS_EXCLUDED_DOMAINS` **add** to it, for their own system only.
   - **With nothing set in Railway, every list is exactly what's live
     today.** A test checks this for Meta and for Google.
   - **To add an agency later, add it to `AGENCY_DOMAINS` once.** Every list
     only grows; a value can't remove the defaults.
2. **Salesforce never receives an agency lead.** The check sits at the very
   top of `pushToSalesforce`, the one function every Salesforce Lead write
   goes through:
   - `/submit`;
   - the Salesforce retry sweep;
   - the Cal booking safety net;
   - the RevenueHero booking safety net;
   - `backfill-sf.js`.

   An agency lead gets no create and no update, and Salesforce isn't even
   asked whether it already has the Lead.
3. **Our own test submissions are skipped by the same check. Separate commit
   (`51c8f3f`), and its own decision.** It uses `isInternalSubmission`
   unchanged: our addresses, `gushwork.ai`, `test.com`, `example.com`, and the
   staging site. `/submit` already skipped these, but **the two booking safety
   nets did not**, so a test booking made straight from a calendar link still
   created a Salesforce Lead. In the last 90 days that happened 0 times, so
   this closes a gap rather than fixing a live leak.
4. **SDR list:** agency rows are **marked** on screen with an "agency" badge
   naming the domain, and **left out of the CSV export**. The tab's count says
   how many were left out, and so does an `X-Agency-Rows-Excluded` header on
   the CSV response.

## How a skip is recorded: neither synced nor failed

A skip is a **return**, never an error: `{ skipped, detail }`. Each place
handles it:

- **`/submit`:** stamps nothing. Not "synced", because Salesforce doesn't
  have it; not "failed", so the retry sweep never picks it up.
- **The retry sweep**, for a lead that failed *before* this change: still
  not stamped synced. It's taken off the retry queue (`sf_sync_retryable =
  false`, with "not sent: agency domain …" as the reason), so it can't loop
  and end in a "Retries exhausted" alert.
- **Both safety nets:** they only react to failures, so a skip already passes
  through quietly.
- **The backfill:** counts it as **skipped**, never FAILED. A **dry run says
  "skipped"**, not "would create", and spends no Salesforce call on the lead.

The log line names the matched domain, never the email address:
`[SF] ⏭ not sent to Salesforce — agency domain flighted.co (by email)`.

## Not changed, as asked

- **The AWS mirror** (`gw_form_leads`) still receives agency leads.
- **The dialer.** `sdr-calling` keeps its own deny list, which already has
  both agencies (by email domain only).
- **Existing Salesforce records** (4 agency Leads, 1 Opportunity) are left
  as they are. The booking routes can still add booking fields to an
  agency Lead that **already** exists; no new one is ever created.
- `INTERNAL_TEST_EMAILS`, `ELV_EXCLUDED_DOMAINS` and `isInternalSubmission`
  are unchanged.
- **No blocking and no Meta event changes.** Meta's agency list is unchanged
  unless `AGENCY_DOMAINS` is set.

## One limit

**The two booking safety nets know only an email.** A Cal or RevenueHero
booking carries no website, so there an agency is matched by email domain
only. I didn't add a website taken from enrichment, because that would also
start sending a website field to Salesforce. In the last 90 days, 0 of 7
safety-net bookings were agency.

## Dry run: last 90 days, read-only

`tools/agency-exclusion-dry-run.js`, run against the real leads table with
the real rule and the real `isInternalSubmission`. Counts only.

| | |
|---|---|
| Leads that would go to Salesforce | 2,861 |
| **Would now be skipped: agency** | **4** (all matched by email) |
| **Would now be skipped: ours** | **17** (already skipped at `/submit` for form leads) |
| Already in Salesforce, per our own sync record | 1 agency, 0 ours |
| Booking safety-net rows | 7, all normal |
| On the Salesforce retry queue now | 0 agency, 0 ours |
| Agency people on (roughly) the SDR list | 1 |

**Why "already in Salesforce" reads 1:** our own sync record only started
recently, so it undercounts. Salesforce itself showed **2** agency Leads
created in those 90 days.

## What I checked

- **Tests: all 16 suites pass, 5,405 assertions** (`measure.js --check`,
  read in full). The new suite, `test-agency-exclusions.js`, has 76:
  - **`/submit`, the Cal safety net and the RevenueHero safety net**, run in
    the real booted app. Agency by email and by website (email only for the
    webhooks), our own, and a normal lead. For each: what actually reached a
    stubbed Salesforce, and that a skip stamped neither synced nor failed.
  - **The retry sweep**, the real function from `index.js`, for an agency by
    email, an agency by website, and ours. For each: nothing reached
    Salesforce, nothing was stamped, it came off the queue, and no alert
    fired.
  - **The backfill**, in both dry and real runs, by email and by website, plus
    its after-push skip branch reached directly.
  - **The SDR list**: the JSON marks and the CSV leaves out, each by email and
    by website.
  - **The shared list**: the defaults, how it extends, that Meta's and
    Google's lists are unchanged when nothing is set, and that a lookalike
    domain isn't a match.
  - **`pushToSalesforce` itself**: a skip resolves, never throws, and makes
    zero Salesforce calls.
- **Mutation testing: 16 of 16 caught.** I broke the code 16 ways and
  watched the tests fail:
  - the check removed;
  - email not matched;
  - website not matched;
  - a substring match;
  - the defaults emptied;
  - `AGENCY_DOMAINS` replacing the defaults;
  - `/submit` stamping a skip as synced;
  - the retry sweep stamping a skip as synced;
  - the retry sweep leaving a skip on the queue;
  - the backfill without its pre-check;
  - the backfill counting a skip as FAILED;
  - our own submissions not passed to Salesforce;
  - the SDR CSV keeping agency rows;
  - SDR rows not marked;
  - Meta ignoring `AGENCY_DOMAINS`;
  - Google ignoring `AGENCY_DOMAINS`.

## Two mistakes along the way, both fixed

- **I committed a failing test (`97802b4`).** My command took the exit code
  of the output filter, not the test. The branch it tested worked; the
  assertion counted the wrong thing. The fix is the next commit (`a859aab`).
  I kept both rather than rewriting history, so the record shows what
  happened. The first mutation run was measured against that failing suite,
  so I threw it away and ran all 16 again on the fixed one.
- **Moving the check broke an older test.** `test-batch2.js` copies one
  region of `salesforce.js` by position, and my new code had landed inside
  that region. I moved the new code below `pushToSalesforce`, so that region
  is byte-for-byte what it was. Two other test fixes were needed:
  - its copy of the backfill now receives the skip rule it reads;
  - one pinned line now matches the new skip-aware `.then` on `/submit`, with
    the same intent: a success is still recorded.

## Not covered

- **The old dashboard** (`/monitor/classic`) doesn't show the agency badge.
  It reads the same route, so its CSV export **does** leave them out.
- **The dashboard's "why was Meta withheld" filter** still doesn't know
  about agencies. That's the follow-up from #132, still open.

I'll stop here until you say merge.
