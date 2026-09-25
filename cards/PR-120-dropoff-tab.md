# PR 120 — Dropoff tab: where inbound leads go, by week or month

Branch `feat/dropoff-tab`. Written 25 Sept 2026.

## Why

Swapnil asked twice in one Slack thread: 85 form fills, 51 booked, what
happened to the other 34, and then "a week on week report for last 12 weeks
with reasons of dropoff as rows and count in the columns, filter by source,
so we can see if we should be worried or not". A hand-written answer is
wrong the next morning.

## What it does and does not touch

**Read-only.** No blocking behaviour changes, no Meta event fires or stops
firing, no write path is touched. The only outbound thing added is a Slack
digest.

## Verified — actually run

- **Full bar green, run bare, read in full:** 12 suites, 4,402 assertions.
  `test-non-icp-routes.js` 488 -> 538.
- **The SQL was EXECUTED against the production Railway database** through a
  lift of the real `dropoffReport` out of `index.js`, not a copy. 20 checks:
  the ladder sums to the grand total and to every period total in both
  modes, 12 buckets at both grains, the partial flags, the source filter
  narrowing while the source LIST stays unfiltered, explicit ranges, and the
  grain/mode fallbacks. All passed.
- **The digest was executed** with `sendOpsSlack` stubbed, and its blocks
  read back and checked by eye. Nothing was posted to Slack.
- **Four mutations, all CAUGHT** via `measure.js --mutation`, against a
  committed baseline:
  1. mislabelling the step-2 row as step 1
  2. `partial: false` — the partial-period flag disabled
  3. an empty period reporting `0%` instead of `null`
  4. the grain whitelist removed, so a raw value reaches `date_trunc`

## Asserted only structurally

Nothing. Every assertion added here either drives the route over HTTP or
evaluates the dashboard JS and reads what was painted.

## Never executed

- **The Slack digest has never posted for real.** The repo's rule is that
  every alert path is fired once on purpose before launch. This one has been
  executed with the sender stubbed, which is one level short. It fires
  Mondays 09:00 ET; `DROPOFF_DIGEST_ENABLED=false` stops it. **Worth firing
  by hand before the first Monday**, the way `tools/fire-alert.js` does.
- **The tab has not been opened in a real browser.** The loader is evaluated
  in the suite's stubbed DOM and every painted number is read back, which is
  what caught the Model tab's "0 companies classified" class of bug — but a
  stubbed DOM is not Chrome.

## The mutation that survived first, and what changed because of it

Mutating the step-2 label in `DROPOFF_STAGES` **passed a full bar**. The tab
section renders a fixture, so it proves the painting and says nothing about
the server's own outcome list. A second block now drives the real route and
asserts its real payload — the ladder keys, the labels, the sums. That is
the same shape as this repo's oldest lesson: the assertion was one level
short of the thing worth caring about.

## The bug that executing the SQL found

`node-postgres` parses a `date` column into a JS `Date` in the **process's**
local timezone. `toISOString().slice(0,10)` therefore shifted every bucket a
day west of UTC: the report came back **completely empty on an IST laptop
and would have been correct on Railway**. Reading the code would not have
found it, and a test on Railway would not have either. The bucket is now
`to_char(..., 'YYYY-MM-DD')`, so there is nothing to parse.

## Numbers this depends on, and when they go stale

Measured 25 Sept 2026 over the 12 weeks to that date. They are in CLAUDE.md
and in the commit messages as measurements with a date, not as constants:

- 3,309 leads / 3,096 people; booking 65.8% / 69.1%
- 469 leads with no `utm_source` recorded by the form as paid (459 Facebook,
  9 Instagram, 1 Google); Meta 2,051 -> 2,519, Direct 991 -> 486
- 578 leads in the "left on step 2" bucket, all carrying `sell_to`, none
  carrying a phone, 0 carrying a `website_check_reason`
- booking lag: 99.6% of bookings land within an hour of step 1, which is
  what makes a period cohort safe to close

## Open, deliberately not done here

1. **A lead stopped on step 2 by a blocking website verdict leaves no
   trace.** `handleStep2Next` returns before `submitLead()`, so nothing is
   persisted. Some unknown slice of the 578 were turned back rather than
   leaving, and the table cannot say which. Fixing it is a change to both
   form files plus a Webflow re-pin.
2. **Internal test submissions are included**, per the standing house rule.
   The payload reports the count so it is visible; it is not subtracted.
3. **No CSV export.** The SDR list's export has a documented drift trap
   between its server and client column lists; adding a second one is worth
   doing deliberately, not as a side effect.
