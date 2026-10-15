# PR: releasing a company must switch off the automatic follow-up email

Branch `non-icp/release-no-followup-email`, from `main` at `775a13c`. 15 Oct 2026.
**Not merged: waiting for your review.** Fixes a bug in the release feature (#142/#143), which
was mine, and which emailed a real person three times.

**What happened.** specialtyprogramgroup.com was released from the Blocked tab at 7:19 PM IST
(note: "Might be a fit - He reached out on email, after 3 tries."). Releasing cleared
`non_icp_blocked` on its three old leads (3, 13 and 7 days old, completed, never booked). The
follow-up cron selects on `non_icp_blocked IS NOT TRUE`, `loops_sent = false` and no booking, so
it read them as three ordinary drop-offs. Result, all visible in #bot-inbound-leads and the logs:
- the health check called them **"3 waiting more than 5h"** ("Follow-ups are stuck in the queue");
- the cron posted **three "Completed Form - Did Not Book"** alerts for the SPG contact;
- and **emailed the contact three "you did not finish booking" messages**, hours after a person
  had replied to them by hand. They then booked through the direct link, which worked (the
  booking was not refused as blocked, because the release had cleared the block).
The three emails cannot be recalled.

**Why I missed it.** The #142/#143 cards listed what the release does NOT do (Salesforce, the
AWS mirror, Meta, messaging the person) and never asked the converse: which automations act on a
lead the moment it stops being blocked. I tested the release against the blocking paths only.

**The fix.** The release's UPDATE now also sets `loops_sent = TRUE`: a released company's old
leads are being handled by a person, so the automatic follow-up is marked done, in the same
statement that clears the block. The dashboard's confirm box, the Slack post and the command-line
output now say so in words.

**What else acts on a lead once it stops being blocked, checked rather than assumed**
(every place that selects on `non_icp_blocked IS NOT TRUE`, 12 of them):
- `/cron/send-partials`: the one that bit. Fixed here.
- `checkRecoveryHealth`: same population, so a released lead is now neither emailed nor "stuck".
- `runPartnerStackConversionRetry` and `runSalesforceRetrySweep`: only pick up rows already
  carrying a failure stamp / retry flag, which a blocked lead never had. Not affected.
- `/monitor/sdr`: the released leads now appear on the SDR list. Intended; an SDR may ring them.
- the rest are counters and dashboards (`/monitor/metrics`, `overviewReport`, `/monitor/leads`,
  `metaWithheldSql`, `partnerRevenueGaps`, `startPartnerStackSfStateRefresh`) and
  `runNonIcpBookedRecheck`, which respects the release.

**Not changed:** who is blocked, anything about the block itself, Meta, Salesforce.

**Verified, run and read:**
- Full bar, bare: 16 suites, 5783 assertions (baseline saved: non-icp 803 -> 807, routes 689 -> 691,
  monitor-next 476 -> 477).
- The changed UPDATE was run against production as `EXPLAIN` inside a read-only transaction that
  was rolled back: it parses, the column exists, it plans on the existing indexes, nothing changed.
- Executed against a recording database: the UPDATE carries `loops_sent = TRUE` and is still scoped to
  the one company. Over HTTP: the same statement from the release route, and the Slack post says the
  follow-up email was switched off. In the dashboard's stubbed DOM: the confirm box says so.
- A source assertion ties the fix to the two queries that matter: the cron AND the health check both
  require `loops_sent = false`, and share the same exclusions, so a released lead is out of both.
- The original bug put back as a mutation (the release stops setting `loops_sent`): CAUGHT by two suites.

**NOT verified, never executed:**
- **The cron itself was not run against a released lead.** The suites assert the two queries and the
  UPDATE as text and the statement was EXPLAINed; no test drives a release and then a cron tick.
  That would need a database, which the repo's rules say not to stand up on a branch.
- Any other release before this deploys repeats the incident for that company's OLD leads (a fresh
  lead under two hours old is not picked up yet).

**Known gap:** the three emails and three Slack alerts already sent for SPG stand. Their rows are
now `loops_sent = true`, `followup_attempts = 1`, so nothing further will send.
