# PR: a released company is never sent the automatic follow-up email

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

**The fix, two layers.**
1. **Old leads.** The release's UPDATE now also sets `loops_sent = TRUE`: a released company's old
leads are being handled by a person, so the automatic follow-up is marked done, in the same
statement that clears the block. The dashboard's confirm box, the Slack post and the command-line
output now say so in words.
2. **New drop-offs (added after the first version of this PR, at your request).** Our database
   showed no booking for the SPG contact (the direct-link booking never reached it), so if he
   opened the form again and dropped off, a NEW row would have been emailed two hours later. The
   follow-up cron now skips any lead whose company is released (`nonIcpExemptDomainFor`, the same
   one-minute-cached read as everywhere else), marks it done like any terminal outcome, and still
   posts the "Completed Form - Did Not Book" Slack alert on purpose, so a person sees them start
   again. This CHANGES WHO GETS AN AUTOMATIC EMAIL (not who is blocked, not Meta).

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
- Full bar, bare: 16 suites, 5791 assertions (baseline saved: non-icp 803 -> 807, routes 689 -> 699,
  monitor-next 476 -> 477).
- **The follow-up cron DRIVEN over HTTP with a recording mail transport** (the suite now stubs
  `nodemailer`): an ordinary drop-off IS emailed and counted; a released company is NOT emailed, its
  Slack post still fires, it is marked done with no attempt counted; released through the website side
  too; in one run only the released company is skipped; and the SAME lead with the company not released
  is emailed (the control).
- The changed UPDATE was run against production as `EXPLAIN` inside a read-only transaction that
  was rolled back: it parses, the column exists, it plans on the existing indexes, nothing changed.
- Executed against a recording database: the UPDATE carries `loops_sent = TRUE` and is still scoped to
  the one company. Over HTTP: the same statement from the release route, and the Slack post says the
  follow-up email was switched off. In the dashboard's stubbed DOM: the confirm box says so.
- A source assertion ties the fix to the two queries that matter: the cron AND the health check both
  require `loops_sent = false`, and share the same exclusions, so a released lead is out of both.
- Two mutations, both CAUGHT: the release stops setting `loops_sent` (the original bug, two suites), and
  the cron emails released companies again (4 assertions).

**NOT verified, never executed:**
- **No test drives a real release and THEN a cron tick against a database.** The release's UPDATE is
  asserted as text and was EXPLAINed; the cron's skip is driven with a stubbed database. The two meet
  only in production. That would need a database, which the repo's rules say not to stand up on a branch.
- Any other release before this deploys repeats the incident for that company's OLD leads (a fresh
  lead under two hours old is not picked up yet).

**Known gap:** the three emails and three Slack alerts already sent for SPG stand. Their rows are
now `loops_sent = true`, `followup_attempts = 1`, so nothing further will send.
