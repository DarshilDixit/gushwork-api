# PR: Blocked tab: likely buyers across ALL blocked people

Branch `monitor/blocked-all-buyers`, from `main` at `fccdf96` (PR 147). 16 Oct 2026.
**Not merged: waiting for your review.**
**Changes who is BLOCKED: nothing. Changes Meta: nothing. Changes email: nothing.** It is a dashboard read, plus one new filter on a read route.

## What you asked for
"11 of 157 look like buyers" in the header, a filter on the main list (server side), the reason stated on the section itself, and for the released companies their tries and where they came from, with "how did you hear about us" as the main source.

## What changed
- **ONE role-hint function on the server** (`nonIcpRoleHint` in `index.js`). The browser copy is gone; it only displays `role_hint` and `role_reason`. The Slack post is unchanged.
- **Header:** "6 of 156 blocked people look like buyers", over every blocked person (our own tests left out), people not leads.
- **Needs a look** now has a third reason, a buyer-type role, so every likely buyer is in it (before, 7 of 11 were in neither of the other two reasons). The review feed returns totals and one list of every buyer's address.
- **Main list:** `All blocked / Likely buyers`. Likely buyers sends those addresses to `/monitor/leads?emails=` (one bound text[] parameter, lower-cased, at most 300, a bad address is a 400), so the list is filtered and paged by the server. No buyers shows an empty list and never asks the server with nothing. A failed switch shows the failure, never the other list under a pressed button.
- **Reason in words on every row**, in both lists, including why there is NO hint ("company under 20 staff", "no company size from Apollo", "owner level, not a corporate or marketing role").
- **Where they came from:** how they heard of us first (the form's own label), the ad source or referrer under it, campaign in the tooltip. Typed free text ("sadsad") is shown as typed and never used as the channel. Same cell in Needs a look and the released list; the date row in the main list shows the first line.
- **Released companies:** tries (submitted sessions), people, and where they came from.
- The review feed now reads the enrichment record behind the lead row, like the list does, so the two hints agree.

## FIX 1: this changes who gets "Likely buyer", on purpose, and it is why the number fell
A level with **no title** and a title at a company of **unknown size** both used to count as buyers. Neither does now. Unknown size usually means Apollo has no company record, which is a tiny agency. Measured on live data (read-only, 16 Oct): **buyers 11 -> 6** among 156 people. The six: Inland (VP of Leasing), Member Benefits (CMO), Anthem (Director), Weichert (Manager), Plum (Product Manager, Growth), USI (VP Benefits). Owners, founders and partners still get no hint.
Of 156 people, 73 have no title and 33 are agent-type.

## What I did NOT change
Blocking, the model layer, Meta, Salesforce, the dialer, the Slack post, who is released, the follow-up email cron. The hint blocks and releases nobody.

## Verified (run, not read)
- Full bar bare: 16 suites, **5899** assertions, 0 failed (5855 -> 5899; re-baselined, +14 non-ICP logic, +16 routes, +14 dashboard).
- The role-hint function is EXECUTED on real titles in `test-non-icp.js`; both routes are BOOTED in `test-non-icp-routes.js`; the dashboard JS is evaluated against payloads in `test-monitor-next.js`.
- **SQL executed** read-only against production (transaction, rolled back): the new review feed returned 156 people, 28 rows, 6 buyers; the released query ran.
- **Mutation:** 17 deliberate breakages, each against the whole bar. 16 caught on the first run. One SURVIVED (the guard that stops a failed switch showing the unfiltered list): my test started from an empty list, so it could not see it. Test fixed and the mutation re-run: caught.
- Layout in real Chrome (36 combinations, 360 to 1440, light and dark): clean except one finding below. The "Came from" labels broke mid-word ("Faceboo / k (Paid)") at 1280; fixed and re-checked.

## Needs you to know
- **Layout finding that is NOT from this PR:** the change-log table inside an opened row is 16 to 172px wider than its card at 360, 768 and 1024. It reproduces identically on `main` (checked with the same tool against `main`'s code), and only shows now because a lead with a change log is among the first eight rows. Not fixed here.
- **Not verified:** I did not click the new buttons in a real browser, and did not eyeball dark mode (layout checks only). The preview serves the real `/monitor/leads` route from production, so the preview shims `role_hint` and the emails filter in memory; the real filter's SQL condition is exercised only by the stubbed route test.
- **Known leftover:** the hint is per row in the list and per person in the review feed, so one person with two differently-enriched rows can read "Likely buyer" on one and "No hint" on the other (Member Benefits' CMO does today).
- The Needs a look count went 26 -> 28 (buyers added, and enrichment now read).
- The preview tool and the Blocked/Released read functions were lifted/extracted so they can run read-only.
