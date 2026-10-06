# Background: The Dropoff tab and the weekly digest

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## "Left on step 2", and blocked verdicts leave no trace (was `CLAUDE.md` lines 1988-2002)

**"LEFT ON STEP 2" IS NOT "left at step 1", AND THE LABEL DECIDES WHICH
SCREEN SOMEBODY GOES AND FIXES.** A `leads` row is written by
`savePartial(1)` at the **end** of `handleStep1Next`, after the email and
`sell_to` validate — so a row exists only once the visitor completed step 1
and pressed Next, and **everyone in that bucket reached step 2**. Confirmed
in data: all 578 such leads in the 12 weeks to 25 Sept carry `sell_to` (a
step-1 field) and **none carries a phone** (a step-2 field). It is the
biggest single bucket, every week since July.

**And a lead stopped ON step 2 by a blocking website verdict leaves NO
TRACE.** `handleStep2Next` returns before `submitLead()` on `nxdomain`,
`brand_mismatch` or `mailbox_domain`, so nothing is persisted: measured, 0
of those 578 rows carry a `website_check_reason`. You cannot tell from
`leads` who was turned back from who simply left.

## Source, leads vs people, the bucket as text (was `CLAUDE.md` lines 2003-2034)

**SOURCE READS TWO FIELDS, AND READING ONLY `utm_source` UNDERSTATES META BY
ABOUT A QUARTER.** `DROPOFF_SOURCE_SQL` takes `utm_source` first, then falls
back to `hear_about_us` — which `gushwork-form.js` prefills from the
**referrer** as well as the utm, and then hides. So when the UTMs are lost in
the Facebook and Instagram in-app browsers (31.2% in-app against 1.6% on a
normal mobile browser) the referrer still names the platform. Measured over
12 weeks to 25 Sept: **469 leads carried no `utm_source` at all** and were
recorded by the form as paid — 459 Facebook, 9 Instagram, 1 Google. Reading
`utm_source` alone gives Meta 2,051 and Direct/organic 991; reading both
gives **2,519 and 486**.

**The human free text in that column is deliberately NOT bucketed.** It runs
to dozens of spellings plus junk, and keying a channel off free text is
exactly the substring trap `Source_Bucket__c` already fell into. It stays in
`Direct / organic`, which therefore means "no trackable click" rather than
"arrived directly".

**LEADS OR PEOPLE IS A TOGGLE, NOT A CORRECTION.** `mode=people` resolves the
ladder per `lower(email)`, placing a person in the period they **first**
arrived and carrying the **best** outcome any of their sessions reached.
Measured over the same 12 weeks: **3,309 leads against 3,096 people, booking
65.8% against 69.1%** — a repeat attempt is usually somebody who got there in
the end, so per-person always reads better and neither is the honest number
alone.

**THE BUCKET COMES BACK AS TEXT, and that is load-bearing.**
`node-postgres` parses a `date` column into a JS `Date` in the **process's**
local zone, so `toISOString()` shifted every bucket a day west — silently
empty on an IST laptop and correct on Railway. An environment-dependent bug
is worse than a broken one. `to_char(..., 'YYYY-MM-DD')` removes the parse.
Found by executing the query, not by reading it.

## The weekly digest (was `CLAUDE.md` lines 2035-2067)

**The weekly digest was FIRED ON PURPOSE on 25 Sept** — `node tools/fire-non-icp-slack.js dropoff-digest`, Slack 200, real numbers over the real table. "We asserted it alerts" and "we watched it alert" are different claims, and the gap between them hid 21 dead call sites here.

**The weekly digest leaves out our own test submissions, and says how many**
("Leaves out N of our own test submissions from last week."), because it reads
`dropoffReport`. `tests/test-batch2.js` §33 builds the real digest and reads
that line.

**The weekly digest reports the COMPLETED week, never the current one.**
`runDropoffDigest`, Mondays in the **09:00 ET hour** (`DROPOFF_DIGEST_HOUR_ET`,
`DROPOFF_DIGEST_ENABLED=false` to stop it). Pinned to Eastern, so the local
time moves with US daylight saving — **18:30 IST in summer, 19:30 in winter**.
Sending a partial week to a channel is how a reader concludes the funnel
collapsed on a Monday morning, and no caveat survives being read on a phone.
It leads with whether the week sits **inside** the prior eleven weeks' range,
because the answer is usually "this is normal" and a digest that always reads
like an alarm gets muted along with the week that matters.

**IT GOES TO THE LEADS CHANNEL (`sendSlack`), NOT THE ALERTS ONE.** Authorised
by Darshil on 25 Sept after the first hand-fired one landed in
`bot-n8n-alerts`. Nothing in it is broken and nobody has to act on it, so it
is not an alert; the alerts channel is where things that need fixing go, and a
recurring no-action post there is how that channel starts being muted — taking
the week that matters with it. The near-miss digest still uses `sendOpsSlack`,
so the two deliberately differ.

**THE TICK IS 15 MINUTES, NOT 60, AND THAT IS NOT FUSSINESS.** `setInterval`
starts counting at **boot**, not on the hour, so with an hourly tick a deploy
at 09:05 on a Monday puts the ticks at 10:05 and 11:05 and **that week is
silently skipped**. A missed weekly digest is invisible where a duplicate is
merely annoying, so the trade goes toward sending. The guard keys on the ET
**day** stamp, so four ticks inside the 09:00 hour still send exactly once.
**The near-miss digest had the identical gap and was fixed in the same change** — both now tick at 15 minutes, and they should stay in step. Its own week guard is unchanged, so the documented double-send on a deploy inside the hour still applies to both: that trade deliberately favours sending, because a duplicate is visible and a miss is not.
