# gushwork-api

Inbound lead capture, verification and routing for gushwork.ai. Node + Express on
Railway, Postgres, no build step. `index.js` is ~6,000 lines and holds most of the
system.

Explain things in plain language — no jargon, no making things sound more complex
than they are.

---

## The one rule everything else follows

**A lead is worth more than a verdict.**

Every check in this system exists to add information, never to stand between a real
person and a demo booking. When a checker breaks, times out, gets rate limited, or
hits something it doesn't understand, it **fails open** — the lead goes through and
the check reports that it couldn't decide.

Three things follow from that, and they are not negotiable:

1. **"We could not check" is never recorded as "we checked and it is bad."**
   A DNS resolver failure is not a fact about someone's domain. A captcha wall is
   not a thin page. A timeout is not a dead site. Each of these gets its own
   verdict that says what actually happened.

2. **Blocking is the highest-risk action in the codebase.** Only three verdicts
   block, all decided client-side in `gushwork-form.js`: `nxdomain`,
   `brand_mismatch`, `mailbox_domain`. Do not add a fourth without asking. The
   server-side checks never block — they inform.

3. **Suppressing a Meta event is a real cost, not a safe default.** It removes a
   conversion signal from the ad algorithm. Treat "should this fire Meta?" as a
   business decision to surface, not a judgement call to make quietly.

When a change would alter which leads get blocked or which fire Meta events, say so
explicitly in your summary. Never let that happen as a side effect.

---

## Layout

Every file in the repo root, so this list can't quietly go stale the way it did
before — a file missing from here reads as "forgotten," not "not documented yet."

| File | What it's for |
|---|---|
| `index.js` | Routes, website checking, email verification, alerting, the monitor dashboard, cron |
| `db.js` | Schema + migrations. Runs on every boot; everything is `IF NOT EXISTS` |
| `salesforce.js` | Lead upsert by email. Refresh-token OAuth |
| `meta-capi.js` | Conversions API — `Lead`, `Schedule`, `StartTrial`, `Contact` |
| `loops.js` | Loops.so contact push for the lead-magnet landing page |
| `partnerstack.js` | PartnerStack API. TWO hosts and TWO auth schemes: `partnerlinks.io` conversion (Bearer tracking token) and `api.partnerstack.com` v2 partnerships + actions (Basic public:secret) |
| `lead-magnet.js` | `/lm/*` routes. Separate table, deliberately not joined to `leads` |
| `backfill-sf.js` | Manual recovery tool for re-syncing leads to Salesforce after a broken connection or outage. Not mounted by default — see below |
| `gushwork-form.js` | The `/demo` form frontend. Lives here and is served live by jsDelivr — see below |
| `gushwork-form-popup.js` | The Google Ads popup/modal form frontend. Lives here and is served live by jsDelivr — see below |
| `package.json` | Dependencies, scripts, Node engine constraint |
| `package-lock.json` | Locked dependency versions, committed so Railway installs exactly what was tested |
| `.gitignore` | Keeps `node_modules/`, `.env`, logs, and local Claude settings out of the repo |
| `README.md` | Repo landing blurb, not living documentation. This file is |
| `tests/` | The test files described under Deploying |
| `docs/partnerstack.md` | PartnerStack handover: the two-step model, every ps_ column, env vars, test procedure, known gaps |
| `CLAUDE.md` | This file |

**`gushwork-form.js` and `gushwork-form-popup.js` are in this repo, not a separate
one,** and jsDelivr serves both from it. **They are pinned to a commit SHA, not to
`main`** — see below, because it changes what "deploying a form fix" means.
`darshildixit.github.io/gushwork-embeds` is a genuinely different repo — it holds
the Webflow CSS/JS embeds, not these two files. Don't confuse the two.

### Deploying a form change — the Webflow step

**A `git push` does NOT ship a form change.** The two form files reach production
through a `<script src>` in **Webflow → Project Settings → Custom Code**, and that
URL names an immutable commit SHA:

```
https://cdn.jsdelivr.net/gh/DarshilDixit/gushwork-api@<40-char-sha>/gushwork-form.js
https://cdn.jsdelivr.net/gh/DarshilDixit/gushwork-api@<40-char-sha>/gushwork-form-popup.js
```

So shipping a form fix is **two** steps, and the second one is outside this repo:

1. Merge to `main` as usual.
2. Take the new `main` SHA (`git rev-parse HEAD`) and update **both** script tags
   in Webflow, then republish.

Miss step 2 and the fix is in `main`, the tests pass, Railway has redeployed — and
every real lead is still running the old file. Nothing in this repo will tell you.

**Why SHA and not `@main`.** jsDelivr treats a SHA path as immutable and caches it
permanently, so it either serves those exact bytes or 404s. `@main` is a mutable
ref served best-effort, which needs a cache purge — and purges do not reliably
take. Pinning removes the purge from the process entirely.

**Use the full 40-character SHA.** Short SHAs work today but are ambiguous as the
repo grows, and a collision resolves to the wrong file rather than erroring.

**Pin both files to the same SHA**, even when only one of them changed. The bytes
would be identical either way — a commit SHA names a snapshot of the whole repo,
so asking for an untouched file at a newer SHA returns the same blob — but pinning
them together is the only thing that records that the pair was *tested* together.

**Confirming the swap actually took**: load the page and read the console banner.
`/demo` logs `Form initialised v…`, the Ads page logs `… (Google Ads)`. If the
version there is not the one you just merged, Webflow is still serving the old
pin — the deploy is not done, however green this repo looks.

**`backfill-sf.js` is a kept tool, not dead code.** It re-syncs leads to Salesforce
after a broken connection or outage. Its `/admin/backfill-sf` route is deliberately
*not* mounted in `index.js` — it should only run when someone decides to run it.
Mount it temporarily when a recovery is needed, then remove the route again. Don't
delete the file.

## Tables

- **`leads`** — one row per form session, upserted as the visitor progresses.
  A row here means someone reached at least step 1 (entered an email).
- **`form_sessions`** — one row per page load. Kept separate on purpose: writing
  page loads into `leads` would break every query that assumes a row means someone
  reached step 1. Join on `session_id` when you want a funnel.
- **`lead_magnet_leads`** — the LP funnel. Never joined to `leads` at write time.
- **`enrichment_data`** — Apollo responses.

---

## Definitions

Every number on the monitor dashboard must conform to this section. Where a number
deliberately departs from it, the label on screen has to say so in words a
non-engineer reads correctly. Added because the dashboard had accumulated numbers
whose labels and calculations disagreed — a chart called "sessions" that counted
leads, a tooltip claiming two different populations were identical.

### The four nouns

**Session** — one row in `form_sessions`. One visitor arriving on a form page.
The id lives in `sessionStorage`, so a landing-page → `/demo` journey in one tab is
**one** session with `hits` incremented, not two. Keys on `form_sessions.created_at`
(first seen). Session recording only began **21 Aug 2026, 10:32 UTC**; there is no
session data before that and queries must say "not tracked" rather than 0.

**Lead** — one row in `leads`. A session that got as far as entering an email
(step 1). Keys on `leads.created_at`. One person can have several leads. A lead is
never deleted, so counts only go up.

**Completed** — `leads.completed = true`. The visitor reached step 2 and `/submit`
succeeded. Keys on **`submitted_at`**. Caveat that has bitten us: the Cal and
RevenueHero safety-net branches create rows with `completed = true` and
`submitted_at` already set for someone who booked without ever touching the form.
They are real leads and they are not form completions.

**Booked** — `leads.booking_uid IS NOT NULL`. Keys on **`booked_at`**, falling back
to `created_at` where `booked_at` is null (rows predating that column). Always use
`COALESCE(booked_at, created_at)` when ordering by when a booking happened —
comparing a null `booked_at` yields null, the comparison quietly fails, and the row
is counted as un-booked.

### The stage ladder

Exactly four stages, **mutually exclusive and exhaustive**, resolved in this
priority order. Any lead is in exactly one, so the four always sum to the total:

1. **Booked** — `booking_uid IS NOT NULL`
2. **Disqualified** — not booked, and `disqualified IS TRUE`
3. **Completed** — not booked, not disqualified, and `completed IS TRUE`
4. **Step 1** — everything else

Use `IS TRUE` / `IS NOT TRUE`, never `= true` / `= false`: a null flag on an old row
must land in a stage rather than vanishing from all four. The stage filter, the
stage badge and any stage count all read from this one ladder. If you add a stage,
it goes in the ladder or it doesn't exist.

### The PartnerStack lifecycle ladder

Exactly eight states, **mutually exclusive and exhaustive**, one per partner
**domain**, resolved in this priority order. Every counter on the Partners tab
is a `COUNT FILTER` over this one column, so the numbers cannot disagree with
each other. Same rule as the stage ladder above: if you add a state, it goes in
the ladder or it does not exist.

1. **qualified** — `ps_qualified_sent_at IS NOT NULL`
2. **qualification_failed** — `ps_qualify_failed_at IS NOT NULL`
3. **conversion_failed** — `ps_signup_failed_at IS NOT NULL` **and not since converted**
4. **demo_done_not_qualified** — earliest `start_time` is in the past
5. **awaiting_demo** — `booking_uid IS NOT NULL`
6. **converted** — `ps_signup_sent_at IS NOT NULL`
7. **skipped** — `ps_signup_skipped_reason IS NOT NULL`
8. **conversion_pending** — everything else

**The order is not the progression order, deliberately.** A success always
outranks its own failure, because a domain that failed and later succeeded is
fine. But an *unresolved* conversion failure outranks every later stage it
blocks: a domain whose conversion never landed can never be qualified, so
showing it as "awaiting demo" would hide the only fact worth acting on. That is
exactly what happened on 4 Sept — a 400 on the qualification released the claim
correctly and nothing on any card moved.

A domain matching two states takes the **first** match, never a blend.

**Keyed by DOMAIN, because that is the unit PartnerStack pays on** — one
conversion and one qualification per customer key, ever. Leads with **no usable
domain** cannot be keyed that way, so they are counted **separately, as leads**,
in their own field, and the UI says "leads, not companies" on that chip. Folding
them into a domain count would reintroduce the mixed-unit arithmetic that made
the old counters irreconcilable.

The query is bounded to `PS_LADDER_WINDOW_D` (180 days, matching the Salesforce
lookback) **except for unresolved failures, which are included regardless of
age** — otherwise a domain that failed months ago and was never fixed would
silently drop out of "Needs attention", the one number that has to be complete.

**conversion_failed and qualification_failed are the two red states** and are
summed into "Needs attention" — the only number on the tab that means someone
has to act today. Both also fire a Slack alert at the moment of failure, via
`alertOps` with its normal cooldown, because a state you have to remember to
check is half a fix.

### Bookings: two different questions, and they are not interchangeable

These look like one question and are not. Collapsing them onto a single rule
breaks whichever one you didn't have in mind.

**1. "Is this person an SDR target?" — no time comparison.**
Does any lead row sharing this `lower(email)` have a `booking_uid`? If yes, they
have a booking; don't call them. When they booked is irrelevant — an SDR ringing
someone who already has a call on the calendar is wrong whether that call was
booked yesterday or in May. Used by the SDR List and by "No booking yet".

**2. "Should this session get a drop-off recovery email?" — the time comparison is
required.** `NOT EXISTS (a booking by this email with booked_at >= this session's
created_at)`. A booking that *predates* the session does not resolve that session's
drop-off: the person came back, started again, and dropped again. Suppressing on
"has ever booked" would silently kill legitimate follow-ups. This is deliberate,
dates from a May 2026 fix, and lives at the recovery cron (`index.js` ~4499) with a
comment saying so. **Do not "unify" it.**

**3. "Recovered bookings"** is a third shape — a completed session with no booking,
followed *later* by one — and uses `COALESCE(booked_at, created_at) >= l.created_at`.

Note the asymmetry between 2 and 3: the cron compares bare `booked_at` and does
**not** COALESCE, because production has zero rows with a null `booked_at` and
there is nothing to defend against. Recovered bookings does COALESCE because it
reads the full history including rows that predate the column. Both are correct for
what they ask.

The dashboard's "Pending recovery" card reports question 2's population, so its
label says what it counts and no longer claims to mirror the cron's exact rule.

### Default population for dashboard numbers

Unless a label says otherwise:

| Question | Default |
|---|---|
| All leads, or completed only? | **All leads.** Filter to completed only where the label says "completed" |
| Deduped by email, or by session? | **Headline numbers are people** — `COUNT(DISTINCT lower(email))`. Session counts are legitimate but must be labelled "sessions" every time they appear |
| Named exception | **"Form entries per day"** on Overview is a deliberate ROW count — see below |
| Dedup key | `lower(email)`, always. Never raw `email` |
| Internal / test addresses | **Included.** See below |
| Webhook-origin leads | **Included**, except `/monitor/funnel` |

**Internal and test addresses are currently INCLUDED in every `leads` number.**
`ELV_EXCLUDED_DOMAINS` (`gushwork.ai`, `test.com`, `example.com`, `example.org`) and
the `b@g.ai` test address are excluded from ELV health and from alerting, and from
nothing else. So Overview, All Leads, SDR List and Duplicates all count our own
testing. This is a known distortion, not a decision anyone made — flag it, don't
quietly "fix" it, because excluding them moves every historical number at once.

**"Form entries per day" is deliberately a row count, not a people count.** It
counts rows in `leads` per ET day — everyone who reached step 1, undeduped, all
stages. That is a departure from the people-by-default rule and it is intentional:
the chart's job is daily inbound volume, and deduping by email would flatten the
repeat-attempt spikes that make a bad day visible. The label says "entries", not
"people" and not "sessions", so it reads correctly. Do not "fix" it to
`COUNT(DISTINCT lower(email))`. Proper session-based charting is separate work.

**Webhook-origin leads** (`prefill_source IN ('rh_webhook','cal_webhook')`) are
included everywhere except `/monitor/funnel`, which excludes them from step1,
submitted and booked and explains why at length. They never touched the form, so
they inflate any form-conversion rate from both sides. Only 9 rows today; left in
deliberately.

### Timezone

**The dashboard is Eastern Time.** Use the IANA name `America/New_York`, never a
fixed offset — DST has to move on its own.

- Every displayed timestamp and every day boundary is ET.
- The Postgres session timezone is `Etc/UTC` (confirmed). So a bare `date_trunc`
  or `::date` buckets in UTC, which is **not** a correct day boundary for this
  dashboard. Write `AT TIME ZONE 'America/New_York'` explicitly.
- Never derive a calendar date in browser code from `getFullYear/getMonth/getDate`
  — those read the viewer's laptop. Format through
  `Intl.DateTimeFormat` with an explicit `timeZone`.
- `/monitor/funnel` is the one deliberate exception and keeps UTC day buckets. Its
  go-live and coverage reasoning is anchored to a UTC instant, and re-bucketing it
  would silently change which day counts as partially covered. Its comments say so.

### Health checks fail LOUD

"A lead is worth more than a verdict" governs the lead path: checkers fail open.
**Health checks are the opposite and must be, because no lead depends on them.** A
health probe that cannot reach its dependency reports red, never green and never
"unknown-styled-as-fine". A green badge means "verified working, just now". If it
can't verify, it must not be green.

---

## Things that will bite you

**The three lists.** `WEBSITE_VERIFIED_REASONS` (line ~424), `RECHECK_WRITEABLE`
(~3386) and `RECHECK_PROTECTED` (~3398) must stay in sync with `gushwork-form.js`
SECTION 3C **and `gushwork-form-popup.js`, which carries its own copy of the same
lists** — see the fork note below. There's a warning comment above them. Adding a
website verdict means deciding its place in all three:

- **`WEBSITE_VERIFIED_REASONS`** → does Meta fire?
- **`RECHECK_WRITEABLE`** → can the historical recheck tool overwrite it?
  Anything meaning "we didn't get a real answer" stays **out**.
- **`RECHECK_PROTECTED`** → verdicts that depend on the lead's email and can't be
  re-derived from the domain alone.

**Backticks inside SQL comments break the file.** The SQL in this repo lives in
JS template literals, and the house style is a long `/* ... */` comment inside the
query explaining the incident behind it. A backtick in that comment — writing
`` `leads` `` or quoting an expression — **terminates the template literal**, and
the error surfaces as `SyntaxError: missing ) after argument list` pointing at the
`pool.query(` line, not at the comment. Four of these happened in one sitting. Use
plain words inside SQL comments, and run `node --check index.js` before committing.

**`gushwork-form-popup.js` is a FORK of `gushwork-form.js`, not a sibling.**
The Ads file exists only to present the booking step as a fullscreen modal
opened after step 2. Every other line is meant to be the same code, and there is
no shared module — the two files are edited independently, so a fix applied to
one is a fix applied to half the traffic. This has already gone wrong once: the
Ads file forked at `/demo` v5.3.0 on 14 Aug 2026 and silently missed v5.6.0 and
v5.7.0/v5.7.1, so for twelve days Google Ads leads got no DNS fallback, no
email-in-website-field catch, and no typo nudge. **A change to `gushwork-form.js`
is not finished until it is in `gushwork-form-popup.js` too.**
`node tests/test-ads-parity.js` now enforces that — it lifts both files and
compares them, and it also pins the modal as deliberate so a future sync cannot
"tidy" the fork's own presentation away.

**Two copies of the label map.** `WEBSITE_REASON_LABELS` is a normal JS object.
The monitor dashboard has a second copy (`var WLBL=`) inside a JS string that gets
sent to the browser. Both need updating, and the string one uses `\u2014` for em
dashes with a **single** backslash. Getting that wrong renders a literal `\u2014`
in the dashboard.

**Two more pairs that must stay in sync.** Same shape as the label map above:

- `SDR_SEARCH_COLUMNS` (server, in the `/monitor/sdr` route) and
  `SDR_SEARCH_FIELDS` (client, in the dashboard JS) are the fields the SDR
  search matches. The table filters in the browser; the CSV export filters on
  the server. If they drift, the export silently stops matching what is on
  screen. A test lifts both and asserts they are equal.
- The System Health check ids in `HEALTH_SEVERITY` / `HEALTH_ALERT_META`
  (server) and `HIDS` (client) map checks to dashboard rows. A check with no
  entry in `HIDS` renders nowhere; a row id with no check paints red as
  "No result". Both are asserted.

**`/monitor/health` is a real probe, and slow on purpose.** It queries the AWS
mirror across a WAN, so it is deliberately kept OFF the dashboard's 60-second
poll — it runs at load, every five minutes, on tab open and on the Re-check
button. The same checks run from `startHeartbeat()` every 30 minutes so a
failure alerts with the tab closed. Do not fold it into `/monitor/metrics`.

**`/monitor/website-recheck` is a POST.** It was a GET that rewrites lead rows
and runs two `ALTER TABLE`s, which a link prefetch or an unfurled URL could
have fired. Nothing in the UI calls it; run it with
`curl -X POST`.

**`/monitor` is not one page.** It's the dashboard plus several sub-routes that
feed it data. A reader who greps for a single `/monitor` handler expecting to find
everything will miss most of it.

**Booking arrives by three routes.** `/booking-confirmed` (browser-fired),
`/booking-confirmed-webhook` (Cal), `/booking-confirmed-webhook-rh` (RevenueHero).
Any change to booking behaviour has to be applied to all three. A fix on one is a
fix on one third.

**PartnerStack: `partnerStackCustomerKey` is the only place a domain becomes a
customer key.** PartnerStack counts one conversion per customer key for the life
of the account, so two spellings of one company means either an affiliate paid
twice or a real referral swallowed as a duplicate. Website, email and the three
warehouse customer tables all go through this one function. Do not normalise a
domain inline anywhere near this integration.

**A disqualified lead never fires a conversion, and that is a GUARD now, not a
flow property.** No disqualified lead reaches `/submit` today — `b2c_or_mixed`
and `waitlist` both call `savePartial(1)` and then show a terminal step — but
that lives in two forked frontend files which have drifted apart before, and the
cost of the drift here is paying an affiliate $50 for a B2C waitlist signup.
`runPartnerStackSignup` checks the flag explicitly and a test asserts it runs
before the domain and test-email checks.

**Every `/submit` logs whether a partner was present.** An organic lead used to
produce no PartnerStack line at all, so the logs could not tell "no partner
traffic yet" from "capture is broken" — which cost real time on the first
deploy. `[PartnerStack] No partner on this submit (…)` is the negative case, and
each skip says which guard stopped it.

**The PartnerStack handover doc is `docs/partnerstack.md`.** Anything below is
the short version; that file has the full picture including the known gaps.

**Two PartnerStack hosts, two auth schemes, one env.** The conversion goes to
`partnerlinks.io/conversion/xid` with `PARTNERSTACK_TRACKING_TOKEN` as a
**Bearer** token. The partnerships lookup and the qualification action go to
`api.partnerstack.com/api/v2/*` with **Basic** base64(`PARTNERSTACK_PUBLIC_KEY`:
`PARTNERSTACK_SECRET_KEY`). Both credentials sit in the same environment and
using one where the other belongs returns a 401 that reads like a bad password
rather than the wrong scheme. A test asserts `sendConversion` never reaches for
the key pair.

**Partner identity resolves in three layers and never blocks a lead.** Process
memory, then any earlier lead row already carrying the name, then the v2 API. A
FAILED lookup is deliberately not cached — caching it would pin every future
lead from that partner to "unknown" for the life of the dyno.

`partnerIdentityNoNetwork()` is the first two layers only and is awaited BEFORE
Slack fires. An earlier version peeked at the in-memory Map alone, which meant
every deploy cleared it and the first partner lead after a restart posted a raw
hex key to Slack even though the database already had the name from an earlier
lead. The database layer is one indexed lookup
(`leads_ps_partner_key_resolved_idx`) and only runs when a partner key is
present, so organic leads pay nothing. The API call is the slow part and stays
deferred; `upgradePartnerHearAboutUs` corrects the row afterwards — in our
table, on the AWS mirror, and in Salesforce where the AE is looking.

**One display chain, three surfaces: name → email → raw key.**
`partnerDisplayName()` is used by Slack, the dashboard and `hear_about_us` so
the same partner cannot read three different ways. An email tells an SDR who
they are dealing with; a hex key tells them nothing they can search for. The
`hear_about_us` upgrade treats BOTH weaker rungs as replaceable, so a row
showing the email is lifted to the name when it resolves — but only values this
code wrote, never a referral or anything a human typed.

**`hear_about_us`: an existing referral outranks a partner.** `gw_ref_email` is a
named human vouching for the lead and is a stronger signal than an affiliate
link, so anything already starting with `Referral -` is left alone. The upgrade
only ever rewrites the exact `Partner - <key>` placeholder this code wrote; a
referral or anything a human typed is never touched.

**A late-arriving single field needs its own targeted AWS write, NEVER
`syncToAWS`.** That upsert's conflict clause sets
`disqualified = EXCLUDED.disqualified` with no COALESCE, so handing it a partial
object passes `disqualified` as false and CLEARS a real disqualification on the
mirror the dialer reads. `syncBookingToAWS`, `syncPartnerIdentityToAWS` and
`syncHearAboutUsToAWS` all exist for this reason. A test catches the regression.

**Partner revenue gaps is a WORK QUEUE, not a health check.** `/monitor/partner-gaps`
finds the two ways a partner referral silently never pays: (A) no conversion was
ever sent for that domain, and (B) the demo happened and no Opportunity exists
for an AE to tick. It is deliberately NOT in System Health — a green badge there
means "verified working, just now", and a lead waiting on an AE is normal
latency, so wiring it in would leave the dashboard permanently amber and train
people to ignore it. A test asserts `runHealthChecks` never calls it.

The tempting version of this check — "booked but no `ps_qualified_sent_at`" —
is wrong and was rejected. It conflates three states and only one is a bug: no
Opportunity (broken), Opportunity awaiting an AE (normal), and Opportunity the
AE deliberately did not tick (correct, and PERMANENT). The third never clears,
so the queue fills with correct non-payments and the real failures become
invisible inside it.

Check A is keyed by DOMAIN, not by lead: the conversion fires once per domain
ever, so the second lead from a domain legitimately has a null
`ps_signup_sent_at`. Only a domain where NO row was ever sent is a miss.

`leads.start_time` is TEXT, so every cast to timestamptz there is wrapped in a
`CASE WHEN start_time ~ '^[0-9]{4}...'`. A bare cast in a WHERE clause is not
safe: Postgres does not guarantee the regex runs first, and one malformed row
takes the whole query down.

When Salesforce cannot be reached, check B reports **unavailable**, never zero.
The card shows `N+?` rather than a total. "We could not check" is not "we
checked and it is fine" — the same rule the lead-path checkers follow, pointed
the other way.

**Step 10 is a POLLER, not a Salesforce Flow callout.** A Flow that calls out
fails inside Salesforce where nobody on this team would see it, and it couples an
AE ticking `Qualified_Demo__c` to our service being up at that instant. Polling
every 15 minutes means a missed window is just a later window. The join between
the two systems is the DOMAIN — `Account.Website` first, the primary contact's
email domain as fallback, both through `partnerStackCustomerKey`. Only domains
that already converted (`ps_signup_sent_at IS NOT NULL`) can be qualified: an
action for a `customer_key` PartnerStack has never seen is a no-op at best.

**The automated eligibility check is BUILT AND OFF for the MVP.** Rejections are
decided by hand at payout approval. Everything below is dormant behind
`PS_ELIGIBILITY_ENABLED`, default off — set it to the string `true` in the
Railway env to turn it on, no rebuild needed. The conversion call deliberately
does **not** consult it: today every partner lead that is not one of our own
test addresses converts. Wiring the check into that path is a decision for
someone to make, not a tidy-up, and a test asserts the two stay unconnected.

**"Once per domain, ever" is enforced by the DATABASE, not by application
code.** `leads_ps_signup_once_idx` is a UNIQUE PARTIAL index on
`ps_customer_key WHERE ps_signup_sent_at IS NOT NULL`. `runPartnerStackSignup`
CLAIMS the domain with a conditional UPDATE *before* the HTTP call and only the
winner sends; a concurrent claim surfaces as `23505` and is read as
"already sent". Checking then sending would race — two submits for one domain
arriving together would both fire, and PartnerStack cannot undo a double credit.
If the send fails the claim is RELEASED, because a stamp on a conversion that
never arrived is silent, permanent, and costs the affiliate a real payout.

**PartnerStack eligibility FAILS CLOSED, and that is deliberate.** It is the one
check in the repo that does not follow "a lead is worth more than a verdict" —
because it does not touch the lead at all. It decides whether an *affiliate* gets
paid. The conversion call fires once per key forever and cannot be recalled,
while a conversion we skipped is still in the log to send by hand, so a check
that cannot run returns `check_failed` rather than waving it through. No lead is
ever blocked, delayed or de-Meta'd by it.

**PartnerStack eligibility runs AFTER `res.json()`, and must stay there.** It is
fire-and-forget in `/submit`, next to `finaliseElvVerdict`, and it only runs when
`ps_xid` is present. Three things keep the warehouse off a lead's critical path
and all three are load-bearing:

- the rule (b) query is wrapped in `withTimeout(..., PS_CUSTOMER_QUERY_TIMEOUT_MS)`
  — `awsPool` has **no** `statement_timeout`, so an RDS instance that accepts
  connections but answers slowly hangs forever otherwise. Fail-closed does not
  save you here: a hang never reaches the catch. Three hung queries also exhaust
  `awsPool` (`max: 3`) and starve `syncToAWS`.
- the customer cache is warmed at boot and every 30 min by
  `startPartnerStackCacheWarm()`, so no lead ever pays for the fetch. Measured
  cost when healthy: 0.8–3.4s across the WAN.
- eligibility is never awaited in the route.

**Step 5's conversion call goes after this verdict, not before it.** Moving it
earlier to "save a round trip" puts a third-party HTTP call in front of a waiting
lead. `tests/test-partnerstack.js` asserts the ordering and that it is not
awaited; both mutations are caught.

**The contact source behind eligibility rule (a) is a registry, on purpose.**
`PS_CONTACT_SOURCES` / `PS_CONTACT_ACTIVE` in `index.js`. Today it is prior
inbound form leads only. The warehouse also holds two live outbound logs
(`gist.gtm_coldemail_sends_master`, indexed on domain;
`gist.gtm_outbound_multisource`, NOT indexed on `website_url`) which may be
switched on later. Measure before switching one on: against 90 days of real
leads, prior form lead rejects 9.7%, cold email in the 90 days before the submit
25.6%, dials 4.9%. A naive "emailed in the last 90 calendar days" reading looks
like 39.6%, but 17.4 points of that is our own sequencer following UP on an
inbound lead, which is not prior contact.

**Rule (b)'s 12-month clause is unenforceable and says so out loud.** Nothing in
`gw_prod` can date a churn: `customer_contract_terms` has zero churned rows,
`gist_accountsmaster` has an `End_Date` on 2 of 330 rows, and
`public.subscriptions` has no row with a future billing date. Current-customer is
checked; the 12-month half is returned as `unverified: ['customer_last_12_months']`
on every pass so it is visible rather than silently passing. The clause stays in
the affiliate terms — we just cannot enforce it here yet.

**Six verification columns never reach the AWS mirror.** `elv_status`,
`elv_checked_at`, `website_check_failed`, `website_check_reason`,
`website_check_reason_prev` and `website_rechecked_at` exist on Railway `leads`
and not on `gw_form_leads`. The dialer cannot see whether a lead was verified.
Known, not fixed here.

**Known open bug:** the duplicate-booking guard looks up the *newest* lead row per
email and asks whether that row has a booking. A second form submission creates a
newer row, so the same person can take two calendar slots. Known, deferred by the
owner — do not "fix" it without asking.

---

## Deploying

`git push` to `main` → Railway builds and deploys. **There is no staging and no CI.**
A push is a production release.

So: work on a branch, run the tests, and only merge when they pass.

```bash
node tests/test-batch1.js       # logic, no dependencies
node tests/test-batch2.js       # logic, no dependencies
node tests/test-batch-a.js      # logic, no dependencies
node tests/test-ads-parity.js   # the two form files against each other, no dependencies
node tests/test-partnerstack.js # PartnerStack steps 1-10, no dependencies
node tests/test-batch1-db.js    # needs DATABASE_URL
node tests/test-batch1-e2e.js   # boots the real server, needs DATABASE_URL
```

**The five dependency-free suites are the bar.** They run anywhere in about a
second each — run all five after any change to `index.js`, `lead-magnet.js`, or
either form file, always. Do not install Postgres and do not point anything at
the production database from a feature branch.

Tests read the real functions out of `index.js` rather than a copy. A test that
exercises a duplicate of the source can pass while production is broken. Keep it
that way.

---

## Style

Comments explain **why**, anchored to the incident that caused the code. Real
examples in the file: the spam lead that booked without submitting, the Shenzhen
manufacturer blocked by a DNS-over-HTTPS failure, the SiteGround captcha that looked
like a thin page. Match that. A comment restating what the line does is noise; a
comment naming the lead that broke is worth keeping.

Keep the existing structure. This is one large file on purpose for now — don't
reorganise it as a side effect of another change. Splitting it up is a real idea,
but a deliberate one, on its own.

Plain, direct language in Slack alerts and dashboard labels. They're read by SDRs,
not engineers. "Domain registered but no website on it", not `parked_confirmed`.

---

## Working with Darshil

- Say what you changed and what you did **not** change.
- Flag anything that alters blocking or Meta behaviour, loudly.
- If a fix has a half you can't do here (anything in the two frontend files), say
  which half is missing rather than implying it's complete.
- Don't claim something is verified unless it was actually run. "Syntax checks" and
  "tests pass" are different statements.
- When you're unsure whether something is a bug or intentional, ask. This codebase
  has a lot of deliberate-looking oddities that really are deliberate.
