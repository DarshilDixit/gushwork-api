# gushwork-api

Inbound lead capture, verification and routing for gushwork.ai. Node + Express on
Railway, Postgres, no build step. `index.js` is ~4,400 lines and holds most of the
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
| `lead-magnet.js` | `/lm/*` routes. Separate table, deliberately not joined to `leads` |
| `backfill-sf.js` | Manual recovery tool for re-syncing leads to Salesforce after a broken connection or outage. Not mounted by default — see below |
| `gushwork-form.js` | The `/demo` form frontend. Lives here and is served live by jsDelivr — see below |
| `gushwork-form-popup.js` | The Google Ads popup/modal form frontend. Lives here and is served live by jsDelivr — see below |
| `package.json` | Dependencies, scripts, Node engine constraint |
| `package-lock.json` | Locked dependency versions, committed so Railway installs exactly what was tested |
| `.gitignore` | Keeps `node_modules/`, `.env`, logs, and local Claude settings out of the repo |
| `README.md` | Repo landing blurb, not living documentation. This file is |
| `tests/` | The three test files described under Deploying |
| `CLAUDE.md` | This file |

**`gushwork-form.js` and `gushwork-form-popup.js` are in this repo, not a separate
one.** jsDelivr serves both straight from `main`:
`https://cdn.jsdelivr.net/gh/DarshilDixit/gushwork-api@main/gushwork-form.js`.
Committing either file is a **live production change to the forms** — it needs a
jsDelivr cache purge to take effect, unlike everything else here, which just needs
a Railway redeploy. `darshildixit.github.io/gushwork-embeds` is a genuinely
different repo — it holds the Webflow CSS/JS embeds, not these two files. Don't
confuse the two.

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
SECTION 3C. There's a warning comment above them. Adding a website verdict means
deciding its place in all three:

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
node tests/test-batch1-db.js    # needs DATABASE_URL
node tests/test-batch1-e2e.js   # boots the real server, needs DATABASE_URL
```

The first one runs anywhere in about a second — run it after any change to
`index.js`, always.

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
