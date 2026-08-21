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

**Two copies of the label map.** `WEBSITE_REASON_LABELS` is a normal JS object.
The monitor dashboard has a second copy (`var WLBL=`) inside a JS string that gets
sent to the browser. Both need updating, and the string one uses `\u2014` for em
dashes with a **single** backslash. Getting that wrong renders a literal `\u2014`
in the dashboard.

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
