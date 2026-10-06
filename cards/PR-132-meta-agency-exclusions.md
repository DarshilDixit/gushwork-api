# PR 132: Flighted and Upraw leads never fire Meta events

Branch `feat/meta-agency-exclusions`, from `main` after #131. Written 7 Oct
2026. **Not merged: waiting for your review.**

**This changes which leads fire Meta events.** Once merged, a lead from
Flighted or Upraw fires **none** of StartTrial, Lead or Schedule (nor the lead
magnet's Contact). Nothing else about those leads changes.

## How many it affects: last 90 days, read-only

**There's no record of which Meta events each lead actually fired.** The
repo doesn't log Meta sends per lead, and Railway keeps logs only for the
last two deploys. So I **replayed** every lead's stored details through the
real Meta rules, lifted out of `index.js`. Those rules are our own-test check,
the non-ICP suppression, disqualified, free email, the website check, and
whether the lead submitted or booked.

| Domain | Leads | Matched by | Submitted | Booked | StartTrial | Lead | Schedule |
|---|---|---|---|---|---|---|---|
| `uprawmedia.com` | 6 | email | 4 | 3 | 6 | 4 | 3 |
| `flighted.co` | 1 | email | 0 | 0 | 1 | 0 | 0 |
| **Total** | **7** | | **4** | **3** | **7** | **4** | **3** |

- **About 14 Meta events in 90 days**, each telling Facebook to find more
  people like a media buyer.
- **No agency lead was matched by website alone.** All 7 used their own
  email domain.
- **Cross-check:** both agency leads created after 15 Sept (when the Meta
  value stamp was introduced) carry a stamp. That stamp is only written when
  a Meta event fired with a value, so the two methods agree.
- **None of the 7 was ours, suppressed as non-ICP, or on an unverified
  website.** So nothing else was already stopping them.
- **All-time** (from the earlier check): 16 Flighted lead rows and 7 Upraw.

## What changed

**The setting:** `META_EXCLUDED_DOMAINS`. The defaults are `flighted.co` and
`uprawmedia.com`. A value set in Railway **adds** to that list and can't
remove the defaults, so a typo can't let an agency back in.

**How a lead matches:** the email's domain **or** the website's domain.
- An exact match or a subdomain counts, and `www.` is ignored.
- Upper or lower case doesn't matter.
- **Never a substring:** `notflighted.co` and `flighted.co.example.test` don't
  match.
- It uses the same matching code as the Google upload, so the two lists
  can't disagree about what "matches" means.

**Where the check lives: one function every Meta event passes through.**
It's inside `sendEvent` in `meta-capi.js`, which handles:
- StartTrial from `/partial`;
- Lead from `/submit`;
- Schedule from all three booking routes;
- Contact from the lead-magnet page.

A test checks that nothing else in the codebase talks to Meta directly. A
future call site therefore can't skip the check.

**What a skipped event does:**
- It's logged with its reason, e.g. `[Meta CAPI] ⏭ Lead suppressed — agency
  domain flighted.co (by email), META_EXCLUDED_DOMAINS: session …`.
- It **isn't a failure**, so no Meta alert counts it.
- It **isn't a success** either, so it doesn't reset the Meta failure count.

**Two small edits in `index.js`:**
- **`/partial` now passes the lead's website into StartTrial.** Before, it
  only sent the email, so an agency using a personal email but its own
  website would have slipped through. Meta never receives that field; it's
  only used for the check.
- **`meta_predicted_ltv` stays empty for an agency lead.** That column
  records the value Meta was sent, so it mustn't claim one for an event that
  never left.

## Unchanged, as you asked

- `ELV_EXCLUDED_DOMAINS`, `INTERNAL_TEST_EMAILS` and `isInternalSubmission`
  are untouched. A test checks the agencies aren't in them.
- **Salesforce, the AWS mirror and the dialer** still receive agency leads
  exactly as before.
- **Blocking is unchanged.** No lead is turned away.

## What I checked

- **Tests: all 15 suites pass, 5,329 assertions** (`measure.js --check`,
  read in full). 67 are new, in `test-batch2.js`, and they **execute** the
  real Meta code against a stubbed Meta that counts what's sent.
- **Every event was tested**: StartTrial, Lead, Schedule and Contact. Each
  was run four ways: an agency by email domain, an agency by email in mixed
  case, an agency by website, and an agency by a website subdomain. For every
  one:
  - **nothing reached Meta**;
  - nothing raised an alert;
  - nothing counted as a success.
- **For each event, a normal lead still fires**, and a lookalike domain
  still fires.
- **Settings**: the defaults, a Railway value extending them, and a lead
  with no email or website never being treated as an agency.
- **Mutation testing: 11 of 11 caught.** I broke the code eleven ways:
  - the check removed;
  - email not checked;
  - website not checked;
  - a substring match;
  - a skip counted as a failure;
  - a skip counted as a success;
  - the default list emptied;
  - a Railway value replacing the defaults;
  - `/partial` no longer passing the website;
  - each of the two value-stamp guards removed.

**Honest limits:**
- **Three checks only read the code**, they don't run it: the `/partial`
  website being passed in, and the two value-stamp guards. The check itself,
  in `sendEvent`, is fully executed for every event.
- **No real Meta request was made.**
- **The full app wasn't booted for this change.** The existing booting
  suites (`/submit`, and the routes that send Meta's StartTrial and Lead)
  still pass unchanged, which shows normal leads are unaffected.

## Not done, one follow-up to decide

**The dashboard's "why was Meta withheld" filter doesn't know about
agencies yet.** That's `metaWithheldReason` and its database version, behind
the All leads filter. An agency lead will appear there as if Meta fired.
CLAUDE.md warns about exactly this: a new reason added to the Meta path
without the dashboard. I kept this PR small, as you asked, so the dashboard
part is separate. It needs the same domain match written in SQL, and a test
that the code version and the database version agree. Tell me if you want
it.

I'll stop here until you say merge.
