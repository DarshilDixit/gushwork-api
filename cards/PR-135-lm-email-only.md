# PR 135: lead magnet takes a business email and nothing else

Branch `lm/email-only-business`, from `main` at `cbe2b98` (after #134). Written
7 Oct 2026. **Not merged: waiting for your review.** Commits `7056413` (the
change and its tests) and `edbe696` (baseline).

## Why

`/buyer-questions` moved from the 150-questions list to the 20-prompt AI
visibility pack (the PDF behind `gush.to/visibility-report`). Swapnil's call:
"just take their business email". The page copy was rewritten and published
on 7 Oct; the form fields come off next, in Webflow.

## What changed, all in `lead-magnet.js`, `/lm/submit` only

1. **Industry, product and sell-to are no longer required.** The route used
   to answer `400 Missing required fields` without them, so the email-only
   page would have been refused on every submit.
2. **Free mailboxes are refused**: `422`, `status: 'free_email'`, message
   "Please use your work email, not a personal one like Gmail or Yahoo."
   - The check uses the injected `FREE_EMAIL_DOMAINS` (16 domains). The page
     refuses first, from its own list of about 140, so this is a backstop
     for anything that skips the page, like `gateEmail` above it.
   - It runs after `gateEmail` and **before anything is written**, so a
     refused address leaves no row, no Contact and no Loops push.
3. **The duplicate check keys on email and session only.** It compared
   `product_or_service = $2`; with the product blank that is NULL against
   `''`, which never matches, so a double-tap would have fired a second Meta
   `Contact`.
4. Header comment updated. **The Loops `source` label still says "Buyers
   Questions"**, left alone on purpose until Loops is checked, in case a Loop
   filters on it.

## What did NOT change

- **The demo form, `leads`, Salesforce, the dialer: untouched.** This table
  never reaches any of them.
- **Meta:** `Contact` still fires once per accepted signup, as before.
  Refused personal addresses fire nothing, which they did fire before (5 of
  our own gmail tests in August did). **Nothing changes which demo leads
  are blocked.**
- `/lm/track`, the dashboard routes, Loops, the webhook.

## Order matters

**Deploy this before the Webflow page loses its fields.** It is
backward-compatible with today's page (the old fields are simply no longer
required), so merging first is safe; the reverse order breaks every signup.

## Verified, and how

- **Full bar, bare:** 16 suites, 5422 assertions, 0 failed. Only move:
  test-batch-a 339 to 356, re-baselined in `edbe696`.
- **The new section EXECUTES the real handler** out of `lead-magnet.js`,
  with Meta, Loops and DNS stubbed and a stubbed pool. 17 assertions: gmail
  refused before any write, a business email alone accepted, one Contact
  and one Loops push, website still derived from the email domain, the
  duplicate check's SQL and params, a second submit marked duplicate with
  no second Contact, malformed address and bad session still refused.
- **Mutations, each measured bare with `--mutation`, all CAUGHT by
  test-batch-a:** free check disabled (10 failed), old required-fields
  check put back (10 failed), duplicate check keyed on product again
  (2 failed). The first run of the second one was read through `grep`
  against the rule, so it was rerun bare; same verdict.
- **The new duplicate SELECT was EXPLAINed on Railway** inside `BEGIN READ
  ONLY`; Postgres planned it (Seq Scan, as before).

## Not verified

- **Not run against a real browser or a real deploy.** That happens with
  the Webflow half.
- **A latent fault in test-batch-a, not fixed here:** the health/ui block
  builds the dashboard client without `esc`, and `checkHealth` leaves an
  unawaited `loadEnrichCoverage` that rejects later. On `main` the suite
  exits first. My first draft added one `setImmediate`, which gave that
  rejection time to fire and crash the suite. I removed the tick rather
  than touch that harness; it is still there for the next test that yields.

## The other halves

- **Webflow (not in this repo):** remove website, industry, product and
  sell-to from the modal; the page script refuses personal addresses and
  submits straight from the hero email. Prepared, held unpublished until
  this merges.
- **Loops:** not checked yet, by your choice. The thank-you screen says
  "we have sent it", which is only true once the Loop is confirmed.
