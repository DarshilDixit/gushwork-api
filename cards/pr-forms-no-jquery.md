# PR: the form files stop loading jQuery (form v5.21.0)

Branch `forms/perf-no-jquery`, from `main` at `08ecc1e`. 17 Oct 2026.
**Not merged: waiting for your review.** No Webflow page and no gushwork-web
pin was touched. Nothing about who is blocked, which leads fire Meta, routing,
the B2C rule or any server call changes.

**Why:** you asked for lighter, faster form scripts on phones with no change in
behaviour. Both files loaded jQuery 3.6.0 from cdnjs before intl-tel-input and
used it for three calls. Every other line was already plain DOM.

**Stopped after step 1.** Step 2 (load intl-tel-input's `utils.js` only when the
phone field is used) is **not shipped**, because it cannot be done without
changing what a visitor sees. Reasons below. v5.21.0 is the final version here.

## What changed (both files, identical block)

| v5.20.0 | v5.21.0 |
|---|---|
| `typeof $ === 'undefined' \|\| …` guard | dropped. It only ran after jQuery's own onload, so it never decided anything. The `window.intlTelInput` half stays |
| `$('input[ms-code-phone-number]').each(function () { var input = this; …` | `document.querySelectorAll('input[ms-code-phone-number]').forEach(function (input) { …`. jQuery passes that selector to `querySelectorAll` itself: same static list, same order |
| `$(input).attr('ms-code-phone-number')` | `input.getAttribute('ms-code-phone-number')`. Never missing, because the input was found by that attribute |
| jQuery → intl-tel-input → utils, three serial loads | intl-tel-input → utils, two |

Plus the version bump in five places (two banners, two headers with changelog
entries, the `test-partnerstack` pin), `Tracks /demo v5.21.0` in the Ads header,
and comments.

## Every jQuery use (the inventory you asked for)

`grep` of both files for `$(`, `$.`, `jQuery`, `.on(`, `.ready`, `.val(`,
`.ajax`, `.data(`, `.css(`, `.show(`, `.hide(`, `.trigger(`, `.prop(`,
`:visible`. The only uses are the three above, all in `initPhoneInputs`.
- No event delegation, `.on`, `.trigger`, `return false`, `.val()`, `.css`,
  `.show/.hide`, `.data()`, `.prop` or `$(document).ready` anywhere.
- `closest`, `:checked`, `getAttribute('data-rh-router…')` and the
  `DOMContentLoaded` wait were already native, not jQuery. So the router id is
  read as a string ('6804', '6138'), with no `.data()` number conversion before
  or after.
- `.each` gave `this` = the element; `forEach` passes the element as the
  argument. The body is otherwise unchanged.

## Sizes (measured, not estimated)

| File | v5.20.0 raw | v5.21.0 raw | v5.20.0 gzip -9 | v5.21.0 gzip -9 |
|---|---|---|---|---|
| gushwork-form.js | 169,962 | 171,471 (+1,509) | 52,889 | 53,440 (+551) |
| gushwork-form-popup.js | 198,923 | 200,193 (+1,270) | 60,605 | 61,042 (+437) |

The files got slightly bigger (comments and changelog). What leaves every form
page load, measured by fetching the exact URLs the files request:

| From cdnjs | raw | gzip -9 | as sent (br) | after v5.21.0 |
|---|---|---|---|---|
| jquery 3.6.0 `jquery.min.js` | 89,501 | 30,889 | 27,938 | **no longer requested** |
| intl-tel-input 17.0.8 `intlTelInput.min.js` | 29,401 | 10,767 | 8,889 | still requested |
| intl-tel-input 17.0.8 `utils.js` | 246,621 | 54,931 | 45,687 | still requested (step 2 not shipped) |
| intl-tel-input CSS | 19,157 | 2,852 | | still requested |

So, per form page load: one request fewer, about 27 KB less sent, about 88 KB
less JavaScript to parse, and one serial round trip fewer before the phone
field is set up. That is bytes and request counts only. **I did not measure
main-thread time or real load time.** That needs a browser and none was used.

## Verified (run, not just read)

- **Full bar, run bare and read in full after the last commit: 17 suites, 6,669
  assertions, all pass.** Before this branch: 16 suites, 5,943. The only count
  changes are the new suite (726). `test-session-payload` stays at 173: one
  assertion was rewritten (it pinned jQuery as the first script load; it now
  checks the first `loadScript` call, whatever it loads).
- **New suite `tests/test-form-differential.js`** runs v5.20.0 (read from git at
  `08ecc1e`) and the working-tree file **whole**, side by side, in a hand-built
  fake page made from the markup the live pages serve. It records every fetch
  (url, method, headers, body), dataLayer push, RevenueHero call (router id,
  submit payload, embed, open), navigation, history entry, storage and cookie
  write, console line, DOM change, timer and intl-tel-input call. It requires
  OLD and NEW to be **identical, in order**. 54 compared cases, plus seven
  runs that pin the intended differences (below):
  - `/demo` (with and without the needs checkboxes), `/ai-demo`,
    `/lead-to-order`, `/order-to-cash`, `/aeo`, `/start`, a lander with no
    phone field, and one with two phone fields. Each page on its live file,
    and a few on the other file.
  - B2B, B2C (and "actually we're B2B"), Mixed (waitlist); organic, Google cpc
    CRM and AEO campaigns, an L2O campaign on a non-ad medium, an O2C campaign
    from the 30-day cookie; `b@g.ai`; a gmail lead (phone required, India
    flag); a server non-ICP block; a cached client block; the calendar hold;
    an invalid phone then fixed (Enter key, Back, Escape); RevenueHero failing;
    `MEETING_BOOKED` delivered twice; GTM's dataLayer already present; the
    script running after the page parsed (defer); utils or intl-tel-input
    failing to load, or loading but defining nothing.
  - Each case runs twice. **Aligned:** the libraries reach NEW when they
    reached OLD, and everything matches, timestamps included. **Same
    latency:** NEW sets the phone up a round trip sooner, and everything the
    visitor does after load, the final page and the set of page-load effects
    all still match.
  - It also checks the result itself, not just sameness: router ids (6138 by
    default, 6804 on CRM pages, CRM campaigns and the AI-CRM tick), the exact
    redirect strings (`/thank-you?attendeeName=Ada&industry=real_estate` and
    `…insurance`), the exact dataLayer objects, E.164 phones (`+12015550123`,
    `+912015550123`), whether `/submit` was sent, `disqualified` on
    `/partial`, and that no console line contains an email.
- **16 mutations, each CAUGHT** by `measure.js --mutation`, with all 17 suites
  completed at their usual counts. In `/demo`: phone selector changed, keyup
  formatter dropped, GTM push moved after step 3, `/thank-you` path changed,
  default router 6138→6139, utils loaded before intl-tel-input, float-label
  timer 100→150 ms, `attendeeName` renamed, the `typeof window.intlTelInput`
  guard removed, the L2O campaign rule dropped. In the Ads file:
  preferred-country split changed, `_iti` not attached, only the first phone
  field set up, phone setup run before utils, `user_data` renamed,
  `/order-to-cash` dropped from `B2C_ALLOWED_PATHS`. Nine of the 16 were caught
  only by the new suite: no existing suite runs the phone setup. Only the
  "loads but defines nothing" case reaches the guard, so without that case the
  guard removal could not have been caught. I added the case before running
  that mutation, so this is reasoned from the code, not measured. The
  mutations ran against commit `c48dc6c`; the commits after it change comments
  and add the head-pin cases, and the bar was re-run green after each. I
  committed before each run and restored from a byte-exact backup.

## The differences that remain (read these)

1. **HEAD-PINNED /ai-demo: a timing window where the phone field is no longer
   set up. This is the one real risk.** /ai-demo loads the Ads file in its
   `<head>`. Every other form page loads it in the footer (I fetched 18 live
   pages). Its phone input has no `ms-code-phone-number`, and the Ads backfill
   adds it at `DOMContentLoaded`, which waits for the whole page, the
   parser-blocking body scripts included. `initPhoneInputs` looks once, when
   utils lands. **v5.20.0 already loses this race** whenever the page is slower
   than three serial cdnjs loads. In that case the field is never set up, there
   is no number check, and raw digits are sent. With jQuery gone, NEW looks one
   round trip sooner, so the window where it misses gets wider: OLD set the
   field up and NEW does not. The suite pins this at three page-ready times. I
   cannot measure how often it happens without a browser. **Not fixed here,
   because a fix changes behaviour too.** Two options, your call:
   - (a) In `initPhoneInputs`'s caller, wait for `DOMContentLoaded` when
     `document.readyState === 'loading'`. That is three lines. NEW then sets
     the field up whenever OLD did, and also in the case where OLD missed it
     (a fix on top).
   - (b) While re-pinning, move /ai-demo's tag from head to footer like every
     other page. That removes the race for old and new.
2. **`window.jQuery` / `window.$` are no longer set by these files.** Every
   Webflow page loads its own jQuery 3.5.1. v5.20.0 **overwrote** it with 3.6.0
   once cdnjs answered, so Webflow pages now keep their own copy. The only
   inline page script using `$` on the 18 pages I fetched is the video modal
   (`$('.video-modal-bg')`, `fadeIn`, `on('click')`). It runs at
   `DOMContentLoaded` and works with either version. The Astro pages
   (gushwork-web) load no jQuery, so there `window.jQuery` is now absent.
   Nothing in gushwork-web's `src/` uses it, and its tracking review lists
   jQuery as "nothing depends on it". **GTM tags are in no repo and were not
   checked.** A custom HTML tag that calls `$` and relied on our copy would now
   break on the Astro pages.
3. **The jQuery-failure path.** Before: there was no `onerror`, so if the cdnjs
   jQuery request failed, the chain stopped, the phone field stayed plain (no
   number check) and raw digits were sent. That failure can no longer happen
   because the request is gone. The intl-tel-input and utils failure paths are
   unchanged and still fail open the same way (suite checks both).

## Step 2: why lazy `utils.js` was not shipped

Checked against the intl-tel-input 17.0.8 source cdnjs serves:
- `isValidNumber()` returns `null` when utils is not loaded.
  `validateStep2` does `if (!phoneEl._iti.isValidNumber())`, so a visitor who
  types a valid number and clicks before utils arrives would get "Please enter
  a valid phone number." and be stopped. Waiting for utils inside validation
  makes the step asynchronous, which changes the step flow. Skipping the check
  weakens validation. Both are ruled out.
- `getNumber()` returns `""` without utils, and the form then falls back to the
  raw typed value. So `/submit` and RevenueHero would get raw digits instead of
  E.164 for anyone who submits before it lands.
- The keyup/change formatter is skipped until utils loads, so the number shows
  unformatted while typing. When utils lands late, intl-tel-input reformats the
  field under the visitor.
- An untouched phone field is fine either way (an empty field skips the check),
  but a visitor who tabs in and types straight away races a 45.7 KB (br)
  download on a phone.

A version that only starts utils when step 2 appears would still have the same
race for fast visitors. It would save the download for people who leave at step
1, but it is a behaviour change, so it would need your decision.

## NOT verified (never run)

- A real browser, on any page. No main-thread or load-time numbers. No look at
  the real intl-tel-input, real jQuery or real RevenueHero. The suite's jQuery is
  a stand-in for exactly the three calls v5.20.0 made (anything else throws), and
  intl-tel-input and RevenueHero are fakes that record calls. The fake page does
  not model partial parsing, so "head pin" is modelled only as a late
  `DOMContentLoaded`.
- GTM tags (see 2).
- The suite needs `git` (it reads the reference with `git show`). CLAUDE.md
  still says "sixteen" suites. I did not edit CLAUDE.md. Adding the new suite to
  its list and the "boots a route" note is yours to do or ask for.

**Shipping it** still needs the pin step after a merge, both files to the same
40-character SHA (Webflow per page, gushwork-web with new SRI hashes), and then
the banner should show `v5.21.0` / `v5.21.0-ads`.

## Review and follow-up (17 Oct 2026, after the agent's report)

**Deliberate behaviour change added: the phone setup waits for the page to be ready.**
`/ai-demo` loads the Ads file in its `<head>` (checked on the live page). Its phone field is only tagged at DOMContentLoaded, and the setup looks once, when `utils.js` lands. Dropping jQuery made that look a round trip sooner, so the miss (plain field, no number check, raw digits to Salesforce and RevenueHero) would have become more common. Both files now wait for DOMContentLoaded when the page is still loading. On a footer pin the page is normally parsed by then, so the setup runs at once, as before. On a footer page where `utils.js` lands before parsing finishes, the setup now runs a little later than in v5.20.0. The version stays v5.21.0, because v5.21.0 was never shipped.

**Verified by the reviewer, not carried over from the agent's report:**
- jQuery was used in exactly one place (the phone setup), and the replacements match. The loop body has no `return false`. The old files already need a modern browser (`=>`, `const`, `?.`, `NodeList.forEach` elsewhere), so no browser support is lost.
- No tag in the live GTM container (version 72) calls jQuery, so the "a GTM tag relied on the form's jQuery" risk is closed.
- Sizes re-measured: raw bytes match the agent's figures exactly. Gzip differs by a few bytes (a different gzip build). jQuery: 89,501 raw, 27,938 as sent.
- Full bar run bare: 17 suites, 6,699 assertions, 0 failed.

**Test gaps found in review, and closed:**
- Every case arrived from Google, so the "came from another gushwork.ai page" branch never ran: renaming `gw_previous_page` SURVIVED. Two cases now arrive from gushwork.ai pages. Re-run: CAUGHT.
- The head-pin expectations now pin the fix: NEW sets the phone up at every page-ready time, and the only difference from OLD is the case OLD got wrong.
- Removing the new wait from the Ads file: CAUGHT. Removing it from `gushwork-form.js` alone SURVIVED (no `/demo` head-pin case). A timing check now requires the setup to run at or after page-ready in both files. Re-run: CAUGHT.
- Also CAUGHT in review: the phone error message changed, the phone sent to RevenueHero without its `+`, and the floating label not synced.

**Still not run:** any real browser. The suite is a model, not a page.
