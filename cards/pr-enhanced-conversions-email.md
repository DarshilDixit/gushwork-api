# PR: the GTM events carry the lead's email (form v5.20.0)

Branch `forms/enhanced-conversions-email`, from `main` at `c1e2fb7`. 16 Oct 2026.
**Not merged: waiting for your review.** No Webflow page was re-pinned and no
page was touched. Nothing about who is blocked, which leads fire Meta, routing,
the B2C rule, or any server call changes.

**Why:** the Ads specialist asked for it. Google Ads enhanced conversions got
the email by letting the Google tag scrape the page for an email field. That
can quietly stop working when the site moves from Webflow to Astro. Handing the
email over at submit means GTM's Ads tags can send it as user-provided data. It
is also the address our server-side conversion upload matches on.

**What (both form files, identical code):**
- `gw_form_submitted` and `gw_demo_booked` now push the email with the event:

  ```js
  { event: 'gw_form_submitted', user_data: { email: 'name@company.com' } }
  { event: 'gw_demo_booked',    user_data: { email: 'name@company.com' } }
  ```

  For GTM: map a User-Provided Data variable's email to the Data Layer
  variable `user_data.email`. The email is trimmed and lowercased, and sent as
  plain text (Google hashes it). It is not hashed in the browser, because these
  files have no SHA-256 helper. No phone, because the phone field falls back to
  raw typed text when the phone library has not loaded, so it is not reliably
  in E.164.
- The email comes from `formState.email`. That is the address verified at step
  1 and the same one `/submit` sends to the server. Neither push site had a
  normalised copy; they read `formState.email` and the new helper normalises
  it. Nothing reads the form field again.
- **Each event now fires once per page load.** That is new behaviour. On
  `/demo`, pressing Back on the calendar returns to step 2, and pressing Next
  again ran the whole submit again and pushed a second `gw_form_submitted` for
  the same person. That happens today on `main`. A repeated `MEETING_BOOKED`
  would also have pushed twice. The guard only lives in memory.
- The `MEETING_BOOKED` console line now prints only the meeting id and time.
  Before, it printed RevenueHero's whole `attributes` object, which can include
  the guest's details. I could not check what RevenueHero actually puts in it,
  so I narrowed the line to be safe.
- Version bump, five places: two banners, two header comments with changelog
  entries, and the pinned test. Also `Tracks /demo v5.20.0` in the Ads header.

**Not changed:** the event names; when the events fire; the test-email guard;
the non-ICP returns (the client-side cached block and the server backstop both
still return before the push); `/submit`, `/booking-confirmed` and their
payloads; Meta; RevenueHero; the step flow; storage. The email goes nowhere
new: not into a URL, cookie, storage key or console line. (`gw_email` in
localStorage and the `gw_enrich_<email>` keys were already there before this.)

**Verified (run, not just read):**
- Full bar, run bare and read in full: all 16 suites pass, 5943 assertions.
  The baseline moved from 5899 to 5943 and was saved. The only change is
  `test-session-payload` going from 129 to 173 (22 new assertions per file).
- The new section runs the REAL `handleStep2Next` and `initRHBookingListener`,
  lifted from each file along with the helpers they call, against stubs. For
  both files it checks:
  - a submit with `'  LowerCased@Trimmed.COM '` pushes exactly
    `{event:'gw_form_submitted', user_data:{email:'lowercased@trimmed.com'}}`;
  - a second submit on the same page pushes nothing more;
  - `MEETING_BOOKED` pushes `gw_demo_booked` with the same email, and a repeat
    pushes nothing;
  - any other message pushes nothing;
  - an existing dataLayer gets added to, not replaced;
  - `b@g.ai` pushes neither event;
  - a server-blocked submit and a client-blocked submit push nothing;
  - a blank email pushes the event with no `user_data`;
  - `/booking-confirmed` is still posted with the meeting id;
  - no console line contains the email, including when RevenueHero's fixture
    carries it.
- The tests for "nothing was pushed" also check that the flow got past the
  push site (it reached step 3, or it redirected) and that the handler's
  `catch` never ran. So a handler that crashed cannot pass them.
- Nine mutations, each one reported CAUGHT by `measure.js --mutation` with all
  16 suites completed and the usual 5943 assertions. In `/demo`: no lowercasing,
  no `user_data`, no once-guard, test-email guard removed from the submit push,
  push moved above the non-ICP backstop, booking log back to printing the whole
  object. In the Ads file: no trimming, test-email guard removed from the
  booking push, the backstop's `return` removed. Each was undone and the tree
  checked clean before the final bar.

**NOT verified (never run):**
- A real browser, real GTM or a real RevenueHero embed. I did not check GTM
  Preview or Tag Assistant, and I did not check whether Google accepts the
  user-provided data. Someone has to map `user_data.email` in GTM. Until they
  do, the new key does nothing.
- What RevenueHero's `MEETING_BOOKED` message really contains. My test fixture
  is made up.

**Worth knowing:**
- **`gw_form_submitted` still fires for a lead the calendar hold later turns
  away.** The push happens before `awaitNonIcpVerdict`, which can still answer
  "blocked" and redirect. This is the same known gap already written up in that
  code. I left the timing alone as asked. Now that the event carries the email,
  that lead's email reaches GTM too.
- The `MEETING_BOOKED` listener does not check which origin the message came
  from, so any frame on the page could post one. That was true before this PR.
  The once-guard caps the damage at one event.
- `test-ads-parity` does not compare the new helper between the two files. The
  new tests run each file separately, which is what catches drift in either one
  (the Ads-only mutations above were caught that way).
- Shipping this still needs the Webflow step (or the Astro pin and SRI hash):
  pin both files to the merged `main` SHA, then check the banner shows
  `v5.20.0` / `v5.20.0-ads`.
