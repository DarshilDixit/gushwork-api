# PR 130: consent on every Google Ads conversion, behind a switch

Branch `feat/gads-consent`, from `main` after #129. Written 6 Oct 2026.
**Not merged: waiting for your review. Nothing has been sent for real.**

## What it does

When `GADS_CONSENT_GRANTED` is `true`, every conversion the Google Ads upload
sends says the person granted **Ad User Data** and **Ad Personalization**. That
is the same claim Lorenzo's sheet makes today.

## Nothing changes when this merges

- **The switch is off by default.** Only the exact text `true` turns it on;
  `True` or `yes` leave it off.
- **Off means the field isn't sent at all**, exactly as in #129. It is never
  sent as "denied" or "not specified".
- **The upload itself is still off.** No `GADS_` variable is set in Railway.
- **Nothing here changes which leads are blocked or which Meta events fire.**
  It changes one field on the Google request, and only when switched on.

## What Google's documentation says

From the Data Manager API reference, read today:

- **The field is `consent`, with two parts:**
  `{ "adUserData": …, "adPersonalization": … }`.
- **Each part takes one of three values:** `CONSENT_GRANTED`,
  `CONSENT_DENIED` or `CONSENT_STATUS_UNSPECIFIED`.
- **It can go on the whole request or on each event**, and *"User-level
  consent overrides request-level consent, and can be specified in each
  Event."*

I put it **on each event**, as you asked. That also means it's part of the
event saved before sending, so a retry sends exactly what the first try did.

## What I checked

- **Tests: all 15 suites pass, 5,219 assertions** (`measure.js --check`,
  read in full). The Google suite went from 152 to 170. The new tests check:
  - the switch is off by default, and a typo leaves it off;
  - when on, both parts carry exactly `CONSENT_GRANTED`, under the
    reference's field names;
  - when off, the field is absent from the event and from the request;
  - **a whole sweep, run twice** (on, then off), with the request that reaches
    the stubbed Google read back each time. On, it carries consent and is
    still validate-only. Off, the word "consent" appears nowhere in the body;
  - booting the real app with no consent setting still sends none.
- **Mutation testing: 8 of 8 caught.** I broke the code eight ways (always
  sent, never sent, on by default, the wrong value, one part missing, the
  setting ignored, and two more) and watched the tests fail. Two tests were
  weak on the first run, and I fixed them:
  - one **crashed** instead of failing;
  - one checked a protection that freezing the shared value already gives.
- **One live validate-only test, accepted by Google.** It used a made-up lead
  (`validate-test@example.com`, `+12025550123`, a fake click ID), built by
  this branch's own code with the switch on, and refused to run unless
  validate-only was set. The response, in full:

  ```
  HTTP 200
  { "requestId": "v-88f3507c-2711-4fcb-b8b5-1a307a5795ec" }
  ```

## Not verified

- **A 200 shows Google accepted the request, not that it checked the
  consent values.** Earlier today a made-up action ID was refused, so Google
  does check *some* fields in this mode. I haven't tried a deliberately wrong
  consent value to prove it checks this one. That would be one more
  validate-only call, if you want it.
- **Nothing has been sent for real.**

## Things to know

- **Switching it on doesn't change a conversion that's already queued.** A
  booking already claimed but not yet sent keeps the version without consent
  on its retries. Bookings only checked in validate-only mode are rebuilt
  when real sending starts, so they pick up the setting then.
- **Turning it on is your claim, not Google's default.** It tells Google
  every person consented, the same as Lorenzo's sheet. Whether that's true
  for every lead is a policy question, which is why it's a switch.

I'll stop here until you say merge.
